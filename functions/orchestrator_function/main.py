import os
import json
import base64
import logging
import hashlib
import re
from datetime import datetime, timezone

from google.cloud import storage, bigquery
import functions_framework

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

PROJECT_ID = os.environ.get('GCP_PROJECT')
BIGQUERY_DATASET_ID = os.environ.get('BIGQUERY_DATASET_ID')
BIGQUERY_TABLE_ID = os.environ.get('BIGQUERY_TABLE_ID')
MANAGED_BUCKET_LABEL_KEY = os.environ.get('MANAGED_BUCKET_LABEL_KEY', "autotagger-managed")
MANAGED_BUCKET_LABEL_VALUE = os.environ.get('MANAGED_BUCKET_LABEL_VALUE', "true")
try:
    DEFAULT_BUCKET_LABELS = json.loads(os.environ.get('DEFAULT_RESOURCE_LABELS_JSON', '{}'))
except json.JSONDecodeError:
    logger.warning("Orchestrator: Kunne ikke parse DEFAULT_RESOURCE_LABELS_JSON. Bruker tom dict.")
    DEFAULT_BUCKET_LABELS = {}

HASHING_ALGORITHMS = ['sha256', 'sha512', 'md5'] 

def _get_deterministic_hash_algorithm(gcs_path): # For bucket hash
    path_hash = hashlib.sha1(gcs_path.encode('utf-8')).digest()
    path_as_int = int.from_bytes(path_hash, 'big')
    return HASHING_ALGORITHMS[path_as_int % len(HASHING_ALGORITHMS)]

def _write_to_bigquery(bq_client, row_to_insert):
    if not all([PROJECT_ID, BIGQUERY_DATASET_ID, BIGQUERY_TABLE_ID]):
        logger.error("Orchestrator: BigQuery miljøvariabler er ikke satt.")
        return
    table_id_full = f"{PROJECT_ID}.{BIGQUERY_DATASET_ID}.{BIGQUERY_TABLE_ID}"
    errors = bq_client.insert_rows_json(table_id_full, [row_to_insert])
    if errors: logger.error(f"Orchestrator: Klarte ikke skrive BQ-rad for {row_to_insert['gcs_path']}: {errors}")
    else: logger.info(f"Orchestrator: Skrev {row_to_insert['item_type']} rad for {row_to_insert['gcs_path']} til BQ.")

def _sanitize_label_value(value_str, max_length=63):
    if value_str is None: return ""
    s = str(value_str).lower()
    s = re.sub(r'[^a-z0-9_-]', '', s)
    return s[:max_length] if len(s) > 0 else "_"

@functions_framework.cloud_event
def main_handler(event):
    # ... (som før)
    if not PROJECT_ID: logger.critical("Orchestrator: GCP_PROJECT mangler."); return
    try:
        log_entry_str = base64.b64decode(event.data['message']['data']).decode('utf-8'); log_entry = json.loads(log_entry_str)
        proto_payload = log_entry.get('protoPayload', {}); resource_name = proto_payload.get('resourceName', ''); method_name = proto_payload.get('methodName', '')
        if "storage.buckets.create" not in method_name.lower(): return
        bucket_name_parts = resource_name.split('buckets/');
        if len(bucket_name_parts) < 2 or not bucket_name_parts[1]: logger.error(f"Orch: Fikk ikke bucket-navn: '{resource_name}'"); return
        bucket_name = bucket_name_parts[-1].split('/')[0]
        handle_bucket_create(bucket_name, log_entry)
    except Exception as e: logger.error(f"Orch: Uventet feil: {e}", exc_info=True)


def handle_bucket_create(bucket_name, log_entry):
    logger.info(f"Orchestrator: Behandler opprettelse av bucket '{bucket_name}'.")
    storage_client = storage.Client(project=PROJECT_ID)
    bq_client = bigquery.Client(project=PROJECT_ID)
    
    try:
        bucket = storage_client.get_bucket(bucket_name)
    except Exception as e_get_bucket:
        logger.error(f"Orchestrator: Kunne ikke hente bucket-objekt for '{bucket_name}': {e_get_bucket}. Avslutter.")
        return

    # --- Labels ---
    principal_email = log_entry.get('protoPayload', {}).get('authenticationInfo', {}).get('principalEmail', 'unknown')
    created_by = _sanitize_label_value(principal_email.split('@')[0].replace('.', '-'))
    labels_to_apply = {**DEFAULT_BUCKET_LABELS}
    labels_to_apply['createdby'] = created_by if created_by else "unknown"
    created_on_date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d") 
    log_timestamp_str = log_entry.get('timestamp') 
    if log_timestamp_str:
        try: log_dt = datetime.fromisoformat(log_timestamp_str.replace('Z', '+00:00')); created_on_date_str = log_dt.strftime("%Y-%m-%d")
        except ValueError: logger.warning(f"Orch: Kunne ikke parse log timestamp '{log_timestamp_str}'.")
    labels_to_apply['createdon'] = created_on_date_str
    labels_to_apply[MANAGED_BUCKET_LABEL_KEY] = MANAGED_BUCKET_LABEL_VALUE
    # Cost center er fjernet som en *egen* label som Orchestrator setter her, den blir en del av bucket.labels

    try:
        bucket.reload(); current_labels = bucket.labels if bucket.labels else {}
        # La labels_to_apply overskrive hvis DM 2.0 satt labels som overlapper med defaults
        final_labels = {**current_labels, **labels_to_apply} 
        # Sørg for at cost-center er med i final_labels hvis den var i default_bucket_labels
        # eller hvis den skal utledes fra bucket-navn (den logikken er fjernet for enkelhet her, men kan legges til)
        if 'cost-center' not in final_labels and DEFAULT_BUCKET_LABELS.get('cost-center'): # Eksempel
            final_labels['cost-center'] = DEFAULT_BUCKET_LABELS.get('cost-center')

        bucket.labels = final_labels; bucket.patch()
        logger.info(f"Orchestrator: Labels {final_labels} satt for bucket '{bucket_name}'.")
        # Etter patch, last labels på nytt for å få med det som faktisk ble satt på GCS
        bucket.reload()
        actual_gcs_labels = dict(bucket.labels) if bucket.labels else {}

    except Exception as e: 
        logger.error(f"Orchestrator: Kunne ikke sette labels for bucket '{bucket_name}': {e}", exc_info=True)
        actual_gcs_labels = {} # Fallback til tom dict hvis feil

    # --- Forbered data for BigQuery ---
    gcs_path = f"gs://{bucket_name}"
    chosen_algo = _get_deterministic_hash_algorithm(gcs_path)
    hasher = hashlib.new(chosen_algo); hasher.update(bucket_name.encode('utf-8'))
    current_ts_iso = datetime.now(timezone.utc).isoformat()
    
    time_created_val = bucket.time_created.isoformat() if bucket.time_created else None
    time_updated_val = bucket.updated.isoformat() if bucket.updated else None

    gcp_resource_details_dict = {
        # "time_created_utc" og "time_updated_utc" er nå egne kolonner, men kan også inkluderes her for fullstendighet
        "gcs_time_created_raw": str(bucket.time_created) if bucket.time_created else None, # Råformat
        "gcs_time_updated_raw": str(bucket.updated) if bucket.updated else None,       # Råformat
        "location": bucket.location,
        "default_storage_class": bucket.storage_class, # Merk: dette er default for bucket, ikke nødvendigvis for alle objekter
        "versioning_enabled": bucket.versioning_enabled,
        "labels": actual_gcs_labels, # Bruk de faktiske labelene fra GCS
        "project_number": bucket.project_number,
        # "owner_entity": bucket.owner.get('entity') if bucket.owner else None, # Ofte None
        "requester_pays": bucket.requester_pays,
        "retention_period_seconds": bucket.retention_period,
        "default_kms_key_name": bucket.default_kms_key_name,
        "default_event_based_hold": bucket.default_event_based_hold,
        "location_type": bucket.location_type,
    }
    # Legg til IAM konfig hvis ønskelig, men kan være stor/kompleks
    # gcp_resource_details_dict["iam_configuration"] = {
    #     "uniform_bucket_level_access_enabled": bucket.iam_configuration.uniform_bucket_level_access_enabled if bucket.iam_configuration else None
    # }


    row_to_insert = {
        "gcs_path": gcs_path, "item_type": "BUCKET",
        "hash_algorithm": chosen_algo, "hash_value": hasher.hexdigest(),
        "last_updated_utc": current_ts_iso, 
        "status": "ACTIVE",
        "gcs_metadata": None, 
        "file_format": None,
        # Sett individuelle kolonner
        "file_size_bytes": None, 
        "content_type": None,
        "time_created_utc": time_created_val or current_ts_iso, # Bruk GCS-tid, fallback til nå
        "time_updated_utc": time_updated_val or time_created_val or current_ts_iso, # Bruk GCS-tid, fallback
        "storage_class": bucket.storage_class, # Bucketens default storage class
        # cost_center er ikke lenger en egen toppnivå kolonne
        "gcp_resource_details": json.dumps(gcp_resource_details_dict, sort_keys=True),
    }
    _write_to_bigquery(bq_client, row_to_insert)