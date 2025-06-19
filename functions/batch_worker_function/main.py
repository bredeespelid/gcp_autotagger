import os
import json
import logging
import hashlib
from datetime import datetime, timezone, timedelta # timedelta brukes kun for logging/sammenligning nå
import pathlib

from google.cloud import storage, bigquery
from google.cloud.exceptions import NotFound, PreconditionFailed, Forbidden
import functions_framework

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

PROJECT_ID = os.environ.get('GCP_PROJECT')
BIGQUERY_DATASET_ID = os.environ.get('BIGQUERY_DATASET_ID')
BIGQUERY_TABLE_ID = os.environ.get('BIGQUERY_TABLE_ID')
MANAGED_BUCKET_LABEL_KEY = os.environ.get('MANAGED_BUCKET_LABEL_KEY', "autotagger-managed")
MANAGED_BUCKET_LABEL_VALUE = os.environ.get('MANAGED_BUCKET_LABEL_VALUE', "true")
METADATA_APPLIED_MARKER_KEY = os.environ.get('METADATA_APPLIED_MARKER_KEY', 'autometadata_by_batch_worker_utc')
LAST_SCANNED_LABEL_KEY = os.environ.get('LAST_SCANNED_LABEL_KEY', "autotagger-last-scanned-utc") # For denne raske skanningen
HASH_ALGO_METADATA_KEY = os.environ.get('HASH_ALGO_METADATA_KEY', 'autotagger_hash_algorithm')
HASH_VALUE_METADATA_KEY = os.environ.get('HASH_VALUE_METADATA_KEY', 'autotagger_hash_value')

# Hentes fra miljøvariabel satt i Terraform (f.eks. "PATH_BASED_SHA256")
PATH_HASH_ALGORITHM_NAME_FOR_OBJECTS = os.environ.get('PATH_HASH_ALGORITHM', "PATH_BASED_SHA256")
# Fast konstant for bucket-stier/navn
BUCKET_PATH_HASH_ALGORITHM_NAME = "BUCKET_PATH_SHA256"

try:
    DEFAULT_OBJECT_METADATA = json.loads(os.environ.get('DEFAULT_OBJECT_METADATA_JSON', '{}'))
except json.JSONDecodeError:
    logger.warning("BatchWorker: Kunne ikke parse DEFAULT_OBJECT_METADATA_JSON. Bruker tom dict.")
    DEFAULT_OBJECT_METADATA = {}

def _generate_sha256_hash(input_string: str) -> str:
    hasher = hashlib.sha256()
    hasher.update(input_string.encode('utf-8'))
    return hasher.hexdigest()

def _sanitize_metadata_value(value):
    return str(value) if value is not None else ""

def _get_file_format(blob_name_with_path: str):
    if not blob_name_with_path: return None
    file_name = pathlib.Path(blob_name_with_path).name
    suffix = pathlib.Path(file_name).suffix
    return suffix.lower() if suffix else None

def _safe_getattr(obj, attr, default=None):
    """Hjelpefunksjon for å trygt hente attributter."""
    return getattr(obj, attr, default)

def _safe_isoformat(dt_obj):
    """Hjelpefunksjon for trygg ISO-formatering av datetime-objekter."""
    return dt_obj.isoformat() if dt_obj else None

def _collect_gcp_resource_details_for_bucket(gcs_bucket_obj, actual_gcs_labels):
    iam_config = _safe_getattr(gcs_bucket_obj, 'iam_configuration')
    details = {
        "gcs_time_created_raw": str(_safe_getattr(gcs_bucket_obj, 'time_created')),
        "gcs_time_updated_raw": str(_safe_getattr(gcs_bucket_obj, 'updated')),
        "location": _safe_getattr(gcs_bucket_obj, 'location'),
        "default_storage_class": _safe_getattr(gcs_bucket_obj, 'storage_class'),
        "versioning_enabled": _safe_getattr(gcs_bucket_obj, 'versioning_enabled'),
        "labels": dict(actual_gcs_labels) if actual_gcs_labels else {},
        "project_number": _safe_getattr(gcs_bucket_obj, 'project_number'),
        "requester_pays": _safe_getattr(gcs_bucket_obj, 'requester_pays'),
        "retention_period_seconds": _safe_getattr(gcs_bucket_obj, 'retention_period'),
        "default_kms_key_name": _safe_getattr(gcs_bucket_obj, 'default_kms_key_name'),
        "default_event_based_hold": _safe_getattr(gcs_bucket_obj, 'default_event_based_hold'),
        "location_type": _safe_getattr(gcs_bucket_obj, 'location_type'),
        "iam_configuration_uniform_bucket_level_access_enabled": _safe_getattr(iam_config, 'uniform_bucket_level_access_enabled') if iam_config else None
    }
    return details

def _collect_gcp_resource_details_for_object(gcs_blob_obj):
    cust_enc = _safe_getattr(gcs_blob_obj, 'customer_encryption')
    retention_exp_time = _safe_getattr(gcs_blob_obj, 'retention_expiration_time')
    details = {
        "gcs_time_created_raw": str(_safe_getattr(gcs_blob_obj, 'time_created')),
        "gcs_time_updated_raw": str(_safe_getattr(gcs_blob_obj, 'updated')),
        "generation": _safe_getattr(gcs_blob_obj, 'generation'),
        "metageneration": _safe_getattr(gcs_blob_obj, 'metageneration'),
        "crc32c_hash_gcs": _safe_getattr(gcs_blob_obj, 'crc32c'),
        "md5_hash_gcs": _safe_getattr(gcs_blob_obj, 'md5_hash'),
        "kms_key_name": _safe_getattr(gcs_blob_obj, 'kms_key_name'),
        "event_based_hold": _safe_getattr(gcs_blob_obj, 'event_based_hold'),
        "temporary_hold": _safe_getattr(gcs_blob_obj, 'temporary_hold'),
        "retention_expiration_time": _safe_isoformat(retention_exp_time),
        "customer_encryption_algorithm": _safe_getattr(cust_enc, 'algorithm') if cust_enc else None,
    }
    return details

# _build_scalar_query_parameters is NOT needed in this InsertOnlyV1 version of batch_worker
# as it does not run DML queries that require typed parameters.

@functions_framework.http
def main_batch_handler(request):
    run_dt_utc = datetime.now(timezone.utc)
    run_ts_iso = run_dt_utc.isoformat()
    run_ts_label = run_dt_utc.strftime("%Y%m%d%H%M%S")

    logger.info(f"BatchWorker (InsertOnlyV1) starter (Run: {run_ts_iso})...")
    if not all([PROJECT_ID, BIGQUERY_DATASET_ID, BIGQUERY_TABLE_ID]):
        logger.critical("BatchWorker: Nødvendige miljøvariabler mangler. Avslutter.")
        return ("Konfigurasjonsfeil", 500)

    storage_client = storage.Client(project=PROJECT_ID)
    bq_client = bigquery.Client(project=PROJECT_ID)
    total_new_items_processed_for_insert = 0
    total_bq_rows_inserted_this_run = 0
    bq_table_id_full = f"{PROJECT_ID}.{BIGQUERY_DATASET_ID}.{BIGQUERY_TABLE_ID}"

    try:
        for bucket_gcs_obj in storage_client.list_buckets():
            bucket_name_for_log = getattr(bucket_gcs_obj, 'name', 'UKJENT_BUCKET_NAVN')
            try:
                bucket_gcs_obj.reload()
                if not (bucket_gcs_obj.labels and bucket_gcs_obj.labels.get(MANAGED_BUCKET_LABEL_KEY) == MANAGED_BUCKET_LABEL_VALUE):
                    continue
            except Forbidden as e_forbidden:
                logger.warning(f"BatchWorker: Ingen tilgang til å sjekke/lese bucket {bucket_name_for_log}, hopper over: {e_forbidden}")
                continue
            except Exception as e:
                logger.warning(f"BatchWorker: Kunne ikke sjekke labels for bucket {bucket_name_for_log}, hopper over: {e}")
                continue

            bucket_name = bucket_gcs_obj.name
            logger.info(f"BatchWorker: Skanner administrert bucket for nye items: {bucket_name}")
            bq_rows_to_insert_this_iteration = []
            bucket_gcs_path = f"gs://{bucket_name}"

            # --- 1. Prosesser BUCKET (Insert hvis ny) ---
            # Sjekk om bucket allerede finnes i BQ
            query_bucket_bq_exists = f"SELECT gcs_path FROM `{bq_table_id_full}` WHERE gcs_path = @path_b_check AND item_type = 'BUCKET' LIMIT 1"
            params_b_check_exists = [bigquery.ScalarQueryParameter("path_b_check", "STRING", bucket_gcs_path)]
            bucket_bq_exists = False
            e_select_bucket_occurred = False # Flag for å vite om den feilet
            try:
                bq_bucket_res_query = bq_client.query(query_bucket_bq_exists, job_config=bigquery.QueryJobConfig(query_parameters=params_b_check_exists))
                bq_bucket_res = list(bq_bucket_res_query.result())
                bucket_bq_exists = len(bq_bucket_res) > 0
            except Exception as e_bq_select_b:
                e_select_bucket_occurred = True
                error_details = getattr(e_bq_select_b, 'errors', None)
                logger.error(f"BatchWorker: Feil ved SELECT for bucket {bucket_gcs_path} i BQ: {e_bq_select_b}. Details: {error_details}", exc_info=True)
                # Fortsett, men vi kan ikke vite om bucket eksisterer
            
            if not bucket_bq_exists and not e_select_bucket_occurred: # Bare insert hvis den *ikke* finnes OG vi *ikke* fikk feil ved sjekk
                gcs_bucket_details_dict = _collect_gcp_resource_details_for_bucket(bucket_gcs_obj, bucket_gcs_obj.labels)
                data_for_bucket_bq_insert = {
                    "gcs_path": bucket_gcs_path,
                    "item_type": "BUCKET",
                    "status": "ACTIVE",
                    "hash_algorithm": BUCKET_PATH_HASH_ALGORITHM_NAME,
                    "hash_value": _generate_sha256_hash(bucket_gcs_path),
                    "time_created_utc": _safe_isoformat(bucket_gcs_obj.time_created),
                    "time_updated_utc": _safe_isoformat(bucket_gcs_obj.updated), # Ved første insert er updated = created
                    "storage_class": _safe_getattr(bucket_gcs_obj, 'storage_class'),
                    "gcp_resource_details": json.dumps(gcs_bucket_details_dict, sort_keys=True),
                    "last_updated_utc": run_ts_iso, # Tidspunkt for denne BQ-operasjonen
                    "gcs_metadata": None, "file_format": None,
                    "file_size_bytes": None, "content_type": None
                }
                bq_rows_to_insert_this_iteration.append(data_for_bucket_bq_insert)
                total_new_items_processed_for_insert +=1
                logger.info(f"BatchWorker: Bucket {bucket_gcs_path} er ny eller mangler i BQ, planlegger INSERT.")
            elif bucket_bq_exists:
                logger.debug(f"BatchWorker: Bucket {bucket_gcs_path} eksisterer allerede i BQ. Oppdateringer håndteres av separat prosess.")
            elif e_select_bucket_occurred:
                 logger.warning(f"BatchWorker: Kunne ikke verifisere om bucket {bucket_gcs_path} eksisterer i BQ pga feil. Hopper over potensiell insert.")


            # --- 2. Håndter individuelle OBJEKTER (Insert hvis nye, GCS patch) ---
            # Hent alle eksisterende objektstier i BQ for denne bucket for å unngå SELECT per objekt
            existing_bq_object_paths = set()
            e_select_existing_obj_occurred = False
            try:
                query_existing_obj = f"SELECT gcs_path FROM `{bq_table_id_full}` WHERE item_type = 'OBJECT' AND STARTS_WITH(gcs_path, @bucket_prefix)"
                params_existing_obj = [bigquery.ScalarQueryParameter("bucket_prefix", "STRING", f"{bucket_gcs_path}/")]
                existing_obj_query_job = bq_client.query(query_existing_obj, job_config=bigquery.QueryJobConfig(query_parameters=params_existing_obj))
                for row in existing_obj_query_job.result():
                    existing_bq_object_paths.add(row.gcs_path)
            except Exception as e_select_existing:
                e_select_existing_obj_occurred = True
                error_details = getattr(e_select_existing, 'errors', None)
                logger.error(f"BatchWorker: Kunne ikke hente eksisterende objekter fra BQ for {bucket_name}: {e_select_existing}. Details: {error_details}. Faller tilbake til individuell sjekk.", exc_info=True)

            for blob in storage_client.list_blobs(bucket_name):
                full_gcs_path = f"gs://{bucket_name}/{blob.name}"
                try:
                    # GCS Metadata Patching (forblir likt)
                    blob.reload()
                    object_path_hash_algo_for_bq = PATH_HASH_ALGORITHM_NAME_FOR_OBJECTS
                    object_path_hash_value_for_bq = _generate_sha256_hash(full_gcs_path)
                    effective_gcs_meta_for_patch = {**(blob.metadata or {}), **DEFAULT_OBJECT_METADATA}
                    effective_gcs_meta_for_patch[METADATA_APPLIED_MARKER_KEY] = run_ts_iso
                    effective_gcs_meta_for_patch[HASH_ALGO_METADATA_KEY] = object_path_hash_algo_for_bq
                    effective_gcs_meta_for_patch[HASH_VALUE_METADATA_KEY] = object_path_hash_value_for_bq
                    sanitized_meta_for_patch = {k: _sanitize_metadata_value(v) for k, v in effective_gcs_meta_for_patch.items()}

                    object_custom_gcs_meta_for_bq_after_patch = blob.metadata
                    if sanitized_meta_for_patch != blob.metadata:
                        blob.metadata = sanitized_meta_for_patch
                        try:
                            blob.patch()
                            object_custom_gcs_meta_for_bq_after_patch = blob.metadata # Oppdater etter vellykket patch
                            logger.info(f"BatchWorker: Patchet GCS metadata for {full_gcs_path}")
                        except NotFound: logger.warning(f"BatchWorker: Objekt {full_gcs_path} forsvant under GCS patch."); continue
                        except Forbidden: logger.warning(f"BatchWorker: Ingen tilgang til å patche metadata for {full_gcs_path}. Fortsetter til BQ-logikk.");
                        except PreconditionFailed: logger.warning(f"BatchWorker: GCS Patch Precondition Failed for {full_gcs_path}. Fortsetter til BQ-logikk.");
                        except Exception as e_patch: logger.error(f"BatchWorker: Feil ved GCS patch for {full_gcs_path}: {e_patch}. Fortsetter til BQ-logikk.")

                    # Sjekk om objektet finnes i BQ
                    obj_bq_exists_current_blob = False
                    if not e_select_existing_obj_occurred: # Bruk pre-hentet sett hvis det var vellykket
                        obj_bq_exists_current_blob = full_gcs_path in existing_bq_object_paths
                    else: # Fallback til individuell sjekk
                        try:
                            query_bq_obj_indiv = f"SELECT gcs_path FROM `{bq_table_id_full}` WHERE gcs_path = @p_obj_check AND item_type = 'OBJECT' LIMIT 1"
                            params_bq_obj_indiv = [bigquery.ScalarQueryParameter("p_obj_check", "STRING", full_gcs_path)]
                            bq_obj_list_query = bq_client.query(query_bq_obj_indiv, job_config=bigquery.QueryJobConfig(query_parameters=params_bq_obj_indiv))
                            obj_bq_exists_current_blob = len(list(bq_obj_list_query.result())) > 0
                        except Exception as e_bq_select_obj_indiv:
                            error_details = getattr(e_bq_select_obj_indiv, 'errors', None)
                            logger.error(f"BatchWorker: Feil ved individuell SELECT for object {full_gcs_path} i BQ: {e_bq_select_obj_indiv}. Details: {error_details}. Hopper over BQ insert sjekk.", exc_info=True)
                            continue

                    if not obj_bq_exists_current_blob:
                        # Objektet er nytt, forbered for BQ insert
                        gcs_object_details_dict = _collect_gcp_resource_details_for_object(blob)
                        file_format_for_bq = _get_file_format(full_gcs_path)
                        insert_data_obj = {
                            "gcs_path": full_gcs_path, "item_type": "OBJECT", "status": "ACTIVE",
                            "hash_algorithm": object_path_hash_algo_for_bq,
                            "hash_value": object_path_hash_value_for_bq,
                            "last_updated_utc": run_ts_iso,
                            "gcs_metadata": json.dumps(object_custom_gcs_meta_for_bq_after_patch, sort_keys=True) if object_custom_gcs_meta_for_bq_after_patch else None,
                            "gcp_resource_details": json.dumps(gcs_object_details_dict, sort_keys=True),
                            "file_format": file_format_for_bq,
                            "file_size_bytes": _safe_getattr(blob, 'size'),
                            "content_type": _safe_getattr(blob, 'content_type'),
                            "time_created_utc": _safe_isoformat(_safe_getattr(blob, 'time_created')),
                            "time_updated_utc": _safe_isoformat(_safe_getattr(blob, 'updated')),
                            "storage_class": _safe_getattr(blob, 'storage_class')
                        }
                        bq_rows_to_insert_this_iteration.append(insert_data_obj)
                        total_new_items_processed_for_insert += 1
                        logger.info(f"BatchWorker: Objekt {full_gcs_path} er nytt eller mangler i BQ, planlegger INSERT.")
                    else:
                        logger.debug(f"BatchWorker: Objekt {full_gcs_path} eksisterer allerede i BQ. Oppdateringer håndteres av separat prosess.")

                except NotFound: logger.warning(f"BatchWorker: Objekt {full_gcs_path} forsvant under behandling. Hopper over."); continue
                except Exception as e_blob_processing:
                    error_details = getattr(e_blob_processing, 'errors', None)
                    logger.error(f"BatchWorker: Uventet feil for objekt {blob.name if 'blob' in locals() else 'UKJENT_BLOB'} ({full_gcs_path}) i {bucket_name}: {e_blob_processing}. Details: {error_details}", exc_info=True)


            if bq_rows_to_insert_this_iteration:
                try:
                    errors = bq_client.insert_rows_json(bq_table_id_full, bq_rows_to_insert_this_iteration)
                    if errors:
                        logger.error(f"BatchWorker: BQ insert feil for {bucket_name}:")
                        for error_entry in errors:
                            index = error_entry.get('index', -1)
                            row_data_str = "UKJENT_RAD_DATA_VED_FEIL"
                            if 0 <= index < len(bq_rows_to_insert_this_iteration):
                                try: row_data_str = json.dumps(bq_rows_to_insert_this_iteration[index])
                                except TypeError: row_data_str = str(bq_rows_to_insert_this_iteration[index]) # Fallback
                            logger.error(f"  Feilet på rad data (index {index}): {row_data_str}")
                            logger.error(f"  BQ Feil detaljer: {error_entry.get('errors')}")
                    else:
                        total_bq_rows_inserted_this_run += len(bq_rows_to_insert_this_iteration)
                        logger.info(f"BatchWorker: Satte inn {len(bq_rows_to_insert_this_iteration)} nye rader for {bucket_name}.")
                except Exception as e_insert_json:
                    error_details = getattr(e_insert_json, 'errors', None)
                    logger.error(f"BatchWorker: Kritisk feil under insert_rows_json for {bucket_name}: {e_insert_json}. Details: {error_details}", exc_info=True)

            # Oppdater GCS bucket label for denne raske skanningen
            try:
                if bucket_gcs_obj.labels is None: bucket_gcs_obj.labels = {}
                bucket_gcs_obj.labels[LAST_SCANNED_LABEL_KEY] = run_ts_label
                bucket_gcs_obj.patch()
            except Exception as e_label_patch:
                logger.error(f"BatchWorker: Label oppdateringsfeil for {bucket_name}: {e_label_patch}")

        logger.info(f"BatchWorker (InsertOnlyV1) fullført. {total_new_items_processed_for_insert} nye items behandlet for BQ insert. {total_bq_rows_inserted_this_run} rader satt inn i BQ.")
        return (f"Batch run (InsertOnlyV1) {run_ts_iso} completed.", 200)

    except Exception as e_main_loop:
        logger.error(f"BatchWorker: Alvorlig feil i hovedløkke: {e_main_loop}", exc_info=True)
        return (f"Error during batch run (InsertOnlyV1) {run_ts_iso}", 500)