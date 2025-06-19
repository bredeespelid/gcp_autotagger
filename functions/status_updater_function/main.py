import os
import base64
import json
import logging
from datetime import datetime, timezone
import re 

from google.cloud import bigquery
import functions_framework

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

PROJECT_ID = os.environ.get('GCP_PROJECT')
BIGQUERY_DATASET_ID = os.environ.get('BIGQUERY_DATASET_ID')
BIGQUERY_TABLE_ID = os.environ.get('BIGQUERY_TABLE_ID')

def _get_file_format_from_path_for_status_updater(gcs_path_str):
    if not gcs_path_str: return None
    file_name_match = re.search(r'[^/]+$', gcs_path_str)
    if not file_name_match: return None
    file_name = file_name_match.group(0)
    suffix_match = re.search(r'(\.[^.]+)$', file_name)
    return suffix_match.group(1).lower() if suffix_match else None

@functions_framework.cloud_event
def main_handler(event):
    # ... (som før)
    if not all([PROJECT_ID, BIGQUERY_DATASET_ID, BIGQUERY_TABLE_ID]): logger.critical("StatusUpdater: BQ env vars mangler."); return
    try:
        log_entry_str = base64.b64decode(event.data['message']['data']).decode('utf-8'); log_entry = json.loads(log_entry_str)
        proto_payload = log_entry.get('protoPayload', {}); method_name = proto_payload.get('methodName', '')
        if "delete" not in method_name.lower(): return
        resource_name = proto_payload.get('resourceName', ''); gcs_path = None
        if 'buckets/' in resource_name: path_parts = resource_name.split('buckets/', 1)[1]; gcs_path = f"gs://{path_parts.replace('/objects/', '/', 1)}"
        if not gcs_path: logger.error(f"StatusUpdater: Fikk ikke gcs_path fra: {resource_name}"); return
        update_status_in_bigquery(gcs_path)
    except Exception as e: logger.error(f"StatusUpdater: Feil: {e}", exc_info=True)


def update_status_in_bigquery(gcs_path):
    bq_client = bigquery.Client(project=PROJECT_ID)
    table_id_full = f"{PROJECT_ID}.{BIGQUERY_DATASET_ID}.{BIGQUERY_TABLE_ID}"
    
    is_object = bool(re.search(r'^gs://[^/]+/.+$', gcs_path))
    item_type_val = 'OBJECT' if is_object else 'BUCKET'
    file_format_val = _get_file_format_from_path_for_status_updater(gcs_path) if is_object else None

    # Definer alle kolonner som skal settes ved INSERT
    # De som ikke er relevante for et slettet, ukatalogisert item, settes til NULL
    insert_columns = "(gcs_path, item_type, hash_algorithm, hash_value, last_updated_utc, status, " \
                     "file_format, file_size_bytes, content_type, time_created_utc, time_updated_utc, " \
                     "storage_class, gcs_metadata, gcp_resource_details)"
    insert_values = "(@p_merge, @it_ins, NULL, NULL, @n_merge, 'DELETED', " \
                    "@ff_ins, NULL, NULL, NULL, NULL, " \
                    "NULL, NULL, NULL)"


    merge_query = f"""
    MERGE `{table_id_full}` AS T
    USING (SELECT @p_merge AS gcs_p_param) AS S ON T.gcs_path = S.gcs_p_param
    WHEN MATCHED THEN UPDATE SET status = 'DELETED', last_updated_utc = @n_merge
    WHEN NOT MATCHED BY TARGET THEN 
      INSERT {insert_columns} 
      VALUES {insert_values}
    """ 
    
    params = [
        bigquery.ScalarQueryParameter("p_merge", "STRING", gcs_path),
        bigquery.ScalarQueryParameter("n_merge", "TIMESTAMP", datetime.now(timezone.utc)),
        bigquery.ScalarQueryParameter("it_ins", "STRING", item_type_val),
        bigquery.ScalarQueryParameter("ff_ins", "STRING", file_format_val),
    ]
    try:
        job = bq_client.query(merge_query, job_config=bigquery.QueryJobConfig(query_parameters=params)); job.result()
        logger.info(f"StatusUpdater: Status for {gcs_path} satt/opprettet som DELETED i BQ.")
    except Exception as e: logger.error(f"StatusUpdater: Klarte ikke kjøre MERGE for {gcs_path}: {e}")