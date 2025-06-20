import os
import json
import logging
import hashlib
from datetime import datetime, timezone, timedelta # timedelta brukes for sammenligning
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
LAST_SCANNED_LABEL_KEY_RECON = os.environ.get('LAST_SCANNED_LABEL_KEY_RECON', "autotagger-last-reconciled-utc") # Egen label for denne

# To generate path-based hashes for CONSISTENCY, even though they are primarily set by batch_worker
PATH_HASH_ALGORITHM_NAME_FOR_OBJECTS = os.environ.get('PATH_HASH_ALGORITHM', "PATH_BASED_SHA256")
BUCKET_PATH_HASH_ALGORITHM_NAME = "BUCKET_PATH_SHA256"


# --- Helper functions (reused from batch_worker) ---

def _generate_sha256_hash(input_string: str) -> str:

        """
    Generate a SHA-256 hash from the given input string.

    Args:
        input_string (str): The input text to be hashed.

    Returns:
        str: The SHA-256 hash represented as a hexadecimal string.
    """
    
    hasher = hashlib.sha256()
    hasher.update(input_string.encode('utf-8'))
    return hasher.hexdigest()

def _get_file_format(blob_name_with_path: str):
        """
    Extract the file extension (format) from a given blob or file path.

    Args:
        blob_name_with_path (str): The full path or name of the blob or file.

    Returns:
        str or None: The lowercase file extension including the dot (e.g., '.json'),
                     or None if no extension is found or the input is empty.
    """

    if not blob_name_with_path: return None
    file_name = pathlib.Path(blob_name_with_path).name
    suffix = pathlib.Path(file_name).suffix
    return suffix.lower() if suffix else None

def _safe_getattr(obj, attr, default=None):
        """
    Safely retrieve an attribute from an object, returning a default value if the attribute is missing.

    Args:
        obj: The object to inspect.
        attr (str): The name of the attribute to retrieve.
        default: The value to return if the attribute does not exist. Defaults to None.

    Returns:
        Any: The value of the attribute if it exists, otherwise the default value.
    """
    return getattr(obj, attr, default)



def _safe_isoformat(dt_obj):
        """
    Safely convert a datetime object to an ISO 8601 formatted string.

    Args:
        dt_obj (datetime or None): The datetime object to format.

    Returns:
        str or None: The ISO 8601 formatted string if dt_obj is provided, otherwise None.
    """
    
    return dt_obj.isoformat() if dt_obj else None

def _collect_gcp_resource_details_for_bucket(gcs_bucket_obj, actual_gcs_labels):

        """
    Collect metadata and configuration details from a Google Cloud Storage bucket object.

    Args:
        gcs_bucket_obj: The GCS bucket object from which metadata is extracted.
        actual_gcs_labels (dict or None): Dictionary of GCS labels applied to the bucket.

    Returns:
        dict: A dictionary containing key metadata fields, including creation and update timestamps,
              location, storage class, versioning status, encryption settings, IAM configuration, and labels.
    """
    
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

        """
    Extract metadata and configuration details from a GCS blob (object).

    Args:
        gcs_blob_obj: The Google Cloud Storage blob object to extract metadata from.

    Returns:
        dict: A dictionary containing object-level metadata including creation and update timestamps,
              generation and metageneration, checksums (CRC32C and MD5), KMS key reference, hold settings,
              retention expiration, and encryption algorithm if applicable.
    """
    
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

def _build_scalar_query_parameters(param_dict: dict) -> list: 
        """
    Construct a list of BigQuery ScalarQueryParameter objects from a dictionary of key-value pairs.

    This function infers BigQuery-compatible types for each parameter based on the value's Python type
    and key name conventions (e.g., keys ending in "_utc" are treated as TIMESTAMP, certain fields as JSON).
    The output is suitable for use in parameterized DML and query jobs.

    Args:
        param_dict (dict): A dictionary where keys are parameter names and values are their corresponding values.

    Returns:
        list: A list of bigquery.ScalarQueryParameter objects with appropriate types.
    """
    typed_params = []
    for key, value in param_dict.items():
        bq_type = "STRING"
        if isinstance(value, bool): bq_type = "BOOL"
        elif isinstance(value, int): bq_type = "INT64"
        elif isinstance(value, float): bq_type = "FLOAT64"
        elif (key.endswith("_utc") or key.endswith("_timestamp")) and isinstance(value, str):
            bq_type = "TIMESTAMP"
        elif key in ["gcs_metadata", "gcp_resource_details"] and isinstance(value, str):
            bq_type = "JSON"
        elif value is None:
            if key.endswith("_utc") or key.endswith("_timestamp"): bq_type = "TIMESTAMP"
            elif key in ["gcs_metadata", "gcp_resource_details"]: bq_type = "JSON"
            elif key == "file_size_bytes": bq_type = "INT64"
        typed_params.append(bigquery.ScalarQueryParameter(key, bq_type, value))
    return typed_params

@functions_framework.http
def main_reconciliation_handler(request):

    """
    HTTP-triggered Cloud Function that performs a full reconciliation between GCS and BigQuery records.

    This function performs three main operations:
      1. **Bucket-level Reconciliation**: Updates metadata in BigQuery for managed GCS buckets
         if discrepancies are found between GCS and BQ.
      2. **Object Deletion Detection**: Identifies objects marked as 'ACTIVE' in BigQuery
         but no longer present in GCS, and updates their status to 'DELETED'.
      3. **Object Metadata Reconciliation**: Updates metadata for GCS objects in BigQuery
         if changes are detected (e.g., timestamps, storage class, file format, content type).

    The function assumes that all buckets are pre-inserted by a separate batch ingestion process.

    Args:
        request (flask.Request): The HTTP request triggering the function. The body is ignored.

    Returns:
        Tuple[str, int]: A message string and an HTTP status code.
                         Returns 200 on successful reconciliation, or 500 on error.
    """
    
    run_dt_utc = datetime.now(timezone.utc)
    run_ts_iso = run_dt_utc.isoformat()
    run_ts_label = run_dt_utc.strftime("%Y%m%d%H%M%S")

    logger.info(f"FullReconciliationWorker (DML_UpdatesV1) starter (Run: {run_ts_iso})...")
    if not all([PROJECT_ID, BIGQUERY_DATASET_ID, BIGQUERY_TABLE_ID]):
        logger.critical("FullReconciliationWorker: Nødvendige miljøvariabler mangler. Avslutter.")
        return ("Konfigurasjonsfeil", 500)

    storage_client = storage.Client(project=PROJECT_ID)
    bq_client = bigquery.Client(project=PROJECT_ID)
    total_bq_rows_updated_this_run = 0
    total_bq_rows_marked_deleted_this_run = 0
    bq_table_id_full = f"{PROJECT_ID}.{BIGQUERY_DATASET_ID}.{BIGQUERY_TABLE_ID}"

    try:
        for bucket_gcs_obj in storage_client.list_buckets():
            bucket_name_for_log = getattr(bucket_gcs_obj, 'name', 'UKJENT_BUCKET_NAVN')
            try:
                bucket_gcs_obj.reload()
                if not (bucket_gcs_obj.labels and bucket_gcs_obj.labels.get(MANAGED_BUCKET_LABEL_KEY) == MANAGED_BUCKET_LABEL_VALUE):
                    continue
            except Forbidden as e_forbidden:
                logger.warning(f"FullReconciliationWorker: Ingen tilgang til å sjekke/lese bucket {bucket_name_for_log}, hopper over: {e_forbidden}")
                continue
            except Exception as e:
                logger.warning(f"FullReconciliationWorker: Kunne ikke sjekke labels for bucket {bucket_name_for_log}, hopper over: {e}")
                continue

            bucket_name = bucket_gcs_obj.name
            logger.info(f"FullReconciliationWorker: Kjører full synkronisering for bucket: {bucket_name}")
            bucket_gcs_path = f"gs://{bucket_name}"

            # --- 1. Update the BUCKET row in BQ if necessary ---
            query_bucket_bq = f"""
                SELECT status, hash_algorithm, hash_value, gcp_resource_details,
                       time_created_utc, time_updated_utc, storage_class,
                       last_updated_utc AS bq_last_updated_utc
                FROM `{bq_table_id_full}`
                WHERE gcs_path = @path_b_check AND item_type = 'BUCKET' LIMIT 1
            """
            params_b_check = [bigquery.ScalarQueryParameter("path_b_check", "STRING", bucket_gcs_path)]
            try:
                bq_bucket_res_query = bq_client.query(query_bucket_bq, job_config=bigquery.QueryJobConfig(query_parameters=params_b_check))
                bq_bucket_res_list = list(bq_bucket_res_query.result())
                if not bq_bucket_res_list:
                    logger.warning(f"FullReconciliationWorker: Fant ikke bucket {bucket_gcs_path} i BQ. BatchWorker bør ha opprettet denne. Hopper over bucket-oppdatering.")
                else:
                    bq_b_data = bq_bucket_res_list[0]
                    gcs_bucket_details_dict = _collect_gcp_resource_details_for_bucket(bucket_gcs_obj, bucket_gcs_obj.labels)
                    
                    # Data fra GCS som skal sammenlignes/oppdateres
                    data_from_gcs_for_bucket_update = {
                        "status": "ACTIVE", # Antar ACTIVE for en administrert bucket som fortsatt eksisterer
                        "hash_algorithm": BUCKET_PATH_HASH_ALGORITHM_NAME, # Bør ikke endres
                        "hash_value": _generate_sha256_hash(bucket_gcs_path), # Bør ikke endres
                        "time_created_utc": _safe_isoformat(bucket_gcs_obj.time_created),
                        "time_updated_utc": _safe_isoformat(bucket_gcs_obj.updated),
                        "storage_class": _safe_getattr(bucket_gcs_obj, 'storage_class'),
                        "gcp_resource_details": json.dumps(gcs_bucket_details_dict, sort_keys=True),
                        # last_updated_utc settes ved BQ update
                    }

                    upd_dict_b = {}
                    for key, gcs_val in data_from_gcs_for_bucket_update.items():
                        bq_val = getattr(bq_b_data, key, None)
                        gcs_val_comp = gcs_val; bq_val_comp = bq_val
                        if key.endswith("_utc") or key.endswith("_timestamp"):
                            gcs_val_comp = (gcs_val or "").split('.')[0]
                            bq_val_comp = (_safe_isoformat(bq_val) or "").split('.')[0]
                        elif key == "gcp_resource_details": pass # Sammenlignes som strenger

                        if gcs_val_comp != bq_val_comp:
                            upd_dict_b[key] = gcs_val
                    
                    if upd_dict_b:
                        upd_dict_b["last_updated_utc"] = run_ts_iso
                        upd_clauses_b = [f"{k} = @{k}" for k in upd_dict_b.keys()]
                        upd_params_b = _build_scalar_query_parameters(upd_dict_b)
                        upd_params_b.append(bigquery.ScalarQueryParameter("p_b_where", "STRING", bucket_gcs_path))
                        
                        query_b_upd = f"UPDATE `{bq_table_id_full}` SET {', '.join(upd_clauses_b)} WHERE gcs_path = @p_b_where AND item_type = 'BUCKET'"
                        job_b_upd = bq_client.query(query_b_upd, job_config=bigquery.QueryJobConfig(query_parameters=upd_params_b))
                        job_b_upd.result() # Vent på ferdigstillelse
                        if job_b_upd.num_dml_affected_rows:
                            total_bq_rows_updated_this_run += job_b_upd.num_dml_affected_rows
                            logger.info(f"FullReconciliationWorker: Oppdaterte BQ-rad for bucket {bucket_name}.")

            except Exception as e_bq_bucket_update:
                error_details = getattr(e_bq_bucket_update, 'errors', None)
                logger.error(f"FullReconciliationWorker: Feil ved BQ bucket update for {bucket_name}: {e_bq_bucket_update}. Details: {error_details}", exc_info=True)


            # --- 2. Mark deleted objects in BQ ---
            try:
                gcs_obj_paths_set = {f"gs://{bucket_name}/{b.name}" for b in storage_client.list_blobs(bucket_name)}
                
                query_bq_active_obj = f"SELECT gcs_path FROM `{bq_table_id_full}` WHERE STARTS_WITH(gcs_path, @bp_recon) AND item_type = 'OBJECT' AND status = 'ACTIVE'"
                params_bq_active_obj = [bigquery.ScalarQueryParameter("bp_recon", "STRING", f"{bucket_gcs_path}/")]
                job_config_active_obj = bigquery.QueryJobConfig(query_parameters=params_bq_active_obj)
                
                bq_active_paths_query = bq_client.query(query_bq_active_obj, job_config=job_config_active_obj)
                bq_active_paths_set = {r.gcs_path for r in bq_active_paths_query.result()}
                
                to_mark_deleted_paths = list(bq_active_paths_set - gcs_obj_paths_set)

                if to_mark_deleted_paths:
                    logger.info(f"FullReconciliationWorker: Fant {len(to_mark_deleted_paths)} objekter i BQ for {bucket_name} som mangler i GCS.")
                    for i in range(0, len(to_mark_deleted_paths), 200): # Batch DML
                        current_batch_paths = to_mark_deleted_paths[i:i+200]
                        params_del_obj = [
                            bigquery.ArrayQueryParameter("p_del_list", "STRING", current_batch_paths),
                            bigquery.ScalarQueryParameter("n_del_ts", "TIMESTAMP", run_dt_utc)
                        ]
                        query_del_obj = f"UPDATE `{bq_table_id_full}` SET status = 'DELETED', last_updated_utc = @n_del_ts WHERE gcs_path IN UNNEST(@p_del_list) AND item_type = 'OBJECT'"
                        job_del = bq_client.query(query_del_obj, job_config=bigquery.QueryJobConfig(query_parameters=params_del_obj))
                        job_del.result()
                        num_affected_del = job_del.num_dml_affected_rows if job_del.num_dml_affected_rows is not None else 0
                        if num_affected_del > 0:
                            total_bq_rows_marked_deleted_this_run += num_affected_del
                            logger.info(f"FullReconciliationWorker: Markerte {num_affected_del} objekter som DELETED i BQ for {bucket_name} (batch {i//200 + 1}).")
            except Exception as e_recon_deleted:
                error_details = getattr(e_recon_deleted, 'errors', None)
                logger.error(f"FullReconciliationWorker: Feil ved markering av slettede objekter for {bucket_name}: {e_recon_deleted}. Details: {error_details}", exc_info=True)


            # --- 3. Update individual OBJECT rows in BQ if necessary ---

            for blob in storage_client.list_blobs(bucket_name):
                full_gcs_path = f"gs://{bucket_name}/{blob.name}"
                try:
                    blob.reload() # Få ferskeste data fra GCS
                    
                    # Retrieve existing row from BQ

                    query_bq_obj_select = f"""SELECT gcs_path, status, hash_value, hash_algorithm, file_format,
                                                   gcs_metadata, gcp_resource_details,
                                                   file_size_bytes, content_type, time_created_utc, time_updated_utc, storage_class,
                                                   last_updated_utc AS bq_last_updated_utc
                                           FROM `{bq_table_id_full}`
                                           WHERE gcs_path = @p_obj_check AND item_type = 'OBJECT' LIMIT 1""" # Antar at den er ACTIVE hvis den skal oppdateres
                    params_bq_obj_select = [bigquery.ScalarQueryParameter("p_obj_check", "STRING", full_gcs_path)]
                    bq_obj_list_query = bq_client.query(query_bq_obj_select, job_config=bigquery.QueryJobConfig(query_parameters=params_bq_obj_select))
                    bq_obj_res_list = list(bq_obj_list_query.result())

                    if not bq_obj_res_list:
                        logger.debug(f"FullReconciliationWorker: Fant ikke objekt {full_gcs_path} i BQ for oppdatering. BatchWorker bør ha opprettet det.")
                        continue # Gå til neste blob, dette er ikke en insert-jobb

                    bq_data_for_comparison = bq_obj_res_list[0]
                    if bq_data_for_comparison.status == 'DELETED': # Ikke oppdater et objekt som allerede er markert slettet av denne jobben eller status_updater
                        logger.debug(f"FullReconciliationWorker: Objekt {full_gcs_path} er allerede markert DELETED. Hopper over oppdatering.")
                        continue

                    gcs_object_details_dict = _collect_gcp_resource_details_for_object(blob)
                    file_format_for_bq = _get_file_format(full_gcs_path)
                    
                    # GCS metadata here should reflect what's on the object, as batch_worker has patched it
                    # We rely on GCS having the "authoritative" metadata that BQ should reflect

                    current_gcs_metadata = blob.metadata 

                    data_from_gcs_for_obj_update = {
                        "status": "ACTIVE", # Hvis det finnes i GCS og ikke er slettet, er det aktivt
                        # Hash algorithm/value should ideally not change for an existing path.
                        # If they do, it implies a fundamental change. For simplicity, we don't update them here,
                        # but one could add logic to verify/flag if they mismatch.
                        # "hash_algorithm": PATH_HASH_ALGORITHM_NAME_FOR_OBJECTS,
                        # "hash_value": _generate_sha256_hash(full_gcs_path),
                        "gcs_metadata": json.dumps(current_gcs_metadata, sort_keys=True) if current_gcs_metadata else None,
                        "gcp_resource_details": json.dumps(gcs_object_details_dict, sort_keys=True),
                        "file_format": file_format_for_bq,
                        "file_size_bytes": _safe_getattr(blob, 'size'),
                        "content_type": _safe_getattr(blob, 'content_type'),
                        "time_created_utc": _safe_isoformat(_safe_getattr(blob, 'time_created')), # Bør ikke endres mye
                        "time_updated_utc": _safe_isoformat(_safe_getattr(blob, 'updated')),
                        "storage_class": _safe_getattr(blob, 'storage_class')
                        # last_updated_utc settes av BQ-oppdateringen
                    }

                    upd_dict_obj = {}
                    for key, gcs_value in data_from_gcs_for_obj_update.items():
                        bq_value = getattr(bq_data_for_comparison, key, None)
                        gcs_val_comp = gcs_value; bq_val_comp = bq_value
                        if key.endswith("_utc") or key.endswith("_timestamp"):
                            gcs_ts_str = (gcs_value or "").split('.')[0] if gcs_value else ""
                            bq_ts_str = (_safe_isoformat(bq_value) or "").split('.')[0] if bq_value else ""
                            if gcs_ts_str != bq_ts_str: upd_dict_obj[key] = gcs_value
                        elif key in ["gcs_metadata", "gcp_resource_details"]:
                            if (gcs_value or "") != (bq_value or ""): upd_dict_obj[key] = gcs_value
                        elif gcs_value != bq_value: upd_dict_obj[key] = gcs_value
                    
                    if upd_dict_obj:
                        upd_dict_obj["last_updated_utc"] = run_ts_iso
                        update_set_clauses_obj = [f"{col} = @{col}" for col in upd_dict_obj.keys()]
                        query_upd_obj = f"UPDATE `{bq_table_id_full}` SET {', '.join(update_set_clauses_obj)} WHERE gcs_path = @gcs_p_u_obj AND item_type = 'OBJECT'"
                        
                        typed_params_upd_obj_list = _build_scalar_query_parameters(upd_dict_obj)
                        typed_params_upd_obj_list.append(bigquery.ScalarQueryParameter("gcs_p_u_obj", "STRING", full_gcs_path))
                        
                        job_upd_o = bq_client.query(query_upd_obj, job_config=bigquery.QueryJobConfig(query_parameters=typed_params_upd_obj_list))
                        job_upd_o.result()
                        if job_upd_o.num_dml_affected_rows:
                            total_bq_rows_updated_this_run += job_upd_o.num_dml_affected_rows
                            logger.info(f"FullReconciliationWorker: Oppdaterte BQ-rad for objekt {full_gcs_path}.")
                
                except NotFound: 
                    logger.warning(f"FullReconciliationWorker: Objekt {full_gcs_path} forsvant under GCS reload for oppdatering. Slettes i neste runde hvis ikke allerede gjort.")
                    continue
                except Exception as e_bq_obj_update:
                    error_details = getattr(e_bq_obj_update, 'errors', None)
                    logger.error(f"FullReconciliationWorker: Feil ved BQ object update for {full_gcs_path}: {e_bq_obj_update}. Details: {error_details}", exc_info=True)

           # Update GCS bucket label for this synchronization

            try:
                if bucket_gcs_obj.labels is None: bucket_gcs_obj.labels = {}
                bucket_gcs_obj.labels[LAST_SCANNED_LABEL_KEY_RECON] = run_ts_label
                bucket_gcs_obj.patch()
            except Exception as e_label:
                logger.error(f"FullReconciliationWorker: Kunne ikke oppdatere label {LAST_SCANNED_LABEL_KEY_RECON} for bucket {bucket_name}: {e_label}")

        logger.info(f"FullReconciliationWorker (DML_UpdatesV1) fullført. {total_bq_rows_updated_this_run} rader oppdatert. {total_bq_rows_marked_deleted_this_run} rader markert slettet i BQ.")
        return (f"Full Reconciliation run (DML_UpdatesV1) {run_ts_iso} completed.", 200)

    except Exception as e_main:
        logger.error(f"FullReconciliationWorker: Alvorlig feil i hovedløkke: {e_main}", exc_info=True)
        return (f"Error during Full Reconciliation run (DML_UpdatesV1) {run_ts_iso}", 500)
