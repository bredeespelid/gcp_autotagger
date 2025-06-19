# GCP Auto-Tagger

## Project Structure

## System Architecture

![System Overview](https://github.com/bredeespelid/gcp_autotagger/blob/main/info/info.png?raw=true)

The repository is organized as follows:

```text
.
├── functions/
│   ├── batch_worker_function/
│   │   ├── main.py
│   │   └── requirements.txt
│   ├── full_reconciliation_worker_function/
│   │   ├── main.py
│   │   └── requirements.txt
│   ├── orchestrator_function/
│   │   ├── main.py
│   │   └── requirements.txt
│   └── status_updater_function/
│       ├── main.py
│       └── requirements.txt
├── terraform/
│   ├── main.tf
│   ├── variables.tf
│   ├── apis.tf
│   ├── bigquery.tf
│   ├── pubsub_logging.tf
│   ├── storage.tf
│   ├── functions_orchestrator.tf
│   ├── functions_batch_worker.tf
│   ├── functions_status_updater.tf
│   ├── functions_reconciliation.tf
│   └── iam.tf
└── README.md

```

This project implements an advanced system for automatically "tagging" (cataloging) Google Cloud Storage (GCS) resources (buckets and objects) into a central BigQuery table. The system utilizes path-based hashes for unique identification and tracks various metadata and properties.

## Architectural Overview

The system consists of several Cloud Functions working in concert, orchestrated by Cloud Scheduler and Pub/Sub-based event triggers:

1.  **Orchestrator Function (`orchestrator_function`):**
    *   **Trigger:** Pub/Sub message from a Cloud Logging Sink (filtered for `storage.buckets.create` events).
    *   **Responsibilities:**
        *   When a new GCS bucket is created, this function patches the bucket with default labels (e.g., `createdby`, `createdon`, `managed-by`).
        *   Creates an initial record for the new bucket in the BigQuery catalog via streaming insert (`insert_rows_json`).

2.  **Batch Worker Function (`batch_worker_function`):**
    *   **Trigger:** Cloud Scheduler (e.g., every 30 minutes (or 6h or 12, 24?). Runs as an HTTP-triggered function.
    *   **Responsibilities ("InsertOnlyV1"):**
        *   Scans all GCS buckets tagged as "managed".
        *   Identifies new buckets not present in the BigQuery catalog and adds them via streaming insert.
        *   Identifies new objects within "managed" buckets not present in the BigQuery catalog and adds them via streaming insert.
        *   Patches GCS objects with standardized metadata (including a path-based hash and a marker indicating they have been processed).
        *   Updates a label on the GCS bucket (`autotagger-last-scanned-utc`) to indicate the last quick scan time.
    *   **Important:** This function does *not* perform DML `UPDATE` operations on existing BigQuery records to avoid conflicts with BigQuery's streaming buffer.

3.  **Full Reconciliation Worker Function (`full_reconciliation_worker_function`):**
    *   **Trigger:** Cloud Scheduler (e.g., every 1 (or 4-6-12-24?) hours, significantly less frequent than the Batch Worker). Runs as an HTTP-triggered function.
    *   **Responsibilities ("DML Updates"):**
        *   Scans all GCS buckets tagged as "managed".
        *   Compares metadata and properties of existing buckets in GCS against the BigQuery catalog and performs DML `UPDATE` operations if discrepancies are found.
        *   Compares metadata and properties of existing objects in GCS against the BigQuery catalog and performs DML `UPDATE` operations if discrepancies are found.
        *   Identifies objects present in the BigQuery catalog (with status `ACTIVE`) but no longer existing in GCS, and performs DML `UPDATE` operations to set their status to `DELETED`.
        *   Updates a label on the GCS bucket (`autotagger-last-reconciled-utc`) to indicate the last full reconciliation time.

4.  **Status Updater Function (`status_updater_function`):**
    *   **Trigger:** Pub/Sub message from a Cloud Logging Sink (filtered for `storage.objects.delete` and `storage.buckets.delete` events).
    *   **Responsibilities:**
        *   When a GCS object or bucket is deleted, this function uses a `MERGE` statement in BigQuery.
        *   If the record exists, its status is updated to `DELETED`.
        *   If the record does *not* exist (e.g., an object deleted before the `batch_worker` could catalog it), a new record is created with status `DELETED` to ensure the deletion is registered.

## Important Note on GCS Bucket Interaction & Storage Classes

**Manual interaction with GCS objects (e.g., downloading, viewing metadata through the console, or direct API GET requests) for buckets utilizing "cold" storage classes like `ARCHIVE` or `COLDLINE` can incur significant costs and potentially move objects to hotter, more expensive storage tiers.**

*   **Avoid Manual File Access in Cold Storage:** If a bucket is intended for long-term archival (`ARCHIVE` storage class), direct access to its objects should be minimized or avoided unless a data retrieval operation is explicitly intended. Operations like listing objects are generally low-cost, but accessing object content or metadata can trigger retrieval fees and early deletion fees if objects are moved from cold storage prematurely.
*   **Automated System Design:** This auto-tagger system is designed to primarily interact with object *metadata* for cataloging purposes. The `batch_worker` and `full_reconciliation_worker` perform `list_blobs` and `blob.reload()` (which fetches metadata). These operations are generally inexpensive even on colder tiers.
*   **Cost Implications:** Be mindful of the storage class of your buckets. Frequent, automated metadata reads are typically fine, but any workflow (manual or automated) that frequently *retrieves object data* from cold tiers will lead to higher costs. This system itself does not download object content.

**The principle is: Let the automated system manage the cataloging. Avoid manual operations on objects in managed buckets, especially if they are in cold storage, to prevent unintended cost or storage class transitions.**

## Key Infrastructure Components (defined in Terraform)

*   **Google BigQuery Dataset & Table:**
    *   Dataset: `data_asset_catalog`
    *   Table: `gcs_hash_inventory` (partitioned on `last_updated_utc`, clustered on `item_type`, `gcs_path`)
*   **Google Cloud Functions (Gen 2):** One for each of the above functions, each with its own Service Account.
*   **Google Cloud Scheduler:** Two jobs to trigger the `batch_worker_function` (frequently) and the `full_reconciliation_worker_function` (less frequently).
*   **Google Pub/Sub Topic:** `gcs-autotagger-lifecycle-events` to receive messages from the logging sink.
*   **Google Cloud Logging Sink:** Filters relevant GCS audit logs and sends them to the Pub/Sub topic.
*   **IAM Roles and Permissions:** Carefully defined for each Service Account to adhere to the principle of least privilege (as practically as possible).
*   **GCS Bucket for Source Code:** A central bucket (`<project_id>-gcf-autotagger-src`) to store the zipped function code archives.


## BigQuery Catalog Data Example

The `gcs_hash_inventory` table in BigQuery stores detailed information about cataloged GCS buckets and objects. Here's an example of what some key fields in a few records might look like:

| gcs_path                                                                    | item_type | status | hash_value (SHA256)                                       | file_format | file_size_bytes | time_created_utc          | last_updated_utc          | gcs_metadata (JSON snippet)                      | gcp_resource_details (JSON snippet)              |
| :-------------------------------------------------------------------------- | :-------- | :----- | :-------------------------------------------------------- | :---------- | :-------------- | :------------------------ | :------------------------ | :----------------------------------------------- | :----------------------------------------------- |
| `gs://gcf-v2-sources-37446377071-europe-west1`                               | BUCKET    | ACTIVE | `aaea04b1...46b44c28`                                     | `null`      | `null`          | `2025-06-18 13:18:26 UTC` | `2025-06-19 12:55:01 UTC` | `null`                                           | `{"labels":{"autotagger-managed":"true", ...}}`  |
| `gs://gcf-v2-sources-37446377071-europe-west1/gcs-autotag-batch-worker/...zip` | OBJECT    | ACTIVE | `c33bb864...518c2471fd`                                     | `.zip`      | `5136`          | `2025-06-19 12:53:37 UTC` | `2025-06-19 12:55:01 UTC` | `{"autometadata_by_batch_worker_utc": ...}`       | `{"generation":1750337617198906, "crc32c...}`    |
| `gs://new_update`                                                           | BUCKET    | ACTIVE | `a0dd9225...2178a67f`                                     | `null`      | `null`          | `2025-06-19 11:38:02 UTC` | `2025-06-19 12:55:01 UTC` | `null`                                           | `{"labels":{"managed-by":"gcs-autotagger", ...}}`|
| `gs://new_update/TTI.00001624.sgy`                                          | OBJECT    | ACTIVE | `41d47ec6...988b554d74`                                     | `.sgy`      | `2256760`       | `2025-06-19 12:13:37 UTC` | `2025-06-19 12:55:01 UTC` | `{"data-source":"gcs-autotagger-upload", ...}`   | `{"md5_hash_gcs":"+JLqLId1IuNz...", ...}`       |
| `gs://some-bucket/some-deleted-object.txt`                                  | OBJECT    | DELETED| `(hash_value_of_deleted_object)`                          | `.txt`      | `(size_before_delete)`| `(creation_time)`         | `(deletion_scan_time)`    | `(metadata_before_delete_or_null)`             | `(details_before_delete_or_null)`              |

# Accessing Catalog Data in Microsoft Fabric (Example)

This example demonstrates how to connect to BigQuery via Microsoft Fabric Notebooks, extract data, and write it to Delta Lake format. It also includes logic for incremental refresh using an `update_time` column.

```python
# STEP 1: (In Fabric Notebook) Ensure Spark session is available
# spark = SparkSession.builder.appName("GCS_Inventory_Ingest").getOrCreate()  # Usually pre-configured

# STEP 2: Read GCP service account key from Lakehouse Files
# Assumes the key file 'bq-fabric-key.json' is in the 'Files' root of your Lakehouse
df_key = spark.read.option("multiline", "true").json("Files/bq-fabric-key.json")
key_dict = df_key.toPandas().iloc[0].to_dict()

# STEP 3: Import necessary modules
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
from datetime import datetime
from pyspark.sql.functions import col, max as spark_max

# STEP 4: Create BigQuery client
credentials = service_account.Credentials.from_service_account_info(key_dict)
client = bigquery.Client(credentials=credentials, project=key_dict["project_id"])

# STEP 5: Get latest update_time from existing Delta Table (for incremental logic)
try:
    latest_ts = spark.read.table("gcs_hash_inventory_delta").agg(spark_max("update_time")).collect()[0][0]
    latest_ts_str = latest_ts.strftime("%Y-%m-%d %H:%M:%S")
    print(f"Latest update_time found: {latest_ts_str}")
except:
    latest_ts_str = None
    print("No existing Delta table found or empty. Performing full load.")

# STEP 6: Define SQL query
if latest_ts_str:
    query = f"""
    SELECT *
    FROM `sandbox-gcp-tags.data_asset_catalog.gcs_hash_inventory`
    WHERE update_time > TIMESTAMP('{latest_ts_str}')
    """
else:
    query = """
    SELECT *
    FROM `sandbox-gcp-tags.data_asset_catalog.gcs_hash_inventory`
    """

# STEP 7: Execute query and load data into a pandas DataFrame
df_bq_pandas = client.query(query).to_dataframe()

# STEP 8: (Optional) Convert nested fields to strings if needed
# json_cols_to_stringify = ["gcs_metadata", "gcp_resource_details"]
# for col in json_cols_to_stringify:
#     if col in df_bq_pandas.columns:
#         df_bq_pandas[col] = df_bq_pandas[col].apply(lambda x: json.dumps(x) if x is not None else None)

# STEP 9: Convert pandas DataFrame to Spark DataFrame
df_spark = spark.createDataFrame(df_bq_pandas)

# STEP 10: Write data to Delta Table (append for incremental, overwrite for full load)
if latest_ts_str:
    df_spark.write.format("delta").mode("append").saveAsTable("gcs_hash_inventory_delta")
else:
    df_spark.write.format("delta").mode("overwrite").saveAsTable("gcs_hash_inventory_delta")

# STEP 11: Verify by reading from Delta Table
display(spark.read.table("gcs_hash_inventory_delta"))
```
