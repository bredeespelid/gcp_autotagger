# GCP Metadata-Tagger

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


The system catalogs GCS buckets and objects into BigQuery using SHA256-based hashes. It updates metadata based on Pub/Sub triggers and scheduled jobs.

## Cloud Functions

### Orchestrator
- Triggered by `bucket.create` events (Logging → Pub/Sub)
- Applies default labels to the new bucket
- Inserts bucket metadata into BigQuery

### Batch Worker
- Triggered by Cloud Scheduler
- Scans all managed buckets
- Adds new buckets/objects to BigQuery
- Patches object metadata
- Sets `autotagger-last-scanned-utc` label

### Reconciliation Worker
- Triggered by Cloud Scheduler (less frequent)
- Compares GCS vs. BigQuery state
- Updates or marks deleted records via DML
- Sets `autotagger-last-reconciled-utc` label

### Status Updater
- Triggered by `object.delete` and `bucket.delete` events
- Uses `MERGE` to mark records as `DELETED` in BigQuery
- Creates a `DELETED` record if it didn’t exist before

## Cold Storage Considerations

Avoid manual access to buckets with `ARCHIVE` or `COLDLINE` classes. This system only fetches metadata (`list_blobs`, `blob.reload()`), which is low-cost. Accessing content may incur retrieval or early deletion fees.

## Terraform Components

- BigQuery: `data_asset_catalog.gcs_hash_inventory`
- Cloud Scheduler jobs for batch and reconciliation
- Pub/Sub + Logging Sink for event flow
- Cloud Functions (Gen 2), one per component
- IAM roles scoped per function
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
