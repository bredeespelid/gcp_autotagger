resource "google_service_account" "batch_worker_sa" {
  project      = var.project_id
  account_id   = "gcs-autotag-batch-worker"
  display_name = "SA for GCS Autotagger Batch Worker"
}

resource "google_project_iam_member" "batch_worker_sa_permissions" {
  project  = var.project_id
  for_each = toset(["roles/storage.admin", "roles/bigquery.dataEditor", "roles/bigquery.jobUser"])
  role     = each.key
  member   = "serviceAccount:${google_service_account.batch_worker_sa.email}"
}

data "archive_file" "batch_worker_zip" {
  type        = "zip"
  source_dir  = "../functions/batch_worker_function/"
  output_path = "/tmp/batch_worker_function.zip"
}

resource "google_storage_bucket_object" "batch_worker_archive" {
  name   = "sources/batch-worker-${data.archive_file.batch_worker_zip.output_md5}.zip"
  bucket = google_storage_bucket.function_source_bucket.name
  source = data.archive_file.batch_worker_zip.output_path
}

resource "google_cloudfunctions2_function" "batch_worker_function" {
  project     = var.project_id
  name        = "gcs-autotag-batch-worker"
  location    = var.region
  description = "Catalogs NEW GCS items and patches GCS metadata."

  build_config {
    runtime     = "python312"
    entry_point = "main_batch_handler"
    source {
      storage_source {
        bucket = google_storage_bucket.function_source_bucket.name
        object = google_storage_bucket_object.batch_worker_archive.name
      }
    }
  }

  service_config {
    service_account_email = google_service_account.batch_worker_sa.email
    timeout_seconds       = 540
    available_memory      = "512Mi"
    environment_variables = {
      DEFAULT_OBJECT_METADATA_JSON = jsonencode(var.default_object_metadata)
      GCP_PROJECT                  = var.project_id
      BIGQUERY_DATASET_ID          = google_bigquery_dataset.hashing_catalog_dataset.dataset_id
      BIGQUERY_TABLE_ID            = google_bigquery_table.hash_inventory_table.table_id
      MANAGED_BUCKET_LABEL_KEY     = "autotagger-managed"
      MANAGED_BUCKET_LABEL_VALUE   = "true"
      METADATA_APPLIED_MARKER_KEY  = "autometadata_by_batch_worker_utc"
      LAST_SCANNED_LABEL_KEY       = "autotagger-last-scanned-utc"
      HASH_ALGO_METADATA_KEY       = "autotagger_hash_algorithm"
      HASH_VALUE_METADATA_KEY      = "autotagger_hash_value"
      PATH_HASH_ALGORITHM          = "PATH_BASED_SHA256"
    }
  }
  depends_on = [
    google_project_service.apis,
    google_storage_bucket_iam_member.gcf_service_agent_can_read_source
  ]
}

resource "google_cloud_scheduler_job" "trigger_batch_worker" {
  project     = var.project_id
  name        = "trigger-gcs-autotag-batch-worker"
  description = "Triggers the GCS Autotagger Batch Worker (InsertOnly) function periodically."
  schedule    = "*/30 * * * *"
  time_zone   = "Etc/UTC"

  http_target {
    uri         = google_cloudfunctions2_function.batch_worker_function.service_config[0].uri
    http_method = "POST"
    oidc_token {
      service_account_email = google_service_account.batch_worker_sa.email
    }
  }
  depends_on = [google_cloudfunctions2_function.batch_worker_function]
}

resource "google_cloud_run_service_iam_member" "scheduler_invokes_batch_worker" {
  project  = google_cloudfunctions2_function.batch_worker_function.project
  location = google_cloudfunctions2_function.batch_worker_function.location
  service  = google_cloudfunctions2_function.batch_worker_function.name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.batch_worker_sa.email}"
}
