resource "google_service_account" "full_reconciliation_worker_sa" {
  project      = var.project_id
  account_id   = "gcs-autotag-recon-worker"
  display_name = "SA for GCS Autotagger Full Reconciliation Worker"
}

resource "google_project_iam_member" "full_reconciliation_worker_sa_permissions" {
  project = var.project_id
  for_each = toset(["roles/storage.admin", "roles/bigquery.dataEditor", "roles/bigquery.jobUser"])
  role    = each.key
  member  = "serviceAccount:${google_service_account.full_reconciliation_worker_sa.email}"
}

data "archive_file" "full_reconciliation_worker_zip" {
  type        = "zip"
  source_dir  = "../functions/full_reconciliation_worker_function/"
  output_path = "/tmp/full_reconciliation_worker_function.zip"
}

resource "google_storage_bucket_object" "full_reconciliation_worker_archive" {
  name   = "sources/full-reconciliation-worker-${data.archive_file.full_reconciliation_worker_zip.output_md5}.zip"
  bucket = google_storage_bucket.function_source_bucket.name
  source = data.archive_file.full_reconciliation_worker_zip.output_path
}

resource "google_cloudfunctions2_function" "full_reconciliation_worker_function" {
  project     = var.project_id
  name        = "gcs-autotag-full-reconciliation-worker"
  location    = var.region
  description = "Performs full GCS to BigQuery reconciliation, including DML updates and deletions."

  build_config {
    runtime     = "python312"
    entry_point = "main_reconciliation_handler"
    source {
      storage_source {
        bucket = google_storage_bucket.function_source_bucket.name
        object = google_storage_bucket_object.full_reconciliation_worker_archive.name
      }
    }
  }

  service_config {
    service_account_email = google_service_account.full_reconciliation_worker_sa.email
    timeout_seconds       = 540
    available_memory      = "512Mi"
    environment_variables = {
      GCP_PROJECT                      = var.project_id
      BIGQUERY_DATASET_ID              = google_bigquery_dataset.hashing_catalog_dataset.dataset_id
      BIGQUERY_TABLE_ID                = google_bigquery_table.hash_inventory_table.table_id
      MANAGED_BUCKET_LABEL_KEY         = "autotagger-managed"
      MANAGED_BUCKET_LABEL_VALUE       = "true"
      LAST_SCANNED_LABEL_KEY_RECON     = "autotagger-last-reconciled-utc"
      PATH_HASH_ALGORITHM              = "PATH_BASED_SHA256"
    }
  }
  depends_on = [
    google_project_service.apis,
    google_storage_bucket_iam_member.gcf_service_agent_can_read_source,
    google_service_account.full_reconciliation_worker_sa
  ]
}

resource "google_cloud_scheduler_job" "trigger_full_reconciliation_worker" {
  project     = var.project_id
  name        = "trigger-gcs-autotag-full-reconciliation"
  description = "Triggers the GCS Autotagger Full Reconciliation Worker function periodically."
  schedule    = "0 */2 * * *"
  time_zone   = "Etc/UTC"

  http_target {
    uri         = google_cloudfunctions2_function.full_reconciliation_worker_function.service_config[0].uri
    http_method = "POST"
    oidc_token {
      service_account_email = google_service_account.full_reconciliation_worker_sa.email
    }
  }
  depends_on = [google_cloudfunctions2_function.full_reconciliation_worker_function]
}

resource "google_cloud_run_service_iam_member" "scheduler_invokes_full_reconciliation_worker" {
  project  = google_cloudfunctions2_function.full_reconciliation_worker_function.project
  location = google_cloudfunctions2_function.full_reconciliation_worker_function.location
  service  = google_cloudfunctions2_function.full_reconciliation_worker_function.name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.full_reconciliation_worker_sa.email}"
}
