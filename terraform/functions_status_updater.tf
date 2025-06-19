resource "google_service_account" "status_updater_sa" {
  project      = var.project_id
  account_id   = "gcs-autotag-status-updater"
  display_name = "SA for GCS Autotagger Status Updater"
}

resource "google_project_iam_member" "status_updater_sa_bigquery_permissions" {
  project  = var.project_id
  for_each = toset(["roles/bigquery.dataEditor", "roles/bigquery.jobUser"])
  role     = each.key
  member   = "serviceAccount:${google_service_account.status_updater_sa.email}"
}

data "archive_file" "status_updater_zip" {
  type        = "zip"
  source_dir  = "../functions/status_updater_function/"
  output_path = "/tmp/status_updater_function.zip"
}

resource "google_storage_bucket_object" "status_updater_archive" {
  name   = "sources/status-updater-${data.archive_file.status_updater_zip.output_md5}.zip"
  bucket = google_storage_bucket.function_source_bucket.name
  source = data.archive_file.status_updater_zip.output_path
}

resource "google_cloudfunctions2_function" "status_updater_function" {
  project     = var.project_id
  name        = "gcs-autotag-status-updater"
  location    = var.region
  description = "Updates asset status in BQ on GCS delete events."

  build_config {
    runtime     = "python312"
    entry_point = "main_handler"
    source {
      storage_source {
        bucket = google_storage_bucket.function_source_bucket.name
        object = google_storage_bucket_object.status_updater_archive.name
      }
    }
  }

  service_config {
    service_account_email = google_service_account.status_updater_sa.email
    environment_variables = {
      GCP_PROJECT         = var.project_id
      BIGQUERY_DATASET_ID = google_bigquery_dataset.hashing_catalog_dataset.dataset_id
      BIGQUERY_TABLE_ID   = google_bigquery_table.hash_inventory_table.table_id
    }
  }

  event_trigger {
    trigger_region = var.region
    event_type     = "google.cloud.pubsub.topic.v1.messagePublished"
    pubsub_topic   = google_pubsub_topic.infra_lifecycle_events.id
    retry_policy   = "RETRY_POLICY_RETRY"
  }
  depends_on = [
    google_project_service.apis,
    google_storage_bucket_iam_member.gcf_service_agent_can_read_source
  ]
}