resource "google_service_account" "orchestrator_sa" {
  project      = var.project_id
  account_id   = "gcs-autotag-orchestrator"
  display_name = "SA for GCS Autotagger Orchestrator"
}

resource "google_project_iam_member" "orchestrator_sa_permissions" {
  project  = var.project_id
  for_each = toset(["roles/storage.admin", "roles/bigquery.dataEditor", "roles/bigquery.jobUser"])
  role     = each.key
  member   = "serviceAccount:${google_service_account.orchestrator_sa.email}"
}

data "archive_file" "orchestrator_zip" {
  type        = "zip"
  source_dir  = "../functions/orchestrator_function/"
  output_path = "/tmp/orchestrator_function.zip"
}

resource "google_storage_bucket_object" "orchestrator_archive" {
  name   = "sources/orchestrator-${data.archive_file.orchestrator_zip.output_md5}.zip"
  bucket = google_storage_bucket.function_source_bucket.name
  source = data.archive_file.orchestrator_zip.output_path
}

resource "google_cloudfunctions2_function" "orchestrator_function" {
  project     = var.project_id
  name        = "gcs-autotag-orchestrator"
  location    = var.region
  description = "Handles GCS bucket creation events for labeling and initial BQ entry."

  build_config {
    runtime     = "python312"
    entry_point = "main_handler"
    source {
      storage_source {
        bucket = google_storage_bucket.function_source_bucket.name
        object = google_storage_bucket_object.orchestrator_archive.name
      }
    }
  }

  service_config {
    service_account_email = google_service_account.orchestrator_sa.email
    environment_variables = {
      DEFAULT_RESOURCE_LABELS_JSON = jsonencode(var.default_resource_labels)
      GCP_PROJECT                  = var.project_id
      BIGQUERY_DATASET_ID          = google_bigquery_dataset.hashing_catalog_dataset.dataset_id
      BIGQUERY_TABLE_ID            = google_bigquery_table.hash_inventory_table.table_id
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
