resource "google_project_iam_member" "orchestrator_sa_permissions" {
  project  = var.project_id
  for_each = toset(["roles/storage.admin", "roles/bigquery.dataEditor", "roles/bigquery.jobUser"])
  role     = each.key
  member   = "serviceAccount:${google_service_account.orchestrator_sa.email}"
}

resource "google_project_iam_member" "batch_worker_sa_permissions" {
  project  = var.project_id
  for_each = toset(["roles/storage.admin", "roles/bigquery.dataEditor", "roles/bigquery.jobUser"])
  role     = each.key
  member   = "serviceAccount:${google_service_account.batch_worker_sa.email}"
}

resource "google_project_iam_member" "status_updater_sa_bigquery_permissions" {
  project  = var.project_id
  for_each = toset(["roles/bigquery.dataEditor", "roles/bigquery.jobUser"])
  role     = each.key
  member   = "serviceAccount:${google_service_account.status_updater_sa.email}"
}

resource "google_project_iam_member" "full_reconciliation_worker_sa_permissions" {
  project  = var.project_id
  for_each = toset(["roles/storage.admin", "roles/bigquery.dataEditor", "roles/bigquery.jobUser"])
  role     = each.key
  member   = "serviceAccount:${google_service_account.full_reconciliation_worker_sa.email}"
}

resource "google_storage_bucket_iam_member" "cloudbuild_can_read_source" {
  bucket = google_storage_bucket.function_source_bucket.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${data.google_project.project.number}@cloudbuild.gserviceaccount.com"
}

resource "google_storage_bucket_iam_member" "gcf_service_agent_can_read_source" {
  bucket = google_storage_bucket.function_source_bucket.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:service-${data.google_project.project.number}@serverless-robot-prod.iam.gserviceaccount.com"
}

resource "google_pubsub_topic_iam_member" "infra_sink_publisher" {
  project    = var.project_id
  topic      = google_pubsub_topic.infra_lifecycle_events.name
  role       = "roles/pubsub.publisher"
  member     = google_logging_project_sink.infra_lifecycle_sink.writer_identity
}

resource "google_cloud_run_service_iam_member" "scheduler_invokes_batch_worker" {
  project  = google_cloudfunctions2_function.batch_worker_function.project
  location = google_cloudfunctions2_function.batch_worker_function.location
  service  = google_cloudfunctions2_function.batch_worker_function.name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.batch_worker_sa.email}"
}

resource "google_cloud_run_service_iam_member" "scheduler_invokes_full_reconciliation_worker" {
  project  = google_cloudfunctions2_function.full_reconciliation_worker_function.project
  location = google_cloudfunctions2_function.full_reconciliation_worker_function.location
  service  = google_cloudfunctions2_function.full_reconciliation_worker_function.name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.full_reconciliation_worker_sa.email}"
}