resource "google_project_service" "apis" {
  project = var.project_id
  for_each = toset([
    "cloudfunctions.googleapis.com", "cloudbuild.googleapis.com", "storage.googleapis.com",
    "logging.googleapis.com", "pubsub.googleapis.com", "eventarc.googleapis.com",
    "run.googleapis.com", "iam.googleapis.com", "cloudscheduler.googleapis.com",
    "bigquery.googleapis.com", "artifactregistry.googleapis.com"
  ])
  service            = each.key
  disable_on_destroy = false
}
