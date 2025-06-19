resource "google_pubsub_topic" "infra_lifecycle_events" {
  name       = "gcs-autotagger-lifecycle-events"
  project    = var.project_id
  depends_on = [google_project_service.apis]
}

resource "google_logging_project_sink" "infra_lifecycle_sink" {
  name                   = "gcs-autotagger-lifecycle-sink"
  project                = var.project_id
  destination            = "pubsub.googleapis.com/${google_pubsub_topic.infra_lifecycle_events.id}"
  filter                 = "resource.type=(\"gcs_bucket\" OR \"gcs_object\") AND protoPayload.methodName:(\"storage.buckets.create\" OR \"storage.objects.delete\" OR \"storage.buckets.delete\")"
  unique_writer_identity = true
  depends_on             = [google_pubsub_topic.infra_lifecycle_events]
}

resource "google_pubsub_topic_iam_member" "infra_sink_publisher" {
  project    = var.project_id
  topic      = google_pubsub_topic.infra_lifecycle_events.name
  role       = "roles/pubsub.publisher"
  member     = google_logging_project_sink.infra_lifecycle_sink.writer_identity
  depends_on = [google_logging_project_sink.infra_lifecycle_sink]
}
