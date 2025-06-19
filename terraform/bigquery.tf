resource "google_bigquery_dataset" "hashing_catalog_dataset" {
  dataset_id  = "data_asset_catalog"
  project     = var.project_id
  location    = var.region
  description = "Authoritative catalog for GCS assets with path-based hashes."
  depends_on  = [google_project_service.apis]
}

resource "google_bigquery_table" "hash_inventory_table" {
  dataset_id          = google_bigquery_dataset.hashing_catalog_dataset.dataset_id
  table_id            = "gcs_hash_inventory"
  project             = var.project_id
  deletion_protection = false
  schema = file("schemas/hash_inventory_schema.json")
  depends_on = [google_bigquery_dataset.hashing_catalog_dataset]
}