variable "project_id" {
  description = "GCP Project ID where this infrastructure will be deployed."
  type        = string
  default     = "sandbox-gcp-tags"
}

variable "region" {
  description = "GCP Region for all resources."
  type        = string
  default     = "europe-west1"
}

variable "default_resource_labels" {
  description = "Default labels to apply to new buckets."
  type        = map(string)
  default     = { "managed-by" = "gcs-autotagger", "data-tier" = "raw" }
}

variable "default_object_metadata" {
  description = "Default metadata to apply to new objects."
  type        = map(string)
  default     = { "data-source" = "gcs-autotagger-upload", "data-quality" = "raw-unverified" }
}