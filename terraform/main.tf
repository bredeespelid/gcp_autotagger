terraform {
  required_version = ">= 1.2"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = ">= 5.15"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

data "google_project" "project" {}
