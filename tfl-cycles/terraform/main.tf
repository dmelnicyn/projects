terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "5.38.0"
    }
  }
}

provider "google" {
  project     = var.project_id
  region      = var.region
  credentials = var.credentials # Service account with iam.serviceAccountTokenCreator role
}

resource "google_storage_bucket" "my_bucket" {
  provider      = google
  name          = var.gcs_bucket_name
  location      = var.region
  force_destroy = true
  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}

resource "google_bigquery_dataset" "your_bigquery_dataset" {
  dataset_id = var.bq_dataset_name
  location   = var.region
}