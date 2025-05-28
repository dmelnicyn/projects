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

data "google_client_openid_userinfo" "me" {
}

resource "google_os_login_ssh_public_key" "cache" {
  user =  data.google_client_openid_userinfo.me.email
  key  = file("/path/to/"your.pub")  
}

resource "google_compute_instance" "default" {
  name         = "your-project-vm"
  machine_type = "e2-standard-4"
  zone         = "us-central1-c"
  project      = var.project.id
  tags = ["your-project"]

  metadata = {
    enable-osconfig = "TRUE"
    ssh-keys        = "airflow_user:${file("/path/to/"your.pub")}"
  }

  boot_disk {
    initialize_params {
      image = "debian-cloud/debian-12-bookworm"
      size  = 100
    }
  }
  
  network_interface {
    network       = "default"
    subnetwork    = "default"
    access_config {
      network_tier = "PREMIUM"
    }
  }

  service_account {
    email  = "var.target_service_account_email"
    scopes = ["cloud-platform"] 
  }
  
  scheduling {
    preemptible       = false
    automatic_restart = true
    on_host_maintenance = "MIGRATE"
    provisioning_model = "STANDARD"
  }
  
  shielded_instance_config {
    enable_secure_boot          = false
    enable_vtpm                 = true
    enable_integrity_monitoring = true
  }
  metadata_startup_script = file("projects/tfl-cycles/terraform/install_docker_run_compose.sh")
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

resource "google_bigquery_dataset" "my_dataset" {
  dataset_id = var.bq_dataset_name
  location   = var.region
  delete_contents_on_destroy = true
  labels = {
    environment = "dev"
    purpose     = "your-project"
  }
}

resource "google_storage_bucket_iam_member" "gcs_access" {
  bucket = google_storage_bucket.my_bucket.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${var.target_service_account_email}"
}

resource "google_bigquery_dataset_iam_member" "dataset_access" {
  dataset_id = google_bigquery_dataset.my_dataset.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${var.target_service_account_email}"
}

resource "google_project_iam_member" "bq_admin" {
  project = var.project_id
  role    = "roles/bigquery.admin"
  member  = "serviceAccount:${var.target_service_account_email}"
}

resource "google_project_iam_member" "gcs_admin" {
  project = var.project_id
  role    = "roles/storage.admin"
  member  = "serviceAccount:${var.target_service_account_email}"
}

resource "google_project_iam_member" "viewer" {
  project = var.project_id
  role    = "roles/viewer"
  member  = "serviceAccount:${var.target_service_account_email}"
}

output "gcs_bucket_name" {
  description = "The name of the GCS bucket."
  value       = google_storage_bucket.my_bucket.name
}

output "bigquery_dataset_id" {
  description = "The ID of the BigQuery dataset."
  value       = google_bigquery_dataset.my_dataset.dataset_id
}

output "service_account_email" {
  description = "The service account granted access."
  value       = var.target_service_account_email
}