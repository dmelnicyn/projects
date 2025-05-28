variable "credentials" {
  type        = string
  description = "Path to the service account key file"
  default     = "/path/to/"key.json"
}

variable "project_id" {
  type        = string
  description = "Google Cloud project ID"
  default     = "your-project"
}

variable "region" {
  type        = string
  description = "GCP region to deploy resources in"
  default     = "us-central1"
}

variable "bq_dataset_name" {
  type        = string
  description = "The name of the BigQuery dataset to create"
  default     = "your-project"
}

variable "gcs_bucket_name" {
  type        = string
  description = "Name of the GCS bucket to create"
  default     = "your-gcs-bucket"

  validation {
    condition     = length(var.gcs_bucket_name) >= 3
    error_message = "Bucket name must be at least 3 characters long."
  }
}

variable "gcs_storage_class" {
  type        = string
  description = "Storage class of the GCS bucket"
  default     = "STANDARD"
}

variable "target_service_account_email" {
  type        = string
  description = "Email address of the service account to grant IAM roles"
  default     = "your@project-id.iam.gserviceaccount.com"
}
