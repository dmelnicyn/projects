variable "credentials" {
  description = "Path to the service account key file"
  default = "/path/to/your/credentials.json"
}

variable "project_id" {
  description = "Project"
  default     = "your_bigquery_dataset_id"
}

variable "region" {
  description = "Region"
  #Update the below to your desired region
  default = "us-central1"
}

variable "bq_dataset_name" {
  description = "My BigQuery Dataset Name"
  #Update the below to what you want your dataset to be called
  default = "your_bigquery_dataset"
}

variable "gcs_bucket_name" {
  description = "My Storage Bucket Name"
  #Update the below to a unique bucket name
  default = "your_gcs_bucket"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}

variable "target_service_account_email" {
  description = "Service Account Email"
  default     = "your_service_account@project.iam.gserviceaccount.com"
}