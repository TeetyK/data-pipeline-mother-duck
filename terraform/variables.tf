variable "service_account_path" {
  description = "Google Cloud Service Account"
  default     = "F:\\data-pipeline-mother-duck\\keys\\service_account.json"
}
variable "project_id" {
  description = "Google Project ID"
  default     = "pipeline-partice"
}
variable "billing_id" {
  description = "Google Billing Project"
  default     = "pipeline-partice"
}
variable "gcs_bucket_name" {
  description = "GCS Bucket Name"
  default     = "pipeline-partice"
}
variable "region" {
  description = "Google Cloud region"
  default     = "us-central1"
}
variable "zone" {
  description = "Google Cloud zone"
  default     = "us-central1-a"
}
variable "location" {
  description = "Google Cloud location"
  default     = "US"
}
variable "bigquery_bronze_dataset_name" {
  description = "Bigquery bronze layer"
  default     = "bronze"
}
variable "bigquery_silver_dataset_name" {
  description = "Bigquery silver layer"
  default     = "silver"
}
variable "bigquery_gold_dataset_name" {
  description = "Bigquery silver layer"
  default     = "gold"
}


