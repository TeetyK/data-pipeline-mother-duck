#Terraform Google provider
provider "google"{
    project = var.project_id
    region = var.region
    zone = var.zone
    credentials = file(var.service_account_path)
}
#Terraform Google stronge bucket
resource "google_storage_bucket" "gcs_bucket"{
    name = var.gcs_bucket_name
    location = var.location
    force_destroy = true
}

# bronze layer
resource "google_bigquery_dataset" "bronze" {
    dataset_id = var.bigquery_bronze_dataset_name
    description = "This's a bronze layer in BugQuery"
    location = var.location
    default_table_expiration_ms = 3600000
}
# silver layer
resource "google_bigquery_dataset" "silver" {
    dataset_id = var.bigquery_silver_dataset_name
    description = "This's a silver layer in BugQuery"
    location = var.location
    default_table_expiration_ms = 3600000
}
# gold layer
resource "google_bigquery_dataset" "gold" {
    dataset_id = var.bigquery_gold_dataset_name
    description = "This's a gold layer in BugQuery"
    location = var.location
    default_table_expiration_ms = 3600000
}
