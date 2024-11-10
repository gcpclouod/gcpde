terraform {
  required_providers {
    google = {
      source = "hashicorp/google"
      version = "6.8.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# GCS Module
module "gcs_buckets" {
  source        = "./modules/gcs"
  project_id    = var.project_id
  region        = var.region
  bucket_names  = var.bucket_names
  create_buckets = var.create_buckets  # Pass the create flags
}


# BigQuery module
module "bigquery" {
  source     = "./modules/bigquery"
  project_id = var.project_id
  region     = var.region
  dataset_id = var.dataset_id
}

# Dataflow module
module "dataflow" {
  source            = "./modules/dataflow"
  project_id        = var.project_id
  region            = var.region
  dataflow_job_name = var.dataflow_job_name
  template_location = var.template_location
  temp_gcs_location = var.temp_gcs_location
  parameters = {
    input = "gs://bucket_shabana/input/food_orders_daily.csv"
  }
}

