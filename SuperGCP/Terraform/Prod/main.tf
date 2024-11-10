terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 3.5"
    }
  }
  required_version = ">= 0.12"
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# Call GCS module
module "gcs" {
  source      = "./modules/gcs"
  project_id  = var.project_id
  region      = var.region
  bucket_name = var.bucket_name
}

# Call BigQuery module
module "bigquery" {
  source     = "./modules/bigquery"
  project_id = var.project_id
  region     = var.region
  dataset_id = var.dataset_id
}

# Call Dataflow module
module "dataflow" {
  source            = "./modules/dataflow"
  project_id        = var.project_id
  region            = var.region
  dataflow_job_name = var.dataflow_job_name  # Pass the job name here    # Reference the job name from dev.tfvars
  template_location = "gs://bucket_shabana/classic_templates/my_template" # GCS path for your Dataflow template
  temp_gcs_location = "gs://bucket_shabana/temp"     # Temporary GCS path for Dataflow
  parameters        = {
    input = "gs://bucket_shabana/input/food_orders_daily.csv" 
  }
}

