variable "project_id" {
  description = "The ID of the project in GCP"
  type        = string
}

variable "region" {
  description = "The region to deploy the resources"
  type        = string
}

variable "bucket_names" {
  description = "List of GCS bucket names"
  type        = list(string)
}

variable "create_buckets" {
  description = "List of flags to create each bucket"
  type        = list(bool)
}

variable "dataset_id" {
  description = "The ID of the BigQuery dataset"
  type        = string
}

variable "template_location" {
  description = "GCS path for the Dataflow template"
  type        = string
}

variable "temp_gcs_location" {
  description = "Temporary GCS path for Dataflow"
  type        = string
}

variable "dataflow_job_name" {
  description = "The name of the Dataflow job"
  type        = string
}
