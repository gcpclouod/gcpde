variable "project_id" {
  description = "GCP project ID"
  type        = string
}

variable "region" {
  description = "GCP region"
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

variable "parameters" {
  description = "Parameters for the Dataflow job"
  type        = map(string)
}

variable "dataflow_job_name" {
  description = "The name of the Dataflow job"
  type        = string
}
