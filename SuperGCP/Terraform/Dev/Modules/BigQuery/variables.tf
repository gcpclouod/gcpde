variable "project_id" {
  description = "The ID of the project in GCP"
  type        = string
}

variable "region" {
  description = "The region to deploy the resources"
  type        = string
}

variable "dataset_id" {
  description = "The ID of the BigQuery dataset"
  type        = string
}
