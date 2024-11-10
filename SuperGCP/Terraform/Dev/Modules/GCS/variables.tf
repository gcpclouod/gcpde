# modules/gcs/variables.tf

# Define the project ID variable
variable "project_id" {
  description = "The ID of the project in GCP"
  type        = string
}

# Define the region variable
variable "region" {
  description = "The region to deploy the bucket"
  type        = string
}

# Define the bucket name variable
variable "bucket_names" {
  description = "List of bucket names"
  type        = list(string)
}

variable "create_buckets" {
  description = "List of flags to create each bucket"
  type        = list(bool)
}