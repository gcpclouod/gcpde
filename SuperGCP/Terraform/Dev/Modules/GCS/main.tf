# modules/gcs/main.tf

# Define the Google Cloud Storage Bucket resource

resource "google_storage_bucket" "buckets" {
  for_each = { for idx, bucket_name in var.bucket_names : bucket_name => var.create_buckets[idx] if var.create_buckets[idx] }

  name          = each.key
  location      = var.region
  force_destroy = true  # Change as needed
}