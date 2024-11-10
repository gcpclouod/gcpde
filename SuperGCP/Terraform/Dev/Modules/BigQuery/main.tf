resource "google_bigquery_dataset" "dataset" {
  dataset_id = var.dataset_id
  project    = var.project_id  # Reference to the project_id variable
  location   = var.region       # Reference to the region variable
}

