resource "google_dataflow_job" "dataflow_job" {
  name               = var.dataflow_job_name       # Job name
  template_gcs_path = var.template_location        # GCS path for Dataflow template
  temp_gcs_location  = var.temp_gcs_location       # Temporary GCS path for Dataflow

  parameters = var.parameters  # Using the parameters map
}
