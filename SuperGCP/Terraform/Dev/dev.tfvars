project_id = "woven-name-434311-i8"
region     = "us-central1"   
bucket_names    = ["terraformbucket-batch35", "terraformbucket-batch35-2", "terraformbucket-batch35-3", "terraformbucket-batch35-4"]
create_buckets  = [true, false, true, true]  # Create bucket 1, skip bucket 2, create buckets 3 and 4
dataset_id      = "terraform_dataset_id12345"  # Adjust as needed    
dataflow_job_name = "new_dataflow_job_name"  # Define your job name here
template_location = "gs://gcp35batch-demo/templates/gcs_to_bigquery"
temp_gcs_location = "gs://gcp35batch-demo/temp"

