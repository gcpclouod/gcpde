run command:

python /home/bhaskargsbalina/gcs/data-read-from-pubsub.py --runner DataflowRunner \
    --project woven-name-434311-i8 \
    --region us-central1 \
    --staging_location gcp35batch-demo/staging \
    --temp_location gcp35batch-demo/temp \
    --job_name pubsub-to-bigquery-streaming \
    --streaming


python C:\GCP-Cloud-DE\SuperGCP\gcp_de_demo\dataflow_jobs\pubsub_to_bq_order_move_dataflow_job.py --runner DataflowRunner `
    --project woven-name-434311-i8 `
    --region us-central1 `
    --staging_location gcp35batch-demo/staging `
    --temp_location gcp35batch-demo/temp `
    --job_name pubsub-to-bigquery-streaming1 `
    --streaming


pipeline_args = [
    '--project', 'woven-name-434311-i8',
    '--job_name', 'jobname1',
    '--runner', 'DataflowRunner',
    '--staging_location', 'gs://cloudsql-gcs-export/stage',
    '--temp_location', 'gs://cloudsql-gcs-export/temp',
    '--region', 'us-central1'
]

python read_from_bq.py --runner DirectRunner `
    --project woven-name-434311-i8 `
    --region us-central1 `
    --staging_location gs://cloudsql-gcs-export/staging `
    --temp_location gs://cloudsql-gcs-export/temp `
    --job_name jobname1  
