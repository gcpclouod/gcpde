python beam.py --input_subscription "projects/dev-project-433015/subscriptions/cnn-data-sub" `
               --output_table "dev-project-433015:classictemplate.pubsub_table" `
               --job_name "pubsubtobigquery" `
               --staging_location "gs://classictemplate-demo-bucket/staging" `
               --temp_location "gs://classictemplate-demo-bucket/temp" `
               --region "us-west1" `
               --runner "DataflowRunner" `
               --project "dev-project-433015" `
               --streaming
