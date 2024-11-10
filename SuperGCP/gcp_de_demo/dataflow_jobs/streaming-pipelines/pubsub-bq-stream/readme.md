python beam.py \
    --runner DataflowRunner \
    --project dev-project-433015 \
    --temp_location gs://pubsubviadataflow/temp/ \
    --input_subscription projects/dev-project-433015/subscriptions/cnn-data-topic-sub \
    --output_table dev-project-433015:cnn_project.Customer \
    --region us-central1

python beam.py `
    --runner DataflowRunner `
    --project dev-project-433015 `
    --temp_location gs://pubsubviadataflow/temp/ `
    --input_subscription projects/dev-project-433015/subscriptions/cnn-data-topic-sub `
    --output_table dev-project-433015:cnn_project.Customers `
    --region us-central1
    

Direct Run Command for Dataflow In-built Templated Job:
-------------------------------------------------------

gcloud dataflow jobs run pubsub-to-bq-streaming-job \
    --gcs-location gs://dataflow-templates-us-central1/latest/PubSub_Subscription_to_BigQuery \
    --region us-central1 \
    --staging-location gs://e-commerce-business-bucket/staging/ \ 
    --parameters inputSubscription=projects/dev-project-433015/subscriptions/cnn-data-sub,\
outputTableSpec=dev-project-433015:cnn_project.Customer,\
deadLetterTable=dev-project-433015:food_orders.food_orders_dlq

Dataflow Stream Run Command Submission:
---------------------------------------
python pubsub.py `
    --runner=DirectRunner `
    --project=dev-project-433015 `
    --region=us-central1 `
    --staging_location=gs://e-commerce-business-bucket/staging `
    --temp_location=gs://e-commerce-business-bucket/temp `
    --job_name=newjob `
    --streaming


dataflow runner run command for argparser:(working)(classic template)
----------------------------------------------------------------------
python beam.py `
  --subscription projects/dev-project-433015/subscriptions/cnn-data-sub `
  --bq_table dev-project-433015:classictemplate.pubsub_table `
  --runner DataflowRunner `
  --project dev-project-433015 `
  --staging_location gs://classictemplate-demo-bucket/staging `
  --temp_location gs://classictemplate-demo-bucket/temp `
  --template_location gs://classictemplate-demo-bucket/my_template `
  --region us-west1 `
  --streaming


python beam_with_triggers.py \
  --subscription projects/dev-project-433015/subscriptions/cnn-data-sub \
  --bq_table dev-project-433015:cnn_project.pubsub_table \
  --runner DataflowRunner \
  --project dev-project-433015 \
  --staging_location gs://classictemplate-demo-bucket/staging \
  --temp_location gs://classictemplate-demo-bucket/temp \
  --region us-west1
  --streaming


python beam2.py `
  --runner DataflowRunner `
  --project dev-project-433015 `
  --staging_location gs://classictemplate-demo-bucket/staging `
  --temp_location gs://classictemplate-demo-bucket/temp `
  --template_location gs://classictemplate-demo-bucket/my_template `
  --parameters "subscription=projects/dev-project-433015/subscriptions/cnn-data-sub,bq_table=dev-project-433015:classictemplate.pubsub_table" `
  --region us-west1 `
  --streaming
