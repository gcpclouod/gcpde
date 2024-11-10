import apache_beam as beam
from apache_beam.io import WriteToBigQuery
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions, GoogleCloudOptions
from apache_beam.io import ReadFromPubSub
import json

# Define the BigQuery table reference (as a string)
table_spec = 'first-project-428309:rdm_order_stage.Order_Datlake'

# Define the BigQuery table schema
table_schema = {
    'fields': [
        {
            "name": "customer_id",
            "type": "INTEGER",
            "mode": "REQUIRED"
        },
        {
            "name": "user",
            "type": "STRING",
            "mode": "REQUIRED"
        },
        {
            "name": "items",
            "type": "RECORD",
            "mode": "REPEATED",
            "fields": [
                {
                    "name": "product_id",
                    "type": "INTEGER",
                    "mode": "REQUIRED"
                },
                {
                    "name": "product_name",
                    "type": "STRING",
                    "mode": "REQUIRED"
                },
                {
                    "name": "price",
                    "type": "FLOAT",
                    "mode": "REQUIRED"
                },
                {
                    "name": "quantity",
                    "type": "INTEGER",
                    "mode": "REQUIRED"
                }
            ]
        }
    ]
}

class PrintMessageFn(beam.DoFn):
    def process(self, element):
        # This function processes each message from Pub/Sub
        print(element.decode('utf-8'))
        yield json.loads(element.decode('utf-8'))  # Assuming the data is in JSON format

def run(argv=None):
    # Set up your pipeline options
    options = PipelineOptions(argv)
    options.view_as(StandardOptions).runner = 'DataflowRunner'
    options.view_as(StandardOptions).streaming = True

    # Set up Google Cloud options
    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project = 'first-project-428309'
    google_cloud_options.region = 'asia-south1'
    google_cloud_options.staging_location = 'gs://objecstsforbckt/staging'
    google_cloud_options.temp_location = 'gs://objecstsforbckt/temp'
    google_cloud_options.job_name = 'pubsub-to-bigquery-streaming'

    with beam.Pipeline(options=options) as p:
        (p
         | 'Read from Pub/Sub' >> ReadFromPubSub(subscription='projects/first-project-428309/subscriptions/orders-demo-topic-sub')
         | 'Print to console' >> beam.ParDo(PrintMessageFn())
         | 'Write to BigQuery' >> WriteToBigQuery(
                table_spec,
                schema=table_schema,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
            )
        )

if __name__ == '__main__':
    run()
