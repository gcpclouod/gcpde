import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions
from apache_beam.io.gcp.internal.clients import bigquery
import json
import argparse

# Function to parse the Pub/Sub message
def parse_message(message):
    # Decode the bytes message to string
    message_str = message.decode('utf-8')
    
    # Parse the string as JSON
    message_json = json.loads(message_str)
    
    return {
        'source': message_json.get('source'),
        'quote': message_json.get('quote')
    }

def run(input_subscription, output_table):
    # Define the pipeline options
    pipeline_options = PipelineOptions(
        runner='DataflowRunner',
        num_workers=1, 
        worker_machine_type='n1-standard-8',
        worker_disk_type='pd-ssd',
        worker_disk_size_gb=50,
        machine_type='n1-standard-8'
    )

    google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
    google_cloud_options.project = 'dev-project-433015'
    google_cloud_options.job_name = 'pubsubtobigquery'
    google_cloud_options.staging_location = 'gs://e-commerce-business-bucket/staging'
    google_cloud_options.temp_location = 'gs://e-commerce-business-bucket/temp' 
    pipeline_options.view_as(StandardOptions).runner = 'DataflowRunner'
    pipeline_options.view_as(GoogleCloudOptions).region = 'us-west1'

    # Define the BigQuery table specification
    table_spec = bigquery.TableReference(
        projectId='dev-project-433015',
        datasetId='cnn_project',
        tableId=output_table
    )

    # Define the schema for the BigQuery table
    table_schema = {
        'fields': [
            {'name': 'source', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'quote', 'type': 'STRING', 'mode': 'REQUIRED'}
        ]
    }

    # Use the global pipeline options defined above
    p = beam.Pipeline(options=pipeline_options)

    # Define the pipeline steps
    (
        p
        # Read messages from Pub/Sub
        | 'ReadFromPubSub' >> beam.io.ReadFromPubSub(subscription=input_subscription)
        
        # Decode each Pub/Sub message and parse it as JSON
        | 'ParseMessage' >> beam.Map(parse_message)
        
        # Write the parsed data to BigQuery
        | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
            table_spec,
            schema=table_schema,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
        )
    )

    # Run the pipeline
    result = p.run()
    result.wait_until_finish()

if __name__ == '__main__':
    # Setup argument parser
    parser = argparse.ArgumentParser(description='PubSub to BigQuery Dataflow Pipeline')
    
    # Adding argument for input Pub/Sub subscription
    parser.add_argument(
        '--input_subscription', 
        required=True, 
        help='Input Pub/Sub subscription path. Example: projects/{project}/subscriptions/{subscription}'
    )
    
    # Adding argument for output BigQuery table
    parser.add_argument(
        '--output_table', 
        required=True, 
        help='Output BigQuery table specification. Example: project_id:dataset_id.table_id'
    )
    
    # Parse arguments
    args = parser.parse_args()
    
    # Run the pipeline with arguments
    run(args.input_subscription, args.output_table)
