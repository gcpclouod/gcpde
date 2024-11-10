import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions
from apache_beam.io import WriteToBigQuery
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

def run(argv=None):
    # Argument parser
    parser = argparse.ArgumentParser()
    
    # Add arguments for subscription and BigQuery table
    parser.add_argument(
        '--subscription',
        required=True,
        help='Pub/Sub subscription in the form "projects/{project_id}/subscriptions/{subscription_id}".'
    )
    
    parser.add_argument(
        '--bq_table',
        required=True,
        help='BigQuery table in the form "{project_id}:{dataset_id}.{table_id}".'
    )
    
    # Parse the arguments
    known_args, pipeline_args = parser.parse_known_args(argv)
    
    # Define the pipeline options
    pipeline_options = PipelineOptions(pipeline_args)
    
    google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
    google_cloud_options.project = 'dev-project-433015'
    google_cloud_options.job_name = 'pubsubtobigquery'
    google_cloud_options.staging_location = 'gs://classictemplate-demo-bucket/staging'
    google_cloud_options.temp_location = 'gs://classictemplate-demo-bucket/temp'
    pipeline_options.view_as(StandardOptions).runner = 'DataflowRunner'
    pipeline_options.view_as(GoogleCloudOptions).region = 'us-west1'

    # Extract the BigQuery table specification from the argument
    project_id, dataset_id, table_id = known_args.bq_table.split(':')[0], known_args.bq_table.split(':')[1].split('.')[0], known_args.bq_table.split(':')[1].split('.')[1]
    
    table_spec = bigquery.TableReference(
        projectId=project_id,
        datasetId=dataset_id,
        tableId=table_id
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

    # Define the pipeline steps with windowing
    (
        p
        # Read messages from Pub/Sub
        | 'ReadFromPubSub' >> beam.io.ReadFromPubSub(subscription=known_args.subscription)
        
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
    run()
