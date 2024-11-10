import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import WriteToBigQuery
from apache_beam.io.gcp.internal.clients import bigquery
import json

# Define the BigQuery table specification
table_spec = bigquery.TableReference(
    projectId='dev-project-433015',
    datasetId='cnn_project',
    tableId='pubsub_table'
)

# Define the schema for the BigQuery table
table_schema = {
    'fields': [
        {'name': 'source', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'quote', 'type': 'STRING', 'mode': 'REQUIRED'}
    ]
}

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

def run():
    # Define the pipeline options
    options = PipelineOptions()

    # Your Pub/Sub subscription path
    subscription_path = 'projects/dev-project-433015/subscriptions/cnn-data-sub'

    # Create the Beam pipeline
    p = beam.Pipeline(options=options)

    # Define the pipeline steps
    (
        p
        # Read messages from Pub/Sub
        | 'ReadFromPubSub' >> beam.io.ReadFromPubSub(subscription=subscription_path)
        
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
