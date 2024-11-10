import apache_beam as beam
from apache_beam.io import ReadFromText, WriteToBigQuery
from apache_beam.io.gcp.internal.clients import bigquery
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions
import re


# Define BigQuery table schema
table_spec = bigquery.TableReference(
    projectId='woven-name-434311-i8',
    datasetId='cnn_project',
    tableId='food_orders'
)
# woven-name-434311-i8:cnn_project
table_schema = {
    'fields': [
        {"name": "customer_id", "type": "STRING", "mode": "NULLABLE"},
        {"name": "date", "type": "STRING", "mode": "NULLABLE"},
        {"name": "time", "type": "STRING", "mode": "NULLABLE"},
        {"name": "order_id", "type": "STRING", "mode": "NULLABLE"},
        {"name": "items", "type": "STRING", "mode": "NULLABLE"},
        {"name": "amount", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "mode", "type": "STRING", "mode": "NULLABLE"},
        {"name": "restaurant", "type": "STRING", "mode": "NULLABLE"},
        {"name": "status", "type": "STRING", "mode": "NULLABLE"},
        {"name": "ratings", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "feedback", "type": "STRING", "mode": "NULLABLE"}
    ]
}

# Transformation functions
def remove_last_colon(row):
    cols = row.split(',')
    item = str(cols[4])
    if item.endswith(':'):
        cols[4] = item[:-1]
    return ','.join(cols)

def remove_special_characters(row):
    cols = row.split(',')
    ret = ''
    for col in cols:
        clean_col = re.sub(r'[?%&]', '', col)
        ret += clean_col + ','
    return ret[:-1]

def to_json(csv_str):
    # Split the CSV string into fields
    fields = csv_str.split(',')

    # Ensure that the list has at least 11 elements
    if len(fields) < 11:
        fields += [''] * (11 - len(fields))  # Pad the list with empty strings

    # Convert the fields to a JSON object
    json_str = {
        "customer_id": fields[0],
        "date": fields[1],
        "timestamp": fields[2],  # Corrected key name to 'time' if needed
        "order_id": fields[3],
        "items": fields[4],
        "amount": fields[5],
        "mode": fields[6],
        "restaurant": fields[7],
        "status": fields[8],
        "ratings": fields[9],
        "feedback": fields[10]
    }

    return json_str

	
table_schema = 'customer_id:STRING,date:STRING,timestamp:STRING,order_id:STRING,items:STRING,amount:STRING,mode:STRING,restaurant:STRING,status:STRING,ratings:STRING,feedback:STRING'


# Define custom pipeline options
class CustomOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument(
            '--input',
            dest='input',
            required=True,
            help='Path to the input file in Google Cloud Storage.' 
        ) 

def run():
    # Define the pipeline options
    pipeline_options = PipelineOptions()

    # Add custom options to the pipeline
    custom_options = pipeline_options.view_as(CustomOptions)
    google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
    google_cloud_options.project = 'woven-name-434311-i8'
    google_cloud_options.job_name = 'gcstobq'
    google_cloud_options.staging_location = 'gs://gcp35batch-demo/staging'
    google_cloud_options.temp_location = 'gs://gcp35batch-demo/temp'
    pipeline_options.view_as(StandardOptions).runner = 'DataflowRunner'
    pipeline_options.view_as(GoogleCloudOptions).region = 'us-central1'

    # Use custom input and output options
    input_path = custom_options.input
    
    # Create the pipeline
    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | 'Read from GCS' >> ReadFromText(input_path)
            | 'Removing last colons' >> beam.Map(remove_last_colon)
            | 'Making lowercase' >> beam.Map(lambda row: row.lower())
            | 'Removing Special Characters' >> beam.Map(remove_special_characters)
            | 'Parse and Convert to String' >> beam.Map(to_json)
            | 'Write to BigQuery' >> WriteToBigQuery(
                table_spec,
                schema=table_schema,
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
            )
        )

if __name__ == "__main__":
    run()
