# pylint: disable=unsupported-binary-operation
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import ReadFromBigQuery  # Updated import for BigQuery

# BigQuery connection parameters
BIGQUERY_PROJECT = "woven-name-434311-i8"
BIGQUERY_DATASET = "cloudfunction_demo"
BIGQUERY_TABLE = "cloudfunctiondemotable"

# BigQuery source query
source_query = f"SELECT * FROM `{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE}`"

pipeline_args = [
    '--project', 'woven-name-434311-i8',
    '--job_name', 'jobname1',
    '--runner', 'DirectRunner',
    '--staging_location', 'gs://cloudsql-gcs-export/stage',
    '--temp_location', 'gs://cloudsql-gcs-export/temp',
    '--region', 'us-central1'
]
				 
def run(argv=None):
    with beam.Pipeline(options=PipelineOptions(pipeline_args)) as p:
        # Read data from BigQuery
        bigquery_data = (
            p
            | 'ReadFromBigQuery' >> beam.io.ReadFromBigQuery(
                query=source_query,
                use_standard_sql=True,
                method=beam.io.ReadFromBigQuery.Method.DIRECT_READ
            )
            | 'PrintData' >> beam.Map(print)
        )

        
if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()