import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions
from apache_beam.io import ReadFromText, WriteToText 
import json


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
google_cloud_options.job_name = 'gcstogcs'
google_cloud_options.staging_location = 'gs://dataflow_bucket_demo_210824/staging'
google_cloud_options.temp_location = 'gs://dataflow_bucket_demo_210824/temp'
pipeline_options.view_as(StandardOptions).runner = 'DataflowRunner'
pipeline_options.view_as(GoogleCloudOptions).region = 'us-central1'

# Path to the input Parquet file in GCS
input_path = 'gs://dataflow_bucket_demo_210824/output/output.txt-00000-of-00001'

# Path to the output JSON file in GCS
output_path = 'gs://dataflow_bucket_demo_210824/output3/output3.txt'

# Define a user-defined function (UDF) to add 10 to a number
def add_ten(x):
    return x + 10

# Create the pipeline
with beam.Pipeline(options=pipeline_options) as p:
    (
        p
        | 'Read from Parquet' >> ReadFromText(input_path)  
        | 'The first Transformation - doubling' >> beam.Map(lambda x : int(x) * 2)
        | 'AddTen' >> beam.Map(add_ten)
        | 'Write to File' >> WriteToText(output_path)
    )

'''
In this example, after squaring the numbers using a lambda function, 
I'll add another beam.Map transform that uses a user-defined function (UDF) to add 10 to each squared number.

'''