
#########
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions
from apache_beam.io import ReadFromParquet, WriteToText
import json


class ProcessElement(beam.DoFn):
    default_values = {
        "ipi": 24.0,
        "lpq": 1.0,
        "tot_ord": 7,
        "id": "155024682",
        "g1": 1.0,
        "pb1": 1.0,
        "qP": 1,
        "src": 1,
        "bvId": "155024682",
        "ptc": "PT1323014",
        "lpd": "20240707",
        "b1": 1.0,
        "g2": 1.0,
        "ipi1": 16.75,
        "g3": 1.0
    }

    def generate_items(self, prod_id_list):
        items = []
        for prod_id in prod_id_list:
            item = {
                "ipi": self.default_values["ipi"],
                "lpq": self.default_values["lpq"],
                "tot_ord": self.default_values["tot_ord"],
                "id": self.default_values["id"],
                "g1": self.default_values["g1"],
                "pb1": self.default_values["pb1"],
                "qP": self.default_values["qP"],
                "p": prod_id,
                "src": self.default_values["src"],
                "bvId": self.default_values["bvId"],
                "ptc": self.default_values["ptc"],
                "lpd": self.default_values["lpd"],
                "b1": self.default_values["b1"],
                "g2": self.default_values["g2"],
                "ipi1": self.default_values["ipi1"],
                "g3": self.default_values["g3"]
            }
            items.append(item)
        return items

    def process(self, element):
        prod_id = element['prod_id']
        cid = element['cid']
        prod_id_list = prod_id.split(',')
        items = self.generate_items(prod_id_list)
        data = {"pbs": 1, "ts": 1720602704, "id": str(cid), "ct": 3, "items": items}
        yield json.dumps(data)


def format_records(elements):
    """Format JSON records with commas, except the last one."""
    formatted_elements = [element + ',' for element in elements[:-1]] + [elements[-1]]
    return formatted_elements


# Define the custom pipeline options
class CustomPipelineOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument('--input_parquet_path', type=str, help='Path to the input Parquet file in GCS')
        parser.add_value_provider_argument('--output_json_path', type=str, help='Path to the output JSON files in GCS')


# Define the pipeline options
pipeline_options = PipelineOptions(
    runner='DataflowRunner',
    num_workers=5, 
    worker_machine_type='n1-standard-8',
    worker_disk_type='pd-ssd',
    worker_disk_size_gb=50,
    machine_type='n1-standard-8'
)

custom_options = pipeline_options.view_as(CustomPipelineOptions)

google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
google_cloud_options.project = 'dev-sams-data-generator'
google_cloud_options.job_name = 'parquet-to-json'
google_cloud_options.staging_location = 'gs://poc-sample-parquet/input_file_ds/staging'
google_cloud_options.temp_location = 'gs://poc-sample-parquet/input_file_ds/temp'
pipeline_options.view_as(StandardOptions).runner = 'DataflowRunner'
pipeline_options.view_as(GoogleCloudOptions).region = 'us-central1'


# Create the pipeline
with beam.Pipeline(options=pipeline_options) as p:
    (
        p
        | 'Read from Parquet' >> ReadFromParquet(custom_options.input_parquet_path)
        | 'Process Elements' >> beam.ParDo(ProcessElement())
        | 'Collect to List' >> beam.combiners.ToList()  # Collect all elements into a list
        | 'Format Records' >> beam.FlatMap(format_records)  # Format with commas and newlines
        | 'Write to File' >> WriteToText(
            custom_options.output_json_path,
            num_shards=1,
            shard_name_template='',
            append_trailing_newlines=True  # Ensure newline at the end
        )
    )


##############

gcloud auth activate-service-account --key-file=/home/cloud_user_p_e32682ae/serv.json

gcloud services enable dataflow compute_component logging storage_component storage_api cloudresourcemanager.googleapis.com artifactregistry.googleapis.com cloudbuild.googleapis.com

gcloud artifacts repositories create flex --repository-format=docker --location=us-central1

gcloud dataflow flex-template build gs://shuvabu007/test-py.json --image-gcr-path "us-central1-docker.pkg.dev/playground-s-11-5666c650/flex/test-python:latest" --sdk-language "PYTHON" --flex-template-base-image "PYTHON3" --metadata-file "metadata.json"  --py-path "." --env "FLEX_TEMPLATE_PYTHON_PY_FILE=test.py" --env "FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE=requirements.txt"


gcloud dataflow flex-template run "getting-started-$(date +%Y%m%d-%H%M%S)" \
  --template-file-gcs-location "gs://shuvabu007/test-py.json" \
  --parameters input_parquet_path="gs://shuvabu007/input/part-00000-c7585b4f-7e43-4685-ad20-ebdb71ca97a6-c000.snappy.parquet" \
  --parameters output_parquet_path="gs://shuvabu007/output/output"

### step 1 : first create a directory called image and create the file test.py inside. 

import apache_beam as beam
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions
from apache_beam.io.parquetio import WriteToParquet, ReadFromParquet
import pyarrow as pa
import json


class ProcessElement(beam.DoFn):
    default_values = {
        "ipi": 24.0,
        "lpq": 1.0,
        "tot_ord": 7,
        "id": "155024682",
        "g1": 1.0,
        "pb1": 1.0,
        "qP": 1,
        "src": 1,
        "bvId": "155024682",
        "ptc": "PT1323014",
        "lpd": "20240707",
        "b1": 1.0,
        "g2": 1.0,
        "ipi1": 16.75,
        "g3": 1.0
    }

    def generate_items(self, prod_id_list):
        items = []
        for prod_id in prod_id_list:
            item = {
                "ipi": self.default_values["ipi"],
                "lpq": self.default_values["lpq"],
                "tot_ord": self.default_values["tot_ord"],
                "id": self.default_values["id"],
                "g1": self.default_values["g1"],
                "pb1": self.default_values["pb1"],
                "qP": self.default_values["qP"],
                "p": prod_id,
                "src": self.default_values["src"],
                "bvId": self.default_values["bvId"],
                "ptc": self.default_values["ptc"],
                "lpd": self.default_values["lpd"],
                "b1": self.default_values["b1"],
                "g2": self.default_values["g2"],
                "ipi1": self.default_values["ipi1"],
                "g3": self.default_values["g3"]
            }
            items.append(item)
        return items

    def process(self, element):
        import random
        prod_id = element['prod_id']
        cid = random.randint(1, 1000000) 
        prod_id_list = prod_id.split(',')
        items = self.generate_items(prod_id_list)

        # Serialize items as JSON string
        items_json = json.dumps(items)

        data = {
            "pbs": 1,
            "ts": 1720602704,
            "cid": str(cid),
            "ct": 3,
            "items": items_json  # Store items as JSON string
        }
        yield data


# Define the custom pipeline options
class CustomPipelineOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        # Only include custom arguments not already in GoogleCloudOptions
        parser.add_value_provider_argument('--input_parquet_path', type=str, help='Path to the input Parquet file in GCS')
        parser.add_value_provider_argument('--output_parquet_path', type=str, help='Path to the output Parquet file in GCS')


# Define the pipeline options
pipeline_options = PipelineOptions()
custom_options = pipeline_options.view_as(CustomPipelineOptions)

# Apply the custom options to GoogleCloudOptions and StandardOptions
google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)

# Ensure necessary options are set
if not google_cloud_options.project:
    raise ValueError("The GCP project ID is required.")

pipeline_options.view_as(StandardOptions).runner = 'DataflowRunner'

# Debugging prints to verify the pipeline options
print(f"Final Job Name: {google_cloud_options.job_name}")
print(f"Project: {google_cloud_options.project}")
print(f"Staging Location: {google_cloud_options.staging_location}")
print(f"Temp Location: {google_cloud_options.temp_location}")
print(f"Region: {google_cloud_options.region}")

# Define the schema to match the structure of data using pyarrow
schema = pa.schema([
    ('pbs', pa.int64()),
    ('ts', pa.int64()),
    ('cid', pa.string()),
    ('ct', pa.int64()),
    ('items', pa.string())  # Store items as a JSON string
])

# Create the pipeline
with beam.Pipeline(options=pipeline_options) as p:
    (
        p
        | 'Read from Parquet' >> ReadFromParquet(custom_options.input_parquet_path)
        | 'Process Elements' >> beam.ParDo(ProcessElement())
        | 'Write to Parquet' >> WriteToParquet(
            custom_options.output_parquet_path.get(),
            schema=schema,
            num_shards=1,
            shard_name_template='',
            file_name_suffix='.parquet'
        )
    )


# 2. requirements.txt -
apache-beam[gcp]
pyarrow

#3 . get the service acout.json  file 

#4. create Dockerfile , inside paste the command : 
FROM apache/beam_python3.7_sdk:2.46.0

# Set the working directory
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Install gcloud SDK with updated GPG keys
RUN apt-get update && apt-get install -y curl gnupg2 ca-certificates lsb-release \
    && curl -sSL https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key add - \
    && echo "deb http://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list \
    && apt-get update && apt-get install -y google-cloud-sdk

# Ensure the .config directory exists
RUN mkdir -p /root/.config/gcloud

# Copy the service account key
COPY serv.json /root/.config/gcloud/application_default_credentials.json

# Set the Google Cloud Project environment variable
ENV GOOGLE_CLOUD_PROJECT="playground-s-11-5666c650"

# Authenticate using the service account
RUN gcloud auth activate-service-account --key-file=/root/.config/gcloud/application_default_credentials.json

# Run the pipeline
ENTRYPOINT ["python", "test.py"]


#5. to test docker image I did this . change your project name and bucket and input files details. 
docker run   -e GOOGLE_CLOUD_PROJECT="playground-s-11-8d68d43e"   gcr.io/playground-s-11-8d68d43e/myimg   \
        --project="playground-s-11-8d68d43e"   --job_name="mydataflow"   --staging_location="gs://shuvabuc004/staging"  \
              --temp_location="gs://shuvabuc004/temp"   --region="us-central1"  \
                  --input_parquet_path="gs://shuvabuc004/input/part-00000-c7585b4f-7e43-4685-ad20-ebdb71ca97a6-c000.snappy.parquet" \
        --output_parquet_path="gs://shuvabuc004/out/output-file.parquet"

#6. if successful , I created the flex template . Here you need to give your docker image under image key word and chages all the bucket name. put this file in gcs bucket . 
{
  "name": "My Dataflow Job",
  "description": "This Flex Template processes data using a Dataflow job.",
  "parameters": [
    {
      "name": "input_parquet_path",
      "label": "Input Parquet File Path",
      "helpText": "GCS path to the input Parquet file",
      "paramType": "TEXT",
      "isOptional": false
    },
    {
      "name": "output_parquet_path",
      "label": "Output Parquet File Path",
      "helpText": "GCS path to the output Parquet file",
      "paramType": "TEXT",
      "isOptional": false
    }
  ]
}


#7 . run the command for flex template . change the project and input file name and your flex.json location in gs bucket 

gcloud dataflow flex-template run "mydataflow2" \
  --project="playground-s-11-8d68d43e" \
  --region="us-central1" \
  --template-file-gcs-location "gs://shuvabuc004/flex/flex.json" \
  --parameters project="playground-s-11-8d68d43e",\
job_name="mydataflow2",\
staging_location="gs://shuvabuc004/staging",\
temp_location="gs://shuvabuc004/temp",\
region="us-central1",\
input_parquet_path="gs://shuvabuc004/input/part-00000-c7585b4f-7e43-4685-ad20-ebdb71ca97a6-c000.snappy.parquet",\
output_parquet_path="gs://shuvabuc004/out/output-file.parquet"
