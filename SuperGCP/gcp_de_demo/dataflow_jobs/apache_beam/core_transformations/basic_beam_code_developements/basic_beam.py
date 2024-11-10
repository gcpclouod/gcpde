import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

# External sources
gcs_input_path = 'gs://gcp35batch/texfile.csv'
gcs_output_path = 'gs://gcp35batch/output/texfile.csv'

# Pipeline options
options = PipelineOptions(
    project='woven-name-434311-i8',
    region='us-central1',
    temp_location='gs://gcp35batch/temp_folder',
    staging_location='gs://gcp35batch/staging',
    runner='DataflowRunner',
    job_name='example-dataflow-job'
)

# Function to eliminate the last character from each word
def Eleminating_last_char(element):
    row = element.split()  # Split the string into a list based on spaces
    result = stripping_last_unnecessary_character(row)
    output = ",".join(result)
    return output

# Helper function to strip the last unnecessary character
def stripping_last_unnecessary_character(list):
    emptylist = []
    for i in range(len(list) - 1):  # Strip the last character from each word, except the last one
        emptylist.append(list[i][:len(list[i]) - 1])
    emptylist.append(list[len(list) - 1])  # Keep the last word unchanged
    return emptylist

# Apache Beam pipeline
with beam.Pipeline(options=options) as pipeline:
    # Reading data from GCS
    pcollection = pipeline | 'Read from GCS' >> beam.io.ReadFromText(gcs_input_path, skip_header_lines=1)
    
    # Applying transformation using the UDF
    transformation1 = pcollection | 'Apply UDF to Remove Last Char' >> beam.Map(Eleminating_last_char)
    
    # Writing the transformed data back to GCS
    result = transformation1 | 'Write to GCS' >> beam.io.WriteToText(gcs_output_path)
