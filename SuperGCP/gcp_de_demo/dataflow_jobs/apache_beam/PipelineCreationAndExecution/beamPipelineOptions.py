import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

# Define the pipeline options (e.g., runner type, project ID, etc.)
pipeline_options = PipelineOptions(
    runner='DirectRunner',  # 'DirectRunner' runs the pipeline locally, use 'DataflowRunner' for Google Cloud Dataflow
    project='dev-project-433015',  # replace with your GCP project ID if using Dataflow
    temp_location='gs://classic-template-demo/tmp/'  # temporary location in GCS if using Dataflow
)
'''
    pipeline_options= PipelineOptions(
        runner='DataflowRunner',
        #temp_location='gs://xmltelecom/tmp/',
        #region='us-central1',
        #staging_location='gs://xmltelecom/staging/',
        num_workers=10, # 
        worker_machine_type='n2-highmem-8',
        worker_disk_type='pd-ssd',
        worker_disk_size_gb=100,
        machine_type='n2-highmem-8',
        #disk_size_gb=500,
        #disk_type='pd-ssd',
        #experiments=['use_runner_v2'],
        #template_location='gs://outfiles_parquet/offer_bank/result/template'
        )

from apache_beam.options.pipeline_options import PipelineOptions

pipeline_options = PipelineOptions(
    runner='DataflowRunner',
    temp_location='gs://xmltelecom/tmp/',  # Uncomment with the correct path
    region='us-central1',  # Uncomment with the correct region
    staging_location='gs://xmltelecom/staging/',  # Uncomment with the correct path
    num_workers=2,  # Minimum number of workers
    max_num_workers=3,  # Maximum number of workers
    worker_machine_type='n1-standard-1',  # Use n1-standard-1 machine type
    worker_disk_type='pd-ssd',
    worker_disk_size_gb=100,  # Worker disk size
    machine_type='n1-standard-1',  # Ensure the main machine type is also n1-standard-1
)


'''


# Define a predicate function to filter out even numbers
def lenGreaterthanfour(word):
    if len(word) >= 4:
        return [word] 


# Define the pipeline
with beam.Pipeline(options=pipeline_options) as p:
    # Read input text file
    lines = p | 'Read' >> beam.io.ReadFromText(r'D:\\DailyNotes\\input.txt')     
    # Split the lines into words
    words = lines | 'Split' >> beam.FlatMap(lambda line: line.split()) # o or more than o elements are created
    
        # Count the occurrences of each word
    word_counts = (
        words
        | 'PairWithOne' >> beam.Map(lenGreaterthanfour)
        | 'Printing' >> beam.Map(print)
        
    )


'''
a simple Apache Beam pipeline using the beam.Pipeline class. 
This example reads text data from a file, applies a simple transformation to count the number 
of occurrences of each word, and then writes the results to an output file.
'''