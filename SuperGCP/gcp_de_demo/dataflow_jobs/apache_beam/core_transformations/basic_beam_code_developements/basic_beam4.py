import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions 

# Define custom pipeline options to accept input and output paths
class CustomOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument('--input', type=str, help='Input file path')
        parser.add_value_provider_argument('--output', type=str, help='Output file path')

# Function to convert name columns to uppercase
def uppercase(element):
    row = element.split()
    emptylist = []
    
    for col in range(len(row)-1):
        if col == 1:
            emptylist.append(row[col][:len(row[col])-1].upper())
        else:
            emptylist.append(row[col][:len(row[col])-1])
    emptylist.append(row[len(row)-1])
    
    output = ",".join(emptylist)
    return output

# Main pipeline function
def run():
    # Create pipeline options
    options = PipelineOptions()
    custom_options = options.view_as(CustomOptions)
    
    # Ensure the job can be scaled with temp and stage locations
    pipeline_options = PipelineOptions(
        project='woven-name-434311-i8',
        region='us-central1',
        temp_location='gs://gcp35batch/temp_folder',
        stage_location='gs://gcp35batch/stage_folder',
        runner='DataflowRunner',
        job_name='example-dataflow-job2',
        num_workers=2,
        max_num_workers=3,
        worker_machine_type='n1-standard-1',
        worker_disk_type='pd-ssd',
        worker_disk_size_gb=100,
    )
    
    # Start the pipeline
    with beam.Pipeline(options=pipeline_options) as pipeline:
        # Read the input file from GCS
        pcollection = pipeline | 'Input Pcollection' >> beam.io.ReadFromText(custom_options.input, skip_header_lines=1)
        
        # Apply transformation to uppercase the name columns
        transformation1 = pcollection | 'Make Name columns values uppercased' >> beam.Map(uppercase)
        
        # Write the output to GCS
        transformation1 | 'Storing to GCS' >> beam.io.WriteToText(custom_options.output)

if __name__ == '__main__':
    run()
