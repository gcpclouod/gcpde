
# Apache Beam Dataflow Pipeline with Dynamic Input and Output Paths

This Apache Beam pipeline reads a CSV file from Google Cloud Storage (GCS), processes it by converting name columns to uppercase, and writes the result back to GCS. The input and output file paths can be dynamically passed when running the Dataflow job via command-line arguments.

## Pipeline Overview

- **Input:** CSV file from GCS.
- **Transformation:** Converts name columns to uppercase.
- **Output:** Transformed CSV file written to GCS.

## Custom Pipeline Options

We define custom pipeline options for `input` and `output` paths to be provided dynamically. This allows the same pipeline to be executed with different input and output paths without modifying the code.

### Key Components:
1. **Custom Options:**
   - `--input`: Path to the input file in GCS.
   - `--output`: Path to the output file in GCS.
   
2. **Value Providers:**
   - `beam.io.ReadFromText(custom_options.input)`: Reads input from the path provided via `--input`.
   - `beam.io.WriteToText(custom_options.output)`: Writes output to the path provided via `--output`.

## Python Script

```python
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

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
        job_name='example-dataflow-job',
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
Running the Dataflow Job from Command Line
To execute the Dataflow job, you can pass the input and output file paths as arguments using the command line:


python beam_script.py \
    --runner=DataflowRunner \
    --project=woven-name-434311-i8 \
    --region=us-central1 \
    --temp_location=gs://gcp35batch/temp_folder \
    --stage_location=gs://gcp35batch/stage_folder \
    --input=gs://gcp35batch/texfile.csv \
    --output=gs://gcp35batch/output/output.csv