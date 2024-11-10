import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

class FormatRow(beam.DoFn):
    def process(self, element):
        # Format the row for printing
        yield f'Row: {element}'

# Define the pipeline options with additional parameters
pipeline_options = PipelineOptions(
    flags=[
        '--project=dev-project-433015',
        '--runner=DirectRunner',
        '--temp_location=gs://classic-template-demo/temp/',   # Replace with your GCS bucket path
        '--staging_location=gs://classic-template-demo/staging/',  # Replace with your GCS bucket path
    ]
)

# Define the pipeline
with beam.Pipeline(options=pipeline_options) as p:
    # Read data from the public BigQuery dataset
    query1 = 'SELECT word FROM `bigquery-public-data.samples.shakespeare` WHERE corpus = "hamlet" LIMIT 10'
    query2 = 'SELECT word FROM `bigquery-public-data.samples.shakespeare` WHERE corpus = "macbeth" LIMIT 10'
    
    # Read data from two queries
    shakespeare_data1 = p | 'ReadFromQuery1' >> beam.io.ReadFromBigQuery(query=query1, use_standard_sql=True)
    # Print the results
    shakespeare_data1 | 'PrintResults1' >> beam.Map(print)

    shakespeare_data2 = p | 'ReadFromQuery2' >> beam.io.ReadFromBigQuery(query=query2, use_standard_sql=True)
      # Print the results
    shakespeare_data2 | 'PrintResults2' >> beam.Map(print)
    
    # Merge the two PCollections into one using Flatten
    merged_data = (shakespeare_data1, shakespeare_data2) | 'FlattenTables' >> beam.Flatten()
    
    # Print the results
    merged_data | 'PrintResults3' >> beam.Map(print)

    # Apply a transformation to format the rows for printing
    formatted_output = merged_data | 'FormatRows' >> beam.ParDo(FormatRow())
    
    # Print the results
    formatted_output | 'PrintResults4' >> beam.Map(print)
