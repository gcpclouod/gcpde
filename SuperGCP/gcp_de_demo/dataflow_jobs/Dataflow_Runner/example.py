import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

def run_dataflow_runner_pipeline():
    options = PipelineOptions(
        project='dev-project-433015',
        region='us-central1',
        temp_location='gs://dataflow_flex_template_demo_bucket_12345/temp_folder',
        runner='DataflowRunner',
        job_name='example-dataflow-job'
    )

    with beam.Pipeline(options=options) as p:
        (p
         | 'Create' >> beam.Create([1, 2, 3, 4, 5])
         | 'Multiply by 2' >> beam.Map(lambda x: x * 2)
         | 'Write to GCS' >> beam.io.WriteToText('gs://dataflow_flex_template_demo_bucket_12345/spl_thirty'))

if __name__ == "__main__":
    run_dataflow_runner_pipeline()
