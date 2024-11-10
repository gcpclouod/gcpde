import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam import window
from apache_beam.io.gcp.internal.clients import pubsub
import json
from datetime import datetime

# Define the custom DoFn to process elements
class ProcessMessages(beam.DoFn):
    def process(self, element, window=beam.DoFn.WindowParam):
        # Parse the JSON message
        message = json.loads(element)
        # Extract timestamp and convert to datetime object
        timestamp = datetime.fromisoformat(message['timestamp'])
        # Yield the message and window information
        yield (message['event'], window.start.to_utc_datetime(), window.end.to_utc_datetime())

# Define pipeline options
pipeline_options = PipelineOptions(
    flags=['--project=your-project-id', '--runner=DataflowRunner']  # Use DataflowRunner for streaming
)

# Define the pipeline
with beam.Pipeline(options=pipeline_options) as p:
    # Read messages from Pub/Sub
    messages = (p
        | 'ReadFromPubSub' >> beam.io.ReadFromPubSub(subscription='projects/your-project-id/subscriptions/your-subscription-id')
        | 'WindowInto' >> beam.WindowInto(window.FixedWindows(60))  # 60 seconds window
        | 'ProcessMessages' >> beam.ParDo(ProcessMessages())
    )
    
    # Print the results
    messages | 'PrintResults' >> beam.Map(print)
