# Import necessary libraries
import re
import apache_beam as beam
from apache_beam.runners.interactive.interactive_runner import InteractiveRunner
import apache_beam.runners.interactive.interactive_beam as ib

# Define a PTransform to read words from text
class ReadWordsFromText(beam.PTransform):
    
    def __init__(self, file_pattern):
        self._file_pattern = file_pattern
    
    def expand(self, pcoll):
        return (pcoll.pipeline
                | beam.io.ReadFromText(self._file_pattern)
                | beam.FlatMap(lambda line: re.findall(r'[\w\']+', line.strip(), re.UNICODE)))

# Set up the Apache Beam pipeline with the Interactive Runner
p = beam.Pipeline(InteractiveRunner())

# Define the PTransform to extract words from a Google Cloud Storage file
words = p | 'read' >> ReadWordsFromText('gs://apache-beam-samples/shakespeare/kinglear.txt')

# Define the PTransform to count words
counts = (words 
          | 'count' >> beam.combiners.Count.PerElement())

# Show the results in the interactive runner
ib.show(counts)

# Define the PTransforms to convert words to lowercase and count them
lower_counts = (words
                | "lower" >> beam.Map(lambda word: word.lower())
                | "lower_count" >> beam.combiners.Count.PerElement())

# Show the results of the lowercase word count
ib.show(lower_counts, visualize_data=True)

# Get a Pandas DataFrame from the PCollection
df = ib.collect(lower_counts)

# Show the job graph for the pipeline
ib.show_graph(p)
