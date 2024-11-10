import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

# Define a DoFn to format the results for printing
class FormatResult(beam.DoFn):
    def process(self, element):
        key, value = element
        quantities, prices = value
        yield f'Order ID: {key}, Quantities: {quantities}, Prices: {prices}'

# Define the pipeline options
pipeline_options = PipelineOptions(
    flags=[
        '--project=your-project-id',
        '--runner=DirectRunner',  # Use DataflowRunner for running on Google Cloud Dataflow
    ]
)

# Define the pipeline
with beam.Pipeline(options=pipeline_options) as p:
    # Define two PCollections with key-value pairs
    pcollection1 = p | 'CreatePCollection1' >> beam.Create([
        ('order1', 5),
        ('order2', 3),
        ('order3', 2),
    ])
    
    pcollection2 = p | 'CreatePCollection2' >> beam.Create([
        ('order1', 100.0),
        ('order2', 200.0),
        ('order3', 150.0),
    ])
    
    # Apply CoGroupByKey to group the two PCollections by key
    grouped_data = (
        {
            'quantities': pcollection1,
            'prices': pcollection2
        }
        | 'CoGroupByKey' >> beam.CoGroupByKey()
    )
    
    # Format and print the results
    formatted_output = grouped_data | 'FormatResults' >> beam.ParDo(FormatResult())
    formatted_output | 'PrintResults' >> beam.Map(print)

'''
The beam.CoGroupByKey transform is used to group data from multiple PCollection objects by key. 
It combines elements from several PCollections that share the same key into a single PCollection of key-value pairs, where each value is a dictionary containing the grouped values 
for each input PCollection.
'''