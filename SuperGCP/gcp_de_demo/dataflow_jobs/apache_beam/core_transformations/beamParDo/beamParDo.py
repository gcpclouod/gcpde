import apache_beam as beam

# Define a custom DoFn class for processing strings
class ProcessStrings(beam.DoFn):
    def process(self, element):
        # Convert the string to uppercase and append a suffix
        processed_string = f"{element.upper()}_PROCESSED"
        # Yield the processed string
        yield processed_string    

# Define the pipeline
with beam.Pipeline() as p:
    # Create a PCollection with a list of strings
    strings = p | 'CreateStrings' >> beam.Create([
        'hello',
        'world',
        'apache beam',
        'data processing'
    ])
        # Apply the ParDo transform with the custom DoFn class
    processed_strings = strings | 'ProcessStrings' >> beam.ParDo(ProcessStrings())

    # Print the final results to the console
    processed_strings | 'PrintProcessedStrings' >> beam.Map(print)



# No need to explicitly run the pipeline since it's within a 'with' block
'''
The beam.ParDo transform is one of the most versatile transforms in Apache Beam. 
It allows you to apply a custom processing function (DoFn) to each element of a PCollection. 
ParDo can be used for a wide range of tasks, including filtering, transforming, 
and more complex operations that require parallel processing.
'''