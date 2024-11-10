import apache_beam as beam

# Define a user-defined function (UDF) to filter out short words
def filter_short_words(word):
    if len(word) >= 4:
        yield word

# Define the pipeline
with beam.Pipeline() as p:
    # Create a PCollection with a list of sentences
    sentences = p | 'CreateSentences' >> beam.Create([
        'Apache Beam is awesome',
        'It is powerful and flexible',
        'Learn Beam for data processing'
    ])

    # Apply a FlatMap transform to split each sentence into words using a lambda function
    words = sentences | 'SplitIntoWords' >> beam.FlatMap(lambda sentence: sentence.split())

    # Apply another FlatMap transform using the UDF to filter out short words
    filtered_words = words | 'FilterShortWords' >> beam.FlatMap(filter_short_words)

    # Print the final results to the console
    filtered_words | 'Print' >> beam.Map(print)

# No need to explicitly run the pipeline since it's within a 'with' block

'''
beam.FlatMap is a powerful transformation that allows a function to return zero, one, or more elements for each input element. 
This can be especially useful when you want to split or expand elements.

The example will take a list of sentences, split each sentence into words, 
and then filter out short words (less than 4 characters) using a UDF.

'''