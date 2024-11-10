import apache_beam as beam

# Define a predicate function to filter out even numbers
def is_odd(number):
    return number % 2 == 0

# Define the pipeline
with beam.Pipeline() as p:
    # Create a PCollection with a list of numbers
    numbers = p | 'CreateNumbers' >> beam.Create([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])

    # Apply a Filter transform to keep only odd numbers
    odd_numbers = numbers | 'FilterOddNumbers' >> beam.Filter(is_odd)

    # Print the final results to the console
    odd_numbers | 'Print' >> beam.Map(print)

# No need to explicitly run the pipeline since it's within a 'with' block
'''
he beam.Filter transform is used to filter elements in a PCollection based on a predicate function. 
The predicate function should return True for elements that should be included in the output PCollection 
and False for those that should be excluded.

'''