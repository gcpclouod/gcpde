import apache_beam as beam

# Define the aggregation functions
def sum_values(values):
    return sum(values)

def count_values(values):
    return len(values)

def average_values(values):
    return sum(values) / len(values) if values else 0

def min_value(values):
    return min(values) if values else float('inf')

def max_value(values):
    return max(values) if values else float('-inf')

# Define the pipeline
with beam.Pipeline() as p:
    # Create a PCollection with key-value pairs
    data = p | 'CreateData' >> beam.Create([
        ('CategoryA', 10),
        ('CategoryA', 20),
        ('CategoryB', 5),
        ('CategoryB', 15),
        ('CategoryA', 30),
        ('CategoryB', 10)
    ])

    # Apply various aggregations
    summed = data | 'SumValues' >> beam.CombinePerKey(sum) 
    minimum = data | 'MinValue' >> beam.CombinePerKey(min)
    maximum = data | 'MaxValue' >> beam.CombinePerKey(max)

    # Print the results
    summed | 'PrintSum' >> beam.Map(print) 
    minimum | 'PrintMin' >> beam.Map(print)
    maximum | 'PrintMax' >> beam.Map(print)

# No need to explicitly run the pipeline since it's within a 'with' block 
'''
The beam.CombinePerKey transform is used to perform aggregation operations on values associated with each key 
in a keyed PCollection. This is typically used for operations like summing, averaging, 
or finding the maximum/minimum values per key.

A PCollection of key-value pairs where the key represents a category (e.g., a product) 
and the value represents the quantity sold. We want to compute the total quantity sold for each category.

'''