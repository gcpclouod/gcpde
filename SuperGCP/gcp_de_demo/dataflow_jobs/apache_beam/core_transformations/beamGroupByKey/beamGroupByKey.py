import apache_beam as beam

# Define the pipeline
with beam.Pipeline() as p:
    # Create a PCollection with key-value pairs (category, item)
    items = p | 'CreateItems' >> beam.Create([
        ('Electronics', 'Laptop'),
        ('Clothing', 'T-shirt'),
        ('Electronics', 'Smartphone'),
        ('Clothing', 'Jeans'),
        ('Books', 'Novel'),
        ('Electronics', 'Headphones'),
        ('Books', 'Magazine')
    ])

    # Apply a GroupByKey transform to group items by category
    grouped_items = items | 'GroupByCategory' >> beam.GroupByKey()

    # Print the final results to the console
    grouped_items | 'PrintGroupedItems' >> beam.Map(print)

# No need to explicitly run the pipeline since it's within a 'with' block
