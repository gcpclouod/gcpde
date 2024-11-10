import apache_beam as beam

# Define a function to filter categories with fewer than two items
def filter_categories(category_items):
    category, items = category_items # tuple unpacking
    if len(items) >= 3:
        return [(category, items)]
    return []

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

    # Group items by category
    grouped_items = items | 'GroupByCategory' >> beam.GroupByKey()

    # Filter categories with fewer than two items
    filtered_categories = grouped_items | 'FilterCategories' >> beam.FlatMap(filter_categories)

    # Print the final results to the console
    filtered_categories | 'PrintFilteredCategories' >> beam.Map(print)

# No need to explicitly run the pipeline since it's within a 'with' block
