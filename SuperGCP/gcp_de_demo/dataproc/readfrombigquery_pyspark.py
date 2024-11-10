from pyspark.sql import SparkSession

# Initialize the Spark session
# Create SparkSession
spark = SparkSession.builder \
    .master("yarn") \
    .appName("bigquerydata") \
    .getOrCreate()

# Define the BigQuery dataset and table
project_id = "woven-name-434311-i8"
dataset_id = "cnn_project"
table_id = "Customer"

# Load data from BigQuery
df = spark.read.format('bigquery') \
    .option('table', f'{project_id}:{dataset_id}.{table_id}') \
    .load()

# Show the schema and data
df.printSchema()
df.show()
spark.stop()