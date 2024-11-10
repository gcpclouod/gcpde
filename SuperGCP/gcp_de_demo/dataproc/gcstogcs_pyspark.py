from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder \
    .master("yarn") \
    .appName("dataprocLearning") \
    .getOrCreate()

# Read CSV File from GCS
df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "false") \
    .csv("gs://dataproc-bucket-demo-31/students.csv")

# Perform transformations (e.g., renaming columns)
df = df.withColumnRenamed("CustomerID", "Cust_ID") \
       .withColumnRenamed("FirstName", "F_Name") \
       .withColumnRenamed("LastName", "L_Name") \
       .withColumnRenamed("Email", "Email_ID") \
       .withColumnRenamed("Phone", "Contact") \
       .withColumnRenamed("Address", "Addresses") \
       .withColumnRenamed("Username", "UserName") \
       .withColumnRenamed("Password", "Password") \
       .withColumnRenamed("DateOfBirth", "DOB") \
       .withColumnRenamed("CreatedAt", "Created_Time")

# Write DataFrame to GCS in CSV format
df.write \
    .option("header", "true") \
    .mode("overwrite") \
    .csv("gs://dataproc-bucket-demo-31/output/students_transformed.csv")

# Stop SparkSession
spark.stop()
