import mysql.connector
import pandas as pd
from google.cloud import storage

# Database connection configuration
config = {
    'host': '34.66.73.145',
    'user': 'root',
    'password': 'Hani',
    'database': 'customers'
}

# GCS configuration
bucket_name = 'objecstsforbckt'  # Replace with your GCS bucket name
destination_blob_name = 'customers_data_with_fullname_male.ndjson'  # Desired file name in GCS

# Create a connection to the database
try:
    connection = mysql.connector.connect(**config)
    cursor = connection.cursor()

    # Query to fetch data from the Customers table
    query = "SELECT * FROM Customers"
    cursor.execute(query)

    # Fetch column names
    columns = [desc[0] for desc in cursor.description]

    # Fetch all results
    results = cursor.fetchall()

    # Convert results to a DataFrame
    df = pd.DataFrame(results, columns=columns)

    # Filter DataFrame to include only Male gender
    df = df[df['Gender'] == 'Male']

    # Join FirstName and LastName into a new column FullName
    df['FullName'] = df['FirstName'] + ' ' + df['LastName']

    # Drop FirstName and LastName columns
    df = df.drop(columns=['FirstName', 'LastName'])

    # Convert DataFrame to NDJSON
    ndjson_data = df.to_json(orient='records', lines=True)

    # Upload NDJSON to GCS
    client = storage.Client(project='first-project-428309')
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    # Upload the NDJSON data as a string
    blob.upload_from_string(ndjson_data, content_type='application/x-ndjson')

    print(f"File {destination_blob_name} uploaded to {bucket_name}.")

except mysql.connector.Error as err:
    print(f"Error: {err}")

finally:
    # Close the cursor and connection
    if cursor:
        cursor.close()
    if connection:
        connection.close()
