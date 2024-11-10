from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago
from google.cloud import storage
import mysql.connector
import pandas as pd

# Define the function to perform the task
def load_data_to_gcs():
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

# Define the default_args dictionary
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
}

# Define variables for BigQuery load command
bucket_name = 'objecstsforbckt'
ndjson_file_path = f'gs://{bucket_name}/customers_data_with_fullname_male.ndjson'
schema_file_path = '/tmp/schema/local-schema-file.json'
dataset_table = 'customers_dataset.customers_male'

# Create the DAG
with DAG(
    dag_id='cloud_sql_to_gcs_and_gcs_to_bq_stage_dag',
    default_args=default_args,
    description='An example DAG with PythonOperator and BashOperator',
    schedule_interval=None,  # Adjust schedule as needed
) as dag:

    # Define the dummy task
    start_task = DummyOperator(
        task_id='start_task',
    )

    # Define the Python task to load data into GCS
    load_data_task = PythonOperator(
        task_id='load_data_to_gcs',
        python_callable=load_data_to_gcs,
    )
    copy_json_schema = BashOperator(
    task_id='copy_json_schema',
    bash_command='gsutil cp  gs://us-central1-composer-airflo-6cf103e3-bucket/dags/schema_for_stage_table.json /tmp/schema/local-schema-file.json',
    )

    # Define the Bash task to load NDJSON file from GCS into BigQuery with schema
    bq_command = BashOperator(
        task_id='bq_command',
        bash_command=f"""
        bq load \
        --replace \
        --source_format=NEWLINE_DELIMITED_JSON \
        --schema={schema_file_path} \
        {dataset_table} \
        {ndjson_file_path}
        """,
    )
    trigger = TriggerDagRunOperator(
        task_id='trigger_secondary_dag',
        trigger_dag_id='BQ_MERGE_DAG',    # Optional: define a context for the triggered DAG
    )
    end_task = DummyOperator(
        task_id='end_task',
    )
    # Set task dependencies
    start_task >> load_data_task >> copy_json_schema >> bq_command >> trigger >> end_task
