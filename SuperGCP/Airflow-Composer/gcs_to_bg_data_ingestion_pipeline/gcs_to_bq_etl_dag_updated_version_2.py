# Import necessary Airflow modules and Python standard libraries
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor
from airflow.operators.python import BranchPythonOperator
from airflow.operators.empty import EmptyOperator  # Import EmptyOperator for start/end tasks
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime
from google.cloud import bigquery
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import logging  # For error logging

# Configure logging with custom format and level
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Branching function with logging
def choose_task(**kwargs):
    ti = kwargs['ti']
    sensor_result = ti.xcom_pull(task_ids='gcs_sensor')

    # Log result of GCS sensor
    if sensor_result:
        logging.info("File found in GCS. Proceeding to load data to BigQuery.")
        return 'load_data_to_bigquery'
    else:
        logging.warning("File not found in GCS. Proceeding to send email notification.")
        return 'file_not_found_email'

# Function to load data into BigQuery with detailed logging
def load_data_to_bigquery():
    logging.info("Starting data load to BigQuery...")
    try:
        client = bigquery.Client()
        table_id = "woven-name-434311-i8.batch35.orders10"  # Change the table ID
        job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField("Customer_id", "STRING"),
                bigquery.SchemaField("date", ""),
                bigquery.SchemaField("time", "STRING"),
                bigquery.SchemaField("order_id", "STRING"),
                bigquery.SchemaField("items", "STRING"),
                bigquery.SchemaField("amount", "INTEGER"),
                bigquery.SchemaField("mode", "STRING"),
                bigquery.SchemaField("restaurant", "STRING"),
                bigquery.SchemaField("Status", "STRING"),
                bigquery.SchemaField("ratings", "INTEGER"),
                bigquery.SchemaField("feedback", "STRING"),
            ],
            skip_leading_rows=1,
            source_format=bigquery.SourceFormat.CSV,
        )
        uri = "gs://gcpclouddataengineeringbatch32/food_orders_daily.csv"  # Change the GCS URI
        logging.info(f"Loading data from {uri} to BigQuery table {table_id}")
        load_job = client.load_table_from_uri(uri, table_id, job_config=job_config)
        load_job.result()  # Wait for the job to complete

        destination_table = client.get_table(table_id)
        logging.info(f"Successfully loaded {destination_table.num_rows} rows into {table_id}")

    except Exception as e:
        logging.error(f"Failed to load data to BigQuery: {e}")
        raise  # Re-raise the exception to trigger task failure

# Custom function to send an email using Gmail with logging
def send_email(subject, body, to_email):
    logging.info(f"Preparing to send email to {to_email} with subject '{subject}'")
    try:
        gmail_user = 'gcpcloud305@gmail.com'
        gmail_password = 'nqvl cfhd uboc rpaq'

        msg = MIMEMultipart()
        msg['From'] = gmail_user
        msg['To'] = to_email
        msg['Subject'] = subject
        msg.attach(MIMEText(body, 'html'))

        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
            server.login(gmail_user, gmail_password)
            server.sendmail(gmail_user, to_email, msg.as_string())

        logging.info(f"Email sent successfully to {to_email}")

    except Exception as e:
        logging.error(f"Failed to send email to {to_email}: {e}")
        # Log the error but do not stop the DAG

# Function to send file-not-found email with logging
def file_not_found_email():
    logging.info("Preparing file-not-found email notification.")
    subject = "GCS File Not Found - Airflow DAG"
    body = """
    <h3>Data Ingestion Failure</h3>
    <p>The food_orders_daily.csv file was not found in the GCS bucket: gcpclouddataengineeringbatch32.<br>
    Please ensure the file is uploaded to proceed with the data ingestion pipeline.</p>
    """
    send_email(subject, body, 'lavu2016hani@gmail.com')

# Function to send a success email after data load with logging
def send_success_email():
    logging.info("Preparing data load success email.")
    subject = "BigQuery Load Success - Airflow DAG"
    body = """
    <h3>Data Ingestion Success</h3>
    <p>The data from the food_orders_daily.csv file has been successfully loaded into the BigQuery table: woven-name-434311-i8.batch35.orders10.</p>
    """
    send_email(subject, body, 'lavu2016hani@gmail.com')

# Function to send a failure email after data load failure with logging
def send_failure_email():
    logging.info("Preparing data load failure email.")
    subject = "BigQuery Load Failure - Airflow DAG"
    body = """
    <h3>Data Ingestion Failure</h3>
    <p>There was an error while loading data into the BigQuery table: woven-name-434311-i8.batch35.orders10.<br>
    Please check the logs for more details.</p>
    """
    send_email(subject, body, 'lavu2016hani@gmail.com')

# Define default_args for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 9, 23),
    'retries': 1,
}

# Define the DAG
with DAG(
    dag_id="gcs_to_bigquery_etl_job_with_error_handling",
    default_args=default_args,
    schedule_interval=None,  # Manual trigger or no schedule
    catchup=False
) as dag:

    # Task 1: Start task using EmptyOperator
    start_task = EmptyOperator(task_id='start')

    # Task 2: Check for the existence of the CSV file in the GCS bucket
    gcs_sensor = GCSObjectsWithPrefixExistenceSensor(
        task_id='gcs_sensor',
        bucket='gcpclouddataengineeringbatch32',  # Change the bucket name
        prefix='food_orders_daily',
        mode='poke',
        poke_interval=60,  # Check every 60 seconds
        timeout=300  # Stop after 5 minutes if no file is found
    )

    # Task 3: Define the BranchPythonOperator task
    branching = BranchPythonOperator(
        task_id='branching',
        python_callable=choose_task,
        provide_context=True,
        trigger_rule='all_done'
    )

    # Task 4: Load the data to BigQuery if file exists
    load_data_task = PythonOperator(
        task_id='load_data_to_bigquery',
        python_callable=load_data_to_bigquery
    )

    # Task 5: Send email if file is not found
    send_failure_email_task = PythonOperator(
        task_id='file_not_found_email',
        python_callable=file_not_found_email
    )

    # Task 6: Send success email after data is loaded into BigQuery
    send_success_email_task = PythonOperator(
        task_id='send_success_email',
        python_callable=send_success_email
    )

    # Task 7: Send failure email if BigQuery loading fails
    send_failure_bq_email_task = PythonOperator(
        task_id='send_failure_email',
        python_callable=send_failure_email,
        trigger_rule=TriggerRule.ONE_FAILED  # Send only if load_data_task fails
    )

    # Task 8: End task using EmptyOperator with modified trigger rule
    end_task = EmptyOperator(
        task_id='end',
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    )

    # Define task dependencies
    start_task >> gcs_sensor >> branching  # Start task before sensor
    branching >> load_data_task >> [send_success_email_task, send_failure_bq_email_task] >> end_task  # Success path with failure email handling
    branching >> send_failure_email_task >> end_task  # Failure path