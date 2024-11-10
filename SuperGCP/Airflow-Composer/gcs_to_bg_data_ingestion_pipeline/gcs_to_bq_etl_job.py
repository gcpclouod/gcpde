# Import necessary Airflow modules and Python standard libraries
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor
from datetime import datetime
from google.cloud import bigquery
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# Function to load data into BigQuery
def load_data_to_bigquery():
    client = bigquery.Client()
    table_id = "woven-name-434311-i8.batch35.orders8"
    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("Customer_id", "STRING"),  
            bigquery.SchemaField("date", "STRING"),  
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
    uri = "gs://gcpclouddataengineeringbatch32/food_orders_daily.csv"
    load_job = client.load_table_from_uri(uri, table_id, job_config=job_config)
    load_job.result()
    destination_table = client.get_table(table_id)
    print(f"Loaded {destination_table.num_rows} rows.")

# Custom function to send an email using Gmail with app password
def send_email(subject, body, to_email):
    gmail_user = 'gcpcloud305@gmail.com'
    gmail_password = 'vtuk jzrb xmko fuvt'

    msg = MIMEMultipart()
    msg['From'] = gmail_user
    msg['To'] = to_email
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'html'))

    with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
        server.login(gmail_user, gmail_password)
        server.sendmail(gmail_user, to_email, msg.as_string())

    print(f"Email sent to {to_email}")

# Function to be triggered if the GCS file is not found
def file_not_found_email():
    subject = "GCS File Not Found - Airflow DAG"
    body = """
    <h3>Data Ingestion Failure</h3>
    <p>The food_orders_daily.csv file was not found in the GCS bucket: e-commerce-business-bucket.<br>
    Please ensure the file is uploaded to proceed with the data ingestion pipeline.</p>
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
    dag_id="bigquery_load_job_dag",
    default_args=default_args,
    schedule_interval=None,  
    catchup=False
) as dag:

    # Task 1: Check for the existence of the CSV file in the GCS bucket
    gcs_sensor = GCSObjectsWithPrefixExistenceSensor(
        task_id='gcs_sensor',
        bucket='gcpclouddataengineeringbatch32',
        prefix='food_orders_daily',
        mode='poke',  
        poke_interval=60,  
        timeout=300  
    )

    # Task 2: Load the data to BigQuery if file exists
    load_data_task = PythonOperator(
        task_id='load_data_to_bigquery',
        python_callable=load_data_to_bigquery
    )

    # Task 3: Send email if file is not found
    send_email_task = PythonOperator(
        task_id='file_not_found_email',
        python_callable=file_not_found_email
    )

    # Set the task dependencies
    gcs_sensor >> load_data_task  # If file exists, proceed to load data
    gcs_sensor >> send_email_task  # If file doesn't exist, send email notification
