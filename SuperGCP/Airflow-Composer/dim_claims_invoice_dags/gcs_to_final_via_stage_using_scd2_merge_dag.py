from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_local import GCSToLocalFilesystemOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import pandas_gbq
from google.oauth2 import service_account

# DAG default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 10),
    'retries': 1
}

# Define DAG
dag = DAG(
    'gcs_to_bigquery_invoice_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
)

# Variables
gcs_bucket = 'gcp35batch'
gcs_invoice_file = 'claims_invoices.csv'
local_file_path = '/tmp/invoice_file.csv'
project_id = 'woven-name-434311-i8'
dataset_id = 'claims_datalake'
staging_table = f'{project_id}.{dataset_id}.stg_claim_invoice'
dim_table = f'{project_id}.{dataset_id}.dim_claim_invoice'

# Function to load data into BigQuery staging table
def load_data_to_bigquery(**context):
    # Read the invoice data from the local CSV file
    df = pd.read_csv(local_file_path)
    
    # Data transformations if needed
    df['Clm_invoice_date'] = pd.to_datetime(df['Clm_invoice_date'], format='%d-%m-%Y').dt.date
    
    # Convert the date column to string format for BigQuery compatibility
    df['Clm_invoice_date'] = df['Clm_invoice_date'].astype(str)

    df['Source_timestamp'] = pd.to_datetime(df['Source_timestamp'])

    # Load data into BigQuery staging table using pandas_gbq
    # credentials = service_account.Credentials.from_service_account_file('/path/to/your/keyfile.json')
    pandas_gbq.to_gbq(
        dataframe=df,
        destination_table=f'{dataset_id}.stg_claim_invoice',
        project_id=project_id,
        if_exists='replace',  # Replaces table data
        # credentials=credentials
    )

# Step 1: Download the invoice CSV from GCS to local
download_csv = GCSToLocalFilesystemOperator(
    task_id='download_csv',
    bucket=gcs_bucket,
    object_name=gcs_invoice_file,
    filename=local_file_path,
    dag=dag
)

# Step 2: Load data into BigQuery staging table
load_to_bq = PythonOperator(
    task_id='load_data_to_bigquery',
    python_callable=load_data_to_bigquery,
    dag=dag
)

# Step 3: Merge data from staging to dimension table in BigQuery
merge_query = f"""
MERGE INTO {dim_table} AS target
USING (
    SELECT 
        Clm_invoice_id, 
        Clm_invoice_date, 
        CAST(Clm_invoice_number AS STRING) AS Clm_invoice_number, -- Cast the invoice number to STRING
        Source_timestamp
    FROM {staging_table}
) AS source
ON target.Clm_invoice_id = source.Clm_invoice_id
WHEN MATCHED AND CAST(target.Clm_invoice_number AS STRING) != CAST(source.Clm_invoice_number AS STRING) THEN
  UPDATE SET  
    target.Updated_timestamp = source.Source_timestamp,
    target.CurrentFlag = FALSE
WHEN NOT MATCHED THEN
  INSERT (
    Clm_invoice_key, Clm_invoice_id, Clm_invoice_date, 
    Clm_invoice_number, Inserted_timestamp, CurrentFlag
  )
  VALUES (
    GENERATE_UUID(),
    source.Clm_invoice_id, 
    CAST(source.Clm_invoice_date AS DATE), -- Ensuring correct type for the date
    source.Clm_invoice_number, 
    source.Source_timestamp,
    TRUE
  );
"""

merge_to_dim_table = BigQueryInsertJobOperator(
    task_id='merge_to_dim_table',
    configuration={
        'query': {
            'query': merge_query,
            'useLegacySql': False,
        }
    },
    location='US',
    dag=dag
)

# Step 4: insert_updated_records_to_dim_table data from staging to dimension table in BigQuery
insert_updated_records = f"""
INSERT INTO woven-name-434311-i8.claims_datalake.dim_claim_invoice (
    Clm_invoice_key, 
    Clm_invoice_id, 
    Clm_invoice_date, 
    Clm_invoice_number, 
    Inserted_timestamp, 
    CurrentFlag
)
SELECT 
    GENERATE_UUID(),  -- Generate new UUID as the primary key
    source.Clm_invoice_id, 
    CAST(source.Clm_invoice_date AS DATE),  -- Ensure Clm_invoice_date is cast to DATE type
    CAST(source.Clm_invoice_number AS STRING),  -- Ensure Clm_invoice_number is cast to STRING
    CURRENT_TIMESTAMP(),  -- Use current timestamp for the inserted time
    TRUE  -- Set the CurrentFlag to TRUE
FROM (
    SELECT 
        *, 
        ROW_NUMBER() OVER (PARTITION BY Clm_invoice_id ORDER BY CURRENT_TIMESTAMP()) AS rn  -- Assign row numbers for each invoice_id
    FROM 
        woven-name-434311-i8.claims_datalake.dim_claim_invoice
    WHERE 
        CurrentFlag = FALSE
) AS target
JOIN 
    woven-name-434311-i8.claims_datalake.stg_claim_invoice AS source
ON 
    target.Clm_invoice_id = source.Clm_invoice_id
WHERE 
    target.rn = 1  -- Select only the first unique record for each Clm_invoice_id
    AND CAST(target.Clm_invoice_number AS STRING) != CAST(source.Clm_invoice_number AS STRING);  -- Ensure comparison is between STRING types
"""

insert_updated_records_to_dim_table = BigQueryInsertJobOperator(
    task_id='insert_updated_records_to_dim_table',
    configuration={
        'query': {
            'query': insert_updated_records,
            'useLegacySql': False,
        }
    },
    location='US',
    dag=dag
)

# Step 5: Truncate the staging table after the merge
truncate_staging_table = BigQueryInsertJobOperator(
    task_id='truncate_staging_table',
    configuration={
        'query': {
            'query': f'TRUNCATE TABLE {staging_table};',
            'useLegacySql': False,
        }
    },
    location='US',
    dag=dag
)

# DAG workflow
download_csv >> load_to_bq >> merge_to_dim_table >> insert_updated_records_to_dim_table >> truncate_staging_table
