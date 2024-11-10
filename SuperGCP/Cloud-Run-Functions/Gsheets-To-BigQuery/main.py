import os
import gspread
from gspread_dataframe import set_with_dataframe, get_as_dataframe
from google.oauth2.service_account import Credentials
from google.cloud import secretmanager, bigquery
import pandas as pd
import json
import functions_framework 

@functions_framework.http 
def gsheets_to_bq_cloud_func(request):
    # Define scopes for accessing Google services
    scopes = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive',
        'https://www.googleapis.com/auth/bigquery'
    ]

    # Access Secret Manager to retrieve service account credentials
    client = secretmanager.SecretManagerServiceClient()
    name = "projects/840233991363/secrets/bqtogsheets/versions/1"
    response = client.access_secret_version(name=name)
    payload = response.payload.data.decode("UTF-8")
    service_account_info = json.loads(payload)
    
    # Create credentials from service account info
    credentials = Credentials.from_service_account_info(service_account_info, scopes=scopes)

    # Authorize and connect to Google Sheets
    gc = gspread.authorize(credentials)
    sheet_key = "1LQi7CAc_oaoiFTQMTbGF19iaOLzpdWsbukBWUZ-kptM"
    gs = gc.open_by_key(sheet_key)
    worksheet = gs.worksheet('Sheet1')

    # Retrieve data from Google Sheets and load into a DataFrame
    df = get_as_dataframe(worksheet, dtype=str).dropna(how="all")  # Avoid rows with NaN values
    print(df)

    # Convert columns to appropriate data types based on the BigQuery schema
    try:
        df['emp_id'] = pd.to_numeric(df['emp_id'], errors='coerce', downcast='integer')  # Convert to INTEGER
        df['emp_name'] = df['emp_name'].astype(str)  # Ensure emp_name is STRING
        df['emp_age'] = pd.to_numeric(df['emp_age'], errors='coerce', downcast='integer')  # Convert to INTEGER
        df['emp_salary'] = pd.to_numeric(df['emp_salary'], errors='coerce', downcast='integer')  # Convert to INTEGER
    except KeyError as e:
        print(f"Column not found: {e}")
        return "Error: Missing columns in the DataFrame."

    # Remove any rows with NaN values after conversion
    df = df.dropna()

    # Initialize BigQuery client
    bq_client = bigquery.Client(credentials=credentials, project="woven-name-434311-i8")
    table_id = "woven-name-434311-i8.cloudfunction_demo.cloudfunctiondemotable"

    # Define job configuration with schema
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema=[
            bigquery.SchemaField("emp_id", "INTEGER"),
            bigquery.SchemaField("emp_name", "STRING"),
            bigquery.SchemaField("emp_age", "INTEGER"),
            bigquery.SchemaField("emp_salary", "INTEGER"),
        ]
    )

    # Execute the load job
    load_job = bq_client.load_table_from_dataframe(df, table_id, job_config=job_config)
    load_job.result()  # Wait for the job to complete

    return "Data successfully inserted from Google Sheets to BigQuery"
