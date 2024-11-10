import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build 
import mysql.connector
from datetime import datetime

# Service account credentials file
SERVICE_ACCOUNT_FILE = r'D:\SuperGCPClasses\gcp_de_demo\cloud_sql\gsheets_to_cloud_Sql\gsheet_to_mysql_sa.json'

# Google Sheets API scope
SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']

# Spreadsheet ID and range
SPREADSHEET_ID = '1OSMuXbC5sjIaHePZmi8ZWvY6fYEnpI_TN0dmVMb5R4w'
RANGE_NAME = 'Sheet1'  # Adjust based on your sheet structure

# Authenticate and access the Sheets API
credentials = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE, scopes=SCOPES)
service = build('sheets', 'v4', credentials=credentials)

# Read data from the Google Sheet
sheet = service.spreadsheets()
result = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range=RANGE_NAME).execute()
rows = result.get('values', [])

# Convert the list of rows to a pandas DataFrame
if rows:
    # Assuming the first row is the header
    df = pd.DataFrame(rows[1:], columns=rows[0])
else:
    df = pd.DataFrame()
# Drop the first and last columns using drop
df = df.drop(df.columns[[0, -1]], axis=1)
# Print the DataFrame
print(df)
# Convert necessary columns to appropriate data types
df['Price'] = df['Price'].astype(float)
df['Stock'] = df['Stock'].astype(int)

# Cloud SQL connection details (Connect via Cloud SQL Proxy)
connection_config = {
    'user': 'root',  # You can still use the root user or any MySQL user
    'password': 'hanvikass',  # The password for the MySQL user
    'host': '34.42.216.184',  # The Cloud SQL Proxy will listen on localhost
    'database': 'e-commerce',
    'port': '3306'
}

# Establish connection to the Cloud SQL instance
conn = mysql.connector.connect(**connection_config)
cursor = conn.cursor()

# SQL Insert Query
insert_query = """
    INSERT INTO products (name, description, price, image_url, category, stock, created_at, updated_at)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
"""

# Prepare the data for insertion
for index, row in df.iterrows():
    data = (
        row['Name'],  # name
        row['Description'],  # description
        row['Price'],  # price
        row['Image_url'],  # image_url
        row['Category'],  # category
        row['Stock'],  # stock
        datetime.now(),  # created_at
        datetime.now()  # updated_at
    )
    
    cursor.execute(insert_query, data)

# Commit the transaction
conn.commit()

# Close the cursor and connection
cursor.close()
conn.close()

print("Data inserted successfully into Cloud SQL!")
