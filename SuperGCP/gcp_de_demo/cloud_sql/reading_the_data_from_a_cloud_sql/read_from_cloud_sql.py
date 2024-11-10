import mysql.connector
import time
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

def get_customer_emails():
    # Connect to your Cloud SQL database
    conn = mysql.connector.connect(
        host='34.66.73.145',
        user='root',
        password='Hani',
        database='customers'
    )
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM `customers`.`Customers`")
    emails = [row[3] for row in cursor.fetchall()]
    cursor.close()
    conn.close()
    return emails

# Path to your credentials file
SERVICE_ACCOUNT_FILE = r'D:\SuperGCPClasses\gcp_de_demo\cloud_sql\reading_the_data_from_a_cloud_sql\sa-account.json'

# Scopes required by the Google Drive API
SCOPES = ['https://www.googleapis.com/auth/drive']

# Authenticate and create the Drive API service
creds = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE, scopes=SCOPES)
service = build('drive', 'v3', credentials=creds)

def grant_access_with_retry(folder_id, email_list, max_retries=5):
    for email in email_list:
        attempt = 0
        while attempt < max_retries:
            try:
                permission = {
                    'type': 'user',
                    'role': 'reader',
                    'emailAddress': email
                }
                service.permissions().create(
                    fileId=folder_id,
                    body=permission,
                    fields='id'
                ).execute()
                print(f"Read access granted to {email}")
                break  # Exit the retry loop if successful
            except HttpError as e:
                if e.resp.status == 403:
                    print(f"Rate limit exceeded for {email}. Retrying...")
                    attempt += 1
                    time.sleep(2 ** attempt)  # Exponential backoff
                else:
                    print(f"Failed to grant access to {email}: {e}")
                    break  # Exit the retry loop on other errors
            except Exception as e:
                print(f"Failed to grant access to {email}: {e}")
                break  # Exit the retry loop on other errors

def batch_process_emails(emails, folder_id, batch_size=100):
    for i in range(0, len(emails), batch_size):
        batch = emails[i:i + batch_size]
        grant_access_with_retry(folder_id, batch)
        # Optional: Add a delay between batches
        time.sleep(60)  # Wait for 60 seconds between batches

if __name__ == '__main__':
    # ID of the folder in Google Drive
    FOLDER_ID = '1Iw6cY-Y41yi_gFTfodQtM3JA5JhlseD2'

    # Retrieve customer emails
    customer_emails = get_customer_emails()

    # Process emails in batches
    batch_process_emails(customer_emails, FOLDER_ID)
