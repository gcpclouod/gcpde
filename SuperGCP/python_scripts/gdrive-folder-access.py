import time
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Path to the service account key file
SERVICE_ACCOUNT_FILE = r'D:\\SuperGCPClasses\\gcp_de_demo\\cloud_sql\\gsheets_to_cloud_Sql\\gsheet_to_mysql_sa.json'

# Define the scope for Google Drive API
SCOPES = ['https://www.googleapis.com/auth/drive']

# Authenticate using the service account
creds = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE, scopes=SCOPES)

# Create a Google Drive API service instance
service = build('drive', 'v3', credentials=creds)

# Folder ID where access should be granted
folder_id = '1vtf5Bba0WFf1VI_-28dzhUQ6ThTd8Kp1'

# List of email addresses to give access
emails = [
    "aanya.kailash1@gmail.com",
    "balajis2790@gmail.com",
    "chandanamellimi9@gmail.com",
    "vasunaidu330@gmail.com",
    "vinaydunaboyina313@gmail.com",
    "shabbusk000@gmail.com",
    "sankargcp1122@gmail.com",
    "mani12varma@gmail.com",
    "aravindgajula1999@gmail.com",
    "satishbalina3@gmail.com",
    "manojnamadasu@gmail.com",
    "vivekthota6565@gmail.com",
    "mounikaanionmarketing@gmail.com",
    "ushasri1275@gmail.com",
    "naveensadam99@gmail.com",
    "padmalathanalamati@gmail.com",
    "navyapeteti06@gmail.com",
    "geethaanjali3990@gmail.com",
    "maruthi1213@gmail.com"
]

# Iterate over the email list and assign read access with delay
for email in emails:
    permission = {
        'type': 'user',
        'role': 'reader',
        'emailAddress': email
    }
    try:
        service.permissions().create(
            fileId=folder_id,
            body=permission,
            fields='id'
        ).execute()
        print(f"Read access granted to: {email}")
        time.sleep(3)  # Adding a delay of 2 seconds between requests
    except HttpError as e:
        if e.resp.status == 403 and 'sharingRateLimitExceeded' in str(e):
            print(f"Rate limit exceeded for {email}, retrying after 30 seconds...")
            time.sleep(30)
            try:
                service.permissions().create(
                    fileId=folder_id,
                    body=permission,
                    fields='id'
                ).execute()
                print(f"Retry successful for {email}")
            except HttpError as retry_error:
                print(f"Failed again for {email}: {retry_error}")
        else:
            print(f"An error occurred for {email}: {e}")
