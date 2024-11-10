import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
# Custom function to send an email using Gmail with app password
def send_email(subject, body, to_email):
    gmail_user = 'gcpcloud305@gmail.com'
    gmail_password = 'nqvl cfhd uboc rpaq' # app password

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
    send_email(subject, body, 'lavu2016hani@gmail.com') # please give your gmail address

file_not_found_email()