import os
import json
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import base64
import functions_framework

# Set up the Gmail SMTP server configuration
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587
APP_PASSWORD = 'luir vbdz hyyz wmem'  # Replace with your actual app password

def send_email(sender_email, recipient_email, first_name, last_name):
    subject = "Congratulations on Joining Us!"
    
    # HTML body with background image
    body = f"""
    <html>
    <body style="background-image: url('https://storage.googleapis.com/objecstsforbckts/nature.jpg'); background-size: cover; background-repeat: no-repeat; background-position: center;">
        <div style="background-color: rgba(255, 255, 255, 0.8); padding: 20px; border-radius: 10px;">
            <p>Dear {first_name} {last_name},</p>
            <p>Thanks for joining with us. We are thrilled to have you!</p>
            <p>Best regards,<br>ABC Company</p>
        </div>
    </body>
    </html>
    """
    
    message = MIMEMultipart('alternative')
    message['From'] = sender_email
    message['To'] = recipient_email
    message['Subject'] = subject
    message.attach(MIMEText(body, 'html'))

    try:
        server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
        server.starttls()
        server.login(sender_email, APP_PASSWORD)
        server.send_message(message)
        print(f"Email sent to {recipient_email} successfully!")
    except Exception as e:
        print(f"Failed to send email: {e}")
    finally:
        server.quit()

@functions_framework.cloud_event
def pubsub_email_sender(cloud_event):
    print(f"Received Pub/Sub message: {cloud_event}")

    try:
        # Extract the base64-encoded message data
        pubsub_message = cloud_event.data['message']
        message_data = base64.b64decode(pubsub_message['data']).decode('utf-8')
        
        # Parse the JSON data
        data = json.loads(message_data)
        first_name = data.get('first_name')
        last_name = data.get('last_name')
        email = data.get('email')

        # Check if required fields are present
        if first_name and last_name and email:
            # Send email
            send_email("lavu2018hani@gmail.com", email, first_name, last_name)
        else:
            print("Missing required fields in message data.")
    except Exception as e:
        print(f"Error processing message: {e}")
