import time
import json
import random
from google.cloud import pubsub_v1

# Replace with your Pub/Sub project ID and topic name
project_id = "dev-project-433015"
topic_id = "streamingtopic"

# Initialize a Pub/Sub publisher client
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

def publish_message():
    # Define the initial id
    message_id = 125

    while True:
        # Increment the id for each message
        message_id += 1

        # Create the message payload
        message = {
            "id": message_id,
            "name": "Surya",
            "age": 33,
            "joining_date": "2024-09-03",
            "joining_time": "15:30:00",
            "salary": 75000.50,
            "is_active": False
        }

        # Convert the message to a JSON string
        message_json = json.dumps(message)
        message_bytes = message_json.encode("utf-8")

        # Publish the message to the Pub/Sub topic
        future = publisher.publish(topic_path, data=message_bytes)
        print(f"Published message ID: {message_id}")

        # Wait for 30 seconds before publishing the next message
        time.sleep(30)

if __name__ == "__main__":
    publish_message()
