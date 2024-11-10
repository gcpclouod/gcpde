from google.cloud import pubsub_v1
from datetime import datetime
import time
import json

# Replace these with your own project ID and topic name
project_id = 'your-project-id'
topic_id = 'your-topic-id'

# Create a Pub/Sub client
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

def publish_message(message):
    # Convert message to JSON and encode to bytes
    message_json = json.dumps(message).encode('utf-8')
    # Publish message to the topic
    future = publisher.publish(topic_path, message_json)
    print(f'Published message ID: {future.result()}')

if __name__ == '__main__':
    for i in range(10):  # Publish 10 messages
        message = {
            'event': f'message_{i}',
            'timestamp': datetime.utcnow().isoformat()
        }
        publish_message(message)
        time.sleep(1)  # Sleep for 1 second between messages
