import apache_beam as beam
from apache_beam.io import WriteToBigQuery
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.io import ReadFromPubSub
import json

# Define the BigQuery table reference (as a string)
table_spec = 'woven-name-434311-i8:cnn_project.Orders_realtime_pipeline'

# Define the BigQuery table schema
table_schema = {
    'fields': [
        {
            "name": "customer_id",
            "type": "INTEGER",
            "mode": "REQUIRED"
        },
        {
            "name": "user_email",
            "type": "STRING",
            "mode": "REQUIRED"
        },
        {
            "name": "items",
            "type": "RECORD",
            "mode": "REPEATED",
            "fields": [
                {
                    "name": "id",
                    "type": "INTEGER",
                    "mode": "REQUIRED"
                },
                {
                    "name": "product_id",
                    "type": "INTEGER",
                    "mode": "REQUIRED"
                },
                {
                    "name": "name",
                    "type": "STRING",
                    "mode": "REQUIRED"
                },
                {
                    "name": "price",
                    "type": "FLOAT",
                    "mode": "REQUIRED"
                },
                {
                    "name": "image_url",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "quantity",
                    "type": "INTEGER",
                    "mode": "REQUIRED"
                }
            ]
        }
    ]
}


class FormatForBigQueryFn(beam.DoFn):
    def process(self, element):
        # Decode the element from Pub/Sub
        message = json.loads(element.decode('utf-8'))
        
        # Ensure the format matches BigQuery schema exactly
        formatted_message = {
            'customer_id': int(message['customer_id']),
            'user_email': message['user_email'],
            'items': [
                {
                    'id': int(item.get('id', 0)),  # Assuming `id` should be included, with default 0 if missing
                    'product_id': int(item.get('product_id', 0)),
                    'name': item.get('name', 'Unknown'),  # Default to 'Unknown' if missing
                    'price': float(item.get('price', 0.0)),
                    'image_url': item.get('image_url', None),  # Default to None if missing
                    'quantity': int(item.get('quantity', 0))
                } for item in message.get('items', [])
            ]
        }
        yield formatted_message


def run():
    # Set up your pipeline options
    options = PipelineOptions()
    options.view_as(StandardOptions).runner = 'DataflowRunner'  # Change to 'DirectRunner' for local testing

    # Replace with your subscription ID
    subscription = 'projects/woven-name-434311-i8/subscriptions/e-commerce-topic-sub'

    with beam.Pipeline(options=options) as p:
        (p
         | 'Read from Pub/Sub' >> ReadFromPubSub(subscription=subscription) 
         | 'Format for BigQuery' >> beam.ParDo(FormatForBigQueryFn())
         | 'Write to BigQuery' >> WriteToBigQuery(
                table_spec,
                schema=table_schema,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
            )
        )

if __name__ == '__main__':
    run()
