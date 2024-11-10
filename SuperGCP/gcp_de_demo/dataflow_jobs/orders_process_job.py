import apache_beam as beam 
from apache_beam.options.pipeline_options import PipelineOptions
import json
import datetime

table_schema = {
    'fields': [
        {'name': 'cust_id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'firstname', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'lastname', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'dob', 'type': 'DATE', 'mode': 'NULLABLE'},  # Assuming the date format is consistent
        {'name': 'phone', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'email', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'address', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'gender', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'products', 'type': 'STRING', 'mode': 'REPEATED'},  # Assuming products is a list of strings
        {'name': 'total_price', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'payment_method', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'card_number', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'expiry_date', 'type': 'STRING', 'mode': 'NULLABLE'},  # Assuming the date format is consistent
        {'name': 'cvv', 'type': 'STRING', 'mode': 'NULLABLE'}
    ]
}

# Define your pipeline options
options = PipelineOptions(streaming=True)

class ProcessJSON(beam.DoFn):
    def process(self, element):
        # Convert byte message to string
        message_str = element.decode('utf-8')
        # Parse the JSON string into Python dictionary
        message_dict = json.loads(message_str)
        
        # Convert 'dob' field to the expected format 'YYYY-MM-DD'
        # Assuming 'dob' is in the format 'Tue, 20 Apr 1993 00:00:00 GMT'
        dob = message_dict.get('dob', '')
        if dob:
            try:
                parsed_dob = datetime.datetime.strptime(dob, '%a, %d %b %Y %H:%M:%S %Z')
                message_dict['dob'] = parsed_dob.strftime('%Y-%m-%d')
            except ValueError:
                # Handle invalid date format
                # You might want to log this error or set a default date
                pass
        
        # Convert 'expiry_date' field to the expected format 'YYYY-MM-DD'
        # Assuming 'expiry_date' is in the format 'MM/YYYY'
        expiry_date = message_dict.get('expiry_date', '')
        if expiry_date:
            try:
                parsed_expiry_date = datetime.datetime.strptime(expiry_date, '%m/%Y')
                message_dict['expiry_date'] = parsed_expiry_date.strftime('%Y-%m-%d')
            except ValueError:
                # Handle invalid date format
                # You might want to log this error or set a default expiry date
                pass
        
        # No need to modify 'products' field, it's already in the desired format
        yield message_dict

with beam.Pipeline(options=options) as pipeline:
    # Read messages from Pub/Sub subscription
    messages = (
        pipeline
        | 'Read Pub/Sub messages' >> beam.io.ReadFromPubSub(subscription='projects/first-project-428309/subscriptions/orders-topic-sub')
        | 'Process JSON' >> beam.ParDo(ProcessJSON())
        | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
            table='first-project-428309.rdm_order_stage.orders_table_new',            
            schema=table_schema,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
        )
    )
