from google.cloud import storage
from google.cloud import bigquery


def create_bucket_class_location(bucket_name):
    """
    Create a new bucket in the US region with the coldline storage
    class
    """
    bucket_name = bucket_name

    storage_client = storage.Client(project='woven-name-434311-i8')

    bucket = storage_client.bucket(bucket_name)
    bucket.storage_class = "COLDLINE"
    new_bucket = storage_client.create_bucket(bucket, location="us")

    print(
        "Created bucket {} in {} with storage class {}".format(
            new_bucket.name, new_bucket.location, new_bucket.storage_class
        )
    )
    return new_bucket

result = create_bucket_class_location(bucket_name='cnn-project-prod-bucket') 
print(result)