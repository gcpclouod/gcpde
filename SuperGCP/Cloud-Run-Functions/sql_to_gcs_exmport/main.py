import google.auth
from googleapiclient.discovery import build
import functions_framework

@functions_framework.http
def export_backup(request):
    # Configuration details
    instance_name = 'mysql'                 # Cloud SQL instance name
    project_id = 'woven-name-434311-i8'     # Project ID
    bucket_name = 'sqltogcsexport_bucket_cnn'  # Cloud Storage bucket name
    file_name = 'exported_backup.sql'       # Export file name in GCS

    # Get the credentials
    credentials, _ = google.auth.default()

    # Initialize the Cloud SQL Admin API client
    service = build('sqladmin', 'v1beta4', credentials=credentials)

    # Configure export context
    export_context = {
        "kind": "sql#exportContext",
        "uri": f"gs://{bucket_name}/{file_name}",
        "databases": ["web_logs"],         # Database to export; replace with your database name
        "fileType": "SQL"
    }

    # Create the request to start the export operation
    request = service.instances().export(
        project=project_id,
        instance=instance_name,
        body={"exportContext": export_context}
    )

    # Execute the request
    response = request.execute()

    # Return the operation name or status
    return f'Export operation started: {response["name"]}', 200
