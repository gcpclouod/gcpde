python main.py `
    --runner DataflowRunner `
    --project first-project-428309 `
    --region us-central1 `
    --staging_location gs://classic-template-demo/staging `
    --temp_location gs://classic-template-demo/temp `
    --template_location gs://classic-template-demo/my_template `
    --input gs://classic-template-demo/food_orders_daily.csv  



python main.py --runner DataflowRunner --project first-project-428309 --region aisa-south1 --staging_location gs://objecstsforbckt/staging --temp_location gs://objecstsforbckt/temp --template_location gs://objecstsforbckt/templates/my_template --input gs://objecstsforbckt/Customers.csv --output gs://objecstsforbckt/path/to/output

gcloud dataflow jobs run classic-template-demo-job `
    --gcs-location gs://classic-template-demo/my_template `
    --region us-central1 `
    --parameters input=gs://classic-template-demo/food_orders_daily.csv

gcloud dataflow jobs run my-pipeline-job \
    --gcs-location=gs://my-bucket/templates/my-template \
    --region=us-central1 \
    --parameters \
        javascriptTextTransformGcsPath=gs://my-bucket/udf/transform.js,\
        JSONPath=gs://my-bucket/schemas/my-schema.json,\
        inputFilePattern=gs://my-bucket/data/*.csv,\
        outputTable=my-project:dataset.table_name

gcloud dataflow jobs run pre-built-template-test \
    --gcs-location gs://dataflow-templates-us-central1/latest/GCS_Text_to_BigQuery \
    --region us-central1 \
    --staging-location gs://dataflow_flex_template_demo_bucket_12345/temp/ \
    --parameters inputFilePattern=gs://pre-built-template-dataflow-demo/empdata.csv,\
JSONPath=gs://pre-built-template-dataflow-demo/emp_table_schema.json,\
outputTable=dev-project-433015:food_orders.table_emp,\
bigQueryLoadingTemporaryDirectory=gs://dataflow_flex_template_demo_bucket_12345/temp/,\
javascriptTextTransformGcsPath=gs://pre-built-template-dataflow-demo/java_udf_for_emp_table.js,\
javascriptTextTransformFunctionName=transform


To create and run a Dataflow job with all possible parameter options using the gcloud command:
----------------------------------------------------------------------------------------------
gcloud dataflow jobs run gcstobq-job \
    --gcs-location gs://dataflow-templates-us-central1/latest/GCS_Text_to_BigQuery \
    --region us-central1 \
    --staging-location gs://dataflow_flex_template_demo_bucket_12345/temp/ \ 
    --parameters \
        inputFilePattern=gs://dataflow_flex_template_demo_bucket_12345/sample_data.csv,\
        JSONPath=gs://dataflow_flex_template_demo_bucket_12345/jsonschema.json,\
        outputTable=dev-project-433015:food_orders.fromlocal2,\
        bigQueryLoadingTemporaryDirectory=gs://dataflow_flex_template_demo_bucket_12345/temp/,\
        javascriptTextTransformGcsPath=gs://dataflow_flex_template_demo_bucket_12345/function_one.js,\
        javascriptTextTransformFunctionName=transform \
    --max-workers 10 \
    --num-workers 5 \
    --worker-region us-central1 \
    --worker-zone us-central1-a \
    --machine-type n1-standard-2 \
    --service-account-email your-service-account@your-project.iam.gserviceaccount.com \ 
    --worker-ip-address-configuration INTERNAL \
    --network default \
    --subnetwork projects/your-project/global/networks/default \
    --enable-streaming-engine \
    --encryption-key=projects/your-project/locations/global/keyRings/your-keyring/cryptoKeys/your-key \
    --dataflow-prime

Explanation:
--gcs-location: Specifies the location of the Dataflow template.
--region: The region for the Dataflow job.
--staging-location: The Cloud Storage path for staging temporary files.
--additional-user-labels: Any additional labels you want to apply.
--parameters: Specifies the parameters required by the Dataflow job:
inputFilePattern: Cloud Storage path for the input file(s).
JSONPath: Cloud Storage path for the JSON schema file.
outputTable: BigQuery table to output data.
bigQueryLoadingTemporaryDirectory: Temporary directory for BigQuery loading.
javascriptTextTransformGcsPath: Cloud Storage path for the JavaScript UDF.
javascriptTextTransformFunctionName: Name of the UDF function.
--max-workers: Maximum number of workers for the job.
--num-workers: Initial number of workers.
--worker-region: Region where workers will be located.
--worker-zone: Zone where workers will be located.
--machine-type: Machine type for the workers.
--service-account-email: Service account email to run the job.
--additional-experiments: Any additional experiment flags for the job.
--worker-ip-address-configuration: IP address configuration for workers (INTERNAL or PUBLIC).
--network: Network to which workers will be assigned.
--subnetwork: Subnetwork for workers.
--enable-streaming-engine: Enables the Streaming Engine (only applicable for streaming pipelines).
--encryption-key: Cloud KMS key for encryption.
--dataflow-prime: Enables Dataflow Prime for improved resource utilization.