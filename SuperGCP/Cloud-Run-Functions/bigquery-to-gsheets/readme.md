gcloud functions deploy bqtogsheets --runtime python311 --trigger-http --allow-unauthenticated --entry-point bq_to_gsheets_cloud_func


gcloud functions deploy bqtogsheets-demo1 `
    --runtime python311 `
    --trigger-http `
    --allow-unauthenticated `
    --entry-point bq_to_gsheets_cloud_func `
	--memory 1GB
	
gcloud functions logs read bqtogsheets-demo1

  
projects/840233991363/secrets/bqtogsheets/versions/1


gcloud functions deploy gstobqfunc `
    --runtime python311 `
    --trigger-http `
    --allow-unauthenticated `
    --entry-point gsheets_to_bq_cloud_func `
	--memory 1GB
	
gcloud functions deploy FUNCTION_NAME \
    --trigger-http \
    --allow-unauthenticated=false


gcloud functions add-iam-policy-binding FUNCTION_NAME \
    --member="serviceAccount:YOUR_SERVICE_ACCOUNT@YOUR_PROJECT_ID.iam.gserviceaccount.com" \
    --role="roles/cloudfunctions.invoker"
