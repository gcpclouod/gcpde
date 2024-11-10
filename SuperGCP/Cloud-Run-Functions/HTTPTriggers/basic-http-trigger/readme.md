gcloud functions deploy localhttp-test-func1 `
    --runtime python311 `
    --trigger-http `
    --allow-unauthenticated `
    --timeout 300 `
    --entry-point square_number  