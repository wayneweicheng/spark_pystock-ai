#!/bin/bash
set -e

# Change to the directory containing this script
cd "$(dirname "$0")"

# Step 1: Validate Terraform configuration files
echo "Validating Terraform configuration..."
terraform init
terraform validate

# Step 2: Apply Terraform configuration
echo "Applying Terraform configuration to create resources..."
terraform apply -auto-approve

# Step 3: Extract the Dataproc batch job ID
BATCH_ID=$(terraform output -raw batch_id)
PROJECT_ID=$(terraform output -raw project_id)
REGION=$(terraform output -raw region)

# Step 4: Check if batch already exists - if so, get its status
echo "Checking batch job: $BATCH_ID"
BATCH_STATUS=$(gcloud dataproc batches describe "$BATCH_ID" --region="$REGION" --project="$PROJECT_ID" --format="value(state)" 2>/dev/null || echo "NOT_FOUND")

if [[ "$BATCH_STATUS" != "NOT_FOUND" && "$BATCH_STATUS" != "CANCELLED" && "$BATCH_STATUS" != "FAILED" ]]; then
  echo "Batch job already exists with status: $BATCH_STATUS"
  if [[ "$BATCH_STATUS" == "SUCCEEDED" ]]; then
    echo "Job already completed successfully, skipping."
  else
    echo "Monitoring existing job..."
    gcloud dataproc batches wait "$BATCH_ID" --region="$REGION" --project="$PROJECT_ID"
  fi
else
  # If the batch doesn't exist or was cancelled/failed, destroy and recreate
  echo "Batch doesn't exist or was cancelled/failed. Recreating resources..."
  terraform destroy -auto-approve
  terraform apply -auto-approve
  
  # Get the new batch ID
  BATCH_ID=$(terraform output -raw batch_id)
  
  # Step 5: Monitor the job
  echo "Monitoring Dataproc batch job: $BATCH_ID"
  gcloud dataproc batches wait "$BATCH_ID" --region="$REGION" --project="$PROJECT_ID"
fi

# Step 6: Check job status
JOB_STATUS=$(gcloud dataproc batches describe "$BATCH_ID" --region="$REGION" --project="$PROJECT_ID" --format="value(state)")
echo "Job completed with status: $JOB_STATUS"

# Step 7: Print job output location
OUTPUT_PATH="gs://data-demo-bucket-wayne/transform-stock-tick-sale-vs-bidask/output/hourly_aggregation"
echo "Results are available at: $OUTPUT_PATH"

# Optional: Print the first few lines of the output
echo "Preview of results:"
gsutil ls "${OUTPUT_PATH}/*.csv" 2>/dev/null | head -n 1 | xargs -I {} gsutil cat {} | head -n 5 