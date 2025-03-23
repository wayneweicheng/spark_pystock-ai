#!/bin/bash
set -e

# Configuration variables - modify these as needed
PROJECT_ID="ark-of-data-2000" # Replace with your project ID
REGION="australia-southeast1"
BUCKET_NAME="${PROJECT_ID}-dataproc-files"
TIMESTAMP=$(date +%Y%m%d%H%M%S)
RANDOM_SUFFIX=$(cat /dev/urandom | LC_ALL=C tr -dc 'a-z0-9' | fold -w 8 | head -n 1)
JOB_NAME="pystock-ai-job-${TIMESTAMP}-${RANDOM_SUFFIX}"
INPUT_PATH="gs://data-demo-bucket-wayne/transform-stock-tick-sale-vs-bidask/TransformStockTickSaleVsBidAsk_AX_20240814_20241123222717.parquet"
OUTPUT_PATH="gs://data-demo-bucket-wayne/transform-stock-tick-sale-vs-bidask/output/hourly_aggregation"

# Change to the directory containing this script
cd "$(dirname "$0")"

echo "=== Setting up Google Cloud Dataproc Serverless Job ==="
echo "Project ID: $PROJECT_ID"
echo "Region: $REGION"
echo "Job Name: $JOB_NAME"

# Step 1: Create the storage bucket if it doesn't exist
echo "Creating GCS bucket for job files..."
if ! gsutil ls -b "gs://${BUCKET_NAME}" &>/dev/null; then
  gsutil mb -l ${REGION} "gs://${BUCKET_NAME}"
  echo "Bucket created: gs://${BUCKET_NAME}"
else
  echo "Bucket already exists: gs://${BUCKET_NAME}"
fi

# Step 2: Upload the Python files to GCS
echo "Uploading Python files to GCS..."
gsutil cp ../../src/transform.py "gs://${BUCKET_NAME}/src/transform.py"
gsutil cp ../../src/read_data.py "gs://${BUCKET_NAME}/src/read_data.py"
gsutil cp ../../src/transform_data.py "gs://${BUCKET_NAME}/src/transform_data.py"

# Create the src/__init__.py file
echo "# This file marks src as a Python package" | gsutil cp - "gs://${BUCKET_NAME}/src/__init__.py"

# Step 3: Submit the Dataproc serverless batch job
echo "Submitting Dataproc serverless batch job..."
BATCH_ID=$(gcloud dataproc batches submit pyspark \
  "gs://${BUCKET_NAME}/src/transform.py" \
  --project="${PROJECT_ID}" \
  --region="${REGION}" \
  --batch="${JOB_NAME}" \
  --py-files="gs://${BUCKET_NAME}/src/__init__.py,gs://${BUCKET_NAME}/src/read_data.py,gs://${BUCKET_NAME}/src/transform_data.py" \
  --properties="spark.executor.instances=2,spark.driver.cores=4,spark.executor.cores=4,spark.driver.memory=16g,spark.executor.memory=16g" \
  -- --input-path="${INPUT_PATH}" --output-path="${OUTPUT_PATH}" --verbose --cloud \
  --format=json | jq -r '.id')

echo "Job submitted successfully with ID: $BATCH_ID"

# Step 4: Monitor the job
echo "Monitoring job progress..."
gcloud dataproc batches wait "$BATCH_ID" --region="$REGION" --project="$PROJECT_ID"

# Step 5: Check job status
JOB_STATUS=$(gcloud dataproc batches describe "$BATCH_ID" --region="$REGION" --project="$PROJECT_ID" --format="value(state)")
echo "Job completed with status: $JOB_STATUS"

# Step 6: Print job output location
echo "Results are available at: $OUTPUT_PATH"

# Optional: Print the first few lines of the output
echo "Preview of results:"
gsutil ls "${OUTPUT_PATH}/*.csv" 2>/dev/null | head -n 1 | xargs -I {} gsutil cat {} | head -n 5

echo "=== Job Deployment Complete ===" 