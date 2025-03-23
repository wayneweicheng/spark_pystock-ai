# Google Cloud Dataproc Serverless Batch Job Deployment

This directory contains Terraform configuration to deploy a serverless Google Cloud Dataproc batch job for processing stock data. The job reads data from a GCS bucket, aggregates it by hour, and writes the results back to GCS.

## Prerequisites

1. Google Cloud SDK installed and configured
2. Terraform installed (v1.0.0+)
3. GCP project with necessary APIs enabled:
   - Dataproc API
   - Cloud Storage API
   - Compute Engine API
   - IAM API
4. Authentication with sufficient permissions:
   - Service account credentials or user credentials with required permissions

## Setup

1. Edit `terraform.tfvars` to set your project ID and other configuration options:

```hcl
project_id   = "your-gcp-project-id"
region       = "australia-southeast1"
environment  = "dev"
# service_account = "your-service-account@your-project.iam.gserviceaccount.com"
```

2. Make the deployment scripts executable:

```bash
chmod +x deploy.sh deploy_without_terraform.sh
```

## Deployment Options

### Using Terraform (Recommended)

Run the Terraform deployment script to create all resources and submit the job:

```bash
./deploy.sh
```

This will:

1. Initialize and validate the Terraform configuration
2. Create all required GCP resources
3. Upload the Python files to GCS
4. Submit the Dataproc serverless batch job
5. Wait for job completion and report the status
6. Display a preview of the results

### Without Terraform

For a simpler deployment without using Terraform:

```bash
./deploy_without_terraform.sh
```

You'll need to update the project ID in the script before running it.

## Manual Deployment Steps

If you prefer to run the steps manually:

```bash
# Initialize Terraform
terraform init

# Apply the configuration
terraform apply

# Get the batch ID and project ID
BATCH_ID=$(terraform output -raw batch_id)
PROJECT_ID=$(terraform output -raw project_id)
REGION=$(terraform output -raw region)

# Monitor the job
gcloud dataproc batches wait "$BATCH_ID" --region="$REGION" --project="$PROJECT_ID"
```

## Resources Created

- Google Cloud Storage bucket for job files
- Dataproc serverless batch job

## Cleaning Up

To delete all resources created by Terraform:

```bash
terraform destroy
```

## Input Data

The job expects input data in the following GCS location:
- `gs://data-demo-bucket-wayne/transform-stock-tick-sale-vs-bidask/TransformStockTickSaleVsBidAsk_AX_20240814_20241123222717.parquet`

## Output Data

The job writes results to:
- `gs://data-demo-bucket-wayne/transform-stock-tick-sale-vs-bidask/output/hourly_aggregation/*.csv`

## Code Structure

This deployment uses the same Python files as the local development environment:

- `src/transform.py` - Main entry point with command-line argument handling
- `src/read_data.py` - Functions for creating Spark sessions and reading data
- `src/transform_data.py` - Functions for transforming data and writing results

The code is designed to run both locally and in the cloud by using the `--cloud` flag when running in Dataproc. 