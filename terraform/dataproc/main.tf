provider "google" {
  project = var.project_id
  region  = var.region
  zone    = var.zone
}

# Generate a random suffix for uniqueness
resource "random_id" "suffix" {
  byte_length = 4
}

# Create a Google Cloud Storage bucket for Dataproc temp storage and job files
resource "google_storage_bucket" "dataproc_bucket" {
  name          = "${var.project_id}-dataproc-${var.environment}"
  location      = var.region
  force_destroy = true

  uniform_bucket_level_access = true
}

# Create src/__init__.py for package imports
resource "google_storage_bucket_object" "src_init" {
  name    = "src/__init__.py"
  content = "# This file marks src as a Python package"
  bucket  = google_storage_bucket.dataproc_bucket.name
}

# Upload the PySpark job files to GCS
resource "google_storage_bucket_object" "transform_py" {
  name   = "src/transform.py"
  source = "${path.module}/../../src/transform.py"
  bucket = google_storage_bucket.dataproc_bucket.name
}

resource "google_storage_bucket_object" "read_data_py" {
  name   = "src/read_data.py"
  source = "${path.module}/../../src/read_data.py"
  bucket = google_storage_bucket.dataproc_bucket.name
}

resource "google_storage_bucket_object" "transform_data_py" {
  name   = "src/transform_data.py"
  source = "${path.module}/../../src/transform_data.py"
  bucket = google_storage_bucket.dataproc_bucket.name
}

# Create the Dataproc Serverless Spark Batch
resource "google_dataproc_batch" "spark_batch" {
  # Use a batch ID with a random suffix to ensure uniqueness
  batch_id = "pystock-ai-batch-${formatdate("YYYYMMDD", timestamp())}-${random_id.suffix.hex}" 
  location = var.region
  
  environment_config {
    execution_config {
      service_account = var.service_account
      network_uri     = var.subnetwork # Use network_uri instead of subnetwork as per GCP API
    }
  }

  pyspark_batch {
    main_python_file_uri = "gs://${google_storage_bucket.dataproc_bucket.name}/${google_storage_bucket_object.transform_py.name}"
    
    args = [
      "--input-path", "gs://data-demo-bucket-wayne/transform-stock-tick-sale-vs-bidask/TransformStockTickSaleVsBidAsk_AX_20240814_20241123222717.parquet",
      "--output-path", "gs://data-demo-bucket-wayne/transform-stock-tick-sale-vs-bidask/output/hourly_aggregation",
      "--verbose",
      "--cloud"
    ]

    python_file_uris = [
      "gs://${google_storage_bucket.dataproc_bucket.name}/${google_storage_bucket_object.src_init.name}",
      "gs://${google_storage_bucket.dataproc_bucket.name}/${google_storage_bucket_object.read_data_py.name}",
      "gs://${google_storage_bucket.dataproc_bucket.name}/${google_storage_bucket_object.transform_data_py.name}",
    ]

    jar_file_uris = []
  }

  # Set meaningful labels for the job
  labels = {
    "environment" = var.environment
    "application" = "pystock-ai"
    "managed-by"  = "terraform"
  }

  # Configure runtime and resources for the Dataproc batch
  runtime_config {
    version = "2.1"
    
    properties = {
      "spark.executor.instances" = "2"
      "spark.driver.cores"       = "4"
      "spark.executor.cores"     = "4"
      "spark.driver.memory"      = "16g"
      "spark.executor.memory"    = "16g"
    }
  }
} 