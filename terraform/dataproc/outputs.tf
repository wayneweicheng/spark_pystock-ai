output "batch_id" {
  description = "The ID of the Dataproc batch job"
  value       = google_dataproc_batch.spark_batch.batch_id
}

output "project_id" {
  description = "The GCP project ID"
  value       = var.project_id
}

output "region" {
  description = "The GCP region"
  value       = var.region
}

output "dataproc_bucket" {
  description = "The GCS bucket used for Dataproc job files"
  value       = google_storage_bucket.dataproc_bucket.name
}

output "job_file_uri" {
  description = "The GCS URI of the main PySpark job file"
  value       = "gs://${google_storage_bucket.dataproc_bucket.name}/${google_storage_bucket_object.transform_py.name}"
}

output "output_path" {
  description = "The GCS URI where job results will be written"
  value       = "gs://data-demo-bucket-wayne/transform-stock-tick-sale-vs-bidask/output/hourly_aggregation"
} 