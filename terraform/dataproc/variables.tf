variable "project_id" {
  description = "The GCP project ID"
  type        = string
}

variable "region" {
  description = "The GCP region for resources"
  type        = string
  default     = "us-central1"
}

variable "zone" {
  description = "The GCP zone for resources"
  type        = string
  default     = "us-central1-a"
}

variable "environment" {
  description = "The deployment environment (dev, test, prod)"
  type        = string
  default     = "dev"
}

variable "service_account" {
  description = "The service account email for Dataproc"
  type        = string
  default     = null
}

variable "subnetwork" {
  description = "The fully qualified subnetwork self-link"
  type        = string
  default     = null
} 