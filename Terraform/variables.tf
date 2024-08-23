variable "gcp_svc_key" {
  type        = string
  default = "C:/Users/hp/Documents/projects/dataengineering-reddit-pipeline/Terraform/winged-client-433119-a1-9ceac2a5544e.json"
}
variable "project_id" {
  description = "The GCP project ID"
  type        = string
  default = "winged-client-433119-a1"
}

variable "region" {
  description = "The GCP region to deploy the infrastructure"
  default     = "us-east1"
}

variable "bucket_name" {
  description = "The name of the GCS bucket"
  type        = string
  default = "csvfile_bucket1"
}
