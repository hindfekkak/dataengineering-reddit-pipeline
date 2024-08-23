# bucket

resource "google_bigquery_dataset" "bigquery_dataset" {
  dataset_id   = "bigquery_data_warehouse"
  location     = var.region
  description  = "Dataset for storing bucket data"
  labels = {
    env = "production"
  }
  access {
    role          = "OWNER"
    user_by_email = "fekkakhind25@gmail.com"
  }
}

resource "google_compute_firewall" "allow_all" {
  name    = "allow-all"
  network = "default"  # or specify your network

  allow {
    protocol = "all"
  }

  source_ranges = ["0.0.0.0/0"]  # Allows all inbound traffic
  destination_ranges = ["0.0.0.0/0"]  # Allows all outbound traffic
}

resource "google_service_account" "bigquery_service_account" {
  account_id   = "bigquery-access"
  display_name = "BigQuery Access Service Account"
}

resource "google_project_iam_member" "bigquery_storage_access" {
  project = var.project_id
  role    = "roles/bigquery.user"  # Assigning the BigQuery role
  member  = "serviceAccount:${google_service_account.bigquery_service_account.email}"
}

resource "google_project_iam_member" "storage_access" {
  project = var.project_id
  role    = "roles/storage.objectViewer"  # Read access to Cloud Storage
  member  = "serviceAccount:${google_service_account.bigquery_service_account.email}"
}


resource "google_storage_bucket" "bigquery_bucket" {
  name          = var.bucket_name
  location      = var.region
  force_destroy = true
  # Setting access control to private by default
  uniform_bucket_level_access = true
}

