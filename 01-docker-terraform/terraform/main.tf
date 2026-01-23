terraform {
  required_providers {
    google = {
      source = "hashicorp/google"
      version = "5.12.0"
    }
  }
}


provider "google" {
    project = var.project_id
    region = var.project_region
    credentials = file(var.project_credentials)
}

# Resource to create a Google Cloud Storage bucket named "dtc-de-course-485215-demo-bucket"
resource "google_storage_bucket" "demo-bucket" {
  name          = "dtc-de-course-485215-demo-bucket"
  location      = "europe-west1"
  force_destroy = true  # Ensures the bucket and all its contents are deleted when destroyed

  # Lifecycle rule to automatically delete objects older than 3 days
  lifecycle_rule {
    condition {
      age = 3
    }
    action {
      type = "Delete"
    }
  }

  # Lifecycle rule to abort incomplete multipart uploads older than 1 day
  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}


resource "google_bigquery_dataset" "demo-dataset" {
  dataset_id    = var.bigquery_dataset_id
  location      = var.project_location
}