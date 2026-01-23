terraform {
  required_providers {
    google = {
      source = "hashicorp/google"
      version = "5.12.0"
    }
  }
}


provider "google" {
    project = "dtc-de-course-485215"
    region = "europe-west1"
    credentials = file("keys/service_credentials.json")
}

resource "google_storage_bucket" "demo-bucket" {
  name          = "dtc-de-course-485215-demo-bucket"
  location      = "europe-west1"
  force_destroy = true

  lifecycle_rule {
    condition {
      age = 3
    }
    action {
      type = "Delete"
    }
  }

  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}