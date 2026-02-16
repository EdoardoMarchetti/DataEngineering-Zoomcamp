variable "project_id" {
  description = "The ID of the project"
  type = string
  default = "dtc-de-course-485215"
}

variable "project_region" {
  description = "The region of the project"
  type = string
  default = "europe-west1"
}

variable "project_credentials" {
  description = "The credentials of the project"
  type = string
  default = "../keys/service_credentials.json"
}

variable "project_location" {
  description = "The location of the project"
  type = string
  default = "europe-west1"
}

variable "bigquery_dataset_id"{
    description = "The name of the BigQuery dataset to create"
    type = string
    default = "nytaxi"
} 

variable "gcs_bucket_name" {
    description = "The name of the GCS bucket to create"
    type = string
    default = "dtc-de-course-485215-ny-taxi-data"
}

variable "gcs_storage_class" {
    description = "The storage class to use for the GCS bucket"
    type = string
    default = "STANDARD"
}


