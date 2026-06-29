# Terraform: GCP infrastructure for Kafka CDC Pipeline

terraform {
  required_providers {
    google = { source = "hashicorp/google"; version = "~> 5.0" }
  }
  backend "gcs" {
    bucket = "tfstate-cdc"
    prefix = "kafka-cdc"
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

variable "project_id" { type = string }
variable "region"     { type = string; default = "us-central1" }

# BigQuery CDC dataset
resource "google_bigquery_dataset" "cdc_raw" {
  dataset_id  = "cdc_raw"
  location    = var.region
  description = "Raw CDC events from Debezium/Kafka"
}

# BigQuery tables for CDC events
resource "google_bigquery_table" "streaming_metrics" {
  dataset_id = google_bigquery_dataset.cdc_raw.dataset_id
  table_id   = "streaming_metrics"
  schema = jsonencode([
    { name = "window_start",    type = "TIMESTAMP" },
    { name = "window_end",      type = "TIMESTAMP" },
    { name = "window_secs",     type = "INTEGER"   },
    { name = "total_revenue",   type = "FLOAT"     },
    { name = "order_count",     type = "INTEGER"   },
    { name = "top_products",    type = "STRING"    },
    { name = "channel_revenue", type = "STRING"    },
  ])
  time_partitioning { type = "DAY"; field = "window_start" }
}

# Cloud Run: CDC consumer service
resource "google_cloud_run_v2_service" "cdc_consumer" {
  name     = "cdc-consumer"
  location = var.region

  template {
    containers {
      image = "gcr.io/${var.project_id}/cdc-consumer:latest"
      resources { limits = { cpu = "1", memory = "512Mi" } }
      env { name = "GCP_PROJECT_ID";      value = var.project_id }
      env { name = "BQ_CDC_DATASET";      value = "cdc_raw" }
      env { name = "KAFKA_BOOTSTRAP_SERVERS"; value_source { secret_key_ref { secret = "kafka-bootstrap-servers"; version = "latest" } } }
    }
    scaling { min_instance_count = 1; max_instance_count = 3 }
  }
}

# Service account
resource "google_service_account" "cdc_sa" {
  account_id   = "cdc-pipeline-sa"
  display_name = "CDC Pipeline Service Account"
}
resource "google_project_iam_member" "cdc_bq" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.cdc_sa.email}"
}

output "cdc_consumer_url" { value = google_cloud_run_v2_service.cdc_consumer.uri }