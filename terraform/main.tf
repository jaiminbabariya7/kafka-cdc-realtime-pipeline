# Terraform: GCP infrastructure for the Kafka CDC pipeline
#   - BigQuery dataset + change-log tables written by consumers/cdc_consumer.py
#   - streaming_metrics table written by consumers/stream_processor.py
#   - Cloud Run service for the CDC consumer (always-on CPU, single instance)
#   - Service account with BigQuery write and Secret Manager read access

terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
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

variable "project_id" {
  type = string
}

variable "region" {
  type    = string
  default = "us-central1"
}

variable "consumer_image" {
  type        = string
  description = "Container image for consumers/cdc_consumer.py"
  default     = ""
}

locals {
  image = var.consumer_image != "" ? var.consumer_image : "gcr.io/${var.project_id}/cdc-consumer:latest"

  # Audit columns added to every change-log row by cdc_consumer.to_bq_row()
  cdc_audit = [
    { name = "_cdc_op", type = "STRING", mode = "REQUIRED" },
    { name = "_cdc_ts_ms", type = "INTEGER", mode = "NULLABLE" },
    { name = "_cdc_processed_at", type = "TIMESTAMP", mode = "REQUIRED" },
  ]

  # Column types mirror docker/init.sql (NUMERIC arrives as double, see connector config)
  cdc_tables = {
    customers = [
      { name = "customer_id", type = "STRING" },
      { name = "first_name", type = "STRING" },
      { name = "last_name", type = "STRING" },
      { name = "email", type = "STRING" },
      { name = "country", type = "STRING" },
      { name = "city", type = "STRING" },
      { name = "registration_date", type = "DATE" },
      { name = "is_active", type = "BOOLEAN" },
      { name = "updated_at", type = "TIMESTAMP" },
    ]
    products = [
      { name = "product_id", type = "STRING" },
      { name = "product_name", type = "STRING" },
      { name = "category", type = "STRING" },
      { name = "unit_price", type = "FLOAT" },
      { name = "unit_cost", type = "FLOAT" },
      { name = "stock_qty", type = "INTEGER" },
      { name = "updated_at", type = "TIMESTAMP" },
    ]
    orders = [
      { name = "order_id", type = "STRING" },
      { name = "customer_id", type = "STRING" },
      { name = "order_date", type = "DATE" },
      { name = "status", type = "STRING" },
      { name = "channel", type = "STRING" },
      { name = "net_revenue", type = "FLOAT" },
      { name = "created_at", type = "TIMESTAMP" },
      { name = "updated_at", type = "TIMESTAMP" },
    ]
    order_items = [
      { name = "item_id", type = "STRING" },
      { name = "order_id", type = "STRING" },
      { name = "product_id", type = "STRING" },
      { name = "quantity", type = "INTEGER" },
      { name = "unit_price", type = "FLOAT" },
      { name = "discount_pct", type = "FLOAT" },
      { name = "created_at", type = "TIMESTAMP" },
    ]
  }
}

# ── BigQuery ──────────────────────────────────────────────────────────────────
resource "google_bigquery_dataset" "cdc_raw" {
  dataset_id  = "cdc_raw"
  location    = var.region
  description = "Raw CDC change log from Debezium/Kafka"
}

resource "google_bigquery_table" "cdc" {
  for_each   = local.cdc_tables
  dataset_id = google_bigquery_dataset.cdc_raw.dataset_id
  table_id   = each.key
  schema     = jsonencode(concat(each.value, local.cdc_audit))

  time_partitioning {
    type  = "DAY"
    field = "_cdc_processed_at"
  }
  deletion_protection = false
}

resource "google_bigquery_table" "streaming_metrics" {
  dataset_id = google_bigquery_dataset.cdc_raw.dataset_id
  table_id   = "streaming_metrics"
  schema = jsonencode([
    { name = "window_start", type = "TIMESTAMP" },
    { name = "window_end", type = "TIMESTAMP" },
    { name = "window_secs", type = "INTEGER" },
    { name = "total_revenue", type = "FLOAT" },
    { name = "order_count", type = "INTEGER" },
    { name = "top_products", type = "STRING" },
    { name = "channel_revenue", type = "STRING" },
  ])

  time_partitioning {
    type  = "DAY"
    field = "window_start"
  }
  deletion_protection = false
}

# ── Service account ───────────────────────────────────────────────────────────
resource "google_service_account" "cdc_sa" {
  account_id   = "cdc-pipeline-sa"
  display_name = "CDC Pipeline Service Account"
}

resource "google_project_iam_member" "cdc_bq" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.cdc_sa.email}"
}

# Kafka bootstrap servers are stored in Secret Manager (add a version manually)
resource "google_secret_manager_secret" "kafka_bootstrap" {
  secret_id = "kafka-bootstrap-servers"
  replication {
    auto {}
  }
}

resource "google_secret_manager_secret_iam_member" "cdc_secret_access" {
  secret_id = google_secret_manager_secret.kafka_bootstrap.id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.cdc_sa.email}"
}

# ── Cloud Run: CDC consumer ───────────────────────────────────────────────────
# The consumer polls Kafka continuously, so CPU stays allocated (cpu_idle = false)
# and one instance always runs; it answers health checks on $PORT.
resource "google_cloud_run_v2_service" "cdc_consumer" {
  name     = "cdc-consumer"
  location = var.region
  ingress  = "INGRESS_TRAFFIC_INTERNAL_ONLY"

  template {
    service_account = google_service_account.cdc_sa.email

    scaling {
      min_instance_count = 1
      max_instance_count = 1
    }

    containers {
      image = local.image

      resources {
        limits = {
          cpu    = "1"
          memory = "512Mi"
        }
        cpu_idle = false
      }

      env {
        name  = "GCP_PROJECT_ID"
        value = var.project_id
      }
      env {
        name  = "BQ_CDC_DATASET"
        value = google_bigquery_dataset.cdc_raw.dataset_id
      }
      env {
        name = "KAFKA_BOOTSTRAP_SERVERS"
        value_source {
          secret_key_ref {
            secret  = google_secret_manager_secret.kafka_bootstrap.secret_id
            version = "latest"
          }
        }
      }
    }
  }

  depends_on = [google_secret_manager_secret_iam_member.cdc_secret_access]
}

output "cdc_consumer_service" {
  value = google_cloud_run_v2_service.cdc_consumer.name
}
