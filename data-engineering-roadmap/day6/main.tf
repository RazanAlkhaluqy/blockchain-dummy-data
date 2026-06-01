# ============================================================
# Day 6 — Terraform: Blockchain Data Platform Infrastructure
# Provider: AWS + Google Cloud
# Run: terraform init && terraform plan && terraform apply
# ============================================================

terraform {
  required_version = ">= 1.6.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

# ── Variables ─────────────────────────────────────────────────
variable "project_id"   { default = "blockchain-startup-prod" }
variable "region_aws"   { default = "us-east-1" }
variable "region_gcp"   { default = "US" }
variable "environment"  { default = "prod" }

# ── Providers ─────────────────────────────────────────────────
provider "aws" {
  region = var.region_aws
}

provider "google" {
  project = var.project_id
  region  = "us-central1"
}

# ════════════════════════════════════════════════════════════════
# AWS S3 — Data Lake (raw + staging Parquet files)
# ════════════════════════════════════════════════════════════════

resource "aws_s3_bucket" "data_lake" {
  bucket = "blockchain-data-lake-${var.environment}"

  tags = {
    Name        = "Blockchain Data Lake"
    Environment = var.environment
    Team        = "data-engineering"
  }
}

resource "aws_s3_bucket_versioning" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id

  rule {
    id     = "move-to-glacier"
    status = "Enabled"
    transition {
      days          = 90
      storage_class = "GLACIER"
    }
  }
}

# S3 folder structure (objects with prefixes)
resource "aws_s3_object" "raw_prefix" {
  bucket  = aws_s3_bucket.data_lake.id
  key     = "raw/blockchain-events/"
  content = ""
}

resource "aws_s3_object" "staging_prefix" {
  bucket  = aws_s3_bucket.data_lake.id
  key     = "staging/warehouse-events/"
  content = ""
}

# ════════════════════════════════════════════════════════════════
# Google BigQuery — Data Warehouse
# ════════════════════════════════════════════════════════════════

resource "google_bigquery_dataset" "warehouse" {
  dataset_id    = "blockchain_warehouse"
  friendly_name = "Blockchain Analytics Warehouse"
  description   = "Production data warehouse for blockchain startup analytics"
  location      = var.region_gcp

  labels = {
    environment = var.environment
    team        = "data-engineering"
  }

  access {
    role          = "OWNER"
    special_group = "projectOwners"
  }
}

# Fact table — partitioned by day, clustered by chain + token
resource "google_bigquery_table" "fact_transactions" {
  dataset_id = google_bigquery_dataset.warehouse.dataset_id
  table_id   = "fact_transactions"
  description = "One row per confirmed on-chain transaction"

  time_partitioning {
    type  = "DAY"
    field = "timestamp"
    expiration_ms = 31536000000  # 1 year
  }

  clustering = ["chain", "token_symbol"]

  require_partition_filter = true

  schema = jsonencode([
    { name="event_id",     type="STRING",    mode="REQUIRED" },
    { name="tx_hash",      type="STRING",    mode="REQUIRED" },
    { name="chain",        type="STRING",    mode="NULLABLE" },
    { name="event_type",   type="STRING",    mode="NULLABLE" },
    { name="protocol",     type="STRING",    mode="NULLABLE" },
    { name="from_address", type="STRING",    mode="NULLABLE" },
    { name="to_address",   type="STRING",    mode="NULLABLE" },
    { name="token_symbol", type="STRING",    mode="NULLABLE" },
    { name="amount",       type="NUMERIC",   mode="NULLABLE" },
    { name="amount_usd",   type="NUMERIC",   mode="NULLABLE" },
    { name="gas_fee_usd",  type="NUMERIC",   mode="NULLABLE" },
    { name="year",         type="INTEGER",   mode="NULLABLE" },
    { name="month",        type="INTEGER",   mode="NULLABLE" },
    { name="day",          type="INTEGER",   mode="NULLABLE" },
    { name="timestamp",    type="TIMESTAMP", mode="REQUIRED" },
    { name="loaded_at",    type="TIMESTAMP", mode="NULLABLE" },
  ])
}

# Dimension table — wallet profiles
resource "google_bigquery_table" "dim_wallets" {
  dataset_id = google_bigquery_dataset.warehouse.dataset_id
  table_id   = "dim_wallet_profiles"

  schema = jsonencode([
    { name="wallet_id",       type="INTEGER", mode="REQUIRED" },
    { name="wallet_address",  type="STRING",  mode="REQUIRED" },
    { name="country",         type="STRING",  mode="NULLABLE" },
    { name="wallet_type",     type="STRING",  mode="NULLABLE" },
    { name="risk_score",      type="FLOAT",   mode="NULLABLE" },
    { name="kyc_verified",    type="BOOLEAN", mode="NULLABLE" },
    { name="primary_chain",   type="STRING",  mode="NULLABLE" },
    { name="label",           type="STRING",  mode="NULLABLE" },
  ])
}

# Mart tables (dbt writes these)
resource "google_bigquery_table" "mart_chain" {
  dataset_id = google_bigquery_dataset.warehouse.dataset_id
  table_id   = "mart_chain_analytics"
}

resource "google_bigquery_table" "mart_wallets" {
  dataset_id = google_bigquery_dataset.warehouse.dataset_id
  table_id   = "mart_wallet_leaderboard"
}

# ════════════════════════════════════════════════════════════════
# AWS IAM — Airflow Service Role
# ════════════════════════════════════════════════════════════════

resource "aws_iam_role" "airflow" {
  name = "airflow-data-pipeline-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "airflow.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "airflow_s3" {
  name = "airflow-s3-access"
  role = aws_iam_role.airflow.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["s3:GetObject","s3:PutObject","s3:ListBucket"]
      Resource = [
        aws_s3_bucket.data_lake.arn,
        "${aws_s3_bucket.data_lake.arn}/*"
      ]
    }]
  })
}

# ════════════════════════════════════════════════════════════════
# Outputs — printed after terraform apply
# ════════════════════════════════════════════════════════════════

output "s3_bucket_name" {
  value = aws_s3_bucket.data_lake.bucket
}

output "bigquery_dataset" {
  value = google_bigquery_dataset.warehouse.dataset_id
}

output "airflow_role_arn" {
  value = aws_iam_role.airflow.arn
}
