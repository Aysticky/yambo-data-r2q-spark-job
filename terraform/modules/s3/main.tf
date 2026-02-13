# S3 Module for Data Lake
#
# DATA LAKE ARCHITECTURE:
# This module creates a multi-zone data lake:
# - raw/: Untransformed data from APIs (Parquet)
# - staging/: Intermediate transformations
# - curated/: Clean, business-ready data
# - checkpoints/: Job state and metadata
# - artifacts/: Spark job JARs, Python wheels
#
# PARTITIONING STRATEGY:
# - Partition by date (dt=YYYY-MM-DD) for time-based queries
# - Partition by source (source=stripe, source=square) for multi-source
# - Partition by run_id for idempotency and debugging
#
# LIFECYCLE POLICIES:
# - raw/: 90 days Standard → 365 days Glacier → Delete
# - staging/: 30 days Standard → Delete
# - curated/: Keep forever (or per business requirement)
# - artifacts/: Versioned, keep last 10 versions
#
# COST OPTIMIZATION:
# - Use S3 intelligent-tiering for automatic cost optimization
# - Enable compression (Parquet with Snappy)
# - Set lifecycle policies
# - Use S3 Inventory ($0.0025 per million objects) for large buckets
#
# COMMON ISSUES:
# 1. S3 throttling (503 SlowDown):
#    - Cause: >3500 PUT/sec or >5500 GET/sec per prefix
#    - Solution: Use more date partitions (distribute across prefixes)
#    - Detection: CloudWatch S3 metrics show 5xx errors
#
# 2. Eventual consistency (rare in new regions):
#    - Cause: Reading just-written object returns 404
#    - Solution: Use eu-central-1 (strong consistency since 2020)
#    - Detection: Random 404s on recently written files
#
# 3. High costs:
#    - Cause: Storing too much data in Standard tier
#    - Solution: Enable Intelligent-Tiering or lifecycle policies
#    - Prevention: Monitor S3 storage costs weekly

variable "project_name" {
  description = "Project name"
  type        = string
}

variable "environment" {
  description = "Environment (dev or prod)"
  type        = string
}

variable "common_tags" {
  description = "Common tags for all resources"
  type        = map(string)
  default     = {}
}

# Data lake bucket
resource "aws_s3_bucket" "data_lake" {
  bucket = "${var.project_name}-${var.environment}-data-lake"

  tags = merge(
    var.common_tags,
    {
      Name    = "${var.project_name}-${var.environment}-data-lake"
      Purpose = "DataLakeRawStagingCurated"
    }
  )
}

# Enable versioning for data protection
resource "aws_s3_bucket_versioning" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id

  versioning_configuration {
    status = "Enabled"
  }
}

# Enable encryption at rest
resource "aws_s3_bucket_server_side_encryption_configuration" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = aws_kms_key.s3_key.arn
    }
    bucket_key_enabled = true  # Reduces KMS costs by 99%
  }
}

# KMS key for S3 encryption
resource "aws_kms_key" "s3_key" {
  description             = "KMS key for ${var.project_name}-${var.environment} S3 data lake"
  deletion_window_in_days = var.environment == "prod" ? 30 : 7
  enable_key_rotation     = true

  tags = merge(
    var.common_tags,
    {
      Name = "${var.project_name}-${var.environment}-s3-key"
    }
  )
}

resource "aws_kms_alias" "s3_key" {
  name          = "alias/${var.project_name}-${var.environment}-s3"
  target_key_id = aws_kms_key.s3_key.key_id
}

# Block public access (security best practice)
resource "aws_s3_bucket_public_access_block" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Lifecycle policy for cost optimization
resource "aws_s3_bucket_lifecycle_configuration" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id

  # Raw zone: Keep for 90 days in Standard, then Glacier for 1 year
  rule {
    id     = "raw-zone-lifecycle"
    status = "Enabled"

    filter {
      prefix = "raw/"
    }

    transition {
      days          = 90
      storage_class = "GLACIER"
    }

    expiration {
      days = 365
    }

    noncurrent_version_expiration {
      noncurrent_days = 30
    }
  }

  # Staging zone: Delete after 30 days
  rule {
    id     = "staging-zone-lifecycle"
    status = "Enabled"

    filter {
      prefix = "staging/"
    }

    expiration {
      days = 30
    }

    noncurrent_version_expiration {
      noncurrent_days = 7
    }
  }

  # Checkpoints: Keep for 180 days
  rule {
    id     = "checkpoints-lifecycle"
    status = "Enabled"

    filter {
      prefix = "checkpoints/"
    }

    expiration {
      days = 180
    }
  }

  # Artifacts: Use versioning, keep last 10 versions
  rule {
    id     = "artifacts-lifecycle"
    status = "Enabled"

    filter {
      prefix = "artifacts/"
    }

    noncurrent_version_expiration {
      newer_noncurrent_versions = 10
      noncurrent_days           = 1
    }
  }
}

# Enable intelligent-tiering for automatic cost optimization
resource "aws_s3_bucket_intelligent_tiering_configuration" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id
  name   = "EntireDataLake"

  tiering {
    access_tier = "ARCHIVE_ACCESS"
    days        = 90
  }

  tiering {
    access_tier = "DEEP_ARCHIVE_ACCESS"
    days        = 180
  }
}

# Bucket for Spark event logs (for Spark History Server)
resource "aws_s3_bucket" "spark_logs" {
  bucket = "${var.project_name}-${var.environment}-spark-logs"

  tags = merge(
    var.common_tags,
    {
      Name    = "${var.project_name}-${var.environment}-spark-logs"
      Purpose = "SparkEventLogsHistoryServer"
    }
  )
}

resource "aws_s3_bucket_lifecycle_configuration" "spark_logs" {
  bucket = aws_s3_bucket.spark_logs.id

  rule {
    id     = "spark-logs-retention"
    status = "Enabled"

    filter {}  # Empty filter applies to all objects

    expiration {
      days = var.environment == "prod" ? 90 : 30
    }
  }
}

# Outputs
output "data_lake_bucket_name" {
  description = "Data lake bucket name"
  value       = aws_s3_bucket.data_lake.id
}

output "data_lake_bucket_arn" {
  description = "Data lake bucket ARN"
  value       = aws_s3_bucket.data_lake.arn
}

output "spark_logs_bucket_name" {
  description = "Spark logs bucket name"
  value       = aws_s3_bucket.spark_logs.id
}

output "spark_logs_bucket_arn" {
  description = "Spark logs bucket ARN"
  value       = aws_s3_bucket.spark_logs.arn
}

output "kms_key_arn" {
  description = "KMS key ARN for S3 encryption"
  value       = aws_kms_key.s3_key.arn
}

# PRODUCTION NOTES:
#
# 1. S3 Request Rates:
#    - If you see throttling, you might need S3 Transfer Acceleration
#    - Or use S3 Express One Zone (10x faster but limited regions)
#
# 2. Cost Monitoring:
#    - Set up AWS Budgets alert for S3 costs >$100/month (dev) or >$500/month (prod)
#    - Review S3 Inventory monthly to identify large/old files
#
# 3. Data Governance:
#    - Enable S3 Object Lock for compliance (immutable data)
#    - Use S3 Inventory + Athena to query all objects
#    - Tag objects with classification (PII, public, internal)
#
# 4. Backup Strategy:
#    - S3 is already 11 nines durable (no backup needed for durability)
#    - Enable versioning + MFA delete for protection against accidental deletion
#    - Use S3 Cross-Region Replication for disaster recovery (prod only)
