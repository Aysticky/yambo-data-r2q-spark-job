# DynamoDB Module for Checkpoints
#
# CHECKPOINT TABLE DESIGN:
# - Partition key: checkpoint_key (string) - Unique identifier per data source
# - Attributes stored as flexible JSON (DynamoDB is schemaless)
# 
# EXAMPLE RECORDS:
# {
#   "checkpoint_key": "stripe_charges_prod",
#   "last_extracted_timestamp": "2026-02-12T10:00:00Z",
#   "records_extracted": 1234567,
#   "records_extracted_last_run": 5000,
#   "updated_at": "2026-02-12T11:00:00Z",
#   "updated_by": "spark-job-run-abc123"
# }
#
# CAPACITY MODES:
#
# 1. On-Demand (RECOMMENDED for variable workloads):
#    - Pros: No capacity planning, auto-scales, pay per request
#    - Cons: 5x more expensive than provisioned (but still cheap)
#    - Cost: $1.25 per million writes, $0.25 per million reads
#    - Use case: Dev, low-volume prod, unpredictable traffic
#
# 2. Provisioned:
#    - Pros: Cheaper for predictable workloads
#    - Cons: Need to provision capacity, pay even when not used
#    - Cost: $0.00065 per WCU-hour, $0.00013 per RCU-hour
#    - Use case: High-volume prod with predictable patterns
#
# COST EXAMPLE (checkpoint table):
# - 1 hourly job = 2 requests/hour (1 read, 1 write)
# - 2 requests/hour * 24 hours * 30 days = 1,440 requests/month
# - On-demand cost: ~$0.002/month (negligible)
# - Even with 100 jobs: ~$0.20/month
#
# PERFORMANCE:
# - Read/write latency: <10ms (typically 1-3ms)
# - Throughput: 40,000 RCU/WCU per table per region (more with on-demand)
# - Consistency: Strong consistency for reads (for checkpoints)
#
# COMMON ISSUES:
# 1. ProvisionedThroughputExceededException:
#    - Cause: Exceeded RCU/WCU limits (only in provisioned mode)
#    - Solution: Enable auto-scaling or switch to on-demand
#    - Detection: CloudWatch DynamoDB metrics
#
# 2. Hot partition:
#    - Cause: All jobs using same checkpoint_key
#    - Impact: Throttling even with plenty of capacity
#    - Solution: Use composite keys (e.g., "stripe_charges_2026-02-12")
#    - Prevention: Design partition keys for even distribution
#
# 3. Eventual consistency reads returning stale data:
#    - Symptom: Job reads old checkpoint value
#    - Solution: Use ConsistentRead=true (this module does it)
#    - Cost: 2x RCU cost vs eventually consistent

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

# Checkpoints table
resource "aws_dynamodb_table" "checkpoints" {
  name         = "${var.project_name}-${var.environment}-checkpoints"
  billing_mode = "PAY_PER_REQUEST" # On-demand mode
  hash_key     = "checkpoint_key"

  attribute {
    name = "checkpoint_key"
    type = "S" # String
  }

  # Enable point-in-time recovery (backups)
  point_in_time_recovery {
    enabled = var.environment == "prod" ? true : false
  }

  # Enable encryption at rest
  server_side_encryption {
    enabled     = true
    kms_key_arn = aws_kms_key.dynamodb_key.arn
  }

  # TTL for automatic cleanup (optional)
  # For checkpoints, we usually don't set TTL, but useful for temporary data
  # ttl {
  #   attribute_name = "expiry_time"
  #   enabled        = true
  # }

  tags = merge(
    var.common_tags,
    {
      Name    = "${var.project_name}-${var.environment}-checkpoints"
      Purpose = "Checkpoint tracking for incremental data extraction"
    }
  )
}

# KMS key for DynamoDB encryption
resource "aws_kms_key" "dynamodb_key" {
  description             = "KMS key for ${var.project_name}-${var.environment} DynamoDB"
  deletion_window_in_days = var.environment == "prod" ? 30 : 7
  enable_key_rotation     = true

  tags = merge(
    var.common_tags,
    {
      Name = "${var.project_name}-${var.environment}-dynamodb-key"
    }
  )
}

resource "aws_kms_alias" "dynamodb_key" {
  name          = "alias/${var.project_name}-${var.environment}-dynamodb"
  target_key_id = aws_kms_key.dynamodb_key.key_id
}

# Optional: Job metadata table (for tracking Spark job runs)
# This is separate from checkpoints - stores job execution history
resource "aws_dynamodb_table" "job_metadata" {
  name         = "${var.project_name}-${var.environment}-job-metadata"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "job_id"
  range_key    = "start_time"

  attribute {
    name = "job_id"
    type = "S" # String (e.g., "extract-stripe-charges")
  }

  attribute {
    name = "start_time"
    type = "S" # ISO 8601 timestamp
  }

  # GSI for querying by status
  attribute {
    name = "status"
    type = "S" # running, succeeded, failed
  }

  global_secondary_index {
    name            = "status-index"
    hash_key        = "status"
    range_key       = "start_time"
    projection_type = "ALL"
  }

  # TTL to auto-delete old job records (keep last 90 days)
  ttl {
    attribute_name = "expiry_time"
    enabled        = true
  }

  point_in_time_recovery {
    enabled = var.environment == "prod" ? true : false
  }

  server_side_encryption {
    enabled     = true
    kms_key_arn = aws_kms_key.dynamodb_key.arn
  }

  tags = merge(
    var.common_tags,
    {
      Name    = "${var.project_name}-${var.environment}-job-metadata"
      Purpose = "Spark job execution history and metadata"
    }
  )
}

# CloudWatch alarms for monitoring
resource "aws_cloudwatch_metric_alarm" "checkpoints_read_throttle" {
  count = var.environment == "prod" ? 1 : 0

  alarm_name          = "${var.project_name}-${var.environment}-checkpoints-read-throttle"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = "2"
  metric_name         = "UserErrors"
  namespace           = "AWS/DynamoDB"
  period              = "300" # 5 minutes
  statistic           = "Sum"
  threshold           = "10"
  alarm_description   = "DynamoDB checkpoints table is being throttled"
  alarm_actions       = [] # Add SNS topic ARN for notifications

  dimensions = {
    TableName = aws_dynamodb_table.checkpoints.name
  }

  tags = var.common_tags
}

# Outputs
output "checkpoints_table_name" {
  description = "Checkpoints table name"
  value       = aws_dynamodb_table.checkpoints.name
}

output "checkpoints_table_arn" {
  description = "Checkpoints table ARN"
  value       = aws_dynamodb_table.checkpoints.arn
}

output "job_metadata_table_name" {
  description = "Job metadata table name"
  value       = aws_dynamodb_table.job_metadata.name
}

output "job_metadata_table_arn" {
  description = "Job metadata table ARN"
  value       = aws_dynamodb_table.job_metadata.arn
}

output "dynamodb_kms_key_arn" {
  description = "KMS key ARN for DynamoDB encryption"
  value       = aws_kms_key.dynamodb_key.arn
}

# PRODUCTION NOTES:
#
# 1. Backup Strategy:
#    - Enable point-in-time recovery (PITR) for prod
#    - PITR allows restore to any point in last 35 days
#    - Cost: ~$0.20 per GB-month
#    - Create on-demand backups before major changes
#
# 2. Monitoring:
#    - Track ConsumedReadCapacityUnits and ConsumedWriteCapacityUnits
#    - Alert on UserErrors (throttling) or SystemErrors
#    - Monitor table size (on-demand has no size limit, but costs scale)
#
# 3. Access Patterns:
#    - Checkpoints: GetItem (read) + PutItem (write) per job run
#    - Job metadata: PutItem (on start) + UpdateItem (on completion)
#    - Query by status using GSI for dashboards
#
# 4. Cost Optimization:
#    - On-demand is surprisingly cheap for this use case (<$1/month typically)
#    - Only switch to provisioned if doing >1M operations/month
#    - Use TTL to auto-delete old data (saves storage costs)
