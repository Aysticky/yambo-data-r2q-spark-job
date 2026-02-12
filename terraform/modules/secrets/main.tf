# Secrets Manager Module for API Credentials
#
# SECRETS MANAGEMENT STRATEGY:
# - Use AWS Secrets Manager for sensitive credentials (API keys, tokens)
# - NEVER hardcode secrets in code or environment variables
# - Rotate secrets regularly (Secrets Manager can automate this)
# - Use IAM policies to control access (least privilege)
#
# SECRETS MANAGER VS SYSTEMS MANAGER PARAMETER STORE:
#
# Secrets Manager:
# Pros:
#   - Automatic rotation (built-in Lambda functions)
#   - Audit trail (who accessed when)
#   - Cross-region replication
#   - Versioning
# Cons:
#   - More expensive ($0.40/secret/month + $0.05 per 10K API calls)
#
# Parameter Store:
# Pros:
#   - Free for standard parameters
#   - Simple key-value store
# Cons:
#   - No automatic rotation
#   - Limited versioning
#   - 4KB value size limit (Standard tier)
#
# RECOMMENDATION:
# - Use Secrets Manager for external API credentials (rotation needed)
# - Use Parameter Store for configuration (non-secret, frequently accessed)
#
# SECRET STRUCTURE:
# Store as JSON for flexibility:
# {
#   "api_key": "sk_live_abc123...",
#   "api_url": "https://api.stripe.com/v1",
#   "environment": "production"
# }
#
# ROTATION STRATEGY:
# 1. Create new API key in external system (Stripe dashboard)
# 2. Update secret in Secrets Manager
# 3. Test with new key
# 4. Revoke old API key after 24 hours (grace period)
#
# COMMON ISSUES:
# 1. AccessDeniedException:
#    - Cause: IAM role lacks secretsmanager:GetSecretValue permission
#    - Solution: Add policy to service account IAM role
#    - Detection: Lambda/container logs show 403 error
#
# 2. ResourceNotFoundException:
#    - Cause: Secret name typo or wrong region
#    - Solution: Verify secret exists in same region as compute
#    - Detection: Job fails at startup with "Secret not found"
#
# 3. High Secrets Manager costs:
#    - Cause: Retrieving secret on every request (inefficient)
#    - Solution: Cache secret for 1 hour, refresh only on 401
#    - Example: Caching saves $5/month per 1M requests
#
# 4. Secret rotation breaks running jobs:
#    - Cause: Job cached old secret, new secret rotated
#    - Solution: Implement 401 retry logic (refresh secret on auth failure)
#    - Prevention: Set rotation schedule during maintenance window

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

# KMS key for Secrets Manager encryption
resource "aws_kms_key" "secrets_key" {
  description             = "KMS key for ${var.project_name}-${var.environment} Secrets Manager"
  deletion_window_in_days = var.environment == "prod" ? 30 : 7
  enable_key_rotation     = true

  tags = merge(
    var.common_tags,
    {
      Name = "${var.project_name}-${var.environment}-secrets-key"
    }
  )
}

resource "aws_kms_alias" "secrets_key" {
  name          = "alias/${var.project_name}-${var.environment}-secrets"
  target_key_id = aws_kms_key.secrets_key.key_id
}

# Stripe API credentials
resource "aws_secretsmanager_secret" "stripe_api" {
  name        = "${var.project_name}/${var.environment}/stripe-api"
  description = "Stripe API credentials for data extraction"
  kms_key_id  = aws_kms_key.secrets_key.arn

  # Recovery window before permanent deletion
  recovery_window_in_days = var.environment == "prod" ? 30 : 7

  tags = merge(
    var.common_tags,
    {
      Name        = "${var.project_name}-${var.environment}-stripe-api"
      Purpose     = "Stripe API authentication"
      Environment = var.environment
    }
  )
}

# Placeholder secret value (update manually after creation)
# In production, you would set this through AWS Console or CLI, not in Terraform
# This prevents secrets from being stored in Terraform state
resource "aws_secretsmanager_secret_version" "stripe_api_placeholder" {
  secret_id = aws_secretsmanager_secret.stripe_api.id

  # Placeholder value - UPDATE THIS MANUALLY after Terraform apply
  secret_string = jsonencode({
    api_key     = "PLACEHOLDER_UPDATE_IN_AWS_CONSOLE"
    api_url     = "https://api.stripe.com/v1"
    environment = var.environment
    notes       = "Update this secret with actual API key after Terraform deployment"
  })

  lifecycle {
    ignore_changes = [secret_string]  # Don't overwrite manual updates
  }
}

# General purpose secrets (for other APIs as needed)
resource "aws_secretsmanager_secret" "general_api" {
  name        = "${var.project_name}/${var.environment}/general-api"
  description = "General API credentials (Square, Shopify, etc.)"
  kms_key_id  = aws_kms_key.secrets_key.arn

  recovery_window_in_days = var.environment == "prod" ? 30 : 7

  tags = merge(
    var.common_tags,
    {
      Name    = "${var.project_name}-${var.environment}-general-api"
      Purpose = "General API authentication"
    }
  )
}

resource "aws_secretsmanager_secret_version" "general_api_placeholder" {
  secret_id = aws_secretsmanager_secret.general_api.id

  secret_string = jsonencode({
    placeholder = "UPDATE_WITH_ACTUAL_CREDENTIALS"
  })

  lifecycle {
    ignore_changes = [secret_string]
  }
}

# CloudWatch Log group for secret access audit
resource "aws_cloudwatch_log_group" "secrets_audit" {
  name              = "/aws/secretsmanager/${var.project_name}-${var.environment}/audit"
  retention_in_days = var.environment == "prod" ? 365 : 90
  kms_key_id        = aws_kms_key.secrets_key.arn

  tags = var.common_tags
}

# EventBridge rule to log secret access
resource "aws_cloudwatch_event_rule" "secrets_access" {
  name        = "${var.project_name}-${var.environment}-secrets-access"
  description = "Log Secrets Manager access events"

  event_pattern = jsonencode({
    source      = ["aws.secretsmanager"]
    detail-type = ["AWS API Call via CloudTrail"]
    detail = {
      eventSource = ["secretsmanager.amazonaws.com"]
      eventName = [
        "GetSecretValue",
        "RotateSecret",
        "UpdateSecret"
      ]
    }
  })

  tags = var.common_tags
}

resource "aws_cloudwatch_event_target" "secrets_access" {
  rule      = aws_cloudwatch_event_rule.secrets_access.name
  target_id = "SendToCloudWatchLogs"
  arn       = aws_cloudwatch_log_group.secrets_audit.arn
}

# Outputs
output "stripe_api_secret_name" {
  description = "Stripe API secret name"
  value       = aws_secretsmanager_secret.stripe_api.name
}

output "stripe_api_secret_arn" {
  description = "Stripe API secret ARN"
  value       = aws_secretsmanager_secret.stripe_api.arn
}

output "general_api_secret_name" {
  description = "General API secret name"
  value       = aws_secretsmanager_secret.general_api.name
}

output "general_api_secret_arn" {
  description = "General API secret ARN"
  value       = aws_secretsmanager_secret.general_api.arn
}

output "secrets_kms_key_arn" {
  description = "KMS key ARN for Secrets Manager encryption"
  value       = aws_kms_key.secrets_key.arn
}

# PRODUCTION NOTES:
#
# 1. Setting Secret Values:
#    After Terraform creates the secret, update it manually:
#    
#    # Via AWS CLI
#    aws secretsmanager put-secret-value \
#      --secret-id yambo/prod/stripe-api \
#      --secret-string '{"api_key":"sk_live_abc123...","api_url":"https://api.stripe.com/v1"}'
#    
#    # Via AWS Console
#    - Navigate to Secrets Manager → yambo/prod/stripe-api
#    - Click "Retrieve secret value" → Edit
#    - Update JSON with actual API key
#    - Save
#
# 2. IAM Permissions:
#    Spark jobs need this IAM policy:
#    {
#      "Effect": "Allow",
#      "Action": [
#        "secretsmanager:GetSecretValue",
#        "secretsmanager:DescribeSecret"
#      ],
#      "Resource": [
#        "arn:aws:secretsmanager:eu-central-1:*:secret:yambo/prod/stripe-api-*"
#      ]
#    }
#    
#    Also need KMS permissions if using customer-managed keys:
#    {
#      "Effect": "Allow",
#      "Action": [
#        "kms:Decrypt",
#        "kms:DescribeKey"
#      ],
#      "Resource": "<kms_key_arn>"
#    }
#
# 3. Caching Strategy:
#    - Cache secret for 1 hour in application
#    - Refresh on 401 Unauthorized from API
#    - Don't cache in globals (memory leak in long-running pods)
#    
#    Example Python caching:
#    ```python
#    from functools import lru_cache
#    import time
#    
#    @lru_cache(maxsize=1)
#    def get_cached_secret(cache_bust: int):
#        return secrets_client.get_secret_value(SecretId="...")
#    
#    # Cache for 1 hour (3600 seconds)
#    secret = get_cached_secret(int(time.time() / 3600))
#    ```
#
# 4. Automatic Rotation:
#    To enable automatic rotation:
#    - Create Lambda rotation function
#    - Lambda calls external API to generate new key
#    - Lambda updates secret with new key
#    - Lambda tests new key
#    - Set rotation schedule (e.g., every 30 days)
#    
#    AWS provides templates for common services:
#    - RDS databases (built-in)
#    - Custom APIs (write your own Lambda)
#
# 5. Monitoring:
#    Set CloudWatch alarms for:
#    - Unauthorized access attempts (high priority alert)
#    - Failed rotation attempts
#    - Secrets approaching expiration (if using expiry)
#
# 6. Compliance:
#    - Audit log (CloudTrail) shows who accessed secrets when
#    - Encrypt secrets at rest (KMS)
#    - Rotate secrets every 30-90 days
#    - Use separate secrets for dev/prod (prevent prod access from dev)
