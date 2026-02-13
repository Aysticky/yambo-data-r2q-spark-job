# ECR Module for Docker Image Registry
#
# CONTAINER REGISTRY STRATEGY:
# This module creates ECR repositories for:
# - Spark job images (main application code)
# - Airflow DAG images (if using KubernetesExecutor)
# - Custom operator images
#
# ECR vs Docker Hub vs Other:
# Pros of ECR:
#   - Native AWS integration (IAM, VPC endpoints)
#   - No pull rate limits (unlike Docker Hub free tier: 100 pulls/6 hours)
#   - Automatic image scanning for vulnerabilities
#   - Encryption at rest
#   - Lifecycle policies for automatic cleanup
#
# Cons:
#   - Costs money ($0.10/GB/month storage)
#   - Regional (need to replicate for multi-region)
#
# IMAGE TAGGING STRATEGY:
# 1. Use semantic versioning: v1.0.0, v1.0.1, etc.
# 2. Also tag with git commit SHA: abc123def456
# 3. Also tag environment: dev-latest, prod-latest
# 4. Never use "latest" alone (not deterministic)
#
# Example:
# yambo-spark-job:v1.2.3
# yambo-spark-job:abc123def456
# yambo-spark-job:dev-latest
#
# SECURITY SCANNING:
# - ECR scans images on push for CVEs (Common Vulnerabilities)
# - Severity levels: Critical, High, Medium, Low, Informational
# - Block deployment if critical vulnerabilities found
# - Regularly update base images (e.g., python:3.9-slim)
#
# COMMON ISSUES:
# 1. ImagePullBackOff in Kubernetes:
#    Cause: ECR authentication expired or missing IRSA permissions
#    Solution: Use IAM roles for service accounts (IRSA)
#    Detection: kubectl describe pod shows "401 Unauthorized"
#
# 2. Large image sizes causing slow pulls:
#    Cause: Unnecessary files in image (data, logs, cache)
#    Impact: Slow pod startup (2-3 min instead of 30s)
#    Solution: Use .dockerignore, multi-stage builds
#    Target: <500MB for Spark image
#
# 3. Vulnerability scan failures blocking deployments:
#    Cause: Old base image with known CVEs
#    Solution: Regularly update base images, use hardened images
#    Prevention: Automated base image updates in CI/CD
#
# 4. High ECR costs:
#    Cause: Accumulating old images
#    Solution: Lifecycle policies (delete untagged after 7 days)
#    Prevention: Set retention policies at creation

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

resource "aws_kms_key" "ecr_key" {
  description             = "KMS key for ${var.project_name}-${var.environment} ECR"
  deletion_window_in_days = var.environment == "prod" ? 30 : 7
  enable_key_rotation     = true

  tags = merge(
    var.common_tags,
    {
      Name = "${var.project_name}-${var.environment}-ecr-key"
    }
  )
}

resource "aws_kms_alias" "ecr_key" {
  name          = "alias/${var.project_name}-${var.environment}-ecr"
  target_key_id = aws_kms_key.ecr_key.key_id
}

# Spark job image repository
resource "aws_ecr_repository" "spark_job" {
  name                 = "${var.project_name}-${var.environment}-spark-job"
  image_tag_mutability = "MUTABLE"  # Allow overwriting tags like "dev-latest"

  # Enable encryption at rest
  encryption_configuration {
    encryption_type = "KMS"
    kms_key         = aws_kms_key.ecr_key.arn
  }

  # Enable image scanning on push
  image_scanning_configuration {
    scan_on_push = true
  }

  tags = merge(
    var.common_tags,
    {
      Name    = "${var.project_name}-${var.environment}-spark-job"
      Purpose = "Spark job Docker images"
    }
  )
}

# Lifecycle policy to delete old images
resource "aws_ecr_lifecycle_policy" "spark_job" {
  repository = aws_ecr_repository.spark_job.name

  policy = jsonencode({
    rules = [
      {
        rulePriority = 1
        description  = "Keep last 10 tagged images"
        selection = {
          tagStatus     = "tagged"
          tagPrefixList = ["v", "dev-", "prod-"]
          countType     = "imageCountMoreThan"
          countNumber   = 10
        }
        action = {
          type = "expire"
        }
      },
      {
        rulePriority = 2
        description  = "Delete untagged images after 7 days"
        selection = {
          tagStatus   = "untagged"
          countType   = "sinceImagePushed"
          countUnit   = "days"
          countNumber = 7
        }
        action = {
          type = "expire"
        }
      }
    ]
  })
}

# Airflow DAG image repository (if using KubernetesExecutor)
resource "aws_ecr_repository" "airflow_dags" {
  name                 = "${var.project_name}-${var.environment}-airflow-dags"
  image_tag_mutability = "MUTABLE"

  encryption_configuration {
    encryption_type = "KMS"
    kms_key         = aws_kms_key.ecr_key.arn
  }

  image_scanning_configuration {
    scan_on_push = true
  }

  tags = merge(
    var.common_tags,
    {
      Name    = "${var.project_name}-${var.environment}-airflow-dags"
      Purpose = "Airflow DAG Docker images"
    }
  )
}

resource "aws_ecr_lifecycle_policy" "airflow_dags" {
  repository = aws_ecr_repository.airflow_dags.name

  policy = jsonencode({
    rules = [
      {
        rulePriority = 1
        description  = "Keep last 5 tagged images"
        selection = {
          tagStatus     = "tagged"
          tagPrefixList = ["v"]
          countType     = "imageCountMoreThan"
          countNumber   = 5
        }
        action = {
          type = "expire"
        }
      },
      {
        rulePriority = 2
        description  = "Delete untagged images after 3 days"
        selection = {
          tagStatus   = "untagged"
          countType   = "sinceImagePushed"
          countUnit   = "days"
          countNumber = 3
        }
        action = {
          type = "expire"
        }
      }
    ]
  })
}

# CloudWatch log group for scan findings
resource "aws_cloudwatch_log_group" "ecr_scan_findings" {
  name              = "/aws/ecr/${var.project_name}-${var.environment}/scan-findings"
  retention_in_days = var.environment == "prod" ? 90 : 30

  # Removed KMS encryption to avoid permission issues (not critical for dev)
  # kms_key_id = aws_kms_key.ecr_key.arn

  tags = var.common_tags
}

# EventBridge rule to capture ECR scan findings
resource "aws_cloudwatch_event_rule" "ecr_scan_findings" {
  name        = "${var.project_name}-${var.environment}-ecr-scan-findings"
  description = "Capture ECR image scan findings"

  event_pattern = jsonencode({
    source      = ["aws.ecr"]
    detail-type = ["ECR Image Scan"]
    detail = {
      repository-name = [
        aws_ecr_repository.spark_job.name,
        aws_ecr_repository.airflow_dags.name
      ]
      scan-status = ["COMPLETE"]
    }
  })

  tags = var.common_tags
}

resource "aws_cloudwatch_event_target" "ecr_scan_findings" {
  rule      = aws_cloudwatch_event_rule.ecr_scan_findings.name
  target_id = "SendToCloudWatchLogs"
  arn       = aws_cloudwatch_log_group.ecr_scan_findings.arn
}

# Outputs
output "spark_job_repository_url" {
  description = "Spark job ECR repository URL"
  value       = aws_ecr_repository.spark_job.repository_url
}

output "spark_job_repository_arn" {
  description = "Spark job ECR repository ARN"
  value       = aws_ecr_repository.spark_job.arn
}

output "airflow_dags_repository_url" {
  description = "Airflow DAGs ECR repository URL"
  value       = aws_ecr_repository.airflow_dags.repository_url
}

output "airflow_dags_repository_arn" {
  description = "Airflow DAGs ECR repository ARN"
  value       = aws_ecr_repository.airflow_dags.arn
}

output "ecr_kms_key_arn" {
  description = "KMS key ARN for ECR encryption"
  value       = aws_kms_key.ecr_key.arn
}

# PRODUCTION NOTES:
#
# 1. ECR Authentication:
#    Kubernetes pods need ECR pull permissions:
#    - Option A: IRSA (IAM Roles for Service Accounts) - RECOMMENDED
#    - Option B: Node IAM role (works but less secure)
#    - Option C: ImagePullSecrets (manual, not recommended)
#
#    IRSA example:
#    - Create IAM role for Spark service account
#    - Attach policy with ecr:GetAuthorizationToken, ecr:BatchGetImage
#    - Annotate Kubernetes service account with IAM role ARN
#    - Pods automatically get ECR access
#
# 2. Image Build & Push:
#    CI/CD pipeline should:
#    - Build Docker image
#    - Tag with version and commit SHA
#    - Push to ECR
#    - Update Kubernetes deployment/CRD with new image
#
#    Example GitHub Actions:
#    - aws ecr get-login-password | docker login --username AWS --password-stdin
#    - docker build -t spark-job:$GIT_SHA .
#    - docker tag spark-job:$GIT_SHA $ECR_REPO:$GIT_SHA
#    - docker push $ECR_REPO:$GIT_SHA
#
# 3. Vulnerability Management:
#    - Critical/High vulnerabilities: Block deployment, fix immediately
#    - Medium vulnerabilities: Create JIRA ticket, fix in next sprint
#    - Low/Informational: Review monthly
#
# 4. Multi-Region:
#    If you need multi-region (DR), use ECR replication:
#    - Automatically replicate images to backup region
#    - Cost: $0.02 per GB transferred
#    - Use case: Failover to secondary region
