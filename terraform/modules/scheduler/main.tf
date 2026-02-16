# EventBridge Scheduler Terraform Module
#
# This module creates Lambda + EventBridge for automatic Spark job scheduling
#
# ENTERPRISE SCHEDULING STRATEGY:
# - DEV: Once daily at 2 AM UTC (testing and validation)
# - PROD: Twice daily at 2 AM and 2 PM UTC (regular data updates)
# - Uses Lambda to trigger Spark jobs via kubectl
# - Fully audited with CloudWatch logs and metrics

terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    kubectl = {
      source  = "alekc/kubectl"
      version = "~> 2.0"
    }
  }
}

locals {
  function_name   = "${var.project_name}-spark-trigger-${var.environment}"
  manifest_bucket = "${var.project_name}-${var.environment}-manifests"

  common_tags = merge(
    var.common_tags,
    {
      Component = "Scheduler"
      ManagedBy = "Terraform"
    }
  )
}

# Get current AWS account ID
data "aws_caller_identity" "current" {}

# S3 bucket for storing Kubernetes manifests
resource "aws_s3_bucket" "manifests" {
  bucket = local.manifest_bucket

  tags = merge(
    local.common_tags,
    {
      Name = "Spark Job Manifests"
    }
  )
}

resource "aws_s3_bucket_versioning" "manifests" {
  bucket = aws_s3_bucket.manifests.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "manifests" {
  bucket = aws_s3_bucket.manifests.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# Upload extract job manifest to S3
resource "aws_s3_object" "extract_manifest" {
  bucket = aws_s3_bucket.manifests.id
  key    = "spark-jobs/yambo-extract-job.yaml"
  source = "${path.root}/../../../k8s/spark-application-extract.yaml"

  etag = filemd5("${path.root}/../../../k8s/spark-application-extract.yaml")

  tags = local.common_tags
}

# Lambda execution role
resource "aws_iam_role" "lambda_spark_trigger" {
  name = "${local.function_name}-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
      }
    ]
  })

  tags = local.common_tags
}

# IAM policy: EKS access
resource "aws_iam_role_policy" "lambda_eks_access" {
  name = "eks-access"
  role = aws_iam_role.lambda_spark_trigger.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "eks:DescribeCluster",
          "eks:ListClusters"
        ]
        Resource = var.eks_cluster_arn
      },
      {
        Effect = "Allow"
        Action = [
          "sts:GetCallerIdentity"
        ]
        Resource = "*"
      }
    ]
  })
}

# IAM policy: S3 access to manifests bucket
resource "aws_iam_role_policy" "lambda_s3_access" {
  name = "s3-manifests-access"
  role = aws_iam_role.lambda_spark_trigger.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:ListBucket"
        ]
        Resource = [
          aws_s3_bucket.manifests.arn,
          "${aws_s3_bucket.manifests.arn}/*"
        ]
      }
    ]
  })
}

# IAM policy: CloudWatch Logs
resource "aws_iam_role_policy_attachment" "lambda_logs" {
  role       = aws_iam_role.lambda_spark_trigger.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

# CloudWatch log group for Lambda
resource "aws_cloudwatch_log_group" "lambda_spark_trigger" {
  name              = "/aws/lambda/${local.function_name}"
  retention_in_days = var.log_retention_days

  tags = local.common_tags
}

# Lambda function
resource "aws_lambda_function" "spark_job_trigger" {
  function_name = local.function_name
  role          = aws_iam_role.lambda_spark_trigger.arn

  filename         = "${path.module}/lambda_function.zip"
  source_code_hash = fileexists("${path.module}/lambda_function.zip") ? filebase64sha256("${path.module}/lambda_function.zip") : null

  handler     = "handler.lambda_handler"
  runtime     = "python3.9"
  timeout     = 300 # 5 minutes
  memory_size = 512

  environment {
    variables = {
      EKS_CLUSTER_NAME = var.eks_cluster_name
      NAMESPACE        = "spark-jobs"
      JOB_NAME         = "yambo-extract-job"
      ENVIRONMENT      = var.environment
    }
  }

  depends_on = [
    aws_cloudwatch_log_group.lambda_spark_trigger,
    aws_iam_role_policy.lambda_eks_access,
    aws_iam_role_policy.lambda_s3_access,
    aws_iam_role_policy_attachment.lambda_logs
  ]

  tags = merge(
    local.common_tags,
    {
      Name = "Spark Job Trigger"
    }
  )
}

# EventBridge rule: Extract job schedule
resource "aws_cloudwatch_event_rule" "extract_job_schedule" {
  name        = "${var.project_name}-extract-job-${var.environment}"
  description = "Trigger Spark extract job on schedule"

  # Dev: Once daily at 2 AM UTC
  # Prod: Twice daily at 2 AM and 2 PM UTC
  schedule_expression = var.environment == "prod" ? "cron(0 2,14 * * ? *)" : "cron(0 2 * * ? *)"

  tags = local.common_tags
}

# EventBridge target: Lambda function
resource "aws_cloudwatch_event_target" "lambda_target" {
  rule      = aws_cloudwatch_event_rule.extract_job_schedule.name
  target_id = "TriggerLambda"
  arn       = aws_lambda_function.spark_job_trigger.arn

  input = jsonencode({
    job_name = "yambo-extract-job"
    source   = "eventbridge-schedule"
  })
}

# Lambda permission for EventBridge
resource "aws_lambda_permission" "allow_eventbridge_extract" {
  statement_id  = "AllowExecutionFromEventBridgeExtract"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.spark_job_trigger.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.extract_job_schedule.arn
}

# CloudWatch Alarm: Lambda errors
resource "aws_cloudwatch_metric_alarm" "lambda_errors" {
  alarm_name          = "${local.function_name}-errors"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "Errors"
  namespace           = "AWS/Lambda"
  period              = 300
  statistic           = "Sum"
  threshold           = 0
  alarm_description   = "Alert when Lambda function has errors"

  dimensions = {
    FunctionName = aws_lambda_function.spark_job_trigger.function_name
  }

  tags = local.common_tags
}
# ==============================================================================
# Kubernetes RBAC for Lambda IAM Role
# ==============================================================================

# Get current aws-auth ConfigMap
data "kubectl_file_documents" "aws_auth" {
  content = <<-EOT
    apiVersion: v1
    kind: ConfigMap
    metadata:
      name: aws-auth
      namespace: kube-system
    data:
      mapRoles: |
        - rolearn: ${var.node_instance_role_arn}
          groups:
          - system:bootstrappers
          - system:nodes
          username: system:node:{{EC2PrivateDNSName}}
        - rolearn: ${aws_iam_role.lambda_spark_trigger.arn}
          groups:
          - spark-job-managers
          username: lambda-spark-trigger
      mapUsers: |
        - userarn: arn:aws:sts::${data.aws_caller_identity.current.account_id}:assumed-role/${aws_iam_role.lambda_spark_trigger.name}/${local.function_name}
          groups:
          - spark-job-managers
          username: lambda-spark-trigger-session
  EOT
}

# Update aws-auth ConfigMap to include Lambda role
resource "kubectl_manifest" "aws_auth" {
  yaml_body = data.kubectl_file_documents.aws_auth.documents[0]

  depends_on = [aws_lambda_function.spark_job_trigger]
}

# ClusterRole: Manage Spark applications
resource "kubectl_manifest" "spark_job_manager_role" {
  yaml_body = <<-EOT
    apiVersion: rbac.authorization.k8s.io/v1
    kind: ClusterRole
    metadata:
      name: spark-job-manager
    rules:
    - apiGroups: ["sparkoperator.k8s.io"]
      resources: ["sparkapplications"]
      verbs: ["get", "list", "watch", "create", "update", "patch", "delete"]
    - apiGroups: [""]
      resources: ["pods", "services", "configmaps"]
      verbs: ["get", "list", "watch"]
    - apiGroups: [""]
      resources: ["pods/log"]
      verbs: ["get", "list"]
  EOT

  depends_on = [kubectl_manifest.aws_auth]
}

# ClusterRoleBinding: Bind Lambda IAM role to ClusterRole
resource "kubectl_manifest" "spark_job_manager_binding" {
  yaml_body = <<-EOT
    apiVersion: rbac.authorization.k8s.io/v1
    kind: ClusterRoleBinding
    metadata:
      name: spark-job-manager-binding
    roleRef:
      apiGroup: rbac.authorization.k8s.io
      kind: ClusterRole
      name: spark-job-manager
    subjects:
    - apiGroup: rbac.authorization.k8s.io
      kind: Group
      name: spark-job-managers
  EOT

  depends_on = [kubectl_manifest.spark_job_manager_role]
}