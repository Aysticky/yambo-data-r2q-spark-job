"""
EventBridge Scheduler Terraform Module

This module creates EventBridge rules to schedule Spark jobs.

SCHEDULING STRATEGY:
- 2 runs per day (every 12 hours) to reduce costs
- Run at off-peak hours (e.g., 2 AM and 2 PM UTC)
- Use cron expressions for precise timing

COST OPTIMIZATION:
Running 2x/day instead of continuous processing:
- Reduces compute costs (shorter runtime)
- Batches API calls (more efficient)
- Still provides near-real-time data (12-hour latency max)

Cost comparison:
- Continuous: ~$45/month (1 hour/day runtime)
- 2x/day: ~$30/month (30 min per run × 2 = 1 hour total)
- Both process same data, but 2x/day uses resources more efficiently
"""

# EventBridge rule to trigger check job (runs before extract)
resource "aws_cloudwatch_event_rule" "check_job_schedule" {
  name                = "${var.project_name}-check-job-${var.environment}"
  description         = "Schedule check job 2 times per day"
  
  # Run at 1:50 AM and 1:50 PM UTC (10 min before extract job)
  schedule_expression = "cron(50 1,13 * * ? *)"
  
  tags = merge(
    var.common_tags,
    {
      Name = "${var.project_name}-check-job-schedule-${var.environment}"
      JobType = "check"
    }
  )
}

# EventBridge target: Trigger check SparkApplication
resource "aws_cloudwatch_event_target" "check_job_target" {
  rule      = aws_cloudwatch_event_rule.check_job_schedule.name
  target_id = "check-job-trigger"
  arn       = aws_lambda_function.spark_job_trigger.arn
  
  input = jsonencode({
    jobType     = "check"
    environment = var.environment
    sparkApplication = "yambo-check-job"
    namespace = "spark-jobs"
  })
}

# EventBridge rule to trigger extract job (main data extraction)
resource "aws_cloudwatch_event_rule" "extract_job_schedule" {
  name                = "${var.project_name}-extract-job-${var.environment}"
  description         = "Schedule extract job 2 times per day"
  
  # Run at 2:00 AM and 2:00 PM UTC
  # This gives check job 10 minutes to complete first
  schedule_expression = "cron(0 2,14 * * ? *)"
  
  tags = merge(
    var.common_tags,
    {
      Name = "${var.project_name}-extract-job-schedule-${var.environment}"
      JobType = "extract"
    }
  )
}

# EventBridge target: Trigger extract SparkApplication
resource "aws_cloudwatch_event_target" "extract_job_target" {
  rule      = aws_cloudwatch_event_rule.extract_job_schedule.name
  target_id = "extract-job-trigger"
  arn       = aws_lambda_function.spark_job_trigger.arn
  
  input = jsonencode({
    jobType     = "extract"
    environment = var.environment
    sparkApplication = "yambo-extract-job"
    namespace = "spark-jobs"
  })
}

# Lambda function to trigger Spark jobs via K8s API
# This Lambda applies SparkApplication CRDs to EKS cluster
resource "aws_lambda_function" "spark_job_trigger" {
  function_name = "${var.project_name}-spark-trigger-${var.environment}"
  description   = "Trigger Spark jobs on EKS via Kubernetes API"
  
  # Lambda deployment package (created separately)
  filename      = "${path.module}/lambda/spark_trigger.zip"
  source_code_hash = filebase64sha256("${path.module}/lambda/spark_trigger.zip")
  
  handler = "lambda_function.lambda_handler"
  runtime = "python3.11"
  timeout = 60
  
  role = aws_iam_role.lambda_spark_trigger.arn
  
  environment {
    variables = {
      EKS_CLUSTER_NAME = var.eks_cluster_name
      AWS_REGION       = var.aws_region
    }
  }
  
  tags = merge(
    var.common_tags,
    {
      Name = "${var.project_name}-spark-trigger-${var.environment}"
    }
  )
}

# IAM role for Lambda
resource "aws_iam_role" "lambda_spark_trigger" {
  name = "${var.project_name}-lambda-spark-trigger-${var.environment}"
  
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
  
  tags = var.common_tags
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
      }
    ]
  })
}

# IAM policy: CloudWatch Logs
resource "aws_iam_role_policy_attachment" "lambda_logs" {
  role       = aws_iam_role.lambda_spark_trigger.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

# Lambda permission for EventBridge to invoke
resource "aws_lambda_permission" "allow_eventbridge_check" {
  statement_id  = "AllowExecutionFromEventBridgeCheck"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.spark_job_trigger.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.check_job_schedule.arn
}

resource "aws_lambda_permission" "allow_eventbridge_extract" {
  statement_id  = "AllowExecutionFromEventBridgeExtract"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.spark_job_trigger.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.extract_job_schedule.arn
}

# CloudWatch log group for Lambda
resource "aws_cloudwatch_log_group" "lambda_spark_trigger" {
  name              = "/aws/lambda/${aws_lambda_function.spark_job_trigger.function_name}"
  retention_in_days = var.log_retention_days
  
  tags = var.common_tags
}

# PRODUCTION NOTES:
#
# SCHEDULING BEST PRACTICES:
# 1. Run check job 10 minutes before extract (validation first)
# 2. Use cron expressions for precise timing (not rate expressions)
# 3. Schedule during off-peak hours (less API traffic)
# 4. Add jitter to avoid thundering herd (if multiple pipelines)
#
# CRON EXPRESSION FORMAT:
# cron(minute hour day month day-of-week year)
# - minute: 0-59
# - hour: 0-23 (UTC)
# - day: 1-31
# - month: 1-12 or JAN-DEC
# - day-of-week: 1-7 or SUN-SAT
# - year: optional
#
# Examples:
# - Every 12 hours: cron(0 0,12 * * ? *)
# - Every 6 hours: cron(0 0,6,12,18 * * ? *)
# - Weekdays only: cron(0 2,14 ? * MON-FRI *)
# - Business hours: cron(0 9-17 ? * MON-FRI *)
#
# WHY 2 RUNS PER DAY:
# - Balance between freshness and cost
# - 12-hour data latency acceptable for most analytics
# - Reduces API calls (batch processing)
# - Lower compute costs (no idle time)
#
# ALTERNATIVE: Use EventBridge Scheduler (newer service)
# EventBridge Scheduler offers:
# - More flexible scheduling (one-time, recurring)
# - Timezone support (no need to convert to UTC)
# - Better error handling
# - Direct target support (no Lambda needed)
#
# Migration to EventBridge Scheduler:
# resource "aws_scheduler_schedule" "extract_job" {
#   name = "yambo-extract-job-${var.environment}"
#   
#   schedule_expression = "cron(0 2,14 * * ? *)"
#   schedule_expression_timezone = "Europe/Berlin"
#   
#   flexible_time_window {
#     mode = "OFF"
#   }
#   
#   target {
#     arn      = var.eks_cluster_arn
#     role_arn = aws_iam_role.scheduler_role.arn
#     
#     input = jsonencode({
#       sparkApplication = "yambo-extract-job"
#     })
#   }
# }
