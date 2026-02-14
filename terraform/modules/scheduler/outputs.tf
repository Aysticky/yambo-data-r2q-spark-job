output "extract_job_schedule_arn" {
  description = "ARN of extract job EventBridge rule"
  value       = aws_cloudwatch_event_rule.extract_job_schedule.arn
}

output "lambda_trigger_function_name" {
  description = "Name of Lambda trigger function"
  value       = aws_lambda_function.spark_job_trigger.function_name
}

output "lambda_trigger_function_arn" {
  description = "ARN of Lambda trigger function"
  value       = aws_lambda_function.spark_job_trigger.arn
}

output "manifest_bucket_name" {
  description = "S3 bucket for Kubernetes manifests"
  value       = aws_s3_bucket.manifests.id
}
