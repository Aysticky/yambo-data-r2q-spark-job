output "karpenter_controller_role_arn" {
  description = "Karpenter controller IAM role ARN"
  value       = aws_iam_role.karpenter_controller.arn
}

output "spark_node_pool_name" {
  description = "NodePool name for Spark workloads"
  value       = "spark-workloads"
}

output "system_node_pool_name" {
  description = "NodePool name for system workloads"
  value       = "system-workloads"
}
