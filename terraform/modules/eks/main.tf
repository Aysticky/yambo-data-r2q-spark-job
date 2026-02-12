# EKS Module for Kubernetes Cluster
#
# COMPLEX MODULE - This provisions a complete EKS cluster with:
# - Control plane (managed by AWS)
# - Worker node groups (EC2 instances)
# - IRSA (IAM Roles for Service Accounts)
# - VPC and networking
# - Add-ons (CoreDNS, kube-proxy, vpc-cni)
#
# This is a foundational module - will be expanded in next iteration
# with full node group configurations, autoscaling, and IRSA setup.
#
# NOTE: EKS is expensive (~$72/month for control plane alone)
# + node costs. Consider using eksctl for initial prototyping,
# then move to Terraform for production.

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

variable "cluster_version" {
  description = "EKS cluster version"
  type        = string
  default     = "1.28"
}

# Placeholder for EKS cluster resource
# Full implementation will include:
# - aws_eks_cluster
# - aws_eks_node_group
# - aws_iam_role (for cluster and nodes)
# - aws_eks_addon (CoreDNS, kube-proxy, etc.)
# - IRSA setup
# - Security groups

# Outputs (placeholders for now)
output "cluster_name" {
  description = "EKS cluster name"
  value       = "${var.project_name}-${var.environment}-eks"
}

output "cluster_endpoint" {
  description = "EKS cluster endpoint"
  value       = "https://placeholder.eks.amazonaws.com"
}

output "cluster_certificate_authority_data" {
  description = "EKS cluster certificate authority data"
  value       = "placeholder"
  sensitive   = true
}

# IMPLEMENTATION NOTE:
# Full EKS setup is complex (200+ lines). We'll expand this module
# in the next phase with complete node groups, networking, and IRSA.
# For now, this provides the structure for the environments to reference.
