# Development Environment Configuration
#
# PURPOSE: Dev environment for testing and development
# - Lower capacity (cost optimization)
# - Shorter retention periods
# - On-demand billing where possible
# - Can be destroyed and recreated frequently

terraform {
  required_version = ">= 1.6"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  # Backend configuration for state management
  # IMPORTANT: Create this bucket manually before running terraform init
  # aws s3api create-bucket --bucket yambo-terraform-state-dev --region eu-central-1 --create-bucket-configuration LocationConstraint=eu-central-1
  # aws dynamodb create-table --table-name yambo-terraform-locks-dev --attribute-definitions AttributeName=LockID,AttributeType=S --key-schema AttributeName=LockID,KeyType=HASH --provisioned-throughput ReadCapacityUnits=5,WriteCapacityUnits=5
  backend "s3" {
    bucket         = "yambo-terraform-state-dev"
    key            = "spark-pipeline/terraform.tfstate"
    region         = "eu-central-1"
    encrypt        = true
    dynamodb_table = "yambo-terraform-locks-dev"
  }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project     = "YamboDataPipeline"
      Environment = "dev"
      ManagedBy   = "Terraform"
      Repository  = "yambo-data-r2q-spark-job"
      CostCenter  = "DataEngineering"
    }
  }
}

# Get current AWS account info
data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

locals {
  project_name   = "yambo"
  environment    = "dev"
  aws_account_id = data.aws_caller_identity.current.account_id
  aws_region     = data.aws_region.current.name

  common_tags = {
    Project     = "YamboDataPipeline"
    Environment = "dev"
    ManagedBy   = "Terraform"
  }
}

# S3 Module
module "s3" {
  source = "../../modules/s3"

  project_name = local.project_name
  environment  = local.environment
  common_tags  = local.common_tags
}

# DynamoDB Module
module "dynamodb" {
  source = "../../modules/dynamodb"

  project_name = local.project_name
  environment  = local.environment
  common_tags  = local.common_tags
}

# ECR Module
module "ecr" {
  source = "../../modules/ecr"

  project_name = local.project_name
  environment  = local.environment
  common_tags  = local.common_tags
}

# Secrets Manager Module
module "secrets" {
  source = "../../modules/secrets"

  project_name = local.project_name
  environment  = local.environment
  common_tags  = local.common_tags
}

# EKS Module (placeholder for now)
module "eks" {
  source = "../../modules/eks"

  project_name    = local.project_name
  environment     = local.environment
  aws_region      = var.aws_region
  cluster_version = "1.29" # Incremental upgrade: 1.28 -> 1.29 (AWS requires step-by-step)
  common_tags     = local.common_tags
}

# Automated Spark job scheduler
module "scheduler" {
  source = "../../modules/scheduler"

  project_name     = local.project_name
  environment      = local.environment
  aws_region       = var.aws_region
  eks_cluster_name = module.eks.cluster_name
  eks_cluster_arn  = module.eks.cluster_arn
  common_tags      = local.common_tags
}

# Outputs
output "data_lake_bucket" {
  description = "Data lake S3 bucket name"
  value       = module.s3.data_lake_bucket_name
}

output "spark_logs_bucket" {
  description = "Spark logs S3 bucket name"
  value       = module.s3.spark_logs_bucket_name
}

output "checkpoints_table" {
  description = "DynamoDB checkpoints table name"
  value       = module.dynamodb.checkpoints_table_name
}

output "job_metadata_table" {
  description = "DynamoDB job metadata table name"
  value       = module.dynamodb.job_metadata_table_name
}

output "spark_job_ecr_repo" {
  description = "Spark job ECR repository URL"
  value       = module.ecr.spark_job_repository_url
}

output "stripe_api_secret_name" {
  description = "Stripe API secret name"
  value       = module.secrets.stripe_api_secret_name
}

output "eks_cluster_name" {
  description = "EKS cluster name"
  value       = module.eks.cluster_name
}

output "spark_job_irsa_role_arn" {
  description = "IAM role ARN for Spark jobs (IRSA)"
  value       = module.eks.spark_job_irsa_role_arn
}

output "scheduler_lambda_function" {
  description = "Lambda function name for Spark job trigger"
  value       = module.scheduler.lambda_trigger_function_name
}

output "scheduler_manifest_bucket" {
  description = "S3 bucket for Kubernetes manifests"
  value       = module.scheduler.manifest_bucket_name
}

output "scheduler_cron_rule" {
  description = "EventBridge schedule rule ARN"
  value       = module.scheduler.extract_job_schedule_arn
}
