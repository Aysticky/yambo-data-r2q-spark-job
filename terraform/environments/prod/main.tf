# Production Environment Configuration
#
# PURPOSE: Production environment for live data processing
# - Higher capacity and redundancy
# - Longer retention periods
# - Enhanced monitoring and alerting
# - Stricter security controls

terraform {
  required_version = ">= 1.6"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  # Backend configuration for state management
  backend "s3" {
    bucket         = "yambo-terraform-state-prod"
    key            = "spark-pipeline/terraform.tfstate"
    region         = "eu-central-1"
    encrypt        = true
    dynamodb_table = "yambo-terraform-locks-prod"
  }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project     = "Yambo Data Pipeline"
      Environment = "prod"
      ManagedBy   = "Terraform"
      Repository  = "yambo-data-r2q-spark-job"
      CostCenter  = "DataEngineering"
    }
  }
}

data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

locals {
  project_name   = "yambo"
  environment    = "prod"
  aws_account_id = data.aws_caller_identity.current.account_id
  aws_region     = data.aws_region.current.name

  common_tags = {
    Project     = "Yambo Data Pipeline"
    Environment = "prod"
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
  cluster_version = "1.28"
  common_tags     = local.common_tags
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
