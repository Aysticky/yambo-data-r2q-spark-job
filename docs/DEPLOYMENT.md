# Terraform Deployment Guide

## Overview

This repository uses Terraform to provision AWS infrastructure for the Yambo Spark data pipeline. Infrastructure is organized into modules and deployed per environment (dev/prod).

## Prerequisites

1. **AWS CLI configured** with `sanders-platform-dev` credentials:
   ```bash
   aws configure --profile sanders-platform-dev
   # Enter the access key and secret from ~/.aws/credentials
   ```

2. **Terraform 1.6+** installed:
   ```bash
   terraform --version
   ```

3. **Create S3 backend** (one-time setup per environment):
   ```bash
   # Dev backend
   aws s3api create-bucket \
     --bucket yambo-terraform-state-dev \
     --region eu-central-1 \
     --create-bucket-configuration LocationConstraint=eu-central-1 \
     --profile sanders-platform-dev
   
   aws s3api put-bucket-versioning \
     --bucket yambo-terraform-state-dev \
     --versioning-configuration Status=Enabled \
     --profile sanders-platform-dev
   
   aws dynamodb create-table \
     --table-name yambo-terraform-locks-dev \
     --attribute-definitions AttributeName=LockID,AttributeType=S \
     --key-schema AttributeName=LockID,KeyType=HASH \
     --provisioned-throughput ReadCapacityUnits=5,WriteCapacityUnits=5 \
     --region eu-central-1 \
     --profile sanders-platform-dev
   
   # Prod backend (similar commands with -prod suffix)
   ```

## Deployment Steps

### Deploy to Dev

```bash
# Navigate to dev environment
cd terraform/environments/dev

# Initialize Terraform
terraform init

# Review changes
terraform plan

# Apply changes
terraform apply

# After deployment, note the outputs
terraform output
```

### Deploy to Prod

```bash
# Navigate to prod environment
cd terraform/environments/prod

# Initialize Terraform
terraform init

# Review changes carefully (this is production!)
terraform plan

# Apply with confirmation
terraform apply

# Save outputs
terraform output -json > outputs.json
```

## Module Structure

```
terraform/
├── modules/
│   ├── s3/         # Data lake buckets with lifecycle policies
│   ├── dynamodb/   # Checkpoint tables
│   ├── ecr/        # Docker image registry
│   ├── secrets/    # API credentials storage
│   └── eks/        # Kubernetes cluster (to be expanded)
└── environments/
    ├── dev/        # Development environment
    └── prod/       # Production environment
```

## Post-Deployment Tasks

### 1. Update Secrets Manager

After deployment, update API credentials in Secrets Manager:

```bash
# Update Stripe API secret
aws secretsmanager put-secret-value \
  --secret-id yambo/dev/stripe-api \
  --secret-string '{
    "api_key": "sk_test_your_actual_key_here",
    "api_url": "https://api.stripe.com/v1",
    "environment": "development"
  }' \
  --profile sanders-platform-dev
```

### 2. Verify Resource Creation

```bash
# Check S3 buckets
aws s3 ls | grep yambo-dev

# Check DynamoDB tables
aws dynamodb list-tables | grep yambo-dev

# Check ECR repositories
aws ecr describe-repositories --repository-names yambo-dev-spark-job

# Check secrets
aws secretsmanager list-secrets | grep yambo-dev
```

## Common Issues

### Issue: Backend S3 bucket doesn't exist

**Error**: `Error: Failed to get existing workspaces: S3 bucket does not exist.`

**Solution**: Create the S3 backend bucket first (see Prerequisites section).

### Issue: DynamoDB lock table error

**Error**: `Error acquiring the state lock`

**Solution**: 
```bash
# List locks
aws dynamodb scan --table-name yambo-terraform-locks-dev

# If stuck, manually delete lock (CAREFUL!)
aws dynamodb delete-item \
  --table-name yambo-terraform-locks-dev \
  --key '{"LockID": {"S": "yambo-terraform-state-dev/spark-pipeline/terraform.tfstate"}}'
```

### Issue: Insufficient IAM permissions

**Error**: `AccessDenied` or `UnauthorizedOperation`

**Solution**: Verify `sanders-platform-dev` IAM user has admin privileges or required policies.

## Cost Estimates

### Development Environment
- S3 storage (1GB): ~$0.02/month
- DynamoDB (on-demand): ~$0.10/month
- Secrets Manager (2 secrets): ~$0.80/month
- ECR (images <10GB): ~$1.00/month
- **EKS (when deployed)**: ~$72/month (control plane)
- **EKS nodes (when deployed)**: ~$150-300/month (t3.xlarge x2-3)
- **Total**: ~$224-374/month (mostly EKS)

### Production Environment
- Similar costs but with higher storage and node capacity
- **Estimated**: ~$500-800/month

## Cleanup

To destroy infrastructure (development only):

```bash
cd terraform/environments/dev

# Review what will be deleted
terraform plan -destroy

# Delete (CAREFUL!)
terraform destroy
```

**WARNING**: Never run `terraform destroy` in production without approval!

## Next Steps

1. Deploy base infrastructure (S3, DynamoDB, ECR, Secrets Manager)
2. Update secrets with actual API credentials
3. Build and push Docker images to ECR
4. Deploy EKS cluster (Phase 2)
5. Deploy Spark Operator and Airflow (Phase 2)
6. Deploy data pipeline jobs (Phase 3)

## Support

For issues or questions:
- Check CloudFormation events in AWS Console
- Review Terraform state: `terraform show`
- Check module documentation in `modules/*/main.tf`
