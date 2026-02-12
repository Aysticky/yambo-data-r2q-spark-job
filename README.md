# Yambo Data R2Q Spark Pipeline

**Enterprise-grade REST API to S3 data pipeline using Apache Spark on AWS EKS**

## Project Overview

This pipeline demonstrates real-world big data engineering patterns for ingesting millions of transactional records from REST APIs into S3 using Spark on Kubernetes (EKS). Built for Yambo company following enterprise best practices.

### Why This Architecture?

**Spark on EKS vs AWS Glue:**
- **Better cost control**: Pay only for what you use (EKS nodes can scale to zero)
- **More flexibility**: Full control over Spark configurations and versions
- **Containerized**: Easier dependency management and reproducibility
- **Kubernetes-native**: Integrates with existing K8s infrastructure
- **Lower latency**: No Glue startup overhead (~10 min → ~30 sec)

**When to use EKS over Glue:**
- You need specific Spark versions or custom libraries
- You're processing millions of rows hourly (better throughput)
- You want sub-minute job startup (Glue takes 5-10 minutes)
- Cost optimization matters (Glue charges per second with 10-min minimum)
- You need integration with other K8s services (Airflow, monitoring, etc.)

**When Glue is better:**
- Serverless simplicity without K8s management overhead
- Ad-hoc analytics and smaller datasets
- Tight integration with AWS Glue Data Catalog
- Your team doesn't have K8s expertise

## Architecture

```
┌──────────────┐     ┌────────────────┐     ┌─────────────────┐     ┌──────────┐
│   Airflow    │────>│   EKS Cluster  │────>│   Spark Jobs    │────>│ S3 Lake  │
│  (Scheduler) │     │  (Orchestrate) │     │  Driver/Executors│     │ Partitioned│
└──────────────┘     └────────────────┘     └─────────────────┘     └──────────┘
                            ▲                        │
                            │                        ▼
                     ┌──────────────┐      ┌─────────────────┐
                     │  Secrets     │      │  REST API       │
                     │  Manager     │      │  (Stripe/Square)│
                     └──────────────┘      │  OAuth2 + Pages │
                                           └─────────────────┘
                            │                        │
                            ▼                        ▼
                     ┌──────────────┐      ┌─────────────────┐
                     │  CloudWatch  │      │   DynamoDB      │
                     │  Logs/Metrics│      │  (Checkpoints)  │
                     └──────────────┘      └─────────────────┘
```

## Tech Stack

- **Compute**: AWS EKS (Elastic Kubernetes Service)
- **Processing**: Apache Spark 3.5+ with PySpark
- **Storage**: S3 (raw zone, partitioned Parquet)
- **Orchestration**: Apache Airflow (on EKS)
- **IaC**: Terraform
- **CI/CD**: GitHub Actions
- **Build**: Poetry (dependency management)
- **Container**: Docker (Spark driver + executors)
- **Secrets**: AWS Secrets Manager
- **Checkpoints**: DynamoDB
- **Monitoring**: CloudWatch + Prometheus/Grafana

## Data Source: Stripe API (Pattern 1)

We're using **Stripe API** for realistic transactional data ingestion:

### Why Stripe?

1. **Real transactional data**: Payments, charges, refunds, customers
2. **OAuth2 authentication**: Industry-standard token-based auth
3. **Cursor-based pagination**: Handles millions of records efficiently
4. **Rate limiting**: 100 req/sec (realistic constraint to handle)
5. **Incremental extraction**: `created[gte]` parameter for cursor checkpoint
6. **Well-documented**: Official Python SDK and REST API
7. **Enterprise-grade**: Used by millions of businesses

### API Endpoint Example

```python
GET https://api.stripe.com/v1/charges
Authorization: Bearer sk_live_...

Parameters:
- limit: 100 (max per page)
- starting_after: cursor_id (pagination)
- created[gte]: timestamp (incremental)
- created[lte]: timestamp
```

### Alternate APIs (can switch easily)

- **Square API**: Point-of-sale transactions
- **Shopify API**: E-commerce orders
- **Salesforce API**: CRM data
- **HubSpot API**: Marketing/sales data

##  Quick Start

### Prerequisites

```bash
# Required tools
- AWS CLI configured (sanders-platform-dev credentials)
- kubectl (Kubernetes CLI)
- eksctl (EKS management)
- Terraform 1.6+
- Python 3.9+
- Poetry 1.7+
- Docker
```

### Local Development

```bash
# Clone repository
git clone https://github.com/yambo/yambo-data-r2q-spark-job.git
cd yambo-data-r2q-spark-job

# Install dependencies
poetry install

# Run tests
poetry run pytest

# Build Spark Docker image
docker build -t yambo-spark-job:latest -f docker/Dockerfile .

# Run locally (test mode)
poetry run python src/spark_jobs/main.py --mode local --date 2026-02-12
```

## Enterprise Patterns Implemented

### Pattern 1: OAuth2 + Paginated API + Incremental Cursor

1. **Token Management**
   - Store secrets in AWS Secrets Manager
   - Automatic token refresh on 401/expiry
   - Retry with exponential backoff

2. **Pagination Strategy**
   - Cursor-based (not offset) for millions of rows
   - Parallel page fetching with Spark
   - Handle rate limits (429) gracefully

3. **Incremental Extraction**
   - DynamoDB checkpoint: last successful timestamp
   - Resume from failure without reprocessing
   - Idempotent writes with run_id

4. **Data Quality**
   - Schema validation
   - Duplicate detection
   - Completeness checks
   - Automated anomaly detection

## Project Structure

```
yambo-data-r2q-spark-job/
├── README.md
├── pyproject.toml
├── .gitignore
├── .github/
│   └── workflows/
│       ├── ci.yml
│       ├── deploy-dev.yml
│       └── deploy-prod.yml
├── terraform/
│   ├── modules/
│   │   ├── eks/              # EKS cluster, node groups, IRSA
│   │   ├── s3/               # Data lake buckets
│   │   ├── iam/              # Service accounts, roles
│   │   ├── secrets/          # Secrets Manager
│   │   ├── dynamodb/         # Checkpoints table
│   │   ├── ecr/              # Docker registry
│   │   ├── airflow/          # Airflow on EKS
│   │   └── monitoring/       # CloudWatch, Prometheus
│   └── environments/
│       ├── dev/
│       │   ├── main.tf
│       │   ├── variables.tf
│       │   └── outputs.tf
│       └── prod/
│           ├── main.tf
│           ├── variables.tf
│           └── outputs.tf
├── docker/
│   ├── Dockerfile            # Spark job image
│   ├── requirements.txt
│   └── spark-defaults.conf
├── k8s/
│   ├── spark-operator/       # Spark Operator CRDs
│   ├── airflow/              # Airflow Helm values
│   └── service-accounts/     # IRSA configurations
├── src/
│   ├── spark_jobs/
│   │   ├── __init__.py
│   │   ├── main.py           # Spark entry point
│   │   ├── check_job.py      # API availability check
│   │   ├── extract_job.py    # Data extraction
│   │   └── transformations.py
│   ├── api/
│   │   ├── __init__.py
│   │   ├── auth.py           # OAuth2 token management
│   │   ├── client.py         # API client with retries
│   │   └── pagination.py     # Cursor-based pagination
│   ├── checkpoint/
│   │   ├── __init__.py
│   │   └── manager.py        # DynamoDB checkpoint logic
│   └── utils/
│       ├── __init__.py
│       ├── config.py
│       ├── logging_setup.py
│       └── s3_utils.py
├── airflow/
│   └── dags/
│       └── yambo_ingestion_dag.py
├── tests/
│   ├── unit/
│   ├── integration/
│   └── conftest.py
└── docs/
    ├── ARCHITECTURE.md
    ├── DEPLOYMENT.md
    ├── MONITORING.md
    ├── TROUBLESHOOTING.md
    └── RUNBOOK.md
```

## Security Best Practices

1. **IRSA (IAM Roles for Service Accounts)**: No hardcoded credentials
2. **Secrets Manager**: API keys, tokens encrypted at rest
3. **VPC**: Private subnets for EKS nodes
4. **Encryption**: S3 SSE-KMS, DynamoDB encryption
5. **Least privilege**: Scoped IAM policies per service

## Cost Optimization

| Resource | Dev | Prod | Monthly Cost (est.) |
|----------|-----|------|---------------------|
| EKS Control Plane | $72 | $72 | $144 |
| EKS Nodes (r5.2xlarge) | 2 runs/day × 30 min | 2 runs/day × 30 min | $45 (on-demand) / $14 (spot) |
| S3 Storage (1TB) | - | - | $23 |
| DynamoDB (on-demand) | - | - | $10 |
| Airflow (t3.medium) | 1 | 2 | $50 |
| ECR, Secrets, Logs | - | - | $10-20 |
| **Total (spot)** | **~$115/mo** | **~$190/mo** | **~$305/mo** |
| **Total (on-demand)** | **~$145/mo** | **~$220/mo** | **~$365/mo** |

**Cost-saving strategies:**
- Use Spot instances for Spark executors (70% cheaper) - **RECOMMENDED**
- Run jobs 2x/day instead of continuous (30 min per run vs 24/7)
- Scale EKS nodes to zero during off-hours (dev)
- Use S3 Intelligent-Tiering for older data
- Right-size executor memory/CPU based on data volume

## Monitoring & Alerts

### Key Metrics to Watch

1. **Job Success Rate** (target: >99%)
2. **API Rate Limit Hits** (should be <1% of requests)
3. **Data Freshness** (lag should be <2 hours)
4. **Executor OOM Errors** (should be 0)
5. **S3 Write Throughput** (throttling indicates partitioning issue)
6. **DynamoDB Checkpoint Update Lag**

### Common Issues & Resolution

See [TROUBLESHOOTING.md](docs/TROUBLESHOOTING.md) for detailed runbook.

## Learning Objectives

This project teaches you:

-Building production-grade Spark jobs on Kubernetes  
-REST API best practices (OAuth2, pagination, rate limiting)  
-Terraform for complex AWS infrastructure (EKS, IRSA, etc.)  
-Docker multi-stage builds for Spark  
-Airflow DAG development and scheduling  
-Incremental data extraction patterns  
-Data quality and monitoring at scale  
-Cost optimization for big data workloads  
-GitOps and CI/CD for data pipelines  
-Debugging distributed systems (Spark on K8s)  

##  License

MIT

## Contributors

Platform Team - Yambo Data Engineering