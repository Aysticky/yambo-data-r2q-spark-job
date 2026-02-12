# Airflow DAG configuration
#
# This file contains configuration for deploying Airflow on EKS

# Airflow DAG Notes:
#
# The yambo_data_pipeline.py DAG orchestrates Spark jobs on EKS.
#
# DEPLOYMENT OPTIONS:
#
# Option 1: Managed Workflow for Apache Airflow (MWAA)
# - AWS managed service
# - No infrastructure management
# - Auto scaling
# - Cost: $0.49/hour (~$350/month for small environment)
#
# Option 2: Self-hosted Airflow on EKS
# - Use Helm chart: helm install airflow apache-airflow/airflow
# - More control, lower cost (~$50/month)
# - Requires maintenance
#
# AIRFLOW CONFIGURATION:
#
# 1. Kubernetes Connection:
#    - Go to Admin > Connections
#    - Create "kubernetes_default" connection
#    - Set connection type: Kubernetes
#    - Set in-cluster configuration: true (if Airflow runs in same EKS cluster)
#    - Or provide kubeconfig for external Airflow
#
# 2. Variables:
#    airflow variables set spark_image "ACCOUNT.dkr.ecr.eu-central-1.amazonaws.com/yambo-spark-job:latest"
#    airflow variables set environment "dev"
#    airflow variables set data_lake_bucket "yambo-data-lake-dev"
#
# 3. Secrets:
#    - Use Airflow Secrets Backend to fetch from AWS Secrets Manager
#    - Or use K8s secrets mounted to Airflow pods
#
# INSTALLATION (Self-hosted on EKS):
#
# 1. Add Airflow Helm repo:
#    helm repo add apache-airflow https://airflow.apache.org
#    helm repo update
#
# 2. Create values.yaml (custom configuration):
#    executor: KubernetesExecutor
#    dags:
#      gitSync:
#        enabled: true
#        repo: https://github.com/Aysticky/yambo-data-r2q-spark-job.git
#        branch: develop
#        subPath: dags
#
# 3. Install Airflow:
#    helm install airflow apache-airflow/airflow --namespace airflow --create-namespace -f values.yaml
#
# 4. Access UI:
#    kubectl port-forward svc/airflow-webserver 8080:8080 --namespace airflow
#    Open: http://localhost:8080
#    Default user/pass: admin/admin
#
# ALTERNATIVE: Use EventBridge + Lambda (simpler, serverless)
# - EventBridge schedules Lambda
# - Lambda creates SparkApplication CRD
# - No Airflow needed
# - We already implemented this in terraform/modules/scheduler
#
# RECOMMENDATION:
# For MVP, use EventBridge + Lambda (serverless, simpler).
# Add Airflow later for complex orchestration (dependencies, retries, monitoring).
