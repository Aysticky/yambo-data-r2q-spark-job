"""
Lambda function to trigger Spark jobs on EKS.

This function is invoked by EventBridge on a schedule and triggers
the Spark extract job by applying the Kubernetes manifest.

Environment Variables:
    EKS_CLUSTER_NAME: Name of the EKS cluster
    AWS_REGION: AWS region
    NAMESPACE: Kubernetes namespace for Spark jobs
    JOB_NAME: Name of the SparkApplication to trigger
    ENVIRONMENT: dev or prod
"""

import json
import logging
import os
import subprocess
import tempfile
from datetime import datetime

import boto3

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# AWS clients
eks_client = boto3.client("eks")
s3_client = boto3.client("s3")


def get_kube_config(cluster_name: str, region: str) -> str:
    """
    Generate kubeconfig for the EKS cluster.
    
    Args:
        cluster_name: Name of the EKS cluster
        region: AWS region
        
    Returns:
        Path to the kubeconfig file
    """
    # Get cluster details
    cluster_info = eks_client.describe_cluster(name=cluster_name)
    cluster_endpoint = cluster_info["cluster"]["endpoint"]
    cluster_ca = cluster_info["cluster"]["certificateAuthority"]["data"]
    
    # Create kubeconfig
    kubeconfig = {
        "apiVersion": "v1",
        "kind": "Config",
        "clusters": [
            {
                "name": cluster_name,
                "cluster": {
                    "server": cluster_endpoint,
                    "certificate-authority-data": cluster_ca,
                },
            }
        ],
        "contexts": [
            {
                "name": cluster_name,
                "context": {"cluster": cluster_name, "user": "lambda"},
            }
        ],
        "current-context": cluster_name,
        "users": [
            {
                "name": "lambda",
                "user": {
                    "exec": {
                        "apiVersion": "client.authentication.k8s.io/v1beta1",
                        "command": "aws",
                        "args": [
                            "eks",
                            "get-token",
                            "--cluster-name",
                            cluster_name,
                            "--region",
                            region,
                        ],
                    }
                },
            }
        ],
    }
    
    # Write kubeconfig to temp file
    kubeconfig_path = "/tmp/kubeconfig"
    with open(kubeconfig_path, "w") as f:
        json.dump(kubeconfig, f)
    
    return kubeconfig_path


def trigger_spark_job(
    cluster_name: str,
    region: str,
    namespace: str,
    job_name: str,
    environment: str,
) -> dict:
    """
    Trigger Spark job by deleting and recreating the SparkApplication.
    
    Args:
        cluster_name: Name of the EKS cluster
        region: AWS region
        namespace: Kubernetes namespace
        job_name: Name of the SparkApplication
        environment: dev or prod
        
    Returns:
        Result dictionary with status and message
    """
    try:
        # Get kubeconfig
        kubeconfig_path = get_kube_config(cluster_name, region)
        
        # Delete existing job (if exists)
        logger.info(f"Deleting existing job: {job_name}")
        delete_cmd = [
            "kubectl",
            "delete",
            "sparkapplication",
            job_name,
            "-n",
            namespace,
            "--kubeconfig",
            kubeconfig_path,
            "--ignore-not-found=true",
        ]
        
        result = subprocess.run(
            delete_cmd,
            capture_output=True,
            text=True,
            timeout=30,
        )
        
        if result.returncode != 0 and "not found" not in result.stderr.lower():
            logger.warning(f"Delete warning: {result.stderr}")
        
        # Wait a moment for cleanup
        import time
        time.sleep(5)
        
        # Apply the manifest to create new job
        logger.info(f"Creating new job: {job_name}")
        
        # For Lambda, we need to download the manifest from S3 or embed it
        # For now, we'll use kubectl apply with inline YAML
        manifest_path = f"/tmp/{job_name}.yaml"
        
        # Download manifest from S3 (manifests are stored in deployment bucket)
        manifest_bucket = f"yambo-{environment}-manifests"
        manifest_key = f"spark-jobs/{job_name}.yaml"
        
        try:
            s3_client.download_file(manifest_bucket, manifest_key, manifest_path)
        except Exception as e:
            logger.error(f"Failed to download manifest from S3: {e}")
            raise
        
        # Apply the manifest
        apply_cmd = [
            "kubectl",
            "apply",
            "-f",
            manifest_path,
            "--kubeconfig",
            kubeconfig_path,
        ]
        
        result = subprocess.run(
            apply_cmd,
            capture_output=True,
            text=True,
            timeout=30,
        )
        
        if result.returncode != 0:
            logger.error(f"kubectl apply failed: {result.stderr}")
            raise Exception(f"Failed to apply manifest: {result.stderr}")
        
        logger.info(f"Successfully triggered job: {job_name}")
        logger.info(f"kubectl output: {result.stdout}")
        
        return {
            "status": "success",
            "message": f"Job {job_name} triggered successfully",
            "timestamp": datetime.utcnow().isoformat(),
            "output": result.stdout,
        }
        
    except subprocess.TimeoutExpired:
        logger.error("kubectl command timed out")
        raise Exception("kubectl command timed out")
    except Exception as e:
        logger.error(f"Error triggering job: {str(e)}")
        raise


def lambda_handler(event, context):
    """
    Lambda handler function.
    
    Args:
        event: EventBridge event or manual trigger
        context: Lambda context
        
    Returns:
        Response with execution status
    """
    try:
        # Get configuration from environment
        cluster_name = os.environ.get("EKS_CLUSTER_NAME")
        region = os.environ.get("AWS_REGION", "eu-central-1")
        namespace = os.environ.get("NAMESPACE", "spark-jobs")
        job_name = os.environ.get("JOB_NAME", "yambo-extract-job")
        environment = os.environ.get("ENVIRONMENT", "dev")
        
        logger.info(f"Lambda triggered for {environment} environment")
        logger.info(f"Event: {json.dumps(event)}")
        logger.info(
            f"Triggering job: {job_name} in cluster: {cluster_name}, namespace: {namespace}"
        )
        
        # Trigger the Spark job
        result = trigger_spark_job(
            cluster_name=cluster_name,
            region=region,
            namespace=namespace,
            job_name=job_name,
            environment=environment,
        )
        
        return {
            "statusCode": 200,
            "body": json.dumps(result),
            "headers": {"Content-Type": "application/json"},
        }
        
    except Exception as e:
        logger.error(f"Lambda execution failed: {str(e)}", exc_info=True)
        return {
            "statusCode": 500,
            "body": json.dumps(
                {
                    "status": "error",
                    "message": str(e),
                    "timestamp": datetime.utcnow().isoformat(),
                }
            ),
            "headers": {"Content-Type": "application/json"},
        }
