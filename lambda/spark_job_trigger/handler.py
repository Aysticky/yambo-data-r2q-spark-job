"""
Lambda function to trigger Spark jobs on EKS using Kubernetes Python client.

This avoids bundling kubectl by using the kubernetes library directly.

Environment Variables:
    EKS_CLUSTER_NAME: Name of the EKS cluster
    AWS_REGION: AWS region
    NAMESPACE: Kubernetes namespace for Spark jobs
    JOB_NAME: Name of the SparkApplication to trigger
    ENVIRONMENT: dev or prod
"""

import base64
import json
import logging
import os
import re
from datetime import datetime

import boto3
from botocore.signers import RequestSigner
import yaml
from kubernetes import client
from kubernetes.client.rest import ApiException

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# AWS clients
eks_client = boto3.client("eks")
s3_client = boto3.client("s3")
sts_client = boto3.client("sts")


def get_eks_token(cluster_name: str, region: str) -> str:
    """
    Get authentication token for EKS cluster using STS.
    Generates a pre-signed URL that EKS accepts as authentication.
    """
    try:
        # Create STS client for the region
        sts = boto3.client('sts', region_name=region)
        
        # Get the service ID for signing
        service_id = sts.meta.service_model.service_id
        
        # Create a request signer
        signer = RequestSigner(
            service_id,
            region,
            'sts',
            'v4',
            sts._request_signer._credentials,
            sts.meta.events
        )
        
        # Generate pre-signed URL for GetCallerIdentity
        params = {
            'method': 'GET',
            'url': f'https://sts.{region}.amazonaws.com/?Action=GetCallerIdentity&Version=2011-06-15',
            'body': {},
            'headers': {
                'x-k8s-aws-id': cluster_name
            },
            'context': {}
        }
        
        signed_url = signer.generate_presigned_url(
            params,
            region_name=region,
            expires_in=60,
            operation_name=''
        )
        
        # Remove https:// scheme before encoding
        url_without_https = re.sub(r'^https://', '', signed_url)
        
        # Encode the URL as base64 for the token (removing padding)
        token = 'k8s-aws-v1.' + base64.urlsafe_b64encode(
            url_without_https.encode('utf-8')
        ).decode('utf-8').rstrip('=')
        
        logger.info(f"Successfully generated EKS token for cluster: {cluster_name}")
        return token
        
    except Exception as e:
        logger.error(f"Failed to get EKS token: {e}", exc_info=True)
        raise


def get_kube_client(cluster_name: str, region: str) -> client.CustomObjectsApi:
    """
    Create Kubernetes custom objects API client for EKS.
    
    Args:
        cluster_name: EKS cluster name
        region: AWS region
        
    Returns:
        Kubernetes custom objects API client
    """
    # Get cluster details
    cluster_info = eks_client.describe_cluster(name=cluster_name)
    cluster_endpoint = cluster_info["cluster"]["endpoint"]
    cluster_ca_data = cluster_info["cluster"]["certificateAuthority"]["data"]
    
    # Get authentication token
    bearer_token = get_eks_token(cluster_name, region)
    
    # Write CA cert to temp file
    ca_cert_path = "/tmp/ca.crt"
    with open(ca_cert_path, "wb") as f:
        f.write(base64.b64decode(cluster_ca_data))
    
    # Configure Kubernetes client
    configuration = client.Configuration()
    configuration.host = cluster_endpoint
    configuration.api_key = {"authorization": bearer_token}
    configuration.api_key_prefix = {"authorization": "Bearer"}
    configuration.ssl_ca_cert = ca_cert_path
    configuration.verify_ssl = True
    
    # Create API client
    api_client = client.ApiClient(configuration)
    return client.CustomObjectsApi(api_client)


def download_manifest(environment: str, job_name: str) -> dict:
    """
    Download SparkApplication manifest from S3.
    
    Args:
        environment: Environment (dev/prod)
        job_name: Name of the Spark job
        
    Returns:
        Parsed YAML manifest as dictionary (SparkApplication only)
    """
    bucket = f"yambo-{environment}-manifests"
    key = f"spark-jobs/{job_name}.yaml"
    
    logger.info(f"Downloading manifest from s3://{bucket}/{key}")
    
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        manifest_content = response["Body"].read().decode("utf-8")
        
        # Handle multi-document YAML files (separated by ---)
        # Find the SparkApplication document
        documents = list(yaml.safe_load_all(manifest_content))
        
        for doc in documents:
            if doc and doc.get("kind") == "SparkApplication":
                logger.info(f"Found SparkApplication: {doc.get('metadata', {}).get('name')}")
                return doc
        
        # If no SparkApplication found, raise error
        raise ValueError(f"No SparkApplication found in manifest {key}")
        
    except Exception as e:
        logger.error(f"Failed to download manifest: {e}")
        raise


def trigger_spark_job(
    cluster_name: str,
    region: str,
    namespace: str,
    job_name: str,
    environment: str
) -> dict:
    """
    Trigger a Spark job by creating/updating the SparkApplication.
    
    Args:
        cluster_name: EKS cluster name
        region: AWS region
        namespace: Kubernetes namespace
        job_name: SparkApplication name
        environment: Environment (dev/prod)
        
    Returns:
        Result dictionary with status and details
    """
    try:
        # Get Kubernetes client
        logger.info(f"Connecting to EKS cluster: {cluster_name}")
        k8s_client = get_kube_client(cluster_name, region)
        
        # Download manifest
        manifest = download_manifest(environment, job_name)
        
        # Delete existing SparkApplication if it exists
        try:
            logger.info(f"Deleting existing SparkApplication: {job_name}")
            k8s_client.delete_namespaced_custom_object(
                group="sparkoperator.k8s.io",
                version="v1beta2",
                namespace=namespace,
                plural="sparkapplications",
                name=job_name
            )
            logger.info(f"Deleted existing SparkApplication: {job_name}")
        except ApiException as e:
            if e.status == 404:
                logger.info(f"No existing SparkApplication to delete: {job_name}")
            else:
                raise
        
        # Create new SparkApplication
        logger.info(f"Creating SparkApplication: {job_name}")
        response = k8s_client.create_namespaced_custom_object(
            group="sparkoperator.k8s.io",
            version="v1beta2",
            namespace=namespace,
            plural="sparkapplications",
            body=manifest
        )
        
        logger.info(f"Successfully triggered Spark job: {job_name}")
        
        return {
            "status": "success",
            "message": f"Spark job {job_name} triggered successfully",
            "job_name": job_name,
            "namespace": namespace,
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Failed to trigger Spark job: {str(e)}")
        return {
            "status": "error",
            "message": str(e),
            "job_name": job_name,
            "namespace": namespace,
            "timestamp": datetime.utcnow().isoformat()
        }


def lambda_handler(event, context):
    """
    Lambda handler function.
    
    Args:
        event: Lambda event (from EventBridge)
        context: Lambda context
        
    Returns:
        Response dictionary
    """
    logger.info(f"Received event: {json.dumps(event)}")
    
    # Get environment variables
    cluster_name = os.environ["EKS_CLUSTER_NAME"]
    region = os.environ["AWS_REGION"]
    namespace = os.environ.get("NAMESPACE", "spark-jobs")
    job_name = os.environ.get("JOB_NAME", "yambo-extract-job")
    environment = os.environ["ENVIRONMENT"]
    
    # Override job_name from event if provided
    if "job_name" in event:
        job_name = event["job_name"]
    
    # Trigger job
    result = trigger_spark_job(cluster_name, region, namespace, job_name, environment)
    
    # Return response
    return {
        "statusCode": 200 if result["status"] == "success" else 500,
        "body": json.dumps(result)
    }
