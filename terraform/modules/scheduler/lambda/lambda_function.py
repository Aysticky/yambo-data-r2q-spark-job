"""
Lambda function to trigger Spark jobs on EKS

This Lambda is invoked by EventBridge to create SparkApplication CRDs
on the EKS cluster, which triggers the Spark Operator to run jobs.

DEPLOYMENT:
  cd terraform/modules/scheduler/lambda
  pip install -r requirements.txt -t package/
  cd package && zip -r ../spark_trigger.zip . && cd ..
  zip -g spark_trigger.zip lambda_function.py
"""

import os
import json
import logging
from datetime import datetime
import boto3
from kubernetes import client, config
from kubernetes.client.rest import ApiException

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# AWS clients
eks_client = boto3.client('eks')
sts_client = boto3.client('sts')

# Environment variables
EKS_CLUSTER_NAME = os.environ['EKS_CLUSTER_NAME']
AWS_REGION = os.environ.get('AWS_REGION', 'eu-central-1')


def get_eks_credentials():
    """Get EKS cluster credentials and configure kubectl"""
    cluster_info = eks_client.describe_cluster(name=EKS_CLUSTER_NAME)
    cluster = cluster_info['cluster']
    
    # Get authentication token
    token = get_bearer_token(EKS_CLUSTER_NAME)
    
    # Configure Kubernetes client
    configuration = client.Configuration()
    configuration.host = cluster['endpoint']
    configuration.verify_ssl = True
    configuration.ssl_ca_cert = write_ca_cert(cluster['certificateAuthority']['data'])
    configuration.api_key = {"authorization": "Bearer " + token}
    
    return configuration


def get_bearer_token(cluster_name):
    """Generate EKS authentication token"""
    session = boto3.Session()
    
    # Generate presigned URL for authentication
    url = f"https://sts.{AWS_REGION}.amazonaws.com/?Action=GetCallerIdentity&Version=2011-06-15"
    signed_url = session.get_credentials().get_frozen_credentials()
    
    # Use AWS CLI for token generation (simpler than manual signing)
    import subprocess
    result = subprocess.run(
        ['aws', 'eks', 'get-token', '--cluster-name', cluster_name],
        capture_output=True,
        text=True
    )
    
    token_data = json.loads(result.stdout)
    return token_data['status']['token']


def write_ca_cert(cert_data):
    """Write CA certificate to temp file"""
    import tempfile
    import base64
    
    cert_path = "/tmp/ca.crt"
    with open(cert_path, 'wb') as f:
        f.write(base64.b64decode(cert_data))
    
    return cert_path


def create_spark_application(namespace, job_config):
    """
    Create SparkApplication CRD
    
    This applies the SparkApplication manifest, which triggers the
    Spark Operator to create driver and executor pods.
    """
    # Load SparkApplication template
    spark_app = {
        "apiVersion": "sparkoperator.k8s.io/v1beta2",
        "kind": "SparkApplication",
        "metadata": {
            "name": f"{job_config['sparkApplication']}-{datetime.utcnow().strftime('%Y%m%d-%H%M%S')}",
            "namespace": namespace,
            "labels": {
                "app": "yambo-spark-job",
                "job-type": job_config['jobType'],
                "environment": job_config['environment'],
                "triggered-by": "eventbridge"
            }
        },
        "spec": {
            "type": "Python",
            "pythonVersion": "3",
            "mode": "cluster",
            "image": f"{os.environ.get('ECR_REPOSITORY_URL')}:latest",
            "imagePullPolicy": "Always",
            "mainApplicationFile": "local:///opt/spark/work-dir/src/spark_jobs/main.py",
            "arguments": [
                "--job-type", job_config['jobType'],
                "--environment", job_config['environment']
            ],
            "sparkVersion": "3.5.0",
            "restartPolicy": {
                "type": "OnFailure",
                "onFailureRetries": 2,
                "onFailureRetryInterval": 60
            },
            "driver": {
                "cores": 2,
                "memory": "4096m",
                "serviceAccount": "spark-job-sa"
            },
            "executor": {
                "instances": 3 if job_config['jobType'] == 'extract' else 0,
                "cores": 4,
                "memory": "8192m"
            }
        }
    }
    
    return spark_app


def lambda_handler(event, context):
    """
    Main Lambda handler
    
    Event format:
    {
        "jobType": "check" or "extract",
        "environment": "dev" or "prod",
        "sparkApplication": "yambo-check-job" or "yambo-extract-job",
        "namespace": "spark-jobs"
    }
    """
    logger.info(f"Received event: {json.dumps(event)}")
    
    try:
        # Parse event
        job_type = event['jobType']
        environment = event['environment']
        spark_app_name = event['sparkApplication']
        namespace = event.get('namespace', 'spark-jobs')
        
        logger.info(f"Triggering {job_type} job in {environment} environment")
        
        # Get EKS credentials
        k8s_config = get_eks_credentials()
        client.Configuration.set_default(k8s_config)
        
        # Create custom objects API client
        api = client.CustomObjectsApi()
        
        # Create SparkApplication
        spark_app = create_spark_application(namespace, event)
        
        # Apply SparkApplication CRD
        response = api.create_namespaced_custom_object(
            group="sparkoperator.k8s.io",
            version="v1beta2",
            namespace=namespace,
            plural="sparkapplications",
            body=spark_app
        )
        
        logger.info(f"Created SparkApplication: {response['metadata']['name']}")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': f'Successfully triggered {job_type} job',
                'sparkApplication': response['metadata']['name'],
                'namespace': namespace
            })
        }
        
    except ApiException as e:
        logger.error(f"Kubernetes API error: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': 'Failed to create SparkApplication',
                'details': str(e)
            })
        }
        
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': 'Internal error',
                'details': str(e)
            })
        }


# PRODUCTION NOTES:
#
# AUTHENTICATION:
# Lambda uses IAM role to authenticate with EKS via aws-iam-authenticator.
# The IAM role must be mapped to K8s RBAC in aws-auth ConfigMap:
#
#   kubectl edit configmap aws-auth -n kube-system
#   
#   mapRoles: |
#     - rolearn: arn:aws:iam::ACCOUNT:role/yambo-lambda-spark-trigger-dev
#       username: lambda-trigger
#       groups:
#         - system:masters  # Or create specific role with limited permissions
#
# ALTERNATIVE: Use kubectl in Lambda layer
# Instead of Python Kubernetes client, package kubectl in Lambda layer:
#   aws lambda publish-layer-version --layer-name kubectl --zip-file fileb://kubectl-layer.zip
#   
# Then exec kubectl commands:
#   subprocess.run(['kubectl', 'apply', '-f', 'spark-app.yaml'])
#
# MONITORING:
# - CloudWatch Logs: /aws/lambda/yambo-spark-trigger-dev
# - CloudWatch Metrics: Lambda invocations, errors, duration
# - X-Ray tracing: Enable for distributed tracing
#
# ERROR HANDLING:
# - Retry logic in EventBridge (automatic retries)
# - Dead letter queue for failed invocations
# - Alert on Lambda errors via CloudWatch Alarms
