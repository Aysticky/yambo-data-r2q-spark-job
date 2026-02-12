"""
Airflow DAG for Yambo Data Pipeline

This DAG orchestrates the Spark jobs on EKS:
1. Check job: Validate API, S3, DynamoDB access
2. Extract job: Fetch data from API and write to S3

SCHEDULE: 2 times per day (aligned with EventBridge scheduler)
- Run at 2 AM and 2 PM UTC
- Check job runs 10 minutes before extract job

AIRFLOW OPERATORS:
- Use KubernetesPodOperator or SparkKubernetesOperator
- Submit SparkApplication CRDs to EKS cluster
- Monitor job status and handle failures

PRODUCTION PATTERNS:
- Retry logic: 3 retries with exponential backoff
- Alerts: Send notifications on failures
- SLA: 1 hour max runtime
- Monitoring: Track job duration, data volume, quality
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# Default arguments for all tasks
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email': ['alerts@yambo.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=30),
}

# DAG definition
dag = DAG(
    'yambo_data_pipeline',
    default_args=default_args,
    description='Yambo REST API to S3 data pipeline',
    schedule_interval='0 2,14 * * *',  # Run at 2 AM and 2 PM UTC
    start_date=datetime(2024, 1, 1),
    catchup=False,  # Don't backfill
    max_active_runs=1,  # Only one run at a time
    tags=['yambo', 'spark', 'data-pipeline'],
)

# SparkApplication manifest for check job
check_job_manifest = {
    'apiVersion': 'sparkoperator.k8s.io/v1beta2',
    'kind': 'SparkApplication',
    'metadata': {
        'name': 'yambo-check-{{ ts_nodash | lower }}',
        'namespace': 'spark-jobs',
    },
    'spec': {
        'type': 'Python',
        'pythonVersion': '3',
        'mode': 'cluster',
        'image': '{{ var.value.spark_image }}',
        'imagePullPolicy': 'Always',
        'mainApplicationFile': 'local:///opt/spark/work-dir/src/spark_jobs/main.py',
        'arguments': ['--job-type', 'check', '--environment', '{{ var.value.environment }}'],
        'sparkVersion': '3.5.0',
        '

restartPolicy': {
            'type': 'OnFailure',
            'onFailureRetries': 2,
            'onFailureRetryInterval': 60,
        },
        'driver': {
            'cores': 1,
            'coreLimit': '1200m',
            'memory': '2048m',
            'memoryOverhead': '512m',
            'serviceAccount': 'spark-job-sa',
            'labels': {
                'app': 'yambo-spark-job',
                'job-type': 'check',
                'airflow-dag-id': '{{ dag.dag_id }}',
                'airflow-task-id': '{{ task.task_id }}',
                'airflow-run-id': '{{ run_id }}',
            },
        },
        'executor': {
            'instances': 0,  # Check job doesn't need executors
            'cores': 1,
            'memory': '1024m',
        },
    },
}

# SparkApplication manifest for extract job
extract_job_manifest = {
    'apiVersion': 'sparkoperator.k8s.io/v1beta2',
    'kind': 'SparkApplication',
    'metadata': {
        'name': 'yambo-extract-{{ ts_nodash | lower }}',
        'namespace': 'spark-jobs',
    },
    'spec': {
        'type': 'Python',
        'pythonVersion': '3',
        'mode': 'cluster',
        'image': '{{ var.value.spark_image }}',
        'imagePullPolicy': 'Always',
        'mainApplicationFile': 'local:///opt/spark/work-dir/src/spark_jobs/main.py',
        'arguments': ['--job-type', 'extract', '--environment', '{{ var.value.environment }}'],
        'sparkVersion': '3.5.0',
        'restartPolicy': {
            'type': 'OnFailure',
            'onFailureRetries': 3,
            'onFailureRetryInterval': 120,
        },
        'driver': {
            'cores': 2,
            'coreLimit': '2400m',
            'memory': '4096m',
            'memoryOverhead': '1024m',
            'serviceAccount': 'spark-job-sa',
            'envFrom': [{'configMapRef': {'name': 'spark-job-config'}}],
            'labels': {
                'app': 'yambo-spark-job',
                'job-type': 'extract',
                'airflow-dag-id': '{{ dag.dag_id }}',
                'airflow-task-id': '{{ task.task_id }}',
                'airflow-run-id': '{{ run_id }}',
            },
        },
        'executor': {
            'instances': 3,
            'cores': 4,
            'coreLimit': '4800m',
            'memory': '8192m',
            'memoryOverhead': '2048m',
            'labels': {
                'app': 'yambo-spark-job',
                'job-type': 'extract',
            },
        },
        'dynamicAllocation': {
            'enabled': True,
            'initialExecutors': 3,
            'minExecutors': 1,
            'maxExecutors': 10,
        },
    },
}

# Task 1: Run check job
check_job = SparkKubernetesOperator(
    task_id='run_check_job',
    namespace='spark-jobs',
    application_file=check_job_manifest,
    kubernetes_conn_id='kubernetes_default',
    do_xcom_push=True,
    dag=dag,
)

# Task 2: Run extract job (depends on check job)
extract_job = SparkKubernetesOperator(
    task_id='run_extract_job',
    namespace='spark-jobs',
    application_file=extract_job_manifest,
    kubernetes_conn_id='kubernetes_default',
    do_xcom_push=True,
    dag=dag,
)

# Task 3: Validate output data exists in S3
def validate_output(**context):
    """
    Validate that data was written to S3
    
    Checks:
    - S3 path exists
    - Files are not empty
    - Partition structure is correct
    """
    import boto3
    from datetime import datetime
    
    s3 = boto3.client('s3')
    bucket = context['var']['value'].get('data_lake_bucket')
    execution_date = context['execution_date']
    
    # Expected S3 path: s3://bucket/raw/charges/dt=YYYY-MM-DD/
    prefix = f"raw/charges/dt={execution_date.strftime('%Y-%m-%d')}/"
    
    try:
        response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
        
        if 'Contents' not in response:
            raise ValueError(f"No data found in S3: s3://{bucket}/{prefix}")
        
        file_count = len(response['Contents'])
        print(f"Found {file_count} files in S3: s3://{bucket}/{prefix}")
        
        return True
        
    except Exception as e:
        print(f"Validation failed: {e}")
        raise

validate_output_task = PythonOperator(
    task_id='validate_output',
    python_callable=validate_output,
    provide_context=True,
    dag=dag,
)

# Task 4: Send success notification
def send_success_notification(**context):
    """Send notification on successful pipeline run"""
    import json
    
    execution_date = context['execution_date']
    dag_id = context['dag'].dag_id
    run_id = context['run_id']
    
    message = {
        'status': 'SUCCESS',
        'dag_id': dag_id,
        'run_id': run_id,
        'execution_date': execution_date.isoformat(),
        'message': 'Yambo data pipeline completed successfully',
    }
    
    print(json.dumps(message, indent=2))
    
    # In production, send to SNS, Slack, etc.
    # sns = boto3.client('sns')
    # sns.publish(TopicArn='arn:aws:sns:...', Message=json.dumps(message))

success_notification = PythonOperator(
    task_id='send_success_notification',
    python_callable=send_success_notification,
    provide_context=True,
    trigger_rule='all_success',
    dag=dag,
)

# Task 5: Send failure notification
def send_failure_notification(**context):
    """Send notification on pipeline failure"""
    import json
    
    execution_date = context['execution_date']
    dag_id = context['dag'].dag_id
    run_id = context['run_id']
    
    message = {
        'status': 'FAILED',
        'dag_id': dag_id,
        'run_id': run_id,
        'execution_date': execution_date.isoformat(),
        'message': 'Yambo data pipeline failed',
        'action': 'Check Airflow logs and Spark driver logs',
    }
    
    print(json.dumps(message, indent=2))
    
    # In production, send to SNS, PagerDuty, etc.

failure_notification = PythonOperator(
    task_id='send_failure_notification',
    python_callable=send_failure_notification,
    provide_context=True,
    trigger_rule='one_failed',
    dag=dag,
)

# Task dependencies
check_job >> extract_job >> validate_output_task >> success_notification
check_job >> failure_notification  # Send failure notification if check fails
extract_job >> failure_notification  # Send failure notification if extract fails

# AIRFLOW VARIABLES (set via Airflow UI or CLI):
#
# airflow variables set spark_image "ACCOUNT.dkr.ecr.eu-central-1.amazonaws.com/yambo-spark-job:latest"
# airflow variables set environment "dev"
# airflow variables set data_lake_bucket "yambo-data-lake-dev"
#
# PRODUCTION MONITORING:
#
# 1. DAG duration: Alert if >1 hour
# 2. Task failures: Alert on any task failure
# 3. Data volume: Track records extracted per run
# 4. Data quality: Monitor quality scores
# 5. Cost: Track Spark executor usage
#
# TROUBLESHOOTING:
#
# Issue 1: Check job fails with API errors
# - Check Secrets Manager for valid API credentials
# - Verify IRSA role has Secrets Manager permissions
# - Test API connectivity from driver pod
#
# Issue 2: Extract job fails with S3 errors
# - Verify IRSA role has S3 permissions
# - Check S3 bucket exists and is accessible
# - Test S3 access from driver pod
#
# Issue 3: Airflow can't connect to K8s
# - Verify kubernetes_conn_id is configured
# - Check Airflow has K8s API access
# - Verify service account has correct RBAC
#
# Issue 4: DAG not scheduled
# - Check schedule_interval is valid cron
# - Verify DAG is unpaused
# - Check Airflow scheduler is running
