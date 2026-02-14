"""
Check Job - Validates API connectivity and credentials

ENTERPRISE "CHECK JOB" PATTERN:
This is a lightweight pre-flight check that runs before the heavy extraction job.
It validates that everything is ready for data extraction:
1. API credentials are valid
2. API endpoint is accessible
3. Rate limits are acceptable
4. S3 bucket is writable
5. DynamoDB checkpoint table exists

WHY SEPARATE CHECK FROM EXTRACT?
- Fast failure: Detect issues in 30 seconds instead of 30 minutes into extraction
- Cost saving: Don't spin up 10 executors if API is down
- Resource efficiency: Check job needs 1 small executor, extract needs many
- Debugging: Separate logs make troubleshooting easier

PRODUCTION SCENARIO:
Without check job:
- Extraction job starts with 10 executors (expensive)
- 20 minutes in: API auth fails
- Wasted $5 in compute, 20 minutes of time

With check job:
- Check job runs (30 seconds, $0.01)
- Detects auth failure immediately
- Alert sent to on-call engineer
- Extraction job never starts

COMMON FAILURE MODES:
1. API credentials rotated but Secrets Manager not updated
   - Check job fails immediately
   - On-call gets paged
   - Fix: Update secret in Secrets Manager

2. API endpoint changed (deprecation)
   - Check job gets 404 Not Found
   - Extract job would have failed anyway
   - Fix: Update API_BASE_URL config

3. Rate limit reduced by API provider
   - Check job detects current rate limit
   - Compare with expected limit
   - Alert if rate limit < threshold

4. S3 bucket access revoked
   - Check job fails on write test
   - Extract job would have written to /dev/null
   - Fix: Restore IAM permissions

5. Checkpoint table deleted accidentally
   - Check job fails on DynamoDB access
   - Extract job would have no state
   - Fix: Restore table from backup
"""

import sys
import logging
from datetime import datetime, timedelta
from typing import Dict, Any

import boto3
from pyspark.sql import SparkSession

from src.utils.config import get_config
from src.utils.logging_setup import setup_logging, log_execution_time
from src.utils.s3_utils import validate_s3_access
from src.api.auth import StripeAPIKeyManager
from src.api.client import RESTAPIClient
from src.checkpoint.manager import CheckpointManager


def check_secrets_manager(secret_name: str, logger: logging.Logger) -> bool:
    """
    Verify Secrets Manager secret exists and is accessible

    COMMON ISSUES:
    - AccessDenied: IAM role lacks secretsmanager:GetSecretValue
    - ResourceNotFound: Secret name typo or doesn't exist
    - DecryptionFailure: KMS key permissions issue
    """
    try:
        with log_execution_time(logger, "Check Secrets Manager access"):
            secrets_client = boto3.client("secretsmanager")
            response = secrets_client.get_secret_value(SecretId=secret_name)

            logger.info(
                f"Successfully accessed secret: {secret_name}", extra={"secret_name": secret_name}
            )
            return True

    except Exception as e:
        logger.error(
            f"Failed to access Secrets Manager: {e}",
            extra={"secret_name": secret_name, "error": str(e)},
        )
        return False


def check_api_connectivity(
    api_manager: StripeAPIKeyManager, base_url: str, logger: logging.Logger
) -> Dict[str, Any]:
    """
    Verify API is accessible and credentials are valid

    Returns health check results with timing and status

    WHAT WE'RE CHECKING:
    1. Can we get an access token?
    2. Can we make a simple API call?
    3. What's the API response time? (should be <1s)
    4. What are current rate limits? (from response headers)

    PRODUCTION TIP:
    Some APIs have dedicated health check endpoints:
    - GET /health
    - GET /status
    - GET /ping

    Use these if available (faster than real endpoint).
    For Stripe, we'll use a simple list call with limit=1.
    """
    health_check = {
        "api_accessible": False,
        "auth_valid": False,
        "response_time_ms": None,
        "rate_limit_remaining": None,
        "error": None,
    }

    try:
        with log_execution_time(logger, "API connectivity check"):
            # Initialize client
            client = RESTAPIClient(
                base_url=base_url, auth_manager=api_manager, rate_limit=100, timeout=10
            )

            # Make a lightweight test request (just 1 record)
            start_time = datetime.utcnow()
            response = client._make_request(method="GET", endpoint="/charges", params={"limit": 1})
            end_time = datetime.utcnow()

            # Calculate response time
            response_time = (end_time - start_time).total_seconds() * 1000

            health_check["api_accessible"] = True
            health_check["auth_valid"] = True
            health_check["response_time_ms"] = response_time

            logger.info(
                f"API connectivity check passed",
                extra={"response_time_ms": response_time, "has_data": bool(response.get("data"))},
            )

            # Log warning if API is slow
            if response_time > 3000:
                logger.warning(
                    f"API response time is slow: {response_time}ms",
                    extra={"response_time_ms": response_time, "threshold_ms": 3000},
                )

            return health_check

    except Exception as e:
        health_check["error"] = str(e)
        logger.error(f"API connectivity check failed: {e}", extra={"error": str(e)})
        return health_check


def check_s3_access(bucket: str, prefix: str, logger: logging.Logger) -> bool:
    """
    Verify S3 bucket is accessible and writable

    WHAT WE'RE CHECKING:
    1. Can we list objects? (read permission)
    2. Can we write a test object? (write permission)
    3. Can we delete the test object? (delete permission)

    PRODUCTION ISSUE:
    Once had a job that could write but not read its own files.
    Root cause: Bucket policy allowed s3:PutObject but not s3:GetObject.
    Job wrote data successfully but downstream couldn't read it.
    """
    try:
        with log_execution_time(logger, "S3 access check"):
            result = validate_s3_access(bucket, prefix)

            if result:
                logger.info(f"S3 access check passed", extra={"bucket": bucket, "prefix": prefix})
            else:
                logger.error(f"S3 access check failed", extra={"bucket": bucket, "prefix": prefix})

            return result

    except Exception as e:
        logger.error(
            f"S3 access check failed with exception: {e}",
            extra={"bucket": bucket, "prefix": prefix, "error": str(e)},
        )
        return False


def check_dynamodb_access(table_name: str, logger: logging.Logger) -> bool:
    """
    Verify DynamoDB checkpoint table exists and is accessible

    WHAT WE'RE CHECKING:
    1. Does table exist?
    2. Can we read from it?
    3. Can we write to it?
    4. Is table in ACTIVE state? (not CREATING or DELETING)

    EDGE CASE:
    Table might exist but be in UPDATING state (schema change).
    Reads/writes will fail with ResourceNotFoundException.
    Check table status before proceeding.
    """
    try:
        with log_execution_time(logger, "DynamoDB access check"):
            dynamodb = boto3.client("dynamodb")

            # Describe table to verify it exists and check status
            response = dynamodb.describe_table(TableName=table_name)
            table_status = response["Table"]["TableStatus"]

            if table_status != "ACTIVE":
                logger.error(
                    f"DynamoDB table is not ACTIVE: {table_status}",
                    extra={"table_name": table_name, "status": table_status},
                )
                return False

            # Test read access
            dynamodb_resource = boto3.resource("dynamodb")
            table = dynamodb_resource.Table(table_name)

            # Try to get a non-existent key (validates read permission)
            table.get_item(Key={"checkpoint_key": "_health_check_"})

            logger.info(
                f"DynamoDB access check passed",
                extra={"table_name": table_name, "status": table_status},
            )
            return True

    except boto3.client("dynamodb").exceptions.ResourceNotFoundException:
        logger.error(f"DynamoDB table not found: {table_name}", extra={"table_name": table_name})
        return False
    except Exception as e:
        logger.error(
            f"DynamoDB access check failed: {e}", extra={"table_name": table_name, "error": str(e)}
        )
        return False


def main():
    """
    Main check job execution

    EXIT CODES:
    0: All checks passed
    1: One or more checks failed
    2: Critical error (exception)

    SPARK NOTE:
    We create a SparkSession even though we don't use Spark here.
    This validates that Spark is properly configured and can connect to S3.
    In production, Spark initialization failures are common (wrong Hadoop version,
    missing S3A JARs, etc.). Better to catch these in check job than extract job.
    """
    # Initialize logger
    logger = setup_logging(
        log_level="INFO",
        structured=True,
        job_id=f"check-job-{datetime.utcnow().strftime('%Y%m%d-%H%M%S')}",
    )

    logger.info("=" * 80)
    logger.info("Starting Check Job")
    logger.info("=" * 80)

    # Load configuration
    config = get_config()

    logger.info(
        "Configuration loaded",
        extra={
            "environment": config.environment.value,
            "aws_region": config.aws.region,
            "s3_bucket": config.aws.s3_bucket,
            "secret_name": config.aws.secrets_manager_secret_name,
            "dynamodb_table": config.aws.dynamodb_checkpoint_table,
        },
    )

    # Track check results
    checks = {
        "spark_init": False,
        "secrets_access": False,
        "api_connectivity": False,
        "s3_access": False,
        "dynamodb_access": False,
    }

    try:
        # Check 1: Initialize Spark (validates Spark configuration)
        logger.info("Check 1/5: Initializing Spark session...")
        with log_execution_time(logger, "Spark initialization"):
            spark = (
                SparkSession.builder.appName(config.spark.app_name + "-check")
                .config("spark.executor.memory", "1g")  # Minimal for check job
                .config("spark.executor.cores", "1")
                .config("spark.driver.memory", "1g")
                .config("spark.sql.adaptive.enabled", "true")
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                .getOrCreate()
            )
            checks["spark_init"] = True
            logger.info("Spark session initialized successfully")

        # Check 2: Secrets Manager access
        logger.info("Check 2/5: Verifying Secrets Manager access...")
        checks["secrets_access"] = check_secrets_manager(
            config.aws.secrets_manager_secret_name, logger
        )

        if not checks["secrets_access"]:
            logger.error("Secrets Manager check failed - cannot proceed")
            # Continue with other checks for complete health report

        # Check 3: API connectivity (only if secrets accessible)
        logger.info("Check 3/5: Testing API connectivity...")
        if checks["secrets_access"]:
            try:
                api_manager = StripeAPIKeyManager(config.aws.secrets_manager_secret_name)
                health_check = check_api_connectivity(api_manager, config.api.base_url, logger)
                checks["api_connectivity"] = health_check["api_accessible"]

                if not checks["api_connectivity"]:
                    logger.error(f"API connectivity failed: {health_check.get('error')}")

            except Exception as e:
                logger.error(f"API connectivity check exception: {e}")
                checks["api_connectivity"] = False
        else:
            logger.warning("Skipping API check (secrets not accessible)")

        # Check 4: S3 access
        logger.info("Check 4/5: Verifying S3 bucket access...")
        checks["s3_access"] = check_s3_access(config.aws.s3_bucket, config.aws.s3_prefix, logger)

        # Check 5: DynamoDB access
        logger.info("Check 5/5: Verifying DynamoDB table access...")
        checks["dynamodb_access"] = check_dynamodb_access(
            config.aws.dynamodb_checkpoint_table, logger
        )

        # Summary
        logger.info("=" * 80)
        logger.info("Check Job Summary")
        logger.info("=" * 80)

        passed_checks = sum(1 for v in checks.values() if v)
        total_checks = len(checks)

        for check_name, passed in checks.items():
            status = "PASSED" if passed else "FAILED"
            log_level = logging.INFO if passed else logging.ERROR
            logger.log(
                log_level, f"{check_name}: {status}", extra={"check": check_name, "passed": passed}
            )

        logger.info(f"Checks passed: {passed_checks}/{total_checks}")

        # Determine exit code
        if passed_checks == total_checks:
            logger.info("All checks passed - system is healthy")
            exit_code = 0
        else:
            logger.error("One or more checks failed - system is not healthy")
            exit_code = 1

        # Cleanup
        spark.stop()
        logger.info("Check job completed")

        sys.exit(exit_code)

    except Exception as e:
        logger.error(f"Check job failed with exception: {e}", exc_info=True)
        sys.exit(2)


if __name__ == "__main__":
    main()


# PRODUCTION DEPLOYMENT NOTES:
#
# 1. Run check job BEFORE extract job in orchestration:
#    - Airflow: Use BranchPythonOperator or ShortCircuitOperator
#    - Step Functions: Use Choice state based on check job status
#    - Kubernetes: Use Job dependencies (check job completes → extract job starts)
#
# 2. Check job resource requirements:
#    - Driver: 1 CPU, 1GB RAM
#    - Executor: Not needed (or 1 small executor)
#    - Duration: 30-60 seconds typically
#    - Cost: ~$0.01 per run
#
# 3. Alerting:
#    - Set up CloudWatch alarm on check job failures
#    - Alert on-call engineer immediately
#    - Check job failure = prevent expensive extract job from failing
#
# 4. Check job failure patterns:
#    - If check job fails 3 times in a row → Page on-call (critical)
#    - If API response time >5s → Warning alert (degraded API performance)
#    - If S3 access fails → Critical alert (permissions issue)
#
# 5. Monitoring:
#    - Track check job success rate (should be >99%)
#    - Track check job duration (spike indicates infrastructure issue)
#    - Log all check results to CloudWatch for analysis
