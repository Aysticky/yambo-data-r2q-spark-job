"""
Configuration management for Spark jobs

This module handles all configuration loading from environment variables,
AWS Secrets Manager, and command-line arguments.

ENTERPRISE NOTES:
- Use AWS Secrets Manager for sensitive values (API keys, tokens)
- Use environment variables for infrastructure settings (bucket names, regions)
- Use command-line args for runtime parameters (date ranges, etc.)
- NEVER hardcode credentials or sensitive data
"""

import os
from typing import Optional
from dataclasses import dataclass
from enum import Enum


class Environment(str, Enum):
    """Deployment environment enum"""
    DEV = "dev"
    PROD = "prod"
    LOCAL = "local"


@dataclass
class SparkConfig:
    """Spark-specific configuration"""
    app_name: str
    executor_memory: str
    executor_cores: int
    num_executors: int
    driver_memory: str
    shuffle_partitions: int
    
    @classmethod
    def from_env(cls) -> "SparkConfig":
        """Load Spark config from environment"""
        return cls(
            app_name=os.getenv("SPARK_APP_NAME", "yambo-data-ingestion"),
            executor_memory=os.getenv("SPARK_EXECUTOR_MEMORY", "4g"),
            executor_cores=int(os.getenv("SPARK_EXECUTOR_CORES", "2")),
            num_executors=int(os.getenv("SPARK_NUM_EXECUTORS", "3")),
            driver_memory=os.getenv("SPARK_DRIVER_MEMORY", "2g"),
            shuffle_partitions=int(os.getenv("SPARK_SHUFFLE_PARTITIONS", "200")),
        )


@dataclass
class AWSConfig:
    """AWS infrastructure configuration"""
    region: str
    s3_bucket: str
    s3_prefix: str
    dynamodb_checkpoint_table: str
    secrets_manager_secret_name: str
    
    @classmethod
    def from_env(cls) -> "AWSConfig":
        """Load AWS config from environment"""
        environment = os.getenv("ENVIRONMENT", "dev")
        return cls(
            region=os.getenv("AWS_REGION", "eu-central-1"),
            s3_bucket=os.getenv("S3_DATA_BUCKET", f"yambo-{environment}-data-lake"),
            s3_prefix=os.getenv("S3_DATA_PREFIX", "raw/transactions"),
            dynamodb_checkpoint_table=os.getenv(
                "DYNAMODB_CHECKPOINT_TABLE", 
                f"yambo-{environment}-checkpoints"
            ),
            secrets_manager_secret_name=os.getenv(
                "SECRETS_MANAGER_SECRET_NAME",
                f"yambo/{environment}/stripe-api"
            ),
        )


@dataclass
class APIConfig:
    """REST API configuration"""
    base_url: str
    page_size: int
    max_retries: int
    timeout_seconds: int
    rate_limit_per_second: int
    
    @classmethod
    def from_env(cls) -> "APIConfig":
        """Load API config from environment"""
        return cls(
            base_url=os.getenv("API_BASE_URL", "https://api.stripe.com/v1"),
            page_size=int(os.getenv("API_PAGE_SIZE", "100")),
            max_retries=int(os.getenv("API_MAX_RETRIES", "5")),
            timeout_seconds=int(os.getenv("API_TIMEOUT", "30")),
            rate_limit_per_second=int(os.getenv("API_RATE_LIMIT", "100")),
        )


@dataclass
class JobConfig:
    """Complete job configuration"""
    environment: Environment
    spark: SparkConfig
    aws: AWSConfig
    api: APIConfig
    
    @classmethod
    def from_env(cls) -> "JobConfig":
        """Load complete configuration from environment"""
        env_str = os.getenv("ENVIRONMENT", "dev")
        environment = Environment(env_str)
        
        return cls(
            environment=environment,
            spark=SparkConfig.from_env(),
            aws=AWSConfig.from_env(),
            api=APIConfig.from_env(),
        )
    
    def is_production(self) -> bool:
        """Check if running in production"""
        return self.environment == Environment.PROD
    
    def is_local(self) -> bool:
        """Check if running locally"""
        return self.environment == Environment.LOCAL


def get_config() -> JobConfig:
    """
    Get application configuration
    
    This is the main entry point for configuration access.
    Call this function once at the start of your job.
    """
    return JobConfig.from_env()


# IMPORTANT PRODUCTION NOTES:
#
# 1. NEVER print sensitive config values (API keys, tokens) to logs
# 2. Always validate config before starting job (fail fast)
# 3. Use different configs for dev vs prod (smaller batches in dev)
# 4. Monitor config changes - if someone changes executor memory from 4g to 2g,
#    jobs might start failing with OOM errors
# 5. Document why each config value is set (e.g., "100 page size chosen to balance
#    API rate limits with memory usage")
