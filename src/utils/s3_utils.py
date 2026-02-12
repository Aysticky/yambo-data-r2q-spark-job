"""
S3 utility functions for data lake operations

ENTERPRISE S3 PATTERNS:
1. Always use partitioned writes (dt=YYYY-MM-DD/hour=HH)
2. Use Parquet with Snappy compression
3. Enable S3 intelligent tiering for cost optimization
4. Implement write-audit-commit pattern for atomic writes
5. Use S3 Select when reading subset of columns

COMMON PRODUCTION ISSUES:
1. S3 throttling (503 SlowDown errors):
   - Cause: Too many requests to same S3 prefix/second
   - Solution: Increase partition count (more prefixes)
   - Solution: Enable S3 Transfer Acceleration
   - Prevention: Keep objects per prefix < 3500 requests/sec

2. Small files problem:
   - Symptom: Millions of tiny files (<1MB)
   - Impact: Slow reads, high S3 API costs
   - Solution: Use Spark coalesce/repartition before write
   - Rule of thumb: Target 128MB-1GB file size

3. Data skew:
   - Symptom: Most partitions finish fast, 1-2 take hours
   - Impact: Wasted executor time, job slowdown
   - Solution: Add salting (random prefix) to partition key
   - Detection: Check Spark UI "Tasks" tab for outliers

4. Eventual consistency issues (old S3 regions):
   - Symptom: Just-written files not immediately readable
   - Impact: Rare in eu-central-1 (strong consistency since Dec 2020)
   - Prevention: Use eu-central-1 or newer regions
"""

import logging
from typing import List, Optional
from datetime import datetime
import boto3
from botocore.exceptions import ClientError


logger = logging.getLogger(__name__)


def construct_s3_path(
    bucket: str,
    prefix: str,
    date: datetime,
    partition_by_hour: bool = False,
    run_id: Optional[str] = None,
) -> str:
    """
    Construct S3 path with partitioning scheme
    
    PARTITIONING STRATEGY:
    - By date: Enables time-based queries and pruning
    - By hour: For high-volume data (>1GB/day)
    - By run_id: For idempotency and atomic writes
    
    Args:
        bucket: S3 bucket name
        prefix: Base prefix (e.g., "raw/transactions")
        date: Partition date
        partition_by_hour: Add hour partition
        run_id: Unique run identifier
    
    Returns:
        Full S3 path like: s3://bucket/raw/transactions/dt=2026-02-12/run_id=abc123/
    
    USAGE:
        path = construct_s3_path(
            bucket="yambo-prod-data-lake",
            prefix="raw/stripe_charges",
            date=datetime(2026, 2, 12),
            run_id="run-1707696000"
        )
        # Output: s3://yambo-prod-data-lake/raw/stripe_charges/dt=2026-02-12/run_id=run-1707696000/
    """
    path_parts = [f"s3://{bucket}", prefix]
    
    # Date partition (always include)
    path_parts.append(f"dt={date.strftime('%Y-%m-%d')}")
    
    # Hour partition (optional, for high-volume data)
    if partition_by_hour:
        path_parts.append(f"hour={date.strftime('%H')}")
    
    # Run ID partition (for atomic writes)
    if run_id:
        path_parts.append(f"run_id={run_id}")
    
    return "/".join(path_parts) + "/"


def validate_s3_access(bucket: str, prefix: str) -> bool:
    """
    Validate S3 bucket access before starting job
    
    FAIL-FAST PRINCIPLE:
    Check permissions at job start, not 2 hours into processing.
    This saves time and executor costs.
    
    Args:
        bucket: S3 bucket name
        prefix: Prefix to test
    
    Returns:
        True if accessible, False otherwise
    
    COMMON ERRORS:
    - 403 Forbidden: IAM role lacks s3:PutObject permission
    - 404 Not Found: Bucket doesn't exist
    - 301 Moved: Wrong region (bucket in us-east-1 but client in eu-central-1)
    """
    s3_client = boto3.client("s3")
    
    try:
        # Try to list objects (validates read permission)
        s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
        
        # Try to put a test object (validates write permission)
        test_key = f"{prefix}/_validate_access_{int(datetime.utcnow().timestamp())}.txt"
        s3_client.put_object(
            Bucket=bucket,
            Key=test_key,
            Body=b"test",
        )
        
        # Clean up test object
        s3_client.delete_object(Bucket=bucket, Key=test_key)
        
        logger.info(f"S3 access validated: s3://{bucket}/{prefix}")
        return True
        
    except ClientError as e:
        error_code = e.response["Error"]["Code"]
        logger.error(
            f"S3 access validation failed: {error_code}",
            extra={"bucket": bucket, "prefix": prefix, "error": str(e)},
        )
        return False


def get_latest_partition(bucket: str, prefix: str) -> Optional[str]:
    """
    Get the latest date partition from S3
    
    Useful for incremental loads: "What's the last date we processed?"
    
    Args:
        bucket: S3 bucket name
        prefix: Prefix to search
    
    Returns:
        Latest partition date as string (YYYY-MM-DD) or None
    
    USAGE:
        last_date = get_latest_partition("yambo-prod-data-lake", "raw/transactions")
        if last_date:
            start_date = datetime.strptime(last_date, "%Y-%m-%d") + timedelta(days=1)
    """
    s3_client = boto3.client("s3")
    
    try:
        # List objects with dt= prefix
        response = s3_client.list_objects_v2(
            Bucket=bucket,
            Prefix=f"{prefix}/dt=",
            Delimiter="/",
        )
        
        if "CommonPrefixes" not in response:
            return None
        
        # Extract dates from prefixes
        dates = []
        for prefix_obj in response["CommonPrefixes"]:
            prefix_path = prefix_obj["Prefix"]
            # Extract date from "prefix/dt=2026-02-12/"
            if "dt=" in prefix_path:
                date_str = prefix_path.split("dt=")[1].split("/")[0]
                dates.append(date_str)
        
        return max(dates) if dates else None
        
    except ClientError as e:
        logger.error(f"Failed to get latest partition: {e}")
        return None


def count_objects(bucket: str, prefix: str) -> int:
    """
    Count objects in S3 prefix
    
    Useful for validation: "Did we write the expected number of files?"
    
    WARNING: For prefixes with >1000 objects, this may take time.
    Consider using S3 Inventory for large prefixes.
    """
    s3_client = boto3.client("s3")
    count = 0
    continuation_token = None
    
    try:
        while True:
            kwargs = {"Bucket": bucket, "Prefix": prefix}
            if continuation_token:
                kwargs["ContinuationToken"] = continuation_token
            
            response = s3_client.list_objects_v2(**kwargs)
            count += response.get("KeyCount", 0)
            
            if not response.get("IsTruncated"):
                break
            
            continuation_token = response.get("NextContinuationToken")
        
        return count
        
    except ClientError as e:
        logger.error(f"Failed to count objects: {e}")
        return 0


# PERFORMANCE OPTIMIZATION NOTES:
#
# 1. S3 Request Rates:
#    - 3,500 PUT/COPY/POST/DELETE per second per prefix
#    - 5,500 GET/HEAD per second per prefix
#    - Prefix = everything before the last "/"
#    - Example: s3://bucket/dt=2026-02-12/file1.parquet → prefix is "dt=2026-02-12"
#
# 2. Optimal File Sizes:
#    - Parquet: 128MB - 1GB per file
#    - Too small (<1MB): Slow reads, high API costs
#    - Too large (>5GB): Difficult to parallelize reads
#
# 3. Spark Partitioning Best Practices:
#    - Use df.repartition(N) before write where N = total_data_size / 128MB
#    - Example: 10GB data → repartition(80) → ~128MB files
#    - Use df.coalesce(N) to reduce partitions (no shuffle)
#
# 4. S3 Cost Optimization:
#    - Use .parquet (columnar) → 10x smaller than .json
#    - Use Snappy compression (good balance of speed/compression)
#    - Enable S3 Intelligent-Tiering (auto-move to cheaper storage)
#    - Set lifecycle rules (delete after 90 days if not needed)
