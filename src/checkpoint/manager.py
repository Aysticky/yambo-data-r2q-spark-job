"""
Checkpoint manager for tracking incremental extraction progress

CHECKPOINT PATTERN:
Checkpoints are essential for production data pipelines:
1. Track what data has been extracted ("last_extracted_timestamp")
2. Enable incremental loads (only extract new data)
3. Support resumability (restart failed jobs from last checkpoint)
4. Prevent duplicate processing
5. Provide audit trail (when was data last extracted)

STORAGE OPTIONS:

1. DynamoDB (RECOMMENDED for production):
Pros:
   - Fast reads/writes (<10ms)
   - Atomic updates (no race conditions)
   - Serverless (no management overhead)
   - Good for distributed systems (multiple executors can read/write safely)
   - Supports TTL for automatic cleanup
   
Cons:
   - Requires AWS
   - Costs money (but cheap: ~$0.25/million requests)

2. S3 (simple alternative):
Pros:
   - Very cheap
   - No setup required
   
Cons:
   - Eventual consistency (rare but possible to read stale checkpoint)
   - No atomic updates (race condition if multiple writers)
   - Slower (50-100ms per operation)
   - Use only for single-threaded jobs

3. Database (PostgreSQL, MySQL):
Pros:
   - Strong consistency
   - Transactions
   
Cons:
   - Requires managing database
   - Connection pooling complexity
   - More expensive than DynamoDB

PRODUCTION SCENARIOS:

Scenario 1: Job runs successfully
- Extract data for 2026-02-12
- Write data to S3
- Update checkpoint: last_timestamp=2026-02-12T23:59:59Z
- Next run: Extract data created_after=2026-02-12T23:59:59Z

Scenario 2: Job fails mid-extraction
- Extract data for 2026-02-12
- Processed 500K rows
- Job crashes (OOM, API error, etc.)
- Checkpoint still shows: last_timestamp=2026-02-11T23:59:59Z (not updated yet)
- Restart job: Extracts from 2026-02-11 again
- Some data reprocessed, but idempotent writes prevent duplicates

Scenario 3: Concurrent execution (anti-pattern)
- Job A starts extracting for 2026-02-12
- Job B starts extracting for 2026-02-12 (misconfigured scheduler)
- Both read checkpoint: last_timestamp=2026-02-11
- Both extract same data
- Both update checkpoint (race condition)
- Result: Duplicate data in S3, wasted compute
- Prevention: Use Airflow sensor to prevent concurrent runs

Scenario 4: Backfill (historical load)
- Need to load data from 2025-01-01 to 2026-01-01
- Option A: Reset checkpoint, let job run for 365 days (slow)
- Option B: Pass explicit date range, don't update checkpoint
- Option C: Create separate checkpoint key for backfill job
- Recommended: Option C

COMMON ISSUES:

1. Checkpoint not updated after failure:
   Symptom: Job keeps reprocessing same data
   Cause: Checkpoint updated before write completes
   Solution: Update checkpoint only after successful write
   Pattern: write_data() → verify_write() → update_checkpoint()

2. Checkpoint drift:
   Symptom: Missing data gaps in S3
   Cause: Checkpoint updated but write failed
   Solution: Write-audit-commit pattern (see below)

3. Clock skew:
   Symptom: Missing recent records
   Cause: API clock ahead of our clock
   Solution: Subtract buffer (e.g., checkpoint_time - 5 minutes)

4. Timezone confusion:
   Symptom: Duplicate or missing data at day boundaries
   Cause: Mixing UTC and local timezones
   Solution: ALWAYS use UTC for checkpoints
"""

import logging
from typing import Optional, Dict, Any
from datetime import datetime, timedelta
import json

import boto3
from botocore.exceptions import ClientError


logger = logging.getLogger(__name__)


class CheckpointManager:
    """
    Manages extraction checkpoints in DynamoDB
    
    Table schema:
    - Partition key: checkpoint_key (string) - e.g., "stripe_charges_prod"
    - Attributes:
      - last_extracted_timestamp (string) - ISO 8601 UTC
      - last_extracted_cursor (string) - API cursor (optional)
      - records_extracted (number) - Total
      records_extracted_last_run (number) - Last run count
      - updated_at (string) - Checkpoint update time
      - updated_by (string) - Job ID that updated checkpoint
    
    USAGE:
        manager = CheckpointManager(
            table_name="yambo-prod-checkpoints",
            checkpoint_key="stripe_charges"
        )
        
        # Get last checkpoint
        last_timestamp = manager.get_last_timestamp()
        
        # Extract new data
        records = extract_data(created_after=last_timestamp)
        
        # Write data
        write_to_s3(records)
        
        # Update checkpoint
        manager.update_checkpoint(
            timestamp=datetime.utcnow(),
            records_count=len(records)
        )
    """
    
    def __init__(
        self,
        table_name: str,
        checkpoint_key: str,
        time_buffer_minutes: int = 5,
    ):
        """
        Initialize checkpoint manager
        
        Args:
            table_name: DynamoDB table name
            checkpoint_key: Unique checkpoint identifier (e.g., "stripe_charges_prod")
            time_buffer_minutes: Subtract this from checkpoint for safety (handles clock skew)
        """
        self.table_name = table_name
        self.checkpoint_key = checkpoint_key
        self.time_buffer = timedelta(minutes=time_buffer_minutes)
        
        self.dynamodb = boto3.resource("dynamodb")
        self.table = self.dynamodb.Table(table_name)
    
    def get_last_timestamp(
        self,
        default: Optional[datetime] = None,
    ) -> Optional[datetime]:
        """
        Get last extracted timestamp from checkpoint
        
        Args:
            default: Default timestamp if no checkpoint exists
        
        Returns:
            Last extracted timestamp (with buffer subtracted) or default
        
        BUFFER EXPLANATION:
        If checkpoint says "2026-02-12T10:00:00Z", we return "2026-02-12T09:55:00Z".
        This ensures we don't miss records created between 09:55 and 10:00 due to:
        - Clock skew between our system and API
        - Records with timestamps slightly before they appeared in API
        
        Trade-off: Some records might be extracted twice, but idempotent writes handle this.
        """
        try:
            response = self.table.get_item(
                Key={"checkpoint_key": self.checkpoint_key}
            )
            
            if "Item" in response:
                item = response["Item"]
                timestamp_str = item.get("last_extracted_timestamp")
                
                if timestamp_str:
                    timestamp = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
                    
                    # Subtract buffer for safety
                    timestamp_with_buffer = timestamp - self.time_buffer
                    
                    logger.info(
                        f"Found checkpoint: {timestamp_str} (using {timestamp_with_buffer.isoformat()}Z with buffer)",
                        extra={
                            "checkpoint_key": self.checkpoint_key,
                            "last_timestamp": timestamp_str,
                            "buffer_minutes": self.time_buffer.total_seconds() / 60,
                        },
                    )
                    
                    return timestamp_with_buffer
            
            # No checkpoint found
            logger.info(
                f"No checkpoint found for {self.checkpoint_key}, using default",
                extra={"checkpoint_key": self.checkpoint_key, "default": default},
            )
            return default
            
        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            
            if error_code == "ResourceNotFoundException":
                raise ValueError(
                    f"DynamoDB table not found: {self.table_name}. "
                    "Did you create the table with Terraform?"
                )
            else:
                logger.error(
                    f"Failed to read checkpoint: {e}",
                    extra={"checkpoint_key": self.checkpoint_key},
                )
                raise
    
    def update_checkpoint(
        self,
        timestamp: datetime,
        records_count: int,
        cursor: Optional[str] = None,
        job_id: Optional[str] = None,
        extra_metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Update checkpoint with new extraction state
        
        Args:
            timestamp: New last extracted timestamp (should be max timestamp from extracted data)
            records_count: Number of records extracted in this run
            cursor: API cursor (optional, for pagination resumption)
            job_id: Job identifier (for audit trail)
            extra_metadata: Additional metadata to store
        
        ATOMICITY NOTE:
        This update is atomic in DynamoDB. If multiple processes try to update
        simultaneously, last write wins. This is acceptable for checkpoints
        (we prefer latest timestamp).
        
        For stronger consistency, use conditional updates:
        - Only update if our timestamp > current timestamp
        - Prevents older job from overwriting newer checkpoint
        """
        timestamp_str = timestamp.strftime("%Y-%m-%dT%H:%M:%SZ")
        
        try:
            # Get current checkpoint for comparison
            current_response = self.table.get_item(
                Key={"checkpoint_key": self.checkpoint_key}
            )
            
            current_item = current_response.get("Item", {})
            total_records = current_item.get("records_extracted", 0) + records_count
            
            # Prepare update item
            item = {
                "checkpoint_key": self.checkpoint_key,
                "last_extracted_timestamp": timestamp_str,
                "records_extracted": total_records,
                "records_extracted_last_run": records_count,
                "updated_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
            }
            
            if cursor:
                item["last_extracted_cursor"] = cursor
            
            if job_id:
                item["updated_by"] = job_id
            
            if extra_metadata:
                item["metadata"] = json.dumps(extra_metadata)
            
            # Write to DynamoDB
            self.table.put_item(Item=item)
            
            logger.info(
                f"Updated checkpoint to {timestamp_str}",
                extra={
                    "checkpoint_key": self.checkpoint_key,
                    "timestamp": timestamp_str,
                    "records_count": records_count,
                    "total_records": total_records,
                },
            )
            
        except ClientError as e:
            logger.error(
                f"Failed to update checkpoint: {e}",
                extra={
                    "checkpoint_key": self.checkpoint_key,
                    "timestamp": timestamp_str,
                },
            )
            raise
    
    def get_checkpoint_metadata(self) -> Optional[Dict[str, Any]]:
        """
        Get full checkpoint metadata
        
        Returns:
            Dict with all checkpoint fields or None if not found
        
        Useful for debugging and monitoring:
        - When was checkpoint last updated?
        - How many records extracted total?
        - What job updated it?
        """
        try:
            response = self.table.get_item(
                Key={"checkpoint_key": self.checkpoint_key}
            )
            
            if "Item" in response:
                return dict(response["Item"])
            
            return None
            
        except ClientError as e:
            logger.error(f"Failed to get checkpoint metadata: {e}")
            return None
    
    def reset_checkpoint(self, reason: str) -> None:
        """
        Reset checkpoint (delete from DynamoDB)
        
        WARNING: This will cause full historical re-extraction on next run.
        Only use for:
        - Backfill scenarios
        - Data quality issues requiring re-extraction
        - Testing
        
        Args:
            reason: Reason for reset (logged for audit)
        """
        try:
            self.table.delete_item(
                Key={"checkpoint_key": self.checkpoint_key}
            )
            
            logger.warning(
                f"Reset checkpoint for {self.checkpoint_key}",
                extra={"checkpoint_key": self.checkpoint_key, "reason": reason},
            )
            
        except ClientError as e:
            logger.error(f"Failed to reset checkpoint: {e}")
            raise


# WRITE-AUDIT-COMMIT PATTERN (Advanced):
#
# For critical pipelines, implement 3-stage checkpoint update:
#
# 1. WRITE stage:
#    - Write data to S3 with temporary location
#    - Example: s3://bucket/temp/run_id=abc123/
#
# 2. AUDIT stage:
#    - Verify write succeeded (count records, check file sizes)
#    - Validate data quality (no nulls in key columns, etc.)
#
# 3. COMMIT stage:
#    - Move data to final location (or just mark as committed)
#    - Update checkpoint
#    - Example: s3://bucket/final/dt=2026-02-12/
#
# If any stage fails:
# - WRITE failed → No checkpoint update, retry on next run
# - AUDIT failed → Data in temp location, no checkpoint update, investigate
# - COMMIT failed → Data ready but not committed, manual intervention
#
# This prevents checkpoint drift (checkpoint ahead of actual data).


# MONITORING RECOMMENDATIONS:
#
# 1. Alert on checkpoint age:
#    - If checkpoint not updated in >2 hours (for hourly jobs)
#    - Indicates job failures or stuck jobs
#
# 2. Alert on checkpoint lag:
#    - lag = current_time - last_extracted_timestamp
#    - If lag >4 hours (for hourly extraction)
#    - Indicates falling behind real-time
#
# 3. Track extraction rate:
#    - records_per_hour = records_extracted_last_run / 1 hour
#    - Monitor for sudden drops (API issues, data volume changes)
#
# 4. CloudWatch Dashboard:
#    - Graph: checkpoint timestamp over time (should be continuously increasing)
#    - Graph: records extracted per run (should be relatively stable)
#    - Graph: checkpoint lag (should stay below SLA threshold)
