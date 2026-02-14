"""
Logging configuration for Spark jobs

ENTERPRISE LOGGING BEST PRACTICES:
1. Use structured logging (JSON format) for easy parsing in CloudWatch/Splunk
2. Include correlation IDs to trace requests across distributed systems
3. Log at appropriate levels:
   - DEBUG: Detailed info for troubleshooting (pagination cursors, etc.)
   - INFO: Important milestones (job start, API call success, checkpoint updates)
   - WARNING: Recoverable errors (rate limit hit, retry triggered)
   - ERROR: Failures requiring attention (API down, invalid data)
4. Never log sensitive data (API keys, PII, passwords)
5. Use context managers for timing operations
6. Include metadata: job_id, run_id, partition_id, executor_id

COMMON ISSUES TO WATCH IN LOGS:
- "Rate limit exceeded" → Need to implement backoff or reduce concurrency
- "OutOfMemoryError" → Executor memory too small or data skew
- "Connection timeout" → Network issues or API slow/down
- "Duplicate records detected" → Idempotency issue in writes
"""

import logging
import sys
import json
from datetime import datetime
from typing import Any, Dict, Optional
from contextlib import contextmanager
import time


class StructuredFormatter(logging.Formatter):
    """
    JSON formatter for structured logging

    CloudWatch Insights can parse JSON logs automatically,
    making it easy to query and create dashboards.
    """

    def format(self, record: logging.LogRecord) -> str:
        """Format log record as JSON"""
        log_data: Dict[str, Any] = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }

        # Add exception info if present
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)

        # Add custom fields from extra parameter
        if hasattr(record, "job_id"):
            log_data["job_id"] = record.job_id
        if hasattr(record, "run_id"):
            log_data["run_id"] = record.run_id
        if hasattr(record, "partition_id"):
            log_data["partition_id"] = record.partition_id
        if hasattr(record, "api_endpoint"):
            log_data["api_endpoint"] = record.api_endpoint
        if hasattr(record, "duration_ms"):
            log_data["duration_ms"] = record.duration_ms

        return json.dumps(log_data)


def setup_logging(
    log_level: str = "INFO",
    structured: bool = True,
    job_id: Optional[str] = None,
) -> logging.Logger:
    """
    Configure logging for Spark job

    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR)
        structured: Use JSON structured logging (recommended for production)
        job_id: Unique job identifier to include in all logs

    Returns:
        Configured logger instance

    USAGE:
        logger = setup_logging(log_level="INFO", job_id="extract-2026-02-12")
        logger.info("Starting data extraction", extra={"records_count": 1000})
    """
    logger = logging.getLogger("yambo")
    logger.setLevel(getattr(logging, log_level.upper()))

    # Remove existing handlers
    logger.handlers = []

    # Console handler
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(getattr(logging, log_level.upper()))

    # Use appropriate formatter
    if structured:
        formatter = StructuredFormatter()
    else:
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    handler.setFormatter(formatter)
    logger.addHandler(handler)

    # Add default context
    if job_id:
        logger = logging.LoggerAdapter(logger, {"job_id": job_id})

    return logger


@contextmanager
def log_execution_time(logger: logging.Logger, operation: str, **extra_fields):
    """
    Context manager to log operation execution time

    USAGE:
        with log_execution_time(logger, "API call to /charges", endpoint="/charges"):
            response = fetch_data()

    This is critical for identifying performance bottlenecks:
    - If "API call" takes >5s, the API might be slow
    - If "Spark write" takes >10min, you might have data skew
    - If "Token refresh" takes >2s, network issues or auth service slow
    """
    start_time = time.time()
    logger.info(f"Starting: {operation}", extra=extra_fields)

    try:
        yield
    except Exception as e:
        duration_ms = int((time.time() - start_time) * 1000)
        logger.error(
            f"Failed: {operation}",
            extra={"duration_ms": duration_ms, "error": str(e), **extra_fields},
            exc_info=True,
        )
        raise
    else:
        duration_ms = int((time.time() - start_time) * 1000)
        logger.info(
            f"Completed: {operation}",
            extra={"duration_ms": duration_ms, **extra_fields},
        )


def mask_sensitive_data(data: str, visible_chars: int = 4) -> str:
    """
    Mask sensitive data for logging

    USAGE:
        logger.info(f"Using API key: {mask_sensitive_data(api_key)}")
        # Output: "Using API key: sk_l****"

    NEVER log full API keys, tokens, or passwords. This function helps.
    """
    if not data or len(data) <= visible_chars:
        return "****"
    return data[:visible_chars] + "****"


# PRODUCTION MONITORING TIPS:
#
# 1. Set up CloudWatch Log Insights queries for common patterns:
#    - fields @timestamp, level, message | filter level = "ERROR"
#    - stats count() by operation | sort count desc
#    - fields duration_ms | stats avg(duration_ms), max(duration_ms)
#
# 2. Create CloudWatch alarms for:
#    - Error rate > 1% of total logs
#    - Average duration > 2x baseline
#    - No logs for > 15 minutes (job might be stuck)
#
# 3. Common log analysis queries:
#    - "How many API rate limit hits today?" → filter message like /Rate limit/
#    - "Which partition is slowest?" → stats max(duration_ms) by partition_id
#    - "What's causing OutOfMemory?" → filter module = "executor" and level = "ERROR"
#
# 4. Log volume considerations:
#    - DEBUG logs in prod can generate GBs per hour → expensive
#    - Use INFO for prod, DEBUG only when troubleshooting
#    - Implement log sampling for high-frequency operations (e.g., log every 100th API call)
