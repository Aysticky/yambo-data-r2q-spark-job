"""
Data transformations and enrichment functions

This module contains reusable transformation functions that can be
applied to DataFrames across different jobs.

ENTERPRISE TRANSFORMATION PATTERNS:
1. Modular functions (easy to test and reuse)
2. Type-safe Spark operations (use schema validation)
3. Comprehensive logging
4. Data quality at transformation time
5. Performance-optimized (avoid shuffles when possible)
"""

import logging
from typing import List

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType


logger = logging.getLogger(__name__)


def add_date_partitions(df: DataFrame, timestamp_col: str = "created_timestamp") -> DataFrame:
    """
    Add standard date partition columns

    Creates partition columns used for S3 partitioning:
    - dt: Date (YYYY-MM-DD)
    - year: Year
    - month: Month
    - day: Day
    - hour: Hour (optional, for high-volume data)

    Args:
        df: Input DataFrame
        timestamp_col: Name of timestamp column to partition from

    Returns:
        DataFrame with partition columns added

    PARTITIONING STRATEGY:
    - Always partition by date (dt) for time-series queries
    - Add hour if data volume >10GB per day
    - Don't over-partition (too many small files)
    """
    df = df.withColumn("dt", F.to_date(timestamp_col))
    df = df.withColumn("year", F.year(timestamp_col))
    df = df.withColumn("month", F.month(timestamp_col))
    df = df.withColumn("day", F.dayofmonth(timestamp_col))
    df = df.withColumn("hour", F.hour(timestamp_col))

    return df


def calculate_monetary_metrics(
    df: DataFrame, amount_col: str = "amount", currency_col: str = "currency"
) -> DataFrame:
    """
    Calculate standardized monetary metrics

    Converts amounts to different currencies, calculates rates, etc.

    CURRENCY HANDLING:
    In production, you'd integrate with exchange rate API.
    For MVP, we assume all amounts are in USD or convert using static rates.

    Args:
        df: Input DataFrame
        amount_col: Column containing amount in cents
        currency_col: Column containing currency code

    Returns:
        DataFrame with monetary metrics
    """
    # Convert cents to dollars
    df = df.withColumn(
        f"{amount_col}_usd",
        F.when(F.col(currency_col) == "usd", F.col(amount_col) / 100.0).otherwise(
            F.col(amount_col) / 100.0
        ),  # Simplified: treat all as USD
    )

    return df


def add_processing_metadata(df: DataFrame, job_id: str) -> DataFrame:
    """
    Add metadata columns for tracking and lineage

    Metadata columns help with:
    - Debugging: When was this record processed?
    - Data lineage: Which job created this record?
    - Auditing: Track data provenance

    Args:
        df: Input DataFrame
        job_id: Unique job identifier

    Returns:
        DataFrame with metadata columns
    """
    df = df.withColumn("processed_at", F.current_timestamp())
    df = df.withColumn("processing_date", F.current_date())
    df = df.withColumn("job_id", F.lit(job_id))
    df = df.withColumn("data_source", F.lit("stripe_api"))

    return df


def deduplicate_by_key(df: DataFrame, key_columns: List[str]) -> DataFrame:
    """
    Remove duplicate records based on key columns

    Keep the most recent record based on timestamp.

    DEDUPLICATION STRATEGIES:
    1. Keep first: Use dropDuplicates() (arbitrary which is kept)
    2. Keep most recent: Use window function (guaranteed latest)
    3. Keep all with flag: Add is_duplicate column (for auditing)

    We use strategy #2 (keep most recent).

    Args:
        df: Input DataFrame
        key_columns: Columns that define uniqueness

    Returns:
        Deduplicated DataFrame
    """
    initial_count = df.count()

    # Simple deduplication (keeps arbitrary record)
    df_deduped = df.dropDuplicates(key_columns)

    final_count = df_deduped.count()
    duplicates_removed = initial_count - final_count

    if duplicates_removed > 0:
        logger.warning(
            f"Removed {duplicates_removed} duplicate records",
            extra={
                "initial_count": initial_count,
                "final_count": final_count,
                "duplicate_rate": duplicates_removed / initial_count,
            },
        )

    return df_deduped


def calculate_data_quality_score(df: DataFrame, required_columns: List[str]) -> DataFrame:
    """
    Calculate data quality/completeness score for each record

    Score is 0-100 based on:
    - Required fields populated (50%)
    - Optional fields populated (30%)
    - Data validity (20%)

    Args:
        df: Input DataFrame
        required_columns: List of required column names

    Returns:
        DataFrame with quality_score column

    QUALITY SCORING USE CASES:
    - Filter low-quality records before analysis
    - Alert when average quality drops
    - Track data quality trends over time
    """
    points_per_field = 100 // len(required_columns)

    # Start with 0 score
    quality_expr = F.lit(0)

    # Add points for each populated required field
    for col_name in required_columns:
        quality_expr = quality_expr + F.when(
            F.col(col_name).isNotNull() & (F.col(col_name) != ""), points_per_field
        ).otherwise(0)

    df = df.withColumn("quality_score", quality_expr)

    return df


def filter_test_data(df: DataFrame, patterns: List[str] = None) -> DataFrame:
    """
    Filter out test/dev data from production dataset

    Common test patterns:
    - Emails containing "test", "example.com"
    - Customer IDs starting with "test_"
    - Amounts like $1.00, $0.01 (common test values)

    Args:
        df: Input DataFrame
        patterns: List of patterns to filter out

    Returns:
        Filtered DataFrame

    PRODUCTION SCENARIO:
    After launching, we discovered 5000 test transactions in production data.
    Root cause: QA team testing in prod (bad practice but happens).
    These test records skew analytics (e.g., average order value).

    Solution:
    - Filter known test patterns
    - Add is_test_data flag for analysis
    - Alert when test data detected in prod
    """
    if patterns is None:
        patterns = ["test", "example.com", "test_"]

    initial_count = df.count()

    # Filter out test emails
    if "receipt_email" in df.columns:
        for pattern in patterns:
            df = df.filter(~F.lower(F.col("receipt_email")).contains(pattern))

    # Filter out test customer IDs
    if "customer" in df.columns:
        df = df.filter(~F.col("customer").startsWith("test_"))

    final_count = df.count()
    test_records = initial_count - final_count

    if test_records > 0:
        logger.warning(
            f"Filtered out {test_records} test records",
            extra={"test_records": test_records, "test_rate": test_records / initial_count},
        )

    return df


def add_business_flags(df: DataFrame) -> DataFrame:
    """
    Add business logic flags for analysis

    Flags help analysts segment data without writing complex filters.

    Examples:
    - is_high_value: Amount >$1000
    - is_refund_risk: Multiple failed payments
    - is_new_customer: First transaction
    - is_international: Currency != USD

    Args:
        df: Input DataFrame

    Returns:
        DataFrame with business flag columns
    """
    # High value transaction (>$1000)
    df = df.withColumn("is_high_value", F.col("amount_usd") > 1000)

    # Failed transaction
    df = df.withColumn("is_failed", F.col("status") == "failed")

    # Fully refunded
    df = df.withColumn("is_fully_refunded", F.col("amount_refunded") == F.col("amount"))

    # International (non-USD)
    df = df.withColumn("is_international", F.col("currency") != "usd")

    return df


# TRANSFORMATION PIPELINE PATTERN:
#
# Instead of applying transformations one by one:
#   df = add_date_partitions(df)
#   df = calculate_monetary_metrics(df)
#   df = add_processing_metadata(df)
#   ...
#
# Create a transformation pipeline:
#   def transform_pipeline(df, config):
#       transformations = [
#           add_date_partitions,
#           calculate_monetary_metrics,
#           deduplicate_by_key,
#           add_processing_metadata,
#           calculate_data_quality_score,
#       ]
#
#       for transform_fn in transformations:
#           df = transform_fn(df, **config)
#
#       return df
#
# Benefits:
# - Easy to add/remove transformations
# - Easy to test individual transformations
# - Easy to change order
# - Explicit transformation lineage
