"""
Main entry point for Spark jobs

This serves as a unified entry point that can route to different job types
based on command-line arguments.

USAGE:
  # Run check job
  python -m src.spark_jobs.main --job-type check
  
  # Run extract job
  python -m src.spark_jobs.main --job-type extract
  
  # Run with custom config
  python -m src.spark_jobs.main --job-type extract --environment prod

DOCKER/KUBERNETES USAGE:
  spark-submit \
    --master k8s://https://eks-cluster:443 \
    --deploy-mode cluster \
    --name yambo-extract-job \
    --conf spark.kubernetes.container.image=yambo-spark-job:v1.0.0 \
    local:///opt/spark/work-dir/src/spark_jobs/main.py \
    --job-type extract
"""

import sys
import argparse
from datetime import datetime

from src.spark_jobs import check_job, extract_job


def parse_args():
    """Parse command-line arguments"""
    parser = argparse.ArgumentParser(
        description="Yambo Spark Data Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument(
        "--job-type",
        type=str,
        required=True,
        choices=["check", "extract"],
        help="Type of job to run"
    )
    
    parser.add_argument(
        "--environment",
        type=str,
        default="dev",
        choices=["dev", "prod", "local"],
        help="Environment to run in (default: dev)"
    )
    
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="Date to process (YYYY-MM-DD). Defaults to today."
    )
    
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run in dry-run mode (no writes)"
    )
    
    return parser.parse_args()


def main():
    """Main entry point"""
    args = parse_args()
    
    print("=" * 80)
    print("Yambo Spark Data Pipeline")
    print("=" * 80)
    print(f"Job Type: {args.job_type}")
    print(f"Environment: {args.environment}")
    print(f"Timestamp: {datetime.utcnow().isoformat()}Z")
    print("=" * 80)
    
    # Set environment variable for config
    import os
    os.environ["ENVIRONMENT"] = args.environment
    
    if args.dry_run:
        os.environ["DRY_RUN"] = "true"
        print("DRY RUN MODE: No writes will be performed")
    
    # Route to appropriate job
    if args.job_type == "check":
        sys.path.insert(0, "/opt/spark/work-dir")  # For K8s deployment
        check_job.main()
    elif args.job_type == "extract":
        sys.path.insert(0, "/opt/spark/work-dir")  # For K8s deployment
        extract_job.main()
    else:
        print(f"Unknown job type: {args.job_type}")
        sys.exit(1)


if __name__ == "__main__":
    main()
