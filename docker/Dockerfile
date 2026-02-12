# Multi-stage Dockerfile for Spark jobs on EKS
#
# DESIGN DECISIONS:
# 1. Multi-stage build: Separate build and runtime stages for smaller images
# 2. Base image: apache/spark-py:v3.5.0 (official Spark image)
# 3. Non-root user: Run as spark user (UID 185) for security
# 4. Layer caching: Install dependencies before copying code
# 5. Health check: None (managed by K8s liveness/readiness probes)
#
# BUILD:
#   docker build -t yambo-spark-job:v1.0.0 -f docker/Dockerfile .
#
# RUN LOCALLY:
#   docker run --rm \
#     -e AWS_REGION=eu-central-1 \
#     -e ENVIRONMENT=dev \
#     yambo-spark-job:v1.0.0 \
#     --job-type check
#
# PUSH TO ECR:
#   aws ecr get-login-password --region eu-central-1 | docker login --username AWS --password-stdin <account>.dkr.ecr.eu-central-1.amazonaws.com
#   docker tag yambo-spark-job:v1.0.0 <account>.dkr.ecr.eu-central-1.amazonaws.com/yambo-spark-job:v1.0.0
#   docker push <account>.dkr.ecr.eu-central-1.amazonaws.com/yambo-spark-job:v1.0.0

# Stage 1: Base Spark image
FROM apache/spark-py:v3.5.0 as base

# Metadata
LABEL maintainer="yambo-platform-team"
LABEL description="Yambo Spark data pipeline for REST API to S3"
LABEL version="1.0.0"

# Switch to root for installations
USER root

# Install system dependencies
# - curl: Health checks and debugging
# - procps: Process monitoring (ps, top)
# - tini: Init system for proper signal handling
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        curl=7.88.1-10+deb12u5 \
        procps=2:4.0.2-3 \
        tini=0.19.0-1 && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONPATH=/opt/spark/work-dir:$PYTHONPATH \
    SPARK_HOME=/opt/spark \
    PATH=$SPARK_HOME/bin:$PATH

# Create work directory
WORKDIR /opt/spark/work-dir

# Stage 2: Dependencies
FROM base as dependencies

# Copy dependency files
COPY docker/requirements.txt /tmp/requirements.txt

# Install Python dependencies
# - Use pip install with --no-cache-dir to reduce image size
# - Pin pip version for reproducibility
RUN pip install --no-cache-dir --upgrade pip==23.3.1 && \
    pip install --no-cache-dir -r /tmp/requirements.txt && \
    rm /tmp/requirements.txt

# Stage 3: Application
FROM dependencies as application

# Copy Spark configuration
COPY docker/spark-defaults.conf $SPARK_HOME/conf/spark-defaults.conf

# Copy application code
# Copy source code
COPY src/ /opt/spark/work-dir/src/

# Copy main entry point
COPY src/spark_jobs/main.py /opt/spark/work-dir/

# Copy README for reference
COPY README.md /opt/spark/work-dir/

# Set ownership (spark user exists in base image)
RUN chown -R spark:spark /opt/spark/work-dir

# Switch to non-root user
USER spark

# Set working directory
WORKDIR /opt/spark/work-dir

# Default entrypoint (can be overridden by spark-submit)
ENTRYPOINT ["/usr/bin/tini", "--", "python", "-m", "src.spark_jobs.main"]

# Default arguments (can be overridden)
CMD ["--job-type", "check", "--environment", "dev"]

# DOCKER IMAGE SIZE OPTIMIZATION:
#
# Current size: ~800MB (base Spark image is ~700MB)
#
# Further optimizations:
# 1. Use distroless base (reduces to ~600MB but harder to debug)
# 2. Multi-arch builds (arm64 for Graviton instances)
# 3. Layer caching in CI/CD (reuse dependency layers)
# 4. Minimize installed packages (remove curl/procps in prod)
#
# SECURITY CONSIDERATIONS:
#
# 1. Non-root user (spark:spark)
# 2. No secrets in image (use AWS Secrets Manager)
# 3. Pinned dependency versions (supply chain security)
# 4. Minimal base image (reduced attack surface)
# 5. Regular vulnerability scanning (Trivy, Snyk)
#
# PRODUCTION ISSUES ENCOUNTERED:
#
# Issue 1: /tmp directory permission errors
# - Symptom: Spark fails with "Permission denied: /tmp/spark-xxx"
# - Cause: /tmp owned by root, spark user can't write
# - Fix: RUN mkdir -p /tmp/spark && chown spark:spark /tmp/spark
#
# Issue 2: Missing system libraries for pyarrow
# - Symptom: ImportError: libarrow.so.1400: cannot open shared object
# - Cause: System arrow library not installed
# - Fix: Install libarrow-dev in apt-get (if using system packages)
#        OR use pyarrow wheel (already includes libraries)
#
# Issue 3: DNS resolution failures in EKS
# - Symptom: Can't resolve S3 endpoints, API endpoints
# - Cause: EKS DNS configuration (CoreDNS)
# - Fix: Configure proper security groups, VPC DNS settings
#        Add retries for transient DNS failures
#
# Issue 4: OOM kills in K8s
# - Symptom: Executor pods killed with exit code 137
# - Cause: Memory requests/limits too high, JVM overhead not accounted
# - Fix: Set spark.kubernetes.memoryOverheadFactor=0.4
#        Right-size executor memory (request < limit)
#
# Issue 5: Slow docker builds in CI/CD
# - Symptom: Each build takes 10+ minutes
# - Cause: Not using layer caching, reinstalling deps every time
# - Fix: Use BuildKit caching, cache pip downloads
#        COPY requirements first, then code (better caching)
