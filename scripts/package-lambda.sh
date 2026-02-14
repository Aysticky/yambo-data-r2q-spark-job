#!/bin/bash
# Package Lambda function for deployment

set -e

echo "Packaging Lambda function..."

# Create temp directory
TEMP_DIR=$(mktemp -d)
LAMBDA_DIR="$(dirname "$0")/../lambda/spark_job_trigger"
OUTPUT_ZIP="$(dirname "$0")/../terraform/modules/scheduler/lambda_function.zip"

# Copy Lambda code
cp "$LAMBDA_DIR/handler.py" "$TEMP_DIR/"
cp "$LAMBDA_DIR/requirements.txt" "$TEMP_DIR/"

# Install dependencies
cd "$TEMP_DIR"
pip install -r requirements.txt -t . --quiet

# Create ZIP
zip -r "$OUTPUT_ZIP" . > /dev/null

# Cleanup
cd -
rm -rf "$TEMP_DIR"

echo "Lambda package created: $OUTPUT_ZIP"
echo "Size: $(du -h "$OUTPUT_ZIP" | cut -f1)"
