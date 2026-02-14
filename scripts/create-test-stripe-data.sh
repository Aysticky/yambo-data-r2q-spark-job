#!/bin/bash
# Script to create test Stripe charges for ETL pipeline testing
#
# PURPOSE:
# Generate thousands of test charges so our Spark ETL job has data to extract
# We run this once to populate the test environment
#
# USAGE:
#   ./scripts/create-test-stripe-data.sh -c 5000 -d 7
#
# OPTIONS:
#   -c    Number of charges to create (default: 1000)
#   -d    Days to spread charges across (default: 7)
#   -h    Show help

set -e

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Default values
CHARGES=1000
DAYS=7

usage() {
    echo "Usage: $0 -c <charges> -d <days>"
    echo ""
    echo "Options:"
    echo "  -c    Number of charges to create (default: 1000)"
    echo "  -d    Days to spread charges across (default: 7)"
    echo "  -h    Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 -c 5000 -d 7     # Create 5000 charges over 7 days"
    echo "  $0 -c 10000 -d 30   # Create 10000 charges over 30 days"
    exit 1
}

while getopts "c:d:h" opt; do
    case $opt in
        c) CHARGES=$OPTARG ;;
        d) DAYS=$OPTARG ;;
        h) usage ;;
        *) usage ;;
    esac
done

echo -e "${GREEN}Creating $CHARGES test Stripe charges over $DAYS days...${NC}"

# Check if Stripe CLI is installed
if ! command -v stripe &> /dev/null; then
    echo -e "${RED}Error: Stripe CLI not found!${NC}"
    echo ""
    echo "Install Stripe CLI:"
    echo "  macOS:   brew install stripe/stripe-cli/stripe"
    echo "  Windows: scoop install stripe"
    echo "  Linux:   https://stripe.com/docs/stripe-cli"
    exit 1
fi

# Check if logged in to Stripe
if ! stripe config --list &> /dev/null; then
    echo -e "${YELLOW}Not logged in to Stripe. Running login...${NC}"
    stripe login
fi

# Get current timestamp
NOW=$(date +%s)

# Calculate seconds per charge to spread evenly
SECONDS_PER_DAY=86400
TOTAL_SECONDS=$((DAYS * SECONDS_PER_DAY))
SECONDS_PER_CHARGE=$((TOTAL_SECONDS / CHARGES))

echo -e "${GREEN}Starting charge creation...${NC}"
echo "Progress will be shown every 100 charges"

# Counter for progress
SUCCESS_COUNT=0
FAIL_COUNT=0

# Create charges
for i in $(seq 1 $CHARGES); do
    # Calculate timestamp for this charge (evenly distributed)
    CHARGE_OFFSET=$((i * SECONDS_PER_CHARGE))
    CHARGE_TIMESTAMP=$((NOW - TOTAL_SECONDS + CHARGE_OFFSET))
    
    # Random amount between $10 and $500
    AMOUNT=$((RANDOM % 49000 + 1000))
    
    # Random currency (mostly USD, some EUR/GBP for variety)
    RAND_CURR=$((RANDOM % 100))
    if [ $RAND_CURR -lt 80 ]; then
        CURRENCY="usd"
    elif [ $RAND_CURR -lt 90 ]; then
        CURRENCY="eur"
    else
        CURRENCY="gbp"
    fi
    
    # Random customer ID (simulate 100 different customers)
    CUSTOMER_ID=$((RANDOM % 100))
    
    # Create charge using Stripe CLI
    # Note: Stripe CLI test mode automatically creates test charges
    if stripe charges create \
        --amount=$AMOUNT \
        --currency=$CURRENCY \
        --source=tok_visa \
        --description="Test charge $i - Customer $CUSTOMER_ID" \
        --metadata[customer_id]="test_customer_$CUSTOMER_ID" \
        --metadata[order_id]="order_$(date +%s)_$i" \
        --metadata[test_data]="true" \
        > /dev/null 2>&1; then
        SUCCESS_COUNT=$((SUCCESS_COUNT + 1))
    else
        FAIL_COUNT=$((FAIL_COUNT + 1))
    fi
    
    # Show progress every 100 charges
    if [ $((i % 100)) -eq 0 ]; then
        PERCENT=$((i * 100 / CHARGES))
        echo -e "${GREEN}Progress: $i / $CHARGES charges created ($PERCENT%)${NC}"
    fi
    
    # Small delay to avoid rate limiting (10 charges per second)
    sleep 0.1
done

echo ""
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}Charge Creation Complete!${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""
echo "Results:"
echo "  ✅ Successful: $SUCCESS_COUNT"
echo "  ❌ Failed:     $FAIL_COUNT"
echo "  📊 Total:      $CHARGES"
echo ""
echo "Date Range:"
START_DATE=$(date -d "@$((NOW - TOTAL_SECONDS))" '+%Y-%m-%d %H:%M:%S' 2>/dev/null || date -r $((NOW - TOTAL_SECONDS)) '+%Y-%m-%d %H:%M:%S')
END_DATE=$(date -d "@$NOW" '+%Y-%m-%d %H:%M:%S' 2>/dev/null || date -r $NOW '+%Y-%m-%d %H:%M:%S')
echo "  📅 Start: $START_DATE"
echo "  📅 End:   $END_DATE"
echo ""
echo -e "${GREEN}Next Steps:${NC}"
echo "1. Verify charges in Stripe Dashboard:"
echo "   https://dashboard.stripe.com/test/payments"
echo ""
echo "2. Run the Spark ETL job:"
echo "   kubectl apply -f k8s/spark-application-extract.yaml"
echo ""
echo "3. Check job logs:"
echo "   kubectl logs yambo-extract-job-driver -n spark-jobs --follow"
echo ""
echo "4. Verify data in S3:"
echo "   aws s3 ls s3://yambo-dev-data-lake/raw/transactions/ --recursive"
echo ""
