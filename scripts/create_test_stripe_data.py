"""
Create test Stripe charges for ETL pipeline testing

PURPOSE:
    Generate thousands of test charges so our Spark ETL job has data to extract.
    Charges are evenly distributed across a date range to simulate real usage.

USAGE:
    python scripts/create_test_stripe_data.py --charges 5000 --days 7

REQUIREMENTS:
    pip install stripe click

BEFORE RUNNING:
    Set your Stripe test API key:
    export STRIPE_API_KEY=sk_test_...
    
    Or update yambo/dev/stripe-api secret in AWS Secrets Manager
"""

import os
import sys
import time
import random
from datetime import datetime, timedelta
from typing import Optional

try:
    import stripe
    import click
except ImportError:
    print("ERROR: Required packages not installed!")
    print("\nInstall with:")
    print("  pip install stripe click")
    sys.exit(1)


def get_stripe_api_key() -> Optional[str]:
    """Get Stripe API key from environment or AWS Secrets Manager"""
    
    # Try environment variable first
    api_key = os.getenv("STRIPE_API_KEY")
    if api_key:
        return api_key
    
    # Try AWS Secrets Manager (if boto3 available)
    try:
        import boto3
        import json
        
        secrets_client = boto3.client('secretsmanager', region_name='eu-central-1')
        response = secrets_client.get_secret_value(SecretId='yambo/dev/stripe-api')
        secret = json.loads(response['SecretString'])
        return secret.get('STRIPE_API_KEY')
    except Exception as e:
        print(f"Warning: Could not get API key from Secrets Manager: {e}")
        return None


@click.command()
@click.option(
    '--charges',
    '-c',
    default=1000,
    type=int,
    help='Number of charges to create (default: 1000)'
)
@click.option(
    '--days',
    '-d',
    default=7,
    type=int,
    help='Days to spread charges across (default: 7)'
)
@click.option(
    '--api-key',
    '-k',
    default=None,
    help='Stripe API key (or use STRIPE_API_KEY env var)'
)
@click.option(
    '--progress-interval',
    default=100,
    type=int,
    help='Show progress every N charges (default: 100)'
)
def create_test_charges(
    charges: int,
    days: int,
    api_key: Optional[str],
    progress_interval: int
):
    """
    Create test Stripe charges for ETL pipeline testing
    
    This script creates realistic test data to validate your Spark ETL pipeline.
    Charges are distributed evenly across the date range to simulate real usage patterns.
    
    Examples:
    
        Create 5000 charges over 7 days:
        $ python scripts/create_test_stripe_data.py -c 5000 -d 7
        
        Create 10000 charges over 30 days:
        $ python scripts/create_test_stripe_data.py -c 10000 -d 30
    """
    
    # Get API key
    if not api_key:
        api_key = get_stripe_api_key()
    
    if not api_key:
        click.echo(click.style("ERROR: No Stripe API key found!", fg='red', bold=True))
        click.echo("\nSet API key with:")
        click.echo("  export STRIPE_API_KEY=sk_test_...")
        click.echo("Or:")
        click.echo("  python scripts/create_test_stripe_data.py --api-key sk_test_...")
        sys.exit(1)
    
    # Validate API key format
    if not api_key.startswith('sk_test_'):
        click.echo(click.style("WARNING: API key should start with 'sk_test_' for test mode!", fg='yellow'))
        if not click.confirm("Continue anyway?"):
            sys.exit(1)
    
    # Set Stripe API key
    stripe.api_key = api_key
    
    # Header
    click.echo(click.style("=" * 60, fg='green'))
    click.echo(click.style("Creating Test Stripe Charges", fg='green', bold=True))
    click.echo(click.style("=" * 60, fg='green'))
    click.echo(f"\n  Charges:   {charges:,}")
    click.echo(f"  Days:      {days}")
    click.echo(f"  Per Day:   {charges // days:,} charges/day")
    click.echo(f"  Per Hour:  {charges // (days * 24):,} charges/hour")
    click.echo()
    
    # Calculate time distribution
    now = datetime.utcnow()
    start_date = now - timedelta(days=days)
    total_seconds = int((now - start_date).total_seconds())
    seconds_per_charge = total_seconds / charges
    
    click.echo(f"Date Range:")
    click.echo(f"  Start: {start_date.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    click.echo(f"  End:   {now.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    click.echo()
    
    # Confirm
    if not click.confirm(f"Create {charges:,} test charges?"):
        click.echo("Aborted.")
        sys.exit(0)
    
    click.echo("\n" + click.style("Creating charges...", fg='green'))
    click.echo(f"Progress shown every {progress_interval} charges\n")
    
    # Counters
    success_count = 0
    fail_count = 0
    start_time = time.time()
    
    # Currency distribution (80% USD, 10% EUR, 10% GBP)
    currencies = ['usd'] * 80 + ['eur'] * 10 + ['gbp'] * 10
    
    # Customer IDs (simulate 100 different customers)
    customer_ids = [f"test_customer_{i:03d}" for i in range(100)]
    
    try:
        for i in range(1, charges + 1):
            # Calculate timestamp for this charge (evenly distributed)
            charge_offset = int(i * seconds_per_charge)
            charge_date = start_date + timedelta(seconds=charge_offset)
            
            # Random amount between $10 and $500 (in cents)
            amount = random.randint(1000, 50000)
            
            # Random currency
            currency = random.choice(currencies)
            
            # Random customer
            customer_id = random.choice(customer_ids)
            
            # Create charge
            try:
                charge = stripe.Charge.create(
                    amount=amount,
                    currency=currency,
                    source="tok_visa",  # Test token
                    description=f"Test charge {i} - {customer_id}",
                    metadata={
                        "customer_id": customer_id,
                        "order_id": f"order_{int(charge_date.timestamp())}_{i}",
                        "test_data": "true",
                        "created_by": "create_test_stripe_data.py"
                    }
                )
                success_count += 1
                
            except stripe.error.StripeError as e:
                fail_count += 1
                if fail_count <= 10:  # Only show first 10 errors
                    click.echo(click.style(f"  Error creating charge {i}: {str(e)}", fg='red'))
            
            # Progress update
            if i % progress_interval == 0:
                percent = (i * 100) // charges
                elapsed = time.time() - start_time
                rate = i / elapsed if elapsed > 0 else 0
                eta_seconds = (charges - i) / rate if rate > 0 else 0
                
                click.echo(
                    click.style(
                        f"Progress: {i:,} / {charges:,} ({percent}%) | "
                        f"Rate: {rate:.1f} charges/sec | "
                        f"ETA: {int(eta_seconds)}s",
                        fg='green'
                    )
                )
            
            # Rate limiting: Max 100 requests/second (Stripe test mode limit)
            time.sleep(0.01)
            
    except KeyboardInterrupt:
        click.echo(click.style("\n\nInterrupted by user!", fg='yellow'))
    
    # Summary
    total_time = time.time() - start_time
    click.echo("\n" + click.style("=" * 60, fg='green'))
    click.echo(click.style("Charge Creation Complete!", fg='green', bold=True))
    click.echo(click.style("=" * 60, fg='green'))
    click.echo(f"\nResults:")
    click.echo(click.style(f"  ✅ Successful: {success_count:,}", fg='green'))
    
    if fail_count > 0:
        click.echo(click.style(f"  ❌ Failed:     {fail_count:,}", fg='red'))
    
    click.echo(f"  📊 Total:      {charges:,}")
    click.echo(f"  ⏱️  Time:       {total_time:.1f} seconds")
    click.echo(f"  🚀 Rate:       {success_count / total_time:.1f} charges/sec")
    
    click.echo(f"\nDate Range:")
    click.echo(f"  📅 Start: {start_date.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    click.echo(f"  📅 End:   {now.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    
    # Next steps
    click.echo(click.style("\n🎯 Next Steps:", fg='cyan', bold=True))
    click.echo("\n1. Verify charges in Stripe Dashboard:")
    click.echo("   https://dashboard.stripe.com/test/payments")
    
    click.echo("\n2. Run the Spark ETL job:")
    click.echo("   kubectl apply -f k8s/spark-application-extract.yaml")
    
    click.echo("\n3. Check job logs:")
    click.echo("   kubectl logs yambo-extract-job-driver -n spark-jobs --follow")
    
    click.echo("\n4. Verify data in S3:")
    click.echo("   aws s3 ls s3://yambo-dev-data-lake/raw/transactions/ --recursive")
    
    click.echo("\n5. Check DynamoDB checkpoints:")
    click.echo("   aws dynamodb scan --table-name yambo-dev-checkpoints")
    click.echo()


if __name__ == '__main__':
    create_test_charges()
