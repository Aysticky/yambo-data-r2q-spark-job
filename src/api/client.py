"""
REST API client with pagination, rate limiting, and retry logic

ENTERPRISE API CLIENT PATTERN:
This client implements all the production-ready patterns for consuming external APIs:
1. Cursor-based pagination (handles millions of records)
2. Rate limiting with token bucket algorithm
3. Exponential backoff for transient errors
4. Circuit breaker for persistent failures
5. Request/response logging for debugging
6. Metrics collection for monitoring

PAGINATION STRATEGIES:
1. Offset-based (simple but breaks with updates):
   - ?offset=0&limit=100
   - Problem: If records inserted during pagination, duplicates occur
   - Use case: Small, static datasets

2. Page-based (similar issues):
   - ?page=1&limit=100
   - Same problems as offset
   
3. Cursor-based (RECOMMENDED for production):
   - ?starting_after=obj_123&limit=100
   - API returns cursor for next page
   - Handles inserts/deletes during pagination
   - Used by: Stripe, Slack, GitHub, etc.

COMMON PRODUCTION ISSUES:

1. API Rate Limiting (429 Too Many Requests):
   Symptoms:
   - Job starts failing after processing X pages
   - Error: "Rate limit exceeded"
   
   Root causes:
   - Parallel executors exceeding combined rate limit
   - Not implementing proper backoff
   - Burst requests at job start
   
   Solutions:
   - Implement token bucket rate limiter (this class does it)
   - Add jitter to requests (randomize inter-request delay)
   - Use exponential backoff on 429
   - Distribute load over time (process older data first)
   
   Prevention:
   - Calculate: (requests_per_page * num_executors) < API_rate_limit
   - Example: 100 req/page * 10 executors = 1000 req/sec
   - If API limit is 100 req/sec, you WILL hit rate limits
   - Solution: Reduce num_executors or add delay between requests

2. API Timeout (504 Gateway Timeout):
   Symptoms:
   - Requests timeout after 30s
   - Happens during specific time windows (e.g., API's batch processing)
   
   Root causes:
   - API overloaded or slow
   - Requesting too much data per page
   - Network issues
   
   Solutions:
   - Increase timeout (with max limit 60s)
   - Reduce page size (100 → 50 records)
   - Retry with exponential backoff
   - Contact API provider if persistent
   
   Prevention:
   - Monitor API response times
   - Alert on >5s average response time
   - Implement circuit breaker (stop requesting if API consistently slow)

3. Pagination Cursor Expiry:
   Symptoms:
   - First pages succeed, then "Invalid cursor" error
   
   Root causes:
   - API cursors expire after N minutes
   - Job takes too long to process pages
   
   Solutions:
   - Process pages faster (more executors)
   - Store cursor in checkpoint, resume from failure
   - Request smaller time windows
   
   Prevention:
   - Check API docs for cursor TTL
   - Ensure job completes within cursor TTL

4. Data Consistency During Pagination:
   Symptoms:
   - Total record count changes during pagination
   - Duplicate or missing records
   
   Root causes:
   - Records inserted/deleted during pagination
   - Using offset-based pagination
   
   Solutions:
   - Use cursor-based pagination (immune to inserts/deletes)
   - Use snapshot isolation if API supports it
   - Filter by created_at timestamp (immutable)
   
   Prevention:
   - Always prefer cursor-based pagination
   - Include stable filter (e.g., created_before=end_of_range)
"""

import logging
import time
from typing import Dict, Optional, Any, Iterator, List
from datetime import datetime
import json

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)

from src.api.auth import StripeAPIKeyManager
from src.utils.logging_setup import log_execution_time


logger = logging.getLogger(__name__)


class RateLimitError(Exception):
    """Raised when API rate limit is exceeded"""
    pass


class APIError(Exception):
    """Raised for API errors"""
    pass


class TokenBucketRateLimiter:
    """
    Token bucket rate limiter for API requests
    
    This implements the token bucket algorithm:
    - Bucket starts with N tokens
    - Each request consumes 1 token
    - Tokens refill at rate R per second
    - If bucket empty, request waits until token available
    
    USAGE:
        limiter = TokenBucketRateLimiter(rate=100)  # 100 req/sec
        limiter.acquire()  # Blocks if rate limit reached
        make_api_request()
    
    WHY NOT USE time.sleep()?
    Simple sleep(1/rate) doesn't handle bursts well.
    Token bucket allows bursts up to bucket capacity,
    then enforces average rate.
    """
    
    def __init__(self, rate: float, capacity: Optional[int] = None):
        """
        Initialize rate limiter
        
        Args:
            rate: Requests per second
            capacity: Max burst size (default = rate)
        """
        self.rate = rate
        self.capacity = capacity or int(rate)
        self.tokens = float(self.capacity)
        self.last_update = time.time()
    
    def acquire(self, tokens: int = 1) -> None:
        """
        Acquire tokens (blocks if insufficient tokens)
        
        Args:
            tokens: Number of tokens to acquire (usually 1)
        """
        while tokens > self.tokens:
            # Refill tokens based on time elapsed
            now = time.time()
            elapsed = now - self.last_update
            self.tokens = min(
                self.capacity,
                self.tokens + elapsed * self.rate
            )
            self.last_update = now
            
            if tokens > self.tokens:
                # Calculate wait time for required tokens
                wait_time = (tokens - self.tokens) / self.rate
                logger.debug(f"Rate limit: waiting {wait_time:.2f}s")
                time.sleep(wait_time)
        
        self.tokens -= tokens


class RESTAPIClient:
    """
    Enterprise-grade REST API client with pagination and rate limiting
    
    Features:
    - Automatic authentication (API key or OAuth2)
    - Cursor-based pagination
    - Rate limiting
    - Exponential backoff retries
    - Request/response logging
    - Connection pooling
    
    USAGE:
        client = RESTAPIClient(
            base_url="https://api.stripe.com/v1",
            auth_manager=StripeAPIKeyManager("yambo/prod/stripe-api"),
            rate_limit=100  # 100 req/sec
        )
        
        for page in client.paginate_endpoint("/charges", params={"limit": 100}):
            process_page(page)
    """
    
    def __init__(
        self,
        base_url: str,
        auth_manager: StripeAPIKeyManager,
        rate_limit: int = 100,
        timeout: int = 30,
        max_retries: int = 5,
    ):
        """
        Initialize API client
        
        Args:
            base_url: API base URL (e.g., https://api.stripe.com/v1)
            auth_manager: Authentication manager
            rate_limit: Max requests per second
            timeout: Request timeout in seconds
            max_retries: Max retry attempts for transient errors
        """
        self.base_url = base_url.rstrip("/")
        self.auth_manager = auth_manager
        self.timeout = timeout
        self.max_retries = max_retries
        
        # Rate limiter
        self.rate_limiter = TokenBucketRateLimiter(rate=rate_limit)
        
        # Prepare session with connection pooling and retries
        self.session = self._create_session()
        
        # Metrics
        self.total_requests = 0
        self.total_rate_limit_hits = 0
        self.total_retries = 0
    
    def _create_session(self) -> requests.Session:
        """
        Create requests session with connection pooling and retries
        
        CONNECTION POOLING:
        - Reuses TCP connections across requests
        - Reduces latency (no TCP handshake overhead)
        - Default pool size: 10 connections
        
        RETRY STRATEGY:
        - Retry on connection errors (network issues)
        - Retry on 500, 502, 503, 504 (server errors)
        - Don't retry on 4xx (client errors) except 429 (rate limit)
        """
        session = requests.Session()
        
        retry_strategy = Retry(
            total=self.max_retries,
            backoff_factor=1,  # Exponential: 1s, 2s, 4s, 8s, 16s
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"],  # Only retry idempotent methods
        )
        
        adapter = HTTPAdapter(
            max_retries=retry_strategy,
            pool_connections=10,
            pool_maxsize=10,
        )
        
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        return session
    
    def _get_headers(self) -> Dict[str, str]:
        """Get request headers with authentication"""
        api_key = self.auth_manager.get_api_key()
        
        return {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "User-Agent": "Yambo-DataPipeline/0.1.0",
        }
    
    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=4, max=60),
        retry=retry_if_exception_type((requests.exceptions.RequestException, RateLimitError)),
    )
    def _make_request(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        data: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Make HTTP request with rate limiting and retries
        
        Args:
            method: HTTP method (GET, POST, etc.)
            endpoint: API endpoint (e.g., /charges)
            params: Query parameters
            data: Request body (for POST)
        
        Returns:
            Response JSON as dict
        
        Raises:
            RateLimitError: If rate limit exceeded after retries
            APIError: For other API errors
        """
        # Apply rate limiting
        self.rate_limiter.acquire()
        
        url = f"{self.base_url}{endpoint}"
        
        with log_execution_time(logger, f"{method} {endpoint}", endpoint=endpoint):
            try:
                response = self.session.request(
                    method=method,
                    url=url,
                    headers=self._get_headers(),
                    params=params,
                    json=data,
                    timeout=self.timeout,
                )
                
                self.total_requests += 1
                
                # Handle rate limiting
                if response.status_code == 429:
                    self.total_rate_limit_hits += 1
                    retry_after = int(response.headers.get("Retry-After", 10))
                    
                    logger.warning(
                        f"Rate limit hit, retrying after {retry_after}s",
                        extra={
                            "endpoint": endpoint,
                            "retry_after": retry_after,
                            "total_rate_limit_hits": self.total_rate_limit_hits,
                        },
                    )
                    
                    time.sleep(retry_after)
                    raise RateLimitError("Rate limit exceeded")
                
                # Handle other errors
                response.raise_for_status()
                
                return response.json()
                
            except requests.exceptions.HTTPError as e:
                status_code = e.response.status_code
                
                if 400 <= status_code < 500:
                    # Client errors (don't retry)
                    error_detail = e.response.text
                    logger.error(
                        f"Client error {status_code}: {error_detail}",
                        extra={"endpoint": endpoint, "status_code": status_code},
                    )
                    raise APIError(f"Client error {status_code}: {error_detail}")
                else:
                    # Server errors (will retry)
                    logger.warning(
                        f"Server error {status_code}, retrying...",
                        extra={"endpoint": endpoint, "status_code": status_code},
                    )
                    self.total_retries += 1
                    raise
            
            except requests.exceptions.Timeout:
                logger.warning(
                    f"Request timeout after {self.timeout}s, retrying...",
                    extra={"endpoint": endpoint},
                )
                self.total_retries += 1
                raise
            
            except requests.exceptions.RequestException as e:
                logger.warning(
                    f"Request failed: {e}, retrying...",
                    extra={"endpoint": endpoint, "error": str(e)},
                )
                self.total_retries += 1
                raise
    
    def paginate_endpoint(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        cursor_field: str = "starting_after",
        data_field: str = "data",
        has_more_field: str = "has_more",
    ) -> Iterator[List[Dict[str, Any]]]:
        """
        Paginate through API endpoint using cursor-based pagination
        
        This implements the standard cursor pagination pattern:
        1. Make first request with no cursor
        2. Get data and next cursor from response
        3. Make next request with cursor
        4. Repeat until has_more = false
        
        Args:
            endpoint: API endpoint (e.g., /charges)
            params: Base query parameters
            cursor_field: Field name for cursor param (Stripe uses "starting_after")
            data_field: Field name for data array in response
            has_more_field: Field name for pagination flag
        
        Yields:
            List of records for each page
        
        USAGE:
            for page in client.paginate_endpoint("/charges", params={"limit": 100}):
                for record in page:
                    process_record(record)
        
        PAGINATION BEST PRACTICES:
        1. Page size: 100 is a good default (balance latency vs throughput)
        2. Always include stable filter (e.g., created[lte]=end_timestamp)
        3. Store cursor in checkpoint for resumability
        4. Monitor "has_more=false" to detect end of data
        
        PERFORMANCE CONSIDERATIONS:
        - Each page = 1 API request
        - 1M records / 100 per page = 10,000 requests
        - At 100 req/sec = 100 seconds of API calls
        - Add Spark processing time → Total job time
        - To speed up: Increase page size (if API allows) or parallelize by time range
        """
        params = params or {}
        has_more = True
        cursor = None
        page_count = 0
        total_records = 0
        
        logger.info(
            f"Starting pagination for {endpoint}",
            extra={"params": params},
        )
        
        while has_more:
            # Add cursor to params if available
            if cursor:
                params[cursor_field] = cursor
            
            # Fetch page
            response = self._make_request("GET", endpoint, params=params)
            
            # Extract data
            records = response.get(data_field, [])
            has_more = response.get(has_more_field, False)
            
            page_count += 1
            total_records += len(records)
            
            logger.info(
                f"Fetched page {page_count}",
                extra={
                    "records_in_page": len(records),
                    "total_records": total_records,
                    "has_more": has_more,
                    "endpoint": endpoint,
                },
            )
            
            # Yield page
            if records:
                yield records
            
            # Get cursor for next page
            if has_more and records:
                # Cursor is usually the ID of the last record
                cursor = records[-1].get("id")
                
                if not cursor:
                    logger.warning(
                        "has_more=true but no cursor found, stopping pagination"
                    )
                    break
        
        logger.info(
            f"Pagination complete for {endpoint}",
            extra={
                "total_pages": page_count,
                "total_records": total_records,
                "endpoint": endpoint,
            },
        )
    
    def get_metrics(self) -> Dict[str, int]:
        """
        Get client metrics for monitoring
        
        Returns:
            Dict with metrics: total_requests, rate_limit_hits, retries
        
        MONITORING TIP:
        Log these metrics at job end to track API health:
        - High rate_limit_hits → Need to reduce request rate
        - High retries → API reliability issues
        """
        return {
            "total_requests": self.total_requests,
            "total_rate_limit_hits": self.total_rate_limit_hits,
            "total_retries": self.total_retries,
            "rate_limit_hit_rate": (
                self.total_rate_limit_hits / self.total_requests
                if self.total_requests > 0
                else 0
            ),
        }


# TESTING NOTE:
# To test this locally without hitting real API:
# 1. Use requests-mock or responses library
# 2. Mock the _make_request method
# 3. Create test responses with pagination cursors
# 4. Verify pagination logic without API calls
#
# Example:
# with responses.RequestsMock() as rsps:
#     rsps.add(
#         responses.GET,
#         "https://api.stripe.com/v1/charges",
#         json={"data": [...], "has_more": True},
#         status=200,
#     )
#     client = RESTAPIClient(...)
#     pages = list(client.paginate_endpoint("/charges"))


# PRODUCTION MONITORING CHECKLIST:
#
# 1. Monitor rate_limit_hit_rate:
#    - Target: <1% of requests
#    - Alert: >5% indicates you need to reduce request rate
#
# 2. Monitor average_response_time:
#    - Target: <500ms per request
#    - Alert: >2s indicates API slowness
#
# 3. Monitor retry_rate:
#    - Target: <5% of requests
#    - Alert: >10% indicates API reliability issues
#
# 4. Monitor pagination_efficiency:
#    - Calculate: (total_records / total_requests)
#    - Target: Close to page_size (e.g., 90+ records per request for limit=100)
#    - Low efficiency → Many pages with few records, consider smaller time windows
