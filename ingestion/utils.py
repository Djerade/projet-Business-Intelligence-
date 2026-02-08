"""
Utilities for data ingestion.
Handles retry logic, error handling, and common functions.
"""

import os
import time
import logging
from typing import Optional, Dict, Any
from retrying import retry
import requests
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def get_env_var(key: str, default: Optional[str] = None) -> str:
    """
    Get environment variable with optional default value.
    
    Args:
        key: Environment variable name
        default: Default value if not found
        
    Returns:
        Environment variable value
        
    Raises:
        ValueError: If variable is not found and no default provided
    """
    value = os.getenv(key, default)
    if value is None:
        raise ValueError(f"Environment variable {key} is not set and no default provided")
    return value


@retry(
    stop_max_attempt_number=3,
    wait_exponential_multiplier=1000,
    wait_exponential_max=10000,
    retry_on_exception=lambda e: isinstance(e, (requests.RequestException, ConnectionError))
)
def make_api_request(
    url: str,
    params: Optional[Dict[str, Any]] = None,
    headers: Optional[Dict[str, str]] = None,
    timeout: int = 30
) -> requests.Response:
    """
    Make HTTP request with retry logic.
    
    Args:
        url: API endpoint URL
        params: Query parameters
        headers: Request headers
        timeout: Request timeout in seconds
        
    Returns:
        Response object
        
    Raises:
        requests.RequestException: If request fails after retries
    """
    try:
        response = requests.get(url, params=params, headers=headers, timeout=timeout)
        response.raise_for_status()
        return response
    except requests.RequestException as e:
        logger.error(f"API request failed: {url} - {str(e)}")
        raise


def save_to_parquet(data: Any, filepath: str) -> None:
    """
    Save data to Parquet file.
    
    Args:
        data: DataFrame or data structure to save
        filepath: Full path to output file
    """
    import pandas as pd
    
    # Ensure directory exists
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    
    if isinstance(data, pd.DataFrame):
        data.to_parquet(filepath, index=False, engine='pyarrow')
        logger.info(f"Data saved to {filepath}")
    else:
        raise ValueError(f"Unsupported data type: {type(data)}")


def get_data_path(subdirectory: str, filename: str) -> str:
    """
    Get full path for data file.
    
    Args:
        subdirectory: Subdirectory in data folder (raw or processed)
        filename: Name of the file
        
    Returns:
        Full file path
    """
    base_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data', subdirectory)
    os.makedirs(base_path, exist_ok=True)
    return os.path.join(base_path, filename)


def format_date_for_api(date: datetime) -> str:
    """
    Format date for API requests (YYYY-MM-DD).
    
    Args:
        date: Datetime object
        
    Returns:
        Formatted date string
    """
    return date.strftime('%Y-%m-%d')


def validate_country_code(code: str) -> bool:
    """
    Validate ISO country code format (2 or 3 characters).
    
    Args:
        code: Country code to validate
        
    Returns:
        True if valid format
    """
    return isinstance(code, str) and len(code) in [2, 3] and code.isalpha()
