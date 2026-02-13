"""
Data ingestion utilities for API and database operations.

This module provides robust, production-ready functions for:
- Fetching data from REST APIs with retry logic and error handling
- Writing data to databases with validation and logging
- Managing connections and resources efficiently
"""

from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass
from enum import Enum
import logging
from functools import wraps
import time
from datetime import datetime, timezone

import requests
import pandas as pd
from sqlalchemy import Engine, text
from sqlalchemy.exc import SQLAlchemyError
from loguru import logger


# Configuration and Constants

class HTTPMethod(Enum):
    """HTTP methods supported for API calls."""
    GET = "GET"
    POST = "POST"
    PUT = "PUT"
    DELETE = "DELETE"


@dataclass
class APIConfig:
    """Configuration for API requests."""
    timeout: int = 30
    max_retries: int = 3
    backoff_factor: float = 2.0
    backoff_base: int = 1
    chunk_size: int = 1000
    allowed_status_codes: Tuple[int, ...] = (200, 201)

    def __post_init__(self):
        """Validate configuration values."""
        if self.timeout <= 0:
            raise ValueError("timeout must be greater than 0")
        if self.max_retries < 0:
            raise ValueError("max_retries must be non-negative")
        if self.backoff_factor <= 1:
            raise ValueError("backoff_factor must be greater than 1")


@dataclass
class DatabaseConfig:
    """Configuration for database operations."""
    schema: str
    batch_size: int = 10000
    if_exists: str = "replace"
    dtype: Optional[Dict[str, Any]] = None
    index: bool = False

    def __post_init__(self):
        """Validate configuration values."""
        if not self.schema or not isinstance(self.schema, str):
            raise ValueError("schema must be a non-empty string")
        if self.batch_size <= 0:
            raise ValueError("batch_size must be greater than 0")
        if self.if_exists not in ("fail", "replace", "append"):
            raise ValueError("if_exists must be 'fail', 'replace', or 'append'")


# Custom Exceptions

class DataIngestionError(Exception):
    """Base exception for data ingestion errors."""
    pass


class APIError(DataIngestionError):
    """Exception raised for API-related errors."""
    def __init__(self, message: str, url: str, status_code: Optional[int] = None):
        self.message = message
        self.url = url
        self.status_code = status_code
        super().__init__(message)


class DatabaseError(DataIngestionError):
    """Exception raised for database-related errors."""
    def __init__(self, message: str, table_name: str, records_failed: int = 0):
        self.message = message
        self.table_name = table_name
        self.records_failed = records_failed
        super().__init__(message)


class DataValidationError(DataIngestionError):
    """Exception raised for data validation errors."""
    def __init__(self, message: str, field: Optional[str] = None):
        self.message = message
        self.field = field
        super().__init__(message)


# Decorators

def retry_with_backoff(
    max_retries: int = 3,
    backoff_factor: float = 2.0,
    backoff_base: int = 1,
    exception_types: Tuple[Exception, ...] = (requests.RequestException,),
):
    """
    Decorator to retry a function on exception with exponential backoff.
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except exception_types as e:
                    last_exception = e
                    
                    if attempt < max_retries:
                        delay = backoff_base * (backoff_factor ** attempt)
                        logger.warning(
                            f"Attempt {attempt + 1}/{max_retries + 1} failed for {func.__name__}. "
                            f"Retrying in {delay:.2f}s. Error: {str(e)}"
                        )
                        time.sleep(delay)
                    else:
                        logger.error(
                            f"All {max_retries + 1} attempts failed for {func.__name__}. "
                            f"Final error: {str(e)}"
                        )
            
            raise last_exception
        
        return wrapper
    return decorator


# API Data Fetching

class APIClient:
    """Robust HTTP client for fetching data from REST APIs."""
    
    def __init__(self, config: Optional[APIConfig] = None):
        """Initialize API client."""
        self.config = config or APIConfig()
        self.session = self._create_session()
    
    def _create_session(self) -> requests.Session:
        """Create and configure requests session."""
        session = requests.Session()
        session.headers.update({
            "User-Agent": "DataIngestionClient/1.0",
            "Accept": "application/json",
        })
        return session
    
    def _validate_url(self, url: str) -> None:
        """Validate URL format."""
        if not url or not isinstance(url, str):
            raise ValueError("URL must be a non-empty string")
        if not url.startswith(("http://", "https://")):
            raise ValueError("URL must start with 'http://' or 'https://'")

    def _validate_params(self, params: Optional[Dict[str, Any]]) -> None:
        """Validate request parameters."""
        if params is not None and not isinstance(params, dict):
            raise ValueError("params must be a dictionary or None")

    @retry_with_backoff(exception_types=(requests.RequestException,))
    def fetch(
        self,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        method: HTTPMethod = HTTPMethod.GET,
        headers: Optional[Dict[str, str]] = None,
        **kwargs
    ) -> Dict[str, Any]:
        """
        Fetch data from API endpoint with automatic retry.
        """
        # Validate inputs
        self._validate_url(url)
        self._validate_params(params)

        # Prepare request
        request_headers = self.session.headers.copy()
        if headers:
            request_headers.update(headers)
        
        logger.debug(
            f"Fetching data from {url}",
            extra={"method": method.value, "params": params}
        )

        try:
            # Make request
            response = self.session.request(
                method=method.value,
                url=url,
                params=params,
                headers=request_headers,
                timeout=self.config.timeout,
                **kwargs
            )

            # Check status code
            if response.status_code not in self.config.allowed_status_codes:
                raise APIError(
                    f"HTTP {response.status_code}: {response.reason}",
                    url=url,
                    status_code=response.status_code
                )
            
            # Parse response
            try:
                data = response.json()
            except ValueError as e:
                raise DataValidationError(
                    f"Failed to parse JSON response: {str(e)}"
                )
            
            # Validate response format
            if not isinstance(data, (dict, list)):
                raise DataValidationError(
                    f"Expected dict or list, got {type(data).__name__}"
                )

            logger.info(
                f"Successfully fetched data from {url}",
                extra={
                    "status_code": response.status_code,
                    "content_length": len(response.content),
                    "url": url
                }
            )

            return data

        except APIError:
            raise
        except requests.RequestException as e:
            raise APIError(
                f"Request failed: {str(e)}",
                url=url
            )
        
    def fetch_paginated(
        self,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        page_param: str = "offset",
        page_size_param: str = "limit",
        response_key: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Fetch paginated data from API.
        """
        all_records = []
        offset = 0
        base_params = params or {}

        logger.info(
            f"Starting paginated fetch from {url}",
            extra={"page_size": self.config.chunk_size}
        )

        while True:
            # Build request params
            request_params = base_params.copy()
            request_params[page_param] = offset
            request_params[page_size_param] = self.config.chunk_size

            # Fetch page
            response = self.fetch(url, params=request_params)

            # Extract data from response
            if response_key:
                if not isinstance(response, dict) or response_key not in response:
                    raise DataValidationError(
                        f"Response missing expected key: {response_key}"
                    )
                batch = response[response_key]
            else:
                batch = response if isinstance(response, list) else [response]

            if not batch:
                break

            all_records.extend(batch)
            offset += len(batch)

            logger.debug(
                f"Fetched page with {len(batch)} records. Total: {len(all_records)}"
            )
            
            # Stop if we got fewer records than requested (last page)
            if len(batch) < self.config.chunk_size:
                break

        logger.info(
            f"Completed paginated fetch: {len(all_records)} total records",
            extra={
                "url": url,
                "record_count": len(all_records)
            }
        )

        return all_records

    def close(self) -> None:
        """Close the session."""
        self.session.close()


# Data Validation

class DataValidator:
    """Validate and sanitize data before database insertion."""

    @staticmethod
    def validate_dataframe(
        df: pd.DataFrame,
        required_columns: Optional[List[str]] = None,
        allow_duplicates: bool = True,
    ) -> bool:
        """
        Validate DataFrame integrity.
        """
        # Check if empty
        if df.empty:
            raise DataValidationError("DataFrame is empty")
        
        # Check required columns
        if required_columns:
            missing_cols = set(required_columns) - set(df.columns)
            if missing_cols:
                raise DataValidationError(
                    f"Missing required columns: {missing_cols}",
                    field="columns"
                )
        
        # Check for duplicates
        if not allow_duplicates:
            duplicates = df.duplicated().sum()
            if duplicates > 0:
                logger.warning(
                    f"Found {duplicates} duplicate rows in DataFrame"
                )

        return True

    @staticmethod
    def validate_column_types(
        df: pd.DataFrame,
        expected_dtypes: Dict[str, str],
    ) -> bool:
        """
        Validate column data types.
        """
        for column, expected_dtype in expected_dtypes.items():
            if column not in df.columns:
                raise DataValidationError(
                    f"Column '{column}' not found",
                    field=column
                )
            
            actual_dtype = str(df[column].dtype)
            if actual_dtype != expected_dtype:
                logger.warning(
                    f"Column '{column}' has type {actual_dtype}, "
                    f"expected {expected_dtype}"
                )
            
        return True

    @staticmethod
    def sanitize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
        """
        Sanitize DataFrame for database insertion.
        
        - Remove empty rows
        - Handle null values
        - Add metadata columns
        """
        # Remove completely null rows
        df = df.dropna(how="all")

        # Add metadata
        df["_ingestion_timestamp"] = datetime.now(timezone.utc)

        logger.debug(
            f"Sanitized DataFrame: {len(df)} records remain after cleaning"
        )

        return df

# Database Operations

class DatabaseWriter:
    """
    Robust database writer with validation and error handling.
    
    Features:
    - Transaction management
    - Data validation
    - Batch processing
    - Comprehensive logging
    """

    def __init__(self, engine: Engine, config: Optional[DatabaseConfig] = None):
        """
        Initialize database writer.
        """
        if engine is None:
            raise ValueError("engine cannot be None")

        self.engine = engine
        self.config = config or DatabaseConfig(schema="public")
        self._validate_engine()

    def _validate_engine(self) -> None:
        """Validate database connection."""
        try:
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info("Database connection validated successfully")
        except SQLAlchemyError as e:
            raise DatabaseError(
                f"Failed to validate database connection: {str(e)}",
                table_name="unknown"
            )

    def write(
        self,
        df: pd.DataFrame,
        table_name: str,
        validate: bool = True,
        required_columns: Optional[List[str]] = None,
    ) -> int:
        """
        Write DataFrame to database with validation and error handling.
        """
        if not table_name or not isinstance(table_name, str):
            raise ValueError("table_name must be a non-empty string")

        if df.empty:
            logger.warning(f"Attempting to write empty DataFrame to {table_name}")
            return 0

        # Validate data
        if validate:
            DataValidator.validate_dataframe(df, required_columns)
        
        # Sanitize data
        df = DataValidator.sanitize_dataframe(df)

        # Write to database
        try:
            logger.info(
                f"Writing {len(df)} records to {self.config.schema}.{table_name}",
                extra={
                    "table": table_name,
                    "schema": self.config.schema,
                    "record_count": len(df)
                }
            )

            df.to_sql(
                table_name,
                self.engine,
                schema=self.config.schema,
                if_exists=self.config.if_exists,
                index=self.config.index,
                dtype=self.config.dtype,
                chunksize=self.config.batch_size,
            )

            logger.info(
                f"Successfully wrote {len(df)} records to {self.config.schema}.{table_name}",
                extra={
                    "table": table_name,
                    "schema": self.config.schema,
                    "record_count": len(df),
                    "timestamp": datetime.now(timezone.utc).isoformat()
                }
            )

            return len(df)

        except SQLAlchemyError as e:
            logger.error(
                f"Failed to write {len(df)} records to {table_name}",
                exc_info=True
            )
            raise DatabaseError(
                f"Failed to write data to {table_name}",
                table_name=table_name,
                records_failed=len(df)
            )

    def write_batch(
        self,
        df: pd.DataFrame,
        table_name: str,
        batch_size: Optional[int] = None,
    ) -> int:
        """
        Write large DataFrame in batches.
        """
        batch_size = batch_size or self.config.batch_size
        total_written = 0

        logger.info(
            f"Starting batch write of {len(df)} records in batches of {batch_size}",
            extra={
                "table": table_name,
                "batch_size": batch_size
            }
        )

        for i in range(0, len(df), batch_size):
            batch = df.iloc[i : i + batch_size]
            written = self.write(batch, table_name, validate=False)
            total_written += written
            
            logger.debug(
                f"Batch {i // batch_size + 1} complete: {written} records written"
            )

        logger.info(
            f"Batch write complete: {total_written} total records written",
            extra={
                "table": table_name,
                "total_records": total_written
            }
        )

        return total_written

# Convenience Functions (Backward Compatible)

def fetch_api_data(
    url: str,
    params: Optional[Dict[str, Any]] = None,
    config: Optional[APIConfig] = None,
) -> Dict[str, Any]:
    """
    Fetch data from API endpoint.
    """
    client = APIClient(config=config)
    try:
        return client.fetch(url, params=params)
    finally:
        client.close()


def write_dataframe_to_db(
    df: pd.DataFrame,
    table_name: str,
    engine: Engine,
    schema: str = "public",
    config: Optional[DatabaseConfig] = None,
) -> int:
    """
    Write DataFrame to database.
    """
    db_config = config or DatabaseConfig(schema=schema)
    writer = DatabaseWriter(engine, config=db_config)
    return writer.write(df, table_name, validate=True)