"""
DummyJSON API Data Ingestor

Ingests data from the DummyJSON API with support for
paginated endpoints and robust error handling.

Features:
- Flexible endpoint configuration
- Automatic pagination with configurable batch sizes
- Comprehensive data validation
- Error handling and automatic retry
- Detailed metrics and logging
- Resource management
"""

from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, asdict, field
from enum import Enum
from datetime import datetime
import json
import logging
import os

import pandas as pd
from sqlalchemy import Engine
from loguru import logger
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

from ingestion.base_generator import BaseIngestionGenerator, IngestionException, IngestionMetrics, IngestionStatus
from ingestion.ingestion_util import APIClient, DatabaseWriter, APIConfig, DatabaseConfig, APIError

# Configuration
class DummyJSONEndpoint(Enum):
    """ Available Endpoints in DummyJSON API. """
    CARTS = "carts"
    PRODUCTS = "products"
    USERS = "users"

# PaginationConfig
@dataclass
class PaginationConfig:
    """ Configuration for pagination behaviour. """
    initial_skip: int = 0
    limit: int = 100
    max_results: Optional[int] = None

    def __post_init__(self):
        """ Validation pagination config. """
        if self.initial_skip < 0:
            raise ValueError("initial_skip must be greater than or equal to 0")
        if self.limit <= 0:
            raise ValueError("limit must be greater than 0")
        if self.max_results is not None and self.max_results <= 0:
            raise ValueError("max_results must be greater than 0")

# EndpointConfig
@dataclass
class EndpointConfig:
    """ Configuration for DummyJSON endpoint. """
    endpoint: DummyJSONEndpoint
    table_name: str
    pagination: PaginationConfig = field(default_factory=PaginationConfig)
    required_fields: List[str] = field(default_factory=list)
    response_key: str = ""
    validate_data: bool = True
    sanitize_data: bool = True

# DummyJSONConfig
@dataclass
class DummyJSONConfig:
    """ Configuration for DummyJSON Config. """
    base_url: str = os.getenv("DUMMYJSON_API", "https://dummyjson.com")
    endpoints: Tuple[EndpointConfig, ...] = ()
    fetch_timeout: int = 60
    max_retries: int = 3
    backoff_factor: float = 2.0
    batch_size: int = 5000
    schema: str = "bronze"

    def __post_init__(self):
        """Validate and initialize configuration."""
        if not self.base_url or not isinstance(self.base_url, str):
            raise ValueError("base_url must be a non-empty string")
        if not self.base_url.startswith(("http://", "https://")):
            raise ValueError("base_url must start with http:// or https://")
        if self.batch_size <= 0:
            raise ValueError("batch_size must be greater than 0")

        # If no endpoints provided, use default carts endpoint
        if not self.endpoints:
            self.endpoints = (
                EndpointConfig(
                    endpoint=DummyJSONEndpoint.CARTS,
                    table_name="carts_raw",
                    response_key="carts",
                    required_fields=["id", "userId", "total"],
                ),
            )

# Default EndpointsConfig
DEFAULT_ENDPOINTS = {
    DummyJSONEndpoint.CARTS: EndpointConfig(
        endpoint=DummyJSONEndpoint.CARTS,
        table_name="carts_raw",
        response_key="carts",
        required_fields=["id", "userId", "total"],
    ),
    DummyJSONEndpoint.PRODUCTS: EndpointConfig(
        endpoint=DummyJSONEndpoint.PRODUCTS,
        table_name="products_raw",
        response_key="products",
        required_fields=["id", "title", "price"],
    ),
    DummyJSONEndpoint.USERS: EndpointConfig(
        endpoint=DummyJSONEndpoint.USERS,
        table_name="users_raw",
        response_key="users",
        required_fields=["id", "username", "email"],
    ),
}

# Data Validation
class DummyJSONValidator:
    """ Validation logic for DummyJSON API data. """

    # Validate endpoints to check for valid fields and data deuplications
    @staticmethod
    def validate_endpoint_data(
        df: pd.DataFrame,
        config: EndpointConfig,
    ) -> Tuple[bool, Optional[str]]:
        """
        Validate data from an endpoint.
        
        Args:
            df: DataFrame to validate
            config: Endpoint configuration with required fields
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        if df.empty:
            return False, f"{config.endpoint.value} DataFrame is empty"
        
        # check required fields
        missing_fields = set(config.required_fields) - set(df.columns)
        if missing_fields:
            return False, f"Missing required fields: {missing_fields}"
        
        # check for id uniqueness if id exists
        if "id" in df.columns:
            if df["id"].duplicated().any():
                duplicate_count = df["id"].duplicated().sum()
                return False, f"{duplicate_count} duplicated IDs were found"

            # if id is numeric
            if not pd.api.types.is_numeric_dtype(df["id"]):
                return False, "ID field must be numeric"

        return True, None

    # validate_carts function
    @staticmethod
    def validate_carts(df: pd.DataFrame) -> Tuple[bool, Optional[str]]:
        """Validate carts-specific data."""
        is_valid, error = DummyJSONValidator.validate_endpoint_data(
            df,
            DEFAULT_ENDPOINTS[DummyJSONEndpoint.CARTS]
        )
        if not is_valid:
            return False, error
        
        # Validate numeric fields
        if "total" in df.columns:
            if not pd.api.types.is_numeric_dtype(df["total"]):
                return False, "total field must be numeric"
            if (df["total"] < 0).any():
                return False, "total cannot be negative"
        
        if "discountedTotal" in df.columns:
            if not pd.api.types.is_numeric_dtype(df["discountedTotal"]):
                return False, "discountedTotal field must be numeric"
        
        return True, None
        
    @staticmethod
    def validate_products(df: pd.DataFrame) -> Tuple[bool, Optional[str]]:
        """Validate products-specific data."""
        is_valid, error = DummyJSONValidator.validate_endpoint_data(
            df,
            DEFAULT_ENDPOINTS[DummyJSONEndpoint.PRODUCTS]
        )
        if not is_valid:
            return False, error
        
        # Validate price
        if "price" in df.columns:
            if not pd.api.types.is_numeric_dtype(df["price"]):
                return False, "price field must be numeric"
            if (df["price"] < 0).any():
                return False, "price cannot be negative"
        
        return True, None
    
    @staticmethod
    def validate_users(df: pd.DataFrame) -> Tuple[bool, Optional[str]]:
        """Validate users-specific data."""
        is_valid, error = DummyJSONValidator.validate_endpoint_data(
            df,
            DEFAULT_ENDPOINTS[DummyJSONEndpoint.USERS]
        )
        if not is_valid:
            return False, error
        
        # Validate email format
        if "email" in df.columns:
            if not df["email"].str.contains("@", na=False).all():
                return False, "Invalid email format detected"
        
        return True, None


    # get_validator function
    @staticmethod
    def get_validator(endpoint: DummyJSONEndpoint):
        """ Get validator function for an endpoint. """
        validators = {
            DummyJSONEndpoint.CARTS: DummyJSONValidator.validate_carts,
            DummyJSONEndpoint.PRODUCTS: DummyJSONValidator.validate_products,
            DummyJSONEndpoint.USERS: DummyJSONValidator.validate_users,
        }

        return validators.get(endpoint, DummyJSONValidator.validate_endpoint_data)


# DummyJSON ingestor
class DummyJSONIngestor(BaseIngestionGenerator):
    """ Ingestor for DummyJSON API data. """

    def __init__(
        self,
        engine: Engine,
        config: Optional[DummyJSONConfig] = None,
        api_config: Optional[APIConfig] = None,
        db_config: Optional[DatabaseConfig] = None,
    ):
        """Initialize DummyJSON ingestor."""
        # Initialize configuration
        self.config = config or DummyJSONConfig()
        self.api_config = api_config or APIConfig(
            timeout=self.config.fetch_timeout,
            max_retries=self.config.max_retries,
        )
        self.db_config = db_config or DatabaseConfig(
            schema=self.config.schema,
        )

        # Initialize clients
        self.api_client = APIClient(config=self.api_config)
        self.db_writer = DatabaseWriter(engine, config=self.db_config)

        # Save config object before calling super().__init__()
        config_obj = self.config

        # Initialize base class
        super().__init__(
            source_name="dummyjson",
            engine=engine,
            config=None,
            max_retries=self.config.max_retries,
        )

        # Restore the DummyJSONConfig object (super().__init__() would overwrite it with a dict)
        self.config = config_obj

        # Store DataFrames and metrics
        self._dataframes: Dict[str, pd.DataFrame] = {}
        self._endpoint_metrics: Dict[str, Dict[str, Any]] = {}
        self._raw_data: Dict[str, List[Dict[str, Any]]] = {}
        
        # FIX #1: Initialize _endpoints dictionary
        self._endpoints: Dict[str, EndpointConfig] = {}
        for endpoint_config in self.config.endpoints:
            endpoint_name = endpoint_config.endpoint.value
            self._endpoints[endpoint_name] = endpoint_config

        logger.info(
            f"Initialized DummyJSON ingestor with {len(self.config.endpoints)} endpoints",
            extra={"endpoint_count": len(self.config.endpoints)}
        )

    def fetch_data(self) -> List[Dict[str, Any]]:
        """
        Fetch data from all configured DummyJSON endpoints.
        
        Returns:
            Combined list of all fetched records
            
        Raises:
            IngestionException: If fetch fails
        """
        all_data = []
        self._raw_data = {}

        try:
            for endpoint_config in self.config.endpoints:
                endpoint_name = endpoint_config.endpoint.value
                logger.info(
                    f"Fetching {endpoint_name} from DummyJSON",
                    extra={"endpoint": endpoint_name}
                )

                # Construct URL
                url = f"{self.config.base_url}/{endpoint_name}"

                # Fetch paginated data if needed
                if endpoint_config.pagination:
                    logger.debug(f"Using pagination for {endpoint_name}")
                    data = self.api_client.fetch_paginated(
                        url=url,
                        page_param="skip",
                        page_size_param="limit",
                        response_key=endpoint_config.response_key,
                    )
                else:
                    logger.debug(f"Fetching all {endpoint_name} without pagination")
                    response = self.api_client.fetch(url)
                    data = response.get(endpoint_config.response_key, response)
                    if isinstance(data, dict):
                        data = [data]

                self._raw_data[endpoint_name] = data
                
                # FIX #1: Store data in _dataframes dictionary for use in load_data()
                if data:
                    df = pd.DataFrame(data)
                    self._dataframes[endpoint_name] = df
                    logger.debug(f"Stored {len(df)} records in _dataframes['{endpoint_name}']")
                
                all_data.extend(data)

                logger.info(
                    f"Fetched {endpoint_name}",
                    extra={"endpoint": endpoint_name, "record_count": len(data)}
                )

            return all_data

        except Exception as e:
            raise IngestionException(
                f"Failed to fetch data from DummyJSON: {str(e)}",
                self.source_name
            )

    def _ensure_columns_exist(self, df: pd.DataFrame, table_name: str) -> None:
        """
        Ensure all DataFrame columns exist in the target table.
        Creates missing columns dynamically.
        
        This method prevents the "column does not exist" error by checking
        the database schema and adding missing columns before attempting to write.
        """
        from sqlalchemy import inspect, text, NUMERIC, VARCHAR
        
        try:
            logger.debug(f"Validating columns for table {table_name}")
            
            # Get existing table columns
            inspector = inspect(self.engine)
            if not inspector.has_table(table_name, schema=self.config.schema):
                logger.info(f"Table {table_name} does not exist yet, will be created by to_sql")
                return
            
            existing_cols = {col['name'] for col in inspector.get_columns(table_name, schema=self.config.schema)}
            df_cols = set(df.columns)
            missing_cols = df_cols - existing_cols
            
            if missing_cols:
                logger.warning(f"Missing columns in {table_name}: {missing_cols}. Creating them...")
                
                with self.engine.connect() as conn:
                    for col in missing_cols:
                        # Determine appropriate SQL type based on pandas dtype
                        if df[col].dtype in ['float64', 'float32']:
                            sql_type = "NUMERIC(15, 2)"
                        elif df[col].dtype in ['int64', 'int32', 'int16']:
                            sql_type = "INTEGER"
                        else:
                            sql_type = "TEXT"
                        
                        alter_sql = f'ALTER TABLE "{self.config.schema}"."{table_name}" ADD COLUMN "{col}" {sql_type};'
                        
                        try:
                            conn.execute(text(alter_sql))
                            conn.commit()
                            logger.info(f"Created column {col} ({sql_type}) in {table_name}")
                        except Exception as e:
                            logger.warning(f"Could not create column {col}: {e}. Will attempt write anyway.")
                            conn.rollback()
        
        except Exception as e:
            logger.warning(f"Error validating columns: {e}. Proceeding with write attempt.")


    def load_data(self, data: List[Dict[str, Any]]) -> int:
        """
        Load fetched data into bronze layer by endpoint.
        
        Args:
            data: Combined list of all fetched records
            
        Returns:
            Number of records loaded
            
        Raises:
            IngestionException: If loading fails
        """
        total_loaded = 0
    
        try:
            for endpoint_name, endpoint_config in self._endpoints.items():
                logger.info(
                    f"Loading {endpoint_name} to bronze layer",
                    extra={"endpoint": endpoint_name, "table": endpoint_config.table_name}
                )
                
                df = self._dataframes[endpoint_name]
                if df is None or df.empty:
                    logger.warning(f"No data to load for {endpoint_name}")
                    continue
                
                # Validate data
                self._validate_endpoint_data(endpoint_config, df)
                
                # Sanitize data
                df = self._sanitize_endpoint_data(endpoint_config, df)
                
                # Ensure columns exist before write
                self._ensure_columns_exist(df, endpoint_config.table_name)
                
                # Write to database
                records_loaded = self.db_writer.write(
                    df=df,
                    table_name=endpoint_config.table_name,
                    validate=False,
                    required_columns=endpoint_config.required_fields,
                )
                
                total_loaded += records_loaded
                
                # Store endpoint-specific metrics
                self._endpoint_metrics[endpoint_name] = {
                    "endpoint": endpoint_name,
                    "table": endpoint_config.table_name,
                    "records_loaded": records_loaded,
                    "columns": len(df.columns),
                    "memory_usage_mb": df.memory_usage(deep=True).sum() / (1024 ** 2),
                }
                
                logger.info(
                    f"Loaded {endpoint_name} to bronze",
                    extra={
                        "endpoint": endpoint_name,
                        "table": endpoint_config.table_name,
                        "records_loaded": records_loaded,
                    }
                )
            
            return total_loaded
            
        except Exception as e:
            logger.error("Failed to load data to bronze", exc_info=True)
            raise IngestionException(
                "Failed to load data to bronze",
                self.source_name,
                records_failed=sum(len(df) for df in self._dataframes.values() if df is not None)
            )

    def _validate_endpoint_data(self, config: EndpointConfig, df: pd.DataFrame) -> None:
        """
        Validate data for an endpoint.
        
        Args:
            config: Endpoint configuration
            df: DataFrame to validate
            
        Raises:
            IngestionException: If validation fails
        """
        logger.debug(
            f"Validating {config.endpoint.value} data",
            extra={"endpoint": config.endpoint.value}
        )
        
        # Get endpoint-specific validator
        validator_func = DummyJSONValidator.get_validator(config.endpoint)
        
        # Run validation
        if config.endpoint in [DummyJSONEndpoint.CARTS, DummyJSONEndpoint.PRODUCTS, DummyJSONEndpoint.USERS]:
            is_valid, error_msg = validator_func(df)
        else:
            is_valid, error_msg = validator_func(df, config)
        
        if not is_valid:
            raise IngestionException(
                f"Validation failed for {config.endpoint.value}: {error_msg}",
                self.source_name,
                records_failed=len(df)
            )
        
        logger.info(
            f"Validated {config.endpoint.value}",
            extra={
                "endpoint": config.endpoint.value,
                "record_count": len(df),
            }
        )

    def _sanitize_endpoint_data(self, config: EndpointConfig, df: pd.DataFrame) -> pd.DataFrame:
        """
        Sanitize data for an endpoint.
        
        Args:
            config: Endpoint configuration
            df: DataFrame to sanitize
            
        Returns:
            Sanitized DataFrame
        """
        logger.debug(
            f"Sanitizing {config.endpoint.value} data",
            extra={"endpoint": config.endpoint.value}
        )
        
        # Remove completely null rows
        df = df.dropna(how="all")
        
        # Serialize complex columns (lists/dicts) to JSON
        for col in df.columns:
            if df[col].dtype == "object":
                # Check if any values are dicts or lists
                sample = df[col].dropna().iloc[0] if len(df[col].dropna()) > 0 else None
                if sample is not None and isinstance(sample, (dict, list)):
                    df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x)
        
        # Add ingestion metadata
        df["_ingestion_timestamp"] = self.metrics.start_time
        df["_endpoint"] = config.endpoint.value
        df["_table_name"] = config.table_name
        
        # Endpoint-specific sanitization
        if config.endpoint == DummyJSONEndpoint.CARTS:
            # Round monetary values to 2 decimal places
            for col in ["total", "discountedTotal"]:
                if col in df.columns:
                    df[col] = df[col].round(2)
        
        elif config.endpoint == DummyJSONEndpoint.PRODUCTS:
            # Round price to 2 decimal places
            if "price" in df.columns:
                df["price"] = df["price"].round(2)
            
            # Ensure title is string
            if "title" in df.columns:
                df["title"] = df["title"].astype(str)
        
        elif config.endpoint == DummyJSONEndpoint.USERS:
            # Standardize email casing
            if "email" in df.columns:
                df["email"] = df["email"].str.lower().str.strip()
            
            # Standardize username
            if "username" in df.columns:
                df["username"] = df["username"].str.lower().str.strip()
        
        logger.info(
            f"Sanitized {config.endpoint.value}",
            extra={
                "endpoint": config.endpoint.value,
                "records_before": len(self._dataframes[config.endpoint.value]),
                "records_after": len(df),
            }
        )
        
        return df

    def get_endpoint_metrics(self, endpoint_name: str) -> Dict[str, Any]:
        """
        Get metrics for a specific endpoint.
        
        Args:
            endpoint_name: Name of the endpoint
            
        Returns:
            Dictionary of endpoint-specific metrics
        """
        if endpoint_name not in self._endpoint_metrics:
            return {}
        
        return self._endpoint_metrics[endpoint_name]
    
    def get_all_metrics(self) -> Dict[str, Any]:
        """
        Get comprehensive metrics for all endpoints.
        
        Returns:
            Dictionary of metrics by endpoint
        """
        all_metrics = super().get_metrics()
        
        # Add endpoint-specific metrics
        endpoint_metrics = {}
        for endpoint_config in self.config.endpoints:
            endpoint_name = endpoint_config.endpoint.value
            endpoint_metrics[endpoint_name] = self.get_endpoint_metrics(endpoint_name)
        
        all_metrics["endpoints"] = endpoint_metrics
        return all_metrics
    
    def close(self) -> None:
        """Close API client and database connections."""
        try:
            self.api_client.close()
            logger.info("DummyJSON ingestor closed")
        except Exception as e:
            logger.error(f"Error closing ingestor: {e}")


# Convenience Function (Backward Compatible)

def fetch_dummyjson_carts(
    engine: Engine,
    base_url: str = os.getenv("DUMMYJSON_API", "https://dummyjson.com"),
    pagination_limit: int = 100,
    max_results: Optional[int] = None,
) -> IngestionMetrics:
    """
    Fetch carts from DummyJSON API and load to bronze layer.
    
    Backward compatible with original simple function.
    
    Args:
        engine: SQLAlchemy database engine
        base_url: DummyJSON API base URL
        pagination_limit: Records per page
        max_results: Maximum total records to fetch
        
    Returns:
        IngestionMetrics with ingestion results
        
    Raises:
        IngestionException: If ingestion fails
    """
    config = DummyJSONConfig(
        base_url=base_url,
        endpoints=(
            EndpointConfig(
                endpoint=DummyJSONEndpoint.CARTS,
                table_name="carts_raw",
                response_key="carts",
                required_fields=["id", "userId", "total"],
                pagination=PaginationConfig(
                    limit=pagination_limit,
                    max_results=max_results,
                ),
            ),
        ),
    )
    
    ingestor = DummyJSONIngestor(engine=engine, config=config)
    try:
        return ingestor.run_ingestion()
    finally:
        ingestor.close()