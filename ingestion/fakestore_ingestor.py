"""
FakeStore API Data Ingestor

Ingests data from the FakeStore API with support for
multiple endpoints (orders, products, users) and robust error handling.

Features:
- Multi-endpoint ingestion
- Parallel data fetching
- Comprehensive validation
- Automatic retry and backoff
- Detailed metrics and logging
"""

from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, asdict
from enum import Enum
import logging
from datetime import datetime
import concurrent.futures
import os

import pandas as pd
from sqlalchemy import Engine
from loguru import logger
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Adjusted imports to match workspace filenames
from ingestion.base_generator import BaseIngestionGenerator, IngestionException, IngestionMetrics, IngestionStatus
from ingestion.ingestion_util import APIClient, DatabaseWriter, APIConfig, DatabaseConfig


# Configuration and Constants

class DataSource(Enum):
    """Available data sources in FakeStore API."""
    PRODUCTS = "products"
    USERS = "users"

@dataclass
class FakeStoreConfig:
    """Configuration for FakeStore ingestor."""
    base_url: str = os.getenv("FAKESTORE_API", "https://fakestoreapi.com")
    enabled_sources: Tuple[DataSource, ...] = (
        DataSource.PRODUCTS,
        DataSource.USERS,
    )
    fetch_timeout: int = 60
    max_retries: int = 3
    parallel_fetch: bool = True
    batch_size: int = 1000
    schema: str = "bronze"
    validate_required_fields: bool = True
    required_sources: Tuple[DataSource, ...] = (DataSource.PRODUCTS, DataSource.USERS)


    def __post_init__(self):
        """Validate configuration after initialization."""
        if not self.base_url or not isinstance(self.base_url, str):
            raise ValueError("Base URL for FakeStore API must be provided.")
        if not self.base_url.startswith(("http://", "https://")):
            raise ValueError("base_url must start with http:// or https://")
        if self.batch_size <= 0:
            raise ValueError("Batch size must be a positive integer.")
        if not self.enabled_sources:
            raise ValueError("enabled_sources cannot be empty")


@dataclass
class SourceSchema:
    """Schema definition for each data source."""
    source: DataSource
    required_fields: List[str]
    table_name: str
    id_field: str = "id"


# Schema definitions for each data source
FAKESTORE_SCHEMAS = {
    DataSource.PRODUCTS: SourceSchema(
        source=DataSource.PRODUCTS,
        required_fields=["id", "title", "price", "category"],
        table_name="products_raw",
        id_field="id",
    ),
    DataSource.USERS: SourceSchema(
        source=DataSource.USERS,
        required_fields=["id", "email", "username"],
        table_name="users_raw",
        id_field="id",
    ),
}


# Data Validation

class FakeStoreValidator:
    """Validation logic for FakeStore data."""

    @staticmethod
    def validate_products(df: pd.DataFrame) -> Tuple[bool, Optional[str]]:
        """Validate products data."""
        if df.empty:
            return False, "Products DataFrame is empty"

        # Check for required fields
        required_fields = FAKESTORE_SCHEMAS[DataSource.PRODUCTS].required_fields
        missing_fields = [field for field in required_fields if field not in df.columns]
        if missing_fields:
            return False, f"Products data is missing required fields: {missing_fields}"
        
        # Validate data types
        if not pd.api.types.is_integer_dtype(df["id"]):
            return False, "Products 'id' field must be an integer"
        if not pd.api.types.is_string_dtype(df["title"]):
            return False, "Products 'title' field must be a string"
        if not pd.api.types.is_numeric_dtype(df["price"]):
            return False, "Products 'price' field must be numeric"
        if not pd.api.types.is_string_dtype(df["category"]):
            return False, "Products 'category' field must be a string"

        # Check duplicates
        if df["id"].duplicated().any():
            return False, "Products data contains duplicate 'id' values"
        
        # Validate price values
        if (df["price"] < 0).any():
            return False, "Products 'price' field contains negative values"

        return True, None

    @staticmethod
    def validate_users(df: pd.DataFrame) -> Tuple[bool, Optional[str]]:
        """Validate users data."""
        if df.empty:
            return False, "Users DataFrame is empty"

        # Check for required fields
        required_fields = FAKESTORE_SCHEMAS[DataSource.USERS].required_fields
        missing_fields = [field for field in required_fields if field not in df.columns]
        if missing_fields:
            return False, f"Users data is missing required fields: {missing_fields}"
        
        # Validate data types
        if not pd.api.types.is_integer_dtype(df["id"]):
            return False, "Users 'id' field must be an integer"
        if not pd.api.types.is_string_dtype(df["email"]):
            return False, "Users 'email' field must be a string"
        if not pd.api.types.is_string_dtype(df["username"]):
            return False, "Users 'username' field must be a string"

        # Check duplicates
        if df["id"].duplicated().any():
            return False, "Users data contains duplicate 'id' values"
        
        # Validate email format (basic check)
        if not df["email"].str.contains(r"@", na=False).all():
            return False, "Users 'email' field contains invalid email addresses"

        return True, None


# FakeStore Ingestor

class FakeStoreIngestor(BaseIngestionGenerator):
    """Production-grade ingestor for FakeStore API data.

    Extends BaseIngestionGenerator to work with multiple data sources
    from the FakeStore API.
    """

    def __init__(
        self,
        engine: Engine,
        config: Optional[FakeStoreConfig] = None,
        api_config: Optional[APIConfig] = None,
        db_config: Optional[DatabaseConfig] = None,
    ):
        """Initialize the FakeStore ingestor."""
        # Initialize configuration
        self.config = config or FakeStoreConfig()
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
            source_name="fakestore",
            engine=engine,
            config=None,
            max_retries=self.config.max_retries,
        )
        
        # Restore the FakeStoreConfig object (super().__init__() would overwrite it with a dict)
        self.config = config_obj

        # Store DataFrames
        self._dataframes: Dict[DataSource, pd.DataFrame] = {}
        self._raw_data: Dict[DataSource, List[Dict[str, Any]]] = {}

        logger.info(
            f"Initialized FakeStore ingestor with {len(self.config.enabled_sources)} sources",
            extra={"source_count": len(self.config.enabled_sources)}
        )

    def fetch_data(self) -> List[Dict[str, Any]]:
        """
        Fetch data from FakeStore API.
        
        Returns:
            Combined list of all fetched records
        """
        if self.config.parallel_fetch:
            self._fetch_parallel()
        else:
            self._fetch_sequential()
        
        # Combine all data
        all_data = []
        for source_data in self._raw_data.values():
            all_data.extend(source_data)
        
        return all_data

    def _fetch_sequential(self) -> Dict[DataSource, List[Dict[str, Any]]]:
        """Fetch data sequentially from each enabled endpoint."""
        self._raw_data = {}

        for source in self.config.enabled_sources:
            try:
                url = f"{self.config.base_url}/{source.value}"
                logger.debug(f"Fetching {source.value} from {url}")
                
                data = self.api_client.fetch(url)
                
                # Ensure data is a list
                if isinstance(data, dict):
                    data = [data]
                elif not isinstance(data, list):
                    raise IngestionException(
                        f"Unexpected response type from {source.value}: {type(data)}",
                        self.source_name
                    )
                
                self._raw_data[source] = data
                logger.info(
                    f"Fetched {source.value}",
                    extra={"source": source.value, "record_count": len(data)}
                )
                
            except Exception as e:
                raise IngestionException(
                    f"Failed to fetch {source.value}: {str(e)}",
                    self.source_name
                )
        
        return self._raw_data

    def _fetch_parallel(self) -> Dict[DataSource, List[Dict[str, Any]]]:
        """Fetch data in parallel from each enabled endpoint."""

        results = {}
        errors = {}
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            future_to_source = {
                executor.submit(self._fetch_source, source): source
                for source in self.config.enabled_sources
            }

            for future in concurrent.futures.as_completed(future_to_source):
                source = future_to_source[future]
                try:
                    data = future.result()
                    results[source] = data
                except Exception as e:
                    errors[source] = str(e)
                    if source in self.config.required_sources:
                        logger.error(f"Failed required source {source.value}: {e}")
                    else:
                        logger.warning(f"Failed optional source {source.value}: {e}")

        if any(s in errors for s in self.config.required_sources):
            raise IngestionException("Required sources failed", self.source_name)
        
        self.fetched_data = results
        return results

    def _fetch_source(self, source: DataSource) -> List[Dict[str, Any]]:
        """Fetch data for a single source."""
        url = f"{self.config.base_url}/{source.value}"
        logger.debug(f"Fetching {source.value} from {url}")
        
        data = self.api_client.fetch(url)
        
        # Ensure data is a list
        if isinstance(data, dict):
            data = [data]
        elif not isinstance(data, list):
            raise IngestionException(
                f"Unexpected response type from {source.value}: {type(data)}",
                self.source_name
            )
        
        return data

    def _normalize(self, data: List[Dict[str, Any]], schema: SourceSchema) -> pd.DataFrame:
        """Helper to convert raw list of dicts to a DataFrame."""
        return pd.DataFrame(data)

    def _validate(self, df: pd.DataFrame, schema: SourceSchema) -> None:
        """Helper to trigger source-specific validation."""
        self._validate_source_data(schema.source, df)

    def _sanitize(self, df: pd.DataFrame, schema: SourceSchema) -> pd.DataFrame:
        """Helper to trigger source-specific sanitization."""
        return self._sanitize_source_data(schema.source, df)

    def load_data(self, data: List[Dict[str, Any]]) -> int:
        """Load fetched data into bronze tables."""
        total_loaded = 0
        try:
            # FIX: Use fetched_data dictionary
            for source, source_data in self.fetched_data.items():
                schema = FAKESTORE_SCHEMAS[source]
                
                df = self._normalize(source_data, schema)
                # FIX: Populate _dataframes so _sanitize_source_data can find it
                self._dataframes[source] = df
                
                self._validate(df, schema)
                df = self._sanitize(df, schema)
                
                records_loaded = self.db_writer.write(
                    df=df,
                    table_name=schema.table_name,
                    validate=False,
                    required_columns=schema.required_fields,
                )
                total_loaded += records_loaded
            
            return total_loaded
        except Exception as e:
            logger.error(f"Load failed: {str(e)}")
            raise IngestionException(f"Failed to load: {str(e)}", self.source_name)

    def _validate_source_data(self, source: DataSource, df: pd.DataFrame) -> None:
        """Validate source-specific data."""
        logger.debug(f"Validating {source.value} data")
        
        if source == DataSource.PRODUCTS:
            is_valid, error_msg = FakeStoreValidator.validate_products(df)
        elif source == DataSource.USERS:
            is_valid, error_msg = FakeStoreValidator.validate_users(df)
        else:
            raise ValueError(f"Unknown source: {source}")
        
        if not is_valid:
            raise IngestionException(
                f"Validation failed for {source.value}: {error_msg}",
                self.source_name,
                records_failed=len(df)
            )
        
        logger.info(
            f"Validated {source.value}",
            extra={"source": source.value, "record_count": len(df)}
        )

    def _sanitize_source_data(self, source: DataSource, data: List[Dict[str, Any]]) -> pd.DataFrame:
        """Sanitize and flatten nested data."""
        df = pd.DataFrame(data)
        
        if source == DataSource.USERS:
            # Flatten 'name' (firstname, lastname)
            if 'name' in df.columns:
                name_df = pd.json_normalize(df['name'])
                df = pd.concat([df.drop(columns=['name']), name_df], axis=1)
                
            # Flatten 'address' (city, street, number, zipcode)
            if 'address' in df.columns:
                address_df = pd.json_normalize(df['address'])
                # Note: this will create columns like 'city', 'street', etc.
                # You may want to drop 'geolocation' as it is a nested dict itself
                df = pd.concat([df.drop(columns=['address']), address_df], axis=1)
                if 'geolocation' in df.columns:
                    df = df.drop(columns=['geolocation'])

        if source == DataSource.PRODUCTS:
            # Flatten 'rating' (rate, count)
            if 'rating' in df.columns:
                rating_df = pd.json_normalize(df['rating'])
                rating_df.columns = [f"rating_{col}" for col in rating_df.columns]
                df = pd.concat([df.drop(columns=['rating']), rating_df], axis=1)
        
        # Add ingestion metadata
        df["_ingestion_timestamp"] = self.metrics.start_time
        df["_source"] = source.value
        
        logger.info(
            f"Sanitized {source.value}",
            extra={
                "source": source.value,
                "records_before": len(self._dataframes[source]),
                "records_after": len(df)
            }
        )

        return df

    def get_source_metrics(self, source: DataSource) -> Dict[str, Any]:
        """Get metrics for a specific data source."""
        if source not in self._dataframes:
            return {}
        
        df = self._dataframes[source]
        return {
            "source": source.value,
            "record_count": len(df),
            "column_count": len(df.columns),
            "columns": list(df.columns),
            "memory_usage_mb": df.memory_usage(deep=True).sum() / (1024 ** 2),
        }

    def get_all_metrics(self) -> Dict[str, Any]:
        """Get metrics for all data sources."""
        all_metrics = super().get_metrics()

        # Add source-specific metrics
        source_metrics: Dict[str, Any] = {}
        for source in self.config.enabled_sources:
            source_metrics[source.value] = self.get_source_metrics(source)

        all_metrics["sources"] = source_metrics
        return all_metrics
        
    def close(self) -> None:
        """Close API client and database connections."""
        try:
            self.api_client.close()
            logger.info("FakeStore ingestor closed")
        except Exception as e:
            logger.error(f"Error closing ingestor: {e}")