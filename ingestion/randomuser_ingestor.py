"""
RandomUser API Data Ingestor

Ingests user data from the RandomUser API with support for
flexible result sizes, gender filtering, and robust error handling.

Features:
- Configurable result counts and filters
- Nested JSON normalization
- Comprehensive data validation
- Error handling and automatic retry
- Detailed metrics and logging
- Resource management
"""

from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
from enum import Enum
from datetime import datetime
import os

import pandas as pd
from sqlalchemy import Engine
from loguru import logger
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

from ingestion.base_generator import BaseIngestionGenerator, IngestionException, IngestionMetrics, IngestionStatus
from ingestion.ingestion_util import APIClient, DatabaseWriter, APIConfig, DatabaseConfig, APIError

# Configuration & Constants

class GenderFilter(Enum):
    """Gender filter options for RandomUser API."""
    MALE = "male"
    FEMALE = "female"
    BOTH = "both"  # No filter


class NationalityFilter(Enum):
    """Nationality filter options."""
    AU = "au"  # Australia
    BR = "br"  # Brazil
    CA = "ca"  # Canada
    CH = "ch"  # Switzerland
    DE = "de"  # Germany
    DK = "dk"  # Denmark
    ES = "es"  # Spain
    FI = "fi"  # Finland
    FR = "fr"  # France
    GB = "gb"  # United Kingdom
    IE = "ie"  # Ireland
    IN = "in"  # India
    IR = "ir"  # Iran
    MX = "mx"  # Mexico
    NL = "nl"  # Netherlands
    NO = "no"  # Norway
    NZ = "nz"  # New Zealand
    RS = "rs"  # Serbia
    TR = "tr"  # Turkey
    UA = "ua"  # Ukraine
    US = "us"  # United States
    RANDOM = "random"  # Random


@dataclass
class RandomUserConfig:
    """Configuration for RandomUser ingestor."""
    base_url: str = os.getenv("RANDOMUSER_API", "https://randomuser.me/api")
    results: int = 100  # Number of users to fetch (max 5000 per request)
    seed: Optional[str] = None  # Seed for reproducible results
    gender: GenderFilter = GenderFilter.BOTH
    nationality: Optional[NationalityFilter] = None
    table_name: str = "users_raw"
    fetch_timeout: int = 60
    max_retries: int = 3
    backoff_factor: float = 2.0
    batch_size: int = 5000
    schema: str = "bronze"
    validate_required_fields: bool = True
    normalize_nested: bool = True

    def __post_init__(self):
        """Validate configuration."""
        if not self.base_url or not isinstance(self.base_url, str):
            raise ValueError("base_url must be a non-empty string")
        if not self.base_url.startswith(("http://", "https://")):
            raise ValueError("base_url must start with http:// or https://")
        
        if self.results <= 0 or self.results > 5000:
            raise ValueError("results must be between 1 and 5000")
        
        if not self.table_name or not isinstance(self.table_name, str):
            raise ValueError("table_name must be a non-empty string")
        
        if self.batch_size <= 0:
            raise ValueError("batch_size must be greater than 0")


# Data Validation

class RandomUserValidator:
    """Validation logic for RandomUser API data."""
    
    # Required fields in the response
    REQUIRED_FIELDS = {
        "gender",
        "email",
        "username",
        "name_first",
        "name_last",
        "location_country",
        "dob_age",
    }
    
    @staticmethod
    def validate_user_data(df: pd.DataFrame) -> Tuple[bool, Optional[str]]:
        """
        Validate RandomUser data.
        
        Args:
            df: DataFrame containing user records
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        if df.empty:
            return False, "User DataFrame is empty"
        
        # Check for required fields (after normalization)
        # Note: Column names may be flattened after json_normalize
        required_base_fields = ["gender", "email", "username", "name_first", "name_last"]
        
        # Check for email uniqueness
        if "email" in df.columns:
            if df["email"].duplicated().any():
                duplicate_count = df["email"].duplicated().sum()
                return False, f"Found {duplicate_count} duplicate emails"
            
            # Basic email validation
            if not df["email"].str.contains("@", na=False).all():
                return False, "Invalid email format detected"
        
        # Check for null emails
        if "email" in df.columns:
            if df["email"].isnull().any():
                null_count = df["email"].isnull().sum()
                return False, f"Found {null_count} null email addresses"
        
        # Check age validity
        if "dob_age" in df.columns:
            if not pd.api.types.is_numeric_dtype(df["dob_age"]):
                return False, "Age must be numeric"
            
            if (df["dob_age"] < 0).any():
                return False, "Age cannot be negative"
            
            if (df["dob_age"] > 150).any():
                return False, "Age seems invalid (> 150)"
        
        # Check gender values
        if "gender" in df.columns:
            valid_genders = {"male", "female"}
            if not df["gender"].isin(valid_genders).all():
                invalid = df[~df["gender"].isin(valid_genders)]["gender"].unique()
                return False, f"Invalid gender values: {invalid}"
        
        return True, None
    
    @staticmethod
    def validate_phone_format(df: pd.DataFrame) -> Tuple[bool, Optional[str]]:
        """
        Validate phone number formats (if present).
        
        Args:
            df: DataFrame to validate
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        if "phone" in df.columns:
            # Check that phone is not all nulls
            if df["phone"].isnull().all():
                return False, "All phone numbers are null"
            
            # Check that phone numbers are strings
            if not df["phone"].dtype == "object":
                logger.warning("Phone field should be string type")
        
        return True, None


# Data Normalization & Sanitization

class RandomUserNormalizer:
    """Handle nested JSON normalization for RandomUser data."""
    
    @staticmethod
    def normalize_user_data(data: List[Dict[str, Any]]) -> pd.DataFrame:
        """
        Normalize nested RandomUser API response to flat DataFrame.
        
        RandomUser returns nested JSON like:
        {
            "gender": "male",
            "name": {"first": "John", "last": "Doe"},
            "location": {...},
            "dob": {"date": "...", "age": 30},
            ...
        }
        
        Args:
            data: List of user records from API
            
        Returns:
            Flattened DataFrame
        """
        if not data:
            return pd.DataFrame()
        
        logger.debug(f"Normalizing {len(data)} user records")
        
        # Use pandas json_normalize for nested structures
        df = pd.json_normalize(data)
        
        # Rename columns to be more readable (replace dots with underscores)
        df.columns = [col.replace(".", "_") for col in df.columns]
        
        logger.debug(
            f"Normalized DataFrame has {len(df.columns)} columns",
            extra={"column_count": len(df.columns)}
        )
        
        return df

    @staticmethod
    def extract_key_fields(df: pd.DataFrame) -> pd.DataFrame:
        """
        Extract key user information and create denormalized columns.
        
        Args:
            df: Normalized DataFrame
            
        Returns:
            DataFrame with additional key fields
        """
        # Full name
        if "name_first" in df.columns and "name_last" in df.columns:
            df["full_name"] = df["name_first"] + " " + df["name_last"]
        
        # Formatted phone
        if "phone" in df.columns:
            df["phone_formatted"] = df["phone"].str.replace(r"[^\d\-\+\s]", "", regex=True)
        
        # Age group (for analytics)
        if "dob_age" in df.columns:
            df["age_group"] = pd.cut(
                df["dob_age"],
                bins=[0, 18, 35, 50, 65, 150],
                labels=["<18", "18-34", "35-49", "50-64", "65+"],
                right=True
            )
        
        # Country code extraction (if available)
        if "location_country" in df.columns:
            df["country"] = df["location_country"]
        
        return df


# RandomUser Ingestor

class RandomUserIngestor(BaseIngestionGenerator):
    """Production-grade ingestor for RandomUser API data."""

    def __init__(
        self,
        engine: Engine,
        config: Optional[RandomUserConfig] = None,
        api_config: Optional[APIConfig] = None,
        db_config: Optional[DatabaseConfig] = None,
    ):
        """Initialize RandomUser ingestor."""
        # Initialize configuration
        self.config = config or RandomUserConfig()
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
            source_name="randomuser",
            engine=engine,
            config=None,
            max_retries=self.config.max_retries,
        )

        # Restore the RandomUserConfig object (super().__init__() would overwrite it with a dict)
        self.config = config_obj

        # Store DataFrames and data
        self._dataframe: Optional[pd.DataFrame] = None
        self._raw_data: List[Dict[str, Any]] = []

        logger.info(
            "Initialized RandomUser ingestor",
            extra={"results": self.config.results}
        )

    def fetch_data(self) -> List[Dict[str, Any]]:
        """
        Fetch user data from RandomUser API.
        
        Returns:
            List of user records
            
        Raises:
            IngestionException: If fetch fails
        """
        try:
            logger.info(
                f"Fetching {self.config.results} users from RandomUser API",
                extra={
                    "results": self.config.results,
                    "gender": self.config.gender.value,
                    "nationality": self.config.nationality.value if self.config.nationality else None
                }
            )

            # Build query parameters
            params = {
                "results": self.config.results,
                "format": "json"
            }

            if self.config.gender != GenderFilter.BOTH:
                params["gender"] = self.config.gender.value

            if self.config.nationality:
                params["nat"] = self.config.nationality.value

            if self.config.seed:
                params["seed"] = self.config.seed

            # Fetch data
            url = self.config.base_url
            response = self.api_client.fetch(url, params=params)

            # Extract results array
            if isinstance(response, dict) and "results" in response:
                data = response["results"]
            elif isinstance(response, list):
                data = response
            else:
                raise IngestionException(
                    f"Unexpected response format from RandomUser API",
                    self.source_name
                )

            self._raw_data = data
            logger.info(
                f"Fetched {len(data)} user records from RandomUser API",
                extra={"record_count": len(data)}
            )

            return data

        except Exception as e:
            raise IngestionException(
                f"Failed to fetch data from RandomUser API: {str(e)}",
                self.source_name
            )

    def _normalize_users(self, raw_users: List[Dict[str, Any]]) -> pd.DataFrame:
        """
        Normalize raw user data from RandomUser API into a flat DataFrame.
        
        ### FIX COMMENT ###
        This method flattens the nested user structure returned by RandomUser API
        into a normalized table structure suitable for database storage.
        
        Args:
            raw_users: List of user dictionaries from API
            
        Returns:
            Normalized DataFrame
        """
        normalized_users = []
        
        for idx, user in enumerate(raw_users):
            try:
                # Extract nested data from RandomUser API structure
                name_obj = user.get('name', {})
                location_obj = user.get('location', {})
                login_obj = user.get('login', {})
                picture_obj = user.get('picture', {})
                
                # extract all fields with defaults
                normalized_user = {
                    'first_name': name_obj.get('first', '') if isinstance(name_obj, dict) else '',
                    'last_name': name_obj.get('last', '') if isinstance(name_obj, dict) else '',
                    'email': user.get('email', ''),
                    'phone': user.get('phone', ''),
                    'cell': user.get('cell', ''),
                    'username': login_obj.get('username', '') if isinstance(login_obj, dict) else '',
                    'country': location_obj.get('country', '') if isinstance(location_obj, dict) else '',
                    'state': location_obj.get('state', '') if isinstance(location_obj, dict) else '',
                    'city': location_obj.get('city', '') if isinstance(location_obj, dict) else '',
                    'picture_large': picture_obj.get('large', '') if isinstance(picture_obj, dict) else '',
                    'picture_medium': picture_obj.get('medium', '') if isinstance(picture_obj, dict) else '',
                    'picture_thumbnail': picture_obj.get('thumbnail', '') if isinstance(picture_obj, dict) else '',
                    'gender': user.get('gender', ''),
                    'nationality': user.get('nat', ''),
                    'age': user.get('dob', {}).get('age', 0) if isinstance(user.get('dob'), dict) else 0,
                }
                
                normalized_users.append(normalized_user)
                
            except Exception as e:
                logger.warning(f"Failed to normalize user record at index {idx}: {e}")
                continue
        
        if not normalized_users:
            logger.warning("No user records were successfully normalized")
            return pd.DataFrame()
        
        df = pd.DataFrame(normalized_users)
        logger.info(f"Normalized {len(df)} user records", extra={"record_count": len(df)})
        
        return df


    def load_data(self, data: List[Dict[str, Any]]) -> int:
        """
        Load fetched data into bronze layer.
        
        Args:
            data: List of user records
            
        Returns:
            Number of records loaded
            
        Raises:
            IngestionException: If loading fails
        """
        self._raw_data = data
        
        try:
            logger.info("Starting data normalization and validation")

            # FIX #3: Use 'data' parameter (not 'raw_data')
            # If data is empty, use stored _raw_data from fetch_data
            data_to_normalize = data if data else self._raw_data
            
            if not data_to_normalize:
                # FIX #3: Handle empty data gracefully - log warning and return 0
                logger.warning("No data to normalize from RandomUser API - returning 0 records loaded")
                return 0  # Return 0 instead of raising exception
            
            # Normalize nested JSON
            df = self._normalize_users(data_to_normalize)
        
            logger.debug(f"Normalized {len(df)} records")
            
            if df.empty:
                raise IngestionException(
                    "Normalization failed: resulting DataFrame is empty",
                    self.source_name
                )
            
            # Validate
            self._validate_data(df)
            
            # Sanitize
            df = self._sanitize_data(df)
            
            # Write to database
            records_loaded = self.db_writer.write(
                df=df,
                table_name=self.config.table_name,
                validate=False,
                required_columns=self.config.required_fields,
            )
            
            logger.info(
                f"Loaded {records_loaded} user records to bronze",
                extra={
                    "records_loaded": records_loaded,
                    "table": self.config.table_name,
                }
            )
            
            return records_loaded
            
        except Exception as e:
            logger.error(f"Failed to load data to bronze: {e}", exc_info=True)
            raise IngestionException(
                f"Failed to load data to bronze: {str(e)}",
                self.source_name,
                records_failed=len(data)
            )
    
    def _validate_data(self, df: pd.DataFrame) -> None:
        """
        Validate user data.
        
        Args:
            df: DataFrame to validate
            
        Raises:
            IngestionException: If validation fails
        """
        logger.debug("Validating user data")

        # Check for empty DataFrame
        if df.empty:
            raise IngestionException(
                "Validation failed: User DataFrame is empty",
                self.source_name
            )
    
        # Check for required fields
        required_fields = self.config.required_fields
        missing_fields = set(required_fields) - set(df.columns)
        
        if missing_fields:
            logger.warning(f"Missing optional fields in user data: {missing_fields}")
        
        # Check email format if email column exists
        if 'email' in df.columns:
            invalid_emails = df[df['email'].notna() & ~df['email'].str.contains(r'^[\w\.-]+@[\w\.-]+\.\w+$', na=False)]
            if len(invalid_emails) > 0:
                logger.warning(f"Found {len(invalid_emails)} invalid email formats")
        
        logger.info(
            "Validation passed",
            extra={"record_count": len(df)}
        )

    def _sanitize_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Sanitize user data for database insertion.
        
        Args:
            df: DataFrame to sanitize
            
        Returns:
            Sanitized DataFrame
        """
        logger.debug("Sanitizing user data")
    
        # Remove completely null rows
        df = df.dropna(how='all')
        
        # Standardize string fields
        string_cols = ['first_name', 'last_name', 'email', 'username', 'country', 'city', 'gender']
        for col in string_cols:
            if col in df.columns:
                df[col] = df[col].fillna('').astype(str).str.strip().str.lower()
        
        # Remove duplicates based on email
        if 'email' in df.columns:
            df = df.drop_duplicates(subset=['email'], keep='first')
            logger.info(f"Removed duplicate emails, {len(df)} records remaining")
        
        # Add metadata
        df['_ingestion_timestamp'] = self.metrics.start_time
        df['_source_name'] = self.source_name
        
        logger.info(
            f"Sanitized user data",
            extra={"record_count": len(df)}
        )
        
        return df

    def get_data_profile(self) -> Dict[str, Any]:
        """
        Get profile of ingested data.
        
        Returns:
            Dictionary with data profile information
        """
        if self._dataframe is None or self._dataframe.empty:
            return {}
        
        df = self._dataframe
        
        profile = {
            "total_records": len(df),
            "total_columns": len(df.columns),
            "memory_usage_mb": df.memory_usage(deep=True).sum() / (1024 ** 2),
        }
        
        # Gender distribution
        if "gender" in df.columns:
            profile["gender_distribution"] = df["gender"].value_counts().to_dict()
        
        # Age statistics
        if "dob_age" in df.columns:
            profile["age_stats"] = {
                "min": int(df["dob_age"].min()),
                "max": int(df["dob_age"].max()),
                "mean": float(df["dob_age"].mean()),
                "median": float(df["dob_age"].median()),
            }
        
        # Country distribution (top 10)
        if "location_country" in df.columns:
            profile["top_countries"] = (
                df["location_country"].value_counts().head(10).to_dict()
            )
        
        return profile


    def get_all_metrics(self) -> Dict[str, Any]:
        """
        Get comprehensive metrics for ingestion.
        
        Returns:
            Dictionary of metrics
        """
        all_metrics = super().get_metrics()
        
        # Add data profile
        data_profile = self.get_data_profile()
        all_metrics["data_profile"] = data_profile
        
        return all_metrics
    
    def close(self) -> None:
        """Close API client and database connections."""
        try:
            self.api_client.close()
            logger.info("RandomUser ingestor closed")
        except Exception as e:
            logger.error(f"Error closing ingestor: {e}")


# Convenience Functions (Backward Compatible)

def fetch_random_users(
    engine: Engine,
    base_url: str = os.getenv("RANDOMUSER_API", "https://randomuser.me/api"),
    results: int = 100,
    gender: Optional[GenderFilter] = None,
    nationality: Optional[NationalityFilter] = None,
    seed: Optional[str] = None,
) -> IngestionMetrics:
    """
    Fetch random users and load to bronze layer.
    
    Backward compatible with original simple function.
    
    Args:
        engine: SQLAlchemy database engine
        base_url: RandomUser API base URL
        results: Number of users to fetch (1-5000)
        gender: Optional gender filter
        nationality: Optional nationality filter
        seed: Optional seed for reproducibility
        
    Returns:
        IngestionMetrics with ingestion results
        
    Raises:
        IngestionException: If ingestion fails
    """
    config = RandomUserConfig(
        base_url=base_url,
        results=results,
        gender=gender or GenderFilter.BOTH,
        nationality=nationality,
        seed=seed,
    )
    
    ingestor = RandomUserIngestor(engine=engine, config=config)
    try:
        return ingestor.run_ingestion()
    finally:
        ingestor.close()