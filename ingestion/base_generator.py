from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Any, Dict, Optional, List
from dataclasses import dataclass, asdict
from enum import Enum
import logging
from functools import wraps
import time
from loguru import logger


class IngestionStatus(Enum):
    """Enumeration for ingestion statuses."""
    PENDING = "PENDING"
    IN_PROGRESS = "IN_PROGRESS"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    PARTIAL = "PARTIAL"


@dataclass
class IngestionMetrics:
    """Dataclass to hold ingestion metrics."""
    source_name: str
    start_time: datetime
    end_time: Optional[datetime] = None
    status: IngestionStatus = IngestionStatus.PENDING
    records_fetched: int = 0
    records_loaded: int = 0
    records_failed: int = 0
    error_message: Optional[str] = None
    duration_seconds: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        """Convert the dataclass to a dictionary."""
        return asdict(self)

    def calculate_duration(self) -> float:
        """Calculate the duration of the ingestion process."""
        if self.end_time:
            self.duration_seconds = (self.end_time - self.start_time).total_seconds()
        return self.duration_seconds


class IngestionException(Exception):
    """Custom exception for ingestion errors."""
    def __init__(self, message: str, source_name: str, records_failed: int = 0):
        self.message = message
        self.source_name = source_name
        self.records_failed = records_failed
        super().__init__(message)


def retry_on_exception(max_retries: int = 3, backoff_factor: float = 2.0, backoff_base: int = 1):
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
                except Exception as e:
                    last_exception = e
                    if attempt < max_retries:
                        delay = backoff_base * (backoff_factor ** attempt)
                        logger.warning(
                            f"Attempt {attempt + 1}/{max_retries + 1} failed for {func.__name__}. "
                            f"Retrying in {delay}s. Error: {str(e)}"
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


class BaseIngestionGenerator(ABC):
    """Abstract base class for ingestion generators."""
    
    def __init__(
        self,
        source_name: str,
        engine: Any,
        config: Optional[Dict[str, Any]] = None,
        max_retries: int = 3,
    ):
        """
        Initialize the ingestor.
        """
        if not source_name or not isinstance(source_name, str):
            raise ValueError("source_name must be a non-empty string.")
        
        self.source_name = source_name
        self.engine = engine
        self.config = config or {}
        self.max_retries = max_retries
        self.metrics = IngestionMetrics(
            source_name=source_name,
            start_time=datetime.now(timezone.utc)
        )

        logger.info(f"Initialized ingestion for source: {self.source_name}")

    @abstractmethod
    def fetch_data(self) -> List[Dict[str, Any]]:
        """
        Fetch data from the source.
        
        Must be implemented by subclasses.
        """
        pass

    @abstractmethod
    def load_data(self, data: List[Dict[str, Any]]) -> int:
        """
        Load data into the target system.
        
        Must be implemented by subclasses.
        """
        pass

    def validate_data(self, data: List[Dict[str, Any]]) -> bool:
        """
        Validate the fetched data.
        
        Override in subclass for custom validation logic.
        """
        if not isinstance(data, list):
            raise IngestionException(
                f"Expected list, got {type(data).__name__}",
                self.source_name,
            )

        if not data:
            logger.warning(f"No data fetched for source: {self.source_name}")

        return True

    def sanitize_data(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Sanitize the fetched data.
        
        Removes None values and adds ingestion metadata.
        """
        sanitized_data = []
        for record in data:
            if not isinstance(record, dict):
                logger.warning(f"Skipping non-dict record: {record}")
                continue

            sanitized_record = {
                k: v for k, v in record.items() 
                if v is not None
            }

            sanitized_record['_ingestion_timestamp'] = self.metrics.start_time
            sanitized_record['_source_name'] = self.source_name
            sanitized_data.append(sanitized_record)

        return sanitized_data

    def run_ingestion(self) -> IngestionMetrics:
        """
        Run the ingestion process.
        """
        try:
            self.metrics.status = IngestionStatus.IN_PROGRESS
            raw_data = self._fetch_with_retry()
            
            # FIX: Robust record counting for multi-source (dict) or single-source (list)
            if isinstance(raw_data, list):
                self.metrics.records_fetched = len(raw_data)
            elif isinstance(raw_data, dict):
                self.metrics.records_fetched = sum(len(v) for v in raw_data.values() if isinstance(v, list))
                
            logger.info(
                f"Starting ingestion for source: {self.source_name}",
                extra={"ingestion_metrics": self.metrics.to_dict()}
            )
            
            # Fetch data
            raw_data = self._fetch_with_retry()
            self.metrics.records_fetched = len(raw_data) if isinstance(raw_data, list) else 0
            logger.debug(
                f"Fetched {self.metrics.records_fetched} records from {self.source_name}"
            )

            # Validate data
            self.validate_data(raw_data)

            # Sanitize data
            sanitized_data = self.sanitize_data(raw_data)
            logger.debug(
                f"Sanitized data for {self.source_name}, total records after sanitization: {len(sanitized_data)}"
            )

            # Load data to bronze
            loaded_count = self._load_with_retry(sanitized_data)
            self.metrics.records_loaded = loaded_count

            # Determine final status
            if loaded_count == 0 and self.metrics.records_fetched > 0:
                self.metrics.status = IngestionStatus.PARTIAL 
                logger.warning(
                    f"No records loaded to bronze {self.source_name}: "
                    f"Fetched {self.metrics.records_fetched}, Loaded {loaded_count}"
                )
            else:
                self.metrics.status = IngestionStatus.SUCCESS
            
            logger.info(
                f"Completed ingestion for source: {self.source_name}",
                extra={
                    "source": self.source_name,
                    "records_fetched": self.metrics.records_fetched,
                    "records_loaded": loaded_count,
                    "status": self.metrics.status.value
                }
            )
            
            return self.metrics

        except Exception as e:
            self.metrics.status = IngestionStatus.FAILED
            self.metrics.error_message = str(e)
            logger.error(
                f"Ingestion failed for {self.source_name}",
                extra={
                    "source": self.source_name,
                    "records_loaded": self.metrics.records_loaded,
                    "records_fetched": self.metrics.records_fetched
                },
                exc_info=True
            )
            raise

        finally:
            self.metrics.end_time = datetime.now(timezone.utc)
            self.metrics.calculate_duration()
            logger.info(
                f"Ingestion metrics for {self.source_name}: {self.metrics.to_dict()}"
            )

    @retry_on_exception(max_retries=3, backoff_factor=2.0, backoff_base=1)
    def _fetch_with_retry(self) -> List[Dict[str, Any]]:
        """
        Fetch data with automatic retry logic.
        """
        try:
            return self.fetch_data()

        except Exception as e:
            raise IngestionException(
                f"Error fetching data from {self.source_name}: {str(e)}",
                self.source_name
            )

    @retry_on_exception(max_retries=3, backoff_factor=2.0, backoff_base=1)
    def _load_with_retry(self, data: List[Dict[str, Any]]) -> int:
        """
        Load data with retry logic.
        """
        try:
            return self.load_data(data)
        
        except Exception as e:
            raise IngestionException(
                f"Error loading data to bronze for {self.source_name}: {str(e)}",
                self.source_name,
                records_failed=len(data)
            )

    def get_metrics(self) -> Dict[str, Any]:
        """
        Get the current ingestion metrics.
        """
        return self.metrics.to_dict()
