"""
Backfill Script - CORRECTED VERSION
Location: scripts/backfill.py

Backfills historical data for specified date ranges.
Useful for:
- Initial data load
- Recovering from failures
- Reprocessing specific periods
"""

import os
import sys
import argparse
from pathlib import Path
from typing import Optional, List
from datetime import datetime, timedelta
from dotenv import load_dotenv
import logging
from sqlalchemy import create_engine, text
import pandas as pd
import yaml

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from ingestion.dummyjson_ingestor import DummyJSONIngestor
from ingestion.fakestore_ingestor import FakeStoreIngestor
from ingestion.randomuser_ingestor import RandomUserIngestor
from database.layers.silver.transform_silver import SilverTransform
from database.layers.audit.audit_writer import AuditWriter

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

# Configure logging with separate logs directory
LOG_DIR = Path(__file__).parent.parent / 'logs'
LOG_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    handlers=[
        logging.FileHandler(LOG_DIR / 'backfill.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class BackfillRunner:
    """Handles historical data backfilling."""

    def __init__(self, config_path: str='config'):
        """Initialize backfill runner."""
        self.config_path = Path(config_path)
        self.config = self._load_config()
        self.engine = self._create_engine()
        self.audit = AuditWriter(self.engine)

    def _load_config(self):  # FIXED: Added self parameter
        """Load configuration files."""

        config = {}
        
        # Load environment
        env_file = Path('.env')
        if env_file.exists():
            load_dotenv(env_file)
        
        # Load YAML configs
        for file in ['api_sources.yml', 'database.yml', 'pipeline.yml']:
            file_path = self.config_path / file
            if file_path.exists():
                with open(file_path, 'r') as f:
                    config[file.replace('.yml', '')] = yaml.safe_load(f)
        
        return config

    def _create_engine(self):  
        """Create database engine."""

        db_config = self.config.get('database', {}).get('postgres', {})

        db_url = (
            f"postgresql://{db_config.get('username', 'postgres')}:"
            f"{db_config.get('password', 'postgres')}@"
            f"{db_config.get('host', 'localhost')}:"
            f"{db_config.get('port', 5432)}/"
            f"{db_config.get('database_name', 'DoeEcommerce')}"
        )

        engine = create_engine(db_url, pool_pre_ping=True)
        logger.info("Database engine created")
        return engine

    def backfill_source(
        self,
        source_name: str,
        start_date: datetime,  
        end_date: datetime     
    ):
        """Backfill data from a specific source.
        
        Args:
            source_name: Name of the data source
            start_date: Start date for backfill
            end_date: End date for backfill
        """
        logger.info(f"\n{'=' * 60}")
        logger.info(f"BACKFILLING: {source_name}")
        logger.info(f"DATE RANGE: {start_date.date()} to {end_date.date()}")
        logger.info(f"{'=' * 60}\n")

        # Define ingestors
        ingestors = {
            'dummyjson': DummyJSONIngestor,
            'fakestore': FakeStoreIngestor,
            'randomuser': RandomUserIngestor
        }

        IngestorClass = ingestors.get(source_name)
        if not IngestorClass:
            logger.error(f"Unknown source: {source_name}")  
            return

        try:
            # Run ingestor
            ingestor = IngestorClass(
                source_name=source_name,  
                engine=self.engine,
                config=self.config.get('api_sources', {}).get('sources', {}).get(source_name)  # FIXED: Removed extra space
            )

            metrics = ingestor.run_ingestion()

            # Log to audit
            self.audit.log_ingestion(
                    source_name=source_name,
                    table_name=f"bronze.{source_name}",
                    records_fetched=metrics.records_fetched,
                    records_loaded=metrics.records_loaded
                )

            logger.info(
                f"\nBackfill complete for {source_name}:\n"  
                f" - Records fetched: {metrics.records_fetched}\n"
                f" - Records loaded: {metrics.records_loaded}\n"
                f" - Duration: {metrics.duration_seconds:.2f}s"
            )

        except Exception as e:
            logger.error(f"Backfill failed for {source_name}: {str(e)}")
            raise

    def backfill_table(
        self,
        schema: str,
        table_name: str,
        start_date: datetime,
        end_date: datetime
    ):
        """Backfill a specific table with date filtering.
        
        Args:
            schema: Database schema (bronze, silver)
            table_name: Name of the table
            start_date: Start date for backfill
            end_date: End date for backfill
        """
        logger.info(f"\n{'=' * 60}")
        logger.info(f"BACKFILLING TABLE: {schema}.{table_name}")
        logger.info(f"DATE RANGE: {start_date.date()} to {end_date.date()}")
        logger.info(f"{'=' * 60}\n")

        try:
            with self.engine.connect() as conn:
                # Check if table has timestamp column
                timestamp_col = '_ingestion_timestamp'

                # Delete existing data in date range
                delete_query = text(f"""
                    DELETE FROM {schema}.{table_name}
                    WHERE {timestamp_col} >= :start_date
                    AND {timestamp_col} < :end_date
                """)
                
                result = conn.execute(
                    delete_query,
                    {'start_date': start_date, 'end_date': end_date}
                )
                conn.commit()

                deleted_count = result.rowcount
                logger.info(f"Deleted {deleted_count} existing records")

        except Exception as e:
            logger.error(f"Table backfill failed: {str(e)}")
            raise

    def backfill_date_range(
        self,
        start_date: datetime,
        end_date: datetime,
        sources: Optional[List[str]] = None,
        batch_size_days: int = 1
    ):
        """Backfill data for a date range in batches.
        
        Args:
            start_date: Start date
            end_date: End date
            sources: List of sources to backfill (None = all)
            batch_size_days: Number of days per batch
        """
        logger.info(f"\n{'=' * 60}")
        logger.info("BATCH BACKFILL")
        logger.info(f"DATE RANGE: {start_date.date()} to {end_date.date()}")
        logger.info(f"BATCH SIZE: {batch_size_days} day(s)")
        logger.info(f"{'=' * 60}\n")

        # Define data sources
        all_sources = ['dummyjson', 'fakestore', 'randomuser']
        sources = sources or all_sources

        # Calculate batches
        current_date = start_date
        batch_num = 1

        while current_date < end_date: 
            batch_end = min(
                current_date + timedelta(days=batch_size_days),
                end_date
            )

            logger.info(f"Batch {batch_num}: {current_date.date()} to {batch_end.date()}")

            for source in sources:
                try:
                    self.backfill_source(source, current_date, batch_end)
                except Exception as e:
                    logger.error(f"Batch {batch_num} failed for {source}: {str(e)}")

            current_date = batch_end
            batch_num += 1

        logger.info(f"Batch backfill complete: {batch_num - 1} batches processed")
        
    def verify_backfill(self, schema: str, table_name: str):
        """Verify backfill by checking record counts.
        
        Args:
            schema: Database schema
            table_name: Table name
        """
        logger.info(f"\n{'=' * 60}")
        logger.info(f"VERIFYING: {schema}.{table_name}")
        logger.info(f"{'=' * 60}\n")

        try:
            with self.engine.connect() as conn:
                # Total records
                total = conn.execute(
                    text(f"SELECT COUNT(*) FROM {schema}.{table_name}")
                ).scalar()

                logger.info(f"Total records: {total:,}")

                # Records by date
                if schema == 'bronze':
                    date_query = text(f"""
                        SELECT DATE(_ingestion_timestamp) as date,
                               COUNT(*) as count
                        FROM {schema}.{table_name}
                        GROUP BY DATE(_ingestion_timestamp)
                        ORDER BY date DESC
                        LIMIT 10
                    """)

                    results = conn.execute(date_query).fetchall()

                    logger.info("\nRecent daily counts:")
                    for row in results:
                        logger.info(f" {row[0]}: {row[1]:,} records")

                # Null check
                null_query = text(f"""
                    SELECT COUNT(*) FROM {schema}.{table_name}
                    WHERE id IS NULL
                """)

                null_count = conn.execute(null_query).scalar()

                if null_count > 0:
                    logger.info(f"Found {null_count} records with NULL id")
                else:
                    logger.info("No NULL ids found")

        except Exception as e:
            logger.error(f"Verification failed: {str(e)}")


def main():
    """Main entry point."""

    parser = argparse.ArgumentParser(
        description="Backfill historical data for the pipeline"
    )

    # Date range option
    date_group = parser.add_mutually_exclusive_group(required=True)
    date_group.add_argument(  
        '--days',
        type=int,
        help='Number of days to backfill'
    )
    date_group.add_argument(  
        '--start_date',
        type=str,
        help='Start date (YYYY-MM-DD)'
    )

    parser.add_argument(  
        '--end_date',
        type=str,
        help='End date (YYYY-MM-DD)'
    )
    parser.add_argument(  
        '--source',
        choices=['dummyjson', 'fakestore', 'randomuser', 'all'],
        default='all',
        help='Data source to backfill (default: all)'
    )
    parser.add_argument( 
        '--table',
        help='Specific table to backfill (e.g., products)'
    )
    parser.add_argument(  
        '--schema',
        choices=['bronze', 'silver'],
        default='bronze',
        help='Schema to backfill (default: bronze)'
    )
    parser.add_argument(  
        '--batch_size',
        type=int,
        default=7,
        help='Batch size in days (default: 7)'
    )
    parser.add_argument(  
        '--verify',
        action='store_true',
        help='Verify backfill after completion'
    )
    parser.add_argument( 
        '--config_path',
        default='config',
        help='Path to configuration directory'
    )

    args = parser.parse_args()
    
    # Create logs directory
    os.makedirs('logs', exist_ok=True)

    # Calculate dates
    if args.days:
        end_date = datetime.now()
        start_date = end_date - timedelta(days=args.days)
    else:
        start_date = datetime.strptime(args.start_date, '%Y-%m-%d')
        end_date = (
            datetime.strptime(args.end_date, '%Y-%m-%d')
            if args.end_date
            else datetime.now()
        )
    
    # Initialize backfill runner
    runner = BackfillRunner(config_path=args.config_path)
    
    # Determine sources
    sources = None if args.source == 'all' else [args.source]
    
    # Run backfill
    if args.table:
        # Backfill specific table
        runner.backfill_table(args.schema, args.table, start_date, end_date)
    else:
        # Backfill by date range
        runner.backfill_date_range(
            start_date,
            end_date,
            sources=sources,
            batch_size_days=args.batch_size
        )
    
    # Verify if requested
    if args.verify and args.table:
        runner.verify_backfill(args.schema, args.table)
    
    logger.info("\nBackfill process complete")


if __name__ == "__main__":
    main()