"""
Cleanup Script - FULLY CORRECTED VERSION
Location: scripts/cleanup.py

Performs maintenance and cleanup tasks:
- Archive old data
- Vacuum and analyze tables
- Clean audit logs
- Purge temporary data

Usage:
    python cleanup.py --archive-days 90
    python cleanup.py --vacuum
    python cleanup.py --clean-audit-logs --days 30
    python cleanup.py --purge-bronze
    python cleanup.py --all
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
import yaml

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

# Configure logging with separate logs directory
LOG_DIR = Path(__file__).parent.parent / 'logs'
LOG_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    handlers=[
        logging.FileHandler(LOG_DIR / 'cleanup.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class DatabaseCleaner:
    """Handles database cleanup and maintenance tasks."""

    def __init__(self, config_path: str = 'config'):
        """Initialize database cleaner."""
        self.config_path = Path(config_path)
        self.config = self._load_config()
        self.engine = self._create_engine()
        
    def _load_config(self):
        """Load configuration files."""
        config = {}
        
        # Load environment
        env_file = Path('.env')
        if env_file.exists():
            load_dotenv(env_file)
        
        # Load YAML configs
        for file in ['database.yml', 'pipeline.yml']:
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

        return create_engine(db_url, pool_pre_ping=True, isolation_level="AUTOCOMMIT")

    def archive_old_data(self, schema: str, table: str, days: int):
        """Archive data older than specified days.
        
        Args:
            schema: Database schema
            table: Table name
            days: Archive data older than this many days
        """
        logger.info(f"\n{'=' * 60}")
        logger.info(f"ARCHIVING: {schema}.{table}")
        logger.info(f"ARCHIVE DATA OLDER THAN: {days} days")
        logger.info(f"{'=' * 60}\n")

        cutoff_date = datetime.now() - timedelta(days=days)
        archive_table = f"{table}_archive"

        try:
            with self.engine.connect() as conn:
                # Create archive table if not exists
                create_archive = text(f"""
                    CREATE TABLE IF NOT EXISTS {schema}.{archive_table}
                    (LIKE {schema}.{table} INCLUDING ALL)
                """)

                conn.execute(create_archive)
                logger.info(f"Archive table is ready: {schema}.{archive_table}")

                # Move old data to archive
                archive_query = text(f"""
                    WITH archived AS (
                        DELETE FROM {schema}.{table}
                        WHERE _ingestion_timestamp < :cutoff_date
                        RETURNING *
                    )
                    INSERT INTO {schema}.{archive_table}
                    SELECT * FROM archived
                """)

                result = conn.execute(archive_query, {'cutoff_date': cutoff_date})
                archived_count = result.rowcount

                logger.info(
                    f"Archived {archived_count:,} records to {schema}.{archive_table}"
                )

        except Exception as e:
            logger.error(f"Archive failed: {str(e)}")
            raise

    def vacuum_analyze(self, schema: Optional[str] = None):
        """Vacuum and analyze tables.
        
        Args:
            schema: Specific schema to vacuum (None = all)
        """
        logger.info(f"\n{'=' * 60}")
        logger.info("VACUUM AND ANALYZE")
        logger.info(f"{'=' * 60}\n")

        schemas = [schema] if schema else ['bronze', 'silver', 'gold', 'audit']

        try:
            with self.engine.connect() as conn:
                for schema_name in schemas:
                    logger.info(f"Processing schema: {schema_name}")

                    # Get all tables in schema
                    table_query = text(f"""
                        SELECT tablename
                        FROM pg_tables
                        WHERE schemaname = :schema
                    """)

                    tables = conn.execute(
                        table_query, {'schema': schema_name}
                    ).fetchall()

                    for (table,) in tables:
                        logger.info(f"Vacuuming {schema_name}.{table}")

                        # Vacuum and Analyze
                        vacuum_analyze = text(f"VACUUM ANALYZE {schema_name}.{table}")
                        conn.execute(vacuum_analyze)

                logger.info("Vacuum and analyze complete")

        except Exception as e:
            logger.error(f"Vacuum failed: {str(e)}")
            raise

    def clean_audit_logs(self, days: int = 90):
        """Clean old audit logs.
        
        Args:
            days: Keep logs from last N days
        """
        logger.info(f"\n{'=' * 60}")
        logger.info("CLEANING AUDIT LOGS")
        logger.info(f"KEEPING LAST: {days} days")
        logger.info(f"{'=' * 60}\n")

        cutoff_date = datetime.now() - timedelta(days=days)

        try:
            with self.engine.connect() as conn:
                # Delete logs
                delete_query = text(f"""
                    DELETE FROM audit.ingestion_log
                    WHERE start_time < :cutoff_date
                    AND status IN ('SUCCESS', 'FAILED')
                """)

                result = conn.execute(delete_query, {'cutoff_date': cutoff_date})
                deleted_rows = result.rowcount

                logger.info(f"Deleted {deleted_rows:,} old audit log entries")

        except Exception as e:
            logger.error(f"Audit cleaning failed: {str(e)}")
            raise

    def purge_bronze_layer(self, days: int = 7):
        """Purge bronze layer data after transformation.
        
        Args:
            days: Keep bronze data from last 7 days
        """
        logger.info(f"\n{'=' * 60}")
        logger.info("PURGING BRONZE LAYER")
        logger.info(f"KEEPING LAST: {days} days")
        logger.info(f"{'=' * 60}\n")

        cutoff_date = datetime.now() - timedelta(days=days)
        bronze_tables = ['dummyjson', 'fakestore', 'randomuser']

        try:
            with self.engine.connect() as conn:
                for table in bronze_tables:
                    delete_query = text(f"""
                        DELETE FROM bronze.{table}
                        WHERE _ingestion_timestamp < :cutoff_date
                    """)

                    result = conn.execute(delete_query, {'cutoff_date': cutoff_date})
                    deleted_count = result.rowcount

                    logger.info(f"Purged {deleted_count:,} records from bronze.{table}")

        except Exception as e:
            logger.error(f"Bronze purge failed: {str(e)}")

    def clean_temp_files(self, log_days: int = 30):
        """Clean temporary files and old logs.
        
        Args:
            log_days: Keep log files from last 30 days
        """
        logger.info(f"\n{'=' * 60}")
        logger.info("CLEANING TEMPORARY FILES")
        logger.info(f"{'=' * 60}\n")

        log_dir = Path('logs')
        cutoff_date = datetime.now() - timedelta(days=log_days)

        if not log_dir.exists():
            logger.info("No logs directory found")
            return

        deleted_count = 0
        total_size = 0

        for log_file in log_dir.glob('*.log*'):
            try:
                # Get file modification time
                mtime = datetime.fromtimestamp(log_file.stat().st_mtime)

                if mtime < cutoff_date:
                    file_size = log_file.stat().st_size
                    log_file.unlink()
                    deleted_count += 1
                    total_size += file_size

            except Exception as e:
                logger.warning(f"Could not delete {log_file}: {str(e)}")

        if deleted_count > 0:
            logger.info(
                f"Deleted {deleted_count} old log files "
                f"({total_size / 1024 / 1024:.2f} MB freed)"
            )
        else:
            logger.info("No old log files to delete")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Cleanup and maintenance for the data pipeline"
    )
    
    parser.add_argument(
        '--archive-days',
        type=int,
        help='Archive data older than N days'
    )
    parser.add_argument(
        '--vacuum',
        action='store_true',
        help='Run VACUUM ANALYZE on all tables'
    )
    parser.add_argument(
        '--clean-audit-logs',
        action='store_true',
        help='Clean old audit logs'
    )
    parser.add_argument(
        '--purge-bronze',
        action='store_true',
        help='Purge old bronze layer data'
    )
    parser.add_argument(
        '--clean-temp-files',
        action='store_true',
        help='Clean temporary files and old logs'
    )
    parser.add_argument(
        '--all',
        action='store_true',
        help='Run all cleanup tasks'
    )
    parser.add_argument(
        '--days',
        type=int,
        default=90,
        help='Number of days for retention (default: 90)'
    )
    parser.add_argument(
        '--schema',
        choices=['bronze', 'silver', 'gold', 'audit'],
        help='Specific schema to clean'
    )
    parser.add_argument(
        '--config-path',
        default='config',
        help='Path to configuration directory'
    )
    
    args = parser.parse_args()
    
    # Create logs directory
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    
    # Initialize cleaner
    cleaner = DatabaseCleaner(config_path=args.config_path)
    
    # Run requested tasks
    if args.all or args.archive_days:
        days = args.archive_days or args.days
        tables = ['products', 'orders', 'users', 'carts']
        for table in tables:
            try:
                cleaner.archive_old_data('silver', table, days)
            except Exception as e:
                logger.error(f"Archive failed for {table}: {str(e)}")
    
    if args.all or args.vacuum:
        cleaner.vacuum_analyze(args.schema)
    
    if args.all or args.clean_audit_logs:
        cleaner.clean_audit_logs(args.days)
    
    if args.all or args.purge_bronze:
        cleaner.purge_bronze_layer(days=7)
    
    if args.all or args.clean_temp_files:
        cleaner.clean_temp_files(log_days=30)
    
    logger.info("\nCleanup process complete")


if __name__ == "__main__":
    main()