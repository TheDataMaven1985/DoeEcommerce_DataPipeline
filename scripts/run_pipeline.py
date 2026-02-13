"""
Main Pipeline Runner Script - FIXED VERSION
Location: scripts/run_pipeline.py

Orchestrates the complete data pipeline execution:
1. Ingestion (Bronze Layer)
2. Transformation (Silver Layer)
3. Quality Checks
4. KPI Publishing (Gold Layer)
5. Audit Logging

Usage:
    python run_pipeline.py
    python run_pipeline.py --layer bronze
    python run_pipeline.py --source dummyjson
    python run_pipeline.py --skip-quality-checks
"""

import os
import sys
import argparse
import yaml
from pathlib import Path
from typing import Dict, List, Optional, Any
from datetime import datetime, timezone
from dotenv import load_dotenv
import logging
from sqlalchemy import create_engine

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

# Configure Logging
LOG_DIR = Path(__file__).parent.parent / 'logs'
LOG_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(name)s | %(message)s',
    handlers=[
        logging.FileHandler(LOG_DIR / 'pipeline.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Import pipeline components
try:
    from ingestion.dummyjson_ingestor import DummyJSONIngestor
    from ingestion.fakestore_ingestor import FakeStoreIngestor
    from ingestion.randomuser_ingestor import RandomUserIngestor
    from database.layers.silver.transform_silver import SilverTransform
    from database.layers.gold.publish_gold import GoldPublisher
    from database.layers.quality.quality_checks import QualityChecker
    from database.layers.audit.audit_writer import AuditWriter
except ImportError as e:
    logger.warning(f"Module import warning: {e}")
    logger.warning("Some modules may not be available.")


class RunPipeline:
    """Main Pipeline Orchestrator."""

    def __init__(self, config_path: str = 'config'):
        """Initialize pipeline runner."""
        # Resolve paths relative to project root (parent of scripts directory)
        self.project_root = Path(__file__).parent.parent
        self.config_path = self.project_root / config_path
        self.config = self._load_config()
        self.engine = self._create_engine()
        try:
            self.audit = AuditWriter(self.engine)
        except Exception as e:
            logger.warning(f"AuditWriter not available: {e}")
            self.audit = None
        self.run_id = None

    def _load_config(self) -> Dict[str, Any]:
        """Load configuration files."""
        config = {}

        # Load environment variables
        env_file = self.project_root / '.env'
        if env_file.exists():
            load_dotenv(env_file)
            logger.info("Loaded environment variables")
        else:
            logger.warning(".env file not found")

        # Load YAML config
        config_files = ['api_sources.yml', 'database.yml', 'pipeline.yml']
        for file in config_files:
            file_path = self.config_path / file
            if file_path.exists():
                with open(file_path, 'r') as f:
                    file_config = yaml.safe_load(f)
                    # Substitute environment variables
                    file_config = self._substitute_env_vars(file_config)
                    # Merge the config properly (don't nest it under another key)
                    config.update(file_config)
                logger.info(f"{file} loaded successfully")
            else:
                logger.warning(f"Config file {file} does not exist")

        return config

    def _substitute_env_vars(self, config: Any) -> Any:
        """Recursively substitute environment variables in config."""
        if isinstance(config, dict):
            return {k: self._substitute_env_vars(v) for k, v in config.items()}
        elif isinstance(config, list):
            return [self._substitute_env_vars(item) for item in config]
        elif isinstance(config, str) and config.startswith('${') and config.endswith('}'):
            env_var = config[2:-1]
            return os.getenv(env_var, config)
        return config

    def _create_engine(self):
        """Create database engine."""
        db_config = self.config.get('database', {}).get('postgres', {})

        # Get values with defaults
        host = db_config.get('host', 'localhost')
        port = db_config.get('port', 5432)
        username = db_config.get('username', 'postgres')
        password = db_config.get('password', 'postgres')
        database_name = db_config.get('database_name', 'DoeEcommerce')

        db_url = f"postgresql://{username}:{password}@{host}:{port}/{database_name}"

        engine = create_engine(db_url, pool_pre_ping=True)
        logger.info(f"Database engine created for {database_name}")
        return engine

    def run_ingestion(self, sources: Optional[List[str]] = None) -> Dict[str, Any]:
        """Run data ingestion for specified sources."""
        logger.info("=" * 60)
        logger.info("STARTING INGESTION LAYER (Bronze)")
        logger.info("=" * 60)
        
        results = {'timestamp': datetime.now(timezone.utc), 'sources': []} 
        
        # Define ingestors - note: source_name is passed to BaseIngestionGenerator
        ingestors = {
            'dummyjson': DummyJSONIngestor,
            'fakestore': FakeStoreIngestor,
            'randomuser': RandomUserIngestor
        }

        # Filter sources if specified
        if sources:
            ingestors = {k: v for k, v in ingestors.items() if k in sources}

        # Run each ingestor
        for source_name, IngestorClass in ingestors.items():
            try:
                logger.info(f"\nIngesting from {source_name}")
                
                # Create ingestor - pass only engine, config will be None so ingestor uses its defaults
                # The ingestors read from environment variables when config is None
                ingestor = IngestorClass(engine=self.engine, config=None)

                # Run ingestion
                metrics = ingestor.run_ingestion()

                # Log to audit
                if self.audit:
                    try:
                        self.audit.log_ingestion(
                            source_name=source_name,
                            table_name=f"bronze.{source_name}",
                            records_fetched=metrics.records_fetched,
                            records_loaded=metrics.records_loaded
                        )
                    except Exception as audit_error:
                        logger.warning(f"Audit logging failed: {audit_error}")

                results['sources'].append({
                    'source': source_name,
                    'status': metrics.status.value,
                    'records_fetched': metrics.records_fetched,
                    'records_loaded': metrics.records_loaded,
                    'duration': metrics.duration_seconds
                })

                logger.info(f"{source_name}: {metrics.records_loaded} records loaded")

            except Exception as e:
                logger.error(f"{source_name} Failed: {str(e)}", exc_info=True)
                results['sources'].append({
                    'source': source_name,
                    'status': 'FAILED',
                    'error': str(e) 
                })

        successful = len([s for s in results['sources'] if s.get('status') == 'SUCCESS'])
        logger.info(f"\nIngestion complete: {successful} sources successful")
        return results

    def run_transformation(self) -> Dict[str, Any]:
        """Run silver layer transformations."""
        logger.info("\n" + "=" * 60)
        logger.info("STARTING TRANSFORMATION LAYER (Silver)")
        logger.info("=" * 60)
        
        results = {'timestamp': datetime.now(timezone.utc), 'tables': []}
        
        try:
            transformer = SilverTransform(self.engine)
        except Exception as e:
            logger.error(f"Failed to initialize transformer: {e}")
            return results

        # Map bronze tables to transformation functions
        # Note: Bronze tables are named *_raw (e.g., products_raw, carts_raw)
        transformations = {
            'products_raw': ('products', transformer.transform_products),
            'carts_raw': ('carts', transformer.transform_carts),
            'users_raw': ('users', transformer.transform_users),
        }
        
        for bronze_table, (silver_table, transform_func) in transformations.items():
            try:
                logger.info(f"\nTransforming {bronze_table} -> {silver_table}")

                # Read from bronze
                import pandas as pd
                try:
                    bronze_df = pd.read_sql_table(
                        bronze_table,
                        self.engine,
                        schema='bronze'
                    )
                except Exception as read_error:
                    logger.warning(f"Could not read bronze.{bronze_table}: {read_error}")
                    continue

                if bronze_df.empty:
                    logger.warning(f"No data in bronze.{bronze_table}")
                    continue

                # Transform
                silver_df = transform_func(bronze_df)

                # Load to silver
                records_loaded = transformer.load_to_silver(silver_table, silver_df)

                results['tables'].append({
                    'table': silver_table,
                    'status': 'SUCCESS',
                    'records_transformed': len(silver_df),
                    'records_loaded': records_loaded
                })

                logger.info(f"{silver_table}: {records_loaded} records transformed")

            except Exception as e:
                logger.error(f"{bronze_table} transformation failed: {str(e)}", exc_info=True)
                results['tables'].append({
                    'table': silver_table,
                    'status': 'FAILED',
                    'error': str(e)
                })

        successful = len([t for t in results['tables'] if t.get('status') == 'SUCCESS'])
        logger.info(f"\nTransformation complete: {successful} tables processed")
        return results

    def run_quality_checks(self) -> Dict[str, Any]:
        """Run data quality checks."""
        logger.info("\n" + "=" * 60)
        logger.info("RUNNING QUALITY CHECKS")
        logger.info("=" * 60)

        results = {'timestamp': datetime.now(timezone.utc), 'checks': []}
        
        try:
            checker = QualityChecker(self.engine)
        except Exception as e:
            logger.error(f"Failed to initialize quality checker: {e}")
            return results

        # Tables to check in silver layer
        tables = ['products', 'carts', 'users']

        for table in tables:
            try:
                logger.info(f"\nChecking silver.{table}")
                check_result = checker.run_checks('silver', table)
                results['checks'].append(check_result)

                status = "✓" if check_result.get('failed', 0) == 0 else "✗"
                logger.info(
                    f"{status} {table}: {check_result.get('passed', 0)} passed, "
                    f"{check_result.get('failed', 0)} failed"
                )

            except Exception as e:
                logger.error(f"Quality check failed for {table}: {str(e)}", exc_info=True)
                results['checks'].append({
                    'table': f'silver.{table}',
                    'status': 'FAILED',
                    'error': str(e)
                })

        total_passed = sum(c.get('passed', 0) for c in results['checks'])
        total_failed = sum(c.get('failed', 0) for c in results['checks'])
        logger.info(f"\nQuality checks complete: {total_passed} passed, {total_failed} failed")

        return results

    def run_gold_publishing(self) -> Dict[str, Any]:
        """Publish KPIs to gold layer."""
        logger.info("\n" + "=" * 60)
        logger.info("PUBLISHING KPIs (Gold Layer)")
        logger.info("=" * 60)
        
        try:
            publisher = GoldPublisher(self.engine)
            results = publisher.publish_all_kpis()
        except Exception as e:
            logger.error(f"Failed to publish KPIs: {e}", exc_info=True)
            return {'timestamp': datetime.now(timezone.utc), 'kpis': [], 'error': str(e)}
        
        for kpi in results.get('kpis', []):
            status = "✓" if kpi.get('status') == 'SUCCESS' else "✗"
            logger.info(f"{status} {kpi.get('table', 'unknown')}: {kpi.get('status')}")
        
        logger.info("\nKPI publishing complete")
        return results

    def run_full_pipeline(
        self,
        sources: Optional[List[str]] = None,
        skip_quality: bool = False
    ) -> Dict[str, Any]:
        """Run the complete pipeline."""

        start_time = datetime.now(timezone.utc)
        
        if self.audit:
            try:
                self.run_id = self.audit.log_pipeline_start("DoeEcommercePipeline")
            except Exception as e:
                logger.warning(f"Could not log pipeline start: {e}")
                self.run_id = 1
        else:
            self.run_id = 1

        logger.info("\n" + "=" * 60)
        logger.info(f"PIPELINE RUN ID: {self.run_id}")
        logger.info(f"START TIME: {start_time.strftime('%Y-%m-%d %H:%M:%S UTC')}")
        logger.info("=" * 60)

        results = {
            'run_id': self.run_id,
            'start_time': start_time,
            'stages': {}
        }

        try:
            # Stage 1: Ingestion
            results['stages']['ingestion'] = self.run_ingestion(sources)

            # Stage 2: Transformation
            results['stages']['transformation'] = self.run_transformation()

            # Stage 3: Quality checks
            if not skip_quality:
                results['stages']['quality_checks'] = self.run_quality_checks()

            # Stage 4: Gold Publishing
            results['stages']['gold_publishing'] = self.run_gold_publishing()

            # Audit
            if self.audit:
                try:
                    self.audit.log_pipeline_end(self.run_id, 'SUCCESS')
                except Exception as e:
                    logger.warning(f"Could not log pipeline end: {e}")
            results['status'] = 'SUCCESS'

        except Exception as e:
            logger.error(f"\nPipeline failed: {str(e)}", exc_info=True)
            if self.audit:
                try:
                    self.audit.log_pipeline_end(self.run_id, 'FAILED')
                except Exception as audit_error:
                    logger.warning(f"Could not log pipeline failure: {audit_error}")
            results['status'] = 'FAILED'
            results['error'] = str(e)

        finally:
            end_time = datetime.now(timezone.utc)
            duration = (end_time - start_time).total_seconds()
            results['end_time'] = end_time
            results['duration_seconds'] = duration
            
            logger.info("\n" + "=" * 60)
            logger.info(f"PIPELINE {results.get('status', 'UNKNOWN')}")
            logger.info(f"DURATION: {duration:.2f} seconds")
            logger.info(f"END TIME: {end_time.strftime('%Y-%m-%d %H:%M:%S UTC')}")
            logger.info("=" * 60)
        
        return results


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Run the DoeEcommerce data pipeline" 
    )
    parser.add_argument(
        '--layer',
        choices=['bronze', 'silver', 'gold', 'quality', 'all'],
        default='all',
        help='Pipeline layer to run (default: all)'
    )
    parser.add_argument(
        '--source',
        choices=['dummyjson', 'fakestore', 'randomuser'],
        help="Specific source to ingest (bronze layer only)"
    )
    parser.add_argument(
        '--skip-quality-checks',
        action='store_true',
        help='Skip quality checks'
    )
    parser.add_argument(
        '--config-path',
        default='config',
        help='Path to configuration directory (default: config)'
    )

    args = parser.parse_args()
    
    # Create logs directory
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    try:
        # Initialize Pipeline
        pipeline = RunPipeline(config_path=args.config_path)

        # Run requested layer
        if args.layer == 'bronze':
            sources = [args.source] if args.source else None
            pipeline.run_ingestion(sources)
        elif args.layer == 'silver':
            pipeline.run_transformation()
        elif args.layer == 'gold':
            pipeline.run_gold_publishing()
        elif args.layer == 'quality':
            pipeline.run_quality_checks()
        else:
            # Run full pipeline
            sources = [args.source] if args.source else None
            pipeline.run_full_pipeline(
                sources=sources,
                skip_quality=args.skip_quality_checks
            )
    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()