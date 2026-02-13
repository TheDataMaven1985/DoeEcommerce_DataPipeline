"""
Audit Layer: Pipeline Auditing
Location: database/layers/audit/audit_writer.py

Logs all pipeline activities
"""

from datetime import datetime, timezone
from sqlalchemy import text
import logging

logger = logging.getLogger(__name__)


class AuditWriter:
    """Log pipeline activities"""
    
    def __init__(self, engine):
        self.engine = engine
    
    def log_pipeline_start(self, pipeline_name: str) -> int:
        """Log pipeline start"""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("""
                    INSERT INTO audit.ingestion_log
                    (source_name, table_name, status, start_time)
                    VALUES (:name, :table_name, :status, :start)
                    RETURNING log_id
                """), {
                    'name': pipeline_name,
                    'table_name': pipeline_name.split('_')[0] if '_' in pipeline_name else 'default',
                    'status': 'IN_PROGRESS',
                    'start': datetime.now(timezone.utc)
                })

                run_id = result.scalar()
                conn.commit()
                logger.info(f" Pipeline run {run_id} started")
                return run_id

        except Exception as e:
            logger.error(f" Error: {str(e)}")
            return 0
    
    def log_ingestion(self, source_name: str, table_name: str,
                     records_fetched: int, records_loaded: int, start_time: datetime):
        """Log ingestion"""
        try:
            with self.engine.connect() as conn:
                conn.execute(text("""
                    INSERT INTO audit.ingestion_log
                    (source_name, table_name, records_fetched, records_loaded, status, start_time, end_time)
                    VALUES (:source, :table, :fetched, :loaded, :status, :start_time, :end_time)
                """), {
                    'source': source_name,
                    'table': table_name,
                    'fetched': records_fetched,
                    'loaded': records_loaded,
                    'status': 'SUCCESS',
                    'start_time': start_time,
                    'end_time': datetime.now(timezone.utc)
                })
                conn.commit()
                logger.info(f" Logged {table_name}")
        except Exception as e:
            logger.error(f" Error: {str(e)}")
    
    def log_pipeline_end(self, run_id: int, status: str = 'SUCCESS'):
        """Log pipeline end"""
        try:
            with self.engine.connect() as conn:
                conn.execute(text("""
                    UPDATE audit.ingestion_log
                    SET status = :status, end_time = :end
                    WHERE log_id = :id
                """), {
                    'status': status,
                    'end': datetime.now(timezone.utc),
                    'id': run_id
                })
                conn.commit()
                logger.info(f" Pipeline run {run_id} ended: {status}")
        except Exception as e:
            logger.error(f" Error: {str(e)}")