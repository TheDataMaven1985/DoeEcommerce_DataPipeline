"""
Quality Layer: Data Quality Checks
Location: database/layers/quality/quality_checks.py

Validates data quality
"""

from typing import Dict, List, Any, Tuple
from sqlalchemy import text
import logging

logger = logging.getLogger(__name__)


class QualityChecker:
    """Perform data quality checks"""
    
    def __init__(self, engine):
        self.engine = engine
    
    def check_null_values(self, schema: str, table: str, column: str) -> Tuple[bool, int]:
        """Check for NULL values"""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(f"""
                    SELECT COUNT(*) FROM {schema}.{table}
                    WHERE {column} IS NULL
                """)).scalar()
                return result == 0, result
        except Exception as e:
            logger.error(f"Check failed: {str(e)}")
            return False, -1
    
    def check_duplicates(self, schema: str, table: str, column: str) -> Tuple[bool, int]:
        """Check for duplicates"""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(f"""
                    SELECT COUNT(*) FROM (
                        SELECT {column}, COUNT(*) as cnt
                        FROM {schema}.{table}
                        WHERE {column} IS NOT NULL
                        GROUP BY {column}
                        HAVING COUNT(*) > 1
                    ) duplicates
                """)).scalar()
                return result == 0, result
        except Exception as e:
            logger.error(f"Check failed: {str(e)}")
            return False, -1
    
    def run_checks(self, schema: str, table: str) -> Dict[str, Any]:
        """Run all checks"""
        results = {'table': f'{schema}.{table}', 'checks': [], 'passed': 0, 'failed': 0}
        
        # Map table names to their primary key columns
        primary_key_map = {
            'products': 'product_id',
            'orders': 'order_id',
            'users': 'user_id',
            'carts': 'cart_id',
            'products_raw': 'id',
            'orders_raw': 'id',
            'users_raw': 'id',
            'carts_raw': 'id',
        }
        
        # Get the primary key column for this table
        pk_column = primary_key_map.get(table, 'id')
        
        # Check nulls in primary key
        passed, count = self.check_null_values(schema, table, pk_column)
        results['checks'].append({'name': f'null_check_{pk_column}', 'passed': passed})
        if passed: results['passed'] += 1
        else: results['failed'] += 1
        
        logger.info(f"Quality checks: {results['passed']} passed, {results['failed']} failed")
        return results
