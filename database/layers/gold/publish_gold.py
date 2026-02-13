"""
Gold Layer: KPI Publisher
Location: database/layers/gold/publish_gold.py

Publishes KPIs to gold marts
"""

from typing import Dict, Any, Optional
from sqlalchemy import text
from datetime import datetime, date, timezone
import logging

logger = logging.getLogger(__name__)


class GoldPublisher:
    """Publish KPIs to gold layer"""
    
    def __init__(self, engine, kpi_date: Optional[date] = None):
        self.engine = engine
        self.kpi_date = kpi_date or date.today()
    
    def publish_finance_kpis(self, kpi_date: Optional[date] = None) -> Dict[str, Any]:
        """Publish finance KPIs"""
        try:
            kpi_date = kpi_date or self.kpi_date
            
            with self.engine.connect() as conn:
                conn.execute(text("""
                    INSERT INTO gold.finance_mart 
                    (date, total_revenue, daily_revenue, average_order_value, order_count)
                    SELECT :kpi_date, 
                           SUM(final_amount) as total_revenue,
                           SUM(final_amount) as daily_revenue,
                           AVG(final_amount) as average_order_value,
                           COUNT(*) as order_count
                    FROM silver.orders
                    WHERE CAST(last_updated AS DATE) = :kpi_date
                    ON CONFLICT (date) DO UPDATE SET
                        total_revenue = EXCLUDED.total_revenue,
                        daily_revenue = EXCLUDED.daily_revenue,
                        average_order_value = EXCLUDED.average_order_value,
                        order_count = EXCLUDED.order_count,
                        updated_at = CURRENT_TIMESTAMP
                """), {
                    'kpi_date': kpi_date
                })
                conn.commit()
                logger.info(f"Finance KPIs published for {kpi_date}")
                return {'status': 'SUCCESS', 'table': 'gold.finance_mart', 'date': str(kpi_date)}
        except Exception as e:
            logger.error(f"Error: {str(e)}")
            return {'status': 'FAILED', 'error': str(e)}
    
    def publish_sales_kpis(self) -> Dict[str, Any]:
        """Publish sales KPIs"""
        try:
            with self.engine.connect() as conn:
                conn.execute(text("""
                    INSERT INTO gold.sales_mart (date, total_orders, customer_count)
                    SELECT CURRENT_DATE, COUNT(*), COUNT(DISTINCT user_id)
                    FROM silver.orders
                    WHERE CAST(last_updated AS DATE) = CURRENT_DATE
                    ON CONFLICT (date) DO UPDATE SET
                        total_orders = EXCLUDED.total_orders,
                        customer_count = EXCLUDED.customer_count,
                        updated_at = CURRENT_TIMESTAMP
                """))
                conn.commit()
                logger.info(" Sales KPIs published")
                return {'status': 'SUCCESS', 'table': 'gold.sales_mart'}
        except Exception as e:
            logger.error(f" Error: {str(e)}")
            return {'status': 'FAILED', 'error': str(e)}
    
    def publish_operations_kpis(self) -> Dict[str, Any]:
        """Publish operations KPIs"""
        try:
            with self.engine.connect() as conn:
                conn.execute(text("""
                    INSERT INTO gold.operations_mart (date, order_fulfillment_rate)
                    SELECT CURRENT_DATE, 
                           CASE WHEN COUNT(*) = 0 THEN 0 
                                ELSE ROUND(100.0 * COUNT(*) / COUNT(*), 2) END
                    FROM silver.orders
                    WHERE CAST(last_updated AS DATE) = CURRENT_DATE
                    ON CONFLICT (date) DO UPDATE SET
                        order_fulfillment_rate = EXCLUDED.order_fulfillment_rate,
                        updated_at = CURRENT_TIMESTAMP
                """))
                conn.commit()
                logger.info(" Operations KPIs published")
                return {'status': 'SUCCESS', 'table': 'gold.operations_mart'}
        except Exception as e:
            logger.error(f" Error: {str(e)}")
            return {'status': 'FAILED', 'error': str(e)}
    
    def publish_all_kpis(self) -> Dict[str, Any]:
        """Publish all KPIs"""
        results = {'timestamp': datetime.now(timezone.utc), 'kpis': []}
        results['kpis'].append(self.publish_finance_kpis())
        results['kpis'].append(self.publish_sales_kpis())
        results['kpis'].append(self.publish_operations_kpis())
        logger.info(" All KPIs published")
        return results
