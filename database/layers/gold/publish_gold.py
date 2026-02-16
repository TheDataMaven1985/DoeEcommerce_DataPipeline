"""
Gold Layer: KPI Publisher
Location: database/layers/gold/publish_gold.py
"""

from typing import Dict, Any, Optional
from sqlalchemy import text
from datetime import datetime, date, timezone
import logging

logger = logging.getLogger(__name__)

class GoldPublisher:
    """Publish KPIs using Carts, Products, and Users instead of Orders"""
    
    def __init__(self, engine, kpi_date: Optional[date] = None):
        self.engine = engine
        self.kpi_date = kpi_date or date.today()
    
    def publish_finance_kpis(self, kpi_date: Optional[date] = None) -> Dict[str, Any]:
        """Finance KPIs: Tracking Potential Revenue from Carts"""
        try:
            kpi_date = kpi_date or self.kpi_date
            with self.engine.connect() as conn:
                conn.execute(text("""
                    INSERT INTO gold.finance_mart 
                    (date, total_revenue, daily_revenue, average_order_value, carts_count)
                    SELECT :kpi_date, 
                           SUM(total_value),
                           SUM(total_value),
                           AVG(total_value),
                           COUNT(*)
                    FROM silver.carts
                    WHERE CAST(last_updated AS DATE) = :kpi_date
                    ON CONFLICT (date) DO UPDATE SET
                        total_revenue = EXCLUDED.total_revenue,
                        daily_revenue = EXCLUDED.daily_revenue,
                        average_order_value = EXCLUDED.average_order_value,
                        carts_count = EXCLUDED.carts_count, -- Fixed name
                        updated_at = CURRENT_TIMESTAMP
                """), {'kpi_date': kpi_date})
                conn.commit()
                return {'status': 'SUCCESS', 'table': 'gold.finance_mart', 'date': str(kpi_date)}
        except Exception as e:
            logger.error(f"Finance KPI Error: {str(e)}")
            return {'status': 'FAILED', 'error': str(e)}

    def publish_sales_kpis(self) -> Dict[str, Any]:
        """Publish sales KPIs using Cart and Product counts"""
        try:
            with self.engine.connect() as conn:
                conn.execute(text("""
                    INSERT INTO gold.sales_mart (date, total_carts, customer_count, product_count)
                    SELECT 
                        CURRENT_DATE, 
                        (SELECT COUNT(*) FROM silver.carts WHERE CAST(last_updated AS DATE) = CURRENT_DATE),
                        (SELECT COUNT(DISTINCT user_id) FROM silver.carts WHERE CAST(last_updated AS DATE) = CURRENT_DATE),
                        (SELECT COUNT(*) FROM silver.products)
                    ON CONFLICT (date) DO UPDATE SET
                        total_carts = EXCLUDED.total_carts,
                        customer_count = EXCLUDED.customer_count,
                        product_count = EXCLUDED.product_count,
                        updated_at = CURRENT_TIMESTAMP
                """))
                conn.commit()
                return {'status': 'SUCCESS', 'table': 'gold.sales_mart'}
        except Exception as e:
            logger.error(f"Sales KPI Error: {str(e)}")
            return {'status': 'FAILED', 'error': str(e)}

    def publish_operations_kpis(self) -> Dict[str, Any]:
        """Operations KPIs: Tracking Average Discounting Rates"""
        try:
            with self.engine.connect() as conn:
                conn.execute(text("""
                    INSERT INTO gold.operations_mart (date, order_fulfillment_rate)
                    SELECT CURRENT_DATE, 
                           COALESCE(AVG(discount_percentage), 0)
                    FROM silver.carts
                    WHERE CAST(last_updated AS DATE) = CURRENT_DATE
                    ON CONFLICT (date) DO UPDATE SET
                        order_fulfillment_rate = EXCLUDED.order_fulfillment_rate,
                        updated_at = CURRENT_TIMESTAMP
                """))
                conn.commit()
                return {'status': 'SUCCESS', 'table': 'gold.operations_mart'}
        except Exception as e:
            logger.error(f"Operations KPI Error: {str(e)}")
            return {'status': 'FAILED', 'error': str(e)}

    def publish_all_kpis(self) -> Dict[str, Any]:
        """Publish all KPIs"""
        results = {'timestamp': datetime.now(timezone.utc), 'kpis': []}
        results['kpis'].append(self.publish_finance_kpis())
        results['kpis'].append(self.publish_sales_kpis())
        results['kpis'].append(self.publish_operations_kpis())
        return results