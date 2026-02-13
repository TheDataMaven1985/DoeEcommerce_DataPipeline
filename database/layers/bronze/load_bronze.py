"""
Bronze Layer: Data Loader
Location: database/layers/bronze/load_bronze.py

Loads raw data into bronze layer tables
"""

import pandas as pd 
from typing import Dict, List, Any
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

class BronzeLoader:
    """ Load raw data into bronze tables. """

    def __init__(self, engine, schema: str = 'bronze'):
        self.engine = engine
        self.schema = schema
        self.metrics = {'tables_loaded': 0, 'tables_rows_inserted': 0}

    def load_products(self, data: List[Dict[str, Any]], source_name: str) -> Dict[str, Any]:
        """ load products data into bronze.products_raw. """
        return self._load_table('products_raw', data, source_name, {
            'id': '_source_id', 
            'title': 'title', 
            'category': 'category',
            'description': 'description',
            'image': 'image',
            'rating_rate': 'rating',
            'rating_count': 'rating_count'
        })

    def load_users(self, data: List[Dict[str, Any]], source_name: str) -> Dict[str, Any]:
        """ load users data into bronze.users_raw. """
        return self._load_table('users_raw', data, source_name, {
            'id': '_source_id',
            'username': 'username',
            'email': 'email',
            'firstname': 'first_name',
            'lastname': 'last_name',
            'city': 'city',
            'phone': 'phone'
        })

    def load_orders(self, data: List[Dict[str, Any]], source_name: str) -> Dict[str, Any]:
        """ load orders data into bronze.orders_raw. """
        return self._load_table('orders_raw', data, source_name, {
            'id': '_source_id',
            'userid': 'userid',
            'date': 'date'
        })

    def load_carts(self, data: List[Dict[str, Any]], source_name: str) -> Dict[str, Any]:
        """ load carts data into bronze.carts_raw. """
        return self._load_table('carts_raw', data, source_name, {
            'id': '_source_id',
            'userid': 'userid',
            'total': 'total',
            'discountedtotal': 'discountedtotal'
        })

    def load_table(self, table_name: str, data: List[Dict[str, Any]], 
              source_name: str, mapping: Dict[str, str]) -> Dict[str, Any]:
    """ Load data into a specific table. """  # FIXED: Method name and signature
    try:
        if not data:
            return {'table': table_name, 'rows_inserted': 0, 'status': 'SUCCESS'}
        
        df = pd.DataFrame(data)
        df = df[[col for col in mapping.keys() if col in df.columns]].copy()
        df.rename(columns=mapping, inplace=True)
        df['_source_name'] = source_name
        df['_ingestion_timestamp'] = datetime.utcnow()

        df.to_sql(table_name, self.engine, schema=self.schema, 
                 if_exists='append', index=False, chunksize=1000)

        rows = len(df)
        self.metrics['total_rows_inserted'] += rows
        self.metrics['table_loaded'] += 1
        logger.info(f" Loaded {rows} rows into {self.schema}.{table_name}")

        return {'table': table_name, 'rows_inserted': rows, 'status': 'SUCCESS'}
    except Exception as e:
        logger.error(f" Error: {str(e)}")
        return {'table': table_name, 'rows_inserted': 0, 'status': 'FAILED', 'error': str(e)}

    def get_metrics(self) -> Dict[str, Any]:
        """ Get metrics. """
        return self.metrics.copy()