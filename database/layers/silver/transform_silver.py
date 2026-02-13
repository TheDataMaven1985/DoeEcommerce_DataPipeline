"""
Silver Layer: Data Transformer
Location: database/layers/silver/transform_silver.py

Transforms and deduplicates bronze data to silver
"""

import pandas as pd 
from typing import Dict, List, Any
from datetime import datetime, timezone
from sqlalchemy import text
import logging
import numpy as np

logger = logging.getLogger(__name__)


class SilverTransform:
    """ Transfrom bronze data to silver. """

    def __init__(self, engine):
        self.engine = engine

    def transform_products(self, bronze_df: pd.DataFrame) -> pd.DataFrame:
        """ Transform products data: deduplicate and clean. """

        df = bronze_df.copy()
        df = df.sort_values('_ingestion_timestamp').drop_duplicates(
            subset=['id'], keep='last'
        )
        df['product_id'] = df['id'].astype('int64')  
        df['price'] = df['price'].astype('float64')
        df['last_updated'] = datetime.now(timezone.utc)
        return df[['product_id', 'title', 'price', 'category', 'last_updated']].drop_duplicates()

    def transform_orders(self, bronze_df: pd.DataFrame) -> pd.DataFrame:
        """ Transform orders data: deduplicate and clean. """

        df = bronze_df.copy()
        df = df.sort_values('_ingestion_timestamp').drop_duplicates(
            subset=['id'], keep='last')
        df['order_id'] = df['id'].astype('int64')  
        df['user_id'] = df['userid'].astype('int64')
        df['total_amount'] = df['total_amount'].astype('float64')
        df['final_amount'] = df['total_amount']
        df['last_updated'] = datetime.now(timezone.utc)
        return df[['order_id', 'user_id', 'total_amount', 'final_amount', 'last_updated']].drop_duplicates()

    def transform_users(self, bronze_df: pd.DataFrame) -> pd.DataFrame:
        """ Transform users data: dedupicate and clean. """

        df = bronze_df.copy()
        if 'email' in df.columns:
            df = df.sort_values('_ingestion_timestamp').drop_duplicates(
                subset=['email'], keep='last')
        df['user_id'] = df['id'].astype('int64')
        df['full_name'] = (df['firstname'].fillna('') + ' ' + 
                       df['lastname'].fillna('')).str.strip()
        df['last_updated'] = datetime.now(timezone.utc)
        return df[['user_id', 'email', 'full_name', 'last_updated']].drop_duplicates()

    def transform_carts(self, bronze_df: pd.DataFrame) -> pd.DataFrame:
        """ Transform carts data: deduplicate and clean. """

        df = bronze_df.copy()
        df = df.sort_values('_ingestion_timestamp').drop_duplicates(
            subset=['id'], keep='last')
        df['cart_id'] = df['id'].astype('int64')
        df['user_id'] = df['userId'].astype('int64')
        df['total_value'] = df['total'].astype('float64')
        
        # Handle zero division properly
        df['discount_percentage'] = np.where(
            df['total'] > 0,
            (((df['total'] - df['discountedTotal']) / df['total']) * 100).round(2),
            0.0
        )
        
        df['last_updated'] = datetime.now(timezone.utc)
        return df[['cart_id', 'user_id', 'total_value', 'discount_percentage', 'last_updated']].drop_duplicates()

    def load_to_silver(self, table_name: str, df: pd.DataFrame) -> int:
        """ Load to silver table using UPSERT. """

        if df.empty:
            return 0

        # Define PK for each table
        pk_mapping = {
            'products': 'product_id',
            'orders': 'order_id',
            'users': 'user_id',
            'carts': 'cart_id'
        }

        pk = pk_mapping.get(table_name)

        try: 
            with self.engine.begin() as conn:
                # Create a temporary table for upsert
                temp_table = f"{table_name}_temp"
                df.to_sql(temp_table, conn, schema='silver', if_exists='replace', index=False)

                # Perform UPSERT (Delete old records and insert new ones) ensuring we have lastest version of each record
                if pk:
                    conn.execute(text(f"""
                        DELETE FROM silver.{table_name}
                        WHERE {pk} IN (SELECT {pk} FROM silver.{temp_table});
                    """))

                # Insert the fresh data
                df.to_sql(table_name, conn, schema='silver', if_exists='append', index=False)

                # Cleanup
                conn.execute(text(f"DROP TABLE silver.{temp_table};"))

            logger.info(f" Successfully upserted {len(df)} records into silver.{table_name}")
            return len(df)

        except Exception as e:
            logger.error(f" Silver Load Error for {table_name}: {str(e)}")
            raise
