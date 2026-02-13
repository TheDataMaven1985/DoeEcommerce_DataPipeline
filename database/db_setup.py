"""
PostgreSQL Database Setup Script for Data Pipeline

Creates all necessary schemas and tables for the DoeEcommerce data pipeline:
- Bronze Layer (Raw ingested data)
- Silver Layer (Cleaned and deduplicated data)
- Gold Layer (Department-specific marts)
- Audit Layer (Tracking tables)

Usage:
    python setup_database.py
    python setup_database.py --host localhost --port 5432 --user postgres
    python setup_database.py --env-file .env
"""

import os
import sys
import argparse
from pathlib import Path
from typing import Optional, Dict, List
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DatabaseSetup:
    """Setup PostgreSQL database for data pipeline."""
    
    def __init__(
        self,
        host: str = "localhost",
        port: int = 5432,
        user: str = "postgres",
        password: str = "postgres",
        database: str = "DoeEcommerce",
        dry_run: bool = False,
    ):
        """Initialize database setup.
        
        Args:
            host: PostgreSQL host
            port: PostgreSQL port
            user: PostgreSQL user
            password: PostgreSQL password
            database: Database name to create
            dry_run: If True, only print SQL without executing
        """
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.dry_run = dry_run
        self.connection = None
        self.cursor = None
    
    def connect(self) -> bool:
        """Connect to PostgreSQL."""
        try:
            import psycopg2
            self.connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database="postgres"  # Connect to default database first
            )
            self.connection.autocommit = True
            self.cursor = self.connection.cursor()
            logger.info(f" Connected to PostgreSQL ({self.host}:{self.port})")
            return True
        except Exception as e:
            logger.error(f" Failed to connect to PostgreSQL: {e}")
            return False
    
    def disconnect(self):
        """Disconnect from PostgreSQL."""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        logger.info(" Disconnected from PostgreSQL")
    
    def execute(self, query: str) -> bool:
        """Execute SQL query.
        
        Args:
            query: SQL query to execute
            
        Returns:
            True if successful, False otherwise
        """
        try:
            if self.dry_run:
                logger.info(f"\n[DRY RUN] Would execute:\n{query}\n")
                return True
            
            self.cursor.execute(query)
            logger.debug(f" Executed: {query[:50]}...")
            return True
        except Exception as e:
            logger.error(f" Query failed: {e}")
            logger.error(f"Query: {query[:100]}...")
            return False
    
    def create_database(self) -> bool:
        """Create the main database."""
        logger.info(f"\nCreating database '{self.database}'...")
        
        # Validate database name first
        import re
        if not re.match(r'^[a-zA-Z0-9_]+$', self.database):
            logger.error(f"Invalid database name: {self.database}")
            return False
        
        # Check if database exists 
        check_query = "SELECT datname FROM pg_database WHERE LOWER(datname) = LOWER(%(dbname)s)"
        self.cursor.execute(check_query, {'dbname': self.database})
        
        result = self.cursor.fetchone()
        if result:
            # Use the actual database name from PostgreSQL
            actual_db_name = result[0]
            logger.info(f" Database '{actual_db_name}' already exists")
            # Reconnect to the existing database
            if self.cursor:
                self.cursor.close()
            if self.connection:
                self.connection.close()
            
            import psycopg2
            self.connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=actual_db_name  
            )
            self.connection.autocommit = True
            self.cursor = self.connection.cursor()
            logger.info(f" Connected to existing '{actual_db_name}' database")
            return True
        
        # PostgreSQL converts unquoted identifiers to lowercase, so use lowercase
        db_name_lower = self.database.lower()
        create_query = f"CREATE DATABASE {db_name_lower} ENCODING 'UTF8';"
        
        if self.execute(create_query):
            logger.info(f" Created database '{db_name_lower}'")
            
            # Reconnect to the new database
            # Close existing cursor and connection manually instead of using disconnect()
            if self.cursor:
                self.cursor.close()
            if self.connection:
                self.connection.close()
            
            # Import psycopg2 and reconnect
            import psycopg2
            self.connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=db_name_lower  # Use lowercase name
            )
            self.connection.autocommit = True
            self.cursor = self.connection.cursor()
            logger.info(f" Connected to '{db_name_lower}' database")
            return True
        
        return False
    
    def create_schemas(self) -> bool:
        """Create database schemas."""
        logger.info("\n Creating schemas...")
        
        schemas = {
            'bronze': 'Raw ingested data from APIs',
            'silver': 'Cleaned and deduplicated data',
            'gold': 'Department-specific data marts',
            'audit': 'Tracking and audit logs'
        }
        
        for schema_name, description in schemas.items():
            query = f"""
            CREATE SCHEMA IF NOT EXISTS {schema_name}
            AUTHORIZATION {self.user};
            COMMENT ON SCHEMA {schema_name} IS '{description}';
            """
            
            if self.execute(query):
                logger.info(f" Created schema '{schema_name}'")
            else:
                logger.error(f" Failed to create schema '{schema_name}'")
                return False
        
        return True
    
    def create_bronze_tables(self) -> bool:
        """Create bronze layer tables (raw data)."""
        logger.info("\n Creating bronze layer tables...")
        
        tables = {
            'products_raw': self._get_products_table_ddl(),
            'carts_raw': self._get_carts_table_ddl(),
            'orders_raw': self._get_orders_table_ddl(),
            'users_raw': self._get_users_table_ddl(),
        }
        
        for table_name, ddl in tables.items():
            if self.execute(ddl):
                logger.info(f" Created table 'bronze.{table_name}'")
            else:
                logger.error(f" Failed to create table 'bronze.{table_name}'")
                return False
        
        return True
    
    def create_silver_tables(self) -> bool:
        """Create silver layer tables (cleaned data)."""
        logger.info("\n Creating silver layer tables...")
        
        tables = {
            'products': self._get_products_silver_ddl(),
            'carts': self._get_carts_silver_ddl(),
            'orders': self._get_orders_silver_ddl(),
            'users': self._get_users_silver_ddl(),
        }
        
        for table_name, ddl in tables.items():
            if self.execute(ddl):
                logger.info(f" Created table 'silver.{table_name}'")
            else:
                logger.error(f" Failed to create table 'silver.{table_name}'")
                return False
        
        return True
    
    def create_gold_tables(self) -> bool:
        """Create gold layer tables (department marts)."""
        logger.info("\n Creating gold layer tables...")
        
        # Finance mart
        finance_query = """
        CREATE TABLE IF NOT EXISTS gold.finance_mart (
            mart_id SERIAL PRIMARY KEY,
            date DATE NOT NULL UNIQUE,
            total_revenue DECIMAL(15, 2),
            daily_revenue DECIMAL(15, 2),
            average_order_value DECIMAL(15, 2),
            order_count INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_finance_date ON gold.finance_mart(date);
        COMMENT ON TABLE gold.finance_mart IS 'Finance department KPI mart';
        """
        
        # Sales mart
        sales_query = """
        CREATE TABLE IF NOT EXISTS gold.sales_mart (
            mart_id SERIAL PRIMARY KEY,
            date DATE NOT NULL UNIQUE,
            total_orders INTEGER,
            top_products VARCHAR(500),
            customer_count INTEGER,
            product_count INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_sales_date ON gold.sales_mart(date);
        COMMENT ON TABLE gold.sales_mart IS 'Sales department KPI mart';
        """
        
        # Operations mart
        operations_query = """
        CREATE TABLE IF NOT EXISTS gold.operations_mart (
            mart_id SERIAL PRIMARY KEY,
            date DATE NOT NULL UNIQUE,
            order_fulfillment_rate DECIMAL(5, 2),
            cart_abandonment_rate DECIMAL(5, 2),
            avg_processing_time_hours DECIMAL(10, 2),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_operations_date ON gold.operations_mart(date);
        COMMENT ON TABLE gold.operations_mart IS 'Operations department KPI mart';
        """
        
        for table_name, query in [
            ('finance_mart', finance_query),
            ('sales_mart', sales_query),
            ('operations_mart', operations_query),
        ]:
            if self.execute(query):
                logger.info(f" Created table 'gold.{table_name}'")
            else:
                logger.error(f" Failed to create table 'gold.{table_name}'")
                return False
        
        return True
    
    def create_audit_tables(self) -> bool:
        """Create audit tracking tables."""
        logger.info("\n Creating audit layer tables...")
        
        audit_query = """
        CREATE TABLE IF NOT EXISTS audit.ingestion_log (
            log_id SERIAL PRIMARY KEY,
            source_name VARCHAR(100) NOT NULL,
            table_name VARCHAR(100) NOT NULL,
            records_fetched INTEGER DEFAULT 0,
            records_loaded INTEGER DEFAULT 0,
            records_failed INTEGER DEFAULT 0,
            status VARCHAR(50),
            start_time TIMESTAMP NOT NULL,
            end_time TIMESTAMP,
            duration_seconds DECIMAL(10, 2),
            error_message TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_ingestion_source ON audit.ingestion_log(source_name);
        CREATE INDEX IF NOT EXISTS idx_ingestion_date ON audit.ingestion_log(created_at);
        COMMENT ON TABLE audit.ingestion_log IS 'Ingestion process tracking and auditing';
        """
        
        if self.execute(audit_query):
            logger.info(f" Created table 'audit.ingestion_log'")
        else:
            logger.error(f" Failed to create audit table")
            return False
        
        return True
    
    def create_views(self) -> bool:
        """Create useful views."""
        logger.info("\n Creating views...")
        
        # Daily revenue view
        daily_revenue_view = """
        CREATE OR REPLACE VIEW gold.vw_daily_revenue AS
        SELECT
            CAST(o.last_updated AS DATE) as order_date,
            COUNT(*) as total_orders,
            SUM(COALESCE(c.total_value, 0)) as total_revenue,
            AVG(COALESCE(c.total_value, 0)) as avg_order_value,
            COUNT(DISTINCT o.user_id) as unique_customers
        FROM silver.orders o
        LEFT JOIN silver.carts c ON o.user_id = c.user_id
        GROUP BY CAST(o.last_updated AS DATE)
        ORDER BY order_date DESC;
        """
        
        # Product popularity view
        product_popularity_view = """
        CREATE OR REPLACE VIEW gold.vw_product_popularity AS
        SELECT
            p.product_id,
            p.title,
            p.category,
            0 as order_count,
            p.price as avg_price,
            0 as total_value
        FROM silver.products p
        ORDER BY p.price DESC;
        """
        
        # User activity view
        user_activity_view = """
        CREATE OR REPLACE VIEW gold.vw_user_activity AS
        SELECT
            u.user_id,
            u.email,
            u.full_name,
            COUNT(DISTINCT o.order_id) as total_orders,
            COUNT(DISTINCT c.cart_id) as total_carts,
            MAX(CAST(o.last_updated AS DATE)) as last_order_date
        FROM silver.users u
        LEFT JOIN silver.orders o ON u.user_id = o.user_id
        LEFT JOIN silver.carts c ON u.user_id = c.user_id
        GROUP BY u.user_id, u.email, u.full_name
        ORDER BY total_orders DESC;
        """
        
        views = [
            ('vw_daily_revenue', daily_revenue_view),
            ('vw_product_popularity', product_popularity_view),
            ('vw_user_activity', user_activity_view),
        ]
        
        for view_name, view_query in views:
            if self.execute(view_query):
                logger.info(f" Created view 'gold.{view_name}'")
            else:
                logger.error(f" Failed to create view 'gold.{view_name}'")
                return False
        
        return True
    
    def setup_complete(self) -> bool:
        """Print setup completion summary."""
        logger.info("\n" + "=" * 80)
        logger.info(" DATABASE SETUP COMPLETE!")
        logger.info("=" * 80)
        
        logger.info("\n Created Structure:")
        logger.info("  Schemas:")
        logger.info("    • bronze  - Raw ingested data")
        logger.info("    • silver  - Cleaned data")
        logger.info("    • gold    - Analytics and marts")
        logger.info("    • audit   - Tracking and logs")
        
        logger.info("\n  Bronze Tables (Raw Data):")
        logger.info("    • products_raw - Products from APIs")
        logger.info("    • carts_raw    - Shopping carts")
        logger.info("    • orders_raw   - Orders")
        logger.info("    • users_raw    - User data")
        
        logger.info("\n  Silver Tables (Cleaned Data):")
        logger.info("    • products - Deduplicated products")
        logger.info("    • carts    - Cleaned carts")
        logger.info("    • orders   - Clean orders")
        logger.info("    • users    - Clean users")
        
        logger.info("\n  Gold Tables (Analytics):")
        logger.info("    • finance_mart   - Finance KPIs")
        logger.info("    • sales_mart     - Sales KPIs")
        logger.info("    • operations_mart - Operations KPIs")
        
        logger.info("\n  Audit Tables:")
        logger.info("    • ingestion_log - Process tracking")
        
        logger.info("\n  Views:")
        logger.info("    • vw_daily_revenue      - Daily revenue metrics")
        logger.info("    • vw_product_popularity - Product analytics")
        logger.info("    • vw_user_activity      - User engagement")
        
        logger.info("\n" + "=" * 80)
        logger.info(" Ready to start ingestion!")
        logger.info("=" * 80)
        
        return True
    
    def run(self) -> bool:
        """Run complete database setup."""
        logger.info("=" * 80)
        logger.info("  DATABASE SETUP FOR DOE ECOMMERCE PIPELINE")
        logger.info("=" * 80)
        
        if self.dry_run:
            logger.info("\n DRY RUN MODE - No changes will be made\n")
        
        # Connect
        if not self.connect():
            return False
        
        try:
            # Create database
            if not self.create_database():
                return False
            
            # Create schemas
            if not self.create_schemas():
                return False
            
            # Create tables
            if not self.create_bronze_tables():
                return False
            
            if not self.create_silver_tables():
                return False
            
            if not self.create_gold_tables():
                return False
            
            if not self.create_audit_tables():
                return False
            
            # Create views
            if not self.create_views():
                return False
            
            # Setup complete
            self.setup_complete()
            
            return True
        
        finally:
            self.disconnect()
    
    # DDL Queries
    
    @staticmethod
    def _get_products_table_ddl() -> str:
        """Get products raw table DDL."""
        return """
        CREATE TABLE IF NOT EXISTS bronze.products_raw (
            id INTEGER PRIMARY KEY,
            title VARCHAR(500),
            price DECIMAL(15, 2),
            category VARCHAR(100),
            description TEXT,
            image VARCHAR(500),
            rating DECIMAL(3, 2),
            _ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            _source_name VARCHAR(100),
            _endpoint VARCHAR(100),
            _table_name VARCHAR(100)
        );
        CREATE INDEX IF NOT EXISTS idx_products_category ON bronze.products_raw(category);
        COMMENT ON TABLE bronze.products_raw IS 'Raw product data from all sources';
        """
    
    @staticmethod
    def _get_carts_table_ddl() -> str:
        """Get carts raw table DDL."""
        return """
        CREATE TABLE IF NOT EXISTS bronze.carts_raw (
            id INTEGER PRIMARY KEY,
            userId INTEGER,
            total DECIMAL(15, 2),
            discountedTotal DECIMAL(15, 2),
            products TEXT,
            totalProducts INTEGER,
            totalQuantity INTEGER,
            _ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            _source_name VARCHAR(100),
            _endpoint VARCHAR(100),
            _table_name VARCHAR(100)
        );
        CREATE INDEX IF NOT EXISTS idx_carts_user ON bronze.carts_raw(userId);
        COMMENT ON TABLE bronze.carts_raw IS 'Raw cart data from all sources';
        """
    
    @staticmethod
    def _get_orders_table_ddl() -> str:
        """Get orders raw table DDL."""
        return """
        CREATE TABLE IF NOT EXISTS bronze.orders_raw (
            id INTEGER PRIMARY KEY,
            userId INTEGER,
            date DATE,
            _ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            _source_name VARCHAR(100),
            _endpoint VARCHAR(100),
            _table_name VARCHAR(100)
        );
        CREATE INDEX IF NOT EXISTS idx_orders_user ON bronze.orders_raw(userId);
        CREATE INDEX IF NOT EXISTS idx_orders_date ON bronze.orders_raw(date);
        COMMENT ON TABLE bronze.orders_raw IS 'Raw order data from all sources';
        """
    
    @staticmethod
    def _get_users_table_ddl() -> str:
        """Get users raw table DDL."""
        return """
        CREATE TABLE IF NOT EXISTS bronze.users_raw (
            id INTEGER PRIMARY KEY,
            username VARCHAR(100) UNIQUE,
            email VARCHAR(255) UNIQUE,
            gender VARCHAR(20),
            phone VARCHAR(20),
            address TEXT,
            _ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            _source_name VARCHAR(100),
            _endpoint VARCHAR(100),
            _table_name VARCHAR(100)
        );
        CREATE INDEX IF NOT EXISTS idx_users_email ON bronze.users_raw(email);
        CREATE INDEX IF NOT EXISTS idx_users_username ON bronze.users_raw(username);
        COMMENT ON TABLE bronze.users_raw IS 'Raw user data from all sources';
        """
    
    @staticmethod
    def _get_products_silver_ddl() -> str:
        """Get products silver table DDL."""
        return """
        CREATE TABLE IF NOT EXISTS silver.products (
            product_id INTEGER PRIMARY KEY,
            title VARCHAR(500) NOT NULL,
            price DECIMAL(15, 2) NOT NULL,
            category VARCHAR(100),
            is_available BOOLEAN DEFAULT TRUE,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_silver_products_category ON silver.products(category);
        CREATE INDEX IF NOT EXISTS idx_silver_products_price ON silver.products(price);
        COMMENT ON TABLE silver.products IS 'Cleaned and deduplicated products';
        """
    
    @staticmethod
    def _get_carts_silver_ddl() -> str:
        """Get carts silver table DDL."""
        return """
        CREATE TABLE IF NOT EXISTS silver.carts (
            cart_id INTEGER PRIMARY KEY,
            user_id INTEGER NOT NULL,
            total_value DECIMAL(15, 2),
            discount_percentage DECIMAL(5, 2),
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_silver_carts_user ON silver.carts(user_id);
        COMMENT ON TABLE silver.carts IS 'Cleaned and deduplicated carts';
        """
    
    @staticmethod
    def _get_orders_silver_ddl() -> str:
        """Get orders silver table DDL."""
        return """
        CREATE TABLE IF NOT EXISTS silver.orders (
            order_id INTEGER PRIMARY KEY,
            user_id INTEGER NOT NULL,
            total_amount DECIMAL(15, 2) NOT NULL,
            final_amount DECIMAL(15, 2) NOT NULL,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_silver_orders_user ON silver.orders(user_id);
        COMMENT ON TABLE silver.orders IS 'Cleaned and deduplicated orders';
        """
    
    @staticmethod
    def _get_users_silver_ddl() -> str:
        """Get users silver table DDL."""
        return """
        CREATE TABLE IF NOT EXISTS silver.users (
            user_id INTEGER PRIMARY KEY,
            email VARCHAR(255) NOT NULL UNIQUE,
            full_name VARCHAR(255),
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_silver_users_email ON silver.users(email);
        COMMENT ON TABLE silver.users IS 'Cleaned and deduplicated users';
        """


def load_env_file(env_file: str) -> Dict[str, str]:
    """Load environment variables from .env file."""
    env_vars = {}
    try:
        with open(env_file, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    if '=' in line:
                        key, value = line.split('=', 1)
                        env_vars[key.strip()] = value.strip()
        logger.info(f" Loaded environment variables from {env_file}")
        return env_vars
    except FileNotFoundError:
        logger.warning(f" .env file not found: {env_file}")
        return {}


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Setup PostgreSQL database for DoeEcommerce data pipeline'
    )
    parser.add_argument(
        '--host',
        default=os.getenv('DB_HOST', 'localhost'),
        help='PostgreSQL host (default: localhost)'
    )
    parser.add_argument(
        '--port',
        type=int,
        default=int(os.getenv('DB_PORT', 5432)),
        help='PostgreSQL port (default: 5432)'
    )
    parser.add_argument(
        '--user',
        default=os.getenv('DB_USER', 'postgres'),
        help='PostgreSQL user (default: postgres)'
    )
    parser.add_argument(
        '--password',
        default=os.getenv('DB_PASSWORD', 'postgres'),
        help='PostgreSQL password'
    )
    parser.add_argument(
        '--database',
        default=os.getenv('DB_NAME', 'DoeEcommerce'),
        help='Database name (default: DoeEcommerce)'
    )
    parser.add_argument(
        '--env-file',
        type=str,
        help='Load environment variables from .env file'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show SQL without executing'
    )
    
    args = parser.parse_args()
    
    # Load from env file if provided
    if args.env_file:
        env_vars = load_env_file(args.env_file)
        args.host = env_vars.get('DB_HOST', args.host)
        args.port = int(env_vars.get('DB_PORT', args.port))
        args.user = env_vars.get('DB_USER', args.user)
        args.password = env_vars.get('DB_PASSWORD', args.password)
        args.database = env_vars.get('DB_NAME', args.database)
    
    # Setup database
    setup = DatabaseSetup(
        host=args.host,
        port=args.port,
        user=args.user,
        password=args.password,
        database=args.database,
        dry_run=args.dry_run,
    )
    
    success = setup.run()
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()