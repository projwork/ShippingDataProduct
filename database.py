"""
Database Configuration and Connection Management

This module handles PostgreSQL database connections for the FastAPI analytical API.
It provides SQLAlchemy engine, session management, and database utilities.
"""

import os
import logging
from contextlib import contextmanager
from typing import Generator
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import QueuePool

# Load environment variables
load_dotenv()

# Database configuration from environment
POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'localhost')
POSTGRES_PORT = int(os.getenv('POSTGRES_PORT', 5432))
POSTGRES_DATABASE = os.getenv('POSTGRES_DATABASE', 'ethiopian_medical_data')
POSTGRES_USERNAME = os.getenv('POSTGRES_USERNAME')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')

# Database URL
DATABASE_URL = f"postgresql://{POSTGRES_USERNAME}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DATABASE}"

# SQLAlchemy setup
engine = create_engine(
    DATABASE_URL,
    poolclass=QueuePool,
    pool_size=10,
    max_overflow=20,
    pool_pre_ping=True,
    echo=False  # Set to True for SQL debugging
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_database_connection():
    """
    Get a database connection for testing purposes.
    
    Returns:
        Database connection or None if failed
    """
    try:
        connection = engine.connect()
        logger.info("✅ Database connection successful")
        return connection
    except Exception as e:
        logger.error(f"❌ Database connection failed: {e}")
        return None


def test_database_connection() -> bool:
    """
    Test the database connection and validate access to key tables.
    
    Returns:
        bool: True if connection successful and tables accessible
    """
    try:
        with engine.connect() as connection:
            # Test basic connection
            result = connection.execute(text("SELECT 1"))
            assert result.fetchone()[0] == 1
            
            # Test access to key dbt tables
            tables_to_check = [
                'marts.fct_messages',
                'marts.fct_image_detections', 
                'marts.dim_channels',
                'marts.dim_dates'
            ]
            
            for table in tables_to_check:
                try:
                    result = connection.execute(text(f"SELECT COUNT(*) FROM {table}"))
                    count = result.fetchone()[0]
                    logger.info(f"✅ Table {table}: {count} records")
                except Exception as e:
                    logger.error(f"❌ Unable to access table {table}: {e}")
                    return False
            
            logger.info("✅ All database tables accessible")
            return True
            
    except Exception as e:
        logger.error(f"❌ Database connection test failed: {e}")
        return False


def get_db() -> Generator[Session, None, None]:
    """
    Dependency to get database session for FastAPI endpoints.
    
    Yields:
        Database session
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@contextmanager
def get_db_session():
    """
    Context manager for database sessions.
    
    Yields:
        Database session
    """
    db = SessionLocal()
    try:
        yield db
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def execute_analytical_query(query: str, params: dict = None) -> list:
    """
    Execute analytical SQL query and return results.
    
    Args:
        query: SQL query string
        params: Query parameters
        
    Returns:
        List of query results
    """
    try:
        with engine.connect() as connection:
            result = connection.execute(text(query), params or {})
            return [dict(row._mapping) for row in result]
    except Exception as e:
        logger.error(f"❌ Query execution failed: {e}")
        logger.error(f"Query: {query}")
        raise


def get_table_info(schema: str = 'marts') -> dict:
    """
    Get information about available tables in the database.
    
    Args:
        schema: Database schema to inspect
        
    Returns:
        Dictionary with table information
    """
    query = """
    SELECT 
        table_name,
        column_name,
        data_type,
        is_nullable
    FROM information_schema.columns 
    WHERE table_schema = :schema
    ORDER BY table_name, ordinal_position
    """
    
    try:
        results = execute_analytical_query(query, {"schema": schema})
        
        tables = {}
        for row in results:
            table_name = row['table_name']
            if table_name not in tables:
                tables[table_name] = []
            tables[table_name].append({
                'column': row['column_name'],
                'type': row['data_type'],
                'nullable': row['is_nullable']
            })
        
        return tables
    except Exception as e:
        logger.error(f"❌ Failed to get table info: {e}")
        return {}


if __name__ == "__main__":
    """Test database connection when run directly."""
    print("🔍 Testing Database Connection...")
    
    if test_database_connection():
        print("✅ Database connection successful!")
        
        # Print table information
        print("\n📊 Available Tables:")
        tables = get_table_info()
        for table_name, columns in tables.items():
            print(f"\n  📋 {table_name}:")
            for col in columns[:5]:  # Show first 5 columns
                print(f"    - {col['column']} ({col['type']})")
            if len(columns) > 5:
                print(f"    ... and {len(columns) - 5} more columns")
    else:
        print("❌ Database connection failed!")
        print("💡 Check your .env file and database configuration") 