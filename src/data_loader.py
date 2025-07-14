#!/usr/bin/env python3
"""
Data Loader Module for Ethiopian Medical Telegram Data

This module provides functionality to load raw JSON data from the data lake
into a PostgreSQL database for further processing with dbt.
"""

import asyncio
import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor, execute_values
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class PostgreSQLDataLoader:
    """
    PostgreSQL data loader for Ethiopian medical Telegram data.
    
    Features:
    - Bulk data loading from JSON files
    - Schema management and table creation
    - Data validation and error handling
    - Incremental loading capabilities
    """
    
    def __init__(self):
        """Initialize the PostgreSQL data loader."""
        self.connection_params = {
            'host': os.getenv('POSTGRES_HOST', 'localhost'),
            'port': int(os.getenv('POSTGRES_PORT', 5432)),
            'database': os.getenv('POSTGRES_DATABASE', 'ethiopian_medical_data'),
            'user': os.getenv('POSTGRES_USERNAME'),
            'password': os.getenv('POSTGRES_PASSWORD')
        }
        
        # Schema configuration
        self.raw_schema = os.getenv('POSTGRES_RAW_SCHEMA', 'raw')
        self.staging_schema = os.getenv('POSTGRES_STAGING_SCHEMA', 'staging')
        self.marts_schema = os.getenv('POSTGRES_MARTS_SCHEMA', 'marts')
        
        # Validate required environment variables
        if not all([self.connection_params['user'], self.connection_params['password']]):
            raise ValueError("PostgreSQL credentials not found in environment variables")
        
        # Setup logging
        self._setup_logging()
        
        # Connection pool
        self.connection = None
    
    def _setup_logging(self):
        """Setup logging for the data loader."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
    
    def connect(self):
        """Establish connection to PostgreSQL database."""
        try:
            self.connection = psycopg2.connect(**self.connection_params)
            self.connection.autocommit = True
            self.logger.info("Successfully connected to PostgreSQL database")
            return True
        except Exception as e:
            self.logger.error(f"Failed to connect to PostgreSQL: {e}")
            return False
    
    def disconnect(self):
        """Close PostgreSQL connection."""
        if self.connection:
            self.connection.close()
            self.logger.info("Disconnected from PostgreSQL database")
    
    def create_schemas(self):
        """Create necessary schemas if they don't exist."""
        schemas = [self.raw_schema, self.staging_schema, self.marts_schema]
        
        try:
            with self.connection.cursor() as cursor:
                for schema in schemas:
                    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
                    self.logger.info(f"Schema '{schema}' created or already exists")
            return True
        except Exception as e:
            self.logger.error(f"Error creating schemas: {e}")
            return False
    
    def create_raw_tables(self):
        """Create raw tables to store telegram data."""
        
        # Raw telegram messages table
        telegram_messages_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.raw_schema}.telegram_messages (
            message_id BIGINT,
            channel VARCHAR(100),
            message_date TIMESTAMP WITH TIME ZONE,
            text TEXT,
            views INTEGER DEFAULT 0,
            forwards INTEGER DEFAULT 0,
            replies INTEGER DEFAULT 0,
            media_type VARCHAR(50),
            media_path TEXT,
            media_size BIGINT,
            text_length INTEGER,
            word_count INTEGER,
            has_media BOOLEAN DEFAULT FALSE,
            raw_data JSONB,
            extracted_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            UNIQUE(message_id, channel)
        )
        """
        
        # Raw media files table
        media_files_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.raw_schema}.media_files (
            id SERIAL PRIMARY KEY,
            message_id BIGINT,
            channel VARCHAR(100),
            file_path TEXT,
            file_name VARCHAR(255),
            file_type VARCHAR(50),
            file_size BIGINT,
            extracted_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            FOREIGN KEY (message_id, channel) REFERENCES {self.raw_schema}.telegram_messages(message_id, channel)
        )
        """
        
        # Channel metadata table
        channels_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.raw_schema}.channels (
            channel_name VARCHAR(100) PRIMARY KEY,
            channel_display_name VARCHAR(200),
            channel_type VARCHAR(50) DEFAULT 'medical',
            description TEXT,
            first_scraped_at TIMESTAMP WITH TIME ZONE,
            last_scraped_at TIMESTAMP WITH TIME ZONE,
            total_messages INTEGER DEFAULT 0,
            is_active BOOLEAN DEFAULT TRUE
        )
        """
        
        # Scraping metadata table
        scraping_metadata_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.raw_schema}.scraping_runs (
            id SERIAL PRIMARY KEY,
            run_date DATE,
            channels_scraped TEXT[],
            total_messages INTEGER,
            successful_channels INTEGER,
            failed_channels INTEGER,
            run_duration_seconds INTEGER,
            metadata JSONB,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        )
        """
        
        try:
            with self.connection.cursor() as cursor:
                # Create tables
                cursor.execute(telegram_messages_sql)
                cursor.execute(media_files_sql)
                cursor.execute(channels_sql)
                cursor.execute(scraping_metadata_sql)
                
                # Create indexes for better performance
                indexes = [
                    f"CREATE INDEX IF NOT EXISTS idx_telegram_messages_channel ON {self.raw_schema}.telegram_messages(channel)",
                    f"CREATE INDEX IF NOT EXISTS idx_telegram_messages_date ON {self.raw_schema}.telegram_messages(message_date)",
                    f"CREATE INDEX IF NOT EXISTS idx_telegram_messages_media ON {self.raw_schema}.telegram_messages(has_media)",
                    f"CREATE INDEX IF NOT EXISTS idx_media_files_message ON {self.raw_schema}.media_files(message_id, channel)",
                ]
                
                for index_sql in indexes:
                    cursor.execute(index_sql)
                
                self.logger.info("Raw tables and indexes created successfully")
            return True
        except Exception as e:
            self.logger.error(f"Error creating raw tables: {e}")
            return False
    
    def load_telegram_data(self, data_dir: Path) -> bool:
        """
        Load telegram data from JSON files into PostgreSQL.
        
        Args:
            data_dir: Path to the data directory containing JSON files
            
        Returns:
            bool: Success status
        """
        try:
            # Find the most recent data directory
            telegram_data_dir = data_dir / "raw" / "telegram_messages"
            
            if not telegram_data_dir.exists():
                self.logger.error(f"Telegram data directory not found: {telegram_data_dir}")
                return False
            
            # Get the most recent date directory
            date_dirs = [d for d in telegram_data_dir.glob("*") if d.is_dir()]
            if not date_dirs:
                self.logger.error("No date directories found in telegram data")
                return False
            
            latest_date_dir = max(date_dirs, key=lambda x: x.name)
            self.logger.info(f"Loading data from: {latest_date_dir}")
            
            # Load combined data file
            combined_file = latest_date_dir / "combined_medical_channels.json"
            metadata_file = latest_date_dir / "scrape_metadata.json"
            
            if not combined_file.exists():
                self.logger.error(f"Combined data file not found: {combined_file}")
                return False
            
            # Load and process data
            with open(combined_file, 'r', encoding='utf-8') as f:
                messages_data = json.load(f)
            
            if metadata_file.exists():
                with open(metadata_file, 'r', encoding='utf-8') as f:
                    metadata = json.load(f)
            else:
                metadata = {}
            
            # Process and load messages
            self._load_messages(messages_data)
            
            # Load metadata
            self._load_scraping_metadata(metadata, latest_date_dir.name)
            
            # Update channel information
            self._update_channels(messages_data)
            
            self.logger.info(f"Successfully loaded {len(messages_data)} messages")
            return True
            
        except Exception as e:
            self.logger.error(f"Error loading telegram data: {e}")
            return False
    
    def _load_messages(self, messages_data: List[Dict[str, Any]]):
        """Load messages data into the database."""
        
        # Prepare data for bulk insert
        processed_messages = []
        
        for msg in messages_data:
            # Clean and validate data
            processed_msg = (
                msg.get('id'),
                msg.get('channel', '').strip(),
                msg.get('date'),
                msg.get('text', ''),
                msg.get('views', 0),
                msg.get('forwards', 0),
                msg.get('replies', 0),
                msg.get('media_type'),
                msg.get('media_path'),
                msg.get('media_size', 0),
                len(msg.get('text', '')),
                len(msg.get('text', '').split()) if msg.get('text') else 0,
                msg.get('media_type') is not None,
                json.dumps(msg.get('raw_data', {}))
            )
            processed_messages.append(processed_msg)
        
        # Bulk insert with conflict resolution
        insert_sql = f"""
        INSERT INTO {self.raw_schema}.telegram_messages (
            message_id, channel, message_date, text, views, forwards, replies,
            media_type, media_path, media_size, text_length, word_count, has_media, raw_data
        ) VALUES %s
        ON CONFLICT (message_id, channel) DO UPDATE SET
            views = EXCLUDED.views,
            forwards = EXCLUDED.forwards,
            replies = EXCLUDED.replies,
            extracted_at = NOW()
        """
        
        try:
            with self.connection.cursor() as cursor:
                execute_values(cursor, insert_sql, processed_messages, page_size=1000)
                self.logger.info(f"Loaded {len(processed_messages)} messages into database")
        except Exception as e:
            self.logger.error(f"Error loading messages: {e}")
            raise
    
    def _load_scraping_metadata(self, metadata: Dict[str, Any], run_date: str):
        """Load scraping run metadata."""
        
        insert_sql = f"""
        INSERT INTO {self.raw_schema}.scraping_runs (
            run_date, channels_scraped, total_messages, successful_channels,
            failed_channels, metadata
        ) VALUES (%s, %s, %s, %s, %s, %s)
        """
        
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(insert_sql, (
                    run_date,
                    metadata.get('channels_scraped', []),
                    metadata.get('total_messages', 0),
                    len(metadata.get('channels_scraped', [])),
                    0,  # Calculate failed channels if needed
                    json.dumps(metadata)
                ))
                self.logger.info("Loaded scraping metadata")
        except Exception as e:
            self.logger.error(f"Error loading scraping metadata: {e}")
    
    def _update_channels(self, messages_data: List[Dict[str, Any]]):
        """Update channel information based on messages."""
        
        # Extract unique channels and their info
        channels_info = {}
        for msg in messages_data:
            channel = msg.get('channel', '').strip()
            if channel and channel not in channels_info:
                channels_info[channel] = {
                    'display_name': channel.replace('@', '').title(),
                    'first_message_date': msg.get('date'),
                    'message_count': 0
                }
            if channel in channels_info:
                channels_info[channel]['message_count'] += 1
        
        # Insert or update channel information
        upsert_sql = f"""
        INSERT INTO {self.raw_schema}.channels (
            channel_name, channel_display_name, first_scraped_at, 
            last_scraped_at, total_messages
        ) VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (channel_name) DO UPDATE SET
            last_scraped_at = NOW(),
            total_messages = EXCLUDED.total_messages
        """
        
        try:
            with self.connection.cursor() as cursor:
                for channel, info in channels_info.items():
                    cursor.execute(upsert_sql, (
                        channel,
                        info['display_name'],
                        info['first_message_date'],
                        datetime.now(),
                        info['message_count']
                    ))
                self.logger.info(f"Updated information for {len(channels_info)} channels")
        except Exception as e:
            self.logger.error(f"Error updating channels: {e}")
    
    def get_data_summary(self) -> Dict[str, Any]:
        """Get a summary of loaded data."""
        try:
            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                # Messages summary
                cursor.execute(f"""
                    SELECT 
                        COUNT(*) as total_messages,
                        COUNT(DISTINCT channel) as unique_channels,
                        MIN(message_date) as earliest_message,
                        MAX(message_date) as latest_message,
                        SUM(CASE WHEN has_media THEN 1 ELSE 0 END) as messages_with_media
                    FROM {self.raw_schema}.telegram_messages
                """)
                messages_summary = cursor.fetchone()
                
                # Channel summary
                cursor.execute(f"""
                    SELECT channel, COUNT(*) as message_count
                    FROM {self.raw_schema}.telegram_messages
                    GROUP BY channel
                    ORDER BY message_count DESC
                """)
                channel_summary = cursor.fetchall()
                
                return {
                    'messages_summary': dict(messages_summary),
                    'channel_summary': [dict(row) for row in channel_summary]
                }
        except Exception as e:
            self.logger.error(f"Error getting data summary: {e}")
            return {}


def main():
    """Main function to load data into PostgreSQL."""
    
    print("🗄️ Ethiopian Medical Data - PostgreSQL Loader")
    print("=" * 50)
    
    # Initialize data loader
    try:
        loader = PostgreSQLDataLoader()
        print("✅ Data loader initialized")
    except Exception as e:
        print(f"❌ Failed to initialize data loader: {e}")
        return 1
    
    # Connect to database
    if not loader.connect():
        print("❌ Failed to connect to PostgreSQL database")
        return 1
    
    try:
        # Create schemas and tables
        print("\n🔧 Setting up database structure...")
        if not loader.create_schemas():
            print("❌ Failed to create schemas")
            return 1
        
        if not loader.create_raw_tables():
            print("❌ Failed to create raw tables")
            return 1
        
        print("✅ Database structure ready")
        
        # Load data
        print("\n📊 Loading data from JSON files...")
        data_dir = Path("data")
        
        if not loader.load_telegram_data(data_dir):
            print("❌ Failed to load telegram data")
            return 1
        
        print("✅ Data loading completed")
        
        # Get summary
        print("\n📈 Data Summary:")
        summary = loader.get_data_summary()
        
        if summary:
            msg_summary = summary['messages_summary']
            print(f"  Total Messages: {msg_summary.get('total_messages', 0):,}")
            print(f"  Unique Channels: {msg_summary.get('unique_channels', 0)}")
            print(f"  Date Range: {msg_summary.get('earliest_message')} to {msg_summary.get('latest_message')}")
            print(f"  Messages with Media: {msg_summary.get('messages_with_media', 0):,}")
            
            print(f"\n📋 Channel Breakdown:")
            for channel_info in summary['channel_summary']:
                print(f"  {channel_info['channel']}: {channel_info['message_count']:,} messages")
        
        print(f"\n🎯 Next Steps:")
        print("1. Install dbt: pip install dbt-postgres")
        print("2. Initialize dbt project: dbt init")
        print("3. Run dbt models: dbt run")
        print("4. Generate documentation: dbt docs generate")
        
        return 0
        
    finally:
        loader.disconnect()


if __name__ == "__main__":
    sys.exit(main()) 