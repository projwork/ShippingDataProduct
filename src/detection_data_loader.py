#!/usr/bin/env python3
"""
Object Detection Data Loader Module

This module provides functionality to load YOLOv8 object detection results
from JSON files into PostgreSQL database for dbt processing.
"""

import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any

try:
    import pandas as pd
    import psycopg2
    from psycopg2.extras import RealDictCursor, execute_values
    from dotenv import load_dotenv
except ImportError as e:
    print(f"❌ Missing required package: {e}")
    print("💡 Install missing packages with: pip install -r requirements.txt")
    sys.exit(1)

# Load environment variables
load_dotenv()

class DetectionDataLoader:
    """
    PostgreSQL data loader for object detection results.
    
    Features:
    - Load object detection JSON results into PostgreSQL
    - Create raw tables for detection data
    - Data validation and error handling
    - Integration with existing telegram data
    """
    
    def __init__(self):
        """Initialize the detection data loader."""
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
        
        # Connection
        self.connection = None
    
    def _setup_logging(self):
        """Setup logging configuration."""
        log_dir = Path("data/logs")
        log_dir.mkdir(parents=True, exist_ok=True)
        
        log_file = log_dir / f"detection_data_loader_{datetime.now().strftime('%Y%m%d')}.log"
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )
        
        self.logger = logging.getLogger(__name__)
    
    def connect(self) -> bool:
        """
        Connect to PostgreSQL database.
        
        Returns:
            bool: Connection success status
        """
        try:
            self.connection = psycopg2.connect(**self.connection_params)
            self.connection.autocommit = True
            self.logger.info("Successfully connected to PostgreSQL database")
            return True
        except Exception as e:
            self.logger.error(f"Error connecting to PostgreSQL: {e}")
            return False
    
    def disconnect(self):
        """Disconnect from PostgreSQL database."""
        if self.connection:
            self.connection.close()
            self.logger.info("Disconnected from PostgreSQL database")
    
    def create_detection_tables(self) -> bool:
        """
        Create tables for object detection data.
        
        Returns:
            bool: Success status
        """
        # Raw object detections table
        detections_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.raw_schema}.object_detections (
            id SERIAL PRIMARY KEY,
            detection_id VARCHAR(255),
            message_id BIGINT,
            channel VARCHAR(100),
            image_file TEXT,
            image_filename VARCHAR(255),
            class_id INTEGER,
            class_name VARCHAR(100),
            medical_category VARCHAR(100),
            confidence_score FLOAT,
            bounding_box_x1 FLOAT,
            bounding_box_y1 FLOAT,
            bounding_box_x2 FLOAT,
            bounding_box_y2 FLOAT,
            bounding_box_width FLOAT,
            bounding_box_height FLOAT,
            detection_timestamp TIMESTAMP WITH TIME ZONE,
            processing_date DATE,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            UNIQUE(detection_id)
        )
        """
        
        # Detection processing runs table
        processing_runs_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.raw_schema}.detection_processing_runs (
            id SERIAL PRIMARY KEY,
            run_date DATE,
            model_name VARCHAR(100),
            confidence_threshold FLOAT,
            channels_processed TEXT[],
            total_images INTEGER,
            total_detections INTEGER,
            processing_duration_seconds INTEGER,
            metadata JSONB,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        )
        """
        
        try:
            with self.connection.cursor() as cursor:
                # Create tables
                cursor.execute(detections_sql)
                cursor.execute(processing_runs_sql)
                
                # Create indexes for better performance
                indexes = [
                    f"CREATE INDEX IF NOT EXISTS idx_object_detections_message ON {self.raw_schema}.object_detections(message_id, channel)",
                    f"CREATE INDEX IF NOT EXISTS idx_object_detections_class ON {self.raw_schema}.object_detections(class_name)",
                    f"CREATE INDEX IF NOT EXISTS idx_object_detections_medical_category ON {self.raw_schema}.object_detections(medical_category)",
                    f"CREATE INDEX IF NOT EXISTS idx_object_detections_confidence ON {self.raw_schema}.object_detections(confidence_score)",
                    f"CREATE INDEX IF NOT EXISTS idx_object_detections_date ON {self.raw_schema}.object_detections(processing_date)",
                    f"CREATE INDEX IF NOT EXISTS idx_detection_runs_date ON {self.raw_schema}.detection_processing_runs(run_date)",
                ]
                
                for index_sql in indexes:
                    cursor.execute(index_sql)
                
                self.logger.info("Detection tables and indexes created successfully")
            return True
        except Exception as e:
            self.logger.error(f"Error creating detection tables: {e}")
            return False
    
    def load_detection_data(self, data_dir: Path) -> bool:
        """
        Load object detection data from JSON files into PostgreSQL.
        
        Args:
            data_dir: Path to the data directory containing detection results
            
        Returns:
            bool: Success status
        """
        try:
            # Find the detection results directory
            detections_data_dir = data_dir / "object_detections"
            
            if not detections_data_dir.exists():
                self.logger.error(f"Detection data directory not found: {detections_data_dir}")
                return False
            
            # Get the most recent date directory
            date_dirs = [d for d in detections_data_dir.glob("*") if d.is_dir()]
            if not date_dirs:
                self.logger.error("No date directories found in detection data")
                return False
            
            latest_date_dir = max(date_dirs, key=lambda x: x.name)
            self.logger.info(f"Loading detection data from: {latest_date_dir}")
            
            # Load combined detection results
            combined_file = latest_date_dir / "combined_detections.json"
            
            if not combined_file.exists():
                self.logger.error(f"Combined detection file not found: {combined_file}")
                return False
            
            # Load and process data
            with open(combined_file, 'r', encoding='utf-8') as f:
                detection_data = json.load(f)
            
            # Process and load detections
            total_loaded = self._load_detections(detection_data, latest_date_dir.name)
            
            # Load processing run metadata
            self._load_processing_run_metadata(detection_data, latest_date_dir.name)
            
            self.logger.info(f"Successfully loaded {total_loaded} detection records")
            return True
            
        except Exception as e:
            self.logger.error(f"Error loading detection data: {e}")
            return False
    
    def _load_detections(self, detection_data: Dict[str, Any], processing_date: str) -> int:
        """Load detection records into the database."""
        
        all_detections = []
        
        # Extract detections from channel results
        channel_results = detection_data.get('channel_results', {})
        
        for channel, results in channel_results.items():
            if 'detections' in results:
                detections = results['detections']
                
                for detection in detections:
                    # Prepare detection record
                    bounding_box = detection.get('bounding_box', {})
                    
                    detection_record = (
                        detection.get('detection_id'),
                        detection.get('message_id'),
                        detection.get('channel'),
                        detection.get('image_file'),
                        detection.get('image_filename'),
                        detection.get('class_id'),
                        detection.get('class_name'),
                        detection.get('medical_category'),
                        detection.get('confidence_score'),
                        bounding_box.get('x1'),
                        bounding_box.get('y1'),
                        bounding_box.get('x2'),
                        bounding_box.get('y2'),
                        bounding_box.get('width'),
                        bounding_box.get('height'),
                        detection.get('detection_timestamp'),
                        processing_date
                    )
                    
                    all_detections.append(detection_record)
        
        if not all_detections:
            self.logger.warning("No detection records found to load")
            return 0
        
        # Bulk insert with conflict resolution
        insert_sql = f"""
        INSERT INTO {self.raw_schema}.object_detections (
            detection_id, message_id, channel, image_file, image_filename,
            class_id, class_name, medical_category, confidence_score,
            bounding_box_x1, bounding_box_y1, bounding_box_x2, bounding_box_y2,
            bounding_box_width, bounding_box_height, detection_timestamp, processing_date
        ) VALUES %s
        ON CONFLICT (detection_id) DO UPDATE SET
            confidence_score = EXCLUDED.confidence_score,
            detection_timestamp = EXCLUDED.detection_timestamp
        """
        
        try:
            with self.connection.cursor() as cursor:
                execute_values(cursor, insert_sql, all_detections, page_size=1000)
            
            self.logger.info(f"Loaded {len(all_detections)} detection records")
            return len(all_detections)
            
        except Exception as e:
            self.logger.error(f"Error loading detection records: {e}")
            return 0
    
    def _load_processing_run_metadata(self, detection_data: Dict[str, Any], processing_date: str):
        """Load processing run metadata."""
        
        # Extract metadata
        total_channels = detection_data.get('total_channels', 0)
        total_detections = detection_data.get('total_detections', 0)
        processing_timestamp = detection_data.get('processing_timestamp')
        
        # Calculate total images from channel results
        total_images = 0
        channels_processed = []
        
        channel_results = detection_data.get('channel_results', {})
        for channel, results in channel_results.items():
            if 'processed_images' in results:
                total_images += results.get('processed_images', 0)
                channels_processed.append(channel)
        
        metadata_record = (
            processing_date,
            'yolov8n.pt',  # Default model (could be extracted from metadata)
            0.25,  # Default confidence threshold
            channels_processed,
            total_images,
            total_detections,
            0,  # Processing duration (not available from current data)
            json.dumps(detection_data)
        )
        
        insert_sql = f"""
        INSERT INTO {self.raw_schema}.detection_processing_runs (
            run_date, model_name, confidence_threshold, channels_processed,
            total_images, total_detections, processing_duration_seconds, metadata
        ) VALUES %s
        ON CONFLICT DO NOTHING
        """
        
        try:
            with self.connection.cursor() as cursor:
                execute_values(cursor, insert_sql, [metadata_record])
            
            self.logger.info("Loaded processing run metadata")
            
        except Exception as e:
            self.logger.error(f"Error loading processing run metadata: {e}")
    
    def get_detection_summary(self) -> Dict[str, Any]:
        """
        Get summary of loaded detection data.
        
        Returns:
            Dictionary containing detection data summary
        """
        try:
            summary_sql = f"""
            SELECT 
                COUNT(*) as total_detections,
                COUNT(DISTINCT message_id) as unique_messages_with_detections,
                COUNT(DISTINCT channel) as channels_with_detections,
                COUNT(DISTINCT class_name) as unique_object_classes,
                COUNT(DISTINCT medical_category) as unique_medical_categories,
                AVG(confidence_score) as avg_confidence_score,
                MIN(confidence_score) as min_confidence_score,
                MAX(confidence_score) as max_confidence_score,
                MIN(processing_date) as earliest_processing_date,
                MAX(processing_date) as latest_processing_date
            FROM {self.raw_schema}.object_detections
            """
            
            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(summary_sql)
                summary = dict(cursor.fetchone())
            
            # Get top object classes
            top_classes_sql = f"""
            SELECT class_name, COUNT(*) as detection_count
            FROM {self.raw_schema}.object_detections
            GROUP BY class_name
            ORDER BY detection_count DESC
            LIMIT 10
            """
            
            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(top_classes_sql)
                top_classes = [dict(row) for row in cursor.fetchall()]
            
            summary['top_object_classes'] = top_classes
            
            # Get medical category distribution
            medical_categories_sql = f"""
            SELECT medical_category, COUNT(*) as detection_count
            FROM {self.raw_schema}.object_detections
            GROUP BY medical_category
            ORDER BY detection_count DESC
            """
            
            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(medical_categories_sql)
                medical_categories = [dict(row) for row in cursor.fetchall()]
            
            summary['medical_category_distribution'] = medical_categories
            
            return summary
            
        except Exception as e:
            self.logger.error(f"Error getting detection summary: {e}")
            return {}


def main():
    """Main function to load detection data into PostgreSQL."""
    
    print("🔍 Ethiopian Medical Object Detection - Data Loader")
    print("=" * 55)
    
    # Initialize data loader
    try:
        loader = DetectionDataLoader()
        print("✅ Detection data loader initialized")
    except Exception as e:
        print(f"❌ Failed to initialize data loader: {e}")
        return 1
    
    # Connect to database
    if not loader.connect():
        print("❌ Failed to connect to PostgreSQL database")
        return 1
    
    try:
        # Create detection tables
        print("\n🔧 Setting up detection tables...")
        if not loader.create_detection_tables():
            print("❌ Failed to create detection tables")
            return 1
        
        print("✅ Detection tables ready")
        
        # Load detection data
        print("\n📊 Loading object detection data...")
        data_dir = Path("data")
        
        if not loader.load_detection_data(data_dir):
            print("❌ Failed to load detection data")
            return 1
        
        print("✅ Detection data loading completed")
        
        # Get summary
        print("\n📈 Detection Data Summary:")
        summary = loader.get_detection_summary()
        
        if summary:
            print(f"📊 Total Detections: {summary.get('total_detections', 0):,}")
            print(f"📷 Messages with Detections: {summary.get('unique_messages_with_detections', 0):,}")
            print(f"📂 Channels with Detections: {summary.get('channels_with_detections', 0)}")
            print(f"🏷️  Unique Object Classes: {summary.get('unique_object_classes', 0)}")
            print(f"🏥 Medical Categories: {summary.get('unique_medical_categories', 0)}")
            print(f"🎯 Avg Confidence: {summary.get('avg_confidence_score', 0):.3f}")
            
            # Top object classes
            top_classes = summary.get('top_object_classes', [])
            if top_classes:
                print("\n🏆 Top Object Classes:")
                for i, class_info in enumerate(top_classes[:5], 1):
                    print(f"  {i}. {class_info['class_name']}: {class_info['detection_count']:,} detections")
        
        print("\n🎯 Next Steps:")
        print("  1. Run dbt to create staging and mart models")
        print("  2. Create fct_image_detections fact table")
        print("  3. Analyze object detection results in notebook")
        
        return 0
        
    except Exception as e:
        print(f"❌ Error during data loading: {e}")
        return 1
    
    finally:
        loader.disconnect()


if __name__ == "__main__":
    try:
        exit_code = main()
        sys.exit(exit_code)
        
    except KeyboardInterrupt:
        print("\n⚠️  Data loading interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n💥 Unexpected error: {e}")
        sys.exit(1) 