"""
CRUD Operations for Ethiopian Medical Data Analytics API

This module provides database operations for analytical queries that power the FastAPI endpoints.
All operations query the dbt-transformed data in the marts schema to answer business questions.
"""

import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any, Tuple
from sqlalchemy.orm import Session
from sqlalchemy import text, func, desc, asc, and_, or_
import time
import re

from database import execute_analytical_query, get_db_session
from models import (
    FctMessages, FctImageDetections, DimChannels, DimDates, 
    FctChannelDailySummary
)
from schemas import (
    ProductMention, ChannelInfo, DailyActivity, MessageMatch,
    ChannelMetrics, TrendDataPoint, QueryMetadata
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class AnalyticalCRUD:
    """Class containing all analytical database operations."""
    
    @staticmethod
    def get_query_metadata(start_time: float, row_count: int) -> QueryMetadata:
        """Generate query execution metadata."""
        execution_time = (time.time() - start_time) * 1000  # Convert to milliseconds
        
        # Determine complexity based on execution time and row count
        if execution_time < 100 and row_count < 1000:
            complexity = "simple"
        elif execution_time < 500 and row_count < 10000:
            complexity = "moderate"
        else:
            complexity = "complex"
            
        return QueryMetadata(
            execution_time_ms=round(execution_time, 2),
            rows_processed=row_count,
            cache_hit=False,  # Could implement caching later
            query_complexity=complexity
        )

    @staticmethod
    def get_top_products(limit: int = 10, days_back: int = 30) -> Tuple[List[ProductMention], QueryMetadata]:
        """
        Get the most frequently mentioned medical products/drugs across all channels.
        
        Args:
            limit: Number of top products to return
            days_back: Number of days to look back for analysis
            
        Returns:
            Tuple of (product mentions list, query metadata)
        """
        start_time = time.time()
        
        # Calculate date threshold
        cutoff_date = datetime.now() - timedelta(days=days_back)
        
        query = """
        WITH product_mentions AS (
            SELECT 
                LOWER(TRIM(unnest(string_to_array(
                    regexp_replace(message_text, '[^a-zA-Z0-9\\s]', ' ', 'g'), 
                    ' '
                )))) as potential_product,
                channel,
                message_date,
                engagement_score as sentiment_score,
                is_pharmacy_content,
                1 as price_mentions
            FROM marts.fct_messages 
            WHERE message_date >= :cutoff_date
                AND message_text IS NOT NULL
                AND length(message_text) > 10
                AND (is_pharmacy_content = true OR is_medical_equipment_content = true OR is_healthcare_content = true)
        ),
        medical_products AS (
            SELECT potential_product
            FROM product_mentions
            WHERE potential_product IN (
                'paracetamol', 'ibuprofen', 'aspirin', 'amoxicillin', 'metformin',
                'insulin', 'morphine', 'codeine', 'tramadol', 'diclofenac',
                'prednisolone', 'dexamethasone', 'omeprazole', 'ranitidine',
                'simvastatin', 'atorvastatin', 'lisinopril', 'amlodipine',
                'metronidazole', 'ciprofloxacin', 'azithromycin', 'doxycycline',
                'hydrochlorothiazide', 'furosemide', 'warfarin', 'heparin',
                'salbutamol', 'prednisolone', 'hydrocortisone', 'betamethasone'
            )
            AND length(potential_product) >= 4
        ),
        product_stats AS (
            SELECT 
                pm.potential_product as product_name,
                COUNT(*) as mention_count,
                array_agg(DISTINCT pm.channel) as channels,
                AVG(pm.sentiment_score) as avg_sentiment,
                SUM(pm.price_mentions) as price_mentions,
                MAX(pm.message_date) as last_mentioned
            FROM product_mentions pm
            INNER JOIN medical_products mp ON pm.potential_product = mp.potential_product
            GROUP BY pm.potential_product
        )
        SELECT 
            product_name,
            mention_count,
            channels,
            COALESCE(avg_sentiment, 0) as avg_sentiment,
            COALESCE(price_mentions, 0) as price_mentions,
            last_mentioned
        FROM product_stats
        WHERE mention_count >= 2  -- Filter out single mentions
        ORDER BY mention_count DESC, last_mentioned DESC
        LIMIT :limit
        """
        
        try:
            results = execute_analytical_query(query, {
                "cutoff_date": cutoff_date,
                "limit": limit
            })
            
            products = []
            for row in results:
                products.append(ProductMention(
                    product_name=row['product_name'].title(),
                    mention_count=row['mention_count'],
                    channels=row['channels'],
                    avg_sentiment=min(max(round(float(row['avg_sentiment']) / 1000, 3), -1), 1) if row['avg_sentiment'] else None,
                    price_mentions=row['price_mentions'],
                    last_mentioned=row['last_mentioned']
                ))
            
            metadata = AnalyticalCRUD.get_query_metadata(start_time, len(results))
            return products, metadata
            
        except Exception as e:
            logger.error(f"Error in get_top_products: {e}")
            raise

    @staticmethod
    def get_channel_activity(channel_name: str, days_back: int = 30) -> Tuple[ChannelInfo, List[DailyActivity], Dict, QueryMetadata]:
        """
        Get posting activity and statistics for a specific channel.
        
        Args:
            channel_name: Name of the channel (with or without @)
            days_back: Number of days to analyze
            
        Returns:
            Tuple of (channel info, daily activity list, summary stats, metadata)
        """
        start_time = time.time()
        
        # Normalize channel name
        if not channel_name.startswith('@'):
            channel_name = f'@{channel_name}'
        
        cutoff_date = datetime.now() - timedelta(days=days_back)
        
        # Get channel information
        channel_info_query = """
        SELECT 
            c.channel_name,
            c.channel_display_name,
            COALESCE(c.channel_category, 'medical') as category,
            c.is_medical_related,
            c.subscriber_count,
            COUNT(m.message_id) as total_messages
        FROM marts.dim_channels c
        LEFT JOIN marts.fct_messages m ON c.channel_key = m.channel_key
        WHERE c.channel_name = :channel_name
        GROUP BY c.channel_key, c.channel_name, c.channel_display_name, 
                 c.channel_category, c.is_medical_related, c.subscriber_count
        """
        
        channel_results = execute_analytical_query(channel_info_query, {"channel_name": channel_name})
        
        if not channel_results:
            raise ValueError(f"Channel {channel_name} not found")
        
        channel_data = channel_results[0]
        channel_info = ChannelInfo(
            channel_name=channel_data['channel_name'],
            display_name=channel_data['channel_display_name'] or channel_data['channel_name'],
            category=channel_data['category'],
            is_medical=channel_data['is_medical_related'] or False,
            subscriber_count=channel_data['subscriber_count'],
            total_messages=channel_data['total_messages']
        )
        
        # Get daily activity
        daily_activity_query = """
        SELECT 
            DATE(message_date) as activity_date,
            COUNT(*) as message_count,
            SUM(CASE WHEN has_media THEN 1 ELSE 0 END) as media_count,
            AVG(engagement_score) as avg_sentiment,
            MODE() WITHIN GROUP (ORDER BY message_hour) as peak_hour
        FROM marts.fct_messages
        WHERE channel = :channel_name
            AND message_date >= :cutoff_date
        GROUP BY DATE(message_date)
        ORDER BY activity_date
        """
        
        daily_results = execute_analytical_query(daily_activity_query, {
            "channel_name": channel_name,
            "cutoff_date": cutoff_date
        })
        
        daily_activities = []
        for row in daily_results:
            daily_activities.append(DailyActivity(
                date=row['activity_date'],
                message_count=row['message_count'],
                media_count=row['media_count'],
                avg_sentiment=round(row['avg_sentiment'], 3) if row['avg_sentiment'] else None,
                peak_hour=row['peak_hour']
            ))
        
        # Get summary statistics
        summary_query = """
        SELECT 
            COUNT(*) as total_messages,
            SUM(CASE WHEN has_media THEN 1 ELSE 0 END) as total_media,
            AVG(text_length) as avg_message_length,
            AVG(engagement_score) as avg_sentiment,
            SUM(CASE WHEN (is_pharmacy_content OR is_medical_equipment_content OR is_healthcare_content) THEN 1 ELSE 0 END) as medical_messages,
            SUM(CASE WHEN is_pharmacy_content THEN 1 ELSE 0 END) as price_messages,
            COUNT(DISTINCT DATE(message_date)) as active_days
        FROM marts.fct_messages
        WHERE channel = :channel_name
            AND message_date >= :cutoff_date
        """
        
        summary_results = execute_analytical_query(summary_query, {
            "channel_name": channel_name,
            "cutoff_date": cutoff_date
        })
        
        summary_stats = {}
        if summary_results:
            row = summary_results[0]
            summary_stats = {
                "total_messages": row['total_messages'],
                "total_media": row['total_media'],
                "media_percentage": round((row['total_media'] / max(row['total_messages'], 1)) * 100, 2),
                "avg_message_length": round(row['avg_message_length'], 2) if row['avg_message_length'] else 0,
                "avg_sentiment": round(row['avg_sentiment'], 3) if row['avg_sentiment'] else 0,
                "medical_messages": row['medical_messages'],
                "price_messages": row['price_messages'],
                "active_days": row['active_days'],
                "avg_daily_posts": round(row['total_messages'] / max(row['active_days'], 1), 2)
            }
        
        # Get top posting hours
        hours_query = """
        SELECT message_hour, COUNT(*) as post_count
        FROM marts.fct_messages
        WHERE channel = :channel_name
            AND message_date >= :cutoff_date
            AND message_hour IS NOT NULL
        GROUP BY message_hour
        ORDER BY post_count DESC
        LIMIT 5
        """
        
        hours_results = execute_analytical_query(hours_query, {
            "channel_name": channel_name,
            "cutoff_date": cutoff_date
        })
        
        top_posting_hours = [row['message_hour'] for row in hours_results]
        
        metadata = AnalyticalCRUD.get_query_metadata(start_time, len(daily_results))
        return channel_info, daily_activities, summary_stats, top_posting_hours, metadata

    @staticmethod
    def search_messages(
        query: str, 
        channels: Optional[List[str]] = None,
        date_from: Optional[datetime] = None,
        date_to: Optional[datetime] = None,
        has_media: Optional[bool] = None,
        sentiment: Optional[str] = None,
        min_relevance: float = 0.0,
        limit: int = 20,
        offset: int = 0
    ) -> Tuple[List[MessageMatch], int, QueryMetadata]:
        """
        Search for messages containing specific keywords with filters.
        
        Args:
            query: Search query string
            channels: List of channels to search in
            date_from: Start date for search
            date_to: End date for search
            has_media: Filter by media presence
            sentiment: Filter by sentiment ('positive', 'negative', 'neutral')
            min_relevance: Minimum relevance score
            limit: Number of results to return
            offset: Number of results to skip
            
        Returns:
            Tuple of (message matches, total count, metadata)
        """
        start_time = time.time()
        
        # Clean and prepare search query
        search_terms = [term.strip().lower() for term in query.split() if len(term.strip()) > 2]
        
        if not search_terms:
            return [], 0, AnalyticalCRUD.get_query_metadata(start_time, 0)
        
        # Build WHERE conditions
        where_conditions = ["message_text IS NOT NULL"]
        params = {"limit": limit, "offset": offset}
        
        # Add search condition
        search_conditions = []
        for i, term in enumerate(search_terms):
            search_conditions.append(f"LOWER(message_text) LIKE :term_{i}")
            params[f"term_{i}"] = f"%{term}%"
        
        where_conditions.append(f"({' OR '.join(search_conditions)})")
        
        # Add optional filters
        if channels:
            channel_conditions = []
            for i, channel in enumerate(channels):
                if not channel.startswith('@'):
                    channel = f'@{channel}'
                channel_conditions.append(f"channel = :channel_{i}")
                params[f"channel_{i}"] = channel
            where_conditions.append(f"({' OR '.join(channel_conditions)})")
        
        if date_from:
            where_conditions.append("message_date >= :date_from")
            params["date_from"] = date_from
            
        if date_to:
            where_conditions.append("message_date <= :date_to")
            params["date_to"] = date_to
            
        if has_media is not None:
            where_conditions.append("has_media = :has_media")
            params["has_media"] = has_media
            
        if sentiment:
            # Map sentiment to engagement categories since we don't have sentiment_label
            if sentiment == "positive":
                where_conditions.append("engagement_category = 'high'")
            elif sentiment == "negative":
                where_conditions.append("engagement_category = 'low'")
            else:
                where_conditions.append("engagement_category = 'medium'")
        
        # Build the main search query
        search_query = f"""
        WITH search_results AS (
            SELECT 
                message_id,
                channel,
                LEFT(message_text, 200) as message_text,
                message_date,
                engagement_category as sentiment_label,
                has_media,
                -- Calculate relevance score based on term matches
                (
                    {' + '.join([f"(CASE WHEN LOWER(message_text) LIKE :term_{i} THEN 1 ELSE 0 END)" for i in range(len(search_terms))])}
                ) * 1.0 / {len(search_terms)} as relevance_score
            FROM marts.fct_messages
            WHERE {' AND '.join(where_conditions)}
        )
        SELECT *
        FROM search_results
        WHERE relevance_score >= :min_relevance
        ORDER BY relevance_score DESC, message_date DESC
        LIMIT :limit OFFSET :offset
        """
        
        params["min_relevance"] = min_relevance
        
        # Get total count for pagination
        count_query = f"""
        WITH search_results AS (
            SELECT 
                message_id,
                (
                    {' + '.join([f"(CASE WHEN LOWER(message_text) LIKE :term_{i} THEN 1 ELSE 0 END)" for i in range(len(search_terms))])}
                ) * 1.0 / {len(search_terms)} as relevance_score
            FROM marts.fct_messages
            WHERE {' AND '.join(where_conditions)}
        )
        SELECT COUNT(*) as total_count
        FROM search_results
        WHERE relevance_score >= :min_relevance
        """
        
        try:
            # Execute both queries
            results = execute_analytical_query(search_query, params)
            count_results = execute_analytical_query(count_query, params)
            
            total_count = count_results[0]['total_count'] if count_results else 0
            
            # Process results
            matches = []
            for row in results:
                # Determine which terms matched
                matched_terms = []
                text_lower = row['message_text'].lower() if row['message_text'] else ""
                for term in search_terms:
                    if term in text_lower:
                        matched_terms.append(term)
                
                matches.append(MessageMatch(
                    message_id=row['message_id'],
                    channel=row['channel'],
                    message_text=row['message_text'] or "",
                    message_date=row['message_date'],
                    sentiment=row['sentiment_label'],
                    has_media=row['has_media'] or False,
                    relevance_score=round(row['relevance_score'], 3),
                    matched_terms=matched_terms
                ))
            
            metadata = AnalyticalCRUD.get_query_metadata(start_time, len(results))
            return matches, total_count, metadata
            
        except Exception as e:
            logger.error(f"Error in search_messages: {e}")
            raise

    @staticmethod
    def get_channel_comparison(days_back: int = 30) -> Tuple[List[ChannelMetrics], QueryMetadata]:
        """
        Compare performance metrics across all channels.
        
        Args:
            days_back: Number of days to analyze
            
        Returns:
            Tuple of (channel metrics list, metadata)
        """
        start_time = time.time()
        
        cutoff_date = datetime.now() - timedelta(days=days_back)
        
        query = """
        WITH channel_stats AS (
            SELECT 
                m.channel,
                COUNT(*) as total_messages,
                COUNT(*) * 1.0 / :days_back as avg_daily_posts,
                SUM(CASE WHEN m.has_media THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as media_percentage,
                AVG(m.engagement_score) as avg_engagement_score,
                SUM(CASE WHEN (m.is_pharmacy_content OR m.is_medical_equipment_content OR m.is_healthcare_content) THEN 1 ELSE 0 END) * 1.0 / COUNT(*) as medical_content_ratio,
                -- Engagement score based on multiple factors
                (
                    AVG(m.engagement_score) * 0.3 +
                    (SUM(CASE WHEN m.has_media THEN 1 ELSE 0 END) * 1.0 / COUNT(*)) * 0.3 +
                    (COUNT(*) / :days_back / 10.0) * 0.2 +  -- Posting frequency
                    (SUM(CASE WHEN (m.is_pharmacy_content OR m.is_medical_equipment_content OR m.is_healthcare_content) THEN 1 ELSE 0 END) * 1.0 / COUNT(*)) * 0.2
                ) as engagement_score
            FROM marts.fct_messages m
            WHERE m.message_date >= :cutoff_date
            GROUP BY m.channel
        )
        SELECT 
            channel as channel_name,
            total_messages,
            ROUND(avg_daily_posts, 2) as avg_daily_posts,
            ROUND(media_percentage, 2) as media_percentage,
            ROUND(COALESCE(avg_engagement_score, 0), 3) as avg_sentiment,
            ROUND(medical_content_ratio, 3) as medical_content_ratio,
            ROUND(engagement_score, 3) as engagement_score
        FROM channel_stats
        ORDER BY engagement_score DESC
        """
        
        try:
            results = execute_analytical_query(query, {
                "cutoff_date": cutoff_date,
                "days_back": days_back
            })
            
            metrics = []
            for row in results:
                metrics.append(ChannelMetrics(
                    channel_name=row['channel_name'],
                    total_messages=row['total_messages'],
                    avg_daily_posts=row['avg_daily_posts'],
                    media_percentage=row['media_percentage'],
                    avg_sentiment=row['avg_sentiment'],
                    medical_content_ratio=row['medical_content_ratio'],
                    engagement_score=row['engagement_score']
                ))
            
            metadata = AnalyticalCRUD.get_query_metadata(start_time, len(results))
            return metrics, metadata
            
        except Exception as e:
            logger.error(f"Error in get_channel_comparison: {e}")
            raise

    @staticmethod
    def get_daily_trends(
        metric: str = "message_count", 
        days_back: int = 30,
        channel: Optional[str] = None
    ) -> Tuple[List[TrendDataPoint], QueryMetadata]:
        """
        Get daily trends for various metrics.
        
        Args:
            metric: Metric to analyze ('message_count', 'sentiment', 'media_ratio', 'medical_content')
            days_back: Number of days to analyze
            channel: Optional channel filter
            
        Returns:
            Tuple of (trend data points, metadata)
        """
        start_time = time.time()
        
        cutoff_date = datetime.now() - timedelta(days=days_back)
        
        # Define metric calculations
        metric_calculations = {
            "message_count": "COUNT(*)",
            "sentiment": "AVG(engagement_score)",
            "media_ratio": "SUM(CASE WHEN has_media THEN 1 ELSE 0 END) * 1.0 / COUNT(*)",
            "medical_content": "SUM(CASE WHEN (is_pharmacy_content OR is_medical_equipment_content OR is_healthcare_content) THEN 1 ELSE 0 END) * 1.0 / COUNT(*)"
        }
        
        if metric not in metric_calculations:
            raise ValueError(f"Unknown metric: {metric}")
        
        where_clause = "message_date >= :cutoff_date"
        params = {"cutoff_date": cutoff_date}
        
        if channel:
            if not channel.startswith('@'):
                channel = f'@{channel}'
            where_clause += " AND channel = :channel"
            params["channel"] = channel
        
        query = f"""
        SELECT 
            DATE(message_date) as trend_date,
            {metric_calculations[metric]} as metric_value
        FROM marts.fct_messages
        WHERE {where_clause}
        GROUP BY DATE(message_date)
        ORDER BY trend_date
        """
        
        try:
            results = execute_analytical_query(query, params)
            
            trends = []
            for row in results:
                trends.append(TrendDataPoint(
                    date=row['trend_date'],
                    value=round(row['metric_value'], 3) if row['metric_value'] else 0,
                    label=f"{metric}_{row['trend_date'].strftime('%Y-%m-%d')}"
                ))
            
            metadata = AnalyticalCRUD.get_query_metadata(start_time, len(results))
            return trends, metadata
            
        except Exception as e:
            logger.error(f"Error in get_daily_trends: {e}")
            raise

    @staticmethod
    def get_object_detection_summary(days_back: int = 30) -> Tuple[Dict[str, Any], QueryMetadata]:
        """
        Get summary of object detection results.
        
        Args:
            days_back: Number of days to analyze
            
        Returns:
            Tuple of (detection summary, metadata)
        """
        start_time = time.time()
        
        cutoff_date = datetime.now() - timedelta(days=days_back)
        
        query = """
        SELECT 
            COUNT(*) as total_detections,
            COUNT(DISTINCT class_name) as unique_objects,
            SUM(CASE WHEN is_medical_relevant THEN 1 ELSE 0 END) as medical_objects,
            AVG(confidence_score) as avg_confidence,
            SUM(CASE WHEN contains_person THEN 1 ELSE 0 END) as person_detections,
            SUM(CASE WHEN contains_medical_equipment THEN 1 ELSE 0 END) as equipment_detections,
            SUM(CASE WHEN contains_hygiene_products THEN 1 ELSE 0 END) as hygiene_detections
        FROM marts.fct_image_detections
        WHERE detection_timestamp >= :cutoff_date
        """
        
        # Get top detected objects
        top_objects_query = """
        SELECT 
            class_name,
            COUNT(*) as detection_count,
            AVG(confidence_score) as avg_confidence
        FROM marts.fct_image_detections
        WHERE detection_timestamp >= :cutoff_date
        GROUP BY class_name
        ORDER BY detection_count DESC
        LIMIT 10
        """
        
        try:
            summary_results = execute_analytical_query(query, {"cutoff_date": cutoff_date})
            objects_results = execute_analytical_query(top_objects_query, {"cutoff_date": cutoff_date})
            
            summary = {}
            if summary_results:
                row = summary_results[0]
                summary = {
                    "total_detections": row['total_detections'],
                    "unique_objects": row['unique_objects'],
                    "medical_objects": row['medical_objects'],
                    "avg_confidence": round(row['avg_confidence'], 3) if row['avg_confidence'] else 0,
                    "person_detections": row['person_detections'],
                    "equipment_detections": row['equipment_detections'],
                    "hygiene_detections": row['hygiene_detections'],
                    "top_objects": [
                        {
                            "class_name": obj['class_name'],
                            "count": obj['detection_count'],
                            "avg_confidence": round(obj['avg_confidence'], 3)
                        }
                        for obj in objects_results
                    ]
                }
            
            metadata = AnalyticalCRUD.get_query_metadata(start_time, len(summary_results) + len(objects_results))
            return summary, metadata
            
        except Exception as e:
            logger.error(f"Error in get_object_detection_summary: {e}")
            raise

    @staticmethod
    def get_health_check() -> Tuple[Dict[str, Any], QueryMetadata]:
        """
        Get API health check information.
        
        Returns:
            Tuple of (health check data, metadata)
        """
        start_time = time.time()
        
        query = """
        SELECT 
            (SELECT COUNT(*) FROM marts.fct_messages) as total_messages,
            (SELECT COUNT(*) FROM marts.fct_image_detections) as total_detections,
            (SELECT MAX(message_date) FROM marts.fct_messages) as last_message_date,
            (SELECT MAX(detection_timestamp) FROM marts.fct_image_detections) as last_detection_date
        """
        
        try:
            results = execute_analytical_query(query, {})
            
            health_data = {}
            if results:
                row = results[0]
                health_data = {
                    "database_status": "healthy",
                    "total_messages": row['total_messages'],
                    "total_detections": row['total_detections'],
                    "last_update": max(
                        row['last_message_date'] or datetime.min,
                        row['last_detection_date'] or datetime.min
                    )
                }
            else:
                health_data = {
                    "database_status": "error",
                    "total_messages": 0,
                    "total_detections": 0,
                    "last_update": datetime.now()
                }
            
            metadata = AnalyticalCRUD.get_query_metadata(start_time, len(results))
            return health_data, metadata
            
        except Exception as e:
            logger.error(f"Error in get_health_check: {e}")
            return {
                "database_status": "error",
                "total_messages": 0,
                "total_detections": 0,
                "last_update": datetime.now()
            }, AnalyticalCRUD.get_query_metadata(start_time, 0)


if __name__ == "__main__":
    """Test CRUD operations when run directly."""
    print("🔍 Testing Analytical CRUD Operations...")
    
    try:
        # Test get_top_products
        print("\n📊 Testing get_top_products...")
        products, metadata = AnalyticalCRUD.get_top_products(limit=5)
        print(f"   Found {len(products)} products")
        print(f"   Query time: {metadata.execution_time_ms}ms")
        
        # Test get_health_check
        print("\n🏥 Testing get_health_check...")
        health, metadata = AnalyticalCRUD.get_health_check()
        print(f"   Database status: {health['database_status']}")
        print(f"   Total messages: {health['total_messages']}")
        print(f"   Query time: {metadata.execution_time_ms}ms")
        
        # Test search_messages
        print("\n🔍 Testing search_messages...")
        matches, total, metadata = AnalyticalCRUD.search_messages("medicine", limit=3)
        print(f"   Found {len(matches)} matches out of {total} total")
        print(f"   Query time: {metadata.execution_time_ms}ms")
        
        print("\n✅ All CRUD operations tested successfully!")
        
    except Exception as e:
        print(f"\n❌ Error testing CRUD operations: {e}")
        raise 