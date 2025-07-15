"""
Pydantic Schemas for Ethiopian Medical Data Analytics API

This module defines Pydantic models for request/response validation and serialization
for the FastAPI analytical endpoints. These schemas ensure data integrity and provide
automatic API documentation.
"""

from datetime import datetime
from typing import List, Optional, Union, Dict, Any
from pydantic import BaseModel, Field, field_validator
from enum import Enum


class ConfidenceLevel(str, Enum):
    """Enumeration for detection confidence levels."""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"


class SentimentLabel(str, Enum):
    """Enumeration for sentiment analysis labels."""
    POSITIVE = "positive"
    NEGATIVE = "negative"
    NEUTRAL = "neutral"


class MedicalCategory(str, Enum):
    """Enumeration for medical object categories."""
    PERSONNEL = "medical_staff"
    CONTAINERS = "medical_bottle"
    INSTRUMENTS = "medical_instruments"
    EQUIPMENT = "medical_equipment"
    HYGIENE = "hygiene_products"
    DOCUMENTATION = "documentation"
    TECHNOLOGY = "technology"
    OTHER = "other"


# Base response schemas
class BaseResponse(BaseModel):
    """Base response schema with common metadata."""
    success: bool = True
    message: str = "Request completed successfully"
    timestamp: datetime = Field(default_factory=datetime.now)
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class PaginationInfo(BaseModel):
    """Pagination information for list responses."""
    page: int = Field(ge=1, description="Current page number")
    page_size: int = Field(ge=1, le=100, description="Number of items per page")
    total_items: int = Field(ge=0, description="Total number of items")
    total_pages: int = Field(ge=0, description="Total number of pages")
    has_next: bool = Field(description="Whether there are more pages")
    has_previous: bool = Field(description="Whether there are previous pages")


# Product/Item related schemas
class ProductMention(BaseModel):
    """Schema for product mentions in messages."""
    product_name: str = Field(description="Name of the mentioned product")
    mention_count: int = Field(ge=0, description="Number of times mentioned")
    channels: List[str] = Field(description="Channels where product is mentioned")
    avg_sentiment: Optional[float] = Field(ge=-1, le=1, description="Average sentiment score")
    price_mentions: int = Field(ge=0, description="Number of times price is mentioned")
    last_mentioned: datetime = Field(description="Last time product was mentioned")


class TopProductsResponse(BaseResponse):
    """Response schema for top products endpoint."""
    products: List[ProductMention] = Field(description="List of top mentioned products")
    total_products: int = Field(ge=0, description="Total number of unique products")
    analysis_period: str = Field(description="Time period analyzed")
    
    
# Channel activity schemas
class DailyActivity(BaseModel):
    """Schema for daily activity data."""
    date: datetime = Field(description="Date of activity")
    message_count: int = Field(ge=0, description="Number of messages posted")
    media_count: int = Field(ge=0, description="Number of media messages")
    avg_sentiment: Optional[float] = Field(description="Average sentiment score")
    peak_hour: Optional[int] = Field(ge=0, le=23, description="Peak posting hour")


class ChannelInfo(BaseModel):
    """Schema for channel information."""
    channel_name: str = Field(description="Channel username")
    display_name: str = Field(description="Channel display name")
    category: str = Field(description="Channel category")
    is_medical: bool = Field(description="Whether channel is medical-related")
    subscriber_count: Optional[int] = Field(description="Number of subscribers")
    total_messages: int = Field(ge=0, description="Total messages analyzed")


class ChannelActivityResponse(BaseResponse):
    """Response schema for channel activity endpoint."""
    channel_info: ChannelInfo = Field(description="Channel information")
    daily_activity: List[DailyActivity] = Field(description="Daily activity data")
    summary_stats: Dict[str, Union[int, float]] = Field(description="Summary statistics")
    top_posting_hours: List[int] = Field(description="Most active posting hours")


# Message search schemas
class MessageMatch(BaseModel):
    """Schema for message search results."""
    message_id: int = Field(description="Unique message ID")
    channel: str = Field(description="Channel name")
    message_text: str = Field(description="Message content (truncated)")
    message_date: datetime = Field(description="When message was posted")
    sentiment: Optional[str] = Field(description="Message sentiment")
    has_media: bool = Field(description="Whether message contains media")
    relevance_score: float = Field(ge=0, le=1, description="Search relevance score")
    matched_terms: List[str] = Field(description="Search terms that matched")


class SearchFilters(BaseModel):
    """Schema for search filters."""
    channels: Optional[List[str]] = Field(default=None, description="Filter by specific channels")
    date_from: Optional[datetime] = Field(default=None, description="Search from this date")
    date_to: Optional[datetime] = Field(default=None, description="Search until this date")
    has_media: Optional[bool] = Field(default=None, description="Filter by media presence")
    sentiment: Optional[SentimentLabel] = Field(default=None, description="Filter by sentiment")
    min_relevance: float = Field(default=0.0, ge=0, le=1, description="Minimum relevance score")


class MessageSearchResponse(BaseResponse):
    """Response schema for message search endpoint."""
    query: str = Field(description="Search query used")
    matches: List[MessageMatch] = Field(description="Search results")
    total_matches: int = Field(ge=0, description="Total number of matches found")
    search_filters: Optional[SearchFilters] = Field(description="Applied filters")
    pagination: Optional[PaginationInfo] = Field(description="Pagination information")


# Object detection related schemas
class DetectedObject(BaseModel):
    """Schema for detected objects in images."""
    class_name: str = Field(description="Object class name")
    medical_category: MedicalCategory = Field(description="Medical category classification")
    confidence: float = Field(ge=0, le=1, description="Detection confidence score")
    confidence_level: ConfidenceLevel = Field(description="Confidence level category")
    position: Dict[str, float] = Field(description="Bounding box coordinates")
    medical_relevance: int = Field(ge=1, le=5, description="Medical relevance score")


class ImageDetectionSummary(BaseModel):
    """Schema for image detection summaries."""
    total_detections: int = Field(ge=0, description="Total number of detections")
    unique_objects: int = Field(ge=0, description="Number of unique object types")
    medical_objects: int = Field(ge=0, description="Number of medical-relevant objects")
    avg_confidence: float = Field(ge=0, le=1, description="Average confidence score")
    top_objects: List[Dict[str, Union[str, int]]] = Field(description="Most detected objects")


# Analytics and trends schemas
class TrendDataPoint(BaseModel):
    """Schema for trend data points."""
    date: datetime = Field(description="Date of the data point")
    value: Union[int, float] = Field(description="Trend value")
    label: Optional[str] = Field(description="Data point label")


class TrendAnalysis(BaseModel):
    """Schema for trend analysis results."""
    trend_type: str = Field(description="Type of trend analysis")
    period: str = Field(description="Analysis period")
    data_points: List[TrendDataPoint] = Field(description="Trend data")
    trend_direction: str = Field(description="Overall trend direction")
    growth_rate: Optional[float] = Field(description="Growth rate percentage")


# Channel comparison schemas
class ChannelMetrics(BaseModel):
    """Schema for channel comparison metrics."""
    channel_name: str = Field(description="Channel name")
    total_messages: int = Field(ge=0, description="Total messages")
    avg_daily_posts: float = Field(ge=0, description="Average posts per day")
    media_percentage: float = Field(ge=0, le=100, description="Percentage of media messages")
    avg_sentiment: float = Field(ge=-1, le=1, description="Average sentiment score")
    medical_content_ratio: float = Field(ge=0, le=1, description="Ratio of medical content")
    engagement_score: float = Field(ge=0, description="Calculated engagement score")


class ChannelComparisonResponse(BaseResponse):
    """Response schema for channel comparison."""
    channels: List[ChannelMetrics] = Field(description="Channel metrics comparison")
    comparison_period: str = Field(description="Period used for comparison")
    top_performer: str = Field(description="Best performing channel")
    metrics_compared: List[str] = Field(description="Metrics included in comparison")


# Health check and API info schemas
class APIInfo(BaseModel):
    """Schema for API information."""
    name: str = Field(default="Ethiopian Medical Data Analytics API")
    version: str = Field(default="1.0.0")
    description: str = Field(default="Analytical API for Ethiopian medical Telegram data")
    endpoints: List[str] = Field(description="Available API endpoints")


class HealthCheck(BaseResponse):
    """Schema for health check endpoint."""
    database_status: str = Field(description="Database connection status")
    total_messages: int = Field(description="Total messages in database")
    total_detections: int = Field(description="Total object detections")
    last_update: datetime = Field(description="Last data update timestamp")


# Error response schemas
class ErrorDetail(BaseModel):
    """Schema for error details."""
    code: str = Field(description="Error code")
    message: str = Field(description="Error message")
    field: Optional[str] = Field(default=None, description="Field that caused the error")


class ErrorResponse(BaseModel):
    """Schema for error responses."""
    success: bool = False
    message: str = Field(description="Error message")
    timestamp: datetime = Field(default_factory=datetime.now)
    errors: Optional[List[ErrorDetail]] = Field(description="Detailed error information")
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


# Request schemas for POST endpoints
class SearchRequest(BaseModel):
    """Schema for search requests."""
    query: str = Field(min_length=1, max_length=200, description="Search query")
    filters: Optional[SearchFilters] = Field(description="Search filters")
    page: int = Field(default=1, ge=1, description="Page number")
    page_size: int = Field(default=20, ge=1, le=100, description="Results per page")
    
    @field_validator('query')
    @classmethod
    def validate_query(cls, v):
        """Validate search query."""
        if not v.strip():
            raise ValueError('Query cannot be empty')
        return v.strip()


class DateRangeRequest(BaseModel):
    """Schema for date range requests."""
    start_date: datetime = Field(description="Start date for analysis")
    end_date: datetime = Field(description="End date for analysis")
    
    @field_validator('end_date')
    @classmethod
    def validate_date_range(cls, v, info):
        """Validate that end_date is after start_date."""
        if hasattr(info, 'data') and 'start_date' in info.data and v < info.data['start_date']:
            raise ValueError('End date must be after start date')
        return v


# Configuration and metadata schemas
class QueryMetadata(BaseModel):
    """Schema for query execution metadata."""
    execution_time_ms: float = Field(description="Query execution time in milliseconds")
    rows_processed: int = Field(description="Number of rows processed")
    cache_hit: bool = Field(default=False, description="Whether result was cached")
    query_complexity: str = Field(description="Query complexity level")


class AnalyticalResponse(BaseResponse):
    """Enhanced base response for analytical endpoints."""
    data: Any = Field(description="Response data")
    metadata: Optional[QueryMetadata] = Field(description="Query execution metadata")
    suggestions: Optional[List[str]] = Field(description="Related query suggestions")


if __name__ == "__main__":
    """Test schema validation and serialization."""
    print("🔍 Testing Pydantic Schemas...")
    
    # Test basic response schema
    base_response = BaseResponse()
    print(f"✅ BaseResponse: {base_response.model_dump_json()}")
    
    # Test product mention schema
    product = ProductMention(
        product_name="Paracetamol",
        mention_count=15,
        channels=["@pharmacy1", "@medical2"],
        avg_sentiment=0.7,
        price_mentions=5,
        last_mentioned=datetime.now()
    )
    print(f"✅ ProductMention: {product.model_dump()}")
    
    # Test error response
    error = ErrorResponse(
        message="Test error",
        errors=[ErrorDetail(code="TEST_001", message="Test error detail")]
    )
    print(f"✅ ErrorResponse: {error.model_dump_json()}")
    
    print("✅ All schemas validated successfully!") 