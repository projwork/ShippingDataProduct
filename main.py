"""
Ethiopian Medical Data Analytics API

This FastAPI application provides analytical endpoints for exploring Ethiopian medical 
business data scraped from Telegram channels. It serves insights about medical products,
channel activity, message search, and object detection results.

Main endpoints:
- GET /api/reports/top-products: Most frequently mentioned medical products
- GET /api/channels/{channel_name}/activity: Channel posting activity analysis  
- GET /api/search/messages: Search messages with various filters
- Additional analytics endpoints for trends, comparisons, and health checks

Author: Kara Solutions Data Engineering Team
Version: 1.0.0
"""

from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any
import logging
import time

from fastapi import FastAPI, HTTPException, Depends, Query, Path
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session

from database import get_db, test_database_connection
from crud import AnalyticalCRUD
from schemas import (
    # Response schemas
    TopProductsResponse, ChannelActivityResponse, MessageSearchResponse,
    ChannelComparisonResponse, HealthCheck, APIInfo, ErrorResponse,
    AnalyticalResponse, TrendAnalysis,
    
    # Request schemas  
    SearchRequest, DateRangeRequest,
    
    # Data schemas
    ProductMention, ChannelInfo, MessageMatch, ChannelMetrics,
    TrendDataPoint, QueryMetadata,
    
    # Enums
    SentimentLabel
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# FastAPI app initialization
app = FastAPI(
    title="Ethiopian Medical Data Analytics API",
    description="""
    🏥 **Ethiopian Medical Data Analytics API** 🏥
    
    This API provides comprehensive analytics for Ethiopian medical business data 
    scraped from Telegram channels. Explore medical product mentions, channel activity, 
    content search, and visual object detection insights.
    
    ## Features
    - **Product Analytics**: Top mentioned medical products and drugs
    - **Channel Activity**: Posting patterns and engagement metrics
    - **Content Search**: Full-text search with advanced filters
    - **Visual Analytics**: Object detection results from medical images
    - **Trend Analysis**: Daily, weekly, and monthly trends
    - **Channel Comparison**: Performance metrics across channels
    
    ## Data Sources
    - Telegram medical channels (@lobelia4cosmetics, @tikvahpharma)
    - YOLOv8 object detection on medical images
    - dbt-transformed data warehouse with star schema
    
    Built with ❤️ by Kara Solutions Data Engineering Team
    """,
    version="1.0.0",
    contact={
        "name": "Kara Solutions",
        "email": "data@karasolutions.et",
    },
    license_info={
        "name": "MIT License",
        "url": "https://opensource.org/licenses/MIT",
    },
)

# CORS middleware for frontend integration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure properly for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Exception handlers
@app.exception_handler(HTTPException)
async def http_exception_handler(request, exc):
    """Handle HTTP exceptions with consistent error format."""
    return JSONResponse(
        status_code=exc.status_code,
        content=ErrorResponse(
            message=exc.detail,
            timestamp=datetime.now()
        ).model_dump()
    )

@app.exception_handler(Exception)
async def general_exception_handler(request, exc):
    """Handle general exceptions."""
    logger.error(f"Unhandled exception: {exc}")
    return JSONResponse(
        status_code=500,
        content=ErrorResponse(
            message="Internal server error occurred",
            timestamp=datetime.now()
        ).model_dump()
    )

# Startup event
@app.on_event("startup")
async def startup_event():
    """Initialize the application and test database connection."""
    logger.info("🚀 Starting Ethiopian Medical Data Analytics API...")
    
    if test_database_connection():
        logger.info("✅ Database connection successful")
    else:
        logger.error("❌ Database connection failed")
        raise Exception("Could not connect to database")

# Root endpoint
@app.get("/", response_model=APIInfo)
async def root():
    """API information and available endpoints."""
    return APIInfo(
        endpoints=[
            "/api/reports/top-products",
            "/api/channels/{channel_name}/activity",
            "/api/search/messages",
            "/api/analytics/trends",
            "/api/analytics/channel-comparison",
            "/api/analytics/object-detection-summary",
            "/health"
        ]
    )

# Health check endpoint
@app.get("/health", response_model=HealthCheck)
async def health_check():
    """API health check with database status and data counts."""
    try:
        health_data, metadata = AnalyticalCRUD.get_health_check()
        
        return HealthCheck(
            database_status=health_data["database_status"],
            total_messages=health_data["total_messages"],
            total_detections=health_data["total_detections"],
            last_update=health_data["last_update"]
        )
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        raise HTTPException(status_code=503, detail="Service unavailable")

# ==========================================
# MAIN ANALYTICAL ENDPOINTS
# ==========================================

@app.get("/api/reports/top-products", response_model=TopProductsResponse)
async def get_top_products(
    limit: int = Query(default=10, ge=1, le=50, description="Number of top products to return"),
    days_back: int = Query(default=30, ge=1, le=365, description="Number of days to analyze")
):
    """
    📊 **Get Top Medical Products**
    
    Returns the most frequently mentioned medical products and drugs across all channels.
    Includes mention count, channels, sentiment analysis, and pricing information.
    
    **Business Question**: What are the top 10 most frequently mentioned medical products or drugs?
    """
    try:
        products, metadata = AnalyticalCRUD.get_top_products(limit=limit, days_back=days_back)
        
        return TopProductsResponse(
            products=products,
            total_products=len(products),
            analysis_period=f"Last {days_back} days"
        )
    except Exception as e:
        logger.error(f"Error in get_top_products: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/channels/{channel_name}/activity", response_model=ChannelActivityResponse)
async def get_channel_activity(
    channel_name: str = Path(..., description="Channel name (with or without @)"),
    days_back: int = Query(default=30, ge=1, le=365, description="Number of days to analyze")
):
    """
    📈 **Get Channel Activity Analysis**
    
    Returns detailed posting activity and engagement metrics for a specific channel.
    Includes daily activity, peak posting hours, and comprehensive statistics.
    
    **Business Question**: What are the daily and weekly trends in posting volume for health-related topics?
    """
    try:
        channel_info, daily_activity, summary_stats, top_hours, metadata = AnalyticalCRUD.get_channel_activity(
            channel_name=channel_name, 
            days_back=days_back
        )
        
        return ChannelActivityResponse(
            channel_info=channel_info,
            daily_activity=daily_activity,
            summary_stats=summary_stats,
            top_posting_hours=top_hours
        )
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Error in get_channel_activity: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/search/messages", response_model=MessageSearchResponse)
async def search_messages(
    query: str = Query(..., min_length=1, max_length=200, description="Search query"),
    channels: Optional[List[str]] = Query(default=None, description="Filter by specific channels"),
    date_from: Optional[datetime] = Query(default=None, description="Search from this date"),
    date_to: Optional[datetime] = Query(default=None, description="Search until this date"),
    has_media: Optional[bool] = Query(default=None, description="Filter by media presence"),
    sentiment: Optional[SentimentLabel] = Query(default=None, description="Filter by sentiment"),
    min_relevance: float = Query(default=0.0, ge=0, le=1, description="Minimum relevance score"),
    page: int = Query(default=1, ge=1, description="Page number"),
    page_size: int = Query(default=20, ge=1, le=100, description="Results per page")
):
    """
    🔍 **Search Messages**
    
    Searches for messages containing specific keywords with advanced filtering options.
    Supports full-text search, date ranges, channel filtering, and relevance scoring.
    
    **Business Question**: How does the price or availability of a specific product vary across different channels?
    """
    try:
        # Calculate offset for pagination
        offset = (page - 1) * page_size
        
        matches, total_count, metadata = AnalyticalCRUD.search_messages(
            query=query,
            channels=channels,
            date_from=date_from,
            date_to=date_to,
            has_media=has_media,
            sentiment=sentiment.value if sentiment else None,
            min_relevance=min_relevance,
            limit=page_size,
            offset=offset
        )
        
        # Calculate pagination info
        total_pages = (total_count + page_size - 1) // page_size
        
        from schemas import PaginationInfo, SearchFilters
        pagination = PaginationInfo(
            page=page,
            page_size=page_size,
            total_items=total_count,
            total_pages=total_pages,
            has_next=page < total_pages,
            has_previous=page > 1
        )
        
        search_filters = SearchFilters(
            channels=channels,
            date_from=date_from,
            date_to=date_to,
            has_media=has_media,
            sentiment=sentiment,
            min_relevance=min_relevance
        )
        
        return MessageSearchResponse(
            query=query,
            matches=matches,
            total_matches=total_count,
            search_filters=search_filters,
            pagination=pagination
        )
    except Exception as e:
        logger.error(f"Error in search_messages: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ==========================================
# ADDITIONAL ANALYTICS ENDPOINTS
# ==========================================

@app.get("/api/analytics/channel-comparison", response_model=ChannelComparisonResponse)
async def get_channel_comparison(
    days_back: int = Query(default=30, ge=1, le=365, description="Number of days to analyze")
):
    """
    🏆 **Channel Performance Comparison**
    
    Compares performance metrics across all channels including engagement scores,
    posting frequency, media usage, and medical content ratios.
    """
    try:
        metrics, metadata = AnalyticalCRUD.get_channel_comparison(days_back=days_back)
        
        if not metrics:
            raise HTTPException(status_code=404, detail="No channel data found")
        
        # Find top performer based on engagement score
        top_performer = max(metrics, key=lambda x: x.engagement_score)
        
        return ChannelComparisonResponse(
            channels=metrics,
            comparison_period=f"Last {days_back} days",
            top_performer=top_performer.channel_name,
            metrics_compared=[
                "total_messages", "avg_daily_posts", "media_percentage",
                "avg_sentiment", "medical_content_ratio", "engagement_score"
            ]
        )
    except Exception as e:
        logger.error(f"Error in get_channel_comparison: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/analytics/trends", response_model=AnalyticalResponse)
async def get_daily_trends(
    metric: str = Query(default="message_count", description="Metric to analyze"),
    days_back: int = Query(default=30, ge=1, le=365, description="Number of days to analyze"),
    channel: Optional[str] = Query(default=None, description="Optional channel filter")
):
    """
    📊 **Daily Trends Analysis**
    
    Analyzes daily trends for various metrics like message count, sentiment, 
    media ratio, and medical content across specified time periods.
    """
    try:
        trends, metadata = AnalyticalCRUD.get_daily_trends(
            metric=metric,
            days_back=days_back,
            channel=channel
        )
        
        # Calculate trend direction
        if len(trends) >= 2:
            first_half = trends[:len(trends)//2]
            second_half = trends[len(trends)//2:]
            
            avg_first = sum(t.value for t in first_half) / len(first_half)
            avg_second = sum(t.value for t in second_half) / len(second_half)
            
            if avg_second > avg_first * 1.05:
                trend_direction = "increasing"
                growth_rate = ((avg_second - avg_first) / avg_first) * 100
            elif avg_second < avg_first * 0.95:
                trend_direction = "decreasing"  
                growth_rate = ((avg_second - avg_first) / avg_first) * 100
            else:
                trend_direction = "stable"
                growth_rate = 0.0
        else:
            trend_direction = "insufficient_data"
            growth_rate = 0.0
        
        trend_analysis = TrendAnalysis(
            trend_type=metric,
            period=f"Last {days_back} days" + (f" for {channel}" if channel else ""),
            data_points=trends,
            trend_direction=trend_direction,
            growth_rate=round(growth_rate, 2) if growth_rate else None
        )
        
        return AnalyticalResponse(
            data=trend_analysis.model_dump(),
            metadata=metadata
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error in get_daily_trends: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/analytics/object-detection-summary", response_model=AnalyticalResponse)
async def get_object_detection_summary(
    days_back: int = Query(default=30, ge=1, le=365, description="Number of days to analyze")
):
    """
    👁️ **Object Detection Summary**
    
    Provides comprehensive summary of YOLOv8 object detection results from medical images.
    Answers: Which channels have the most visual content (e.g., images of pills vs. creams)?
    """
    try:
        summary, metadata = AnalyticalCRUD.get_object_detection_summary(days_back=days_back)
        
        return AnalyticalResponse(
            data=summary,
            metadata=metadata,
            suggestions=[
                "Try filtering by specific medical categories",
                "Compare detection confidence across channels",
                "Analyze medical equipment vs hygiene products"
            ]
        )
    except Exception as e:
        logger.error(f"Error in get_object_detection_summary: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ==========================================
# UTILITY ENDPOINTS
# ==========================================

@app.get("/api/channels", response_model=List[str])
async def get_available_channels():
    """Get list of available channels for filtering."""
    try:
        from database import execute_analytical_query
        
        query = "SELECT DISTINCT channel FROM marts.fct_messages ORDER BY channel"
        results = execute_analytical_query(query)
        
        return [row['channel'] for row in results]
    except Exception as e:
        logger.error(f"Error getting channels: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/metrics", response_model=List[str])
async def get_available_metrics():
    """Get list of available metrics for trend analysis."""
    return ["message_count", "sentiment", "media_ratio", "medical_content"]

@app.get("/api/stats", response_model=Dict[str, Any])
async def get_api_stats():
    """Get API usage statistics and data overview."""
    try:
        health_data, _ = AnalyticalCRUD.get_health_check()
        
        from database import execute_analytical_query
        
        # Get data range
        date_range_query = """
        SELECT 
            MIN(message_date) as earliest_date,
            MAX(message_date) as latest_date
        FROM marts.fct_messages
        """
        
        date_results = execute_analytical_query(date_range_query)
        date_info = date_results[0] if date_results else {}
        
        return {
            "total_messages": health_data["total_messages"],
            "total_detections": health_data["total_detections"],
            "data_range": {
                "earliest_date": date_info.get("earliest_date"),
                "latest_date": date_info.get("latest_date")
            },
            "last_update": health_data["last_update"],
            "api_version": "1.0.0",
            "endpoints_available": 8
        }
    except Exception as e:
        logger.error(f"Error getting API stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Development helper endpoint (remove in production)
@app.get("/api/debug/database-tables")
async def get_database_tables():
    """Debug endpoint to show available database tables and columns."""
    try:
        from database import get_table_info
        
        tables = get_table_info('marts')
        return {"tables": tables}
    except Exception as e:
        logger.error(f"Error getting table info: {e}")
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    
    print("🏥 Starting Ethiopian Medical Data Analytics API...")
    print("📖 API Documentation: http://localhost:8000/docs")
    print("🔧 Interactive API: http://localhost:8000/redoc")
    
    uvicorn.run(
        "main:app", 
        host="0.0.0.0", 
        port=8000, 
        reload=True,  # Remove in production
        log_level="info"
    ) 