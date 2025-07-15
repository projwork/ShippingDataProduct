"""
SQLAlchemy Models for Ethiopian Medical Data Analytics

This module defines SQLAlchemy models that correspond to the dbt fact and dimension tables
in the PostgreSQL data warehouse. These models enable structured querying for the FastAPI endpoints.
"""

from sqlalchemy import Column, String, Integer, BigInteger, Float, DateTime, Boolean, Text, Numeric
from sqlalchemy.sql import func
from database import Base


class DimChannels(Base):
    """Dimension table for Telegram channels."""
    __tablename__ = 'dim_channels'
    __table_args__ = {'schema': 'marts'}
    
    channel_key = Column(Text, primary_key=True)
    channel_name = Column(String)
    channel_display_name = Column(String)
    channel_name_clean = Column(Text)
    channel_type = Column(String)
    channel_category = Column(String)
    channel_description = Column(Text)
    subscriber_count = Column(BigInteger)
    post_frequency = Column(String)
    language = Column(String)
    country = Column(String)
    verification_status = Column(String)
    creation_date = Column(DateTime)
    last_post_date = Column(DateTime)
    is_medical_related = Column(Boolean)
    is_pharmacy = Column(Boolean)
    is_active = Column(Boolean)


class DimDates(Base):
    """Dimension table for dates."""
    __tablename__ = 'dim_dates'
    __table_args__ = {'schema': 'marts'}
    
    date_day = Column(DateTime, primary_key=True)
    year = Column(Numeric)
    quarter = Column(Numeric)
    month = Column(Numeric)
    week_of_year = Column(Numeric)
    day_of_month = Column(Numeric)
    day_of_week = Column(Numeric)
    day_name = Column(Text)
    month_name = Column(Text)
    quarter_name = Column(Text)
    is_weekend = Column(Boolean)
    is_holiday = Column(Boolean)
    is_month_start = Column(Boolean)
    is_month_end = Column(Boolean)
    is_quarter_start = Column(Boolean)
    is_quarter_end = Column(Boolean)
    is_year_start = Column(Boolean)
    is_year_end = Column(Boolean)
    days_from_start = Column(Numeric)
    week_start_date = Column(DateTime)
    week_end_date = Column(DateTime)
    month_start_date = Column(DateTime)


class FctMessages(Base):
    """Fact table for Telegram messages."""
    __tablename__ = 'fct_messages'
    __table_args__ = {'schema': 'marts'}
    
    message_key = Column(Text, primary_key=True)
    channel_key = Column(Text)
    message_date_key = Column(DateTime)
    message_id = Column(BigInteger)
    channel = Column(String)
    message_text = Column(Text)
    message_date = Column(DateTime)
    message_text_clean = Column(Text)
    has_media = Column(Boolean)
    media_type = Column(String)
    message_length = Column(Integer)
    word_count = Column(Integer)
    has_url = Column(Boolean)
    url_count = Column(Integer)
    has_hashtag = Column(Boolean)
    hashtag_count = Column(Integer)
    has_mention = Column(Boolean)
    mention_count = Column(Integer)
    has_emoji = Column(Boolean)
    emoji_count = Column(Integer)
    language_detected = Column(String)
    sentiment_score = Column(Float)
    sentiment_label = Column(String)
    contains_price = Column(Boolean)
    price_mentions = Column(Integer)
    contains_phone = Column(Boolean)
    phone_numbers = Column(Integer)
    contains_medical_terms = Column(Boolean)
    medical_keywords_count = Column(Integer)
    is_promotional = Column(Boolean)
    is_educational = Column(Boolean)
    is_advertisement = Column(Boolean)
    engagement_category = Column(String)
    message_type = Column(String)
    quality_score = Column(Float)
    post_hour = Column(Integer)
    is_weekend_post = Column(Boolean)
    response_time_hours = Column(Float)


class FctImageDetections(Base):
    """Fact table for image object detections."""
    __tablename__ = 'fct_image_detections'
    __table_args__ = {'schema': 'marts'}
    
    detection_key = Column(Text, primary_key=True)
    message_key = Column(Text)
    channel_key = Column(Text)
    detection_date_key = Column(DateTime)
    detection_id = Column(String)
    message_id = Column(BigInteger)
    channel = Column(String)
    image_path = Column(Text)
    class_id = Column(Integer)
    class_name = Column(String)
    medical_category = Column(String)
    confidence_score = Column(Float)
    bounding_box_x1 = Column(Float)
    bounding_box_y1 = Column(Float)
    bounding_box_x2 = Column(Float)
    bounding_box_y2 = Column(Float)
    bounding_box_width = Column(Float)
    bounding_box_height = Column(Float)
    bounding_box_area = Column(Float)
    bounding_box_center_x = Column(Float)
    bounding_box_center_y = Column(Float)
    confidence_level = Column(String)
    is_medical_relevant = Column(Boolean)
    object_prominence_score = Column(Float)
    position_horizontal = Column(String)
    position_vertical = Column(String)
    detection_quality = Column(String)
    medical_relevance_score = Column(Integer)
    contains_person = Column(Boolean)
    contains_medical_equipment = Column(Boolean)
    contains_hygiene_products = Column(Boolean)
    detection_timestamp = Column(DateTime)


class FctChannelDailySummary(Base):
    """Fact table for daily channel summaries."""
    __tablename__ = 'fct_channel_daily_summary'
    __table_args__ = {'schema': 'marts'}
    
    daily_summary_key = Column(Text, primary_key=True)
    channel_key = Column(Text)
    date_day = Column(DateTime)
    channel = Column(String)
    channel_category = Column(Text)
    total_messages = Column(BigInteger)
    total_media_messages = Column(BigInteger)
    total_text_messages = Column(BigInteger)
    media_percentage = Column(Float)
    avg_message_length = Column(Float)
    avg_word_count = Column(Float)
    total_urls = Column(BigInteger)
    total_hashtags = Column(BigInteger)
    total_mentions = Column(BigInteger)
    total_emojis = Column(BigInteger)
    avg_sentiment_score = Column(Float)
    positive_messages = Column(BigInteger)
    negative_messages = Column(BigInteger)
    neutral_messages = Column(BigInteger)
    promotional_messages = Column(BigInteger)
    educational_messages = Column(BigInteger)
    advertisement_messages = Column(BigInteger)
    messages_with_prices = Column(BigInteger)
    messages_with_phones = Column(BigInteger)
    messages_with_medical_terms = Column(BigInteger)
    avg_medical_keywords = Column(Float)
    total_image_detections = Column(BigInteger)
    unique_detected_objects = Column(BigInteger)
    avg_detection_confidence = Column(Float)
    medical_relevant_detections = Column(BigInteger)
    person_detections = Column(BigInteger)
    medical_equipment_detections = Column(BigInteger)
    hygiene_product_detections = Column(BigInteger)
    avg_quality_score = Column(Float)
    peak_posting_hour = Column(Integer)
    posting_hours_active = Column(Integer)
    weekend_posts = Column(BigInteger)
    weekend_percentage = Column(Float)
    first_post_time = Column(DateTime)
    last_post_time = Column(DateTime)
    posting_duration_hours = Column(Float)


# Base query mixins for common analytics operations
class AnalyticalQueryMixin:
    """Mixin class with common analytical query methods."""
    
    @classmethod
    def get_top_items(cls, session, column, limit=10, filter_condition=None):
        """Get top N items by count for a given column."""
        query = session.query(
            getattr(cls, column).label('item'),
            func.count('*').label('count')
        ).group_by(getattr(cls, column))
        
        if filter_condition:
            query = query.filter(filter_condition)
            
        return query.order_by(func.count('*').desc()).limit(limit).all()
    
    @classmethod
    def get_daily_trends(cls, session, date_column, filter_condition=None):
        """Get daily trend data."""
        query = session.query(
            func.date(getattr(cls, date_column)).label('date'),
            func.count('*').label('count')
        ).group_by(func.date(getattr(cls, date_column)))
        
        if filter_condition:
            query = query.filter(filter_condition)
            
        return query.order_by(func.date(getattr(cls, date_column))).all()


# Add the mixin to fact tables
class FctMessagesAnalytical(FctMessages, AnalyticalQueryMixin):
    """Extended FctMessages with analytical methods."""
    pass


class FctImageDetectionsAnalytical(FctImageDetections, AnalyticalQueryMixin):
    """Extended FctImageDetections with analytical methods."""
    pass


# Table mapping for dynamic queries
TABLE_MAPPING = {
    'messages': FctMessages,
    'image_detections': FctImageDetections,
    'channels': DimChannels,
    'dates': DimDates,
    'daily_summary': FctChannelDailySummary
}


def get_model_by_name(table_name: str):
    """Get SQLAlchemy model by table name."""
    return TABLE_MAPPING.get(table_name.lower())


if __name__ == "__main__":
    """Test model imports and basic functionality."""
    print("🔍 Testing SQLAlchemy Models...")
    
    # Test that all models can be imported
    models = [DimChannels, DimDates, FctMessages, FctImageDetections, FctChannelDailySummary]
    
    for model in models:
        print(f"✅ {model.__name__} model loaded successfully")
        print(f"   Table: {model.__tablename__}")
        print(f"   Schema: {model.__table_args__.get('schema', 'public')}")
        print(f"   Columns: {len(model.__table__.columns)}")
        print()
    
    print(f"📊 Total models defined: {len(models)}")
    print("✅ All models loaded successfully!") 