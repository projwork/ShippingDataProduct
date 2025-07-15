# 🏥 Ethiopian Medical Data Analytics API

A FastAPI-powered analytical API for exploring Ethiopian medical business data scraped from Telegram channels. This API provides comprehensive insights about medical products, channel activity, content search, and visual object detection results.

## 📊 Overview

This API serves analytical insights to answer key business questions:

1. **What are the top 10 most frequently mentioned medical products or drugs across all channels?**
2. **How does the price or availability of a specific product vary across different channels?**
3. **Which channels have the most visual content (e.g., images of pills vs. creams)?**
4. **What are the daily and weekly trends in posting volume for health-related topics?**

## 🚀 Quick Start

### Prerequisites

- Python 3.8+
- PostgreSQL database with dbt-transformed data
- All dependencies installed (see `requirements.txt`)

### Installation & Setup

1. **Clone and navigate to the project directory:**

```bash
cd shipping-data-product
```

2. **Install dependencies:**

```bash
pip install -r requirements.txt
```

3. **Set up environment variables:**
   Create a `.env` file with your database credentials:

```env
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DATABASE=ethiopian_medical_data
POSTGRES_USERNAME=your_username
POSTGRES_PASSWORD=your_password
```

4. **Start the API server:**

```bash
python main.py
```

The API will be available at: `http://localhost:8000`

### Alternative Startup (Using Uvicorn)

```bash
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

## 📖 API Documentation

### Interactive Documentation

Once the server is running, access the interactive API documentation:

- **Swagger UI**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc
- **OpenAPI Schema**: http://localhost:8000/openapi.json

### Quick Health Check

Test if the API is working:

```bash
curl http://localhost:8000/health
```

## 🎯 Main Endpoints

### 1. Top Medical Products

**Endpoint**: `GET /api/reports/top-products`

**Description**: Get the most frequently mentioned medical products across all channels.

**Parameters**:

- `limit` (optional): Number of products to return (1-50, default: 10)
- `days_back` (optional): Days to analyze (1-365, default: 30)

**Example**:

```bash
# Get top 5 products from last 7 days
curl "http://localhost:8000/api/reports/top-products?limit=5&days_back=7"
```

**Sample Response**:

```json
{
  "success": true,
  "message": "Request completed successfully",
  "timestamp": "2025-07-15T22:00:00.000Z",
  "products": [
    {
      "product_name": "Paracetamol",
      "mention_count": 45,
      "channels": ["@lobelia4cosmetics", "@tikvahpharma"],
      "avg_sentiment": 0.623,
      "price_mentions": 12,
      "last_mentioned": "2025-07-15T18:30:00.000Z"
    }
  ],
  "total_products": 5,
  "analysis_period": "Last 7 days"
}
```

### 2. Channel Activity Analysis

**Endpoint**: `GET /api/channels/{channel_name}/activity`

**Description**: Get detailed posting activity and engagement metrics for a specific channel.

**Parameters**:

- `channel_name` (path): Channel name (with or without @)
- `days_back` (optional): Days to analyze (1-365, default: 30)

**Example**:

```bash
# Get activity for @lobelia4cosmetics from last 14 days
curl "http://localhost:8000/api/channels/@lobelia4cosmetics/activity?days_back=14"

# Or without @ symbol
curl "http://localhost:8000/api/channels/lobelia4cosmetics/activity?days_back=14"
```

**Sample Response**:

```json
{
  "success": true,
  "message": "Request completed successfully",
  "timestamp": "2025-07-15T22:00:00.000Z",
  "channel_info": {
    "channel_name": "@lobelia4cosmetics",
    "display_name": "Lobelia Cosmetics",
    "category": "cosmetics",
    "is_medical": true,
    "subscriber_count": 15000,
    "total_messages": 342
  },
  "daily_activity": [
    {
      "date": "2025-07-15T00:00:00.000Z",
      "message_count": 23,
      "media_count": 15,
      "avg_sentiment": 0.654,
      "peak_hour": 14
    }
  ],
  "summary_stats": {
    "total_messages": 342,
    "media_percentage": 65.2,
    "avg_daily_posts": 12.4,
    "avg_sentiment": 0.623
  },
  "top_posting_hours": [14, 10, 16, 9, 15]
}
```

### 3. Message Search

**Endpoint**: `GET /api/search/messages`

**Description**: Search messages with advanced filtering options.

**Parameters**:

- `query` (required): Search query string
- `channels` (optional): Filter by specific channels
- `date_from` (optional): Start date (ISO format)
- `date_to` (optional): End date (ISO format)
- `has_media` (optional): Filter by media presence (true/false)
- `sentiment` (optional): Filter by sentiment (positive/negative/neutral)
- `min_relevance` (optional): Minimum relevance score (0-1, default: 0)
- `page` (optional): Page number (default: 1)
- `page_size` (optional): Results per page (1-100, default: 20)

**Example**:

```bash
# Search for "paracetamol" in all channels
curl "http://localhost:8000/api/search/messages?query=paracetamol"

# Advanced search with filters
curl "http://localhost:8000/api/search/messages?query=medicine&channels=@lobelia4cosmetics&has_media=true&page=1&page_size=10"
```

**Sample Response**:

```json
{
  "success": true,
  "message": "Request completed successfully",
  "timestamp": "2025-07-15T22:00:00.000Z",
  "query": "paracetamol",
  "matches": [
    {
      "message_id": 12345,
      "channel": "@tikvahpharma",
      "message_text": "High quality Paracetamol tablets available now. Contact us for pricing and availability...",
      "message_date": "2025-07-15T14:30:00.000Z",
      "sentiment": "positive",
      "has_media": true,
      "relevance_score": 0.95,
      "matched_terms": ["paracetamol"]
    }
  ],
  "total_matches": 25,
  "pagination": {
    "page": 1,
    "page_size": 20,
    "total_items": 25,
    "total_pages": 2,
    "has_next": true,
    "has_previous": false
  }
}
```

## 🔍 Additional Analytics Endpoints

### Channel Comparison

```bash
curl "http://localhost:8000/api/analytics/channel-comparison?days_back=30"
```

### Daily Trends Analysis

```bash
# Message count trends
curl "http://localhost:8000/api/analytics/trends?metric=message_count&days_back=30"

# Sentiment trends for specific channel
curl "http://localhost:8000/api/analytics/trends?metric=sentiment&channel=@lobelia4cosmetics&days_back=14"
```

### Object Detection Summary

```bash
curl "http://localhost:8000/api/analytics/object-detection-summary?days_back=30"
```

## 🛠 Utility Endpoints

### Get Available Channels

```bash
curl "http://localhost:8000/api/channels"
```

### Get Available Metrics

```bash
curl "http://localhost:8000/api/metrics"
```

### API Statistics

```bash
curl "http://localhost:8000/api/stats"
```

## 📱 Frontend Integration

### Using JavaScript/Fetch

```javascript
// Get top products
async function getTopProducts() {
  const response = await fetch(
    "http://localhost:8000/api/reports/top-products?limit=5"
  );
  const data = await response.json();
  console.log(data.products);
}

// Search messages
async function searchMessages(query) {
  const response = await fetch(
    `http://localhost:8000/api/search/messages?query=${encodeURIComponent(
      query
    )}`
  );
  const data = await response.json();
  return data.matches;
}

// Get channel activity
async function getChannelActivity(channelName) {
  const response = await fetch(
    `http://localhost:8000/api/channels/${encodeURIComponent(
      channelName
    )}/activity`
  );
  const data = await response.json();
  return data;
}
```

### Using Python Requests

```python
import requests

# API base URL
BASE_URL = "http://localhost:8000"

# Get top products
response = requests.get(f"{BASE_URL}/api/reports/top-products", params={
    "limit": 10,
    "days_back": 30
})
products = response.json()

# Search messages
response = requests.get(f"{BASE_URL}/api/search/messages", params={
    "query": "paracetamol",
    "page_size": 20
})
search_results = response.json()

# Get channel activity
response = requests.get(f"{BASE_URL}/api/channels/@lobelia4cosmetics/activity")
activity = response.json()
```

### Using cURL

```bash
# Set base URL
BASE_URL="http://localhost:8000"

# Get top products with headers
curl -H "Accept: application/json" \
     -H "Content-Type: application/json" \
     "$BASE_URL/api/reports/top-products?limit=5"

# Search with POST (if implemented)
curl -X POST \
     -H "Content-Type: application/json" \
     -d '{"query": "medicine", "page_size": 10}' \
     "$BASE_URL/api/search/messages"
```

## 🧪 Testing the API

### Using the Interactive Documentation

1. Open http://localhost:8000/docs in your browser
2. Click on any endpoint to expand it
3. Click "Try it out" button
4. Fill in the parameters
5. Click "Execute" to test

### Using Postman

1. Import the OpenAPI schema from http://localhost:8000/openapi.json
2. Create a new environment with `base_url` = `http://localhost:8000`
3. Test each endpoint with different parameters

### Sample Test Queries

```bash
# Test health check
curl http://localhost:8000/health

# Test with no parameters (should use defaults)
curl http://localhost:8000/api/reports/top-products

# Test search with special characters
curl "http://localhost:8000/api/search/messages?query=covid-19"

# Test channel activity for both channels
curl http://localhost:8000/api/channels/lobelia4cosmetics/activity
curl http://localhost:8000/api/channels/tikvahpharma/activity

# Test trends for different metrics
curl "http://localhost:8000/api/analytics/trends?metric=message_count"
curl "http://localhost:8000/api/analytics/trends?metric=sentiment"
curl "http://localhost:8000/api/analytics/trends?metric=media_ratio"
```

## ⚙️ Configuration

### Environment Variables

| Variable            | Description       | Default                |
| ------------------- | ----------------- | ---------------------- |
| `POSTGRES_HOST`     | Database host     | localhost              |
| `POSTGRES_PORT`     | Database port     | 5432                   |
| `POSTGRES_DATABASE` | Database name     | ethiopian_medical_data |
| `POSTGRES_USERNAME` | Database username | Required               |
| `POSTGRES_PASSWORD` | Database password | Required               |

### API Configuration

The API can be configured by modifying `main.py`:

- **CORS settings**: Update `allow_origins` for production
- **Port and host**: Change in the `uvicorn.run()` call
- **Logging level**: Modify `log_level` parameter
- **Rate limiting**: Add middleware for production use

## 🔒 Security Considerations

### For Production Deployment

1. **Environment Variables**: Use proper secret management
2. **CORS**: Restrict `allow_origins` to your frontend domains
3. **HTTPS**: Use SSL/TLS encryption
4. **Rate Limiting**: Implement request rate limiting
5. **Authentication**: Add API key or JWT authentication
6. **Input Validation**: Already implemented with Pydantic

### Example Production Settings

```python
# In main.py for production
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://yourdomain.com"],  # Specific domains only
    allow_credentials=True,
    allow_methods=["GET", "POST"],  # Limit methods
    allow_headers=["*"],
)
```

## 📊 Response Format

All API responses follow a consistent format:

### Success Response

```json
{
  "success": true,
  "message": "Request completed successfully",
  "timestamp": "2025-07-15T22:00:00.000Z",
  "data": {
    /* endpoint-specific data */
  }
}
```

### Error Response

```json
{
  "success": false,
  "message": "Error description",
  "timestamp": "2025-07-15T22:00:00.000Z",
  "errors": [
    {
      "code": "VALIDATION_ERROR",
      "message": "Field validation failed",
      "field": "limit"
    }
  ]
}
```

## 🐛 Troubleshooting

### Common Issues

1. **Database Connection Error**

   ```
   Solution: Check your .env file and database credentials
   ```

2. **No Data Returned**

   ```
   Solution: Verify your database has data and run dbt models
   ```

3. **Import Errors**

   ```
   Solution: Ensure all dependencies are installed: pip install -r requirements.txt
   ```

4. **Port Already in Use**
   ```
   Solution: Change port in main.py or kill existing process
   ```

### Debug Mode

Enable debug logging by setting:

```bash
export LOG_LEVEL=DEBUG
```

### API Debug Endpoint

```bash
curl http://localhost:8000/api/debug/database-tables
```

## 📈 Performance

### Query Performance

- Most endpoints return results in < 1 second
- Complex aggregations may take up to 5 seconds
- Pagination is implemented for large result sets
- Database indexes optimize common queries

### Caching

Consider implementing Redis caching for production:

```python
# Example caching middleware
from fastapi_cache import FastAPICache
from fastapi_cache.backends.redis import RedisBackend

@app.on_event("startup")
async def startup():
    redis = aioredis.from_url("redis://localhost", encoding="utf8", decode_responses=True)
    FastAPICache.init(RedisBackend(redis), prefix="medical-api-cache")
```

## 🤝 Contributing

### API Development Guidelines

1. **Endpoint Naming**: Use RESTful conventions
2. **Response Schemas**: Always use Pydantic models
3. **Error Handling**: Implement consistent error responses
4. **Documentation**: Update docstrings and README
5. **Testing**: Add unit tests for new endpoints

### Adding New Endpoints

1. Create CRUD function in `crud.py`
2. Define response schema in `schemas.py`
3. Add endpoint in `main.py`
4. Update this README
5. Test thoroughly

## 📞 Support

For questions or issues:

- **Email**: data@karasolutions.et
- **Documentation**: http://localhost:8000/docs
- **GitHub Issues**: Create an issue in the repository

## 📄 License

MIT License - see LICENSE file for details.

---

**Built with ❤️ by Kara Solutions Data Engineering Team**

_Last updated: July 15, 2025_
