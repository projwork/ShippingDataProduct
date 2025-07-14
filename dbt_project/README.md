# Ethiopian Medical Data Warehouse - dbt Project

This dbt project transforms raw Telegram data from Ethiopian medical channels into a clean, well-structured data warehouse optimized for analytics.

## 📊 Project Overview

### Architecture

The project follows a layered approach:

- **Raw Layer**: Untransformed data from PostgreSQL raw schema
- **Staging Layer**: Cleaned and lightly transformed data
- **Marts Layer**: Business-ready dimensional models

### Star Schema Design

```
                    ┌─────────────┐
                    │ dim_dates   │
                    └─────────────┘
                           │
                           │
    ┌─────────────┐        │        ┌─────────────────┐
    │ dim_channels│◄───────┼───────►│  fct_messages   │
    └─────────────┘        │        └─────────────────┘
                           │
                           │
                    ┌─────────────────────────┐
                    │ fct_channel_daily_summary│
                    └─────────────────────────┘
```

## 🗂️ Project Structure

```
dbt_project/
├── models/
│   ├── staging/           # Cleaned raw data
│   │   ├── _sources.yml   # Raw data source definitions
│   │   ├── _models.yml    # Staging model documentation
│   │   ├── stg_telegram_messages.sql
│   │   └── stg_channels.sql
│   └── marts/             # Business-ready models
│       ├── dimensions/    # Dimension tables
│       │   ├── dim_channels.sql
│       │   └── dim_dates.sql
│       ├── facts/         # Fact tables
│       │   ├── fct_messages.sql
│       │   └── fct_channel_daily_summary.sql
│       └── _models.yml    # Mart model documentation
├── tests/                 # Custom data tests
├── macros/               # Reusable SQL functions
├── dbt_project.yml       # Project configuration
└── profiles.yml          # Database connection settings
```

## 🚀 Getting Started

### Prerequisites

1. PostgreSQL database with raw data loaded
2. dbt and dbt-postgres installed
3. Environment variables configured

### Installation

```bash
# Install dependencies
pip install dbt-postgres

# Install dbt packages
dbt deps

# Test database connection
dbt debug
```

### Running the Project

```bash
# Run all models
dbt run

# Run with full refresh
dbt run --full-refresh

# Run specific models
dbt run --select staging
dbt run --select marts

# Run tests
dbt test

# Generate documentation
dbt docs generate
dbt docs serve
```

## 📋 Data Models

### Staging Models

#### `stg_telegram_messages`

Cleaned telegram messages with:

- Standardized data types
- Content classification flags
- Engagement categorization
- Date component extraction

#### `stg_channels`

Channel information with:

- Channel categorization
- Activity level assessment
- Status determination

### Dimension Tables

#### `dim_channels`

Channel master data including:

- Channel metadata
- Activity metrics
- Categorization
- Status tracking

#### `dim_dates`

Comprehensive date dimension with:

- Standard date components
- Business calendar flags
- Ethiopian seasonal context
- Relative date indicators

### Fact Tables

#### `fct_messages`

Granular message-level analytics:

- Message content metrics
- Engagement scores
- Content classification
- Quality assessment

#### `fct_channel_daily_summary`

Aggregated daily channel metrics:

- Message counts by type
- Engagement summaries
- Content distribution
- Time-based patterns

## 🧪 Data Quality & Testing

### Built-in Tests

- **Uniqueness**: Primary keys and unique constraints
- **Not Null**: Critical fields validation
- **Referential Integrity**: Foreign key relationships
- **Accepted Values**: Categorical field validation
- **Range Checks**: Numeric field boundaries

### Custom Tests

- **No Duplicate Messages**: Ensures message uniqueness
- **Engagement Logic**: Validates calculation accuracy
- **Daily Summary Consistency**: Verifies aggregation integrity

### Data Quality Rules

1. Every message must have a valid channel and date
2. Engagement scores must be non-negative
3. Content classifications are mutually consistent
4. Daily summaries match detail-level aggregations

## 📈 Key Metrics & KPIs

### Channel Performance

- Total messages per channel
- Average engagement score
- Content type distribution
- Activity trends

### Content Analysis

- Message quality assessment
- Content category breakdown
- Media usage patterns
- Engagement drivers

### Temporal Patterns

- Daily/hourly posting patterns
- Seasonal trends
- Day-of-week analysis
- Growth rates

## 🔧 Configuration

### Environment Variables

Required in your `.env` file:

```env
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DATABASE=ethiopian_medical_data
POSTGRES_USERNAME=your_username
POSTGRES_PASSWORD=your_password
POSTGRES_RAW_SCHEMA=raw
POSTGRES_STAGING_SCHEMA=staging
POSTGRES_MARTS_SCHEMA=marts
```

### Model Configurations

- **Staging**: Materialized as views for efficiency
- **Marts**: Materialized as tables for performance
- **Indexes**: Strategic indexing on key columns
- **Schema Separation**: Clean layer isolation

## 📚 Documentation

### Generating Documentation

```bash
# Generate docs
dbt docs generate

# Serve docs locally
dbt docs serve
```

The documentation includes:

- Model lineage diagrams
- Column descriptions
- Test results
- Source freshness
- Model performance metrics

## 🔄 Development Workflow

### Adding New Models

1. Create SQL file in appropriate directory
2. Add model documentation in `_models.yml`
3. Add relevant tests
4. Run and validate: `dbt run --select new_model`
5. Test: `dbt test --select new_model`

### Best Practices

- Follow naming conventions (`stg_`, `dim_`, `fct_`)
- Document all models and columns
- Add tests for critical business logic
- Use consistent SQL formatting
- Implement proper error handling

## 🚨 Troubleshooting

### Common Issues

1. **Connection Error**: Check PostgreSQL credentials
2. **Schema Not Found**: Ensure schemas exist in database
3. **Model Failures**: Check source data availability
4. **Test Failures**: Validate data quality issues

### Debug Commands

```bash
# Check connection
dbt debug

# Compile without running
dbt compile

# Run specific model with logs
dbt run --select model_name --log-level debug

# Show model SQL
dbt show --select model_name
```

## 📊 Usage Examples

### Common Queries

#### Channel Performance Analysis

```sql
SELECT
    dc.channel_display_name,
    SUM(fcd.total_messages) as total_messages,
    AVG(fcd.engagement_rate) as avg_engagement_rate
FROM marts.fct_channel_daily_summary fcd
JOIN marts.dim_channels dc ON fcd.channel_key = dc.channel_key
WHERE fcd.date_day >= current_date - 30
GROUP BY dc.channel_display_name
ORDER BY total_messages DESC;
```

#### Content Type Trends

```sql
SELECT
    dd.year_month,
    SUM(CASE WHEN fm.is_pharmacy_content THEN 1 ELSE 0 END) as pharmacy_posts,
    SUM(CASE WHEN fm.is_cosmetics_content THEN 1 ELSE 0 END) as cosmetics_posts,
    SUM(CASE WHEN fm.is_healthcare_content THEN 1 ELSE 0 END) as healthcare_posts
FROM marts.fct_messages fm
JOIN marts.dim_dates dd ON fm.message_date_key = dd.date_day
GROUP BY dd.year_month
ORDER BY dd.year_month;
```

## 🔮 Future Enhancements

### Planned Features

- Incremental model updates
- Real-time data refresh
- Advanced analytics models
- Machine learning integration
- Data quality monitoring
- Automated alerting

### Contributing

1. Fork the project
2. Create feature branch
3. Add models with tests and documentation
4. Submit pull request

---

**Data Warehouse Built with ❤️ using dbt**
