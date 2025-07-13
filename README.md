# Ethiopian Medical Telegram Data Scraper

A comprehensive data scraping and analysis system for Ethiopian medical and healthcare Telegram channels. This project provides tools to collect, process, and analyze data from Telegram channels focused on medical businesses, pharmacies, cosmetics, and healthcare providers in Ethiopia.

## 🏥 Project Overview

This project implements a robust data pipeline for:

- **Data Collection**: Scraping messages and media from Ethiopian medical Telegram channels
- **Data Storage**: Organized, partitioned storage in JSON format with date-based directories
- **Image Collection**: Automated download of images for object detection and analysis
- **Data Analysis**: Comprehensive analysis including sentiment, engagement, and content categorization
- **Reporting**: Jupyter notebook-based analysis and visualization

## 🎯 Target Channels

The scraper focuses on Ethiopian medical and healthcare channels:

- **@lobelia4cosmetics** - Lobelia Cosmetics
- **@tikvahpharma** - Tikvah Pharma
- **Chemed Telegram Channel** - Medical news and updates
- Additional channels from [et.tgstat.com/medicine](https://et.tgstat.com/medicine)

## 📁 Project Structure

```
shipping-data-product/
├── data/                           # Data storage directory
│   ├── raw/                       # Raw scraped data
│   │   └── telegram_messages/     # Message data by date
│   │       └── YYYY-MM-DD/        # Date-partitioned directories
│   │           ├── channel_name.json
│   │           ├── combined_medical_channels.json
│   │           └── scrape_metadata.json
│   ├── images/                    # Downloaded images
│   │   └── channel_name/          # Channel-specific images
│   ├── processed/                 # Processed data (future use)
│   └── logs/                      # Application logs
├── src/                           # Source code modules
│   ├── __init__.py
│   └── data_ingestion.py          # Core data ingestion class
├── scripts/                       # Executable scripts
│   ├── __init__.py
│   ├── medical_telegram_scraper.py # Main scraping script
│   └── telegram_scrapper.py       # Original scraper (reference)
├── notebooks/                     # Jupyter notebooks
│   └── medical_data_analysis.ipynb # Comprehensive analysis notebook
├── tests/                         # Test files
├── requirements.txt               # Python dependencies
├── .env                          # Environment variables (API credentials)
├── .gitignore                    # Git ignore rules
└── README.md                     # This file
```

## 🚀 Quick Start

### 1. Prerequisites

- Python 3.8 or higher
- Telegram account with API access
- Git (for cloning the repository)

### 2. Installation

```bash
# Clone the repository
git clone <repository-url>
cd shipping-data-product

# Create virtual environment (recommended)
python -m venv venv

# Activate virtual environment
# On Windows:
venv\Scripts\activate
# On macOS/Linux:
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### 3. Telegram API Setup

1. **Get API Credentials**:

   - Go to [https://my.telegram.org/apps](https://my.telegram.org/apps)
   - Log in with your phone number
   - Create a new app:
     - App title: "Medical Channel Scraper"
     - Short name: "medical_scraper"
     - Description: "Ethiopian medical channel data scraper"
   - Note down your **API ID** and **API Hash**

2. **Configure Environment Variables**:

   ```bash
   # Edit the .env file
   nano .env
   ```

   Update with your credentials:

   ```env
   # Replace with your actual credentials
   TELEGRAM_API_ID=your_api_id_here
   TELEGRAM_API_HASH=your_api_hash_here
   TELEGRAM_PHONE_NUMBER=your_phone_number_here
   ```

### 4. Run the Scraper

```bash
# Run the medical channel scraper
python scripts/medical_telegram_scraper.py

# For help and options
python scripts/medical_telegram_scraper.py --help
```

### 5. Analyze the Data

```bash
# Start Jupyter Notebook
jupyter notebook

# Open the analysis notebook
# Navigate to notebooks/medical_data_analysis.ipynb
```

## 📊 Data Collection Process

### Authentication Flow

1. First run will prompt for phone verification code
2. If 2FA is enabled, you'll be prompted for your password
3. Session is saved for future runs

### Data Structure

Each scraped message includes:

- **Basic Info**: ID, channel, date, text content
- **Engagement**: Views, forwards, replies count
- **Media**: Type, file path, size (if applicable)
- **Metadata**: Raw Telegram data for advanced analysis

### Storage Format

```json
{
  "id": 123,
  "channel": "@lobelia4cosmetics",
  "date": "2024-01-15T10:30:00",
  "text": "New skincare product available...",
  "views": 1250,
  "forwards": 23,
  "replies": 5,
  "media_type": "photo",
  "media_path": "images/lobelia4cosmetics/123_20240115_103000.jpg",
  "media_size": 156789,
  "raw_data": {...}
}
```

## 🔧 Configuration

### Scraper Settings

Edit `SCRAPER_CONFIG` in `scripts/medical_telegram_scraper.py`:

```python
SCRAPER_CONFIG = {
    'limit_per_channel': 1000,     # Messages per channel
    'days_back': 30,               # Days to scrape back
    'rate_limit_delay': 2,         # Seconds between requests
    'max_retries': 3,              # Maximum retry attempts
}
```

### Channel Management

Add/remove channels in `MEDICAL_CHANNELS` list:

```python
MEDICAL_CHANNELS = [
    '@lobelia4cosmetics',
    '@tikvahpharma',
    '@your_new_channel',
    # Add more channels here
]
```

## 📈 Analysis Features

The included Jupyter notebook provides:

### 1. Data Exploration

- Message count and distribution
- Channel activity comparison
- Date range and temporal patterns

### 2. Text Analysis

- Content categorization (pharmacy, cosmetics, healthcare, etc.)
- Sentiment analysis
- Word frequency and word clouds
- Ethiopian and English text processing

### 3. Engagement Analysis

- View and forward statistics
- Engagement scoring
- Top-performing content identification

### 4. Media Analysis

- Image and media file statistics
- Media type distribution
- File size analysis

### 5. Temporal Analysis

- Peak activity hours
- Day-of-week patterns
- Message frequency trends

### 6. Advanced Analytics

- Correlation analysis
- Predictive insights
- Business intelligence metrics

## 🛠️ Customization

### Adding New Channels

1. Find channels on [et.tgstat.com/medicine](https://et.tgstat.com/medicine)
2. Add to `MEDICAL_CHANNELS` list
3. Ensure you have access to the channel
4. Run the scraper

### Extending Analysis

1. Add custom keywords to `medical_keywords` in the notebook
2. Create new categorization functions
3. Add custom visualizations
4. Export results to different formats

### Custom Data Processing

Create new modules in `src/` directory:

```python
# src/custom_processor.py
class CustomDataProcessor:
    def process_medical_data(self, data):
        # Your custom processing logic
        return processed_data
```

## 🚨 Important Notes

### Rate Limiting

- The scraper includes built-in rate limiting
- Respect Telegram's API limits
- Monitor for rate limit warnings in logs

### Data Privacy

- Only scrape public channels
- Respect channel privacy settings
- Follow Ethiopian data protection laws

### API Compliance

- Keep API credentials secure
- Don't share session files
- Monitor API usage

### Error Handling

- Check logs in `data/logs/` for errors
- Retry failed channels manually if needed
- Report persistent issues

## 📝 Logging

Comprehensive logging is implemented:

- **File Logs**: `data/logs/telegram_scraper_YYYYMMDD.log`
- **Console Output**: Real-time progress updates
- **Error Tracking**: Detailed error messages and stack traces

## 🔍 Troubleshooting

### Common Issues

1. **Authentication Failed**

   ```bash
   # Check your .env file
   cat .env
   # Verify API credentials at https://my.telegram.org/apps
   ```

2. **No Data Scraped**

   - Verify channel names (include @ symbol)
   - Check if channels are public
   - Ensure you have access to the channels

3. **Rate Limiting**

   - Wait for the specified time
   - Increase `rate_limit_delay` in config
   - Monitor API usage

4. **Missing Dependencies**

   ```bash
   pip install --upgrade -r requirements.txt
   ```

5. **Permission Errors**
   - Check file permissions
   - Ensure data directory is writable
   - Run with appropriate user permissions

### Getting Help

1. Check the logs in `data/logs/`
2. Review error messages in console output
3. Verify your environment setup
4. Check Telegram API status

## 📚 Advanced Usage

### Batch Processing

```bash
# Process multiple date ranges
python scripts/medical_telegram_scraper.py --days-back 60
```

### Custom Analysis

```python
# In your custom script
from src.data_ingestion import TelegramDataIngestion

# Initialize with custom settings
scraper = TelegramDataIngestion(base_data_dir="custom_data")
data = await scraper.scrape_channel("@custom_channel", limit=500)
```

### Integration with Other Tools

- Export data to CSV for Excel analysis
- Connect to databases for storage
- Use with machine learning pipelines
- API integration for real-time monitoring

## 📊 Performance Considerations

### Optimization Tips

1. Use appropriate `limit_per_channel` values
2. Implement incremental scraping for large channels
3. Monitor memory usage with large datasets
4. Use SSD storage for better I/O performance

### Resource Usage

- **Memory**: ~100MB per 10,000 messages
- **Storage**: ~50MB per 1,000 messages with media
- **Network**: Depends on media content volume

## 🔄 Future Enhancements

### Planned Features

1. **Real-time Monitoring**: Live data collection
2. **API Service**: REST API for data access
3. **Dashboard**: Web-based analytics dashboard
4. **Machine Learning**: Automated content classification
5. **Multi-language Support**: Enhanced Ethiopian language processing

### Contributing

1. Fork the repository
2. Create feature branches
3. Add tests for new functionality
4. Submit pull requests with detailed descriptions

## 📞 Support

For issues, questions, or contributions:

1. Check this README first
2. Review the code documentation
3. Check existing issues
4. Create detailed bug reports
5. Suggest improvements

## 📄 License

This project is intended for educational and research purposes. Please ensure compliance with:

- Telegram Terms of Service
- Ethiopian data protection laws
- Channel-specific terms and conditions
- Academic and commercial use guidelines

---

**Happy Scraping! 🚀**

_Remember to use this tool responsibly and ethically. Always respect the privacy and terms of service of the channels you're scraping._
