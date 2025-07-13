#!/usr/bin/env python3
"""
Medical Telegram Scraper for Ethiopian Healthcare Channels

This script scrapes data from Ethiopian medical and healthcare Telegram channels
including pharmacies, cosmetics, and medical equipment providers.

Usage:
    python scripts/medical_telegram_scraper.py
    
Requirements:
    - Telegram API credentials in .env file
    - Required packages in requirements.txt
"""

import sys
import asyncio
import time
from pathlib import Path
from datetime import datetime

# Add project root to path for imports
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.data_ingestion import TelegramDataIngestion

# Ethiopian Medical and Healthcare Channels
MEDICAL_CHANNELS = [
    # Specified channels
    '@lobelia4cosmetics',      # Lobelia Cosmetics
    '@tikvahpharma',          # Tikvah Pharma
    
    # Additional Ethiopian medical channels (to be discovered from et.tgstat.com)
    '@medicalethiopia',        # Medical Ethiopia (if available)
    '@pharmacyethiopia',       # Pharmacy Ethiopia
    
    # Add more channels as discovered from https://et.tgstat.com/medicine
]

# Configuration
SCRAPER_CONFIG = {
    'limit_per_channel': 1000,     # Messages per channel
    'days_back': 30,               # Days to scrape back
    'rate_limit_delay': 2,         # Seconds between requests
    'max_retries': 4,              # Maximum retry attempts
}

def print_banner():
    """Print application banner."""
    print("=" * 60)
    print("🏥 ETHIOPIAN MEDICAL TELEGRAM SCRAPER 🏥")
    print("=" * 60)
    print("Scraping medical and healthcare channels from Ethiopia")
    print(f"Target channels: {len(MEDICAL_CHANNELS)}")
    print(f"Messages per channel: {SCRAPER_CONFIG['limit_per_channel']}")
    print(f"Days back: {SCRAPER_CONFIG['days_back']}")
    print("=" * 60)

def print_channels_info():
    """Print information about channels to be scraped."""
    print("\n📋 CHANNELS TO SCRAPE:")
    for i, channel in enumerate(MEDICAL_CHANNELS, 1):
        print(f"  {i:2d}. {channel}")
    print()

def print_progress(current: int, total: int, channel: str):
    """Print scraping progress."""
    progress = (current / total) * 100
    print(f"📊 Progress: {current}/{total} ({progress:.1f}%) - Current: {channel}")

async def validate_channels(ingestion_system):
    """
    Validate that channels exist and are accessible.
    
    Args:
        ingestion_system: TelegramDataIngestion instance
        
    Returns:
        List of valid channels
    """
    print("🔍 Validating channels...")
    valid_channels = []
    
    if not await ingestion_system.authenticate():
        print("❌ Authentication failed")
        return []
    
    for channel in MEDICAL_CHANNELS:
        try:
            await ingestion_system.client.get_entity(channel)
            valid_channels.append(channel)
            print(f"  ✅ {channel} - Valid")
        except Exception as e:
            print(f"  ❌ {channel} - Invalid: {e}")
    
    await ingestion_system.client.disconnect()
    return valid_channels

async def main():
    """Main scraping function."""
    start_time = time.time()
    
    # Print banner and info
    print_banner()
    print_channels_info()
    
    # Initialize the data ingestion system
    print("🚀 Initializing Telegram Data Ingestion System...")
    try:
        ingestion_system = TelegramDataIngestion(base_data_dir="data")
        print("✅ Data ingestion system initialized successfully")
    except Exception as e:
        print(f"❌ Failed to initialize data ingestion system: {e}")
        print("💡 Make sure your .env file contains valid Telegram API credentials")
        return 1
    
    # Validate channels
    valid_channels = await validate_channels(ingestion_system)
    if not valid_channels:
        print("❌ No valid channels found. Please check channel names and your access.")
        return 1
    
    print(f"✅ Found {len(valid_channels)} valid channels out of {len(MEDICAL_CHANNELS)}")
    
    # Start scraping
    print("\n🔄 Starting data scraping...")
    try:
        all_data = await ingestion_system.scrape_multiple_channels(
            valid_channels,
            limit_per_channel=SCRAPER_CONFIG['limit_per_channel'],
            days_back=SCRAPER_CONFIG['days_back']
        )
        
        # Calculate and print results
        total_messages = sum(len(data) for data in all_data.values())
        elapsed_time = time.time() - start_time
        
        print("\n" + "=" * 60)
        print("✅ SCRAPING COMPLETED SUCCESSFULLY!")
        print("=" * 60)
        print(f"📊 Total messages scraped: {total_messages:,}")
        print(f"📂 Channels processed: {len(all_data)}")
        print(f"⏱️  Time elapsed: {elapsed_time:.1f} seconds")
        print(f"📁 Data saved to: data/raw/telegram_messages/{datetime.now().strftime('%Y-%m-%d')}/")
        print()
        
        # Print channel-wise statistics
        print("📈 CHANNEL STATISTICS:")
        for channel, data in all_data.items():
            print(f"  {channel}: {len(data):,} messages")
        
        # Print data structure info
        print("\n📁 DATA STRUCTURE:")
        print("  data/")
        print("  ├── raw/")
        print("  │   └── telegram_messages/")
        print(f"  │       └── {datetime.now().strftime('%Y-%m-%d')}/")
        print("  │           ├── individual_channel_files.json")
        print("  │           ├── combined_medical_channels.json")
        print("  │           └── scrape_metadata.json")
        print("  ├── images/")
        print("  │   └── [channel_name]/")
        print("  │       └── [downloaded_images]")
        print("  └── logs/")
        print("      └── telegram_scraper_YYYYMMDD.log")
        
        # Print next steps
        print("\n🎯 NEXT STEPS:")
        print("  1. Review scraped data in data/raw/telegram_messages/")
        print("  2. Check downloaded images in data/images/")
        print("  3. Review logs in data/logs/")
        print("  4. Run the analysis notebook: notebooks/medical_data_analysis.ipynb")
        
        return 0
        
    except Exception as e:
        print(f"\n❌ Error during scraping: {e}")
        print("💡 Check the logs in data/logs/ for more details")
        return 1

def print_usage_info():
    """Print usage information."""
    print("\n" + "=" * 60)
    print("📖 USAGE INFORMATION")
    print("=" * 60)
    print("Before running this script, ensure you have:")
    print("1. Created a .env file with your Telegram API credentials")
    print("2. Installed required packages: pip install -r requirements.txt")
    print("3. Have access to the target Telegram channels")
    print()
    print("To run the scraper:")
    print("  python scripts/medical_telegram_scraper.py")
    print()
    print("Configuration can be modified in the SCRAPER_CONFIG dictionary")
    print("Channel list can be updated in the MEDICAL_CHANNELS list")
    print("=" * 60)

if __name__ == "__main__":
    try:
        # Check if help is requested
        if len(sys.argv) > 1 and sys.argv[1] in ['-h', '--help', 'help']:
            print_usage_info()
            sys.exit(0)
        
        # Run the scraper
        exit_code = asyncio.run(main())
        sys.exit(exit_code)
        
    except KeyboardInterrupt:
        print("\n⚠️  Scraping interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n💥 Unexpected error: {e}")
        sys.exit(1) 