#!/usr/bin/env python3
"""
Enhanced Telegram Scraper for Ethiopian E-commerce Channels

This is an updated version of the original scraper with:
- Support for multiple Ethiopian channels
- Better rate limiting
- Improved error handling
- Structured data storage in /data folder
"""

import sys
import asyncio
from pathlib import Path

# Add project root to path for imports
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.data_ingestion import TelegramDataIngestion

# Ethiopian E-commerce Channels (including the original and new ones)
CHANNELS = [
    '@sinayelj',
    '@Shewabrand', 
    '@helloomarketethiopia',
    '@modernshoppingcenter',
    '@qnashcom',
    '@Shageronlinestore'  # Original channel
]

async def main():
    """Main function to scrape all Ethiopian e-commerce channels."""
    
    print("=== Ethiopian E-commerce Telegram Scraper ===")
    print(f"Channels to scrape: {len(CHANNELS)}")
    for i, channel in enumerate(CHANNELS, 1):
        print(f"  {i}. {channel}")
    print()
    
    # Initialize the data ingestion system
    ingestion_system = TelegramDataIngestion()
    
    try:
        # Scrape all channels with rate limiting
        print("Starting data scraping...")
        all_data = await ingestion_system.scrape_multiple_channels(
            CHANNELS,
            limit_per_channel=1000  # Adjust based on your needs
        )
        
        # Print summary
        total_messages = sum(len(data) for data in all_data.values())
        print(f"\n=== SCRAPING COMPLETED ===")
        print(f"Total messages scraped: {total_messages}")
        print(f"Channels successfully scraped: {len(all_data)}")
        
        for channel, data in all_data.items():
            print(f"  {channel}: {len(data)} messages")
        
        # Data is automatically saved in data/raw/ directory
        print(f"\nData saved to: data/raw/")
        print("Combined dataset also created with all channels' data")
        
    except Exception as e:
        print(f"Error during scraping: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    # Run the async main function
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
