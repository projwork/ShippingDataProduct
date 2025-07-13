#!/usr/bin/env python3
"""
Telegram Data Ingestion Module for Ethiopian Medical Businesses

This module provides functionality to scrape data from Telegram channels
focused on Ethiopian medical businesses including pharmacies, cosmetics,
and healthcare providers.
"""

import asyncio
import json
import logging
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Any
import aiohttp
import aiofiles
from telethon import TelegramClient, events
from telethon.errors import SessionPasswordNeededError, FloodWaitError
from telethon.tl.types import MessageMediaPhoto, MessageMediaDocument
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class TelegramDataIngestion:
    """
    A comprehensive Telegram data ingestion system for Ethiopian medical businesses.
    
    Features:
    - Multi-channel scraping with rate limiting
    - Image and media collection
    - Structured data storage with partitioning
    - Comprehensive logging
    - Error handling and recovery
    """
    
    def __init__(self, base_data_dir: str = "data"):
        """
        Initialize the Telegram data ingestion system.
        
        Args:
            base_data_dir: Base directory for data storage
        """
        self.base_data_dir = Path(base_data_dir)
        self.raw_data_dir = self.base_data_dir / "raw"
        self.images_dir = self.base_data_dir / "images"
        
        # Create directories
        self.raw_data_dir.mkdir(parents=True, exist_ok=True)
        self.images_dir.mkdir(parents=True, exist_ok=True)
        
        # Setup logging
        self._setup_logging()
        
        # Initialize Telegram client
        self.client = None
        self._initialize_client()
        
        # Rate limiting settings
        self.rate_limit_delay = 2  # seconds between requests
        self.max_retries = 3
        
    def _setup_logging(self):
        """Setup comprehensive logging system."""
        log_dir = self.base_data_dir / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        
        # Create formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        
        # Setup file handler
        file_handler = logging.FileHandler(
            log_dir / f"telegram_scraper_{datetime.now().strftime('%Y%m%d')}.log"
        )
        file_handler.setFormatter(formatter)
        
        # Setup console handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        
        # Configure logger
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)
        
    def _initialize_client(self):
        """Initialize Telegram client with API credentials."""
        api_id = os.getenv('TELEGRAM_API_ID')
        api_hash = os.getenv('TELEGRAM_API_HASH')
        phone_number = os.getenv('TELEGRAM_PHONE_NUMBER')
        
        if not all([api_id, api_hash, phone_number]):
            raise ValueError(
                "Missing Telegram API credentials. Please check your .env file."
            )
        
        self.client = TelegramClient(
            'medical_scraper_session',
            int(api_id),
            api_hash
        )
        
        self.phone_number = phone_number
        
    async def authenticate(self):
        """Authenticate with Telegram API."""
        try:
            await self.client.connect()
            
            if not await self.client.is_user_authorized():
                await self.client.send_code_request(self.phone_number)
                code = input('Enter the verification code: ')
                await self.client.sign_in(self.phone_number, code)
                
            self.logger.info("Successfully authenticated with Telegram")
            return True
            
        except SessionPasswordNeededError:
            password = input('Enter your 2FA password: ')
            await self.client.sign_in(password=password)
            self.logger.info("Successfully authenticated with 2FA")
            return True
            
        except Exception as e:
            self.logger.error(f"Authentication failed: {e}")
            return False
    
    async def scrape_channel(self, channel_username: str, limit: int = 1000, 
                           days_back: int = 30) -> List[Dict[str, Any]]:
        """
        Scrape messages from a specific channel.
        
        Args:
            channel_username: Telegram channel username (with or without @)
            limit: Maximum number of messages to scrape
            days_back: Number of days to go back in history
            
        Returns:
            List of message dictionaries
        """
        messages = []
        
        try:
            # Clean channel username
            if not channel_username.startswith('@'):
                channel_username = f'@{channel_username}'
            
            self.logger.info(f"Starting to scrape channel: {channel_username}")
            
            # Get channel entity
            channel = await self.client.get_entity(channel_username)
            
            # Calculate date range (timezone-aware)
            end_date = datetime.now(timezone.utc)
            start_date = end_date - timedelta(days=days_back)
            
            # Scrape messages
            async for message in self.client.iter_messages(
                channel, 
                limit=limit,
                offset_date=end_date,
                reverse=False
            ):
                # Skip messages older than start_date
                if message.date < start_date:
                    break
                
                # Process message
                message_data = await self._process_message(message, channel_username)
                if message_data:
                    messages.append(message_data)
                
                # Rate limiting
                await asyncio.sleep(self.rate_limit_delay)
            
            self.logger.info(f"Scraped {len(messages)} messages from {channel_username}")
            return messages
            
        except FloodWaitError as e:
            self.logger.warning(f"Rate limited for {e.seconds} seconds")
            await asyncio.sleep(e.seconds)
            return await self.scrape_channel(channel_username, limit, days_back)
            
        except Exception as e:
            self.logger.error(f"Error scraping channel {channel_username}: {e}")
            return []
    
    async def _process_message(self, message, channel_username: str) -> Optional[Dict[str, Any]]:
        """
        Process a single message and extract relevant data.
        
        Args:
            message: Telethon message object
            channel_username: Channel username for context
            
        Returns:
            Processed message dictionary or None
        """
        try:
            message_data = {
                'id': message.id,
                'channel': channel_username,
                'date': message.date.isoformat(),
                'text': message.text or '',
                'views': getattr(message, 'views', 0),
                'forwards': getattr(message, 'forwards', 0),
                'replies': message.replies.replies if hasattr(message, 'replies') and message.replies and hasattr(message.replies, 'replies') else 0,
                'media_type': None,
                'media_path': None,
                'raw_data': {
                    'message_id': message.id,
                    'from_id': str(message.from_id) if message.from_id else None,
                    'reply_to_msg_id': message.reply_to_msg_id,
                    'grouped_id': message.grouped_id,
                }
            }
            
            # Handle media
            if message.media:
                media_info = await self._download_media(message, channel_username)
                if media_info:
                    message_data['media_type'] = media_info['type']
                    message_data['media_path'] = media_info['path']
                    message_data['media_size'] = media_info.get('size', 0)
            
            return message_data
            
        except Exception as e:
            self.logger.error(f"Error processing message {message.id}: {e}")
            return None
    
    async def _download_media(self, message, channel_username: str) -> Optional[Dict[str, Any]]:
        """
        Download media from a message.
        
        Args:
            message: Telethon message object
            channel_username: Channel username for organization
            
        Returns:
            Media information dictionary or None
        """
        try:
            # Create channel-specific media directory
            channel_media_dir = self.images_dir / channel_username.replace('@', '')
            channel_media_dir.mkdir(parents=True, exist_ok=True)
            
            # Determine media type and extension
            media_type = None
            file_ext = None
            
            if isinstance(message.media, MessageMediaPhoto):
                media_type = 'photo'
                file_ext = '.jpg'
            elif isinstance(message.media, MessageMediaDocument):
                if message.media.document.mime_type.startswith('image/'):
                    media_type = 'image'
                    file_ext = '.' + message.media.document.mime_type.split('/')[1]
                elif message.media.document.mime_type.startswith('video/'):
                    media_type = 'video'
                    file_ext = '.' + message.media.document.mime_type.split('/')[1]
                else:
                    media_type = 'document'
                    file_ext = '.bin'
            
            if not media_type:
                return None
            
            # Generate filename
            filename = f"{message.id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}{file_ext}"
            file_path = channel_media_dir / filename
            
            # Download media
            await self.client.download_media(message.media, file_path)
            
            # Get file size
            file_size = file_path.stat().st_size if file_path.exists() else 0
            
            return {
                'type': media_type,
                'path': str(file_path.relative_to(self.base_data_dir)),
                'size': file_size,
                'filename': filename
            }
            
        except Exception as e:
            self.logger.error(f"Error downloading media for message {message.id}: {e}")
            return None
    
    async def scrape_multiple_channels(self, channels: List[str], 
                                     limit_per_channel: int = 1000,
                                     days_back: int = 30) -> Dict[str, List[Dict[str, Any]]]:
        """
        Scrape multiple channels with rate limiting.
        
        Args:
            channels: List of channel usernames
            limit_per_channel: Maximum messages per channel
            days_back: Number of days to go back
            
        Returns:
            Dictionary mapping channel names to their scraped data
        """
        if not await self.authenticate():
            raise Exception("Failed to authenticate with Telegram")
        
        all_data = {}
        
        for channel in channels:
            try:
                self.logger.info(f"Processing channel: {channel}")
                
                # Scrape channel data
                channel_data = await self.scrape_channel(channel, limit_per_channel, days_back)
                
                if channel_data:
                    all_data[channel] = channel_data
                    
                    # Save individual channel data
                    await self._save_channel_data(channel, channel_data)
                    
                    self.logger.info(f"Successfully scraped {len(channel_data)} messages from {channel}")
                else:
                    self.logger.warning(f"No data scraped from {channel}")
                
                # Rate limiting between channels
                await asyncio.sleep(self.rate_limit_delay * 2)
                
            except Exception as e:
                self.logger.error(f"Error processing channel {channel}: {e}")
                continue
        
        # Save combined dataset
        if all_data:
            await self._save_combined_data(all_data)
        
        await self.client.disconnect()
        return all_data
    
    async def _save_channel_data(self, channel: str, data: List[Dict[str, Any]]):
        """
        Save channel data with date partitioning.
        
        Args:
            channel: Channel username
            data: List of message dictionaries
        """
        try:
            # Create date-partitioned directory structure
            today = datetime.now().strftime('%Y-%m-%d')
            channel_clean = channel.replace('@', '')
            
            channel_dir = self.raw_data_dir / "telegram_messages" / today
            channel_dir.mkdir(parents=True, exist_ok=True)
            
            # Save channel data
            file_path = channel_dir / f"{channel_clean}.json"
            
            async with aiofiles.open(file_path, 'w', encoding='utf-8') as f:
                await f.write(json.dumps(data, ensure_ascii=False, indent=2))
            
            self.logger.info(f"Saved {len(data)} messages to {file_path}")
            
        except Exception as e:
            self.logger.error(f"Error saving data for channel {channel}: {e}")
    
    async def _save_combined_data(self, all_data: Dict[str, List[Dict[str, Any]]]):
        """
        Save combined dataset from all channels.
        
        Args:
            all_data: Dictionary of all scraped data
        """
        try:
            today = datetime.now().strftime('%Y-%m-%d')
            combined_dir = self.raw_data_dir / "telegram_messages" / today
            combined_dir.mkdir(parents=True, exist_ok=True)
            
            # Flatten all data into a single list
            combined_data = []
            for channel, messages in all_data.items():
                combined_data.extend(messages)
            
            # Save combined data
            file_path = combined_dir / "combined_medical_channels.json"
            
            async with aiofiles.open(file_path, 'w', encoding='utf-8') as f:
                await f.write(json.dumps(combined_data, ensure_ascii=False, indent=2))
            
            self.logger.info(f"Saved combined dataset with {len(combined_data)} messages to {file_path}")
            
            # Save metadata
            metadata = {
                'scrape_date': today,
                'total_messages': len(combined_data),
                'channels_scraped': list(all_data.keys()),
                'channel_counts': {channel: len(messages) for channel, messages in all_data.items()}
            }
            
            metadata_path = combined_dir / "scrape_metadata.json"
            async with aiofiles.open(metadata_path, 'w', encoding='utf-8') as f:
                await f.write(json.dumps(metadata, ensure_ascii=False, indent=2))
            
        except Exception as e:
            self.logger.error(f"Error saving combined data: {e}") 