#!/usr/bin/env python3
"""
Object Detection Module for Ethiopian Medical Telegram Images

This module provides functionality to analyze images using YOLOv8 object detection
for Ethiopian medical and healthcare Telegram channels.

Features:
- YOLOv8 model loading and inference
- Batch image processing
- Medical-specific object classification
- Confidence scoring and filtering
- Results storage and management
"""

import os
import logging
import json
import time
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime

try:
    import cv2
    import numpy as np
    from PIL import Image
    from ultralytics import YOLO
    import torch
except ImportError as e:
    print(f"❌ Missing required package for object detection: {e}")
    print("💡 Install missing packages with: pip install ultralytics opencv-python torch")
    raise ImportError(f"Object detection dependencies missing: {e}")

class ObjectDetectionEngine:
    """
    YOLOv8-based object detection engine for medical Telegram images.
    
    Features:
    - Pre-trained YOLOv8 model inference
    - Medical image classification
    - Batch processing capabilities
    - Confidence filtering
    - Results persistence
    """
    
    def __init__(self, 
                 model_name: str = 'yolov8n.pt',
                 confidence_threshold: float = 0.25,
                 device: str = 'auto',
                 base_data_dir: str = "data"):
        """
        Initialize the object detection engine.
        
        Args:
            model_name: YOLOv8 model variant (yolov8n.pt, yolov8s.pt, yolov8m.pt, yolov8l.pt, yolov8x.pt)
            confidence_threshold: Minimum confidence score for detections
            device: Device for inference ('auto', 'cpu', 'cuda')
            base_data_dir: Base directory for data storage
        """
        self.model_name = model_name
        self.confidence_threshold = confidence_threshold
        self.base_data_dir = Path(base_data_dir)
        
        # Setup directories
        self.images_dir = self.base_data_dir / "images"
        self.detections_dir = self.base_data_dir / "object_detections"
        self.detections_dir.mkdir(parents=True, exist_ok=True)
        
        # Setup logging
        self._setup_logging()
        
        # Initialize model
        self.model = None
        self.device = self._setup_device(device)
        self._load_model()
        
        # Medical-related object classes (COCO dataset classes relevant to medical/healthcare)
        self.medical_relevant_classes = {
            'person': 'medical_staff',
            'bottle': 'medical_bottle',
            'cup': 'medical_container',
            'bowl': 'medical_container',
            'book': 'medical_literature',
            'cell phone': 'communication_device',
            'laptop': 'medical_equipment',
            'mouse': 'computer_accessory',
            'keyboard': 'computer_accessory',
            'scissors': 'medical_instrument',
            'teddy bear': 'comfort_item',
            'hair drier': 'personal_care',
            'toothbrush': 'hygiene_product',
            'spoon': 'medical_utensil',
            'knife': 'medical_instrument',
            'fork': 'medical_utensil',
            'chair': 'medical_furniture',
            'couch': 'medical_furniture',
            'bed': 'medical_furniture',
            'dining table': 'medical_furniture',
            'toilet': 'sanitary_equipment',
            'tv': 'information_display',
            'clock': 'timing_device',
            'vase': 'decorative_medical',
            'backpack': 'medical_supply_bag',
            'handbag': 'personal_medical_bag',
            'tie': 'professional_attire',
            'suitcase': 'medical_supply_case'
        }
    
    def _setup_logging(self):
        """Setup logging configuration."""
        log_dir = self.base_data_dir / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        
        log_file = log_dir / f"object_detection_{datetime.now().strftime('%Y%m%d')}.log"
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )
        
        self.logger = logging.getLogger(__name__)
    
    def _setup_device(self, device: str) -> str:
        """Setup computation device."""
        if device == 'auto':
            if torch.cuda.is_available():
                device = 'cuda'
                self.logger.info("CUDA available - using GPU")
            else:
                device = 'cpu'
                self.logger.info("CUDA not available - using CPU")
        
        self.logger.info(f"Using device: {device}")
        return device
    
    def _load_model(self):
        """Load YOLOv8 model."""
        try:
            self.logger.info(f"Loading YOLOv8 model: {self.model_name}")
            self.model = YOLO(self.model_name)
            
            # Move model to specified device
            if self.device == 'cuda' and torch.cuda.is_available():
                self.model.to('cuda')
            
            self.logger.info(f"Model loaded successfully on {self.device}")
            
            # Get model info
            if hasattr(self.model, 'names'):
                self.class_names = self.model.names
                self.logger.info(f"Model can detect {len(self.class_names)} object classes")
            
        except Exception as e:
            self.logger.error(f"Error loading model {self.model_name}: {e}")
            raise
    
    def detect_objects_in_image(self, image_path: Path) -> List[Dict[str, Any]]:
        """
        Detect objects in a single image.
        
        Args:
            image_path: Path to the image file
            
        Returns:
            List of detection dictionaries
        """
        try:
            # Validate image file
            if not image_path.exists():
                self.logger.error(f"Image file not found: {image_path}")
                return []
            
            # Run inference
            results = self.model(str(image_path), conf=self.confidence_threshold)
            
            detections = []
            
            for result in results:
                # Extract detection data
                boxes = result.boxes
                
                if boxes is not None:
                    for i, box in enumerate(boxes):
                        # Get detection info
                        confidence = float(box.conf[0])
                        class_id = int(box.cls[0])
                        class_name = self.class_names.get(class_id, f'unknown_{class_id}')
                        
                        # Map to medical-relevant category
                        medical_category = self.medical_relevant_classes.get(class_name, 'general_object')
                        
                        # Get bounding box coordinates
                        x1, y1, x2, y2 = box.xyxy[0].tolist()
                        
                        detection = {
                            'detection_id': f"{image_path.stem}_{i}_{int(datetime.now().timestamp()*1000)}",
                            'class_id': class_id,
                            'class_name': class_name,
                            'medical_category': medical_category,
                            'confidence_score': confidence,
                            'bounding_box': {
                                'x1': x1, 'y1': y1, 'x2': x2, 'y2': y2,
                                'width': x2 - x1, 'height': y2 - y1
                            },
                            'detection_timestamp': datetime.now().isoformat()
                        }
                        
                        detections.append(detection)
            
            self.logger.info(f"Detected {len(detections)} objects in {image_path.name}")
            return detections
            
        except Exception as e:
            self.logger.error(f"Error detecting objects in {image_path}: {e}")
            return []
    
    def process_channel_images(self, channel_name: str) -> Dict[str, Any]:
        """
        Process all images for a specific channel.
        
        Args:
            channel_name: Name of the Telegram channel
            
        Returns:
            Processing results summary
        """
        channel_clean = channel_name.replace('@', '')
        channel_images_dir = self.images_dir / channel_clean
        
        if not channel_images_dir.exists():
            self.logger.warning(f"Channel images directory not found: {channel_images_dir}")
            return {'status': 'error', 'message': 'Channel directory not found'}
        
        # Find image files
        image_extensions = ['.jpg', '.jpeg', '.png', '.bmp', '.tiff']
        image_files = []
        
        for ext in image_extensions:
            image_files.extend(list(channel_images_dir.glob(f"*{ext}")))
            image_files.extend(list(channel_images_dir.glob(f"*{ext.upper()}")))
        
        if not image_files:
            self.logger.warning(f"No image files found in {channel_images_dir}")
            return {'status': 'warning', 'message': 'No images found'}
        
        self.logger.info(f"Processing {len(image_files)} images for channel {channel_name}")
        
        # Process images
        all_detections = []
        processed_count = 0
        error_count = 0
        
        for image_path in image_files:
            try:
                # Extract message ID from filename (format: messageId_timestamp.ext)
                message_id = self._extract_message_id(image_path.name)
                
                # Detect objects
                detections = self.detect_objects_in_image(image_path)
                
                # Add metadata to each detection
                for detection in detections:
                    detection.update({
                        'message_id': message_id,
                        'channel': channel_name,
                        'image_file': str(image_path.relative_to(self.base_data_dir)),
                        'image_filename': image_path.name
                    })
                
                all_detections.extend(detections)
                processed_count += 1
                
                # Progress logging
                if processed_count % 10 == 0:
                    self.logger.info(f"Processed {processed_count}/{len(image_files)} images")
                
            except Exception as e:
                self.logger.error(f"Error processing {image_path}: {e}")
                error_count += 1
                continue
        
        # Save results
        results_summary = {
            'channel': channel_name,
            'total_images': len(image_files),
            'processed_images': processed_count,
            'error_images': error_count,
            'total_detections': len(all_detections),
            'processing_timestamp': datetime.now().isoformat(),
            'detections': all_detections
        }
        
        # Save channel results
        self._save_channel_detections(channel_clean, results_summary)
        
        self.logger.info(f"Channel {channel_name} processing complete: "
                        f"{processed_count} images, {len(all_detections)} detections")
        
        return results_summary
    
    def process_all_channels(self) -> Dict[str, Any]:
        """
        Process images for all channels in the images directory.
        
        Returns:
            Combined processing results
        """
        if not self.images_dir.exists():
            self.logger.error(f"Images directory not found: {self.images_dir}")
            return {'status': 'error', 'message': 'Images directory not found'}
        
        # Find all channel directories
        channel_dirs = [d for d in self.images_dir.iterdir() if d.is_dir()]
        
        if not channel_dirs:
            self.logger.warning("No channel directories found")
            return {'status': 'warning', 'message': 'No channels found'}
        
        self.logger.info(f"Processing {len(channel_dirs)} channels")
        
        all_results = {}
        total_detections = 0
        
        for channel_dir in channel_dirs:
            channel_name = f"@{channel_dir.name}"
            
            try:
                channel_results = self.process_channel_images(channel_name)
                all_results[channel_name] = channel_results
                
                if 'total_detections' in channel_results:
                    total_detections += channel_results['total_detections']
                    
            except Exception as e:
                self.logger.error(f"Error processing channel {channel_name}: {e}")
                all_results[channel_name] = {
                    'status': 'error',
                    'message': str(e)
                }
        
        # Save combined results
        combined_results = {
            'processing_timestamp': datetime.now().isoformat(),
            'total_channels': len(channel_dirs),
            'total_detections': total_detections,
            'channel_results': all_results
        }
        
        self._save_combined_detections(combined_results)
        
        self.logger.info(f"All channels processing complete: "
                        f"{len(channel_dirs)} channels, {total_detections} total detections")
        
        return combined_results
    
    def _extract_message_id(self, filename: str) -> Optional[int]:
        """
        Extract message ID from image filename.
        
        Args:
            filename: Image filename (format: messageId_timestamp.ext)
            
        Returns:
            Message ID or None if not extractable
        """
        try:
            # Expected format: messageId_timestamp.ext
            parts = filename.split('_')
            if len(parts) >= 2:
                return int(parts[0])
            return None
        except (ValueError, IndexError):
            self.logger.warning(f"Could not extract message ID from filename: {filename}")
            return None
    
    def _save_channel_detections(self, channel_name: str, results: Dict[str, Any]):
        """Save detection results for a specific channel."""
        try:
            today = datetime.now().strftime('%Y-%m-%d')
            results_dir = self.detections_dir / today
            results_dir.mkdir(parents=True, exist_ok=True)
            
            filename = f"{channel_name}_detections.json"
            filepath = results_dir / filename
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(results, f, ensure_ascii=False, indent=2)
            
            self.logger.info(f"Saved detection results to {filepath}")
            
        except Exception as e:
            self.logger.error(f"Error saving channel detections: {e}")
    
    def _save_combined_detections(self, results: Dict[str, Any]):
        """Save combined detection results from all channels."""
        try:
            today = datetime.now().strftime('%Y-%m-%d')
            results_dir = self.detections_dir / today
            results_dir.mkdir(parents=True, exist_ok=True)
            
            filepath = results_dir / "combined_detections.json"
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(results, f, ensure_ascii=False, indent=2)
            
            self.logger.info(f"Saved combined detection results to {filepath}")
            
        except Exception as e:
            self.logger.error(f"Error saving combined detections: {e}")
    
    def get_detection_statistics(self, channel_name: Optional[str] = None) -> Dict[str, Any]:
        """
        Get statistics about object detections.
        
        Args:
            channel_name: Specific channel name, or None for all channels
            
        Returns:
            Detection statistics
        """
        try:
            # Find most recent detection results
            if not self.detections_dir.exists():
                return {'status': 'error', 'message': 'No detection results found'}
            
            date_dirs = [d for d in self.detections_dir.glob("*") if d.is_dir()]
            if not date_dirs:
                return {'status': 'error', 'message': 'No detection date directories found'}
            
            latest_date_dir = max(date_dirs, key=lambda x: x.name)
            
            if channel_name:
                # Channel-specific statistics
                channel_clean = channel_name.replace('@', '')
                channel_file = latest_date_dir / f"{channel_clean}_detections.json"
                
                if not channel_file.exists():
                    return {'status': 'error', 'message': f'No results for channel {channel_name}'}
                
                with open(channel_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                return self._calculate_channel_stats(data)
            
            else:
                # Combined statistics
                combined_file = latest_date_dir / "combined_detections.json"
                
                if not combined_file.exists():
                    return {'status': 'error', 'message': 'No combined results found'}
                
                with open(combined_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                return self._calculate_combined_stats(data)
                
        except Exception as e:
            self.logger.error(f"Error getting detection statistics: {e}")
            return {'status': 'error', 'message': str(e)}
    
    def _calculate_channel_stats(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate statistics for a single channel."""
        detections = data.get('detections', [])
        
        if not detections:
            return {'status': 'warning', 'message': 'No detections found'}
        
        # Object class distribution
        class_counts = {}
        medical_category_counts = {}
        confidence_scores = []
        
        for detection in detections:
            class_name = detection.get('class_name', 'unknown')
            medical_category = detection.get('medical_category', 'unknown')
            confidence = detection.get('confidence_score', 0)
            
            class_counts[class_name] = class_counts.get(class_name, 0) + 1
            medical_category_counts[medical_category] = medical_category_counts.get(medical_category, 0) + 1
            confidence_scores.append(confidence)
        
        stats = {
            'channel': data.get('channel'),
            'total_images': data.get('processed_images', 0),
            'total_detections': len(detections),
            'avg_detections_per_image': len(detections) / max(data.get('processed_images', 1), 1),
            'avg_confidence': sum(confidence_scores) / len(confidence_scores) if confidence_scores else 0,
            'min_confidence': min(confidence_scores) if confidence_scores else 0,
            'max_confidence': max(confidence_scores) if confidence_scores else 0,
            'object_classes': dict(sorted(class_counts.items(), key=lambda x: x[1], reverse=True)),
            'medical_categories': dict(sorted(medical_category_counts.items(), key=lambda x: x[1], reverse=True)),
            'processing_timestamp': data.get('processing_timestamp')
        }
        
        return stats
    
    def _calculate_combined_stats(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate combined statistics across all channels."""
        channel_results = data.get('channel_results', {})
        
        total_images = 0
        total_detections = 0
        all_class_counts = {}
        all_medical_category_counts = {}
        all_confidence_scores = []
        
        for channel, results in channel_results.items():
            if 'detections' in results:
                detections = results['detections']
                total_images += results.get('processed_images', 0)
                total_detections += len(detections)
                
                for detection in detections:
                    class_name = detection.get('class_name', 'unknown')
                    medical_category = detection.get('medical_category', 'unknown')
                    confidence = detection.get('confidence_score', 0)
                    
                    all_class_counts[class_name] = all_class_counts.get(class_name, 0) + 1
                    all_medical_category_counts[medical_category] = all_medical_category_counts.get(medical_category, 0) + 1
                    all_confidence_scores.append(confidence)
        
        stats = {
            'total_channels': data.get('total_channels', 0),
            'total_images': total_images,
            'total_detections': total_detections,
            'avg_detections_per_image': total_detections / max(total_images, 1),
            'avg_confidence': sum(all_confidence_scores) / len(all_confidence_scores) if all_confidence_scores else 0,
            'min_confidence': min(all_confidence_scores) if all_confidence_scores else 0,
            'max_confidence': max(all_confidence_scores) if all_confidence_scores else 0,
            'object_classes': dict(sorted(all_class_counts.items(), key=lambda x: x[1], reverse=True)),
            'medical_categories': dict(sorted(all_medical_category_counts.items(), key=lambda x: x[1], reverse=True)),
            'processing_timestamp': data.get('processing_timestamp')
        }
        
        return stats 