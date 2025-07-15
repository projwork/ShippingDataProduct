#!/usr/bin/env python3
"""
Object Detection Script for Ethiopian Medical Telegram Images

This script processes images from scraped Telegram channels using YOLOv8
object detection and stores the results for integration with the data warehouse.

Usage:
    python scripts/detect_objects.py [--channel CHANNEL_NAME] [--model MODEL_NAME]
    
Examples:
    python scripts/detect_objects.py  # Process all channels
    python scripts/detect_objects.py --channel @lobelia4cosmetics  # Process specific channel
    python scripts/detect_objects.py --model yolov8m.pt  # Use medium model
"""

import sys
import argparse
import time
from pathlib import Path
from datetime import datetime

# Add project root to path for imports
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.object_detection import ObjectDetectionEngine

def print_banner():
    """Print application banner."""
    print("=" * 60)
    print("🔍 YOLO OBJECT DETECTION FOR MEDICAL IMAGES 🔍")
    print("=" * 60)
    print("Analyzing medical images from Ethiopian Telegram channels")
    print("Using YOLOv8 for object detection and classification")
    print("=" * 60)

def print_model_info():
    """Print information about available YOLO models."""
    print("\n📋 AVAILABLE YOLO MODELS:")
    models = {
        'yolov8n.pt': 'Nano - Fastest, lowest accuracy',
        'yolov8s.pt': 'Small - Fast, moderate accuracy',
        'yolov8m.pt': 'Medium - Balanced speed/accuracy',
        'yolov8l.pt': 'Large - Slower, high accuracy',
        'yolov8x.pt': 'Extra Large - Slowest, highest accuracy'
    }
    
    for model, description in models.items():
        print(f"  • {model}: {description}")
    print()

def print_progress(current: int, total: int, item: str):
    """Print processing progress."""
    progress = (current / total) * 100
    print(f"📊 Progress: {current}/{total} ({progress:.1f}%) - Current: {item}")

def print_detection_summary(results: dict):
    """Print detection results summary."""
    print("\n" + "=" * 60)
    print("📈 DETECTION RESULTS SUMMARY")
    print("=" * 60)
    
    if 'channel_results' in results:
        # Combined results
        print(f"🏥 Total Channels: {results.get('total_channels', 0)}")
        print(f"📊 Total Detections: {results.get('total_detections', 0):,}")
        
        print("\n📋 CHANNEL BREAKDOWN:")
        for channel, channel_results in results.get('channel_results', {}).items():
            if 'total_detections' in channel_results:
                images = channel_results.get('processed_images', 0)
                detections = channel_results.get('total_detections', 0)
                print(f"  {channel}: {images} images, {detections} detections")
    
    else:
        # Single channel results
        channel = results.get('channel', 'Unknown')
        images = results.get('processed_images', 0)
        detections = results.get('total_detections', 0)
        errors = results.get('error_images', 0)
        
        print(f"🏥 Channel: {channel}")
        print(f"📷 Images Processed: {images}")
        print(f"❌ Images with Errors: {errors}")
        print(f"📊 Total Detections: {detections}")
        
        if images > 0:
            avg_detections = detections / images
            print(f"📈 Average Detections per Image: {avg_detections:.2f}")

def print_statistics(stats: dict):
    """Print detailed detection statistics."""
    if stats.get('status') == 'error':
        print(f"❌ Error getting statistics: {stats.get('message')}")
        return
    
    print("\n" + "=" * 60)
    print("📊 DETECTION STATISTICS")
    print("=" * 60)
    
    print(f"📷 Total Images: {stats.get('total_images', 0):,}")
    print(f"🔍 Total Detections: {stats.get('total_detections', 0):,}")
    print(f"📈 Avg Detections/Image: {stats.get('avg_detections_per_image', 0):.2f}")
    print(f"🎯 Avg Confidence: {stats.get('avg_confidence', 0):.3f}")
    print(f"📉 Min Confidence: {stats.get('min_confidence', 0):.3f}")
    print(f"📈 Max Confidence: {stats.get('max_confidence', 0):.3f}")
    
    # Top object classes
    object_classes = stats.get('object_classes', {})
    if object_classes:
        print("\n🏷️ TOP DETECTED OBJECT CLASSES:")
        for i, (class_name, count) in enumerate(list(object_classes.items())[:10], 1):
            print(f"  {i:2d}. {class_name}: {count:,} detections")
    
    # Medical categories
    medical_categories = stats.get('medical_categories', {})
    if medical_categories:
        print("\n🏥 MEDICAL CATEGORY DISTRIBUTION:")
        for category, count in medical_categories.items():
            print(f"  • {category}: {count:,} detections")

def main():
    """Main object detection function."""
    parser = argparse.ArgumentParser(
        description="YOLOv8 Object Detection for Ethiopian Medical Telegram Images",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scripts/detect_objects.py
  python scripts/detect_objects.py --channel @lobelia4cosmetics
  python scripts/detect_objects.py --model yolov8m.pt --confidence 0.3
  python scripts/detect_objects.py --stats-only
        """
    )
    
    parser.add_argument(
        '--channel', '-c',
        help='Specific channel to process (e.g., @lobelia4cosmetics)'
    )
    
    parser.add_argument(
        '--model', '-m',
        default='yolov8n.pt',
        choices=['yolov8n.pt', 'yolov8s.pt', 'yolov8m.pt', 'yolov8l.pt', 'yolov8x.pt'],
        help='YOLOv8 model to use (default: yolov8n.pt)'
    )
    
    parser.add_argument(
        '--confidence', '-conf',
        type=float,
        default=0.25,
        help='Confidence threshold for detections (default: 0.25)'
    )
    
    parser.add_argument(
        '--device',
        choices=['auto', 'cpu', 'cuda'],
        default='auto',
        help='Device for inference (default: auto)'
    )
    
    parser.add_argument(
        '--stats-only',
        action='store_true',
        help='Only show statistics from previous runs'
    )
    
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose output'
    )
    
    args = parser.parse_args()
    
    # Print banner
    print_banner()
    
    if args.stats_only:
        print("📊 Loading detection statistics...")
        
        try:
            engine = ObjectDetectionEngine(
                model_name=args.model,
                confidence_threshold=args.confidence,
                device=args.device
            )
            
            stats = engine.get_detection_statistics(args.channel)
            print_statistics(stats)
            
        except Exception as e:
            print(f"❌ Error loading statistics: {e}")
            return 1
        
        return 0
    
    # Show model info if verbose
    if args.verbose:
        print_model_info()
    
    start_time = time.time()
    
    # Initialize detection engine
    print("\n🚀 Initializing Object Detection Engine...")
    try:
        engine = ObjectDetectionEngine(
            model_name=args.model,
            confidence_threshold=args.confidence,
            device=args.device
        )
        print(f"✅ Detection engine initialized with {args.model}")
        print(f"🎯 Confidence threshold: {args.confidence}")
        print(f"💻 Device: {args.device}")
        
    except Exception as e:
        print(f"❌ Failed to initialize detection engine: {e}")
        print("💡 Make sure ultralytics is installed: pip install ultralytics")
        return 1
    
    # Process images
    print(f"\n🔄 Starting object detection...")
    try:
        if args.channel:
            # Process specific channel
            print(f"📂 Processing channel: {args.channel}")
            results = engine.process_channel_images(args.channel)
        else:
            # Process all channels
            print("📂 Processing all channels...")
            results = engine.process_all_channels()
        
        # Calculate and print results
        elapsed_time = time.time() - start_time
        
        print_detection_summary(results)
        
        print(f"\n⏱️  Processing Time: {elapsed_time:.1f} seconds")
        print(f"📁 Results saved to: data/object_detections/{datetime.now().strftime('%Y-%m-%d')}/")
        
        # Show statistics
        if args.verbose:
            print("\n🔍 Generating detailed statistics...")
            stats = engine.get_detection_statistics(args.channel)
            print_statistics(stats)
        
        # Print next steps
        print("\n🎯 NEXT STEPS:")
        print("  1. Review detection results in data/object_detections/")
        print("  2. Load detection data into PostgreSQL with detection_data_loader.py")
        print("  3. Run dbt to create fct_image_detections fact table")
        print("  4. Analyze object detection results in the notebook")
        
        return 0
        
    except Exception as e:
        print(f"\n❌ Error during object detection: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        return 1

if __name__ == "__main__":
    try:
        exit_code = main()
        sys.exit(exit_code)
        
    except KeyboardInterrupt:
        print("\n⚠️  Object detection interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n💥 Unexpected error: {e}")
        sys.exit(1) 