#!/usr/bin/env python3
"""
Complete Object Detection Pipeline for Ethiopian Medical Telegram Data

This script runs the complete object detection pipeline:
1. Install/check dependencies
2. Run YOLOv8 object detection on images
3. Load detection results into PostgreSQL
4. Run dbt to create fact tables
5. Generate analysis report

Usage:
    python scripts/run_object_detection_pipeline.py [--model MODEL] [--confidence CONF]
    
Examples:
    python scripts/run_object_detection_pipeline.py
    python scripts/run_object_detection_pipeline.py --model yolov8m.pt --confidence 0.3
"""

import sys
import subprocess
import argparse
import time
from pathlib import Path
from datetime import datetime

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

def print_banner():
    """Print pipeline banner."""
    print("=" * 70)
    print("🔍 YOLO OBJECT DETECTION PIPELINE - ETHIOPIAN MEDICAL DATA 🔍")
    print("=" * 70)
    print("Complete pipeline for object detection and data warehouse integration")
    print("=" * 70)

def print_step(step_num: int, title: str):
    """Print step header."""
    print(f"\n{'='*10} STEP {step_num}: {title.upper()} {'='*10}")

def run_command(command: str, description: str) -> bool:
    """
    Run a shell command and return success status.
    
    Args:
        command: Command to execute
        description: Description for logging
        
    Returns:
        bool: Success status
    """
    print(f"🔄 {description}...")
    print(f"📝 Command: {command}")
    
    try:
        result = subprocess.run(command, shell=True, capture_output=True, text=True, encoding='utf-8', errors='replace')
        
        if result.returncode == 0:
            print(f"✅ {description} completed successfully")
            if result.stdout:
                # Limit output to avoid flooding the console
                output_lines = result.stdout.strip().split('\n')
                if len(output_lines) > 5:
                    print(f"📄 Output: {output_lines[0]}...{output_lines[-1]} ({len(output_lines)} lines)")
                else:
                    print(f"📄 Output: {result.stdout.strip()}")
            return True
        else:
            print(f"❌ {description} failed")
            if result.stderr:
                # Limit error output to avoid flooding
                error_lines = result.stderr.strip().split('\n')
                if len(error_lines) > 10:
                    print(f"📄 Error: {error_lines[0]}...{error_lines[-1]} ({len(error_lines)} lines)")
                else:
                    print(f"📄 Error: {result.stderr.strip()}")
            return False
            
    except Exception as e:
        print(f"❌ {description} failed with exception: {e}")
        return False

def check_dependencies() -> bool:
    """Check if required dependencies are installed."""
    print_step(1, "CHECKING DEPENDENCIES")
    
    dependencies = [
        ("python", "Python interpreter"),
        ("pip", "Python package manager"),
    ]
    
    all_ok = True
    
    for cmd, desc in dependencies:
        if not run_command(f"{cmd} --version", f"Checking {desc}"):
            all_ok = False
    
    # Check Python packages
    python_packages = [
        ("ultralytics", "ultralytics"),
        ("psycopg2", "psycopg2"),
        ("dbt-postgres", "dbt"),  # dbt-postgres is imported as 'dbt'
        ("pandas", "pandas"),
        ("numpy", "numpy")
    ]
    
    print("\n🐍 Checking Python Packages:")
    for package_name, import_name in python_packages:
        try:
            __import__(import_name)
            print(f"  ✅ {package_name}")
        except ImportError:
            print(f"  ❌ {package_name} (missing)")
            all_ok = False
    
    if not all_ok:
        print("\n💡 Install missing dependencies with:")
        print("   pip install -r requirements.txt")
        
    return all_ok

def run_object_detection(model_name: str, confidence: float) -> bool:
    """Run object detection on images."""
    print_step(2, "RUNNING OBJECT DETECTION")
    
    command = f"python scripts/detect_objects.py --model {model_name} --confidence {confidence} --verbose"
    return run_command(command, "Object detection analysis")

def load_detection_data() -> bool:
    """Load detection results into PostgreSQL."""
    print_step(3, "LOADING DETECTION DATA")
    
    command = "python src/detection_data_loader.py"
    return run_command(command, "Loading detection data into PostgreSQL")

def run_dbt_models() -> bool:
    """Run dbt models to create staging and fact tables."""
    print_step(4, "RUNNING DBT MODELS")
    
    # Check if dbt_project directory exists
    dbt_project_dir = Path("dbt_project")
    if not dbt_project_dir.exists():
        print("❌ dbt_project directory not found")
        return False
    
    commands = [
        ("dbt deps", "Installing dbt dependencies"),
        ("dbt run --select stg_object_detections", "Running staging model for object detections"),
        ("dbt run --select fct_image_detections", "Running fact table for image detections"),
        ("dbt test --select fct_image_detections", "Testing fact table"),
    ]
    
    for command, description in commands:
        # Use absolute path to avoid path issues
        full_command = f"cd {dbt_project_dir.absolute()} && {command}"
        if not run_command(full_command, description):
            return False
    
    return True

def generate_analysis_report() -> bool:
    """Generate analysis report using the notebook."""
    print_step(5, "GENERATING ANALYSIS REPORT")
    
    print("📊 Analysis can be viewed in the Jupyter notebook:")
    print("   1. Start Jupyter: jupyter notebook")
    print("   2. Open: notebooks/medical_data_analysis.ipynb")
    print("   3. Run cells 25-28 for object detection analysis")
    
    return True

def print_completion_summary(start_time: float, success: bool):
    """Print pipeline completion summary."""
    elapsed_time = time.time() - start_time
    
    print("\n" + "=" * 70)
    if success:
        print("✅ OBJECT DETECTION PIPELINE COMPLETED SUCCESSFULLY!")
    else:
        print("❌ OBJECT DETECTION PIPELINE FAILED!")
    print("=" * 70)
    
    print(f"⏱️  Total Runtime: {elapsed_time:.1f} seconds")
    print(f"📅 Completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    if success:
        print("\n🎯 RESULTS AVAILABLE:")
        print("  📁 Object detection results: data/object_detections/")
        print("  🗄️  PostgreSQL tables: raw.object_detections, marts.fct_image_detections") 
        print("  📊 Analysis notebook: notebooks/medical_data_analysis.ipynb (cells 25-28)")
        print("  📋 dbt documentation: dbt docs generate && dbt docs serve")
        
        print("\n💡 NEXT STEPS:")
        print("  1. Review object detection results in the data warehouse")
        print("  2. Analyze visual patterns in medical content")
        print("  3. Integrate object detection insights with engagement analysis")
        print("  4. Build dashboards using the new fct_image_detections table")
    else:
        print("\n🔧 TROUBLESHOOTING:")
        print("  1. Check database connection and credentials")
        print("  2. Ensure all dependencies are installed")
        print("  3. Verify image files exist in data/images/")
        print("  4. Check logs for detailed error messages")

def main():
    """Main pipeline function."""
    parser = argparse.ArgumentParser(
        description="Complete Object Detection Pipeline for Ethiopian Medical Data",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument(
        '--model', '-m',
        default='yolov8n.pt',
        choices=['yolov8n.pt', 'yolov8s.pt', 'yolov8m.pt', 'yolov8l.pt', 'yolov8x.pt'],
        help='YOLOv8 model to use (default: yolov8n.pt)'
    )
    
    parser.add_argument(
        '--confidence', '-c',
        type=float,
        default=0.25,
        help='Confidence threshold for detections (default: 0.25)'
    )
    
    parser.add_argument(
        '--skip-detection',
        action='store_true',
        help='Skip object detection step (if already completed)'
    )
    
    parser.add_argument(
        '--skip-dbt',
        action='store_true',
        help='Skip dbt model execution'
    )
    
    args = parser.parse_args()
    
    # Start pipeline
    start_time = time.time()
    print_banner()
    
    print(f"🎯 Configuration:")
    print(f"   Model: {args.model}")
    print(f"   Confidence: {args.confidence}")
    print(f"   Skip Detection: {args.skip_detection}")
    print(f"   Skip dbt: {args.skip_dbt}")
    
    # Step 1: Check dependencies
    if not check_dependencies():
        print("❌ Pipeline stopped - missing dependencies")
        return 1
    
    # Step 2: Run object detection (optional skip)
    if not args.skip_detection:
        if not run_object_detection(args.model, args.confidence):
            print("❌ Pipeline stopped - object detection failed")
            return 1
    else:
        print_step(2, "SKIPPING OBJECT DETECTION")
        print("⏭️  Object detection step skipped by user")
    
    # Step 3: Load detection data
    if not load_detection_data():
        print("❌ Pipeline stopped - data loading failed")
        return 1
    
    # Step 4: Run dbt models (optional skip)
    if not args.skip_dbt:
        if not run_dbt_models():
            print("❌ Pipeline stopped - dbt execution failed")
            return 1
    else:
        print_step(4, "SKIPPING DBT MODELS")
        print("⏭️  dbt execution step skipped by user")
    
    # Step 5: Generate analysis report
    if not generate_analysis_report():
        print("❌ Pipeline stopped - report generation failed")
        return 1
    
    # Print completion summary
    print_completion_summary(start_time, True)
    return 0

if __name__ == "__main__":
    try:
        exit_code = main()
        sys.exit(exit_code)
        
    except KeyboardInterrupt:
        print("\n⚠️  Pipeline interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n💥 Unexpected error: {e}")
        sys.exit(1) 