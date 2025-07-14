#!/usr/bin/env python3
"""
Load Raw Data to PostgreSQL Script

This script loads raw JSON data from the data lake into PostgreSQL
for processing with dbt.
"""

import sys
from pathlib import Path

# Add project root to path for imports
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.data_loader import main

if __name__ == "__main__":
    sys.exit(main()) 