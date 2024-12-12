# setup_directories.py
import os
from datetime import datetime
from pathlib import Path

def create_project_structure():
    """Create the necessary directory structure for the project."""
    # Base directories
    directories = [
        'src/core',
        'src/utils',
        'src/scripts',
        'configs/index_configs',
        'output/2024/12',
        'logs',
        'tests/core',
        'tests/utils',
        'docs/implementation',
        'docs/guides'
    ]
    
    # Create each directory
    for directory in directories:
        Path(directory).mkdir(parents=True, exist_ok=True)
        print(f"Created directory: {directory}")

if __name__ == "__main__":
    create_project_structure()