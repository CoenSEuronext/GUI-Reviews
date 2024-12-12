"""Configuration file for data paths and directory structures."""

import os
from datetime import datetime
from pathlib import Path

class PathConfig:
    def __init__(self):
        # Base directories
        self.root_dir = Path(__file__).parent.parent
        self.current_month = datetime.now().strftime("%Y%m")
        
        # Data directories
        self.data_paths = {
            'dlf_folder': Path(r"V:\PM-Indices-IndexOperations\General\Daily downloadfiles\Monthly Archive"),
            'data_folder': Path(r"V:\PM-Indices-IndexOperations\Review Files") / self.current_month,
            'output_folder': self.root_dir / "output" / self.current_month,
            'log_folder': self.root_dir / "logs",
        }
        
        # Input file paths
        self.input_files = {
            'developed_market': "Developed Market.xlsx",
            'free_float': "FF.xlsx",
            'oekom': "Oekom Trust&Carbon.xlsx",
            'icb': "ICB.xlsx",
            'sesamm': "SESAMM.xlsx",
            'nace': "NACE.xlsx"
        }
        
        # EOD file pattern
        self.eod_pattern = "TTMIndex{area}1_GIS_EOD_{type}_{date}.csv"
        
        # Create required directories
        self._create_directories()
    
    def _create_directories(self):
        """Create necessary directories if they don't exist."""
        for path in self.data_paths.values():
            path.mkdir(parents=True, exist_ok=True)
    
    def get_input_path(self, file_key: str) -> Path:
        """Get full path for input file."""
        return self.data_paths['data_folder'] / self.input_files[file_key]
    
    def get_eod_path(self, area: str, type: str, date: str) -> Path:
        """Get full path for EOD file."""
        filename = self.eod_pattern.format(area=area, type=type, date=date)
        return self.data_paths['dlf_folder'] / filename
    
    def get_output_path(self, index_name: str) -> Path:
        """Get output directory for specific index."""
        output_dir = self.data_paths['output_folder'] / index_name
        output_dir.mkdir(parents=True, exist_ok=True)
        return output_dir
    
    def get_log_path(self) -> Path:
        """Get path for log files."""
        return self.data_paths['log_folder']
    
    @property
    def paths(self) -> dict:
        """Get dictionary of all paths."""
        return {
            'root': str(self.root_dir),
            'data': str(self.data_paths['data_folder']),
            'dlf': str(self.data_paths['dlf_folder']),
            'output': str(self.data_paths['output_folder']),
            'logs': str(self.data_paths['log_folder'])
        }