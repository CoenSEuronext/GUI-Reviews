import os
import pandas as pd
from datetime import datetime
from typing import Dict, Tuple, Any
from ..utils.helpers import read_semicolon_csv

class DataLoader:
    """Handles loading and initial processing of all required data files."""
    
    def __init__(self, paths: Dict[str, str], config: Dict[str, Any]):
        self.paths = paths
        self.config = config
        
    def load_all_data(self) -> Dict[str, pd.DataFrame]:
        """Load all required dataframes for the index review."""
        try:
            data = {
                'developed_market': self._load_developed_market(),
                'ff': self._load_free_float(),
                'oekom': self._load_oekom(),
                'icb': self._load_icb(),
                'sesamm': self._load_sesamm(),
            }
            
            # Load EOD data
            index_eod, stock_eod = self._load_eod_data()
            data['index_eod'] = index_eod
            data['stock_eod'] = stock_eod
            
            return data
            
        except Exception as e:
            raise Exception(f"Error loading data: {str(e)}")
    
    def _load_developed_market(self) -> pd.DataFrame:
        """Load and process developed market data."""
        return pd.read_excel(
            os.path.join(self.paths['data_folder'], "Developed Market.xlsx")
        )
    
    def _load_free_float(self) -> pd.DataFrame:
        """Load and process free float data."""
        return pd.read_excel(
            os.path.join(self.paths['data_folder'], "FF.xlsx")
        )
    
    def _load_oekom(self) -> pd.DataFrame:
        """Load and process Oekom Trust&Carbon data."""
        return pd.read_excel(
            os.path.join(self.paths['data_folder'], "Oekom Trust&Carbon.xlsx"),
            header=1
        )
    
    def _load_icb(self) -> pd.DataFrame:
        """Load and process ICB data."""
        return pd.read_excel(
            os.path.join(self.paths['data_folder'], "ICB.xlsx"),
            header=3
        )
    
    def _load_sesamm(self) -> pd.DataFrame:
        """Load and process SESAMM data."""
        return pd.read_excel(
            os.path.join(self.paths['data_folder'], "SESAMM.xlsx")
        )
    
    def _load_eod_data(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Load and process EOD data for both US and EU."""
        # Load US data
        index_eod_us = read_semicolon_csv(
            os.path.join(
                self.paths['dlf_folder'],
                f"TTMIndex{self.config['area']}1_GIS_EOD_INDEX_{self.config['date']}.csv"
            ),
            encoding="latin1"
        )
        stock_eod_us = read_semicolon_csv(
            os.path.join(
                self.paths['dlf_folder'],
                f"TTMIndex{self.config['area']}1_GIS_EOD_STOCK_{self.config['date']}.csv"
            ),
            encoding="latin1"
        )
        
        # Load EU data
        index_eod_eu = read_semicolon_csv(
            os.path.join(
                self.paths['dlf_folder'],
                f"TTMIndex{self.config['area2']}1_GIS_EOD_INDEX_{self.config['date']}.csv"
            ),
            encoding="latin1"
        )
        stock_eod_eu = read_semicolon_csv(
            os.path.join(
                self.paths['dlf_folder'],
                f"TTMIndex{self.config['area2']}1_GIS_EOD_STOCK_{self.config['date']}.csv"
            ),
            encoding="latin1"
        )
        
        # Combine data
        return (
            pd.concat([index_eod_us, index_eod_eu], ignore_index=True),
            pd.concat([stock_eod_us, stock_eod_eu], ignore_index=True)
        )