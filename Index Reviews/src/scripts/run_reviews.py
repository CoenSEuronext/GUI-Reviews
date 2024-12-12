import os
import yaml
import logging
from datetime import datetime
from typing import Dict, List, Any
from pathlib import Path
import pandas as pd

from ..core.data_loader import DataLoader
from ..core.screener import IndexScreener
from ..core.selector import IndexSelector
from ..core.capper import IndexCapper
from ..utils.helpers import setup_logging, load_config

class ReviewRunner:
    """Handles the execution of index reviews."""
    
    def __init__(self, config_path: str):
        self.config_path = config_path
        self.logger = setup_logging('review_runner')
        self.date = datetime.now().strftime("%Y%m")
        
    def run_all_reviews(self):
        """Run all configured index reviews."""
        try:
            # Load all index configs
            configs = self._load_all_configs()
            self.logger.info(f"Loaded {len(configs)} index configurations")
            
            # Run each review
            results = []
            for config in configs:
                try:
                    result = self.run_single_review(config)
                    results.append(result)
                except Exception as e:
                    self.logger.error(f"Error processing review for {config['name']}: {str(e)}")
            
            # Generate summary report
            self._generate_summary(results)
            
        except Exception as e:
            self.logger.error(f"Error in review process: {str(e)}")
            raise
    
    def run_single_review(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Run review for a single index."""
        self.logger.info(f"Starting review for {config['name']}")
        start_time = datetime.now()
        
        try:
            # Initialize components
            data_loader = DataLoader(self._get_paths(), config)
            data = data_loader.load_all_data()
            
            # Screen universe
            screener = IndexScreener(data, config)
            screened_df = screener.apply_all_screens(data['developed_market'])
            
            # Select stocks
            selector = IndexSelector(data, config)
            xpar_selected, noxpar_selected = selector.select_stocks(screened_df)
            
            # Apply capping
            capper = IndexCapper(data, config)
            final_df = capper.apply_capping(xpar_selected, noxpar_selected)
            
            # Save results
            output_path = self._save_results(final_df, config)
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            result = {
                'index': config['name'],
                'status': 'success',
                'duration': duration,
                'constituents': len(final_df),
                'output_path': output_path,
                'timestamp': end_time
            }
            
            self.logger.info(f"Completed review for {config['name']}")
            return result
            
        except Exception as e:
            self.logger.error(f"Error in review for {config['name']}: {str(e)}")
            return {
                'index': config['name'],
                'status': 'failed',
                'error': str(e),
                'timestamp': datetime.now()
            }
    
    def _load_all_configs(self) -> List[Dict[str, Any]]:
        """Load all index configurations."""
        config_dir = Path(self.config_path) / 'index_configs'
        configs = []
        
        for config_file in config_dir.glob('*.yaml'):
            with open(config_file, 'r') as f:
                config = yaml.safe_load(f)
                configs.append(config)
        
        return configs
    
    def _get_paths(self) -> Dict[str, str]:
        """Get data file paths."""
        with open(Path(self.config_path) / 'path_configs.yaml', 'r') as f:
            return yaml.safe_load(f)
    
    def _save_results(self, df: pd.DataFrame, config: Dict[str, Any]) -> str:
        """Save review results to appropriate location."""
        # Create output directory if it doesn't exist
        output_dir = Path('output') / datetime.now().strftime("%Y/%m") / config['name']
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Save file
        output_path = output_dir / f"{config['name']}_review_{self.date}.xlsx"
        df.to_excel(output_path, index=False)
        
        return str(output_path)
    
    def _generate_summary(self, results: List[Dict[str, Any]]):
        """Generate summary of all reviews."""
        summary_df = pd.DataFrame(results)
        
        # Save summary
        summary_path = Path('output') / datetime.now().strftime("%Y/%m") / 'review_summary.xlsx'
        summary_df.to_excel(summary_path, index=False)
        
        self.logger.info(f"Generated review summary at {summary_path}")

def main():
    """Main execution function."""
    try:
        config_path = os.getenv('INDEX_REVIEW_CONFIG', 'configs')
        runner = ReviewRunner(config_path)
        runner.run_all_reviews()
    except Exception as e:
        logging.error(f"Error in main execution: {str(e)}")
        raise

if __name__ == "__main__":
    main()