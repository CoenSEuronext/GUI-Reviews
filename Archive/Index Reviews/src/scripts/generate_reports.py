import os
import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any
import logging
from ..utils.helpers import setup_logging

class ReportGenerator:
    """Generates various reports for index reviews."""
    
    def __init__(self):
        self.logger = setup_logging('report_generator')
        self.date = datetime.now().strftime("%Y%m")
        
    def generate_all_reports(self):
        """Generate all configured reports."""
        try:
            # Load review data
            review_data = self._load_review_data()
            
            # Generate different types of reports
            self._generate_constituent_changes(review_data)
            self._generate_exclusion_summary(review_data)
            self._generate_sector_analysis(review_data)
            self._generate_weight_distribution(review_data)
            
            self.logger.info("Completed generating all reports")
            
        except Exception as e:
            self.logger.error(f"Error generating reports: {str(e)}")
            raise
    
    def _load_review_data(self) -> Dict[str, pd.DataFrame]:
        """Load review results for all indices."""
        output_dir = Path('output') / datetime.now().strftime("%Y/%m")
        data = {}
        
        for index_dir in output_dir.glob('*'):
            if index_dir.is_dir() and not index_dir.name.startswith('.'):
                review_files = list(index_dir.glob('*_review_*.xlsx'))
                if review_files:
                    latest_file = max(review_files, key=lambda x: x.stat().st_mtime)
                    data[index_dir.name] = pd.read_excel(latest_file)
        
        return data
    
    def _generate_constituent_changes(self, review_data: Dict[str, pd.DataFrame]):
        """Generate report showing constituent changes."""
        changes = []
        
        for index_name, current_df in review_data.items():
            try:
                # Load previous review data
                previous_df = self._load_previous_review(index_name)
                
                if previous_df is not None:
                    # Compare constituents
                    additions = set(current_df['ISIN']) - set(previous_df['ISIN'])
                    deletions = set(previous_df['ISIN']) - set(current_df['ISIN'])
                    
                    changes.append({
                        'Index': index_name,
                        'Additions': len(additions),
                        'Deletions': len(deletions),
                        'Net Change': len(additions) - len(deletions),
                        'Turnover': self._calculate_turnover(current_df, previous_df)
                    })
            
            except Exception as e:
                self.logger.error(f"Error processing changes for {index_name}: {str(e)}")
        
        # Create and save report
        changes_df = pd.DataFrame(changes)
        self._save_report(changes_df, 'constituent_changes')
    
    def _generate_exclusion_summary(self, review_data: Dict[str, pd.DataFrame]):
        """Generate summary of exclusions by category."""
        try:
            # Load screening data
            screening_dir = Path('output') / datetime.now().strftime("%Y/%m") / 'screening'
            screening_data = {}
            
            for file in screening_dir.glob('*_screening.xlsx'):
                index_name = file.stem.split('_')[0]
                screening_data[index_name] = pd.read_excel(file)
            
            # Analyze exclusions
            exclusion_summary = []
            exclusion_categories = [
                'exclude_currency', 'exclude_turnover_EUR', 'exclude_NBROverallFlag',
                'exclude_weapons', 'exclude_energy', 'exclude_tobacco',
                'exclude_StaffRating'
            ]
            
            for index_name, df in screening_data.items():
                summary = {'Index': index_name}
                for category in exclusion_categories:
                    summary[category] = (df['exclude'] == category).sum()
                exclusion_summary.append(summary)
            
            # Create and save report
            exclusion_df = pd.DataFrame(exclusion_summary)
            self._save_report(exclusion_df, 'exclusion_summary')
            
        except Exception as e:
            self.logger.error(f"Error generating exclusion summary: {str(e)}")
    
    def _generate_sector_analysis(self, review_data: Dict[str, pd.DataFrame]):
        """Generate sector distribution analysis."""
        try:
            sector_analysis = []
            
            for index_name, df in review_data.items():
                # Merge with ICB data to get sector information
                icb_data = pd.read_excel(Path('data') / 'ICB.xlsx')
                merged_df = df.merge(icb_data, left_on='ISIN', right_on='ISIN Code', how='left')
                
                # Calculate sector weights
                sector_weights = merged_df.groupby('Industry')['Final Capping'].sum()
                
                sector_analysis.append({
                    'Index': index_name,
                    'Largest Sector': sector_weights.index[0],
                    'Largest Weight': sector_weights.iloc[0],
                    'Number of Sectors': len(sector_weights),
                    'HHI': self._calculate_hhi(sector_weights)
                })
            
            # Create and save report
            sector_df = pd.DataFrame(sector_analysis)
            self._save_report(sector_df, 'sector_analysis')
            
        except Exception as e:
            self.logger.error(f"Error generating sector analysis: {str(e)}")
    
    def _generate_weight_distribution(self, review_data: Dict[str, pd.DataFrame]):
        """Generate weight distribution analysis."""
        try:
            weight_analysis = []
            
            for index_name, df in review_data.items():
                weights = df['Final Capping']
                
                analysis = {
                    'Index': index_name,
                    'Max Weight': weights.max(),
                    'Min Weight': weights.min(),
                    'Median Weight': weights.median(),
                    'Top 10 Concentration': weights.nlargest(10).sum(),
                    'Effective N': 1 / (weights ** 2).sum()
                }
                
                weight_analysis.append(analysis)
            
            # Create and save report
            weight_df = pd.DataFrame(weight_analysis)
            self._save_report(weight_df, 'weight_distribution')
            
        except Exception as e:
            self.logger.error(f"Error generating weight distribution analysis: {str(e)}")
    
    def _load_previous_review(self, index_name: str) -> pd.DataFrame:
        """Load previous review data for comparison."""
        try:
            # Calculate previous month
            current = datetime.strptime(self.date, "%Y%m")
            prev_month = (current.replace(day=1) - pd.DateOffset(days=1)).strftime("%Y%m")
            
            # Look for previous review file
            prev_path = Path('output') / prev_month / index_name / f"{index_name}_review_{prev_month}.xlsx"
            
            if prev_path.exists():
                return pd.read_excel(prev_path)
            return None
            
        except Exception as e:
            self.logger.error(f"Error loading previous review for {index_name}: {str(e)}")
            return None
    
    def _calculate_turnover(self, current_df: pd.DataFrame, previous_df: pd.DataFrame) -> float:
        """Calculate index turnover."""
        try:
            current_weights = dict(zip(current_df['ISIN'], current_df['Final Capping']))
            previous_weights = dict(zip(previous_df['ISIN'], previous_df['Final Capping']))
            
            turnover = 0
            all_isins = set(current_weights.keys()) | set(previous_weights.keys())
            
            for isin in all_isins:
                current_weight = current_weights.get(isin, 0)
                previous_weight = previous_weights.get(isin, 0)
                turnover += abs(current_weight - previous_weight)
            
            return turnover / 2
            
        except Exception as e:
            self.logger.error(f"Error calculating turnover: {str(e)}")
            return None
    
    def _calculate_hhi(self, weights: pd.Series) -> float:
        """Calculate Herfindahl-Hirschman Index."""
        return (weights ** 2).sum()
    
    def _save_report(self, df: pd.DataFrame, report_name: str):
        """Save report to appropriate location."""
        output_dir = Path('output') / datetime.now().strftime("%Y/%m") / 'reports'
        output_dir.mkdir(parents=True, exist_ok=True)
        
        output_path = output_dir / f"{report_name}_{self.date}.xlsx"
        df.to_excel(output_path, index=False)
        
        self.logger.info(f"Saved {report_name} report to {output_path}")

def main():
    """Main execution function."""
    try:
        generator = ReportGenerator()
        generator.generate_all_reports()
    except Exception as e:
        logging.error(f"Error in main execution: {str(e)}")
        raise

if __name__ == "__main__":
    main()