import pandas as pd
import numpy as np
from typing import Dict, List, Any

class IndexScreener:
    """Handles all screening operations for index constituents."""
    
    def __init__(self, data: Dict[str, pd.DataFrame], config: Dict[str, Any]):
        self.data = data
        self.config = config
        
    def apply_all_screens(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply all screening criteria to the dataframe."""
        df = df.copy()
        
        # Add basic flags
        df = self._add_flags(df)
        
        # Initialize exclude column
        df['exclude'] = None
        
        # Apply screens in sequence
        df = self._apply_currency_screen(df)
        df = self._apply_layoff_screen(df)
        df = self._apply_turnover_screen(df)
        df = self._apply_standards_screen(df)
        df = self._apply_weapons_screen(df)
        df = self._apply_energy_screen(df)
        df = self._apply_tobacco_screen(df)
        df = self._apply_staff_rating_screen(df)
        
        return df
    
    def _add_flags(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add XPAR and Area flags to dataframe."""
        df['XPAR Flag'] = df['MIC'].apply(lambda x: 1 if x == 'XPAR' else 0)
        df['Area Flag'] = df['index'].apply(
            lambda x: 'NA' if 'NA500' in str(x)
            else 'AS' if 'AS500' in str(x)
            else 'EU' if 'EU500' in str(x)
            else None
        )
        return df
    
    def _apply_currency_screen(self, df: pd.DataFrame) -> pd.DataFrame:
        """Screen out non-major currencies."""
        allowed_currencies = ['EUR', 'JPY', 'USD', 'CAD', 'GBP']
        df['exclude'] = np.where(
            ~df['Currency (Local)'].isin(allowed_currencies),
            'exclude_currency',
            df['exclude']
        )
        return df
    
    def _apply_layoff_screen(self, df: pd.DataFrame) -> pd.DataFrame:
        """Screen based on layoff scores."""
        df['exclude'] = np.where(
            ~df['ISIN'].isin(self.data['sesamm']['ISIN']) & 
            (df['exclude'].isna()),
            'exclude_layoff_score_6m',
            df['exclude']
        )
        return df
    
    def _apply_turnover_screen(self, df: pd.DataFrame) -> pd.DataFrame:
        """Screen based on turnover threshold."""
        df['exclude'] = np.where(
            (df['3 months ADTV'] < 10000000) & 
            (df['exclude'].isna()),
            'exclude_turnover_EUR',
            df['exclude']
        )
        return df
    
    def _apply_standards_screen(self, df: pd.DataFrame) -> pd.DataFrame:
        """Screen based on international standards breaches."""
        nbr_red_flags = self.data['oekom'][
            self.data['oekom']['NBR Overall Flag'] == 'RED'
        ]['ISIN'].tolist()
        
        df['exclude'] = np.where(
            (df['ISIN'].isin(nbr_red_flags)) & 
            (df['exclude'].isna()),
            'exclude_NBROverallFlag',
            df['exclude']
        )
        return df
    
    def _apply_weapons_screen(self, df: pd.DataFrame) -> pd.DataFrame:
        """Screen based on controversial weapons involvement."""
        weapons_criteria = {
            'Biological Weapons - Overall Flag': 'exclude_BiologicalWeaponsFlag',
            'Chemical Weapons - Overall Flag': 'exclude_ChemicalWeaponsFlag',
            'Nuclear Weapons Inside NPT - Overall Flag': 'exclude_NuclearWeaponsFlag',
            'Nuclear Weapons Outside NPT - Overall Flag': 'exclude_NuclearWeaponsNonNPTFlag',
            'Cluster Munitions - Overall Flag': 'exclude_ClusterMunitionsFlag',
            'Depleted Uranium - Overall Flag': 'exclude_DepletedUraniumFlag',
            'Anti-personnel Mines - Overall Flag': 'exclude_APMinesFlag',
            'White Phosphorous Weapons - Overall Flag': 'exclude_WhitePhosphorusFlag'
        }
        
        for column, exclude_value in weapons_criteria.items():
            flagged_isins = self.data['oekom'][
                self.data['oekom'][column].isin(['RED', 'Amber'])
            ]['ISIN'].tolist()
            
            df['exclude'] = np.where(
                (df['ISIN'].isin(flagged_isins)) & 
                (df['exclude'].isna()),
                exclude_value,
                df['exclude']
            )
        
        return df
    
    def _apply_energy_screen(self, df: pd.DataFrame) -> pd.DataFrame:
        """Screen based on energy-related criteria."""
        oekom_df = self.data['oekom']
        
        # Convert relevant columns to numeric
        energy_columns = [
            'Coal Mining and Power Gen - Maximum Percentage of Revenues (%)',
            'FossilFuelProdMaxRev',
            'FossilFuelDistMaxRev',
            'Power Generation - Thermal Maximum Percentage of Revenues (%)'
        ]
        
        for col in energy_columns:
            oekom_df[col] = pd.to_numeric(oekom_df[col], errors='coerce')
        
        # Apply energy criteria
        energy_criteria = {
            'Coal': {
                'condition': lambda df: df['Coal Mining and Power Gen - Maximum Percentage of Revenues (%)'] >= 0.01,
                'exclude_value': 'exclude_CoalMining'
            },
            'FossilFuel': {
                'condition': lambda df: (df['FossilFuelProdMaxRev'] + df['FossilFuelDistMaxRev']) >= 0.10,
                'exclude_value': 'exclude_FossilFuel'
            },
            'Thermal': {
                'condition': lambda df: df['Power Generation - Thermal Maximum Percentage of Revenues (%)'] >= 0.50,
                'exclude_value': 'exclude_ThermalPower'
            }
        }
        
        for criterion in energy_criteria.values():
            excluded_isins = oekom_df[criterion['condition'](oekom_df)]['ISIN'].tolist()
            df['exclude'] = np.where(
                (df['ISIN'].isin(excluded_isins)) & 
                (df['exclude'].isna()),
                criterion['exclude_value'],
                df['exclude']
            )
        
        return df
    
    def _apply_tobacco_screen(self, df: pd.DataFrame) -> pd.DataFrame:
        """Screen based on tobacco-related criteria."""
        oekom_df = self.data['oekom']
        
        # Convert tobacco columns to numeric
        tobacco_columns = [
            'Tobacco - Production Maximum Percentage of Revenues (%)',
            'Tobacco - Distribution Maximum Percentage of Revenues (%)'
        ]
        
        for col in tobacco_columns:
            oekom_df[col] = pd.to_numeric(oekom_df[col], errors='coerce')
        
        # Apply tobacco criteria
        tobacco_criteria = {
            'TobaccoProduction': {
                'condition': lambda df: df['Tobacco - Production Maximum Percentage of Revenues (%)'] > 0,
                'exclude_value': 'exclude_TobaccoProduction'
            },
            'TobaccoDistribution': {
                'condition': lambda df: df['Tobacco - Distribution Maximum Percentage of Revenues (%)'] >= 0.15,
                'exclude_value': 'exclude_TobaccoDistribution'
            }
        }
        
        for criterion in tobacco_criteria.values():
            excluded_isins = oekom_df[criterion['condition'](oekom_df)]['ISIN'].tolist()
            df['exclude'] = np.where(
                (df['ISIN'].isin(excluded_isins)) & 
                (df['exclude'].isna()),
                criterion['exclude_value'],
                df['exclude']
            )
        
        return df
    
    def _apply_staff_rating_screen(self, df: pd.DataFrame) -> pd.DataFrame:
        """Screen based on staff rating criteria."""
        # Prepare analysis dataframe
        analysis_df = (
            self.data['oekom'][self.data['oekom']['ISIN'].isin(df['ISIN'])]
            .merge(
                df[['ISIN', 'Area Flag']],
                on='ISIN',
                how='left'
            )
            .merge(
                self.data['icb'],
                left_on='ISIN',
                right_on='ISIN Code',
                how='left'
            )
            .drop_duplicates(subset=['ISIN'])
        )
        
        # Convert staff rating to numeric
        analysis_df['CRStaffRatingNum'] = pd.to_numeric(
            analysis_df['CRStaffRatingNum'],
            errors='coerce'
        ).fillna(3)
        
        # Process each sector/area group
        excluded_isins = []
        for (sector, area), group in analysis_df.groupby(['Supersector Code', 'Area Flag']):
            sorted_group = group.sort_values('CRStaffRatingNum')
            n_companies = len(group)
            n_to_exclude = int(np.floor(n_companies * 0.1999999999))
            bottom_isins = sorted_group['ISIN'].iloc[:n_to_exclude].tolist()
            excluded_isins.extend(bottom_isins)
        
        # Update exclude column
        df['exclude'] = np.where(
            (df['ISIN'].isin(excluded_isins)) & 
            (df['exclude'].isna()),
            'exclude_StaffRating',
            df['exclude']
        )
        
        return df