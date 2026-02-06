import pandas as pd
import numpy as np
from datetime import datetime
import os
import traceback
from config import DLF_FOLDER, DATA_FOLDER, DATA_FOLDER2
from utils.logging_utils import setup_logging
from utils.data_loader import load_eod_data, load_reference_data
from utils.inclusion_exclusion import inclusion_exclusion_analysis

logger = setup_logging(__name__)

def run_sectorial_review(date, co_date, effective_date, index="ETPFB", isin="NLIX00005982", 
                   area="US", area2="EU", type="STOCK", universe="developed_market", 
                   feed="Reuters", currency="EUR", year=None):

    try:
        # Dictionary mapping Mnemo codes to index information for Price indices only
        year = year or str(datetime.strptime(date, '%Y%m%d').year)

        # Set data folder for current month
        current_data_folder = os.path.join(DATA_FOLDER2, date[:6])

        # Load data with error handling
        logger.info("Loading EOD data...")
        index_eod_df, stock_eod_df, stock_co_df, fx_lookup_df = load_eod_data(date, co_date, area, area2, DLF_FOLDER)

        logger.info("Loading reference data...")
        
        ref_data = load_reference_data(
            current_data_folder,
            ['ff', 'developed_market', 'icb', 'eusp', 'deup', 'edwp']
        )

        # Original universes
        universe_df = ref_data['developed_market']
        ff_df = ref_data['ff']
        icb_df = ref_data['icb']
        
        # New universes
        eusp_df = ref_data['eusp']
        deup_df = ref_data['deup']
        edwp_df = ref_data['edwp']
        
        # Filter dataframes based on index column
        na500_df = universe_df[universe_df['index'].str.contains('NA500', na=False)]
        transatlantic_df = universe_df[universe_df['index'].str.contains('NA500|EU500', na=False)]
        ez300_df = universe_df[universe_df['index'].str.contains('EZ300', na=False)]

        # Filter symbols once
        symbols_filtered = stock_eod_df[
            stock_eod_df['#Symbol'].str.len() < 12
        ][['Isin Code', '#Symbol']].drop_duplicates(subset=['Isin Code'], keep='first')

        # Function to apply data preparation operations
        def prepare_dataframe(df, symbols_filtered, stock_eod_df, stock_co_df, ff_df, icb_df, currency, 
                             exclude_xtse=False, adtv_column='3 months ADTV', adtv_threshold=5000000):
            """
            Prepare dataframe with flexible ADTV column name handling
            
            Args:
                df: Input dataframe
                symbols_filtered: Filtered symbols dataframe
                stock_eod_df: EOD stock data
                stock_co_df: CO stock data
                ff_df: Free float data
                icb_df: ICB classification data
                currency: Target currency
                exclude_xtse: Whether to exclude XTSE stocks
                adtv_column: Name of ADTV column ('3 months ADTV' or '3M AVG Turnover EUR')
                adtv_threshold: Minimum ADTV threshold
            """
            result_df = (df
                # Initial renaming
                .rename(columns={
                    'NOSH': 'Number of Shares',
                    'ISIN': 'ISIN code',
                    'Name': 'Company',
                })
                # Merge symbols
                .merge(
                    symbols_filtered,
                    left_on='ISIN code',
                    right_on='Isin Code',
                    how='left'
                )
                .drop('Isin Code', axis=1)
                # Merge FX data
                .merge(
                    stock_eod_df[stock_eod_df['Index Curr'] == currency][['#Symbol', 'FX/Index Ccy']].drop_duplicates(subset='#Symbol', keep='first'),
                    on='#Symbol',
                    how='left'
                )
                # Merge EOD prices
                .merge(
                    stock_eod_df[['#Symbol', 'Close Prc']].drop_duplicates(subset='#Symbol', keep='first'),
                    on='#Symbol',
                    how='left',
                    suffixes=('', '_EOD')
                )
                # Merge CO prices
                .merge(
                    stock_co_df[['#Symbol', 'Close Prc']].drop_duplicates(subset='#Symbol', keep='first'),
                    on='#Symbol',
                    how='left',
                    suffixes=('_EOD', '_CO')
                )
                # Merge FF data for Free Float Round
                .merge(
                    ff_df[['ISIN Code:', 'Free Float Round:']].drop_duplicates(subset='ISIN Code:', keep='first'),
                    left_on='ISIN code',
                    right_on='ISIN Code:',
                    how='left'
                )
                .drop('ISIN Code:', axis=1)
                # Merge ICB data for Subsector Code
                .merge(
                    icb_df[['ISIN Code', 'Subsector Code', 'Industry Code', 'Sector Code', 'Supersector Code']].drop_duplicates(subset='ISIN Code', keep='first'),
                    left_on='ISIN code',
                    right_on='ISIN Code',
                    how='left'
                )
                .drop('ISIN Code', axis=1)
            )
            
            # Standardize ADTV column name to a common internal name
            if adtv_column in result_df.columns:
                result_df['ADTV'] = result_df[adtv_column]
            else:
                logger.warning(f"ADTV column '{adtv_column}' not found in dataframe. Available columns: {result_df.columns.tolist()}")
                result_df['ADTV'] = np.nan
            
            # Create general exclusion flags
            result_df['Exclusion_XTSE'] = result_df['MIC'].str.contains('XTSE', na=False) if exclude_xtse else False
            result_df['Exclusion_3m_ADTV'] = (result_df['ADTV'] < adtv_threshold) | result_df['ADTV'].isna()
            
            # Create general exclusion flag (True if ANY exclusion criteria is met)
            result_df['General_Exclusion'] = result_df['Exclusion_XTSE'] | result_df['Exclusion_3m_ADTV']
            
            # Filter out excluded companies
            result_df = result_df[~result_df['General_Exclusion']]
            
            return result_df

        # Apply to all universes with appropriate ADTV column names
        logger.info("Preparing original universes (NA500, Transatlantic, EZ300)...")
        na500_selection_df = prepare_dataframe(na500_df, symbols_filtered, stock_eod_df, stock_co_df, ff_df, icb_df, currency, 
                                               exclude_xtse=True, adtv_column='3 months ADTV')
        transatlantic_selection_df = prepare_dataframe(transatlantic_df, symbols_filtered, stock_eod_df, stock_co_df, ff_df, icb_df, currency, 
                                                       exclude_xtse=True, adtv_column='3 months ADTV')
        ez300_selection_df = prepare_dataframe(ez300_df, symbols_filtered, stock_eod_df, stock_co_df, ff_df, icb_df, currency, 
                                               exclude_xtse=False, adtv_column='3 months ADTV')
        
        logger.info("Preparing new universes (EUSP, DEUP, EDWP)...")
        eusp_selection_df = prepare_dataframe(eusp_df, symbols_filtered, stock_eod_df, stock_co_df, ff_df, icb_df, currency, 
                                              exclude_xtse=False, adtv_column='3M AVG Turnover EUR')
        deup_selection_df = prepare_dataframe(deup_df, symbols_filtered, stock_eod_df, stock_co_df, ff_df, icb_df, currency, 
                                              exclude_xtse=False, adtv_column='3M AVG Turnover EUR')
        edwp_selection_df = prepare_dataframe(edwp_df, symbols_filtered, stock_eod_df, stock_co_df, ff_df, icb_df, currency, 
                                              exclude_xtse=False, adtv_column='3M AVG Turnover EUR')
        
        # Dictionary of universe DataFrames for easy reference
        universe_mapping = {
            'na500': na500_selection_df,
            'transatlantic': transatlantic_selection_df,
            'ez300': ez300_selection_df,
            'eusp': eusp_selection_df,
            'deup': deup_selection_df,
            'edwp': edwp_selection_df
        }
        
        # Updated sectorial indices dictionary with both original and new indices
        sectorial_indices = {
            # ===== ORIGINAL INDICES (Industry Code based) =====
            'TECHP': {
                'isincode': 'NLIX00003359',
                'starting_universe': 'ez300',
                'classification_type': 'industry',
                'industry_code': 10
            },
            'ENRGP': {
                'isincode': 'NLIX00003318',
                'starting_universe': 'ez300',
                'classification_type': 'industry',
                'industry_code': 60
            },
            'UTIL': {
                'isincode': 'NLIX00003375',
                'starting_universe': 'ez300',
                'classification_type': 'industry',
                'industry_code': 65
            },
            'BASM': {
                'isincode': 'NLIX00003284',
                'starting_universe': 'ez300',
                'classification_type': 'industry',
                'industry_code': 55
            },
            'FINA': {
                'isincode': 'NLIX00003326',
                'starting_universe': 'ez300',
                'classification_type': 'industry',
                'industry_code': 30
            },
            'CSTA': {
                'isincode': 'NLIX00003300',
                'starting_universe': 'ez300',
                'classification_type': 'industry',
                'industry_code': 45
            },
            'TELEP': {
                'isincode': 'NLIX00003367',
                'starting_universe': 'ez300',
                'classification_type': 'industry',
                'industry_code': 15
            },
            'HEAC': {
                'isincode': 'NLIX00003334',
                'starting_universe': 'ez300',
                'classification_type': 'industry',
                'industry_code': 20
            },
            'INDU': {
                'isincode': 'NLIX00003342',
                'starting_universe': 'ez300',
                'classification_type': 'industry',
                'industry_code': 50
            },
            'CDIS': {
                'isincode': 'NLIX00003292',
                'starting_universe': 'ez300',
                'classification_type': 'industry',
                'industry_code': 40
            },
            'TBMA': {
                'isincode': 'NLIX00003920',
                'starting_universe': 'transatlantic',
                'classification_type': 'industry',
                'industry_code': 55
            },
            'TCDI': {
                'isincode': 'NLIX00003771',
                'starting_universe': 'transatlantic',
                'classification_type': 'industry',
                'industry_code': 40
            },
            'TCST': {
                'isincode': 'NLIX00003748',
                'starting_universe': 'transatlantic',
                'classification_type': 'industry',
                'industry_code': 45
            },
            'TENR': {
                'isincode': 'NLIX00003714',
                'starting_universe': 'transatlantic',
                'classification_type': 'industry',
                'industry_code': 60
            },
            'TFINP': {
                'isincode': 'NLIX00003805',
                'starting_universe': 'transatlantic',
                'classification_type': 'industry',
                'industry_code': 30
            },
            'THEC': {
                'isincode': 'NLIX00003896',
                'starting_universe': 'transatlantic',
                'classification_type': 'industry',
                'industry_code': 20
            },
            'TIND': {
                'isincode': 'NLIX00003839',
                'starting_universe': 'transatlantic',
                'classification_type': 'industry',
                'industry_code': 50
            },
            'TTEC': {
                'isincode': 'NLIX00003680',
                'starting_universe': 'transatlantic',
                'classification_type': 'industry',
                'industry_code': 10
            },
            'TTEL': {
                'isincode': 'NLIX00003862',
                'starting_universe': 'transatlantic',
                'classification_type': 'industry',
                'industry_code': 15
            },
            'TUTI': {
                'isincode': 'NLIX00003953',
                'starting_universe': 'transatlantic',
                'classification_type': 'industry',
                'industry_code': 65
            },
            'UUTI': {
                'isincode': 'NLIX00005206',
                'starting_universe': 'na500',
                'classification_type': 'industry',
                'industry_code': 65
            },
            'UTEL': {
                'isincode': 'NLIX00005115',
                'starting_universe': 'na500',
                'classification_type': 'industry',
                'industry_code': 15
            },
            'UTEC': {
                'isincode': 'NLIX00004936',
                'starting_universe': 'na500',
                'classification_type': 'industry',
                'industry_code': 10
            },
            'UIND': {
                'isincode': 'NLIX00005081',
                'starting_universe': 'na500',
                'classification_type': 'industry',
                'industry_code': 50
            },
            'UHEC': {
                'isincode': 'NLIX00005149',
                'starting_universe': 'na500',
                'classification_type': 'industry',
                'industry_code': 20
            },
            'UFIN': {
                'isincode': 'NLIX00005057',
                'starting_universe': 'na500',
                'classification_type': 'industry',
                'industry_code': 30
            },
            'UENR': {
                'isincode': 'NLIX00004969',
                'starting_universe': 'na500',
                'classification_type': 'industry',
                'industry_code': 60
            },
            'UCST': {
                'isincode': 'NLIX00004993',
                'starting_universe': 'na500',
                'classification_type': 'industry',
                'industry_code': 45
            },
            'UCDI': {
                'isincode': 'NLIX00005024',
                'starting_universe': 'na500',
                'classification_type': 'industry',
                'industry_code': 40
            },
            'UBMA': {
                'isincode': 'NLIX00005172',
                'starting_universe': 'na500',
                'classification_type': 'industry',
                'industry_code': 55
            },
            'EEUS': {
                'isincode': 'NLIX00007590',
                'starting_universe': 'deup',
                'classification_type': 'subsector',
                'industry_code': [10102020, 10102010]
            },
            'EEAPP': {
                'isincode': 'NLIX00007657',
                'starting_universe': 'deup',
                'classification_type': 'sector',
                'industry_code': 401010
            },
            'EECP': {
                'isincode': 'NLIX00007715',
                'starting_universe': 'deup',
                'classification_type': 'supersector',
                'industry_code': 4020
            },
            'EEBRP': {
                'isincode': 'NLIX00007749',
                'starting_universe': 'deup',
                'classification_type': 'supersector',
                'industry_code': 5510
            },
            'EEUU': {
                'isincode': 'NLIX00007806',
                'starting_universe': 'deup',
                'classification_type': 'supersector',
                'industry_code': 6510
            },
            'EEOG': {
                'isincode': 'NLIX00007897',
                'starting_universe': 'deup',
                'classification_type': 'sector',
                'industry_code': 601010
            },
            'EUOG': {
                'isincode': 'NLIX00007921',
                'starting_universe': 'eusp',
                'classification_type': 'sector',
                'industry_code': 601010
            },
            'EWHC': {
                'isincode': 'NLIX00007624',
                'starting_universe': 'edwp',
                'classification_type': 'subsector',
                'industry_code': [20102010, 20102020, 20102015, 20103010]  # Multiple codes
            },
            'EWAP': {
                'isincode': 'NLIX00007681',
                'starting_universe': 'edwp',
                'classification_type': 'sector',
                'industry_code': 401010
            },
            'EWBR': {
                'isincode': 'NLIX00007772',
                'starting_universe': 'edwp',
                'classification_type': 'supersector',
                'industry_code': 5510
            },
            'EWOU': {
                'isincode': 'NLIX00007863',
                'starting_universe': 'edwp',
                'classification_type': 'supersector',
                'industry_code': 6510
            },
            'EWOG': {
                'isincode': 'NLIX00007954',
                'starting_universe': 'edwp',
                'classification_type': 'sector',
                'industry_code': 601010
            },
            'IECDP': {
                'isincode': 'NLIX00009133',
                'starting_universe': 'deup',
                'classification_type': 'industry',
                'industry_code': 40
            },
            'IECSP': {
                'isincode': 'NLIX00009166',
                'starting_universe': 'deup',
                'classification_type': 'industry',
                'industry_code': 45
            },
            'IETCP': {
                'isincode': 'NLIX00009190',
                'starting_universe': 'deup',
                'classification_type': 'industry',
                'industry_code': 15
            },
            'IEENP': {
                'isincode': 'NLIX00009224',
                'starting_universe': 'deup',
                'classification_type': 'industry',
                'industry_code': 60
            },
            'IEFIP': {
                'isincode': 'NLIX00009257',
                'starting_universe': 'deup',
                'classification_type': 'industry',
                'industry_code': 30
            },
            'IEHCP': {
                'isincode': 'NLIX00009281',
                'starting_universe': 'deup',
                'classification_type': 'industry',
                'industry_code': 20
            },
            'IETLP': {
                'isincode': 'NLIX00009315',
                'starting_universe': 'deup',
                'classification_type': 'industry',
                'industry_code': 10
            },
            'IEINP': {
                'isincode': 'NLIX00009349',
                'starting_universe': 'deup',
                'classification_type': 'industry',
                'industry_code': 50
            },
            'IEBMP': {
                'isincode': 'NLIX00009372',
                'starting_universe': 'deup',
                'classification_type': 'industry',
                'industry_code': 55
            },
            'IEUTP': {
                'isincode': 'NLIX00009406',
                'starting_universe': 'deup',
                'classification_type': 'industry',
                'industry_code': 65
            },
            'IEREP': {
                'isincode': 'NLIX00009430',
                'starting_universe': 'deup',
                'classification_type': 'industry',
                'industry_code': 35
            },
            'IWBMP': {
                'isincode': 'NLIX00009802',
                'starting_universe': 'edwp',
                'classification_type': 'industry',
                'industry_code': 55
            },
            'IWCDP': {
                'isincode': 'NLIX00009562',
                'starting_universe': 'edwp',
                'classification_type': 'industry',
                'industry_code': 40
            },
            'IWCSP': {
                'isincode': 'NLIX00009596',
                'starting_universe': 'edwp',
                'classification_type': 'industry',
                'industry_code': 45
            },
            'IWENP': {
                'isincode': 'NLIX00009653',
                'starting_universe': 'edwp',
                'classification_type': 'industry',
                'industry_code': 60
            },
            'IWFIP': {
                'isincode': 'NLIX00009687',
                'starting_universe': 'edwp',
                'classification_type': 'industry',
                'industry_code': 30
            },
            'IWHCP': {
                'isincode': 'NLIX00009711',
                'starting_universe': 'edwp',
                'classification_type': 'industry',
                'industry_code': 20
            },
            'IWINP': {
                'isincode': 'NLIX00009778',
                'starting_universe': 'edwp',
                'classification_type': 'industry',
                'industry_code': 50
            },
            'IWREP': {
                'isincode': 'NLIX00009869',
                'starting_universe': 'edwp',
                'classification_type': 'industry',
                'industry_code': 35
            },
            'IWTCP': {
                'isincode': 'NLIX00009745',
                'starting_universe': 'edwp',
                'classification_type': 'industry',
                'industry_code': 10
            },
            'IWTLP': {
                'isincode': 'NLIX00009620',
                'starting_universe': 'edwp',
                'classification_type': 'industry',
                'industry_code': 15
            },
            'IWUTP': {
                'isincode': 'NLIX00009836',
                'starting_universe': 'edwp',
                'classification_type': 'industry',
                'industry_code': 65
            },
        }

        def calculate_capped_weights(weights, cap_limit=0.2, max_iterations=100, tolerance=1e-8):
            """
            Calculate capped weights with iterative redistribution of excess weight.
            Companies exceeding the cap will have exactly cap_limit weight (20%).
            Excess weight is redistributed proportionally to companies below the cap
            based on their ORIGINAL weights.
            
            This function now expects input weights that sum to 1.0.
            
            Args:
                weights: pandas Series of original weights (should sum to 1.0)
                cap_limit: maximum weight allowed (default 0.2 for 20%)
                max_iterations: maximum number of redistribution iterations
                tolerance: convergence tolerance
            
            Returns:
                pandas Series of final capped weights (sum = 1.0, max <= cap_limit)
            """
            import pandas as pd
            
            # Verify input weights sum to 1.0
            original_sum = weights.sum()
            if abs(original_sum - 1.0) > tolerance:
                raise ValueError(f"Input weights sum to {original_sum:.8f}, expected 1.0. Fix weight calculation first.")
            
            # Store original weights for proportional redistribution
            original_weights = weights.copy()
            current_weights = weights.copy()
            is_capped = pd.Series(False, index=weights.index)  # Track permanently capped companies
            
            # Iterative redistribution
            for iteration in range(max_iterations):
                # Identify companies that exceed cap and aren't already permanently capped
                needs_capping = (current_weights > cap_limit) & (~is_capped)
                
                if not needs_capping.any():
                    # No more companies need capping
                    break
                    
                # Calculate excess weight from companies that need capping
                excess_weight = (current_weights[needs_capping] - cap_limit).sum()
                
                # Cap these companies at exactly cap_limit and mark as permanently capped
                current_weights[needs_capping] = cap_limit
                is_capped[needs_capping] = True
                
                # Find companies eligible to receive redistributed weight
                # (not capped AND currently below cap limit)
                can_receive = (~is_capped) & (current_weights < cap_limit)
                
                if not can_receive.any() or excess_weight <= tolerance:
                    # No eligible companies or no meaningful excess weight
                    break
                    
                # Use original weights of eligible companies for proportional redistribution
                eligible_original_weights = original_weights[can_receive]
                total_eligible_original = eligible_original_weights.sum()
                
                if total_eligible_original <= tolerance:
                    # No meaningful original weights to use for redistribution
                    break
                    
                # Calculate how much weight each eligible company should receive
                redistribution_shares = eligible_original_weights / total_eligible_original
                additional_weights = excess_weight * redistribution_shares
                
                # Apply the additional weights
                current_weights[can_receive] += additional_weights
            
            # Verify final results (should naturally be correct now)
            total_weight = current_weights.sum()
            max_weight = current_weights.max()
            
            if abs(total_weight - 1.0) > tolerance:
                raise ValueError(f"Algorithm failed: Total weight is {total_weight:.8f}, not 1.0")
            
            if max_weight > cap_limit + tolerance:
                raise ValueError(f"Algorithm failed: Max weight is {max_weight:.8f}, exceeds cap of {cap_limit}")
            
            return current_weights

        def process_sectorial_index(index_code, index_info, universe_mapping):
            """
            Process a single sectorial index with flexible classification system support
            
            Args:
                index_code: Index code (e.g., 'TECHP', 'EEUS')
                index_info: Dictionary with index configuration
                universe_mapping: Dictionary mapping universe names to DataFrames
            """
            try:
                isin = index_info['isincode']
                universe_name = index_info['starting_universe']
                classification_type = index_info['classification_type']
                industry_codes = index_info['industry_code']
                
                # Get the appropriate universe DataFrame
                universe_df = universe_mapping[universe_name]
                
                # Determine the classification column based on type
                classification_column_map = {
                    'industry': 'Industry Code',
                    'subsector': 'Subsector Code',
                    'sector': 'Sector Code',
                    'supersector': 'Supersector Code'
                }
                
                classification_column = classification_column_map[classification_type]
                
                logger.info(f"Processing {index_code}: using {classification_column} from {universe_name} universe")
                
                # Get index market cap (for reference/logging only)
                index_mcap = index_eod_df.loc[index_eod_df['#Symbol'] == isin, 'Mkt Cap'].iloc[0]
                
                # Filter universe by classification code(s)
                # Handle both single code and list of codes
                if isinstance(industry_codes, list):
                    logger.info(f"Filtering by multiple codes: {industry_codes}")
                    selection_df = universe_df[
                        universe_df[classification_column].isin(industry_codes)
                    ].copy()
                else:
                    logger.info(f"Filtering by single code: {industry_codes}")
                    selection_df = universe_df[
                        universe_df[classification_column] == industry_codes
                    ].copy()
                
                if selection_df.empty:
                    logger.warning(f"No companies found for {index_code} with {classification_column} = {industry_codes}")
                    return None
                
                logger.info(f"Found {len(selection_df)} companies for {index_code}")
                
                # Use the merged Free Float Round value, fallback to original Free Float if not available  
                selection_df["Free Float"] = selection_df["Free Float Round:"]
                selection_df['Original Market Cap'] = selection_df['Close Prc_EOD'] * selection_df['Number of Shares'] * selection_df['FX/Index Ccy'] * selection_df['Free Float']
                
                # Calculate weights: Divide by sum of selected companies' market caps
                total_selected_mcap = selection_df['Original Market Cap'].sum()
                selection_df['Weight'] = selection_df['Original Market Cap'] / total_selected_mcap
                
                # Verify weights sum to 1.0
                weights_sum = selection_df['Weight'].sum()
                logger.info(f"Total weight before capping for {index_code}: {weights_sum:.8f}")
                
                # This should be very close to 1.0 (within floating point precision)
                assert abs(weights_sum - 1.0) < 1e-10, f"Weights don't sum to 1.0 for {index_code}: {weights_sum}"

                # Calculate capped weights
                capped_weights = calculate_capped_weights(selection_df['Weight'], cap_limit=0.2)
                selection_df['Capped Weight'] = capped_weights
                
                # Calculate capping factors for reporting purposes (final_weight / original_weight)
                selection_df['Capping Factor'] = selection_df['Capped Weight'] / selection_df['Weight']

                # Scale capping factors to 1 by dividing by the maximum capping factor
                max_capping_factor = selection_df['Capping Factor'].max()
                selection_df['Capping Factor'] = selection_df['Capping Factor'] / max_capping_factor

                logger.info(f"Scaled capping factors for {index_code} (max factor was {max_capping_factor:.6f})")
                logger.info(f"Capping factors now range from {selection_df['Capping Factor'].min():.6f} to {selection_df['Capping Factor'].max():.6f}")
                
                # Log capping results
                capped_companies = selection_df[selection_df['Capped Weight'] < selection_df['Weight'] * 0.9999]
                boosted_companies = selection_df[selection_df['Capped Weight'] > selection_df['Weight'] * 1.0001]
                
                if not capped_companies.empty:
                    logger.info(f"Capped {len(capped_companies)} companies to 20% in {index_code}:")
                    for _, company in capped_companies.iterrows():
                        logger.info(f"  {company['Company']}: {company['Weight']:.4f} -> {company['Capped Weight']:.4f}")
                
                if not boosted_companies.empty:
                    logger.info(f"Boosted {len(boosted_companies)} companies due to redistribution in {index_code}:")
                    for _, company in boosted_companies.iterrows():
                        logger.info(f"  {company['Company']}: {company['Weight']:.4f} -> {company['Capped Weight']:.4f}")

                # Verify final results
                total_capped_weight = selection_df['Capped Weight'].sum()
                max_weight = selection_df['Capped Weight'].max()
                
                logger.info(f"Total weight after capping for {index_code}: {total_capped_weight:.6f}")
                logger.info(f"Maximum individual weight for {index_code}: {max_weight:.6f} (cap: 0.200000)")

                # Log comparison with original index market cap (for information)
                logger.info(f"Index market cap for {index_code}: {index_mcap:,.0f}")
                logger.info(f"Sum of selected companies market cap: {total_selected_mcap:,.0f}")
                coverage_ratio = total_selected_mcap / index_mcap if index_mcap > 0 else 0
                logger.info(f"Coverage ratio (selected/index): {coverage_ratio:.4f}")

                # Assertions
                assert abs(total_capped_weight - 1.0) < 1e-8, f"Total weight {total_capped_weight} is not equal to 1.0 for {index_code}"
                assert max_weight <= 0.2 + 1e-8, f"Max weight {max_weight} exceeds 20% cap for {index_code}"
                
                # Prepare sectorial index DataFrame
                selection_df['Effective Date of Review'] = effective_date
                sectorial_df = (
                    selection_df[
                        ['Company', 'ISIN code', 'MIC', 'Number of Shares', 'Free Float', 'Capping Factor', 
                        'Effective Date of Review', 'Currency (Local)']
                    ]
                    .rename(columns={'ISIN code': 'ISIN Code'})
                    .rename(columns={'Currency (Local)': 'Currency'})
                    .sort_values('Company')
                )
                
                # Perform Inclusion/Exclusion Analysis
                analysis_results = inclusion_exclusion_analysis(
                    selection_df, 
                    stock_eod_df, 
                    index_code, 
                    isin_column='ISIN code'
                )
                
                return {
                    'index_code': index_code,
                    'isin': isin,
                    'universe_name': universe_name,
                    'classification_type': classification_type,
                    'classification_codes': industry_codes,
                    'index_mcap': index_mcap,
                    'selected_mcap': total_selected_mcap,
                    'coverage_ratio': coverage_ratio,
                    'composition_df': sectorial_df,
                    'selection_df': selection_df,
                    'inclusion_df': analysis_results['inclusion_df'],
                    'exclusion_df': analysis_results['exclusion_df'],
                    'dlf_df': universe_df,
                    'dlf_name': f"{universe_name.upper()}_DLF"
                }
                
            except Exception as e:
                logger.error(f"Error processing {index_code}: {str(e)}")
                logger.error(traceback.format_exc())
                return None

        # Process all sectorial indices
        logger.info(f"Processing all {len(sectorial_indices)} sectorial indices...")
        all_results = {}

        for index_code, index_info in sectorial_indices.items():
            logger.info(f"Processing {index_code}...")
            result = process_sectorial_index(index_code, index_info, universe_mapping)
            if result:
                all_results[index_code] = result
            else:
                logger.warning(f"Failed to process {index_code}")

        # Save results to Excel files
        try:
            output_dir = os.path.join(os.getcwd(), 'output')
            os.makedirs(output_dir, exist_ok=True)
        
            # Create filename with timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            # Save each sectorial index to separate file
            saved_files = []
            for index_code, result in all_results.items():
                file_path = os.path.join(output_dir, f'{index_code}_sectorial_{timestamp}.xlsx')
                
                logger.info(f"Saving {index_code} output to: {file_path}")
                with pd.ExcelWriter(file_path) as writer:
                    result['composition_df'].to_excel(writer, sheet_name='Index Composition', index=False)
                    result['inclusion_df'].to_excel(writer, sheet_name='Inclusion', index=False)
                    result['exclusion_df'].to_excel(writer, sheet_name='Exclusion', index=False)
                    result['selection_df'].to_excel(writer, sheet_name='Full Universe', index=False)
                    
                    # Add the appropriate DLF DataFrame
                    if result['dlf_df'] is not None:
                        result['dlf_df'].to_excel(writer, sheet_name=result['dlf_name'], index=False)
                        logger.info(f"Added {result['dlf_name']} sheet to {index_code}")
                    
                    # Enhanced index info sheet with coverage metrics and classification info
                    index_info_df = pd.DataFrame({
                        'Metric': ['Index Market Cap', 'Selected Companies Market Cap', 'Coverage Ratio', 
                                  'Number of Companies', 'DLF Universe', 'Classification Type', 'Classification Code(s)'],
                        'Value': [result['index_mcap'], result['selected_mcap'], result['coverage_ratio'], 
                                len(result['composition_df']), result['dlf_name'], 
                                result['classification_type'], str(result['classification_codes'])]
                    })
                    index_info_df.to_excel(writer, sheet_name='Index Info', index=False)
                
                saved_files.append(file_path)
            
            # Create summary file with all indices
            summary_path = os.path.join(output_dir, f'All_Sectorial_Summary_{timestamp}.xlsx')
            logger.info(f"Creating summary file: {summary_path}")
            
            with pd.ExcelWriter(summary_path) as writer:
                # Create enhanced summary sheet
                summary_data = []
                for index_code, result in all_results.items():
                    summary_data.append({
                        'Index Code': index_code,
                        'ISIN': result['isin'],
                        'DLF Universe': result['dlf_name'],
                        'Classification Type': result['classification_type'],
                        'Classification Code(s)': str(result['classification_codes']),
                        'Index Market Cap': result['index_mcap'],
                        'Selected Market Cap': result['selected_mcap'],
                        'Coverage Ratio': result['coverage_ratio'],
                        'Number of Companies': len(result['composition_df'])
                    })
                
                summary_df = pd.DataFrame(summary_data)
                summary_df.to_excel(writer, sheet_name='Summary', index=False)
                
                # Add individual composition sheets (limited to first 35 due to Excel sheet limits)
                for i, (index_code, result) in enumerate(all_results.items()):
                    if i < 35:  # Excel has a limit on number of sheets
                        result['composition_df'].to_excel(writer, sheet_name=f'{index_code}_Comp', index=False)

            logger.info(f"Successfully processed {len(all_results)} sectorial indices")
            logger.info(f"Individual files saved: {len(saved_files)}")
            logger.info(f"Summary file: {summary_path}")

            return {
                "status": "success",
                "message": f"Review completed successfully for {len(all_results)} sectorial indices",
                "data": {
                    "individual_files": saved_files,
                    "summary_file": summary_path,
                    "processed_indices": list(all_results.keys())
                }
            }
        
        except Exception as e:
            error_msg = f"Error saving output files: {str(e)}"
            logger.error(error_msg)
            logger.error(traceback.format_exc())
            return {"status": "error", "message": error_msg, "data": None}
   
    except Exception as e:
        logger.error(f"Error during review calculation: {str(e)}")
        logger.error(traceback.format_exc())
        return {
            "status": "error",
            "message": f"Error during review calculation: {str(e)}",
            "traceback": traceback.format_exc(),
            "data": None
        }