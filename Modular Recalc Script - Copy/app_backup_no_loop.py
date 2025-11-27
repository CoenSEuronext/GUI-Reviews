import pandas as pd
import datetime
import os

# Input EOD to be recalculated
RELEVANT_EOD_DATE = "20250623"

# Stock prices dictionary
stock_prices = {
}

# NEW: Free Float Coefficient replacements
# Format: {('ISIN', 'Index'): new_free_float_coeff}
free_float_replacements = {
    ('NO0010345853', 'ENEU'): 0.5,
    ('GB0007980591', 'ENEU'): 1,
    ('GB00BG12Y042', 'ENEU'): 0.8,
    ('IT0003132476', 'ENEU'): 0.7,
    ('NO0010096985', 'ENEU'): 0.35,
    ('PTGAL0AM0009', 'ENEU'): 0.6,
    ('FR0011726835', 'ENEU'): 0.95,
    ('GB00BMBVGQ36', 'ENEU'): 0.45,
    ('AT0000743059', 'ENEU'): 0.45,
    ('ES0173516115', 'ENEU'): 1,
    ('GB00BP6MXD84', 'ENEU'): 1,
    ('LU0075646355', 'ENEU'): 0.75,
    ('FR0000120271', 'ENEU'): 0.9,
    ('NO0011202772', 'ENEU'): 0.35,
    ('ES0132105018', 'BREU'): 0.75,
    ('GB00BTK05J60', 'BREU'): 1,
    ('GB0000456144', 'BREU'): 0.4,
    ('LU1598757687', 'BREU'): 0.5,
    ('SE0020050417', 'BREU'): 1,
    ('GB00BL6K5J42', 'BREU'): 0.75,
    ('GB00B2QPKJ12', 'BREU'): 0.25,
    ('JE00B4T3BW64', 'BREU'): 0.8,
    ('NO0005052605', 'BREU'): 0.65,
    ('GB0007188757', 'BREU'): 0.85,
    ('SE0000108227', 'BREU'): 0.85,
    ('SE0000120669', 'BREU'): 0.85,
    ('LU2598331598', 'BREU'): 0.25,
    ('FR0013506730', 'BREU'): 0.7,
    ('AT0000937503', 'BREU'): 0.6,
}

# NEW: Divisor replacements
# Format: {'Mnemo': new_divisor}
divisor_replacements = {
    'BREU': 136988305.520272,
    'ENEU': 92295075.4649432,
}

# Get current timestamp for the output filename
timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

# Function to generate previous day's date
def get_previous_business_day(date_str):
    """Get previous business day (skip weekends)"""
    date_obj = datetime.datetime.strptime(date_str, "%Y%m%d")
    
    # Subtract one day
    prev_day = date_obj - datetime.timedelta(days=1)
    
    # Keep going back until we find a weekday (Monday=0, Sunday=6)
    while prev_day.weekday() > 4:  # Saturday=5, Sunday=6
        prev_day = prev_day - datetime.timedelta(days=1)
    
    return prev_day.strftime("%Y%m%d")


# List of Price Index mnemonics to process
mnemonics = [
    "BREU",
    "ENEU",
]

# Insert Index to be calculated + Underlying Index
mnemonics_tr4_perc = {
    "BRD5": "NLIX00005578",
    "ENED5": "NLIX00005537"
    }

# Insert Index to be calculated + Underlying Index
mnemonics_tr4_points = {
    "BRD5P": "NLIX00005586",
    "EED5P": "NLIX00005545"
}

def load_data_with_encoding_fallback():
    """Load data with encoding fallback mechanism - EU files only"""
    encodings = ['latin1', 'windows-1252', 'utf-8']
    
    # File date configuration
    current_stock_eod_date = RELEVANT_EOD_DATE
    current_index_eod_date = RELEVANT_EOD_DATE
    
    prev_stock_eod_date = get_previous_business_day(current_stock_eod_date)
    prev_index_eod_date = get_previous_business_day(current_index_eod_date)
    
    # Use only the primary path
    base_path = r"C:\Users\CSonneveld\OneDrive - Euronext\Documents\Projects\Modular Recalc Script - Copy\Data folder"
    
    for encoding in encodings:
        try:
            print(f"Trying to load data with {encoding} encoding...")
            
            # EU File paths only
            eu_stock_eod_path = os.path.join(base_path, f"TTMIndexEU1_GIS_EOD_STOCK_{current_stock_eod_date}.csv")
            eu_index_eod_path = os.path.join(base_path, f"TTMIndexEU1_GIS_EOD_INDEX_{current_index_eod_date}.csv")
            eu_stock_eod_t1_path = os.path.join(base_path, f"TTMIndexEU1_GIS_EOD_STOCK_{prev_stock_eod_date}.csv")
            eu_index_eod_t1_path = os.path.join(base_path, f"TTMIndexEU1_GIS_EOD_INDEX_{prev_index_eod_date}.csv")
            
            # Load EU dataframes only
            stock_eod_df = pd.read_csv(eu_stock_eod_path, sep=';', encoding=encoding)
            index_eod_df = pd.read_csv(eu_index_eod_path, sep=';', encoding=encoding)
            stock_eod_df_t1 = pd.read_csv(eu_stock_eod_t1_path, sep=';', encoding=encoding)
            index_eod_df_t1 = pd.read_csv(eu_index_eod_t1_path, sep=';', encoding=encoding)
            
            print(f"Successfully loaded EU data with {encoding} encoding")
            print(f"EU data sizes:")
            print(f"  Stock EOD: {len(stock_eod_df)} rows")
            print(f"  Index EOD: {len(index_eod_df)} rows")
            print(f"  Stock EOD T-1: {len(stock_eod_df_t1)} rows")
            print(f"  Index EOD T-1: {len(index_eod_df_t1)} rows")
            
            return stock_eod_df, index_eod_df, stock_eod_df_t1, index_eod_df_t1
            
        except Exception as e:
            print(f"Failed to load with {encoding} encoding: {e}")
            continue
    
    raise Exception("Failed to load data with any encoding")

def update_stock_prices(stock_eod_df):
    """Update stock prices using the stock_prices dictionary"""
    def update_close_price(row):
        symbol = str(row['#Symbol']).strip() if not pd.isna(row['#Symbol']) else ""
        if symbol in stock_prices:
            return stock_prices[symbol]
        else:
            return row['Close Prc']
    
    original_prices = stock_eod_df['Close Prc'].copy()
    stock_eod_df['Close Prc'] = stock_eod_df.apply(update_close_price, axis=1)
    
    updated_count = (original_prices != stock_eod_df['Close Prc']).sum()
    print(f"Updated {updated_count} price records from the stock_prices dictionary")
    
    return stock_eod_df

def update_free_float_coefficients(stock_eod_df):
    """Update free float coefficients using the free_float_replacements dictionary"""
    def update_free_float(row):
        isin = str(row['Isin Code']).strip() if not pd.isna(row['Isin Code']) else ""
        index = str(row['Index']).strip() if not pd.isna(row['Index']) else ""
        key = (isin, index)
        
        if key in free_float_replacements:
            return free_float_replacements[key]
        else:
            return row['Free float-Coeff']
    
    original_coeffs = stock_eod_df['Free float-Coeff'].copy()
    stock_eod_df['Free float-Coeff'] = stock_eod_df.apply(update_free_float, axis=1)
    
    updated_count = (original_coeffs != stock_eod_df['Free float-Coeff']).sum()
    print(f"Updated {updated_count} free float coefficient records from the free_float_replacements dictionary")
    
    return stock_eod_df

def update_divisors(index_eod_df):
    """Update divisors using the divisor_replacements dictionary"""
    def update_divisor(row):
        mnemo = str(row['Mnemo']).strip() if not pd.isna(row['Mnemo']) else ""
        
        if mnemo in divisor_replacements:
            return divisor_replacements[mnemo]
        else:
            return row['Divisor']
    
    original_divisors = index_eod_df['Divisor'].copy()
    index_eod_df['Divisor'] = index_eod_df.apply(update_divisor, axis=1)
    
    updated_count = (original_divisors != index_eod_df['Divisor']).sum()
    print(f"Updated {updated_count} divisor records from the divisor_replacements dictionary")
    
    return index_eod_df

def recalculate_mass_values(stock_eod_df, index_eod_df, mnemonics):
    """Recalculate Effect Gross Total Return and Effect Net Total Return for the given mnemonics"""
    print("\n" + "="*80)
    print("RECALCULATING GROSS AND NET MASS VALUES")
    print("="*80)
    
    # Ensure new_Index_Mcap is calculated
    if 'new_Index_Mcap' not in stock_eod_df.columns:
        stock_eod_df['new_Index_Mcap'] = (
            stock_eod_df['Close Prc'] * 
            stock_eod_df['Shares'] * 
            stock_eod_df['Free float-Coeff'] * 
            stock_eod_df['Capping Factor-Coeff'] * 
            stock_eod_df['FX/Index Ccy']
        )
    
    # Create lookup dictionaries for finding Gross and Net ISINs
    isin_lookup = dict(zip(index_eod_df['Mnemo'], index_eod_df['IsinCode']))
    gross_isin_lookup = dict(zip(index_eod_df['IsinCode'], index_eod_df['ISIN Gross Return version']))
    net_isin_lookup = dict(zip(index_eod_df['IsinCode'], index_eod_df['ISIN Net Return version']))
    
    # Process each mnemo
    for mnemo in mnemonics:
        print(f"\n{'-'*80}")
        print(f"Processing Index: {mnemo}")
        print(f"{'-'*80}")
        
        # Filter stocks for this index
        index_stocks = stock_eod_df[stock_eod_df['Index'] == mnemo].copy()
        
        if index_stocks.empty:
            print(f"  WARNING: No stocks found for index {mnemo}")
            continue
        
        print(f"  Found {len(index_stocks)} stocks in index")
        
        # Calculate total market cap for this index
        total_mcap = index_stocks['new_Index_Mcap'].sum()
        
        if total_mcap == 0:
            print(f"  WARNING: Total market cap is zero for index {mnemo}")
            continue
        
        print(f"  Total Market Cap: {total_mcap:,.2f}")
        
        # Calculate new Pct Wght for each stock
        index_stocks['new_Pct_Wght'] = index_stocks['new_Index_Mcap'] / total_mcap
        
        # Calculate gross mass per stock
        index_stocks['gross_mass_per_stock'] = (
            index_stocks['Source gross div'] *
            index_stocks['Dividend Rate'] *
            index_stocks['Shares'] *
            index_stocks['Free float-Coeff'] *
            index_stocks['Capping Factor-Coeff'] *
            index_stocks['new_Pct_Wght']
        )
        
        # Calculate net mass per stock
        index_stocks['net_mass_per_stock'] = (
            index_stocks['Source net div'] *
            index_stocks['Dividend Rate'] *
            index_stocks['Shares'] *
            index_stocks['Free float-Coeff'] *
            index_stocks['Capping Factor-Coeff'] *
            index_stocks['new_Pct_Wght']
        )
        
        # Sum across all stocks in the index
        total_gross_mass = index_stocks['gross_mass_per_stock'].sum()
        total_net_mass = index_stocks['net_mass_per_stock'].sum()
        
        print(f"  Total Gross Mass (before divisor): {total_gross_mass:.10f}")
        print(f"  Total Net Mass (before divisor): {total_net_mass:.10f}")
        
        # Get the divisor for this index
        divisor_row = index_eod_df[index_eod_df['Mnemo'] == mnemo]
        if divisor_row.empty:
            print(f"  WARNING: Divisor not found for mnemo {mnemo}")
            continue
        
        divisor = divisor_row['Divisor'].values[0]
        
        if divisor == 0 or pd.isna(divisor):
            print(f"  WARNING: Invalid divisor ({divisor}) for mnemo {mnemo}")
            continue
        
        print(f"  Divisor: {divisor:,.10f}")
        
        # Calculate final Effect values
        effect_gross_total_return = total_gross_mass / divisor
        effect_net_total_return = total_net_mass / divisor
        
        print(f"\n  CALCULATED VALUES:")
        print(f"    New Effect Gross Total Return: {effect_gross_total_return:.10f}")
        print(f"    New Effect Net Total Return:   {effect_net_total_return:.10f}")
        
        # Get the Price Index ISIN
        price_isin = isin_lookup.get(mnemo, None)
        
        if price_isin is None:
            print(f"  WARNING: Price ISIN not found for mnemo {mnemo}")
            continue
        
        # Get Gross and Net Index ISINs
        gross_isin = gross_isin_lookup.get(price_isin, None)
        net_isin = net_isin_lookup.get(price_isin, None)
        
        print(f"\n  ISIN MAPPING:")
        print(f"    Price Index ISIN: {price_isin}")
        print(f"    Gross Index ISIN: {gross_isin}")
        print(f"    Net Index ISIN:   {net_isin}")
        
        # Update the index_eod_df with new Effect Gross Total Return
        if gross_isin is not None:
            gross_mask = index_eod_df['IsinCode'] == gross_isin
            if gross_mask.any():
                old_value = index_eod_df.loc[gross_mask, 'Effect Gross Total Return'].values[0]
                index_eod_df.loc[gross_mask, 'Effect Gross Total Return'] = effect_gross_total_return
                print(f"\n  GROSS INDEX UPDATE:")
                print(f"    Old Value: {old_value:.10f}")
                print(f"    New Value: {effect_gross_total_return:.10f}")
                print(f"    Change:    {effect_gross_total_return - old_value:.10f}")
            else:
                print(f"  WARNING: Gross ISIN {gross_isin} not found in index_eod_df")
        
        # Update the index_eod_df with new Effect Net Total Return
        if net_isin is not None:
            net_mask = index_eod_df['IsinCode'] == net_isin
            if net_mask.any():
                old_value = index_eod_df.loc[net_mask, 'Effect Net Total Return '].values[0]
                index_eod_df.loc[net_mask, 'Effect Net Total Return '] = effect_net_total_return
                print(f"\n  NET INDEX UPDATE:")
                print(f"    Old Value: {old_value:.10f}")
                print(f"    New Value: {effect_net_total_return:.10f}")
                print(f"    Change:    {effect_net_total_return - old_value:.10f}")
            else:
                print(f"  WARNING: Net ISIN {net_isin} not found in index_eod_df")
    
    print("\n" + "="*80)
    print("MASS RECALCULATION COMPLETE")
    print("="*80 + "\n")
    
    return index_eod_df

def calculate_index_levels(stock_eod_df, index_eod_df, index_eod_df_t1, mnemonics):
    """Calculate index levels for the given mnemonics"""
    # Calculate new_Index_Mcap
    stock_eod_df['new_Index_Mcap'] = (
        stock_eod_df['Close Prc'] * 
        stock_eod_df['Shares'] * 
        stock_eod_df['Free float-Coeff'] * 
        stock_eod_df['Capping Factor-Coeff'] * 
        stock_eod_df['FX/Index Ccy']
    )
    
    # Create lookup dictionaries
    isin_lookup = dict(zip(index_eod_df['Mnemo'], index_eod_df['IsinCode']))
    gross_isin_lookup = dict(zip(index_eod_df['IsinCode'], index_eod_df['ISIN Gross Return version']))
    net_isin_lookup = dict(zip(index_eod_df['IsinCode'], index_eod_df['ISIN Net Return version']))
    gross_mass_lookup = dict(zip(index_eod_df['IsinCode'], index_eod_df['Effect Gross Total Return']))
    net_mass_lookup = dict(zip(index_eod_df['IsinCode'], index_eod_df['Effect Net Total Return ']))
    
    # Create t-1 lookup dictionaries
    price_t1_lookup = {}
    gross_t1_lookup = {}
    net_t1_lookup = {}
    
    for idx, row in index_eod_df_t1.iterrows():
        if not pd.isna(row['IsinCode']):
            t1_value = row['t0 IV unround'] if 't0 IV unround' in index_eod_df_t1.columns else None
            price_t1_lookup[row['IsinCode']] = t1_value
            gross_t1_lookup[row['IsinCode']] = t1_value
            net_t1_lookup[row['IsinCode']] = t1_value
    
    # Calculate results
    results = []
    for mnemo in mnemonics:
        filtered_df = stock_eod_df[stock_eod_df['Index'] == mnemo]
        total_mcap = filtered_df['new_Index_Mcap'].sum()
        
        divisor_row = index_eod_df[index_eod_df['Mnemo'] == mnemo]
        divisor = divisor_row['Divisor'].values[0] if not divisor_row.empty else None
        
        price_level = total_mcap / divisor if divisor is not None and divisor != 0 else None
        price_level_round = round(price_level, 8) if price_level is not None else None
        
        price_isin = isin_lookup.get(mnemo, None)
        gross_isin = gross_isin_lookup.get(price_isin, None) if price_isin else None
        net_isin = net_isin_lookup.get(price_isin, None) if price_isin else None
        
        gross_mass = gross_mass_lookup.get(gross_isin, None) if gross_isin else None
        net_mass = net_mass_lookup.get(net_isin, None) if net_isin else None
        
        price_t1 = price_t1_lookup.get(price_isin, None) if price_isin else None
        gross_t1 = gross_t1_lookup.get(gross_isin, None) if gross_isin else None
        net_t1 = net_t1_lookup.get(net_isin, None) if net_isin else None
        
        gross_level_unrounded = None
        net_level_unrounded = None
        gross_level = None
        net_level = None
        
        # Calculate gross level if gross_t1 is available
        if price_t1 is not None and price_t1 != 0 and gross_t1 is not None:
            if price_level_round is not None and gross_mass is not None:
                gross_level_unrounded = gross_t1 * ((price_level_round + gross_mass) / price_t1)
                gross_level = round(gross_level_unrounded, 8)

        # Calculate net level if net_t1 is available (independent of gross_t1)
        if price_t1 is not None and price_t1 != 0 and net_t1 is not None:
            if price_level_round is not None and net_mass is not None:
                net_level_unrounded = net_t1 * ((price_level_round + net_mass) / price_t1)
                net_level = round(net_level_unrounded, 8)
        
        results.append({
            'Index': mnemo, 
            'Total_Index_Mcap': total_mcap,
            'Divisor': divisor,
            'Price_Level': price_level,
            'Price_Level_Round': price_level_round,
            'Price_Isin': price_isin,
            'Price_t-1': price_t1,
            'Gross_Isin': gross_isin,
            'Net_Isin': net_isin,
            'Gross_Mass': gross_mass,
            'Net_Mass': net_mass,
            'Gross_t-1': gross_t1,
            'Net_t-1': net_t1,
            'Gross_Level_Unrounded': gross_level_unrounded,
            'Gross_Level': gross_level,
            'Net_Level_Unrounded': net_level_unrounded,
            'Net_Level': net_level
        })

    return pd.DataFrame(results)

def calculate_decrement_level(mnemo, isin, index_eod_df, index_eod_df_t1, results_df, current_date, prev_date):
    """Calculate decrement level using the formula: DIt = DIt-1 * (UIt / UIt-1 - Dcr * day / yearly_days)"""
    result = {
        'Mnemo': mnemo,
        'ISIN': isin,
        'DIt_1': None,
        'UIt_1': None,
        'UIt': None,
        'Dcr': None,
        'Day': None,
        'Yearly_Days': None,
        'Ratio_UIt_UIt_1': None,
        'Decrement_Factor': None,
        'Decrement_Level': None,
        'Error_Message': None
    }
    
    try:
        # Get DIt-1
        dit_1_row = index_eod_df_t1[index_eod_df_t1['Mnemo'] == mnemo]
        if not dit_1_row.empty and 't0 IV unround' in index_eod_df_t1.columns:
            result['DIt_1'] = dit_1_row['t0 IV unround'].values[0]
        else:
            result['Error_Message'] = f"DIt-1 not found for mnemo {mnemo}"
        
        # Get UIt-1 - Look up the ISIN from dictionary in results_df
        underlying_level_t1 = None
        underlying_level = None
        
        # First try to find the isin in Gross_Isin column
        gross_match = results_df[results_df['Gross_Isin'] == isin]
        if not gross_match.empty:
            underlying_level_t1 = gross_match['Gross_t-1'].iloc[0]
            underlying_level = gross_match['Gross_Level'].iloc[0]
        else:
            # If not found in Gross_Isin, try Net_Isin
            net_match = results_df[results_df['Net_Isin'] == isin]
            if not net_match.empty:
                underlying_level_t1 = net_match['Net_t-1'].iloc[0]
                underlying_level = net_match['Net_Level'].iloc[0]
        
        # If still not found, fallback to direct lookup
        if underlying_level_t1 is None:
            uit_1_row = index_eod_df_t1[index_eod_df_t1['IsinCode'] == isin]
            if not uit_1_row.empty and 't0 IV unround' in index_eod_df_t1.columns:
                underlying_level_t1 = uit_1_row['t0 IV unround'].values[0]
        
        result['UIt_1'] = underlying_level_t1
        if underlying_level_t1 is None:
            result['Error_Message'] = f"UIt-1 not found for ISIN {isin}"
        
        # Get UIt - use the value found above or fallback to direct lookup
        if underlying_level is None:
            uit_current_row = index_eod_df[index_eod_df['IsinCode'] == isin]
            if not uit_current_row.empty and 't0 IV unround' in index_eod_df.columns:
                underlying_level = uit_current_row['t0 IV unround'].values[0]
        
        result['UIt'] = underlying_level
        if underlying_level is None:
            result['Error_Message'] = f"UIt not found for ISIN {isin}"
        
        # Get Dcr
        dcr_row = index_eod_df[index_eod_df['Mnemo'] == mnemo]
        if not dcr_row.empty and 'Return Value' in index_eod_df.columns:
            result['Dcr'] = dcr_row['Return Value'].values[0]
        else:
            result['Error_Message'] = f"Dcr (Return Value) not found for mnemo {mnemo}"
        
        # Get Yearly Days
        yearly_days_row = index_eod_df[index_eod_df['Mnemo'] == mnemo]
        if not yearly_days_row.empty and 'Yearly Days' in index_eod_df.columns:
            result['Yearly_Days'] = yearly_days_row['Yearly Days'].values[0]
        else:
            result['Error_Message'] = f"Yearly Days not found for mnemo {mnemo}"
            result['Yearly_Days'] = 365
        
        # Calculate day from file dates
        result['Day'] = (current_date - prev_date).days
        
        # Calculate intermediate values
        if result['UIt'] is not None and result['UIt_1'] is not None and result['UIt_1'] != 0:
            result['Ratio_UIt_UIt_1'] = result['UIt'] / result['UIt_1']
        
        if result['Dcr'] is not None and result['Day'] is not None and result['Yearly_Days'] is not None:
            result['Decrement_Factor'] = result['Dcr'] * result['Day'] / result['Yearly_Days']
        
        # Calculate final decrement level
        if all(val is not None for val in [result['DIt_1'], result['Ratio_UIt_UIt_1'], result['Decrement_Factor']]):
            if result['Ratio_UIt_UIt_1'] >= result['Decrement_Factor']:
                dit = result['DIt_1'] * (result['Ratio_UIt_UIt_1'] - result['Decrement_Factor'])
                result['Decrement_Level'] = round(dit, 8)
            else:
                result['Error_Message'] = "Ratio smaller than decrement factor - would result in negative level"
        else:
            missing_vals = [k for k, v in result.items() if k in ['DIt_1', 'Ratio_UIt_UIt_1', 'Decrement_Factor'] and v is None]
            result['Error_Message'] = f"Missing values for calculation: {missing_vals}"
            
    except Exception as e:
        result['Error_Message'] = f"Exception: {str(e)}"
    
    return result

def calculate_decrement_points_level(mnemo, isin, index_eod_df, index_eod_df_t1, results_df, current_date, prev_date):
    """Calculate decrement points level using the formula: DPIt = DPIt-1 * (DuRt / DuRt-1 - Points * day / yearly_days)"""
    result = {
        'Mnemo': mnemo,
        'ISIN': isin,
        'Underlying_Index': None,
        'DPIt_1': None,
        'DuRt_1': None,
        'DuRt': None,
        'Points': None,
        'Day': None,
        'Yearly_Days': None,
        'Ratio_DuRt_DuRt_1': None,
        'Points_Factor': None,
        'Decrement_Points_Level': None,
        'Error_Message': None
    }
    
    try:
        # Get Underlying Index
        underlying_index_row = index_eod_df[index_eod_df['Mnemo'] == mnemo]
        if not underlying_index_row.empty and 'ISIN Underlying Price Index' in index_eod_df.columns:
            result['Underlying_Index'] = underlying_index_row['ISIN Underlying Price Index'].values[0]
        else:
            result['Underlying_Index'] = None
        
        # Get DPIt-1
        dpit_1_row = index_eod_df_t1[index_eod_df_t1['Mnemo'] == mnemo]
        if not dpit_1_row.empty and 't0 IV unround' in index_eod_df_t1.columns:
            result['DPIt_1'] = dpit_1_row['t0 IV unround'].values[0]
        else:
            result['Error_Message'] = f"DPIt-1 not found for mnemo {mnemo}"
        
        # Get DuRt-1 - Look up the ISIN from dictionary in results_df
        underlying_level_t1 = None
        underlying_level = None
        
        # First try to find the isin in Gross_Isin column
        gross_match = results_df[results_df['Gross_Isin'] == isin]
        if not gross_match.empty:
            underlying_level_t1 = gross_match['Gross_t-1'].iloc[0]
            underlying_level = gross_match['Gross_Level'].iloc[0]
        else:
            # If not found in Gross_Isin, try Net_Isin
            net_match = results_df[results_df['Net_Isin'] == isin]
            if not net_match.empty:
                underlying_level_t1 = net_match['Net_t-1'].iloc[0]
                underlying_level = net_match['Net_Level'].iloc[0]
        
        # If still not found, fallback to direct lookup
        if underlying_level_t1 is None:
            durt_1_row = index_eod_df_t1[index_eod_df_t1['IsinCode'] == isin]
            if not durt_1_row.empty and 't0 IV unround' in index_eod_df_t1.columns:
                underlying_level_t1 = durt_1_row['t0 IV unround'].values[0]
        
        result['DuRt_1'] = underlying_level_t1
        if underlying_level_t1 is None:
            result['Error_Message'] = f"DuRt-1 not found for ISIN {isin}"
        
        # Get DuRt - use the value found above or fallback to direct lookup
        if underlying_level is None:
            durt_current_row = index_eod_df[index_eod_df['IsinCode'] == isin]
            if not durt_current_row.empty and 't0 IV unround' in index_eod_df.columns:
                underlying_level = durt_current_row['t0 IV unround'].values[0]
        
        result['DuRt'] = underlying_level
        if underlying_level is None:
            result['Error_Message'] = f"DuRt not found for ISIN {isin}"
        
        # Get Points
        points_row = index_eod_df[index_eod_df['Mnemo'] == mnemo]
        if not points_row.empty and 'Return Value' in index_eod_df.columns:
            result['Points'] = points_row['Return Value'].values[0]
        else:
            result['Error_Message'] = f"Points (Return Value) not found for mnemo {mnemo}"
        
        # Get Yearly Days
        yearly_days_row = index_eod_df[index_eod_df['Mnemo'] == mnemo]
        if not yearly_days_row.empty and 'Yearly Days' in index_eod_df.columns:
            result['Yearly_Days'] = yearly_days_row['Yearly Days'].values[0]
        else:
            result['Error_Message'] = f"Yearly Days not found for mnemo {mnemo}"
            result['Yearly_Days'] = 365
        
        # Calculate day from file dates
        result['Day'] = (current_date - prev_date).days
        
        # Calculate intermediate values
        if result['DuRt'] is not None and result['DuRt_1'] is not None and result['DuRt_1'] != 0:
            result['Ratio_DuRt_DuRt_1'] = result['DuRt'] / result['DuRt_1']
        
        if result['Points'] is not None and result['Day'] is not None and result['Yearly_Days'] is not None:
            result['Points_Factor'] = result['Points'] * result['Day'] / result['Yearly_Days']
        
        # Calculate final decrement points level
        if all(val is not None for val in [result['DPIt_1'], result['Ratio_DuRt_DuRt_1'], result['Points_Factor']]):
            if result['Ratio_DuRt_DuRt_1'] >= result['Points_Factor']:
                dpit = result['DPIt_1'] * result['Ratio_DuRt_DuRt_1'] - result['Points_Factor']
                result['Decrement_Points_Level'] = round(dpit, 8)
            else:
                result['Error_Message'] = "Ratio smaller than points factor - would result in negative level"
        else:
            missing_vals = [k for k, v in result.items() if k in ['DPIt_1', 'Ratio_DuRt_DuRt_1', 'Points_Factor'] and v is None]
            result['Error_Message'] = f"Missing values for calculation: {missing_vals}"
            
    except Exception as e:
        result['Error_Message'] = f"Exception: {str(e)}"
    
    return result

def save_results_to_excel(results_df, decrement_df, decrement_points_df, stock_eod_df, stock_eod_df_t1, index_eod_df, index_eod_df_t1, timestamp):
    """Save all results to Excel with proper sheet names"""
    output_path = fr"C:\Users\CSonneveld\OneDrive - Euronext\Documents\Projects\Modular Recalc Script - Copy\Output\Level_Recalc_{timestamp}.xlsx"
    
    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        results_df.to_excel(writer, sheet_name='Index_Totals', index=False)
        decrement_df.to_excel(writer, sheet_name='Decrement_Levels_Percentage', index=False)
        decrement_points_df.to_excel(writer, sheet_name='Decrement_Levels_Points', index=False)
        stock_eod_df.to_excel(writer, sheet_name='Stock_EOD_Data', index=False)
        stock_eod_df_t1.to_excel(writer, sheet_name='Stock_EOD_Data_T-1', index=False)
        index_eod_df.to_excel(writer, sheet_name='Index_EOD_Data', index=False)
        index_eod_df_t1.to_excel(writer, sheet_name='Index_EOD_Data_T-1', index=False)
    
    print("\n" + "="*80)
    print("RESULTS SAVED")
    print("="*80)
    print(f"Output file: {output_path}")
    print("\nSummary of results:")
    print(f"  Index Totals: {len(results_df)} rows")
    print(f"  Decrement Percentage Levels: {len(decrement_df)} rows")
    print(f"  Decrement Points Levels: {len(decrement_points_df)} rows")
    print("="*80 + "\n")
    
    return output_path

# Main execution
try:
    print("\n" + "="*80)
    print("INDEX RECALCULATION SCRIPT")
    print("="*80)
    print(f"Date: {RELEVANT_EOD_DATE}")
    print(f"Timestamp: {timestamp}")
    print("="*80 + "\n")
    
    # Define dates at the start
    current_stock_eod_date = RELEVANT_EOD_DATE
    current_index_eod_date = RELEVANT_EOD_DATE
    prev_stock_eod_date = get_previous_business_day(current_stock_eod_date)
    prev_index_eod_date = get_previous_business_day(current_index_eod_date)
    
    print(f"Current EOD Date: {current_stock_eod_date}")
    print(f"Previous EOD Date: {prev_stock_eod_date}\n")
    
    # Convert to datetime objects for day calculation
    current_date = datetime.datetime.strptime(current_stock_eod_date, "%Y%m%d")
    prev_date = datetime.datetime.strptime(prev_stock_eod_date, "%Y%m%d")
    
    # Load data (EU only now)
    stock_eod_df, index_eod_df, stock_eod_df_t1, index_eod_df_t1 = load_data_with_encoding_fallback()
    
    # Apply replacements
    print("\n" + "="*80)
    print("APPLYING REPLACEMENTS")
    print("="*80)
    stock_eod_df = update_stock_prices(stock_eod_df)
    stock_eod_df = update_free_float_coefficients(stock_eod_df)
    index_eod_df = update_divisors(index_eod_df)
    print("="*80 + "\n")
    
    # Recalculate mass values (Effect Gross Total Return and Effect Net Total Return)
    index_eod_df = recalculate_mass_values(stock_eod_df, index_eod_df, mnemonics)
    
    # Calculate index levels
    print("="*80)
    print("CALCULATING INDEX LEVELS")
    print("="*80)
    results_df = calculate_index_levels(stock_eod_df, index_eod_df, index_eod_df_t1, mnemonics)
    print(f"Calculated levels for {len(results_df)} indices")
    print("="*80 + "\n")
    
    # Calculate decrement percentage levels
    print("="*80)
    print("CALCULATING DECREMENT PERCENTAGE LEVELS")
    print("="*80)
    decrement_results = []
    for mnemo, isin in mnemonics_tr4_perc.items():
        result = calculate_decrement_level(mnemo, isin, index_eod_df, index_eod_df_t1, results_df, current_date, prev_date)
        decrement_results.append(result)
    
    decrement_df = pd.DataFrame(decrement_results)
    print(f"Calculated decrement levels for {len(decrement_df)} indices")
    print(f"Successfully calculated levels: {decrement_df['Decrement_Level'].notna().sum()}")
    print("="*80 + "\n")
    
    # Calculate decrement points levels
    print("="*80)
    print("CALCULATING DECREMENT POINTS LEVELS")
    print("="*80)
    decrement_points_results = []
    for mnemo, isin in mnemonics_tr4_points.items():
        result = calculate_decrement_points_level(mnemo, isin, index_eod_df, index_eod_df_t1, results_df, current_date, prev_date)
        decrement_points_results.append(result)
    
    decrement_points_df = pd.DataFrame(decrement_points_results)
    print(f"Calculated decrement points levels for {len(decrement_points_df)} indices")
    print(f"Successfully calculated points levels: {decrement_points_df['Decrement_Points_Level'].notna().sum()}")
    print("="*80 + "\n")
    
    # Save all results
    output_path = save_results_to_excel(results_df, decrement_df, decrement_points_df, stock_eod_df, stock_eod_df_t1, index_eod_df, index_eod_df_t1, timestamp)
    
    print("="*80)
    print("SCRIPT COMPLETED SUCCESSFULLY")
    print("="*80 + "\n")
    
except Exception as e:
    print("\n" + "="*80)
    print("SCRIPT FAILED")
    print("="*80)
    print(f"Error: {e}")
    print("="*80 + "\n")
    import traceback
    traceback.print_exc()