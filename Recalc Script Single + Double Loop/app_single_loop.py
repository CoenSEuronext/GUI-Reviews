import pandas as pd
import datetime
import os
from collections import defaultdict

# ============================================================================
# CONFIGURATION
# ============================================================================

# Date range for recalculation
START_DATE = "20250623"
END_DATE = "20250919"

# Stock prices dictionary
stock_prices = {
    "LLYVA.O": 0,
    "SPR.N": 0
}

# Free Float Coefficient replacements
# Format: {('ISIN', 'Index'): new_free_float_coeff}
free_float_replacements = {

}

# Divisor replacements
# Format: {'Mnemo': new_divisor}
divisor_replacements = {

}

# List of Price Index mnemonics to process
mnemonics = [
    "EDWPT",
    "EWSL",
    "EWMS",
    "DNAPT",
    "EUSPT",
    "EWSL",
    "EUMS"
    
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

# Base path for data files
BASE_PATH = r"\\pbgfshqa08601v\gis_ttm\Archive"

# Output path
OUTPUT_BASE_PATH = r"C:\Users\CSonneveld\OneDrive - Euronext\Documents\Projects\Modular Recalc Script - Copy\Output"

# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def get_previous_business_day(date_str):
    """Get previous business day (skip weekends)"""
    date_obj = datetime.datetime.strptime(date_str, "%Y%m%d")
    prev_day = date_obj - datetime.timedelta(days=1)
    while prev_day.weekday() > 4:
        prev_day = prev_day - datetime.timedelta(days=1)
    return prev_day.strftime("%Y%m%d")

def generate_business_days(start_date_str, end_date_str):
    """Generate list of business days between start and end dates (inclusive)"""
    start_date = datetime.datetime.strptime(start_date_str, "%Y%m%d")
    end_date = datetime.datetime.strptime(end_date_str, "%Y%m%d")
    
    business_days = []
    current_date = start_date
    
    while current_date <= end_date:
        # Only add weekdays (Monday=0, Sunday=6)
        if current_date.weekday() < 5:
            business_days.append(current_date.strftime("%Y%m%d"))
        current_date += datetime.timedelta(days=1)
    
    return business_days

def load_stock_file(date_str, encoding='latin1'):
    """Load stock EOD file for a specific date"""
    file_path = os.path.join(BASE_PATH, f"TTMIndexEU1_GIS_EOD_STOCK_{date_str}.csv")
    return pd.read_csv(file_path, sep=';', encoding=encoding)

def load_index_file(date_str, encoding='latin1'):
    """Load index EOD file for a specific date"""
    file_path = os.path.join(BASE_PATH, f"TTMIndexEU1_GIS_EOD_INDEX_{date_str}.csv")
    return pd.read_csv(file_path, sep=';', encoding=encoding)

def load_data_with_encoding_fallback(current_date, prev_date):
    """Load data with encoding fallback mechanism"""
    encodings = ['latin1', 'windows-1252', 'utf-8']
    
    for encoding in encodings:
        try:
            print(f"  Trying to load data with {encoding} encoding...")
            stock_eod_df = load_stock_file(current_date, encoding)
            index_eod_df = load_index_file(current_date, encoding)
            stock_eod_df_t1 = load_stock_file(prev_date, encoding)
            index_eod_df_t1 = load_index_file(prev_date, encoding)
            
            print(f"  Successfully loaded data with {encoding} encoding")
            return stock_eod_df, index_eod_df, stock_eod_df_t1, index_eod_df_t1
            
        except Exception as e:
            print(f"  Failed with {encoding} encoding: {e}")
            continue
    
    raise Exception(f"Failed to load data for date {current_date} with any encoding")

# ============================================================================
# DATA UPDATE FUNCTIONS
# ============================================================================

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
    print(f"    Updated {updated_count} price records")
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
    print(f"    Updated {updated_count} free float coefficient records")
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
    print(f"    Updated {updated_count} divisor records")
    return index_eod_df

# ============================================================================
# CALCULATION FUNCTIONS
# ============================================================================

def recalculate_mass_values(stock_eod_df, index_eod_df, mnemonics):
    """Recalculate Effect Gross Total Return and Effect Net Total Return"""
    if 'new_Index_Mcap' not in stock_eod_df.columns:
        stock_eod_df['new_Index_Mcap'] = (
            stock_eod_df['Close Prc'] * 
            stock_eod_df['Shares'] * 
            stock_eod_df['Free float-Coeff'] * 
            stock_eod_df['Capping Factor-Coeff'] * 
            stock_eod_df['FX/Index Ccy']
        )
    
    isin_lookup = dict(zip(index_eod_df['Mnemo'], index_eod_df['IsinCode']))
    gross_isin_lookup = dict(zip(index_eod_df['IsinCode'], index_eod_df['ISIN Gross Return version']))
    net_isin_lookup = dict(zip(index_eod_df['IsinCode'], index_eod_df['ISIN Net Return version']))
    
    for mnemo in mnemonics:
        index_stocks = stock_eod_df[stock_eod_df['Index'] == mnemo].copy()
        
        if index_stocks.empty:
            continue
        
        total_mcap = index_stocks['new_Index_Mcap'].sum()
        
        if total_mcap == 0:
            continue
        
        index_stocks['new_Pct_Wght'] = index_stocks['new_Index_Mcap'] / total_mcap
        
        index_stocks['gross_mass_per_stock'] = (
            index_stocks['Source gross div'] *
            index_stocks['Dividend Rate'] *
            index_stocks['Shares'] *
            index_stocks['Free float-Coeff'] *
            index_stocks['Capping Factor-Coeff'] *
            index_stocks['new_Pct_Wght']
        )
        
        index_stocks['net_mass_per_stock'] = (
            index_stocks['Source net div'] *
            index_stocks['Dividend Rate'] *
            index_stocks['Shares'] *
            index_stocks['Free float-Coeff'] *
            index_stocks['Capping Factor-Coeff'] *
            index_stocks['new_Pct_Wght']
        )
        
        total_gross_mass = index_stocks['gross_mass_per_stock'].sum()
        total_net_mass = index_stocks['net_mass_per_stock'].sum()
        
        divisor_row = index_eod_df[index_eod_df['Mnemo'] == mnemo]
        if divisor_row.empty:
            continue
        
        divisor = divisor_row['Divisor'].values[0]
        
        if divisor == 0 or pd.isna(divisor):
            continue
        
        effect_gross_total_return = total_gross_mass / divisor
        effect_net_total_return = total_net_mass / divisor
        
        price_isin = isin_lookup.get(mnemo, None)
        
        if price_isin is None:
            continue
        
        gross_isin = gross_isin_lookup.get(price_isin, None)
        net_isin = net_isin_lookup.get(price_isin, None)
        
        if gross_isin is not None:
            gross_mask = index_eod_df['IsinCode'] == gross_isin
            if gross_mask.any():
                index_eod_df.loc[gross_mask, 'Effect Gross Total Return'] = effect_gross_total_return
        
        if net_isin is not None:
            net_mask = index_eod_df['IsinCode'] == net_isin
            if net_mask.any():
                index_eod_df.loc[net_mask, 'Effect Net Total Return '] = effect_net_total_return
    
    return index_eod_df

def calculate_index_levels(stock_eod_df, index_eod_df, index_eod_df_t1, mnemonics):
    """Calculate index levels for the given mnemonics"""
    stock_eod_df['new_Index_Mcap'] = (
        stock_eod_df['Close Prc'] * 
        stock_eod_df['Shares'] * 
        stock_eod_df['Free float-Coeff'] * 
        stock_eod_df['Capping Factor-Coeff'] * 
        stock_eod_df['FX/Index Ccy']
    )
    
    isin_lookup = dict(zip(index_eod_df['Mnemo'], index_eod_df['IsinCode']))
    gross_isin_lookup = dict(zip(index_eod_df['IsinCode'], index_eod_df['ISIN Gross Return version']))
    net_isin_lookup = dict(zip(index_eod_df['IsinCode'], index_eod_df['ISIN Net Return version']))
    gross_mass_lookup = dict(zip(index_eod_df['IsinCode'], index_eod_df['Effect Gross Total Return']))
    net_mass_lookup = dict(zip(index_eod_df['IsinCode'], index_eod_df['Effect Net Total Return ']))
    
    price_t1_lookup = {}
    gross_t1_lookup = {}
    net_t1_lookup = {}
    
    for idx, row in index_eod_df_t1.iterrows():
        if not pd.isna(row['IsinCode']):
            t1_value = row['t0 IV unround'] if 't0 IV unround' in index_eod_df_t1.columns else None
            price_t1_lookup[row['IsinCode']] = t1_value
            gross_t1_lookup[row['IsinCode']] = t1_value
            net_t1_lookup[row['IsinCode']] = t1_value
    
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
        
        if price_t1 is not None and price_t1 != 0 and gross_t1 is not None:
            if price_level_round is not None and gross_mass is not None:
                gross_level_unrounded = gross_t1 * ((price_level_round + gross_mass) / price_t1)
                gross_level = round(gross_level_unrounded, 8)

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
        dit_1_row = index_eod_df_t1[index_eod_df_t1['Mnemo'] == mnemo]
        if not dit_1_row.empty and 't0 IV unround' in index_eod_df_t1.columns:
            result['DIt_1'] = dit_1_row['t0 IV unround'].values[0]
        else:
            result['Error_Message'] = f"DIt-1 not found for mnemo {mnemo}"
        
        underlying_level_t1 = None
        underlying_level = None
        
        gross_match = results_df[results_df['Gross_Isin'] == isin]
        if not gross_match.empty:
            underlying_level_t1 = gross_match['Gross_t-1'].iloc[0]
            underlying_level = gross_match['Gross_Level'].iloc[0]
        else:
            net_match = results_df[results_df['Net_Isin'] == isin]
            if not net_match.empty:
                underlying_level_t1 = net_match['Net_t-1'].iloc[0]
                underlying_level = net_match['Net_Level'].iloc[0]
        
        if underlying_level_t1 is None:
            uit_1_row = index_eod_df_t1[index_eod_df_t1['IsinCode'] == isin]
            if not uit_1_row.empty and 't0 IV unround' in index_eod_df_t1.columns:
                underlying_level_t1 = uit_1_row['t0 IV unround'].values[0]
        
        result['UIt_1'] = underlying_level_t1
        if underlying_level_t1 is None:
            result['Error_Message'] = f"UIt-1 not found for ISIN {isin}"
        
        if underlying_level is None:
            uit_current_row = index_eod_df[index_eod_df['IsinCode'] == isin]
            if not uit_current_row.empty and 't0 IV unround' in index_eod_df.columns:
                underlying_level = uit_current_row['t0 IV unround'].values[0]
        
        result['UIt'] = underlying_level
        if underlying_level is None:
            result['Error_Message'] = f"UIt not found for ISIN {isin}"
        
        dcr_row = index_eod_df[index_eod_df['Mnemo'] == mnemo]
        if not dcr_row.empty and 'Return Value' in index_eod_df.columns:
            result['Dcr'] = dcr_row['Return Value'].values[0]
        else:
            result['Error_Message'] = f"Dcr (Return Value) not found for mnemo {mnemo}"
        
        yearly_days_row = index_eod_df[index_eod_df['Mnemo'] == mnemo]
        if not yearly_days_row.empty and 'Yearly Days' in index_eod_df.columns:
            result['Yearly_Days'] = yearly_days_row['Yearly Days'].values[0]
        else:
            result['Error_Message'] = f"Yearly Days not found for mnemo {mnemo}"
            result['Yearly_Days'] = 365
        
        result['Day'] = (current_date - prev_date).days
        
        if result['UIt'] is not None and result['UIt_1'] is not None and result['UIt_1'] != 0:
            result['Ratio_UIt_UIt_1'] = result['UIt'] / result['UIt_1']
        
        if result['Dcr'] is not None and result['Day'] is not None and result['Yearly_Days'] is not None:
            result['Decrement_Factor'] = result['Dcr'] * result['Day'] / result['Yearly_Days']
        
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
        underlying_index_row = index_eod_df[index_eod_df['Mnemo'] == mnemo]
        if not underlying_index_row.empty and 'ISIN Underlying Price Index' in index_eod_df.columns:
            result['Underlying_Index'] = underlying_index_row['ISIN Underlying Price Index'].values[0]
        else:
            result['Underlying_Index'] = None
        
        dpit_1_row = index_eod_df_t1[index_eod_df_t1['Mnemo'] == mnemo]
        if not dpit_1_row.empty and 't0 IV unround' in index_eod_df_t1.columns:
            result['DPIt_1'] = dpit_1_row['t0 IV unround'].values[0]
        else:
            result['Error_Message'] = f"DPIt-1 not found for mnemo {mnemo}"
        
        underlying_level_t1 = None
        underlying_level = None
        
        gross_match = results_df[results_df['Gross_Isin'] == isin]
        if not gross_match.empty:
            underlying_level_t1 = gross_match['Gross_t-1'].iloc[0]
            underlying_level = gross_match['Gross_Level'].iloc[0]
        else:
            net_match = results_df[results_df['Net_Isin'] == isin]
            if not net_match.empty:
                underlying_level_t1 = net_match['Net_t-1'].iloc[0]
                underlying_level = net_match['Net_Level'].iloc[0]
        
        if underlying_level_t1 is None:
            durt_1_row = index_eod_df_t1[index_eod_df_t1['IsinCode'] == isin]
            if not durt_1_row.empty and 't0 IV unround' in index_eod_df_t1.columns:
                underlying_level_t1 = durt_1_row['t0 IV unround'].values[0]
        
        result['DuRt_1'] = underlying_level_t1
        if underlying_level_t1 is None:
            result['Error_Message'] = f"DuRt-1 not found for ISIN {isin}"
        
        if underlying_level is None:
            durt_current_row = index_eod_df[index_eod_df['IsinCode'] == isin]
            if not durt_current_row.empty and 't0 IV unround' in index_eod_df.columns:
                underlying_level = durt_current_row['t0 IV unround'].values[0]
        
        result['DuRt'] = underlying_level
        if underlying_level is None:
            result['Error_Message'] = f"DuRt not found for ISIN {isin}"
        
        points_row = index_eod_df[index_eod_df['Mnemo'] == mnemo]
        if not points_row.empty and 'Return Value' in index_eod_df.columns:
            result['Points'] = points_row['Return Value'].values[0]
        else:
            result['Error_Message'] = f"Points (Return Value) not found for mnemo {mnemo}"
        
        yearly_days_row = index_eod_df[index_eod_df['Mnemo'] == mnemo]
        if not yearly_days_row.empty and 'Yearly Days' in index_eod_df.columns:
            result['Yearly_Days'] = yearly_days_row['Yearly Days'].values[0]
        else:
            result['Error_Message'] = f"Yearly Days not found for mnemo {mnemo}"
            result['Yearly_Days'] = 365
        
        result['Day'] = (current_date - prev_date).days
        
        if result['DuRt'] is not None and result['DuRt_1'] is not None and result['DuRt_1'] != 0:
            result['Ratio_DuRt_DuRt_1'] = result['DuRt'] / result['DuRt_1']
        
        if result['Points'] is not None and result['Day'] is not None and result['Yearly_Days'] is not None:
            result['Points_Factor'] = result['Points'] * result['Day'] / result['Yearly_Days']
        
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

def update_index_with_calculated_levels(index_eod_df, results_df, decrement_df, decrement_points_df):
    """Update index_eod_df with calculated levels to use as next day's T-1"""
    index_eod_df = index_eod_df.copy()
    
    # Update Price, Gross, and Net levels
    for idx, row in results_df.iterrows():
        # Update Price Index level
        if row['Price_Isin'] is not None and row['Price_Level_Round'] is not None:
            mask = index_eod_df['IsinCode'] == row['Price_Isin']
            if mask.any():
                index_eod_df.loc[mask, 't0 IV unround'] = row['Price_Level_Round']
        
        # Update Gross level
        if row['Gross_Isin'] is not None and row['Gross_Level'] is not None:
            mask = index_eod_df['IsinCode'] == row['Gross_Isin']
            if mask.any():
                index_eod_df.loc[mask, 't0 IV unround'] = row['Gross_Level']
        
        # Update Net level
        if row['Net_Isin'] is not None and row['Net_Level'] is not None:
            mask = index_eod_df['IsinCode'] == row['Net_Isin']
            if mask.any():
                index_eod_df.loc[mask, 't0 IV unround'] = row['Net_Level']
    
    # Update Decrement Percentage levels
    for idx, row in decrement_df.iterrows():
        if row['Decrement_Level'] is not None:
            mask = index_eod_df['Mnemo'] == row['Mnemo']
            if mask.any():
                index_eod_df.loc[mask, 't0 IV unround'] = row['Decrement_Level']
    
    # Update Decrement Points levels
    for idx, row in decrement_points_df.iterrows():
        if row['Decrement_Points_Level'] is not None:
            mask = index_eod_df['Mnemo'] == row['Mnemo']
            if mask.any():
                index_eod_df.loc[mask, 't0 IV unround'] = row['Decrement_Points_Level']
    
    return index_eod_df

# ============================================================================
# OUTPUT FUNCTIONS
# ============================================================================

def save_multi_day_results(all_results, timestamp):
    """Save all results to Excel with sheets organized by index"""
    output_path = os.path.join(OUTPUT_BASE_PATH, f"Multi_Day_Recalc_{START_DATE}_to_{END_DATE}_{timestamp}.xlsx")
    
    # Organize data by index
    price_data = defaultdict(list)
    gross_data = defaultdict(list)
    net_data = defaultdict(list)
    decrement_perc_data = []
    decrement_points_data = []
    
    for date, results in all_results:
        results_df = results['results_df']
        decrement_df = results['decrement_df']
        decrement_points_df = results['decrement_points_df']
        
        # Organize by index
        for idx, row in results_df.iterrows():
            index_name = row['Index']
            price_data[index_name].append({
                'Date': date,
                'Price_Level': row['Price_Level_Round'],
                'Total_Mcap': row['Total_Index_Mcap'],
                'Divisor': row['Divisor'],
                'Price_t-1': row['Price_t-1']
            })
            
            gross_data[index_name].append({
                'Date': date,
                'Gross_Level': row['Gross_Level'],
                'Gross_Mass': row['Gross_Mass'],
                'Gross_t-1': row['Gross_t-1']
            })
            
            net_data[index_name].append({
                'Date': date,
                'Net_Level': row['Net_Level'],
                'Net_Mass': row['Net_Mass'],
                'Net_t-1': row['Net_t-1']
            })
        
        # Collect decrement data
        for idx, row in decrement_df.iterrows():
            decrement_perc_data.append({
                'Date': date,
                'Mnemo': row['Mnemo'],
                'Decrement_Level': row['Decrement_Level'],
                'DIt_1': row['DIt_1'],
                'UIt': row['UIt'],
                'UIt_1': row['UIt_1'],
                'Dcr': row['Dcr'],
                'Error_Message': row['Error_Message']
            })
        
        for idx, row in decrement_points_df.iterrows():
            decrement_points_data.append({
                'Date': date,
                'Mnemo': row['Mnemo'],
                'Decrement_Points_Level': row['Decrement_Points_Level'],
                'DPIt_1': row['DPIt_1'],
                'DuRt': row['DuRt'],
                'DuRt_1': row['DuRt_1'],
                'Points': row['Points'],
                'Error_Message': row['Error_Message']
            })
    
    # Write to Excel
    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        # Write price sheets
        for index_name in sorted(price_data.keys()):
            df = pd.DataFrame(price_data[index_name])
            sheet_name = f"{index_name}_Price"
            df.to_excel(writer, sheet_name=sheet_name, index=False)
        
        # Write gross sheets
        for index_name in sorted(gross_data.keys()):
            df = pd.DataFrame(gross_data[index_name])
            sheet_name = f"{index_name}_Gross"
            df.to_excel(writer, sheet_name=sheet_name, index=False)
        
        # Write net sheets
        for index_name in sorted(net_data.keys()):
            df = pd.DataFrame(net_data[index_name])
            sheet_name = f"{index_name}_Net"
            df.to_excel(writer, sheet_name=sheet_name, index=False)
        
        # Write decrement sheets
        if decrement_perc_data:
            df = pd.DataFrame(decrement_perc_data)
            df.to_excel(writer, sheet_name='Decrement_Percentage', index=False)
        
        if decrement_points_data:
            df = pd.DataFrame(decrement_points_data)
            df.to_excel(writer, sheet_name='Decrement_Points', index=False)
    
    print(f"\nResults saved to: {output_path}")
    return output_path

# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    
    print("\n" + "="*80)
    print("MULTI-DAY INDEX RECALCULATION SCRIPT")
    print("="*80)
    print(f"Start Date: {START_DATE}")
    print(f"End Date: {END_DATE}")
    print(f"Timestamp: {timestamp}")
    print("="*80 + "\n")
    
    # Generate business days
    business_days = generate_business_days(START_DATE, END_DATE)
    print(f"Processing {len(business_days)} business days")
    print(f"Dates: {business_days[0]} to {business_days[-1]}\n")
    
    # Storage for results
    all_results = []
    previous_index_eod_df = None
    
    # Process each day
    for i, current_date in enumerate(business_days):
        print("="*80)
        print(f"PROCESSING DAY {i+1}/{len(business_days)}: {current_date}")
        print("="*80)
        
        try:
            # Get previous date
            if i == 0:
                # First iteration - load from file
                prev_date = get_previous_business_day(current_date)
                print(f"First day - loading T-1 from file: {prev_date}")
            else:
                # Use previous business day from our list
                prev_date = business_days[i-1]
                print(f"Using previous recalculated day as T-1: {prev_date}")
            
            # Load current day's data
            print("\nLoading data...")
            if i == 0:
                # First iteration - load both files
                stock_eod_df, index_eod_df, stock_eod_df_t1, index_eod_df_t1 = load_data_with_encoding_fallback(current_date, prev_date)
            else:
                # Use recalculated previous day as T-1
                stock_eod_df, index_eod_df, stock_eod_df_t1, _ = load_data_with_encoding_fallback(current_date, prev_date)
                index_eod_df_t1 = previous_index_eod_df
                print(f"  Using recalculated index data from {prev_date} as T-1")
            
            # Convert dates for day calculation
            current_date_obj = datetime.datetime.strptime(current_date, "%Y%m%d")
            prev_date_obj = datetime.datetime.strptime(prev_date, "%Y%m%d")
            
            # Apply replacements
            print("\n  Applying replacements...")
            stock_eod_df = update_stock_prices(stock_eod_df)
            stock_eod_df = update_free_float_coefficients(stock_eod_df)
            index_eod_df = update_divisors(index_eod_df)
            
            # Recalculate mass values
            print("\n  Recalculating mass values...")
            index_eod_df = recalculate_mass_values(stock_eod_df, index_eod_df, mnemonics)
            
            # Calculate index levels
            print("\n  Calculating index levels...")
            results_df = calculate_index_levels(stock_eod_df, index_eod_df, index_eod_df_t1, mnemonics)
            
            # Calculate decrement percentage levels
            print("\n  Calculating decrement percentage levels...")
            decrement_results = []
            for mnemo, isin in mnemonics_tr4_perc.items():
                result = calculate_decrement_level(mnemo, isin, index_eod_df, index_eod_df_t1, results_df, current_date_obj, prev_date_obj)
                decrement_results.append(result)
            decrement_df = pd.DataFrame(decrement_results)
            
            # Calculate decrement points levels
            print("\n  Calculating decrement points levels...")
            decrement_points_results = []
            for mnemo, isin in mnemonics_tr4_points.items():
                result = calculate_decrement_points_level(mnemo, isin, index_eod_df, index_eod_df_t1, results_df, current_date_obj, prev_date_obj)
                decrement_points_results.append(result)
            decrement_points_df = pd.DataFrame(decrement_points_results)
            
            # Update index_eod_df with calculated levels for next iteration
            index_eod_df = update_index_with_calculated_levels(index_eod_df, results_df, decrement_df, decrement_points_df)
            
            # Store results
            all_results.append((current_date, {
                'results_df': results_df,
                'decrement_df': decrement_df,
                'decrement_points_df': decrement_points_df
            }))
            
            # Store for next iteration
            previous_index_eod_df = index_eod_df.copy()
            
            print(f"\n  Day {current_date} completed successfully")
            print(f"  Calculated {len(results_df)} price indices")
            print(f"  Calculated {len(decrement_df)} decrement percentage indices")
            print(f"  Calculated {len(decrement_points_df)} decrement points indices")
            
        except Exception as e:
            print(f"\n!!! ERROR processing date {current_date}: {e}")
            print("Stopping execution due to error")
            import traceback
            traceback.print_exc()
            break
    
    # Save all results
    print("\n" + "="*80)
    print("SAVING RESULTS")
    print("="*80)
    output_path = save_multi_day_results(all_results, timestamp)
    
    print("\n" + "="*80)
    print("MULTI-DAY RECALCULATION COMPLETED")
    print("="*80)
    print(f"Processed {len(all_results)} days successfully")
    print(f"Output file: {output_path}")
    print("="*80 + "\n")

if __name__ == "__main__":
    main()