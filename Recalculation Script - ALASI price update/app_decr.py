import pandas as pd
import datetime
import os

# Stock prices dictionary
stock_prices = {
    "NO0013531616": 109.4,
    "NSCIT5670962": 260.6,
}

# Get current timestamp for the output filename
timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

# Function to generate previous day's date
def get_previous_day(date_str):
    # Convert string date to datetime object
    date_obj = datetime.datetime.strptime(date_str, "%Y%m%d")
    # Subtract one day
    prev_day = date_obj - datetime.timedelta(days=1)
    # Return as string in same format
    return prev_day.strftime("%Y%m%d")

# List of mnemonics to process
mnemonics = [
    "ALASI"
]

mnemonics_tr4_perc = {

    }
mnemonics_tr4_points = {
}
def load_data_with_encoding_fallback():
    """Load data with encoding fallback mechanism - combines US and EU files"""
    encodings = ['latin1', 'windows-1252', 'utf-8']
    
    # File date configuration
    current_stock_eod_date = "20250729"
    current_index_eod_date = "20250729"
    
    prev_stock_eod_date = get_previous_day(current_stock_eod_date)
    prev_index_eod_date = get_previous_day(current_index_eod_date)
    
    # Use only the primary path
    base_path = r"V:\PM-Indices-IndexOperations\General\Daily downloadfiles\Monthly Archive"
    
    for encoding in encodings:
        try:
            print(f"Trying to load data with {encoding} encoding...")
            
            # US File paths
            us_stock_eod_path = os.path.join(base_path, f"TTMIndexUS1_GIS_EOD_STOCK_{current_stock_eod_date}.csv")
            us_index_eod_path = os.path.join(base_path, f"TTMIndexUS1_GIS_EOD_INDEX_{current_index_eod_date}.csv")
            us_stock_eod_t1_path = os.path.join(base_path, f"TTMIndexUS1_GIS_EOD_STOCK_{prev_stock_eod_date}.csv")
            us_index_eod_t1_path = os.path.join(base_path, f"TTMIndexUS1_GIS_EOD_INDEX_{prev_index_eod_date}.csv")
            
            # EU File paths
            eu_stock_eod_path = os.path.join(base_path, f"TTMIndexEU1_GIS_EOD_STOCK_{current_stock_eod_date}.csv")
            eu_index_eod_path = os.path.join(base_path, f"TTMIndexEU1_GIS_EOD_INDEX_{current_index_eod_date}.csv")
            eu_stock_eod_t1_path = os.path.join(base_path, f"TTMIndexEU1_GIS_EOD_STOCK_{prev_stock_eod_date}.csv")
            eu_index_eod_t1_path = os.path.join(base_path, f"TTMIndexEU1_GIS_EOD_INDEX_{prev_index_eod_date}.csv")
            
            # Load US dataframes
            us_stock_eod_df = pd.read_csv(us_stock_eod_path, sep=';', encoding=encoding)
            us_index_eod_df = pd.read_csv(us_index_eod_path, sep=';', encoding=encoding)
            us_stock_eod_df_t1 = pd.read_csv(us_stock_eod_t1_path, sep=';', encoding=encoding)
            us_index_eod_df_t1 = pd.read_csv(us_index_eod_t1_path, sep=';', encoding=encoding)
            
            # Load EU dataframes
            eu_stock_eod_df = pd.read_csv(eu_stock_eod_path, sep=';', encoding=encoding)
            eu_index_eod_df = pd.read_csv(eu_index_eod_path, sep=';', encoding=encoding)
            eu_stock_eod_df_t1 = pd.read_csv(eu_stock_eod_t1_path, sep=';', encoding=encoding)
            eu_index_eod_df_t1 = pd.read_csv(eu_index_eod_t1_path, sep=';', encoding=encoding)
            
            # Combine US and EU dataframes
            stock_eod_df = pd.concat([us_stock_eod_df, eu_stock_eod_df], ignore_index=True)
            index_eod_df = pd.concat([us_index_eod_df, eu_index_eod_df], ignore_index=True)
            stock_eod_df_t1 = pd.concat([us_stock_eod_df_t1, eu_stock_eod_df_t1], ignore_index=True)
            index_eod_df_t1 = pd.concat([us_index_eod_df_t1, eu_index_eod_df_t1], ignore_index=True)
            
            print(f"Successfully loaded and combined US and EU data with {encoding} encoding")
            print(f"Combined data sizes:")
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
        
        if price_t1 is not None and price_t1 != 0 and gross_t1 is not None:
            if price_level_round is not None and gross_mass is not None:
                gross_level_unrounded = gross_t1 * ((price_level_round + gross_mass) / price_t1)
                gross_level = round(gross_level_unrounded, 8)
            
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

def calculate_decrement_level(mnemo, isin, index_eod_df, index_eod_df_t1, results_df):
    """Calculate decrement level using the formula: DIt = DIt−1 * (UIt / UIt−1 - Dcr * day / 365)"""
    result = {
        'Mnemo': mnemo,
        'ISIN': isin,
        'DIt_1': None,
        'UIt_1': None,
        'UIt': None,
        'Dcr': None,
        'Day': None,
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
        
        # Get day
        if 'System Date' in index_eod_df.columns and 'System Date' in index_eod_df_t1.columns:
            try:
                current_date = pd.to_datetime(index_eod_df['System Date'].iloc[0], format='%d-%m-%Y')
                previous_date = pd.to_datetime(index_eod_df_t1['System Date'].iloc[0], format='%d-%m-%Y')
                result['Day'] = (current_date - previous_date).days
            except:
                result['Day'] = 1
        else:
            result['Day'] = 1
        
        # Calculate intermediate values
        if result['UIt'] is not None and result['UIt_1'] is not None and result['UIt_1'] != 0:
            result['Ratio_UIt_UIt_1'] = result['UIt'] / result['UIt_1']
        
        if result['Dcr'] is not None and result['Day'] is not None:
            result['Decrement_Factor'] = result['Dcr'] * result['Day'] / 365
        
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

def calculate_decrement_points_level(mnemo, isin, index_eod_df, index_eod_df_t1, results_df):
    """Calculate decrement points level using the formula: DPIt = DPIt−1 * (DuRt / DuRt−1 - Points * day / 365)"""
    result = {
        'Mnemo': mnemo,
        'ISIN': isin,
        'DPIt_1': None,
        'DuRt_1': None,
        'DuRt': None,
        'Points': None,
        'Day': None,
        'Ratio_DuRt_DuRt_1': None,
        'Points_Factor': None,
        'Decrement_Points_Level': None,
        'Error_Message': None
    }
    
    try:
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
            
        # Get day
        if 'System Date' in index_eod_df.columns and 'System Date' in index_eod_df_t1.columns:
            try:
                current_date = pd.to_datetime(index_eod_df['System Date'].iloc[0], format='%d-%m-%Y')
                previous_date = pd.to_datetime(index_eod_df_t1['System Date'].iloc[0], format='%d-%m-%Y')
                result['Day'] = (current_date - previous_date).days
            except:
                result['Day'] = 1
        else:
            result['Day'] = 1
        
        # Calculate intermediate values
        if result['DuRt'] is not None and result['DuRt_1'] is not None and result['DuRt_1'] != 0:
            result['Ratio_DuRt_DuRt_1'] = result['DuRt'] / result['DuRt_1']
        
        if result['Points'] is not None and result['Day'] is not None:
            result['Points_Factor'] = result['Points'] * result['Day'] / 365
        
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
    output_path = fr"C:\Users\CSonneveld\OneDrive - Euronext\Documents\Projects\Recalculation Script - ALASI price update\Output\Decr_Recalc_{timestamp}.xlsx"
    
    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        results_df.to_excel(writer, sheet_name='Index_Totals', index=False)
        decrement_df.to_excel(writer, sheet_name='Decrement_Levels_Percentage', index=False)
        decrement_points_df.to_excel(writer, sheet_name='Decrement_Levels_Points', index=False)
        stock_eod_df.to_excel(writer, sheet_name='Stock_EOD_Data', index=False)
        stock_eod_df_t1.to_excel(writer, sheet_name='Stock_EOD_Data_T-1', index=False)
        index_eod_df.to_excel(writer, sheet_name='Index_EOD_Data', index=False)
        index_eod_df_t1.to_excel(writer, sheet_name='Index_EOD_Data_T-1', index=False)
    
    print("Results saved to:", output_path)
    print("Summary of results:")
    print("Index Totals:", len(results_df), "rows")
    print("Decrement Percentage Levels:", len(decrement_df), "rows")
    print("Decrement Points Levels:", len(decrement_points_df), "rows")
    
    return output_path

# Main execution
try:
    # Load data
    stock_eod_df, index_eod_df, stock_eod_df_t1, index_eod_df_t1 = load_data_with_encoding_fallback()
    
    # Update stock prices
    stock_eod_df = update_stock_prices(stock_eod_df)
    
    # Calculate index levels
    results_df = calculate_index_levels(stock_eod_df, index_eod_df, index_eod_df_t1, mnemonics)
    
    # Calculate decrement percentage levels
    print("Calculating decrement levels for TR4 percentage indices...")
    if mnemonics_tr4_perc:  # Check if dictionary is not empty
        decrement_results = []
        for mnemo, isin in mnemonics_tr4_perc.items():
            result = calculate_decrement_level(mnemo, isin, index_eod_df, index_eod_df_t1, results_df)
            decrement_results.append(result)
        
        decrement_df = pd.DataFrame(decrement_results)
        print(f"Calculated decrement levels for {len(decrement_df)} indices")
        print(f"Successfully calculated levels: {decrement_df['Decrement_Level'].notna().sum()}")
    else:
        print("No TR4 percentage indices to calculate - skipping")
        decrement_df = pd.DataFrame()  # Create empty DataFrame
    
    # Calculate decrement points levels
    print("Calculating decrement points levels for TR4 points indices...")
    if mnemonics_tr4_points:  # Check if dictionary is not empty
        decrement_points_results = []
        for mnemo, isin in mnemonics_tr4_points.items():
            result = calculate_decrement_points_level(mnemo, isin, index_eod_df, index_eod_df_t1, results_df)
            decrement_points_results.append(result)
        
        decrement_points_df = pd.DataFrame(decrement_points_results)
        print(f"Calculated decrement points levels for {len(decrement_points_df)} indices")
        print(f"Successfully calculated points levels: {decrement_points_df['Decrement_Points_Level'].notna().sum()}")
    else:
        print("No TR4 points indices to calculate - skipping")
        decrement_points_df = pd.DataFrame()  # Create empty DataFrame
    
    # Save all results
    output_path = save_results_to_excel(results_df, decrement_df, decrement_points_df, stock_eod_df, stock_eod_df_t1, index_eod_df, index_eod_df_t1, timestamp)
    
    print("Script completed successfully!")
    
except Exception as e:
    print(f"Script failed with error: {e}")
    import traceback
    traceback.print_exc()