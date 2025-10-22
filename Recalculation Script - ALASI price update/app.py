import pandas as pd
import datetime
import os

# Stock prices dictionary
stock_prices = {
    "AACM.ST": 109.4,
    "AAK.ST": 260.6,
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

def load_data_with_encoding_fallback():
    """Load data with encoding fallback mechanism - combines US and EU files"""
    encodings = ['latin1', 'windows-1252', 'utf-8']
    
    # File date configuration
    current_stock_eod_date = "20250930"
    current_index_eod_date = "20250930"
    current_stock_sod_date = "20251001"
    current_index_sod_date = "20251001"
    
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
            us_stock_sod_path = os.path.join(base_path, f"TTMIndexUS1_GIS_SOD_STOCK_{current_stock_sod_date}.csv")
            us_index_sod_path = os.path.join(base_path, f"TTMIndexUS1_GIS_SOD_INDEX_{current_index_sod_date}.csv")
            us_stock_eod_t1_path = os.path.join(base_path, f"TTMIndexUS1_GIS_EOD_STOCK_{prev_stock_eod_date}.csv")
            us_index_eod_t1_path = os.path.join(base_path, f"TTMIndexUS1_GIS_EOD_INDEX_{prev_index_eod_date}.csv")
            
            # EU File paths
            eu_stock_eod_path = os.path.join(base_path, f"TTMIndexEU1_GIS_EOD_STOCK_{current_stock_eod_date}.csv")
            eu_index_eod_path = os.path.join(base_path, f"TTMIndexEU1_GIS_EOD_INDEX_{current_index_eod_date}.csv")
            eu_stock_sod_path = os.path.join(base_path, f"TTMIndexEU1_GIS_SOD_STOCK_{current_stock_sod_date}.csv")
            eu_index_sod_path = os.path.join(base_path, f"TTMIndexEU1_GIS_SOD_INDEX_{current_index_sod_date}.csv")
            eu_stock_eod_t1_path = os.path.join(base_path, f"TTMIndexEU1_GIS_EOD_STOCK_{prev_stock_eod_date}.csv")
            eu_index_eod_t1_path = os.path.join(base_path, f"TTMIndexEU1_GIS_EOD_INDEX_{prev_index_eod_date}.csv")
            
            # Load US dataframes
            us_stock_eod_df = pd.read_csv(us_stock_eod_path, sep=';', encoding=encoding)
            us_index_eod_df = pd.read_csv(us_index_eod_path, sep=';', encoding=encoding)
            us_stock_sod_df = pd.read_csv(us_stock_sod_path, sep=';', encoding=encoding)
            us_index_sod_df = pd.read_csv(us_index_sod_path, sep=';', encoding=encoding)
            us_stock_eod_df_t1 = pd.read_csv(us_stock_eod_t1_path, sep=';', encoding=encoding)
            us_index_eod_df_t1 = pd.read_csv(us_index_eod_t1_path, sep=';', encoding=encoding)
            
            # Load EU dataframes
            eu_stock_eod_df = pd.read_csv(eu_stock_eod_path, sep=';', encoding=encoding)
            eu_index_eod_df = pd.read_csv(eu_index_eod_path, sep=';', encoding=encoding)
            eu_stock_sod_df = pd.read_csv(eu_stock_sod_path, sep=';', encoding=encoding)
            eu_index_sod_df = pd.read_csv(eu_index_sod_path, sep=';', encoding=encoding)
            eu_stock_eod_df_t1 = pd.read_csv(eu_stock_eod_t1_path, sep=';', encoding=encoding)
            eu_index_eod_df_t1 = pd.read_csv(eu_index_eod_t1_path, sep=';', encoding=encoding)
            
            # Combine US and EU dataframes
            stock_eod_df = pd.concat([us_stock_eod_df, eu_stock_eod_df], ignore_index=True)
            index_eod_df = pd.concat([us_index_eod_df, eu_index_eod_df], ignore_index=True)
            stock_sod_df = pd.concat([us_stock_sod_df, eu_stock_sod_df], ignore_index=True)
            index_sod_df = pd.concat([us_index_sod_df, eu_index_sod_df], ignore_index=True)
            stock_eod_df_t1 = pd.concat([us_stock_eod_df_t1, eu_stock_eod_df_t1], ignore_index=True)
            index_eod_df_t1 = pd.concat([us_index_eod_df_t1, eu_index_eod_df_t1], ignore_index=True)
            
            print(f"Successfully loaded and combined US and EU data with {encoding} encoding")
            print(f"Combined data sizes:")
            print(f"  Stock EOD: {len(stock_eod_df)} rows")
            print(f"  Index EOD: {len(index_eod_df)} rows")
            print(f"  Stock SOD: {len(stock_sod_df)} rows")
            print(f"  Index SOD: {len(index_sod_df)} rows")
            print(f"  Stock EOD T-1: {len(stock_eod_df_t1)} rows")
            print(f"  Index EOD T-1: {len(index_eod_df_t1)} rows")
            
            return stock_eod_df, index_eod_df, stock_sod_df, index_sod_df, stock_eod_df_t1, index_eod_df_t1
            
        except Exception as e:
            print(f"Failed to load with {encoding} encoding: {e}")
            continue
    
    raise Exception("Failed to load data with any encoding")

# List of mnemonics to process
mnemonics = [
    "ALASI"
]

# Main execution
try:
    # Load combined US and EU data using the enhanced function
    stock_eod_df, index_eod_df, stock_sod_df, index_sod_df, stock_eod_df_t1, index_eod_df_t1 = load_data_with_encoding_fallback()
    
    # Print some debugging information
    print("First few rows of combined stock_eod_df:")
    print(stock_eod_df[['#Symbol', 'Index', 'Close Prc']].head())
    
    # Update Close Prc values using the stock_prices dictionary
    def update_close_price(row):
        symbol = str(row['#Symbol']).strip() if not pd.isna(row['#Symbol']) else ""
        if symbol in stock_prices:
            return stock_prices[symbol]
        else:
            return row['Close Prc']  # Keep original price if not found in dictionary
    
    # Apply the price update
    original_prices = stock_eod_df['Close Prc'].copy()
    stock_eod_df['Close Prc'] = stock_eod_df.apply(update_close_price, axis=1)
    
    # Count how many prices were updated
    updated_count = (original_prices != stock_eod_df['Close Prc']).sum()
    print(f"\nUpdated {updated_count} price records from the stock_prices dictionary")
    
    # Show some examples of updated prices
    price_changes = stock_eod_df[original_prices != stock_eod_df['Close Prc']]
    if not price_changes.empty:
        print("\nExample price updates:")
        for i, row in price_changes.head(5).iterrows():
            symbol = row['#Symbol']
            new_price = row['Close Prc']
            old_price = original_prices.iloc[i]
            print(f"{symbol}: {old_price} → {new_price}")
    
    # Calculate new_Index_Mcap using existing FX/Index Ccy column
    stock_eod_df['new_Index_Mcap'] = (
        stock_eod_df['Close Prc'] * 
        stock_eod_df['Shares'] * 
        stock_eod_df['Free float-Coeff'] * 
        stock_eod_df['Capping Factor-Coeff'] * 
        stock_eod_df['FX/Index Ccy']
    )
    
    # Create dictionaries to map Mnemo to the various ISIN codes from index_eod_df
    isin_lookup = dict(zip(index_eod_df['Mnemo'], index_eod_df['IsinCode']))
    
    # Create dictionaries to map Price ISIN to Gross and Net Return ISIN versions
    gross_isin_lookup = dict(zip(index_eod_df['IsinCode'], index_eod_df['ISIN Gross Return version']))
    net_isin_lookup = dict(zip(index_eod_df['IsinCode'], index_eod_df['ISIN Net Return version']))
    
    # Create dictionaries to map ISINs to Effect Gross and Net Total Return values
    gross_mass_lookup = dict(zip(index_eod_df['IsinCode'], index_eod_df['Effect Gross Total Return']))
    net_mass_lookup = dict(zip(index_eod_df['IsinCode'], index_eod_df['Effect Net Total Return ']))
    
    # Create dictionaries to map ISINs to t0 IV unround from previous day's index data
    # Create price_t1_lookup for Price ISINs
    price_t1_lookup = {}
    for idx, row in index_eod_df_t1.iterrows():
        if not pd.isna(row['IsinCode']):
            price_t1_lookup[row['IsinCode']] = row['t0 IV unround'] if 't0 IV unround' in index_eod_df_t1.columns else None
    
    # For Gross ISINs
    gross_t1_lookup = {}
    for idx, row in index_eod_df_t1.iterrows():
        if not pd.isna(row['IsinCode']):
            gross_t1_lookup[row['IsinCode']] = row['t0 IV unround'] if 't0 IV unround' in index_eod_df_t1.columns else None
    
    # For Net ISINs
    net_t1_lookup = {}
    for idx, row in index_eod_df_t1.iterrows():
        if not pd.isna(row['IsinCode']):
            net_t1_lookup[row['IsinCode']] = row['t0 IV unround'] if 't0 IV unround' in index_eod_df_t1.columns else None
    
    # Create a dataframe to store the results
    results = []
    
    # Calculate the sum of new_Index_Mcap for each mnemo in the list
    for mnemo in mnemonics:
        # Filter the dataframe where Index equals the mnemo
        filtered_df = stock_eod_df[stock_eod_df['Index'] == mnemo]
        
        # Calculate the sum of new_Index_Mcap
        total_mcap = filtered_df['new_Index_Mcap'].sum()
        
        # Look up the divisor from index_eod_df where Mnemo equals the mnemo
        divisor_row = index_eod_df[index_eod_df['Mnemo'] == mnemo]
        divisor = divisor_row['Divisor'].values[0] if not divisor_row.empty else None
        
        # Calculate Price Level (Total_Index_Mcap / Divisor)
        price_level = total_mcap / divisor if divisor is not None and divisor != 0 else None
        price_level_round = round(price_level, 8) if price_level is not None else None
        
        # Look up the IsinCode for the mnemo
        price_isin = isin_lookup.get(mnemo, None)
        
        # Look up the Gross and Net Return ISINs using the Price ISIN
        gross_isin = gross_isin_lookup.get(price_isin, None) if price_isin else None
        net_isin = net_isin_lookup.get(price_isin, None) if price_isin else None
        
        # Look up the Gross and Net Mass values using the Gross ISIN
        gross_mass = gross_mass_lookup.get(gross_isin, None) if gross_isin else None
        net_mass = net_mass_lookup.get(net_isin, None) if net_isin else None
        
        # Look up the t-1 values for Price, Gross and Net ISINs
        price_t1 = price_t1_lookup.get(price_isin, None) if price_isin else None
        gross_t1 = gross_t1_lookup.get(gross_isin, None) if gross_isin else None
        net_t1 = net_t1_lookup.get(net_isin, None) if net_isin else None
        
        # Calculate Gross_Level and Net_Level using the formula (both rounded and unrounded versions)
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
        
        # Append the result to the list
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
    
    # Create a dataframe from the results
    results_df = pd.DataFrame(results)
    
    # Create Excel writer object with the timestamped filename
    output_path = fr"C:\Users\CSonneveld\OneDrive - Euronext\Documents\Projects\Recalculation Script - ALASI price update\Output\Recalculation_{timestamp}.xlsx"
    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        # Write each dataframe to a different sheet
        results_df.to_excel(writer, sheet_name='Index_Totals', index=False)
        stock_eod_df.to_excel(writer, sheet_name='Stock_EOD', index=False)
        stock_eod_df_t1.to_excel(writer, sheet_name='Stock_EOD_T-1', index=False)
        index_eod_df.to_excel(writer, sheet_name='Index_EOD', index=False)
        index_eod_df_t1.to_excel(writer, sheet_name='Index_EOD_T-1', index=False)
        stock_sod_df.to_excel(writer, sheet_name='Stock_SOD', index=False)
        index_sod_df.to_excel(writer, sheet_name='Index_SOD', index=False)
    
    print("Results saved to:", output_path)
    
except Exception as e:
    print(f"Script execution failed: {e}")
    print("This could be due to missing files or encoding issues.")
    print("Please check that all US and EU files exist in the specified directory.")