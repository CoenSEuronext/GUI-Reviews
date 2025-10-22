import pandas as pd
import datetime
import os

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
    "EU500", "LC100", "AS500", "JPCLE", "ESBTP", "DEUP", "DAPPR", "DASP", 
    "DPAP", "EJPP", "ECHP", "EUKP", "DEUPT", "DASPT", "EJPPT", "ECHPT", 
    "EUKPT", "ELUXP", "DAREP", "DEREP", "ESVEP", "ENEU", "BREU", "EUADP", 
    "AAX", "ENCLE", "ECOP", "ECOEW", "ES1EP", "WATPR", "NLIN", "NLCG", 
    "NLFIN", "REITE", "BIOTK", "ENVEU"
]

# Try with different encodings
try:
    # Current day files (T)
    current_stock_eod_date = "20250403"
    current_index_eod_date = "20250403"
    current_stock_sod_date = "20250404"
    current_index_sod_date = "20250404" 

    # Previous day files (T-1)
    prev_stock_eod_date = get_previous_day(current_stock_eod_date)
    prev_index_eod_date = get_previous_day(current_index_eod_date)
    
    # Base path
    base_path = r"V:\PM-Indices-IndexOperations\General\Daily downloadfiles\Monthly Archive"
    
    # Current day file paths
    stock_eod_path = os.path.join(base_path, f"TTMIndexEU1_GIS_EOD_STOCK_{current_stock_eod_date}.csv")
    index_eod_path = os.path.join(base_path, f"TTMIndexEU1_GIS_EOD_INDEX_{current_index_eod_date}.csv")
    stock_sod_path = os.path.join(base_path, f"TTMIndexEU1_GIS_SOD_STOCK_{current_stock_sod_date}.csv")
    index_sod_path = os.path.join(base_path, f"TTMIndexEU1_GIS_SOD_INDEX_{current_index_sod_date}.csv")
    
    # Previous day file paths
    stock_eod_t1_path = os.path.join(base_path, f"TTMIndexEU1_GIS_EOD_STOCK_{prev_stock_eod_date}.csv")
    index_eod_t1_path = os.path.join(base_path, f"TTMIndexEU1_GIS_EOD_INDEX_{prev_index_eod_date}.csv")
    
    # First try with 'latin1' encoding (also known as ISO-8859-1)
    # Current day dataframes
    stock_eod_df = pd.read_csv(stock_eod_path, sep=';', encoding='latin1')
    index_eod_df = pd.read_csv(index_eod_path, sep=';', encoding='latin1')
    stock_sod_df = pd.read_csv(stock_sod_path, sep=';', encoding='latin1')
    index_sod_df = pd.read_csv(index_sod_path, sep=';', encoding='latin1')
    
    # Previous day dataframes
    stock_eod_df_t1 = pd.read_csv(stock_eod_t1_path, sep=';', encoding='latin1')
    index_eod_df_t1 = pd.read_csv(index_eod_t1_path, sep=';', encoding='latin1')
    
    # Print some debugging information
    print("First few rows of stock_sod_df:")
    print(stock_sod_df[['#Symbol', 'Index', 'FX/Index Ccy']].head())
    
    print("\nFirst few rows of stock_eod_df:")
    print(stock_eod_df[['#Symbol', 'Index']].head())
    
    # Create a lookup dictionary from stock_sod_df with data cleaning
    lookup_dict = {}
    for index, row in stock_sod_df.iterrows():
        # Clean and standardize the keys
        symbol = str(row['#Symbol']).strip() if not pd.isna(row['#Symbol']) else ""
        index_val = str(row['Index']).strip() if not pd.isna(row['Index']) else ""
        
        # Create a composite key using cleaned Symbol and Index
        key = (symbol, index_val)
        
        # Store the corresponding FX/Index Ccy value
        if not pd.isna(row['FX/Index Ccy']):
            lookup_dict[key] = row['FX/Index Ccy']

    # Create the new column in stock_eod_df with cleaned lookup
    def lookup_fx_index_ccy(row):
        # Clean and standardize the keys from stock_eod_df in the same way
        symbol = str(row['#Symbol']).strip() if not pd.isna(row['#Symbol']) else ""
        index_val = str(row['Index']).strip() if not pd.isna(row['Index']) else ""
        
        key = (symbol, index_val)
        return lookup_dict.get(key, None)  # Return None if key not found

    # Add some debugging to see what's happening
    print("\nExample keys from stock_sod_df (first 3 rows):")
    for i, row in stock_sod_df.head(3).iterrows():
        clean_symbol = str(row['#Symbol']).strip() if not pd.isna(row['#Symbol']) else ""
        clean_index = str(row['Index']).strip() if not pd.isna(row['Index']) else ""
        print(f"Key: ({clean_symbol}, {clean_index}), Value: {row['FX/Index Ccy']}")

    print("\nExample lookups from stock_eod_df (first 3 rows):")
    for i, row in stock_eod_df.head(3).iterrows():
        clean_symbol = str(row['#Symbol']).strip() if not pd.isna(row['#Symbol']) else ""
        clean_index = str(row['Index']).strip() if not pd.isna(row['Index']) else ""
        key = (clean_symbol, clean_index)
        print(f"Looking up key: {key}, Found: {lookup_dict.get(key, 'NOT FOUND')}")
    
    # Apply the lookup function with cleaned data
    stock_eod_df['new_FX/Index Ccy'] = stock_eod_df.apply(lookup_fx_index_ccy, axis=1)
    
    # Check if we have any NaN values in the new column
    nan_count = stock_eod_df['new_FX/Index Ccy'].isna().sum()
    print(f"\nNumber of NaN values in new_FX/Index Ccy: {nan_count} out of {len(stock_eod_df)} rows")
    
    # Calculate new_Index_Mcap
    stock_eod_df['new_Index_Mcap'] = (
        stock_eod_df['Close Prc'] * 
        stock_eod_df['Shares'] * 
        stock_eod_df['Free float-Coeff'] * 
        stock_eod_df['Capping Factor-Coeff'] * 
        stock_eod_df['new_FX/Index Ccy']
    )
    
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
        
        # Append the result to the list
        results.append({
            'Index': mnemo, 
            'Total_Index_Mcap': total_mcap,
            'Divisor': divisor,
            'Price_Level': price_level
        })
    
    # Create a dataframe from the results
    results_df = pd.DataFrame(results)
    
    
    # Create Excel writer object with the timestamped filename
    output_path = fr"C:\Users\CSonneveld\OneDrive - Euronext\Documents\Projects\Recalculation Script\Output\Index_Analysis_{timestamp}.xlsx"
    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        # Write each dataframe to a different sheet
        results_df.to_excel(writer, sheet_name='Index_Totals', index=False)
        stock_eod_df.to_excel(writer, sheet_name='Stock_EOD_Data', index=False)
        stock_eod_df_t1.to_excel(writer, sheet_name='Stock_EOD_Data_T-1', index=False)
        index_eod_df.to_excel(writer, sheet_name='Index_EOD_Data', index=False)
        index_eod_df_t1.to_excel(writer, sheet_name='Index_EOD_Data_T-1', index=False)
    
    print("Results saved to:", output_path)
    print("Summary of results:")
    print(results_df)

except Exception as e:
    print(f"First attempt failed with latin1 encoding: {e}")
    
    try:
        # Current day files (T)
        current_stock_eod_date = "20250402"
        current_index_eod_date = "20250402"
        current_stock_sod_date = "20250403"
        current_index_sod_date = "20250403" # Corrected from 2025040

        # Previous day files (T-1)
        prev_stock_eod_date = get_previous_day(current_stock_eod_date)
        prev_index_eod_date = get_previous_day(current_index_eod_date)
        
        # Base path
        base_path = r"C:\Users\CSonneveld\OneDrive - Euronext\Documents\Projects\Recalculation Script\Data"
        
        # Current day file paths
        stock_eod_path = os.path.join(base_path, f"TTMIndexEU1_GIS_EOD_STOCK_{current_stock_eod_date}.csv")
        index_eod_path = os.path.join(base_path, f"TTMIndexEU1_GIS_EOD_INDEX_{current_index_eod_date}.csv")
        stock_sod_path = os.path.join(base_path, f"TTMIndexEU1_GIS_SOD_STOCK_{current_stock_sod_date}.csv")
        index_sod_path = os.path.join(base_path, f"TTMIndexEU1_GIS_SOD_INDEX_{current_index_sod_date}.csv")
        
        # Previous day file paths
        stock_eod_t1_path = os.path.join(base_path, f"TTMIndexEU1_GIS_EOD_STOCK_{prev_stock_eod_date}.csv")
        index_eod_t1_path = os.path.join(base_path, f"TTMIndexEU1_GIS_EOD_INDEX_{prev_index_eod_date}.csv")
        
        # Current day dataframes (using windows-1252 encoding this time)
        stock_eod_df = pd.read_csv(stock_eod_path, sep=';', encoding='windows-1252')
        index_eod_df = pd.read_csv(index_eod_path, sep=';', encoding='windows-1252')
        stock_sod_df = pd.read_csv(stock_sod_path, sep=';', encoding='windows-1252')
        index_sod_df = pd.read_csv(index_sod_path, sep=';', encoding='windows-1252')
        
        # Previous day dataframes
        stock_eod_df_t1 = pd.read_csv(stock_eod_t1_path, sep=';', encoding='windows-1252')
        index_eod_df_t1 = pd.read_csv(index_eod_t1_path, sep=';', encoding='windows-1252')
        
        # Print some debugging information
        print("First few rows of stock_sod_df:")
        print(stock_sod_df[['#Symbol', 'Index', 'FX/Index Ccy']].head())
        
        print("\nFirst few rows of stock_eod_df:")
        print(stock_eod_df[['#Symbol', 'Index']].head())
        
        # Create a lookup dictionary from stock_sod_df with data cleaning
        lookup_dict = {}
        for index, row in stock_sod_df.iterrows():
            # Clean and standardize the keys
            symbol = str(row['#Symbol']).strip() if not pd.isna(row['#Symbol']) else ""
            index_val = str(row['Index']).strip() if not pd.isna(row['Index']) else ""
            
            # Create a composite key using cleaned Symbol and Index
            key = (symbol, index_val)
            
            # Store the corresponding FX/Index Ccy value
            if not pd.isna(row['FX/Index Ccy']):
                lookup_dict[key] = row['FX/Index Ccy']

        # Create the new column in stock_eod_df with cleaned lookup
        def lookup_fx_index_ccy(row):
            # Clean and standardize the keys from stock_eod_df in the same way
            symbol = str(row['#Symbol']).strip() if not pd.isna(row['#Symbol']) else ""
            index_val = str(row['Index']).strip() if not pd.isna(row['Index']) else ""
            
            key = (symbol, index_val)
            return lookup_dict.get(key, None)  # Return None if key not found

        # Add some debugging to see what's happening
        print("\nExample keys from stock_sod_df (first 3 rows):")
        for i, row in stock_sod_df.head(3).iterrows():
            clean_symbol = str(row['#Symbol']).strip() if not pd.isna(row['#Symbol']) else ""
            clean_index = str(row['Index']).strip() if not pd.isna(row['Index']) else ""
            print(f"Key: ({clean_symbol}, {clean_index}), Value: {row['FX/Index Ccy']}")

        print("\nExample lookups from stock_eod_df (first 3 rows):")
        for i, row in stock_eod_df.head(3).iterrows():
            clean_symbol = str(row['#Symbol']).strip() if not pd.isna(row['#Symbol']) else ""
            clean_index = str(row['Index']).strip() if not pd.isna(row['Index']) else ""
            key = (clean_symbol, clean_index)
            print(f"Looking up key: {key}, Found: {lookup_dict.get(key, 'NOT FOUND')}")
        
        # Apply the lookup function with cleaned data
        stock_eod_df['new_FX/Index Ccy'] = stock_eod_df.apply(lookup_fx_index_ccy, axis=1)
        
        # Check if we have any NaN values in the new column
        nan_count = stock_eod_df['new_FX/Index Ccy'].isna().sum()
        print(f"\nNumber of NaN values in new_FX/Index Ccy: {nan_count} out of {len(stock_eod_df)} rows")
        
        # Calculate new_Index_Mcap
        stock_eod_df['new_Index_Mcap'] = (
            stock_eod_df['Close Prc'] * 
            stock_eod_df['Shares'] * 
            stock_eod_df['Free float-Coeff'] * 
            stock_eod_df['Capping Factor-Coeff'] * 
            stock_eod_df['new_FX/Index Ccy']
        )
        
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
            
            # Append the result to the list
            results.append({
                'Index': mnemo, 
                'Total_Index_Mcap': total_mcap,
                'Divisor': divisor,
                'Price_Level': price_level
            })
        
        # Create a dataframe from the results
        results_df = pd.DataFrame(results)
        
        # Add timestamp column to the results
        results_df['Timestamp'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Create Excel writer object with the timestamped filename
        output_path = fr"C:\Users\CSonneveld\OneDrive - Euronext\Documents\Projects\Recalculation Script\Output\Index_Analysis_{timestamp}.xlsx"
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            # Write each dataframe to a different sheet
            results_df.to_excel(writer, sheet_name='Index_Totals', index=False)
            stock_eod_df.to_excel(writer, sheet_name='Stock_EOD_Data', index=False)
            stock_eod_df_t1.to_excel(writer, sheet_name='Stock_EOD_Data_T-1', index=False)
            index_eod_df.to_excel(writer, sheet_name='Index_EOD_Data', index=False)
            index_eod_df_t1.to_excel(writer, sheet_name='Index_EOD_Data_T-1', index=False)
        
        print("Results saved to:", output_path)
        print("Summary of results:")
        print(results_df)
        
    except Exception as e:
        print(f"Second attempt failed with windows-1252 encoding: {e}")