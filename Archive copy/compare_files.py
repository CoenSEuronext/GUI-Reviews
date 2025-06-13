import pandas as pd
import os
import logging
import logging.handlers
import stat
from datetime import datetime
import numpy as np
import time
import functools

# Timer decorator for performance monitoring
def timer(func):
    """Decorator to time function execution"""
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        duration = end_time - start_time
        print(f"⏱️  {func.__name__} took {duration:.2f} seconds")
        logger.info(f"Function {func.__name__} took {duration:.2f} seconds")
        return result
    return wrapper

# Get the directory where the script is located
script_dir = os.path.dirname(os.path.abspath(__file__))
log_file = os.path.join(script_dir, 'file_comparison.log')

# Set up logging
try:
    handler = logging.handlers.RotatingFileHandler(
        log_file,
        maxBytes=10*1024*1024,  # 10MB
        backupCount=5,
        encoding='utf-8'
    )
    formatter = logging.Formatter('%(asctime)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    handler.setFormatter(formatter)
    
    logger = logging.getLogger('FileComparison')
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)
except Exception as e:
    print(f"Warning: Could not set up logging to file: {str(e)}")
    # Set up console-only logging as fallback
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

# Output directory
OUTPUT_DIR = r"C:\Users\CSonneveld\OneDrive - Euronext\Documents\Projects\Archive copy\destination\Check files output"

# Create output directory if it doesn't exist
if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)
    logger.info(f"Created output directory: {OUTPUT_DIR}")

def make_file_writable(file_path):
    """Temporarily make a read-only file writable for reading"""
    try:
        # Get current permissions
        current_permissions = os.stat(file_path).st_mode
        
        # Add read permission for current user
        os.chmod(file_path, current_permissions | stat.S_IREAD)
        
        return True
    except Exception as e:
        logger.warning(f"Could not modify permissions for {file_path}: {str(e)}")
        return False

@timer
def read_excel_file(file_path):
    """Read Excel file and return DataFrame - optimized for large files"""
    try:
        logger.info(f"Reading file: {os.path.basename(file_path)}")
        print(f"Reading file: {os.path.basename(file_path)}")
        
        # Check if file exists
        if not os.path.exists(file_path):
            logger.error(f"File not found: {file_path}")
            print(f"File not found: {file_path}")
            return None
        
        # Try to make file readable if there are permission issues
        make_file_writable(file_path)
        
        # Read with openpyxl (faster for large files)
        try:
            # Use chunksize and optimize data types for better performance
            df = pd.read_excel(file_path, engine='openpyxl')
            
            # Immediate data type optimization to save memory and processing time
            for col in df.columns:
                if df[col].dtype == 'object':
                    # Convert to string and strip whitespace once
                    df[col] = df[col].astype(str).str.strip()
                elif df[col].dtype in ['float64', 'int64']:
                    # Keep numeric columns as-is for now
                    pass
            
        except ImportError:
            print("Error: openpyxl is not installed. Please install it using:")
            print("pip install openpyxl")
            logger.error("openpyxl is not installed")
            return None
        except Exception as e:
            logger.error(f"Failed to read file: {str(e)}")
            print(f"Error reading file: {str(e)}")
            return None
        
        logger.info(f"Successfully read {len(df)} rows and {len(df.columns)} columns from {os.path.basename(file_path)}")
        print(f"Successfully read {len(df)} rows and {len(df.columns)} columns")
        
        return df
        
    except Exception as e:
        logger.error(f"Error reading file {file_path}: {str(e)}")
        print(f"Error reading file {file_path}: {str(e)}")
        return None

@timer
def prepare_dataframes(df1, df2):
    """Prepare dataframes for fast comparison using indexing"""
    print("Preparing dataframes for comparison...")
    
    # Clean data once upfront
    print("Cleaning data...")
    
    # Replace infinite values and NaN with empty strings
    df1_clean = df1.replace([np.inf, -np.inf], '').fillna('')
    df2_clean = df2.replace([np.inf, -np.inf], '').fillna('')
    
    # Create composite index for fast lookups
    print("Creating indexes...")
    df1_clean['composite_key'] = df1_clean['#Symbol'].astype(str) + '|' + df1_clean['Index'].astype(str)
    df2_clean['composite_key'] = df2_clean['#Symbol'].astype(str) + '|' + df2_clean['Index'].astype(str)
    
    # Set index for O(1) lookup instead of O(n) search
    df1_indexed = df1_clean.set_index('composite_key')
    df2_indexed = df2_clean.set_index('composite_key')
    
    print(f"Prepared dataframes: {len(df1_indexed)} and {len(df2_indexed)} rows")
    
    return df1_indexed, df2_indexed

@timer
def find_differences_vectorized(df1_indexed, df2_indexed):
    """Find differences using vectorized operations - much faster"""
    print("Finding common records...")
    
    # Find intersection of indices (much faster than manual comparison)
    common_keys = df1_indexed.index.intersection(df2_indexed.index)
    print(f"Found {len(common_keys)} common records to compare")
    
    if len(common_keys) == 0:
        return pd.DataFrame(), common_keys
    
    # Get subsets for common keys only
    df1_common = df1_indexed.loc[common_keys]
    df2_common = df2_indexed.loc[common_keys]
    
    print("Comparing critical fields...")
    
    # Vectorized comparison for critical fields
    critical_fields = ['ICBCode', 'Shares', 'Free float-Coeff', 'Capping Factor-Coeff']
    
    # Create boolean mask for differences
    has_differences = pd.Series(False, index=common_keys)
    changes_per_record = pd.Series('', index=common_keys)
    
    for field in critical_fields:
        if field in df1_common.columns and field in df2_common.columns:
            # Handle numeric fields with tolerance
            if field in ['Shares', 'Free float-Coeff', 'Capping Factor-Coeff']:
                # Convert to numeric, errors='coerce' converts non-numeric to NaN
                val1 = pd.to_numeric(df1_common[field], errors='coerce').fillna(0)
                val2 = pd.to_numeric(df2_common[field], errors='coerce').fillna(0)
                
                # Use appropriate tolerance for comparison
                tolerance = 1e-6 if field == 'Shares' else 1e-10
                field_diff = abs(val1 - val2) > tolerance
            else:
                # String comparison for ICBCode
                field_diff = df1_common[field].astype(str) != df2_common[field].astype(str)
            
            # Update masks
            has_differences |= field_diff
            
            # Track which fields changed (for debugging)
            field_changes = field_diff.map(lambda x: field if x else '')
            changes_per_record = changes_per_record + field_changes.map(lambda x: f'{x},' if x else '')
    
    # Get only records with meaningful differences
    diff_keys = common_keys[has_differences]
    print(f"Found {len(diff_keys)} records with meaningful differences")
    
    if len(diff_keys) == 0:
        return pd.DataFrame(), common_keys
    
    # Build differences dataframe efficiently
    print("Building differences dataframe...")
    
    df1_diff = df1_indexed.loc[diff_keys]
    df2_diff = df2_indexed.loc[diff_keys]
    
    # Create differences dataframe with vectorized operations
    differences_data = {
        'Rank': range(1, len(diff_keys) + 1),
        'Code': df1_diff['#Symbol'].astype(str) + df1_diff['Index'].astype(str),
        '#Symbol': df1_diff['#Symbol'],
        'System date': df1_diff.get('System date', ''),
        'Adjust Reason': df2_diff.get('Adjust Reason', ''),
        'Isin Code': df1_diff.get('Isin Code', ''),
        'Country': df1_diff.get('Country', ''),
        'Mnemo': df1_diff.get('Mnemo', ''),
        'Name': df1_diff.get('Name', ''),
        'MIC': df1_diff.get('MIC', ''),
        'Prev ICB': df1_diff.get('ICBCode', ''),
        'New ICB': df2_diff.get('ICBCode', ''),
        'Close Prc': df2_diff.get('Close Prc', ''),
        'Adj Closing price': df2_diff.get('Adj Closing price', ''),
        'Net Div': df2_diff.get('Net Div', ''),
        'Gross Div': df2_diff.get('Gross Div', ''),
        'Index': df1_diff['Index'],
        'Prev. Shares': df1_diff.get('Shares', ''),
        'New Shares': df2_diff.get('Shares', ''),
        'Prev FF': df1_diff.get('Free float-Coeff', ''),
        'New FF': df2_diff.get('Free float-Coeff', ''),
        'Prev Capping': df1_diff.get('Capping Factor-Coeff', ''),
        'New Capping': df2_diff.get('Capping Factor-Coeff', '')
    }
    
    diff_df = pd.DataFrame(differences_data)
    
    return diff_df, common_keys

@timer
def write_excel_optimized(diff_df, df1_clean, df2_clean, output_path):
    """Write Excel file with optimized bulk operations and original formatting"""
    print("Writing Excel file with detailed formatting...")
    
    try:
        # Create Excel writer
        writer = pd.ExcelWriter(output_path, engine='xlsxwriter')
        workbook = writer.book
        
        # Set workbook options
        workbook.nan_inf_to_errors = True
        
        # Define formats (same as original)
        orange_format = workbook.add_format({'bg_color': '#FFC000', 'font_name': 'Verdana', 'font_size': 10})
        red_format = workbook.add_format({'bg_color': '#FF0000', 'font_color': '#FFFFFF', 'font_name': 'Verdana', 'font_size': 10})
        normal_format = workbook.add_format({'font_name': 'Verdana', 'font_size': 10})
        header_format = workbook.add_format({
            'bold': True,
            'font_name': 'Verdana',
            'font_size': 10,
            'bg_color': '#D9D9D9'
        })
        
        # Write Sheet 1: Differences
        diff_df.to_excel(writer, sheet_name='Differences', index=False)
        worksheet1 = writer.sheets['Differences']
        
        # Apply header formatting
        for col_num, value in enumerate(diff_df.columns.values):
            worksheet1.write(0, col_num, value, header_format)
        
        # Apply conditional formatting if there are differences (original logic restored)
        if not diff_df.empty:
            # Apply formatting row by row (keeping original detailed formatting logic)
            for row_idx in range(len(diff_df)):
                excel_row = row_idx + 1  # Excel is 1-indexed, +1 for header
                
                # Get column indices
                new_shares_col = diff_df.columns.get_loc('New Shares')
                prev_shares_col = diff_df.columns.get_loc('Prev. Shares')
                new_ff_col = diff_df.columns.get_loc('New FF')
                prev_ff_col = diff_df.columns.get_loc('Prev FF')
                new_capping_col = diff_df.columns.get_loc('New Capping')
                prev_capping_col = diff_df.columns.get_loc('Prev Capping')
                
                # Get values for comparison
                new_shares = diff_df.iloc[row_idx]['New Shares']
                prev_shares = diff_df.iloc[row_idx]['Prev. Shares']
                new_ff = diff_df.iloc[row_idx]['New FF']
                prev_ff = diff_df.iloc[row_idx]['Prev FF']
                new_capping = diff_df.iloc[row_idx]['New Capping']
                prev_capping = diff_df.iloc[row_idx]['Prev Capping']
                
                # Apply formatting to New Shares (original logic)
                if pd.notna(new_shares) and pd.notna(prev_shares) and str(new_shares) != str(prev_shares):
                    worksheet1.write(excel_row, new_shares_col, new_shares, orange_format)
                else:
                    worksheet1.write(excel_row, new_shares_col, new_shares, normal_format)
                
                # Apply formatting to New FF (original complex logic)
                new_ff_clean = '' if pd.isna(new_ff) else new_ff
                try:
                    new_ff_numeric = float(new_ff_clean) if new_ff_clean != '' else None
                except (ValueError, TypeError):
                    new_ff_numeric = None
                
                if new_ff_clean == '' or new_ff_numeric is None or new_ff_numeric > 1:
                    worksheet1.write(excel_row, new_ff_col, new_ff_clean, red_format)
                elif pd.notna(new_ff) and pd.notna(prev_ff) and str(new_ff).strip() != str(prev_ff).strip():
                    worksheet1.write(excel_row, new_ff_col, new_ff_clean, orange_format)
                else:
                    worksheet1.write(excel_row, new_ff_col, new_ff_clean, normal_format)
                
                # Apply formatting to New Capping (original logic)
                if pd.notna(new_capping) and pd.notna(prev_capping) and str(new_capping) != str(prev_capping):
                    worksheet1.write(excel_row, new_capping_col, new_capping, orange_format)
                else:
                    worksheet1.write(excel_row, new_capping_col, new_capping, normal_format)
                
                # Write other columns with normal format (original logic)
                for col_idx, col_name in enumerate(diff_df.columns):
                    if col_idx not in [new_shares_col, new_ff_col, new_capping_col]:
                        value = diff_df.iloc[row_idx][col_name]
                        # Clean the value before writing
                        clean_value = '' if pd.isna(value) or value in [np.inf, -np.inf] else value
                        worksheet1.write(excel_row, col_idx, clean_value, normal_format)
        
        # Auto-adjust column widths for Sheet 1
        for i, col in enumerate(diff_df.columns):
            max_length = max(len(str(col)), 10)  # Minimum width of 10
            if not diff_df.empty:
                max_length = max(max_length, diff_df[col].astype(str).str.len().max())
            worksheet1.set_column(i, i, min(max_length + 2, 50))  # Max width of 50
        
        # Write Sheet 2: Raw data file one (with original formatting)
        df1_clean_output = df1_clean.drop('composite_key', axis=1, errors='ignore')
        df1_clean_output = df1_clean_output.replace([np.inf, -np.inf], '').fillna('')
        df1_clean_output.to_excel(writer, sheet_name='Raw Data File 1', index=False)
        worksheet2 = writer.sheets['Raw Data File 1']
        
        # Apply header formatting and normal formatting (original logic)
        for col_num, value in enumerate(df1_clean_output.columns.values):
            worksheet2.write(0, col_num, value, header_format)
        
        # Apply normal format to data rows
        for row_idx in range(len(df1_clean_output)):
            for col_idx, col_name in enumerate(df1_clean_output.columns):
                value = df1_clean_output.iloc[row_idx][col_name]
                worksheet2.write(row_idx + 1, col_idx, value, normal_format)
        
        # Auto-adjust column widths for Sheet 2
        for i, col in enumerate(df1_clean_output.columns):
            max_length = max(len(str(col)), 10)
            if not df1_clean_output.empty:
                max_length = max(max_length, df1_clean_output[col].astype(str).str.len().max())
            worksheet2.set_column(i, i, min(max_length + 2, 50))
        
        # Write Sheet 3: Raw data file two (with original formatting)
        df2_clean_output = df2_clean.drop('composite_key', axis=1, errors='ignore')
        df2_clean_output = df2_clean_output.replace([np.inf, -np.inf], '').fillna('')
        df2_clean_output.to_excel(writer, sheet_name='Raw Data File 2', index=False)
        worksheet3 = writer.sheets['Raw Data File 2']
        
        # Apply header formatting and normal formatting (original logic)
        for col_num, value in enumerate(df2_clean_output.columns.values):
            worksheet3.write(0, col_num, value, header_format)
        
        # Apply normal format to data rows
        for row_idx in range(len(df2_clean_output)):
            for col_idx, col_name in enumerate(df2_clean_output.columns):
                value = df2_clean_output.iloc[row_idx][col_name]
                worksheet3.write(row_idx + 1, col_idx, value, normal_format)
        
        # Auto-adjust column widths for Sheet 3
        for i, col in enumerate(df2_clean_output.columns):
            max_length = max(len(str(col)), 10)
            if not df2_clean_output.empty:
                max_length = max(max_length, df2_clean_output[col].astype(str).str.len().max())
            worksheet3.set_column(i, i, min(max_length + 2, 50))
        
        # Close the writer
        writer.close()
        
        logger.info(f"Successfully created comparison report: {os.path.basename(output_path)}")
        print(f"Successfully created comparison report: {os.path.basename(output_path)}")
        print(f"Found {len(diff_df)} differences between the files")
        
        return True
        
    except Exception as e:
        logger.error(f"Error writing Excel file: {str(e)}")
        print(f"Error writing Excel file: {str(e)}")
        return False

@timer
def compare_files(file1_path, file2_path, output_path):
    """Main comparison function with optimizations for large files"""
    try:
        logger.info("Starting optimized file comparison...")
        print("Starting optimized file comparison for large files...")
        
        # Step 1: Read files
        df1 = read_excel_file(file1_path)
        df2 = read_excel_file(file2_path)
        
        if df1 is None or df2 is None:
            logger.error("Failed to read one or both files")
            print("Failed to read one or both files")
            return False
        
        # Print basic info
        print(f"File 1: {len(df1)} rows, {len(df1.columns)} columns")
        print(f"File 2: {len(df2)} rows, {len(df2.columns)} columns")
        print(f"File 1 columns: {list(df1.columns)}")
        
        # Step 2: Prepare dataframes for fast comparison
        df1_indexed, df2_indexed = prepare_dataframes(df1, df2)
        
        # Step 3: Find differences using vectorized operations
        diff_df, common_keys = find_differences_vectorized(df1_indexed, df2_indexed)
        
        print(f"Found {len(diff_df)} total differences")
        logger.info(f"Found {len(diff_df)} differences out of {len(common_keys)} common records")
        
        # Step 4: Write Excel file with optimized bulk operations
        success = write_excel_optimized(diff_df, df1_indexed, df2_indexed, output_path)
        
        if success:
            logger.info(f"Successfully created comparison report: {os.path.basename(output_path)}")
            print(f"Successfully created comparison report: {os.path.basename(output_path)}")
            print(f"Total differences found: {len(diff_df)}")
        
        return success
        
    except Exception as e:
        logger.error(f"Error comparing files: {str(e)}")
        print(f"Error comparing files: {str(e)}")
        return False

def main():
    """Main function to run the optimized file comparison"""
    print("=== GIS Morning Stock Changes Comparison - OPTIMIZED ===")
    logger.info("Starting optimized GIS Morning Stock Changes comparison")
    
    # File paths
    file1_path = r"C:\Users\CSonneveld\OneDrive - Euronext\Documents\Projects\Archive copy\destination\Manual\EU_MANUAL_US_NXTD_STOCK_MERGED_20250605.xlsx"
    file2_path = r"C:\Users\CSonneveld\OneDrive - Euronext\Documents\Projects\Archive copy\destination\SOD\EU_SOD_US_SOD_STOCK_MERGED_20250606.xlsx"
    output_path = os.path.join(OUTPUT_DIR, "GIS Morning stock changes.xlsx")
    
    # Check if input files exist
    if not os.path.exists(file1_path):
        logger.error(f"File 1 not found: {file1_path}")
        print(f"File 1 not found: {file1_path}")
        return
    
    if not os.path.exists(file2_path):
        logger.error(f"File 2 not found: {file2_path}")
        print(f"File 2 not found: {file2_path}")
        return
    
    # Overall timing
    start_time = time.time()
    
    # Perform comparison
    success = compare_files(file1_path, file2_path, output_path)
    
    total_time = time.time() - start_time
    
    if success:
        print(f"\nComparison completed successfully in {total_time:.1f} seconds!")
        print(f"Output saved to: {output_path}")
        logger.info(f"File comparison completed successfully in {total_time:.1f} seconds")
    else:
        print("Comparison failed. Check the log file for details.")
        logger.error("File comparison failed")

if __name__ == "__main__":
    main()