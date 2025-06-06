import pandas as pd
import os
import logging
import logging.handlers
import stat
from datetime import datetime
import numpy as np

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

def read_excel_file(file_path):
    """Read Excel file and return DataFrame"""
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
        
        # Try reading with xlsxwriter first (engine we used for writing)
        try:
            df = pd.read_excel(file_path, engine='openpyxl')
        except ImportError:
            print("Error: openpyxl is not installed. Please install it using:")
            print("pip install openpyxl")
            logger.error("openpyxl is not installed")
            return None
        except Exception as e:
            logger.warning(f"Failed to read with openpyxl: {str(e)}")
            # Try with xlrd as fallback
            try:
                df = pd.read_excel(file_path, engine='xlrd')
            except Exception as e2:
                logger.error(f"Failed to read with xlrd: {str(e2)}")
                # Last resort - try without specifying engine
                try:
                    df = pd.read_excel(file_path)
                except Exception as e3:
                    logger.error(f"All read attempts failed: {str(e3)}")
                    print(f"Error reading file: {str(e3)}")
                    return None
        
        logger.info(f"Successfully read {len(df)} rows from {os.path.basename(file_path)}")
        print(f"Successfully read {len(df)} rows from {os.path.basename(file_path)}")
        
        return df
        
    except Exception as e:
        logger.error(f"Error reading file {file_path}: {str(e)}")
        print(f"Error reading file {file_path}: {str(e)}")
        return None

def compare_files(file1_path, file2_path, output_path):
    """Compare two Excel files and generate comparison report"""
    try:
        logger.info("Starting file comparison...")
        print("Starting file comparison...")
        
        # Read both files
        df1 = read_excel_file(file1_path)
        df2 = read_excel_file(file2_path)
        
        if df1 is None or df2 is None:
            logger.error("Failed to read one or both files")
            print("Failed to read one or both files")
            return False
        
        # Print column names for debugging
        print(f"File 1 columns: {list(df1.columns)}")
        print(f"File 2 columns: {list(df2.columns)}")
        
        # Print first few rows to understand data structure
        print(f"\nFile 1 sample data:")
        print(df1[['#Symbol', 'Index']].head(3))
        print(f"\nFile 2 sample data:")
        print(df2[['#Symbol', 'Index']].head(3))
        
        # Create matching keys for both dataframes
        df1['match_key'] = df1['#Symbol'].astype(str) + '|' + df1['Index'].astype(str)
        df2['match_key'] = df2['#Symbol'].astype(str) + '|' + df2['Index'].astype(str)
        
        # Find rows that exist in both files
        common_keys = set(df1['match_key']).intersection(set(df2['match_key']))
        logger.info(f"Found {len(common_keys)} common records to compare")
        print(f"Found {len(common_keys)} common records to compare")
        
        # Debug: Print some example keys
        print(f"Example match keys: {list(common_keys)[:5]}")
        
        # Prepare differences list
        differences = []
        rank = 1
        
        for key in common_keys:
            row1 = df1[df1['match_key'] == key].iloc[0]
            row2 = df2[df2['match_key'] == key].iloc[0]
            
            # Only check the 4 critical fields that indicate real business changes
            meaningful_difference = False
            changes_detected = []
            
            # Check ICBCode
            icb1 = str(row1.get('ICBCode', '')).strip()
            icb2 = str(row2.get('ICBCode', '')).strip()
            if icb1 != icb2:
                meaningful_difference = True
                changes_detected.append('ICBCode')
            
            # Check Shares
            shares1 = row1.get('Shares', '')
            shares2 = row2.get('Shares', '')
            try:
                shares1_num = float(shares1) if pd.notna(shares1) and str(shares1).strip() != '' else 0.0
                shares2_num = float(shares2) if pd.notna(shares2) and str(shares2).strip() != '' else 0.0
                if abs(shares1_num - shares2_num) > 1e-6:  # Allow for small rounding differences
                    meaningful_difference = True
                    changes_detected.append('Shares')
            except (ValueError, TypeError):
                # If can't convert to numbers, compare as strings
                if str(shares1).strip() != str(shares2).strip():
                    meaningful_difference = True
                    changes_detected.append('Shares')
            
            # Check Free float-Coeff
            ff1 = row1.get('Free float-Coeff', '')
            ff2 = row2.get('Free float-Coeff', '')
            try:
                ff1_num = float(ff1) if pd.notna(ff1) and str(ff1).strip() != '' else 0.0
                ff2_num = float(ff2) if pd.notna(ff2) and str(ff2).strip() != '' else 0.0
                if abs(ff1_num - ff2_num) > 1e-10:
                    meaningful_difference = True
                    changes_detected.append('Free float-Coeff')
            except (ValueError, TypeError):
                if str(ff1).strip() != str(ff2).strip():
                    meaningful_difference = True
                    changes_detected.append('Free float-Coeff')
            
            # Check Capping Factor-Coeff
            cap1 = row1.get('Capping Factor-Coeff', '')
            cap2 = row2.get('Capping Factor-Coeff', '')
            try:
                cap1_num = float(cap1) if pd.notna(cap1) and str(cap1).strip() != '' else 0.0
                cap2_num = float(cap2) if pd.notna(cap2) and str(cap2).strip() != '' else 0.0
                if abs(cap1_num - cap2_num) > 1e-10:
                    meaningful_difference = True
                    changes_detected.append('Capping Factor-Coeff')
            except (ValueError, TypeError):
                if str(cap1).strip() != str(cap2).strip():
                    meaningful_difference = True
                    changes_detected.append('Capping Factor-Coeff')
            
            # Only add to differences if one of the 4 critical fields actually changed
            if meaningful_difference:
                print(f"Found difference for {row1['#Symbol']}-{row1['Index']}: {', '.join(changes_detected)}")
                
                diff_row = {
                    'Rank': rank,
                    'Code': str(row1['#Symbol']) + str(row1['Index']),
                    '#Symbol': row1['#Symbol'],
                    'System date': row1.get('System date', ''),
                    'Adjust Reason': row2.get('Adjust Reason', ''),
                    'Isin Code': row1.get('Isin Code', ''),
                    'Country': row1.get('Country', ''),
                    'Mnemo': row1.get('Mnemo', ''),
                    'Name': row1.get('Name', ''),
                    'MIC': row1.get('MIC', ''),
                    'Prev ICB': row1.get('ICBCode', ''),
                    'New ICB': row2.get('ICBCode', ''),
                    'Close Prc': row2.get('Close Prc', ''),
                    'Adj Closing price': row2.get('Adj Closing price', ''),
                    'Net Div': row2.get('Net Div', ''),
                    'Gross Div': row2.get('Gross Div', ''),
                    'Index': row1['Index'],
                    'Prev. Shares': row1.get('Shares', ''),
                    'New Shares': row2.get('Shares', ''),
                    'Prev FF': row1.get('Free float-Coeff', ''),
                    'New FF': row2.get('Free float-Coeff', ''),
                    'Prev Capping': row1.get('Capping Factor-Coeff', ''),
                    'New Capping': row2.get('Capping Factor-Coeff', '')
                }
                
                differences.append(diff_row)
                rank += 1
        
        logger.info(f"Found {len(differences)} differences")
        print(f"Found {len(differences)} differences")
        
        # Create differences DataFrame
        if differences:
            diff_df = pd.DataFrame(differences)
            # Clean up NaN and infinite values
            diff_df = diff_df.replace([np.inf, -np.inf], '')  # Replace infinite values with empty string
            diff_df = diff_df.fillna('')  # Replace NaN with empty string
        else:
            # Create empty DataFrame with the required columns
            columns = ['Rank', 'Code', '#Symbol', 'System date', 'Adjust Reason', 'Isin Code', 
                      'Country', 'Mnemo', 'Name', 'MIC', 'Prev ICB', 'New ICB', 'Close Prc', 
                      'Adj Closing price', 'Net Div', 'Gross Div', 'Index', 'Prev. Shares', 
                      'New Shares', 'Prev FF', 'New FF', 'Prev Capping', 'New Capping']
            diff_df = pd.DataFrame(columns=columns)
        
        # Create Excel writer with XlsxWriter engine for formatting
        writer = pd.ExcelWriter(output_path, engine='xlsxwriter')
        workbook = writer.book
        
        # Set workbook options to handle NaN/INF values
        workbook.nan_inf_to_errors = True
        
        # Define formats for conditional formatting
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
        
        # Apply conditional formatting if there are differences
        if not diff_df.empty:
            # Apply formatting row by row
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
                
                # Apply formatting to New Shares
                if pd.notna(new_shares) and pd.notna(prev_shares) and str(new_shares) != str(prev_shares):
                    worksheet1.write(excel_row, new_shares_col, new_shares, orange_format)
                else:
                    worksheet1.write(excel_row, new_shares_col, new_shares, normal_format)
                
                # Apply formatting to New FF
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
                
                # Apply formatting to New Capping
                if pd.notna(new_capping) and pd.notna(prev_capping) and str(new_capping) != str(prev_capping):
                    worksheet1.write(excel_row, new_capping_col, new_capping, orange_format)
                else:
                    worksheet1.write(excel_row, new_capping_col, new_capping, normal_format)
                
                # Write other columns with normal format
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
        
        # Write Sheet 2: Raw data file one
        df1_clean = df1.drop('match_key', axis=1)  # Remove the temporary match key
        # Clean up NaN and infinite values in raw data
        df1_clean = df1_clean.replace([np.inf, -np.inf], '')  # Replace infinite values
        df1_clean = df1_clean.fillna('')  # Replace NaN values
        df1_clean.to_excel(writer, sheet_name='Raw Data File 1', index=False)
        worksheet2 = writer.sheets['Raw Data File 1']
        
        # Apply header formatting and normal formatting
        for col_num, value in enumerate(df1_clean.columns.values):
            worksheet2.write(0, col_num, value, header_format)
        
        # Apply normal format to data rows
        for row_idx in range(len(df1_clean)):
            for col_idx, col_name in enumerate(df1_clean.columns):
                value = df1_clean.iloc[row_idx][col_name]
                worksheet2.write(row_idx + 1, col_idx, value, normal_format)
        
        # Auto-adjust column widths for Sheet 2
        for i, col in enumerate(df1_clean.columns):
            max_length = max(len(str(col)), 10)
            if not df1_clean.empty:
                max_length = max(max_length, df1_clean[col].astype(str).str.len().max())
            worksheet2.set_column(i, i, min(max_length + 2, 50))
        
        # Write Sheet 3: Raw data file two
        df2_clean = df2.drop('match_key', axis=1)  # Remove the temporary match key
        # Clean up NaN and infinite values in raw data
        df2_clean = df2_clean.replace([np.inf, -np.inf], '')  # Replace infinite values
        df2_clean = df2_clean.fillna('')  # Replace NaN values
        df2_clean.to_excel(writer, sheet_name='Raw Data File 2', index=False)
        worksheet3 = writer.sheets['Raw Data File 2']
        
        # Apply header formatting and normal formatting
        for col_num, value in enumerate(df2_clean.columns.values):
            worksheet3.write(0, col_num, value, header_format)
        
        # Apply normal format to data rows
        for row_idx in range(len(df2_clean)):
            for col_idx, col_name in enumerate(df2_clean.columns):
                value = df2_clean.iloc[row_idx][col_name]
                worksheet3.write(row_idx + 1, col_idx, value, normal_format)
        
        # Auto-adjust column widths for Sheet 3
        for i, col in enumerate(df2_clean.columns):
            max_length = max(len(str(col)), 10)
            if not df2_clean.empty:
                max_length = max(max_length, df2_clean[col].astype(str).str.len().max())
            worksheet3.set_column(i, i, min(max_length + 2, 50))
        
        # Close the writer
        writer.close()
        
        logger.info(f"Successfully created comparison report: {os.path.basename(output_path)}")
        print(f"Successfully created comparison report: {os.path.basename(output_path)}")
        print(f"Found {len(differences)} differences between the files")
        
        return True
        
    except Exception as e:
        logger.error(f"Error comparing files: {str(e)}")
        print(f"Error comparing files: {str(e)}")
        return False

def main():
    """Main function to run the file comparison"""
    print("=== GIS Morning Stock Changes Comparison ===")
    logger.info("Starting GIS Morning Stock Changes comparison")
    
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
    
    # Perform comparison
    success = compare_files(file1_path, file2_path, output_path)
    
    if success:
        print(f"\nComparison completed successfully!")
        print(f"Output saved to: {output_path}")
        logger.info("File comparison completed successfully")
    else:
        print("Comparison failed. Check the log file for details.")
        logger.error("File comparison failed")

if __name__ == "__main__":
    main()