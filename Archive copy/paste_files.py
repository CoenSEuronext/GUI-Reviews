import os
import time
import shutil
import csv
import logging
import logging.handlers
import stat
from watchdog.observers.polling import PollingObserver  # Changed to PollingObserver
from watchdog.events import FileSystemEventHandler
from datetime import datetime, timedelta
import re
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
import calendar

# Initialize holidays list
HOLIDAYS = []

# Global variable to track processed files
processed_files_today = set()

# Get the directory where the script is located
script_dir = os.path.dirname(os.path.abspath(__file__))
log_file = os.path.join(script_dir, 'csv_merger.log')

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
    
    logger = logging.getLogger('CSVMerger')
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

# Path definitions
SOURCE_FOLDER = r"V:\PM-Indices-IndexOperations\General\Daily downloadfiles\Monthly Archive"
MANUAL_OUTPUT_FOLDER = r"C:\Users\CSonneveld\OneDrive - Euronext\Documents\Projects\Archive copy\destination\Manual"
EOD_OUTPUT_FOLDER = r"C:\Users\CSonneveld\OneDrive - Euronext\Documents\Projects\Archive copy\destination\EOD"
SOD_OUTPUT_FOLDER = r"C:\Users\CSonneveld\OneDrive - Euronext\Documents\Projects\Archive copy\destination\SOD"

# Create output folders if they don't exist
for folder in [MANUAL_OUTPUT_FOLDER, EOD_OUTPUT_FOLDER, SOD_OUTPUT_FOLDER]:
    if not os.path.exists(folder):
        os.makedirs(folder)
        logger.info(f"Created output folder: {folder}")

def set_file_readonly(file_path):
    """Set a file to read-only mode"""
    try:
        # Get current file permissions
        current_permissions = os.stat(file_path).st_mode
        
        # Remove write permissions for owner, group, and others
        readonly_permissions = current_permissions & ~stat.S_IWRITE & ~stat.S_IWGRP & ~stat.S_IWOTH
        
        # Set the new permissions
        os.chmod(file_path, readonly_permissions)
        
        logger.info(f"Set file to read-only: {os.path.basename(file_path)}")
        # Removed print statement
        return True
        
    except Exception as e:
        logger.error(f"Error setting file to read-only {os.path.basename(file_path)}: {str(e)}")
        print(f"Error setting file to read-only {os.path.basename(file_path)}: {str(e)}")
        return False

def get_current_date_string():
    """Get current date in YYYYMMDD format"""
    return datetime.now().strftime('%Y%m%d')

def get_previous_workday_date(current_date=None):
    """Get the previous workday date in YYYYMMDD format, accounting for weekends and holidays"""
    if current_date is None:
        current_date = datetime.now()
    elif isinstance(current_date, str):
        # Convert YYYYMMDD string to datetime
        current_date = datetime.strptime(current_date, '%Y%m%d')
    
    # Go back one day
    prev_date = current_date - timedelta(days=1)
    
    # If it's a weekend or holiday, go back until we find a workday
    while prev_date.weekday() >= 5 or prev_date.strftime('%Y%m%d') in HOLIDAYS:  # 5 = Saturday, 6 = Sunday
        prev_date = prev_date - timedelta(days=1)
    
    # Return in YYYYMMDD format
    return prev_date.strftime('%Y%m%d')

def extract_date_from_filename(filename):
    """Extract date from filename using regex to find 'yyyymmdd' pattern"""
    date_match = re.search(r'(\d{8})', filename)
    if date_match:
        date_str = date_match.group(1)
        try:
            return date_str
        except ValueError:
            return None
    return None

def convert_single_csv_to_xlsx(csv_path, output_path):
    """Convert a single CSV file to XLSX format with proper formatting"""
    try:
        logger.info(f"Converting file: {os.path.basename(csv_path)} to XLSX")
        # Removed print statement
        
        # Read the CSV file with pandas - using latin1 encoding and semicolon delimiter
        df = pd.read_csv(csv_path, encoding='latin1', sep=';')
        
        # Make sure the output path has .xlsx extension
        if not output_path.endswith('.xlsx'):
            output_path = output_path.replace('.csv', '.xlsx')
            
        # Create an Excel writer using XlsxWriter as the engine
        writer = pd.ExcelWriter(output_path, engine='xlsxwriter')
        
        # Get workbook and define formats
        workbook = writer.book
        
        # Create a plain header format (no bold, no underline)
        header_format = workbook.add_format({
            'bold': False,
            'underline': False,
            'bottom': 0,
            'font_name': 'Verdana',
            'font_size': 10
        })
        
        # Create the default cell format for the entire worksheet
        cell_format = workbook.add_format({
            'font_name': 'Verdana',
            'font_size': 10
        })
        
        # Write the DataFrame to Excel without the index
        df.to_excel(writer, index=False, sheet_name='Data')
        
        # Get the worksheet and apply formats
        worksheet = writer.sheets['Data']
        
        # Apply Verdana 10 to the entire worksheet
        worksheet.set_column(0, 100, None, cell_format)  # Apply to all columns
        
        # Apply the plain header format to the first row
        for col_num, value in enumerate(df.columns.values):
            worksheet.write(0, col_num, value, header_format)
        
        # Apply date formatting to column B (typically the date column)
        date_format = workbook.add_format({
            'num_format': 'mm/dd/yyyy;@',
            'font_name': 'Verdana',
            'font_size': 10
        })
        
        # Column B in Excel is column 1 (0-indexed) in the DataFrame + 1 for the Excel column
        worksheet.set_column(1, 1, None, date_format)
        
        # Close the writer to save the file
        writer.close()
        
        # Set the output file to read-only
        set_file_readonly(output_path)
        
        logger.info(f"Successfully converted file: {os.path.basename(output_path)}")
        print(f"Successfully converted file: {os.path.basename(output_path)}")
        return True
        
    except Exception as e:
        logger.error(f"Error converting file: {str(e)}")
        print(f"Error converting file: {str(e)}")
        return False

def merge_csv_files(file1_path, file2_path, output_path):
    """Merge two CSV files and save as XLSX, keeping all rows from both files"""
    try:
        logger.info(f"Merging files: {os.path.basename(file1_path)} and {os.path.basename(file2_path)}")
        # Removed print statement
        
        # Read the first file with pandas - using latin1 encoding and semicolon delimiter
        df1 = pd.read_csv(file1_path, encoding='latin1', sep=';')
        
        # Read the second file with pandas
        df2 = pd.read_csv(file2_path, encoding='latin1', sep=';')
        
        # Combine the dataframes - keeping all rows including header of second file
        merged_df = pd.concat([df1, df2])
        
        # Convert column I to numeric if it exists (8th column, 0-indexed)
        if len(merged_df.columns) > 8:
            col_I_name = merged_df.columns[8]
            # Try to convert the column to numeric
            try:
                # Force conversion to numeric values
                merged_df[col_I_name] = pd.to_numeric(merged_df[col_I_name], errors='coerce')
                # Replace NaN with 0 or original value
                numeric_mask = pd.isna(merged_df[col_I_name])
                if numeric_mask.any():
                    original_values = df1[col_I_name].copy()
                    merged_df.loc[numeric_mask, col_I_name] = original_values.loc[numeric_mask]
            except Exception as e:
                logger.warning(f"Could not convert column I to numeric: {str(e)}")
        
        # Make sure the output path has .xlsx extension
        if not output_path.endswith('.xlsx'):
            output_path = output_path.replace('.csv', '.xlsx')
            
        # Create an Excel writer using XlsxWriter as the engine
        writer = pd.ExcelWriter(output_path, engine='xlsxwriter')
        
        # Get workbook and define formats
        workbook = writer.book
        
        # Create a plain header format (no bold, no underline)
        header_format = workbook.add_format({
            'bold': False,
            'underline': False,
            'bottom': 0,
            'font_name': 'Verdana',
            'font_size': 10
        })
        
        # Create the default cell format for the entire worksheet
        cell_format = workbook.add_format({
            'font_name': 'Verdana',
            'font_size': 10
        })
        
        # Write the DataFrame to Excel without the index
        merged_df.to_excel(writer, index=False, sheet_name='Merged_Data')
        
        # Get the worksheet and apply formats
        worksheet = writer.sheets['Merged_Data']
        
        # Apply Verdana 10 to the entire worksheet
        worksheet.set_column(0, 100, None, cell_format)  # Apply to all columns
        
        # Apply the plain header format to the first row
        for col_num, value in enumerate(merged_df.columns.values):
            worksheet.write(0, col_num, value, header_format)
        
        # Apply date formatting to column B (typically the date column)
        date_format = workbook.add_format({
            'num_format': 'mm/dd/yyyy;@',
            'font_name': 'Verdana',
            'font_size': 10
        })
        
        # Column B in Excel is column 1 (0-indexed) in the DataFrame + 1 for the Excel column
        worksheet.set_column(1, 1, None, date_format)
        
        # For column I, manually write each cell as a number instead of applying format
        if len(merged_df.columns) > 8:
            col_I_name = merged_df.columns[8]
            num_format = workbook.add_format({
                'font_name': 'Verdana',
                'font_size': 10,
                'num_format': '0'  # Integer format
            })
            
            # Write each cell in column I as a numeric value
            for row_idx, value in enumerate(merged_df[col_I_name]):
                try:
                    # Add 1 to row_idx to account for header row
                    if pd.notna(value):
                        # Try to convert to float first
                        num_value = float(value)
                        worksheet.write_number(row_idx + 1, 8, num_value, num_format)
                except (ValueError, TypeError):
                    # If conversion fails, write as is
                    worksheet.write(row_idx + 1, 8, value, cell_format)
        
        # Close the writer to save the file
        writer.close()
        
        # Set the output file to read-only
        set_file_readonly(output_path)
        
        logger.info(f"Successfully merged files: {os.path.basename(output_path)}")
        print(f"Successfully merged files: {os.path.basename(output_path)}")
        return True
        
    except Exception as e:
        logger.error(f"Error merging files: {str(e)}")
        print(f"Error merging files: {str(e)}")
        
        # Fallback method if pandas fails - with similar formatting
        try:
            logger.info(f"Attempting alternative merge method for: {os.path.basename(file1_path)} and {os.path.basename(file2_path)}")
            # Removed print statement
            
            # Read the first file
            with open(file1_path, 'r', encoding='latin1', newline='') as f1:
                reader1 = csv.reader(f1, delimiter=';')
                data1 = list(reader1)
            
            # Read the second file (keeping all rows including header)
            with open(file2_path, 'r', encoding='latin1', newline='') as f2:
                reader2 = csv.reader(f2, delimiter=';')
                data2 = list(reader2)  # Keep all rows including header
            
            # Combine data
            merged_data = data1 + data2
            
            # Convert the combined data to a pandas DataFrame
            df = pd.DataFrame(merged_data)
            
            # Make sure the output path has .xlsx extension
            if not output_path.endswith('.xlsx'):
                output_path = output_path.replace('.csv', '.xlsx')
                
            # Create an Excel writer using XlsxWriter as the engine
            writer = pd.ExcelWriter(output_path, engine='xlsxwriter')
            
            # Get workbook and define formats
            workbook = writer.book
            
            # Create a plain header format (no bold, no underline)
            header_format = workbook.add_format({
                'bold': False,
                'underline': False,
                'bottom': 0,
                'font_name': 'Verdana',
                'font_size': 10
            })
            
            # Create the default cell format for the entire worksheet
            cell_format = workbook.add_format({
                'font_name': 'Verdana',
                'font_size': 10
            })
            
            # Write the DataFrame to Excel without the index
            df.to_excel(writer, index=False, header=False, sheet_name='Merged_Data')
            
            # Get the worksheet and apply formats
            worksheet = writer.sheets['Merged_Data']
            
            # Apply Verdana 10 to the entire worksheet
            worksheet.set_column(0, 100, None, cell_format)  # Apply to all columns
            
            # Apply the plain header format to the first row if there's data
            if len(df) > 0 and len(df.columns) > 0:
                for col_num in range(len(df.columns)):
                    if len(df) > 0:  # Make sure there's at least one row
                        value = df.iloc[0, col_num] if not pd.isna(df.iloc[0, col_num]) else ""
                        worksheet.write(0, col_num, value, header_format)
            
            # Apply date formatting to column B
            date_format = workbook.add_format({
                'num_format': 'mm/dd/yyyy;@',
                'font_name': 'Verdana',
                'font_size': 10
            })
            worksheet.set_column(1, 1, None, date_format)
            
            # Apply text-as-number formatting to column I
            text_number_format = workbook.add_format({
                'num_format': '0',  # Format as a number without decimals
                'font_name': 'Verdana',
                'font_size': 10
            })
            
            # Column I in Excel is column 8 (0-indexed) in the DataFrame
            worksheet.set_column(8, 8, None, text_number_format)
            
            # Close the writer to save the file
            writer.close()
            
            # Set the output file to read-only
            set_file_readonly(output_path)
            
            logger.info(f"Successfully merged files using alternative method: {os.path.basename(output_path)}")
            print(f"Successfully merged files using alternative method: {os.path.basename(output_path)}")
            return True
            
        except Exception as e2:
            logger.error(f"Alternative merge method also failed: {str(e2)}")
            print(f"Alternative merge method also failed: {str(e2)}")
            return False

def get_merge_groups(date_str):
    """Define merge groups for a specific date"""
    return {
        "MANUAL": {
            "files": [
                f"TTMIndexEU1_GIS_MANUAL_STOCK_{date_str}.csv",
                f"TTMIndexUS1_GIS_NXTD_STOCK_{date_str}.csv",
                f"TTMIndexEU1_GIS_MANUAL_INDEX_{date_str}.csv",
                f"TTMIndexUS1_GIS_NXTD_INDEX_{date_str}.csv"
            ],
            "output_dir": MANUAL_OUTPUT_FOLDER,
            "output_files": [
                f"EU_MANUAL_US_NXTD_STOCK_MERGED_{date_str}.xlsx",
                f"EU_MANUAL_US_NXTD_INDEX_MERGED_{date_str}.xlsx"
            ],
            "merge_pairs": [
                (0, 1, 0),  # (file1_index, file2_index, output_index)
                (2, 3, 1)
            ],
            "single_files": []  # No single file conversions for MANUAL
        },
        "EOD": {
            "files": [
                f"TTMIndexEU1_GIS_EOD_STOCK_{date_str}.csv",
                f"TTMIndexUS1_GIS_EOD_STOCK_{date_str}.csv",
                f"TTMIndexEU1_GIS_EOD_INDEX_{date_str}.csv",
                f"TTMIndexUS1_GIS_EOD_INDEX_{date_str}.csv",
                f"TTMStrategy_GIS_EOD_INDEX_{date_str}.csv"  # Added strategy file
            ],
            "output_dir": EOD_OUTPUT_FOLDER,
            "output_files": [
                f"EU_EOD_US_EOD_STOCK_MERGED_{date_str}.xlsx",
                f"EU_EOD_US_EOD_INDEX_MERGED_{date_str}.xlsx",
                f"TTMStrategy_GIS_EOD_INDEX_{date_str}.xlsx"  # Added strategy output
            ],
            "merge_pairs": [
                (0, 1, 0),
                (2, 3, 1)
            ],
            "single_files": [
                (4, 2)  # (source_file_index, output_file_index) for strategy file
            ]
        },
        "SOD": {
            "files": [
                f"TTMIndexEU1_GIS_SOD_STOCK_{date_str}.csv",
                f"TTMIndexUS1_GIS_SOD_STOCK_{date_str}.csv",
                f"TTMIndexEU1_GIS_SOD_INDEX_{date_str}.csv",
                f"TTMIndexUS1_GIS_SOD_INDEX_{date_str}.csv"
            ],
            "output_dir": SOD_OUTPUT_FOLDER,
            "output_files": [
                f"EU_SOD_US_SOD_STOCK_MERGED_{date_str}.xlsx",
                f"EU_SOD_US_SOD_INDEX_MERGED_{date_str}.xlsx"
            ],
            "merge_pairs": [
                (0, 1, 0),
                (2, 3, 1)
            ],
            "single_files": []  # No single file conversions for SOD
        }
    }

def check_previous_workday_files():
    """Check if output files from the previous workday exist and create them if missing"""
    try:
        # Get current date and previous workday
        current_date = get_current_date_string()
        prev_workday = get_previous_workday_date()
        
        logger.debug(f"Checking for previous workday ({prev_workday}) output files...")
        # Don't print to console to avoid excessive messages
        
        # Get merge groups for previous workday
        prev_merge_groups = get_merge_groups(prev_workday)
        
        # For each merge group, check if output files exist
        for group_name, group_data in prev_merge_groups.items():
            missing_outputs = []
            
            # Check if each output file exists
            for output_file in group_data["output_files"]:
                output_path = os.path.join(group_data["output_dir"], output_file)
                
                if not os.path.exists(output_path):
                    missing_outputs.append(output_file)
            
            # If any output files are missing, check if source files exist to create them
            if missing_outputs:
                logger.info(f"Missing {group_name} output files from previous workday: {missing_outputs}")
                # Removed print statement
                
                # Check if all source files for this group exist
                source_files_exist = True
                for file in group_data["files"]:
                    file_path = os.path.join(SOURCE_FOLDER, file)
                    if not os.path.exists(file_path):
                        source_files_exist = False
                        break
                
                if source_files_exist:
                    logger.info(f"Found all source files for {group_name} from previous workday. Creating missing output files.")
                    # Removed print statement
                    
                    # Perform merges for this group
                    for file1_idx, file2_idx, output_idx in group_data["merge_pairs"]:
                        if group_data["output_files"][output_idx] in missing_outputs:
                            file1_path = os.path.join(SOURCE_FOLDER, group_data["files"][file1_idx])
                            file2_path = os.path.join(SOURCE_FOLDER, group_data["files"][file2_idx])
                            output_path = os.path.join(group_data["output_dir"], group_data["output_files"][output_idx])
                            
                            if merge_csv_files(file1_path, file2_path, output_path):
                                logger.info(f"Created previous workday file: {os.path.basename(output_path)}")
                                # Removed print statement
                    
                    # Handle single file conversions for this group
                    if "single_files" in group_data:
                        for source_idx, output_idx in group_data["single_files"]:
                            if group_data["output_files"][output_idx] in missing_outputs:
                                source_path = os.path.join(SOURCE_FOLDER, group_data["files"][source_idx])
                                output_path = os.path.join(group_data["output_dir"], group_data["output_files"][output_idx])
                                
                                if convert_single_csv_to_xlsx(source_path, output_path):
                                    logger.info(f"Created previous workday file: {os.path.basename(output_path)}")
                                    # Removed print statement
                else:
                    missing_files = [file for file in group_data["files"] if not os.path.exists(os.path.join(SOURCE_FOLDER, file))]
                    logger.info(f"Cannot create {group_name} output files for previous workday. Missing source files: {missing_files}")
                    # Removed print statement
            else:
                logger.debug(f"All {group_name} output files from previous workday already exist.")
    
    except Exception as e:
        logger.error(f"Error checking previous workday files: {str(e)}")
        print(f"Error checking previous workday files: {str(e)}")

def check_files_for_merge():
    """Check if all necessary files are available for merging"""
    try:
        global processed_files_today
        
        # Get all files in the source directory
        files = os.listdir(SOURCE_FOLDER)
        
        # Get current date string
        current_date = get_current_date_string()
        
        # Reset processed files if it's a new day
        current_day_key = f"daily_check_{current_date}"
        if current_day_key not in processed_files_today:
            processed_files_today.clear()
            processed_files_today.add(current_day_key)
        
        # Get merge groups for current date
        merge_groups = get_merge_groups(current_date)
        
        # Check each merge group
        for group_name, group_data in merge_groups.items():
            # Process merge operations
            merge_files_exist = True
            merge_files_needed = set()
            for file1_idx, file2_idx, output_idx in group_data["merge_pairs"]:
                merge_files_needed.add(file1_idx)
                merge_files_needed.add(file2_idx)
            
            # Check if merge files exist
            for file_idx in merge_files_needed:
                file_path = os.path.join(SOURCE_FOLDER, group_data["files"][file_idx])
                if not os.path.exists(file_path):
                    merge_files_exist = False
                    break
            
            # Create a unique key for this merge group operation
            merge_key = f"{group_name}_merge_{current_date}"
            
            if merge_files_exist and merge_key not in processed_files_today:
                logger.info(f"All {group_name} merge files found. Starting merge process.")
                # Removed print statement
                
                # Mark this merge operation as processed
                processed_files_today.add(merge_key)
                
                # Perform merges for this group
                for file1_idx, file2_idx, output_idx in group_data["merge_pairs"]:
                    file1_path = os.path.join(SOURCE_FOLDER, group_data["files"][file1_idx])
                    file2_path = os.path.join(SOURCE_FOLDER, group_data["files"][file2_idx])
                    output_path = os.path.join(group_data["output_dir"], group_data["output_files"][output_idx])
                    
                    # Skip if output file already exists
                    if os.path.exists(output_path):
                        logger.debug(f"Output file already exists: {os.path.basename(output_path)}. Skipping merge.")
                        continue
                    
                    if merge_csv_files(file1_path, file2_path, output_path):
                        logger.info(f"Merged: {os.path.basename(file1_path)} + {os.path.basename(file2_path)} -> {os.path.basename(output_path)}")
            elif merge_files_exist and merge_key in processed_files_today:
                # Files exist but already processed - no need to log
                pass
            else:
                merge_missing_files = [group_data["files"][idx] for idx in merge_files_needed if not os.path.exists(os.path.join(SOURCE_FOLDER, group_data["files"][idx]))]
                # Only log missing files once per day
                missing_key = f"{group_name}_missing_{current_date}"
                if missing_key not in processed_files_today:
                    logger.debug(f"Not all {group_name} merge files found. Missing: {merge_missing_files}")
                    processed_files_today.add(missing_key)
            
            # Process single file conversions independently
            if "single_files" in group_data:
                for source_idx, output_idx in group_data["single_files"]:
                    source_path = os.path.join(SOURCE_FOLDER, group_data["files"][source_idx])
                    output_path = os.path.join(group_data["output_dir"], group_data["output_files"][output_idx])
                    
                    # Create a unique key for this single file operation
                    single_file_key = f"{group_name}_single_{source_idx}_{current_date}"
                    
                    # Check if source file exists and hasn't been processed
                    if os.path.exists(source_path) and single_file_key not in processed_files_today:
                        # Skip if output file already exists
                        if os.path.exists(output_path):
                            logger.debug(f"Output file already exists: {os.path.basename(output_path)}. Skipping conversion.")
                            processed_files_today.add(single_file_key)
                            continue
                        
                        logger.info(f"Found {group_name} single file for conversion: {os.path.basename(source_path)}")
                        # Removed print statement
                        
                        # Mark this single file operation as processed
                        processed_files_today.add(single_file_key)
                        
                        if convert_single_csv_to_xlsx(source_path, output_path):
                            logger.info(f"Converted: {os.path.basename(source_path)} -> {os.path.basename(output_path)}")
                    elif not os.path.exists(source_path):
                        # Only log missing single files once per day
                        missing_single_key = f"{group_name}_single_missing_{source_idx}_{current_date}"
                        if missing_single_key not in processed_files_today:
                            logger.debug(f"Single file not found: {group_data['files'][source_idx]}")
                            processed_files_today.add(missing_single_key)
    
    except Exception as e:
        logger.error(f"Error checking files for merge: {str(e)}")
        print(f"Error checking files for merge: {str(e)}")

class FolderHandler(FileSystemEventHandler):
    def on_created(self, event):
        if event.is_directory:
            return
        logger.info(f"New file detected: {os.path.basename(event.src_path)}")
        # Force a small delay to ensure the file is fully written
        time.sleep(1)
        check_files_for_merge()
        # Also check previous workday files in case they were just added
        check_previous_workday_files()
        
    def on_modified(self, event):
        if event.is_directory:
            return
        logger.info(f"File modified: {os.path.basename(event.src_path)}")
        # Force a small delay to ensure the file is fully written
        time.sleep(1)
        check_files_for_merge()
        # Also check previous workday files in case they were just modified
        check_previous_workday_files()

def add_holiday_list(holidays=None):
    """Add a list of holidays (in YYYYMMDD format) to skip when calculating previous workday
    
    Args:
        holidays (list): List of holiday dates in YYYYMMDD format
    """
    global HOLIDAYS
    if holidays is None:
        # Default holidays for 2024-2044
        HOLIDAYS = [
            # 2024 Holidays
            "20240101", "20240329", "20240401", "20240501", "20241225", "20241226", "20241231",
            # 2025 Holidays 
            "20250101", "20250418", "20250421", "20250501", "20251225", "20251226", "20251231",
            # 2026 Holidays
            "20260101", "20260403", "20260406", "20260501", "20261225", "20261226", "20261231",
            # 2027 Holidays
            "20270101", "20270326", "20270329", "20270501", "20271225", "20271226", "20271231",
            # 2028 Holidays
            "20280101", "20280414", "20280417", "20280501", "20281225", "20281226", "20281231",
            # 2029 Holidays
            "20290101", "20290330", "20290402", "20290501", "20291225", "20291226", "20291231",
            # 2030 Holidays
            "20300101", "20300419", "20300422", "20300501", "20301225", "20301226", "20301231",
            # 2031 Holidays
            "20310101", "20310411", "20310414", "20310501", "20311225", "20311226", "20311231",
            # 2032 Holidays
            "20320101", "20320326", "20320329", "20320501", "20321225", "20321226", "20321231",
            # 2033 Holidays
            "20330101", "20330415", "20330418", "20330501", "20331225", "20331226", "20331231",
            # 2034 Holidays
            "20340101", "20340407", "20340410", "20340501", "20341225", "20341226", "20341231",
            # 2035 Holidays
            "20350101", "20350323", "20350326", "20350501", "20351225", "20351226", "20351231",
            # 2036 Holidays
            "20360101", "20360411", "20360414", "20360501", "20361225", "20361226", "20361231",
            # 2037 Holidays
            "20370101", "20370403", "20370406", "20370501", "20371225", "20371226", "20371231",
            # 2038 Holidays
            "20380101", "20380423", "20380426", "20380501", "20381225", "20381226", "20381231",
            # 2039 Holidays
            "20390101", "20390408", "20390411", "20390501", "20391225", "20391226", "20391231",
            # 2040 Holidays
            "20400101", "20400330", "20400402", "20400501", "20401225", "20401226", "20401231",
            # 2041 Holidays
            "20410101", "20410419", "20410422", "20410501", "20411225", "20411226", "20411231",
            # 2042 Holidays
            "20420101", "20420404", "20420407", "20420501", "20421225", "20421226", "20421231",
            # 2043 Holidays
            "20430101", "20430327", "20430330", "20430501", "20431225", "20431226", "20431231",
            # 2044 Holidays
            "20440101", "20440415", "20440418", "20440501", "20441225", "20441226", "20441231"
        ]
    else:
        HOLIDAYS = holidays
    
    logger.info(f"Added {len(HOLIDAYS)} holidays to the exclusion list")
    return HOLIDAYS

def monitor_folder():
    """Monitor the source folder for file changes"""
    logger.info("Starting folder monitor for CSV merging...")
    print("Starting folder monitor for CSV merging...")
    
    # Initialize holidays list
    add_holiday_list()
    
    # First check for previous workday's files
    logger.info("Checking previous workday files...")
    # Removed print statement
    check_previous_workday_files()
    
    # Then check if any existing files can be merged for current day
    logger.info("Checking existing files for current day...")
    # Removed print statement
    check_files_for_merge()
    
    # Set up folder monitoring with PollingObserver
    event_handler = FolderHandler()
    observer = PollingObserver(timeout=1)  # Poll every 1 second
    observer.schedule(event_handler, SOURCE_FOLDER, recursive=False)
    observer.start()
    
    print(f"\nMonitoring folder: {SOURCE_FOLDER}")
    print("Monitor is running... (Press Ctrl+C to stop)")
    
    try:
        while True:
            # Periodically check for files that can be merged
            check_files_for_merge()
            # Also check for previous workday files without console output
            check_previous_workday_files()
            time.sleep(30)  # Check every 30 seconds
    except KeyboardInterrupt:
        observer.stop()
        print("\nMonitoring stopped")
        logger.info("Monitoring stopped")
    
    observer.join()

if __name__ == "__main__":
    monitor_folder()