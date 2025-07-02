import time
import os
import shutil
from watchdog.observers import Observer
from watchdog.observers.polling import PollingObserver
from watchdog.events import FileSystemEventHandler
from datetime import datetime, timedelta
import logging
import logging.handlers
import re
from concurrent.futures import ThreadPoolExecutor
import queue
import socket
import threading
import csv
import stat
import pandas as pd
import calendar
import functools
import subprocess

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
log_file = os.path.join(script_dir, 'unified_monitor.log')

try:
    handler = logging.handlers.RotatingFileHandler(
        log_file,
        maxBytes=10*1024*1024,  # 10MB
        backupCount=5,
        encoding='utf-8'
    )
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    handler.setFormatter(formatter)
    
    logger = logging.getLogger('UnifiedMonitor')
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)
except Exception as e:
    print(f"Warning: Could not set up logging to file: {str(e)}")
    # Set up console-only logging as fallback
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

# Network timeout settings
socket.setdefaulttimeout(30)  # 30 seconds timeout for network operations

# Queue for rate limiting
file_queue = queue.Queue()
MAX_QUEUE_SIZE = 100
NETWORK_DELAY = 0.1  # 100ms delay between operations

# CSV Merger specific variables
HOLIDAYS = []
processed_files_today = set()

# ============================================================================
# CONFIGURATION SECTION
# ============================================================================

# File monitoring paths
SOURCE_FOLDER = r"\\pbgfshqa08601v\gis_ttm\Archive"
DESTINATION_FOLDER = r"V:\PM-Indices-IndexOperations\General\Daily downloadfiles\Monthly Archive"

# CSV merger output paths
MANUAL_OUTPUT_FOLDER = r"V:\PM-Indices-IndexOperations\General\Daily downloadfiles\Monthly Archive\Merged files\Manual"
EOD_OUTPUT_FOLDER = r"V:\PM-Indices-IndexOperations\General\Daily downloadfiles\Monthly Archive\Merged files\EOD"
SOD_OUTPUT_FOLDER = r"V:\PM-Indices-IndexOperations\General\Daily downloadfiles\Monthly Archive\Merged files\SOD"

# ============================================================================
# FILE MONITORING FUNCTIONS
# ============================================================================

def extract_date_from_filename(filename):
    """Extract date from filename using regex to find 'yyyymmdd' pattern"""
    date_match = re.search(r'(\d{8})', filename)
    if date_match:
        date_str = date_match.group(1)
        try:
            return datetime.strptime(date_str, '%Y%m%d')
        except ValueError:
            return None
    return None

@timer
def compare_folders(source_path, destination_path):
    """Compare source and destination folders to identify missing files"""
    try:
        # Get lists of files in both folders
        source_files = set(f for f in os.listdir(source_path) if os.path.isfile(os.path.join(source_path, f)))
        dest_files = set(f for f in os.listdir(destination_path) if os.path.isfile(os.path.join(destination_path, f)))
        
        # Find missing files that are within 100 days
        missing_files = []
        for filename in source_files - dest_files:
            file_date = extract_date_from_filename(filename)
            if file_date and (datetime.now() - file_date).days <= 100:
                missing_files.append(filename)
        
        if missing_files:
            logger.info(f"Found {len(missing_files)} missing files:")
            for file in missing_files[:10]:  # Log first 10 missing files
                logger.info(f"Missing file: {file}")
                print(f"Missing file: {file}")
            if len(missing_files) > 10:
                logger.info(f"... and {len(missing_files) - 10} more")
                print(f"... and {len(missing_files) - 10} more")
        
        return missing_files
    except Exception as e:
        logger.error(f"Error comparing folders: {str(e)}")
        return []

def retry_operation(operation, max_retries=3, delay=5):
    """Retry an operation with exponential backoff"""
    for attempt in range(max_retries):
        try:
            return operation()
        except (OSError, IOError) as e:
            if attempt == max_retries - 1:
                raise
            wait_time = delay * (2 ** attempt)
            logger.warning(f"Operation failed, retrying in {wait_time} seconds...")
            time.sleep(wait_time)

def safe_file_operation(operation, file_path, error_msg):
    """Safely perform a file operation with retries"""
    try:
        return retry_operation(lambda: operation(file_path))
    except Exception as e:
        logger.error(f"{error_msg}: {str(e)}")
        return None

def should_copy_file(src_path, dest_path):
    """Check if file should be copied with network-safe operations - OPTIMIZED"""
    try:
        # If destination doesn't exist, definitely copy
        if not os.path.exists(dest_path):
            logger.debug(f"File {os.path.basename(src_path)} doesn't exist in destination - will copy")
            return True
        
        # OPTIMIZED: Quick size check first (fastest comparison)
        try:
            src_size = os.path.getsize(src_path)
            dest_size = os.path.getsize(dest_path)
            
            if src_size != dest_size:
                logger.info(f"File {os.path.basename(src_path)} has different size - will copy")
                return True
        except (OSError, IOError):
            # If we can't get sizes, assume we need to copy
            return True
        
        # OPTIMIZED: Skip modification time check for speed (size check is usually sufficient)
        # Files of same size are very likely to be identical
        return False
            
    except Exception as e:
        logger.error(f"Error comparing files: {str(e)}")
        return False

def copy_file(src_path, dest_path, filename):
    """Copy file with network safety measures and set as read-only"""
    try:
        if should_copy_file(src_path, dest_path):
            def do_copy():
                logger.info(f"Starting copy of {filename}")
                # Remove destination file if it exists to ensure clean copy
                if os.path.exists(dest_path):
                    try:
                        # Remove read-only attribute if it exists
                        os.chmod(dest_path, 0o777)  # Give all permissions temporarily
                        os.remove(dest_path)
                    except Exception as e:
                        logger.error(f"Error removing existing file {filename}: {str(e)}")
                        raise
                    
                # Copy the file
                shutil.copy2(src_path, dest_path)
                
                # Verify the copy
                if not os.path.exists(dest_path):
                    raise Exception("File not copied successfully")
                
                if os.path.getsize(src_path) != os.path.getsize(dest_path):
                    raise Exception("File sizes don't match after copy")
                
                # Set destination file as read-only
                os.chmod(dest_path, 0o444)  # Read-only for all users
                
                logger.info(f"Successfully copied {filename}")
                time.sleep(NETWORK_DELAY)  # Rate limiting
            
            retry_operation(do_copy)
            print(f"Copied {filename} to destination and set as read-only")
            
            # REMOVED: Immediate CSV merge check - now only done periodically
            
            return True
        else:
            logger.debug(f"Skipped {filename} - already up to date")
            return False
    except Exception as e:
        logger.error(f"Error copying {filename}: {str(e)}")
        print(f"Error copying {filename}: {str(e)}")
        return False

def move_file_to_archive(src_path, archive_path, filename):
    """Move file to archive folder with safety measures"""
    try:
        def do_move():
            logger.info(f"Starting move of {filename} to archive")
            
            # Remove read-only attribute if it exists
            if os.path.exists(src_path):
                try:
                    os.chmod(src_path, 0o777)  # Give all permissions temporarily
                except Exception as e:
                    logger.warning(f"Could not change permissions for {filename}: {str(e)}")
            
            # Move the file
            shutil.move(src_path, archive_path)
            
            # Verify the move
            if not os.path.exists(archive_path):
                raise Exception("File not moved successfully")
            
            # Set archive file as read-only
            os.chmod(archive_path, 0o444)  # Read-only for all users
            
            logger.info(f"Successfully moved {filename} to archive")
            time.sleep(NETWORK_DELAY)  # Rate limiting
        
        retry_operation(do_move)
        print(f"Moved {filename} to archive folder")
        return True
    except Exception as e:
        logger.error(f"Error moving {filename} to archive: {str(e)}")
        print(f"Error moving {filename} to archive: {str(e)}")
        return False

@timer
def process_file_batch(files, source_path, destination_path):
    """Process a batch of files with early filtering"""
    copied_files = []
    skipped_files = 0
    
    for filename in files:
        # Quick early filtering - check if destination exists first
        dest_path = os.path.join(destination_path, filename)
        if os.path.exists(dest_path):
            skipped_files += 1
            continue  # Skip expensive checks if file already exists
        
        src_path = os.path.join(source_path, filename)
        if not os.path.isfile(src_path):
            continue
            
        file_date = extract_date_from_filename(filename)
        if file_date:
            # Only copy if file is within 100 days
            if (datetime.now() - file_date).days <= 100:
                if copy_file(src_path, dest_path, filename):
                    copied_files.append(filename)
    
    if skipped_files > 0:
        logger.debug(f"Skipped {skipped_files} files that already exist in destination")
    
    # REMOVED: CSV merge triggering from batch processing

@timer
def bulk_copy_missing_files(source_path, destination_path, missing_files):
    """Use robocopy for bulk copying - much faster for network operations"""
    if not missing_files:
        print("No files to bulk copy.")
        return True
    
    try:
        # Create temporary file list for robocopy
        temp_list = os.path.join(script_dir, 'temp_copy_list.txt')
        with open(temp_list, 'w', encoding='utf-8') as f:
            for filename in missing_files:
                f.write(f"{filename}\n")
        
        print(f"Bulk copying {len(missing_files)} files with robocopy...")
        logger.info(f"Starting bulk copy of {len(missing_files)} files")
        
        # Use robocopy with file list - much faster than individual copies
        cmd = [
            'robocopy', 
            source_path, 
            destination_path,
            '/XO',              # Skip older files
            '/MT:8',            # Multi-threaded (8 threads)
            '/R:3',             # 3 retries
            '/W:1',             # 1 second wait between retries
            '/NP',              # No progress per file (cleaner output)
            '/NDL',             # No directory listing
            '/NJH',             # No job header
            '/NJS',             # No job summary
            '/TEE',             # Output to console and log
        ]
        
        # Add each file individually to robocopy command
        for filename in missing_files:
            cmd.append(filename)
        
        # Execute robocopy
        start_time = time.time()
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=3600)  # 1 hour timeout
        
        copy_time = time.time() - start_time
        print(f"Robocopy completed in {copy_time:.1f} seconds")
        
        # Robocopy exit codes: 0=no files copied, 1=files copied successfully, 2=extra files/dirs found
        if result.returncode in [0, 1, 2]:
            logger.info(f"Robocopy completed successfully. Return code: {result.returncode}")
            
            # Set files to read-only in bulk
            print("Setting copied files to read-only...")
            readonly_count = 0
            for filename in missing_files:
                dest_file = os.path.join(destination_path, filename)
                if os.path.exists(dest_file):
                    try:
                        os.chmod(dest_file, 0o444)  # Read-only
                        readonly_count += 1
                    except Exception as e:
                        logger.warning(f"Could not set {filename} to read-only: {str(e)}")
            
            print(f"Bulk copy completed successfully! {readonly_count} files set to read-only")
            logger.info(f"Bulk copy successful: {readonly_count} files copied and set to read-only")
            
        else:
            logger.error(f"Robocopy failed with return code: {result.returncode}")
            logger.error(f"Robocopy stderr: {result.stderr}")
            print(f"Robocopy failed with return code: {result.returncode}")
            print(f"Error details: {result.stderr}")
            return False
        
        # Clean up temp file
        try:
            os.remove(temp_list)
        except:
            pass
        
        return True
        
    except subprocess.TimeoutExpired:
        logger.error("Robocopy operation timed out after 1 hour")
        print("Robocopy operation timed out")
        return False
    except Exception as e:
        logger.error(f"Error in bulk copy: {str(e)}")
        print(f"Error in bulk copy: {str(e)}")
        return False

@timer
def process_existing_files_bulk(source_path, destination_path, max_age_days):
    """Process existing files using bulk copy strategy"""
    print("Identifying missing files for bulk copy...")
    
    try:
        # Step 1: Quickly identify ALL missing files
        missing_files = compare_folders(source_path, destination_path)
        
        if not missing_files:
            print("No missing files found - destination is up to date!")
            return
        
        # Step 2: Filter by date (within max_age_days)
        recent_missing = []
        for filename in missing_files:
            file_date = extract_date_from_filename(filename)
            if file_date and (datetime.now() - file_date).days <= max_age_days:
                recent_missing.append(filename)
        
        old_files_count = len(missing_files) - len(recent_missing)
        if old_files_count > 0:
            print(f"Skipping {old_files_count} files older than {max_age_days} days")
        
        if not recent_missing:
            print(f"No recent files (within {max_age_days} days) need copying")
            return
        
        print(f"Found {len(recent_missing)} recent files that need copying")
        logger.info(f"Identified {len(recent_missing)} files for bulk copy")
        
        # Step 3: Bulk copy all missing files at once
        success = bulk_copy_missing_files(source_path, destination_path, recent_missing)
        
        if success:
            print("Bulk file copying completed successfully!")
        else:
            print("Bulk copy had issues - check logs for details")
            # Fallback to individual file copying
            print("Falling back to individual file copying...")
            for filename in recent_missing[:100]:  # Limit fallback to first 100 files
                src_path = os.path.join(source_path, filename)
                dest_path = os.path.join(destination_path, filename)
                copy_file(src_path, dest_path, filename)
        
        print("File copying phase completed. CSV merging will be handled periodically.")
        
    except Exception as e:
        logger.error(f"Error during bulk file processing: {str(e)}")
        print(f"Error during bulk file processing: {str(e)}")
        # Fallback to original method
        print("Falling back to original file processing method...")
        process_existing_files_original(source_path, destination_path, max_age_days)
@timer
def process_existing_files_original(source_path, destination_path, max_age_days):
    """Original file processing method as fallback"""
    print("Scanning for files...")
    
    try:
        # OPTIMIZED: Faster file scanning without batching
        start_scan = time.time()
        all_files = [f for f in os.listdir(source_path) if os.path.isfile(os.path.join(source_path, f))]
        scan_time = time.time() - start_scan
        
        total_files = len(all_files)
        print(f"Found {total_files} files to process (scan took {scan_time:.1f}s)")
        logger.info(f"Found {total_files} files to process")
        
        # OPTIMIZED: Larger batch size and sequential processing (no threading)
        batch_size = 50000  # Larger batches for fewer function calls
        
        for i in range(0, total_files, batch_size):
            batch = all_files[i:i + batch_size]
            batch_start = time.time()
            print(f"Processing files {i + 1} to {min(i + batch_size, total_files)} of {total_files}")
            
            # REMOVED: ThreadPoolExecutor - process sequentially for better network performance
            process_file_batch(batch, source_path, destination_path)
            
            batch_time = time.time() - batch_start
            print(f"  ✓ Batch completed in {batch_time:.1f}s")
            
            # REMOVED: Rate limiting sleep between batches for speed
        
        # After processing, verify and report missing files
        print("\nVerifying file copy completion...")
        missing_files = compare_folders(source_path, destination_path)
        if missing_files:
            print(f"\nWARNING: {len(missing_files)} files were not copied successfully.")
            # Try to copy missing files again
            print("Attempting to copy missing files...")
            process_file_batch(missing_files, source_path, destination_path)
        
        # REMOVED: Immediate CSV merger check after initial processing
        print("\nFile copying completed. CSV merging will be handled periodically.")
                
    except Exception as e:
        logger.error(f"Error during initial scan: {str(e)}")
        print(f"Error during initial scan: {str(e)}")

def check_and_move_old_files(destination_path, archive_folder):
    """Check destination folder for files older than 120 days and move them to archive"""
    try:
        # Ensure archive folder exists
        if not os.path.exists(archive_folder):
            os.makedirs(archive_folder)
            logger.info(f"Created archive folder: {archive_folder}")
        
        current_date = datetime.now()
        moved_count = 0
        
        for filename in os.listdir(destination_path):
            file_path = os.path.join(destination_path, filename)
            if os.path.isfile(file_path):
                file_date = extract_date_from_filename(filename)
                if file_date:
                    days_old = (current_date - file_date).days
                    if days_old >= 120:
                        archive_path = os.path.join(archive_folder, filename)
                        
                        # Check if file already exists in archive
                        if os.path.exists(archive_path):
                            logger.debug(f"File {filename} already exists in archive, skipping")
                            continue
                        
                        print(f"Moving old file to archive: {filename} ({days_old} days old)")
                        logger.info(f"Moving old file to archive: {filename} - {days_old} days old")
                        
                        if move_file_to_archive(file_path, archive_path, filename):
                            moved_count += 1
        
        if moved_count > 0:
            print(f"Moved {moved_count} old files to archive folder")
            logger.info(f"Moved {moved_count} old files to archive folder")
        
    except Exception as e:
        logger.error(f"Error checking and moving old files: {str(e)}")
        print(f"Error checking and moving old files: {str(e)}")

# ============================================================================
# CSV MERGER FUNCTIONS
# ============================================================================

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
        return True
        
    except Exception as e:
        logger.error(f"Error setting file to read-only {os.path.basename(file_path)}: {str(e)}")
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

def extract_date_from_csv_filename(filename):
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
        logger.info(f"Converting CSV file: {os.path.basename(csv_path)} to XLSX")
        
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
        
        logger.info(f"Successfully converted CSV file: {os.path.basename(output_path)}")
        print(f"Successfully converted CSV file: {os.path.basename(output_path)}")
        return True
        
    except Exception as e:
        logger.error(f"Error converting CSV file: {str(e)}")
        print(f"Error converting CSV file: {str(e)}")
        return False

def merge_csv_files(file1_path, file2_path, output_path):
    """Merge two CSV files and save as XLSX, keeping all rows from both files"""
    try:
        logger.info(f"Merging CSV files: {os.path.basename(file1_path)} and {os.path.basename(file2_path)}")
        
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
        
        logger.info(f"Successfully merged CSV files: {os.path.basename(output_path)}")
        print(f"Successfully merged CSV files: {os.path.basename(output_path)}")
        return True
        
    except Exception as e:
        logger.error(f"Error merging CSV files: {str(e)}")
        print(f"Error merging CSV files: {str(e)}")
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
                
                # Check if all source files for this group exist
                source_files_exist = True
                for file in group_data["files"]:
                    file_path = os.path.join(DESTINATION_FOLDER, file)  # Use destination folder as source for CSV merger
                    if not os.path.exists(file_path):
                        source_files_exist = False
                        break
                
                if source_files_exist:
                    logger.info(f"Found all source files for {group_name} from previous workday. Creating missing output files.")
                    
                    # Perform merges for this group
                    for file1_idx, file2_idx, output_idx in group_data["merge_pairs"]:
                        if group_data["output_files"][output_idx] in missing_outputs:
                            file1_path = os.path.join(DESTINATION_FOLDER, group_data["files"][file1_idx])
                            file2_path = os.path.join(DESTINATION_FOLDER, group_data["files"][file2_idx])
                            output_path = os.path.join(group_data["output_dir"], group_data["output_files"][output_idx])
                            
                            if merge_csv_files(file1_path, file2_path, output_path):
                                logger.info(f"Created previous workday file: {os.path.basename(output_path)}")
                    
                    # Handle single file conversions for this group
                    if "single_files" in group_data:
                        for source_idx, output_idx in group_data["single_files"]:
                            if group_data["output_files"][output_idx] in missing_outputs:
                                source_path = os.path.join(DESTINATION_FOLDER, group_data["files"][source_idx])
                                output_path = os.path.join(group_data["output_dir"], group_data["output_files"][output_idx])
                                
                                if convert_single_csv_to_xlsx(source_path, output_path):
                                    logger.info(f"Created previous workday file: {os.path.basename(output_path)}")
                else:
                    missing_files = [file for file in group_data["files"] if not os.path.exists(os.path.join(DESTINATION_FOLDER, file))]
                    logger.info(f"Cannot create {group_name} output files for previous workday. Missing source files: {missing_files}")
            else:
                logger.debug(f"All {group_name} output files from previous workday already exist.")
    
    except Exception as e:
        logger.error(f"Error checking previous workday files: {str(e)}")
        print(f"Error checking previous workday files: {str(e)}")

def check_files_for_merge():
    """Check if all necessary files are available for merging"""
    try:
        global processed_files_today
        
        # Get all files in the destination directory (where copied files are stored)
        if not os.path.exists(DESTINATION_FOLDER):
            logger.warning(f"Destination folder does not exist: {DESTINATION_FOLDER}")
            return
            
        files = os.listdir(DESTINATION_FOLDER)
        
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
                file_path = os.path.join(DESTINATION_FOLDER, group_data["files"][file_idx])
                if not os.path.exists(file_path):
                    merge_files_exist = False
                    break
            
            # Create a unique key for this merge group operation
            merge_key = f"{group_name}_merge_{current_date}"
            
            if merge_files_exist and merge_key not in processed_files_today:
                logger.info(f"All {group_name} merge files found. Starting merge process.")
                
                # Mark this merge operation as processed
                processed_files_today.add(merge_key)
                
                # Perform merges for this group
                for file1_idx, file2_idx, output_idx in group_data["merge_pairs"]:
                    file1_path = os.path.join(DESTINATION_FOLDER, group_data["files"][file1_idx])
                    file2_path = os.path.join(DESTINATION_FOLDER, group_data["files"][file2_idx])
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
                merge_missing_files = [group_data["files"][idx] for idx in merge_files_needed if not os.path.exists(os.path.join(DESTINATION_FOLDER, group_data["files"][idx]))]
                # Only log missing files once per day
                missing_key = f"{group_name}_missing_{current_date}"
                if missing_key not in processed_files_today:
                    logger.debug(f"Not all {group_name} merge files found. Missing: {merge_missing_files}")
                    processed_files_today.add(missing_key)
            
            # Process single file conversions independently
            if "single_files" in group_data:
                for source_idx, output_idx in group_data["single_files"]:
                    source_path = os.path.join(DESTINATION_FOLDER, group_data["files"][source_idx])
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

# ============================================================================
# MAIN UNIFIED MONITORING CLASS AND FUNCTIONS
# ============================================================================

class UnifiedFileHandler(FileSystemEventHandler):
    def __init__(self, source_path, destination_path, max_age_days=100):
        self.source_path = source_path
        self.destination_path = destination_path
        self.max_age_days = max_age_days

    def on_created(self, event):
        if event.is_directory:
            return
        file_queue.put((event.src_path, datetime.now()))

    def on_modified(self, event):
        if event.is_directory:
            return
        file_queue.put((event.src_path, datetime.now()))

def process_queue(destination_path, max_age_days):
    """Process files from the queue with rate limiting"""
    # REMOVED: CSV file tracking and batch processing from queue
    
    while True:
        try:
            src_path, timestamp = file_queue.get(timeout=1)
            if datetime.now() - timestamp > timedelta(minutes=5):
                continue  # Skip if file is too old
                
            file_name = os.path.basename(src_path)
            dest_path = os.path.join(destination_path, file_name)
            
            file_date = extract_date_from_filename(file_name)
            if file_date and (datetime.now() - file_date).days <= max_age_days:
                copy_file(src_path, dest_path, file_name)
                
            time.sleep(NETWORK_DELAY)  # Rate limiting
        except queue.Empty:
            time.sleep(0.1)
        except Exception as e:
            logger.error(f"Error processing queued file: {str(e)}")

def initialize_output_folders():
    """Create output folders if they don't exist"""
    output_folders = [MANUAL_OUTPUT_FOLDER, EOD_OUTPUT_FOLDER, SOD_OUTPUT_FOLDER]
    
    for folder in output_folders:
        if not os.path.exists(folder):
            try:
                os.makedirs(folder)
                logger.info(f"Created output folder: {folder}")
                print(f"Created output folder: {folder}")
            except Exception as e:
                logger.error(f"Error creating output folder {folder}: {str(e)}")
                print(f"Error creating output folder {folder}: {str(e)}")

def monitor_unified():
    """Main unified monitoring function"""
    # Create destination folder if it doesn't exist
    if not os.path.exists(DESTINATION_FOLDER):
        os.makedirs(DESTINATION_FOLDER)
        logger.info(f"Created destination folder: {DESTINATION_FOLDER}")

    # Create CSV output folders
    initialize_output_folders()

    # Initialize holidays list for CSV merger
    add_holiday_list()

    # Define archive folder path
    archive_folder = os.path.join(DESTINATION_FOLDER, "Archive")

    print("=" * 80)
    print("UNIFIED FILE MONITOR AND CSV MERGER - OPTIMIZED VERSION")
    print("=" * 80)
    print("Starting unified monitoring system...")
    logger.info("Starting unified monitoring system...")
    
    # Check destination folder for old files and move them to archive
    print("Checking for old files to move to archive...")
    check_and_move_old_files(DESTINATION_FOLDER, archive_folder)
    
    # Start queue processing thread
    queue_thread = threading.Thread(
        target=process_queue,
        args=(DESTINATION_FOLDER, 100),  # max_age_days = 100
        daemon=True
    )
    queue_thread.start()
    
    # Process existing files first (now using bulk copy for speed)
    process_existing_files_bulk(SOURCE_FOLDER, DESTINATION_FOLDER, 100)
    
    # Set up file system monitoring
    event_handler = UnifiedFileHandler(SOURCE_FOLDER, DESTINATION_FOLDER, 100)
    observer = Observer()
    observer.schedule(event_handler, SOURCE_FOLDER, recursive=False)
    observer.start()

    print(f"\nFile Monitoring:")
    print(f"  Source: {SOURCE_FOLDER}")
    print(f"  Destination: {DESTINATION_FOLDER}")
    print(f"  Archive: {archive_folder}")
    print(f"  Max age: 100 days")
    print(f"\nCSV Processing:")
    print(f"  Manual outputs: {MANUAL_OUTPUT_FOLDER}")
    print(f"  EOD outputs: {EOD_OUTPUT_FOLDER}")
    print(f"  SOD outputs: {SOD_OUTPUT_FOLDER}")
    print(f"  Check interval: Every 2 minutes")
    print(f"\nBoth systems are running... (Press Ctrl+C to stop)")
    print("=" * 80)

    try:
        while True:
            # Periodically check and move old files (every 2 minutes)
            check_and_move_old_files(DESTINATION_FOLDER, archive_folder)
            
            # Periodically check for CSV files to merge (every 2 minutes)
            logger.info("Running periodic CSV merger check...")
            check_files_for_merge()
            check_previous_workday_files()
            
            time.sleep(120)  # Check every 2 minutes instead of 30 minutes
    except KeyboardInterrupt:
        observer.stop()
        print("\n" + "=" * 80)
        print("SHUTDOWN")
        print("=" * 80)
        print("Monitoring stopped")
        logger.info("Unified monitoring stopped")
    
    observer.join()

if __name__ == "__main__":
    monitor_unified()