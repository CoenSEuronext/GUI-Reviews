import time
import os
import shutil
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from datetime import datetime, timedelta
import logging
import re
from concurrent.futures import ThreadPoolExecutor
import queue
import socket
import threading

# Get the directory where the script is located
script_dir = os.path.dirname(os.path.abspath(__file__))
log_file = os.path.join(script_dir, 'folder_monitor.log')

# Set up logging
logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Network timeout settings
socket.setdefaulttimeout(30)  # 30 seconds timeout for network operations

# Queue for rate limiting
file_queue = queue.Queue()
MAX_QUEUE_SIZE = 100
NETWORK_DELAY = 0.1  # 100ms delay between operations

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

def is_within_date_range(file_date, max_age_days):
    if not file_date:
        return False
    return datetime.now() - file_date <= timedelta(days=max_age_days)

def retry_operation(operation, max_retries=3, delay=5):
    """Retry an operation with exponential backoff"""
    for attempt in range(max_retries):
        try:
            return operation()
        except (OSError, IOError) as e:
            if attempt == max_retries - 1:
                raise
            wait_time = delay * (2 ** attempt)
            logging.warning(f"Operation failed, retrying in {wait_time} seconds...")
            time.sleep(wait_time)

def safe_file_operation(operation, file_path, error_msg):
    """Safely perform a file operation with retries"""
    try:
        return retry_operation(lambda: operation(file_path))
    except Exception as e:
        logging.error(f"{error_msg}: {str(e)}")
        return None

def should_copy_file(src_path, dest_path):
    """Check if file should be copied with network-safe operations"""
    try:
        # If destination doesn't exist, definitely copy
        if not os.path.exists(dest_path):
            logging.info(f"File {os.path.basename(src_path)} doesn't exist in destination - will copy")
            return True
        
        # Get file sizes
        src_size = safe_file_operation(os.path.getsize, src_path, "Error getting source file size")
        dest_size = safe_file_operation(os.path.getsize, dest_path, "Error getting destination file size")
        
        if src_size != dest_size:
            logging.info(f"File {os.path.basename(src_path)} has different size - will copy")
            return True
        
        # Compare modification times
        src_mtime = safe_file_operation(os.path.getmtime, src_path, "Error getting source modification time")
        dest_mtime = safe_file_operation(os.path.getmtime, dest_path, "Error getting destination modification time")
        
        if src_mtime and dest_mtime and src_mtime > dest_mtime:
            logging.info(f"File {os.path.basename(src_path)} is newer - will copy")
            return True
            
        return False
    except Exception as e:
        logging.error(f"Error comparing files: {str(e)}")
        return False

def copy_file(src_path, dest_path, filename):
    """Copy file with network safety measures"""
    try:
        if should_copy_file(src_path, dest_path):
            def do_copy():
                # Remove destination file if it exists to ensure clean copy
                if os.path.exists(dest_path):
                    os.remove(dest_path)
                shutil.copy2(src_path, dest_path)
                time.sleep(NETWORK_DELAY)  # Rate limiting
            
            retry_operation(do_copy)
            logging.info(f"Copied {filename} to destination")
            print(f"Copied {filename} to destination")
        else:
            logging.debug(f"Skipped {filename} - already up to date")
    except Exception as e:
        logging.error(f"Error copying {filename}: {str(e)}")
        print(f"Error copying {filename}: {str(e)}")

def process_file_batch(files, source_path, destination_path, max_age_days):
    """Process a batch of files"""
    for filename in files:
        src_path = os.path.join(source_path, filename)
        if not os.path.isfile(src_path):
            continue
            
        file_date = extract_date_from_filename(filename)
        if file_date and is_within_date_range(file_date, max_age_days):
            dest_path = os.path.join(destination_path, filename)
            copy_file(src_path, dest_path, filename)

def process_existing_files(source_path, destination_path, max_age_days):
    """Process existing files with batching and rate limiting"""
    print("Scanning for files...")
    
    try:
        # Get list of files in batches
        all_files = []
        batch_size = 10000
        with os.scandir(source_path) as scanner:
            batch = []
            for entry in scanner:
                if entry.is_file():
                    batch.append(entry.name)
                    if len(batch) >= batch_size:
                        all_files.extend(batch)
                        print(f"Scanned {len(all_files)} files...")
                        batch = []
            if batch:
                all_files.extend(batch)
        
        total_files = len(all_files)
        print(f"Found {total_files} files to process")
        
        # Process files in batches using thread pool
        with ThreadPoolExecutor(max_workers=4) as executor:
            for i in range(0, total_files, batch_size):
                batch = all_files[i:i + batch_size]
                executor.submit(process_file_batch, batch, source_path, destination_path, max_age_days)
                print(f"Processing files {i + 1} to {min(i + batch_size, total_files)} of {total_files}")
                time.sleep(NETWORK_DELAY)  # Rate limiting between batches
                
    except Exception as e:
        logging.error(f"Error during initial scan: {str(e)}")
        print(f"Error during initial scan: {str(e)}")

class FileHandler(FileSystemEventHandler):
    def __init__(self, source_path, destination_path, max_age_days=30):
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
        # Also queue modified files for processing
        file_queue.put((event.src_path, datetime.now()))

def process_queue(destination_path, max_age_days):
    """Process files from the queue with rate limiting"""
    while True:
        try:
            src_path, timestamp = file_queue.get(timeout=1)
            if datetime.now() - timestamp > timedelta(minutes=5):
                continue  # Skip if file is too old
                
            file_name = os.path.basename(src_path)
            dest_path = os.path.join(destination_path, file_name)
            
            file_date = extract_date_from_filename(file_name)
            if file_date and is_within_date_range(file_date, max_age_days):
                copy_file(src_path, dest_path, file_name)
                
            time.sleep(NETWORK_DELAY)  # Rate limiting
        except queue.Empty:
            time.sleep(0.1)
        except Exception as e:
            logging.error(f"Error processing queued file: {str(e)}")

def cleanup_old_files(folder_path, max_age_days):
    """Remove files older than max_age_days based on filename date"""
    files_removed = 0

    for filename in os.listdir(folder_path):
        file_path = os.path.join(folder_path, filename)
        if os.path.isfile(file_path):
            file_date = extract_date_from_filename(filename)
            if file_date and not is_within_date_range(file_date, max_age_days):
                try:
                    os.remove(file_path)
                    files_removed += 1
                    logging.info(f"Removed old file: {filename}")
                    print(f"Removed old file: {filename}")
                except Exception as e:
                    logging.error(f"Error removing {filename}: {str(e)}")
                    print(f"Error removing {filename}: {str(e)}")
    
    return files_removed

def monitor_folder(source_path, destination_path, max_age_days=30):
    if not os.path.exists(destination_path):
        os.makedirs(destination_path)

    print("Starting folder monitor...")
    logging.info("Starting folder monitor...")
    
    # Start queue processor thread with proper parameters
    queue_thread = threading.Thread(
        target=process_queue,
        args=(destination_path, max_age_days),
        daemon=True
    )
    queue_thread.start()
    
    # Process existing files
    process_existing_files(source_path, destination_path, max_age_days)
    
    # Initialize event handler and observer
    event_handler = FileHandler(source_path, destination_path, max_age_days)
    observer = Observer()
    observer.schedule(event_handler, source_path, recursive=False)
    observer.start()

    print(f"\nMonitoring folder: {source_path}")
    print(f"Files will be copied to: {destination_path}")
    print(f"Only files with dates newer than {max_age_days} days will be processed")
    print("Monitor is running... (Press Ctrl+C to stop)")

    try:
        while True:
            cleanup_old_files(destination_path, max_age_days)
            time.sleep(3600)  # Check every hour
    except KeyboardInterrupt:
        observer.stop()
        print("\nMonitoring stopped")
        logging.info("Monitoring stopped")
        
    observer.join()

if __name__ == "__main__":
    SOURCE_FOLDER = r"\\pbgfshqa08601v\gis_ttm\Archive"
    DESTINATION_FOLDER = r"V:\PM-Indices-IndexOperations\General\Daily downloadfiles\Monthly Archive"
    MAX_AGE_DAYS = 30
    
    try:
        monitor_folder(SOURCE_FOLDER, DESTINATION_FOLDER, MAX_AGE_DAYS)
    except KeyboardInterrupt:
        print("\nScript interrupted by user. Exiting...")
    except Exception as e:
        print(f"Error: {str(e)}")