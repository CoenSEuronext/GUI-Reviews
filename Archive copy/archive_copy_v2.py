import time
import os
import shutil
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from datetime import datetime, timedelta
import logging
import logging.handlers
import re
from concurrent.futures import ThreadPoolExecutor
import queue
import socket
import threading

# Get the directory where the script is located
script_dir = os.path.dirname(os.path.abspath(__file__))
log_file = os.path.join(script_dir, 'folder_monitor.log')

try:
    handler = logging.handlers.RotatingFileHandler(
        log_file,
        maxBytes=10*1024*1024,  # 10MB
        backupCount=5,
        encoding='utf-8'
    )
    formatter = logging.Formatter('%(asctime)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    handler.setFormatter(formatter)
    
    logger = logging.getLogger('FolderMonitor')
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
            logging.info(f"Found {len(missing_files)} missing files:")
            for file in missing_files[:10]:  # Log first 10 missing files
                logging.info(f"Missing file: {file}")
                print(f"Missing file: {file}")
            if len(missing_files) > 10:
                logging.info(f"... and {len(missing_files) - 10} more")
                print(f"... and {len(missing_files) - 10} more")
        
        return missing_files
    except Exception as e:
        logging.error(f"Error comparing folders: {str(e)}")
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
    """Copy file with network safety measures and set as read-only"""
    try:
        if should_copy_file(src_path, dest_path):
            def do_copy():
                logging.info(f"Starting copy of {filename}")
                # Remove destination file if it exists to ensure clean copy
                if os.path.exists(dest_path):
                    try:
                        # Remove read-only attribute if it exists
                        os.chmod(dest_path, 0o777)  # Give all permissions temporarily
                        os.remove(dest_path)
                    except Exception as e:
                        logging.error(f"Error removing existing file {filename}: {str(e)}")
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
                
                logging.info(f"Successfully copied {filename}")
                time.sleep(NETWORK_DELAY)  # Rate limiting
            
            retry_operation(do_copy)
            print(f"Copied {filename} to destination and set as read-only")
        else:
            logging.debug(f"Skipped {filename} - already up to date")
    except Exception as e:
        logging.error(f"Error copying {filename}: {str(e)}")
        print(f"Error copying {filename}: {str(e)}")

def move_file_to_archive(src_path, archive_path, filename):
    """Move file to archive folder with safety measures"""
    try:
        def do_move():
            logging.info(f"Starting move of {filename} to archive")
            
            # Remove read-only attribute if it exists
            if os.path.exists(src_path):
                try:
                    os.chmod(src_path, 0o777)  # Give all permissions temporarily
                except Exception as e:
                    logging.warning(f"Could not change permissions for {filename}: {str(e)}")
            
            # Move the file
            shutil.move(src_path, archive_path)
            
            # Verify the move
            if not os.path.exists(archive_path):
                raise Exception("File not moved successfully")
            
            # Set archive file as read-only
            os.chmod(archive_path, 0o444)  # Read-only for all users
            
            logging.info(f"Successfully moved {filename} to archive")
            time.sleep(NETWORK_DELAY)  # Rate limiting
        
        retry_operation(do_move)
        print(f"Moved {filename} to archive folder")
        return True
    except Exception as e:
        logging.error(f"Error moving {filename} to archive: {str(e)}")
        print(f"Error moving {filename} to archive: {str(e)}")
        return False

def process_file_batch(files, source_path, destination_path):
    """Process a batch of files"""
    for filename in files:
        src_path = os.path.join(source_path, filename)
        if not os.path.isfile(src_path):
            continue
            
        file_date = extract_date_from_filename(filename)
        if file_date:
            dest_path = os.path.join(destination_path, filename)
            # Only copy if file is within 100 days
            if (datetime.now() - file_date).days <= 100:
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
        logging.info(f"Found {total_files} files to process")
        
        # Process files in batches using thread pool
        with ThreadPoolExecutor(max_workers=4) as executor:
            for i in range(0, total_files, batch_size):
                batch = all_files[i:i + batch_size]
                executor.submit(process_file_batch, batch, source_path, destination_path)
                print(f"Processing files {i + 1} to {min(i + batch_size, total_files)} of {total_files}")
                time.sleep(NETWORK_DELAY)  # Rate limiting between batches
        
        # After processing, verify and report missing files
        print("\nVerifying file copy completion...")
        missing_files = compare_folders(source_path, destination_path)
        if missing_files:
            print(f"\nWARNING: {len(missing_files)} files were not copied successfully.")
            # Try to copy missing files again
            print("Attempting to copy missing files...")
            process_file_batch(missing_files, source_path, destination_path)
                
    except Exception as e:
        logging.error(f"Error during initial scan: {str(e)}")
        print(f"Error during initial scan: {str(e)}")

class FileHandler(FileSystemEventHandler):
    def __init__(self, source_path, destination_path, max_age_days=100):  # Added back with default 100
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
            logging.error(f"Error processing queued file: {str(e)}")

def check_and_move_old_files(destination_path, archive_folder):
    """Check destination folder for files older than 120 days and move them to archive"""
    try:
        # Ensure archive folder exists
        if not os.path.exists(archive_folder):
            os.makedirs(archive_folder)
            logging.info(f"Created archive folder: {archive_folder}")
        
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
                            logging.info(f"File {filename} already exists in archive, skipping")
                            continue
                        
                        print(f"Moving old file to archive: {filename} ({days_old} days old)")
                        logging.info(f"Moving old file to archive: {filename} - {days_old} days old")
                        
                        if move_file_to_archive(file_path, archive_path, filename):
                            moved_count += 1
        
        if moved_count > 0:
            print(f"Moved {moved_count} old files to archive folder")
            logging.info(f"Moved {moved_count} old files to archive folder")
        
    except Exception as e:
        logging.error(f"Error checking and moving old files: {str(e)}")
        print(f"Error checking and moving old files: {str(e)}")

def monitor_folder(source_path, destination_path, max_age_days=100):
    if not os.path.exists(destination_path):
        os.makedirs(destination_path)

    # Define archive folder path
    archive_folder = os.path.join(destination_path, "Archive")

    print("Starting folder monitor...")
    logging.info("Starting folder monitor...")
    
    # Check destination folder for old files and move them to archive
    print("Checking for old files to move to archive...")
    check_and_move_old_files(destination_path, archive_folder)
    
    # Rest of the monitor_folder function remains the same
    queue_thread = threading.Thread(
        target=process_queue,
        args=(destination_path, max_age_days),
        daemon=True
    )
    queue_thread.start()
    
    process_existing_files(source_path, destination_path, max_age_days)
    
    event_handler = FileHandler(source_path, destination_path, max_age_days)
    observer = Observer()
    observer.schedule(event_handler, source_path, recursive=False)
    observer.start()

    print(f"\nMonitoring folder: {source_path}")
    print(f"Files will be copied to: {destination_path}")
    print(f"Old files (120+ days) will be moved to: {archive_folder}")
    print(f"Files up to {max_age_days} days old will be copied")
    print("Monitor is running... (Press Ctrl+C to stop)")

    try:
        while True:
            check_and_move_old_files(destination_path, archive_folder)  # Periodically check and move old files
            time.sleep(3600)  # Check every hour
    except KeyboardInterrupt:
        observer.stop()
        print("\nMonitoring stopped")
        logging.info("Monitoring stopped")
    
    observer.join()

if __name__ == "__main__":
    SOURCE_FOLDER = r"\\pbgfshqa08601v\gis_ttm\Archive"
    DESTINATION_FOLDER = r"V:\PM-Indices-IndexOperations\General\Daily downloadfiles\Monthly Archive"
    
    monitor_folder(SOURCE_FOLDER, DESTINATION_FOLDER)