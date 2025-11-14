import pandas as pd
import os
import logging
import logging.handlers
import stat
from datetime import datetime, timedelta
import numpy as np
import time
import functools
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import threading
from pathlib import Path

# Holidays list
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

# Comparison configurations
COMPARISON_CONFIGS = {
    'morning_stock': {
        'file_suffix': 'STOCK',
        'key_fields': ['#Symbol', 'Index'],
        'output_filename': 'GIS Morning Stock changes_{date}.xlsx',
        'comparison_function': 'find_differences_vectorized_morning_stock',
        'use_previous_workday': True,
        'manual_folder': 'manual',
        'allow_overwrite': False,
        'file_extension': 'xlsx'
    },
    'morning_index': {
        'file_suffix': 'INDEX',
        'key_fields': ['#Symbol'],
        'output_filename': 'GIS Morning Index changes_{date}.xlsx',
        'comparison_function': 'find_differences_vectorized_morning_index',
        'use_previous_workday': True,
        'manual_folder': 'manual',
        'allow_overwrite': False,
        'file_extension': 'xlsx'
    },
    'afternoon_stock': {
        'file_suffix': 'STOCK',
        'key_fields': ['#Symbol', 'Index'],
        'output_filename': 'GIS Afternoon Stock changes_{date}.xlsx',
        'comparison_function': 'find_differences_vectorized_morning_stock',
        'use_previous_workday': False,
        'manual_folder': 'afternoon_manual',
        'allow_overwrite': True,
        'overwrite_until': '18:10',
        'file_extension': 'xlsx'
    },
    'afternoon_index': {
        'file_suffix': 'INDEX',
        'key_fields': ['#Symbol'],
        'output_filename': 'GIS Afternoon Index changes_{date}.xlsx',
        'comparison_function': 'find_differences_vectorized_morning_index',
        'use_previous_workday': False,
        'manual_folder': 'afternoon_manual',
        'allow_overwrite': True,
        'overwrite_until': '18:10',
        'file_extension': 'xlsx'
    },
    'evening_stock': {
        'file_suffix': 'STOCK',
        'key_fields': ['#Symbol', 'Index'],
        'output_filename': 'GIS Evening Stock changes_{date}.xlsx',
        'comparison_function': 'find_differences_vectorized_morning_stock',
        'use_previous_workday': False,
        'eod_folder': 'eod',
        'manual_folder': 'eod_manual',
        'allow_overwrite': True,
        'overwrite_until': '23:00',
        'file_extension': 'csv',
        'file1_prefix': 'EOD',
        'file2_prefix': 'MANUAL'
    },
    'evening_index': {
        'file_suffix': 'INDEX',
        'key_fields': ['#Symbol'],
        'output_filename': 'GIS Evening Index changes_{date}.xlsx',
        'comparison_function': 'find_differences_vectorized_morning_index',
        'use_previous_workday': False,
        'eod_folder': 'eod',
        'manual_folder': 'eod_manual',
        'allow_overwrite': True,
        'overwrite_until': '23:00',
        'file_extension': 'csv',
        'file1_prefix': 'EOD',
        'file2_prefix': 'MANUAL'
    }
}

# Allowed mnemonics for index comparison (Mnemo values that should NOT be highlighted blue)
ALLOWED_MNEMONICS = {
    'BERE', 'BELAS', 'N100', 'N150', 'FRRE', 'ISEIN', 'ISETE', 'ISEBM', 'ISCG', 'ISECS', 
    'ISEHC', 'ISUT', 'ISRE', 'ISEFI', 'ISEQ', 'ECETH', 'ECSOL', 'ECADA', 'ECXRP', 'ECDOT', 
    'ECMAT', 'ECAVA', 'AAX', 'REPOT', 'NLRE', 'NLUT', 'NOKFW', 'OSEAP', 'OAAXP', 'OTECP', 
    'OTELP', 'OHCP', 'OFINP', 'OREP', 'OCDP', 'OCSP', 'OINP', 'OBMP', 'OENP', 'OUTP', 
    'SSENP', 'SSSFP', 'SSSHP', 'PAX', 'NLOG', 'NLBM', 'NLIN', 'NLCG', 'NLHC', 'NLCS', 
    'NLTEL', 'NLFIN', 'NLTEC', 'FROG', 'FRBM', 'FRIN', 'FRCG', 'FRHC', 'FRCS', 'FRTEL', 
    'FRUT', 'FRFIN', 'FRTEC', 'ALASI', 'BIOTK', 'NAOII', 'BVL', 'BEBMP', 'PTBMP', 'PTINP', 
    'PTCGP', 'PTCSP', 'PTTLP', 'PTUTP', 'PTFIP', 'PTTEP', 'BECSP', 'BEUTP', 'BETP', 'BEFIP', 
    'BETEP', 'BEHCP', 'BEINP', 'BECGP', 'BELCP', 'BEOGP', 'PTHCP', 'PTOGP'
}

# Purple mnemonics for special highlighting
PURPLE_MNEMONICS = {'AEX BANK', 'PX1'}

# Purple index mnemonics (for Name column)
PURPLE_INDEX_MNEMONICS = {
    'AEX', 'AMX', 'BEL20', 'CESGP', 'AEXDI', 'C4CD', 'EZBDI', 
    'C4SD', 'CACDI', 'PX1', 'BANK', 'ENESG', 'ISE20', 'OBXP', 'PSI20'
}

# Excluded index values for Isin Code cross-reference check
EXCLUDED_INDEX_VALUES = {
    'DUUSC', 'DUMEU', 'DUMUS', 'PFAEX', 'PFEES', 'PFPX1', 'PFOSF', 'PFOSB', 
    'PFCSB', 'PFMES', 'PFC4E', 'PFLCE', 'PFLC1', 'PFEBL', 'PFBEL', 'PFFRI', 
    'PFFRD', 'BSWPF', 'PFLCW'
}

# Timer decorator for performance monitoring
def timer(func):
    """Decorator to time function execution"""
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        duration = end_time - start_time
        print(f"  {func.__name__} took {duration:.2f} seconds")
        logger.info(f"Function {func.__name__} took {duration:.2f} seconds")
        return result
    return wrapper

# Get the directory where the script is located
script_dir = os.path.dirname(os.path.abspath(__file__))
log_file = os.path.join(script_dir, 'file_monitor.log')

# Set up logging
try:
    handler = logging.handlers.RotatingFileHandler(
        log_file,
        maxBytes=10*1024*1024,
        backupCount=5,
        encoding='utf-8'
    )
    formatter = logging.Formatter('%(asctime)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    handler.setFormatter(formatter)
    
    logger = logging.getLogger('FileMonitor')
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)
except Exception as e:
    print(f"Warning: Could not set up logging to file: {str(e)}")
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

# Configuration
MONITOR_FOLDERS = {
    'manual': r"V:\PM-Indices-IndexOperations\General\Daily downloadfiles\Monthly Archive\Merged files\Manual",
    'sod': r"V:\PM-Indices-IndexOperations\General\Daily downloadfiles\Monthly Archive\Merged files\SOD",
    'afternoon_manual': r"V:\PM-Indices-IndexOperations\General\Daily downloadfiles\Monthly Archive\Merged files\Afternoon + Evening Manuals",
    'eod': r"V:\PM-Indices-IndexOperations\General\Daily downloadfiles\Monthly Archive",
    'eod_manual': r"V:\PM-Indices-IndexOperations\General\Daily downloadfiles\Monthly Archive"
}

OUTPUT_DIR = r"C:\Users\CSonneveld\OneDrive - Euronext\Documents\Projects\Archive copy\destination\Check files output"

if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)
    logger.info(f"Created output directory: {OUTPUT_DIR}")

def get_previous_workday(date=None):
    """Get the previous workday (excluding weekends and holidays)"""
    if date is None:
        date = datetime.now()
    
    prev_day = date - timedelta(days=1)
    
    while True:
        if prev_day.weekday() >= 5:
            prev_day = prev_day - timedelta(days=1)
            continue
        
        date_str = prev_day.strftime("%Y%m%d")
        if date_str in HOLIDAYS:
            prev_day = prev_day - timedelta(days=1)
            continue
        
        break
    
    return prev_day

def get_current_workday(date=None):
    """Get the current workday (excluding weekends and holidays)"""
    if date is None:
        date = datetime.now()
    
    while True:
        if date.weekday() >= 5:
            date = date - timedelta(days=1)
            continue
        
        date_str = date.strftime("%Y%m%d")
        if date_str in HOLIDAYS:
            date = date - timedelta(days=1)
            continue
        
        break
    
    return date

def should_allow_overwrite(comparison_type):
    """Check if overwrite is allowed based on time and comparison config"""
    config = COMPARISON_CONFIGS.get(comparison_type)
    if not config or not config.get('allow_overwrite'):
        return False
    
    overwrite_until = config.get('overwrite_until')
    if not overwrite_until:
        return True
    
    # Parse overwrite_until time (format: "HH:MM")
    try:
        cutoff_hour, cutoff_minute = map(int, overwrite_until.split(':'))
        cutoff_time = datetime.now().replace(hour=cutoff_hour, minute=cutoff_minute, second=0, microsecond=0)
        
        return datetime.now() < cutoff_time
    except:
        return True
    
def should_regenerate_output(input_files, output_path):
    """Check if output file should be regenerated based on input file timestamps
    
    Args:
        input_files: List of input file paths
        output_path: Path to the output file
        
    Returns:
        bool: True if any input file is newer than output file, False otherwise
    """
    # If output doesn't exist, always regenerate
    if not os.path.exists(output_path):
        return True
    
    try:
        output_mtime = os.path.getmtime(output_path)
        
        # Check if any input file is newer than output
        for input_file in input_files:
            if os.path.exists(input_file):
                input_mtime = os.path.getmtime(input_file)
                if input_mtime > output_mtime:
                    logger.info(f"Input file {os.path.basename(input_file)} is newer than output file")
                    return True
        
        logger.info(f"Output file is up to date, no regeneration needed")
        return False
        
    except Exception as e:
        logger.warning(f"Error checking file timestamps: {str(e)}")
        # On error, default to regenerating to be safe
        return True

def get_expected_filenames(comparison_type='morning_stock'):
    """Get the expected filenames for today's comparison"""
    current_workday = get_current_workday()
    previous_workday = get_previous_workday()
    
    config = COMPARISON_CONFIGS.get(comparison_type, COMPARISON_CONFIGS['morning_stock'])
    file_suffix = config['file_suffix']
    use_previous_workday = config.get('use_previous_workday', True)
    file_extension = config.get('file_extension', 'xlsx')
    
    # For evening comparisons
    if comparison_type.startswith('evening_'):
        file1_prefix = config.get('file1_prefix', 'EOD')
        file2_prefix = config.get('file2_prefix', 'MANUAL')
        date_str = current_workday.strftime("%Y%m%d")
        
        expected_files = {
            'file1': f"TTMIndexEU1_GIS_{file1_prefix}_{file_suffix}_{date_str}.{file_extension}",
            'file2': f"TTMIndexEU1_GIS_{file2_prefix}_{file_suffix}_{date_str}.{file_extension}"
        }
        
        logger.info(f"Expected {file_suffix} files: {file1_prefix}={expected_files['file1']}, {file2_prefix}={expected_files['file2']}")
        
        return expected_files
    
    # For morning and afternoon comparisons
    sod_date = current_workday.strftime("%Y%m%d")
    
    # For afternoon comparisons, manual file also uses current date
    if use_previous_workday:
        manual_date = previous_workday.strftime("%Y%m%d")
    else:
        manual_date = current_workday.strftime("%Y%m%d")
    
    expected_files = {
        'sod': f"TTMIndexEU1_GIS_SOD_{file_suffix}_{sod_date}.{file_extension}",
        'manual': f"TTMIndexEU1_GIS_MANUAL_{file_suffix}_{manual_date}.{file_extension}"
    }
    
    logger.info(f"Expected {file_suffix} files: SOD={expected_files['sod']}, Manual={expected_files['manual']}")
    
    return expected_files

def file_exists_and_ready(filepath):
    """Check if file exists and is ready (not being written to)
    
    Enhanced version with multiple stability checks and CSV validation
    """
    if not os.path.exists(filepath):
        return False
    
    try:
        # First check: Can we open and read the file?
        with open(filepath, 'rb') as f:
            f.read(1024)
        
        # Second check: Wait and verify file size is stable (not still being written)
        # For CSV files, check multiple times to ensure stability
        is_csv = filepath.lower().endswith('.csv')
        stability_checks = 3 if is_csv else 2
        
        for check_num in range(stability_checks):
            initial_size = os.path.getsize(filepath)
            initial_mtime = os.path.getmtime(filepath)
            
            time.sleep(2)  # Wait 2 seconds between checks
            
            final_size = os.path.getsize(filepath)
            final_mtime = os.path.getmtime(filepath)
            
            if initial_size != final_size or initial_mtime != final_mtime:
                logger.info(f"File {os.path.basename(filepath)} is still being written (check {check_num + 1}/{stability_checks})")
                return False
        
        # Third check: For CSV files, validate basic readability
        if is_csv:
            try:
                # Try to read with multiple encodings to verify file is readable
                encodings_to_test = ['utf-8', 'latin-1', 'cp1252']
                readable = False
                
                for encoding in encodings_to_test:
                    try:
                        with open(filepath, 'r', encoding=encoding, errors='strict') as f:
                            # Try to read first 50 lines to catch structural issues
                            line_count = 0
                            for line in f:
                                line_count += 1
                                if line_count >= 50:
                                    break
                        readable = True
                        break
                    except (UnicodeDecodeError, UnicodeError):
                        continue
                    except Exception as e:
                        logger.warning(f"CSV file {os.path.basename(filepath)} failed readability check with {encoding}: {str(e)}")
                        continue
                
                if not readable:
                    logger.warning(f"CSV file {os.path.basename(filepath)} could not be read with any standard encoding")
                    return False
                
                # Additional check: Try to parse as CSV to detect structural issues
                # Try multiple delimiters to find the right one
                try:
                    delimiters_to_test = [',', ';', '\t', '|']
                    valid_structure = False
                    max_columns_found = 0
                    
                    for delimiter in delimiters_to_test:
                        try:
                            test_df = pd.read_csv(filepath, nrows=5, encoding=encoding, delimiter=delimiter, engine='python', on_bad_lines='skip')
                            if test_df is not None and len(test_df.columns) > max_columns_found:
                                max_columns_found = len(test_df.columns)
                                if len(test_df.columns) > 1:
                                    valid_structure = True
                                    logger.info(f"CSV file {os.path.basename(filepath)} has valid structure with delimiter '{delimiter}' ({len(test_df.columns)} columns)")
                        except:
                            continue
                    
                    # If we found at least 2 columns with any delimiter, file is valid
                    if not valid_structure:
                        logger.warning(f"CSV file {os.path.basename(filepath)} appears to have structural issues (max {max_columns_found} columns found)")
                        return False
                except Exception as e:
                    logger.warning(f"CSV file {os.path.basename(filepath)} failed pandas parsing check: {str(e)}")
                    return False
                    
            except Exception as e:
                logger.warning(f"CSV file {os.path.basename(filepath)} failed validation: {str(e)}")
                return False
        
        logger.info(f"File {os.path.basename(filepath)} is ready (passed all stability checks)")
        return True
        
    except (IOError, OSError) as e:
        logger.warning(f"File {os.path.basename(filepath)} not ready: {str(e)}")
        return False

def check_files_available(comparison_type='morning_stock'):
    """Check if both expected files are available"""
    expected_files = get_expected_filenames(comparison_type)
    config = COMPARISON_CONFIGS.get(comparison_type, COMPARISON_CONFIGS['morning_stock'])
    
    # For evening comparisons
    if comparison_type.startswith('evening_'):
        eod_folder = config.get('eod_folder', 'eod')
        manual_folder = config.get('manual_folder', 'eod_manual')
        
        file1_path = os.path.join(MONITOR_FOLDERS[eod_folder], expected_files['file1'])
        file2_path = os.path.join(MONITOR_FOLDERS[manual_folder], expected_files['file2'])
        
        file1_ready = file_exists_and_ready(file1_path)
        file2_ready = file_exists_and_ready(file2_path)
        
        logger.info(f"{comparison_type.upper()} file status: EOD={file1_ready}, Manual={file2_ready}")
        
        if file1_ready and file2_ready:
            return {
                'available': True,
                'file1_path': file1_path,
                'file2_path': file2_path
            }
        else:
            return {
                'available': False,
                'file1_path': None,
                'file2_path': None
            }
    
    # For morning and afternoon comparisons
    manual_folder = config.get('manual_folder', 'manual')
    
    sod_path = os.path.join(MONITOR_FOLDERS['sod'], expected_files['sod'])
    manual_path = os.path.join(MONITOR_FOLDERS[manual_folder], expected_files['manual'])
    
    sod_ready = file_exists_and_ready(sod_path)
    manual_ready = file_exists_and_ready(manual_path)
    
    logger.info(f"{comparison_type.upper()} file status: SOD={sod_ready}, Manual={manual_ready}")
    
    if sod_ready and manual_ready:
        return {
            'available': True,
            'sod_path': sod_path,
            'manual_path': manual_path
        }
    else:
        return {
            'available': False,
            'sod_path': None,
            'manual_path': None
        }

def make_file_writable(file_path):
    """Temporarily make a read-only file writable for reading"""
    try:
        current_permissions = os.stat(file_path).st_mode
        os.chmod(file_path, current_permissions | stat.S_IREAD)
        return True
    except Exception as e:
        logger.warning(f"Could not modify permissions for {file_path}: {str(e)}")
        return False

@timer
def read_excel_file(file_path):
    """Read Excel or CSV file and return DataFrame - optimized for large files with robust error handling"""
    try:
        logger.info(f"Reading file: {os.path.basename(file_path)}")
        
        if not os.path.exists(file_path):
            logger.error(f"File not found: {file_path}")
            return None
        
        make_file_writable(file_path)
        
        # Determine file type by extension
        file_ext = os.path.splitext(file_path)[1].lower()
        
        try:
            if file_ext == '.csv':
                # Enhanced CSV reading with better error handling
                encodings_to_try = ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1', 'utf-16']
                delimiters_to_try = [',', ';', '\t', '|']
                
                df = None
                last_error = None
                successful_encoding = None
                successful_delimiter = None
                
                for encoding in encodings_to_try:
                    for delimiter in delimiters_to_try:
                        try:
                            # Use on_bad_lines='skip' to handle malformed lines
                            df = pd.read_csv(
                                file_path, 
                                encoding=encoding,
                                delimiter=delimiter,
                                engine='python',  # More flexible parser
                                on_bad_lines='skip',  # Skip problematic lines instead of failing
                                encoding_errors='replace'  # Replace invalid characters instead of failing
                            )
                            
                            # Check if we got reasonable data (has columns and rows)
                            if df is not None and len(df.columns) > 1 and len(df) > 0:
                                successful_encoding = encoding
                                successful_delimiter = delimiter
                                logger.info(f"Successfully read CSV with encoding={encoding}, delimiter='{delimiter}', {len(df)} rows, {len(df.columns)} columns")
                                break
                        except Exception as e:
                            last_error = e
                            continue
                    
                    if df is not None and len(df.columns) > 1:
                        break
                
                if df is None or len(df.columns) <= 1:
                    logger.error(f"Failed to read CSV file with all encoding/delimiter combinations.")
                    logger.error(f"Last error: {str(last_error)}")
                    logger.error(f"File may be corrupted, incomplete, or still being written.")
                    return None
            else:
                # Read Excel file
                df = pd.read_excel(file_path, engine='openpyxl')
            
            # Clean up string columns
            for col in df.columns:
                if df[col].dtype == 'object':
                    df[col] = df[col].astype(str).str.strip()
            
        except ImportError:
            print("Error: openpyxl is not installed. Please install it using:")
            print("pip install openpyxl")
            logger.error("openpyxl is not installed")
            return None
        except Exception as e:
            logger.error(f"Failed to read file: {str(e)}")
            logger.error(f"This could indicate the file is still being written or is corrupted")
            return None
        
        logger.info(f"Successfully read {len(df)} rows and {len(df.columns)} columns from {os.path.basename(file_path)}")
        
        return df
        
    except Exception as e:
        logger.error(f"Error reading file {file_path}: {str(e)}")
        return None

@timer
def prepare_dataframes(df1, df2, key_fields):
    """Prepare dataframes for fast comparison using indexing"""
    df1_clean = df1.replace([np.inf, -np.inf], '').fillna('')
    df2_clean = df2.replace([np.inf, -np.inf], '').fillna('')
    
    df1_clean['composite_key'] = df1_clean[key_fields].astype(str).agg('|'.join, axis=1)
    df2_clean['composite_key'] = df2_clean[key_fields].astype(str).agg('|'.join, axis=1)
    
    df1_indexed = df1_clean.set_index('composite_key')
    df2_indexed = df2_clean.set_index('composite_key')
    
    return df1_indexed, df2_indexed

@timer
def find_differences_vectorized_morning_stock(df1_indexed, df2_indexed, is_afternoon=False):
    """Find differences for morning stock comparison - vectorized operations
    
    Args:
        df1_indexed: First dataframe (Manual for morning, SOD for afternoon)
        df2_indexed: Second dataframe (SOD for morning, Manual for afternoon)
        is_afternoon: If True, also include removals (rows in df1/SOD but not in df2/Manual)
                      and additions (rows in df2/Manual but not in df1/SOD)
    """
    common_keys = df1_indexed.index.intersection(df2_indexed.index)
    
    if len(common_keys) == 0:
        diff_df = pd.DataFrame()
    else:
        df1_common = df1_indexed.loc[common_keys]
        df2_common = df2_indexed.loc[common_keys]
        
        critical_fields = ['ICBCode', 'Shares', 'Free float-Coeff', 'Capping Factor-Coeff']
        
        has_differences = pd.Series(False, index=common_keys)
        
        for field in critical_fields:
            if field in df1_common.columns and field in df2_common.columns:
                if field in ['Shares', 'Free float-Coeff', 'Capping Factor-Coeff']:
                    val1 = pd.to_numeric(df1_common[field], errors='coerce').fillna(0)
                    val2 = pd.to_numeric(df2_common[field], errors='coerce').fillna(0)
                    
                    tolerance = 1e-6 if field == 'Shares' else 1e-10
                    field_diff = abs(val1 - val2) > tolerance
                else:
                    field_diff = df1_common[field].astype(str) != df2_common[field].astype(str)
                
                has_differences |= field_diff
        
        # Also include rows with formatting conditions of interest (even without differences)
        has_formatting_interest = pd.Series(False, index=common_keys)
        
        # Check for BOTH Close Prc AND Adj Closing price empty or 0
        if 'Close Prc' in df2_common.columns and 'Adj Closing price' in df2_common.columns:
            close_prc_vals = pd.to_numeric(df2_common['Close Prc'], errors='coerce')
            adj_closing_vals = pd.to_numeric(df2_common['Adj Closing price'], errors='coerce')
            
            close_prc_empty = (close_prc_vals.isna() | (close_prc_vals == 0))
            adj_closing_empty = (adj_closing_vals.isna() | (adj_closing_vals == 0))
            
            # Both must be empty/0 together (AND condition)
            has_formatting_interest |= (close_prc_empty & adj_closing_empty)
            
            # For afternoon/evening: also include rows where Close Prc != Adj Closing price
            if is_afternoon:
                close_adj_different = (
                    (~close_prc_empty) & (~adj_closing_empty) & 
                    (abs(close_prc_vals - adj_closing_vals) > 1e-10)
                )
                has_formatting_interest |= close_adj_different
        
        # For afternoon/evening: include rows with positive dividends
        if is_afternoon:
            if 'Source net div' in df2_common.columns:
                net_div_vals = pd.to_numeric(df2_common['Source net div'], errors='coerce').fillna(0)
                has_formatting_interest |= (net_div_vals > 0)
            
            if 'Source gross div' in df2_common.columns:
                gross_div_vals = pd.to_numeric(df2_common['Source gross div'], errors='coerce').fillna(0)
                has_formatting_interest |= (gross_div_vals > 0)
        
        # Combine both conditions: actual differences OR formatting interest
        diff_keys = common_keys[has_differences | has_formatting_interest]
        
        if len(diff_keys) == 0:
            diff_df = pd.DataFrame()
        else:
            df1_diff = df1_indexed.loc[diff_keys]
            df2_diff = df2_indexed.loc[diff_keys]
            
            # Perform Isin Code cross-reference lookup
            # Get unique Isin Codes from the differences
            isin_codes = df1_diff.get('Isin Code', '').astype(str)
            
            # Prepare lookup column (column X) - for each Isin Code, find matching #Symbols in SOD
            cross_ref_symbols = []
            
            for isin_code in isin_codes:
                if pd.isna(isin_code) or str(isin_code).strip() == '':
                    cross_ref_symbols.append('')
                    continue
                
                # Filter df2_indexed for matching Isin Code and excluded Index values
                if 'Isin Code' in df2_indexed.columns and 'Index' in df2_indexed.columns:
                    # Find rows with matching Isin Code
                    matching_rows = df2_indexed[df2_indexed['Isin Code'].astype(str) == str(isin_code).strip()]
                    
                    # Filter out excluded Index values
                    if len(matching_rows) > 0:
                        filtered_rows = matching_rows[~matching_rows['Index'].astype(str).str.strip().isin(EXCLUDED_INDEX_VALUES)]
                        
                        # Get unique #Symbol values
                        if len(filtered_rows) > 0 and '#Symbol' in filtered_rows.columns:
                            unique_symbols = filtered_rows['#Symbol'].astype(str).str.strip().unique()
                            unique_symbols = [s for s in unique_symbols if s != '' and s != 'nan']
                            cross_ref_symbols.append(';'.join(sorted(unique_symbols)))
                        else:
                            cross_ref_symbols.append('')
                    else:
                        cross_ref_symbols.append('')
                else:
                    cross_ref_symbols.append('')
            
            # FIXED: Always use Source dividend columns for all comparisons
            differences_data = {
                'Rank': range(1, len(diff_keys) + 1),
                'Code': df1_diff['#Symbol'].astype(str) + df1_diff['Index'].astype(str),
                '#Symbol': df1_diff['#Symbol'],
                'Sys date': df1_diff.get('System date', ''),
                'Adj. Rsn': df2_diff.get('Adjust Reason', ''),
                'Isin Code': df1_diff.get('Isin Code', ''),
                'Country': df1_diff.get('Country', ''),
                'Mnemo': df1_diff.get('Mnemo', ''),
                'Name': df1_diff.get('Name', ''),
                'MIC': df1_diff.get('MIC', ''),
                'Prev ICB': df1_diff.get('ICBCode', ''),
                'New ICB': df2_diff.get('ICBCode', ''),
                'Close Prc': df2_diff.get('Close Prc', ''),
                'Adj Closing price': df2_diff.get('Adj Closing price', ''),
                'Net Div': df2_diff.get('Source net div', ''),
                'Gross Div': df2_diff.get('Source gross div', ''),
                'Index': df1_diff['Index'],
                'Prev. Shares': df1_diff.get('Shares', ''),
                'New Shares': df2_diff.get('Shares', ''),
                'Prev FF': df1_diff.get('Free float-Coeff', ''),
                'New FF': df2_diff.get('Free float-Coeff', ''),
                'Prev Capping': df1_diff.get('Capping Factor-Coeff', ''),
                'New Capping': df2_diff.get('Capping Factor-Coeff', ''),
                'Cross-Ref Symbols': cross_ref_symbols
            }
            
            diff_df = pd.DataFrame(differences_data)
    
    # For afternoon comparisons, also include removals and additions
    if is_afternoon:
        # First handle removals (rows in SOD but not in Manual)
        removed_keys = df1_indexed.index.difference(df2_indexed.index)
        
        if len(removed_keys) > 0:
            df1_removed = df1_indexed.loc[removed_keys]
            
            # Perform Isin Code cross-reference lookup for removed rows
            isin_codes_removed = df1_removed.get('Isin Code', '').astype(str)
            cross_ref_symbols_removed = []
            
            for isin_code in isin_codes_removed:
                if pd.isna(isin_code) or str(isin_code).strip() == '':
                    cross_ref_symbols_removed.append('')
                    continue
                
                # Look up in df2_indexed (Manual file)
                if 'Isin Code' in df2_indexed.columns and 'Index' in df2_indexed.columns:
                    matching_rows = df2_indexed[df2_indexed['Isin Code'].astype(str) == str(isin_code).strip()]
                    
                    if len(matching_rows) > 0:
                        filtered_rows = matching_rows[~matching_rows['Index'].astype(str).str.strip().isin(EXCLUDED_INDEX_VALUES)]
                        
                        if len(filtered_rows) > 0 and '#Symbol' in filtered_rows.columns:
                            unique_symbols = filtered_rows['#Symbol'].astype(str).str.strip().unique()
                            unique_symbols = [s for s in unique_symbols if s != '' and s != 'nan']
                            cross_ref_symbols_removed.append(';'.join(sorted(unique_symbols)))
                        else:
                            cross_ref_symbols_removed.append('')
                    else:
                        cross_ref_symbols_removed.append('')
                else:
                    cross_ref_symbols_removed.append('')
            
            # Create removal rows with SOD values and #N/A for Manual values
            removal_data = {
                'Rank': range(len(diff_df) + 1, len(diff_df) + len(removed_keys) + 1),
                'Code': df1_removed['#Symbol'].astype(str) + df1_removed['Index'].astype(str),
                '#Symbol': df1_removed['#Symbol'],
                'Sys date': df1_removed.get('System date', ''),
                'Adj. Rsn': 'Removal',  # Special value for removals
                'Isin Code': df1_removed.get('Isin Code', ''),
                'Country': df1_removed.get('Country', ''),
                'Mnemo': df1_removed.get('Mnemo', ''),
                'Name': df1_removed.get('Name', ''),
                'MIC': df1_removed.get('MIC', ''),
                'Prev ICB': df1_removed.get('ICBCode', ''),
                'New ICB': '#N/A',
                'Close Prc': '#N/A',
                'Adj Closing price': '#N/A',
                'Net Div': '#N/A',
                'Gross Div': '#N/A',
                'Index': df1_removed['Index'],
                'Prev. Shares': df1_removed.get('Shares', ''),
                'New Shares': '#N/A',
                'Prev FF': df1_removed.get('Free float-Coeff', ''),
                'New FF': '#N/A',
                'Prev Capping': df1_removed.get('Capping Factor-Coeff', ''),
                'New Capping': '#N/A',
                'Cross-Ref Symbols': cross_ref_symbols_removed
            }
            
            removal_df = pd.DataFrame(removal_data)
            
            # Combine differences and removals
            diff_df = pd.concat([diff_df, removal_df], ignore_index=True)
            # Re-rank
            diff_df['Rank'] = range(1, len(diff_df) + 1)
        
        # Now handle additions (rows in Manual but not in SOD)
        added_keys = df2_indexed.index.difference(df1_indexed.index)
        
        if len(added_keys) > 0:
            df2_added = df2_indexed.loc[added_keys]
            
            # Perform Isin Code cross-reference lookup for added rows
            isin_codes_added = df2_added.get('Isin Code', '').astype(str)
            cross_ref_symbols_added = []
            
            for isin_code in isin_codes_added:
                if pd.isna(isin_code) or str(isin_code).strip() == '':
                    cross_ref_symbols_added.append('')
                    continue
                
                # Look up in df2_indexed (Manual file)
                if 'Isin Code' in df2_indexed.columns and 'Index' in df2_indexed.columns:
                    matching_rows = df2_indexed[df2_indexed['Isin Code'].astype(str) == str(isin_code).strip()]
                    
                    if len(matching_rows) > 0:
                        filtered_rows = matching_rows[~matching_rows['Index'].astype(str).str.strip().isin(EXCLUDED_INDEX_VALUES)]
                        
                        if len(filtered_rows) > 0 and '#Symbol' in filtered_rows.columns:
                            unique_symbols = filtered_rows['#Symbol'].astype(str).str.strip().unique()
                            unique_symbols = [s for s in unique_symbols if s != '' and s != 'nan']
                            cross_ref_symbols_added.append(';'.join(sorted(unique_symbols)))
                        else:
                            cross_ref_symbols_added.append('')
                    else:
                        cross_ref_symbols_added.append('')
                else:
                    cross_ref_symbols_added.append('')
            
            # FIXED: Create addition rows with Source dividend columns
            addition_data = {
                'Rank': range(len(diff_df) + 1, len(diff_df) + len(added_keys) + 1),
                'Code': df2_added['#Symbol'].astype(str) + df2_added['Index'].astype(str),
                '#Symbol': df2_added['#Symbol'],
                'Sys date': df2_added.get('System date', ''),
                'Adj. Rsn': 'Add Composition',  # Special value for additions
                'Isin Code': df2_added.get('Isin Code', ''),
                'Country': df2_added.get('Country', ''),
                'Mnemo': df2_added.get('Mnemo', ''),
                'Name': df2_added.get('Name', ''),
                'MIC': df2_added.get('MIC', ''),
                'Prev ICB': '#N/A',
                'New ICB': df2_added.get('ICBCode', ''),
                'Close Prc': df2_added.get('Close Prc', ''),
                'Adj Closing price': df2_added.get('Adj Closing price', ''),
                'Net Div': df2_added.get('Source net div', ''),
                'Gross Div': df2_added.get('Source gross div', ''),
                'Index': df2_added['Index'],
                'Prev. Shares': '#N/A',
                'New Shares': df2_added.get('Shares', ''),
                'Prev FF': '#N/A',
                'New FF': df2_added.get('Free float-Coeff', ''),
                'Prev Capping': '#N/A',
                'New Capping': df2_added.get('Capping Factor-Coeff', ''),
                'Cross-Ref Symbols': cross_ref_symbols_added
            }
            
            addition_df = pd.DataFrame(addition_data)
            
            # Combine with existing data
            diff_df = pd.concat([diff_df, addition_df], ignore_index=True)
            # Re-rank
            diff_df['Rank'] = range(1, len(diff_df) + 1)
    
    return diff_df, common_keys

@timer
def find_differences_vectorized_morning_index(df1_indexed, df2_indexed, is_afternoon=False):
    """Find differences for morning index comparison - vectorized operations
    
    Args:
        df1_indexed: First dataframe (Manual for morning, SOD for afternoon)
        df2_indexed: Second dataframe (SOD for morning, Manual for afternoon)
        is_afternoon: If True, also include removals (rows in df1/SOD but not in df2/Manual)
                      and additions (rows in df2/Manual but not in df1/SOD)
    """
    common_keys = df1_indexed.index.intersection(df2_indexed.index)
    
    if len(common_keys) == 0:
        diff_df = pd.DataFrame()
    else:
        df1_common = df1_indexed.loc[common_keys]
        df2_common = df2_indexed.loc[common_keys]
        
        critical_fields = ['Divisor', 't0 IV', 't0 IV unround', 'Mkt Cap', 'Nr of components']
        
        has_differences = pd.Series(False, index=common_keys)
        
        for field in critical_fields:
            if field in df1_common.columns and field in df2_common.columns:
                val1 = pd.to_numeric(df1_common[field], errors='coerce').fillna(0)
                val2 = pd.to_numeric(df2_common[field], errors='coerce').fillna(0)
                
                tolerance = 1e-10
                field_diff = abs(val1 - val2) > tolerance
                
                has_differences |= field_diff
        
        diff_keys = common_keys[has_differences]
        
        if len(diff_keys) == 0:
            diff_df = pd.DataFrame()
        else:
            df1_diff = df1_indexed.loc[diff_keys]
            df2_diff = df2_indexed.loc[diff_keys]
            
            differences_data = {
                'Rank': range(1, len(diff_keys) + 1),
                '#Symbol': df2_diff['#Symbol'],
                'Sys Date': df2_diff.get('System Date', ''),
                'IsinCode': df2_diff.get('IsinCode', ''),
                'Cntry': df2_diff.get('Country', ''),
                'Mnemo': df2_diff.get('Mnemo', ''),
                'Name': df2_diff.get('Name', ''),
                'MIC': df2_diff.get('MIC', ''),
                'Prev Divisor': df1_diff.get('Divisor', ''),
                'New Divisor': df2_diff.get('Divisor', ''),
                'Prev t0 IV': df1_diff.get('t0 IV', ''),
                't0 IV   SOD': df2_diff.get('t0 IV', ''),
                'Prev t0 IV unround': df1_diff.get('t0 IV unround', ''),
                't0 IV unround': df2_diff.get('t0 IV unround', ''),
                'Prev Mkt Cap': df1_diff.get('Mkt Cap', ''),
                'New Mkt Cap': df2_diff.get('Mkt Cap', ''),
                'Prev Nr of comp': df1_diff.get('Nr of components', ''),
                'Nr of comp': df2_diff.get('Nr of components', '')
            }
            
            diff_df = pd.DataFrame(differences_data)
    
    # For afternoon comparisons, also include removals and additions
    if is_afternoon:
        # First handle removals (rows in SOD but not in Manual)
        removed_keys = df1_indexed.index.difference(df2_indexed.index)
        
        if len(removed_keys) > 0:
            df1_removed = df1_indexed.loc[removed_keys]
            
            # Create removal rows with SOD values and #N/A for Manual values
            removal_data = {
                'Rank': range(len(diff_df) + 1, len(diff_df) + len(removed_keys) + 1),
                '#Symbol': df1_removed['#Symbol'],
                'Sys Date': df1_removed.get('System Date', ''),
                'IsinCode': df1_removed.get('IsinCode', ''),
                'Cntry': df1_removed.get('Country', ''),
                'Mnemo': df1_removed.get('Mnemo', ''),
                'Name': df1_removed.get('Name', ''),
                'MIC': df1_removed.get('MIC', ''),
                'Prev Divisor': df1_removed.get('Divisor', ''),
                'New Divisor': '#N/A',
                'Prev t0 IV': df1_removed.get('t0 IV', ''),
                't0 IV   SOD': '#N/A',
                'Prev t0 IV unround': df1_removed.get('t0 IV unround', ''),
                't0 IV unround': '#N/A',
                'Prev Mkt Cap': df1_removed.get('Mkt Cap', ''),
                'New Mkt Cap': '#N/A',
                'Prev Nr of comp': df1_removed.get('Nr of components', ''),
                'Nr of comp': '#N/A'
            }
            
            removal_df = pd.DataFrame(removal_data)
            
            # Combine differences and removals
            diff_df = pd.concat([diff_df, removal_df], ignore_index=True)
            # Re-rank
            diff_df['Rank'] = range(1, len(diff_df) + 1)
        
        # Now handle additions (rows in Manual but not in SOD)
        added_keys = df2_indexed.index.difference(df1_indexed.index)
        
        if len(added_keys) > 0:
            df2_added = df2_indexed.loc[added_keys]
            
            # Create addition rows with Manual values and #N/A for SOD values
            addition_data = {
                'Rank': range(len(diff_df) + 1, len(diff_df) + len(added_keys) + 1),
                '#Symbol': df2_added['#Symbol'],
                'Sys Date': df2_added.get('System Date', ''),
                'IsinCode': df2_added.get('IsinCode', ''),
                'Cntry': df2_added.get('Country', ''),
                'Mnemo': df2_added.get('Mnemo', ''),
                'Name': df2_added.get('Name', ''),
                'MIC': df2_added.get('MIC', ''),
                'Prev Divisor': '#N/A',
                'New Divisor': df2_added.get('Divisor', ''),
                'Prev t0 IV': '#N/A',
                't0 IV   SOD': df2_added.get('t0 IV', ''),
                'Prev t0 IV unround': '#N/A',
                't0 IV unround': df2_added.get('t0 IV unround', ''),
                'Prev Mkt Cap': '#N/A',
                'New Mkt Cap': df2_added.get('Mkt Cap', ''),
                'Prev Nr of comp': '#N/A',
                'Nr of comp': df2_added.get('Nr of components', '')
            }
            
            addition_df = pd.DataFrame(addition_data)
            
            # Combine with existing data
            diff_df = pd.concat([diff_df, addition_df], ignore_index=True)
            # Re-rank
            diff_df['Rank'] = range(1, len(diff_df) + 1)
    
    return diff_df, common_keys

def excel_column_name(col_index):
    """Convert column index to Excel column name (A, B, ..., Z, AA, AB, ...)"""
    result = ""
    while col_index >= 0:
        result = chr(65 + col_index % 26) + result
        col_index = col_index // 26 - 1
        if col_index < 0:
            break
    return result

@timer
def write_excel_optimized(diff_df, df1_clean, df2_clean, output_path, comparison_type='stock'):
    """Write Excel file with optimized bulk operations and formatting"""
    try:
        writer = pd.ExcelWriter(output_path, engine='xlsxwriter')
        workbook = writer.book
        workbook.nan_inf_to_errors = True
        
        orange_format = workbook.add_format({'bg_color': '#FFC000', 'font_name': 'Verdana', 'font_size': 10})
        red_format = workbook.add_format({'bg_color': '#FF0000', 'font_color': '#FFFFFF', 'font_name': 'Verdana', 'font_size': 10})
        blue_format = workbook.add_format({'bg_color': '#0070C0', 'font_color': '#FFFFFF', 'font_name': 'Verdana', 'font_size': 10})
        purple_format = workbook.add_format({'bg_color': '#800080', 'font_color': '#FFFFFF', 'font_name': 'Verdana', 'font_size': 10})
        black_format = workbook.add_format({'bg_color': '#000000', 'font_color': '#FFFFFF', 'font_name': 'Verdana', 'font_size': 10})
        normal_format = workbook.add_format({'font_name': 'Verdana', 'font_size': 10})
        header_format = workbook.add_format({
            'bold': True,
            'font_name': 'Verdana',
            'font_size': 10,
            'bg_color': '#D9D9D9'
        })
        
        # Sheet 1: Differences
        if not diff_df.empty:
            diff_df_formatted = diff_df.copy()
            
            # Determine if this is a stock or index comparison
            is_stock = comparison_type in ['morning_stock', 'afternoon_stock', 'evening_stock']
            is_index = comparison_type in ['morning_index', 'afternoon_index', 'evening_index']
            
            # Apply formatting based on comparison type
            if is_stock:
                diff_df_formatted['_shares_changed'] = False
                diff_df_formatted['_prev_shares_blue'] = False
                diff_df_formatted['_ff_red'] = False
                diff_df_formatted['_ff_orange'] = False
                diff_df_formatted['_capping_changed'] = False
                diff_df_formatted['_adj_price_changed'] = False
                diff_df_formatted['_icb_changed'] = False
                diff_df_formatted['_div_positive'] = False
                diff_df_formatted['_mnemo_allowed'] = False
                diff_df_formatted['_mnemo_purple'] = False
                diff_df_formatted['_name_purple'] = False
                diff_df_formatted['_close_prc_red'] = False
                diff_df_formatted['_adj_closing_price_red'] = False
                diff_df_formatted['_symbol_not_in_crossref'] = False
                diff_df_formatted['_is_removal'] = False
                
                for idx in range(len(diff_df)):
                    # Check if this is a removal row
                    adj_rsn = str(diff_df.iloc[idx].get('Adj. Rsn', '')).strip()
                    is_removal = adj_rsn == 'Removal'
                    diff_df_formatted.iloc[idx, diff_df_formatted.columns.get_loc('_is_removal')] = is_removal
                    
                    # Check Mnemo first
                    mnemo_value = str(diff_df.iloc[idx].get('Mnemo', '')).strip()
                    mnemo_allowed = mnemo_value in ALLOWED_MNEMONICS
                    diff_df_formatted.iloc[idx, diff_df_formatted.columns.get_loc('_mnemo_allowed')] = mnemo_allowed
                    
                    # Check if shares changed - for New Shares (orange) and Prev Shares (blue)
                    new_shares = diff_df.iloc[idx]['New Shares']
                    prev_shares = diff_df.iloc[idx]['Prev. Shares']
                    shares_changed = (
                        pd.notna(new_shares) and pd.notna(prev_shares) and str(new_shares) != str(prev_shares)
                    )
                    diff_df_formatted.iloc[idx, diff_df_formatted.columns.get_loc('_shares_changed')] = shares_changed
                    
                    # COMBINED: Prev Shares gets blue ONLY if Mnemo is in ALLOWED_MNEMONICS AND shares changed
                    prev_shares_blue = mnemo_allowed and shares_changed
                    diff_df_formatted.iloc[idx, diff_df_formatted.columns.get_loc('_prev_shares_blue')] = prev_shares_blue
                    
                    # Check FF (Free Float)
                    new_ff = diff_df.iloc[idx]['New FF']
                    prev_ff = diff_df.iloc[idx]['Prev FF']
                    new_ff_clean = '' if pd.isna(new_ff) else new_ff
                    try:
                        new_ff_numeric = float(new_ff_clean) if new_ff_clean != '' else None
                    except (ValueError, TypeError):
                        new_ff_numeric = None
                    
                    ff_red = new_ff_clean == '' or new_ff_numeric is None or (new_ff_numeric is not None and new_ff_numeric > 1)
                    ff_orange = not ff_red and pd.notna(new_ff) and pd.notna(prev_ff) and str(new_ff).strip() != str(prev_ff).strip()
                    
                    diff_df_formatted.iloc[idx, diff_df_formatted.columns.get_loc('_ff_red')] = ff_red
                    diff_df_formatted.iloc[idx, diff_df_formatted.columns.get_loc('_ff_orange')] = ff_orange
                    
                    # Check Capping
                    new_capping = diff_df.iloc[idx]['New Capping']
                    prev_capping = diff_df.iloc[idx]['Prev Capping']
                    diff_df_formatted.iloc[idx, diff_df_formatted.columns.get_loc('_capping_changed')] = (
                        pd.notna(new_capping) and pd.notna(prev_capping) and str(new_capping) != str(prev_capping)
                    )
                    
                    # Check if Adj Closing price is different from Close Prc
                    adj_closing_price = diff_df.iloc[idx]['Adj Closing price']
                    close_prc = diff_df.iloc[idx]['Close Prc']
                    diff_df_formatted.iloc[idx, diff_df_formatted.columns.get_loc('_adj_price_changed')] = (
                        pd.notna(adj_closing_price) and pd.notna(close_prc) and str(adj_closing_price).strip() != str(close_prc).strip()
                    )
                    
                    # Check if New ICB is different from Prev ICB
                    new_icb = diff_df.iloc[idx]['New ICB']
                    prev_icb = diff_df.iloc[idx]['Prev ICB']
                    diff_df_formatted.iloc[idx, diff_df_formatted.columns.get_loc('_icb_changed')] = (
                        pd.notna(new_icb) and pd.notna(prev_icb) and str(new_icb).strip() != str(prev_icb).strip()
                    )
                    
                    # Check if Net Div is a positive number
                    net_div = diff_df.iloc[idx]['Net Div']
                    try:
                        net_div_numeric = float(net_div) if pd.notna(net_div) and str(net_div).strip() != '' else None
                        div_positive = net_div_numeric is not None and net_div_numeric > 0
                    except (ValueError, TypeError):
                        div_positive = False
                    diff_df_formatted.iloc[idx, diff_df_formatted.columns.get_loc('_div_positive')] = div_positive
                    
                    # NEW: Check if Mnemo is in PURPLE_MNEMONICS (AEX BANK or PX1)
                    mnemo_purple = mnemo_value in PURPLE_MNEMONICS
                    diff_df_formatted.iloc[idx, diff_df_formatted.columns.get_loc('_mnemo_purple')] = mnemo_purple
                    
                    # NEW: Check if Mnemo is in PURPLE_INDEX_MNEMONICS (for Name column)
                    name_purple = mnemo_value in PURPLE_INDEX_MNEMONICS
                    diff_df_formatted.iloc[idx, diff_df_formatted.columns.get_loc('_name_purple')] = name_purple
                    
                    # NEW: Check if Close Prc is empty or 0
                    close_prc_val = diff_df.iloc[idx]['Close Prc']
                    try:
                        close_prc_numeric = float(close_prc_val) if pd.notna(close_prc_val) and str(close_prc_val).strip() != '' else None
                        close_prc_red = close_prc_numeric is None or close_prc_numeric == 0
                    except (ValueError, TypeError):
                        close_prc_red = True
                    diff_df_formatted.iloc[idx, diff_df_formatted.columns.get_loc('_close_prc_red')] = close_prc_red
                    
                    # NEW: Check if Adj Closing price is empty or 0
                    adj_closing_val = diff_df.iloc[idx]['Adj Closing price']
                    try:
                        adj_closing_numeric = float(adj_closing_val) if pd.notna(adj_closing_val) and str(adj_closing_val).strip() != '' else None
                        adj_closing_red = adj_closing_numeric is None or adj_closing_numeric == 0
                    except (ValueError, TypeError):
                        adj_closing_red = True
                    diff_df_formatted.iloc[idx, diff_df_formatted.columns.get_loc('_adj_closing_price_red')] = adj_closing_red
                    
                    # NEW: Check if all Cross-Ref Symbols values exist in the #Symbol column of the output sheet
                    cross_ref_symbols_str = str(diff_df.iloc[idx].get('Cross-Ref Symbols', '')).strip()
                    
                    if cross_ref_symbols_str != '' and cross_ref_symbols_str != 'nan':
                        # Split the semicolon-separated list
                        cross_ref_list = [s.strip() for s in cross_ref_symbols_str.split(';') if s.strip() != '']
                        
                        # Get all #Symbol values from the entire output sheet
                        all_symbols_in_output = set(diff_df['#Symbol'].astype(str).str.strip())
                        
                        # Check if ALL cross-ref symbols exist in the output sheet's #Symbol column
                        all_exist = all(symbol in all_symbols_in_output for symbol in cross_ref_list)
                        
                        # Flag if NOT all exist
                        symbol_not_in_crossref = not all_exist
                    else:
                        # If cross-ref list is empty, don't flag it
                        symbol_not_in_crossref = False
                    
                    diff_df_formatted.iloc[idx, diff_df_formatted.columns.get_loc('_symbol_not_in_crossref')] = symbol_not_in_crossref
                
                diff_df_main = diff_df_formatted.drop([
                    '_shares_changed', '_prev_shares_blue', '_ff_red', '_ff_orange', '_capping_changed', 
                    '_adj_price_changed', '_icb_changed', '_div_positive', '_mnemo_allowed', '_mnemo_purple',
                    '_name_purple', '_close_prc_red', '_adj_closing_price_red', '_symbol_not_in_crossref', '_is_removal'
                ], axis=1)
            
            elif is_index:
                diff_df_formatted['_divisor_changed'] = False
                diff_df_formatted['_t0iv_changed'] = False
                diff_df_formatted['_t0iv_unround_changed'] = False
                diff_df_formatted['_mktcap_changed'] = False
                diff_df_formatted['_nrcomp_changed'] = False
                diff_df_formatted['_mnemo_blue'] = False
                diff_df_formatted['_is_removal'] = False
                
                for idx in range(len(diff_df)):
                    # Check if this is a removal row (check if New Divisor is '#N/A')
                    new_divisor = str(diff_df.iloc[idx].get('New Divisor', '')).strip()
                    is_removal = new_divisor == '#N/A'
                    diff_df_formatted.iloc[idx, diff_df_formatted.columns.get_loc('_is_removal')] = is_removal
                    
                    for field_pair, flag_col in [
                        (('Prev Divisor', 'New Divisor'), '_divisor_changed'),
                        (('Prev t0 IV', 't0 IV   SOD'), '_t0iv_changed'),
                        (('Prev t0 IV unround', 't0 IV unround'), '_t0iv_unround_changed'),
                        (('Prev Mkt Cap', 'New Mkt Cap'), '_mktcap_changed'),
                        (('Prev Nr of comp', 'Nr of comp'), '_nrcomp_changed')
                    ]:
                        prev_val = diff_df.iloc[idx][field_pair[0]]
                        new_val = diff_df.iloc[idx][field_pair[1]]
                        diff_df_formatted.iloc[idx, diff_df_formatted.columns.get_loc(flag_col)] = (
                            pd.notna(new_val) and pd.notna(prev_val) and str(new_val) != str(prev_val)
                        )
                    
                    # Check if Mnemo should be blue (not in allowed list AND divisor changed)
                    mnemo_value = str(diff_df.iloc[idx].get('Mnemo', '')).strip()
                    divisor_changed = diff_df_formatted.iloc[idx]['_divisor_changed']
                    mnemo_blue = (mnemo_value not in ALLOWED_MNEMONICS) and divisor_changed
                    diff_df_formatted.iloc[idx, diff_df_formatted.columns.get_loc('_mnemo_blue')] = mnemo_blue
                
                diff_df_main = diff_df_formatted.drop([
                    '_divisor_changed', '_t0iv_changed', '_t0iv_unround_changed', 
                    '_mktcap_changed', '_nrcomp_changed', '_mnemo_blue', '_is_removal'
                ], axis=1)
            else:
                diff_df_main = diff_df_formatted
            
            diff_df_main.to_excel(writer, sheet_name='Differences', index=False, startrow=4, header=False)
            
            worksheet1 = writer.sheets['Differences']
            max_rank = diff_df['Rank'].max()
            worksheet1.write(0, 0, max_rank, normal_format)
            
            for col_num, value in enumerate(diff_df.columns.values):
                worksheet1.write(2, col_num, value, header_format)
            
            data_start_row = 4
            data_end_row = data_start_row + len(diff_df) - 1
            
            # Apply conditional formatting
            for col_idx in range(len(diff_df.columns)):
                col_name = excel_column_name(col_idx)
                col_range = f"{col_name}{data_start_row + 1}:{col_name}{data_end_row + 1}"
                worksheet1.conditional_format(col_range, {
                    'type': 'no_blanks',
                    'format': normal_format
                })
            
            # Apply specific cell formatting based on comparison type
            if is_stock:
                symbol_col = diff_df.columns.get_loc('#Symbol')
                new_shares_col = diff_df.columns.get_loc('New Shares')
                prev_shares_col = diff_df.columns.get_loc('Prev. Shares')
                new_ff_col = diff_df.columns.get_loc('New FF')
                new_capping_col = diff_df.columns.get_loc('New Capping')
                adj_closing_price_col = diff_df.columns.get_loc('Adj Closing price')
                new_icb_col = diff_df.columns.get_loc('New ICB')
                net_div_col = diff_df.columns.get_loc('Net Div')
                gross_div_col = diff_df.columns.get_loc('Gross Div')
                mnemo_col = diff_df.columns.get_loc('Mnemo')
                name_col = diff_df.columns.get_loc('Name')
                close_prc_col = diff_df.columns.get_loc('Close Prc')
                
                for row_idx in range(len(diff_df)):
                    excel_row = row_idx + data_start_row
                    
                    # Orange: New Shares changed
                    if diff_df_formatted.iloc[row_idx]['_shares_changed']:
                        worksheet1.write(excel_row, new_shares_col, diff_df.iloc[row_idx]['New Shares'], orange_format)
                    
                    # NEW: Blue: Prev Shares when shares changed
                    if diff_df_formatted.iloc[row_idx]['_prev_shares_blue']:
                        worksheet1.write(excel_row, prev_shares_col, diff_df.iloc[row_idx]['Prev. Shares'], blue_format)
                    
                    # FF formatting
                    if diff_df_formatted.iloc[row_idx]['_ff_red']:
                        worksheet1.write(excel_row, new_ff_col, diff_df.iloc[row_idx]['New FF'], red_format)
                    elif diff_df_formatted.iloc[row_idx]['_ff_orange']:
                        worksheet1.write(excel_row, new_ff_col, diff_df.iloc[row_idx]['New FF'], orange_format)
                    
                    # Capping changed
                    if diff_df_formatted.iloc[row_idx]['_capping_changed']:
                        worksheet1.write(excel_row, new_capping_col, diff_df.iloc[row_idx]['New Capping'], orange_format)
                    
                    # Adj price changed
                    if diff_df_formatted.iloc[row_idx]['_adj_price_changed']:
                        worksheet1.write(excel_row, adj_closing_price_col, diff_df.iloc[row_idx]['Adj Closing price'], orange_format)
                    
                    # ICB changed
                    if diff_df_formatted.iloc[row_idx]['_icb_changed']:
                        worksheet1.write(excel_row, new_icb_col, diff_df.iloc[row_idx]['New ICB'], orange_format)
                    
                    # Dividend positive
                    if diff_df_formatted.iloc[row_idx]['_div_positive']:
                        worksheet1.write(excel_row, net_div_col, diff_df.iloc[row_idx]['Net Div'], orange_format)
                        worksheet1.write(excel_row, gross_div_col, diff_df.iloc[row_idx]['Gross Div'], orange_format)
                    
                    # NEW: Purple background for Mnemo if AEX BANK or PX1
                    if diff_df_formatted.iloc[row_idx]['_mnemo_purple']:
                        worksheet1.write(excel_row, mnemo_col, diff_df.iloc[row_idx]['Mnemo'], purple_format)
                    
                    # NEW: Purple background for Name if Mnemo is in PURPLE_INDEX_MNEMONICS
                    if diff_df_formatted.iloc[row_idx]['_name_purple']:
                        worksheet1.write(excel_row, name_col, diff_df.iloc[row_idx]['Name'], purple_format)
                    
                    # NEW: Red background for Close Prc if empty or 0
                    if diff_df_formatted.iloc[row_idx]['_close_prc_red']:
                        worksheet1.write(excel_row, close_prc_col, diff_df.iloc[row_idx]['Close Prc'], red_format)
                    
                    # NEW: Red background for Adj Closing price if empty or 0 (only if not already orange from adj_price_changed)
                    if diff_df_formatted.iloc[row_idx]['_adj_closing_price_red'] and not diff_df_formatted.iloc[row_idx]['_adj_price_changed']:
                        worksheet1.write(excel_row, adj_closing_price_col, diff_df.iloc[row_idx]['Adj Closing price'], red_format)
                    
                    # NEW: Black background for #Symbol if not in Cross-Ref Symbols list
                    if diff_df_formatted.iloc[row_idx]['_symbol_not_in_crossref']:
                        worksheet1.write(excel_row, symbol_col, diff_df.iloc[row_idx]['#Symbol'], black_format)
            
            elif is_index:
                field_cols = {
                    'New Divisor': diff_df.columns.get_loc('New Divisor'),
                    't0 IV   SOD': diff_df.columns.get_loc('t0 IV   SOD'),
                    't0 IV unround': diff_df.columns.get_loc('t0 IV unround'),
                    'New Mkt Cap': diff_df.columns.get_loc('New Mkt Cap'),
                    'Nr of comp': diff_df.columns.get_loc('Nr of comp')
                }
                
                flag_cols = ['_divisor_changed', '_t0iv_changed', '_t0iv_unround_changed', '_mktcap_changed', '_nrcomp_changed']
                output_cols = ['New Divisor', 't0 IV   SOD', 't0 IV unround', 'New Mkt Cap', 'Nr of comp']
                
                for row_idx in range(len(diff_df)):
                    excel_row = row_idx + data_start_row
                    
                    for flag_col, output_col in zip(flag_cols, output_cols):
                        if diff_df_formatted.iloc[row_idx][flag_col]:
                            worksheet1.write(excel_row, field_cols[output_col], diff_df.iloc[row_idx][output_col], orange_format)
                    
                    # Apply blue formatting to Mnemo if needed
                    if diff_df_formatted.iloc[row_idx]['_mnemo_blue']:
                        mnemo_col = diff_df.columns.get_loc('Mnemo')
                        worksheet1.write(excel_row, mnemo_col, diff_df.iloc[row_idx]['Mnemo'], blue_format)
        
        else:
            worksheet1 = workbook.add_worksheet('Differences')
            worksheet1.write(0, 0, 0, normal_format)
            for col_num, value in enumerate(diff_df.columns.values):
                worksheet1.write(2, col_num, value, header_format)
        
        for i, col in enumerate(diff_df.columns):
            max_length = max(len(str(col)), 10)
            if not diff_df.empty:
                max_length = max(max_length, diff_df[col].astype(str).str.len().max())
            worksheet1.set_column(i, i, min(max_length + 2, 50))
        
        # Sheet 2: Raw Data File 1
        df1_clean_output = df1_clean.drop('composite_key', axis=1, errors='ignore')
        df1_clean_output = df1_clean_output.replace([np.inf, -np.inf], np.nan)
        
        for col in df1_clean_output.columns:
            if df1_clean_output[col].dtype == 'object':
                df1_clean_output[col] = df1_clean_output[col].fillna('')
        
        if not df1_clean_output.empty:
            for col in df1_clean_output.select_dtypes(include=['object']).columns:
                df1_clean_output[col] = df1_clean_output[col].astype(str).replace(['nan', 'NaN', 'None'], '')
            
            df1_clean_output.to_excel(writer, sheet_name='Raw Data File 1', index=False, startrow=4, header=False)
            worksheet2 = writer.sheets['Raw Data File 1']
            
            for col_num, value in enumerate(df1_clean_output.columns.values):
                worksheet2.write(2, col_num, value, header_format)
            
            if len(df1_clean_output) > 0:
                end_col_name = excel_column_name(len(df1_clean_output.columns) - 1)
                data_range = f"A5:{end_col_name}{4 + len(df1_clean_output)}"
                worksheet2.conditional_format(data_range, {
                    'type': 'no_blanks',
                    'format': normal_format
                })
        else:
            worksheet2 = workbook.add_worksheet('Raw Data File 1')
        
        for i, col in enumerate(df1_clean_output.columns):
            max_length = max(len(str(col)), 10)
            if not df1_clean_output.empty:
                max_length = max(max_length, df1_clean_output[col].astype(str).str.len().max())
            worksheet2.set_column(i, i, min(max_length + 2, 50))
        
        # Sheet 3: Raw Data File 2
        df2_clean_output = df2_clean.drop('composite_key', axis=1, errors='ignore')
        df2_clean_output = df2_clean_output.replace([np.inf, -np.inf], np.nan)
        
        for col in df2_clean_output.columns:
            if df2_clean_output[col].dtype == 'object':
                df2_clean_output[col] = df2_clean_output[col].fillna('')
        
        if not df2_clean_output.empty:
            for col in df2_clean_output.select_dtypes(include=['object']).columns:
                df2_clean_output[col] = df2_clean_output[col].astype(str).replace(['nan', 'NaN', 'None'], '')
            
            df2_clean_output.to_excel(writer, sheet_name='Raw Data File 2', index=False, startrow=4, header=False)
            worksheet3 = writer.sheets['Raw Data File 2']
            
            for col_num, value in enumerate(df2_clean_output.columns.values):
                worksheet3.write(2, col_num, value, header_format)
            
            if len(df2_clean_output) > 0:
                end_col_name = excel_column_name(len(df2_clean_output.columns) - 1)
                data_range = f"A5:{end_col_name}{4 + len(df2_clean_output)}"
                worksheet3.conditional_format(data_range, {
                    'type': 'no_blanks',
                    'format': normal_format
                })
        else:
            worksheet3 = workbook.add_worksheet('Raw Data File 2')
        
        for i, col in enumerate(df2_clean_output.columns):
            max_length = max(len(str(col)), 10)
            if not df2_clean_output.empty:
                max_length = max(max_length, df2_clean_output[col].astype(str).str.len().max())
            worksheet3.set_column(i, i, min(max_length + 2, 50))
        
        writer.close()
        
        logger.info(f"Successfully wrote Excel file: {output_path}")
        return True
        
    except Exception as e:
        logger.error(f"Error writing Excel file: {str(e)}")
        return False

def perform_comparison(comparison_type='morning_stock'):
    """Perform the file comparison and generate output"""
    try:
        print(f"\n{'='*60}")
        print(f"Starting {comparison_type.upper()} comparison...")
        print(f"{'='*60}\n")
        
        file_status = check_files_available(comparison_type)
        
        if not file_status['available']:
            logger.info(f"{comparison_type.upper()} files not available yet")
            print(f"{comparison_type.upper()} files not available yet.")
            return False
        
        # Generate output filename with current date
        current_date = datetime.now().strftime("%Y%m%d")
        config = COMPARISON_CONFIGS[comparison_type]
        output_filename = config['output_filename'].format(date=current_date)
        output_path = os.path.join(OUTPUT_DIR, output_filename)
        
        # Check if output file already exists and whether overwrite is allowed

        if os.path.exists(output_path):
            if config.get('allow_overwrite', False):
                if should_allow_overwrite(comparison_type):
                    # For afternoon and evening, check if input files are newer
                    # Get input file paths
                    if comparison_type.startswith('evening_'):
                        input_files = [file_status['file1_path'], file_status['file2_path']]
                    else:
                        input_files = [file_status['sod_path'], file_status['manual_path']]
                    
                    # Only regenerate if input files are newer than output
                    if should_regenerate_output(input_files, output_path):
                        logger.info(f"{comparison_type.upper()} output file exists but input files are newer. Regenerating...")
                        print(f"  {comparison_type.upper()} output file exists but input files are newer.")
                    else:
                        logger.info(f"{comparison_type.upper()} output file is up to date. Skipping.")
                        print(f"  {comparison_type.upper()} output file is up to date.")
                        print("  Skipping comparison.\n")
                        return True
                else:
                    logger.info(f"{comparison_type.upper()} output file exists and overwrite time has passed. Skipping.")
                    print(f"  {comparison_type.upper()} output file already exists and overwrite time has passed.")
                    print("  Skipping comparison.\n")
                    return True
            else:
                logger.info(f"{comparison_type.upper()} output file already exists: {output_filename}. Skipping comparison.")
                print(f"  {comparison_type.upper()} output file already exists: {output_filename}")
                print("  Skipping comparison.\n")
                return True
        
        # Get file paths based on comparison type
        if comparison_type.startswith('evening_'):
            file1_path = file_status['file1_path']
            file2_path = file_status['file2_path']
            
            print(f"Files found:")
            print(f"  EOD: {os.path.basename(file1_path)}")
            print(f"  Manual: {os.path.basename(file2_path)}")
            print()
        else:
            sod_path = file_status['sod_path']
            manual_path = file_status['manual_path']
            
            print(f"Files found:")
            print(f"  SOD: {os.path.basename(sod_path)}")
            print(f"  Manual: {os.path.basename(manual_path)}")
            print()
        
        # Read files
        # For afternoon and evening comparisons: older file is df1, newer file is df2
        # For morning comparisons: Manual is df1 (older), SOD is df2 (newer)
        is_afternoon = comparison_type.startswith('afternoon_')
        is_evening = comparison_type.startswith('evening_')
        
        if is_evening:
            # For evening: EOD is df1 (older), Manual is df2 (newer)
            df1 = read_excel_file(file1_path)
            df2 = read_excel_file(file2_path)
        elif is_afternoon:
            df1 = read_excel_file(sod_path)
            df2 = read_excel_file(manual_path)
        else:
            df1 = read_excel_file(manual_path)
            df2 = read_excel_file(sod_path)
        
        if df1 is None or df2 is None:
            logger.error("Failed to read one or both files")
            return False
        
        # Get comparison configuration
        key_fields = config['key_fields']
        
        # Prepare dataframes
        df1_indexed, df2_indexed = prepare_dataframes(df1, df2, key_fields)
        
        # Find differences
        comparison_function = config['comparison_function']
        # Evening comparisons should include removals and additions like afternoon
        is_afternoon_or_evening = is_afternoon or is_evening
        
        if comparison_function == 'find_differences_vectorized_morning_stock':
            diff_df, common_keys = find_differences_vectorized_morning_stock(df1_indexed, df2_indexed, is_afternoon_or_evening)
        elif comparison_function == 'find_differences_vectorized_morning_index':
            diff_df, common_keys = find_differences_vectorized_morning_index(df1_indexed, df2_indexed, is_afternoon_or_evening)
        else:
            logger.error(f"Unknown comparison function: {comparison_function}")
            return False
        
        # Write output file
        success = write_excel_optimized(diff_df, df1_indexed, df2_indexed, output_path, comparison_type)
        
        if success:
            print(f"\n  {comparison_type.upper()} comparison completed successfully!")
            print(f"  Output file: {output_filename}")
            print(f"  Changes found: {len(diff_df)}")
            print(f"  Common records: {len(common_keys)}")
            logger.info(f"{comparison_type.upper()} comparison completed. Changes: {len(diff_df)}, Common records: {len(common_keys)}")
        else:
            print(f"\n  Failed to write {comparison_type.UPPER()} output file")
            logger.error(f"Failed to write {comparison_type.upper()} output file")
        
        return success
        
    except Exception as e:
        logger.error(f"Error during {comparison_type} comparison: {str(e)}")
        print(f"Error during {comparison_type} comparison: {str(e)}")
        return False

def perform_all_comparisons():
    """Perform all configured comparisons"""
    results = {}
    for comparison_type in COMPARISON_CONFIGS.keys():
        results[comparison_type] = perform_comparison(comparison_type)
    return results

class FileMonitorHandler(FileSystemEventHandler):
    """Handle file system events for monitoring"""
    
    def __init__(self):
        self.last_check = {}
        self.check_interval = 30
        
    def on_created(self, event):
        """Handle file creation events"""
        if event.is_directory:
            return
        
        self.check_and_process(event.src_path)
    
    def on_modified(self, event):
        """Handle file modification events"""
        if event.is_directory:
            return
        
        self.check_and_process(event.src_path)
    
    def check_and_process(self, file_path):
        """Check if files are available and process them"""
        current_time = time.time()
        
        # Determine which comparison type based on filename and folder
        filename = os.path.basename(file_path)
        folder_path = os.path.dirname(file_path)
        comparison_type = None
        
        # Check if file is in afternoon manual folder
        is_afternoon_manual = 'Afternoon' in folder_path
        
        # Check if file is EOD or Manual in the Monthly Archive folder (for evening)
        is_eod_file = 'EOD' in filename and 'Monthly Archive' in folder_path and 'Merged files' not in folder_path
        is_evening_manual = 'MANUAL' in filename and 'Monthly Archive' in folder_path and 'Merged files' not in folder_path and '.csv' in filename
        
        for comp_type, config in COMPARISON_CONFIGS.items():
            if config['file_suffix'] in filename:
                # Match evening comparisons
                if comp_type.startswith('evening_') and (is_eod_file or is_evening_manual):
                    # Only trigger on Manual file arrival, and only if EOD file already exists
                    if is_evening_manual:
                        comparison_type = comp_type
                        # Check if EOD file exists first
                        expected_files = get_expected_filenames(comp_type)
                        eod_path = os.path.join(MONITOR_FOLDERS['eod'], expected_files['file1'])
                        if not file_exists_and_ready(eod_path):
                            logger.info(f"Evening Manual file detected but EOD file not ready yet for {comp_type}")
                            return
                    else:
                        # If it's an EOD file, don't trigger yet - wait for Manual
                        return
                    break
                # Match afternoon comparisons with afternoon manual files
                elif is_afternoon_manual and comp_type.startswith('afternoon_'):
                    comparison_type = comp_type
                    break
                # Match morning comparisons with regular manual or SOD files
                elif not is_afternoon_manual and not is_eod_file and not is_evening_manual and not comp_type.startswith('afternoon_') and not comp_type.startswith('evening_'):
                    comparison_type = comp_type
                    break
        
        if comparison_type is None:
            return
        
        # Avoid checking too frequently for each comparison type
        if comparison_type in self.last_check:
            if current_time - self.last_check[comparison_type] < self.check_interval:
                return
        
        self.last_check[comparison_type] = current_time
        
        # Wait a bit for file to be completely written
        time.sleep(5)
        
        if perform_comparison(comparison_type):
            logger.info(f"{comparison_type.upper()} comparison completed successfully!")
            print(f"{comparison_type.upper()} comparison completed successfully!")
        else:
            logger.info(f"{comparison_type.upper()} files not ready yet or comparison failed")

def start_monitoring():
    """Start monitoring the specified folders"""
    logger.info("Starting file monitoring...")
    print("=== GIS Changes File Monitor ===")
    print("Monitoring folders for file changes...")
    
    # Display expected files for all comparison types
    for comparison_type in COMPARISON_CONFIGS.keys():
        expected_files = get_expected_filenames(comparison_type)
        config = COMPARISON_CONFIGS[comparison_type]
        
        if comparison_type.startswith('evening_'):
            print(f"\nLooking for {comparison_type.upper()} files:")
            print(f"  EOD: {expected_files['file1']}")
            print(f"  Manual: {expected_files['file2']}")
        else:
            manual_folder = config.get('manual_folder', 'manual')
            print(f"\nLooking for {comparison_type.upper()} files:")
            print(f"  SOD: {expected_files['sod']}")
            print(f"  Manual ({manual_folder}): {expected_files['manual']}")
    
    print(f"\nFolders:")
    print(f"  Morning Manual: {MONITOR_FOLDERS['manual']}")
    print(f"  SOD: {MONITOR_FOLDERS['sod']}")
    print(f"  Afternoon Manual: {MONITOR_FOLDERS['afternoon_manual']}")
    print(f"  EOD/Evening Manual: {MONITOR_FOLDERS['eod']}")
    print()
    
    # Check if files already exist
    print("Checking if files already exist...")
    results = perform_all_comparisons()
    
    if any(results.values()):
        print("\nSome comparisons completed!")
    else:
        print("\nNo files available yet, starting monitoring...")
    
    # Set up file monitors
    event_handler = FileMonitorHandler()
    observer = Observer()
    
    # Monitor all folders
    try:
        observer.schedule(event_handler, MONITOR_FOLDERS['manual'], recursive=False)
        observer.schedule(event_handler, MONITOR_FOLDERS['sod'], recursive=False)
        observer.schedule(event_handler, MONITOR_FOLDERS['afternoon_manual'], recursive=False)
        observer.schedule(event_handler, MONITOR_FOLDERS['eod'], recursive=False)
        
        observer.start()
        logger.info("File monitoring started successfully")
        print("File monitoring started. Press Ctrl+C to stop.")
        
        try:
            while True:
                time.sleep(60)
                
                # Periodic check in case file events were missed
                current_time = datetime.now()
                if current_time.minute % 5 == 0:
                    logger.info("Periodic check for files...")
                    perform_all_comparisons()
                
        except KeyboardInterrupt:
            observer.stop()
            logger.info("File monitoring stopped by user")
            print("\nFile monitoring stopped.")
        
        observer.join()
        
    except Exception as e:
        logger.error(f"Error setting up file monitoring: {str(e)}")
        print(f"Error setting up file monitoring: {str(e)}")
        
        # Fallback to polling if file monitoring fails
        print("Falling back to polling mode...")
        polling_mode()

def polling_mode():
    """Fallback polling mode if file monitoring fails"""
    logger.info("Starting polling mode...")
    print("Starting polling mode - checking for files every 2 minutes...")
    
    try:
        while True:
            logger.info("Polling for files...")
            results = perform_all_comparisons()
            
            if any(results.values()):
                logger.info("Some comparisons completed successfully!")
                print("Some comparisons completed successfully!")
                
                # After successful comparison, wait longer before next check
                print("Waiting 30 minutes before next check...")
                time.sleep(30 * 60)
            else:
                # Files not ready, check again in 2 minutes
                time.sleep(2 * 60)
                
    except KeyboardInterrupt:
        logger.info("Polling stopped by user")
        print("\nPolling stopped.")

def manual_check(comparison_type=None):
    """Perform a manual check and comparison"""
    print("=== Manual File Check ===")
    
    if comparison_type:
        # Check specific comparison type
        expected_files = get_expected_filenames(comparison_type)
        config = COMPARISON_CONFIGS[comparison_type]
        
        if comparison_type.startswith('evening_'):
            print(f"\nExpected {comparison_type.upper()} files:")
            print(f"  EOD: {expected_files['file1']}")
            print(f"  Manual: {expected_files['file2']}")
        else:
            manual_folder = config.get('manual_folder', 'manual')
            print(f"\nExpected {comparison_type.upper()} files:")
            print(f"  SOD: {expected_files['sod']}")
            print(f"  Manual ({manual_folder}): {expected_files['manual']}")
        print()
        
        file_status = check_files_available(comparison_type)
        
        print(f"File availability status:")
        print(f"  {comparison_type.upper()} files available: {file_status['available']}")
        print()
        
        if file_status['available']:
            print(f"Files are available! Starting {comparison_type.upper()} comparison...")
            return perform_comparison(comparison_type)
        else:
            print("Files are not available.")
            return False
    else:
        # Check all comparison types
        for comp_type in COMPARISON_CONFIGS.keys():
            expected_files = get_expected_filenames(comp_type)
            config = COMPARISON_CONFIGS[comp_type]
            
            if comp_type.startswith('evening_'):
                print(f"\nExpected {comp_type.upper()} files:")
                print(f"  EOD: {expected_files['file1']}")
                print(f"  Manual: {expected_files['file2']}")
            else:
                manual_folder = config.get('manual_folder', 'manual')
                print(f"\nExpected {comp_type.upper()} files:")
                print(f"  SOD: {expected_files['sod']}")
                print(f"  Manual ({manual_folder}): {expected_files['manual']}")
        
        print()
        results = perform_all_comparisons()
        
        print(f"\nResults:")
        for comp_type, success in results.items():
            status = "Success" if success else "Failed/Not Available"
            print(f"  {comp_type.upper()}: {status}")
        
        return any(results.values())

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        if sys.argv[1] == "--check":
            # Manual check mode
            comparison_type = sys.argv[2] if len(sys.argv) > 2 else None
            manual_check(comparison_type)
        elif sys.argv[1] in COMPARISON_CONFIGS.keys():
            # Run specific comparison
            perform_comparison(sys.argv[1])
        else:
            print("Usage:")
            print("  python script.py                    - Start monitoring mode")
            print("  python script.py --check            - Manual check all comparisons")
            print("  python script.py --check <type>     - Manual check specific comparison")
            print("  python script.py <type>             - Run specific comparison once")
            print(f"  Available types: {', '.join(COMPARISON_CONFIGS.keys())}")
    else:
        # Start monitoring mode
        start_monitoring()