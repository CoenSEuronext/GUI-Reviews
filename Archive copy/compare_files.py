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
    'stock': {
        'file_suffix': 'STOCK',
        'key_fields': ['#Symbol', 'Index'],
        'output_filename': 'GIS Morning Stock changes_{date}.xlsx',
        'comparison_function': 'find_differences_vectorized_morning_stock'
    },
    'index': {
        'file_suffix': 'INDEX',
        'key_fields': ['#Symbol'],
        'output_filename': 'GIS Morning Index changes_{date}.xlsx',
        'comparison_function': 'find_differences_vectorized_morning_index'
    }
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
    'sod': r"V:\PM-Indices-IndexOperations\General\Daily downloadfiles\Monthly Archive\Merged files\SOD"
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

def get_expected_filenames(comparison_type='stock'):
    """Get the expected filenames for today's comparison"""
    current_workday = get_current_workday()
    previous_workday = get_previous_workday()
    
    sod_date = current_workday.strftime("%Y%m%d")
    manual_date = previous_workday.strftime("%Y%m%d")
    
    config = COMPARISON_CONFIGS.get(comparison_type, COMPARISON_CONFIGS['stock'])
    file_suffix = config['file_suffix']
    
    expected_files = {
        'sod': f"TTMIndexEU1_GIS_SOD_{file_suffix}_{sod_date}.xlsx",
        'manual': f"TTMIndexEU1_GIS_MANUAL_{file_suffix}_{manual_date}.xlsx"
    }
    
    logger.info(f"Expected {file_suffix} files: SOD={expected_files['sod']}, Manual={expected_files['manual']}")
    
    return expected_files

def file_exists_and_ready(filepath):
    """Check if file exists and is ready (not being written to)"""
    if not os.path.exists(filepath):
        return False
    
    try:
        with open(filepath, 'rb') as f:
            f.read(1024)
        return True
    except (IOError, OSError):
        return False

def check_files_available(comparison_type='stock'):
    """Check if both expected files are available"""
    expected_files = get_expected_filenames(comparison_type)
    
    sod_path = os.path.join(MONITOR_FOLDERS['sod'], expected_files['sod'])
    manual_path = os.path.join(MONITOR_FOLDERS['manual'], expected_files['manual'])
    
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
    """Read Excel file and return DataFrame - optimized for large files"""
    try:
        logger.info(f"Reading file: {os.path.basename(file_path)}")
        
        if not os.path.exists(file_path):
            logger.error(f"File not found: {file_path}")
            return None
        
        make_file_writable(file_path)
        
        try:
            df = pd.read_excel(file_path, engine='openpyxl')
            
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
def find_differences_vectorized_morning_stock(df1_indexed, df2_indexed):
    """Find differences for morning stock comparison - vectorized operations"""
    common_keys = df1_indexed.index.intersection(df2_indexed.index)
    
    if len(common_keys) == 0:
        return pd.DataFrame(), common_keys
    
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
    
    diff_keys = common_keys[has_differences]
    
    if len(diff_keys) == 0:
        return pd.DataFrame(), common_keys
    
    df1_diff = df1_indexed.loc[diff_keys]
    df2_diff = df2_indexed.loc[diff_keys]
    
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
def find_differences_vectorized_morning_index(df1_indexed, df2_indexed):
    """Find differences for morning index comparison - vectorized operations"""
    common_keys = df1_indexed.index.intersection(df2_indexed.index)
    
    if len(common_keys) == 0:
        return pd.DataFrame(), common_keys
    
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
        return pd.DataFrame(), common_keys
    
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
            
            # Apply formatting based on comparison type
            if comparison_type == 'stock':
                diff_df_formatted['_shares_changed'] = False
                diff_df_formatted['_ff_red'] = False
                diff_df_formatted['_ff_orange'] = False
                diff_df_formatted['_capping_changed'] = False
                
                for idx in range(len(diff_df)):
                    new_shares = diff_df.iloc[idx]['New Shares']
                    prev_shares = diff_df.iloc[idx]['Prev. Shares']
                    diff_df_formatted.iloc[idx, diff_df_formatted.columns.get_loc('_shares_changed')] = (
                        pd.notna(new_shares) and pd.notna(prev_shares) and str(new_shares) != str(prev_shares)
                    )
                    
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
                    
                    new_capping = diff_df.iloc[idx]['New Capping']
                    prev_capping = diff_df.iloc[idx]['Prev Capping']
                    diff_df_formatted.iloc[idx, diff_df_formatted.columns.get_loc('_capping_changed')] = (
                        pd.notna(new_capping) and pd.notna(prev_capping) and str(new_capping) != str(prev_capping)
                    )
                
                diff_df_main = diff_df_formatted.drop(['_shares_changed', '_ff_red', '_ff_orange', '_capping_changed'], axis=1)
            
            elif comparison_type == 'index':
                diff_df_formatted['_divisor_changed'] = False
                diff_df_formatted['_t0iv_changed'] = False
                diff_df_formatted['_t0iv_unround_changed'] = False
                diff_df_formatted['_mktcap_changed'] = False
                diff_df_formatted['_nrcomp_changed'] = False
                
                for idx in range(len(diff_df)):
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
                
                diff_df_main = diff_df_formatted.drop([
                    '_divisor_changed', '_t0iv_changed', '_t0iv_unround_changed', 
                    '_mktcap_changed', '_nrcomp_changed'
                ], axis=1)
            else:
                diff_df_main = diff_df_formatted
            
            diff_df_main.to_excel(writer, sheet_name='Differences', index=False, startrow=4, header=False)
            
            worksheet1 = writer.sheets['Differences']
            max_rank = diff_df['Rank'].max()
            worksheet1.write(0, 0, max_rank, normal_format)
            
            for col_num, value in enumerate(diff_df.columns.values):
                worksheet1.write(2, col_num, value, header_format)
            
            worksheet1.set_row(3, None, None, {'hidden': True})
            
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
            if comparison_type == 'stock':
                new_shares_col = diff_df.columns.get_loc('New Shares')
                new_ff_col = diff_df.columns.get_loc('New FF')
                new_capping_col = diff_df.columns.get_loc('New Capping')
                
                for row_idx in range(len(diff_df)):
                    excel_row = row_idx + data_start_row
                    
                    if diff_df_formatted.iloc[row_idx]['_shares_changed']:
                        worksheet1.write(excel_row, new_shares_col, diff_df.iloc[row_idx]['New Shares'], orange_format)
                    
                    if diff_df_formatted.iloc[row_idx]['_ff_red']:
                        worksheet1.write(excel_row, new_ff_col, diff_df.iloc[row_idx]['New FF'], red_format)
                    elif diff_df_formatted.iloc[row_idx]['_ff_orange']:
                        worksheet1.write(excel_row, new_ff_col, diff_df.iloc[row_idx]['New FF'], orange_format)
                    
                    if diff_df_formatted.iloc[row_idx]['_capping_changed']:
                        worksheet1.write(excel_row, new_capping_col, diff_df.iloc[row_idx]['New Capping'], orange_format)
            
            elif comparison_type == 'index':
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
        
        else:
            worksheet1 = workbook.add_worksheet('Differences')
            worksheet1.write(0, 0, 0, normal_format)
            for col_num, value in enumerate(diff_df.columns.values):
                worksheet1.write(2, col_num, value, header_format)
            worksheet1.set_row(3, None, None, {'hidden': True})
        
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
            
            worksheet2.set_row(3, None, None, {'hidden': True})
            
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
            
            worksheet3.set_row(3, None, None, {'hidden': True})
            
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
        
        logger.info(f"Successfully created comparison report: {os.path.basename(output_path)}")
        print(f"Successfully created comparison report: {os.path.basename(output_path)}")
        print(f"Found {len(diff_df)} differences between the files")
        
        return True
        
    except Exception as e:
        logger.error(f"Error writing Excel file: {str(e)}")
        print(f"Error writing Excel file: {str(e)}")
        return False

@timer
def compare_files(file1_path, file2_path, output_path, comparison_type='stock'):
    """Main comparison function with optimizations for large files"""
    try:
        logger.info(f"Starting {comparison_type} file comparison...")
        
        df1 = read_excel_file(file1_path)
        df2 = read_excel_file(file2_path)
        
        if df1 is None or df2 is None:
            logger.error("Failed to read one or both files")
            return False
        
        config = COMPARISON_CONFIGS.get(comparison_type, COMPARISON_CONFIGS['stock'])
        key_fields = config['key_fields']
        
        df1_indexed, df2_indexed = prepare_dataframes(df1, df2, key_fields)
        
        # Call the appropriate comparison function
        comparison_function_name = config['comparison_function']
        if comparison_function_name == 'find_differences_vectorized_morning_stock':
            diff_df, common_keys = find_differences_vectorized_morning_stock(df1_indexed, df2_indexed)
        elif comparison_function_name == 'find_differences_vectorized_morning_index':
            diff_df, common_keys = find_differences_vectorized_morning_index(df1_indexed, df2_indexed)
        else:
            logger.error(f"Unknown comparison function: {comparison_function_name}")
            return False
        
        logger.info(f"Found {len(diff_df)} differences out of {len(common_keys)} common records")
        
        success = write_excel_optimized(diff_df, df1_indexed, df2_indexed, output_path, comparison_type)
        
        if success:
            logger.info(f"Successfully created comparison report: {os.path.basename(output_path)}")
            print(f"Total differences found: {len(diff_df)}")
        
        return success
        
    except Exception as e:
        logger.error(f"Error comparing files: {str(e)}")
        print(f"Error comparing files: {str(e)}")
        return False

def perform_comparison(comparison_type='stock'):
    """Perform the file comparison when files are available"""
    try:
        file_status = check_files_available(comparison_type)
        
        if file_status['available']:
            logger.info(f"{comparison_type.upper()} files are available, starting comparison...")
            
            current_date = datetime.now().strftime("%Y%m%d")
            config = COMPARISON_CONFIGS.get(comparison_type, COMPARISON_CONFIGS['stock'])
            output_filename = config['output_filename'].format(date=current_date)
            output_path = os.path.join(OUTPUT_DIR, output_filename)
            
            start_time = time.time()
            
            success = compare_files(
                file_status['manual_path'], 
                file_status['sod_path'], 
                output_path,
                comparison_type
            )
            
            total_time = time.time() - start_time
            
            if success:
                print(f"{comparison_type.upper()} comparison completed successfully in {total_time:.1f} seconds!")
                print(f"Output saved to: {output_path}")
                logger.info(f"{comparison_type.upper()} file comparison completed successfully in {total_time:.1f} seconds")
                return True
            else:
                print(f"{comparison_type.upper()} comparison failed. Check the log file for details.")
                logger.error(f"{comparison_type.upper()} file comparison failed")
                return False
        else:
            logger.info(f"{comparison_type.upper()} files are not available yet")
            return False
    
    except Exception as e:
        logger.error(f"Error performing {comparison_type} comparison: {str(e)}")
        print(f"Error performing {comparison_type} comparison: {str(e)}")
        return False

def perform_all_comparisons():
    """Perform all configured comparisons"""
    results = {}
    for comparison_type in COMPARISON_CONFIGS.keys():
        logger.info(f"Attempting {comparison_type.upper()} comparison...")
        print(f"\n=== {comparison_type.upper()} Comparison ===")
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
        
        # Determine which comparison type based on filename
        filename = os.path.basename(file_path)
        comparison_type = None
        
        for comp_type, config in COMPARISON_CONFIGS.items():
            if config['file_suffix'] in filename:
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
        print(f"\nLooking for {comparison_type.upper()} files:")
        print(f"  Manual: {expected_files['manual']}")
        print(f"  SOD: {expected_files['sod']}")
    
    print(f"\nFolders:")
    print(f"  Manual: {MONITOR_FOLDERS['manual']}")
    print(f"  SOD: {MONITOR_FOLDERS['sod']}")
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
    
    # Monitor both folders
    try:
        observer.schedule(event_handler, MONITOR_FOLDERS['manual'], recursive=False)
        observer.schedule(event_handler, MONITOR_FOLDERS['sod'], recursive=False)
        
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
        
        print(f"\nExpected {comparison_type.upper()} files:")
        print(f"  Manual: {expected_files['manual']}")
        print(f"  SOD: {expected_files['sod']}")
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
            
            print(f"\nExpected {comp_type.upper()} files:")
            print(f"  Manual: {expected_files['manual']}")
            print(f"  SOD: {expected_files['sod']}")
        
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