import os
import pandas as pd
from datetime import datetime
from pathlib import Path
import re

def find_newest_file_without_value(folder_path, search_value="BEOG", column_name="Mnemo"):
    """
    Find the newest CSV file that does NOT contain the specified value
    in the specified column
    
    Args:
        folder_path: Path to folder containing CSV files
        search_value: Value to check for absence
        column_name: Name of the column to search in
    
    Returns:
        Dict with info about the newest file without the value, or None
    """
    
    folder = Path(folder_path)
    pattern = "TTMIndexEU1_GIS_SOD_INDEX_*.csv"
    csv_files = list(folder.glob(pattern))
    
    if not csv_files:
        print(f"No CSV files found matching pattern '{pattern}' in the specified folder.")
        return None
    
    files_without_value = []
    
    print(f"Checking {len(csv_files)} files for absence of '{search_value}' in '{column_name}' column...")
    
    for file_path in csv_files:
        try:
            # Extract date from filename
            date_match = re.search(r'TTMIndexEU1_GIS_SOD_INDEX_(\d{8})\.csv', file_path.name)
            if not date_match:
                print(f"Could not extract date from filename: {file_path.name}")
                continue
            
            date_string = date_match.group(1)
            file_date = datetime.strptime(date_string, "%Y%m%d")
            
            # Read CSV file and clean column names
            try:
                # First try with semicolon delimiter (common in European CSV files)
                df = pd.read_csv(file_path, delimiter=';')
                # Strip whitespace from column names
                df.columns = df.columns.str.strip()
                
                # If we still have only one column, try comma delimiter
                if len(df.columns) == 1:
                    df = pd.read_csv(file_path, delimiter=',')
                    df.columns = df.columns.str.strip()
                    
            except Exception as e:
                print(f"Error reading {file_path.name}: {e}")
                continue
            
            # Debug: Show available columns for the first file or when column not found
            if column_name not in df.columns:
                print(f"Column '{column_name}' not found in {file_path.name}")
                print(f"Available columns: {list(df.columns)}")
                # Try to find similar column names
                similar_cols = [col for col in df.columns if 'mnemo' in col.lower() or 'index' in col.lower()]
                if similar_cols:
                    print(f"Similar columns found: {similar_cols}")
                continue
            
            # Check if the value is NOT present
            matching_rows = df[df[column_name] == search_value]
            
            if matching_rows.empty:
                # Value not found - this is what we want
                print(f"✓ {file_path.name} does NOT contain '{search_value}'")
                files_without_value.append({
                    'filename': file_path.name,
                    'date_string': date_string,
                    'file_date': file_date,
                    'full_path': str(file_path)
                })
            else:
                print(f"✗ {file_path.name} contains '{search_value}' ({len(matching_rows)} occurrences)")
            
        except Exception as e:
            print(f"Error processing {file_path.name}: {str(e)}")
    
    # Find the newest file without the value
    if files_without_value:
        # Sort by date (newest first)
        files_without_value.sort(key=lambda x: x['file_date'], reverse=True)
        newest_file = files_without_value[0]
        
        print(f"\n=== RESULT ===")
        print(f"Newest file WITHOUT '{search_value}' in '{column_name}' column:")
        print(f"File: {newest_file['filename']}")
        print(f"Date: {newest_file['file_date'].strftime('%Y-%m-%d')}")
        print(f"Full path: {newest_file['full_path']}")
        
        print(f"\nTotal files without '{search_value}': {len(files_without_value)}")
        
        return newest_file
    else:
        print(f"\nAll files contain '{search_value}' in '{column_name}' column.")
        print("No files found without this value.")
        return None

# Usage example
if __name__ == "__main__":
    folder_path = r"\\pbgfshqa08601v\gis_ttm\Archive"  # Change this to your folder path
    
    result = find_newest_file_without_value(folder_path, search_value="BEOG", column_name="Mnemo")
    
    if result:
        print(f"\nThe newest file without 'BEOG' is: {result['filename']}")
    else:
        print("\nNo suitable file found.")

# Required: pip install pandas