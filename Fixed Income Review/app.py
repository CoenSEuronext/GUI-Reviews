import pandas as pd
import datetime as dt
import os

def process_fixed_income_data(file_path, h16r12_path):
    try:
        # Define the columns we want to extract - matching exact names from the file
        selected_columns = [
            'bondCode',
            'bondType', 
            'description',
            'MarketCode',
            'issuerCountry',
            'issuerCategory',
            'maturityDate',
            'CouponType',
            'Currency'
        ]
        
        # Read main CSV with the known delimiter and encoding
        universe_df = pd.read_csv(file_path, 
                        delimiter=';',
                        encoding='utf-8',
                        usecols=selected_columns)
        
        # Read H16R12 file
        h16r12_df = pd.read_csv(h16r12_path, 
                               encoding='utf-8')  # First read all columns
        
        # Clean column names by stripping whitespace and removing quotes
        h16r12_df.columns = h16r12_df.columns.str.strip().str.replace('"', '')
        
        # Select and rename the required columns
        h16r12_df = h16r12_df[[
            'Official ISIN (RS17)',  # This column contains the ISIN
            'Issued Capital (A7)'    # This column contains the issued capital
        ]]
        
        # Rename columns to make merging easier
        h16r12_df = h16r12_df.rename(columns={
            'Official ISIN (RS17)': 'bondCode',
            'Issued Capital (A7)': 'issuedCapital'
        })
        
        # Merge the dataframes
        universe_df = universe_df.merge(
            h16r12_df,
            on='bondCode',
            how='left'  # Keep all records from universe_df
        )
        
        # Add Exclusion 1 column
        universe_df['Exclusion 1'] = universe_df['MarketCode'].isin(['EBM', 'TRS']).astype(int)
        
        # Add Exclusion 2 column
        included_countries = ['AT', 'BE', 'FR', 'DE', 'IE', 'IT', 'ES', 'FI', 'PT', 'NL']
        universe_df['Exclusion 2'] = (~universe_df['issuerCountry'].isin(included_countries)).astype(int)
        
        # Add Exclusion 3 column (1 if NOT equal to GOVT NATIONAL)
        universe_df['Exclusion 3'] = (universe_df['issuerCategory'] != 'GOVT NATIONAL').astype(int)
        
        # Add Total Exclusion column (1 if any exclusion is 1)
        universe_df['Total Exclusion'] = ((universe_df['Exclusion 1'] == 1) | 
                                        (universe_df['Exclusion 2'] == 1) | 
                                        (universe_df['Exclusion 3'] == 1)).astype(int)
        
        
        return universe_df

    except FileNotFoundError:
        print(f"Error: File not found.")
        return None
    except Exception as e:
        print(f"Error processing file: {str(e)}")
        return None

# Usage
if __name__ == "__main__":
    # Get current directory
    current_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Define file paths
    file_path = r"C:\Users\CSonneveld\OneDrive - Euronext\Documents\Projects\Fixed Income Review\Data\DVE_MTS_CMF_20250127_SDP_REFERENCEDATA.csv"
    h16r12_path = r"C:\Users\CSonneveld\OneDrive - Euronext\Documents\Projects\Fixed Income Review\Data\H16R12.20250127.CSV"
    output_file = os.path.join(current_dir, "processed_fixed_income_data.xlsx")
    
    universe_df = process_fixed_income_data(file_path, h16r12_path)
    if universe_df is not None:        
        try:
            # Export processed data to an Excel file
            universe_df.to_excel(output_file, index=False, sheet_name='Universe')
            print(f"File saved successfully to: {output_file}")
            
            # Open the processed file with default application
            if os.path.exists(output_file):
                os.startfile(output_file)
                print(f"\nOpening {output_file}...")
            else:
                print(f"Error: Output file not found at {output_file}")
                
        except Exception as e:
            print(f"Error saving or opening file: {e}")