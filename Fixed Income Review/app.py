import pandas as pd
import datetime as dt
import os
from dateutil.relativedelta import relativedelta

def process_fixed_income_data(file_path, h16r12_path, cutoff_date_str, effective_date_str):
    try:
        # Parse the cutoff date and effective date
        cutoff_date = pd.to_datetime(cutoff_date_str, format='%Y%m%d')
        effective_date = pd.to_datetime(effective_date_str, format='%Y%m%d')
        
        # Calculate min maturity threshold (1 year after effective date)
        min_maturity_threshold = effective_date + relativedelta(years=1)
        
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
            'Currency',
            'issueDate'  # Added issueDate for MEEUG eligibility check
        ]
        
        # Read main CSV with the known delimiter and encoding
        universe_df = pd.read_csv(file_path, 
                        delimiter=';',
                        encoding='utf-8',
                        usecols=selected_columns)
        
        # Print sample data for debugging
        print("\nSample maturity dates:")
        print(universe_df['maturityDate'].head(10))
        
        # Convert maturityDate to datetime - need to check the format first
        # Try a custom parsing approach
        def parse_date(date_str):
            try:
                # If date is in format YYYYMMDD
                if len(str(date_str)) == 8:
                    year = int(str(date_str)[:4])
                    month = int(str(date_str)[4:6])
                    day = int(str(date_str)[6:8])
                    return pd.Timestamp(year=year, month=month, day=day)
                # Add other formats as needed
                return pd.NaT
            except:
                return pd.NaT
                
        universe_df['maturityDate'] = universe_df['maturityDate'].apply(parse_date)
        universe_df['issueDate'] = universe_df['issueDate'].apply(parse_date)
        
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
        
        # Convert issuedCapital to numeric (removing any commas or other non-numeric characters)
        h16r12_df['issuedCapital'] = pd.to_numeric(h16r12_df['issuedCapital'], errors='coerce')
        
        # Merge the dataframes
        universe_df = universe_df.merge(
            h16r12_df,
            on='bondCode',
            how='left'  # Keep all records from universe_df
        )
        
        # Add a flag for bonds maturing within 1 year of effective date
        universe_df['matures_within_1year'] = (universe_df['maturityDate'] < min_maturity_threshold).astype(int)
        
        # Print maturity date range for debugging
        print("\nMaturity date range after conversion:")
        print("Min maturity date:", universe_df['maturityDate'].min())
        print("Max maturity date:", universe_df['maturityDate'].max())
        print(f"Effective date + 1 year (min threshold): {min_maturity_threshold}")
        print(f"Bonds maturing within 1 year of effective date: {universe_df['matures_within_1year'].sum()}")
        
        # Convert CouponType to string to ensure consistent comparison
        universe_df['CouponType'] = universe_df['CouponType'].astype(str)
        
        # Add Exclusion 1 column
        universe_df['Exclusion 1'] = universe_df['MarketCode'].isin(['EBM', 'TRS']).astype(int)
        
        # Add Exclusion 2 column - excluding everything NOT in these countries
        included_countries = ['AT', 'BE', 'FR', 'DE', 'IE', 'IT', 'ES', 'FI', 'PT', 'NL']
        universe_df['Exclusion 2'] = (~universe_df['issuerCountry'].isin(included_countries)).astype(int)
        
        # Add Exclusion 3 column (1 if NOT equal to GOVT NATIONAL)
        universe_df['Exclusion 3'] = (universe_df['issuerCategory'] != 'GOVT NATIONAL').astype(int)
        
        # Add Exclusion 4 column (1 if Currency is NOT EUR)
        universe_df['Exclusion 4'] = (universe_df['Currency'] != 'EUR').astype(int)
        
        # Add Exclusion 5 column (1 if CouponType is NOT 1)
        # Checking multiple possible representations of "1"
        universe_df['Exclusion 5'] = (~universe_df['CouponType'].isin(['1', '1.0', 1, 1.0])).astype(int)
        
        # Add Exclusion 6 column (1 if issuedCapital <= 2000000000 or NaN)
        universe_df['Exclusion 6'] = ((universe_df['issuedCapital'] <= 2000000000) | 
                                     universe_df['issuedCapital'].isna()).astype(int)
        
        # Define country-specific bond types mapping
        country_bond_types = {
            'AT': ['ATS'],
            'BE': ['OLO'],
            'FI': ['RFG'],
            'FR': ['OAT'],
            'DE': ['DEM'],
            'IE': ['IRL'],
            'IT': ['BTP'],
            'NL': ['DSL'],
            'PT': ['PTE'],
            'ES': ['OBE', 'BON']
        }
        
        # Add Exclusion 7: bond type should match the country's specific types
        def check_bond_type_by_country(row):
            country = row['issuerCountry']
            bond_type = row['bondType']
            
            # If country is in our mapping, check if bond type matches
            if country in country_bond_types:
                return int(bond_type not in country_bond_types[country])
            return 1  # Exclude if country not in mapping
        
        universe_df['Exclusion 7'] = universe_df.apply(check_bond_type_by_country, axis=1)
        
        # Add Total Exclusion column (1 if any exclusion is 1)
        universe_df['Total Exclusion'] = ((universe_df['Exclusion 1'] == 1) | 
                                        (universe_df['Exclusion 2'] == 1) | 
                                        (universe_df['Exclusion 3'] == 1) |
                                        (universe_df['Exclusion 4'] == 1) |
                                        (universe_df['Exclusion 5'] == 1) |
                                        (universe_df['Exclusion 6'] == 1) |
                                        (universe_df['Exclusion 7'] == 1)).astype(int)
        
        # Generate dynamic date ranges based on the cutoff date
        year_1 = cutoff_date + relativedelta(years=1)
        year_3 = cutoff_date + relativedelta(years=3)
        year_5 = cutoff_date + relativedelta(years=5)
        year_7 = cutoff_date + relativedelta(years=7)
        year_10 = cutoff_date + relativedelta(years=10)
        year_15 = cutoff_date + relativedelta(years=15)
        year_25 = cutoff_date + relativedelta(years=25)
        
        # Define index mnemo specifications dynamically
        index_specs = [
            {"Mnemo": "MEBG", "BondType": ['ATS','OLO','DEM','OBE','BON','RFG','OAT','IRL','BTP','DSL','PTE'], 
             "MinMat": year_1, "MaxMat": None},
            {"Mnemo": "ME1G", "BondType": ['ATS','OLO','DEM','OBE','BON','RFG','OAT','IRL','BTP','DSL','PTE'], 
             "MinMat": year_1, "MaxMat": year_3},
            {"Mnemo": "ME3G", "BondType": ['ATS','OLO','DEM','OBE','BON','RFG','OAT','IRL','BTP','DSL','PTE'], 
             "MinMat": year_3, "MaxMat": year_5},
            {"Mnemo": "ME5G", "BondType": ['ATS','OLO','DEM','OBE','BON','RFG','OAT','IRL','BTP','DSL','PTE'], 
             "MinMat": year_5, "MaxMat": year_7},
            {"Mnemo": "ME7G", "BondType": ['ATS','OLO','DEM','OBE','BON','RFG','OAT','IRL','BTP','DSL','PTE'], 
             "MinMat": year_7, "MaxMat": year_10},
            {"Mnemo": "ME10G", "BondType": ['ATS','OLO','DEM','OBE','BON','RFG','OAT','IRL','BTP','DSL','PTE'], 
             "MinMat": year_10, "MaxMat": year_15},
            {"Mnemo": "MEG15", "BondType": ['ATS','OLO','DEM','OBE','BON','RFG','OAT','IRL','BTP','DSL','PTE'], 
             "MinMat": year_15, "MaxMat": None},
            {"Mnemo": "ME15G", "BondType": ['ATS','OLO','DEM','OBE','BON','RFG','OAT','IRL','BTP','DSL','PTE'], 
             "MinMat": year_15, "MaxMat": year_25},
            {"Mnemo": "MEG25", "BondType": ['ATS','OLO','DEM','OBE','BON','RFG','OAT','IRL','BTP','DSL','PTE'], 
             "MinMat": year_25, "MaxMat": None},
            {"Mnemo": "MESPG", "BondType": ['OBE','BON'], 
             "MinMat": year_1, "MaxMat": None},
            {"Mnemo": "MEFRG", "BondType": ['OAT','BTAN'], 
             "MinMat": year_1, "MaxMat": None},
            {"Mnemo": "MEGRG", "BondType": ['DEM'], 
             "MinMat": year_1, "MaxMat": None},
            {"Mnemo": "MEITG", "BondType": ['BTP'], 
             "MinMat": year_1, "MaxMat": None},
            {"Mnemo": "MI1G", "BondType": ['BTP'], 
             "MinMat": year_1, "MaxMat": year_3},
            {"Mnemo": "MI3G", "BondType": ['BTP'], 
             "MinMat": year_3, "MaxMat": year_5},
            {"Mnemo": "MI5G", "BondType": ['BTP'], 
             "MinMat": year_5, "MaxMat": year_7},
            {"Mnemo": "MI7G", "BondType": ['BTP'], 
             "MinMat": year_7, "MaxMat": year_10},
            {"Mnemo": "MI10G", "BondType": ['BTP'], 
             "MinMat": year_10, "MaxMat": year_15},
            {"Mnemo": "MI15G", "BondType": ['BTP'], 
             "MinMat": year_15, "MaxMat": year_25},
            {"Mnemo": "MIG25", "BondType": ['BTP'], 
             "MinMat": year_25, "MaxMat": None},
            {"Mnemo": "MEATG", "BondType": ['ATS'], 
             "MinMat": year_1, "MaxMat": None},
            {"Mnemo": "MEBEG", "BondType": ['OLO'], 
             "MinMat": year_1, "MaxMat": None},
            {"Mnemo": "MESUG", "BondType": ['RFG'], 
             "MinMat": year_1, "MaxMat": None},
            {"Mnemo": "MEIRG", "BondType": ['IRL'], 
             "MinMat": year_1, "MaxMat": None},
            {"Mnemo": "MENLG", "BondType": ['DSL'], 
             "MinMat": year_1, "MaxMat": None},
            {"Mnemo": "MEPTG", "BondType": ['PTE'], 
             "MinMat": year_1, "MaxMat": None},
            # MEEUG index
            {"Mnemo": "MEEUG", "BondType": ['NXG'], 
             "MinMat": min_maturity_threshold, "MaxMat": None, 
             "SpecialCriteria": {
                 "Currency": "EUR",
                 "CouponType": "1",
                 "IssuedCapital": 3000000000,
                 "MaxIssueDate": cutoff_date
             }},
            # New MEUBG index (comprehensive all-bonds index)
            {"Mnemo": "MEUBG", "BondType": ['ATS','OLO','DEM','OBE','BON','RFG','OAT','IRL','BTP','DSL','PTE','NXG'], 
             "MinMat": min_maturity_threshold, "MaxMat": None,
             "SpecialCriteria": {
                 "Currency": "EUR",
                 "CouponType": "1",
                 "IssuedCapital_EU": 3000000000,  # 3 billion for EU bonds
                 "IssuedCapital_Govt": 2000000000,  # 2 billion for government bonds
             }}
        ]
        
        # Print dynamic date ranges for verification
        print(f"\nDynamic date ranges based on cutoff date {cutoff_date.strftime('%Y-%m-%d')}:")
        print(f"1 year: {year_1.strftime('%Y-%m-%d')}")
        print(f"3 years: {year_3.strftime('%Y-%m-%d')}")
        print(f"5 years: {year_5.strftime('%Y-%m-%d')}")
        print(f"7 years: {year_7.strftime('%Y-%m-%d')}")
        print(f"10 years: {year_10.strftime('%Y-%m-%d')}")
        print(f"15 years: {year_15.strftime('%Y-%m-%d')}")
        print(f"25 years: {year_25.strftime('%Y-%m-%d')}")
        
        # Function to check if a bond is eligible for an index
        def check_index_eligibility(row, index_spec):
            # Check if maturity is at least 1 year after effective date for all indices
            if row['matures_within_1year'] == 1:
                return 0
            
            # For MEUBG, use its specific criteria
            if index_spec["Mnemo"] == "MEUBG":
                spec_criteria = index_spec["SpecialCriteria"]
                
                # Check if bond type is eligible
                if row['bondType'] not in index_spec["BondType"]:
                    return 0
                
                # Skip if maturity date is missing
                if pd.isna(row['maturityDate']):
                    return 0
                    
                # Check maturity date criteria
                if row['maturityDate'] < index_spec["MinMat"]:
                    return 0
                    
                if index_spec["MaxMat"] and row['maturityDate'] > index_spec["MaxMat"]:
                    return 0
                
                # Check Currency
                if row['Currency'] != spec_criteria["Currency"]:
                    return 0
                
                # Check CouponType
                if row['CouponType'] != spec_criteria["CouponType"]:
                    return 0
                
                # Check issuedCapital - different thresholds for EU vs govt bonds
                if pd.isna(row['issuedCapital']):
                    return 0
                    
                # EU bonds need 3 billion minimum
                if row['bondType'] == 'NXG' and row['issuedCapital'] < spec_criteria["IssuedCapital_EU"]:
                    return 0
                    
                # Government bonds need 2 billion minimum
                if row['bondType'] != 'NXG' and row['issuedCapital'] < spec_criteria["IssuedCapital_Govt"]:
                    return 0
                
                return 1
                
            # Special criteria for MEEUG
            elif index_spec["Mnemo"] == "MEEUG":
                # Check if bond type is eligible
                if row['bondType'] not in index_spec["BondType"]:
                    return 0
                
                # Check if maturity date meets the criteria
                mat_date = row['maturityDate']
                
                # Skip if maturity date is missing
                if pd.isna(mat_date):
                    return 0
                    
                # Check maturity date criteria
                if mat_date < index_spec["MinMat"]:
                    return 0
                    
                if index_spec["MaxMat"] and mat_date > index_spec["MaxMat"]:
                    return 0
                
                spec_criteria = index_spec["SpecialCriteria"]
                
                # Check Currency
                if row['Currency'] != spec_criteria["Currency"]:
                    return 0
                
                # Check CouponType
                if row['CouponType'] != spec_criteria["CouponType"]:
                    return 0
                
                # Check issuedCapital
                if pd.isna(row['issuedCapital']) or row['issuedCapital'] < spec_criteria["IssuedCapital"]:
                    return 0
                
                # Check issueDate (must be before cutoff date)
                if pd.isna(row['issueDate']) or row['issueDate'] > spec_criteria["MaxIssueDate"]:
                    return 0
                
                return 1
            
            # For all other indices (regular criteria)
            else:
                # If bond is excluded, it's not eligible for any index
                if row['Total Exclusion'] == 1:
                    return 0
                    
                # Check if bond type is eligible
                if row['bondType'] not in index_spec["BondType"]:
                    return 0
                
                # Check if maturity date meets the criteria
                mat_date = row['maturityDate']
                
                # Skip if maturity date is missing
                if pd.isna(mat_date):
                    return 0
                    
                # Check maturity date criteria
                if mat_date < index_spec["MinMat"]:
                    return 0
                    
                if index_spec["MaxMat"] and mat_date > index_spec["MaxMat"]:
                    return 0
                
                return 1
        
        # Add index eligibility columns
        for spec in index_specs:
            col_name = f"EligibleFor_{spec['Mnemo']}"
            universe_df[col_name] = universe_df.apply(lambda row: check_index_eligibility(row, spec), axis=1)
        
        # Print eligibility statistics
        print("\nIndex eligibility statistics:")
        for spec in index_specs:
            col_name = f"EligibleFor_{spec['Mnemo']}"
            eligible = universe_df[col_name].sum()
            print(f"{col_name}: {eligible} out of {len(universe_df)} ({eligible/len(universe_df)*100:.2f}%)")
        
        # Basic data cleaning - only dropping NaN values for key columns
        universe_df = universe_df.dropna(subset=[
            'bondCode', 'bondType', 'description', 'MarketCode',
            'issuerCountry', 'issuerCategory',
            'CouponType', 'Currency'
        ])
        
        return universe_df

    except FileNotFoundError:
        print(f"Error: File not found.")
        return None
    except Exception as e:
        print(f"Error processing file: {str(e)}")
        import traceback
        traceback.print_exc()
        return None

# Usage
if __name__ == "__main__":
    # Get current directory
    current_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Define file paths
    file_path = r"C:\Users\CSonneveld\OneDrive - Euronext\Documents\Projects\Fixed Income Review\Data\DVE_MTS_CMF_20250127_SDP_REFERENCEDATA.csv"
    h16r12_path = r"C:\Users\CSonneveld\OneDrive - Euronext\Documents\Projects\Fixed Income Review\Data\H16R12.20250127.CSV"
    output_file = os.path.join(current_dir, "processed_fixed_income_data.xlsx")
    
    # Define cutoff date and effective date - now you can easily change these for future reviews
    cutoff_date = "20250127"      # Format: YYYYMMDD
    effective_date = "20250131"   # Format: YYYYMMDD
    
    universe_df = process_fixed_income_data(file_path, h16r12_path, cutoff_date, effective_date)
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