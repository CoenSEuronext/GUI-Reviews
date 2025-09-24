import pandas as pd
import datetime as dt
import os
from dateutil.relativedelta import relativedelta
import glob

def process_inflation_linked_indices(universe_df, selection_df, effective_date_str, cutoff_date_str):
    """
    Process inflation-linked government bond index (MGILG) based on the rulebook.
    
    Args:
        universe_df: DataFrame containing the full universe of bonds
        selection_df: DataFrame containing the filtered selection with other indices
        effective_date_str: String representing the effective date (YYYYMMDD)
        cutoff_date_str: String representing the cutoff date (YYYYMMDD)
        
    Returns:
        Updated selection_df with inflation-linked index eligibility column
    """
    import pandas as pd
    from dateutil.relativedelta import relativedelta
    
    # Parse the dates
    effective_date = pd.to_datetime(effective_date_str, format='%Y%m%d')
    cutoff_date = pd.to_datetime(cutoff_date_str, format='%Y%m%d')
    
    # Calculate maturity threshold
    one_year_after = effective_date + relativedelta(years=1)
    
    print(f"\nProcessing Inflation-Linked Government Bond Index:")
    print(f"Maturity threshold: >= {one_year_after.strftime('%Y-%m-%d')}")
    
    # Define inflation-linked bond types by country mapping
    inflation_linked_types_by_country = {
        'FR': ['OAI'],         # France
        'DE': ['GIL'],         # Germany
        'IT': ['BTi'],         # Italy
        'ES': ['SIL', 'BNI']   # Spain
    }
    
    # Create a copy of universe_df to work with
    inflation_linked_df = universe_df.copy()
    
    # Initial screening for inflation-linked bonds
    # 1. Check if bond type matches country's specific inflation-linked types
    def check_inflation_linked_type(row):
        country = row['issuerCountry']
        bond_type = row['bondType']
        
        if country in inflation_linked_types_by_country:
            return bond_type in inflation_linked_types_by_country[country]
        return False
    
    inflation_linked_df['is_valid_inflation_linked_type'] = inflation_linked_df.apply(check_inflation_linked_type, axis=1)
    
    # Function to check inflation-linked bond eligibility
    def check_inflation_linked_eligibility(row):
        # Skip if not a valid inflation-linked bond type
        if not row['is_valid_inflation_linked_type']:
            return 0
            
        # Currency check - must be EUR
        if row['Currency'] != 'EUR':
            return 0
            
        # Bond type check - must be fixed coupon (1)
        if row['CouponType'] not in ['3', '3.0', 3, 3.0]:
            return 0
            
        # Amount outstanding check - must be ≥ 2 billion EUR
        if pd.isna(row['issuedCapital']) or row['issuedCapital'] < 2000000000:
            return 0
            
        # Issue date check - must be on or before cutoff date
        if pd.isna(row['issueDate']) or row['issueDate'] > cutoff_date:
            return 0
            
        # Maturity check - must be at least 1 year from effective date
        mat_date = row['maturityDate']
        if pd.isna(mat_date) or mat_date < one_year_after:
            return 0
            
        return 1
    
    # Apply eligibility check for the inflation-linked index
    inflation_linked_df['EligibleFor_MGILG'] = inflation_linked_df.apply(check_inflation_linked_eligibility, axis=1)
    
    # Print inflation-linked index eligibility statistics
    mgilg_eligible = inflation_linked_df['EligibleFor_MGILG'].sum()
    
    print(f"\nInflation-linked index eligibility statistics:")
    print(f"EligibleFor_MGILG (Euro Government Inflation-Linked): {mgilg_eligible} out of {len(inflation_linked_df)} ({mgilg_eligible/len(inflation_linked_df)*100:.2f}%)")
    
    # Copy eligible inflation-linked bonds to selection_df
    for _, bond in inflation_linked_df.iterrows():
        if bond['EligibleFor_MGILG'] == 1:
            # Check if this bond is already in selection_df
            if bond['bondCode'] in selection_df['bondCode'].values:
                # Update existing row
                mask = selection_df['bondCode'] == bond['bondCode']
                selection_df.loc[mask, 'EligibleFor_MGILG'] = bond['EligibleFor_MGILG']
            else:
                # Add bond specific eligibility
                new_row = bond.copy()
                # Add columns that exist in selection_df but not in inflation_linked_df with default values
                for col in selection_df.columns:
                    if col not in new_row and col.startswith('EligibleFor_'):
                        new_row[col] = 0
                
                # Add exclusion columns if not present
                exclusion_cols = ['Exclusion 4', 'Exclusion 5', 'Exclusion 6', 'Exclusion 7', 'Total Exclusion']
                for col in exclusion_cols:
                    if col not in new_row:
                        new_row[col] = 1  # Mark as excluded for regular bond indices
                
                # Add to selection_df
                selection_df = pd.concat([selection_df, pd.DataFrame([new_row])], ignore_index=True)
    
    # Ensure all instruments in selection_df have explicit zeros for non-eligible MGILG
    # Create column if it doesn't exist
    if 'EligibleFor_MGILG' not in selection_df.columns:
        selection_df['EligibleFor_MGILG'] = 0
    else:
        # Fill NaN values with 0
        selection_df['EligibleFor_MGILG'] = selection_df['EligibleFor_MGILG'].fillna(0).astype(int)
    
    return selection_df

def process_government_bill_indices(universe_df, selection_df, effective_date_str, cutoff_date_str):
    """
    Process government bill indices (MEGBG, MIGBG, MFGBG) based on the rulebook.
    
    Args:
        universe_df: DataFrame containing the full universe of bonds/bills
        selection_df: DataFrame containing the filtered selection with other indices
        effective_date_str: String representing the effective date (YYYYMMDD)
        cutoff_date_str: String representing the cutoff date (YYYYMMDD)
        
    Returns:
        Updated selection_df with bill indices eligibility columns
    """
    # Parse the dates
    effective_date = pd.to_datetime(effective_date_str, format='%Y%m%d')
    cutoff_date = pd.to_datetime(cutoff_date_str, format='%Y%m%d')
    
    # Calculate maturity thresholds
    one_month_after = effective_date + relativedelta(months=1)
    one_year_after = effective_date + relativedelta(years=1)
    
    print(f"\nProcessing Government Bill Indices:")
    print(f"Maturity range: >= {one_month_after.strftime('%Y-%m-%d')} and < {one_year_after.strftime('%Y-%m-%d')}")
    
    # Define bill type by country mapping
    bill_type_by_country = {
        'AT': ['ATB'],  # Austria
        'BE': ['BTC'],  # Belgium
        'FI': ['RFT'],  # Finland
        'FR': ['FTB'],  # France
        'DE': ['GTC'],  # Germany
        'IE': ['ITB'],  # Ireland
        'IT': ['BOT'],  # Italy
        'NL': ['DTC'],  # Netherlands
        'PT': ['PTC'],  # Portugal
        'ES': ['LET']   # Spain
    }
    
    # Create a copy of universe_df to work with
    bills_df = universe_df.copy()
    
    # Initial screening for bills
    # 1. Check if bill type matches country's specific bill types
    def check_bill_type(row):
        country = row['issuerCountry']
        bond_type = row['bondType']
        
        if country in bill_type_by_country:
            return bond_type in bill_type_by_country[country]
        return False
    
    bills_df['is_valid_bill_type'] = bills_df.apply(check_bill_type, axis=1)
    
    # Function to check bill eligibility
    def check_bill_eligibility(row, country_filter=None):
        # Skip if not a valid bill type
        if not row['is_valid_bill_type']:
            return 0
            
        # Currency check - must be EUR
        if row['Currency'] != 'EUR':
            return 0
            
        # Bond type check - must be fixed coupon (1)
        if row['CouponType'] not in ['0', '0.0', 0, 0.0]:
            return 0
            
        # Amount outstanding check - must be ≥ 2 billion EUR
        if pd.isna(row['issuedCapital']) or row['issuedCapital'] < 2000000000:
            return 0
            
        # Issue date check - must be on or before cutoff date
        if pd.isna(row['issueDate']) or row['issueDate'] > cutoff_date:
            return 0
            
        # Maturity check - between 1 month and 1 year from effective date
        mat_date = row['maturityDate']
        if pd.isna(mat_date) or mat_date < one_month_after or mat_date >= one_year_after:
            return 0
            
        # Country check for specific indices
        if country_filter and row['issuerCountry'] != country_filter:
            return 0
            
        return 1
    
    # Apply eligibility checks for each bill index
    # Euro Government Bill Index (all eligible countries)
    bills_df['EligibleFor_MEGBG'] = bills_df.apply(
        lambda row: check_bill_eligibility(row), axis=1
    )
    
    # Italy Government Bill Index (Italy only)
    bills_df['EligibleFor_MIGBG'] = bills_df.apply(
        lambda row: check_bill_eligibility(row, country_filter='IT'), axis=1
    )
    
    # France Government Bill Index (France only)
    bills_df['EligibleFor_MFGBG'] = bills_df.apply(
        lambda row: check_bill_eligibility(row, country_filter='FR'), axis=1
    )
    
    # Print bill index eligibility statistics
    megbg_eligible = bills_df['EligibleFor_MEGBG'].sum()
    migbg_eligible = bills_df['EligibleFor_MIGBG'].sum()
    mfgbg_eligible = bills_df['EligibleFor_MFGBG'].sum()
    
    print(f"\nBill index eligibility statistics:")
    print(f"EligibleFor_MEGBG (Euro Government Bill): {megbg_eligible} out of {len(bills_df)} ({megbg_eligible/len(bills_df)*100:.2f}%)")
    print(f"EligibleFor_MIGBG (Italy Government Bill): {migbg_eligible} out of {len(bills_df)} ({migbg_eligible/len(bills_df)*100:.2f}%)")
    print(f"EligibleFor_MFGBG (France Government Bill): {mfgbg_eligible} out of {len(bills_df)} ({mfgbg_eligible/len(bills_df)*100:.2f}%)")
    
    # Copy eligible bills to selection_df
    for _, bill in bills_df.iterrows():
        if bill['EligibleFor_MEGBG'] == 1 or bill['EligibleFor_MIGBG'] == 1 or bill['EligibleFor_MFGBG'] == 1:
            # Check if this bill is already in selection_df
            if bill['bondCode'] in selection_df['bondCode'].values:
                # Update existing row
                mask = selection_df['bondCode'] == bill['bondCode']
                selection_df.loc[mask, 'EligibleFor_MEGBG'] = bill['EligibleFor_MEGBG']
                selection_df.loc[mask, 'EligibleFor_MIGBG'] = bill['EligibleFor_MIGBG']
                selection_df.loc[mask, 'EligibleFor_MFGBG'] = bill['EligibleFor_MFGBG']
            else:
                # Add bill specific eligibility
                new_row = bill.copy()
                # Add columns that exist in selection_df but not in bills_df with default values
                for col in selection_df.columns:
                    if col not in new_row and col.startswith('EligibleFor_'):
                        new_row[col] = 0
                
                # Add exclusion columns if not present
                exclusion_cols = ['Exclusion 4', 'Exclusion 5', 'Exclusion 6', 'Exclusion 7', 'Total Exclusion']
                for col in exclusion_cols:
                    if col not in new_row:
                        new_row[col] = 1  # Mark as excluded for bond indices
                
                # Add to selection_df
                selection_df = pd.concat([selection_df, pd.DataFrame([new_row])], ignore_index=True)
    
    # Ensure all bill index columns exist in selection_df
    for col in ['EligibleFor_MEGBG', 'EligibleFor_MIGBG', 'EligibleFor_MFGBG']:
        if col not in selection_df.columns:
            selection_df[col] = 0
    # Ensure all bill index columns exist in selection_df with explicit zeros (not NaN)
    for col in ['EligibleFor_MEGBG', 'EligibleFor_MIGBG', 'EligibleFor_MFGBG']:
        if col not in selection_df.columns:
            selection_df[col] = 0
        else:
            # Fill NaN values with 0
            selection_df[col] = selection_df[col].fillna(0).astype(int)
    
    return selection_df

def process_fixed_income_data(cmf_file_path_pattern, bv_file_path_pattern, h16r12_path, cutoff_date_str, effective_date_str):
    try:
        import glob
        import pandas as pd
        from dateutil.relativedelta import relativedelta
        
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
        
        # Find matching files for CMF and BV patterns
        cmf_files = glob.glob(cmf_file_path_pattern)
        bv_files = glob.glob(bv_file_path_pattern)
        
        if not cmf_files:
            raise FileNotFoundError(f"No files found matching pattern: {cmf_file_path_pattern}")
        
        print(f"Found {len(cmf_files)} CMF files: {cmf_files}")
        print(f"Found {len(bv_files)} BV files: {bv_files}")
        
        # Read CMF file(s) - use the first one if multiple files match
        cmf_file = cmf_files[0]
        print(f"\nReading primary file (CMF): {cmf_file}")
        cmf_df = pd.read_csv(cmf_file, 
                             delimiter=';',
                             encoding='utf-8',
                             usecols=selected_columns)
        
        print(f"Primary file (CMF) record count: {len(cmf_df)}")
        
        # Check if we have BV files to merge
        if bv_files:
            bv_file = bv_files[0]
            print(f"\nReading secondary file (BV): {bv_file}")
            bv_df = pd.read_csv(bv_file,
                               delimiter=';',
                               encoding='utf-8',
                               usecols=selected_columns)
            
            print(f"Secondary file (BV) record count: {len(bv_df)}")
            
            # Track record counts for reporting
            cmf_records = len(cmf_df)
            bv_records = len(bv_df)
            
            # Create a set of bond codes from CMF file to check for duplicates
            cmf_bond_codes = set(cmf_df['bondCode'])
            
            # Filter BV dataframe to include only bonds not in CMF
            bv_df_unique = bv_df[~bv_df['bondCode'].isin(cmf_bond_codes)]
            
            print(f"Unique records from secondary file (BV): {len(bv_df_unique)} (excluding {bv_records - len(bv_df_unique)} duplicates)")
            
            # Concatenate the dataframes (CMF first, then unique BV records)
            universe_df = pd.concat([cmf_df, bv_df_unique], ignore_index=True)
            
            print(f"Combined dataset record count: {len(universe_df)}")
            print(f"Number of unique bond codes: {universe_df['bondCode'].nunique()}")
        else:
            # Just use the CMF file if no BV files
            universe_df = cmf_df
            print("\nNo secondary (BV) files found. Proceeding with primary file only.")
        
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
        
        # Create Initial Exclusion column for universe_df
        universe_df['Initial Exclusion'] = ((universe_df['Exclusion 1'] == 1) | 
                                         (universe_df['Exclusion 2'] == 1) | 
                                         (universe_df['Exclusion 3'] == 1)).astype(int)
        
        # First create a copy of the entire universe for MEEUG and MEUBG processing
        eu_bonds_df = universe_df.copy()
        
        # Filter universe based on first 3 exclusions to create selection dataframe
        selection_df = universe_df[universe_df['Initial Exclusion'] == 0].copy()
        
        # Print information about the initial filtering
        print(f"\nInitial universe size: {len(universe_df)} bonds")
        print(f"Bonds excluded by initial criteria: {universe_df['Initial Exclusion'].sum()}")
        print(f"Selection dataframe size after initial filtering: {len(selection_df)} bonds")
        
        # Continue with additional exclusions on the filtered selection_df
        # Add Exclusion 4 column (1 if Currency is NOT EUR)
        selection_df['Exclusion 4'] = (selection_df['Currency'] != 'EUR').astype(int)
        
        # Add Exclusion 5 column (1 if CouponType is NOT 1)
        # Checking multiple possible representations of "1"
        selection_df['Exclusion 5'] = (~selection_df['CouponType'].isin(['1', '1.0', 1, 1.0])).astype(int)
        
        # Add Exclusion 6 column (1 if issuedCapital <= 2000000000 or NaN)
        selection_df['Exclusion 6'] = ((selection_df['issuedCapital'] <= 2000000000) | 
                                     selection_df['issuedCapital'].isna()).astype(int)
        
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
        
        selection_df['Exclusion 7'] = selection_df.apply(check_bond_type_by_country, axis=1)
        
        # Add Total Exclusion column (1 if any exclusion is 1)
        selection_df['Total Exclusion'] = ((selection_df['Exclusion 4'] == 1) | 
                                        (selection_df['Exclusion 5'] == 1) |
                                        (selection_df['Exclusion 6'] == 1) |
                                        (selection_df['Exclusion 7'] == 1)).astype(int)
        
        # Print exclusion statistics for the filtered selection
        print("\nExclusion statistics in selection dataframe:")
        for col in ['Exclusion 4', 'Exclusion 5', 'Exclusion 6', 'Exclusion 7', 'Total Exclusion']:
            excluded = selection_df[col].sum()
            print(f"{col}: {excluded} out of {len(selection_df)} ({excluded/len(selection_df)*100:.2f}%)")
        
        # Generate dynamic date ranges based on the effective date
        year_1 = effective_date + relativedelta(years=1)
        year_3 = effective_date + relativedelta(years=3)
        year_5 = effective_date + relativedelta(years=5)
        year_7 = effective_date + relativedelta(years=7)
        year_10 = effective_date + relativedelta(years=10)
        year_15 = effective_date + relativedelta(years=15)
        year_25 = effective_date + relativedelta(years=25)
        
        year_3_e = effective_date + relativedelta(years=3, days=-1)
        year_5_e = effective_date + relativedelta(years=5, days=-1)
        year_7_e = effective_date + relativedelta(years=7, days=-1)
        year_10_e = effective_date + relativedelta(years=10, days=-1)
        year_15_e = effective_date + relativedelta(years=15, days=-1)
        year_25_e = effective_date + relativedelta(years=25, days=-1)
        
        
        # Define index mnemo specifications dynamically
        index_specs = [
            {"Mnemo": "MEBG", "BondType": ['ATS','OLO','DEM','OBE','BON','RFG','OAT','IRL','BTP','DSL','PTE'], 
             "MinMat": year_1, "MaxMat": None},
            {"Mnemo": "ME1G", "BondType": ['ATS','OLO','DEM','OBE','BON','RFG','OAT','IRL','BTP','DSL','PTE'], 
             "MinMat": year_1, "MaxMat": year_3_e},
            {"Mnemo": "ME3G", "BondType": ['ATS','OLO','DEM','OBE','BON','RFG','OAT','IRL','BTP','DSL','PTE'], 
             "MinMat": year_3, "MaxMat": year_5_e},
            {"Mnemo": "ME5G", "BondType": ['ATS','OLO','DEM','OBE','BON','RFG','OAT','IRL','BTP','DSL','PTE'], 
             "MinMat": year_5, "MaxMat": year_7_e},
            {"Mnemo": "ME7G", "BondType": ['ATS','OLO','DEM','OBE','BON','RFG','OAT','IRL','BTP','DSL','PTE'], 
             "MinMat": year_7, "MaxMat": year_10_e},
            {"Mnemo": "ME10G", "BondType": ['ATS','OLO','DEM','OBE','BON','RFG','OAT','IRL','BTP','DSL','PTE'], 
             "MinMat": year_10, "MaxMat": year_15_e},
            {"Mnemo": "MEG15", "BondType": ['ATS','OLO','DEM','OBE','BON','RFG','OAT','IRL','BTP','DSL','PTE'], 
             "MinMat": year_15, "MaxMat": None},
            {"Mnemo": "ME15G", "BondType": ['ATS','OLO','DEM','OBE','BON','RFG','OAT','IRL','BTP','DSL','PTE'], 
             "MinMat": year_15, "MaxMat": year_25_e},
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
             "MinMat": year_1, "MaxMat": year_3_e},
            {"Mnemo": "MI3G", "BondType": ['BTP'], 
             "MinMat": year_3, "MaxMat": year_5_e},
            {"Mnemo": "MI5G", "BondType": ['BTP'], 
             "MinMat": year_5, "MaxMat": year_7_e},
            {"Mnemo": "MI7G", "BondType": ['BTP'], 
             "MinMat": year_7, "MaxMat": year_10_e},
            {"Mnemo": "MI10G", "BondType": ['BTP'], 
             "MinMat": year_10, "MaxMat": year_15_e},
            {"Mnemo": "MI15G", "BondType": ['BTP'], 
             "MinMat": year_15, "MaxMat": year_25_e},
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
             "MinMat": year_1, "MaxMat": None}
        ]
        
        # Print dynamic date ranges for verification
        print(f"\nDynamic date ranges based on effective date {effective_date.strftime('%Y-%m-%d')}:")
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
            
            # For all regular indices
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
            
            # For regular indices (not special ones), check Total Exclusion
            if row['Total Exclusion'] == 1:
                return 0
                
            return 1
        
        # Add index eligibility columns for regular indices
        for spec in index_specs:
            col_name = f"EligibleFor_{spec['Mnemo']}"
            selection_df[col_name] = selection_df.apply(lambda row: check_index_eligibility(row, spec), axis=1)
        
        # Function for MEEUG eligibility check
        def check_meeug_eligibility(row):
            # Check if maturity is at least 1 year after effective date
            if row['matures_within_1year'] == 1:
                return 0
                
            # Check if bond type is eligible (NXG for EU bonds)
            if row['bondType'] != 'NXG':
                return 0
            
            # Check if maturity date meets the criteria
            mat_date = row['maturityDate']
            
            # Skip if maturity date is missing
            if pd.isna(mat_date):
                return 0
                
            # Check maturity date criteria
            if mat_date < min_maturity_threshold:
                return 0
            
            # Check Currency
            if row['Currency'] != 'EUR':
                return 0
            
            # Check CouponType
            if row['CouponType'] != '1':
                return 0
            
            # Check issuedCapital
            if pd.isna(row['issuedCapital']) or row['issuedCapital'] < 3000000000:
                return 0
            
            # Check issueDate (must be before cutoff date)
            if pd.isna(row['issueDate']) or row['issueDate'] > cutoff_date:
                return 0
            
            return 1
        
        # Function for MEUBG eligibility check
        def check_meubg_eligibility(row):
            # Check if maturity is at least 1 year after effective date
            if row['matures_within_1year'] == 1:
                return 0
                
            # Check if bond type is eligible
            valid_bond_types = ['ATS','OLO','DEM','OBE','BON','RFG','OAT','IRL','BTP','DSL','PTE','NXG']
            if row['bondType'] not in valid_bond_types:
                return 0
            
            # Check if maturity date meets the criteria
            mat_date = row['maturityDate']
            
            # Skip if maturity date is missing
            if pd.isna(mat_date):
                return 0
                
            # Check maturity date criteria
            if mat_date < min_maturity_threshold:
                return 0
            
            # Check Currency
            if row['Currency'] != 'EUR':
                return 0
            
            # Check CouponType
            if row['CouponType'] != '1':
                return 0
            
            # Check issuedCapital - different thresholds for EU vs govt bonds
            if pd.isna(row['issuedCapital']):
                return 0
                
            # EU bonds need 3 billion minimum
            if row['bondType'] == 'NXG' and row['issuedCapital'] < 3000000000:
                return 0
                
            # Government bonds need 2 billion minimum
            if row['bondType'] != 'NXG' and row['issuedCapital'] < 2000000000:
                return 0
            
            return 1
        
        # Apply MEEUG and MEUBG eligibility checks to the whole universe
        eu_bonds_df['EligibleFor_MEEUG'] = eu_bonds_df.apply(check_meeug_eligibility, axis=1)
        eu_bonds_df['EligibleFor_MEUBG'] = eu_bonds_df.apply(check_meubg_eligibility, axis=1)
        
        # Print EU index eligibility statistics
        meeug_eligible = eu_bonds_df['EligibleFor_MEEUG'].sum()
        meubg_eligible = eu_bonds_df['EligibleFor_MEUBG'].sum()
        print(f"\nEU-specific index statistics:")
        print(f"EligibleFor_MEEUG: {meeug_eligible} out of {len(eu_bonds_df)} ({meeug_eligible/len(eu_bonds_df)*100:.2f}%)")
        print(f"EligibleFor_MEUBG: {meubg_eligible} out of {len(eu_bonds_df)} ({meubg_eligible/len(eu_bonds_df)*100:.2f}%)")
        
        # Print regular index eligibility statistics
        print("\nRegular index eligibility statistics:")
        for spec in index_specs:
            col_name = f"EligibleFor_{spec['Mnemo']}"
            if col_name not in ['EligibleFor_MEEUG', 'EligibleFor_MEUBG']:  # Skip the special indices
                eligible = selection_df[col_name].sum()
                print(f"{col_name}: {eligible} out of {len(selection_df)} ({eligible/len(selection_df)*100:.2f}%)")
        
        # Drop any rows with missing essential data
        selection_df = selection_df.dropna(subset=[
            'bondCode', 'bondType', 'description', 'MarketCode',
            'issuerCountry', 'issuerCategory',
            'CouponType', 'Currency'
        ])
        
        # Copy EU-specific index eligibility back to selection_df
        for bond_code in selection_df['bondCode']:
            if bond_code in eu_bonds_df['bondCode'].values:
                # Get the eligibility values from eu_bonds_df
                eu_mask = eu_bonds_df['bondCode'] == bond_code
                meeug_eligible = eu_bonds_df.loc[eu_mask, 'EligibleFor_MEEUG'].values[0]
                meubg_eligible = eu_bonds_df.loc[eu_mask, 'EligibleFor_MEUBG'].values[0]
                
                # Update selection_df
                sel_mask = selection_df['bondCode'] == bond_code
                selection_df.loc[sel_mask, 'EligibleFor_MEEUG'] = meeug_eligible
                selection_df.loc[sel_mask, 'EligibleFor_MEUBG'] = meubg_eligible
        
        # Process Government Bill Indices
        print("\nProcessing Government Bill indices (MEGBG, MIGBG, MFGBG)...")
        selection_df = process_government_bill_indices(universe_df, selection_df, effective_date_str, cutoff_date_str)
        
        # Process Inflation-Linked Index
        print("\nProcessing Inflation-Linked Government Bond Index (MGILG)...")
        selection_df = process_inflation_linked_indices(universe_df, selection_df, effective_date_str, cutoff_date_str)
        
        # For EU bonds not in selection_df but eligible for MEEUG or MEUBG, add them to selection_df
        for _, eu_bond in eu_bonds_df.iterrows():
            if eu_bond['bondType'] == 'NXG' and (eu_bond['EligibleFor_MEEUG'] == 1 or eu_bond['EligibleFor_MEUBG'] == 1):
                if eu_bond['bondCode'] not in selection_df['bondCode'].values:
                    # Need to add this EU bond to selection_df
                    # First calculate all exclusions
                    eu_bond['Exclusion 4'] = 1 if eu_bond['Currency'] != 'EUR' else 0
                    eu_bond['Exclusion 5'] = 1 if eu_bond['CouponType'] not in ['1', '1.0', 1, 1.0] else 0
                    eu_bond['Exclusion 6'] = 1 if (eu_bond['issuedCapital'] <= 2000000000 or pd.isna(eu_bond['issuedCapital'])) else 0
                    eu_bond['Exclusion 7'] = 0  # NXG bonds don't follow country-specific bond types
                    
                    # Add Total Exclusion
                    eu_bond['Total Exclusion'] = 1 if (eu_bond['Exclusion 4'] == 1 or 
                                               eu_bond['Exclusion 5'] == 1 or
                                               eu_bond['Exclusion 6'] == 1 or
                                               eu_bond['Exclusion 7'] == 1) else 0
                    
                    # Set all other index eligibility to 0
                    for spec in index_specs:
                        col_name = f"EligibleFor_{spec['Mnemo']}"
                        if col_name not in ['EligibleFor_MEEUG', 'EligibleFor_MEUBG']:
                            eu_bond[col_name] = 0
                    
                    # Set bill indices and inflation-linked index to 0
                    for col in ['EligibleFor_MEGBG', 'EligibleFor_MIGBG', 'EligibleFor_MFGBG', 'EligibleFor_MGILG']:
                        eu_bond[col] = 0
                    
                    # Add to selection_df
                    selection_df = pd.concat([selection_df, pd.DataFrame([eu_bond])], ignore_index=True)
        
        # Simplify universe_df to only include essential columns
        essential_columns = [
            'bondCode',
            'bondType', 
            'description',
            'MarketCode',
            'issuerCountry',
            'issuerCategory',
            'maturityDate',
            'CouponType',
            'Currency',
            'issueDate',
            'issuedCapital',
            'Exclusion 1',
            'Exclusion 2',
            'Exclusion 3',
            'Initial Exclusion'
        ]
        
        universe_df = universe_df[essential_columns]
        
        # Ensure all column values are filled with zeros not NaN
        for col in selection_df.columns:
            if col.startswith('EligibleFor_'):
                selection_df[col] = selection_df[col].fillna(0).astype(int)
        
        # Final output statistics
        print(f"\nProcessing complete!")
        print(f"Universe size: {len(universe_df)} instruments")
        print(f"Selection size: {len(selection_df)} instruments")
        
        # Count eligible instruments for all indices
        eligible_counts = {}
        for col in selection_df.columns:
            if col.startswith('EligibleFor_'):
                index_name = col.replace('EligibleFor_', '')
                count = selection_df[col].sum()
                eligible_counts[index_name] = count
        
        print("\nEligible instrument counts by index:")
        for index_name, count in sorted(eligible_counts.items()):
            print(f"{index_name}: {count} instruments")
                
        return universe_df, selection_df, eu_bonds_df
    
    except FileNotFoundError as e:
        print(f"Error: {str(e)}")
        return None, None, None
    except Exception as e:
        print(f"Error processing file: {str(e)}")
        import traceback
        traceback.print_exc()
        return None, None, None
    
def compare_constituents(calculated_df, reference_file_path):
    try:
        # Read the reference file
        reference_df = pd.read_csv(reference_file_path)
        
        print("\nComparing constituents with reference file...")
        print(f"Reference file: {os.path.basename(reference_file_path)}")
        
        # Get all index columns (excluding the ISIN column)
        index_columns = [col for col in calculated_df.columns if col != 'ISIN']
        
        # Create a dictionary to store comparison results
        comparison_results = {}
        
        # Compare each index
        for index_name in index_columns:
            # Get ISINs where the index value is 1 in calculated data
            calc_isins = set(calculated_df[calculated_df[index_name] == 1]['ISIN'])
            
            # Get ISINs where the index value is 1 in reference data
            if index_name in reference_df.columns:
                ref_isins = set(reference_df[reference_df[index_name] == 1]['ISIN'])
                
                # Calculate differences
                only_in_calc = calc_isins - ref_isins
                only_in_ref = ref_isins - calc_isins
                
                # Store results
                comparison_results[index_name] = {
                    'calc_count': len(calc_isins),
                    'ref_count': len(ref_isins),
                    'match_count': len(calc_isins.intersection(ref_isins)),
                    'only_in_calc': only_in_calc,
                    'only_in_ref': only_in_ref,
                    'match_percentage': len(calc_isins.intersection(ref_isins)) / max(len(calc_isins.union(ref_isins)), 1) * 100
                }
            else:
                print(f"Warning: Index {index_name} not found in reference file")
        
        # Create comparison summary dataframe
        summary_data = []
        for index_name, result in comparison_results.items():
            summary_data.append({
                'Index': index_name,
                'Calculated_Count': result['calc_count'],
                'Reference_Count': result['ref_count'],
                'Matching_Count': result['match_count'],
                'Only_in_Calculated': len(result['only_in_calc']),
                'Only_in_Reference': len(result['only_in_ref']),
                'Match_Percentage': result['match_percentage']
            })
        
        summary_df = pd.DataFrame(summary_data)
        
        # Create detailed differences dataframes
        differences_by_index = {}
        for index_name, result in comparison_results.items():
            if result['only_in_calc'] or result['only_in_ref']:
                diff_data = []
                
                for isin in result['only_in_calc']:
                    diff_data.append({'ISIN': isin, 'Source': 'Calculated Only'})
                
                for isin in result['only_in_ref']:
                    diff_data.append({'ISIN': isin, 'Source': 'Reference Only'})
                
                differences_by_index[index_name] = pd.DataFrame(diff_data)
        
        return summary_df, differences_by_index
    
    except Exception as e:
        print(f"Error comparing constituents: {str(e)}")
        import traceback
        traceback.print_exc()
        return None, None

# Usage
if __name__ == "__main__":
    # Get current directory
    current_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Define cutoff date and effective date - now you can easily change these for future reviews
    cutoff_date = "20250825"      # Format: YYYYMMDD
    effective_date = "20250901"   # Format: YYYYMMDD
    
    # Create an output folder if it doesn't exist
    output_folder = os.path.join(current_dir, "Output")
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
        print(f"Created output folder: {output_folder}")
    
    # Get current timestamp for the output file
    current_timestamp = dt.datetime.now().strftime("_%H%M%S")
    
    # Define file paths with dynamic cutoff date and using globs to match patterns
    cmf_file_pattern = fr"V:\PM-Indices-IndexOperations\Indices\Fixed Income indices\Review\Fixed Income Review\Data\DVE_MTS*CMF*{cutoff_date}_SDP_REFERENCEDATA.csv"
    bv_file_pattern = fr"V:\PM-Indices-IndexOperations\Indices\Fixed Income indices\Review\Fixed Income Review\Data\DVE_MTS*BV*{cutoff_date}_SDP_REFERENCEDATA.csv"
    h16r12_path = fr"V:\PM-Indices-IndexOperations\Indices\Fixed Income indices\Review\Fixed Income Review\Data\H16R12.{cutoff_date}.CSV"
    output_file = os.path.join(output_folder, f"Fixed_Income_Review_{cutoff_date}_{current_timestamp}.xlsx")
    
    # Use the modified function that handles both CMF and BV files
    universe_df, selection_df, eu_bonds_df = process_fixed_income_data(
        cmf_file_pattern, 
        bv_file_pattern, 
        h16r12_path, 
        cutoff_date, 
        effective_date
    )
    
    if universe_df is not None:        
        # In the output section:
        try:
            # Export processed data to an Excel file with multiple sheets
            with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
                universe_df.to_excel(writer, sheet_name='Universe', index=False)
                selection_df.to_excel(writer, sheet_name='Selection', index=False)
                
                # Create a simplified constituents sheet
                constituents_df = selection_df[['bondCode'] + 
                                            [col for col in selection_df.columns if col.startswith('EligibleFor_')]].copy()
                
                # Rename bondCode to ISIN
                constituents_df = constituents_df.rename(columns={'bondCode': 'ISIN'})
                
                # Rename the columns to remove 'EligibleFor_' prefix
                rename_dict = {col: col.replace('EligibleFor_', '') for col in constituents_df.columns if col.startswith('EligibleFor_')}
                constituents_df = constituents_df.rename(columns=rename_dict)
                
                # Sort the constituents by ISIN
                constituents_df = constituents_df.sort_values(by='ISIN')
                
                # Export it as a third sheet
                constituents_df.to_excel(writer, sheet_name='Constituents', index=False)
                
                # Compare with reference file
                # Extract the directory from the CMF file pattern
                data_dir = os.path.dirname(cmf_file_pattern)
                reference_file = os.path.join(data_dir, f"Euronext_FI_Review_{cutoff_date}.csv")
                if os.path.exists(reference_file):
                    summary_df, differences = compare_constituents(constituents_df, reference_file)
                    
                    # Export comparison results
                    if summary_df is not None:
                        summary_df.to_excel(writer, sheet_name='Comparison_Summary', index=False)
                        
                        # Export detailed differences
                        for index_name, diff_df in differences.items():
                            # Excel has a 31 character limit for sheet names
                            sheet_name = f"Diff_{index_name}"[:31]
                            diff_df.to_excel(writer, sheet_name=sheet_name, index=False)
                else:
                    print(f"Reference file not found: {reference_file}")
                
                # For debugging, also save the EU bonds dataframe
                if eu_bonds_df is not None and 'EligibleFor_MEEUG' in eu_bonds_df.columns:
                    eu_eligible = eu_bonds_df[(eu_bonds_df['EligibleFor_MEEUG'] == 1) | 
                                        (eu_bonds_df['EligibleFor_MEUBG'] == 1)]
                    eu_eligible.to_excel(writer, sheet_name='EU_Eligible', index=False)
            
            print(f"File saved successfully to: {output_file}")
            
            # Open the processed file with default application
            if os.path.exists(output_file):
                os.startfile(output_file)
                print(f"\nOpening {output_file}...")
            else:
                print(f"Error: Output file not found at {output_file}")
                
        except Exception as e:
            print(f"Error saving or opening file: {str(e)}")