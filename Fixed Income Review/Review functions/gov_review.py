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
    import pandas as pd
    from dateutil.relativedelta import relativedelta
    
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
        if row['CouponType'] not in ['1', '1.0', 1, 1.0]:
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
    
    return selection_df