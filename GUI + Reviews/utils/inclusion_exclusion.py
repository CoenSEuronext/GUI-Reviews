import pandas as pd

def inclusion_exclusion_analysis(selection_df, stock_eod_df, index, isin_column='ISIN code'):
    """
    Perform inclusion and exclusion analysis between selection_df and stock_eod_df.
    
    Args:
        selection_df (DataFrame): DataFrame with company details
        stock_eod_df (DataFrame): DataFrame with stock data
        index (str): Index to filter stock_eod_df
        isin_column (str, optional): Column name for ISIN in selection_df
    
    Returns:
        Dict containing inclusion and exclusion DataFrames
    """
    # Filter stock_eod_df based on the provided index
    filtered_stock_eod_df = stock_eod_df[stock_eod_df['Index'] == index]
    
    # Identify ISINs in stock_eod_df
    isin_set = set(filtered_stock_eod_df['Isin Code'])
    
    # Mark inclusion/exclusion in selection_df
    selection_df['Inclusion'] = selection_df[isin_column].apply(lambda x: x not in isin_set)
    
    # Prepare inclusion DataFrame
    inclusion_df = selection_df[selection_df['Inclusion']][[
        'Company', 
        isin_column, 
        'MIC'
    ]].sort_values('Company')
    
    # Prepare exclusion DataFrame
    exclusion_df = filtered_stock_eod_df[
        ~filtered_stock_eod_df['Isin Code'].isin(selection_df[isin_column])
    ][['Name', 'Isin Code', 'MIC']].drop_duplicates(subset=['Isin Code']).sort_values('Name').rename(columns={'Isin Code': 'ISIN code',
                                                                        'Name': 'Company'})

    return {
        'inclusion_df': inclusion_df,
        'exclusion_df': exclusion_df
    }