import os
import pandas as pd
import numpy as np
from datetime import datetime
from functions import read_semicolon_csv

# Set the path to the folder where data is stored
dlf_folder = r"V:\PM-Indices-IndexOperations\General\Daily downloadfiles\Monthly Archive"
data_folder = r"V:\PM-Indices-IndexOperations\Review Files" + "\\" + datetime.now().strftime("%Y%m")

date = "20241217"
effective_date = "23-Dec-24"
area = "US"
area2 = "EU"
type = "STOCK"
universe = "Developed Market"
index = "FRD4P"
isin = "FRIX00003031"
feed = "Reuters"
currency = "EUR"
year = "2024"

# Load files into DataFrames from the specified folder
developed_market_df = pd.read_excel(os.path.join(data_folder, "Developed Market.xlsx"))
ff_df = pd.read_excel(os.path.join(data_folder, "FF.xlsx"))
Oekom_TrustCarbon_df = pd.read_excel(
    os.path.join(data_folder, "Oekom Trust&Carbon.xlsx"),
    header=1
)
icb_df = pd.read_excel(
    os.path.join(data_folder, "ICB.xlsx"),
    header=3
)
nace_df = pd.read_excel(os.path.join(data_folder, "NACE.xlsx"))
sesamm_df = pd.read_excel(os.path.join(data_folder, "SESAMM.xlsx"))
index_eod_us_df = read_semicolon_csv(os.path.join(dlf_folder, "TTMIndex"+ area + "1_GIS_EOD_INDEX_" + date + ".csv"), encoding="latin1")
stock_eod_us_df = read_semicolon_csv(os.path.join(dlf_folder, "TTMIndex"+ area + "1_GIS_EOD_STOCK_" + date + ".csv"), encoding="latin1")
index_eod_eu_df = read_semicolon_csv(os.path.join(dlf_folder, "TTMIndex"+ area2 + "1_GIS_EOD_INDEX_" + date + ".csv"), encoding="latin1")
stock_eod_eu_df = read_semicolon_csv(os.path.join(dlf_folder, "TTMIndex"+ area2 + "1_GIS_EOD_STOCK_" + date + ".csv"), encoding="latin1")


index_eod_df = pd.concat([index_eod_us_df, index_eod_eu_df], ignore_index=True)
stock_eod_df = pd.concat([stock_eod_us_df, stock_eod_eu_df], ignore_index=True)

# Add Flag for XPAR or NON Xpar MIC
developed_market_df['XPAR Flag'] = developed_market_df['MIC'].apply(lambda x: 1 if x == 'XPAR' else 0)

# Add Area Flag
developed_market_df['Area Flag'] = developed_market_df['index'].apply(
    lambda x: 'NA' if 'NA500' in str(x) 
    else 'AS' if 'AS500' in str(x)
    else 'EU' if 'EU500' in str(x)
    else None
)



# Create exclude column
developed_market_df['exclude'] = None

# Exclusion for non-major currencies
allowed_currencies = ['EUR', 'JPY', 'USD', 'CAD', 'GBP']
developed_market_df['exclude'] = np.where(
    ~developed_market_df['Currency (Local)'].isin(allowed_currencies),
    'exclude_currency',
    developed_market_df['exclude']
)

# Exclusion for if there is no SesamM Layoff score
developed_market_df['exclude'] = np.where(
    ~developed_market_df['ISIN'].isin(sesamm_df['ISIN']) & 
    (developed_market_df['exclude'].isna()),
    'exclude_layoff_score_6m',
    developed_market_df['exclude']
)

#Exclusion for 3 months aver. Turnover EUR
developed_market_df['exclude'] = np.where(
    (developed_market_df['3 months ADTV'] < 10000000) & 
    (developed_market_df['exclude'].isna()),
    'exclude_turnover_EUR',
    developed_market_df['exclude']
)

#Exclusion for Breaches of internatonal standards
NBR_Overall_Flag_Red = Oekom_TrustCarbon_df[Oekom_TrustCarbon_df['NBR Overall Flag'] == 'RED']['ISIN'].tolist()

# Update developed_market_df exclude column
developed_market_df['exclude'] = np.where(
    (developed_market_df['ISIN'].isin(NBR_Overall_Flag_Red)) & 
    (developed_market_df['exclude'].isna()),
    'exclude_NBROverallFlag',
    developed_market_df['exclude']
)

# Exclusion for Controversial Weapons

# Define the exclusion criteria mapping
exclusion_criteria = {
    'Biological Weapons - Overall Flag': 'exclude_BiologicalWeaponsFlag',
    'Chemical Weapons - Overall Flag': 'exclude_ChemicalWeaponsFlag',
    'Nuclear Weapons Inside NPT - Overall Flag': 'exclude_NuclearWeaponsFlag',
    'Nuclear Weapons Outside NPT - Overall Flag': 'exclude_NuclearWeaponsNonNPTFlag',
    'Cluster Munitions - Overall Flag': 'exclude_ClusterMunitionsFlag',
    'Depleted Uranium - Overall Flag': 'exclude_DepletedUraniumFlag',
    'Anti-personnel Mines - Overall Flag': 'exclude_APMinesFlag',
    'White Phosphorous Weapons - Overall Flag': 'exclude_WhitePhosphorusFlag'
}

# Process each exclusion criterion
for column, exclude_value in exclusion_criteria.items():
    # Get ISINs that have either 'Red' or 'Amber' flags
    flagged_isins = Oekom_TrustCarbon_df[
        Oekom_TrustCarbon_df[column].isin(['RED', 'Amber'])
    ]['ISIN'].tolist()
    
    # Update the exclude column where applicable
    developed_market_df['exclude'] = np.where(
        (developed_market_df['ISIN'].isin(flagged_isins)) & 
        (developed_market_df['exclude'].isna()),
        exclude_value,
        developed_market_df['exclude']
    )




#Exclusion for Energy Screening

Oekom_TrustCarbon_df['Coal Mining and Power Gen - Maximum Percentage of Revenues (%)'] = pd.to_numeric(Oekom_TrustCarbon_df['Coal Mining and Power Gen - Maximum Percentage of Revenues (%)'], errors='coerce')
Oekom_TrustCarbon_df['FossilFuelProdMaxRev'] = pd.to_numeric(Oekom_TrustCarbon_df['FossilFuelProdMaxRev'], errors='coerce')
Oekom_TrustCarbon_df['FossilFuelDistMaxRev'] = pd.to_numeric(Oekom_TrustCarbon_df['FossilFuelDistMaxRev'], errors='coerce')
Oekom_TrustCarbon_df['Power Generation - Thermal Maximum Percentage of Revenues (%)'] = pd.to_numeric(Oekom_TrustCarbon_df['Power Generation - Thermal Maximum Percentage of Revenues (%)'], errors='coerce')
# Define the energy screening criteria
energy_criteria = {
    'Coal': {
        'condition': lambda df: df['Coal Mining and Power Gen - Maximum Percentage of Revenues (%)'] >= 0.01,
        'exclude_value': 'exclude_CoalMining'
    },
    'FossilFuel': {
        'condition': lambda df: (df['FossilFuelProdMaxRev'] + df['FossilFuelDistMaxRev']) >= 0.10,
        'exclude_value': 'exclude_FossilFuel'
    },
    'Thermal': {
        'condition': lambda df: df['Power Generation - Thermal Maximum Percentage of Revenues (%)'] >= 0.50,
        'exclude_value': 'exclude_ThermalPower'
    }
}

# Process each criterion
for criterion in energy_criteria.values():
    # Get ISINs that meet the exclusion condition
    excluded_isins = Oekom_TrustCarbon_df[
        criterion['condition'](Oekom_TrustCarbon_df)
    ]['ISIN'].tolist()
    
    # Update the exclude column
    developed_market_df['exclude'] = np.where(
        (developed_market_df['ISIN'].isin(excluded_isins)) & 
        (developed_market_df['exclude'].isna()),
        criterion['exclude_value'],
        developed_market_df['exclude']
    )

# Exclusion for SBT alignment

# Get list of high climate impact NACE codes
high_impact_nace = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'L']

# Get ISINs from nace_df that have high impact NACE codes
high_impact_isins = nace_df[nace_df['NACE'].isin(high_impact_nace)]['ISIN'].tolist()

# Define the SBT screening criterion
sbt_criterion = {
   'SBT': {
       'condition': lambda df: (
           (df['ClimateGHGReductionTargets'] != 'Approved SBT') & 
           (df['ISIN'].isin(high_impact_isins))
       ),
       'exclude_value': 'exclude_SBT_NACE'
   }
}

# Process the criterion
for criterion in sbt_criterion.values():
   # Get ISINs that meet the exclusion condition
   excluded_isins = Oekom_TrustCarbon_df[
       criterion['condition'](Oekom_TrustCarbon_df)
   ]['ISIN'].tolist()
   
   # Update the exclude column
   developed_market_df['exclude'] = np.where(
       (developed_market_df['ISIN'].isin(excluded_isins)) & 
       (developed_market_df['exclude'].isna()),
       criterion['exclude_value'],
       developed_market_df['exclude']
   )

# Tobacco Screening Exclusions
# Transforming to Numeric
Oekom_TrustCarbon_df['Tobacco - Production Maximum Percentage of Revenues (%)'] = pd.to_numeric(Oekom_TrustCarbon_df['Tobacco - Production Maximum Percentage of Revenues (%)'], errors='coerce')
Oekom_TrustCarbon_df['Tobacco - Distribution Maximum Percentage of Revenues (%)'] = pd.to_numeric(Oekom_TrustCarbon_df['Tobacco - Distribution Maximum Percentage of Revenues (%)'], errors='coerce')

# Define the tobacco screening criteria
tobacco_criteria = {
   'TobaccoProduction': {
       'condition': lambda df: df['Tobacco - Production Maximum Percentage of Revenues (%)'] > 0,
       'exclude_value': 'exclude_TobaccoProduction'
   },
   'TobaccoDistribution': {
       'condition': lambda df: df['Tobacco - Distribution Maximum Percentage of Revenues (%)'] >= 0.15, 
       'exclude_value': 'exclude_TobaccoDistribution'
   }
}

# Process each criterion
for criterion in tobacco_criteria.values():
   # Get ISINs that meet the exclusion condition
   excluded_isins = Oekom_TrustCarbon_df[
       criterion['condition'](Oekom_TrustCarbon_df)
   ]['ISIN'].tolist()
   
   # Update the exclude column
   developed_market_df['exclude'] = np.where(
       (developed_market_df['ISIN'].isin(excluded_isins)) & 
       (developed_market_df['exclude'].isna()),
       criterion['exclude_value'],
       developed_market_df['exclude']
   )

#Layoff Screening
# Define the layoff screening criterion
layoff_criterion = {
   'Layoff': {
       'condition': lambda df: df['layoff_score_6m'] > 0,
       'exclude_value': 'exclude_Layoff'
   }
}

# Process the criterion
for criterion in layoff_criterion.values():
   # Get ISINs that meet the exclusion condition
   excluded_isins = sesamm_df[
       criterion['condition'](sesamm_df)
   ]['ISIN'].tolist()
   
   # Update the exclude column
   developed_market_df['exclude'] = np.where(
       (developed_market_df['ISIN'].isin(excluded_isins)) & 
       (developed_market_df['exclude'].isna()),
       criterion['exclude_value'],
       developed_market_df['exclude']
   )



# Staff Rating Screening

# First get list of ISINs from developed_market_df
developed_market_isins = developed_market_df['ISIN'].tolist()

# Merge and filter for only companies in developed_market_df
analysis_df = (Oekom_TrustCarbon_df[Oekom_TrustCarbon_df['ISIN'].isin(developed_market_isins)]
    .merge(
        developed_market_df[['ISIN', 'Area Flag']],
        on='ISIN',
        how='left'
    )
    .merge(
        icb_df,
        left_on='ISIN',
        right_on='ISIN Code',
        how='left'
    )
    .drop_duplicates(subset=['ISIN'])  # Add this to remove duplicates
)

# Convert to numeric and fill NaN with 0
analysis_df['CRStaffRatingNum'] = pd.to_numeric(analysis_df['CRStaffRatingNum'], errors='coerce').fillna(3)

# Create an empty list to collect ISINs to exclude
excluded_isins = []

# Process each group separately
for (sector, area), group in analysis_df.groupby(['Supersector Code', 'Area Flag']):
   # Sort group by CRStaffRatingNum
   sorted_group = group.sort_values('CRStaffRatingNum')
   
   # Calculate number of companies to exclude (20% of total count, rounded down)
   n_companies = len(group)
   n_to_exclude = int(np.floor(n_companies * 0.1999999999))
   
   # Get the ISINs of the bottom n_to_exclude companies by CRStaffRatingNum
   bottom_isins = sorted_group['ISIN'].iloc[:n_to_exclude].tolist()
   excluded_isins.extend(bottom_isins)

# Update the exclude column in developed_market_df
developed_market_df['exclude'] = np.where(
   (developed_market_df['ISIN'].isin(excluded_isins)) & 
   (developed_market_df['exclude'].isna()),
   'exclude_StaffRating',
   developed_market_df['exclude']
)




# Step 3: Selection Ranking.

# Create selection_df with non-excluded companies
selection_df = developed_market_df[developed_market_df['exclude'].isna()].copy()
 
# Merge selection_df with job creation scores and staff ratings
selection_df = selection_df.merge(
   sesamm_df[['ISIN', 'Job_score_3Y']], 
   on='ISIN',
   how='left'
).merge(
   analysis_df[['ISIN', 'CRStaffRatingNum']],
   on='ISIN',
   how='left'
)


def select_top_stocks(df, mic_type, n_stocks=[20, 25]):
    """
    Select top n stocks based on Job_score_3Y and CRStaffRatingNum for given MIC type.
    
    Args:
        df: DataFrame containing the stock data
        mic_type: 'XPAR' or 'NOXPAR'
        n_stocks: Number of stocks to select (default 20)
    
    Returns:

        DataFrame with selected stocks
    """
    # Filter for MIC type
    if mic_type == 'XPAR':
        filtered_df = df[df['MIC'] == 'XPAR'].copy()
    else:  # NOXPAR
        filtered_df = df[df['MIC'] != 'XPAR'].copy()
    
    # Convert to numeric and handle NaN values
    filtered_df['Job_score_3Y'] = pd.to_numeric(filtered_df['Job_score_3Y'], errors='coerce')
    filtered_df['CRStaffRatingNum'] = pd.to_numeric(filtered_df['CRStaffRatingNum'], errors='coerce')
    
    # Sort by Job_score_3Y (descending) and CRStaffRatingNum (descending)
    sorted_df = filtered_df.sort_values(
        by=['Job_score_3Y', 'CRStaffRatingNum'],  # Changed from number_jobs
        ascending=[False, False],
        na_position='last'
    )
    
    # Select top n stocks
    selected_stocks = sorted_df.head(n_stocks)
    
    return selected_stocks

# Select top stocks for both XPAR and NOXPAR
xpar_selected_25 = select_top_stocks(selection_df, 'XPAR', 25)
noxpar_selected_25 = select_top_stocks(selection_df, 'NOXPAR', 25)
xpar_selected_20 = select_top_stocks(selection_df, 'XPAR', 20)
noxpar_selected_20 = select_top_stocks(selection_df, 'NOXPAR', 20)

# Combine the selections
full_selection_df = pd.concat([xpar_selected_25, noxpar_selected_25])
final_selection_df = pd.concat([xpar_selected_20, noxpar_selected_20])








def get_index_currency(row, index_df):
    """
    Get the currency for an index by matching Index with Mnemo
    """
    mask = index_df['Mnemo'] == row['Index']
    matches = index_df[mask]
    if not matches.empty:
        return matches.iloc[0]['Curr']
    return None

# Add Index Currency column to stock_eod_df
stock_eod_df['Index Currency'] = stock_eod_df.apply(
    lambda row: get_index_currency(row, index_eod_df), axis=1
)
stock_eod_df['ISIN/Index'] = stock_eod_df['Isin Code'] + stock_eod_df['Index']
stock_eod_df['Index Currency']
stock_eod_df['id5'] = stock_eod_df['#Symbol'] + stock_eod_df['Index Currency']
stock_eod_df['Reuters/Optiq'] = stock_eod_df['#Symbol'].str.len().apply(lambda x: 'Reuters' if x < 12 else 'Optiq')

def get_stock_info(row, stock_df, target_currency):
    """
    Get stock info including FX rate by matching Symbol + currency with id5
    """
    # First match for Symbol and Price
    mask = (stock_df['Isin Code'] == row['ISIN']) & \
           (stock_df['MIC'] == row['MIC']) & \
           (stock_df['Reuters/Optiq'] == 'Reuters')
    
    matches = stock_df[mask]
    
    if not matches.empty:
        first_match = matches.iloc[0]
        # Create the lookup value (Symbol + currency)
        lookup_id5 = f"{first_match['#Symbol']}{target_currency}"
        
        # Find FX rate using id5
        fx_mask = stock_df['id5'] == lookup_id5
        fx_matches = stock_df[fx_mask]
        
        fx_rate = fx_matches.iloc[0]['FX/Index Ccy'] if not fx_matches.empty else None
        
        return pd.Series({
            'Symbol': first_match['#Symbol'],
            'Price': first_match['Close Prc'],
            'FX Rate': fx_rate
        })
    return pd.Series({'Symbol': None, 'Price': None, 'FX Rate': None})

# Add Symbol, Price, and FX Rate columns to xpar_selected
xpar_selected_20[['Symbol', 'Price', 'FX Rate']] = xpar_selected_20.apply(
    lambda row: get_stock_info(row, stock_eod_df, currency), axis=1
)

# Add Symbol, Price, and FX Rate columns to noxpar_selected
noxpar_selected_20[['Symbol', 'Price', 'FX Rate']] = noxpar_selected_20.apply(
    lambda row: get_stock_info(row, stock_eod_df, currency), axis=1
)

def get_free_float(row, ff_dataframe):
   """
   Get Free Float Round: value by matching ISIN
   """
   mask = ff_dataframe['ISIN Code:'] == row['ISIN']
   matches = ff_dataframe[mask]
   if not matches.empty:
       return matches.iloc[0]['Free Float Round:']
   return None

# Add Free Float Round: column to xpar_selected
xpar_selected_20['Free Float'] = xpar_selected_20.apply(
   lambda row: get_free_float(row, ff_df), axis=1
)

# Add Free Float Round: column to noxpar_selected
noxpar_selected_20['Free Float'] = noxpar_selected_20.apply(
   lambda row: get_free_float(row, ff_df), axis=1
)

xpar_selected_20['Price in Index Currency'] = xpar_selected_20['Price'] * xpar_selected_20['FX Rate']
noxpar_selected_20['Price in Index Currency'] = noxpar_selected_20['Price'] * noxpar_selected_20['FX Rate']
xpar_selected_20['Original market cap'] = xpar_selected_20['Price in Index Currency'] * xpar_selected_20['NOSH'] * xpar_selected_20['Free Float']
noxpar_selected_20['Original market cap'] = noxpar_selected_20['Price in Index Currency'] * noxpar_selected_20['NOSH'] * noxpar_selected_20['Free Float']

def apply_capping(df, step, cap_threshold=0.2, final_step=False):
    current_step = step
    next_step = step + 1
    
    # Use previous Mcap if available, otherwise use Original market cap
    prev_mcap = f'Mcap {current_step-1}' if current_step > 1 else 'Original market cap'
    
    # Count capped items and calculate new market cap
    n_capping = (df[f'Capping {current_step}'] == 1).sum()
    perc_no_cap = 1 - (n_capping * cap_threshold)
    mcap_capping = df[df[f'Capping {current_step}'] == 1][prev_mcap].sum()
    new_mcap = (df[prev_mcap].sum() - mcap_capping) / perc_no_cap
    
    # Calculate new market cap and weight
    df[f'Mcap {current_step}'] = df.apply(
        lambda row: cap_threshold * new_mcap if row[f'Capping {current_step}'] == 1 else row[prev_mcap],
        axis=1
    )
    df[f'Weight {current_step}'] = df[f'Mcap {current_step}'] / new_mcap
    
    # Only add next Capping if not the final step
    if not final_step:
        df[f'Capping {next_step}'] = df[f'Weight {current_step}'].apply(lambda x: 1 if x > cap_threshold else 0)
    
    return df

# Initial setup
index_mkt_cap = index_eod_df[index_eod_df['IsinCode'] == isin]['Mkt Cap'].iloc[0]
ffmc_world = noxpar_selected_20['Original market cap'].sum()
ffmc_france = xpar_selected_20['Original market cap'].sum()
ffmc_total = ffmc_france + ffmc_world

# Initial weights
xpar_selected_20['Weight'] = xpar_selected_20['Original market cap'] / ffmc_france
noxpar_selected_20['Weight'] = noxpar_selected_20['Original market cap'] / ffmc_world
xpar_selected_20['Capping 1'] = xpar_selected_20['Weight'].apply(lambda x: 1 if x > 0.2 else 0)
noxpar_selected_20['Capping 1'] = noxpar_selected_20['Weight'].apply(lambda x: 1 if x > 0.2 else 0)

# Apply capping process three times
for step in [1, 2]:  
   xpar_selected_20 = apply_capping(xpar_selected_20, step)
   noxpar_selected_20 = apply_capping(noxpar_selected_20, step)

# Final step without creating next Capping column
xpar_selected_20 = apply_capping(xpar_selected_20, 3, final_step=True)
noxpar_selected_20 = apply_capping(noxpar_selected_20, 3, final_step=True)

xpar_selected_20['Final Capping'] = (xpar_selected_20['Weight 3'] * ffmc_total) / xpar_selected_20['Original market cap']
noxpar_selected_20['Final Capping'] = (noxpar_selected_20['Weight 3'] * ffmc_total) / noxpar_selected_20['Original market cap']


# Combine into final selection if needed
final_selection_df = pd.concat([xpar_selected_20, noxpar_selected_20])
max_capping = final_selection_df['Final Capping'].max()
final_selection_df['Final Capping'] = (final_selection_df['Final Capping'] / max_capping).round(14)
final_selection_df['Effective Date of Review'] = effective_date
FRD4P_df = final_selection_df[[
   'Name', 
   'ISIN', 
   'MIC', 
   'NOSH', 
   'Free Float',
   'Final Capping',
   'Effective Date of Review',  # Assuming this is the Free Float column
   'Currency (Local)'  # Assuming this is the Final Capping
]].copy()

# Optionally rename columns if needed
FRD4P_df = FRD4P_df.rename(columns={
   'Currency (Local)': 'Currency',
   'Name': 'Company',
   'ISIN': 'ISIN Code',
   'NOSH': 'Number of Shares',
   'Final Capping': 'Capping Factor'


})
FRD4P_df = FRD4P_df.sort_values('Company')

# developed_market_df.to_excel('developed_market_df.xlsx', index=False)

# os.startfile('developed_market_df.xlsx')

# full_selection_df.to_excel('full_selection_df.xlsx', index=False)

# os.startfile('full_selection_df.xlsx')

# Launch developed_market_df
FRD4P_df.to_excel('FRD4P_df.xlsx', index=False)

os.startfile('FRD4P_df.xlsx')

