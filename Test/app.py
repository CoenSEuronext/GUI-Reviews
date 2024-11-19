import os
import pandas as pd
import numpy as np
from datetime import datetime
from functions import read_semicolon_csv

# Set the path to the folder where data is stored
data_folder = r"C:\Users\CSonneveld\OneDrive - Euronext\Documents\Projects\Data"
date = "20240917"
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
climate_ghg_df = pd.read_excel(os.path.join(data_folder, "ClimateGHG.xlsx"))
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
job_creation_df = pd.read_excel(os.path.join(data_folder, "Job Creation.xlsx"))
nace_df = pd.read_excel(os.path.join(data_folder, "NACE.xlsx"))
sesamm_df = pd.read_excel(os.path.join(data_folder, "SESAMM.xlsx"))
index_eod_us_df = read_semicolon_csv(os.path.join(data_folder, "TTMIndex"+ area + "1_GIS_EOD_INDEX_" + date + ".csv"), encoding="latin1")
stock_eod_us_df = read_semicolon_csv(os.path.join(data_folder, "TTMIndex"+ area + "1_GIS_EOD_STOCK_" + date + ".csv"), encoding="latin1")
index_eod_eu_df = read_semicolon_csv(os.path.join(data_folder, "TTMIndex"+ area2 + "1_GIS_EOD_INDEX_" + date + ".csv"), encoding="latin1")
stock_eod_eu_df = read_semicolon_csv(os.path.join(data_folder, "TTMIndex"+ area2 + "1_GIS_EOD_STOCK_" + date + ".csv"), encoding="latin1")


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

# Exclusion for if there is no SesamM Layoff score + creation of Exclude column
developed_market_df['exclude'] = np.where(
    ~developed_market_df['ISIN'].isin(sesamm_df['ISIN']),
    'exclude_layoff_score_6m',
    None
)

#Exclusion for 3 months aver. Turnover EUR
developed_market_df['exclude'] = np.where(
    (developed_market_df['3 months aver. Turnover EUR'] < 10000000) & 
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
           (df['Date'].astype(str).str.contains('2024')) &
           (df['ISIN'].isin(high_impact_isins))
       ),
       'exclude_value': 'exclude_SBT_NACE'
   }
}

# Process the criterion
for criterion in sbt_criterion.values():
   # Get ISINs that meet the exclusion condition
   excluded_isins = climate_ghg_df[
       criterion['condition'](climate_ghg_df)
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



# Print total exclusions
# count = (developed_market_df['exclude'] == 'exclude_StaffRating').sum()
# print(f"\nTotal exclude_StaffRating: {count} companies excluded")


# Step 3: Selection Ranking.

# Create selection_df with non-excluded companies
selection_df = developed_market_df[developed_market_df['exclude'].isna()].copy()
 
# Merge selection_df with job creation scores and staff ratings
selection_df = selection_df.merge(
   job_creation_df[['ISIN', 'number_jobs']], 
   on='ISIN',
   how='left'
).merge(
   analysis_df[['ISIN', 'CRStaffRatingNum']],
   on='ISIN',
   how='left'
)


def select_top_stocks(df, mic_type, n_stocks=20):
    """
    Select top n stocks based on number_jobs and CRStaffRatingNum for given MIC type.
    
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
    filtered_df['number_jobs'] = pd.to_numeric(filtered_df['number_jobs'], errors='coerce')
    filtered_df['CRStaffRatingNum'] = pd.to_numeric(filtered_df['CRStaffRatingNum'], errors='coerce')
    
    # Sort by number_jobs (descending) and CRStaffRatingNum (descending)
    sorted_df = filtered_df.sort_values(
        by=['number_jobs', 'CRStaffRatingNum'],
        ascending=[False, False],
        na_position='last'
    )
    
    # Select top n stocks
    selected_stocks = sorted_df.head(n_stocks)
    
    return selected_stocks

# Select top stocks for both XPAR and NOXPAR
xpar_selected = select_top_stocks(selection_df, 'XPAR')
noxpar_selected = select_top_stocks(selection_df, 'NOXPAR')

# Combine the selections
final_selection_df = pd.concat([xpar_selected, noxpar_selected])


total_mkt_cap = index_eod_df[index_eod_df['Mnemo'] == 'FRD4P']['Mkt Cap'].iloc[0]
print(total_mkt_cap)

stock_eod_df['id1'] = stock_eod_df['Isin Code'] + ' ' + stock_eod_df['Index']
stock_eod_df['Reuters/Optiq'] = stock_eod_df['#Symbol'].str.len().apply(lambda x: 'Reuters' if x < 12 else 'Optiq')
stock_eod_df['id2'] = stock_eod_df['#Symbol'] + ' ' + stock_eod_df['Currency']
stock_eod_df['id3'] = stock_eod_df['#Symbol'] + ' ' + stock_eod_df['Currency']
stock_eod_df['id4'] = stock_eod_df['#Symbol'] + ' ' + stock_eod_df['Currency']
stock_eod_df['id5'] = stock_eod_df['#Symbol'] + ' ' + stock_eod_df['Currency']
print(stock_eod_df[['#Symbol', 'Currency', 'id5']].head())

final_selection_df = final_selection_df.merge(
   ff_df[['ISIN Code:', 'Free Float Round:']],
   left_on='ISIN',
   right_on='ISIN Code:',
   how='left'
).merge(
   stock_eod_df[
       (stock_eod_df['Index'] == 'FRD4P')
   ][['Isin Code', 'FX/Index Ccy', 'Close Prc']],
   left_on='ISIN',
   right_on='Isin Code',
   how='left'
).merge(
   developed_market_df[['ISIN', 'NOSH']],
   on='ISIN',
   how='left'
)



# index_eod_df Free float market cap needed
# stock_eod_df individual free float market cap - price




# Launch developed_market_df
final_selection_df.to_excel('final_selection_df.xlsx', index=False)

os.startfile('final_selection_df.xlsx')
