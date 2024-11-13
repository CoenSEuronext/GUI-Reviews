import os
import pandas as pd
import numpy as np
from datetime import datetime
from functions import read_semicolon_csv

# Set the path to the folder where data is stored
data_folder = r"C:\Users\CSonneveld\OneDrive - Euronext\Documents\Projects\Data"
date = "20241108"
area = "EU"
type = "STOCK"
universe = "Developed Market"
index = "FRD4P"
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
index_eod_df = read_semicolon_csv(os.path.join(data_folder, "TTMIndexEU1_GIS_EOD_INDEX_20241108.csv"), encoding="latin1")
stock_eod_df = read_semicolon_csv(os.path.join(data_folder, "TTMIndexEU1_GIS_EOD_STOCK_20241108.csv"), encoding="latin1")

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

# First merge the ICB super sector information and Area Flag
Oekom_TrustCarbon_df = Oekom_TrustCarbon_df.merge(
    developed_market_df[['ISIN', 'Area Flag']],
    on='ISIN',
    how='left'
).merge(
    icb_df,
    left_on='ISIN',
    right_on='ISIN Code', 
    how='left'
)

# Make sure CRStaffRatingNum is numeric
Oekom_TrustCarbon_df['CRStaffRatingNum'] = pd.to_numeric(Oekom_TrustCarbon_df['CRStaffRatingNum'], errors='coerce').fillna(0)

# Let's add some verification prints
print("\nExample calculations:")
for area in Oekom_TrustCarbon_df['Area Flag'].unique():
    for sector in Oekom_TrustCarbon_df['Supersector Code'].unique():
        subset = Oekom_TrustCarbon_df[
            (Oekom_TrustCarbon_df['Area Flag'] == area) & 
            (Oekom_TrustCarbon_df['Supersector Code'] == sector)
        ]
        if len(subset) > 0:
            percentile_20 = subset['CRStaffRatingNum'].quantile(0.2)
            count_below = len(subset[subset['CRStaffRatingNum'] < percentile_20])
            print(f"Area: {area}, Sector: {sector}")
            print(f"20th percentile: {percentile_20:.2f}")
            print(f"Companies below threshold: {count_below}")
            print("---")

# Calculate 20th percentile by region and super sector
percentiles = Oekom_TrustCarbon_df.groupby(['Supersector Code', 'Area Flag'])['CRStaffRatingNum'].transform(
    lambda x: x.quantile(0.2)
)

# Get ISINs of companies below their group's 20th percentile
excluded_isins = Oekom_TrustCarbon_df[
    Oekom_TrustCarbon_df['CRStaffRatingNum'] < percentiles
]['ISIN'].tolist()

# Update the exclude column in developed_market_df
developed_market_df['exclude'] = np.where(
    (developed_market_df['ISIN'].isin(excluded_isins)) & 
    (developed_market_df['exclude'].isna()),
    'exclude_StaffRating',
    developed_market_df['exclude']
)

# Print summary of exclusions
count = (developed_market_df['exclude'] == 'exclude_StaffRating').sum()
print(f"\nTotal exclude_StaffRating: {count} companies excluded")



developed_market_df.to_excel('developed_market.xlsx', index=False)

os.startfile('developed_market.xlsx')
