import pandas as pd
import os
import numpy as np
from decimal import Decimal, getcontext

# Set decimal precision high enough for your needs
getcontext().prec = 36  # Set precision to 36 digits

# Set pandas to display full numbers without scientific notation
pd.set_option('display.float_format', '{:.2f}'.format)  # Format with 2 decimal places
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)  # Increase display width

# Define file paths
base_folder = r"V:\PM-Indices-IndexOperations\General\Daily Folders\202505\20250509\Correction Crayon GR"
output_folder = r"V:\PM-Indices-IndexOperations\General\Daily Folders\202505\20250509\Correction Crayon GR"
stock_eu_path = os.path.join(base_folder, "COEN_NXTD_STOCK_EU.csv")
stock_us_path = os.path.join(base_folder, "COEN_NXTD_STOCK_US.csv")
index_eu_path = os.path.join(base_folder, "COEN_NXTD_INDEX_EU.csv")
index_us_path = os.path.join(base_folder, "COEN_NXTD_INDEX_US.csv")

# Output paths
combined_stock_path = os.path.join(output_folder, "COEN_NXTD_STOCK_COMBINED.csv")
combined_index_path = os.path.join(output_folder, "COEN_NXTD_INDEX_COMBINED.csv")
output_summary_path = os.path.join(output_folder, "INDEX_SUMMARY_OUTPUT.csv")
readable_output_path = os.path.join(output_folder, "INDEX_SUMMARY_READABLE.csv")
excel_output_path = os.path.join(output_folder, "INDEX_SUMMARY.xlsx")

# Step 1: Read the CSV files with Latin-1 encoding and semicolon separator
print("Reading input files...")
stock_eu = pd.read_csv(stock_eu_path, encoding='latin1', sep=';')
stock_us = pd.read_csv(stock_us_path, encoding='latin1', sep=';')
index_eu = pd.read_csv(index_eu_path, encoding='latin1', sep=';')
index_us = pd.read_csv(index_us_path, encoding='latin1', sep=';')

# Step 2: Append US files to EU files (without headers)
print("Appending US files to EU files...")
combined_stock = pd.concat([stock_eu, stock_us], ignore_index=True)
combined_index = pd.concat([index_eu, index_us], ignore_index=True)

# Step 3: Calculate New_Mkt_Cap column for the STOCK file using Decimal for high precision
print("Calculating New_Mkt_Cap...")

# Function to convert a value to Decimal safely
def to_decimal(val):
    try:
        return Decimal(str(val))  # Convert to string first to preserve precision
    except:
        return Decimal('0')

# Calculate with high precision
combined_stock['New_Mkt_Cap'] = combined_stock.apply(
    lambda row: (to_decimal(row['Adj Closing price']) * 
                to_decimal(row['Shares']) * 
                to_decimal(row['Free float-Coeff']) * 
                to_decimal(row['Capping Factor-Coeff']) * 
                to_decimal(row['FX/Index Ccy'])),
    axis=1
)

# For saving to CSV, use string representation to maintain full precision
def format_decimal_numbers(df):
    formatted_df = df.copy()
    for col in formatted_df.columns:
        if col == 'New_Mkt_Cap' or (
            formatted_df[col].dtype == 'object' and 
            len(formatted_df) > 0 and 
            isinstance(formatted_df[col].iloc[0], Decimal)
        ):
            # For Decimal columns, convert to string to preserve full precision
            formatted_df[col] = formatted_df[col].apply(lambda x: str(x) if isinstance(x, Decimal) else x)
    return formatted_df

# Step 4: Save the combined files with Latin-1 encoding and semicolon separator
print("Saving combined files...")
combined_stock_formatted = format_decimal_numbers(combined_stock)
combined_index_formatted = format_decimal_numbers(combined_index)
combined_stock_formatted.to_csv(combined_stock_path, index=False, encoding='latin1', sep=';')
combined_index_formatted.to_csv(combined_index_path, index=False, encoding='latin1', sep=';')

# Step 5: Filter for the specified indexes and calculate the sum using Decimal
print("Creating summary for specified indexes...")
target_indexes = ['EDWPT', 'EDWP', 'EETAP', 'EETEP', 'DEUPT', 'DEUP']
filtered_stock = combined_stock[combined_stock['Index'].isin(target_indexes)]

# Group by Index and sum the New_Mkt_Cap
# We need a custom aggregation to sum Decimal objects correctly
def sum_decimals(x):
    return sum(x)

summary = filtered_stock.groupby('Index')['New_Mkt_Cap'].apply(sum_decimals).reset_index()
summary.columns = ['Index', 'Total_Market_Cap']

# Step 6: Save the summary to output files in different formats
print("Saving summary outputs...")

# Save standard CSV with semicolon separator
summary_formatted = format_decimal_numbers(summary)
summary_formatted.to_csv(output_summary_path, index=False, encoding='latin1', sep=';')

# Save readable CSV
summary.to_csv(readable_output_path, index=False)

# Save as Excel file which can handle large numbers better
try:
    summary_excel = summary.copy()
    summary_excel['Total_Market_Cap'] = summary_excel['Total_Market_Cap'].apply(lambda x: str(x))
    summary_excel.to_excel(excel_output_path, index=False)
    print(f"Excel summary saved to: {excel_output_path}")
except Exception as e:
    print(f"Could not save Excel file: {e}")

print("Processing complete!")
print(f"Combined stock file saved to: {combined_stock_path}")
print(f"Combined index file saved to: {combined_index_path}")
print(f"Summary output saved to: {output_summary_path}")
print(f"Readable summary saved to: {readable_output_path}")

# Display the summary
print("\nIndex Market Cap Summary:")
# Format for display: Index name followed by the full precision number
for idx, row in summary.iterrows():
    index_name = row['Index']
    market_cap = row['Total_Market_Cap']
    print(f"{index_name}: {market_cap}")