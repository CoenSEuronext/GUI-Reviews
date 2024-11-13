import os
import pandas as pd

data_folder = r"C:\Users\CSonneveld\OneDrive - Euronext\Documents\Projects\Data"

print(pd.read_excel(os.path.join(data_folder, "ClimateGHG.xlsx")))