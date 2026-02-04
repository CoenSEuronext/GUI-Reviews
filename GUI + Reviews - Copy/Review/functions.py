# functions.py

import pandas as pd

def read_semicolon_csv(file_path, encoding="utf-8"):
    return pd.read_csv(file_path, sep=';', encoding=encoding)




# developed_market_df.to_excel('debug_output.xlsx', index=False)
# os.startfile('debug_output.xlsx')