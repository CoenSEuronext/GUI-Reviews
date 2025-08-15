import pandas as pd

def read_semicolon_csv(file_path, encoding="utf-8"):
    return pd.read_csv(file_path, sep=';', encoding=encoding)