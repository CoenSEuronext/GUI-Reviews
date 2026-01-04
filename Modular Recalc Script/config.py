"""
Configuration file for Index Level Recalculation Script
"""

# EOD date to be recalculated (format: YYYYMMDD)
RELEVANT_EOD_DATE = "20251224"

# Stock prices to override in the calculation
stock_prices = {
    "/ARG.NZ": 1.23,
    "/AIA.NZ": 8.28,
    "/CHI.NZ": 2.92,
    "/CNU.NZ": 9.41,
    "/CEN.NZ": 9.39,
    "/EBO.NZ": 28.04,
    "/FPH.NZ": 37.86,
    "/FBU.NZ": 3.63,
    "/FCG.NZ": 5.92,
    "/FRW.NZ": 14.04,
    "/GMT.NZ": 1.96,
    "/HLG.NZ": 9.78,
    "/HGH.NZ": 1.14,
    "/IFT.NZ": 11.31,
    "/MFT.NZ": 69.01,
    "/MEL.NZ": 5.55,
    "/NPH.NZ": 3.68,
    "/PCT.NZ": 1.19,
    "/PFI.NZ": 2.37,
    "/SCL.NZ": 3.89,
    "/SKL.NZ": 5.21,
    "/SKT.NZ": 3.34,
    "/SKC.NZ": 0.895,
    "/SPK.NZ": 2.25,
    "/SPG.NZ": 1.365,
    "/SUM.NZ": 12.28,
    "/THL.NZ": 2.55,
    "/TWR.NZ": 1.995,
    "/TRA.NZ": 8.3,
    "/VHP.NZ": 1.98
}

# List of Price Index mnemonics to process
mnemonics = [
    "DAPPT",
    "DAREP",
    "DPAPT",
    "AS500",
    "DPAP",
    "DAPPR",
]

# TR4 Percentage-based Decrement Indices (Index Mnemonic: Underlying ISIN)
mnemonics_tr4_perc = {
    "MNEMO": "UNDERLYINGISIN"
}

# TR4 Points-based Decrement Indices (Index Mnemonic: Underlying ISIN)
mnemonics_tr4_points = {
    "MNEMO": "UNDERLYINGISIN"
}

# Base path for daily download files
BASE_PATH = r"V:\PM-Indices-IndexOperations\General\Daily downloadfiles\Monthly Archive"

# Output directory for results
OUTPUT_DIR = r"C:\Users\CSonneveld\OneDrive - Euronext\Documents\Projects\Modular Recalc Script\Output"