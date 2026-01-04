What the project does:
app.py
It replaces incorrect prices in both EU and US downloadfiles and recalculates all indices that are asked to be recalculated.
app_eu.py
It replaces incorrect prices in both EU downloadfiles and recalculates all indices that are asked to be recalculated.
app_us.py
It replaces incorrect prices in both US downloadfiles and recalculates all indices that are asked to be recalculated.

All depend on the same config.py file that you need to configure by following the steps jotted down below:

Step 1:
input recalc date in RELEVANT_EOD_DATE

Step 2:
Add stock price in stock_prices dict in the format below:
stock_prices = {
"AAPL.O": 23.323,
"MSFT.O": 11.323
}

Step 3:
Add all Price Index Mnemos in mnemonics:
mnemonics = [
"WCAMP",
"GSCSP",
"TCAMP",
]

Step 4:
Add all tr4 perc + their underlying index in mnemonics_tr4_perc
mnemonics_tr4_perc = {
    "LC3WD": "FR0013522596",
    }

Step 5:
Add all tr4 point + their underlying index in mnemonics_tr4_points
mnemonics_tr4_points = {
    "WCAMB": "FRESG0000355",
    "GSCSD": "FRESG0000884",
    "TCAMB": "FR0014005GG7"
}

Step 6:
Run the script in the terminal

Step 7:
Open output from folder:
