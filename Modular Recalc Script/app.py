import pandas as pd
import datetime
import os

# Stock prices dictionary
stock_prices = {
"/AAV.TO":11.52,
"/AGI.TO":43.94,
"/AIF.TO":57.15,
"/ALS.TO":38.17,
"/AP_u.TO":14.33,
"/ARE.TO":30.95,
"/ARIS.TO":14.26,
"/ATD.TO":69.61,
"/ATRL.TO":97.88,
"/ATZ.TO":100.31,
"/AYA.TO":15.47,
"/BB.TO":6.75,
"/BBUC.TO":50.18,
"/BDGI.TO":73.49,
"/BEI_u.TO":63.87,
"/BIR.TO":6.69,
"/BMO.TO":175.49,
"/BNS.TO":92.36,
"/BTE.TO":3.35,
"/BYD.TO":224.71,
"/CAR_u.TO":38.18,
"/CCLb.TO":77.42,
"/CG.TO":16.56,
"/CHP_u.TO":14.67,
"/CJ.TO":8.04,
"/CLS.TO":490.61,
"/CNR.TO":134.4,
"/CP.TO":99.36,
"/CPX.TO":73.59,
"/CRT_u.TO":16.16,
"/CSH_u.TO":20.83,
"/CTCa.TO":161.84,
"/CVE.TO":23.91,
"/DFY.TO":64.89,
"/DIR_u.TO":12.1,
"/DOO.TO":87.62,
"/DSG.TO":124.03,
"/EFN.TO":37.87,
"/EFX.TO":18.25,
"/ELD.TO":36.23,
"/EMPa.TO":48.2,
"/ENGH.TO":21.03,
"/EQX.TO":15.28,
"/ERO.TO":28.99,
"/EXE.TO":16.31,
"/FFH.TO":2238.79,
"/FNV.TO":264.4,
"/FTS.TO":69.99,
"/GEI.TO":23.75,
"/GIBa.TO":121.78,
"/H.TO":51.26,
"/HPSa.TO":215,
"/IAG.TO":167.52,
"/IMO.TO":126.19,
"/JWEL.TO":34.5,
"/KEY.TO":41.17,
"/KMP_u.TO":17.17,
"/L.TO":55.71,
"/AAUC.TO":21.8,
"/ABX.TO":46.45,
"/ACOx.TO":52.71,
"/AEM.TO":228.35,
"/AII.TO":9.21,
"/ALA.TO":41.02,
"/AND.TO":54.96,
"/AQN.TO":7.82,
"/ARX.TO":25.86,
"/LIF.TO":29,
"/LSPD.TO":16.66,
"/LUN.TO":22.44,
"/MAU.TO":6.93,
"/MDA.TO":27.4,
"/MFI.TO":26.58,
"/MG.TO":69.26,
"/MRU.TO":93.45,
"/MX.TO":52.3,
"/NFI.TO":14.28,
"/NGD.TO":10.23,
"/NPI.TO":25.78,
"/NVA.TO":17.05,
"/NWH_u.TO":5.03,
"/OLA.TO":14.35,
"/OR.TO":44.77,
"/PET.TO":35.7,
"/PMZ_u.TO":15.23,
"/POU.TO":23.32,
"/PPL.TO":52.95,
"/PSI.TO":11.88,
"/PXT.TO":18.59,
"/QSR.TO":92.48,
"/RCH.TO":38.94,
"/REI_u.TO":18.64,
"/RUS.TO":42.78,
"/SES.TO":17.12,
"/SJ.TO":78.66,
"/SKE.TO":22.87,
"/SLF.TO":85.74,
"/SOBO.TO":35.97,
"/SRU_u.TO":26.4,
"/STN.TO":153.91,
"/SVM.TO":9.11,
"/T.TO":20.5,
"/TCLa.TO":19.5,
"/TD.TO":115.18,
"/TFII.TO":125.55,
"/TKO.TO":6.28,
"/TPZ.TO":24.84,
"/TRI.TO":218.92,
"/TSU.TO":38.09,
"/TVK.TO":130.49,
"/TXG.TO":58.2,
"/VNP.TO":20.76,
"/WCN.TO":232.94,
"/WDO.TO":20.86,
"/WFG.TO":84.5,
"/WPK.TO":41.46,
"/WSP.TO":267.7,
"/X.TO":51.93,
"/ATH.TO":7.05,
"/ATS.TO":37.93,
"/BAM.TO":76.43,
"/BBDb.TO":199.7,
"/BCE.TO":31.88,
"/BDT.TO":29.89,
"/BIPC.TO":64.43,
"/BITF.TO":5.79,
"/BLX.TO":27.95,
"/BN.TO":64.62,
"/BNT.TO":64.72,
"/BTO.TO":5.97,
"/CAE.TO":38.84,
"/CCO.TO":141.77,
"/CEU.TO":9.45,
"/CIGI.TO":223.21,
"/CJT.TO":81.23,
"/CM.TO":117.84,
"/CNQ.TO":44.9,
"/CPKR.TO":16.16,
"/CRR_u.TO":14.83,
"/CS.TO":12.29,
"/CSU.TO":3584.85,
"/CU.TO":39.36,
"/DML.TO":4.11,
"/DOL.TO":182.6,
"/DPM.TO":30.13,
"/DSV.TO":5.57,
"/EFR.TO":24.52,
"/EIF.TO":77.73,
"/EMA.TO":66.53,
"/ENB.TO":65.52,
"/EQB.TO":90.19,
"/FCR_u.TO":18.61,
"/FM.TO":28.48,
"/FOM.TO":3.78,
"/FRU.TO":14.14,
"/FSV.TO":221.04,
"/FTT.TO":75.25,
"/GFL.TO":61.51,
"/GIL.TO":81.5,
"/GMIN.TO":27.75,
"/GSY.TO":167.94,
"/GWO.TO":59.54,
"/HBM.TO":22.27,
"/HWX.TO":7.44,
"/IFC.TO":263.4,
"/IGM.TO":54.71,
"/IMG.TO":16.23,
"/IVN.TO":13.62,
"/K.TO":32.9,
"/KNT.TO":18.3,
"/KXS.TO":170.36,
"/LB.TO":33.61,
"/LNR.TO":76.35,
"/LUG.TO":96.46,
"/MEG.TO":29.82,
"/MEQ.TO":187.99,
"/MFC.TO":46.42,
"/MTL.TO":14.14,
"/NA.TO":158.21,
"/NGEX.TO":22.29,
"/NTR.TO":76.68,
"/NWC.TO":45.57,
"/NXE.TO":12.85,
"/OGC.TO":31.09,
"/ONEX.TO":120.08,
"/OTEX.TO":53.64,
"/PBH.TO":97.5,
"/PD.TO":84.51,
"/PEY.TO":20.72,
"/PKI.TO":39.98,
"/POW.TO":66.3,
"/PPTA.TO":31.84,
"/PSK.TO":25.1,
"/QBRb.TO":44.68,
"/RBA.TO":136.88,
"/RCIb.TO":54.05,
"/RUP.TO":5.61,
"/RY.TO":207.22,
"/SAP.TO":33.8,
"/SIA.TO":19.1,
"/SIS.TO":22.13,
"/SPB.TO":8.01,
"/SSRM.TO":32.18,
"/SU.TO":55.68,
"/SVI.TO":4.9,
"/SXGC.TO":8.09,
"/TA.TO":24.76,
"/TCW.TO":5.27,
"/TECKb.TO":60,
"/TFPM.TO":39.43,
"/TIH.TO":165.91,
"/TOU.TO":61.47,
"/TRP.TO":70.33,
"/TVE.TO":6.26,
"/VET.TO":10.54,
"/VZLA.TO":5.76,
"/WCP.TO":10.48,
"/WELL.TO":5.16,
"/WN.TO":85.32,
"/WPM.TO":136.6,
"/AC.TO":18.85,
"/ADEN.TO":35.37,
"/AFN.TO":36.09,
"/BHC.TO":9.6,
"/BLDP.TO":5.01,
"/CF.TO":11.77,
"/CMG.TO":5.33,
"/DNTL.TO":10.9,
"/D_u.TO":18.37,
"/GLXY.TO":49.05,
"/GRT_u.TO":78.66,
"/HUT.TO":77.37,
"/IIP_u.TO":13.38,
"/AG.TO":17.59,
"/MER.TO":1.82,
"/NOA.TO":21.69,
"/SII.TO":115.16,
"/AX_u.TO":6,
"/BEP_u.TO":44.29,
"/CAS.TO":11.14,
"/CFP.TO":12.22,
"/DCBO.TO":35.92,
"/DHT_u.TO":16.18,
"/HR_u.TO":11.19,
"/MATR.TO":10.79,
"/MRE.TO":10.25,
"/NG.TO":11.41,
"/OBE.TO":7.99,
"/SEA.TO":32.96,
"/WTE.TO":25.53,
}

# Get current timestamp for the output filename
timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

# Function to generate previous day's date
def get_previous_business_day(date_str):
    """Get previous business day (skip weekends)"""
    date_obj = datetime.datetime.strptime(date_str, "%Y%m%d")
    
    # Subtract one day
    prev_day = date_obj - datetime.timedelta(days=1)
    
    # Keep going back until we find a weekday (Monday=0, Sunday=6)
    while prev_day.weekday() > 4:  # Saturday=5, Sunday=6
        prev_day = prev_day - datetime.timedelta(days=1)
    
    return prev_day.strftime("%Y%m%d")
# List of mnemonics to process
mnemonics = [
    "CANPT",
    "ECC5P",
    "DNAPT",
    "DWREP",
    "EDWPT",
    "ELECP",
    "EBSPW",
    "EBSWP",
    "EGOPR",
    "EGHW",
    "EG60",
    "FILVP",
    "EGEL",
    "EGES",
    "ERGCP",
    "ERGSP",
    "EAIWP",
    "EBEWP",
    "BISWP",
    "CANP",
    "ECWPR",
    "ENDMP",
    "EDMPU",
    "DNAP",
    "EDWP",
    "EDWPE",
    "EDWSP",
    "EVEWP",
    "ENZTP",
    "FRD4P",
    "FRI4P",
    "EIFRP",
    "LC3WP",
    "NA500",
    "PABUP",
    "ERGBP",
    "ESECP",
    "ENVW",
    "ENTP",
    "ENWP",
    "EWAP",
    "EWBR",
    "EWCSP",
    "EWOG",
    "EWOU"
]

mnemonics_tr4_perc = {
    "ECC5D": "FR0013533569",
    "ECC3D": "FR0013533569",
    "ELED5": "NLIX00007566",
    "EGOD3": "FRESG0002864",
    "FILVD": "FRIX00002736",
    "FILV3": "FRIX00002736",
    "ERGBD": "NL0012645246",
    "ERGCD": "NL0012645212",
    "ERGSD": "NL0012939060",
    "FRD4D": "FRIX00003049",
    "FRI4D": "FRIX00003650",
    "LC3WD": "FR0013522596",
    "PABU5": "FRCLIM000155",
    "ESED5": "NLIX00005933"
    }
mnemonics_tr4_points = {
    "ELE50": "NLIX00007558",
    "FRID5": "FRIX00003668",
    "ESD5P": "NLIX00005941"
}
def load_data_with_encoding_fallback():
    """Load data with encoding fallback mechanism - combines US and EU files"""
    encodings = ['latin1', 'windows-1252', 'utf-8']
    
    # File date configuration
    current_stock_eod_date = "20251103"
    current_index_eod_date = "20251103"
    
    prev_stock_eod_date = get_previous_business_day(current_stock_eod_date)
    prev_index_eod_date = get_previous_business_day(current_index_eod_date)
    
    # Use only the primary path
    base_path = r"V:\PM-Indices-IndexOperations\General\Daily downloadfiles\Monthly Archive"
    
    for encoding in encodings:
        try:
            print(f"Trying to load data with {encoding} encoding...")
            
            # US File paths
            us_stock_eod_path = os.path.join(base_path, f"TTMIndexUS1_GIS_EOD_STOCK_{current_stock_eod_date}.csv")
            us_index_eod_path = os.path.join(base_path, f"TTMIndexUS1_GIS_EOD_INDEX_{current_index_eod_date}.csv")
            us_stock_eod_t1_path = os.path.join(base_path, f"TTMIndexUS1_GIS_EOD_STOCK_{prev_stock_eod_date}.csv")
            us_index_eod_t1_path = os.path.join(base_path, f"TTMIndexUS1_GIS_EOD_INDEX_{prev_index_eod_date}.csv")
            
            # EU File paths
            eu_stock_eod_path = os.path.join(base_path, f"TTMIndexEU1_GIS_EOD_STOCK_{current_stock_eod_date}.csv")
            eu_index_eod_path = os.path.join(base_path, f"TTMIndexEU1_GIS_EOD_INDEX_{current_index_eod_date}.csv")
            eu_stock_eod_t1_path = os.path.join(base_path, f"TTMIndexEU1_GIS_EOD_STOCK_{prev_stock_eod_date}.csv")
            eu_index_eod_t1_path = os.path.join(base_path, f"TTMIndexEU1_GIS_EOD_INDEX_{prev_index_eod_date}.csv")
            
            # Load US dataframes
            us_stock_eod_df = pd.read_csv(us_stock_eod_path, sep=';', encoding=encoding)
            us_index_eod_df = pd.read_csv(us_index_eod_path, sep=';', encoding=encoding)
            us_stock_eod_df_t1 = pd.read_csv(us_stock_eod_t1_path, sep=';', encoding=encoding)
            us_index_eod_df_t1 = pd.read_csv(us_index_eod_t1_path, sep=';', encoding=encoding)
            
            # Load EU dataframes
            eu_stock_eod_df = pd.read_csv(eu_stock_eod_path, sep=';', encoding=encoding)
            eu_index_eod_df = pd.read_csv(eu_index_eod_path, sep=';', encoding=encoding)
            eu_stock_eod_df_t1 = pd.read_csv(eu_stock_eod_t1_path, sep=';', encoding=encoding)
            eu_index_eod_df_t1 = pd.read_csv(eu_index_eod_t1_path, sep=';', encoding=encoding)
            
            # Combine US and EU dataframes
            stock_eod_df = pd.concat([us_stock_eod_df, eu_stock_eod_df], ignore_index=True)
            index_eod_df = pd.concat([us_index_eod_df, eu_index_eod_df], ignore_index=True)
            stock_eod_df_t1 = pd.concat([us_stock_eod_df_t1, eu_stock_eod_df_t1], ignore_index=True)
            index_eod_df_t1 = pd.concat([us_index_eod_df_t1, eu_index_eod_df_t1], ignore_index=True)
            
            print(f"Successfully loaded and combined US and EU data with {encoding} encoding")
            print(f"Combined data sizes:")
            print(f"  Stock EOD: {len(stock_eod_df)} rows")
            print(f"  Index EOD: {len(index_eod_df)} rows")
            print(f"  Stock EOD T-1: {len(stock_eod_df_t1)} rows")
            print(f"  Index EOD T-1: {len(index_eod_df_t1)} rows")
            
            return stock_eod_df, index_eod_df, stock_eod_df_t1, index_eod_df_t1
            
        except Exception as e:
            print(f"Failed to load with {encoding} encoding: {e}")
            continue
    
    raise Exception("Failed to load data with any encoding")

def update_stock_prices(stock_eod_df):
    """Update stock prices using the stock_prices dictionary"""
    def update_close_price(row):
        symbol = str(row['#Symbol']).strip() if not pd.isna(row['#Symbol']) else ""
        if symbol in stock_prices:
            return stock_prices[symbol]
        else:
            return row['Close Prc']
    
    original_prices = stock_eod_df['Close Prc'].copy()
    stock_eod_df['Close Prc'] = stock_eod_df.apply(update_close_price, axis=1)
    
    updated_count = (original_prices != stock_eod_df['Close Prc']).sum()
    print(f"Updated {updated_count} price records from the stock_prices dictionary")
    
    return stock_eod_df

def calculate_index_levels(stock_eod_df, index_eod_df, index_eod_df_t1, mnemonics):
    """Calculate index levels for the given mnemonics"""
    # Calculate new_Index_Mcap
    stock_eod_df['new_Index_Mcap'] = (
        stock_eod_df['Close Prc'] * 
        stock_eod_df['Shares'] * 
        stock_eod_df['Free float-Coeff'] * 
        stock_eod_df['Capping Factor-Coeff'] * 
        stock_eod_df['FX/Index Ccy']
    )
    
    # Create lookup dictionaries
    isin_lookup = dict(zip(index_eod_df['Mnemo'], index_eod_df['IsinCode']))
    gross_isin_lookup = dict(zip(index_eod_df['IsinCode'], index_eod_df['ISIN Gross Return version']))
    net_isin_lookup = dict(zip(index_eod_df['IsinCode'], index_eod_df['ISIN Net Return version']))
    gross_mass_lookup = dict(zip(index_eod_df['IsinCode'], index_eod_df['Effect Gross Total Return']))
    net_mass_lookup = dict(zip(index_eod_df['IsinCode'], index_eod_df['Effect Net Total Return ']))
    
    # Create t-1 lookup dictionaries
    price_t1_lookup = {}
    gross_t1_lookup = {}
    net_t1_lookup = {}
    
    for idx, row in index_eod_df_t1.iterrows():
        if not pd.isna(row['IsinCode']):
            t1_value = row['t0 IV unround'] if 't0 IV unround' in index_eod_df_t1.columns else None
            price_t1_lookup[row['IsinCode']] = t1_value
            gross_t1_lookup[row['IsinCode']] = t1_value
            net_t1_lookup[row['IsinCode']] = t1_value
    
    # Calculate results
    results = []
    for mnemo in mnemonics:
        filtered_df = stock_eod_df[stock_eod_df['Index'] == mnemo]
        total_mcap = filtered_df['new_Index_Mcap'].sum()
        
        divisor_row = index_eod_df[index_eod_df['Mnemo'] == mnemo]
        divisor = divisor_row['Divisor'].values[0] if not divisor_row.empty else None
        
        price_level = total_mcap / divisor if divisor is not None and divisor != 0 else None
        price_level_round = round(price_level, 8) if price_level is not None else None
        
        price_isin = isin_lookup.get(mnemo, None)
        gross_isin = gross_isin_lookup.get(price_isin, None) if price_isin else None
        net_isin = net_isin_lookup.get(price_isin, None) if price_isin else None
        
        gross_mass = gross_mass_lookup.get(gross_isin, None) if gross_isin else None
        net_mass = net_mass_lookup.get(net_isin, None) if net_isin else None
        
        price_t1 = price_t1_lookup.get(price_isin, None) if price_isin else None
        gross_t1 = gross_t1_lookup.get(gross_isin, None) if gross_isin else None
        net_t1 = net_t1_lookup.get(net_isin, None) if net_isin else None
        
        gross_level_unrounded = None
        net_level_unrounded = None
        gross_level = None
        net_level = None
        
        # Calculate gross level if gross_t1 is available
        if price_t1 is not None and price_t1 != 0 and gross_t1 is not None:
            if price_level_round is not None and gross_mass is not None:
                gross_level_unrounded = gross_t1 * ((price_level_round + gross_mass) / price_t1)
                gross_level = round(gross_level_unrounded, 8)

        # Calculate net level if net_t1 is available (independent of gross_t1)
        if price_t1 is not None and price_t1 != 0 and net_t1 is not None:
            if price_level_round is not None and net_mass is not None:
                net_level_unrounded = net_t1 * ((price_level_round + net_mass) / price_t1)
                net_level = round(net_level_unrounded, 8)
        
        results.append({
            'Index': mnemo, 
            'Total_Index_Mcap': total_mcap,
            'Divisor': divisor,
            'Price_Level': price_level,
            'Price_Level_Round': price_level_round,
            'Price_Isin': price_isin,
            'Price_t-1': price_t1,
            'Gross_Isin': gross_isin,
            'Net_Isin': net_isin,
            'Gross_Mass': gross_mass,
            'Net_Mass': net_mass,
            'Gross_t-1': gross_t1,
            'Net_t-1': net_t1,
            'Gross_Level_Unrounded': gross_level_unrounded,
            'Gross_Level': gross_level,
            'Net_Level_Unrounded': net_level_unrounded,
            'Net_Level': net_level
        })

    return pd.DataFrame(results)

def calculate_decrement_level(mnemo, isin, index_eod_df, index_eod_df_t1, results_df, current_date, prev_date):
    """Calculate decrement level using the formula: DIt = DIt−1 * (UIt / UIt−1 - Dcr * day / yearly_days)"""
    result = {
        'Mnemo': mnemo,
        'ISIN': isin,
        'DIt_1': None,
        'UIt_1': None,
        'UIt': None,
        'Dcr': None,
        'Day': None,
        'Yearly_Days': None,
        'Ratio_UIt_UIt_1': None,
        'Decrement_Factor': None,
        'Decrement_Level': None,
        'Error_Message': None
    }
    
    try:
        # Get DIt-1
        dit_1_row = index_eod_df_t1[index_eod_df_t1['Mnemo'] == mnemo]
        if not dit_1_row.empty and 't0 IV unround' in index_eod_df_t1.columns:
            result['DIt_1'] = dit_1_row['t0 IV unround'].values[0]
        else:
            result['Error_Message'] = f"DIt-1 not found for mnemo {mnemo}"
        
        # Get UIt-1 - Look up the ISIN from dictionary in results_df
        underlying_level_t1 = None
        underlying_level = None
        
        # First try to find the isin in Gross_Isin column
        gross_match = results_df[results_df['Gross_Isin'] == isin]
        if not gross_match.empty:
            underlying_level_t1 = gross_match['Gross_t-1'].iloc[0]
            underlying_level = gross_match['Gross_Level'].iloc[0]
        else:
            # If not found in Gross_Isin, try Net_Isin
            net_match = results_df[results_df['Net_Isin'] == isin]
            if not net_match.empty:
                underlying_level_t1 = net_match['Net_t-1'].iloc[0]
                underlying_level = net_match['Net_Level'].iloc[0]
        
        # If still not found, fallback to direct lookup
        if underlying_level_t1 is None:
            uit_1_row = index_eod_df_t1[index_eod_df_t1['IsinCode'] == isin]
            if not uit_1_row.empty and 't0 IV unround' in index_eod_df_t1.columns:
                underlying_level_t1 = uit_1_row['t0 IV unround'].values[0]
        
        result['UIt_1'] = underlying_level_t1
        if underlying_level_t1 is None:
            result['Error_Message'] = f"UIt-1 not found for ISIN {isin}"
        
        # Get UIt - use the value found above or fallback to direct lookup
        if underlying_level is None:
            uit_current_row = index_eod_df[index_eod_df['IsinCode'] == isin]
            if not uit_current_row.empty and 't0 IV unround' in index_eod_df.columns:
                underlying_level = uit_current_row['t0 IV unround'].values[0]
        
        result['UIt'] = underlying_level
        if underlying_level is None:
            result['Error_Message'] = f"UIt not found for ISIN {isin}"
        
        # Get Dcr
        dcr_row = index_eod_df[index_eod_df['Mnemo'] == mnemo]
        if not dcr_row.empty and 'Return Value' in index_eod_df.columns:
            result['Dcr'] = dcr_row['Return Value'].values[0]
        else:
            result['Error_Message'] = f"Dcr (Return Value) not found for mnemo {mnemo}"
        
        # Get Yearly Days
        yearly_days_row = index_eod_df[index_eod_df['Mnemo'] == mnemo]
        if not yearly_days_row.empty and 'Yearly Days' in index_eod_df.columns:
            result['Yearly_Days'] = yearly_days_row['Yearly Days'].values[0]
        else:
            result['Error_Message'] = f"Yearly Days not found for mnemo {mnemo}"
            result['Yearly_Days'] = 365
        
        # Calculate day from file dates
        result['Day'] = (current_date - prev_date).days
        
        # Calculate intermediate values
        if result['UIt'] is not None and result['UIt_1'] is not None and result['UIt_1'] != 0:
            result['Ratio_UIt_UIt_1'] = result['UIt'] / result['UIt_1']
        
        if result['Dcr'] is not None and result['Day'] is not None and result['Yearly_Days'] is not None:
            result['Decrement_Factor'] = result['Dcr'] * result['Day'] / result['Yearly_Days']
        
        # Calculate final decrement level
        if all(val is not None for val in [result['DIt_1'], result['Ratio_UIt_UIt_1'], result['Decrement_Factor']]):
            if result['Ratio_UIt_UIt_1'] >= result['Decrement_Factor']:
                dit = result['DIt_1'] * (result['Ratio_UIt_UIt_1'] - result['Decrement_Factor'])
                result['Decrement_Level'] = round(dit, 8)
            else:
                result['Error_Message'] = "Ratio smaller than decrement factor - would result in negative level"
        else:
            missing_vals = [k for k, v in result.items() if k in ['DIt_1', 'Ratio_UIt_UIt_1', 'Decrement_Factor'] and v is None]
            result['Error_Message'] = f"Missing values for calculation: {missing_vals}"
            
    except Exception as e:
        result['Error_Message'] = f"Exception: {str(e)}"
    
    return result

def calculate_decrement_points_level(mnemo, isin, index_eod_df, index_eod_df_t1, results_df, current_date, prev_date):
    """Calculate decrement points level using the formula: DPIt = DPIt−1 * (DuRt / DuRt−1 - Points * day / yearly_days)"""
    result = {
        'Mnemo': mnemo,
        'ISIN': isin,
        'Underlying_Index': None,
        'DPIt_1': None,
        'DuRt_1': None,
        'DuRt': None,
        'Points': None,
        'Day': None,
        'Yearly_Days': None,
        'Ratio_DuRt_DuRt_1': None,
        'Points_Factor': None,
        'Decrement_Points_Level': None,
        'Error_Message': None
    }
    
    try:
        # Get Underlying Index
        underlying_index_row = index_eod_df[index_eod_df['Mnemo'] == mnemo]
        if not underlying_index_row.empty and 'ISIN Underlying Price Index' in index_eod_df.columns:
            result['Underlying_Index'] = underlying_index_row['ISIN Underlying Price Index'].values[0]
        else:
            result['Underlying_Index'] = None
        
        # Get DPIt-1
        dpit_1_row = index_eod_df_t1[index_eod_df_t1['Mnemo'] == mnemo]
        if not dpit_1_row.empty and 't0 IV unround' in index_eod_df_t1.columns:
            result['DPIt_1'] = dpit_1_row['t0 IV unround'].values[0]
        else:
            result['Error_Message'] = f"DPIt-1 not found for mnemo {mnemo}"
        
        # Get DuRt-1 - Look up the ISIN from dictionary in results_df
        underlying_level_t1 = None
        underlying_level = None
        
        # First try to find the isin in Gross_Isin column
        gross_match = results_df[results_df['Gross_Isin'] == isin]
        if not gross_match.empty:
            underlying_level_t1 = gross_match['Gross_t-1'].iloc[0]
            underlying_level = gross_match['Gross_Level'].iloc[0]
        else:
            # If not found in Gross_Isin, try Net_Isin
            net_match = results_df[results_df['Net_Isin'] == isin]
            if not net_match.empty:
                underlying_level_t1 = net_match['Net_t-1'].iloc[0]
                underlying_level = net_match['Net_Level'].iloc[0]
        
        # If still not found, fallback to direct lookup
        if underlying_level_t1 is None:
            durt_1_row = index_eod_df_t1[index_eod_df_t1['IsinCode'] == isin]
            if not durt_1_row.empty and 't0 IV unround' in index_eod_df_t1.columns:
                underlying_level_t1 = durt_1_row['t0 IV unround'].values[0]
        
        result['DuRt_1'] = underlying_level_t1
        if underlying_level_t1 is None:
            result['Error_Message'] = f"DuRt-1 not found for ISIN {isin}"
        
        # Get DuRt - use the value found above or fallback to direct lookup
        if underlying_level is None:
            durt_current_row = index_eod_df[index_eod_df['IsinCode'] == isin]
            if not durt_current_row.empty and 't0 IV unround' in index_eod_df.columns:
                underlying_level = durt_current_row['t0 IV unround'].values[0]
        
        result['DuRt'] = underlying_level
        if underlying_level is None:
            result['Error_Message'] = f"DuRt not found for ISIN {isin}"
        
        # Get Points
        points_row = index_eod_df[index_eod_df['Mnemo'] == mnemo]
        if not points_row.empty and 'Return Value' in index_eod_df.columns:
            result['Points'] = points_row['Return Value'].values[0]
        else:
            result['Error_Message'] = f"Points (Return Value) not found for mnemo {mnemo}"
        
        # Get Yearly Days
        yearly_days_row = index_eod_df[index_eod_df['Mnemo'] == mnemo]
        if not yearly_days_row.empty and 'Yearly Days' in index_eod_df.columns:
            result['Yearly_Days'] = yearly_days_row['Yearly Days'].values[0]
        else:
            result['Error_Message'] = f"Yearly Days not found for mnemo {mnemo}"
            result['Yearly_Days'] = 365
        
        # Calculate day from file dates
        result['Day'] = (current_date - prev_date).days
        
        # Calculate intermediate values
        if result['DuRt'] is not None and result['DuRt_1'] is not None and result['DuRt_1'] != 0:
            result['Ratio_DuRt_DuRt_1'] = result['DuRt'] / result['DuRt_1']
        
        if result['Points'] is not None and result['Day'] is not None and result['Yearly_Days'] is not None:
            result['Points_Factor'] = result['Points'] * result['Day'] / result['Yearly_Days']
        
        # Calculate final decrement points level
        if all(val is not None for val in [result['DPIt_1'], result['Ratio_DuRt_DuRt_1'], result['Points_Factor']]):
            if result['Ratio_DuRt_DuRt_1'] >= result['Points_Factor']:
                dpit = result['DPIt_1'] * result['Ratio_DuRt_DuRt_1'] - result['Points_Factor']
                result['Decrement_Points_Level'] = round(dpit, 8)
            else:
                result['Error_Message'] = "Ratio smaller than points factor - would result in negative level"
        else:
            missing_vals = [k for k, v in result.items() if k in ['DPIt_1', 'Ratio_DuRt_DuRt_1', 'Points_Factor'] and v is None]
            result['Error_Message'] = f"Missing values for calculation: {missing_vals}"
            
    except Exception as e:
        result['Error_Message'] = f"Exception: {str(e)}"
    
    return result

def save_results_to_excel(results_df, decrement_df, decrement_points_df, stock_eod_df, stock_eod_df_t1, index_eod_df, index_eod_df_t1, timestamp):
    """Save all results to Excel with proper sheet names"""
    output_path = fr"C:\Users\CSonneveld\OneDrive - Euronext\Documents\Projects\Modular Recalc Script\Output\Decr_Recalc_{timestamp}.xlsx"
    
    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        results_df.to_excel(writer, sheet_name='Index_Totals', index=False)
        decrement_df.to_excel(writer, sheet_name='Decrement_Levels_Percentage', index=False)
        decrement_points_df.to_excel(writer, sheet_name='Decrement_Levels_Points', index=False)
        stock_eod_df.to_excel(writer, sheet_name='Stock_EOD_Data', index=False)
        stock_eod_df_t1.to_excel(writer, sheet_name='Stock_EOD_Data_T-1', index=False)
        index_eod_df.to_excel(writer, sheet_name='Index_EOD_Data', index=False)
        index_eod_df_t1.to_excel(writer, sheet_name='Index_EOD_Data_T-1', index=False)
    
    print("Results saved to:", output_path)
    print("Summary of results:")
    print("Index Totals:", len(results_df), "rows")
    print("Decrement Percentage Levels:", len(decrement_df), "rows")
    print("Decrement Points Levels:", len(decrement_points_df), "rows")
    
    return output_path

# Main execution
try:
    # Define dates at the start
    current_stock_eod_date = "20251103"
    current_index_eod_date = "20251103"
    prev_stock_eod_date = get_previous_business_day(current_stock_eod_date)
    prev_index_eod_date = get_previous_business_day(current_index_eod_date)
    
    # Convert to datetime objects for day calculation
    current_date = datetime.datetime.strptime(current_stock_eod_date, "%Y%m%d")
    prev_date = datetime.datetime.strptime(prev_stock_eod_date, "%Y%m%d")
    
    # Load data
    stock_eod_df, index_eod_df, stock_eod_df_t1, index_eod_df_t1 = load_data_with_encoding_fallback()
    
    # Update stock prices
    stock_eod_df = update_stock_prices(stock_eod_df)
    
    # Calculate index levels
    results_df = calculate_index_levels(stock_eod_df, index_eod_df, index_eod_df_t1, mnemonics)
    
    # Calculate decrement percentage levels
    print("Calculating decrement levels for TR4 percentage indices...")
    decrement_results = []
    for mnemo, isin in mnemonics_tr4_perc.items():
        result = calculate_decrement_level(mnemo, isin, index_eod_df, index_eod_df_t1, results_df, current_date, prev_date)
        decrement_results.append(result)
    
    decrement_df = pd.DataFrame(decrement_results)
    print(f"Calculated decrement levels for {len(decrement_df)} indices")
    print(f"Successfully calculated levels: {decrement_df['Decrement_Level'].notna().sum()}")
    
    # Calculate decrement points levels
    print("Calculating decrement points levels for TR4 points indices...")
    decrement_points_results = []
    for mnemo, isin in mnemonics_tr4_points.items():
        result = calculate_decrement_points_level(mnemo, isin, index_eod_df, index_eod_df_t1, results_df, current_date, prev_date)
        decrement_points_results.append(result)
    
    decrement_points_df = pd.DataFrame(decrement_points_results)
    print(f"Calculated decrement points levels for {len(decrement_points_df)} indices")
    print(f"Successfully calculated points levels: {decrement_points_df['Decrement_Points_Level'].notna().sum()}")
    
    # Save all results
    output_path = save_results_to_excel(results_df, decrement_df, decrement_points_df, stock_eod_df, stock_eod_df_t1, index_eod_df, index_eod_df_t1, timestamp)
    
    print("Script completed successfully!")
    
except Exception as e:
    print(f"Script failed with error: {e}")
    import traceback
    traceback.print_exc()