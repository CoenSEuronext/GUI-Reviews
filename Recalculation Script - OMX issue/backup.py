import pandas as pd
import datetime
import os

# Stock prices dictionary
stock_prices = {
    "AACM.ST": 109.4,
    "AAK.ST": 260.6,
    "SAGAd.ST": 33.5,
    "SAGAb.ST": 210.8,
    "ACADE.ST": 89.2,
    "ALIFb.ST": 172.7,
    "ANODb.ST": 112.8,
    "ADDTb.ST": 330.8,
    "ADMCM.HE": 50.2,
    "AFRY.ST": 157.2,
    "AKTIA.HE": 10.14,
    "ALFA.ST": 424.6,
    "ALIG.ST": 168.2,
    "ALLEI.ST": 70.3,
    "ALLIGOb.ST": 107.4,
    "AMBEA.ST": 123.1,
    "APOTEA.ST": 116.62,
    "AQ.ST": 198.9,
    "ARJOb.ST": 34.8,
    "ASKER.ST": 103.84,
    "ASMDEEb.ST": 123.92,
    "ASSAb.ST": 324.8,
    "ATCOa.ST": 152.75,
    "ATCOb.ST": 136.1,
    "ATRLJb.ST": 33.005,
    "AVANZ.ST": 352.7,
    "AXFO.ST": 294.8,
    "BEIAb.ST": 257,
    "BEIJb.ST": 166.7,
    "BERGb.ST": 315.5,
    "BRKa.N": 713704.75,
    "BETSb.ST": 166.3,
    "BETCO.ST": 139.6,
    "BHGF.ST": 24.6,
    "BILIa.ST": 117.5,
    "BIOAb.ST": 210.8,
    "BIOGb.ST": 98.2,
    "BOL.ST": 309.7,
    "BONAVb.ST": 11.5,
    "BONEX.ST": 335.4,
    "BOOZT.ST": 88.35,
    "BRAV.ST": 92.15,
    "BTSb.ST": 207.5,
    "BUFAB.ST": 97.97,
    "BURE.ST": 293.2,
    "BMAX.ST": 60,
    "CAMX.ST": 709.5,
    "CAPMAN.HE": 1.954,
    "CAST.ST": 113.25,
    "CATE.ST": 458.8,
    "CEVI.ST": 177.8,
    "LEU.N": 224.08,
    "CRNOSb.ST": 9.53,
    "CIBUS.ST": 175.65,
    "CINT.ST": 7.325,
    "CTY1S.HE": 3.76,
    "CLOEb.ST": 31.68,
    "COOR.ST": 46.74,
    "COREb.ST": 4.474,
    "CTTS.ST": 214,
    "DIOS.ST": 65.25,
    "DOMETIC.ST": 50.8,
    "DUNI.ST": 91.5,
    "DUST.ST": 1.674,
    "DYVOX.ST": 127.5,
    "EAST9.ST": 48.5,
    "ELUXb.ST": 61.96,
    "EPROb.ST": 67.3,
    "EKTAb.ST": 49.18,
    "ELISA.HE": 45.96,
    "EMBRACb.ST": 103,
    "ENENTO.HE": 15.56,
    "ENGCONb.ST": 76.8,
    "EPEND.ST": 119.2,
    "EPIRa.ST": 204.5,
    "EPIRb.ST": 180.7,
    "EQTAB.ST": 331.6,
    "ERICb.ST": 71.6,
    "ESSITYb.ST": 245,
    "EVOG.ST": 877.4,
    "FSECURE.HE": 1.668,
    "FABG.ST": 83.4,
    "FAG.ST": 38,
    "FARON.HE": 2.255,
    "BALDb.ST": 68.48,
    "EMILb.ST": 50.4,
    "FPARa.ST": 51.1,
    "FIA1S.HE": 2.924,
    "FORTUM.HE": 16.44,
    "GRANG.ST": 135.5,
    "GREENL.ST": 57.9,
    "GRK.HE": 14.28,
    "HANZA.ST": 108.6,
    "HARVIA.HE": 51.6,
    "HAYPP.ST": 146.4,
    "HEBAb.ST": 31.2,
    "HEM.ST": 283,
    "HMb.ST": 136.7,
    "HEXAb.ST": 110.55,
    "HTRO.ST": 21.09,
    "HPOLb.ST": 85.6,
    "HIAB.HE": 59.85,
    "HMSN.ST": 420,
    "HOFI.ST": 97.1,
    "HOLMb.ST": 375.8,
    "HUFVa.ST": 119.8,
    "HUH1V.HE": 31.12,
    "HUMBLE.ST": 7.95,
    "HUSQb.ST": 55.02,
    "IDUNb.ST": 388,
    "IMPsdba.ST": 70.2,
    "INDUc.ST": 370.7,
    "INDUa.ST": 371,
    "INDT.ST": 242,
    "INSTAL.ST": 26.3,
    "INTEAb.ST": 77.1,
    "INTE.ST": 125.4,
    "IPCOR.ST": 167.7,
    "INTRUM.ST": 56.18,
    "ORES.ST": 130.4,
    "INVEb.ST": 291.35,
    "IVSO.ST": 321.5,
    "INWI.ST": 183.3,
    "ITAB.ST": 17.82,
    "JM.ST": 145.8,
    "KALMAR.HE": 39.1,
    "KAMBI.ST": 141.1,
    "KARNELb.ST": 62,
    "KARNO.ST": 120,
    "KEMIRA.HE": 19.06,
    "KEMPOWR.HE": 16.69,
    "KESKOB.HE": 19.42,
    "KINVb.ST": 89.82,
    "KNOW.ST": 119.8,
    "KOJAMO.HE": 10.98,
    "KNEBV.HE": 54.58,
    "KCRA.HE": 74.3,
    "LAGRb.ST": 229.8,
    "LATOb.ST": 255.2,
    "LIFCOb.ST": 352,
    "LIMET.ST": 382.5,
    "LIAB.ST": 214.8,
    "LOGIb.ST": 16.4,
    "LOOMIS.ST": 386,
    "LUNDb.ST": 488,
    "MANTA.HE": 5.926,
    "MEKKO.HE": 13.34,
    "MEDCAP.ST": 642,
    "MCOVb.ST": 279,
    "MEKO.ST": 88.8,
    "METSB.HE": 3.146,
    "METSO.HE": 11.345,
    "MILDEF.ST": 173.9,
    "MIPS.ST": 418,
    "MTGb.ST": 102.7,
    "MMGRb.ST": 158.4,
    "MTRS.ST": 137,
    "MYCR.ST": 213.15,
    "NCAB.ST": 54.45,
    "NCCb.ST": 186.6,
    "NESTE.HE": 14.09,
    "NEWAb.ST": 121.4,
    "NIBEb.ST": 44.89,
    "NOBI.ST": 4.728,
    "NOKIA.HE": 3.592,
    "TYRES.HE": 8,
    "NOLAb.ST": 59,
    "NDAFI.HE": 12.77,
    "SAVE.ST": 264,
    "NORION.ST": 61.8,
    "NOTE.ST": 191.2,
    "NP3.ST": 267.5,
    "NYFO.ST": 86.7,
    "OEMb.ST": 136.4,
    "OLVAS.HE": 34,
    "ORNBV.HE": 70.55,
    "OUT1V.HE": 3.452,
    "PANDXb.ST": 176.2,
    "PDXI.ST": 168.7,
    "PEABb.ST": 74.4,
    "PIHLIS.HE": 16.35,
    "PLAZb.ST": 70.7,
    "PACT.ST": 97.5,
    "PUUILO.HE": 13.72,
    "QTCOM.HE": 60.6,
    "RAIVV.HE": 2.525,
    "RATOb.ST": 35.12,
    "RAYb.ST": 356.5,
    "REJLb.ST": 201,
    "REG1V.HE": 28.35,
    "RUSTA.ST": 78.15,
    "RVRC.ST": 44.94,
    "SAABb.ST": 525.8,
    "SAMPO.HE": 9.406,
    "SAND.ST": 243.1,
    "SBBb.ST": 5.036,
    "SCST.ST": 93.5,
    "SHOTE.ST": 81.45,
    "SDIPb.ST": 200.2,
    "SEBc.ST": 174.2,
    "SECTb.ST": 369,
    "SECUb.ST": 147,
    "SINCH.ST": 34.4,
    "SEBa.ST": 172.05,
    "SKAb.ST": 232.5,
    "SKFb.ST": 233.2,
    "SEYE.ST": 65.3,
    "SSABa.ST": 58.14,
    "SSABb.ST": 57.18,
    "SFRG.ST": 5.235,
    "LINDEX.HE": 2.845,
    "STERV.HE": 9.444,
    "STORb.ST": 11.935,
    "STORYb.ST": 85.35,
    "SUSW.ST": 152.6,
    "SVEAF.ST": 37.9,
    "SCAb.ST": 126.7,
    "SHBb.ST": 191.2,
    "SHBa.ST": 119.6,
    "SWECb.ST": 156.2,
    "SWEDa.ST": 259.2,
    "SECARE.ST": 38.65,
    "SLPb.ST": 40.15,
    "SOBIV.ST": 282.6,
    "SYNSAM.ST": 54.2,
    "SYSR.ST": 95.4,
    "TEL2b.ST": 151.45,
    "TELIA.ST": 35.08,
    "TTALO.HE": 10.77,
    "TFBANK.ST": 133.8,
    "THULE.ST": 282.8,
    "TIETO.HE": 15.23,
    "TOKMAN.HE": 8.795,
    "TRELb.ST": 358.4,
    "TROAX.ST": 136.6,
    "TRUEb.ST": 50.15,
    "UPM.HE": 23.99,
    "VAIAS.HE": 47.45,
    "VALMT.HE": 31.38,
    "VBGb.ST": 285.6,
    "VERVE.ST": 30.64,
    "VESTUM.ST": 9.72,
    "VIMIAN.ST": 34.34,
    "VITb.ST": 388,
    "VITR.ST": 143.3,
    "VOLVb.ST": 284.4,
    "VOLVa.ST": 284.6,
    "VOLCARb.ST": 19.535,
    "WALLb.ST": 44.9,
    "WRT1V.HE": 24.14,
    "WIHL.ST": 97.65,
    "XVIVO.ST": 198.2,
    "YIT.HE": 3.114,
    "YUBICO.ST": 135.8,
    "ZZb.ST": 250,
    "ADDVb.ST": 1.466,
    "BICO.ST": 38.3,
    "BITTI.HE": 9.79,
    "BRE2.ST": 3.265,
    "COREpref.ST": 245.5,
    "FG.ST": 30.9,
    "FINGb.ST": 0.0096,
    "FLERIE.ST": 43.4,
    "GENOb.ST": 27.75,
    "GOFORE.HE": 18.08,
    "HNSA.ST": 28.07,
    "IARb.ST": 178,
    "ICP1V.HE": 10.62,
    "INVEa.ST": 291.2,
    "JOMA.ST": 62.6,
    "KFASTb.ST": 16.76,
    "LIC.ST": 79.4,
    "MUSTI.HE": 20.55,
    "NANOFH.HE": 1.034,
    "NDASE.ST": 12.77,
    "NEOBO.ST": 17.16,
    "NETIb.ST": 3.985,
    "OMASP.HE": 8.96,
    "ORRON.ST": 5.015,
    "RESURS.ST": 28,
    "SANOMA.HE": 9.6,
    "SCENS.ST": 0.785,
    "SPEC.ST": 0.1688,
    "SSABBH.HE": 57.18,
    "TEQ.ST": 156,
    "VNV.ST": 20.18,
    "VPLAYb.ST": 0.9122
}

# Get current timestamp for the output filename
timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

# Function to generate previous day's date
def get_previous_day(date_str):
    # Convert string date to datetime object
    date_obj = datetime.datetime.strptime(date_str, "%Y%m%d")
    # Subtract one day
    prev_day = date_obj - datetime.timedelta(days=1)
    # Return as string in same format
    return prev_day.strftime("%Y%m%d")

# List of mnemonics to process
mnemonics = [
    "ENVEO", "ENVEU", "LC100", "EZWTP", "SES5P", "BANK", "EEEPR", "EESGP", 
    "WATPR", "EZENP", "EZ300", "EU500", "ERI5P", "ESG1P", "ECO5P", "EZN1P", 
    "EZ15P", "EZ60P", "ES1EP", "CLE5P", "ESE4P", "ESG50", "ECOEW", "ECOP", 
    "ENCLE", "ESE30", "EITD", "EFESP", "EESO", "EENS", "ESAL", "EZ50P", 
    "EDEFP", "EETAP", "EETEP", "EUADP", "BREU", "ESVEP", "DEREP", "EZSCP", 
    "UTIL", "TELEP", "INDU", "HEAC", "FINA", "ENRGP", "CSTA", "BASM", 
    "DEZPT", "DEUPT", "DEZP", "DEUP", "EZCLA", "CLAMP", "ESGCP", "EZSFP", 
    "EPABP", "GOVEP", "LC1EP", "ESGEP", "ENESG", "GRE5P", "EESAP", "TERPR", 
    "BIOCP", "ESBTP", "ZSBTP", "EETPR", "EQGEP", "ES4PP", "EZ6PP", "EBLPP", 
    "EBSEP", "EGSPP", "EPSP", "ENVW", "ENDMP", "ENWP", "ETE5P", "WESGP", 
    "ESGTP", "WLENP", "EG60", "EGHW", "DWREP", "TUTI", "TBMA", "THEC", 
    "TTEL", "TIND", "TFINP", "TCDI", "TCST", "TENR", "TTEC", "AERDP", 
    "EDWPT", "EDWP", "ENZTP", "HSPCP", "HSPAP", "ENTP", "EDMPU", "EDWEP", 
    "TESGP", "ECC5P", "LC3WP", "WIFRP", "FILVP", "EIFRP", "EAIWP", "EGOPR", 
    "BISWP", "EBEWP", "EBSTP", "EBSPW", "EBSWP", "PBTAP", "TPABP"
]


# Try with different encodings
try:
    # Current day files (T)
    current_stock_eod_date = "20250729"
    current_index_eod_date = "20250729"
    current_stock_sod_date = "20250730"
    current_index_sod_date = "20250730" 

    # Previous day files (T-1)
    prev_stock_eod_date = get_previous_day(current_stock_eod_date)
    prev_index_eod_date = get_previous_day(current_index_eod_date)
    
    # Base path
    base_path = r"V:\PM-Indices-IndexOperations\General\Daily downloadfiles\Monthly Archive"
    
    # Current day file paths
    stock_eod_path = os.path.join(base_path, f"TTMIndexUS1_GIS_EOD_STOCK_{current_stock_eod_date}.csv")
    index_eod_path = os.path.join(base_path, f"TTMIndexUS1_GIS_EOD_INDEX_{current_index_eod_date}.csv")
    stock_sod_path = os.path.join(base_path, f"TTMIndexUS1_GIS_SOD_STOCK_{current_stock_sod_date}.csv")
    index_sod_path = os.path.join(base_path, f"TTMIndexUS1_GIS_SOD_INDEX_{current_index_sod_date}.csv")
    
    # Previous day file paths
    stock_eod_t1_path = os.path.join(base_path, f"TTMIndexUS1_GIS_EOD_STOCK_{prev_stock_eod_date}.csv")
    index_eod_t1_path = os.path.join(base_path, f"TTMIndexUS1_GIS_EOD_INDEX_{prev_index_eod_date}.csv")
    
    # First try with 'latin1' encoding (also known as ISO-8859-1)
    # Current day dataframes
    stock_eod_df = pd.read_csv(stock_eod_path, sep=';', encoding='latin1')
    index_eod_df = pd.read_csv(index_eod_path, sep=';', encoding='latin1')
    stock_sod_df = pd.read_csv(stock_sod_path, sep=';', encoding='latin1')
    index_sod_df = pd.read_csv(index_sod_path, sep=';', encoding='latin1')
    
    # Previous day dataframes
    stock_eod_df_t1 = pd.read_csv(stock_eod_t1_path, sep=';', encoding='latin1')
    index_eod_df_t1 = pd.read_csv(index_eod_t1_path, sep=';', encoding='latin1')
    
    # Print some debugging information
    print("First few rows of stock_eod_df:")
    print(stock_eod_df[['#Symbol', 'Index', 'Close Prc']].head())
    
    # Update Close Prc values using the stock_prices dictionary
    def update_close_price(row):
        symbol = str(row['#Symbol']).strip() if not pd.isna(row['#Symbol']) else ""
        if symbol in stock_prices:
            return stock_prices[symbol]
        else:
            return row['Close Prc']  # Keep original price if not found in dictionary
    
    # Apply the price update
    original_prices = stock_eod_df['Close Prc'].copy()
    stock_eod_df['Close Prc'] = stock_eod_df.apply(update_close_price, axis=1)
    
    # Count how many prices were updated
    updated_count = (original_prices != stock_eod_df['Close Prc']).sum()
    print(f"\nUpdated {updated_count} price records from the stock_prices dictionary")
    
    # Show some examples of updated prices
    price_changes = stock_eod_df[original_prices != stock_eod_df['Close Prc']]
    if not price_changes.empty:
        print("\nExample price updates:")
        for i, row in price_changes.head(5).iterrows():
            symbol = row['#Symbol']
            new_price = row['Close Prc']
            old_price = original_prices.iloc[i]
            print(f"{symbol}: {old_price} → {new_price}")
    
    # Calculate new_Index_Mcap using existing FX/Index Ccy column
    stock_eod_df['new_Index_Mcap'] = (
        stock_eod_df['Close Prc'] * 
        stock_eod_df['Shares'] * 
        stock_eod_df['Free float-Coeff'] * 
        stock_eod_df['Capping Factor-Coeff'] * 
        stock_eod_df['FX/Index Ccy']
    )
    
    # Create dictionaries to map Mnemo to the various ISIN codes from index_eod_df
    isin_lookup = dict(zip(index_eod_df['Mnemo'], index_eod_df['IsinCode']))
    
    # Create dictionaries to map Price ISIN to Gross and Net Return ISIN versions
    gross_isin_lookup = dict(zip(index_eod_df['IsinCode'], index_eod_df['ISIN Gross Return version']))
    net_isin_lookup = dict(zip(index_eod_df['IsinCode'], index_eod_df['ISIN Net Return version']))
    
    # Create dictionaries to map ISINs to Effect Gross and Net Total Return values
    gross_mass_lookup = dict(zip(index_eod_df['IsinCode'], index_eod_df['Effect Gross Total Return']))
    net_mass_lookup = dict(zip(index_eod_df['IsinCode'], index_eod_df['Effect Net Total Return ']))
    
    # Create dictionaries to map ISINs to t0 IV unround from previous day's index data
    # Create price_t1_lookup for Price ISINs
    price_t1_lookup = {}
    for idx, row in index_eod_df_t1.iterrows():
        if not pd.isna(row['IsinCode']):
            price_t1_lookup[row['IsinCode']] = row['t0 IV unround'] if 't0 IV unround' in index_eod_df_t1.columns else None
    
    # For Gross ISINs
    gross_t1_lookup = {}
    for idx, row in index_eod_df_t1.iterrows():
        if not pd.isna(row['IsinCode']):
            gross_t1_lookup[row['IsinCode']] = row['t0 IV unround'] if 't0 IV unround' in index_eod_df_t1.columns else None
    
    # For Net ISINs
    net_t1_lookup = {}
    for idx, row in index_eod_df_t1.iterrows():
        if not pd.isna(row['IsinCode']):
            net_t1_lookup[row['IsinCode']] = row['t0 IV unround'] if 't0 IV unround' in index_eod_df_t1.columns else None
    
    # Create a dataframe to store the results
    results = []
    
    # Calculate the sum of new_Index_Mcap for each mnemo in the list
    for mnemo in mnemonics:
        # Filter the dataframe where Index equals the mnemo
        filtered_df = stock_eod_df[stock_eod_df['Index'] == mnemo]
        
        # Calculate the sum of new_Index_Mcap
        total_mcap = filtered_df['new_Index_Mcap'].sum()
        
        # Look up the divisor from index_eod_df where Mnemo equals the mnemo
        divisor_row = index_eod_df[index_eod_df['Mnemo'] == mnemo]
        divisor = divisor_row['Divisor'].values[0] if not divisor_row.empty else None
        
        # Calculate Price Level (Total_Index_Mcap / Divisor)
        price_level = total_mcap / divisor if divisor is not None and divisor != 0 else None
        
        # Look up the IsinCode for the mnemo
        price_isin = isin_lookup.get(mnemo, None)
        
        # Look up the Gross and Net Return ISINs using the Price ISIN
        gross_isin = gross_isin_lookup.get(price_isin, None) if price_isin else None
        net_isin = net_isin_lookup.get(price_isin, None) if price_isin else None
        
        # Look up the Gross and Net Mass values using the Gross ISIN
        gross_mass = gross_mass_lookup.get(gross_isin, None) if gross_isin else None
        net_mass = net_mass_lookup.get(net_isin, None) if net_isin else None
        
        # Look up the t-1 values for Price, Gross and Net ISINs
        price_t1 = price_t1_lookup.get(price_isin, None) if price_isin else None
        gross_t1 = gross_t1_lookup.get(gross_isin, None) if gross_isin else None
        net_t1 = net_t1_lookup.get(net_isin, None) if net_isin else None
        
        # Calculate Gross_Level and Net_Level using the formula
        gross_level = None
        net_level = None
        
        if price_t1 is not None and price_t1 != 0 and gross_t1 is not None:
            if price_level is not None and gross_mass is not None:
                gross_level = round(gross_t1 * ((price_level + gross_mass) / price_t1), 8)
            
            if price_level is not None and net_mass is not None:
                net_level = round(net_t1 * ((price_level + net_mass) / price_t1), 8)
        
        # Append the result to the list
        results.append({
            'Index': mnemo, 
            'Total_Index_Mcap': total_mcap,
            'Divisor': divisor,
            'Price_Level': price_level,
            'Price_Isin': price_isin,
            'Price_t-1': price_t1,
            'Gross_Isin': gross_isin,
            'Net_Isin': net_isin,
            'Gross_Mass': gross_mass,
            'Net_Mass': net_mass,
            'Gross_t-1': gross_t1,
            'Net_t-1': net_t1,
            'Gross_Level': gross_level,
            'Net_Level': net_level
        })
    
    # Create a dataframe from the results
    results_df = pd.DataFrame(results)
    
    # Create Excel writer object with the timestamped filename
    output_path = fr"C:\Users\CSonneveld\OneDrive - Euronext\Documents\Projects\Recalculation Script - OMX issue\Output\Index_Analysis_{timestamp}.xlsx"
    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        # Write each dataframe to a different sheet
        results_df.to_excel(writer, sheet_name='Index_Totals', index=False)
        stock_eod_df.to_excel(writer, sheet_name='Stock_EOD_Data', index=False)
        stock_eod_df_t1.to_excel(writer, sheet_name='Stock_EOD_Data_T-1', index=False)
        index_eod_df.to_excel(writer, sheet_name='Index_EOD_Data', index=False)
        index_eod_df_t1.to_excel(writer, sheet_name='Index_EOD_Data_T-1', index=False)
    
    print("Results saved to:", output_path)
    print("Summary of results:")
    print(results_df)

except Exception as e:
    print(f"First attempt failed with latin1 encoding: {e}")
    
    try:
        # Current day files (T)
        current_stock_eod_date = "20250729"
        current_index_eod_date = "20250729"
        current_stock_sod_date = "20250730"
        current_index_sod_date = "20250730"

        # Previous day files (T-1)
        prev_stock_eod_date = get_previous_day(current_stock_eod_date)
        prev_index_eod_date = get_previous_day(current_index_eod_date)
        
        # Base path
        base_path = r"C:\Users\CSonneveld\OneDrive - Euronext\Documents\Projects\Recalculation Script\Data"
        
        # Current day file paths
        stock_eod_path = os.path.join(base_path, f"TTMIndexUS1_GIS_EOD_STOCK_{current_stock_eod_date}.csv")
        index_eod_path = os.path.join(base_path, f"TTMIndexUS1_GIS_EOD_INDEX_{current_index_eod_date}.csv")
        stock_sod_path = os.path.join(base_path, f"TTMIndexUS1_GIS_SOD_STOCK_{current_stock_sod_date}.csv")
        index_sod_path = os.path.join(base_path, f"TTMIndexUS1_GIS_SOD_INDEX_{current_index_sod_date}.csv")
        
        # Previous day file paths
        stock_eod_t1_path = os.path.join(base_path, f"TTMIndexUS1_GIS_EOD_STOCK_{prev_stock_eod_date}.csv")
        index_eod_t1_path = os.path.join(base_path, f"TTMIndexUS1_GIS_EOD_INDEX_{prev_index_eod_date}.csv")
        
        # Current day dataframes (using windows-1252 encoding this time)
        stock_eod_df = pd.read_csv(stock_eod_path, sep=';', encoding='windows-1252')
        index_eod_df = pd.read_csv(index_eod_path, sep=';', encoding='windows-1252')
        stock_sod_df = pd.read_csv(stock_sod_path, sep=';', encoding='windows-1252')
        index_sod_df = pd.read_csv(index_sod_path, sep=';', encoding='windows-1252')
        
        # Previous day dataframes
        stock_eod_df_t1 = pd.read_csv(stock_eod_t1_path, sep=';', encoding='windows-1252')
        index_eod_df_t1 = pd.read_csv(index_eod_t1_path, sep=';', encoding='windows-1252')
        
        # Print some debugging information
        print("First few rows of stock_eod_df:")
        print(stock_eod_df[['#Symbol', 'Index', 'Close Prc']].head())
        
        # Update Close Prc values using the stock_prices dictionary
        def update_close_price(row):
            symbol = str(row['#Symbol']).strip() if not pd.isna(row['#Symbol']) else ""
            if symbol in stock_prices:
                return stock_prices[symbol]
            else:
                return row['Close Prc']  # Keep original price if not found in dictionary
        
        # Apply the price update
        original_prices = stock_eod_df['Close Prc'].copy()
        stock_eod_df['Close Prc'] = stock_eod_df.apply(update_close_price, axis=1)
        
        # Count how many prices were updated
        updated_count = (original_prices != stock_eod_df['Close Prc']).sum()
        print(f"\nUpdated {updated_count} price records from the stock_prices dictionary")
        
        # Show some examples of updated prices
        price_changes = stock_eod_df[original_prices != stock_eod_df['Close Prc']]
        if not price_changes.empty:
            print("\nExample price updates:")
            for i, row in price_changes.head(5).iterrows():
                symbol = row['#Symbol']
                new_price = row['Close Prc']
                old_price = original_prices.iloc[i]
                print(f"{symbol}: {old_price} → {new_price}")
        
        # Calculate new_Index_Mcap using existing FX/Index Ccy column
        stock_eod_df['new_Index_Mcap'] = (
            stock_eod_df['Close Prc'] * 
            stock_eod_df['Shares'] * 
            stock_eod_df['Free float-Coeff'] * 
            stock_eod_df['Capping Factor-Coeff'] * 
            stock_eod_df['FX/Index Ccy']
        )
        
        # Create dictionaries to map Mnemo to the various ISIN codes from index_eod_df
        isin_lookup = dict(zip(index_eod_df['Mnemo'], index_eod_df['IsinCode']))
        
        # Create dictionaries to map Price ISIN to Gross and Net Return ISIN versions
        gross_isin_lookup = dict(zip(index_eod_df['IsinCode'], index_eod_df['ISIN Gross Return version']))
        net_isin_lookup = dict(zip(index_eod_df['IsinCode'], index_eod_df['ISIN Net Return version']))
        
        # Create dictionaries to map ISINs to Effect Gross and Net Total Return values
        gross_mass_lookup = dict(zip(index_eod_df['IsinCode'], index_eod_df['Effect Gross Total Return']))
        net_mass_lookup = dict(zip(index_eod_df['IsinCode'], index_eod_df['Effect Net Total Return ']))
        
        # Create dictionaries to map ISINs to t0 IV unround from previous day's index data
        # Create price_t1_lookup for Price ISINs
        price_t1_lookup = {}
        for idx, row in index_eod_df_t1.iterrows():
            if not pd.isna(row['IsinCode']):
                price_t1_lookup[row['IsinCode']] = row['t0 IV unround'] if 't0 IV unround' in index_eod_df_t1.columns else None
        
        # For Gross ISINs
        gross_t1_lookup = {}
        for idx, row in index_eod_df_t1.iterrows():
            if not pd.isna(row['IsinCode']):
                gross_t1_lookup[row['IsinCode']] = row['t0 IV unround'] if 't0 IV unround' in index_eod_df_t1.columns else None
        
        # For Net ISINs
        net_t1_lookup = {}
        for idx, row in index_eod_df_t1.iterrows():
            if not pd.isna(row['IsinCode']):
                net_t1_lookup[row['IsinCode']] = row['t0 IV unround'] if 't0 IV unround' in index_eod_df_t1.columns else None
        
        # Create a dataframe to store the results
        results = []
        
        # Calculate the sum of new_Index_Mcap for each mnemo in the list
        for mnemo in mnemonics:
            # Filter the dataframe where Index equals the mnemo
            filtered_df = stock_eod_df[stock_eod_df['Index'] == mnemo]
            
            # Calculate the sum of new_Index_Mcap
            total_mcap = filtered_df['new_Index_Mcap'].sum()
            
            # Look up the divisor from index_eod_df where Mnemo equals the mnemo
            divisor_row = index_eod_df[index_eod_df['Mnemo'] == mnemo]
            divisor = divisor_row['Divisor'].values[0] if not divisor_row.empty else None
            
            # Calculate Price Level (Total_Index_Mcap / Divisor)
            price_level = total_mcap / divisor if divisor is not None and divisor != 0 else None
            
            # Look up the IsinCode for the mnemo
            price_isin = isin_lookup.get(mnemo, None)
            
            # Look up the Gross and Net Return ISINs using the Price ISIN
            gross_isin = gross_isin_lookup.get(price_isin, None) if price_isin else None
            net_isin = net_isin_lookup.get(price_isin, None) if price_isin else None
            
            # Look up the Gross and Net Mass values using the Gross ISIN
            gross_mass = gross_mass_lookup.get(gross_isin, None) if gross_isin else None
            net_mass = net_mass_lookup.get(net_isin, None) if net_isin else None
            
            # Look up the t-1 values for Price, Gross and Net ISINs
            price_t1 = price_t1_lookup.get(price_isin, None) if price_isin else None
            gross_t1 = gross_t1_lookup.get(gross_isin, None) if gross_isin else None
            net_t1 = net_t1_lookup.get(net_isin, None) if net_isin else None
            
            # Calculate Gross_Level and Net_Level using the formula
            gross_level = None
            net_level = None
            
            if price_t1 is not None and price_t1 != 0 and gross_t1 is not None:
                if price_level is not None and gross_mass is not None:
                    gross_level = round(gross_t1 * ((price_level + gross_mass) / price_t1), 8)
                
                if price_level is not None and net_mass is not None:
                    net_level = round(net_t1 * ((price_level + net_mass) / price_t1), 8)
            
            # Append the result to the list
            results.append({
                'Index': mnemo, 
                'Total_Index_Mcap': total_mcap,
                'Divisor': divisor,
                'Price_Level': price_level,
                'Price_Isin': price_isin,
                'Price_t-1': price_t1,
                'Gross_Isin': gross_isin,
                'Net_Isin': net_isin,
                'Gross_Mass': gross_mass,
                'Net_Mass': net_mass,
                'Gross_t-1': gross_t1,
                'Net_t-1': net_t1,
                'Gross_Level': gross_level,
                'Net_Level': net_level
            })
        
        # Create a dataframe from the results
        results_df = pd.DataFrame(results)
        
        # Add timestamp column to the results
        results_df['Timestamp'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Create Excel writer object with the timestamped filename
        output_path = fr"C:\Users\CSonneveld\OneDrive - Euronext\Documents\Projects\Recalculation Script - OMX issue\Output\Index_Analysis_{timestamp}.xlsx"
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            # Write each dataframe to a different sheet
            results_df.to_excel(writer, sheet_name='Index_Totals', index=False)
            stock_eod_df.to_excel(writer, sheet_name='Stock_EOD_Data', index=False)
            stock_eod_df_t1.to_excel(writer, sheet_name='Stock_EOD_Data_T-1', index=False)
            index_eod_df.to_excel(writer, sheet_name='Index_EOD_Data', index=False)
            index_eod_df_t1.to_excel(writer, sheet_name='Index_EOD_Data_T-1', index=False)
        
        print("Results saved to:", output_path)
        print("Summary of results:")
        print(results_df)
        
    except Exception as e:
        print(f"Second attempt failed with windows-1252 encoding: {e}")