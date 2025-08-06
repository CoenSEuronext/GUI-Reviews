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
    "AFRY.ST": 157.4,
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
    "SAABb.ST": 525.9,
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
    "SSABb.ST": 57.16,
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
    "TEQ.ST": 156,
    "VNV.ST": 20.18,
    "VPLAYb.ST": 0.9122,
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

mnemonics_tr4_perc = {
    "EZ150": "NL0012949119",
    "TPAB5": "FRCLIM000114",
    "TER5D": "FRESG0002708",
    "TER35": "FRESG0002708",
    "PBT5D": "FRCLIM000262",
    "ESGTL": "NL0013908783",
    "EZEN3": "NL0013352586",
    "WIFRD": "FRIX00002785",
    "ESGE4": "NL0012758551",
    "EBLPD": "FRESG0000397",
    "EPAB5": "FR0014003PM1",
    "BANKD": "NL0015000AT9",
    "LC3WD": "FR0013522596",
    "ETE5D": "NL0015000BQ3",
    "ESG1D": "NL0013025604",
    "ERI5E": "NL0013217748",
    "EPSD3": "FRCLIM000411",
    "ECO50": "NL0012328827",
    "ESGEC": "FR0014004HG8",
    "ESN3D": "FRESG0001312",
    "EZSFD": "FR0014004FO6",
    "EBSE4": "FRESG0000108",
    "TLESG": "FR0014004PU2",
    "ARD35": "NLIX00003102",
    "AERD5": "NLIX00003102",
    "BRD5": "NLIX00005578",
    "WAT5D": "NL0013908833",
    "EBLD5": "FRESG0000397",
    "COP5E": "NL0011923065",
    "ESVED": "NLIX00005255",
    "WAT4D": "NL0013908833",
    "EZESG": "NL0013941032",
    "ESG8D": "FR0013468865",
    "ESGD5": "FR0013468865",
    "COP5D": "NL0011923065",
    "ESGEL": "NL0012758551",
    "CLIE5": "NL0012758593",
    "EZNE1": "NL0012949150",
    "ECO5E": "NL0013025562",
    "EZENV": "NL0013352578",
    "FILV3": "FRIX00002736",
    "ESGED": "NL0013025604",
    "WLENV": "NL0013352610",
    "FILVD": "FRIX00002736",
    "WLESG": "NL0013940992",
    "GRE50": "FR0013457868",
    "GREG5": "FR0013457876",
    "ESGEZ": "FR0013477023",
    "ESGZD": "FR0013477023",
    "ESG4E": "NL0012758551",
    "BIOC3": "FRESG0002591",
    "BIOC5": "FRESG0002591",
    "ESGDE": "NL0013025604",
    "ESG8E": "FR0013468865",
    "ECO4D": "NL0013025562",
    "ESGD4": "FR0013468865",
    "ECC5D": "FR0013533569",
    "EZEN4": "NL0013352578",
    "EZWTR": "NL00150005F3",
    "EPAB4": "FR0014003PM1",
    "GOVEZ": "FR0014003PT6",
    "EBSE5": "FRESG0000108",
    "EEED5": "NL0015000AB7",
    "ZSN3D": "FRESG0001221",
    "ZSN4D": "FRESG0001221",
    "ZSN5D": "FRESG0001221",
    "ZSG3D": "FRESG0001239",
    "ECC3D": "FR0013533569",
    "ZSG4D": "FRESG0001239",
    "ZSG5D": "FRESG0001239",
    "ESN4D": "FRESG0001312",
    "ESN5D": "FRESG0001312",
    "ESDG3": "FRESG0001320",
    "ESDG4": "FRESG0001320",
    "ESDG5": "FRESG0001320",
    "ES4P4": "FRESG0000553",
    "ES4P5": "FRESG0000553",
    "EZ6P4": "FRESG0000504",
    "EZ6P5": "FRESG0000504",
    "EBST4": "FRESG0000413",
    "EBST5": "FRESG0000413",
    "EET4D": "FRESG0001171",
    "EET5D": "FRESG0001171",
    "ESOD5": "NLIX00006444",
    "ESVD4": "NLIX00005255",
    "EADD5": "NLIX00005826",
    "EFSD5": "NLIX00006592",
    "EZ5D5": "NLIX00006105",
    "EDEFD": "NLIX00005990",
    "E30D5": "NLIX00007152",
    "EGOD3": "FRESG0002864"
    }
mnemonics_tr4_points = {
    "TER50": "FRESG0002716",
    "TER5N": "FRESG0002708",
    "PBT50": "FRCLIM000270",
    "EZ6PD": "FRESG0000512",
    "EBSED": "FRESG0000116",
    "EBSTD": "FRESG0000421",
    "ES1ED": "NL0012758643",
    "EPABD": "FR0014003PL3",
    "BRD5P": "NLIX00005586",
    "CLAMB": "FR0014004XP6",
    "ESG5D": "NL0012481766",
    "SES50": "NL0015000EC7",
    "TPD5P": "FRCLIM000122",
    "EZ60": "NL0012846281",
    "EAD5P": "NLIX00005834",
    "EFS50": "NLIX00006600",
    "EZ50D": "NLIX00006113",
    "EDE5D": "NLIX00006006"
}
def load_data_with_encoding_fallback():
    """Load data with encoding fallback mechanism - combines US and EU files"""
    encodings = ['latin1', 'windows-1252', 'utf-8']
    
    # File date configuration
    current_stock_eod_date = "20250729"
    current_index_eod_date = "20250729"
    
    prev_stock_eod_date = get_previous_day(current_stock_eod_date)
    prev_index_eod_date = get_previous_day(current_index_eod_date)
    
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
        
        if price_t1 is not None and price_t1 != 0 and gross_t1 is not None:
            if price_level_round is not None and gross_mass is not None:
                gross_level_unrounded = gross_t1 * ((price_level_round + gross_mass) / price_t1)
                gross_level = round(gross_level_unrounded, 8)
            
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

def calculate_decrement_level(mnemo, isin, index_eod_df, index_eod_df_t1, results_df):
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
        
        # Get Yearly Days - NEW: retrieve from index_eod_df instead of hardcoded 365
        yearly_days_row = index_eod_df[index_eod_df['Mnemo'] == mnemo]
        if not yearly_days_row.empty and 'Yearly Days' in index_eod_df.columns:
            result['Yearly_Days'] = yearly_days_row['Yearly Days'].values[0]
        else:
            result['Error_Message'] = f"Yearly Days not found for mnemo {mnemo}"
            # Fallback to 365 if not found
            result['Yearly_Days'] = 365
        
        # Get day
        if 'System Date' in index_eod_df.columns and 'System Date' in index_eod_df_t1.columns:
            try:
                current_date = pd.to_datetime(index_eod_df['System Date'].iloc[0])
                previous_date = pd.to_datetime(index_eod_df_t1['System Date'].iloc[0])
                result['Day'] = (current_date - previous_date).days
            except:
                result['Day'] = 1
        else:
            result['Day'] = 1
        
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

def calculate_decrement_points_level(mnemo, isin, index_eod_df, index_eod_df_t1, results_df):
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
        # Get Underlying Index - NEW: retrieve ISIN Underlying Price Index from index_eod_df
        underlying_index_row = index_eod_df[index_eod_df['Mnemo'] == mnemo]
        if not underlying_index_row.empty and 'ISIN Underlying Price Index' in index_eod_df.columns:
            result['Underlying_Index'] = underlying_index_row['ISIN Underlying Price Index'].values[0]
        else:
            result['Underlying_Index'] = None  # Not an error, as not all indices may have this field
        
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
        
        # Get Yearly Days - NEW: retrieve from index_eod_df instead of hardcoded 365
        yearly_days_row = index_eod_df[index_eod_df['Mnemo'] == mnemo]
        if not yearly_days_row.empty and 'Yearly Days' in index_eod_df.columns:
            result['Yearly_Days'] = yearly_days_row['Yearly Days'].values[0]
        else:
            result['Error_Message'] = f"Yearly Days not found for mnemo {mnemo}"
            # Fallback to 365 if not found
            result['Yearly_Days'] = 365
        
        # Get day
        if 'System Date' in index_eod_df.columns and 'System Date' in index_eod_df_t1.columns:
            try:
                current_date = pd.to_datetime(index_eod_df['System Date'].iloc[0])
                previous_date = pd.to_datetime(index_eod_df_t1['System Date'].iloc[0])
                result['Day'] = (current_date - previous_date).days
            except:
                result['Day'] = 1
        else:
            result['Day'] = 1
        
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
        result = calculate_decrement_level(mnemo, isin, index_eod_df, index_eod_df_t1, results_df)
        decrement_results.append(result)
    
    decrement_df = pd.DataFrame(decrement_results)
    print(f"Calculated decrement levels for {len(decrement_df)} indices")
    print(f"Successfully calculated levels: {decrement_df['Decrement_Level'].notna().sum()}")
    
    # Calculate decrement points levels
    print("Calculating decrement points levels for TR4 points indices...")
    decrement_points_results = []
    for mnemo, isin in mnemonics_tr4_points.items():
        result = calculate_decrement_points_level(mnemo, isin, index_eod_df, index_eod_df_t1, results_df)
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