# config.py
# Network paths
DLF_FOLDER = r"\\pbgfshqa08601v\gis_ttm\Archive"
DATA_FOLDER = r"V:\PM-Indices-IndexOperations\Review Files"
DATA_FOLDER2 = r"C:\Users\CSonneveld\OneDrive - Euronext\Documents\Projects\GUI + Reviews"

# Index Configurations
INDEX_CONFIGS = {
    "FRI4P": {
        "index": "FRI4P",
        "isin": "FRIX00003643",
        "output_key": "fri4p_path"
    },
    "FRD4P": {
        "index": "FRD4P",
        "isin": "FRIX00003031",
        "output_key": "frd4p_path"
    },  
    "EGSPP": {
        "index": "EGSPP",
        "isin": "FRIX00003031",
        "output_key": "egspp_path"
    },
    "GICP": {
        "index": "GICP",
        "isin": "NLIX00005321",
        "output_key": "gicp_path"
    },
    "EDWPT": {
        "index": "EDWPT",
        "isin": "NLIX00001932",
        "output_key": "edwpt_path"
    },
    "EDWP": {
        "index": "EDWP",
        "isin": "NLIX00001577",
        "output_key": "edwp_path"
    },
    "F4RIP": {
        "index": "F4RIP",
        "isin": "FR0013376209",
        "output_key": "f4rip_path"
    },
    "SES5P": {
        "index": "SES5P",
        "isin": "NL0015000EF0",
        "output_key": "ses5p_path"
    },
    "AERDP": {
        "index": "AERDP",
        "isin": "NLIX00003086",
        "output_key": "aerdp_path"
    },
    "BNEW": {
        "index": "BNEW",
        "isin": "NL0011376116",
        "output_key": "bnew_path"
    },
    "AEXEW": {
        "index": "AEXEW",
        "isin": "QS0011159744",
        "output_key": "aexew_path"
    },
    "CACEW": {
        "index": "CACEW",
        "isin": "QS0011159777",
        "output_key": "cacew_path"
    },
    "CLEW": {
        "index": "CLEW",
        "isin": "FR0012663292",
        "output_key": "clew_path"
    },
    "SBF80": {
        "index": "SBF80",
        "isin": "FR0013017936",
        "output_key": "sbf80_path"
    },
    "WIFRP": {
        "index": "WIFRP",
        "isin": "FRIX00002777",
        "output_key": "wifrp_path"
    },
    "LC100": {
        "index": "LC100",
        "isin": "QS0011131735",
        "output_key": "lc100_path"
    },
    "LC3WP": {
        "index": "LC3WP",
        "isin": "FR0013522588",
        "output_key": "lc3wp_path"
    },
    "LC1EP": {
        "index": "LC1EP",
        "isin": "FR0013522554",
        "output_key": "lc100_path"
    },
    "FRECP": {
        "index": "FRECP",
        "isin": "FR0013349057",
        "output_key": "frecp_path"
    },
    "FRN4P": {
        "index": "FRN4P",
        "isin": "FR0013354156",
        "output_key": "frn4p_path"
    },
    "FR20P": {
        "index": "FR20P",
        "isin": "FR0013355948",
        "output_key": "fr20p_path"
    },
    "EZ40P": {
        "index": "EZ40P",
        "isin": "NL0012731871",
        "output_key": "ez40p_path"
    },
    "EZ60P": {
        "index": "EZ60P",
        "isin": "NL0012846265",
        "output_key": "ez60p_path"
    },
    "EZ15P": {
        "index": "EZ15P",
        "isin": "NL0012949101",
        "output_key": "ez15p_path"
    },
    "EZN1P": {
        "index": "EZN1P",
        "isin": "NL0012949143",
        "output_key": "ezn1p_path"
    },
    "EFMEP": {
        "index": "EFMEP",
        "isin": "NL0012730451",
        "output_key": "efmep_path"
    },
    "ERI5P": {
        "index": "ERI5P",
        "isin": "NL0013217730",
        "output_key": "eri5p_path"
    },
    "BE1P": {
        "index": "BE1P",
        "isin": "NLIX00005388",
        "output_key": "be1p_path"
    },
    "EUS5P": {
        "index": "EUS5P",
        "isin": "NL0013216468",
        "output_key": "eus5p_path"
    },
    "EDEFP": {
        "index": "EDEFP",
        "isin": "NLIX00005982",
        "output_key": "edefp_path"
    },
    "ETPFB": {
        "index": "ETPFB",
        "isin": "NLIX00006535",
        "output_key": "etpfb_path"
    },
    "ELUXP": {
        "index": "ELUXP",
        "isin": "NLIX00002930",
        "output_key": "eluxp_path"
    },
    "ESVEP": {
        "index": "ESVEP",
        "isin": "NLIX00005230",
        "output_key": "esvep_path"
    },
    "SECTORIAL": {
        "index": "SECTORIAL",
        "isin": "SECTORIAL",
        "output_key": "sectorial_path"
    },
    "DWREP": {
        "index": "DWREP",
        "isin": "NLIX00004894",
        "output_key": "dwrep_path"
    },
    "DEREP": {
        "index": "DEREP",
        "isin": "NLIX00004860",
        "output_key": "derep_path"
    },
    "DAREP": {
        "index": "DAREP",
        "isin": "NLIX00004837",
        "output_key": "darep_path"
    },
    "EUREP": {
        "index": "EUREP",
        "isin": "NLIX00004803",
        "output_key": "eurep_path"
    },
    "GSFBP": {
        "index": "GSFBP",
        "isin": "NLIX00005735",
        "output_key": "gsfbp_path"
    },
    "EESF": {
        "index": "EESF",
        "isin": "NLIX00006170",
        "output_key": "eesf_path"
    },
    "ETSEP": {
        "index": "ETSEP",
        "isin": "NLIX00007095",
        "output_key": "etsep_path"
    },
    "ELTFP": {
        "index": "ELTFP",
        "isin": "NLIX00007327",
        "output_key": "eltfp_path"
    },
    "ELECP": {
        "index": "ELECP",
        "isin": "NLIX00007541",
        "output_key": "elecp_path"
    },
    "EUADP": {
        "index": "EUADP",
        "isin": "NLIX00005818",
        "output_key": "euadp_path"
    },
    "EEFAP": {
        "index": "EEFAP",
        "isin": "NLIX00008051",
        "output_key": "eefap_path"
    },
    "EES2": {
        "index": "EES2",
        "isin": "NLIX00007053",
        "output_key": "ees2_path"
    },
    "EFESP": {
        "index": "EFESP",
        "isin": "NLIX00006584",
        "output_key": "efesp_path"
    },
    "AEXAT": {
        "index": "AEXAT",
        "isin": "NL0010614491",
        "output_key": "aexat_path"
    },
    "AETAW": {
        "index": "AETAW",
        "isin": "NL0010614525",
        "output_key": "aetaw_path"
    },
    "ENVB": {
        "index": "ENVB",
        "isin": "QS0011256235",
        "output_key": "envb_path"
    },
    "ES2PR": {
        "index": "ES2PR",
        "isin": "NLIX00008218",
        "output_key": "es2pr_path"
    },
    "EZSL": {
        "index": "EZSL",
        "isin": "NLIX00008655",
        "output_key": "ezsl_path"
    },
    "EUMS": {
        "index": "EUMS",
        "isin": "NLIX00008747",
        "output_key": "eums_path"
    },
    "EZMS": {
        "index": "EZMS",
        "isin": "NLIX00008689",
        "output_key": "ezms_path"
    },
    "EEMSC": {
        "index": "EEMSC",
        "isin": "NLIX00008622",
        "output_key": "eemsc_path"
    },
    "EWMS": {
        "index": "EWMS",
        "isin": "NLIX00008564",
        "output_key": "ewms_path"
    },
    "EUSL": {
        "index": "EUSL",
        "isin": "NLIX00008713",
        "output_key": "eusl_path"
    },
    "EESL": {
        "index": "EESL",
        "isin": "NLIX00008598",
        "output_key": "eesl_path"
    },
    "EWSL": {
        "index": "EWSL",
        "isin": "NLIX00008531",
        "output_key": "ewsl_path"
    },
    "ELUX": {
        "index": "ELUX",
        "isin": "NLIX00008424",
        "output_key": "elux_path"
    },
    "EZCLA": {
        "index": "EZCLA",
        "isin": "FR0014005IK5",
        "output_key": "ezcla_path"
    },
    "USCLE": {
        "index": "USCLE",
        "isin": "FR0014005IJ7",
        "output_key": "uscle_path"
    },
    "TCAMP": {
        "index": "TCAMP",
        "isin": "FR0014005GI3",
        "output_key": "tcamp_path"
    },
    "GSCSP": {
        "index": "GSCSP",
        "isin": "FRESG0000876",
        "output_key": "gscsp_path"
    },
    "WCAMP": {
        "index": "WCAMP",
        "isin": "FRESG0000348",
        "output_key": "wcamp_path"
    },
    "EHNI": {
        "index": "EHNI",
        "isin": "FRESG0002815",
        "output_key": "ehni_path"
    },
    "USCLA": {
        "index": "USCLA",
        "isin": "FR0014005GE2",
        "output_key": "uscla_path"
    },
    "USC3P": {
        "index": "USC3P",
        "isin": "FRESG0000249",
        "output_key": "usc3p_path"
    },
    "UC3PE": {
        "index": "UC3PE",
        "isin": "FRESG0000272",
        "output_key": "uc3pe_path"
    },
    "CLAMP": {
        "index": "CLAMP",
        "isin": "FR0014004XR2",
        "output_key": "clamp_path"
    },
    "JPCLA": {
        "index": "JPCLA",
        "isin": "FRESG0000181",
        "output_key": "jpcla_path"
    },
    "JPCLE": {
        "index": "JPCLE",
        "isin": "FRESG0000215",
        "output_key": "jpcle_path"
    },
    "FCLSP": {
        "index": "FCLSP",
        "isin": "FRESG0001478",
        "output_key": "fclsp_path"
    },
    "EAIB": {
        "index": "EAIB",
        "isin": "NLIX00008366",
        "output_key": "eaib_path"
    },
    "EEDF": {
        "index": "EEDF",
        "isin": "NLIX00008473",
        "output_key": "eedf_path"
    },
    "EHCF": {
        "index": "EHCF",
        "isin": "NLIX00008168",
        "output_key": "ehcf_path"
    },
    "EIAPR": {
        "index": "EIAPR",
        "isin": "NLIX00005628",
        "output_key": "eiapr_path"
    },
    "EMLS": {
        "index": "EMLS",
        "isin": "NLIX00008903",
        "output_key": "emls_path"
    },
    "C6RIP": {
        "index": "C6RIP",
        "isin": "FR0013376258",
        "output_key": "c6rip_path"
    },
    "INFRP": {
        "index": "INFRP",
        "isin": "FRIX00002876",
        "output_key": "infrp_path"
    },
    "ES2PR_BACKUP": {
        "index": "ES2PR_backup",
        "isin": "NLIX00008218",
        "output_key": "es2pr_backup_path"
    },
    "ENVU": {
        "index": "ENVU",
        "isin": "QS0011250931",
        "output_key": "envu_path"
    },
    "ENVEO": {
        "index": "ENVEO",
        "isin": "QS0011256201",
        "output_key": "enveo_path"
    },
    "ENVEU": {
        "index": "ENVEU",
        "isin": "QS0011250873",
        "output_key": "enveu_path"
    },
    "ENVF": {
        "index": "ENVF",
        "isin": "QS0011250907",
        "output_key": "envf_path"
    },
    "ENVUK": {
        "index": "ENVUK",
        "isin": "QS0011250931",
        "output_key": "envuk_path"
    },
    "ENVW": {
        "index": "ENVW",
        "isin": "QS0011250840",
        "output_key": "envw_path"
    }
    # Add new indices here following the same pattern
}
BATCH_CONFIG = {
    "max_concurrent_reviews": 3,  # Adjust based on your system capacity
    "timeout_minutes": 30,        # Timeout for individual reviews
    "retry_attempts": 1,          # Number of retry attempts for failed reviews
    "progress_update_interval": 1 # Seconds between progress updates
}

# Group configurations for common batch operations
REVIEW_GROUPS = {
    "french_indices": ["FRI4P", "FRD4P", "FRECP", "FRN4P", "FR20P"],
    "dutch_indices": ["EDWP", "EDWPT", "GICP", "AERDP", "BNEW"],
    "eurozone_indices": ["EZ40P", "EZ60P", "EZ15P", "EZN1P"],
    "sustainability_indices": ["AEXEW", "CACEW", "CLEW"],
    "all_indices": list(INDEX_CONFIGS.keys())
}

ALL_MNEMOS = [
    "ENVU", "ENVW", "NA500", "ENDMP", "ENWP", "EWCSP", "EVEWP", "ETE5P", "EUEPR", "WESGP", 
    "ESGTP", "WLENP", "EUS5P", "ERGSP", "ERGBP", "ERGCP", "EIAPR", "TTIT", "UML1P", "GICP", 
    "UUTI", "UBMA", "UHEC", "UTEL", "UIND", "UFIN", "UCDI", "UCST", "UENR", "UTEC", 
    "DWREP", "EUREP", "TUTI", "TBMA", "THEC", "TTEL", "TIND", "TFINP", "TCDI", "TCST", 
    "TENR", "TTEC", "AERDP", "EUSCS", "CANPT", "EUSPT", "DNAPT", "EDWPT", "CANP", "EUSP", 
    "DNAP", "EDWP", "ENZTP", "HSPCP", "HSPAP", "ENTP", "EDMPU", "EDWEP", "USCLE", "TCAMP", 
    "USCLA", "TESGP", "TCEPR", "ECC5P", "LC3WP", "FRI4P", "FRD4P", "INFRP", "WIFRP", "FILVP", 
    "EIFRP", "ECWPR", "EAIWP", "BISWP", "EBEWP", "BSWPF", "GSCSP", "EBSTP", "WCAMP", "UC3PE", 
    "USC3P", "EBSPW", "EBSWP", "GHCPR", "TP1CP", "PBT4P", "PBTAP", "PABTP", "PABUP", "TPABP", 
    "PFLCW", "DUMUS", "DUUSC", "ENVB", "ENVEO", "ENVUK", "ENVF", "ENVEU", "PTOGP", "BEOGP", 
    "BELCP", "BECGP", "BEINP", "BEHCP", "BETEP", "BEFIP", "BETP", "BEUTP", "BECSP", "PTTEP", 
    "PTFIP", "PTUTP", "PTTLP", "PTCSP", "PTCGP", "PTINP", "PTBMP", "BEBMP", "BVL", "CACLG", 
    "NAOII", "CACEW", "AEXEW", "LC100", "BIOTK", "REITE", "ALASI", "FRTEC", "FRFIN", "FRUT", 
    "FRTEL", "FRCS", "FRHC", "FRCG", "FRIN", "FRBM", "FROG", "NLTEC", "NLFIN", "NLTEL", 
    "NLCS", "NLHC", "NLCG", "NLIN", "NLBM", "NLOG", "PAX", "CACMS", "CACS", "CACMD", 
    "CN20", "PTEB", "PSI20", "PIRUT", "OEXOP", "OBXEP", "OSEPB", "OSEXP", "OSEPX", "SSSHP", 
    "SSSFP", "SSENP", "OBXUP", "OSEEP", "OBOSP", "OUTP", "OENP", "OBMP", "OINP", "OCSP", 
    "OCDP", "OREP", "OFINP", "OHCP", "OTELP", "OTECP", "OAAXP", "OSESP", "OSEMP", "OSEFP", 
    "OSEBP", "OSEAP", "OBSHP", "OBSFP", "OBXP", "OIRUT", "AS500", "CAIN3", "CAIN2", "NLUT", 
    "EZWTP", "CAIN5", "SES5P", "BANK", "EEEPR", "NLRE", "EESGP", "WATPR", "EZENP", "EZ300", 
    "EU500", "ERI5P", "CEE1P", "FREEP", "CEE3P", "BESGP", "ESG1P", "ECO5P", "FRENP", "EZN1P", 
    "EZ15P", "ES1EP", "CLE5P", "ESE4P", "EZ40P", "FGINP", "EFMEP", "EFGP", "COR3P", "ESG50", 
    "EFGEW", "ECOEW", "EBLRE", "EC1EW", "ECOP", "ENCLE", "CAIND", "BNEW", "AETAW", "AEXAT", 
    "AMX", "ASCX", "AAX", "MIRUT", "AEX", "AIRUT", "GSFBP", "BREU", "ENEU", "BE1P", 
    "SEZTP", "ESVEP", "DEREP", "DAREP", "PFAEX", "EZSCP", "UTIL", "TELEP", "TECHP", "INDU", 
    "HEAC", "FINA", "ENRGP", "CSTA", "CDIS", "BASM", "ELUXP", "ENEZ5", "EUKPT", "ECHPT", 
    "EJPPT", "DPAPT", "DASPT", "DAPPT", "DEZPT", "DEUPT", "EUKP", "ECHP", "EJPP", "DPAP", 
    "DASP", "DAPPR", "DEZP", "DEUP", "TECLP", "AESGP", "ISEQ", "ISEFI", "ISESM", "ISECA", 
    "ISE20", "IIRUT", "ISRE", "ISUT", "ISEHC", "ISECS", "ISCG", "ISEBM", "ISETE", "ISEIN", 
    "MESGP", "EZCLA", "FRSOP", "CLAMP", "ESGCP", "EZSFP", "FPABP", "CPABP", "EPABP", "GOVEP", 
    "CESGP", "LC1EP", "FRRE", "GRF5P", "ESGEP", "ESGFP", "ENESG", "GRE5P", "FESGP", "FRTPR", 
    "C6RIP", "F4RIP", "ESF5P", "FR20P", "FRN4P", "FRECP", "CLF4P", "ESF4P", "C4SD", "CAGOV", 
    "CLEJP", "CLEWJ", "SBF80", "CLEW", "ENPME", "EIRUT", "CACT", "PX4", "N150", "N100", 
    "PX1", "CIRUT", "SGACP", "SPRPR", "SMBGP", "SREP", "SSCP", "SVEP1", "SMIP", "SSOP", 
    "SURP", "SKLP", "SINP", "SKEP", "SCRP", "SSTP", "SAMP", "SEIP", "SSFP", "SSHP", 
    "SCAP", "SSAP", "SSGP", "SBNP", "SAXP", "SBOP", "SORP", "STEP", "SENP", "SGS1P", 
    "SG03P", "SGBP1", "SGRNP", "SSS3P", "SGG5P", "SGS4P", "SGSC", "PFPX1", "SGS3P", "SGG4P", 
    "SGU2P", "SGEEP", "SGA4P", "SGG3P", "SGB5P", "EIASP", "SGB4P", "SGG2P", "SGSAP", "SGEP3", 
    "PFOSF", "PFOSB", "CTRFD", "SGEP2", "SGVIP", "SGKEP", "SGU1P", "SMMLP", "SMSGP", "SGSP1", 
    "SMEP1", "SGBP3", "SGGP1", "SSS2P", "SGEP1", "SGEIP", "SGURP", "SBCAP", "SBSTP", "SGS2P", 
    "SGLIP", "SGSTP", "SGB3P", "SGC1P", "SSBNP", "NCACI", "CACIN", "SSINP", "SSKEP", "SSCAP", 
    "SSSTP", "SSMTP", "SSENI", "SSSNP", "SSSAP", "SSS1P", "SSAXP", "SSACP", "SGCAP", "SBBP", 
    "SBO1P", "SBENP", "SG01P", "SCS1P", "SGSGP", "SGB1P", "SGA1P", "SGBP", "SGT1P", "SGORP", 
    "SGTEP", "TERPR", "BIOCP", "FCLSP", "ESBTP", "ZSBTP", "EETPR", "PFC4E", "CSBTP", "EQGEP", 
    "EQGFP", "ENFRP", "ES4PP", "EZ6PP", "FREMP", "EBLPP", "BIOEP", "JPCLE", "JPCLA", "EBSEP", 
    "CAPAP", "EGSPP", "EPSP", "PFLCE", "PFLC1", "PFEBL", "DUMEU", "BELS", "BELM", "BEL20", 
    "BELAS", "BIRUT", "ESGBP", "BERE", "EESF", "ETPFB", "ETSEP", "ELTFP", "ELECP", "EUADP",
    "EEFAP", "EES2", "EFESP",  "ES2PR", "EZSL", "EWMS", "EEMSC", "EZMS", "EDEFP", "ELUX",
    "EUMS", "EUSL", "EESL", "EWSL", "EHNI", "EMLS", "EIAPR", "EHCF", "EAIB", "EEDF", "EUOG",
    "EEUS", "EWHC", "EZ60P", "EEAPP", "EEOG", "EEUU", "EEBRP", "EECP", "EWAP", "EWBR", "EWOU",
    "EWOG"
]


def get_index_config(review_type):
    """Get configuration for a specific review type"""
    review_type = review_type.upper()
    if review_type not in INDEX_CONFIGS:
        raise ValueError(f"Unknown review type: {review_type}")
    return INDEX_CONFIGS[review_type]
def get_batch_config():
    """Get batch processing configuration"""
    return BATCH_CONFIG

def get_review_group(group_name):
    """Get a predefined group of reviews"""
    return REVIEW_GROUPS.get(group_name, [])