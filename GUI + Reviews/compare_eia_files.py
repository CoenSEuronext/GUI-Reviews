import os
import pandas as pd
import re
import subprocess
from pathlib import Path
from datetime import datetime

def get_mnemo_from_filename(filename):
    """Extract mnemo from filename using the pattern 'MNEMO_' that appears in all files"""
    # List all possible mnemos
    mnemos = [
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
    "BELAS", "BIRUT", "ESGBP", "BERE"
]
    
    # Look for pattern "MNEMO_" in the filename
    for mnemo in mnemos:
        pattern = mnemo + "_EDWP"
        if pattern in filename or pattern.replace("_", "-") in filename:
            return mnemo
            
    # Alternative regex approach
    pattern = r'(CANP|CANPT|DAPPR|DAPPT|DASP|DASPT|DEUP|DEUPT|DEZP|DEZPT|DNAP|DNAPT|DPAP|DPAPT|ECHP|ECHPT|EDWP|EDWPT|EJPP|EJPPT|EUKP|EUKPT|EUSP|EUSPT|AEXEW|BNEW|CACEW|CLEW|FRD4P|FRI4P|WIFRP|GICP|FRECP|FRN4P|FR20P|EZ40P|EFMEP|EZ15P|EZN1P|EUS5P|ERI5P|BE1P|EDEFP|EZ60P)[_-]'
    match = re.search(pattern, filename)
    if match:
        return match.group(1)
    
    return None

def find_matching_files(coen_folder, dataiku_folder):
    """Find files with matching mnemos in both folders"""
    coen_files = {}
    dataiku_files = {}
    
    # Get all Excel files in Coen folder
    for file in os.listdir(coen_folder):
        if file.endswith('.xlsx'):
            mnemo = get_mnemo_from_filename(file)
            if mnemo:
                coen_files[mnemo] = os.path.join(coen_folder, file)
    
    # Get all Excel files in Dataiku folder
    for file in os.listdir(dataiku_folder):
        if file.endswith('.xlsx'):
            mnemo = get_mnemo_from_filename(file)
            if mnemo:
                dataiku_files[mnemo] = os.path.join(dataiku_folder, file)
    
    # Find matching mnemos
    matching_mnemos = set(coen_files.keys()) & set(dataiku_files.keys())
    
    # Return pairs of matching files
    return [(mnemo, coen_files[mnemo], dataiku_files[mnemo]) for mnemo in matching_mnemos]


def load_excel_data(file_path, is_dataiku_file=False):
    """Load Excel file and return the first sheet as DataFrame
    
    Args:
        file_path: Path to the Excel file
        is_dataiku_file: True if this is a Dataiku file (headers in row 2), 
                        False if this is a Coen file (headers in row 1)
    """
    try:
        # Get all sheet names
        xl = pd.ExcelFile(file_path)
        sheet_names = xl.sheet_names
        
        # Look for sheets with 'Composition' in the name
        composition_sheets = [sheet for sheet in sheet_names if 'Composition' in sheet]
        
        if composition_sheets:
            # Use the first composition sheet
            sheet_name = composition_sheets[0]
        else:
            # Use the first sheet if no composition sheet found
            sheet_name = sheet_names[0]
        
        # Read the selected sheet with appropriate header location
        if is_dataiku_file:
            # Dataiku files: headers in row 2, so skip first row
            df = pd.read_excel(file_path, sheet_name=sheet_name, skiprows=1, header=0)
        else:
            # Coen files: headers in row 1, no skipping needed
            df = pd.read_excel(file_path, sheet_name=sheet_name, header=0)
            
        return df
    except Exception as e:
        print(f"Error loading {file_path}: {str(e)}")
        return None

def are_currencies_equivalent(currency1, currency2):
    """Check if two currency codes represent the same currency with acceptable variations"""
    if currency1 == currency2:
        return True
    
    # Define equivalent currency pairs
    equivalent_pairs = {
        ('GBX', 'GBP'),  # British Pence and British Pound
        ('GBP', 'GBX'),  # British Pound and British Pence
        ('ILA', 'ILS'),  # Israeli shekel variations
        ('ILS', 'ILA'),  # Israeli shekel variations
    }
    
    return (currency1, currency2) in equivalent_pairs

def compare_files(coen_file, dataiku_file):
    """Compare two Excel files for ISIN codes and specified fields"""
    # Load files with appropriate header settings
    coen_df = load_excel_data(coen_file, is_dataiku_file=False)  # Coen file
    dataiku_df = load_excel_data(dataiku_file, is_dataiku_file=True)  # Dataiku file
    
    if coen_df is None or dataiku_df is None:
        return {
            'status': 'ERROR',
            'message': 'Could not load one or both files'
        }
    
    # Check if 'ISIN Code' column exists in both DataFrames
    if 'ISIN Code' not in coen_df.columns or 'ISIN Code' not in dataiku_df.columns:
        # Print available columns for debugging
        print(f"  Available columns in Coen file: {list(coen_df.columns)[:10]}...")  # First 10 columns
        print(f"  Available columns in Dataiku file: {list(dataiku_df.columns)[:10]}...")  # First 10 columns
        
        return {
            'status': 'ERROR',
            'message': f"Missing 'ISIN Code' column: Coen: {'ISIN Code' in coen_df.columns}, Dataiku: {'ISIN Code' in dataiku_df.columns}"
        }
    
    # Rest of the function remains the same...
    # Get the set of ISIN codes from each file
    coen_isins = set(coen_df['ISIN Code'])
    dataiku_isins = set(dataiku_df['ISIN Code'])
    
    # Check if the ISIN sets match
    same_count = len(coen_isins) == len(dataiku_isins)
    same_isins = coen_isins == dataiku_isins
    
    # Find differences
    coen_only = coen_isins - dataiku_isins
    dataiku_only = dataiku_isins - coen_isins
    
    # Fields to compare - handle both "Final Capping" and "Capping Factor" column names
    base_fields = ['MIC', 'Number of Shares', 'Free Float', 'Currency']
    capping_column_names = {'Final Capping', 'Capping Factor'}
    
    # Determine which capping column name to use based on what's in the DataFrames
    capping_field = None
    coen_capping_field = None
    dataiku_capping_field = None
    
    for field in capping_column_names:
        if field in coen_df.columns:
            coen_capping_field = field
        if field in dataiku_df.columns:
            dataiku_capping_field = field
    
    # Helper function for float comparison
    def compare_float_values(val1, val2, tolerance=1e-14):
            """Compare float values with ultra-high precision tolerance (14 decimal places)"""
            try:
                # Try to convert both values to float
                float1 = float(val1) if val1 is not None else None
                float2 = float(val2) if val2 is not None else None
                
                # If either value is None, they're equal only if both are None
                if float1 is None or float2 is None:
                    return float1 is None and float2 is None
                
                # Compare with ultra-strict tolerance (14 decimal places)
                return abs(float1 - float2) < tolerance
            except (ValueError, TypeError):
                # If conversion fails, fall back to direct equality
                return val1 == val2
    
    # Initialize field comparison results
    field_results = {}
    
    # Compare fields for matching ISIN codes
    if same_isins:
        # Get common ISINs
        common_isins = coen_isins
        
        # First compare the standard fields
        for field in base_fields:
            if field in coen_df.columns and field in dataiku_df.columns:
                # Create dictionaries mapping ISIN to field value for easier comparison
                coen_dict = dict(zip(coen_df['ISIN Code'], coen_df[field]))
                dataiku_dict = dict(zip(dataiku_df['ISIN Code'], dataiku_df[field]))
                
                # Compare values for each ISIN
                mismatches = 0
                mismatch_examples = []
                
                for isin in common_isins:
                    coen_value = coen_dict.get(isin)
                    dataiku_value = dataiku_dict.get(isin)
                    
                    # Special handling for Currency field
                    if field == 'Currency':
                        if not are_currencies_equivalent(coen_value, dataiku_value):
                            mismatches += 1
                            if len(mismatch_examples) < 5:  # Limit to 5 examples
                                mismatch_examples.append({
                                    'ISIN': isin,
                                    'Coen value': coen_value,
                                    'Dataiku value': dataiku_value
                                })
                    # For numeric fields, use float comparison with tolerance
                    elif field in ['Number of Shares', 'Free Float']:
                        if not compare_float_values(coen_value, dataiku_value):
                            mismatches += 1
                            if len(mismatch_examples) < 5:  # Limit to 5 examples
                                mismatch_examples.append({
                                    'ISIN': isin,
                                    'Coen value': coen_value,
                                    'Dataiku value': dataiku_value
                                })
                    else:
                        # For non-numeric fields, use direct equality
                        if coen_value != dataiku_value:
                            mismatches += 1
                            if len(mismatch_examples) < 5:  # Limit to 5 examples
                                mismatch_examples.append({
                                    'ISIN': isin,
                                    'Coen value': coen_value,
                                    'Dataiku value': dataiku_value
                                })
                
                if mismatches > 0:
                    field_results[field] = {
                        'match': False,
                        'mismatches': mismatches,
                        'examples': mismatch_examples
                    }
                else:
                    field_results[field] = {
                        'match': True
                    }
            else:
                field_results[field] = {
                    'match': False,
                    'error': f"Field missing: Coen: {field in coen_df.columns}, Dataiku: {field in dataiku_df.columns}"
                }
        
        # Now handle the capping field separately, since it might have a different name in each file
        if coen_capping_field and dataiku_capping_field:
            # Create dictionaries mapping ISIN to capping value for each file
            coen_dict = dict(zip(coen_df['ISIN Code'], coen_df[coen_capping_field]))
            dataiku_dict = dict(zip(dataiku_df['ISIN Code'], dataiku_df[dataiku_capping_field]))
            
            # Compare values for each ISIN
            mismatches = 0
            mismatch_examples = []
            
            for isin in common_isins:
                coen_value = coen_dict.get(isin)
                dataiku_value = dataiku_dict.get(isin)
                
                # Use float comparison with tolerance for capping values
                if not compare_float_values(coen_value, dataiku_value):
                    mismatches += 1
                    if len(mismatch_examples) < 5:  # Limit to 5 examples
                        mismatch_examples.append({
                            'ISIN': isin,
                            'Coen value': coen_value,
                            'Dataiku value': dataiku_value
                        })
            
            display_field_name = "Capping (Final Capping/Capping Factor)"
            if mismatches > 0:
                field_results[display_field_name] = {
                    'match': False,
                    'mismatches': mismatches,
                    'examples': mismatch_examples
                }
            else:
                field_results[display_field_name] = {
                    'match': True
                }
        else:
            display_field_name = "Capping (Final Capping/Capping Factor)"
            field_results[display_field_name] = {
                'match': False,
                'error': f"Field missing: Coen: {coen_capping_field}, Dataiku: {dataiku_capping_field}"
            }
    
    result = {
        'status': 'SUCCESS',
        'same_count': same_count,
        'same_isins': same_isins,
        'coen_isins_count': len(coen_isins),
        'dataiku_isins_count': len(dataiku_isins),
        'coen_only': list(coen_only)[:5],  # First 5 ISINs only in Coen
        'dataiku_only': list(dataiku_only)[:5],  # First 5 ISINs only in Dataiku
        'coen_only_count': len(coen_only),
        'dataiku_only_count': len(dataiku_only),
        'field_results': field_results
    }
    
    return result

def create_excel_report(results, output_path):
    """Create Excel report with comparison results"""
    
    # Create a new Excel writer
    writer = pd.ExcelWriter(output_path, engine='xlsxwriter')
    
    # Create DataFrames for each sheet
    
    # Summary Sheet - overall view of all comparisons with individual field matches
    summary_data = []
    for mnemo, result in results.items():
        if result['status'] == 'SUCCESS':
            # Get individual field match status
            field_statuses = {}
            for field_name, field_result in result['field_results'].items():
                if 'match' in field_result:
                    field_statuses[field_name] = field_result['match']
            
            # Create a dictionary for this row
            row_data = {
                'Mnemo': mnemo,
                'ISIN Count Match': result['same_count'],
                'ISIN Set Match': result['same_isins'],
                'Coen ISINs Count': result['coen_isins_count'],
                'Dataiku ISINs Count': result['dataiku_isins_count'],
                # Individual field match columns
                'MIC Match': field_statuses.get('MIC', False),
                'Shares Match': field_statuses.get('Number of Shares', False),
                'Free Float Match': field_statuses.get('Free Float', False),
                'Capping Match': field_statuses.get('Capping (Final Capping/Capping Factor)', False),
                'Currency Match': field_statuses.get('Currency', False),
            }
            
            # Overall status
            all_fields_match = all(field_statuses.values())
            row_data['Status'] = 'OK' if (result['same_isins'] and all_fields_match) else 'ISSUE'
            
            summary_data.append(row_data)
        else:
            # For error rows
            summary_data.append({
                'Mnemo': mnemo,
                'Status': 'ERROR',
                'Error Message': result['message']
            })
    
    summary_df = pd.DataFrame(summary_data)
    summary_df.to_excel(writer, sheet_name='Summary', index=False)
    
    # Format the summary sheet
    workbook = writer.book
    summary_sheet = writer.sheets['Summary']
    
    # Add formats
    header_format = workbook.add_format({
        'bold': True,
        'text_wrap': True,
        'valign': 'vcenter',
        'bg_color': '#D9D9D9',
        'border': 1
    })
    
    ok_format = workbook.add_format({'bg_color': '#C6EFCE'})
    issue_format = workbook.add_format({'bg_color': '#FFC7CE'})
    true_format = workbook.add_format({'bg_color': '#C6EFCE', 'align': 'center'})
    false_format = workbook.add_format({'bg_color': '#FFC7CE', 'align': 'center'})
    
    # Apply header format to first row
    for col_num, value in enumerate(summary_df.columns.values):
        summary_sheet.write(0, col_num, value, header_format)
    
    # Apply conditional formatting
    match_columns = ['MIC Match', 'Shares Match', 'Free Float Match', 'Capping Match', 'Currency Match', 
                     'ISIN Count Match', 'ISIN Set Match']
    
    for row_num, row in enumerate(summary_df.values):
        for col_num, value in enumerate(row):
            col_name = summary_df.columns[col_num]
            
            # Format boolean match columns
            if col_name in match_columns and isinstance(value, bool):
                if value:
                    summary_sheet.write(row_num + 1, col_num, 'YES', true_format)
                else:
                    summary_sheet.write(row_num + 1, col_num, 'NO', false_format)
            
            # Format status column
            elif col_name == 'Status':
                if value == 'OK':
                    summary_sheet.write(row_num + 1, col_num, value, ok_format)
                else:
                    summary_sheet.write(row_num + 1, col_num, value, issue_format)
    
    # Auto-adjust column widths
    for i, column in enumerate(summary_df.columns):
        column_width = max(len(str(column)), summary_df[column].astype(str).map(len).max())
        summary_sheet.set_column(i, i, column_width + 2)
    
    # Field Mismatches Sheet - details of field comparison issues
    field_mismatch_data = []
    for mnemo, result in results.items():
        if result['status'] == 'SUCCESS':
            for field, field_result in result['field_results'].items():
                if 'match' in field_result and not field_result['match']:
                    if 'error' in field_result:
                        field_mismatch_data.append({
                            'Mnemo': mnemo,
                            'Field': field,
                            'Issue Type': 'Error',
                            'Details': field_result['error']
                        })
                    elif 'examples' in field_result:
                        for example in field_result['examples']:
                            field_mismatch_data.append({
                                'Mnemo': mnemo,
                                'Field': field,
                                'Issue Type': 'Value Mismatch',
                                'ISIN': example['ISIN'],
                                'Coen Value': example['Coen value'],
                                'Dataiku Value': example['Dataiku value']
                            })
    
    if field_mismatch_data:
        field_mismatch_df = pd.DataFrame(field_mismatch_data)
        field_mismatch_df.to_excel(writer, sheet_name='Field Mismatches', index=False)
        
        # Format the field mismatches sheet
        mismatch_sheet = writer.sheets['Field Mismatches']
        for col_num, value in enumerate(field_mismatch_df.columns.values):
            mismatch_sheet.write(0, col_num, value, header_format)
            
        # Auto-adjust column widths
        for i, column in enumerate(field_mismatch_df.columns):
            column_width = max(len(str(column)), field_mismatch_df[column].astype(str).map(len).max())
            mismatch_sheet.set_column(i, i, column_width + 2)
    
    # ISIN Differences Sheet - ISINs that appear in only one of the files
    isin_diff_data = []
    for mnemo, result in results.items():
        if result['status'] == 'SUCCESS' and not result['same_isins']:
            for isin in result['coen_only']:
                isin_diff_data.append({
                    'Mnemo': mnemo,
                    'ISIN': isin,
                    'Present In': 'Coen Only'
                })
            
            for isin in result['dataiku_only']:
                isin_diff_data.append({
                    'Mnemo': mnemo,
                    'ISIN': isin,
                    'Present In': 'Dataiku Only'
                })
    
    if isin_diff_data:
        isin_diff_df = pd.DataFrame(isin_diff_data)
        isin_diff_df.to_excel(writer, sheet_name='ISIN Differences', index=False)
        
        # Format the ISIN differences sheet
        isin_sheet = writer.sheets['ISIN Differences']
        for col_num, value in enumerate(isin_diff_df.columns.values):
            isin_sheet.write(0, col_num, value, header_format)
            
        # Auto-adjust column widths
        for i, column in enumerate(isin_diff_df.columns):
            column_width = max(len(str(column)), isin_diff_df[column].astype(str).map(len).max())
            isin_sheet.set_column(i, i, column_width + 2)
    
    # Save the Excel file
    writer.close()
    
    return output_path

def main():
    # Set folder paths
    coen_folder = r"C:\Users\CSonneveld\OneDrive - Euronext\Documents\Projects\GUI + Reviews\202506\Review 202506\Coen"
    dataiku_folder = r"C:\Users\CSonneveld\OneDrive - Euronext\Documents\Projects\GUI + Reviews\202506\Review 202506\Dataiku"
    
    # Set output folder path
    output_folder = r"C:\Users\CSonneveld\OneDrive - Euronext\Documents\Projects\GUI + Reviews\Review Comparison"
    os.makedirs(output_folder, exist_ok=True)  # Create folder if it doesn't exist
    
    # Find matching files
    matching_files = find_matching_files(coen_folder, dataiku_folder)
    
    print(f"Found {len(matching_files)} matching file pairs")
    
    # Compare each pair of files
    results = {}
    for mnemo, coen_file, dataiku_file in matching_files:
        print(f"\nComparing {mnemo}...")
        print(f"  Coen file: {Path(coen_file).name}")
        print(f"  Dataiku file: {Path(dataiku_file).name}")
        
        comparison_result = compare_files(coen_file, dataiku_file)
        results[mnemo] = comparison_result
        
        # Print summary of comparison
        if comparison_result['status'] == 'SUCCESS':
            print(f"  ISIN counts match: {comparison_result['same_count']}")
            print(f"  ISIN codes match: {comparison_result['same_isins']}")
            
            if not comparison_result['same_isins']:
                print(f"  Coen has {comparison_result['coen_only_count']} unique ISINs")
                print(f"  Dataiku has {comparison_result['dataiku_only_count']} unique ISINs")
            
            # Print field comparison results
            fields_match = all(field['match'] for field in comparison_result['field_results'].values() 
                              if 'match' in field)
            print(f"  All fields match: {fields_match}")
            
            if not fields_match:
                for field, field_result in comparison_result['field_results'].items():
                    if 'match' in field_result and not field_result['match']:
                        if 'error' in field_result:
                            print(f"  Field '{field}': {field_result['error']}")
                        else:
                            print(f"  Field '{field}': {field_result['mismatches']} mismatches")
        else:
            print(f"  Error: {comparison_result['message']}")
    
    # Overall summary
    all_match = all(r['status'] == 'SUCCESS' and r['same_isins'] and
                    all(field['match'] for field in r['field_results'].values() if 'match' in field)
                    for r in results.values())
    
    print("\n\n=== OVERALL SUMMARY ===")
    print(f"All files match: {all_match}")
    
    # List files with issues
    if not all_match:
        print("\nFiles with issues:")
        for mnemo, result in results.items():
            if result['status'] != 'SUCCESS' or not result['same_isins'] or not all(field['match'] for field in result['field_results'].values() if 'match' in field):
                print(f"- {mnemo}")
    
    # Create Excel report
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = os.path.join(output_folder, f"index_comparison_results_{timestamp}.xlsx")
    excel_report_path = create_excel_report(results, output_path)
    
    print(f"\nExcel report saved to: {excel_report_path}")
    
    # Automatically open the Excel file
    try:
        os.startfile(excel_report_path)
        print("Excel report opened automatically.")
    except AttributeError:
        # os.startfile is only available on Windows
        try:
            # Try platform-specific commands
            if os.name == 'posix':  # For Mac/Linux
                subprocess.call(('open', excel_report_path))
            else:  # Should not reach here on Windows, but as a fallback
                subprocess.call(('xdg-open', excel_report_path))
            print("Excel report opened automatically.")
        except Exception as e:
            print(f"Note: Could not open Excel report automatically. Please open it manually. Error: {e}")
    
    return results

if __name__ == "__main__":
    main()