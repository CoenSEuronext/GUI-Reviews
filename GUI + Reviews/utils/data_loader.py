# utils/data_loader.py
import os
import pandas as pd

from .file_cache import (
    load_df_cached,
    make_variant,
    read_excel_cached,
    read_semicolon_csv_cached,
    get_excel_sheet_names_cached,
)


def load_eod_data(date, co_date, area, area2, dlf_folder):
    """
    Load and combine EOD data.
    NOTE: We always load BOTH US + EU regions (area/area2 inputs are ignored).
    """
    region_us = "US"
    region_eu = "EU"

    def safe_load_csv(file_path, description):
        try:
            df = read_semicolon_csv_cached(file_path, encoding="latin1")
            if df is None:
                print(f"Warning: {description} file not found: {file_path}")
            return df
        except Exception as e:
            print(f"Error loading {description}: {str(e)}")
            return None

    files_to_load = {
        "index_eod_us": os.path.join(dlf_folder, f"TTMIndex{region_us}1_GIS_EOD_INDEX_{date}.csv"),
        "stock_eod_us": os.path.join(dlf_folder, f"TTMIndex{region_us}1_GIS_EOD_STOCK_{date}.csv"),
        "index_eod_eu": os.path.join(dlf_folder, f"TTMIndex{region_eu}1_GIS_EOD_INDEX_{date}.csv"),
        "stock_eod_eu": os.path.join(dlf_folder, f"TTMIndex{region_eu}1_GIS_EOD_STOCK_{date}.csv"),
        "stock_co_us": os.path.join(dlf_folder, f"TTMIndex{region_us}1_GIS_EOD_STOCK_{co_date}.csv"),
        "stock_co_eu": os.path.join(dlf_folder, f"TTMIndex{region_eu}1_GIS_EOD_STOCK_{co_date}.csv"),
    }

    loaded_data = {}
    for key, file_path in files_to_load.items():
        loaded_data[key] = safe_load_csv(file_path, key.replace("_", " ").title())

    index_dfs = [df for df in [loaded_data["index_eod_us"], loaded_data["index_eod_eu"]] if df is not None]
    if not index_dfs:
        raise ValueError("No index EOD files found! At least one index file is required.")

    index_eod_df = pd.concat(index_dfs, ignore_index=True) if len(index_dfs) > 1 else index_dfs[0]
    print(f"Loaded index data from {len(index_dfs)} file(s)")

    stock_eod_dfs = [df for df in [loaded_data["stock_eod_us"], loaded_data["stock_eod_eu"]] if df is not None]
    if not stock_eod_dfs:
        raise ValueError("No stock EOD files found! At least one stock EOD file is required.")

    stock_eod_df = pd.concat(stock_eod_dfs, ignore_index=True) if len(stock_eod_dfs) > 1 else stock_eod_dfs[0]
    print(f"Loaded stock EOD data from {len(stock_eod_dfs)} file(s)")

    stock_co_dfs = [df for df in [loaded_data["stock_co_us"], loaded_data["stock_co_eu"]] if df is not None]
    if not stock_co_dfs:
        raise ValueError("No stock CO files found! At least one stock CO file is required.")

    stock_co_df = pd.concat(stock_co_dfs, ignore_index=True) if len(stock_co_dfs) > 1 else stock_co_dfs[0]
    print(f"Loaded stock CO data from {len(stock_co_dfs)} file(s)")

    # Add 'Index Curr' column to stock_eod_df by merging with index_eod_df
    stock_eod_df = stock_eod_df.merge(
        index_eod_df[["Mnemo", "Curr"]],
        left_on="Index",
        right_on="Mnemo",
        how="left",
        suffixes=("", "_index"),
    )
    stock_eod_df = stock_eod_df.rename(columns={"Curr": "Index Curr"}).drop(columns=["Mnemo_index"])

    # Add 'Index Curr' column to stock_co_df by merging with index_eod_df
    stock_co_df = stock_co_df.merge(
        index_eod_df[["Mnemo", "Curr"]],
        left_on="Index",
        right_on="Mnemo",
        how="left",
        suffixes=("", "_index"),
    )
    stock_co_df = stock_co_df.rename(columns={"Curr": "Index Curr"}).drop(columns=["Mnemo_index"])

    # FX lookup table
    fx_lookup_df = stock_eod_df[["Currency", "Index Curr", "FX/Index Ccy"]].copy()
    fx_lookup_df["Currency_Normalized"] = fx_lookup_df["Currency"].replace({"GBX": "GBP"})
    fx_lookup_df = (
        fx_lookup_df[["Currency_Normalized", "Index Curr", "FX/Index Ccy"]]
        .drop_duplicates(subset=["Currency_Normalized", "Index Curr"], keep="first")
        .rename(columns={"Currency_Normalized": "From_Currency", "Index Curr": "To_Currency"})
    )

    print(f"Created FX lookup table with {len(fx_lookup_df)} unique currency pairs")
    print(
        f"Available currency pairs: {fx_lookup_df['From_Currency'].nunique()} currencies -> {fx_lookup_df['To_Currency'].nunique()} index currencies"
    )

    return index_eod_df, stock_eod_df, stock_co_df, fx_lookup_df


def load_reference_data(current_data_folder, required_files=None, universe_name=None, sheet_names=None):
    """
    Load reference data files (cached per underlying file + read parameters + postprocessing).
    """
    sheet_names = sheet_names or {}

    def _cached_excel(path, *, cache_tag: str, **kwargs):
        variant = make_variant("excel", cache_tag=cache_tag, **kwargs)
        return load_df_cached(path, variant, loader=lambda: pd.read_excel(path, **kwargs))

    def _cached_excel_dropdup(path, *, cache_tag: str, subset, keep="first", **kwargs):
        variant = make_variant("excel", cache_tag=cache_tag, dropdup_subset=tuple(subset), dropdup_keep=keep, **kwargs)
        return load_df_cached(
            path,
            variant,
            loader=lambda: pd.read_excel(path, **kwargs).drop_duplicates(subset=list(subset), keep=keep),
        )

    def _cached_csv_semicolon(path, *, cache_tag: str, encoding="latin1"):
        variant = make_variant("csv:semicolon", cache_tag=cache_tag, encoding=encoding)
        return load_df_cached(path, variant, loader=lambda: read_semicolon_csv_cached(path, encoding=encoding))

    all_files = {
        "ff": {
            "filename": "FF.xlsx",
            "loader": lambda f: _cached_excel_dropdup(f, cache_tag="ff", subset=["ISIN Code:"], keep="first"),
        },
        "developed_market": {
            "filename": "Developed Market.xlsx",
            "loader": lambda f: _cached_excel(f, cache_tag="developed_market"),
        },
        "icb": {
            "filename": "ICB.xlsx",
            "loader": lambda f: _cached_excel(f, cache_tag="icb", header=3),
        },
        "98_universe": {
            "filename": "universe_investable_final_ffmc.csv",
            "loader": lambda f: _cached_csv_semicolon(f, cache_tag="98_universe", encoding="latin1"),
        },
        "emerging_market": {
            "filename": "Emerging Market.xlsx",
            "loader": lambda f: _cached_excel(f, cache_tag="emerging_market"),
        },
        "nace": {
            "filename": "NACE.xlsx",
            "loader": lambda f: _cached_excel(f, cache_tag="nace"),
        },
        "oekom_trustcarbon": {
            "filename": "Oekom Trust&Carbon.xlsx",
            "loader": lambda f: _cached_excel(f, cache_tag="oekom_trustcarbon", header=1),
        },
        "sesamm": {
            "filename": "SESAMm.xlsx",
            "loader": lambda f: _cached_excel(f, cache_tag="sesamm"),
        },
        "cac_family": {
            "filename": "CAC Family.xlsx",
            "loader": lambda f, sheet=None: _cached_excel(f, cache_tag="cac_family", header=1, sheet_name=sheet if sheet else 0),
        },
        "aex_family": {
            "filename": "AEX Family.xlsx",
            "loader": lambda f, sheet=None: _cached_excel(f, cache_tag="aex_family", header=1, sheet_name=sheet if sheet else 0),
        },
        "oekom_score": {
            "filename": "Oekom Score.xlsx",
            "loader": lambda f: _cached_excel(f, cache_tag="oekom_score"),
        },
        "eurozone_300": {
            "filename": "Eurozone 300.xlsx",
            "loader": lambda f: _cached_excel(f, cache_tag="eurozone_300"),
        },
        "north_america_500": {
            "filename": "North America 500.xlsx",
            "loader": lambda f: _cached_excel(f, cache_tag="north_america_500"),
        },
        "europe_500": {
            "filename": "Europe 500.xlsx",
            "loader": lambda f: _cached_excel(f, cache_tag="europe_500"),
        },
        "asia_pacific_500": {
            "filename": "Asia Pacific 500.xlsx",
            "loader": lambda f: _cached_excel(f, cache_tag="asia_pacific_500"),
        },
        "aex_bel": {
            "filename": "AEX BEL20.xlsx",
            "loader": lambda f: _cached_excel(f, cache_tag="aex_bel"),
        },
        "master_report": {
            "filename": "Master Report.xlsx",
            "loader": lambda f: _cached_excel(f, cache_tag="master_report", header=1),
        },
        "eu_taxonomy_pocket": {
            "filename": "EuTaxonomyPocket_after_Committee.xlsx",
            "loader": lambda f, sheet=None: _cached_excel(f, cache_tag="eu_taxonomy_pocket", sheet_name=sheet if sheet else 0),
        },
        "gafi_black_list": {
            "filename": "20250221_GAFI_Black_List.xlsx",
            "loader": lambda f, sheet=None: _cached_excel(f, cache_tag="gafi_black_list", sheet_name=sheet if sheet else 0),
        },
        "gafi_grey_list": {
            "filename": "20250221_GAFI_Grey_List.xlsx",
            "loader": lambda f, sheet=None: _cached_excel(f, cache_tag="gafi_grey_list", sheet_name=sheet if sheet else 0),
        },
        "non_fiscally_cooperative_with_eu": {
            "filename": "20250221_Non_Fiscally_Cooperative_with_EU.xlsx",
            "loader": lambda f, sheet=None: _cached_excel(f, cache_tag="non_fiscally_cooperative_with_eu", sheet_name=sheet if sheet else 0),
        },
        "cdp_climate": {
            "filename": "CDP Climate.xlsx",
            "loader": lambda f, sheet=None: _cached_excel(f, cache_tag="cdp_climate", sheet_name=sheet if sheet else 0),
        },
        "euronext_world": {
            "filename": "Euronext World.xlsx",
            "loader": lambda f: _cached_excel(f, cache_tag="euronext_world"),
        },
        "sbf_120": {
            "filename": "SBF120.xlsx",
            "loader": lambda f: _cached_excel(f, cache_tag="sbf_120", header=1),
        },
        "edwpt": {
            "filename": "EDWPT.xlsx",
            "loader": lambda f: _cached_excel(f, cache_tag="edwpt"),
        },
        "euspt": {
            "filename": "EUSPT.xlsx",
            "loader": lambda f: _cached_excel(f, cache_tag="euspt"),
        },
        "deupt": {
            "filename": "DEUPT.xlsx",
            "loader": lambda f: _cached_excel(f, cache_tag="deupt"),
        },
        "dappt": {
            "filename": "DAPPT.xlsx",
            "loader": lambda f: _cached_excel(f, cache_tag="dappt"),
        },
        "deup": {
            "filename": "DEUP.xlsx",
            "loader": lambda f: _cached_excel(f, cache_tag="deup"),
        },
        "dezp": {
            "filename": "DEZP.xlsx",
            "loader": lambda f: _cached_excel(f, cache_tag="dezp"),
        },
        "edwp": {
            "filename": "EDWP.xlsx",
            "loader": lambda f: _cached_excel(f, cache_tag="edwp"),
        },
        "eusp": {
            "filename": "EUSP.xlsx",
            "loader": lambda f: _cached_excel(f, cache_tag="eusp"),
        },
        "sustainalytics": {
            "filename": "Sustainalytics.xlsx",
            "loader": lambda f: _cached_excel(f, cache_tag="sustainalytics"),
        },
        "dezpt": {
            "filename": "DEZPT.xlsx",
            "loader": lambda f: _cached_excel(f, cache_tag="dezpt"),
        },
    }

    if required_files is None:
        required_files = [k for k in all_files.keys() if k != "universe" or universe_name is not None]

    results = {}

    for file_key in required_files:
        if file_key not in all_files:
            continue

        try:
            file_path = os.path.join(current_data_folder, all_files[file_key]["filename"])

            if file_key in sheet_names:
                sheet = sheet_names[file_key]

                if file_key in ["cac_family", "aex_family"]:
                    sheets = get_excel_sheet_names_cached(file_path)
                    if sheets is None:
                        print(f"Error checking sheets in {file_path}: file not found")
                        results[file_key] = None
                        continue
                    if sheet not in sheets:
                        print(
                            f"Sheet '{sheet}' not found in {file_path}. Available sheets: {', '.join(sheets)}"
                        )
                        results[file_key] = None
                        continue

                    results[file_key] = all_files[file_key]["loader"](file_path, sheet)
                else:
                    results[file_key] = all_files[file_key]["loader"](file_path, sheet)
            else:
                results[file_key] = all_files[file_key]["loader"](file_path)

        except Exception as e:
            print(f"Error loading {file_key}: {str(e)}")
            results[file_key] = None

    return results
