# entp_review.py
#
# Euronext Transatlantic 500 (ENTP) review script
# Rulebook-aligned:
# - Universe: Euronext Europe 500 + North America 500
# - Selection: all companies in the Index Universe
# - Weighting: Free Float Market Cap weighted (no ESG ranking, no capping)
# - Capping factor: not applied -> 1
# - Free Float: from ff file (percent)
# - Base currency: EUR (used for FX/Index Ccy merge; output currency remains local like ENVEU)
#
# Output conventions follow existing review scripts (ENVEU-style).

import os
import traceback
from datetime import datetime

import numpy as np
import pandas as pd

from config import DLF_FOLDER, DATA_FOLDER2
from utils.logging_utils import setup_logging
from utils.data_loader import load_eod_data, load_reference_data
from utils.inclusion_exclusion import inclusion_exclusion_analysis

logger = setup_logging(__name__)


def run_entp_review(
    date,
    co_date,
    effective_date,
    index="ENTP",
    isin="NLIX00000926",
    area="US",
    area2="EU",
    type="STOCK",
    universe_eu="europe_500",
    universe_na="north_america_500",
    feed="Reuters",
    currency="EUR",
    year=None,
):
    try:
        year = year or str(datetime.strptime(date, "%Y%m%d").year)
        current_data_folder = os.path.join(DATA_FOLDER2, date[:6])

        logger.info("Loading EOD data...")
        index_eod_df, stock_eod_df, stock_co_df, fx_lookup_df = load_eod_data(
            date, co_date, area, area2, DLF_FOLDER
        )

        logger.info("Loading reference data...")
        ref_keys = ["ff", universe_eu, universe_na, "icb"]
        ref_data = load_reference_data(current_data_folder, ref_keys)

        if universe_eu not in ref_data or ref_data[universe_eu] is None or ref_data[universe_eu].empty:
            raise KeyError(f"Universe '{universe_eu}' missing/empty in reference data. Keys: {list(ref_data.keys())}")

        if universe_na not in ref_data or ref_data[universe_na] is None or ref_data[universe_na].empty:
            raise KeyError(f"Universe '{universe_na}' missing/empty in reference data. Keys: {list(ref_data.keys())}")

        if "ff" not in ref_data or ref_data["ff"] is None or ref_data["ff"].empty:
            raise KeyError(f"Reference key 'ff' missing/empty. Keys: {list(ref_data.keys())}")

        # ------------------------------------------------------------------
        # Build Index Universe = Europe 500 + North America 500 (straight concat)
        # ------------------------------------------------------------------
        eu_df = ref_data[universe_eu].copy()
        na_df = ref_data[universe_na].copy()

        universe_ref_df = pd.concat([eu_df, na_df], ignore_index=True)

        required_universe_cols = ["ISIN", "MIC", "Name", "NOSH"]
        missing_universe_cols = [c for c in required_universe_cols if c not in universe_ref_df.columns]
        if missing_universe_cols:
            raise KeyError(
                f"Universe missing required columns: {missing_universe_cols}. "
                f"Columns: {list(universe_ref_df.columns)}"
            )

        # Currency output like ENVEU -> prefer "Currency (Local)"
        if "Currency (Local)" not in universe_ref_df.columns:
            if "Currency" in universe_ref_df.columns:
                universe_ref_df["Currency (Local)"] = universe_ref_df["Currency"]
            else:
                # Last-resort fallback to base currency (keeps pipeline running)
                universe_ref_df["Currency (Local)"] = currency

        logger.info(f"Starting universe size (EU+NA): {len(universe_ref_df)} stocks")

        # ------------------------------------------------------------------
        # RIC MODE mapping + EOD/CO prices + FX/Index Ccy (same pattern as ENVEU)
        # ------------------------------------------------------------------
        required_eod_cols = ["#Symbol", "Isin Code", "MIC", "Close Prc", "Index Curr", "FX/Index Ccy"]
        missing_eod_cols = [c for c in required_eod_cols if c not in stock_eod_df.columns]
        if missing_eod_cols:
            raise KeyError(
                f"stock_eod_df missing required columns: {missing_eod_cols}. Columns: {list(stock_eod_df.columns)}"
            )

        required_co_cols = ["#Symbol", "Close Prc"]
        missing_co_cols = [c for c in required_co_cols if c not in stock_co_df.columns]
        if missing_co_cols:
            raise KeyError(
                f"stock_co_df missing required columns: {missing_co_cols}. Columns: {list(stock_co_df.columns)}"
            )

        logger.info("Building ISIN+MIC -> #Symbol mapping (RIC mode, len(#Symbol) < 12)...")
        ric_rows = stock_eod_df[stock_eod_df["#Symbol"].astype(str).str.len() < 12].copy()
        symbols_filtered = (
            ric_rows[["Isin Code", "MIC", "#Symbol"]]
            .drop_duplicates(subset=["Isin Code", "MIC"], keep="first")
            .rename(columns={"Isin Code": "ISIN"})
        )

        # FX merge frame: filter Index Curr == base currency (EUR), dedupe by #Symbol
        stock_eod_fx_df = stock_eod_df[stock_eod_df["Index Curr"] == currency].copy()
        fx_merge_df = (
            stock_eod_fx_df[["#Symbol", "FX/Index Ccy"]]
            .drop_duplicates(subset="#Symbol", keep="first")
            .copy()
        )

        eod_price_merge_df = (
            stock_eod_df[["#Symbol", "Close Prc"]]
            .drop_duplicates(subset="#Symbol", keep="first")
            .rename(columns={"Close Prc": "Close Prc_EOD"})
        )

        co_price_merge_df = (
            stock_co_df[["#Symbol", "Close Prc"]]
            .drop_duplicates(subset="#Symbol", keep="first")
            .rename(columns={"Close Prc": "Close Prc_CO"})
        )

        ff_cols = ["ISIN Code:", "Free Float Round:"]
        missing_ff_cols = [c for c in ff_cols if c not in ref_data["ff"].columns]
        if missing_ff_cols:
            raise KeyError(
                f"ff missing columns: {missing_ff_cols}. Columns: {list(ref_data['ff'].columns)}"
            )

        # Build base_df (universe + mappings)
        base_df = (
            universe_ref_df
            .merge(symbols_filtered, on=["ISIN", "MIC"], how="left")
            .merge(fx_merge_df, on="#Symbol", how="left")
            .merge(eod_price_merge_df, on="#Symbol", how="left")
            .merge(co_price_merge_df, on="#Symbol", how="left")
            .merge(
                ref_data["ff"][ff_cols].drop_duplicates(subset="ISIN Code:", keep="first"),
                left_on="ISIN",
                right_on="ISIN Code:",
                how="left",
            )
            .drop("ISIN Code:", axis=1)
        )

        if base_df is None or base_df.empty:
            raise ValueError("Failed to build base universe dataframe (base_df is empty).")

        missing_symbol = int(base_df["#Symbol"].isna().sum())
        missing_fx = int(base_df["FX/Index Ccy"].isna().sum())
        missing_price = int(base_df["Close Prc_EOD"].isna().sum())
        logger.info(
            f"Universe rows: {len(base_df)} | Missing #Symbol: {missing_symbol} | Missing FX: {missing_fx} | Missing EOD price: {missing_price}"
        )

        # ------------------------------------------------------------------
        # ENTP selection = entire universe (no ranking, no exclusions specified)
        # ------------------------------------------------------------------
        universe_df = base_df.copy()
        selection_df = universe_df.copy()

        # Output fields:
        # - Number of Shares: use NOSH (listed shares) from universe data
        # - Free Float: from ff file, in percent ("Free Float Round:")
        # - Capping Factor: not applied -> 1
        selection_df["Number_of_Shares_Output"] = pd.to_numeric(selection_df["NOSH"], errors="coerce")
        selection_df["Number_of_Shares_Output"] = np.round(selection_df["Number_of_Shares_Output"])

        selection_df["Free_Float_Output"] = pd.to_numeric(selection_df["Free Float Round:"], errors="coerce")

        selection_df["Capping_Factor"] = 1.0
        selection_df["Effective Date of Review"] = effective_date

        ENTP_df = (
            selection_df[
                [
                    "Name",
                    "ISIN",
                    "MIC",
                    "Number_of_Shares_Output",
                    "Free_Float_Output",
                    "Capping_Factor",
                    "Effective Date of Review",
                    "Currency (Local)",
                ]
            ]
            .rename(
                columns={
                    "Name": "Company",
                    "ISIN": "ISIN Code",
                    "Number_of_Shares_Output": "Number of Shares",
                    "Free_Float_Output": "Free Float",
                    "Capping_Factor": "Capping Factor",
                    "Currency (Local)": "Currency",
                }
            )
            .sort_values("Company")
        )

        logger.info(f"Selected {len(ENTP_df)} constituents for {index} index (full universe)")

        # Inclusion/Exclusion analysis
        logger.info("Performing inclusion/exclusion analysis...")
        analysis_results = inclusion_exclusion_analysis(
            ENTP_df,
            stock_eod_df,
            index,
            isin_column="ISIN Code",
        )
        inclusion_df = analysis_results.get("inclusion_df", pd.DataFrame())
        exclusion_df = analysis_results.get("exclusion_df", pd.DataFrame())

        logger.info(f"Inclusions: {len(inclusion_df)}, Exclusions: {len(exclusion_df)}")

        # Save output
        try:
            output_dir = os.path.join(os.getcwd(), "output")
            os.makedirs(output_dir, exist_ok=True)

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            entp_path = os.path.join(output_dir, f"ENTP_df_{timestamp}.xlsx")

            logger.info(f"Saving ENTP output to: {entp_path}")
            with pd.ExcelWriter(entp_path) as writer:
                ENTP_df.to_excel(writer, sheet_name=index + " Composition", index=False)
                inclusion_df.to_excel(writer, sheet_name="Inclusion", index=False)
                exclusion_df.to_excel(writer, sheet_name="Exclusion", index=False)
                selection_df.to_excel(writer, sheet_name="Selection", index=False)
                universe_df.to_excel(writer, sheet_name="Full Universe", index=False)

            return {
                "status": "success",
                "message": "Review completed successfully",
                "data": {"entp_path": entp_path},
            }

        except Exception as e:
            error_msg = f"Error saving output file: {str(e)}"
            logger.error(error_msg)
            return {"status": "error", "message": error_msg, "data": None}

    except Exception as e:
        logger.error(f"Error during review calculation: {str(e)}")
        logger.error(traceback.format_exc())
        return {
            "status": "error",
            "message": f"Error during review calculation: {str(e)}",
            "traceback": traceback.format_exc(),
            "data": None,
        }
