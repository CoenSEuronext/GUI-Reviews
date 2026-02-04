# enztp_review.py
#
# Euronext EZ Transatlantic (ENZTP) review script
# Rulebook (v23-02, effective 13 Mar 2023):
# - Universe: companies included in Euronext Eurozone 300 AND North America 500 at review
# - Selection: all companies in the Index Universe
# - Weighting: Free Float Market Cap weighted (no capping factor)
# - Base currency: EUR

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


def run_enztp_review(
    date,
    co_date,
    effective_date,
    index="ENZTP",
    isin="NLIX00001015",
    area="US",
    area2="EU",
    type="STOCK",
    universe_eu="eurozone_300",
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
        ref_keys = ["ff", universe_eu, universe_na]
        ref_data = load_reference_data(current_data_folder, ref_keys)

        # --------------------------
        # Validate universe refs
        # --------------------------
        if universe_eu not in ref_data or ref_data[universe_eu] is None or ref_data[universe_eu].empty:
            raise KeyError(
                f"Universe '{universe_eu}' missing/empty in reference data. Keys: {list(ref_data.keys())}"
            )
        if universe_na not in ref_data or ref_data[universe_na] is None or ref_data[universe_na].empty:
            raise KeyError(
                f"Universe '{universe_na}' missing/empty in reference data. Keys: {list(ref_data.keys())}"
            )

        eu_df = ref_data[universe_eu].copy()
        na_df = ref_data[universe_na].copy()

        # eurozone_300 has 'Currency' (not 'Currency (Local)'); normalize to 'Currency (Local)'
        def _ensure_currency_local(df: pd.DataFrame, fallback: str) -> pd.DataFrame:
            if "Currency (Local)" in df.columns:
                return df
            if "Currency" in df.columns:
                df["Currency (Local)"] = df["Currency"]
                return df
            df["Currency (Local)"] = fallback
            return df

        eu_df = _ensure_currency_local(eu_df, currency)
        na_df = _ensure_currency_local(na_df, currency)

        required_universe_cols = ["ISIN", "MIC", "Name", "NOSH", "Currency (Local)"]
        for uni_name, uni_df in [(universe_eu, eu_df), (universe_na, na_df)]:
            missing = [c for c in required_universe_cols if c not in uni_df.columns]
            if missing:
                raise KeyError(
                    f"Universe '{uni_name}' missing required columns: {missing}. "
                    f"Columns: {list(uni_df.columns)}"
                )

        # --------------------------
        # Build combined Index Universe (straight concat)
        # --------------------------
        universe_ref_df = pd.concat([eu_df, na_df], ignore_index=True)
        logger.info(
            f"Universe concat sizes: {universe_eu}={len(eu_df)} | {universe_na}={len(na_df)} | combined={len(universe_ref_df)}"
        )

        # --------------------------
        # RIC MODE mapping + FX + prices (same pattern as ENVEU/ENTP)
        # --------------------------
        required_eod_cols = ["#Symbol", "Isin Code", "MIC", "Close Prc", "Index Curr", "FX/Index Ccy"]
        missing_eod_cols = [c for c in required_eod_cols if c not in stock_eod_df.columns]
        if missing_eod_cols:
            raise KeyError(
                f"stock_eod_df missing required columns: {missing_eod_cols}. "
                f"Columns: {list(stock_eod_df.columns)}"
            )

        required_co_cols = ["#Symbol", "Close Prc"]
        missing_co_cols = [c for c in required_co_cols if c not in stock_co_df.columns]
        if missing_co_cols:
            raise KeyError(
                f"stock_co_df missing required columns: {missing_co_cols}. "
                f"Columns: {list(stock_co_df.columns)}"
            )

        logger.info("Building ISIN+MIC -> #Symbol mapping (RIC mode, len(#Symbol) < 12)...")
        ric_rows = stock_eod_df[stock_eod_df["#Symbol"].astype(str).str.len() < 12].copy()
        symbols_filtered = (
            ric_rows[["Isin Code", "MIC", "#Symbol"]]
            .drop_duplicates(subset=["Isin Code", "MIC"], keep="first")
            .rename(columns={"Isin Code": "ISIN"})
        )

        # FX in index base currency (EUR)
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

        if "ff" not in ref_data or ref_data["ff"] is None or ref_data["ff"].empty:
            raise KeyError(f"Missing/empty reference key 'ff'. Keys: {list(ref_data.keys())}")

        ff_cols = ["ISIN Code:", "Free Float Round:"]
        missing_ff_cols = [c for c in ff_cols if c not in ref_data["ff"].columns]
        if missing_ff_cols:
            raise KeyError(
                f"ff missing columns: {missing_ff_cols}. Columns: {list(ref_data['ff'].columns)}"
            )

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
            raise ValueError("Combined universe dataframe is empty after merges.")

        missing_symbol = int(base_df["#Symbol"].isna().sum())
        missing_fx = int(base_df["FX/Index Ccy"].isna().sum())
        missing_ff = int(base_df["Free Float Round:"].isna().sum())
        logger.info(
            f"Combined universe rows: {len(base_df)} | Missing #Symbol: {missing_symbol} | Missing FX: {missing_fx} | Missing Free Float%: {missing_ff}"
        )

        # --------------------------
        # Selection: all in universe
        # --------------------------
        logger.info("Selection: taking all companies from Index Universe...")
        selection_df = base_df.copy()
        logger.info(f"Selected {len(selection_df)} constituents for {index}")

        # Output fields:
        # - Number of Shares: NOSH from universe snapshot
        # - Free Float: ff file percent (do NOT /100)
        # - Capping Factor: 1
        selection_df["Number_of_Shares_Output"] = pd.to_numeric(selection_df["NOSH"], errors="coerce")
        selection_df["Number_of_Shares_Output"] = np.round(selection_df["Number_of_Shares_Output"])

        selection_df["Free_Float_Output"] = pd.to_numeric(selection_df["Free Float Round:"], errors="coerce")

        selection_df["Capping_Factor"] = 1.0
        selection_df["Effective Date of Review"] = effective_date

        ENZTP_df = (
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

        # Inclusion/Exclusion analysis
        logger.info("Performing inclusion/exclusion analysis...")
        analysis_results = inclusion_exclusion_analysis(
            ENZTP_df,
            stock_eod_df,
            index,
            isin_column="ISIN Code",
        )
        inclusion_df = analysis_results.get("inclusion_df", pd.DataFrame())
        exclusion_df = analysis_results.get("exclusion_df", pd.DataFrame())

        logger.info(f"Inclusions: {len(inclusion_df)}, Exclusions: {len(exclusion_df)}")

        # Save output (match existing scripts naming)
        try:
            output_dir = os.path.join(os.getcwd(), "output")
            os.makedirs(output_dir, exist_ok=True)

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            enztp_path = os.path.join(output_dir, f"ENZTP_df_{timestamp}.xlsx")

            logger.info(f"Saving ENZTP output to: {enztp_path}")
            with pd.ExcelWriter(enztp_path) as writer:
                ENZTP_df.to_excel(writer, sheet_name=index + " Composition", index=False)
                inclusion_df.to_excel(writer, sheet_name="Inclusion", index=False)
                exclusion_df.to_excel(writer, sheet_name="Exclusion", index=False)
                selection_df.to_excel(writer, sheet_name="Selection", index=False)
                base_df.to_excel(writer, sheet_name="Full Universe", index=False)

            return {
                "status": "success",
                "message": "Review completed successfully",
                "data": {"enztp_path": enztp_path},
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
