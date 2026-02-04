# Review/reviews/envuk_review.py
#
# Euronext Sustainable UK 20 (ENVUK) review script
#
# Key fix:
# - Use Reuters "Index Curr == EUR" slice consistently for BOTH price and FX, then use the EUR price directly
#   (do NOT multiply by FX again). This prevents the double-conversion that was inflating NOSH by ~100 * FX.
#
# Notes:
# - Keeps "Currency (Local)" from the universe ref file for output labelling (GBP -> GBX alignment kept)
# - Tie-break FF Market Cap now uses EUR-view price directly (no FX multiplier)
# - Shares calc uses EUR-view price directly (no FX multiplier)
# - Output sorted alphabetically ignoring punctuation/apostrophes (so D'IETEREN sorts like DIETEREN)

import pandas as pd
import numpy as np
from datetime import datetime
import os
import traceback

from Review.functions import read_semicolon_csv  # kept for consistency with repo style
from config import DLF_FOLDER, DATA_FOLDER, DATA_FOLDER2
from utils.logging_utils import setup_logging
from utils.data_loader import load_eod_data, load_reference_data
from utils.inclusion_exclusion import inclusion_exclusion_analysis

logger = setup_logging(__name__)


def _company_sort_key(name: str) -> str:
    if name is None:
        return ""
    # Ignore punctuation/apostrophes etc for sorting
    return (
        str(name)
        .upper()
        .strip()
        .replace("’", "'")
        .replace("`", "'")
    )


def run_envuk_review(
    date,
    co_date,
    effective_date,
    index="ENVUK",
    isin="QS0011250931",  # Environment UK Index ISIN
    area="US",
    area2="EU",
    type="STOCK",
    universe="europe_500",  # STRICT: rulebook universe starts from Europe 500
    feed="Reuters",
    currency="EUR",  # Index/base currency for the EUR-view Reuters slice
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
        ref_keys = ["ff", universe, "icb", "sustainalytics"]
        ref_data = load_reference_data(current_data_folder, ref_keys)

        # STRICT: require universe key
        if universe not in ref_data:
            raise KeyError(
                f"Universe '{universe}' not found in reference data keys: {list(ref_data.keys())}"
            )
        universe_ref_df = ref_data[universe]
        if universe_ref_df is None or universe_ref_df.empty:
            raise ValueError(f"Universe reference dataframe '{universe}' is empty")

        # STRICT: require ISIN + MIC in universe
        missing_universe_cols = [c for c in ["ISIN", "MIC"] if c not in universe_ref_df.columns]
        if missing_universe_cols:
            raise KeyError(
                f"Universe '{universe}' missing required columns: {missing_universe_cols}. "
                f"Columns: {list(universe_ref_df.columns)}"
            )

        # Keep currency column consistent with other scripts (used for output labelling)
        if "Currency (Local)" not in universe_ref_df.columns:
            raise KeyError(
                f"Universe '{universe}' missing required column 'Currency (Local)'. "
                f"Columns: {list(universe_ref_df.columns)}"
            )

        # Align currency labelling (GBP -> GBX) for output alignment with market data conventions
        universe_ref_df = universe_ref_df.copy()
        universe_ref_df["Currency (Local)"] = universe_ref_df["Currency (Local)"].replace({"GBP": "GBX"})

        # ------------------------------------------------------------------
        # REQUIRED COLUMNS
        # ------------------------------------------------------------------
        required_eod_cols = ["#Symbol", "Isin Code", "MIC", "Close Prc", "Index Curr", "FX/Index Ccy", "Currency"]
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

        # ------------------------------------------------------------------
        # BUILD ISIN+MIC -> #Symbol mapping (RIC mode, len(#Symbol) < 12)
        # ------------------------------------------------------------------
        logger.info("Building ISIN+MIC -> #Symbol mapping (RIC mode, len(#Symbol) < 12)...")
        ric_rows = stock_eod_df[stock_eod_df["#Symbol"].astype(str).str.len() < 12].copy()

        symbols_filtered = (
            ric_rows[["Isin Code", "MIC", "#Symbol"]]
            .drop_duplicates(subset=["Isin Code", "MIC"], keep="first")
            .rename(columns={"Isin Code": "ISIN"})
        )

        # ------------------------------------------------------------------
        # CRITICAL FIX:
        # Use a single consistent EUR-view slice for BOTH price and FX.
        # If we take Close Prc from Index Curr == EUR, it is already in EUR,
        # so we MUST NOT multiply by FX again later.
        # ------------------------------------------------------------------
        stock_eod_eur_df = stock_eod_df[stock_eod_df["Index Curr"] == currency].copy()

        eur_merge_df = (
            stock_eod_eur_df[["#Symbol", "Close Prc", "FX/Index Ccy", "Currency"]]
            .drop_duplicates(subset="#Symbol", keep="first")
            .rename(columns={"Close Prc": "Close Prc_EUR"})
        )

        # CO price (as provided; typically local)
        co_price_merge_df = (
            stock_co_df[["#Symbol", "Close Prc"]]
            .drop_duplicates(subset="#Symbol", keep="first")
            .rename(columns={"Close Prc": "Close Prc_CO"})
        )

        # Sustainalytics required columns
        if "sustainalytics" not in ref_data:
            raise KeyError(f"Missing reference key 'sustainalytics'. Keys: {list(ref_data.keys())}")
        sust_cols = ["ISIN", "ESG Risk Score", "Overall Global Compact Compliance Status"]
        missing_sust_cols = [c for c in sust_cols if c not in ref_data["sustainalytics"].columns]
        if missing_sust_cols:
            raise KeyError(
                f"sustainalytics missing columns: {missing_sust_cols}. "
                f"Columns: {list(ref_data['sustainalytics'].columns)}"
            )

        # Free float file required for tie-break FFMC
        if "ff" not in ref_data:
            raise KeyError(f"Missing reference key 'ff'. Keys: {list(ref_data.keys())}")
        ff_cols = ["ISIN Code:", "Free Float Round:"]
        missing_ff_cols = [c for c in ff_cols if c not in ref_data["ff"].columns]
        if missing_ff_cols:
            raise KeyError(
                f"ff missing columns: {missing_ff_cols}. "
                f"Columns: {list(ref_data['ff'].columns)}"
            )

        # ------------------------------------------------------------------
        # Build base_df (note: uses EUR-view Close Prc_EUR)
        # ------------------------------------------------------------------
        base_df = (
            universe_ref_df
            .merge(symbols_filtered, on=["ISIN", "MIC"], how="left")
            .merge(eur_merge_df, on="#Symbol", how="left")
            .merge(co_price_merge_df, on="#Symbol", how="left")
            .merge(
                ref_data["sustainalytics"][sust_cols].drop_duplicates(subset="ISIN", keep="first"),
                on="ISIN",
                how="left",
            )
            .merge(
                ref_data["ff"][ff_cols].drop_duplicates(subset="ISIN Code:", keep="first"),
                left_on="ISIN",
                right_on="ISIN Code:",
                how="left",
            )
            .drop("ISIN Code:", axis=1)
        )

        if base_df is None or len(base_df) == 0:
            raise ValueError("Failed to load one or more required reference data files")

        missing_symbol = int(base_df["#Symbol"].isna().sum())
        missing_price_eur = int(base_df["Close Prc_EUR"].isna().sum())
        missing_fx = int(base_df["FX/Index Ccy"].isna().sum())
        logger.info(
            f"Universe rows: {len(base_df)} | Missing #Symbol: {missing_symbol} | "
            f"Missing EUR price: {missing_price_eur} | Missing FX: {missing_fx}"
        )

        # STEP 1: Index Universe = Europe 500 AND MIC = XLON
        logger.info("Step 1: Using Euronext Europe 500 as starting universe...")
        universe_df = base_df.copy()
        logger.info(f"Starting universe size: {len(universe_df)} stocks")

        logger.info("Filtering for Main Listing on London Stock Exchange (MIC: XLON)...")
        universe_df = universe_df[universe_df["MIC"] == "XLON"].copy()
        logger.info(f"Universe size after MIC filter (XLON): {len(universe_df)} stocks")

        # STEP 2: Exclude Sustainalytics NON-COMPLIANT
        logger.info("Step 2: Applying Sustainalytics NON-COMPLIANT exclusion...")
        universe_df = universe_df[
            (universe_df["Overall Global Compact Compliance Status"] != "Non-Compliant")
            | (universe_df["Overall Global Compact Compliance Status"].isna())
        ].copy()
        logger.info(f"Universe size after NON-COMPLIANT screening: {len(universe_df)} stocks")

        # STEP 3: Rank by ESG Risk Score asc; tie-break by FF Market Cap desc
        logger.info("Step 3: Ranking by ESG Risk Score and Free Float Market Cap...")

        # Guard required cols for FFMC calc (NOSH is expected in universe reference)
        if "NOSH" not in universe_df.columns:
            raise KeyError(
                f"Universe '{universe}' missing required column 'NOSH' for FF Market Cap tie-break. "
                f"Columns: {list(universe_df.columns)}"
            )

        universe_df["ESG_Risk_Rating"] = pd.to_numeric(
            universe_df["ESG Risk Score"], errors="coerce"
        ).fillna(999.0)

        # IMPORTANT: Use EUR-view price directly; do NOT multiply by FX here.
        universe_df["FF_Market_Cap"] = (
            pd.to_numeric(universe_df["NOSH"], errors="coerce")
            * pd.to_numeric(universe_df["Close Prc_EUR"], errors="coerce")
            * (pd.to_numeric(universe_df["Free Float Round:"], errors="coerce") / 100.0)
        )

        universe_df = universe_df.sort_values(
            by=["ESG_Risk_Rating", "FF_Market_Cap"],
            ascending=[True, False],
        ).reset_index(drop=True)

        # STEP 4: Select top 20
        logger.info("Step 4: Selecting top 20 constituents...")
        selection_df = universe_df.head(20).copy()
        logger.info(f"Selected {len(selection_df)} constituents for {index} index")

        # WEIGHTING: 1 / ESG risk score (no explicit cap in this rulebook)
        logger.info("Calculating ESG-based weights (1 / ESG risk score)...")

        risk = pd.to_numeric(selection_df["ESG Risk Score"], errors="coerce")
        finite_risk = risk[np.isfinite(risk) & (risk > 0)]
        worst_risk = float(finite_risk.max()) if len(finite_risk) else 1000.0
        risk_for_weight = risk.where((risk > 0) & np.isfinite(risk), worst_risk)

        selection_df["ESG_Risk_For_Weight"] = risk_for_weight
        selection_df["ESG_Score"] = 1.0 / selection_df["ESG_Risk_For_Weight"]

        total_esg_score = float(selection_df["ESG_Score"].sum())
        if total_esg_score <= 0 or not np.isfinite(total_esg_score):
            raise ValueError("Total ESG score is invalid (cannot compute weights).")

        selection_df["Weight_Uncapped"] = selection_df["ESG_Score"] / total_esg_score

        # No capping step here (rulebook doesn't specify one)
        selection_df["Weight_Final"] = selection_df["Weight_Uncapped"] / selection_df["Weight_Uncapped"].sum()
                # Calculate raw capping factor
        selection_df["Capping_Factor"] = np.where(
            selection_df["Weight_Uncapped"] > 0,
            selection_df["Weight_Final"] / selection_df["Weight_Uncapped"],
            1.0
        )

        # Normalize capping factors by dividing by the maximum capping factor
        max_capping = selection_df["Capping_Factor"].max()
        if max_capping > 0 and np.isfinite(max_capping):
            selection_df["Capping_Factor"] = selection_df["Capping_Factor"] / max_capping

        # Round to 14 decimal places
        selection_df["Capping_Factor"] = selection_df["Capping_Factor"].round(14)

        logger.info(
            f"Weight range: {selection_df['Weight_Final'].min():.2%} to {selection_df['Weight_Final'].max():.2%}"
        )

        # INDEX MARKET CAP ANCHOR
        if "#Symbol" not in index_eod_df.columns or "Mkt Cap" not in index_eod_df.columns:
            raise KeyError(
                f"index_eod_df missing required columns '#Symbol' and/or 'Mkt Cap'. "
                f"Columns: {list(index_eod_df.columns)}"
            )

        idx_mask = index_eod_df["#Symbol"].astype(str).str.strip().eq(str(isin).strip())
        if not idx_mask.any():
            raise KeyError(
                f"Index ISIN '{isin}' not found in index_eod_df['#Symbol'] - cannot fetch index market cap."
            )

        index_mcap = float(index_eod_df.loc[idx_mask, "Mkt Cap"].iloc[0])

        # Price in INDEX currency (EUR): use EUR-view Close Prc directly (do NOT multiply by FX)
        selection_df["Price_Index_Ccy"] = pd.to_numeric(selection_df["Close Prc_EUR"], errors="coerce")

        selection_df["Target_Market_Cap"] = selection_df["Weight_Final"] * index_mcap
        selection_df["Number_of_Shares_Calculated"] = np.round(
            pd.to_numeric(selection_df["Target_Market_Cap"], errors="coerce")
            / pd.to_numeric(selection_df["Price_Index_Ccy"], errors="coerce")
        )

        selection_df["Free Float companies"] = 1
        selection_df["Effective Date of Review"] = effective_date

        # Output composition
        ENVUK_df = (
            selection_df[
                [
                    "Name",
                    "ISIN",
                    "MIC",
                    "Number_of_Shares_Calculated",
                    "Free Float companies",
                    "Capping_Factor",
                    "Effective Date of Review",
                    "Currency (Local)",
                ]
            ]
            .rename(
                columns={
                    "Name": "Company",
                    "ISIN": "ISIN Code",
                    "Number_of_Shares_Calculated": "Number of Shares",
                    "Free Float companies": "Free Float",
                    "Capping_Factor": "Capping Factor",
                    "Currency (Local)": "Currency",
                }
            )
        )

        # Sorting: ignore punctuation/apostrophes so D'IETEREN sorts like DIETEREN
        ENVUK_df["_Company_Sort_Key"] = (
            ENVUK_df["Company"]
            .astype(str)
            .str.upper()
            .str.replace("’", "'", regex=False)
            .str.replace("`", "'", regex=False)
            .str.replace(r"[^A-Z0-9]+", "", regex=True)
        )
        ENVUK_df = ENVUK_df.sort_values("_Company_Sort_Key").drop(columns=["_Company_Sort_Key"]).reset_index(drop=True)

        # Inclusion/Exclusion analysis
        logger.info("Performing inclusion/exclusion analysis...")
        analysis_results = inclusion_exclusion_analysis(
            ENVUK_df,
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
            envuk_path = os.path.join(output_dir, f"ENVUK_df_{timestamp}.xlsx")

            logger.info(f"Saving ENVUK output to: {envuk_path}")
            with pd.ExcelWriter(envuk_path) as writer:
                ENVUK_df.to_excel(writer, sheet_name=index + " Composition", index=False)
                inclusion_df.to_excel(writer, sheet_name="Inclusion", index=False)
                exclusion_df.to_excel(writer, sheet_name="Exclusion", index=False)
                pd.DataFrame({"Index Market Cap": [index_mcap], "Index Currency": [currency]}).to_excel(
                    writer, sheet_name="Index Market Cap", index=False
                )
                selection_df.to_excel(writer, sheet_name="Selection", index=False)
                universe_df.to_excel(writer, sheet_name="Full Universe", index=False)

            return {
                "status": "success",
                "message": "Review completed successfully",
                "data": {"envuk_path": envuk_path},
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
