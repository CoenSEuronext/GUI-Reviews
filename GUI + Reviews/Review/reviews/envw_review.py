# envw_review.py

import pandas as pd
import numpy as np
from datetime import datetime
import os
import traceback

from Review.functions import read_semicolon_csv
from config import DLF_FOLDER, DATA_FOLDER, DATA_FOLDER2
from utils.logging_utils import setup_logging
from utils.data_loader import load_eod_data, load_reference_data
from utils.inclusion_exclusion import inclusion_exclusion_analysis

logger = setup_logging(__name__)


def run_envw_review(
    date,
    co_date,
    effective_date,
    index="ENVW",
    isin="QS0011250840",
    area="US",
    area2="EU",
    type="STOCK",
    universe="edwpt",
    feed="Reuters",
    currency="USD",
    year=None,
):
    """
    ENVW review script (kept close to original repo structure):
      - Universe: provided by reference key `universe`
      - Exclude Sustainalytics Non-Compliant
      - Rank by ESG Risk Score asc; tie-break by Free Float Market Cap desc
      - Select top 120
      - Weighting: 1 / ESG risk score
      - Apply 10% cap (iterative while-loop, like original scripts)
      - Shares: Target_Market_Cap / (Close Prc_EOD * FX/Index Ccy)
    """
    try:
        year = year or str(datetime.strptime(date, "%Y%m%d").year)
        current_data_folder = os.path.join(DATA_FOLDER2, date[:6])

        logger.info("Loading EOD data...")
        index_eod_df, stock_eod_df, stock_co_df, fx_lookup_df = load_eod_data(  # Added fx_lookup_df
            date, co_date, area, area2, DLF_FOLDER
        )
        
        # Print first 10 rows of fx_lookup_df
        print("\n=== FX Lookup Table (first 10 rows) ===")
        print(fx_lookup_df.head(10).to_string(index=False))
        print(f"Total currency pairs available: {len(fx_lookup_df)}\n")

        logger.info("Loading reference data...")
        ref_keys = ["ff", universe, "icb", "sustainalytics"]
        ref_data = load_reference_data(current_data_folder, ref_keys)

        if universe not in ref_data:
            raise KeyError(
                f"Universe '{universe}' not found in reference data keys: {list(ref_data.keys())}"
            )

        universe_ref_df = ref_data[universe]
        if universe_ref_df is None or universe_ref_df.empty:
            raise ValueError(f"Universe reference dataframe '{universe}' is empty")

        # ------------------------------------------------------------------
        # RIC MODE (len(#Symbol) < 12):
        # - ISIN+MIC -> #Symbol mapping from stock_eod_df
        # - FX from fx_lookup_df using Currency (Local) -> currency
        # - Prices come from stock_eod_df / stock_co_df on #Symbol
        # ------------------------------------------------------------------
        logger.info("Building ISIN+MIC -> #Symbol mapping (RIC mode, len(#Symbol) < 12)...")
        symbols_filtered = (
            stock_eod_df[stock_eod_df["#Symbol"].astype(str).str.len() < 12][
                ["Isin Code", "MIC", "#Symbol"]
            ]
            .drop_duplicates(subset=["Isin Code", "MIC"], keep="first")
            .rename(columns={"Isin Code": "ISIN"})
        )

        # Use fx_lookup_df to get FX rates for the target currency
        fx_for_currency = fx_lookup_df[fx_lookup_df['To_Currency'] == currency][['From_Currency', 'FX/Index Ccy']].copy()
        
        print(f"\nFX rates available for target currency '{currency}':")
        print(f"  - {len(fx_for_currency)} currency pairs")
        print(f"  - Currencies: {sorted(fx_for_currency['From_Currency'].unique())}\n")

        # Prices: NOT currency-filtered (assume local close)
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

        # Normalize Currency (Local) for GBX -> GBP conversion
        universe_ref_df['Currency_Normalized'] = universe_ref_df['Currency (Local)'].replace({'GBX': 'GBP'})

        base_df = (
            universe_ref_df
            .merge(symbols_filtered, on=["ISIN", "MIC"], how="left")
            .merge(fx_for_currency, left_on='Currency_Normalized', right_on='From_Currency', how="left")
            .drop(['From_Currency', 'Currency_Normalized'], axis=1)
            .merge(eod_price_merge_df, on="#Symbol", how="left")
            .merge(co_price_merge_df, on="#Symbol", how="left")
            .merge(
                ref_data["sustainalytics"][
                    ["ISIN", "ESG Risk Score", "Overall Global Compact Compliance Status"]
                ].drop_duplicates(subset="ISIN", keep="first"),
                on="ISIN",
                how="left",
            )
            .merge(
                ref_data["ff"][["ISIN Code:", "Free Float Round:"]]
                .drop_duplicates(subset="ISIN Code:", keep="first"),
                left_on="ISIN",
                right_on="ISIN Code:",
                how="left",
            )
            .drop("ISIN Code:", axis=1)
        )

        if base_df is None or len(base_df) == 0:
            raise ValueError("Failed to load one or more required reference data files")

        missing_symbol = int(base_df["#Symbol"].isna().sum()) if "#Symbol" in base_df.columns else -1
        missing_fx = int(base_df["FX/Index Ccy"].isna().sum()) if "FX/Index Ccy" in base_df.columns else -1
        missing_price = int(base_df["Close Prc_EOD"].isna().sum()) if "Close Prc_EOD" in base_df.columns else -1
        logger.info(
            f"Universe rows: {len(base_df)} | Missing #Symbol: {missing_symbol} | Missing FX: {missing_fx} | Missing EOD price: {missing_price}"
        )

        logger.info(f"Starting universe size: {len(base_df)} stocks")

        # STEP 1: Universe already is EDWPT
        logger.info(f"Step 1: Using universe '{universe.upper()}' as starting set...")
        universe_df = base_df.copy()

        # STEP 2: ESG compliance screening
        logger.info("Step 2: Applying ESG compliance screening...")
        universe_df = universe_df[
            (universe_df["Overall Global Compact Compliance Status"] != "Non-Compliant")
            | (universe_df["Overall Global Compact Compliance Status"].isna())
        ].copy()
        logger.info(f"Universe size after ESG screening: {len(universe_df)} stocks")

        # Tie-breaker FFMC in index currency using FX
        universe_df["FF_Market_Cap"] = (
            pd.to_numeric(universe_df.get("NOSH"), errors="coerce")
            * pd.to_numeric(universe_df.get("Close Prc_EOD"), errors="coerce")
            * pd.to_numeric(universe_df.get("Free Float Round:"), errors="coerce")
            / 100
            * pd.to_numeric(universe_df.get("FX/Index Ccy"), errors="coerce")
        )

        # STEP 3: Rank
        logger.info("Step 3: Ranking by ESG Risk Score and Free Float Market Cap...")
        universe_df = universe_df.sort_values(
            by=["ESG Risk Score", "FF_Market_Cap"],
            ascending=[True, False],
        ).reset_index(drop=True)

        # STEP 4: Select top 120
        logger.info("Step 4: Selecting top 120 constituents...")
        selection_df = universe_df.head(120).copy()
        logger.info(f"Selected {len(selection_df)} constituents for {index} index")

        # WEIGHTING
        logger.info("Calculating ESG-based weights...")
        risk = pd.to_numeric(selection_df["ESG Risk Score"], errors="coerce")
        finite_risk = risk[np.isfinite(risk) & (risk > 0)]
        worst_risk = float(finite_risk.max()) if len(finite_risk) else 1000.0
        selection_df["ESG_Risk_Rating"] = risk.where(np.isfinite(risk) & (risk > 0), worst_risk)

        selection_df["ESG_Score"] = 1.0 / selection_df["ESG_Risk_Rating"]

        total_esg_score = float(selection_df["ESG_Score"].sum())
        if total_esg_score == 0 or not np.isfinite(total_esg_score):
            raise ValueError("Total ESG score is invalid (cannot compute weights).")

        selection_df["Weight_Uncapped"] = selection_df["ESG_Score"] / total_esg_score

        # 10% cap (original-style while loop)
        logger.info("Applying 10% weight cap...")
        max_weight = 0.10
        selection_df["Weight_Capped"] = selection_df["Weight_Uncapped"].copy()

        capping_iterations = 0
        max_iterations = 50

        while (selection_df["Weight_Capped"] > max_weight).any() and capping_iterations < max_iterations:
            excess_mask = selection_df["Weight_Capped"] > max_weight
            excess_weight = (
                selection_df.loc[excess_mask, "Weight_Capped"].sum()
                - (excess_mask.sum() * max_weight)
            )

            selection_df.loc[excess_mask, "Weight_Capped"] = max_weight

            uncapped_mask = ~excess_mask
            uncapped_sum = selection_df.loc[uncapped_mask, "Weight_Capped"].sum()

            if uncapped_mask.sum() > 0 and uncapped_sum > 0:
                selection_df.loc[uncapped_mask, "Weight_Capped"] += (
                    excess_weight * selection_df.loc[uncapped_mask, "Weight_Capped"] / uncapped_sum
                )

            capping_iterations += 1

        selection_df["Weight_Final"] = selection_df["Weight_Capped"] / selection_df["Weight_Capped"].sum()

        logger.info(f"Capping completed in {capping_iterations} iterations")
        logger.info(
            f"Weight range: {selection_df['Weight_Final'].min():.2%} to {selection_df['Weight_Final'].max():.2%}"
        )

        # INDEX MARKET CAP ANCHOR (keep ISIN match)
        idx_mask = index_eod_df["#Symbol"].astype(str).str.strip().eq(str(isin).strip())
        if not idx_mask.any():
            raise KeyError(
                f"Index ISIN '{isin}' not found in index_eod_df['#Symbol'] - cannot fetch index market cap."
            )
        index_mcap = float(index_eod_df.loc[idx_mask, "Mkt Cap"].iloc[0])

        # Shares calc: MarketCap / (LocalPrice * FX_to_index_ccy)
        selection_df["Target_Market_Cap"] = selection_df["Weight_Final"] * index_mcap
        denom = (
            pd.to_numeric(selection_df["Close Prc_EOD"], errors="coerce")
            * pd.to_numeric(selection_df["FX/Index Ccy"], errors="coerce")
        )

        selection_df["Number_of_Shares_Calculated"] = np.round(
            pd.to_numeric(selection_df["Target_Market_Cap"], errors="coerce") / denom
        )

        selection_df["Capping_Factor"] = selection_df["Weight_Final"] / selection_df["Weight_Uncapped"]
        selection_df["Effective Date of Review"] = effective_date
        selection_df["Free Float companies"] = 1

        # Output currency column (defensive)
        currency_col = None
        for c in ["Currency (Local)", "Currency", "CCY", "Ccy"]:
            if c in selection_df.columns:
                currency_col = c
                break

        cols = [
            "Name",
            "ISIN",
            "MIC",
            "Number_of_Shares_Calculated",
            "Free Float companies",
            "Capping_Factor",
            "Effective Date of Review",
        ]
        if currency_col:
            cols.append(currency_col)

        ENVW_df = (
            selection_df[cols]
            .rename(
                columns={
                    "Name": "Company",
                    "ISIN": "ISIN Code",
                    "Free Float companies": "Free Float",
                    "Capping_Factor": "Capping Factor",
                    "Number_of_Shares_Calculated": "Number of Shares",
                    (currency_col if currency_col else "Currency"): "Currency",
                }
            )
            .sort_values("Company")
        )

        # Inclusion/Exclusion analysis
        logger.info("Performing inclusion/exclusion analysis...")
        analysis_results = inclusion_exclusion_analysis(
            ENVW_df,
            stock_eod_df,
            index,
            isin_column="ISIN Code",
        )
        inclusion_df = analysis_results["inclusion_df"]
        exclusion_df = analysis_results["exclusion_df"]

        logger.info(f"Inclusions: {len(inclusion_df)}, Exclusions: {len(exclusion_df)}")

        # Save output
        try:
            output_dir = os.path.join(os.getcwd(), "output")
            os.makedirs(output_dir, exist_ok=True)

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            envw_path = os.path.join(output_dir, f"ENVW_df_{timestamp}.xlsx")

            logger.info(f"Saving ENVW output to: {envw_path}")
            with pd.ExcelWriter(envw_path) as writer:
                ENVW_df.to_excel(writer, sheet_name=index + " Composition", index=False)
                inclusion_df.to_excel(writer, sheet_name="Inclusion", index=False)
                exclusion_df.to_excel(writer, sheet_name="Exclusion", index=False)
                pd.DataFrame({"Index Market Cap": [index_mcap]}).to_excel(
                    writer, sheet_name="Index Market Cap", index=False
                )
                selection_df.to_excel(writer, sheet_name="Selection", index=False)
                universe_df.to_excel(writer, sheet_name="Full Universe", index=False)

            return {
                "status": "success",
                "message": "Review completed successfully",
                "data": {"envw_path": envw_path},
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

 
def index_universe(base_df):
    logger.info("Step 1: Filtering Index Universe to Euronext Developed World Total Market...")
    universe_df = base_df.copy()
    logger.info(f"Universe size after exchange filtering: {len(universe_df)} stocks")
    return universe_df

