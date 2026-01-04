# enveo_review.py
#
# Euronext Sustainable Euro 120 (ENVEO) review script
# Rulebook (v25-02 effective 11 July 2025):
# - Universe: Eurozone 300
# - Exclude Sustainalytics NON-COMPLIANT (UN Global Compact compliance status)
# - Rank by ESG Risk Score asc, tie-break by Free Float Market Cap desc (Cut-Off / CO)
# - Select top 120
# - Weighting: non-mcap, based on 1 / ESG risk score
# - Base currency: EUR
# - Cap: 10% max weight at reviews
# - Number of Shares: derived from target market cap using CO close in index currency (Close * FX/Index Ccy)
# - Output Free Float = 1 (family convention)

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

def run_enveo_review(
    date: str,
    co_date: str,
    effective_date: str,
    index: str = "ENVEO",
    isin: str = "QS0011256201",
    area: str = "EU",
    area2: str = "US",
    type: str = "STOCK",
    universe: str = "Eurozone 300",
    feed: str = "Reuters",
    currency: str = "EUR",
    year: str = None,
):
    try:
        year = year or str(datetime.strptime(date, "%Y%m%d").year)
        current_data_folder = os.path.join(DATA_FOLDER2, date[:6])

        logger.info("Loading EOD data...")
        index_eod_df, stock_eod_df, stock_co_df, fx_lookup_df = load_eod_data(date, co_date, area, area2, DLF_FOLDER)

        logger.info("Loading reference data...")
        ref_keys = ["ff", "sustainalytics", "icb", "eurozone_300"]
        ref_data = load_reference_data(current_data_folder, ref_keys)

        # -----------------------------
        # Validate reference inputs
        # -----------------------------
        if "eurozone_300" not in ref_data:
            raise KeyError(f"Missing reference key 'eurozone_300'. Keys: {list(ref_data.keys())}")

        universe_ref_df = ref_data["eurozone_300"]
        if universe_ref_df is None or universe_ref_df.empty:
            raise ValueError("Universe reference dataframe 'eurozone_300' is empty")

        if "sustainalytics" not in ref_data or ref_data["sustainalytics"] is None or ref_data["sustainalytics"].empty:
            raise KeyError(f"Missing/empty reference key 'sustainalytics'. Keys: {list(ref_data.keys())}")

        if "ff" not in ref_data or ref_data["ff"] is None or ref_data["ff"].empty:
            raise KeyError(f"Missing/empty reference key 'ff'. Keys: {list(ref_data.keys())}")

        # Universe required columns (no Currency(Local) nonsense)
        required_universe_cols = ["ISIN", "MIC", "Name", "NOSH"]
        missing_universe_cols = [c for c in required_universe_cols if c not in universe_ref_df.columns]
        if missing_universe_cols:
            raise KeyError(
                f"Universe 'eurozone_300' missing required columns: {missing_universe_cols}. "
                f"Columns: {list(universe_ref_df.columns)}"
            )

        # Sustainalytics required columns
        sust_cols = ["ISIN", "ESG Risk Score", "Overall Global Compact Compliance Status"]
        missing_sust_cols = [c for c in sust_cols if c not in ref_data["sustainalytics"].columns]
        if missing_sust_cols:
            raise KeyError(
                f"sustainalytics missing columns: {missing_sust_cols}. "
                f"Columns: {list(ref_data['sustainalytics'].columns)}"
            )

        # Free float required columns (tie-break FFMC)
        ff_cols = ["ISIN Code:", "Free Float Round:"]
        missing_ff_cols = [c for c in ff_cols if c not in ref_data["ff"].columns]
        if missing_ff_cols:
            raise KeyError(
                f"ff missing columns: {missing_ff_cols}. "
                f"Columns: {list(ref_data['ff'].columns)}"
            )

        # CO dataset required columns (we use CO close + FX to EUR via Index Curr)
        required_co_cols = ["#Symbol", "Isin Code", "MIC", "Close Prc", "Index Curr", "FX/Index Ccy"]
        missing_co_cols = [c for c in required_co_cols if c not in stock_co_df.columns]
        if missing_co_cols:
            raise KeyError(
                f"stock_co_df missing required columns: {missing_co_cols}. "
                f"Columns: {list(stock_co_df.columns)}"
            )

        # -----------------------------
        # Build base universe (Eurozone 300)
        # -----------------------------
        base_df = universe_ref_df.rename(
            columns={
                "Name": "Company",
                "ISIN": "ISIN Code",
                "NOSH": "NOSH_Ref",
            }
        ).copy()

        # Deduplicate universe by ISIN Code
        before = len(base_df)
        base_df = base_df.drop_duplicates(subset=["ISIN Code"], keep="first").copy()
        after = len(base_df)
        if after != before:
            logger.warning(f"Removed {before - after} duplicate rows from universe on ISIN Code")

        # -----------------------------
        # Map ISIN+MIC -> #Symbol (RIC mode)
        # -----------------------------
        ric_rows = stock_co_df[stock_co_df["#Symbol"].astype(str).str.len() < 12].copy()
        symbols_filtered = (
            ric_rows[["Isin Code", "MIC", "#Symbol"]]
            .drop_duplicates(subset=["Isin Code", "MIC"], keep="first")
            .rename(columns={"Isin Code": "ISIN Code"})
        )

        # CO close price per #Symbol
        co_price_df = (
            stock_co_df[["#Symbol", "Close Prc"]]
            .drop_duplicates(subset=["#Symbol"], keep="first")
            .rename(columns={"Close Prc": "Close_Prc_CO"})
        )
        eod_price_df = (
            stock_eod_df[["#Symbol", "Close Prc"]]
            .drop_duplicates(subset=["#Symbol"], keep="first")
            .rename(columns={"Close Prc": "Close_Prc_EOD"})
        ) 

        # FX to index currency (EUR) per #Symbol, filtered on Index Curr == EUR
        co_fx_df = stock_co_df[stock_co_df["Index Curr"] == currency].copy()
        co_fx_df = (
            co_fx_df[["#Symbol", "FX/Index Ccy"]]
            .drop_duplicates(subset=["#Symbol"], keep="first")
            .rename(columns={"FX/Index Ccy": "FX_CO"})
        )
        # FX to index currency (EUR) per #Symbol, filtered on Index Curr == EUR
        eod_fx_df = stock_eod_df[stock_eod_df["Index Curr"] == currency].copy()
        eod_fx_df = (
            eod_fx_df[["#Symbol", "FX/Index Ccy"]]
            .drop_duplicates(subset=["#Symbol"], keep="first")
            .rename(columns={"FX/Index Ccy": "FX_EOD"})
        )
        # Merge symbols, CO close, FX
        base_df = (
            base_df
            .merge(symbols_filtered, on=["ISIN Code", "MIC"], how="left")
            .merge(co_price_df, on="#Symbol", how="left")
            .merge(eod_price_df, on="#Symbol", how="left")
            .merge(co_fx_df, on="#Symbol", how="left")
            .merge(eod_fx_df, on="#Symbol", how="left")
        )

        # Merge Sustainalytics (by ISIN)
        sust_df = ref_data["sustainalytics"][sust_cols].drop_duplicates(subset=["ISIN"], keep="first").copy()
        sust_df = sust_df.rename(columns={"ISIN": "ISIN Code"})
        base_df = base_df.merge(sust_df, on="ISIN Code", how="left")

        # Merge Free Float (percent)
        ff_df = ref_data["ff"][ff_cols].drop_duplicates(subset=["ISIN Code:"], keep="first").copy()
        ff_df = ff_df.rename(columns={"ISIN Code:": "ISIN Code", "Free Float Round:": "Free_Float_Pct"})
        base_df = base_df.merge(ff_df, on="ISIN Code", how="left")

        logger.info(
            f"Universe rows: {len(base_df)} | "
            f"Missing #Symbol: {int(base_df['#Symbol'].isna().sum())} | "
            f"Missing CO price: {int(base_df['Close_Prc_CO'].isna().sum())} | "
            f"Missing CO FX({currency}): {int(base_df['FX_CO'].isna().sum())} | "
            f"Missing ESG score: {int(base_df['ESG Risk Score'].isna().sum())} | "
            f"Missing Free Float %: {int(base_df['Free_Float_Pct'].isna().sum())}"
        )

        # -----------------------------
        # STEP 1: Universe = Eurozone 300 (already)
        # -----------------------------
        universe_df = base_df.copy()
        logger.info(f"Starting universe size: {len(universe_df)}")

        # -----------------------------
        # STEP 2: Exclude Sustainalytics NON-COMPLIANT
        # -----------------------------
        logger.info("Step 2: Excluding Sustainalytics Non-Compliant companies...")
        eligible_df = universe_df[
            (universe_df["Overall Global Compact Compliance Status"] != "Non-Compliant")
            | (universe_df["Overall Global Compact Compliance Status"].isna())
        ].copy()
        logger.info(f"Universe size after Non-Compliant screening: {len(eligible_df)}")

        # -----------------------------
        # STEP 3: Rank ESG risk asc, tie-break FFMC desc (on CO)
        # -----------------------------
        logger.info("Step 3: Ranking by ESG Risk Score and FF Market Cap (CO)...")

        eligible_df["NOSH_Ref"] = pd.to_numeric(eligible_df["NOSH_Ref"], errors="coerce")
        eligible_df["Price"] = pd.to_numeric(eligible_df["Price"], errors="coerce")
        eligible_df["Free_Float_Pct"] = pd.to_numeric(eligible_df["Free_Float_Pct"], errors="coerce")

        eligible_df["ESG_Risk_Rating"] = pd.to_numeric(eligible_df["ESG Risk Score"], errors="coerce").fillna(999.0)

        eligible_df["FFMC_CO_EUR"] = (
            eligible_df["NOSH_Ref"]
            * eligible_df["Price"]
            * (eligible_df["Free_Float_Pct"] / 100.0)
        )

        eligible_df = eligible_df.sort_values(
            by=["ESG_Risk_Rating", "FFMC_CO_EUR"],
            ascending=[True, False],
        ).reset_index(drop=True)

        # -----------------------------
        # STEP 4: Select top 120
        # -----------------------------
        logger.info("Step 4: Selecting top 120 constituents...")
        selection_df = eligible_df.head(120).copy()
        logger.info(f"Selected {len(selection_df)} constituents for {index}")

        # -----------------------------
        # STEP 5: Weighting = (1 / ESG risk)
        # -----------------------------
        logger.info("Calculating ESG-based weights (1 / ESG risk)...")

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

        # -----------------------------
        # Apply 10% max weight cap (inline, no helper)
        # -----------------------------
        logger.info("Applying 10% max weight cap (iterative)...")
        max_weight = 0.10

        selection_df["Weight_Capped"] = selection_df["Weight_Uncapped"].copy()
        capping_iterations = 0
        max_iterations = 50

        while (selection_df["Weight_Capped"] > max_weight).any() and capping_iterations < max_iterations:
            excess_mask = selection_df["Weight_Capped"] > max_weight
            num_excess = int(excess_mask.sum())

            excess_weight = float(selection_df.loc[excess_mask, "Weight_Capped"].sum() - (num_excess * max_weight))
            selection_df.loc[excess_mask, "Weight_Capped"] = max_weight

            uncapped_mask = ~excess_mask
            if int(uncapped_mask.sum()) > 0:
                uncapped_sum = float(selection_df.loc[uncapped_mask, "Weight_Capped"].sum())
                if uncapped_sum > 0 and np.isfinite(uncapped_sum):
                    selection_df.loc[uncapped_mask, "Weight_Capped"] += (
                        excess_weight * selection_df.loc[uncapped_mask, "Weight_Capped"] / uncapped_sum
                    )

            capping_iterations += 1

        if capping_iterations >= max_iterations and (selection_df["Weight_Capped"] > max_weight).any():
            logger.warning("Capping hit max iterations; potential tiny cap violations. Normalising anyway.")

        capped_sum = float(selection_df["Weight_Capped"].sum())
        if capped_sum <= 0 or not np.isfinite(capped_sum):
            raise ValueError("Capped weights sum invalid.")

        selection_df["Weight_Final"] = selection_df["Weight_Capped"] / capped_sum

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

        logger.info(f"Capping iterations: {capping_iterations}")
        logger.info(
            f"Weight range: {selection_df['Weight_Final'].min():.2%} to {selection_df['Weight_Final'].max():.2%} "
            f"(<= {max_weight:.0%} expected)"
        )

        # -----------------------------
        # Index market cap anchor
        # -----------------------------
        if "#Symbol" not in index_eod_df.columns or "Mkt Cap" not in index_eod_df.columns:
            raise KeyError(
                f"index_eod_df missing required columns '#Symbol' and/or 'Mkt Cap'. "
                f"Columns: {list(index_eod_df.columns)}"
            )

        idx_mask = index_eod_df["#Symbol"].astype(str).str.strip().eq(str(isin).strip())
        if not idx_mask.any():
            raise KeyError(f"Index ISIN '{isin}' not found in index_eod_df['#Symbol'] - cannot fetch index market cap.")

        index_mcap = float(index_eod_df.loc[idx_mask, "Mkt Cap"].iloc[0])

        # Price in index currency (EUR) on CO date: Close * FX
        selection_df["Price_Index_Ccy_EOD"] = (
            pd.to_numeric(selection_df["Close_Prc_EOD"], errors="coerce")
            * pd.to_numeric(selection_df["FX_EOD"], errors="coerce")
        )

        # Target market cap and shares
        selection_df["Target_Market_Cap"] = selection_df["Weight_Final"] * index_mcap
        selection_df["Number_of_Shares_Calculated"] = np.round(
            pd.to_numeric(selection_df["Target_Market_Cap"], errors="coerce")
            / pd.to_numeric(selection_df["Price_Index_Ccy_EOD"], errors="coerce")
        )

        # Final output
        selection_df["Free Float companies"] = 1
        selection_df["Effective Date of Review"] = effective_date
        selection_df["Currency"] = currency


        ENVEO_df = (
            selection_df[
                [
                    "Company",
                    "ISIN Code",
                    "MIC",
                    "Number_of_Shares_Calculated",
                    "Free Float companies",
                    "Capping_Factor",
                    "Effective Date of Review",
                     "Currency",
                ]
            ]
            .rename(
                columns={
                    "Number_of_Shares_Calculated": "Number of Shares",
                    "Free Float companies": "Free Float",
                    "Capping_Factor": "Capping Factor",
                }
            )
            .sort_values("Company", key=lambda x: x.str.replace("'", ""))  # Sort ignoring apostrophes
        )

        # Inclusion/Exclusion analysis
        logger.info("Performing inclusion/exclusion analysis...")
        analysis_results = inclusion_exclusion_analysis(
            ENVEO_df,
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
            enveo_path = os.path.join(output_dir, f"ENVEO_df_{timestamp}.xlsx")

            logger.info(f"Saving ENVEO output to: {enveo_path}")
            with pd.ExcelWriter(enveo_path) as writer:
                ENVEO_df.to_excel(writer, sheet_name=index + " Composition", index=False)
                inclusion_df.to_excel(writer, sheet_name="Inclusion", index=False)
                exclusion_df.to_excel(writer, sheet_name="Exclusion", index=False)
                pd.DataFrame({"Index Market Cap": [index_mcap], "Index Currency": [currency]}).to_excel(
                    writer, sheet_name="Index Market Cap", index=False
                )
                selection_df.to_excel(writer, sheet_name="Selection", index=False)
                eligible_df.to_excel(writer, sheet_name="Eligible Universe", index=False)
                universe_df.to_excel(writer, sheet_name="Full Universe", index=False)


            return {
                "status": "success",
                "message": "Review completed successfully",
                "data": {"enveo_path": enveo_path},
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