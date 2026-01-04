# envf_review.py
#
# Euronext Sustainable France 20 (ENVF) review script
# Rulebook (v25-02 effective 11 July 2025):
# - Universe: CAC 40 constituents from CAC Family.xlsx, sheet PX1
# - Exclude Sustainalytics NON-COMPLIANT (Global Compact compliance status)
# - Rank by ESG Risk Score asc, tie-break by Free Float Market Cap desc
# - Select top 20
# - Weighting: non-mcap, based on 1 / ESG risk score
# - 10% cap (iterative redistribution)
# - Base currency: EUR (for FX/Index Ccy usage only)
# - Free Float factor not applicable for weighting -> output Free Float = 1

import os
import traceback
from datetime import datetime

import numpy as np
import pandas as pd

from Review.functions import read_semicolon_csv
from config import DLF_FOLDER, DATA_FOLDER, DATA_FOLDER2
from utils.logging_utils import setup_logging
from utils.data_loader import load_eod_data, load_reference_data
from utils.inclusion_exclusion import inclusion_exclusion_analysis

logger = setup_logging(__name__)


def _company_sort_key(name: str) -> str:
    if name is None:
        return ""
    return (
        str(name)
        .upper()
        .strip()
        .replace("’", "'")
        .replace("`", "'")
    )


def run_envf_review(
    date,
    co_date,
    effective_date,
    index="ENVF",
    isin="QS0011250907",
    area="US",
    area2="EU",
    type="STOCK",
    universe="cac_family",
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

        logger.info("Loading reference data (CAC Family PX1 + Sustainalytics)...")
        ref_data = load_reference_data(
            current_data_folder,
            required_files=["cac_family", "sustainalytics"],
            sheet_names={"cac_family": "PX1"},
        )

        cac_df = ref_data.get("cac_family")
        sust_df = ref_data.get("sustainalytics")

        missing = []
        if cac_df is None or cac_df.empty:
            missing.append("CAC Family.xlsx (sheet PX1)")
        if sust_df is None or sust_df.empty:
            missing.append("Sustainalytics.xlsx")
        if missing:
            raise ValueError(f"Missing reference data: {', '.join(missing)}")

        # CAC Family columns are on row 2 in Excel -> loader uses header=1 already
        # Expected: Company, ISIN code, MIC, Number of shares, Free Float, Capping
        required_cac_cols = ["Company", "ISIN code", "MIC", "Number of shares", "Free Float", "Capping"]
        miss_cac = [c for c in required_cac_cols if c not in cac_df.columns]
        if miss_cac:
            raise KeyError(f"CAC Family PX1 missing columns: {miss_cac}. Columns: {list(cac_df.columns)}")

        selection_base = cac_df[required_cac_cols].copy()
        selection_base = selection_base.rename(columns={"ISIN code": "ISIN"})

        # Add Currency from stock_eod_df (like F4RIP), fallback to EUR
        if "Currency" in stock_eod_df.columns:
            selection_base = selection_base.merge(
                stock_eod_df[["Isin Code", "MIC", "Currency"]]
                .drop_duplicates(subset=["Isin Code", "MIC"], keep="first"),
                left_on=["ISIN", "MIC"],
                right_on=["Isin Code", "MIC"],
                how="left",
            ).drop(columns=["Isin Code"])
            selection_base["Currency"] = selection_base["Currency"].fillna("EUR")
        else:
            selection_base["Currency"] = "EUR"

        # Sustainalytics required columns
        sust_cols = ["ISIN", "ESG Risk Score", "Overall Global Compact Compliance Status"]
        miss_sust = [c for c in sust_cols if c not in sust_df.columns]
        if miss_sust:
            raise KeyError(f"Sustainalytics missing columns: {miss_sust}. Columns: {list(sust_df.columns)}")

        selection_base = selection_base.merge(
            sust_df[sust_cols].drop_duplicates(subset="ISIN", keep="first"),
            on="ISIN",
            how="left",
        )

        # Build ISIN+MIC -> #Symbol mapping (RIC mode, len(#Symbol) < 12) for prices/FX
        required_eod_cols = ["#Symbol", "Isin Code", "MIC", "Close Prc", "FX/Index Ccy", "Index Curr"]
        miss_eod = [c for c in required_eod_cols if c not in stock_eod_df.columns]
        if miss_eod:
            raise KeyError(f"stock_eod_df missing columns: {miss_eod}. Columns: {list(stock_eod_df.columns)}")

        ric_rows = stock_eod_df[stock_eod_df["#Symbol"].astype(str).str.len() < 12].copy()
        symbols_filtered = (
            ric_rows[["Isin Code", "MIC", "#Symbol"]]
            .drop_duplicates(subset=["Isin Code", "MIC"], keep="first")
            .rename(columns={"Isin Code": "ISIN"})
        )

        # FX: filter Index Curr == EUR, dedupe by #Symbol
        stock_eod_fx_df = stock_eod_df[stock_eod_df["Index Curr"] == currency].copy()
        fx_merge_df = (
            stock_eod_fx_df[["#Symbol", "FX/Index Ccy"]]
            .drop_duplicates(subset="#Symbol", keep="first")
            .copy()
        )

        # EOD price (local)
        eod_price_merge_df = (
            stock_eod_df[["#Symbol", "Close Prc"]]
            .drop_duplicates(subset="#Symbol", keep="first")
            .rename(columns={"Close Prc": "Close Prc_EOD"})
        )

        selection_df = (
            selection_base
            .merge(symbols_filtered, on=["ISIN", "MIC"], how="left")
            .merge(fx_merge_df, on="#Symbol", how="left")
            .merge(eod_price_merge_df, on="#Symbol", how="left")
        )

        logger.info(
            f"Universe rows: {len(selection_df)} | Missing #Symbol: {int(selection_df['#Symbol'].isna().sum())} "
            f"| Missing FX: {int(selection_df['FX/Index Ccy'].isna().sum())} "
            f"| Missing EOD price: {int(selection_df['Close Prc_EOD'].isna().sum())}"
        )

        # STEP 2: Exclude Sustainalytics NON-COMPLIANT
        logger.info("Step 2: Applying Sustainalytics NON-COMPLIANT exclusion...")
        universe_df = selection_df[
            (selection_df["Overall Global Compact Compliance Status"] != "Non-Compliant")
            | (selection_df["Overall Global Compact Compliance Status"].isna())
        ].copy()
        logger.info(f"Universe size after ESG screening: {len(universe_df)}")

        # Tie-breaker FF Market Cap (EUR)
        # Use CAC file: shares * free float * capping * close * FX
        # (FX is basically 1 for CAC, but we keep it consistent)
        universe_df["FF"] = pd.to_numeric(universe_df["Free Float"], errors="coerce")
        universe_df["CAP"] = pd.to_numeric(universe_df["Capping"], errors="coerce")
        universe_df["NOSH_CAC"] = pd.to_numeric(universe_df["Number of shares"], errors="coerce")
        universe_df["PX_EOD"] = pd.to_numeric(universe_df["Close Prc_EOD"], errors="coerce")
        universe_df["FX"] = pd.to_numeric(universe_df["FX/Index Ccy"], errors="coerce").fillna(1.0)

        universe_df["FF_Market_Cap"] = universe_df["NOSH_CAC"] * universe_df["FF"] * universe_df["CAP"] * universe_df["PX_EOD"] * universe_df["FX"]

        # STEP 3: Rank by ESG Risk Score asc; tie-break by FFMC desc
        logger.info("Step 3: Ranking by ESG Risk Score and FF Market Cap...")
        universe_df["ESG_Risk_Rating"] = pd.to_numeric(universe_df["ESG Risk Score"], errors="coerce").fillna(999.0)

        universe_df = universe_df.sort_values(
            by=["ESG_Risk_Rating", "FF_Market_Cap"],
            ascending=[True, False],
        ).reset_index(drop=True)

        # STEP 4: Select top 20
        logger.info("Step 4: Selecting top 20 constituents...")
        top_n = 20
        selection_df = universe_df.head(top_n).copy()

        # WEIGHTING: 1 / ESG risk score, cap 10%
        logger.info("Calculating ESG-based weights and applying 10% cap...")
        risk = pd.to_numeric(selection_df["ESG Risk Score"], errors="coerce")
        finite_risk = risk[np.isfinite(risk) & (risk > 0)]
        worst_risk = float(finite_risk.max()) if len(finite_risk) else 1000.0
        selection_df["ESG_Risk_For_Weight"] = risk.where((risk > 0) & np.isfinite(risk), worst_risk)

        selection_df["ESG_Score"] = 1.0 / selection_df["ESG_Risk_For_Weight"]
        total_esg = float(selection_df["ESG_Score"].sum())
        if total_esg <= 0 or not np.isfinite(total_esg):
            raise ValueError("Total ESG score invalid, cannot compute weights.")

        selection_df["Weight_Uncapped"] = selection_df["ESG_Score"] / total_esg

        max_weight = 0.10
        selection_df["Weight_Capped"] = selection_df["Weight_Uncapped"].copy()

        it = 0
        max_it = 50
        while (selection_df["Weight_Capped"] > max_weight).any() and it < max_it:
            over = selection_df["Weight_Capped"] > max_weight
            excess = selection_df.loc[over, "Weight_Capped"].sum() - (int(over.sum()) * max_weight)

            selection_df.loc[over, "Weight_Capped"] = max_weight

            under = ~over
            under_sum = selection_df.loc[under, "Weight_Capped"].sum()
            if under_sum > 0 and excess > 0:
                selection_df.loc[under, "Weight_Capped"] += excess * (selection_df.loc[under, "Weight_Capped"] / under_sum)

            it += 1

        selection_df["Weight_Final"] = selection_df["Weight_Capped"] / selection_df["Weight_Capped"].sum()
        selection_df["Capping_Factor"] = selection_df["Weight_Final"] / selection_df["Weight_Uncapped"]

        # INDEX MARKET CAP ANCHOR
        if "#Symbol" not in index_eod_df.columns or "Mkt Cap" not in index_eod_df.columns:
            raise KeyError(f"index_eod_df missing '#Symbol' and/or 'Mkt Cap'. Columns: {list(index_eod_df.columns)}")

        idx_mask = index_eod_df["#Symbol"].astype(str).str.strip().eq(str(isin).strip())
        if not idx_mask.any():
            raise KeyError(f"Index ISIN '{isin}' not found in index_eod_df['#Symbol'] (needed for index mcap).")
        index_mcap = float(index_eod_df.loc[idx_mask, "Mkt Cap"].iloc[0])

        # Shares: target mcap / price in index currency (EUR) = close * FX
        selection_df["Price_Index_Ccy"] = selection_df["PX_EOD"] * selection_df["FX"]
        selection_df["Target_Market_Cap"] = selection_df["Weight_Final"] * index_mcap
        selection_df["Number_of_Shares_Calculated"] = (selection_df["Target_Market_Cap"] / selection_df["Price_Index_Ccy"]).round()

        selection_df["Effective Date of Review"] = effective_date
        selection_df["Free Float companies"] = 1  # output requirement

        # Output dataframe
        ENVF_df = selection_df[
            [
                "Company",
                "ISIN",
                "MIC",
                "Number_of_Shares_Calculated",
                "Free Float companies",
                "Capping_Factor",
                "Effective Date of Review",
                "Currency",
            ]
        ].rename(
            columns={
                "ISIN": "ISIN Code",
                "Number_of_Shares_Calculated": "Number of Shares",
                "Free Float companies": "Free Float",
                "Capping_Factor": "Capping Factor",
            }
        )

        # Sorting: ignore punctuation/apostrophes
        ENVF_df["_Company_Sort_Key"] = (
            ENVF_df["Company"]
            .astype(str)
            .str.upper()
            .str.replace(r"[^A-Z0-9]+", "", regex=True)
        )
        ENVF_df = ENVF_df.sort_values("_Company_Sort_Key").drop(columns=["_Company_Sort_Key"]).reset_index(drop=True)

        # Inclusion/Exclusion analysis
        logger.info("Performing inclusion/exclusion analysis...")
        analysis_results = inclusion_exclusion_analysis(
            ENVF_df,
            stock_eod_df,
            index,
            isin_column="ISIN Code",
        )
        inclusion_df = analysis_results.get("inclusion_df", pd.DataFrame())
        exclusion_df = analysis_results.get("exclusion_df", pd.DataFrame())

        logger.info(f"Inclusions: {len(inclusion_df)}, Exclusions: {len(exclusion_df)}")

        # Save output
        output_dir = os.path.join(os.getcwd(), "output")
        os.makedirs(output_dir, exist_ok=True)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        envf_path = os.path.join(output_dir, f"ENVF_df_{timestamp}.xlsx")

        logger.info(f"Saving ENVF output to: {envf_path}")
        with pd.ExcelWriter(envf_path) as writer:
            ENVF_df.to_excel(writer, sheet_name=index + " Composition", index=False)
            inclusion_df.to_excel(writer, sheet_name="Inclusion", index=False)
            exclusion_df.to_excel(writer, sheet_name="Exclusion", index=False)
            pd.DataFrame({"Index Market Cap": [index_mcap], "Index Currency": [currency]}).to_excel(
                writer, sheet_name="Index Market Cap", index=False
            )
            selection_df.to_excel(writer, sheet_name="Selection", index=False)
            universe_df.to_excel(writer, sheet_name="Full Universe", index=False)

        return {"status": "success", "message": "Review completed successfully", "data": {"envf_path": envf_path}}

    except Exception as e:
        logger.error(f"Error during review calculation: {str(e)}")
        logger.error(traceback.format_exc())
        return {
            "status": "error",
            "message": f"Error during review calculation: {str(e)}",
            "traceback": traceback.format_exc(),
            "data": None,
        }
