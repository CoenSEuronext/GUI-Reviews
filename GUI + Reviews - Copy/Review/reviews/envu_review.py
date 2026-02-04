# envu_review.py

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


def run_envu_review(
    date,
    co_date,
    effective_date,
    index="ENVU",
    isin="QS0011256169",
    area="US",
    area2="EU",
    type="STOCK",
    universe="north_america_500",
    feed="Reuters",
    currency="USD",
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
        ref_data = load_reference_data(current_data_folder, ["ff", universe, "icb", "sustainalytics"])

        if universe not in ref_data or ref_data[universe] is None or ref_data[universe].empty:
            raise KeyError(
                f"Universe '{universe}' missing/empty in reference data. Keys: {list(ref_data.keys())}"
            )

        # Filter symbols once (RIC-like)
        symbols_filtered = (
            stock_eod_df[stock_eod_df["#Symbol"].astype(str).str.len() < 12][["Isin Code", "#Symbol"]]
            .drop_duplicates(subset=["Isin Code"], keep="first")
        )

        # FX slice for rows where Index Curr equals the target index currency (e.g. USD)
        # Keep Currency from EOD as the authoritative local currency label.
        fx_data_filtered = (
            stock_eod_df[stock_eod_df["Index Curr"] == currency][["Currency", "#Symbol", "FX/Index Ccy"]]
            .drop_duplicates(subset=["#Symbol"], keep="first")
        )

        # Chain all data preparation operations
        base_df = (
            ref_data[universe]
            # Merge symbols (adds #Symbol)
            .merge(
                symbols_filtered,
                left_on="ISIN",
                right_on="Isin Code",
                how="left",
            )
            .drop("Isin Code", axis=1)
            # Merge FX data (do NOT merge on 'Currency' because the universe often doesn't have a 'Currency' column)
            .merge(
                fx_data_filtered,
                on="#Symbol",
                how="left",
            )
            # Merge EOD prices
            .merge(
                stock_eod_df[["#Symbol", "Close Prc"]].drop_duplicates(subset="#Symbol", keep="first"),
                on="#Symbol",
                how="left",
                suffixes=("", "_EOD"),
            )
            # Merge CO prices
            .merge(
                stock_co_df[["#Symbol", "Close Prc"]].drop_duplicates(subset="#Symbol", keep="first"),
                on="#Symbol",
                how="left",
                suffixes=("_EOD", "_CO"),
            )
            # Merge Sustainalytics ESG data
            .merge(
                ref_data["sustainalytics"][
                    ["ISIN", "ESG Risk Score", "Overall Global Compact Compliance Status"]
                ].drop_duplicates(subset="ISIN", keep="first"),
                on="ISIN",
                how="left",
            )
            # Merge Free Float %
            .merge(
                ref_data["ff"][["ISIN Code:", "Free Float Round:"]].drop_duplicates(
                    subset="ISIN Code:", keep="first"
                ),
                left_on="ISIN",
                right_on="ISIN Code:",
                how="left",
            )
            .drop("ISIN Code:", axis=1)
        )

        if base_df is None or len(base_df) == 0:
            raise ValueError("Failed to load one or more required reference data files")

        # Ensure Currency exists for output (fallback to index currency if missing)
        if "Currency" not in base_df.columns:
            base_df["Currency"] = currency
        base_df["Currency"] = base_df["Currency"].fillna(currency)

        logger.info(f"Starting universe size: {len(base_df)} stocks")

        # STEP 1: Universe (North America 500 excluding TSX)
        logger.info("Step 1: Using Euronext North America 500 universe...")
        logger.info("Excluding companies with Main Listing on Toronto Stock Exchange (MIC: XTSE)...")
        universe_df = base_df[base_df["MIC"] != "XTSE"].copy()
        logger.info(f"Universe size after TSX exclusion: {len(universe_df)} stocks")

        # STEP 2: ESG Eligibility Screening
        logger.info("Step 2: Applying ESG compliance screening...")
        universe_df = universe_df[
            (universe_df["Overall Global Compact Compliance Status"] != "Non-Compliant")
            | (universe_df["Overall Global Compact Compliance Status"].isna())
        ].copy()
        logger.info(f"Universe size after ESG screening: {len(universe_df)} stocks")

        # FFMC tie-breaker (defensive: if 'Price' missing, use Close Prc_EOD)
        price_for_ffmc = "Price" if "Price" in universe_df.columns else "Close Prc_EOD"

        universe_df["FF_Market_Cap"] = (
            pd.to_numeric(universe_df["NOSH"], errors="coerce")
            * pd.to_numeric(universe_df[price_for_ffmc], errors="coerce")
            * pd.to_numeric(universe_df["Free Float Round:"], errors="coerce")
            / 100.0
            * pd.to_numeric(universe_df["FX/Index Ccy"], errors="coerce")
        )

        # STEP 3: Rank
        logger.info("Step 3: Ranking by ESG Risk Rating and Free Float Market Cap...")
        universe_df["ESG_Risk_Rating"] = pd.to_numeric(universe_df["ESG Risk Score"], errors="coerce").fillna(999)

        universe_df = universe_df.sort_values(
            by=["ESG_Risk_Rating", "FF_Market_Cap"],
            ascending=[True, False],
        ).reset_index(drop=True)

        # STEP 4: Select top 50
        logger.info("Step 4: Selecting top 50 constituents...")
        selection_df = universe_df.head(50).copy()
        logger.info(f"Selected {len(selection_df)} constituents for ENVU index")

        # WEIGHTING
        logger.info("Calculating ESG-based weights...")
        selection_df["ESG_Score"] = 1 / selection_df["ESG_Risk_Rating"]
        total_esg_score = selection_df["ESG_Score"].sum()
        selection_df["Weight_Uncapped"] = selection_df["ESG_Score"] / total_esg_score

        # 10% cap
        logger.info("Applying 10% weight cap...")
        max_weight = 0.10
        selection_df["Weight_Capped"] = selection_df["Weight_Uncapped"].copy()

        capping_iterations = 0
        max_iterations = 20

        while (selection_df["Weight_Capped"] > max_weight).any() and capping_iterations < max_iterations:
            excess_mask = selection_df["Weight_Capped"] > max_weight
            excess_weight = selection_df.loc[excess_mask, "Weight_Capped"].sum() - (
                excess_mask.sum() * max_weight
            )

            selection_df.loc[excess_mask, "Weight_Capped"] = max_weight

            uncapped_mask = ~excess_mask
            if uncapped_mask.sum() > 0:
                uncapped_sum = selection_df.loc[uncapped_mask, "Weight_Capped"].sum()
                if uncapped_sum > 0:
                    selection_df.loc[uncapped_mask, "Weight_Capped"] += (
                        excess_weight * selection_df.loc[uncapped_mask, "Weight_Capped"] / uncapped_sum
                    )

            capping_iterations += 1

        selection_df["Weight_Final"] = selection_df["Weight_Capped"] / selection_df["Weight_Capped"].sum()

        logger.info(f"Capping completed in {capping_iterations} iterations")
        logger.info(
            f"Weight range: {selection_df['Weight_Final'].min():.2%} to {selection_df['Weight_Final'].max():.2%}"
        )

        # Index market cap anchor
        index_mcap = index_eod_df.loc[
            index_eod_df["#Symbol"] == str(isin).strip(), "Mkt Cap"
        ].iloc[0]  # keep your override

        selection_df["Target_Market_Cap"] = selection_df["Weight_Final"] * index_mcap
        selection_df["Number_of_Shares_Calculated"] = np.round(
            selection_df["Target_Market_Cap"]
            / (
                pd.to_numeric(selection_df["Close Prc_EOD"], errors="coerce")
                * pd.to_numeric(selection_df["FX/Index Ccy"], errors="coerce")
            )
        )

        selection_df["Capping_Factor"] = selection_df["Weight_Final"] / selection_df["Weight_Uncapped"]
        selection_df["Effective Date of Review"] = effective_date
        selection_df["Free Float companies"] = 1

        # Final composition
        ENVU_df = (
            selection_df[
                [
                    "Name",
                    "ISIN",
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
                    "Name": "Company",
                    "ISIN": "ISIN Code",
                    "Free Float companies": "Free Float",
                    "Capping_Factor": "Capping Factor",
                    "Number_of_Shares_Calculated": "Number of Shares",
                }
            )
            .sort_values("Company")
        )

        # Inclusion/Exclusion analysis
        logger.info("Performing inclusion/exclusion analysis...")
        analysis_results = inclusion_exclusion_analysis(
            ENVU_df, stock_eod_df, index, isin_column="ISIN Code"
        )
        inclusion_df = analysis_results["inclusion_df"]
        exclusion_df = analysis_results["exclusion_df"]

        logger.info(f"Inclusions: {len(inclusion_df)}, Exclusions: {len(exclusion_df)}")

        # Save output
        try:
            output_dir = os.path.join(os.getcwd(), "output")
            os.makedirs(output_dir, exist_ok=True)

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            envu_path = os.path.join(output_dir, f"ENVU_df_{timestamp}.xlsx")

            logger.info(f"Saving ENVU output to: {envu_path}")
            with pd.ExcelWriter(envu_path) as writer:
                ENVU_df.to_excel(writer, sheet_name=index + " Composition", index=False)
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
                "data": {"envu_path": envu_path},
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
