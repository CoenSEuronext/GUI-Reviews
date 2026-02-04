# eens_review.py
#
# Euronext European Energy Security (EENS) review script
# Rulebook (v25-01, effective 30 Apr 2025):
# - Universe: Euronext Developed Europe Total Market at review
# - Eligibility: specific ICB subsectors (see 2.2)
# - Selection: all eligible companies
# - Weighting: Free Float Market Cap weighted, max weight 20% (capping factor)
# - Base currency: EUR (for weighting computation), output currency: local currency (universe)
#
# Source: Euronext European Energy Security Index Family Rulebook v25-01

import pandas as pd
import numpy as np
from datetime import datetime
import os
import traceback

from config import DLF_FOLDER, DATA_FOLDER2
from utils.logging_utils import setup_logging
from utils.data_loader import load_eod_data, load_reference_data
from utils.inclusion_exclusion import inclusion_exclusion_analysis
from utils.capping_standard import calculate_capped_weights

logger = setup_logging(__name__)


def run_eens_review(
    date,
    co_date,
    effective_date,
    index="EENS",
    isin="NLIX00006246",
    area="US",
    area2="EU",
    type="STOCK",
    # Euronext Developed Europe Total Market universe key
    universe="deupt",
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
        ref_data = load_reference_data(current_data_folder, [universe, "ff", "icb"])

        # --------------------------
        # Validate reference data
        # --------------------------
        if universe not in ref_data or ref_data[universe] is None or ref_data[universe].empty:
            raise KeyError(
                f"Universe '{universe}' missing/empty in reference data. Keys: {list(ref_data.keys())}"
            )
        if "ff" not in ref_data or ref_data["ff"] is None or ref_data["ff"].empty:
            raise KeyError(f"Missing/empty reference key 'ff'. Keys: {list(ref_data.keys())}")
        if "icb" not in ref_data or ref_data["icb"] is None or ref_data["icb"].empty:
            raise KeyError(f"Missing/empty reference key 'icb'. Keys: {list(ref_data.keys())}")

        universe_df = ref_data[universe].copy()
        ff_df = ref_data["ff"].copy()
        icb_df = ref_data["icb"].copy()

        # --------------------------
        # Universe column normalization
        # --------------------------
        required_universe_cols = ["ISIN", "MIC", "Name", "NOSH"]
        missing_uni = [c for c in required_universe_cols if c not in universe_df.columns]
        if missing_uni:
            raise KeyError(
                f"Universe '{universe}' missing required columns: {missing_uni}. "
                f"Columns: {list(universe_df.columns)}"
            )

        # Currency robustness: accept either "Currency (Local)" or "Currency"
        if "Currency (Local)" not in universe_df.columns:
            if "Currency" in universe_df.columns:
                universe_df["Currency (Local)"] = universe_df["Currency"]
                logger.warning(
                    f"Universe '{universe}': 'Currency (Local)' not found; using 'Currency' as fallback."
                )
            else:
                raise KeyError(
                    f"Universe '{universe}' missing currency column. Expected 'Currency (Local)' or 'Currency'. "
                    f"Columns: {list(universe_df.columns)}"
                )

        # Match currency formatting used in EOD (common case: GBP vs GBX)
        universe_df["Currency (Local)"] = universe_df["Currency (Local)"].replace("GBP", "GBX")

        # --------------------------
        # Validate EOD/CO columns
        # --------------------------
        required_eod_cols = ["#Symbol", "Isin Code", "MIC", "Close Prc", "Index Curr", "FX/Index Ccy"]
        missing_eod = [c for c in required_eod_cols if c not in stock_eod_df.columns]
        if missing_eod:
            raise KeyError(
                f"stock_eod_df missing required columns: {missing_eod}. Columns: {list(stock_eod_df.columns)}"
            )

        required_co_cols = ["#Symbol", "Close Prc"]
        missing_co = [c for c in required_co_cols if c not in stock_co_df.columns]
        if missing_co:
            raise KeyError(
                f"stock_co_df missing required columns: {missing_co}. Columns: {list(stock_co_df.columns)}"
            )

        # --------------------------
        # Build ISIN+MIC -> #Symbol mapping (RIC mode)
        # --------------------------
        logger.info("Building ISIN+MIC -> #Symbol mapping (RIC mode, len(#Symbol) < 12)...")
        ric_rows = stock_eod_df[stock_eod_df["#Symbol"].astype(str).str.len() < 12].copy()
        symbols_filtered = (
            ric_rows[["Isin Code", "MIC", "#Symbol"]]
            .drop_duplicates(subset=["Isin Code", "MIC"], keep="first")
            .rename(columns={"Isin Code": "ISIN"})
        )

        # FX in base currency (EUR) for weight calc
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

        # FF columns
        ff_cols = ["ISIN Code:", "Free Float Round:"]
        missing_ff = [c for c in ff_cols if c not in ff_df.columns]
        if missing_ff:
            raise KeyError(f"ff missing columns: {missing_ff}. Columns: {list(ff_df.columns)}")

        # ICB columns
        icb_cols = ["ISIN Code", "Subsector Code"]
        missing_icb = [c for c in icb_cols if c not in icb_df.columns]
        if missing_icb:
            raise KeyError(f"icb missing columns: {missing_icb}. Columns: {list(icb_df.columns)}")

        # --------------------------
        # Base merge (universe + symbol + FX + prices + FF + ICB)
        # --------------------------
        base_df = (
            universe_df
            .merge(symbols_filtered, on=["ISIN", "MIC"], how="left")
            .merge(fx_merge_df, on="#Symbol", how="left")
            .merge(eod_price_merge_df, on="#Symbol", how="left")
            .merge(co_price_merge_df, on="#Symbol", how="left")
            .merge(
                ff_df[ff_cols].drop_duplicates(subset="ISIN Code:", keep="first"),
                left_on="ISIN",
                right_on="ISIN Code:",
                how="left",
            )
            .drop("ISIN Code:", axis=1)
            .merge(
                icb_df[icb_cols].drop_duplicates(subset="ISIN Code", keep="first"),
                left_on="ISIN",
                right_on="ISIN Code",
                how="left",
            )
            .drop("ISIN Code", axis=1)
        )

        if base_df is None or base_df.empty:
            raise ValueError("Universe dataframe is empty after merges.")

        missing_symbol = int(base_df["#Symbol"].isna().sum())
        missing_fx = int(base_df["FX/Index Ccy"].isna().sum())
        missing_ffv = int(base_df["Free Float Round:"].isna().sum())
        missing_icbv = int(base_df["Subsector Code"].isna().sum())
        logger.info(
            f"Base rows: {len(base_df)} | Missing #Symbol: {missing_symbol} | Missing FX: {missing_fx} | "
            f"Missing Free Float%: {missing_ffv} | Missing ICB: {missing_icbv}"
        )

        # --------------------------
        # Step 2: ICB eligibility filter (rulebook list)
        # --------------------------
        eligible_subsectors = {
            60101000,  # Integrated Oil and Gas
            65101015,  # Conventional Electricity
            65101010,  # Alternative Electricity
            60102020,  # Renewable Energy Equipment
            60101010,  # Oil: Crude Producers
            65102020,  # Gas Distribution
            60101035,  # Pipelines
            60102010,  # Alternative Fuels
            60101030,  # Oil Equipment and Services
            65102000,  # Multi-utilities
        }

        base_df["Subsector Code"] = pd.to_numeric(base_df["Subsector Code"], errors="coerce")
        eligible_df = base_df[base_df["Subsector Code"].isin(list(eligible_subsectors))].copy()

        logger.info(f"Universe size: {len(base_df)} | Eligible (ICB filter) size: {len(eligible_df)}")

        if eligible_df.empty:
            raise ValueError("No eligible companies after ICB subsector screening. Check ICB mapping / universe key.")

        # --------------------------
        # Step 3: Rank (FFMC) (informational)
        # --------------------------
        eligible_df["Free Float Factor"] = pd.to_numeric(eligible_df["Free Float Round:"], errors="coerce") / 100.0
        eligible_df["Number of Shares"] = pd.to_numeric(eligible_df["NOSH"], errors="coerce")
        eligible_df["Close Prc_EOD"] = pd.to_numeric(eligible_df["Close Prc_EOD"], errors="coerce")
        eligible_df["FX/Index Ccy"] = pd.to_numeric(eligible_df["FX/Index Ccy"], errors="coerce")

        eligible_df["FFMC_WD"] = (
            eligible_df["Number of Shares"]
            * eligible_df["Free Float Factor"]
            * eligible_df["Close Prc_EOD"]
            * eligible_df["FX/Index Ccy"]
        )

        eligible_df = eligible_df.sort_values("FFMC_WD", ascending=False).reset_index(drop=True)
        eligible_df["Rank_FFMC"] = np.arange(1, len(eligible_df) + 1)

        # --------------------------
        # Step 4: Selection = all eligible
        # --------------------------
        selection_df = eligible_df.copy()
        logger.info(f"Selected {len(selection_df)} constituents for {index} (all eligible).")

        # --------------------------
        # Weighting: FFMC-weighted with 20% cap -> derive capping factors
        # --------------------------
        logger.info("Calculating FFMC weights and applying 20% cap...")

        valid_weight_df = selection_df.dropna(subset=["FFMC_WD"]).copy()
        if valid_weight_df.empty:
            raise ValueError("All selected rows have missing FFMC_WD; cannot compute weights/capping.")

        total_ffmc = valid_weight_df["FFMC_WD"].sum()
        if total_ffmc <= 0:
            raise ValueError(f"Total FFMC_WD is non-positive ({total_ffmc}); cannot compute weights/capping.")

        initial_weights = valid_weight_df["FFMC_WD"] / total_ffmc
        capped_weights = calculate_capped_weights(initial_weights, cap_limit=0.20)

        # Raw capping factors; then normalize so max becomes 1 (matches existing pattern)
        raw_capping_factors = capped_weights / initial_weights
        max_cf = raw_capping_factors.max()
        if pd.isna(max_cf) or max_cf <= 0:
            raise ValueError("Invalid capping factor normalization (max_cf <= 0).")

        normalized_capping_factors = (raw_capping_factors / max_cf).round(14)

        valid_weight_df["Weight_Initial"] = initial_weights
        valid_weight_df["Weight_Capped"] = capped_weights
        valid_weight_df["Capping Factor"] = normalized_capping_factors

        selection_df = selection_df.merge(
            valid_weight_df[["ISIN", "MIC", "Capping Factor", "Weight_Initial", "Weight_Capped"]],
            on=["ISIN", "MIC"],
            how="left",
        )

        selection_df["Effective Date of Review"] = effective_date

        capped_count = int((selection_df["Capping Factor"] < 1.0).sum())
        logger.info(f"Capping applied (CF < 1): {capped_count} constituents | cap_limit=20%")

        # --------------------------
        # Index market cap (for output sheet, not required for calc)
        # --------------------------
        index_mcap = None
        try:
            index_mcap = index_eod_df.loc[
                index_eod_df["#Symbol"] == str(isin).strip(), "Mkt Cap"
            ].iloc[0]
        except Exception:
            logger.warning("Could not read index market cap from index_eod_df (sheet will show NaN).")

        # --------------------------
        # Output composition
        # - Free Float like ENVB: set to 1
        # - Currency = local currency from universe
        # --------------------------
        selection_df["Free Float_Output"] = 1

        EENS_df = (
            selection_df[
                [
                    "Name",
                    "ISIN",
                    "MIC",
                    "Number of Shares",
                    "Free Float_Output",
                    "Capping Factor",
                    "Effective Date of Review",
                    "Currency (Local)",
                ]
            ]
            .rename(
                columns={
                    "Name": "Company",
                    "ISIN": "ISIN Code",
                    "Free Float_Output": "Free Float",
                    "Currency (Local)": "Currency",
                }
            )
            .sort_values("Company")
        )

        # --------------------------
        # Inclusion/Exclusion analysis
        # --------------------------
        logger.info("Performing inclusion/exclusion analysis...")
        analysis_results = inclusion_exclusion_analysis(
            EENS_df,
            stock_eod_df,
            index,
            isin_column="ISIN Code",
        )
        inclusion_df = analysis_results.get("inclusion_df", pd.DataFrame())
        exclusion_df = analysis_results.get("exclusion_df", pd.DataFrame())
        logger.info(f"Inclusions: {len(inclusion_df)}, Exclusions: {len(exclusion_df)}")

        # --------------------------
        # Save output
        # --------------------------
        try:
            output_dir = os.path.join(os.getcwd(), "output")
            os.makedirs(output_dir, exist_ok=True)

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            eens_path = os.path.join(output_dir, f"{index}_df_{timestamp}.xlsx")

            logger.info(f"Saving {index} output to: {eens_path}")
            with pd.ExcelWriter(eens_path) as writer:
                EENS_df.to_excel(writer, sheet_name=index + " Composition", index=False)
                inclusion_df.to_excel(writer, sheet_name="Inclusion", index=False)
                exclusion_df.to_excel(writer, sheet_name="Exclusion", index=False)
                pd.DataFrame({"Index Market Cap": [index_mcap]}).to_excel(
                    writer, sheet_name="Index Market Cap", index=False
                )
                selection_df.to_excel(writer, sheet_name="Selection", index=False)
                base_df.to_excel(writer, sheet_name="Full Universe", index=False)

            return {
                "status": "success",
                "message": "Review completed successfully",
                "data": {"eens_path": eens_path},
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
