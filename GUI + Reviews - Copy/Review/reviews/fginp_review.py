# fginp_review.py
#
# Euronext Top 30 France Germany Industrials EW (FGINP) review script
# Rulebook (v25-01, effective 22 Sep 2025):
# - Universe: Companies included in EN Developed Eurozone Total Market Index (DEUPT),
#   listed in France (XPAR) or Germany (XETR). If <30 eligible Industrials, rulebook
#   allows selecting outside DEUPT from same countries (NOT implemented here -> hard fail).
# - Eligibility: ICB Industry Code = 50 (Industrials)
# - Ranking: 6 months turnover (EUR)
# - Selection: top 30 by turnover
# - Weighting: Equal weight (non-mcap weighted)
# - NOSH: calculated so each company has equal weight in the index based on close prices
#   on the Review Weighting Date (use EOD close + EUR FX like ENVB).
# - Free Float factor: not applied (output Free Float = 1)
# - Capping Factor: not applied (output Capping Factor = 1)
# - Base currency: EUR; output currency: local currency from universe
#
# Index (Price): ISIN NL0012730634, Mnemo FGINP

import os
import traceback
from datetime import datetime

import numpy as np
import pandas as pd

from config import DLF_FOLDER, DATA_FOLDER2
from utils.capping_standard import calculate_capped_weights  # not used, but kept consistent with other scripts
from utils.data_loader import load_eod_data, load_reference_data
from utils.inclusion_exclusion import inclusion_exclusion_analysis
from utils.logging_utils import setup_logging

logger = setup_logging(__name__)


def run_fginp_review(
    date,
    co_date,
    effective_date,
    index="FGINP",
    isin="NL0012730634",
    area="US",
    area2="EU",
    type="STOCK",
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
        ref_data = load_reference_data(current_data_folder, [universe, "icb"])

        # --------------------------
        # Validate reference data
        # --------------------------
        if universe not in ref_data or ref_data[universe] is None or ref_data[universe].empty:
            raise KeyError(
                f"Universe '{universe}' missing/empty in reference data. Keys: {list(ref_data.keys())}"
            )
        if "icb" not in ref_data or ref_data["icb"] is None or ref_data["icb"].empty:
            raise KeyError(f"Missing/empty reference key 'icb'. Keys: {list(ref_data.keys())}")

        universe_df = ref_data[universe].copy()
        icb_df = ref_data["icb"].copy()

        # --------------------------
        # Universe required columns
        # --------------------------
        # Provided deupt headers (per user):
        # Name, Ticker, ISIN, MIC, Currency (Local), Price (EUR), NOSH, ..., 6M AVG Turnover EUR, ...
        required_universe_cols = ["Name", "ISIN", "MIC", "NOSH", "6M AVG Turnover EUR"]
        missing_uni = [c for c in required_universe_cols if c not in universe_df.columns]
        if missing_uni:
            raise KeyError(
                f"Universe '{universe}' missing required columns: {missing_uni}. "
                f"Columns: {list(universe_df.columns)}"
            )

        # Currency column robustness
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

        # Match currency formatting used elsewhere (GBP vs GBX)
        universe_df["Currency (Local)"] = universe_df["Currency (Local)"].replace("GBP", "GBX")

        # --------------------------
        # Validate EOD columns
        # --------------------------
        required_eod_cols = ["#Symbol", "Isin Code", "MIC", "Close Prc", "Index Curr", "FX/Index Ccy"]
        missing_eod = [c for c in required_eod_cols if c not in stock_eod_df.columns]
        if missing_eod:
            raise KeyError(
                f"stock_eod_df missing required columns: {missing_eod}. Columns: {list(stock_eod_df.columns)}"
            )

        # --------------------------
        # ICB required columns (Industry Code = 50)
        # --------------------------
        icb_cols = ["ISIN Code", "Industry Code"]
        missing_icb = [c for c in icb_cols if c not in icb_df.columns]
        if missing_icb:
            raise KeyError(f"icb missing columns: {missing_icb}. Columns: {list(icb_df.columns)}")

        # --------------------------
        # Step 1: Universe filter to France/Germany listings
        # --------------------------
        allowed_mics = ["XPAR", "XETR"]
        universe_df = universe_df[universe_df["MIC"].isin(allowed_mics)].copy()
        logger.info(f"Universe after MIC filter (XPAR/XETR): {len(universe_df)} rows")

        if universe_df.empty:
            raise ValueError("Universe is empty after MIC filter (XPAR/XETR).")

        # --------------------------
        # Merge ICB Industry Code
        # --------------------------
        base_df = (
            universe_df.merge(
                icb_df[icb_cols].drop_duplicates(subset="ISIN Code", keep="first"),
                left_on="ISIN",
                right_on="ISIN Code",
                how="left",
            )
            .drop("ISIN Code", axis=1)
        )

        missing_industry = int(base_df["Industry Code"].isna().sum())
        logger.info(f"Rows: {len(base_df)} | Missing Industry Code: {missing_industry}")

        # --------------------------
        # Step 2: Eligibility = ICB Industry Code 50 (Industrials)
        # --------------------------
        base_df["Industry Code"] = pd.to_numeric(base_df["Industry Code"], errors="coerce")
        eligible_df = base_df[base_df["Industry Code"] == 50].copy()
        logger.info(f"Eligible (Industry Code 50): {len(eligible_df)} rows")

        if eligible_df.empty:
            raise ValueError("No eligible companies after Industry Code screening (Industry Code == 50).")

        # --------------------------
        # Step 3: Ranking by 6M turnover EUR
        # --------------------------
        eligible_df["6M AVG Turnover EUR"] = pd.to_numeric(
            eligible_df["6M AVG Turnover EUR"], errors="coerce"
        )

        # If dual listing for same ISIN exists, keep listing with highest 6M turnover EUR
        eligible_df = (
            eligible_df.sort_values("6M AVG Turnover EUR", ascending=False)
            .drop_duplicates(subset=["ISIN"], keep="first")
            .copy()
        )

        # Drop rows without turnover (cannot be ranked)
        before_drop = len(eligible_df)
        eligible_df = eligible_df.dropna(subset=["6M AVG Turnover EUR"]).copy()
        dropped = before_drop - len(eligible_df)
        if dropped:
            logger.warning(f"Dropped {dropped} eligible rows due to missing 6M AVG Turnover EUR.")

        eligible_df = eligible_df.sort_values("6M AVG Turnover EUR", ascending=False).reset_index(drop=True)
        eligible_df["Rank_Turnover_6M"] = np.arange(1, len(eligible_df) + 1)

        # --------------------------
        # Step 4: Select top 30
        # --------------------------
        top_n = 30
        if len(eligible_df) < top_n:
            # Rulebook has a fallback outside DEUPT; not implemented here.
            raise ValueError(
                f"Only {len(eligible_df)} eligible Industrials in DEUPT for XPAR/XETR (<{top_n}). "
                f"Rulebook fallback outside DEUPT not implemented; stopping."
            )

        selection_df = eligible_df.head(top_n).copy()
        logger.info(f"Selected top {top_n} constituents for {index} by 6M turnover EUR.")

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

        # FX in base currency (EUR) for weight/NOSH calc
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

        # Merge in symbol, FX and EOD close
        selection_df = (
            selection_df.merge(symbols_filtered, on=["ISIN", "MIC"], how="left")
            .merge(fx_merge_df, on="#Symbol", how="left")
            .merge(eod_price_merge_df, on="#Symbol", how="left")
        )

        missing_symbol = int(selection_df["#Symbol"].isna().sum())
        missing_fx = int(selection_df["FX/Index Ccy"].isna().sum())
        missing_px = int(selection_df["Close Prc_EOD"].isna().sum())
        logger.info(
            f"Selection rows: {len(selection_df)} | Missing #Symbol: {missing_symbol} | "
            f"Missing FX: {missing_fx} | Missing EOD Close: {missing_px}"
        )

        if selection_df["Close Prc_EOD"].isna().all():
            raise ValueError("All selected rows have missing Close Prc_EOD; cannot compute NOSH.")

        # --------------------------
        # Weighting = Equal Weight; NOSH like ENVB
        # --------------------------
        index_mcap = None
        try:
            index_mcap = index_eod_df.loc[index_eod_df["#Symbol"] == str(isin).strip(), "Mkt Cap"].iloc[0]
        except Exception as e:
            raise ValueError(
                f"Could not read index market cap for ISIN/ticker '{isin}' from index_eod_df: {e}"
            )

        if pd.isna(index_mcap) or index_mcap <= 0:
            raise ValueError(f"Invalid index market cap: {index_mcap}")

        n = len(selection_df)
        selection_df["Weight_Final"] = 1.0 / n
        selection_df["Target_Market_Cap"] = selection_df["Weight_Final"] * index_mcap

        selection_df["Close Prc_EOD"] = pd.to_numeric(selection_df["Close Prc_EOD"], errors="coerce")
        selection_df["FX/Index Ccy"] = pd.to_numeric(selection_df["FX/Index Ccy"], errors="coerce")

        # For EUR-denominated lines, FX might be 1; still require non-null.
        if selection_df["FX/Index Ccy"].isna().any():
            # Try fallback: if universe already has Price (EUR), derive FX = Price(EUR)/ClosePrc_EOD when possible.
            if "Price (EUR) " in selection_df.columns:
                selection_df["Price (EUR) "] = pd.to_numeric(selection_df["Price (EUR) "], errors="coerce")
                fx_fallback = selection_df["Price (EUR) "] / selection_df["Close Prc_EOD"]
                selection_df["FX/Index Ccy"] = selection_df["FX/Index Ccy"].fillna(fx_fallback)
                logger.warning("Filled missing FX/Index Ccy using Price (EUR)/Close Prc_EOD fallback where possible.")

        if selection_df["FX/Index Ccy"].isna().any():
            bad = selection_df[selection_df["FX/Index Ccy"].isna()][["Name", "ISIN", "MIC", "#Symbol"]]
            raise ValueError(
                f"Missing FX/Index Ccy for {len(bad)} selected rows; cannot compute NOSH. "
                f"Examples:\n{bad.head(10).to_string(index=False)}"
            )

        denom = selection_df["Close Prc_EOD"] * selection_df["FX/Index Ccy"]
        if (denom <= 0).any():
            bad = selection_df[denom <= 0][["Name", "ISIN", "MIC", "Close Prc_EOD", "FX/Index Ccy"]]
            raise ValueError(
                f"Non-positive price*FX for {len(bad)} selected rows; cannot compute NOSH. "
                f"Examples:\n{bad.head(10).to_string(index=False)}"
            )

        selection_df["Number of Shares_Calculated"] = (selection_df["Target_Market_Cap"] / denom).round()

        # As per rulebook: Free Float factor not applied; Capping factor not applied
        selection_df["Free Float_Output"] = 1
        selection_df["Capping Factor"] = 1
        selection_df["Effective Date of Review"] = effective_date

        # --------------------------
        # Final composition output
        # --------------------------
        FGINP_df = (
            selection_df[
                [
                    "Name",
                    "ISIN",
                    "MIC",
                    "Number of Shares_Calculated",
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
                    "Number of Shares_Calculated": "Number of Shares",
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
            FGINP_df,
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
            fginp_path = os.path.join(output_dir, f"{index}_df_{timestamp}.xlsx")

            logger.info(f"Saving {index} output to: {fginp_path}")
            with pd.ExcelWriter(fginp_path) as writer:
                FGINP_df.to_excel(writer, sheet_name=index + " Composition", index=False)
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
                "data": {"fginp_path": fginp_path},
            }

        except Exception as e:
            error_msg = f"Error saving output file: {str(e)}"
            logger.error(error_msg)
            logger.error(traceback.format_exc())
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
