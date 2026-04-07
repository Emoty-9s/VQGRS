# -*- coding: utf-8 -*-
"""Verify EPS This Y growth vs est level (uses same helpers as build_factors_latest). Safe to delete."""
from __future__ import annotations

import math
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

from build_factors_latest import (  # noqa: E402
    build_symbol_effective_date_lookup,
    get_eps_ttm_series_at,
    get_latest_financial_snapshot_at,
    get_prior_fiscal_year_actual_eps_from_ttm_series,
    latest_price_date_per_symbol,
    load_estimates_snapshot,
    load_financials,
    load_prices,
    select_latest_row_from_symbol_effective_lookup,
    standardize_estimates_snapshot_effective_date,
    _float_or_nan,
)


def _eps_this_y_bundle(
    sym: str,
    as_of: str,
    fin_sym_df: pd.DataFrame,
    ea_row: dict | None,
) -> dict:
    """Mirror build_factors_latest EPS This Y / diagnostics (no Forward P/E)."""
    eps_this_y_est = np.nan
    eps_next_y_est = np.nan
    if ea_row is not None:
        eps_this_y_est = _float_or_nan(ea_row.get("epsThisY"))
        eps_next_y_est = _float_or_nan(ea_row.get("epsNextY"))

    row_latest = get_latest_financial_snapshot_at(sym, as_of, fin_sym_df)
    financials_date = row_latest.get("fiscalDate", np.nan) if row_latest else np.nan
    latest_fd_dt = pd.to_datetime(financials_date, errors="coerce") if financials_date is not None else pd.NaT
    latest_fd_anchor = None if pd.isna(latest_fd_dt) else latest_fd_dt

    series_eps = get_eps_ttm_series_at(sym, as_of, fin_sym_df)
    prior_actual_eps = get_prior_fiscal_year_actual_eps_from_ttm_series(
        series_eps, latest_fd_anchor, tolerance_days=180
    )

    eps_this_y_calc_source = "missing_estimate"
    eps_this_y_growth = np.nan
    has_est = eps_this_y_est is not None and not (isinstance(eps_this_y_est, float) and np.isnan(eps_this_y_est))
    if not has_est:
        eps_this_y_calc_source = "missing_estimate"
    elif prior_actual_eps is None:
        eps_this_y_calc_source = "missing_prior_actual"
    elif float(prior_actual_eps) <= 0:
        eps_this_y_calc_source = "invalid_nonpositive_base"
    elif float(eps_this_y_est) <= 0:
        eps_this_y_calc_source = "invalid_nonpositive_base"
    else:
        try:
            _g_ty = float(eps_this_y_est) / float(prior_actual_eps) - 1.0
            if not math.isfinite(_g_ty):
                eps_this_y_growth = np.nan
                eps_this_y_calc_source = "missing_prior_actual"
            else:
                eps_this_y_growth = float(np.clip(_g_ty, -0.95, 3.0))
                eps_this_y_calc_source = "derived_from_estimate_and_prior_actual"
        except (TypeError, ValueError, ZeroDivisionError):
            eps_this_y_growth = np.nan
            eps_this_y_calc_source = "missing_prior_actual"

    price = np.nan  # placeholder; Forward P/E skipped here
    forward_pe = np.nan
    if (
        eps_next_y_est is not None
        and not np.isnan(eps_next_y_est)
        and float(eps_next_y_est) > 0
        and not (isinstance(price, float) and np.isnan(price))
    ):
        forward_pe = float(price) / float(eps_next_y_est)

    return {
        "symbol": sym,
        "EPS This Y (growth)": eps_this_y_growth,
        "EPS This Y Est Level": eps_this_y_est,
        "EPS This Y Base Actual": float(prior_actual_eps) if prior_actual_eps is not None else np.nan,
        "EPS This Y Calc Source": eps_this_y_calc_source,
        "EPS Next Y (out col)": np.nan,
        "EPS Next Y Est Level": eps_next_y_est,
        "Forward P/E skip (no price)": forward_pe,
    }


def main() -> None:
    data_dir = ROOT / "data"
    prices = load_prices(data_dir)
    latest_map = latest_price_date_per_symbol(prices)
    fin = load_financials(data_dir)
    if fin.empty or "symbol" not in fin.columns:
        print("No financials_quarterly — cannot run EPS helper verification.")
        return
    fin["symbol"] = fin["symbol"].astype(str).str.strip().str.upper()
    financials_by_symbol = {
        s: g for s, g in fin.groupby(fin["symbol"], sort=False)
    }
    est = standardize_estimates_snapshot_effective_date(load_estimates_snapshot(data_dir))
    est_lookup = build_symbol_effective_date_lookup(est, symbol_col="symbol")

    # Pick NNE + 9 names with weak fundamentals if possible
    fac_csv = data_dir / "factors_latest.csv"
    picked = ["NNE"]
    if fac_csv.exists():
        fac = pd.read_csv(fac_csv, usecols=lambda c: c in {"symbol", "Income (Net)", "Sales (Rev)"})
        fac["symbol"] = fac["symbol"].astype(str).str.upper()
        ni = pd.to_numeric(fac.get("Income (Net)", pd.Series(np.nan)), errors="coerce")
        risky = fac.loc[ni < 0, "symbol"].dropna().astype(str).str.upper().tolist()
        for s in risky:
            if s not in picked and len(picked) < 10:
                picked.append(s)
    for s in latest_map.index.astype(str).str.upper():
        if len(picked) >= 10:
            break
        if s not in picked:
            picked.append(s)

    rows = []
    for sym in picked[:10]:
        as_of = latest_map.get(sym)
        if not as_of:
            continue
        as_of = str(as_of)[:10]
        fin_sym = financials_by_symbol.get(sym, fin.iloc[0:0])
        ea_row = select_latest_row_from_symbol_effective_lookup(
            est_lookup,
            symbol=sym,
            as_of_date=as_of,
            effective_date_col="effective_date",
        )
        ea_dict = ea_row.to_dict() if ea_row is not None and hasattr(ea_row, "to_dict") else None
        bundle = _eps_this_y_bundle(sym, as_of, fin_sym, ea_dict)
        # Forward P/E with real price
        ser = prices.loc[prices["symbol"].astype(str).str.upper() == sym].sort_values("date")
        ser = ser.loc[ser["date"] <= as_of]
        px = float(ser["close"].iloc[-1]) if not ser.empty else float("nan")
        ene = bundle["EPS Next Y Est Level"]
        fpe = np.nan
        if (
            ene is not None
            and not (isinstance(ene, float) and np.isnan(ene))
            and float(ene) > 0
            and math.isfinite(px)
        ):
            fpe = px / float(ene)
        bundle["Price"] = px
        bundle["Forward P/E"] = fpe
        del bundle["Forward P/E skip (no price)"]
        rows.append(bundle)

    out = pd.DataFrame(rows)
    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", 200)
    print("=== A. Sample (10) — EPS growth vs levels (helper mirror, latest price_date as as_of) ===\n")
    print(out.to_string(index=False))

    print("\n=== B. Sanity (on table) ===")
    if not out.empty:
        same = (out["EPS This Y (growth)"] == out["EPS This Y Est Level"]).fillna(False)
        print(f"  Rows where growth == est level (bad): {int(same.sum())}")
        m = out["EPS This Y Base Actual"] <= 0
        g = out["EPS This Y (growth)"]
        bad = m & g.notna()
        print(f"  Rows base<=0 but growth non-null (bad): {int(bad.sum())}")
        print("  Calc source counts:\n", out["EPS This Y Calc Source"].value_counts(dropna=False).to_string())

    from score_factor_config import FACTOR_SPECS  # noqa: E402

    print("\n=== C. score_factor_config ===")
    sp_this = FACTOR_SPECS.get("EPS This Y")
    sp_next = FACTOR_SPECS.get("EPS Next Y")
    print(f"  EPS This Y: enabled={getattr(sp_this, 'enabled', None)} category={getattr(sp_this, 'category', None)}")
    print(f"  EPS Next Y: enabled={getattr(sp_next, 'enabled', None)} category={getattr(sp_next, 'category', None)}")

    nne = out[out["symbol"] == "NNE"]
    print("\n=== D. NNE ===")
    if nne.empty:
        print("  NNE not in sample (no price row?)")
    else:
        print(nne.T.to_string())


if __name__ == "__main__":
    main()
