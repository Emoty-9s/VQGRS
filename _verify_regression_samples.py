# -*- coding: utf-8 -*-
"""One-off regression check: valuation / OCF / track proxy (before vs after logic). Safe to delete."""
from __future__ import annotations

import logging
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

from build_factors_latest import (  # noqa: E402
    get_financial_rows_available_by_as_of,
    get_ocf_ni_at,
    get_ocf_yoy_at,
    load_financials,
)
from build_final_vqgrs_scores import _load_track_inputs_from_factors_latest  # noqa: E402


def _lower_better_score(series: pd.Series) -> pd.Series:
    x = pd.to_numeric(series, errors="coerce")
    med = float(x.median(skipna=True))
    q25 = float(x.quantile(0.25))
    q75 = float(x.quantile(0.75))
    iqr = q75 - q25
    if not np.isfinite(iqr) or iqr <= 0:
        return pd.Series(np.nan, index=x.index, dtype=float)
    z = (x - med) / iqr
    return pd.Series(np.clip(50.0 - 20.0 * z, 0.0, 100.0), index=x.index, dtype=float)


def _track_proxy_pre_sanitize(work: pd.DataFrame) -> pd.DataFrame:
    """Historical behavior: raw multiples into _lower_better_score."""
    out = work.copy()
    pe_s = _lower_better_score(out["track_input_pe"])
    ps_s = _lower_better_score(out["track_input_ps"])
    ev_s = _lower_better_score(out["track_input_ev_ebitda"])
    valid_pe = pe_s.notna()
    valid_ps = ps_s.notna()
    valid_ev = ev_s.notna()
    val_count = valid_pe.astype(int) + valid_ps.astype(int) + valid_ev.astype(int)
    out["track_A_valuation_valid_count_old"] = val_count.astype(float)
    den = valid_pe.astype(float) * 0.4 + valid_ps.astype(float) * 0.3 + valid_ev.astype(float) * 0.3
    num = pe_s.fillna(0.0) * 0.4 + ps_s.fillna(0.0) * 0.3 + ev_s.fillna(0.0) * 0.3
    out["track_input_v_market_proxy_old"] = np.where((val_count >= 2) & (den > 0), num / den, np.nan)
    return out


def get_ocf_yoy_at_old(symbol, as_of_date, financials: pd.DataFrame) -> float:
    elig = get_financial_rows_available_by_as_of(symbol=symbol, as_of_date=as_of_date, financials=financials)
    if len(elig) < 8 or "operatingCashFlow" not in elig.columns:
        return np.nan
    ocf = pd.to_numeric(elig["operatingCashFlow"], errors="coerce").iloc[:8]
    latest_4 = ocf.iloc[:4]
    prev_4 = ocf.iloc[4:8]
    if latest_4.notna().sum() < 4 or prev_4.notna().sum() < 4:
        return np.nan
    ocf_latest_4 = latest_4.sum()
    ocf_prev_4 = prev_4.sum()
    if pd.isna(ocf_prev_4) or ocf_prev_4 == 0:
        return np.nan
    if pd.isna(ocf_latest_4):
        return np.nan
    try:
        return float(ocf_latest_4) / float(ocf_prev_4) - 1.0
    except Exception:
        return np.nan


def get_ocf_ni_at_old(symbol, as_of_date, financials: pd.DataFrame) -> float:
    elig = get_financial_rows_available_by_as_of(symbol=symbol, as_of_date=as_of_date, financials=financials)
    if len(elig) < 4:
        return np.nan
    if "operatingCashFlow" not in elig.columns or "netIncome" not in elig.columns:
        return np.nan
    g4 = elig.head(4)
    ocf_4 = pd.to_numeric(g4["operatingCashFlow"], errors="coerce")
    ni_4 = pd.to_numeric(g4["netIncome"], errors="coerce")
    if ocf_4.notna().sum() < 4 or ni_4.notna().sum() < 4:
        return np.nan
    ocf_ttm = ocf_4.sum()
    ni_ttm = ni_4.sum()
    if pd.isna(ni_ttm) or ni_ttm == 0:
        return np.nan
    if pd.isna(ocf_ttm):
        return np.nan
    try:
        return float(ocf_ttm) / float(ni_ttm)
    except Exception:
        return np.nan


def _ocf_yoy_reason(old_v: float, new_v: float, elig) -> str:
    if elig is None or elig.empty or len(elig) < 8:
        return "insufficient_quarters"
    ocf = pd.to_numeric(elig["operatingCashFlow"], errors="coerce").iloc[:8]
    latest_4, prev_4 = ocf.iloc[:4], ocf.iloc[4:8]
    if latest_4.notna().sum() < 4 or prev_4.notna().sum() < 4:
        return "need_4_valid_each_window"
    pl, pp = float(prev_4.sum()), float(latest_4.sum())
    if np.isnan(pl) or np.isnan(pp):
        return "nan_sums"
    if pl <= 0 or pp <= 0:
        return "nonpositive_ocf_window(sign_change_or_zero)"
    if abs(pl) < 5_000_000.0:
        return "base_abs_below_MIN_OCF_YOY_BASE_ABS"
    if old_v == new_v or (np.isnan(old_v) and np.isnan(new_v)):
        return "—"
    if np.isnan(new_v) and not np.isnan(old_v):
        return "new_guards_or_clip"
    return "clip_or_numeric"


def _ocf_ni_reason(new_v: float, elig) -> str:
    if elig is None or elig.empty or len(elig) < 4:
        return "insufficient_quarters"
    g4 = elig.head(4)
    ocf_4 = pd.to_numeric(g4["operatingCashFlow"], errors="coerce")
    ni_4 = pd.to_numeric(g4["netIncome"], errors="coerce")
    if ocf_4.notna().sum() < 4 or ni_4.notna().sum() < 4:
        return "need_4_valid_each"
    ni_ttm = float(ni_4.sum())
    ocf_ttm = float(ocf_4.sum())
    if np.isnan(ni_ttm) or np.isnan(ocf_ttm):
        return "nan_ttm"
    if ni_ttm <= 0:
        return "NI_ttm<=0"
    if ocf_ttm <= 0:
        return "OCF_ttm<=0"
    return "—"


def main() -> None:
    logging.getLogger("build_factors_latest").setLevel(logging.ERROR)
    fac_path = ROOT / "data" / "factors_latest.csv"
    if not fac_path.exists():
        print("Missing", fac_path)
        return
    fac = pd.read_csv(fac_path, low_memory=False)
    if "asOfDate" in fac.columns:
        fac["as_of"] = pd.to_datetime(fac["asOfDate"], errors="coerce")
    else:
        fac["as_of"] = pd.NaT
    fac["symbol"] = fac["symbol"].astype(str).str.strip().str.upper()

    for c in ["P/E", "Forward P/E", "P/FCF", "EV/EBITDA", "Income (Net)", "Sales (Rev)", "OCF YoY", "OCF/NI"]:
        if c not in fac.columns:
            fac[c] = np.nan

    latest = fac["as_of"].max()
    snap = fac.loc[fac["as_of"] == latest].copy()
    if snap.empty:
        print("No rows at latest asOfDate")
        return

    def num(s):
        return pd.to_numeric(snap[s], errors="coerce")

    ni = num("Income (Net)")
    pe = num("P/E")
    ev = num("EV/EBITDA")
    risky = snap.loc[(ni < 0) | (pe < 0) | (ev < 0) | num("Sales (Rev)").lt(50_000_000)].copy()
    must = {"NNE"}
    pool = risky["symbol"].tolist()
    picked = [s for s in pool if s in must]
    for s in pool:
        if s not in picked and len(picked) < 10:
            picked.append(s)
    if "NNE" in snap["symbol"].values and "NNE" not in picked:
        picked = ["NNE"] + [x for x in picked if x != "NNE"][:9]
    elif "NNE" not in snap["symbol"].values:
        picked = (picked + snap["symbol"].head(10 - len(picked)).tolist())[:10]

    picked = picked[:10]
    print("=== Sample symbols ===", picked)
    print("=== Latest asOfDate ===", latest)

    # --- A. Valuation ---
    print("\n--- A. Valuation (after = stored CSV; before_valid = finite & could be non-positive multiple) ---")
    sub = snap.set_index("symbol").reindex(picked)
    for sym in picked:
        row = sub.loc[sym] if sym in sub.index else None
        if row is None or (isinstance(row, pd.DataFrame) and row.empty):
            continue
        if isinstance(row, pd.DataFrame):
            row = row.iloc[0]
        vals = {k: row.get(k, np.nan) for k in ["P/E", "Forward P/E", "P/FCF", "EV/EBITDA"]}

        def fin(x):
            try:
                xf = float(x)
                return np.isfinite(xf)
            except (TypeError, ValueError):
                return False

        def after_ok(x):
            return fin(x) and float(x) > 0

        print(
            f"{sym:6} PE={vals['P/E']!s:>12} FwdPE={vals['Forward P/E']!s:>12} "
            f"PFCF={vals['P/FCF']!s:>12} EVEBITDA={vals['EV/EBITDA']!s:>12}"
        )
        print(
            "       before_valid(finite): "
            f"PE={fin(vals['P/E'])} Fwd={fin(vals['Forward P/E'])} "
            f"PFCF={fin(vals['P/FCF'])} EV={fin(vals['EV/EBITDA'])} | "
            "after_valid(>0, scoring intent): "
            f"PE={after_ok(vals['P/E'])} Fwd={after_ok(vals['Forward P/E'])} "
            f"PFCF={after_ok(vals['P/FCF'])} EV={after_ok(vals['EV/EBITDA'])}"
        )

    # --- B. OCF ---
    fin_df = load_financials(ROOT / "data")
    print("\n--- B. OCF YoY / OCF-NI (old = pre-guard functions; new = build_factors_latest) ---")
    pit_ok = (
        not fin_df.empty
        and all(c in fin_df.columns for c in ("symbol", "fiscalDate", "effective_date"))
    )
    if fin_df.empty:
        print("No financials_quarterly.parquet — OCF recompute unavailable.")
    elif not pit_ok:
        print(
            "financials_quarterly exists but PIT cols missing (e.g. effective_date) — "
            "OCF recompute unavailable; showing factors_latest CSV only."
        )
    for sym in picked:
        r = sub.loc[sym]
        if isinstance(r, pd.DataFrame):
            r = r.iloc[0]
        as_of = str(r.get("asOfDate", ""))[:10]
        yo_stored = r.get("OCF YoY", np.nan)
        ni_stored = r.get("OCF/NI", np.nan)
        if not pit_ok:
            print(
                f"{sym:6} factors_csv OCF_YoY={yo_stored!s:>14} OCF/NI={ni_stored!s:>12}"
            )
            continue
        elig = get_financial_rows_available_by_as_of(symbol=sym, as_of_date=as_of, financials=fin_df)
        yo_old = get_ocf_yoy_at_old(sym, as_of, fin_df)
        yo_new = get_ocf_yoy_at(sym, as_of, fin_df)
        ni_old = get_ocf_ni_at_old(sym, as_of, fin_df)
        ni_new = get_ocf_ni_at(sym, as_of, fin_df)
        yr = _ocf_yoy_reason(yo_old, yo_new, elig)
        nr = _ocf_ni_reason(ni_new, elig)
        print(
            f"{sym:6} OCF_YoY old={yo_old!s:>14} new={yo_new!s:>14} | OCF/NI old={ni_old!s:>12} new={ni_new!s:>12}"
        )
        print(
            f"       factors_csv OCF_YoY={yo_stored!s:>8} OCF/NI={ni_stored!s:>8} | notes YoY:{yr} OCF/NI:{nr}"
        )

    # --- C. Track ---
    track_new = _load_track_inputs_from_factors_latest()
    if track_new.empty:
        print("\n--- C. Track: no track inputs ---")
        return
    tsub = track_new.set_index("symbol")
    # Rebuild OLD proxy on same work slice as loader uses (full latest universe)
    work_full = track_new[
        [
            "symbol",
            "as_of_date",
            "track_input_pe",
            "track_input_ps",
            "track_input_ev_ebitda",
        ]
    ].copy()
    old_full = _track_proxy_pre_sanitize(work_full)
    old_full = old_full.set_index("symbol")

    print("\n--- C. Track valuation proxy (old = raw multiples; new = sanitized >0) ---")
    final_path = ROOT / "output" / "scoring" / "final_vqgrs_scores_latest.csv"
    tr_col = "assigned_track" if final_path.exists() else None
    final_df = pd.read_csv(final_path, low_memory=False) if tr_col else pd.DataFrame()
    if not final_df.empty and "symbol" in final_df.columns:
        final_df["symbol"] = final_df["symbol"].astype(str).str.strip().str.upper()
        final_df = final_df.set_index("symbol")

    for sym in picked:
        if sym not in tsub.index:
            print(f"{sym:6} not in track_inputs")
            continue
        r = tsub.loc[sym]
        o = old_full.loc[sym] if sym in old_full.index else None
        pe = r["track_input_pe"]
        ps = r["track_input_ps"]
        ev = r["track_input_ev_ebitda"]
        vc = r["track_A_valuation_valid_count"]
        px = r["track_input_v_market_proxy"]
        vc_old = o["track_A_valuation_valid_count_old"] if o is not None else np.nan
        px_old = o["track_input_v_market_proxy_old"] if o is not None else np.nan
        tr = final_df.loc[sym, tr_col] if tr_col and sym in final_df.index else "n/a"
        print(
            f"{sym:6} pe={pe!s:>10} ps={ps!s:>10} ev_ebitda={ev!s:>10} | "
            f"valid_old={vc_old!s:>3} valid_new={vc!s:>3} | proxy_old={px_old!s:>8} proxy_new={px!s:>8} | track={tr}"
        )


if __name__ == "__main__":
    main()
