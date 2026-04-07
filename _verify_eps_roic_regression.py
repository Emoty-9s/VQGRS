# -*- coding: utf-8 -*-
"""One-off regression check: EPS YoY (positive-to-positive) + ROIC operating IC vs legacy diagnostics."""
from __future__ import annotations

import math
from pathlib import Path

import numpy as np
import pandas as pd

import build_factors_latest as bfl

DATA_DIR = Path("data")


def _load_financials_for_verify() -> pd.DataFrame:
    """
    Match build_factors_latest.load_financials when parquet exists; otherwise CSV fallback.
    If effective_date is absent (common in raw extracts), use fiscalDate as PIT proxy
    so eligibility filters work for regression checks (not a substitute for real filing dates).
    """
    df = bfl.load_financials(DATA_DIR)
    if df.empty:
        csv_p = DATA_DIR / "financials_quarterly.csv"
        if not csv_p.exists():
            return pd.DataFrame()
        df = pd.read_csv(csv_p, low_memory=False)
        if "symbol" in df.columns:
            df["symbol"] = df["symbol"].astype(str).str.strip().str.upper()
        if "fiscalDate" in df.columns:
            df["fiscalDate"] = pd.to_datetime(df["fiscalDate"], errors="coerce").dt.strftime("%Y-%m-%d")
    if "effective_date" not in df.columns and "fiscalDate" in df.columns:
        df = df.copy()
        df["effective_date"] = df["fiscalDate"]
    return df


def _load_prices_for_verify() -> pd.DataFrame:
    df = bfl.load_prices(DATA_DIR)
    if not df.empty:
        return df
    csv_p = DATA_DIR / "prices_eod.csv"
    if not csv_p.exists():
        return df
    df = pd.read_csv(csv_p, low_memory=False)
    cols = ["symbol", "date", "open", "high", "low", "close", "volume"]
    use = [c for c in cols + ["adjClose"] if c in df.columns]
    df = df[use]
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    df = df.dropna(subset=["date"])
    if "adjClose" in df.columns:
        df["close"] = pd.to_numeric(df["adjClose"], errors="coerce").fillna(
            pd.to_numeric(df["close"], errors="coerce")
        )
    return df


def _price_on_or_before(prices: pd.DataFrame, sym: str, as_of: str) -> float:
    g = prices.loc[prices["symbol"].astype(str).str.upper() == sym.upper()].copy()
    if g.empty:
        return float("nan")
    g = g.loc[g["date"].astype(str) <= str(as_of)[:10]].sort_values("date")
    if g.empty:
        return float("nan")
    return float(pd.to_numeric(g["close"].iloc[-1], errors="coerce"))


def eps_ttm_pair(series_eps: pd.DataFrame, tolerance_days: int = 180) -> tuple[float, float, str]:
    """Return (current_ttm, prior_ttm, regime_note) mirroring get_eps_yoy_from_eps_ttm_series picks."""
    if series_eps is None or series_eps.empty:
        return float("nan"), float("nan"), "empty_series"
    s = series_eps.sort_values("fiscalDate").reset_index(drop=True)
    eps_latest = bfl._float_or_nan(s.iloc[-1]["eps_ttm"])
    latest_fd_dt = pd.to_datetime(s.iloc[-1]["fiscalDate"], errors="coerce")
    if eps_latest is None or (isinstance(eps_latest, float) and np.isnan(eps_latest)) or pd.isna(latest_fd_dt):
        return float("nan"), float("nan"), "bad_latest"
    target_1y = latest_fd_dt - pd.DateOffset(days=365)
    eps_prev = bfl.pick_eps_ttm_at_or_near(s, target_1y, tolerance_days=tolerance_days)
    if eps_prev is None or (isinstance(eps_prev, float) and np.isnan(eps_prev)):
        return float(eps_latest), float("nan"), "missing_prior_window"
    cur, prv = float(eps_latest), float(eps_prev)
    if cur > 0 and prv > 0:
        return cur, prv, "positive_to_positive"
    return cur, prv, "nonpositive_regime"


def main() -> None:
    prices = _load_prices_for_verify()
    financials = _load_financials_for_verify()
    if financials.empty or prices.empty:
        print("Missing prices or financials; cannot verify.")
        return

    fin_by = {str(k).upper(): v for k, v in financials.groupby("symbol")}
    latest_map = bfl.latest_price_date_per_symbol(prices)
    avail = set(latest_map.index.astype(str).str.upper())

    # Mix: quality large caps, speculative/loss names, cash-rich, + NNE
    want_eps = [
        "NNE",
        "AAPL",
        "MSFT",
        "NVDA",
        "META",
        "PLUG",
        "RIVN",
        "LCID",
        "SOFI",
        "COIN",
    ]
    want_roic = [
        "NNE",
        "AAPL",
        "MSFT",
        "META",
        "GOOGL",
        "BRK.B",
        "BRK-B",
        "JPM",
        "XOM",
        "CSCO",
    ]

    def run_eps(sym: str) -> dict:
        sym_u = sym.upper()
        if sym_u not in avail:
            return {"symbol": sym_u, "note": "not_in_prices"}
        as_of = str(latest_map.get(sym_u, ""))[:10]
        fin_sym = fin_by.get(sym_u, pd.DataFrame())
        ser = bfl.get_eps_ttm_series_at(sym_u, as_of, fin_sym)
        cur, prv, regime = eps_ttm_pair(ser, 180)
        yoy = bfl.get_eps_yoy_from_eps_ttm_series(ser, tolerance_days=180)
        leg = bfl.get_eps_yoy_legacy_ratio_from_eps_ttm_series(ser, tolerance_days=180)
        calc = "positive_to_positive" if regime == "positive_to_positive" else ""
        return {
            "symbol": sym_u,
            "as_of": as_of,
            "EPS YoY": yoy,
            "EPS YoY Current TTM": cur,
            "EPS YoY Prior TTM": prv,
            "EPS YoY Legacy Ratio": leg,
            "EPS YoY Calc Source": calc or regime,
        }

    def run_roic(sym: str) -> dict:
        sym_u = sym.upper()
        if sym_u not in avail:
            return {"symbol": sym_u, "note": "not_in_prices"}
        as_of = str(latest_map.get(sym_u, ""))[:10]
        fin_sym = fin_by.get(sym_u, pd.DataFrame())
        row_latest = bfl.get_latest_financial_snapshot_at(sym_u, as_of, fin_sym)
        row_prev = bfl.get_prev_financial_snapshot_at(sym_u, as_of, fin_sym)
        row_ttm = bfl.get_ttm_financials_at(sym_u, as_of, fin_sym)
        price = _price_on_or_before(prices, sym_u, as_of)
        sh = bfl._float_or_nan(row_latest.get("sharesOutstanding")) if row_latest else float("nan")
        fin_inds = bfl.build_financial_indicators(row_latest, row_ttm, sh, price, row_prev_quarter=row_prev)
        return {
            "symbol": sym_u,
            "as_of": as_of,
            "ROIC": fin_inds.get("ROIC", np.nan),
            "ROIC NOPAT TTM": fin_inds.get("ROIC NOPAT TTM", np.nan),
            "ROIC IC Latest": fin_inds.get("ROIC IC Latest", np.nan),
            "ROIC IC Prev": fin_inds.get("ROIC IC Prev", np.nan),
            "ROIC IC Avg": fin_inds.get("ROIC IC Avg", np.nan),
            "ROIC Cash Buffer Latest": fin_inds.get("ROIC Cash Buffer Latest", np.nan),
            "ROIC Excess Cash Latest": fin_inds.get("ROIC Excess Cash Latest", np.nan),
            "ROIC Calc Source": fin_inds.get("ROIC Calc Source", ""),
        }

    eps_rows = [run_eps(s) for s in want_eps]
    roic_rows = []
    seen_r = set()
    for s in want_roic:
        su = s.upper().replace(".", "-")
        if su in seen_r:
            continue
        seen_r.add(su)
        roic_rows.append(run_roic(su))

    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", 200)
    print("=== A. EPS YoY sample ===")
    print(pd.DataFrame(eps_rows).to_string(index=False))
    print("\n=== B. ROIC sample ===")
    print(pd.DataFrame(roic_rows).to_string(index=False))

    # NNE block
    print("\n=== C. NNE ===")
    nne_eps = run_eps("NNE")
    nne_roic = run_roic("NNE")
    for label, d in [("EPS / YoY", nne_eps), ("ROIC stack", nne_roic)]:
        print(f"  --- {label} ---")
        for k, v in d.items():
            print(f"    {k}: {v}")

    # Scoring diff vs backup if present
    cur_p = Path("output/scoring/final_vqgrs_scores_latest.csv")
    bak_p = Path("output/scoring/final_vqgrs_scores_latest - 복사본.csv")
    syms = list(dict.fromkeys([r["symbol"] for r in eps_rows if "note" not in r] + ["NNE"]))
    cols = [
        "symbol",
        "track_input_roic",
        "score_Q",
        "score_G",
        "final_score",
        "assigned_track",
    ]
    print("\n=== D. Track / scores (current file; backup compare if exists) ===")
    if cur_p.exists():
        cur = pd.read_csv(cur_p, usecols=lambda c: c in cols, low_memory=False)
        cur = cur[cur["symbol"].astype(str).str.upper().isin(syms)].drop_duplicates("symbol")
        print(cur.to_string(index=False))
        if bak_p.exists():
            bak = pd.read_csv(bak_p, usecols=lambda c: c in cols, low_memory=False)
            bak = bak[bak["symbol"].astype(str).str.upper().isin(syms)].drop_duplicates("symbol")
            m = cur.merge(bak, on="symbol", how="outer", suffixes=("_cur", "_bak"))
            for c in cols[1:]:
                m[f"{c}_changed"] = m[f"{c}_cur"] != m[f"{c}_bak"]
            print("\n--- changes vs backup ---")
            ch = m[[c for c in m.columns if c.endswith("_changed")]].any(axis=1)
            print(m.loc[ch | ch.isna()].to_string(index=False))
    else:
        print("  (no final_vqgrs_scores_latest.csv)")


if __name__ == "__main__":
    main()
