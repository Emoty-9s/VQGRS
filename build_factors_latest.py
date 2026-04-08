# -*- coding: utf-8 -*-
"""
Build factors_latest: one row per symbol, latest snapshot of computed indicators.
Reads Parquet from data dir, outputs factors_latest.parquet and .csv.
"""
from __future__ import annotations

import argparse
import math
import logging
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from bisect import bisect_right

import numpy as np
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

INDEX_SYMBOL = "SP500"
DEFAULT_DATA_DIR = "data"
DEFAULT_OUT = "factors_latest"

# OCF YoY / OCF-NI guards: tiny bases and sign-change windows blow up ratios for dev-stage names.
MIN_OCF_YOY_BASE_ABS = 5_000_000.0
OCF_YOY_CLIP_LOW = -0.95
OCF_YOY_CLIP_HIGH = 3.0
OCF_NI_CLIP_LOW = 0.0
OCF_NI_CLIP_HIGH = 5.0

# -----------------------------------------------------------------------------
# Load
# -----------------------------------------------------------------------------


def load_prices(data_dir: Path) -> pd.DataFrame:
    """
    prices_eod.parquet only.

    Raw OHLCV stays in open/high/low/close/volume. Adjusted, ratio-consistent OHLC for
    indicators lives in open_px/high_px/low_px/close_px with price_adjustment_factor
    (adjClose/close when both valid and close>0, then per-symbol ffill/bfill on finite >0).
    price-based indicators are computed from normalized price columns (*_px) to keep
    adjusted price handling consistent.
    """
    base_cols = ["symbol", "date", "open", "high", "low", "close", "volume"]
    extra_cols = [
        "adjClose",
        "price_adjustment_factor",
        "open_px",
        "high_px",
        "low_px",
        "close_px",
    ]
    out_cols = base_cols + extra_cols
    path = data_dir / "prices_eod.parquet"
    if not path.exists():
        return pd.DataFrame(columns=out_cols)
    df = pd.read_parquet(path)
    use_cols = [c for c in base_cols + ["adjClose"] if c in df.columns]
    df = df[use_cols].copy()
    if "symbol" not in df.columns:
        df["symbol"] = ""
    df["symbol"] = df["symbol"].astype(str).str.strip()
    for c in ("open", "high", "low", "close", "volume"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
        else:
            df[c] = np.nan
    if "adjClose" in df.columns:
        df["adjClose"] = pd.to_numeric(df["adjClose"], errors="coerce")
    else:
        df["adjClose"] = np.nan
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    df = df.dropna(subset=["date"])
    df = df.sort_values(["symbol", "date"], ascending=[True, True]).reset_index(drop=True)

    rc = df["close"]
    ac = df["adjClose"]
    raw_factor = pd.Series(np.nan, index=df.index, dtype=float)
    ok_ratio = ac.notna() & rc.notna() & (rc > 0) & np.isfinite(ac.astype(float)) & np.isfinite(rc.astype(float))
    raw_factor.loc[ok_ratio] = (ac / rc).loc[ok_ratio]
    raw_factor = raw_factor.where(np.isfinite(raw_factor) & (raw_factor > 0), np.nan)
    df["price_adjustment_factor"] = raw_factor.groupby(df["symbol"]).transform(lambda s: s.ffill().bfill())

    f = df["price_adjustment_factor"]
    f_ok = f.notna() & np.isfinite(f) & (f > 0)

    close_px = ac.where(ac.notna() & np.isfinite(ac), np.nan)
    need_scaled = close_px.isna() & f_ok & rc.notna()
    close_px = close_px.where(~need_scaled, rc * f)
    close_px = close_px.where(close_px.notna(), rc)
    df["close_px"] = close_px

    ro, rh, rl = df["open"], df["high"], df["low"]
    df["open_px"] = (ro * f).where(f_ok & ro.notna(), ro)
    df["high_px"] = (rh * f).where(f_ok & rh.notna(), rh)
    df["low_px"] = (rl * f).where(f_ok & rl.notna(), rl)

    for c in out_cols:
        if c not in df.columns:
            df[c] = np.nan
    return df[out_cols].copy()


def _parse_bool_arg(v: Any, default: bool = False) -> bool:
    if v is None:
        return default
    if isinstance(v, bool):
        return v
    s = str(v).strip().lower()
    if s in {"1", "true", "t", "yes", "y"}:
        return True
    if s in {"0", "false", "f", "no", "n"}:
        return False
    return default


def build_backfill_as_of_date_list(
    *,
    start_date: str,
    end_date: str,
    freq: str,
) -> List[str]:
    """
    Build requested asOfDate list (skeleton): does NOT do PIT selection yet.

    freq:
      - daily: step 1 day
      - weekly: step 7 days
      - monthly: step 1 month (same day-of-month as start, clipped by pandas behavior)
    """
    s = pd.to_datetime(start_date, errors="coerce")
    e = pd.to_datetime(end_date, errors="coerce")
    if pd.isna(s) or pd.isna(e):
        return []
    if e < s:
        s, e = e, s

    f = str(freq).strip().lower()
    out: list[str] = []

    if f == "monthly":
        # Explicit default choice for monthly backfill: month START (day=1).
        cur = s.replace(day=1)
        if cur < s:
            cur = cur + pd.DateOffset(months=1)
        while cur <= e:
            out.append(cur.strftime("%Y-%m-%d"))
            cur = cur + pd.DateOffset(months=1)
        return sorted(set(out))

    cur = s
    while cur <= e:
        out.append(cur.strftime("%Y-%m-%d"))
        if f == "daily":
            cur = cur + pd.Timedelta(days=1)
        elif f == "weekly":
            cur = cur + pd.Timedelta(days=7)
        else:
            # Unknown freq: treat as daily for skeleton robustness.
            cur = cur + pd.Timedelta(days=1)

    # Deterministic unique ordering.
    return sorted(set(out))


def select_price_date_for_symbol_at_or_before(
    *,
    price_dates: List[str],
    requested_as_of_date: str,
) -> Tuple[Optional[str], str]:
    """
    Choose latest trading date (price_date) <= requested_as_of_date for a single symbol.

    Returns:
      (price_date, price_source_mode):
        - price_source_mode: exact_trade | prev_trade | no_prior_trade | invalid_requested_as_of
    """
    if not requested_as_of_date:
        return None, "invalid_requested_as_of"
    req = pd.to_datetime(requested_as_of_date, errors="coerce")
    if pd.isna(req):
        return None, "invalid_requested_as_of"
    if not price_dates:
        return None, "no_prior_trade"

    # price_dates are YYYY-MM-DD strings; lex order works.
    idx = bisect_right(price_dates, requested_as_of_date) - 1
    if idx < 0:
        return None, "no_prior_trade"
    chosen = price_dates[idx]
    mode = "exact_trade" if chosen == requested_as_of_date else "prev_trade"
    return chosen, mode


def resolve_previous_trading_date(
    *,
    trading_days: List[str],
    requested_as_of_date: str,
) -> Tuple[Optional[str], str]:
    """
    Resolve "price_date" from requested asOfDate using a "last trading day <= requested" rule.

    Returns:
      (price_date, price_source_mode)
        - price_source_mode: exact_trade | prev_trade | no_prior_trade | invalid_requested_as_of | no_trading_days
    """
    if not requested_as_of_date:
        return None, "invalid_requested_as_of"
    req = pd.to_datetime(requested_as_of_date, errors="coerce")
    if pd.isna(req):
        return None, "invalid_requested_as_of"

    if not trading_days:
        return None, "no_trading_days"

    td = pd.to_datetime(pd.Series(trading_days), errors="coerce").dropna().sort_values()
    elig = td.loc[td <= req]
    if elig.empty:
        return None, "no_prior_trade"

    chosen = elig.max()
    chosen_s = pd.Timestamp(chosen).strftime("%Y-%m-%d")
    req_s = pd.Timestamp(req).strftime("%Y-%m-%d")
    mode = "exact_trade" if chosen_s == req_s else "prev_trade"
    return chosen_s, mode


def build_backfill_asof_schedule_skeleton(
    *,
    as_of_dates: List[str],
    trading_days: List[str],
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for d in as_of_dates:
        price_date, price_source_mode = resolve_previous_trading_date(
            trading_days=trading_days,
            requested_as_of_date=d,
        )
        rows.append(
            {
                "asOfDate": d,
                "price_date": price_date if price_date is not None else np.nan,
                "price_source_mode": price_source_mode,
                "data_cutoff_date": d,
            }
        )
    return pd.DataFrame(rows)


# -----------------------------------------------------------------------------
# Point-in-time (PIT) selection helpers
# -----------------------------------------------------------------------------

def _coerce_to_date_str_series(s: pd.Series) -> pd.Series:
    """
    Convert to YYYY-MM-DD string; invalid/NaT -> NaN.
    Stored as string for compatibility with existing output expectations.
    """
    dt = pd.to_datetime(s, errors="coerce")
    return dt.dt.strftime("%Y-%m-%d")


def _add_effective_date_from_column(
    df: pd.DataFrame,
    *,
    date_col: str,
    source_label: str,
    dataset_name: str,
    effective_date_col: str = "effective_date",
    effective_date_source_col: str = "effective_date_source",
) -> pd.DataFrame:
    """
    Standardize effective_date based on a single date column.
    If the column is missing, we DO NOT silently fallback; effective_date becomes NaN and
    effective_date_source is set to 'missing_date_column'.
    """
    out = df.copy()
    if out.empty:
        out[effective_date_col] = np.nan
        out[effective_date_source_col] = np.nan
        return out
    if date_col not in out.columns:
        log.warning("[%s] missing %s -> effective_date becomes NaN", dataset_name, date_col)
        out[effective_date_col] = np.nan
        out[effective_date_source_col] = "missing_date_column"
        return out
    out[effective_date_col] = _coerce_to_date_str_series(out[date_col])
    # NOTE: Avoid np.where(string, np.nan) dtype promotion issues (NumPy 2.x).
    # Keep missing values as None/NaN while using an object-safe assignment.
    out[effective_date_source_col] = None
    mask = out[effective_date_col].notna()
    out.loc[mask, effective_date_source_col] = source_label
    return out


def interpret_financials_effective_date(financials: pd.DataFrame) -> pd.DataFrame:
    """
    Strict PIT effective-date interpretation for quarterly `financials`.

    Priority (first non-null per row wins):
      acceptedDate -> filedAt -> reportDate -> publishedDate -> asOfDate -> fiscalDate(fallback)

    Adds:
      - effective_date
      - effective_date_source

    Fallback to fiscalDate is explicit and logged (both presence and row-level usage).
    """
    out = financials.copy()
    out["effective_date"] = np.nan
    out["effective_date_source"] = np.nan
    if out.empty:
        return out
    if "fiscalDate" not in out.columns:
        log.warning("[financials] missing fiscalDate; cannot fallback. effective_date will stay NaN.")
        return out

    public_priority = ["acceptedDate", "filedAt", "reportDate", "publishedDate", "asOfDate"]
    public_present = [c for c in public_priority if c in out.columns]
    if not public_present:
        log.warning(
            "[financials] no public/effective date columns found (%s). Falling back to fiscalDate where available.",
            public_priority,
        )

    eff = pd.Series(np.nan, index=out.index, dtype=object)
    eff_src = pd.Series(np.nan, index=out.index, dtype=object)

    for c in public_priority:
        if c not in out.columns:
            continue
        cand = _coerce_to_date_str_series(out[c])
        mask = eff.isna() & cand.notna()
        eff.loc[mask] = cand.loc[mask]
        eff_src.loc[mask] = c

    fiscal_cand = _coerce_to_date_str_series(out["fiscalDate"])
    fallback_mask = eff.isna() & fiscal_cand.notna()
    eff.loc[fallback_mask] = fiscal_cand.loc[fallback_mask]
    eff_src.loc[fallback_mask] = "fiscalDate_fallback"

    out["effective_date"] = eff
    out["effective_date_source"] = eff_src

    # Row-level diagnostics.
    used_counts = out["effective_date_source"].fillna("missing_effective_date").value_counts(dropna=False).to_dict()
    fallback_cnt = int(used_counts.get("fiscalDate_fallback", 0))
    if fallback_cnt > 0:
        log.warning("[financials] effective_date fallback used for %s/%s rows.", fallback_cnt, len(out))
    missing_cnt = int(out["effective_date"].isna().sum())
    if missing_cnt > 0:
        log.warning("[financials] effective_date missing for %s rows after interpretation.", missing_cnt)
    return out


def standardize_snapshot_effective_date_asof(
    df: pd.DataFrame,
    *,
    dataset_name: str,
    asof_col: str = "asOfDate",
) -> pd.DataFrame:
    """Snapshot-like dataset: effective_date = asOfDate (no silent fallback)."""
    return _add_effective_date_from_column(
        df,
        date_col=asof_col,
        source_label=asof_col,
        dataset_name=dataset_name,
    )


def standardize_estimates_snapshot_effective_date(df: pd.DataFrame) -> pd.DataFrame:
    """estimates_snapshot: effective_date = asOfDate."""
    return standardize_snapshot_effective_date_asof(df, dataset_name="estimates_snapshot", asof_col="asOfDate")


def standardize_estimates_quarterly_snapshot_effective_date(df: pd.DataFrame) -> pd.DataFrame:
    """estimates_quarterly_snapshot: effective_date = asOfDate."""
    return standardize_snapshot_effective_date_asof(
        df, dataset_name="estimates_quarterly_snapshot", asof_col="asOfDate"
    )


def standardize_targets_snapshot_effective_date(df: pd.DataFrame) -> pd.DataFrame:
    """targets_snapshot: effective_date = asOfDate."""
    return standardize_snapshot_effective_date_asof(df, dataset_name="targets_snapshot", asof_col="asOfDate")


def standardize_shares_snapshot_effective_date(df: pd.DataFrame) -> pd.DataFrame:
    """shares_snapshot: effective_date = asOfDate."""
    return standardize_snapshot_effective_date_asof(df, dataset_name="shares_snapshot", asof_col="asOfDate")


def standardize_company_facts_snapshot_effective_date(df: pd.DataFrame) -> pd.DataFrame:
    """company_facts_snapshot: effective_date = asOfDate."""
    return standardize_snapshot_effective_date_asof(df, dataset_name="company_facts_snapshot", asof_col="asOfDate")


def standardize_index_membership_effective_date(df: pd.DataFrame) -> pd.DataFrame:
    """index_membership: effective_date = asOfDate."""
    return standardize_snapshot_effective_date_asof(df, dataset_name="index_membership", asof_col="asOfDate")


def standardize_insider_holdings_snapshot_effective_date(df: pd.DataFrame) -> pd.DataFrame:
    """insider_holdings_snapshot: effective_date = asOfDate."""
    return standardize_snapshot_effective_date_asof(df, dataset_name="insider_holdings_snapshot", asof_col="asOfDate")


def standardize_insider_transactions_effective_date(df: pd.DataFrame) -> pd.DataFrame:
    """insider_transactions: effective_date = transactionDate."""
    return _add_effective_date_from_column(
        df,
        date_col="transactionDate",
        source_label="transactionDate",
        dataset_name="insider_transactions",
    )


def standardize_dividends_effective_date(df: pd.DataFrame) -> pd.DataFrame:
    """dividends: effective_date = exDate."""
    return _add_effective_date_from_column(
        df,
        date_col="exDate",
        source_label="exDate",
        dataset_name="dividends",
    )


def standardize_prices_effective_date(df: pd.DataFrame) -> pd.DataFrame:
    """prices_eod: effective_date = date."""
    return _add_effective_date_from_column(
        df,
        date_col="date",
        source_label="date",
        dataset_name="prices_eod",
    )


def latest_row_at_or_before(
    df: pd.DataFrame,
    symbol: Any,
    as_of_date: Any,
    date_col: str = "effective_date",
    *,
    symbol_col: str = "symbol",
) -> Optional[pd.Series]:
    """
    Generic PIT selector:
      latest row for `symbol` with date_col <= as_of_date.

    Returns:
      - pd.Series (selected row) or None
    """
    if df is None or df.empty:
        return None
    if symbol_col not in df.columns or date_col not in df.columns:
        return None

    sym = str(symbol).strip().upper() if symbol is not None else ""
    if not sym:
        return None

    asof_ts = pd.to_datetime(as_of_date, errors="coerce")
    if pd.isna(asof_ts):
        return None

    sub = df.loc[df[symbol_col].astype(str).str.strip().str.upper() == sym].copy()
    if sub.empty:
        return None

    sub["_eff_ts"] = pd.to_datetime(sub[date_col], errors="coerce")
    sub = sub.dropna(subset=["_eff_ts"])
    elig = sub.loc[sub["_eff_ts"] <= asof_ts]
    if elig.empty:
        return None

    idx = elig["_eff_ts"].idxmax()
    return elig.loc[idx].drop(labels=["_eff_ts"], errors="ignore")


def pit_latest_snapshot_per_symbol(
    df: pd.DataFrame,
    *,
    as_of_date: str,
    value_cols: List[str],
    symbol_col: str = "symbol",
    date_col: str = "effective_date",
) -> pd.DataFrame:
    """
    PIT snapshot for a specific as_of_date (helper for future backfill mode).
    Does NOT change factor formulas; it only selects the appropriate rows.
    """
    if df is None or df.empty or symbol_col not in df.columns:
        return pd.DataFrame(columns=[symbol_col, date_col] + value_cols)

    symbols = df[symbol_col].dropna().astype(str).str.strip().str.upper().unique().tolist()
    records: list[dict[str, Any]] = []
    for sym in symbols:
        row = latest_row_at_or_before(
            df,
            sym,
            as_of_date,
            date_col=date_col,
            symbol_col=symbol_col,
        )
        if row is None:
            continue
        rec: dict[str, Any] = {symbol_col: sym, date_col: row.get(date_col)}
        for c in value_cols:
            rec[c] = row.get(c) if c in row.index else np.nan
        records.append(rec)
    out = pd.DataFrame(records)
    if out.empty:
        return pd.DataFrame(columns=[symbol_col, date_col] + value_cols)
    return out


def build_symbol_effective_date_lookup(
    df: pd.DataFrame,
    *,
    symbol_col: str,
    effective_date_col: str = "effective_date",
) -> dict[str, pd.DataFrame]:
    """
    Build per-symbol lookup for PIT selection on effective_date_col <= as_of_date.
    Precomputes a datetime `_eff_ts` column and sorts ascending by `_eff_ts`.
    """
    out: dict[str, pd.DataFrame] = {}
    if df is None or df.empty or symbol_col not in df.columns or effective_date_col not in df.columns:
        return out

    work = df.copy()
    work[symbol_col] = work[symbol_col].astype(str).str.strip().str.upper()
    work["_eff_ts"] = pd.to_datetime(work[effective_date_col], errors="coerce")
    work = work.dropna(subset=[symbol_col, "_eff_ts"])
    if work.empty:
        return out

    for sym, g in work.groupby(symbol_col):
        if not sym:
            continue
        out[str(sym)] = g.sort_values("_eff_ts", ascending=True).reset_index(drop=True)
    return out


def select_latest_row_from_symbol_effective_lookup(
    lookup: dict[str, pd.DataFrame],
    *,
    symbol: Any,
    as_of_date: Any,
    effective_date_col: str = "effective_date",
) -> Optional[pd.Series]:
    """
    Select latest row with effective_date_col <= as_of_date from a pre-built lookup.
    Returns a pd.Series or None.
    """
    if lookup is None:
        return None
    sym = str(symbol).strip().upper() if symbol is not None else ""
    if not sym or sym not in lookup:
        return None
    g = lookup[sym]
    if g is None or g.empty or "_eff_ts" not in g.columns:
        return None
    asof_ts = pd.to_datetime(as_of_date, errors="coerce")
    if pd.isna(asof_ts):
        return None
    eff_ts = g["_eff_ts"].values
    i = np.searchsorted(eff_ts, asof_ts, side="right") - 1
    if i < 0:
        return None
    return g.iloc[int(i)]


# -----------------------------------------------------------------------------
# PIT financial helpers (as_of_date-aware)
# -----------------------------------------------------------------------------

def get_financial_rows_available_by_as_of(
    *,
    symbol: Any,
    as_of_date: Any,
    financials: pd.DataFrame,
    symbol_col: str = "symbol",
    fiscal_col: str = "fiscalDate",
    effective_col: str = "effective_date",
) -> pd.DataFrame:
    """
    Return eligible quarterly financial rows for a symbol where:
      - effective_date <= as_of_date
      - fiscalDate is present
    Duplicate fiscalDate rows (same quarter reported multiple times) are resolved
    by choosing the row with the latest effective_date among eligible rows.

    Returned DF:
      - deduped by fiscalDate
      - sorted by fiscalDate desc (newest first)
    """
    if financials is None or financials.empty:
        return pd.DataFrame()
    if symbol_col not in financials.columns or fiscal_col not in financials.columns or effective_col not in financials.columns:
        log.warning(
            "[financials PIT] missing required cols for PIT selection (%s, %s, %s).",
            symbol_col,
            fiscal_col,
            effective_col,
        )
        return pd.DataFrame()

    sym = str(symbol).strip().upper() if symbol is not None else ""
    if not sym:
        return pd.DataFrame()

    asof_ts = pd.to_datetime(as_of_date, errors="coerce")
    if pd.isna(asof_ts):
        return pd.DataFrame()

    sub = financials.loc[
        financials[symbol_col].astype(str).str.strip().str.upper() == sym
    ].copy()
    if sub.empty:
        return pd.DataFrame()

    sub["_eff_ts"] = pd.to_datetime(sub[effective_col], errors="coerce")
    sub["_fisc_ts"] = pd.to_datetime(sub[fiscal_col], errors="coerce")
    sub = sub.dropna(subset=["_eff_ts", "_fisc_ts"])
    elig = sub.loc[sub["_eff_ts"] <= asof_ts].copy()
    if elig.empty:
        return pd.DataFrame()

    # Deduplicate by fiscalDate, keeping max effective_date among eligible rows.
    idx = elig.groupby(fiscal_col)["_eff_ts"].idxmax()
    elig = elig.loc[idx].copy()

    elig = elig.sort_values(fiscal_col, ascending=False)
    elig = elig.drop(columns=[c for c in ["_eff_ts", "_fisc_ts"] if c in elig.columns], errors="ignore")
    elig = elig.reset_index(drop=True)
    return elig


def get_latest_financial_snapshot_at(
    symbol: Any,
    as_of_date: Any,
    financials: pd.DataFrame,
) -> Optional[Dict[str, Any]]:
    elig = get_financial_rows_available_by_as_of(symbol=symbol, as_of_date=as_of_date, financials=financials)
    if elig.empty:
        return None
    return elig.iloc[0].to_dict()


def get_prev_financial_snapshot_at(
    symbol: Any,
    as_of_date: Any,
    financials: pd.DataFrame,
) -> Optional[Dict[str, Any]]:
    elig = get_financial_rows_available_by_as_of(symbol=symbol, as_of_date=as_of_date, financials=financials)
    if len(elig) < 2:
        return None
    return elig.iloc[1].to_dict()


def get_ttm_financials_at(
    symbol: Any,
    as_of_date: Any,
    financials: pd.DataFrame,
) -> Optional[Dict[str, Any]]:
    """
    Compute TTM using the latest 4 eligible fiscal quarters by fiscalDate desc.
    Columns and aggregation rules are aligned with the legacy latest_financials_and_ttm:
      - flow columns: sum over 4 quarters
      - weightedAverageSharesDiluted: mean over 4 quarters
      - sharesOutstanding: from latest eligible quarter (first)
    """
    elig = get_financial_rows_available_by_as_of(symbol=symbol, as_of_date=as_of_date, financials=financials)
    if len(elig) < 4:
        return None
    g4 = elig.head(4)

    FLOW_SUM_COLS = [
        "netIncome",
        "revenue",
        "EBITDA",
        "EBITDA_reported",
        "EBITDA_operating",
        "reconciledDepreciation",
        "depreciationAndAmortization",
        "freeCashFlow",
        "operatingCashFlow",
        "dividendsPaid",
        "operatingIncome",
        "incomeBeforeTax",
        "incomeTaxExpense",
        "grossProfit",
    ]
    SHARE_MEAN_COLS = ["weightedAverageSharesDiluted"]
    SHARE_LAST_COLS = ["sharesOutstanding"]

    out: Dict[str, Any] = {}
    for c in FLOW_SUM_COLS:
        if c in g4.columns:
            out[c] = pd.to_numeric(g4[c], errors="coerce").sum(min_count=1)
    for c in SHARE_MEAN_COLS:
        if c in g4.columns:
            out[c] = pd.to_numeric(g4[c], errors="coerce").mean()
    for c in SHARE_LAST_COLS:
        if c in g4.columns:
            out[c] = pd.to_numeric(g4[c].iloc[0], errors="coerce")
    return out


def get_eps_ttm_series_at(
    symbol: Any,
    as_of_date: Any,
    financials: pd.DataFrame,
) -> pd.DataFrame:
    """
    Build EPS(TTM) time series using only eligible rows (effective_date <= as_of_date).
    eps_ttm is computed as:
      sum(netIncome over last 4 fiscal quarters) / mean(weightedAverageSharesDiluted over last 4)
    """
    elig = get_financial_rows_available_by_as_of(symbol=symbol, as_of_date=as_of_date, financials=financials)
    if elig.empty:
        return pd.DataFrame()
    if "netIncome" not in elig.columns or "weightedAverageSharesDiluted" not in elig.columns or "fiscalDate" not in elig.columns:
        return pd.DataFrame()

    g = elig.copy()
    g["fiscalDate"] = pd.to_datetime(g["fiscalDate"], errors="coerce")
    g = g.dropna(subset=["fiscalDate"]).sort_values("fiscalDate").reset_index(drop=True)
    g["netIncome"] = pd.to_numeric(g["netIncome"], errors="coerce")
    g["weightedAverageSharesDiluted"] = pd.to_numeric(g["weightedAverageSharesDiluted"], errors="coerce")

    ni_ttm = g["netIncome"].rolling(4, min_periods=4).sum()
    wad_mean = g["weightedAverageSharesDiluted"].rolling(4, min_periods=4).mean()
    eps_ttm = ni_ttm / wad_mean.replace(0, np.nan)

    ser = pd.DataFrame({"fiscalDate": g["fiscalDate"].dt.strftime("%Y-%m-%d"), "eps_ttm": eps_ttm.astype(float)})
    ser = ser.dropna(subset=["eps_ttm"])
    return ser


def get_revenue_yoy_at(
    symbol: Any,
    as_of_date: Any,
    financials: pd.DataFrame,
) -> float:
    elig = get_financial_rows_available_by_as_of(symbol=symbol, as_of_date=as_of_date, financials=financials)
    if len(elig) < 8 or "revenue" not in elig.columns:
        return np.nan
    g4 = pd.to_numeric(elig["revenue"], errors="coerce").iloc[:8]
    latest_4 = g4.iloc[:4]
    prev_4 = g4.iloc[4:8]
    if latest_4.notna().sum() < 4 or prev_4.notna().sum() < 4:
        return np.nan
    rev_latest_4 = latest_4.sum()
    rev_prev_4 = prev_4.sum()
    if pd.isna(rev_prev_4) or rev_prev_4 <= 0:
        return np.nan
    if pd.isna(rev_latest_4):
        return np.nan
    try:
        return float(rev_latest_4) / float(rev_prev_4) - 1.0
    except Exception:
        return np.nan


def get_ocf_yoy_at(
    symbol: Any,
    as_of_date: Any,
    financials: pd.DataFrame,
) -> float:
    # pre-revenue / tiny-base / sign-change cases are excluded from normal OCF YoY interpretation
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
    if pd.isna(ocf_prev_4) or pd.isna(ocf_latest_4):
        return np.nan
    prev_f = float(ocf_prev_4)
    latest_f = float(ocf_latest_4)
    if prev_f <= 0 or latest_f <= 0:
        return np.nan
    if abs(prev_f) < MIN_OCF_YOY_BASE_ABS:
        return np.nan
    try:
        ratio = latest_f / prev_f - 1.0
        return float(np.clip(ratio, OCF_YOY_CLIP_LOW, OCF_YOY_CLIP_HIGH))
    except Exception:
        return np.nan


def get_eps_yoy_at(
    symbol: Any,
    as_of_date: Any,
    financials: pd.DataFrame,
) -> float:
    eps_series = get_eps_ttm_series_at(symbol=symbol, as_of_date=as_of_date, financials=financials)
    # PIT EPS YoY rule: uses ONLY eligible EPS(TTM) series (effective_date <= as_of_date).
    return get_eps_yoy_from_eps_ttm_series(eps_series, tolerance_days=180)


def get_eps_yoy_legacy_ratio_from_eps_ttm_series(
    eps_series: pd.DataFrame,
    *,
    tolerance_days: int = 180,
) -> float:
    """
    Diagnostic only: raw EPS(TTM) YoY ratio without positive-earnings guard.
    Loss / turnaround periods can invert or distort the ratio; do not use for scoring.
    """
    if eps_series is None or eps_series.empty or "fiscalDate" not in eps_series.columns or "eps_ttm" not in eps_series.columns:
        return np.nan
    eps_series = eps_series.sort_values("fiscalDate").reset_index(drop=True)
    if eps_series.empty:
        return np.nan

    eps_latest = _float_or_nan(eps_series.iloc[-1]["eps_ttm"])
    latest_fd_dt = pd.to_datetime(eps_series.iloc[-1]["fiscalDate"], errors="coerce")
    if eps_latest is None or (isinstance(eps_latest, float) and np.isnan(eps_latest)) or pd.isna(latest_fd_dt):
        return np.nan

    target_1y = latest_fd_dt - pd.DateOffset(days=365)
    eps_prev = pick_eps_ttm_at_or_near(eps_series, target_1y, tolerance_days=tolerance_days)
    if eps_prev is None or (isinstance(eps_prev, float) and np.isnan(eps_prev)):
        return np.nan
    if float(eps_prev) == 0.0:
        return np.nan

    try:
        return float(eps_latest) / float(eps_prev) - 1.0
    except Exception:
        return np.nan


def get_eps_yoy_from_eps_ttm_series(
    eps_series: pd.DataFrame,
    *,
    tolerance_days: int = 180,
) -> float:
    """
    Compute EPS YoY from an already-built EPS(TTM) series.

    PIT EPS YoY rule (single source of truth):
      - eps_series must already be constructed using eligible financial rows
        where effective_date <= as_of_date (no future leakage).
      - ~1y earlier EPS(TTM) is selected via nearest fiscalDate within tolerance_days.

    EPS YoY is treated as a standard growth rate only in positive-to-positive EPS regimes;
    negative/turnaround regimes are excluded from normal YoY interpretation (returns NaN).
    """
    if eps_series is None or eps_series.empty or "fiscalDate" not in eps_series.columns or "eps_ttm" not in eps_series.columns:
        return np.nan
    eps_series = eps_series.sort_values("fiscalDate").reset_index(drop=True)
    if eps_series.empty:
        return np.nan

    eps_latest = _float_or_nan(eps_series.iloc[-1]["eps_ttm"])
    latest_fd_dt = pd.to_datetime(eps_series.iloc[-1]["fiscalDate"], errors="coerce")
    if eps_latest is None or (isinstance(eps_latest, float) and np.isnan(eps_latest)) or pd.isna(latest_fd_dt):
        return np.nan

    target_1y = latest_fd_dt - pd.DateOffset(days=365)
    eps_prev = pick_eps_ttm_at_or_near(eps_series, target_1y, tolerance_days=tolerance_days)
    if eps_prev is None or (isinstance(eps_prev, float) and np.isnan(eps_prev)):
        return np.nan

    latest_f = float(eps_latest)
    prior_f = float(eps_prev)
    if latest_f <= 0.0 or prior_f <= 0.0:
        return np.nan

    try:
        yoy = latest_f / prior_f - 1.0
        if not math.isfinite(yoy):
            return np.nan
        # Clip extreme YoY for stability (same band as OCF YoY caps).
        return float(np.clip(yoy, -0.95, 3.0))
    except Exception:
        return np.nan


def get_ocf_ni_at(
    symbol: Any,
    as_of_date: Any,
    financials: pd.DataFrame,
) -> float:
    # OCF/NI is only meaningful for positive NI and positive OCF
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
    if pd.isna(ni_ttm) or pd.isna(ocf_ttm):
        return np.nan
    if float(ni_ttm) <= 0 or float(ocf_ttm) <= 0:
        return np.nan
    try:
        ratio = float(ocf_ttm) / float(ni_ttm)
        return float(np.clip(ratio, OCF_NI_CLIP_LOW, OCF_NI_CLIP_HIGH))
    except Exception:
        return np.nan


def get_interest_coverage_at(
    symbol: Any,
    as_of_date: Any,
    financials: pd.DataFrame,
    *,
    interest_col: Optional[str] = None,
) -> float:
    elig = get_financial_rows_available_by_as_of(symbol=symbol, as_of_date=as_of_date, financials=financials)
    if len(elig) < 4:
        return np.nan
    if "operatingIncome" not in elig.columns or "incomeBeforeTax" not in elig.columns:
        return np.nan

    use_fallback = interest_col is None
    if interest_col is None:
        interest_col = detect_interest_expense_column(financials)
        use_fallback = interest_col is None

    g4 = elig.head(4)
    oi_ttm = pd.to_numeric(g4["operatingIncome"], errors="coerce").sum(min_count=1)
    if pd.isna(oi_ttm):
        return np.nan

    if use_fallback:
        ibt_ttm = pd.to_numeric(g4["incomeBeforeTax"], errors="coerce").sum(min_count=1)
        interest_ttm = oi_ttm - ibt_ttm if not pd.isna(ibt_ttm) else np.nan
    else:
        if interest_col not in g4.columns:
            return np.nan
        interest_ttm = pd.to_numeric(g4[interest_col], errors="coerce").sum(min_count=1)

    if pd.isna(interest_ttm) or abs(float(interest_ttm)) < 1e-12:
        return np.nan
    try:
        return float(oi_ttm) / abs(float(interest_ttm))
    except Exception:
        return np.nan


def get_opm_volatility_at(
    symbol: Any,
    as_of_date: Any,
    financials: pd.DataFrame,
    *,
    window_quarters: int = 8,
    min_quarters: int = 4,
) -> float:
    elig = get_financial_rows_available_by_as_of(symbol=symbol, as_of_date=as_of_date, financials=financials)
    if len(elig) < min_quarters:
        return np.nan
    if "operatingIncome" not in elig.columns or "revenue" not in elig.columns:
        return np.nan

    g = elig.head(window_quarters).copy()
    g["revenue"] = pd.to_numeric(g["revenue"], errors="coerce")
    g = g.loc[g["revenue"].notna() & (g["revenue"] != 0)]
    if len(g) < min_quarters:
        return np.nan

    opm = pd.to_numeric(g["operatingIncome"], errors="coerce") / g["revenue"]
    opm = opm.replace([np.inf, -np.inf], np.nan).dropna()
    if len(opm) < min_quarters:
        return np.nan
    try:
        return float(opm.std())
    except Exception:
        return np.nan


# FIX: sp500 시장 심볼 후보 (^GSPC 없을 때 순서대로 사용).
# 원라이너: sp500_prices.parquet의 symbol이 ^GSPC인지 — (df["symbol"].astype(str).str.strip().str.upper() == "^GSPC").any()
# 원라이너: 필요 컬럼 존재 여부 — ("date" in df.columns and "close" in df.columns)
SP500_SYMBOL_CANDIDATES = ["^GSPC", "GSPC", "SP500", "^SPX", "SPX", "S&P500", "S&P 500"]


def _resolve_sp500_market_df(raw: pd.DataFrame) -> pd.DataFrame:
    """raw(parquet 로드 직후): symbol 있으면 후보로 필터, 없으면 전체 사용(단 date/close 검증). 빈 DF 가능."""
    if raw is None or raw.empty:
        return raw
    # FIX: symbol 컬럼 없으면 전체를 시장 데이터로 사용, date/close 검증
    if "symbol" not in raw.columns:
        log.warning("sp500_prices.parquet에 symbol 컬럼이 없어 전체를 시장 데이터로 사용합니다.")
        if "date" not in raw.columns or "close" not in raw.columns:
            log.warning("sp500_prices에 date 또는 close가 없어 Beta 계산이 스킵됩니다.")
            return pd.DataFrame(columns=["symbol", "date", "close"])
        return raw.copy()
    symbols = raw["symbol"].dropna().astype(str).str.strip().str.upper()
    for cand in SP500_SYMBOL_CANDIDATES:
        if (symbols == cand).any():
            out = raw.loc[raw["symbol"].astype(str).str.strip().str.upper() == cand].copy()
            if "date" in out.columns and "close" in out.columns:
                return out
            return pd.DataFrame(columns=["symbol", "date", "close"])
    log.warning(
        "sp500_prices.parquet에 ^GSPC(또는 후보 %s) 데이터가 없어 Beta 계산이 스킵됩니다.",
        SP500_SYMBOL_CANDIDATES,
    )
    return pd.DataFrame(columns=["symbol", "date", "close"])


def load_sp500_prices(data_dir: Path) -> pd.DataFrame:
    """sp500_prices.parquet: ^GSPC 일봉. 기대 컬럼: symbol, date, close. Parquet만 읽음."""
    path = data_dir / "sp500_prices.parquet"
    if not path.exists():
        return pd.DataFrame(columns=["symbol", "date", "close"])
    df = pd.read_parquet(path)
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    # FIX: symbol 있으면 strip/upper 정규화
    if "symbol" in df.columns:
        df["symbol"] = df["symbol"].astype(str).str.strip().str.upper()
    return df.dropna(subset=["date"])


def load_financials(data_dir: Path) -> pd.DataFrame:
    path = data_dir / "financials_quarterly.parquet"
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_parquet(path)
    if "symbol" in df.columns:
        df["symbol"] = df["symbol"].astype(str).str.strip().str.upper()
    if "fiscalDate" in df.columns:
        df["fiscalDate"] = pd.to_datetime(df["fiscalDate"], errors="coerce").dt.strftime("%Y-%m-%d")
    return df


def load_dividends(data_dir: Path) -> pd.DataFrame:
    path = data_dir / "dividends_events.parquet"
    if not path.exists():
        return pd.DataFrame(columns=["symbol", "exDate", "dividend"])
    df = pd.read_parquet(path, columns=["symbol", "exDate", "dividend"])
    df["symbol"] = df["symbol"].astype(str).str.strip().str.upper()
    df["exDate"] = pd.to_datetime(df["exDate"], errors="coerce").dt.strftime("%Y-%m-%d")
    df["dividend"] = pd.to_numeric(df["dividend"], errors="coerce")
    return df.dropna(subset=["exDate"])


def load_shares(data_dir: Path) -> pd.DataFrame:
    path = data_dir / "shares_snapshot.parquet"
    if not path.exists():
        return pd.DataFrame(columns=["symbol", "asOfDate", "sharesOutstanding", "sharesFloat"])
    df = pd.read_parquet(path)
    if "symbol" in df.columns:
        df["symbol"] = df["symbol"].astype(str).str.strip().str.upper()
    df["asOfDate"] = pd.to_datetime(df["asOfDate"], errors="coerce").dt.strftime("%Y-%m-%d")
    return df


def load_index_membership(data_dir: Path, index_symbol: str) -> pd.DataFrame:
    path = data_dir / "index_membership.parquet"
    if not path.exists():
        return pd.DataFrame(columns=["indexSymbol", "asOfDate", "memberSymbol", "isMember"])
    df = pd.read_parquet(path)
    df = df.loc[df["indexSymbol"].astype(str).str.upper() == index_symbol.upper()]
    if "asOfDate" in df.columns:
        df["asOfDate"] = pd.to_datetime(df["asOfDate"], errors="coerce").dt.strftime("%Y-%m-%d")
    return df


def load_targets(data_dir: Path) -> pd.DataFrame:
    path = data_dir / "targets_snapshot.parquet"
    if not path.exists():
        return pd.DataFrame(columns=["symbol", "asOfDate", "targetPrice"])
    df = pd.read_parquet(path)
    df["asOfDate"] = pd.to_datetime(df["asOfDate"], errors="coerce").dt.strftime("%Y-%m-%d")
    return df


def load_company_facts(data_dir: Path) -> pd.DataFrame:
    """company_facts_snapshot: employees, ipoDate, sharesOutstanding_* (Finviz-style)."""
    path = data_dir / "company_facts_snapshot.parquet"
    if not path.exists():
        return pd.DataFrame(columns=["symbol", "asOfDate", "employees", "ipoDate", "sharesOutstanding_shares", "sharesOutstanding_profile"])
    df = pd.read_parquet(path)
    if "symbol" in df.columns:
        df["symbol"] = df["symbol"].astype(str).str.strip().str.upper()
    if "asOfDate" in df.columns:
        df["asOfDate"] = pd.to_datetime(df["asOfDate"], errors="coerce").dt.strftime("%Y-%m-%d")
    return df


def load_insider_holdings(data_dir: Path) -> pd.DataFrame:
    path = data_dir / "insider_holdings_snapshot.parquet"
    if not path.exists():
        return pd.DataFrame(columns=["symbol", "asOfDate", "reportingName", "securitiesOwned"])
    df = pd.read_parquet(path)
    if "symbol" in df.columns:
        df["symbol"] = df["symbol"].astype(str).str.strip().str.upper()
    if "asOfDate" in df.columns:
        df["asOfDate"] = pd.to_datetime(df["asOfDate"], errors="coerce").dt.strftime("%Y-%m-%d")
    return df


def load_insider_transactions(data_dir: Path) -> pd.DataFrame:
    path = data_dir / "insider_transactions.parquet"
    if not path.exists():
        return pd.DataFrame(columns=["symbol", "transactionDate", "acquisitionOrDisposition", "transactionType", "securitiesTransacted"])
    df = pd.read_parquet(path)
    if "symbol" in df.columns:
        df["symbol"] = df["symbol"].astype(str).str.strip().str.upper()
    return df


def load_estimates_snapshot(data_dir: Path) -> pd.DataFrame:
    path = data_dir / "estimates_snapshot.parquet"
    if not path.exists():
        return pd.DataFrame(columns=["symbol", "asOfDate", "epsThisY", "epsNextY", "epsNextQ", "epsNext5Y"])
    df = pd.read_parquet(path)
    if "asOfDate" in df.columns:
        df["asOfDate"] = pd.to_datetime(df["asOfDate"], errors="coerce").dt.strftime("%Y-%m-%d")
    return df


def load_estimates_quarterly_snapshot(data_dir: Path) -> pd.DataFrame:
    path = data_dir / "estimates_quarterly_snapshot.parquet"
    if not path.exists():
        return pd.DataFrame(columns=["symbol", "asOfDate", "epsNextQ"])
    df = pd.read_parquet(path)
    if "asOfDate" in df.columns:
        df["asOfDate"] = pd.to_datetime(df["asOfDate"], errors="coerce").dt.strftime("%Y-%m-%d")
    return df


# DEPRECATED: legacy latest-only helper, not used by PIT engine main path.
# Kept temporarily for backward compatibility with potential external callers.
def latest_snapshot_per_symbol(df: pd.DataFrame, value_cols: List[str]) -> pd.DataFrame:
    """Symbol별 최신 asOfDate row만 반환. value_cols에 있는 컬럼만 유지."""
    if df.empty or "symbol" not in df.columns or "asOfDate" not in df.columns:
        return pd.DataFrame(columns=["symbol", "asOfDate"] + value_cols)
    tmp = df.copy()
    tmp["symbol"] = tmp["symbol"].astype(str).str.strip().str.upper()
    tmp = tmp.sort_values(["symbol", "asOfDate"], ascending=[True, False])
    tmp = tmp.groupby("symbol").first().reset_index()
    keep_cols = ["symbol", "asOfDate"] + [c for c in value_cols if c in tmp.columns]
    return tmp.reindex(columns=keep_cols)


# -----------------------------------------------------------------------------
# Latest per symbol helpers
# -----------------------------------------------------------------------------
# NOTE:
# - `latest_price_date_per_symbol` / `get_price_series_for_symbol` are ACTIVE in current main path.
# - legacy latest-only helpers below are DEPRECATED and intentionally not referenced by PIT row builder.


def latest_price_date_per_symbol(prices: pd.DataFrame) -> pd.Series:
    g = prices.groupby("symbol", as_index=False)["date"].max()
    return g.set_index("symbol")["date"]


def get_price_series_for_symbol(prices: pd.DataFrame, symbol: str) -> pd.DataFrame:
    p = prices.loc[prices["symbol"] == symbol].sort_values("date").reset_index(drop=True)
    p = p.drop_duplicates(subset=["date"], keep="last")
    return p


# DEPRECATED: legacy latest-only helper, not used by PIT engine main path.
# Kept temporarily for backward compatibility with potential external callers.
def latest_financials_and_ttm(financials: pd.DataFrame) -> Tuple[Dict[str, Dict], Dict[str, Dict], Dict[str, Dict]]:
    """Returns (latest_quarter_by_symbol, prev_quarter_by_symbol, ttm_4q_by_symbol).
    TTM: flow 항목은 sum, share-count 항목은 mean(또는 last). 주식수 합산 시 P/E가 4배 뻥튀기되므로 평균 사용.
    prev_quarter: 직전 분기 row (ROIC 등에서 IC 평균용); 2개 미만 분기인 심볼은 제외."""
    if financials.empty or "symbol" not in financials.columns or "fiscalDate" not in financials.columns:
        return {}, {}, {}

    fin = financials.copy()
    fin["symbol"] = fin["symbol"].astype(str).str.strip().str.upper()
    fin["fiscalDate"] = pd.to_datetime(fin["fiscalDate"], errors="coerce").dt.strftime("%Y-%m-%d")
    fin = fin.dropna(subset=["fiscalDate"])

    fin_desc = fin.sort_values(["symbol", "fiscalDate"], ascending=[True, False])
    latest = fin_desc.groupby("symbol").first().reset_index()
    latest_d = latest.set_index("symbol").to_dict("index")
    # 직전 분기 row (symbol당 2번째 행); ROIC IC 평균용
    prev = fin_desc.groupby("symbol").nth(1).reset_index()
    prev_d = prev.set_index("symbol").to_dict("index") if not prev.empty else {}

    FLOW_SUM_COLS = [
        "netIncome", "revenue", "EBITDA", "freeCashFlow", "operatingCashFlow", "dividendsPaid",
        "operatingIncome", "incomeBeforeTax", "incomeTaxExpense",
        "grossProfit",
    ]
    SHARE_MEAN_COLS = ["weightedAverageSharesDiluted"]
    SHARE_LAST_COLS = ["sharesOutstanding"]

    ttm_d: Dict[str, Dict] = {}
    for sym, g in fin_desc.groupby("symbol"):
        g4 = g.head(4)
        if len(g4) < 4:
            continue
        out: Dict[str, Any] = {}
        for c in FLOW_SUM_COLS:
            if c in g4.columns:
                out[c] = pd.to_numeric(g4[c], errors="coerce").sum(min_count=1)
        for c in SHARE_MEAN_COLS:
            if c in g4.columns:
                out[c] = pd.to_numeric(g4[c], errors="coerce").mean()
        for c in SHARE_LAST_COLS:
            if c in g4.columns:
                out[c] = pd.to_numeric(g4[c].iloc[0], errors="coerce")
        ttm_d[sym] = out
    return latest_d, prev_d, ttm_d


def build_eps_ttm_series(financials: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """Build EPS(TTM) time series per symbol from quarterly financials.
    EPS(TTM) = sum(netIncome 4Q) / mean(weightedAverageSharesDiluted 4Q)."""
    out: Dict[str, pd.DataFrame] = {}
    if financials.empty or "symbol" not in financials.columns or "fiscalDate" not in financials.columns:
        return out
    fin = financials.copy()
    fin["symbol"] = fin["symbol"].astype(str).str.strip().str.upper()
    fin["fiscalDate"] = pd.to_datetime(fin["fiscalDate"], errors="coerce")
    fin = fin.dropna(subset=["fiscalDate"])
    for sym, g in fin.sort_values(["symbol", "fiscalDate"]).groupby("symbol"):
        g = g.sort_values("fiscalDate").reset_index(drop=True)
        ni = pd.to_numeric(g.get("netIncome"), errors="coerce")
        wad = pd.to_numeric(g.get("weightedAverageSharesDiluted"), errors="coerce")
        ni_ttm = ni.rolling(4, min_periods=4).sum()
        wad_mean = wad.rolling(4, min_periods=4).mean()
        eps_ttm = ni_ttm / wad_mean.replace(0, np.nan)
        ser = pd.DataFrame(
            {
                "fiscalDate": g["fiscalDate"],
                "eps_ttm": eps_ttm.astype(float),
            }
        )
        ser = ser.dropna(subset=["eps_ttm"])
        if not ser.empty:
            out[sym] = ser
    return out


def pick_eps_ttm_at_or_near(series_df: pd.DataFrame, target_date: Any, tolerance_days: int = 180) -> float:
    """Pick eps_ttm at fiscalDate nearest to target_date within tolerance_days."""
    if series_df is None or series_df.empty or "fiscalDate" not in series_df.columns or "eps_ttm" not in series_df.columns:
        return np.nan
    if isinstance(target_date, str):
        tgt = pd.to_datetime(target_date, errors="coerce")
    else:
        tgt = pd.to_datetime(target_date, errors="coerce")
    if pd.isna(tgt):
        return np.nan
    dates = pd.to_datetime(series_df["fiscalDate"], errors="coerce")
    diffs = (dates - tgt).abs().dt.days
    if diffs.isna().all():
        return np.nan
    idx = diffs.idxmin()
    if pd.isna(diffs.loc[idx]) or diffs.loc[idx] > tolerance_days:
        return np.nan
    return _float_or_nan(series_df.loc[idx, "eps_ttm"])


def get_prior_fiscal_year_actual_eps_from_ttm_series(
    series_eps: pd.DataFrame | None,
    latest_fd_dt: pd.Timestamp | None,
    tolerance_days: int = 180,
) -> float | None:
    """
    For EPS This Y growth, prior actual EPS is approximated using EPS(TTM) near the prior fiscal year-end.
    This is a derived growth input; FMP does not directly provide a finished 'EPS This Y %' metric in this pipeline.

    Expects ``series_eps`` from ``get_eps_ttm_series_at()`` (fiscalDate, eps_ttm). ``latest_fd_dt`` is the
    anchor fiscal period end (e.g. latest row in that series); prior year is approximated by stepping back
    one calendar year and picking the nearest EPS(TTM) observation within ``tolerance_days``.
    """
    if series_eps is None or getattr(series_eps, "empty", True):
        return None
    if latest_fd_dt is None:
        return None
    try:
        anchor = pd.Timestamp(latest_fd_dt)
    except (TypeError, ValueError):
        return None
    if pd.isna(anchor):
        return None
    try:
        target_prev_fy = anchor - pd.DateOffset(years=1)
    except Exception:
        return None
    if pd.isna(target_prev_fy):
        return None
    picked = pick_eps_ttm_at_or_near(series_eps, target_prev_fy, tolerance_days=tolerance_days)
    try:
        fv = float(picked)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(fv):
        return None
    return fv


# DEPRECATED: legacy latest-only helper, not used by PIT engine main path.
# Kept temporarily for backward compatibility with potential external callers.
def latest_shares_per_symbol(shares: pd.DataFrame) -> pd.DataFrame:
    if shares.empty or "symbol" not in shares.columns:
        return pd.DataFrame(columns=["symbol", "asOfDate", "sharesOutstanding", "sharesFloat"])
    s = shares.sort_values(["symbol", "asOfDate"], ascending=[True, False])
    return s.groupby("symbol").first().reset_index()


def dividend_ttm_for_symbol(dividends: pd.DataFrame, symbol: str, price_date: str, date_series: pd.Series) -> float:
    """Sum dividend where exDate in [price_date-365, price_date]. date_series is sorted list of trading dates for that symbol to resolve '365 days' in trading days if needed."""
    if dividends.empty or symbol not in dividends["symbol"].values:
        return np.nan
    div = dividends.loc[dividends["symbol"] == symbol].copy()
    div = div.dropna(subset=["dividend"])
    if div.empty:
        return np.nan
    # 365 calendar days back from price_date
    try:
        from datetime import datetime, timedelta
        end_d = datetime.strptime(price_date, "%Y-%m-%d")
        start_d = end_d - timedelta(days=365)
        start_s = start_d.strftime("%Y-%m-%d")
    except Exception:
        return np.nan
    div = div.loc[(div["exDate"] >= start_s) & (div["exDate"] <= price_date)]
    if div.empty:
        return np.nan
    return float(div["dividend"].sum())


def dividend_ex_date_for_symbol(dividends: pd.DataFrame, symbol: str, price_date: str) -> Any:
    """Most recent ex-date on/before price_date (PIT-safe). Returns YYYY-MM-DD str or np.nan."""
    if dividends.empty or "symbol" not in dividends.columns or "exDate" not in dividends.columns:
        return np.nan
    sym = symbol.strip().upper()
    div = dividends.loc[dividends["symbol"] == sym]
    if div.empty:
        return np.nan
    div = div.dropna(subset=["exDate"]).copy()
    if div.empty:
        return np.nan
    past = div.loc[div["exDate"] <= price_date]
    if not past.empty:
        return str(past["exDate"].max())[:10]
    return np.nan


def dividend_sum_window(dividends: pd.DataFrame, symbol: str, start_date: str, end_date: str) -> float:
    """Sum of dividend where exDate in [start_date, end_date] (inclusive). Excludes NaN dividend."""
    if dividends.empty or symbol not in dividends["symbol"].values:
        return np.nan
    div = dividends.loc[dividends["symbol"] == symbol].copy()
    div = div.dropna(subset=["dividend"])
    if div.empty:
        return np.nan
    div = div.loc[(div["exDate"] >= start_date) & (div["exDate"] <= end_date)]
    if div.empty:
        return np.nan
    return float(div["dividend"].sum())


# DEPRECATED: legacy latest-only helper, not used by PIT engine main path.
# Kept temporarily for backward compatibility with potential external callers.
def latest_target_per_symbol(targets: pd.DataFrame) -> pd.Series:
    if targets.empty or "symbol" not in targets.columns:
        return pd.Series(dtype=float)
    t = targets.sort_values(["symbol", "asOfDate"], ascending=[True, False])
    t = t.groupby("symbol").first().reset_index()
    return t.set_index("symbol")["targetPrice"]


# DEPRECATED: legacy latest-only helper, not used by PIT engine main path.
# Kept temporarily for backward compatibility with potential external callers.
def latest_index_member(index_df: pd.DataFrame) -> pd.Series:
    if index_df.empty or "memberSymbol" not in index_df.columns:
        return pd.Series(dtype=object)
    idx = index_df.sort_values("asOfDate", ascending=False).iloc[0]
    latest_asof = idx["asOfDate"]
    sub = index_df.loc[index_df["asOfDate"] == latest_asof]
    return sub.set_index("memberSymbol")["isMember"]


# -----------------------------------------------------------------------------
# Finviz-style: company_facts (Employees, IPO Date), Insider Own/Trans
# -----------------------------------------------------------------------------


def build_company_facts_lookup(cf: pd.DataFrame) -> Dict[str, List[Dict[str, Any]]]:
    """symbol -> list of rows sorted by effective/public date desc (newest first).
    Used for public date <= as_of_date lookup.
    Prefer `effective_date` if present; otherwise fall back to `asOfDate`.
    """
    out: Dict[str, List[Dict[str, Any]]] = {}
    if cf.empty or "symbol" not in cf.columns:
        return out
    date_key = "effective_date" if "effective_date" in cf.columns else "asOfDate"
    if date_key not in cf.columns:
        return out
    cf = cf.dropna(subset=["symbol"]).sort_values(["symbol", date_key], ascending=[True, False])
    for sym, g in cf.groupby("symbol"):
        sym = str(sym).strip().upper() if sym else ""
        if not sym:
            continue
        rows = g.to_dict("records")
        out[sym] = [r for r in rows if r.get(date_key)]
    return out


def get_company_facts_at(
    sym: str,
    price_date: str,
    cf_lookup: Dict[str, List[Dict[str, Any]]],
    step_back_for_employees: int = 2,
) -> Dict[str, Any]:
    """Row with effective/public date <= price_date (latest).
    employees: step back up to step_back_for_employees if null.
    Returns debug key `company_facts_public_date`.
    """
    out: Dict[str, Any] = {"employees": np.nan, "ipoDate": np.nan, "shares_out": np.nan, "company_facts_public_date": np.nan}
    if not sym:
        return out
    sym = str(sym).strip().upper()
    rows = cf_lookup.get(sym)
    if not rows:
        return out
    try:
        pd_end = pd.Timestamp(price_date)
    except Exception:
        return out
    has_effective = any(("effective_date" in r and r.get("effective_date")) for r in rows)
    eligible = []
    for r in rows:
        asof = r.get("effective_date") if has_effective else r.get("asOfDate")
        if not asof:
            continue
        try:
            dt = pd.Timestamp(asof)
        except Exception:
            continue
        if dt <= pd_end:
            eligible.append(r)
    if not eligible:
        return out
    chosen = eligible[0]
    out["company_facts_public_date"] = chosen.get("effective_date") if has_effective else chosen.get("asOfDate")
    # shares_out: sharesOutstanding_shares else sharesOutstanding_profile
    so_sh = _float_or_nan(chosen.get("sharesOutstanding_shares"))
    so_pr = _float_or_nan(chosen.get("sharesOutstanding_profile"))
    out["shares_out"] = so_sh if (so_sh is not None and not np.isnan(so_sh) and so_sh > 0) else so_pr
    # ipoDate: YYYY-MM-DD
    ipo = chosen.get("ipoDate")
    if ipo is not None and not pd.isna(ipo):
        try:
            out["ipoDate"] = pd.Timestamp(ipo).strftime("%Y-%m-%d")
        except Exception:
            pass
    # employees: use chosen; if null, step back 1..step_back_for_employees
    for i in range(min(step_back_for_employees + 1, len(eligible))):
        r2 = eligible[i]
        emp = _float_or_nan(r2.get("employees"))
        if emp is not None and not np.isnan(emp) and emp >= 0:
            out["employees"] = emp
            break
    return out


def get_employees_at(sym: str, price_date: str, cf_lookup: Dict[str, List[Dict[str, Any]]]) -> Any:
    """Finviz-style: latest company_facts row with asOfDate <= price_date; step back if employees null."""
    d = get_company_facts_at(sym, price_date, cf_lookup)
    return d.get("employees", np.nan)


def get_ipo_date_at(sym: str, price_date: str, cf_lookup: Dict[str, List[Dict[str, Any]]]) -> Any:
    """Finviz-style: latest company_facts ipoDate with asOfDate <= price_date. YYYY-MM-DD."""
    d = get_company_facts_at(sym, price_date, cf_lookup)
    return d.get("ipoDate", np.nan)


def build_holdings_dedupe_and_totals(holdings: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """symbol -> DataFrame with (asOfDate, total_holdings). Per (symbol, asOfDate): dedupe by reportingName (max securitiesOwned), then sum."""
    out: Dict[str, pd.DataFrame] = {}
    if holdings.empty or "symbol" not in holdings.columns or "securitiesOwned" not in holdings.columns:
        return out
    need = ["symbol", "asOfDate", "securitiesOwned"]
    has_reporting = "reportingName" in holdings.columns
    if has_reporting:
        need.append("reportingName")
    h = holdings.dropna(subset=["symbol"]).copy()
    h["securitiesOwned"] = pd.to_numeric(h["securitiesOwned"], errors="coerce")
    h = h.dropna(subset=["securitiesOwned"])
    if h.empty:
        return out
    if has_reporting:
        # (symbol, asOfDate, reportingName) -> max(securitiesOwned)
        dedupe = h.groupby(["symbol", "asOfDate", "reportingName"], as_index=False)["securitiesOwned"].max()
        tot = dedupe.groupby(["symbol", "asOfDate"], as_index=False)["securitiesOwned"].sum()
    else:
        tot = h.groupby(["symbol", "asOfDate"], as_index=False)["securitiesOwned"].sum()
    tot["symbol"] = tot["symbol"].astype(str).str.strip().str.upper()
    for sym, g in tot.groupby("symbol"):
        sym = str(sym).strip().upper() if sym else ""
        if not sym:
            continue
        out[sym] = g.sort_values("asOfDate", ascending=False).reset_index(drop=True)
    return out


def holdings_total_at(
    sym: str,
    as_of_date: str,
    holdings_by_sym: Dict[str, pd.DataFrame],
) -> Optional[float]:
    """Total insider holdings for symbol at latest snapshot with asOfDate <= as_of_date."""
    if not sym:
        return None
    sym = str(sym).strip().upper()
    df = holdings_by_sym.get(sym)
    if df is None or df.empty:
        return None
    try:
        pd_end = pd.Timestamp(as_of_date)
    except Exception:
        return None
    for _, row in df.iterrows():
        asof = row.get("asOfDate")
        if not asof:
            continue
        try:
            if pd.Timestamp(asof) <= pd_end:
                return float(row["securitiesOwned"])
        except Exception:
            continue
    return None


def holdings_total_at_with_public_date(
    sym: str,
    as_of_date: str,
    holdings_by_sym: Dict[str, pd.DataFrame],
) -> tuple[Optional[float], Optional[str]]:
    """
    Same as `holdings_total_at`, but also returns the chosen snapshot public date (asOfDate).
    """
    holdings = None
    chosen_public_date: Optional[str] = None
    if not sym:
        return np.nan, np.nan
    sym = str(sym).strip().upper()
    df = holdings_by_sym.get(sym)
    if df is None or df.empty:
        return np.nan, np.nan
    try:
        pd_end = pd.Timestamp(as_of_date)
    except Exception:
        return np.nan, np.nan
    for _, row in df.iterrows():
        asof = row.get("asOfDate")
        if not asof:
            continue
        try:
            if pd.Timestamp(asof) <= pd_end:
                holdings = float(row["securitiesOwned"])
                chosen_public_date = str(asof)[:10]
                return holdings, chosen_public_date
        except Exception:
            continue
    return np.nan, np.nan


def holdings_prev_90d(
    sym: str,
    price_date: str,
    holdings_by_sym: Dict[str, pd.DataFrame],
) -> Optional[float]:
    """Holdings total at snapshot closest to (price_date - 90d), asOfDate <= price_date - 90d (Finviz Trans A)."""
    if not sym:
        return None
    sym = str(sym).strip().upper()
    df = holdings_by_sym.get(sym)
    if df is None or df.empty:
        return None
    try:
        target = pd.Timestamp(price_date) - pd.Timedelta(days=90)
    except Exception:
        return None
    best_row = None
    best_diff = None
    for _, row in df.iterrows():
        asof = row.get("asOfDate")
        if not asof:
            continue
        try:
            dt = pd.Timestamp(asof)
            if dt > target:
                continue
            diff = abs((dt - target).total_seconds())
            if best_diff is None or diff < best_diff:
                best_diff = diff
                best_row = row
        except Exception:
            continue
    if best_row is None:
        return None
    return float(best_row["securitiesOwned"])


def build_transactions_group(transactions: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """Symbol -> DataFrame (transactionDate parsed, 90d window for Trans fallback B)."""
    out: Dict[str, pd.DataFrame] = {}
    if transactions.empty or "symbol" not in transactions.columns:
        return out
    need = ["transactionDate", "acquisitionOrDisposition", "transactionType", "securitiesTransacted"]
    if not all(c in transactions.columns for c in need):
        return out
    t = transactions.dropna(subset=["symbol"]).copy()
    t["_tx_date"] = pd.to_datetime(t["transactionDate"], errors="coerce")
    t["securitiesTransacted"] = pd.to_numeric(t["securitiesTransacted"], errors="coerce")
    for sym, g in t.groupby("symbol"):
        sym = str(sym).strip().upper() if sym else ""
        if not sym:
            continue
        out[sym] = g.copy()
    return out


def _transaction_sign(row: pd.Series) -> Optional[int]:
    """+1 acquisition, -1 disposition, None exclude."""
    aod = row.get("acquisitionOrDisposition")
    if aod is not None and not pd.isna(aod):
        aod = str(aod).strip().upper()
        if aod == "A":
            return 1
        if aod == "D":
            return -1
    tt = row.get("transactionType")
    if tt is None or pd.isna(tt):
        return None
    tt_lower = str(tt).lower()
    if "sale" in tt_lower or "sell" in tt_lower or "disposed" in tt_lower:
        return -1
    if "buy" in tt_lower or "purchase" in tt_lower or "acquired" in tt_lower:
        return 1
    return None


def net_trans_shares_90d(sym: str, price_date: str, tx_group: Dict[str, pd.DataFrame]) -> Optional[float]:
    """Net insider transaction shares in (price_date - 90d, price_date] (Finviz Trans B fallback)."""
    df = tx_group.get(str(sym).strip().upper() if sym else "")
    if df is None or df.empty:
        return None
    try:
        end_ts = pd.Timestamp(price_date)
        start_ts = end_ts - pd.Timedelta(days=90)
    except Exception:
        return None
    df = df.dropna(subset=["_tx_date"])
    mask = (df["_tx_date"] > start_ts) & (df["_tx_date"] <= end_ts)
    win = df.loc[mask]
    if win.empty:
        return None
    net = 0.0
    for _, row in win.iterrows():
        sign = _transaction_sign(row)
        if sign is None:
            continue
        qty = row.get("securitiesTransacted")
        if pd.isna(qty):
            continue
        try:
            net += float(qty) * sign
        except (TypeError, ValueError):
            continue
    return net


def insider_own_pct_finviz(
    holdings_now: Optional[float],
    shares_out: Optional[float],
) -> Optional[float]:
    """Insider Own % = 100 * holdings_now / shares_out. Clamp 0..100. 2 decimals."""
    if holdings_now is None or shares_out is None or (isinstance(shares_out, (int, float)) and (np.isnan(shares_out) or shares_out <= 0)):
        return None
    try:
        pct = 100.0 * float(holdings_now) / float(shares_out)
        pct = max(0.0, min(100.0, pct))
        return round(pct, 2)
    except (TypeError, ValueError, ZeroDivisionError):
        return None


def insider_trans_pct_finviz(
    holdings_now: Optional[float],
    holdings_prev: Optional[float],
    net_trans_90d: Optional[float],
    shares_out: Optional[float],
    prefer_holdings_diff: bool = True,
) -> Optional[float]:
    """(A) 100*(holdings_now - holdings_prev)/shares_out if prefer and holdings_prev available; else (B) 100*net_trans_90d/shares_out. Clamp ~[-99, 99]."""
    if shares_out is None or (isinstance(shares_out, (int, float)) and (np.isnan(shares_out) or shares_out <= 0)):
        return None
    try:
        if prefer_holdings_diff and holdings_now is not None and holdings_prev is not None:
            chg = float(holdings_now) - float(holdings_prev)
            pct = 100.0 * chg / float(shares_out)
        elif net_trans_90d is not None:
            pct = 100.0 * float(net_trans_90d) / float(shares_out)
        else:
            return None
        pct = max(-99.0, min(99.0, pct))
        return round(pct, 2)
    except (TypeError, ValueError, ZeroDivisionError):
        return None


def format_insider_own_trans(own_pct: Optional[float], trans_pct: Optional[float]) -> str:
    """'xx.xx% / +x.xx%' or 'N/A / N/A' etc. Always 2 decimals for numbers."""
    own_s = f"{own_pct:.2f}%" if (own_pct is not None and not np.isnan(own_pct)) else "N/A"
    if trans_pct is None or np.isnan(trans_pct):
        trans_s = "N/A"
    else:
        trans_s = f"+{trans_pct:.2f}%" if trans_pct >= 0 else f"{trans_pct:.2f}%"
    return f"{own_s} / {trans_s}"


# -----------------------------------------------------------------------------
# Price-based indicators (one symbol)
# -----------------------------------------------------------------------------

# guard against unresolved split-like discontinuities in vendor price series
EXTREME_DAILY_RETURN_THRESHOLD = 0.80

# Dedupe discontinuity warnings per (symbol, price_date, indicator) within a process.
_discontinuity_warn_keys: set[tuple[str, str, str]] = set()


def _has_extreme_price_discontinuity(
    close_series: pd.Series,
    threshold: float = EXTREME_DAILY_RETURN_THRESHOLD,
) -> bool:
    """
    True if any adjacent daily return on normalized close satisfies |ret| >= threshold.
    Uses ret = close / close.shift(1) - 1 (NaN/invalid pairs excluded from the check).
    """
    if close_series is None or len(close_series) < 2:
        return False
    s = pd.to_numeric(close_series, errors="coerce").astype(float)
    prev = s.shift(1)
    ret = s / prev - 1.0
    valid = prev.notna() & s.notna() & (prev != 0) & np.isfinite(prev) & np.isfinite(s)
    ret = ret.where(valid)
    hit = (ret.abs() >= float(threshold)) & ret.notna()
    return bool(hit.any())


def _close_window_has_discontinuity(
    close: pd.Series,
    ilo: int,
    ihi: int,
    *,
    threshold: float = EXTREME_DAILY_RETURN_THRESHOLD,
) -> bool:
    """Inclusive index window on ``close``; False if fewer than 2 rows."""
    n = len(close)
    if ilo < 0 or ihi >= n or ilo > ihi:
        return False
    return _has_extreme_price_discontinuity(close.iloc[ilo : ihi + 1], threshold)


def _log_price_discontinuity_nan(
    symbol: str | None,
    price_date: str,
    indicator_name: str,
    *,
    reason: str = "extreme_daily_return",
) -> None:
    sym = (symbol or "?").strip().upper() or "?"
    key = (sym, str(price_date)[:10], indicator_name)
    if key in _discontinuity_warn_keys:
        return
    _discontinuity_warn_keys.add(key)
    log.warning(
        "price discontinuity guard: symbol=%s price_date=%s indicator=%s reason=%s",
        sym,
        str(price_date)[:10],
        indicator_name,
        reason,
    )


# Global sanity: flag symbols whose worst close_px daily move is huge (vendor / adjustment issues).
PRICE_QUALITY_ABS_RET_SYMBOL_WARN = 0.95


def summarize_price_series_quality(
    prices: pd.DataFrame,
    symbols: Optional[list[str]] = None,
) -> pd.DataFrame:
    """
    Lightweight per-symbol metrics on the loaded price panel (logging-only consumers).

    Uses normalized ``close_px`` for returns and discontinuity flags; raw ``close`` / ``adjClose``
    for latest-level cross-checks.
    """
    if prices is None or prices.empty or "symbol" not in prices.columns:
        return pd.DataFrame()
    if "close_px" not in prices.columns:
        log.warning("summarize_price_series_quality: close_px column missing; returning empty summary.")
        return pd.DataFrame()
    w = prices.copy()
    w["symbol"] = w["symbol"].astype(str).str.strip().str.upper()
    if symbols:
        keep = {str(s).strip().upper() for s in symbols}
        w = w[w["symbol"].isin(keep)]
    if w.empty:
        return pd.DataFrame()

    rows: list[dict[str, Any]] = []
    for sym, g in w.groupby("symbol", sort=False):
        g = g.sort_values("date")
        n = len(g)
        if n < 1:
            continue
        cp = pd.to_numeric(g["close_px"], errors="coerce").reset_index(drop=True)
        prev = cp.shift(1)
        ret = cp / prev - 1.0
        valid = prev.notna() & cp.notna() & (prev != 0) & np.isfinite(prev) & np.isfinite(cp)
        ret = ret.where(valid)
        max_abs = float(ret.abs().max()) if n > 1 and ret.notna().any() else float("nan")
        if not np.isfinite(max_abs):
            max_abs = float("nan")

        if "price_adjustment_factor" in g.columns:
            fac = pd.to_numeric(g["price_adjustment_factor"], errors="coerce")
            valid_f = fac.notna() & np.isfinite(fac) & (fac > 0)
            valid_adj_factor_ratio = float(valid_f.sum()) / float(len(fac)) if len(fac) else float("nan")
        else:
            valid_adj_factor_ratio = float("nan")

        last = g.iloc[-1]
        latest_raw = pd.to_numeric(last["close"], errors="coerce") if "close" in g.columns else float("nan")
        latest_adj = pd.to_numeric(last["adjClose"], errors="coerce") if "adjClose" in g.columns else float("nan")
        latest_cpx = float(cp.iloc[-1]) if pd.notna(cp.iloc[-1]) else float("nan")

        has_ex = _has_extreme_price_discontinuity(cp, EXTREME_DAILY_RETURN_THRESHOLD)

        rows.append(
            {
                "symbol": sym,
                "first_date": str(g["date"].iloc[0])[:10],
                "last_date": str(g["date"].iloc[-1])[:10],
                "valid_adj_factor_ratio": valid_adj_factor_ratio,
                "max_abs_daily_return_close_px": max_abs,
                "latest_close_px": latest_cpx,
                "latest_close_raw": float(latest_raw) if pd.notna(latest_raw) else float("nan"),
                "latest_adjClose": float(latest_adj) if pd.notna(latest_adj) else float("nan"),
                "has_extreme_discontinuity": bool(has_ex),
            }
        )
    return pd.DataFrame(rows)


def log_price_series_quality_diagnostics(
    prices: pd.DataFrame,
    *,
    mode: str,
    sample_as_of_date: Optional[str] = None,
    top_n: int = 15,
) -> None:
    """
    Logging-only diagnostics for adjusted-price consistency (e.g. BKNG-style cases).
    Runs when mode is ``latest`` or ``sample_as_of_date`` is set.
    """
    m = str(mode).strip().lower()
    sa = (sample_as_of_date or "").strip()
    if m != "latest" and not sa:
        return
    if prices is None or prices.empty:
        log.info("Price quality diagnostics: skipped (empty prices).")
        return

    summ = summarize_price_series_quality(prices)
    if summ.empty:
        log.info("Price quality diagnostics: empty per-symbol summary.")
        return

    col = "max_abs_daily_return_close_px"
    n_ge_095 = int((summ[col] >= PRICE_QUALITY_ABS_RET_SYMBOL_WARN).sum()) if col in summ.columns else 0
    log.info(
        "Price quality self-check: symbol count with max|daily_ret|>=%.2f on close_px: %s",
        PRICE_QUALITY_ABS_RET_SYMBOL_WARN,
        n_ge_095,
    )

    n_all_na_fac = 0
    if "price_adjustment_factor" in prices.columns:
        for _, grp in prices.groupby(prices["symbol"].astype(str).str.strip().str.upper(), sort=False):
            fac = pd.to_numeric(grp["price_adjustment_factor"], errors="coerce")
            if len(fac) and fac.isna().all():
                n_all_na_fac += 1
    log.info("Price quality self-check: symbols with all-NaN price_adjustment_factor: %s", n_all_na_fac)

    if "high_px" in prices.columns and "low_px" in prices.columns:
        hp = pd.to_numeric(prices["high_px"], errors="coerce")
        lp = pd.to_numeric(prices["low_px"], errors="coerce")
        bad_rows = int((hp.notna() & lp.notna() & (hp < lp)).sum())
    else:
        bad_rows = 0
    log.info("Price quality self-check: row count with high_px < low_px: %s", bad_rows)

    top = summ.sort_values(col, ascending=False, na_position="last").head(int(top_n))
    log.info("Price quality: top %s symbols by max_abs_daily_return_close_px (suspicious)", len(top))
    for _, r in top.iterrows():
        log.info(
            "Price quality suspicious: symbol=%s last_date=%s max_abs_ret=%.6f valid_factor_ratio=%.4f discontinuity=%s latest_px=%s raw=%s",
            r["symbol"],
            r["last_date"],
            float(r[col]) if pd.notna(r[col]) else float("nan"),
            float(r["valid_adj_factor_ratio"]) if pd.notna(r["valid_adj_factor_ratio"]) else float("nan"),
            r["has_extreme_discontinuity"],
            r["latest_close_px"],
            r["latest_close_raw"],
        )

    bk = summ.loc[summ["symbol"].astype(str).str.upper() == "BKNG"]
    if not bk.empty:
        r = bk.iloc[0]
        log.info(
            "Price quality BKNG: symbol=%s last_date=%s latest_close_px=%s max_abs_daily_return_close_px=%s has_extreme_discontinuity=%s",
            r["symbol"],
            r["last_date"],
            r["latest_close_px"],
            r[col],
            r["has_extreme_discontinuity"],
        )


def _series_px_or_raw(series: pd.DataFrame, px_col: str, raw_col: str) -> pd.Series:
    """Normalized OHLC (*_px) when column exists, else raw OHLC; always float Series aligned to ``series``."""
    name = px_col if px_col in series.columns else raw_col
    if name not in series.columns:
        return pd.Series(np.nan, index=series.index, dtype=float)
    return pd.to_numeric(series[name], errors="coerce").astype(float)


def _compute_beta_from_returns(stock_ret: pd.Series, mkt_ret: pd.Series) -> float:
    """beta = cov(stock, mkt) / var(mkt). 둘 다 같은 인덱스로 정렬돼 있어야 함."""
    df = pd.concat([stock_ret, mkt_ret], axis=1).dropna()
    if df.shape[0] < 24:
        return np.nan
    s = df.iloc[:, 0].astype(float).values
    m = df.iloc[:, 1].astype(float).values
    var_m = np.var(m, ddof=1)
    if var_m == 0 or np.isnan(var_m):
        return np.nan
    cov_sm = np.cov(s, m, ddof=1)[0, 1]
    beta = cov_sm / var_m
    if np.isnan(beta):
        return np.nan
    beta = max(-10.0, min(10.0, float(beta)))
    return float(beta)


def beta_finviz_style(
    stock_prices: pd.DataFrame,
    mkt_prices: pd.DataFrame,
    price_date: str,
    *,
    months: int = 60,
    min_months: int = 24,
    daily_days: int = 252,
    min_daily: int = 60,
) -> float:
    """
    Finviz 근사:
      1) 월말 종가 기준 월간 수익률로 5년(60개월) beta
      2) 월간 데이터 부족하면 최근 252거래일 일간 beta
    """
    if stock_prices is None or stock_prices.empty or mkt_prices is None or mkt_prices.empty:
        log.debug("beta_finviz_style NaN: stock 또는 mkt 시계열 없음")
        return np.nan

    sp = stock_prices.copy()
    mp = mkt_prices.copy()

    sp["date"] = pd.to_datetime(sp["date"], errors="coerce")
    mp["date"] = pd.to_datetime(mp["date"], errors="coerce")
    sp = sp.dropna(subset=["date"])
    mp = mp.dropna(subset=["date"])
    if sp.empty or mp.empty:
        log.debug("beta_finviz_style NaN: date 파싱 후 시계열 없음")
        return np.nan

    end = pd.to_datetime(price_date, errors="coerce")
    if pd.isna(end):
        log.debug("beta_finviz_style NaN: price_date 파싱 실패")
        return np.nan

    sp = sp.loc[sp["date"] <= end].sort_values("date")
    mp = mp.loc[mp["date"] <= end].sort_values("date")
    if sp.empty or mp.empty:
        log.debug("beta_finviz_style NaN: price_date 이전 데이터 없음")
        return np.nan

    sp["close"] = pd.to_numeric(sp["close"], errors="coerce")
    mp["close"] = pd.to_numeric(mp["close"], errors="coerce")
    sp = sp.dropna(subset=["close"])
    mp = mp.dropna(subset=["close"])
    if sp.empty or mp.empty:
        log.debug("beta_finviz_style NaN: close 파싱/유효 데이터 없음")
        return np.nan

    # (A) 5Y Monthly (월말 종가)
    sp_m = sp.set_index("date")["close"].groupby(pd.Grouper(freq="ME")).last()
    mp_m = mp.set_index("date")["close"].groupby(pd.Grouper(freq="ME")).last()

    sp_ret_m = sp_m.pct_change()
    mp_ret_m = mp_m.pct_change()

    joined_m = pd.concat([sp_ret_m, mp_ret_m], axis=1).dropna()
    # FIX: Beta NaN 원인 진단용 DEBUG (월간 join 샘플 수)
    log.debug("beta_finviz_style 월간 join 샘플수=%s (min_months=%s)", joined_m.shape[0], min_months)
    if joined_m.shape[0] >= min_months:
        joined_m = joined_m.tail(months)
        beta_m = _compute_beta_from_returns(joined_m.iloc[:, 0], joined_m.iloc[:, 1])
        if not np.isnan(beta_m):
            return round(beta_m, 2)

    # (B) Daily fallback (최근 252 trading days)
    sp_d = sp.set_index("date")["close"].sort_index()
    mp_d = mp.set_index("date")["close"].sort_index()

    common_idx = sp_d.index.intersection(mp_d.index)
    # FIX: Beta NaN 원인 진단용 DEBUG (일간 공통 거래일 수)
    log.debug("beta_finviz_style 일간 common_idx 길이=%s (min_daily=%s)", len(common_idx), min_daily)
    if len(common_idx) < min_daily:
        return np.nan

    sp_ret_d = sp_d.loc[common_idx].pct_change()
    mp_ret_d = mp_d.loc[common_idx].pct_change()
    joined_d = pd.concat([sp_ret_d, mp_ret_d], axis=1).dropna()
    if joined_d.shape[0] < min_daily:
        log.debug("beta_finviz_style NaN: 일간 join 유효 행=%s < min_daily", joined_d.shape[0])
        return np.nan

    joined_d = joined_d.tail(daily_days)
    beta_d = _compute_beta_from_returns(joined_d.iloc[:, 0], joined_d.iloc[:, 1])
    if np.isnan(beta_d):
        return np.nan
    return round(beta_d, 2)


def _float_or_nan(x: Any) -> float:
    try:
        return float(x)
    except (TypeError, ValueError):
        return np.nan


def compute_price_indicators(
    series: pd.DataFrame,
    price_date: str,
    *,
    symbol: str | None = None,
) -> Dict[str, float]:
    """
    One-symbol price panel: sorted by date ascending.

    All level/return/ATR style metrics use normalized columns open_px, high_px, low_px, close_px
    when present (from load_prices); otherwise raw open, high, low, close. Volume is unchanged.
    """
    out: Dict[str, float] = {}
    if series.empty or "date" not in series.columns:
        return out
    if "close_px" not in series.columns and "close" not in series.columns:
        return out
    series = series.sort_values("date").reset_index(drop=True)
    idx = series["date"] == price_date
    if not idx.any():
        return out
    loc = int(series.index[idx][0])
    # Open: same px/raw rule as H/L/C (reserved for future bar logic; not used in formulas below).
    open_px_series = _series_px_or_raw(series, "open_px", "open")  # noqa: F841
    high = _series_px_or_raw(series, "high_px", "high")
    low = _series_px_or_raw(series, "low_px", "low")
    close = _series_px_or_raw(series, "close_px", "close")
    volume = pd.to_numeric(series["volume"], errors="coerce").astype(float) if "volume" in series.columns else pd.Series(np.nan, index=series.index)

    price = _float_or_nan(close.iloc[loc])
    out["Price"] = price
    out["Volume"] = _float_or_nan(volume.iloc[loc]) if loc < len(volume) else np.nan

    # Prev Close
    if loc > 0:
        prev_close = _float_or_nan(close.iloc[loc - 1])
        out["Prev Close"] = prev_close
        if prev_close and prev_close != 0:
            out["Change"] = (price - prev_close) / prev_close
        else:
            out["Change"] = np.nan
    else:
        out["Prev Close"] = np.nan
        out["Change"] = np.nan

    # Avg Volume 63
    if loc >= 62:
        out["Avg Volume"] = float(volume.iloc[loc - 62 : loc + 1].mean())
        if out["Avg Volume"] and out["Avg Volume"] != 0 and not np.isnan(out.get("Volume", np.nan)):
            out["Rel Volume"] = out["Volume"] / out["Avg Volume"]
        else:
            out["Rel Volume"] = np.nan
    else:
        out["Avg Volume"] = np.nan
        out["Rel Volume"] = np.nan

    # SMA
    for w, name in [(20, "SMA20"), (50, "SMA50"), (200, "SMA200")]:
        if loc >= w - 1:
            out[name] = float(close.iloc[loc - w + 1 : loc + 1].mean())
        else:
            out[name] = np.nan

    # 52W High / Low (250 trading days)
    if loc >= 249:
        window = close.iloc[loc - 249 : loc + 1]
        out["52W High"] = float(window.max())
        out["52W Low"] = float(window.min())
    else:
        out["52W High"] = np.nan
        out["52W Low"] = np.nan

    # Volatility (63 daily returns, std)
    if loc >= 63:
        if _close_window_has_discontinuity(close, loc - 63, loc):
            out["Volatility"] = np.nan
            _log_price_discontinuity_nan(symbol, price_date, "Volatility")
        else:
            c = close.iloc[loc - 63 : loc + 1].astype(float)
            ret = c / c.shift(1) - 1
            ret = ret.dropna()
            if len(ret) >= 63:
                out["Volatility"] = float(ret.std())
            else:
                out["Volatility"] = np.nan
    else:
        out["Volatility"] = np.nan

    # ATR(14): need 15 rows
    if loc >= 14:
        if _close_window_has_discontinuity(close, loc - 14, loc):
            out["ATR(14)"] = np.nan
            _log_price_discontinuity_nan(symbol, price_date, "ATR(14)")
        else:
            h = high.iloc[loc - 14 : loc + 1]
            l_ = low.iloc[loc - 14 : loc + 1]
            c = close.iloc[loc - 14 : loc + 1]
            tr = np.maximum(h.values - l_.values, np.maximum(np.abs(h.values - c.shift(1).fillna(c.iloc[0]).values), np.abs(l_.values - c.shift(1).fillna(c.iloc[0]).values)))
            out["ATR(14)"] = float(np.mean(tr))
    else:
        out["ATR(14)"] = np.nan

    # RSI(14) Wilder
    if loc >= 14:
        c = close.iloc[: loc + 1].astype(float)
        delta = c.diff()
        gain = delta.clip(lower=0)
        loss = (-delta).clip(lower=0)
        avg_g = gain.iloc[1 : 15].mean()
        avg_l = loss.iloc[1 : 15].mean()
        for i in range(15, len(c)):
            g = gain.iloc[i]
            ls = loss.iloc[i]
            avg_g = (avg_g * 13 + g) / 14
            avg_l = (avg_l * 13 + ls) / 14
        if avg_l and avg_l != 0:
            rs = avg_g / avg_l
            out["RSI(14)"] = 100.0 - (100.0 / (1 + rs))
        else:
            out["RSI(14)"] = 100.0 if avg_g else np.nan
    else:
        out["RSI(14)"] = np.nan

    # Perf (guard long-horizon windows + week/month for consistency)
    for days, name in [
        (5, "Perf Week"),
        (21, "Perf Month"),
        (63, "Perf Quarter"),
        (126, "Perf Half Y"),
        (252, "Perf Year"),
        (252 * 3, "Perf 3Y"),
        (252 * 5, "Perf 5Y"),
        (252 * 10, "Perf 10Y"),
    ]:
        if loc >= days:
            if _close_window_has_discontinuity(close, loc - days, loc):
                out[name] = np.nan
                _log_price_discontinuity_nan(symbol, price_date, name)
            else:
                past_close = _float_or_nan(close.iloc[loc - days])
                if past_close and past_close != 0:
                    out[name] = price / past_close - 1
                else:
                    out[name] = np.nan
        else:
            out[name] = np.nan

    # Perf YTD
    try:
        year = int(price_date[:4])
        year_start = f"{year}-01-01"
        same_year = series.loc[series["date"] >= year_start].loc[series["date"] <= price_date]
        if not same_year.empty:
            sy = same_year.sort_values("date").reset_index(drop=True)
            sy_close = _series_px_or_raw(sy, "close_px", "close")
            if _has_extreme_price_discontinuity(sy_close):
                out["Perf YTD"] = np.nan
                _log_price_discontinuity_nan(symbol, price_date, "Perf YTD")
            else:
                first_close = _float_or_nan(sy_close.iloc[0])
                if first_close and first_close != 0:
                    out["Perf YTD"] = price / first_close - 1
                else:
                    out["Perf YTD"] = np.nan
        else:
            out["Perf YTD"] = np.nan
    except Exception:
        out["Perf YTD"] = np.nan

    out["Beta"] = np.nan  # no market series
    return out


# -----------------------------------------------------------------------------
# Financial / TTM indicators
# -----------------------------------------------------------------------------


def safe_div(a: float, b: float) -> float:
    if b is None or (isinstance(b, (int, float)) and (b == 0 or np.isnan(b))):
        return np.nan
    try:
        return float(a) / float(b)
    except (TypeError, ValueError):
        return np.nan


def compute_operating_invested_capital(
    debt: float | None,
    equity: float | None,
    cash: float | None,
    total_assets: float | None,
    revenue_ttm: float | None,
) -> tuple[float | None, float | None, float | None, float | None]:
    """
    Operating invested capital for ROIC-style denominators.

    ROIC should not subtract all cash for cash-heavy or pre-revenue firms: only excess cash
    (above an operating liquidity buffer) is excluded. A denominator floor caps how small IC
    can become vs assets/equity, reducing unstable ROIC blow-ups.

    Returns:
        (invested_capital, operating_cash_buffer, excess_cash, invested_cap_floor)
    """
    def _fin(x: Any) -> float | None:
        if x is None:
            return None
        try:
            v = float(x)
        except (TypeError, ValueError):
            return None
        return v if math.isfinite(v) else None

    ta = _fin(total_assets)
    rev = _fin(revenue_ttm)
    base_asset_buffer = 0.05 * ta if (ta is not None and ta > 0) else 0.0
    base_revenue_buffer = 0.10 * rev if (rev is not None and rev > 0) else 0.0
    operating_cash_buffer = max(base_asset_buffer, base_revenue_buffer)

    cash_val = 0.0
    if cash is not None:
        c = _fin(cash)
        if c is not None:
            cash_val = c
    excess_cash = max(cash_val - operating_cash_buffer, 0.0)

    eq = _fin(equity)
    floor_assets = 0.10 * ta if (ta is not None and ta > 0) else 0.0
    floor_equity = 0.20 * eq if (eq is not None and eq > 0) else 0.0
    invested_cap_floor = max(floor_assets, floor_equity, 1.0)

    if debt is None or equity is None:
        return (None, operating_cash_buffer, excess_cash, invested_cap_floor)

    d = _fin(debt)
    e = _fin(equity)
    if d is None or e is None:
        return (None, operating_cash_buffer, excess_cash, invested_cap_floor)

    pre_floor_invested_capital = float(d) + float(e) - float(excess_cash)
    if not math.isfinite(pre_floor_invested_capital):
        return (None, operating_cash_buffer, excess_cash, invested_cap_floor)

    invested_capital = max(pre_floor_invested_capital, invested_cap_floor)
    if not math.isfinite(invested_capital):
        return (None, operating_cash_buffer, excess_cash, invested_cap_floor)

    return (invested_capital, operating_cash_buffer, excess_cash, invested_cap_floor)


def build_financial_indicators(
    row_latest: Optional[Dict],
    row_ttm: Optional[Dict],
    shares_out: float,
    price: float,
    row_prev_quarter: Optional[Dict] = None,
) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    if row_latest is None:
        for k in [
            "Income (Net)", "Sales (Rev)", "Book/sh", "Cash/sh", "Payout",
            "EPS (ttm)", "P/E", "P/S", "P/B", "P/C", "P/FCF",
            "Market Cap", "Enterprise Value(EV)", "EV/EBITDA", "EV/EBITDA (Reported)", "EV/EBITDA Source", "EV/Sales",
            "EBITDA (Reported TTM)", "EBITDA (Operating TTM)", "Reconciled Depreciation (TTM)",
            "Quick Ratio", "Current Ratio", "Debt/Eq", "LT Debt/Eq",
            "ROA", "ROE", "ROIC",
            "ROIC NOPAT TTM", "ROIC IC Latest", "ROIC IC Prev", "ROIC IC Avg",
            "ROIC Cash Buffer Latest", "ROIC Cash Buffer Prev",
            "ROIC Excess Cash Latest", "ROIC Excess Cash Prev", "ROIC Calc Source",
            "Gross Margin", "Oper. Margin", "Profit Margin", "Shs Outstand",
        ]:
            out[k] = np.nan if k != "ROIC Calc Source" else ""
        return out

    rev_l = _float_or_nan(row_latest.get("revenue"))
    ni_l = _float_or_nan(row_latest.get("netIncome"))
    gp_l = _float_or_nan(row_latest.get("grossProfit"))
    oi_l = _float_or_nan(row_latest.get("operatingIncome"))
    cash_l = _float_or_nan(row_latest.get("cashAndCashEquivalents"))
    rec_l = _float_or_nan(row_latest.get("receivables"))
    ca_l = _float_or_nan(row_latest.get("currentAssets"))
    cl_l = _float_or_nan(row_latest.get("currentLiabilities"))
    ta_l = _float_or_nan(row_latest.get("totalAssets"))
    eq_l = _float_or_nan(row_latest.get("totalStockholdersEquity"))
    debt_l = _float_or_nan(row_latest.get("totalDebt"))
    ltd_l = _float_or_nan(row_latest.get("longTermDebt"))
    fcf_l = _float_or_nan(row_latest.get("freeCashFlow"))
    so_l = _float_or_nan(row_latest.get("sharesOutstanding"))

    out["Shs Outstand"] = shares_out if not np.isnan(shares_out) else so_l
    sh = out["Shs Outstand"]
    out["Book/sh"] = safe_div(eq_l, sh)
    out["Cash/sh"] = safe_div(cash_l, sh)

    if row_ttm:
        ni_ttm = _float_or_nan(row_ttm.get("netIncome"))
        rev_ttm = _float_or_nan(row_ttm.get("revenue"))
        gp_ttm = _float_or_nan(row_ttm.get("grossProfit"))
        oi_ttm = _float_or_nan(row_ttm.get("operatingIncome"))
        div_paid_ttm = _float_or_nan(row_ttm.get("dividendsPaid"))
        ebitda_ttm = _float_or_nan(row_ttm.get("EBITDA"))
        ebitda_reported_ttm = _float_or_nan(row_ttm.get("EBITDA_reported"))
        ebitda_operating_ttm = _float_or_nan(row_ttm.get("EBITDA_operating"))
        reconciled_dep_ttm = _float_or_nan(row_ttm.get("reconciledDepreciation"))
        fcf_ttm = _float_or_nan(row_ttm.get("freeCashFlow"))
        # TTM diluted shares: mean(4q) from latest_financials_and_ttm (sum 금지 → P/E 4배 뻥튀기 방지)
        wad_ttm = _float_or_nan(row_ttm.get("weightedAverageSharesDiluted"))
        if (wad_ttm is None or np.isnan(wad_ttm)) and shares_out and not np.isnan(shares_out):
            wad_ttm = float(shares_out)
        if (wad_ttm is None or np.isnan(wad_ttm)) and so_l is not None and not np.isnan(so_l):
            wad_ttm = float(so_l)
    else:
        ni_ttm = rev_ttm = gp_ttm = oi_ttm = div_paid_ttm = ebitda_ttm = wad_ttm = fcf_ttm = np.nan
        ebitda_reported_ttm = ebitda_operating_ttm = reconciled_dep_ttm = np.nan

    # Finviz: Income (Net), Sales (Rev) are TTM; fallback to latest quarter
    out["Income (Net)"] = ni_ttm if (row_ttm and ni_ttm is not None and not np.isnan(ni_ttm)) else ni_l
    out["Sales (Rev)"] = rev_ttm if (row_ttm and rev_ttm is not None and not np.isnan(rev_ttm)) else rev_l

    out["Payout"] = np.nan
    if row_ttm and ni_ttm and ni_ttm != 0 and div_paid_ttm is not None and not np.isnan(div_paid_ttm):
        out["Payout"] = abs(div_paid_ttm) / abs(ni_ttm)

    out["Employees"] = np.nan
    out["IPO (Date)"] = np.nan

    # EPS(TTM) = netIncome_ttm / 평균 diluted shares, P/E = Price / EPS(ttm)
    out["EPS (ttm)"] = np.nan
    if (
        wad_ttm is not None and not np.isnan(wad_ttm) and wad_ttm != 0
        and ni_ttm is not None and not np.isnan(ni_ttm)
    ):
        out["EPS (ttm)"] = float(ni_ttm) / float(wad_ttm)
    eps_ttm = out["EPS (ttm)"]
    # P/E: negative or zero EPS(TTM) => NaN (not meaningful as a "cheap" multiple).
    out["P/E"] = np.nan
    if (
        eps_ttm is not None
        and not np.isnan(eps_ttm)
        and float(eps_ttm) > 0
        and price is not None
        and not np.isnan(price)
    ):
        out["P/E"] = float(price) / float(eps_ttm)
    out["P/S"] = safe_div(price, safe_div(rev_ttm, sh)) if row_ttm and sh and rev_ttm is not None else np.nan
    out["P/B"] = safe_div(price, safe_div(eq_l, sh)) if sh else np.nan
    out["P/C"] = safe_div(price, safe_div(cash_l, sh)) if sh else np.nan
    # P/FCF: require positive TTM FCF and positive shares; negative FCF => NaN (not a valid multiple).
    out["P/FCF"] = np.nan
    if (
        row_ttm
        and sh is not None
        and not np.isnan(sh)
        and float(sh) > 0
        and fcf_ttm is not None
        and not np.isnan(fcf_ttm)
        and float(fcf_ttm) > 0
        and price is not None
        and not np.isnan(price)
    ):
        out["P/FCF"] = safe_div(price, safe_div(fcf_ttm, sh))

    out["Market Cap"] = price * sh if sh and not np.isnan(sh) else np.nan
    mc = out["Market Cap"]
    ev = mc + debt_l - cash_l if not np.isnan(mc) and debt_l is not None and cash_l is not None else np.nan
    out["Enterprise Value(EV)"] = ev
    # EBITDA operating basis (strict): if missing, EV/EBITDA must remain NaN.
    out["EBITDA (Reported TTM)"] = ebitda_reported_ttm
    out["EBITDA (Operating TTM)"] = ebitda_operating_ttm
    out["Reconciled Depreciation (TTM)"] = reconciled_dep_ttm

    # EV multiples: require strictly positive EV and EBITDA bases; negative or non-positive bases are not meaningful, not "cheap".
    try:
        ev_f = float(ev) if ev is not None else float("nan")
    except (TypeError, ValueError):
        ev_f = float("nan")
    ev_pos = not np.isnan(ev_f) and ev_f > 0
    ebitda_oper_ok = (
        ebitda_operating_ttm is not None
        and not (isinstance(ebitda_operating_ttm, (int, float)) and np.isnan(ebitda_operating_ttm))
        and float(ebitda_operating_ttm) > 0
    )
    ebitda_rep_ok = (
        ebitda_reported_ttm is not None
        and not (isinstance(ebitda_reported_ttm, (int, float)) and np.isnan(ebitda_reported_ttm))
        and float(ebitda_reported_ttm) > 0
    )

    out["EV/EBITDA Source"] = (
        "operating" if (row_ttm and ev_pos and ebitda_oper_ok) else "missing_operating_ebitda"
    )
    out["EV/EBITDA"] = (
        float(ev) / float(ebitda_operating_ttm) if (row_ttm and ev_pos and ebitda_oper_ok) else np.nan
    )
    out["EV/EBITDA (Reported)"] = (
        float(ev) / float(ebitda_reported_ttm) if (row_ttm and ev_pos and ebitda_rep_ok) else np.nan
    )
    out["EV/Sales"] = safe_div(ev, rev_ttm) if row_ttm and ev is not None else np.nan

    out["Quick Ratio"] = safe_div(cash_l + rec_l, cl_l) if cl_l else np.nan
    out["Current Ratio"] = safe_div(ca_l, cl_l) if cl_l else np.nan
    out["Debt/Eq"] = safe_div(debt_l, eq_l) if eq_l else np.nan
    out["LT Debt/Eq"] = safe_div(ltd_l, eq_l) if eq_l else np.nan

    out["ROA"] = safe_div(ni_ttm, ta_l) if row_ttm and ta_l else np.nan
    out["ROE"] = safe_div(ni_ttm, eq_l) if row_ttm and eq_l else np.nan

    # ROIC = NOPAT_TTM / average operating invested capital (excess cash only + denominator floor; see compute_operating_invested_capital).
    # Denominator: mean(latest quarter IC, prior quarter IC) when both exist; else latest only. TTM revenue drives buffer for both.
    out["ROIC"] = np.nan
    out["ROIC NOPAT TTM"] = np.nan
    out["ROIC IC Latest"] = np.nan
    out["ROIC IC Prev"] = np.nan
    out["ROIC IC Avg"] = np.nan
    out["ROIC Cash Buffer Latest"] = np.nan
    out["ROIC Cash Buffer Prev"] = np.nan
    out["ROIC Excess Cash Latest"] = np.nan
    out["ROIC Excess Cash Prev"] = np.nan
    out["ROIC Calc Source"] = ""

    def _roic_finance_arg(x: Any) -> float | None:
        if x is None:
            return None
        try:
            v = float(x)
        except (TypeError, ValueError):
            return None
        return v if math.isfinite(v) else None

    if row_ttm and row_latest:
        ebit_ttm = _float_or_nan(row_ttm.get("operatingIncome"))
        ibt_ttm = _float_or_nan(row_ttm.get("incomeBeforeTax"))
        tax_exp_ttm = _float_or_nan(row_ttm.get("incomeTaxExpense"))
        rev_ttm_roic = _float_or_nan(row_ttm.get("revenue"))
        # tax_rate: IBT 규모가 작거나 부호 이상 시 0.21; else clamp(abs(tax_exp)/abs(ibt), 0.05, 0.35)
        tax_rate = 0.21
        if rev_ttm_roic is not None and not np.isnan(rev_ttm_roic) and rev_ttm_roic != 0:
            if ibt_ttm is None or np.isnan(ibt_ttm) or abs(ibt_ttm) < rev_ttm_roic * 0.01:
                tax_rate = 0.21
            elif ibt_ttm != 0 and tax_exp_ttm is not None and not np.isnan(tax_exp_ttm):
                raw_tr = abs(float(tax_exp_ttm)) / abs(float(ibt_ttm))
                tax_rate = max(0.05, min(0.35, raw_tr))
        nopat_ttm = np.nan
        if ebit_ttm is not None and not np.isnan(ebit_ttm):
            nopat_ttm = float(ebit_ttm) * (1.0 - float(tax_rate))
        out["ROIC NOPAT TTM"] = nopat_ttm

        ic_latest, buf_l, exc_l, _floor_l = compute_operating_invested_capital(
            _roic_finance_arg(debt_l),
            _roic_finance_arg(eq_l),
            _roic_finance_arg(cash_l),
            _roic_finance_arg(ta_l),
            _roic_finance_arg(rev_ttm_roic),
        )
        out["ROIC IC Latest"] = float(ic_latest) if ic_latest is not None else np.nan
        out["ROIC Cash Buffer Latest"] = float(buf_l) if buf_l is not None else np.nan
        out["ROIC Excess Cash Latest"] = float(exc_l) if exc_l is not None else np.nan

        ic_prev_val: float | None = None
        buf_p: float | None = None
        exc_p: float | None = None
        if row_prev_quarter is not None:
            debt_p = _float_or_nan(row_prev_quarter.get("totalDebt"))
            eq_p = _float_or_nan(row_prev_quarter.get("totalStockholdersEquity"))
            cash_p = _float_or_nan(row_prev_quarter.get("cashAndCashEquivalents"))
            ta_p = _float_or_nan(row_prev_quarter.get("totalAssets"))
            ic_prev_val, buf_p, exc_p, _floor_p = compute_operating_invested_capital(
                _roic_finance_arg(debt_p),
                _roic_finance_arg(eq_p),
                _roic_finance_arg(cash_p),
                _roic_finance_arg(ta_p),
                _roic_finance_arg(rev_ttm_roic),
            )
            out["ROIC IC Prev"] = float(ic_prev_val) if ic_prev_val is not None else np.nan
            out["ROIC Cash Buffer Prev"] = float(buf_p) if buf_p is not None else np.nan
            out["ROIC Excess Cash Prev"] = float(exc_p) if exc_p is not None else np.nan

        ic_avg: float | None = None
        roic_src = ""
        if ic_latest is not None and ic_prev_val is not None:
            ic_avg = (float(ic_latest) + float(ic_prev_val)) / 2.0
            roic_src = "operating_ic_avg"
        elif ic_latest is not None:
            ic_avg = float(ic_latest)
            roic_src = "operating_ic_latest_only"
        else:
            roic_src = "missing_ic"

        out["ROIC IC Avg"] = float(ic_avg) if ic_avg is not None and math.isfinite(ic_avg) else np.nan

        if nopat_ttm is None or (isinstance(nopat_ttm, float) and np.isnan(nopat_ttm)):
            out["ROIC"] = np.nan
            out["ROIC Calc Source"] = "missing_nopat"
        elif ic_avg is None or (isinstance(ic_avg, float) and (not math.isfinite(ic_avg) or np.isnan(ic_avg))):
            out["ROIC"] = np.nan
            out["ROIC Calc Source"] = "missing_ic"
        elif float(ic_avg) <= 0.0:
            out["ROIC"] = np.nan
            out["ROIC Calc Source"] = "nonpositive_ic"
        else:
            out["ROIC"] = float(nopat_ttm) / float(ic_avg)
            out["ROIC Calc Source"] = roic_src

    # Finviz: margins from TTM (grossProfit_ttm/revenue_ttm etc.); fallback to latest quarter
    out["Gross Margin"] = safe_div(gp_ttm, rev_ttm) if row_ttm else (safe_div(gp_l, rev_l) if rev_l else np.nan)
    out["Oper. Margin"] = safe_div(oi_ttm, rev_ttm) if row_ttm else (safe_div(oi_l, rev_l) if rev_l else np.nan)
    out["Profit Margin"] = safe_div(ni_ttm, rev_ttm) if row_ttm else (safe_div(ni_l, rev_l) if rev_l else np.nan)
    return out


# -----------------------------------------------------------------------------
# DEPRECATED legacy latest-only factor map helpers
# -----------------------------------------------------------------------------
# These build symbol->value maps from global latest data and are NOT used by
# the current PIT row-builder execution path.


# DEPRECATED: legacy latest-only helper, not used by PIT engine main path.
def build_revenue_yoy_map(financials: pd.DataFrame) -> Dict[str, float]:
    """Revenue YoY = (latest TTM revenue / prior-year TTM revenue) - 1. Requires at least 8 quarterly rows per symbol."""
    out: Dict[str, float] = {}
    if financials.empty or "symbol" not in financials.columns or "fiscalDate" not in financials.columns or "revenue" not in financials.columns:
        return out
    fin = financials.copy()
    fin["symbol"] = fin["symbol"].astype(str).str.strip().str.upper()
    fin["fiscalDate"] = pd.to_datetime(fin["fiscalDate"], errors="coerce")
    fin = fin.dropna(subset=["fiscalDate"])
    fin["revenue"] = pd.to_numeric(fin["revenue"], errors="coerce")
    for sym, g in fin.groupby("symbol"):
        g = g.sort_values("fiscalDate", ascending=False).reset_index(drop=True)
        if len(g) < 8:
            continue
        latest_4 = pd.to_numeric(g["revenue"].iloc[:4], errors="coerce")
        prev_4 = pd.to_numeric(g["revenue"].iloc[4:8], errors="coerce")
        if latest_4.notna().sum() < 4 or prev_4.notna().sum() < 4:
            continue
        rev_latest_4 = latest_4.sum()
        rev_prev_4 = prev_4.sum()
        if pd.isna(rev_prev_4) or rev_prev_4 <= 0:
            continue
        if pd.isna(rev_latest_4):
            continue
        try:
            out[sym] = float(rev_latest_4) / float(rev_prev_4) - 1.0
        except (TypeError, ValueError, ZeroDivisionError):
            pass
    return out


# DEPRECATED: legacy latest-only helper, not used by PIT engine main path.
def build_ocf_yoy_map(financials: pd.DataFrame) -> Dict[str, float]:
    """OCF YoY = (latest TTM OCF / prior-year TTM OCF) - 1. Requires at least 8 quarterly rows; each 4Q block must have 4 valid operatingCashFlow values."""
    out: Dict[str, float] = {}
    if financials.empty or "symbol" not in financials.columns or "fiscalDate" not in financials.columns or "operatingCashFlow" not in financials.columns:
        return out
    fin = financials.copy()
    fin["symbol"] = fin["symbol"].astype(str).str.strip().str.upper()
    fin["fiscalDate"] = pd.to_datetime(fin["fiscalDate"], errors="coerce")
    fin = fin.dropna(subset=["fiscalDate"])
    fin["operatingCashFlow"] = pd.to_numeric(fin["operatingCashFlow"], errors="coerce")
    for sym, g in fin.groupby("symbol"):
        g = g.sort_values("fiscalDate", ascending=False).reset_index(drop=True)
        if len(g) < 8:
            continue
        latest_4 = pd.to_numeric(g["operatingCashFlow"].iloc[:4], errors="coerce")
        prev_4 = pd.to_numeric(g["operatingCashFlow"].iloc[4:8], errors="coerce")
        if latest_4.notna().sum() < 4 or prev_4.notna().sum() < 4:
            continue
        ocf_latest_4 = latest_4.sum()
        ocf_prev_4 = prev_4.sum()
        if pd.isna(ocf_prev_4) or ocf_prev_4 == 0:
            continue
        if pd.isna(ocf_latest_4):
            continue
        try:
            out[sym] = float(ocf_latest_4) / float(ocf_prev_4) - 1.0
        except (TypeError, ValueError, ZeroDivisionError):
            pass
    return out


# DEPRECATED: legacy latest-only helper, not used by PIT engine main path.
def build_ocf_ni_map(financials: pd.DataFrame) -> Dict[str, float]:
    """OCF/NI = latest TTM operatingCashFlow / latest TTM netIncome. Requires 4 quarters with 4 valid OCF and 4 valid NI."""
    out: Dict[str, float] = {}
    if financials.empty or "symbol" not in financials.columns or "fiscalDate" not in financials.columns:
        return out
    if "operatingCashFlow" not in financials.columns or "netIncome" not in financials.columns:
        return out
    fin = financials.copy()
    fin["symbol"] = fin["symbol"].astype(str).str.strip().str.upper()
    fin["fiscalDate"] = pd.to_datetime(fin["fiscalDate"], errors="coerce")
    fin = fin.dropna(subset=["fiscalDate"])
    fin["operatingCashFlow"] = pd.to_numeric(fin["operatingCashFlow"], errors="coerce")
    fin["netIncome"] = pd.to_numeric(fin["netIncome"], errors="coerce")
    for sym, g in fin.groupby("symbol"):
        g = g.sort_values("fiscalDate", ascending=False).reset_index(drop=True)
        if len(g) < 4:
            continue
        ocf_4 = pd.to_numeric(g["operatingCashFlow"].iloc[:4], errors="coerce")
        ni_4 = pd.to_numeric(g["netIncome"].iloc[:4], errors="coerce")
        if ocf_4.notna().sum() < 4 or ni_4.notna().sum() < 4:
            continue
        ocf_ttm = ocf_4.sum()
        ni_ttm = ni_4.sum()
        if pd.isna(ni_ttm) or ni_ttm == 0:
            continue
        if pd.isna(ocf_ttm):
            continue
        try:
            out[sym] = float(ocf_ttm) / float(ni_ttm)
        except (TypeError, ValueError, ZeroDivisionError):
            pass
    return out


# DEPRECATED: legacy latest-only helper, not used by PIT engine main path.
def build_eps_yoy_map(financials: pd.DataFrame) -> Dict[str, float]:
    """EPS YoY = (latest EPS(TTM) / EPS(TTM) ~1 year earlier) - 1. Reuses build_eps_ttm_series and pick_eps_ttm_at_or_near."""
    out: Dict[str, float] = {}
    eps_series = build_eps_ttm_series(financials)
    if not eps_series:
        return out
    for sym, series_df in eps_series.items():
        if series_df is None or series_df.empty or "fiscalDate" not in series_df.columns or "eps_ttm" not in series_df.columns:
            continue
        series_df = series_df.sort_values("fiscalDate").reset_index(drop=True)
        if series_df.empty:
            continue
        eps_latest = _float_or_nan(series_df.iloc[-1]["eps_ttm"])
        latest_fd = pd.to_datetime(series_df.iloc[-1]["fiscalDate"], errors="coerce")
        if eps_latest is None or np.isnan(eps_latest) or pd.isna(latest_fd):
            continue
        target_1y = latest_fd - pd.DateOffset(days=365)
        eps_prev = pick_eps_ttm_at_or_near(series_df, target_1y, tolerance_days=180)
        if eps_prev is None or np.isnan(eps_prev) or eps_prev == 0:
            continue
        try:
            out[sym] = float(eps_latest) / float(eps_prev) - 1.0
        except (TypeError, ValueError, ZeroDivisionError):
            pass
    return out


def build_shares_history_lookup(shares: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """Per-symbol DataFrame of share snapshots sorted by asOfDate descending (newest first)."""
    out: Dict[str, pd.DataFrame] = {}
    if shares.empty or "symbol" not in shares.columns or "asOfDate" not in shares.columns:
        return out
    df = shares.copy()
    df["symbol"] = df["symbol"].astype(str).str.strip().str.upper()
    df["asOfDate"] = pd.to_datetime(df["asOfDate"], errors="coerce")
    df = df.dropna(subset=["asOfDate"])
    if "sharesOutstanding" not in df.columns:
        return out
    df["sharesOutstanding"] = pd.to_numeric(df["sharesOutstanding"], errors="coerce")
    for sym, g in df.groupby("symbol"):
        g = g.sort_values("asOfDate", ascending=False).reset_index(drop=True)
        out[sym] = g
    return out


def get_shares_at_or_before(sym: str, target_date: str, shares_lookup: Dict[str, pd.DataFrame]) -> float:
    """Latest sharesOutstanding at or before target_date (YYYY-MM-DD). Lookup is per-symbol DF sorted asOfDate desc."""
    if not sym:
        return np.nan
    sym = str(sym).strip().upper()
    g = shares_lookup.get(sym)
    if g is None or g.empty:
        return np.nan
    try:
        tgt = pd.Timestamp(target_date)
    except Exception:
        return np.nan
    dates = pd.to_datetime(g["asOfDate"], errors="coerce")
    g = g.loc[dates <= tgt]
    if g.empty:
        return np.nan
    row = g.iloc[0]
    return _float_or_nan(row.get("sharesOutstanding"))


def get_shares_near_past(
    sym: str, target_date: str, shares_lookup: Dict[str, pd.DataFrame], lookback_days: int = 365
) -> float:
    """Shares outstanding at snapshot nearest to (target_date - lookback_days), using only snapshots on or before that date; allow within 120 days."""
    if not sym:
        return np.nan
    sym = str(sym).strip().upper()
    g = shares_lookup.get(sym)
    if g is None or g.empty:
        return np.nan
    try:
        tgt = pd.Timestamp(target_date) - pd.DateOffset(days=lookback_days)
    except Exception:
        return np.nan
    dates = pd.to_datetime(g["asOfDate"], errors="coerce")
    g = g.copy()
    g["_dt"] = dates
    g = g.dropna(subset=["_dt"])
    if g.empty:
        return np.nan
    past = g.loc[g["_dt"] <= tgt]
    if past.empty:
        return np.nan
    diffs = (past["_dt"] - tgt).abs().dt.days
    idx = diffs.idxmin()
    if pd.isna(diffs.loc[idx]) or diffs.loc[idx] > 120:
        return np.nan
    return _float_or_nan(past.loc[idx, "sharesOutstanding"])


# DEPRECATED: legacy latest-only helper, not used by PIT engine main path.
def build_share_dilution_map(shares: pd.DataFrame, latest_price_map: pd.Series) -> Dict[str, float]:
    """Share Dilution = (Shares_now / Shares_1y_ago) - 1. Uses shares at/before price_date and near price_date - 365."""
    out: Dict[str, float] = {}
    lookup = build_shares_history_lookup(shares)
    if not lookup:
        return out
    for sym in latest_price_map.index:
        price_date = latest_price_map.get(sym)
        if not price_date:
            continue
        price_date_str = str(price_date)[:10]
        shares_now = get_shares_at_or_before(sym, price_date_str, lookup)
        shares_1y = get_shares_near_past(sym, price_date_str, lookup, lookback_days=365)
        if shares_1y is None or np.isnan(shares_1y) or shares_1y == 0:
            continue
        if shares_now is None or np.isnan(shares_now):
            continue
        try:
            out[sym] = float(shares_now) / float(shares_1y) - 1.0
        except (TypeError, ValueError, ZeroDivisionError):
            pass
    return out


def detect_interest_expense_column(financials: pd.DataFrame) -> Optional[str]:
    """Return first present column among known interest-expense-like column names."""
    if financials is None or financials.empty:
        return None
    candidates = [
        "interestExpense",
        "interestAndDebtExpense",
        "interestExpenseNonOperating",
        "interestExpenseNet",
        "netInterestExpense",
        "interestExpenseTotal",
    ]
    for c in candidates:
        if c in financials.columns:
            return c
    return None


# DEPRECATED: legacy latest-only helper, not used by PIT engine main path.
def build_interest_coverage_map(financials: pd.DataFrame) -> Dict[str, float]:
    """Interest Coverage = operatingIncome_ttm / abs(interestExpense_ttm). Fallback: interest_ttm = operatingIncome_ttm - incomeBeforeTax_ttm (approximation; includes possible non-interest non-operating items) if no direct interest column."""
    out: Dict[str, float] = {}
    if financials.empty or "symbol" not in financials.columns or "fiscalDate" not in financials.columns:
        return out
    interest_col = detect_interest_expense_column(financials)
    use_fallback = interest_col is None
    if use_fallback:
        log.info(
            "Interest Coverage: no direct interest expense column in quarterly financials; "
            "using fallback (operatingIncome_ttm - incomeBeforeTax_ttm; approximation may include non-interest items)"
        )
    fin = financials.copy()
    fin["symbol"] = fin["symbol"].astype(str).str.strip().str.upper()
    fin["fiscalDate"] = pd.to_datetime(fin["fiscalDate"], errors="coerce")
    fin = fin.dropna(subset=["fiscalDate"])
    fin_desc = fin.sort_values(["symbol", "fiscalDate"], ascending=[True, False])
    for sym, g in fin_desc.groupby("symbol"):
        g4 = g.head(4)
        if len(g4) < 4:
            continue
        oi_ttm = pd.to_numeric(g4["operatingIncome"], errors="coerce").sum(min_count=1) if "operatingIncome" in g4.columns else np.nan
        if pd.isna(oi_ttm):
            continue
        if use_fallback:
            # Approximation: EBIT - EBT can include non-interest non-operating items; use only when no direct interest column.
            ibt_ttm = pd.to_numeric(g4["incomeBeforeTax"], errors="coerce").sum(min_count=1) if "incomeBeforeTax" in g4.columns else np.nan
            interest_ttm = oi_ttm - ibt_ttm if not pd.isna(ibt_ttm) else np.nan
        else:
            interest_ttm = pd.to_numeric(g4[interest_col], errors="coerce").sum(min_count=1)
        if pd.isna(interest_ttm) or abs(interest_ttm) < 1e-12:
            continue
        try:
            out[sym] = float(oi_ttm) / abs(float(interest_ttm))
        except (TypeError, ValueError, ZeroDivisionError):
            pass
    return out


# DEPRECATED: legacy latest-only helper, not used by PIT engine main path.
def build_opm_volatility_map(
    financials: pd.DataFrame, window_quarters: int = 8, min_quarters: int = 4
) -> Dict[str, float]:
    """OPM volatility = std(quarterly operating margin) over most recent valid quarters (up to 8, minimum 4)."""
    out: Dict[str, float] = {}
    if financials.empty or "symbol" not in financials.columns or "fiscalDate" not in financials.columns:
        return out
    if "operatingIncome" not in financials.columns or "revenue" not in financials.columns:
        return out
    fin = financials.copy()
    fin["symbol"] = fin["symbol"].astype(str).str.strip().str.upper()
    fin["fiscalDate"] = pd.to_datetime(fin["fiscalDate"], errors="coerce")
    fin = fin.dropna(subset=["fiscalDate"])
    fin["operatingIncome"] = pd.to_numeric(fin["operatingIncome"], errors="coerce")
    fin["revenue"] = pd.to_numeric(fin["revenue"], errors="coerce")
    for sym, g in fin.groupby("symbol"):
        g = g.sort_values("fiscalDate", ascending=False).reset_index(drop=True)
        g = g.head(window_quarters)
        g = g.loc[g["revenue"].notna() & (g["revenue"] != 0)]
        if len(g) < min_quarters:
            continue
        opm = g["operatingIncome"] / g["revenue"]
        opm = opm.replace([np.inf, -np.inf], np.nan).dropna()
        if len(opm) < min_quarters:
            continue
        try:
            out[sym] = float(opm.std())
        except (TypeError, ValueError):
            pass
    return out


# -----------------------------------------------------------------------------
# Output columns order
# -----------------------------------------------------------------------------

OUTPUT_COLUMNS = [
    "asOfDate",
    "symbol",
    "price_date",
    "price_source_mode",
    "data_cutoff_date",
    "financials_date",
    "financials_public_date",
    "shares_public_date",
    "estimates_public_date",
    "target_public_date",
    "company_facts_public_date",
    "index_public_date",
    "insider_holdings_public_date",
    "Price", "Prev Close", "Change", "Volume", "Avg Volume", "Rel Volume",
    "SMA20", "SMA50", "SMA200",
    "52W High", "52W Low",
    "Volatility", "ATR(14)", "RSI(14)",
    "Perf Week", "Perf Month", "Perf Quarter", "Perf Half Y", "Perf Year",
    "Perf 3Y", "Perf 5Y", "Perf 10Y", "Perf YTD",
    "Beta",
    "Income (Net)", "Sales (Rev)", "Revenue YoY", "OCF YoY", "Book/sh", "Cash/sh",
    "Dividend TTM",
    "EBITDA (Reported TTM)", "EBITDA (Operating TTM)", "Reconciled Depreciation (TTM)",
    "Payout", "Employees", "IPO (Date)",
    "EPS (ttm)", "EPS YoY", "P/E", "P/S", "P/B", "P/C", "P/FCF",
    "Market Cap",
    "Enterprise Value(EV)",
    "EV/EBITDA",
    "EV/EBITDA (Reported)",
    "EV/EBITDA Source",
    "EV/Sales",
    "Quick Ratio", "Current Ratio", "Debt/Eq", "LT Debt/Eq", "Interest Coverage",
    "ROA", "ROE", "ROIC",
    "ROIC NOPAT TTM",
    "ROIC IC Latest",
    "ROIC IC Prev",
    "ROIC IC Avg",
    "ROIC Cash Buffer Latest",
    "ROIC Cash Buffer Prev",
    "ROIC Excess Cash Latest",
    "ROIC Excess Cash Prev",
    "ROIC Calc Source",
    "Gross Margin", "Oper. Margin", "Profit Margin", "OCF/NI", "OPM volatility",
    "Shs Outstand", "Share Dilution", "Shs Float",
    "Earnings (Date)", "Forward P/E", "PEG", "Dividend Est", "Dividend Gr. 3Y", "Dividend Gr. 5Y", "Dividend Ex-Date",
    "EPS This Y",
    "EPS This Y Est Level",
    "EPS Next Y Est Level",
    "EPS This Y Base Actual",
    "EPS This Y Calc Source",
    "EPS Next Y",
    "EPS Next Q",
    "EPS Next 5Y",
    "Insider Own/Trans", "Inst Own/Trans",
    "Short Float", "Short Interest", "Short Ratio", "Recom",
    "Target Price", "Index",
    # PIT diagnostics (appended to avoid downstream schema breakage).
    "financials_public_date_source",
    "financials_used_fiscaldate_fallback",
]

# Unique key for snapshot accumulation: (asOfDate, symbol).
# Same key in a later run replaces the old row (update); new keys are appended (insert).
FACTORS_KEY_COLS = ["asOfDate", "symbol"]


def load_existing_factors(data_dir: Path, out_base: str, columns: List[str]) -> pd.DataFrame:
    """Load accumulated factors from disk for upsert. Prefer parquet, fallback to csv.
    Returns DF with at least `columns` (missing cols added as NaN); extra cols dropped.
    Normalizes symbol (str.upper().strip()) and asOfDate (YYYY-MM-DD)."""
    path_pq = data_dir / f"{out_base}.parquet"
    path_csv = data_dir / f"{out_base}.csv"
    if path_pq.exists():
        df = pd.read_parquet(path_pq)
    elif path_csv.exists():
        df = pd.read_csv(path_csv)
    else:
        return pd.DataFrame(columns=columns)
    if df.empty:
        return pd.DataFrame(columns=columns)
    for c in columns:
        if c not in df.columns:
            df[c] = np.nan
    df = df.reindex(columns=columns)
    if "symbol" in df.columns:
        df["symbol"] = df["symbol"].astype(str).str.strip().str.upper()
    for c in [
        "asOfDate",
        "price_date",
        "financials_date",
        "financials_public_date",
        "shares_public_date",
        "estimates_public_date",
        "target_public_date",
        "company_facts_public_date",
        "index_public_date",
        "insider_holdings_public_date",
        "data_cutoff_date",
    ]:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce").dt.strftime("%Y-%m-%d")
            df[c] = df[c].astype(str).replace("nan", "").replace("<NA>", "").str[:10]
    return df


def upsert_history(
    existing: pd.DataFrame,
    new_df: pd.DataFrame,
    key_cols: List[str],
    columns: List[str],
) -> pd.DataFrame:
    """Merge existing and new snapshot: (asOfDate, symbol) is unique; new overwrites existing.
    Concat existing then new_df, drop_duplicates(keep='last') so new wins. Reindex to columns, sort asOfDate asc, symbol asc."""
    for df in (existing, new_df):
        for c in columns:
            if c not in df.columns:
                df[c] = np.nan
    existing = existing.reindex(columns=[c for c in columns if c in existing.columns])
    new_df = new_df.reindex(columns=[c for c in columns if c in new_df.columns])
    combined = pd.concat([existing, new_df], ignore_index=True)
    combined = combined.drop_duplicates(subset=key_cols, keep="last")
    combined = combined.reindex(columns=[c for c in columns if c in combined.columns])
    combined = combined.sort_values(key_cols, ascending=[True, True]).reset_index(drop=True)
    return combined


def rebuild_latest_snapshot_from_history(
    history_df: pd.DataFrame,
    *,
    columns: List[str],
    symbol_col: str = "symbol",
    asof_col: str = "asOfDate",
) -> pd.DataFrame:
    """Build symbol-level latest snapshot from history rows."""
    if history_df is None or history_df.empty:
        return pd.DataFrame(columns=columns)

    df = history_df.copy()
    for c in columns:
        if c not in df.columns:
            df[c] = np.nan
    df = df.reindex(columns=[c for c in columns if c in df.columns])
    if symbol_col in df.columns:
        df[symbol_col] = df[symbol_col].astype(str).str.strip().str.upper()

    df["_asof_ts"] = pd.to_datetime(df.get(asof_col), errors="coerce")
    df["_price_ts"] = pd.to_datetime(df.get("price_date"), errors="coerce")
    df = df.sort_values(
        [symbol_col, "_asof_ts", "_price_ts"],
        ascending=[True, False, False],
    )
    latest_df = df.groupby(symbol_col, as_index=False).first()
    latest_df = latest_df.drop(columns=["_asof_ts", "_price_ts"], errors="ignore")
    latest_df = latest_df.reindex(columns=[c for c in columns if c in latest_df.columns])
    latest_df = latest_df.sort_values(symbol_col, ascending=True).reset_index(drop=True)
    return latest_df


# Backward compatibility alias (internal callers should use upsert_history).
def upsert_factors(
    existing: pd.DataFrame,
    new_df: pd.DataFrame,
    key_cols: List[str],
    columns: List[str],
) -> pd.DataFrame:
    return upsert_history(existing, new_df, key_cols, columns)


def validate_pit_leakage_rows(
    df: pd.DataFrame,
    *,
    mode: str = "warn",
) -> pd.DataFrame:
    """
    Validate PIT date constraints:
      - price_date <= asOfDate
      - *_public_date <= asOfDate

    Returns a violations dataframe with row-level details.
    In mode='error', raises ValueError when violations exist.
    """
    if df is None or df.empty:
        return pd.DataFrame(columns=["symbol", "asOfDate", "rule", "column", "value"])

    asof = pd.to_datetime(df.get("asOfDate"), errors="coerce")
    checks = [
        ("price_date", "price_date_le_asof"),
        ("financials_public_date", "financials_public_date_le_asof"),
        ("shares_public_date", "shares_public_date_le_asof"),
        ("estimates_public_date", "estimates_public_date_le_asof"),
        ("target_public_date", "target_public_date_le_asof"),
        ("company_facts_public_date", "company_facts_public_date_le_asof"),
        ("index_public_date", "index_public_date_le_asof"),
        ("insider_holdings_public_date", "insider_holdings_public_date_le_asof"),
    ]

    violations: List[Dict[str, Any]] = []
    for col, rule in checks:
        if col not in df.columns:
            continue
        val = pd.to_datetime(df[col], errors="coerce")
        mask = asof.notna() & val.notna() & (val > asof)
        if not mask.any():
            continue
        bad = df.loc[mask, ["symbol", "asOfDate", col]].copy()
        for _, r in bad.iterrows():
            violations.append(
                {
                    "symbol": r.get("symbol", ""),
                    "asOfDate": r.get("asOfDate", ""),
                    "rule": rule,
                    "column": col,
                    "value": r.get(col, ""),
                }
            )

    vdf = pd.DataFrame(violations, columns=["symbol", "asOfDate", "rule", "column", "value"])
    if not vdf.empty:
        sample = vdf.head(10).to_dict(orient="records")
        log.warning("PIT leakage validation violations: %s rows. sample=%s", len(vdf), sample)
        if str(mode).strip().lower() == "error":
            raise ValueError(f"PIT leakage validation failed with {len(vdf)} violations.")
    return vdf


def log_backfill_diagnostic_samples(
    df: pd.DataFrame,
    *,
    as_of_dates: List[str],
    sample_size: int = 5,
) -> None:
    """Log row samples for backfill diagnostics (per asOfDate)."""
    if df is None or df.empty or not as_of_dates:
        return
    log_cols = [
        "symbol",
        "asOfDate",
        "price_date",
        "financials_date",
        "financials_public_date",
        "financials_public_date_source",
        "financials_used_fiscaldate_fallback",
        "shares_public_date",
        "estimates_public_date",
        "target_public_date",
        "company_facts_public_date",
        "index_public_date",
        "insider_holdings_public_date",
    ]
    avail_cols = [c for c in log_cols if c in df.columns]
    if not avail_cols:
        return

    # If 2020-02-01 exists, always include it first for explicit leak-check example.
    ordered_dates = []
    if "2020-02-01" in as_of_dates:
        ordered_dates.append("2020-02-01")
    for d in as_of_dates:
        if d not in ordered_dates:
            ordered_dates.append(d)

    for d in ordered_dates[:5]:
        g = df.loc[df["asOfDate"] == d]
        if g.empty:
            continue
        sample = g.sort_values("symbol").head(sample_size)[avail_cols].to_dict(orient="records")
        log.info("Backfill diagnostics sample asOfDate=%s rows=%s sample=%s", d, len(g), sample)


def apply_financials_fallback_policy(
    df: pd.DataFrame,
    *,
    policy: str = "allow",
    max_ratio: float = 1.0,
) -> Tuple[pd.DataFrame, pd.DataFrame, Dict[str, Any]]:
    """
    Operate fallback usage policy for financials effective-date fallback rows.

    policy:
      - allow: keep rows, log stats
      - warn: keep rows, always warn when fallback exists
      - strict: drop fallback rows when fallback ratio exceeds max_ratio
    """
    if df is None or df.empty:
        empty = pd.DataFrame(columns=["asOfDate", "symbol", "financials_public_date_source"])
        return df, empty, {"total_rows": 0, "fallback_rows": 0, "fallback_ratio": 0.0, "fallback_symbols": 0}

    out = df.copy()
    if "financials_used_fiscaldate_fallback" not in out.columns:
        out["financials_used_fiscaldate_fallback"] = False

    fb_mask = out["financials_used_fiscaldate_fallback"].fillna(False).astype(bool)
    fb_rows = out.loc[fb_mask].copy()
    total_rows = int(len(out))
    fallback_rows = int(fb_mask.sum())
    fallback_ratio = float(fallback_rows / total_rows) if total_rows > 0 else 0.0
    fallback_symbols = int(fb_rows["symbol"].astype(str).nunique()) if ("symbol" in fb_rows.columns and not fb_rows.empty) else 0

    stats = {
        "total_rows": total_rows,
        "fallback_rows": fallback_rows,
        "fallback_ratio": fallback_ratio,
        "fallback_symbols": fallback_symbols,
    }
    return out, fb_rows, stats


def run_operational_self_checks(
    *,
    out_df: pd.DataFrame,
    history_df: pd.DataFrame,
    latest_df: pd.DataFrame,
    fallback_stats: Dict[str, Any],
    mode: str,
    prices: Optional[pd.DataFrame] = None,
    sample_as_of_date: Optional[str] = None,
) -> None:
    """
    Lightweight runtime checks for operational semantics.
    No external dependency, logging-only (non-breaking).
    """
    # 1) factors_latest must be one row per symbol.
    if latest_df is None or latest_df.empty or "symbol" not in latest_df.columns:
        log.warning("Self-check: latest snapshot is empty or missing symbol column.")
    else:
        dup_cnt = int(latest_df["symbol"].astype(str).duplicated().sum())
        if dup_cnt > 0:
            log.error("Self-check FAILED: factors_latest has duplicate symbols: %s", dup_cnt)
        else:
            log.info("Self-check PASS: factors_latest has one row per symbol (%s symbols).", len(latest_df))

    # 2) latest must be rebuildable from history deterministically.
    rebuilt_latest = rebuild_latest_snapshot_from_history(history_df, columns=OUTPUT_COLUMNS)
    same_shape = len(rebuilt_latest) == len(latest_df)
    same_symbols = (
        rebuilt_latest["symbol"].astype(str).tolist() == latest_df["symbol"].astype(str).tolist()
        if ("symbol" in rebuilt_latest.columns and "symbol" in latest_df.columns and same_shape)
        else False
    )
    if same_shape and same_symbols:
        log.info("Self-check PASS: latest snapshot rebuild from history is consistent.")
    else:
        log.warning(
            "Self-check WARNING: rebuild consistency mismatch (same_shape=%s same_symbols=%s).",
            same_shape,
            same_symbols,
        )

    # 3) fallback usage summary (operational KPI).
    log.info(
        "Self-check KPI: mode=%s fallback_rows=%s total_rows=%s fallback_ratio=%.4f fallback_symbols=%s",
        mode,
        fallback_stats.get("fallback_rows", 0),
        fallback_stats.get("total_rows", 0),
        float(fallback_stats.get("fallback_ratio", 0.0)),
        fallback_stats.get("fallback_symbols", 0),
    )

    log_price_series_quality_diagnostics(
        prices if prices is not None else pd.DataFrame(),
        mode=mode,
        sample_as_of_date=sample_as_of_date,
    )


def main() -> None:
    ap = argparse.ArgumentParser(description="Build factors_latest from Parquet inputs")
    ap.add_argument("--data-dir", default=DEFAULT_DATA_DIR, help="Input data directory")
    ap.add_argument("--out", default=DEFAULT_OUT, help="Output base name (factors_latest)")
    ap.add_argument("--out-history", default=None, help="History output base name (default: factors_history or <out>_history)")
    ap.add_argument("--out-latest", default=None, help="Latest snapshot output base name (default: factors_latest or <out>)")
    ap.add_argument("--index-symbol", default=INDEX_SYMBOL, help="Index for membership (e.g. SP500)")
    ap.add_argument(
        "--mode",
        choices=["latest", "backfill", "snapshot"],
        default="latest",
        help=(
            "latest: per-symbol latest trading-date update (default), "
            "snapshot: strict PIT snapshot at one --as-of-date for all symbols, "
            "backfill: PIT snapshots for an asOfDate schedule"
        ),
    )
    ap.add_argument("--as-of-date", default=None, help="Single asOfDate target (YYYY-MM-DD) for backfill mode")
    ap.add_argument("--start-date", default=None, help="Start asOfDate (YYYY-MM-DD) for backfill mode")
    ap.add_argument("--end-date", default=None, help="End asOfDate (YYYY-MM-DD) for backfill mode")
    ap.add_argument("--freq", default="monthly", choices=["monthly", "weekly", "daily"], help="Backfill cadence")
    ap.add_argument(
        "--output-history",
        default="false",
        help="If true, save backfill schedule (skeleton only) to disk",
    )
    ap.add_argument(
        "--output-diagnostics",
        default="false",
        help="If true, save PIT diagnostics outputs (rows + violations)",
    )
    ap.add_argument(
        "--leakage-validation-mode",
        choices=["warn", "error"],
        default="warn",
        help="PIT leakage validation behavior",
    )
    ap.add_argument(
        "--financials-fallback-policy",
        choices=["allow", "warn", "strict"],
        default="warn",
        help="Policy for rows using financials fiscalDate fallback",
    )
    ap.add_argument(
        "--financials-fallback-max-ratio",
        type=float,
        default=1.0,
        help="Max allowed fallback row ratio (0.0~1.0), used for warn/strict monitoring",
    )
    ap.add_argument(
        "--output-fallback-diagnostics",
        default="false",
        help="If true, save rows using financials fiscalDate fallback",
    )
    ap.add_argument("--sample-symbol", default=None, help="Diagnostic single symbol (e.g. AAPL)")
    ap.add_argument("--sample-as-of-date", default=None, help="Diagnostic single asOfDate (YYYY-MM-DD)")
    args = ap.parse_args()

    data_dir = Path(args.data_dir)
    out_base = args.out
    if args.out_history:
        out_history_base = str(args.out_history).strip()
    else:
        out_history_base = "factors_history" if out_base == DEFAULT_OUT else f"{out_base}_history"
    if args.out_latest:
        out_latest_base = str(args.out_latest).strip()
    else:
        out_latest_base = DEFAULT_OUT if out_base == DEFAULT_OUT else out_base
    index_symbol = args.index_symbol or INDEX_SYMBOL
    mode = str(getattr(args, "mode", "latest")).strip().lower()
    output_history = _parse_bool_arg(getattr(args, "output_history", "false"), default=False)
    output_diagnostics = _parse_bool_arg(getattr(args, "output_diagnostics", "false"), default=False)
    output_fallback_diagnostics = _parse_bool_arg(getattr(args, "output_fallback_diagnostics", "false"), default=False)
    leakage_validation_mode = str(getattr(args, "leakage_validation_mode", "warn")).strip().lower()
    financials_fallback_policy = str(getattr(args, "financials_fallback_policy", "warn")).strip().lower()
    financials_fallback_max_ratio = float(getattr(args, "financials_fallback_max_ratio", 1.0))
    sample_symbol = (
        str(getattr(args, "sample_symbol", "")).strip().upper()
        if getattr(args, "sample_symbol", None)
        else None
    )
    sample_as_of_date = str(getattr(args, "sample_as_of_date", "")).strip() if getattr(args, "sample_as_of_date", None) else None
    as_of_dates_schedule: Optional[List[str]] = None
    if financials_fallback_max_ratio < 0.0 or financials_fallback_max_ratio > 1.0:
        log.error("--financials-fallback-max-ratio must be between 0.0 and 1.0, got %s", financials_fallback_max_ratio)
        sys.exit(2)

    log.info("Loading parquets from %s", data_dir)
    # FIX: prices_eod.parquet 없으면 명확한 에러 로그 후 종료 (Parquet 전용, CSV fallback 금지)
    if not (data_dir / "prices_eod.parquet").exists():
        log.error("prices_eod.parquet가 없습니다. Parquet 전용이므로 종료합니다. data 디렉터리와 파일을 확인하세요.")
        sys.exit(1)
    prices = load_prices(data_dir)
    if mode == "latest":
        log.info("Running latest update using per-symbol latest trading date.")
    elif mode == "snapshot":
        # Strict snapshot schedule: one shared asOfDate for all symbols.
        if not getattr(args, "as_of_date", None):
            log.error("snapshot mode requires --as-of-date YYYY-MM-DD")
            sys.exit(2)
        snap_asof = str(args.as_of_date)
        if pd.isna(pd.to_datetime(snap_asof, errors="coerce")):
            log.error("Invalid --as-of-date for snapshot mode: %s", snap_asof)
            sys.exit(2)
        as_of_dates_schedule = [snap_asof]
        log.info("Running strict PIT snapshot at as_of_date=%s", snap_asof)
    elif mode == "backfill":
        # Backfill schedule (asOfDate list). Factor rows are computed for each asOfDate later.
        if getattr(args, "as_of_date", None):
            as_of_dates = [str(args.as_of_date)]
        else:
            if not getattr(args, "start_date", None) or not getattr(args, "end_date", None):
                log.error("backfill mode requires --as-of-date OR both --start-date and --end-date")
                sys.exit(2)
            as_of_dates = build_backfill_as_of_date_list(
                start_date=str(args.start_date),
                end_date=str(args.end_date),
                freq=str(args.freq),
            )

        if not as_of_dates:
            log.error("No valid backfill asOfDate targets were generated (check date formats and range).")
            sys.exit(2)
        as_of_dates_schedule = as_of_dates

        trading_days: List[str] = []
        if not prices.empty and "date" in prices.columns:
            trading_days = sorted(pd.Series(prices["date"].dropna().unique().tolist()).astype(str).tolist())

        sched_df = build_backfill_asof_schedule_skeleton(
            as_of_dates=as_of_dates,
            trading_days=trading_days,
        )
        log.info("Backfill schedule (skeleton) generated: %s rows", len(sched_df))
        log.info("Backfill schedule sample: %s", sched_df.head(5).to_dict(orient="records"))

        if output_history:
            out_sched_pq = data_dir / f"{out_history_base}_backfill_schedule.parquet"
            out_sched_csv = data_dir / f"{out_history_base}_backfill_schedule.csv"
            try:
                sched_df.to_parquet(out_sched_pq, index=False)
                sched_df.to_csv(out_sched_csv, index=False, encoding="utf-8-sig")
                log.info("Saved backfill schedule: %s", out_sched_pq)
            except Exception as e:
                log.warning("Failed to save backfill schedule: %s", e)
        # Continue into factor computation loop (PIT selection + row generation) below.
    else:
        log.error("Unknown mode: %s", mode)
        sys.exit(2)
    if sample_as_of_date:
        # Diagnostic path: force single asOfDate schedule for quick sanity checks.
        if pd.isna(pd.to_datetime(sample_as_of_date, errors="coerce")):
            log.error("Invalid --sample-as-of-date: %s", sample_as_of_date)
            sys.exit(2)
        as_of_dates_schedule = [sample_as_of_date]
        if mode != "backfill":
            log.info("Diagnostic single-date run: overriding to one asOfDate=%s", sample_as_of_date)

    if prices.empty:
        log.warning("prices_eod.parquet가 비어 있습니다. 처리할 심볼이 없습니다.")
        out_df = pd.DataFrame(columns=OUTPUT_COLUMNS)
    else:
        symbols = prices["symbol"].dropna().astype(str).str.strip().str.upper().unique().tolist()
        if sample_symbol:
            symbols = [s for s in symbols if s == sample_symbol]
            if not symbols:
                log.warning("sample symbol not found in prices: %s", sample_symbol)
        log.info("Symbols: %s", len(symbols))

        # FIX: sp500_prices.parquet 없으면 에러 로그 후 Beta NaN 유지; 있으면 로드 후 심볼 해석
        sp500_raw: pd.DataFrame
        if not (data_dir / "sp500_prices.parquet").exists():
            log.error("sp500_prices.parquet가 없습니다. Beta는 NaN으로 유지됩니다. 파일을 생성하거나 경로를 확인하세요.")
            sp500_raw = pd.DataFrame(columns=["symbol", "date", "close"])
        else:
            sp500_raw = load_sp500_prices(data_dir)
        # FIX: 로드 직후 품질 로그 (진단용)
        if not sp500_raw.empty:
            uniq = sp500_raw["symbol"].dropna().astype(str).str.strip().str.upper().unique() if "symbol" in sp500_raw.columns else []
            sample = list(uniq)[:10] if hasattr(uniq, "__iter__") else []
            date_min = sp500_raw["date"].min() if "date" in sp500_raw.columns else None
            date_max = sp500_raw["date"].max() if "date" in sp500_raw.columns else None
            close_nan_ratio = sp500_raw["close"].isna().mean() if "close" in sp500_raw.columns else None
            log.info(
                "sp500_prices 로드: rows=%s columns=%s symbols_sample=%s date_min=%s date_max=%s close_nan_ratio=%.2f",
                len(sp500_raw), list(sp500_raw.columns), sample, date_min, date_max, close_nan_ratio if close_nan_ratio is not None else float("nan"),
            )
        sp500_df = _resolve_sp500_market_df(sp500_raw)
        if sp500_df.empty or "close" not in sp500_df.columns:
            log.warning("sp500_prices에 ^GSPC(또는 후보 심볼) 데이터가 없어 Beta 계산이 스킵됩니다.")

        financials = load_financials(data_dir)
        # PIT effective-date interpretation (prevents look-ahead leakage into as_of_date snapshots).
        financials = interpret_financials_effective_date(financials)
        financials_by_symbol: Dict[str, pd.DataFrame] = {
            s: g.copy()
            for s, g in financials.groupby(financials["symbol"].astype(str).str.strip().str.upper(), sort=False)
        } if (financials is not None and not financials.empty and "symbol" in financials.columns) else {}
        dividends = load_dividends(data_dir)
        shares_df = load_shares(data_dir)
        index_df = load_index_membership(data_dir, index_symbol)
        targets_df = load_targets(data_dir)
        company_facts = load_company_facts(data_dir)
        insider_holdings_df = load_insider_holdings(data_dir)
        insider_transactions_df = load_insider_transactions(data_dir)
        est_a = load_estimates_snapshot(data_dir)
        est_q = load_estimates_quarterly_snapshot(data_dir)

        # Standardize effective/public date for all snapshot/input datasets used in PIT selection.
        shares_df = standardize_shares_snapshot_effective_date(shares_df)
        targets_df = standardize_targets_snapshot_effective_date(targets_df)
        company_facts = standardize_company_facts_snapshot_effective_date(company_facts)
        index_df = standardize_index_membership_effective_date(index_df)
        insider_holdings_df = standardize_insider_holdings_snapshot_effective_date(insider_holdings_df)
        est_a = standardize_estimates_snapshot_effective_date(est_a)
        est_q = standardize_estimates_quarterly_snapshot_effective_date(est_q)

        cf_lookup = build_company_facts_lookup(company_facts)
        holdings_by_sym = build_holdings_dedupe_and_totals(insider_holdings_df)
        tx_group = build_transactions_group(insider_transactions_df)

        interest_col = detect_interest_expense_column(financials)

        # PIT caches (effective_date <= as_of_date).
        shares_lookup = build_symbol_effective_date_lookup(shares_df, symbol_col="symbol")
        targets_lookup = build_symbol_effective_date_lookup(targets_df, symbol_col="symbol")
        index_lookup = build_symbol_effective_date_lookup(index_df, symbol_col="memberSymbol")
        est_a_lookup = build_symbol_effective_date_lookup(est_a, symbol_col="symbol")
        est_q_lookup = build_symbol_effective_date_lookup(est_q, symbol_col="symbol")
        empty_financials_df = financials.iloc[0:0].copy() if isinstance(financials, pd.DataFrame) else pd.DataFrame()

        # Cache per-symbol price series and trading dates for PIT backfill.
        # Keys are `sym` strings as they appear in `prices` (case-sensitive),
        # since we also pass those exact symbols into get_price_series_for_symbol.
        price_series_lookup: Dict[str, pd.DataFrame] = {}
        price_dates_lookup: Dict[str, List[str]] = {}
        min_price_date_lookup: Dict[str, str] = {}
        for _sym in symbols:
            ser = get_price_series_for_symbol(prices, _sym)
            if ser is None or ser.empty:
                continue
            price_series_lookup[_sym] = ser
            dlist = ser["date"].dropna().astype(str).tolist()
            price_dates_lookup[_sym] = dlist
            if dlist:
                min_price_date_lookup[_sym] = dlist[0]

        # Used for Share Dilution computed per (symbol, price_date) row.
        shares_history_lookup = build_shares_history_lookup(shares_df)

        # asOfDate = per-symbol price_date (계산 기준일)
        rows: List[Dict[str, Any]] = []
        g_fwd_list: List[float] = []
        g_hist_list: List[float] = []
        eps5y_list: List[float] = []
        peg_list: List[float] = []
        debug_eps_samples: List[Dict[str, Any]] = []
        debug_div_samples: List[Dict[str, Any]] = []
        skip_reasons: Dict[str, int] = {
            "missing_price_date": 0,
            "missing_price_series": 0,
            "empty_series_after_price_cutoff": 0,
            "nan_price": 0,
            "no_prior_trade_for_asof": 0,
        }
        processed_asof_dates: set[str] = set()
        diag_row_logged = False
        def build_factor_row_for_symbol_at(
            sym: str,
            as_of_date: str,
            price_date: str,
            price_source_mode: str,
        ) -> Optional[Dict[str, Any]]:
            if not price_date:
                skip_reasons["missing_price_date"] = skip_reasons.get("missing_price_date", 0) + 1
                return None

            series = price_series_lookup.get(sym)
            if series is None or series.empty:
                skip_reasons["missing_price_series"] = skip_reasons.get("missing_price_series", 0) + 1
                return None
            # Price-based indicators must not use observations after the cutoff date.
            series = series.loc[series["date"] <= price_date].copy()
            if series.empty:
                skip_reasons["empty_series_after_price_cutoff"] = skip_reasons.get("empty_series_after_price_cutoff", 0) + 1
                return None

            as_of = as_of_date
            processed_asof_dates.add(as_of)
            price_inds = compute_price_indicators(series, price_date, symbol=sym)
            # Beta: sp500_prices(^GSPC 또는 대체 심볼) 시장 수익률로 Finviz 근사 (5Y 월간 → 252일 일간 fallback)
            beta_val = np.nan
            try:
                if sp500_df is not None and not sp500_df.empty:
                    if "close_px" in series.columns:
                        stock_for_beta = series[["date", "close_px"]].rename(columns={"close_px": "close"})
                    else:
                        stock_for_beta = series[["date", "close"]].copy()
                    mkt_for_beta = sp500_df[["date", "close"]].copy()
                    beta_val = beta_finviz_style(stock_for_beta, mkt_for_beta, price_date)
            except Exception as e:
                log.warning("Beta 계산 실패 sym=%s price_date=%s: %s", sym, price_date, e)
                beta_val = np.nan
            price_inds["Beta"] = beta_val

            price = price_inds.get("Price", np.nan)
            if np.isnan(price):
                skip_reasons["nan_price"] = skip_reasons.get("nan_price", 0) + 1
                return None

            fin_sym_df = financials_by_symbol.get(sym, empty_financials_df)
            row_latest = get_latest_financial_snapshot_at(sym, as_of, fin_sym_df)
            row_prev = get_prev_financial_snapshot_at(sym, as_of, fin_sym_df)
            row_ttm = get_ttm_financials_at(sym, as_of, fin_sym_df)

            financials_date = row_latest.get("fiscalDate", np.nan) if row_latest else np.nan
            financials_public_date = row_latest.get("effective_date", np.nan) if row_latest else np.nan
            financials_public_date_source = row_latest.get("effective_date_source", np.nan) if row_latest else np.nan
            financials_used_fiscaldate_fallback = bool(financials_public_date_source == "fiscalDate_fallback")

            # Debug/public-date columns for snapshot inputs (PIT-safe).
            shares_public_date = np.nan
            estimates_public_date = np.nan
            target_public_date = np.nan
            company_facts_public_date = np.nan
            index_public_date = np.nan
            insider_holdings_public_date = np.nan

            # PIT financial factors derived from eligible quarterly pool.
            revenue_yoy_val = get_revenue_yoy_at(sym, as_of, fin_sym_df)
            ocf_yoy_val = get_ocf_yoy_at(sym, as_of, fin_sym_df)
            ocf_ni_val = get_ocf_ni_at(sym, as_of, fin_sym_df)
            interest_coverage_val = get_interest_coverage_at(
                sym, as_of, fin_sym_df, interest_col=interest_col
            )
            opm_volatility_val = get_opm_volatility_at(sym, as_of, fin_sym_df)
            eps_yoy_val = np.nan

            shares_out = np.nan
            shs_float = np.nan
            shares_row = select_latest_row_from_symbol_effective_lookup(
                shares_lookup,
                symbol=sym,
                as_of_date=as_of,
                effective_date_col="effective_date",
            )
            if shares_row is not None:
                shares_out = _float_or_nan(shares_row.get("sharesOutstanding"))
                shs_float = _float_or_nan(shares_row.get("sharesFloat"))
                shares_public_date = shares_row.get("effective_date", np.nan)
            else:
                shares_out = _float_or_nan(row_latest.get("sharesOutstanding")) if row_latest else np.nan
                shs_float = np.nan
                shares_public_date = financials_public_date if financials_public_date is not None else np.nan
            if (shares_out is None or (isinstance(shares_out, float) and np.isnan(shares_out))) and row_latest:
                shares_out = _float_or_nan(row_latest.get("sharesOutstanding"))
                if shares_public_date is None or (isinstance(shares_public_date, float) and np.isnan(shares_public_date)):
                    shares_public_date = financials_public_date

            div_ttm = dividend_ttm_for_symbol(dividends, sym, price_date, series["date"])
            div_ex_date = dividend_ex_date_for_symbol(dividends, sym, price_date)
            dividend_est = div_ttm

            # Dividend growth 3Y/5Y CAGR: div_now_1y vs div 3y/5y ago over 1y windows
            price_ts = pd.Timestamp(price_date)
            end_1y = price_ts
            start_1y = price_ts - pd.DateOffset(days=365)
            div_now_1y = dividend_sum_window(dividends, sym, start_1y.strftime("%Y-%m-%d"), end_1y.strftime("%Y-%m-%d"))
            end_3y = price_ts - pd.DateOffset(years=3)
            start_3y = end_3y - pd.DateOffset(days=365)
            div_3y_ago_1y = dividend_sum_window(dividends, sym, start_3y.strftime("%Y-%m-%d"), end_3y.strftime("%Y-%m-%d"))
            end_5y = price_ts - pd.DateOffset(years=5)
            start_5y = end_5y - pd.DateOffset(days=365)
            div_5y_ago_1y = dividend_sum_window(dividends, sym, start_5y.strftime("%Y-%m-%d"), end_5y.strftime("%Y-%m-%d"))
            gr3_pct = np.nan
            if (
                div_now_1y is not None and not np.isnan(div_now_1y) and div_now_1y > 0
                and div_3y_ago_1y is not None and not np.isnan(div_3y_ago_1y) and div_3y_ago_1y > 0
            ):
                try:
                    gr3_pct = ((float(div_now_1y) / float(div_3y_ago_1y)) ** (1.0 / 3.0) - 1.0) * 100.0
                    gr3_pct = max(-50.0, min(100.0, gr3_pct))
                except (ZeroDivisionError, ValueError):
                    gr3_pct = np.nan
            gr5_pct = np.nan
            if (
                div_now_1y is not None and not np.isnan(div_now_1y) and div_now_1y > 0
                and div_5y_ago_1y is not None and not np.isnan(div_5y_ago_1y) and div_5y_ago_1y > 0
            ):
                try:
                    gr5_pct = ((float(div_now_1y) / float(div_5y_ago_1y)) ** (1.0 / 5.0) - 1.0) * 100.0
                    gr5_pct = max(-50.0, min(100.0, gr5_pct))
                except (ZeroDivisionError, ValueError):
                    gr5_pct = np.nan

            fin_inds = build_financial_indicators(row_latest, row_ttm, shares_out, price, row_prev_quarter=row_prev)

            # Debug-only: print minimal factor row evidence around EV/EBITDA.
            try:
                do_diag = False
                if sample_symbol and sym == sample_symbol:
                    do_diag = True
                elif output_diagnostics and not diag_row_logged:
                    do_diag = True
                    diag_row_logged = True
                if do_diag:
                    log.info(
                        "[sample EV/EBITDA] symbol=%s asOfDate=%s EV=%s EBITDA_Reported_TTM=%s EBITDA_Operating_TTM=%s EV/EBITDA=%s EV/EBITDA (Reported)=%s EV/EBITDA Source=%s",
                        sym,
                        as_of_date,
                        fin_inds.get("Enterprise Value(EV)", np.nan),
                        fin_inds.get("EBITDA (Reported TTM)", np.nan),
                        fin_inds.get("EBITDA (Operating TTM)", np.nan),
                        fin_inds.get("EV/EBITDA", np.nan),
                        fin_inds.get("EV/EBITDA (Reported)", np.nan),
                        fin_inds.get("EV/EBITDA Source", np.nan),
                    )
            except Exception:
                pass
            # Finviz-style: Employees, IPO (Date) from company_facts (asOfDate/effective <= as_of_date)
            employees_val = get_employees_at(sym, as_of, cf_lookup)
            ipo_date_val = get_ipo_date_at(sym, as_of, cf_lookup)
            fin_inds["Employees"] = employees_val
            fin_inds["IPO (Date)"] = ipo_date_val

            # Finviz-style: Insider Own % (holdings_now/shares_out), Insider Trans % (holdings diff or net trans 90d)
            cf_at = get_company_facts_at(sym, as_of, cf_lookup)
            company_facts_public_date = cf_at.get("company_facts_public_date", np.nan)
            shares_out_insider = cf_at.get("shares_out")
            if shares_out_insider is None or (isinstance(shares_out_insider, (int, float)) and (np.isnan(shares_out_insider) or shares_out_insider <= 0)):
                shares_out_insider = float(shares_out) if (shares_out is not None and not np.isnan(shares_out) and shares_out > 0) else None
            holdings_now, insider_holdings_public_date = holdings_total_at_with_public_date(
                sym, as_of, holdings_by_sym
            )
            holdings_prev = holdings_prev_90d(sym, as_of, holdings_by_sym)
            net_trans = net_trans_shares_90d(sym, as_of, tx_group)
            own_pct = insider_own_pct_finviz(holdings_now, shares_out_insider)
            trans_pct = insider_trans_pct_finviz(holdings_now, holdings_prev, net_trans, shares_out_insider, prefer_holdings_diff=True)
            insider_own_trans_str = format_insider_own_trans(own_pct, trans_pct)

            target_p = np.nan
            target_row = select_latest_row_from_symbol_effective_lookup(
                targets_lookup,
                symbol=sym,
                as_of_date=as_of,
                effective_date_col="effective_date",
            )
            if target_row is not None:
                target_p = _float_or_nan(target_row.get("targetPrice"))
                target_public_date = target_row.get("effective_date", np.nan)
            if not isinstance(target_p, (int, float)):
                target_p = np.nan
            idx_val = np.nan
            idx_row = select_latest_row_from_symbol_effective_lookup(
                index_lookup,
                symbol=sym,
                as_of_date=as_of,
                effective_date_col="effective_date",
            )
            if idx_row is not None:
                idx_val = idx_row.get("isMember", np.nan)
                index_public_date = idx_row.get("effective_date", np.nan)

            # EPS Next Y raw estimate level is preserved for valuation use, but growth scoring is disabled
            # until a normalized definition is added (see EPS Next Y Est Level; Forward P/E uses eps_next_y_est).
            # --- Estimates: annual EPS estimate levels (not growth %); growth derived below for EPS This Y ---
            eps_this_y_est = np.nan
            eps_next_y_est = np.nan
            eps_next_q = np.nan
            est_dates: list[str] = []
            ea_row = select_latest_row_from_symbol_effective_lookup(
                est_a_lookup,
                symbol=sym,
                as_of_date=as_of,
                effective_date_col="effective_date",
            )
            if ea_row is not None:
                eps_this_y_est = _float_or_nan(ea_row.get("epsThisY"))
                eps_next_y_est = _float_or_nan(ea_row.get("epsNextY"))
                est_dates.append(ea_row.get("effective_date", np.nan))
            eq_row = select_latest_row_from_symbol_effective_lookup(
                est_q_lookup,
                symbol=sym,
                as_of_date=as_of,
                effective_date_col="effective_date",
            )
            if eq_row is not None:
                eps_next_q = _float_or_nan(eq_row.get("epsNextQ"))
                est_dates.append(eq_row.get("effective_date", np.nan))
            est_dates = [d for d in est_dates if d is not None and not (isinstance(d, float) and np.isnan(d))]
            estimates_public_date = max(est_dates) if est_dates else np.nan

            # Single EPS(TTM) series for YoY, historical CAGR, and prior-FY base vs estimate level.
            series_eps = get_eps_ttm_series_at(sym, as_of, fin_sym_df)
            latest_fd_dt = pd.to_datetime(financials_date, errors="coerce") if financials_date is not None else pd.NaT
            latest_fd_anchor = None if pd.isna(latest_fd_dt) else latest_fd_dt
            prior_actual_eps = get_prior_fiscal_year_actual_eps_from_ttm_series(
                series_eps, latest_fd_anchor, tolerance_days=180
            )

            # Forward 1Y growth g_fwd from analyst estimate levels: next-year / this-year - 1 (not EPS This Y factor).
            g_fwd = np.nan
            if (
                eps_this_y_est is not None
                and not np.isnan(eps_this_y_est)
                and float(eps_this_y_est) > 0
                and eps_next_y_est is not None
                and not np.isnan(eps_next_y_est)
                and float(eps_next_y_est) > 0
            ):
                g_fwd = (float(eps_next_y_est) / float(eps_this_y_est)) - 1.0

            # EPS This Y (factor) = implied growth vs prior fiscal year actual (TTM near prior FY-end).
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
                        # Clip extreme implied YoY for stability (same spirit as OCF YoY caps).
                        eps_this_y_growth = float(np.clip(_g_ty, -0.95, 3.0))
                        eps_this_y_calc_source = "derived_from_estimate_and_prior_actual"
                except (TypeError, ValueError, ZeroDivisionError):
                    eps_this_y_growth = np.nan
                    eps_this_y_calc_source = "missing_prior_actual"

            # Historical EPS CAGR (5Y with 3Y fallback) from EPS(TTM) time series
            g_hist = np.nan
            if series_eps is not None and not series_eps.empty and not pd.isna(latest_fd_dt):
                # PIT EPS YoY (single source of truth): computed from eligible EPS(TTM) series only.
                eps_yoy_val = get_eps_yoy_from_eps_ttm_series(series_eps, tolerance_days=180)

                eps_now = pick_eps_ttm_at_or_near(series_eps, latest_fd_dt)
                if eps_now is not None and not np.isnan(eps_now) and eps_now > 0:
                    # 5Y CAGR
                    target_5y = latest_fd_dt - pd.DateOffset(years=5)
                    eps_past_5y = pick_eps_ttm_at_or_near(series_eps, target_5y)
                    g_hist_5 = np.nan
                    if eps_past_5y is not None and not np.isnan(eps_past_5y) and eps_past_5y > 0:
                        try:
                            g_hist_5 = (float(eps_now) / float(eps_past_5y)) ** (1.0 / 5.0) - 1.0
                        except (ZeroDivisionError, ValueError):
                            g_hist_5 = np.nan
                    # 3Y CAGR fallback
                    g_hist_3 = np.nan
                    if np.isnan(g_hist_5):
                        target_3y = latest_fd_dt - pd.DateOffset(years=3)
                        eps_past_3y = pick_eps_ttm_at_or_near(series_eps, target_3y)
                        if eps_past_3y is not None and not np.isnan(eps_past_3y) and eps_past_3y > 0:
                            try:
                                g_hist_3 = (float(eps_now) / float(eps_past_3y)) ** (1.0 / 3.0) - 1.0
                            except (ZeroDivisionError, ValueError):
                                g_hist_3 = np.nan
                    if not np.isnan(g_hist_5):
                        g_hist = g_hist_5
                    elif not np.isnan(g_hist_3):
                        g_hist = g_hist_3

            # Hybrid 5Y growth g_5y: combine forward and historical
            g_5y = np.nan
            has_fwd = g_fwd is not None and not np.isnan(g_fwd)
            has_hist = g_hist is not None and not np.isnan(g_hist)
            if has_fwd and has_hist:
                g_5y = 0.6 * float(g_fwd) + 0.4 * float(g_hist)
            elif has_fwd:
                g_5y = float(g_fwd)
            elif has_hist:
                g_5y = float(g_hist)

            # EPS Next 5Y percent, clamped to [-50, 100]
            eps_next_5y_pct = np.nan
            if g_5y is not None and not np.isnan(g_5y):
                eps_next_5y_pct = float(g_5y) * 100.0
                if eps_next_5y_pct < -50.0:
                    eps_next_5y_pct = -50.0
                if eps_next_5y_pct > 100.0:
                    eps_next_5y_pct = 100.0

            # Forward P/E: uses next-year analyst EPS estimate level (not EPS This Y growth).
            forward_pe = np.nan
            if (
                eps_next_y_est is not None
                and not np.isnan(eps_next_y_est)
                and float(eps_next_y_est) > 0
                and price is not None
                and not np.isnan(price)
            ):
                forward_pe = price / eps_next_y_est

            pe = fin_inds.get("P/E", np.nan)
            peg = np.nan
            if (
                pe is not None and not np.isnan(pe)
                and eps_next_5y_pct is not None and not np.isnan(eps_next_5y_pct) and eps_next_5y_pct > 0
            ):
                peg = float(pe) / float(eps_next_5y_pct)

            g_fwd_list.append(g_fwd)
            g_hist_list.append(g_hist)
            eps5y_list.append(eps_next_5y_pct)
            peg_list.append(peg)
            if len(debug_eps_samples) < 5:
                debug_eps_samples.append(
                    {
                        "symbol": sym,
                        "epsThisY_est": eps_this_y_est,
                        "epsNextY_est": eps_next_y_est,
                        "epsThisY_growth": eps_this_y_growth,
                        "g_fwd": g_fwd,
                        "g_hist": g_hist,
                        "epsNext5Y_pct": eps_next_5y_pct,
                        "pe": pe,
                        "peg": peg,
                    }
                )
            if len(debug_div_samples) < 5:
                debug_div_samples.append(
                    {
                        "symbol": sym,
                        "price_date": price_date,
                        "div_now_1y": div_now_1y,
                        "div_3y_ago_1y": div_3y_ago_1y,
                        "div_5y_ago_1y": div_5y_ago_1y,
                        "gr3": gr3_pct,
                        "gr5": gr5_pct,
                        "ex_date": div_ex_date,
                    }
                )

            # Share Dilution computed per snapshot (PIT-safe at row price_date).
            share_dilution = np.nan
            price_date_str = str(price_date)[:10]
            shares_now_sd = get_shares_at_or_before(sym, price_date_str, shares_history_lookup)
            shares_1y_sd = get_shares_near_past(
                sym,
                price_date_str,
                shares_history_lookup,
                lookback_days=365,
            )
            if (
                shares_now_sd is not None
                and not (isinstance(shares_now_sd, float) and np.isnan(shares_now_sd))
                and shares_1y_sd is not None
                and not (isinstance(shares_1y_sd, float) and np.isnan(shares_1y_sd))
                and shares_1y_sd != 0
            ):
                try:
                    share_dilution = float(shares_now_sd) / float(shares_1y_sd) - 1.0
                except (TypeError, ValueError, ZeroDivisionError):
                    share_dilution = np.nan

            row_out = {
                "asOfDate": as_of,
                "symbol": sym,
                "price_date": price_date,
                "price_source_mode": price_source_mode,
                "data_cutoff_date": as_of,
                "financials_date": financials_date if financials_date is not None else np.nan,
                "financials_public_date": financials_public_date if financials_public_date is not None else np.nan,
                "financials_public_date_source": financials_public_date_source,
                "financials_used_fiscaldate_fallback": financials_used_fiscaldate_fallback,
                "shares_public_date": shares_public_date,
                "estimates_public_date": estimates_public_date,
                "target_public_date": target_public_date,
                "company_facts_public_date": company_facts_public_date,
                "index_public_date": index_public_date,
                "insider_holdings_public_date": insider_holdings_public_date,
                **price_inds,
                "Dividend TTM": div_ttm,
                **fin_inds,
                "Revenue YoY": revenue_yoy_val,
                "OCF YoY": ocf_yoy_val,
                "OCF/NI": ocf_ni_val,
                "EPS YoY": eps_yoy_val,
                "Share Dilution": share_dilution,
                "Interest Coverage": interest_coverage_val,
                "OPM volatility": opm_volatility_val,
                "Shs Float": shs_float,
                "Target Price": target_p,
                "Index": idx_val,
                "Earnings (Date)": np.nan,
                "Forward P/E": forward_pe,
                "PEG": peg,
                "Dividend Est": dividend_est,
                "Dividend Gr. 3Y": gr3_pct,
                "Dividend Gr. 5Y": gr5_pct,
                "Dividend Ex-Date": div_ex_date,
                "EPS This Y": eps_this_y_growth
                if (eps_this_y_growth is not None and not (isinstance(eps_this_y_growth, float) and np.isnan(eps_this_y_growth)))
                else np.nan,
                "EPS This Y Est Level": eps_this_y_est
                if (eps_this_y_est is not None and not (isinstance(eps_this_y_est, float) and np.isnan(eps_this_y_est)))
                else np.nan,
                "EPS Next Y Est Level": eps_next_y_est
                if (eps_next_y_est is not None and not (isinstance(eps_next_y_est, float) and np.isnan(eps_next_y_est)))
                else np.nan,
                "EPS This Y Base Actual": float(prior_actual_eps)
                if prior_actual_eps is not None
                else np.nan,
                "EPS This Y Calc Source": eps_this_y_calc_source,
                # Legacy column left blank: canonical next-year estimate level is EPS Next Y Est Level (G factor disabled).
                "EPS Next Y": np.nan,
                "EPS Next Q": eps_next_q if (eps_next_q is not None and not np.isnan(eps_next_q)) else np.nan,
                "EPS Next 5Y": eps_next_5y_pct,
                "Insider Own/Trans": insider_own_trans_str,
                "Inst Own/Trans": np.nan,
                "Short Float": np.nan,
                "Short Interest": np.nan,
                "Short Ratio": np.nan,
                "Recom": np.nan,
            }
            rows.append(row_out)

        # Mode loop: latest-only vs backfill schedule.
        if mode == "latest":
            if sample_as_of_date:
                # Diagnostic single-date run in latest mode: reuse common builder with one asOfDate.
                for sym in symbols:
                    price_dates = price_dates_lookup.get(sym, [])
                    price_date, price_source_mode = select_price_date_for_symbol_at_or_before(
                        price_dates=price_dates,
                        requested_as_of_date=sample_as_of_date,
                    )
                    if not price_date:
                        skip_reasons["no_prior_trade_for_asof"] = skip_reasons.get("no_prior_trade_for_asof", 0) + 1
                        continue
                    build_factor_row_for_symbol_at(
                        sym=sym,
                        as_of_date=sample_as_of_date,
                        price_date=price_date,
                        price_source_mode=price_source_mode,
                    )
            else:
                latest_price_map = latest_price_date_per_symbol(prices)
                log.info("Computed latest price date map")
                for sym in symbols:
                    price_date = latest_price_map.get(sym)
                    if not price_date:
                        continue
                    build_factor_row_for_symbol_at(
                        sym=sym,
                        as_of_date=price_date,
                        price_date=price_date,
                        price_source_mode="latest_price_map",
                    )
        elif mode in {"backfill", "snapshot"}:
            if not as_of_dates_schedule:
                log.error("%s mode requires at least one asOfDate target.", mode)
                as_of_dates_schedule = []
            for as_of_date in as_of_dates_schedule:
                # Only consider symbols that have at least one price observation <= as_of_date.
                symbols_available_by_date = [
                    s for s in symbols if s in min_price_date_lookup and min_price_date_lookup[s] <= as_of_date
                ]
                for sym in symbols_available_by_date:
                    price_dates = price_dates_lookup.get(sym, [])
                    price_date, price_source_mode = select_price_date_for_symbol_at_or_before(
                        price_dates=price_dates,
                        requested_as_of_date=as_of_date,
                    )
                    if not price_date:
                        skip_reasons["no_prior_trade_for_asof"] = skip_reasons.get("no_prior_trade_for_asof", 0) + 1
                        continue
                    build_factor_row_for_symbol_at(
                        sym=sym,
                        as_of_date=as_of_date,
                        price_date=price_date,
                        price_source_mode=price_source_mode,
                    )
        out_df = pd.DataFrame(rows)
        for c in OUTPUT_COLUMNS:
            if c not in out_df.columns:
                out_df[c] = np.nan
        out_df = out_df.reindex(columns=[c for c in OUTPUT_COLUMNS if c in out_df.columns])
        out_df = out_df.sort_values(FACTORS_KEY_COLS, ascending=[True, True]).reset_index(drop=True)
        for c in out_df.select_dtypes(include=[np.number]).columns:
            out_df[c] = out_df[c].astype(float, errors="ignore")
        for c in [
            "asOfDate",
            "price_date",
            "financials_date",
            "financials_public_date",
            "shares_public_date",
            "estimates_public_date",
            "target_public_date",
            "company_facts_public_date",
            "index_public_date",
            "insider_holdings_public_date",
            "data_cutoff_date",
        ]:
            if c in out_df.columns:
                out_df[c] = out_df[c].astype(str).replace("nan", "").str[:10]

    log.info("New snapshot rows: %s", len(out_df))
    log.info(
        "Processing summary: mode=%s requested_asof_dates=%s processed_asof_dates=%s rows=%s",
        mode,
        len(as_of_dates_schedule) if as_of_dates_schedule else ("latest" if mode == "latest" else 0),
        len(processed_asof_dates) if "processed_asof_dates" in locals() else 0,
        len(out_df),
    )
    if "skip_reasons" in locals():
        skip_nonzero = {k: v for k, v in skip_reasons.items() if int(v) > 0}
        log.info("Skip reasons summary: %s", skip_nonzero if skip_nonzero else {"none": 0})

    # Financials fallback policy control/measurement.
    out_df, fallback_rows_df, fallback_stats = apply_financials_fallback_policy(
        out_df,
        policy=financials_fallback_policy,
        max_ratio=financials_fallback_max_ratio,
    )
    log.info(
        "Financials fallback usage: rows=%s/%s ratio=%.4f symbols=%s policy=%s max_ratio=%.4f",
        fallback_stats["fallback_rows"],
        fallback_stats["total_rows"],
        fallback_stats["fallback_ratio"],
        fallback_stats["fallback_symbols"],
        financials_fallback_policy,
        financials_fallback_max_ratio,
    )
    if fallback_stats["fallback_rows"] > 0:
        by_symbol = (
            fallback_rows_df["symbol"].astype(str).value_counts().head(20).to_dict()
            if "symbol" in fallback_rows_df.columns else {}
        )
        by_asof = (
            fallback_rows_df["asOfDate"].astype(str).value_counts().head(10).to_dict()
            if "asOfDate" in fallback_rows_df.columns else {}
        )
        if financials_fallback_policy == "allow":
            log.info("Financials fallback policy=allow. Top symbols=%s top asOfDate=%s", by_symbol, by_asof)
        elif financials_fallback_policy == "warn":
            log.warning("Financials fallback policy=warn. Top symbols=%s top asOfDate=%s", by_symbol, by_asof)

    if fallback_stats["fallback_ratio"] > financials_fallback_max_ratio:
        msg = (
            "Financials fallback ratio exceeded limit: "
            f"{fallback_stats['fallback_ratio']:.4f} > {financials_fallback_max_ratio:.4f}"
        )
        if financials_fallback_policy == "allow":
            log.info("%s (policy=allow, continuing)", msg)
        elif financials_fallback_policy == "warn":
            log.warning("%s (policy=warn, continuing)", msg)
        elif financials_fallback_policy == "strict":
            strict_drop_mask = out_df["financials_used_fiscaldate_fallback"].fillna(False).astype(bool)
            dropped = int(strict_drop_mask.sum())
            out_df = out_df.loc[~strict_drop_mask].copy().reset_index(drop=True)
            log.error("%s (policy=strict, dropped fallback rows=%s, remaining=%s)", msg, dropped, len(out_df))

    if financials_fallback_policy == "strict" and fallback_stats["fallback_ratio"] <= financials_fallback_max_ratio:
        log.info(
            "Financials fallback strict check passed: ratio=%.4f <= %.4f",
            fallback_stats["fallback_ratio"],
            financials_fallback_max_ratio,
        )

    violations_df = validate_pit_leakage_rows(out_df, mode=leakage_validation_mode)
    if mode in {"backfill", "snapshot"} and as_of_dates_schedule:
        log_backfill_diagnostic_samples(out_df, as_of_dates=as_of_dates_schedule, sample_size=5)
    log.info("ROIC non-null: %s / %s", out_df["ROIC"].notna().sum(), len(out_df))
    log.info("Revenue YoY non-null: %s / %s", out_df["Revenue YoY"].notna().sum(), len(out_df))
    log.info("OCF YoY non-null: %s / %s", out_df["OCF YoY"].notna().sum(), len(out_df))
    log.info("OCF/NI non-null: %s / %s", out_df["OCF/NI"].notna().sum(), len(out_df))
    log.info("EPS YoY non-null: %s / %s", out_df["EPS YoY"].notna().sum(), len(out_df))
    log.info("Share Dilution non-null: %s / %s", out_df["Share Dilution"].notna().sum(), len(out_df))
    log.info("Interest Coverage non-null: %s / %s", out_df["Interest Coverage"].notna().sum(), len(out_df))
    log.info("OPM volatility non-null: %s / %s", out_df["OPM volatility"].notna().sum(), len(out_df))
    eps_cols = [
        "EPS This Y",
        "EPS This Y Est Level",
        "EPS Next Y Est Level",
        "EPS This Y Base Actual",
        "EPS Next Y",
        "EPS Next Q",
        "EPS Next 5Y",
    ]
    log.info(
        "EPS columns (float): %s",
        [c for c in eps_cols if c in out_df.columns and np.issubdtype(out_df[c].dtype, np.floating)],
    )
    # Growth diagnostics
    if "g_fwd_list" in locals():
        total = len(g_fwd_list)
        def _nn(xs: List[float]) -> int:
            return sum(1 for v in xs if v is not None and not np.isnan(v))
        log.info("g_fwd non-null: %s / %s", _nn(g_fwd_list), total)
        log.info("g_hist non-null: %s / %s", _nn(g_hist_list), total)
        log.info("EPS Next 5Y non-null: %s / %s", _nn(eps5y_list), total)
        log.info("PEG non-null: %s / %s", _nn(peg_list), total)
        for sample in debug_eps_samples[:5]:
            log.debug(
                "EPS5Y sample %s: epsThisY_est=%s epsNextY_est=%s epsThisY_growth=%s g_fwd=%s g_hist=%s epsNext5Y_pct=%s pe=%s peg=%s",
                sample.get("symbol"),
                sample.get("epsThisY_est"),
                sample.get("epsNextY_est"),
                sample.get("epsThisY_growth"),
                sample.get("g_fwd"),
                sample.get("g_hist"),
                sample.get("epsNext5Y_pct"),
                sample.get("pe"),
                sample.get("peg"),
            )
    if "Dividend Ex-Date" in out_df.columns:
        log.info("Dividend Ex-Date non-null: %s / %s", out_df["Dividend Ex-Date"].notna().sum(), len(out_df))
    if "Dividend Est" in out_df.columns:
        log.info("Dividend Est non-null: %s / %s", out_df["Dividend Est"].notna().sum(), len(out_df))
    if "Dividend Gr. 3Y" in out_df.columns:
        log.info("Dividend Gr. 3Y non-null: %s / %s", out_df["Dividend Gr. 3Y"].notna().sum(), len(out_df))
    if "Dividend Gr. 5Y" in out_df.columns:
        log.info("Dividend Gr. 5Y non-null: %s / %s", out_df["Dividend Gr. 5Y"].notna().sum(), len(out_df))
    if "debug_div_samples" in locals():
        for sample in debug_div_samples[:5]:
            log.debug(
                "Div sample %s: price_date=%s div_now_1y=%s div_3y_ago_1y=%s div_5y_ago_1y=%s gr3=%s gr5=%s ex_date=%s",
                sample.get("symbol"),
                sample.get("price_date"),
                sample.get("div_now_1y"),
                sample.get("div_3y_ago_1y"),
                sample.get("div_5y_ago_1y"),
                sample.get("gr3"),
                sample.get("gr5"),
                sample.get("ex_date"),
            )

    # Unified persistence flow (latest/backfill):
    #  1) upsert newly calculated rows into history by (asOfDate, symbol)
    #  2) rebuild latest snapshot from history (one latest row per symbol)
    history_existing_df = load_existing_factors(data_dir, out_history_base, OUTPUT_COLUMNS)
    history_df = upsert_history(history_existing_df, out_df, FACTORS_KEY_COLS, OUTPUT_COLUMNS)
    log.info("History total rows: %s", len(history_df))
    log.info("History unique keys: %s", history_df[FACTORS_KEY_COLS].drop_duplicates().shape[0])

    for c in OUTPUT_COLUMNS:
        if c not in history_df.columns:
            history_df[c] = np.nan
    history_df = history_df.reindex(columns=[c for c in OUTPUT_COLUMNS if c in history_df.columns])
    for c in history_df.select_dtypes(include=[np.number]).columns:
        history_df[c] = history_df[c].astype(float, errors="ignore")
    for c in [
        "asOfDate",
        "price_date",
        "financials_date",
        "financials_public_date",
        "shares_public_date",
        "estimates_public_date",
        "target_public_date",
        "company_facts_public_date",
        "index_public_date",
        "insider_holdings_public_date",
        "data_cutoff_date",
    ]:
        if c in history_df.columns:
            history_df[c] = history_df[c].astype(str).replace("nan", "").str[:10]
    history_df = history_df.sort_values(FACTORS_KEY_COLS, ascending=[True, True]).reset_index(drop=True)

    latest_df = rebuild_latest_snapshot_from_history(history_df, columns=OUTPUT_COLUMNS)

    history_path_pq = data_dir / f"{out_history_base}.parquet"
    history_path_csv = data_dir / f"{out_history_base}.csv"
    history_df.to_parquet(history_path_pq, index=False)
    history_df.to_csv(history_path_csv, index=False, date_format="%Y-%m-%d")

    latest_path_pq = data_dir / f"{out_latest_base}.parquet"
    latest_path_csv = data_dir / f"{out_latest_base}.csv"
    latest_df.to_parquet(latest_path_pq, index=False)
    latest_df.to_csv(latest_path_csv, index=False, date_format="%Y-%m-%d")

    log.info(
        "Wrote history (%s rows): %s and %s",
        len(history_df),
        history_path_pq,
        history_path_csv,
    )
    log.info(
        "Wrote latest (%s symbols): %s and %s",
        len(latest_df),
        latest_path_pq,
        latest_path_csv,
    )
    run_operational_self_checks(
        out_df=out_df,
        history_df=history_df,
        latest_df=latest_df,
        fallback_stats=fallback_stats if "fallback_stats" in locals() else {},
        mode=mode,
        prices=prices,
        sample_as_of_date=sample_as_of_date,
    )

    if output_diagnostics:
        diag_cols = [
            "asOfDate",
            "symbol",
            "price_date",
            "data_cutoff_date",
            "financials_date",
            "financials_public_date",
            "financials_public_date_source",
            "financials_used_fiscaldate_fallback",
            "shares_public_date",
            "estimates_public_date",
            "target_public_date",
            "company_facts_public_date",
            "index_public_date",
            "insider_holdings_public_date",
            "price_source_mode",
        ]
        diag_cols = [c for c in diag_cols if c in out_df.columns]
        diag_rows = out_df[diag_cols].copy()
        diag_rows["leakage_violation"] = False
        diag_rows["financials_fallback_violation"] = diag_rows["financials_used_fiscaldate_fallback"].fillna(False).astype(bool) if "financials_used_fiscaldate_fallback" in diag_rows.columns else False
        if not violations_df.empty:
            bad_keys = set(
                zip(
                    violations_df["asOfDate"].astype(str).tolist(),
                    violations_df["symbol"].astype(str).tolist(),
                )
            )
            diag_rows["leakage_violation"] = [
                (str(a), str(s)) in bad_keys for a, s in zip(diag_rows["asOfDate"], diag_rows["symbol"])
            ]

        diag_rows = diag_rows.sort_values(["asOfDate", "symbol"], ascending=[True, True]).reset_index(drop=True)
        diag_path_pq = data_dir / f"{out_history_base}_factor_build_diagnostics.parquet"
        diag_path_csv = data_dir / f"{out_history_base}_factor_build_diagnostics.csv"
        diag_rows.to_parquet(diag_path_pq, index=False)
        diag_rows.to_csv(diag_path_csv, index=False, encoding="utf-8-sig")
        log.info("Wrote diagnostics rows: %s and %s", diag_path_pq, diag_path_csv)

        if not violations_df.empty:
            vio_path_pq = data_dir / f"{out_history_base}_factor_build_diagnostics_violations.parquet"
            vio_path_csv = data_dir / f"{out_history_base}_factor_build_diagnostics_violations.csv"
            violations_df.to_parquet(vio_path_pq, index=False)
            violations_df.to_csv(vio_path_csv, index=False, encoding="utf-8-sig")
            log.info("Wrote diagnostics violations: %s and %s", vio_path_pq, vio_path_csv)

    if output_fallback_diagnostics:
        fb_cols = [
            "asOfDate",
            "symbol",
            "price_date",
            "data_cutoff_date",
            "financials_date",
            "financials_public_date",
            "financials_public_date_source",
            "financials_used_fiscaldate_fallback",
            "price_source_mode",
        ]
        if "fallback_rows_df" in locals() and isinstance(fallback_rows_df, pd.DataFrame) and not fallback_rows_df.empty:
            fb_out = fallback_rows_df[[c for c in fb_cols if c in fallback_rows_df.columns]].copy()
        else:
            fb_out = pd.DataFrame(columns=fb_cols)
        fb_out = fb_out.sort_values(["asOfDate", "symbol"], ascending=[True, True]).reset_index(drop=True) if not fb_out.empty else fb_out
        fb_path_pq = data_dir / f"{out_history_base}_financials_fallback_rows.parquet"
        fb_path_csv = data_dir / f"{out_history_base}_financials_fallback_rows.csv"
        fb_out.to_parquet(fb_path_pq, index=False)
        fb_out.to_csv(fb_path_csv, index=False, encoding="utf-8-sig")
        log.info("Wrote financials fallback rows diagnostics: %s and %s (rows=%s)", fb_path_pq, fb_path_csv, len(fb_out))


if __name__ == "__main__":
    main()
