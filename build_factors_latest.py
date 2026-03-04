# -*- coding: utf-8 -*-
"""
Build factors_latest: one row per symbol, latest snapshot of computed indicators.
Reads Parquet from data dir, outputs factors_latest.parquet and .csv.
"""
from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

INDEX_SYMBOL = "SP500"
DEFAULT_DATA_DIR = "data"
DEFAULT_OUT = "factors_latest"

# -----------------------------------------------------------------------------
# Load
# -----------------------------------------------------------------------------


def load_prices(data_dir: Path) -> pd.DataFrame:
    cols = ["symbol", "date", "open", "high", "low", "close", "volume"]
    path = data_dir / "prices_eod.parquet"
    if not path.exists():
        return pd.DataFrame(columns=cols)
    df = pd.read_parquet(path, columns=cols)
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    return df.dropna(subset=["date"])


def load_financials(data_dir: Path) -> pd.DataFrame:
    path = data_dir / "financials_quarterly.parquet"
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_parquet(path)
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
# Latest per symbol (prices, financials, shares, etc.)
# -----------------------------------------------------------------------------


def latest_price_date_per_symbol(prices: pd.DataFrame) -> pd.Series:
    g = prices.groupby("symbol", as_index=False)["date"].max()
    return g.set_index("symbol")["date"]


def get_price_series_for_symbol(prices: pd.DataFrame, symbol: str) -> pd.DataFrame:
    p = prices.loc[prices["symbol"] == symbol].sort_values("date").reset_index(drop=True)
    p = p.drop_duplicates(subset=["date"], keep="last")
    return p


def latest_financials_and_ttm(financials: pd.DataFrame) -> Tuple[Dict[str, Dict], Dict[str, Dict]]:
    """Returns (latest_quarter_by_symbol, ttm_4q_by_symbol).
    TTM: flow 항목은 sum, share-count 항목은 mean(또는 last). 주식수 합산 시 P/E가 4배 뻥튀기되므로 평균 사용."""
    if financials.empty or "symbol" not in financials.columns or "fiscalDate" not in financials.columns:
        return {}, {}

    fin = financials.copy()
    fin["symbol"] = fin["symbol"].astype(str).str.strip().str.upper()
    fin["fiscalDate"] = pd.to_datetime(fin["fiscalDate"], errors="coerce").dt.strftime("%Y-%m-%d")
    fin = fin.dropna(subset=["fiscalDate"])

    fin_desc = fin.sort_values(["symbol", "fiscalDate"], ascending=[True, False])
    latest = fin_desc.groupby("symbol").first().reset_index()
    latest_d = latest.set_index("symbol").to_dict("index")

    FLOW_SUM_COLS = [
        "netIncome", "revenue", "EBITDA", "freeCashFlow", "dividendsPaid",
        "operatingIncome", "incomeBeforeTax", "incomeTaxExpense",
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
    return latest_d, ttm_d


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
    """Next ex-date after price_date, or most recent ex-date on/before price_date. Returns YYYY-MM-DD str or np.nan."""
    if dividends.empty or "symbol" not in dividends.columns or "exDate" not in dividends.columns:
        return np.nan
    sym = symbol.strip().upper()
    div = dividends.loc[dividends["symbol"] == sym]
    if div.empty:
        return np.nan
    div = div.dropna(subset=["exDate"]).copy()
    if div.empty:
        return np.nan
    future = div.loc[div["exDate"] > price_date]
    if not future.empty:
        return str(future["exDate"].min())[:10]
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


def latest_target_per_symbol(targets: pd.DataFrame) -> pd.Series:
    if targets.empty or "symbol" not in targets.columns:
        return pd.Series(dtype=float)
    t = targets.sort_values(["symbol", "asOfDate"], ascending=[True, False])
    t = t.groupby("symbol").first().reset_index()
    return t.set_index("symbol")["targetPrice"]


def latest_index_member(index_df: pd.DataFrame) -> pd.Series:
    if index_df.empty or "memberSymbol" not in index_df.columns:
        return pd.Series(dtype=object)
    idx = index_df.sort_values("asOfDate", ascending=False).iloc[0]
    latest_asof = idx["asOfDate"]
    sub = index_df.loc[index_df["asOfDate"] == latest_asof]
    return sub.set_index("memberSymbol")["isMember"]


# -----------------------------------------------------------------------------
# Price-based indicators (one symbol)
# -----------------------------------------------------------------------------


def _float_or_nan(x: Any) -> float:
    try:
        return float(x)
    except (TypeError, ValueError):
        return np.nan


def compute_price_indicators(
    series: pd.DataFrame,
    price_date: str,
) -> Dict[str, float]:
    """series: sorted by date ascending, columns date, open, high, low, close, volume."""
    out: Dict[str, float] = {}
    if series.empty or "date" not in series.columns or "close" not in series.columns:
        return out
    series = series.sort_values("date").reset_index(drop=True)
    idx = series["date"] == price_date
    if not idx.any():
        return out
    loc = int(series.index[idx][0])
    close = series["close"].astype(float, errors="ignore")
    high = series["high"].astype(float, errors="ignore")
    low = series["low"].astype(float, errors="ignore")
    volume = series["volume"].astype(float, errors="ignore")

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

    n = len(series)
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

    # Perf
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
            first_close = _float_or_nan(same_year.sort_values("date").iloc[0]["close"])
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


def build_financial_indicators(
    row_latest: Optional[Dict],
    row_ttm: Optional[Dict],
    shares_out: float,
    price: float,
) -> Dict[str, float]:
    out: Dict[str, float] = {}
    if row_latest is None:
        for k in [
            "Income (Net)", "Sales (Rev)", "Book/sh", "Cash/sh", "Payout",
            "EPS (ttm)", "P/E", "P/S", "P/B", "P/C", "P/FCF",
            "Market Cap", "Enterprise Value(EV)", "EV/EBITDA", "EV/Sales",
            "Quick Ratio", "Current Ratio", "Debt/Eq", "LT Debt/Eq",
            "ROA", "ROE", "ROIC",
            "Gross Margin", "Oper. Margin", "Profit Margin", "Shs Outstand",
        ]:
            out[k] = np.nan
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

    out["Income (Net)"] = ni_l
    out["Sales (Rev)"] = rev_l
    out["Shs Outstand"] = shares_out if not np.isnan(shares_out) else so_l
    sh = out["Shs Outstand"]
    out["Book/sh"] = safe_div(eq_l, sh)
    out["Cash/sh"] = safe_div(cash_l, sh)

    if row_ttm:
        ni_ttm = _float_or_nan(row_ttm.get("netIncome"))
        rev_ttm = _float_or_nan(row_ttm.get("revenue"))
        div_paid_ttm = _float_or_nan(row_ttm.get("dividendsPaid"))
        ebitda_ttm = _float_or_nan(row_ttm.get("EBITDA"))
        fcf_ttm = _float_or_nan(row_ttm.get("freeCashFlow"))
        # TTM diluted shares: mean(4q) from latest_financials_and_ttm (sum 금지 → P/E 4배 뻥튀기 방지)
        wad_ttm = _float_or_nan(row_ttm.get("weightedAverageSharesDiluted"))
        if (wad_ttm is None or np.isnan(wad_ttm)) and shares_out and not np.isnan(shares_out):
            wad_ttm = float(shares_out)
        if (wad_ttm is None or np.isnan(wad_ttm)) and so_l is not None and not np.isnan(so_l):
            wad_ttm = float(so_l)
    else:
        ni_ttm = rev_ttm = div_paid_ttm = ebitda_ttm = wad_ttm = fcf_ttm = np.nan

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
    out["P/E"] = np.nan
    if eps_ttm is not None and not np.isnan(eps_ttm) and eps_ttm != 0 and price is not None and not np.isnan(price):
        out["P/E"] = float(price) / float(eps_ttm)
    out["P/S"] = safe_div(price, safe_div(rev_ttm, sh)) if row_ttm and sh and rev_ttm is not None else np.nan
    out["P/B"] = safe_div(price, safe_div(eq_l, sh)) if sh else np.nan
    out["P/C"] = safe_div(price, safe_div(cash_l, sh)) if sh else np.nan
    out["P/FCF"] = safe_div(price, safe_div(fcf_ttm, sh)) if row_ttm and sh and fcf_ttm is not None else np.nan

    out["Market Cap"] = price * sh if sh and not np.isnan(sh) else np.nan
    mc = out["Market Cap"]
    ev = mc + debt_l - cash_l if not np.isnan(mc) and debt_l is not None and cash_l is not None else np.nan
    out["Enterprise Value(EV)"] = ev
    out["EV/EBITDA"] = safe_div(ev, ebitda_ttm) if row_ttm and ev is not None else np.nan
    out["EV/Sales"] = safe_div(ev, rev_ttm) if row_ttm and ev is not None else np.nan

    out["Quick Ratio"] = safe_div(cash_l + rec_l, cl_l) if cl_l else np.nan
    out["Current Ratio"] = safe_div(ca_l, cl_l) if cl_l else np.nan
    out["Debt/Eq"] = safe_div(debt_l, eq_l) if eq_l else np.nan
    out["LT Debt/Eq"] = safe_div(ltd_l, eq_l) if eq_l else np.nan

    out["ROA"] = safe_div(ni_ttm, ta_l) if row_ttm and ta_l else np.nan
    out["ROE"] = safe_div(ni_ttm, eq_l) if row_ttm and eq_l else np.nan

    # ROIC = NOPAT_TTM / InvestedCapital_latest (financials_quarterly only)
    out["ROIC"] = np.nan
    if row_ttm and row_latest:
        ebit_ttm = _float_or_nan(row_ttm.get("operatingIncome"))
        ibt_ttm = _float_or_nan(row_ttm.get("incomeBeforeTax"))
        tax_exp_ttm = _float_or_nan(row_ttm.get("incomeTaxExpense"))
        tax_rate = np.nan
        if ibt_ttm is not None and not np.isnan(ibt_ttm) and ibt_ttm != 0 and tax_exp_ttm is not None and not np.isnan(tax_exp_ttm):
            tr = float(tax_exp_ttm) / float(ibt_ttm)
            if 0 <= tr <= 1:
                tax_rate = tr
        if tax_rate is None or np.isnan(tax_rate):
            tax_rate = 0.21
        nopat_ttm = np.nan
        if ebit_ttm is not None and not np.isnan(ebit_ttm):
            nopat_ttm = float(ebit_ttm) * (1.0 - float(tax_rate))
        cash_for_ic = 0.0 if (cash_l is None or np.isnan(cash_l)) else float(cash_l)
        invested_cap = np.nan
        if debt_l is not None and not np.isnan(debt_l) and eq_l is not None and not np.isnan(eq_l):
            invested_cap = float(debt_l) + float(eq_l) - cash_for_ic
        if invested_cap is not None and not np.isnan(invested_cap) and invested_cap != 0 and nopat_ttm is not None and not np.isnan(nopat_ttm):
            out["ROIC"] = float(nopat_ttm) / float(invested_cap)

    out["Gross Margin"] = safe_div(gp_l, rev_l) if rev_l else np.nan
    out["Oper. Margin"] = safe_div(oi_l, rev_l) if rev_l else np.nan
    out["Profit Margin"] = safe_div(ni_l, rev_l) if rev_l else np.nan
    return out


# -----------------------------------------------------------------------------
# Output columns order
# -----------------------------------------------------------------------------

OUTPUT_COLUMNS = [
    "asOfDate", "symbol", "price_date", "financials_date",
    "Price", "Prev Close", "Change", "Volume", "Avg Volume", "Rel Volume",
    "SMA20", "SMA50", "SMA200",
    "52W High", "52W Low",
    "Volatility", "ATR(14)", "RSI(14)",
    "Perf Week", "Perf Month", "Perf Quarter", "Perf Half Y", "Perf Year",
    "Perf 3Y", "Perf 5Y", "Perf 10Y", "Perf YTD",
    "Beta",
    "Income (Net)", "Sales (Rev)", "Book/sh", "Cash/sh",
    "Dividend TTM", "Payout", "Employees", "IPO (Date)",
    "EPS (ttm)", "P/E", "P/S", "P/B", "P/C", "P/FCF",
    "Market Cap", "Enterprise Value(EV)", "EV/EBITDA", "EV/Sales",
    "Quick Ratio", "Current Ratio", "Debt/Eq", "LT Debt/Eq",
    "ROA", "ROE", "ROIC",
    "Gross Margin", "Oper. Margin", "Profit Margin",
    "Shs Outstand", "Shs Float",
    "Earnings (Date)", "Forward P/E", "PEG", "Dividend Est", "Dividend Gr. 3Y", "Dividend Gr. 5Y", "Dividend Ex-Date",
    "EPS This Y", "EPS Next Y", "EPS Next Q", "EPS Next 5Y",
    "Insider Own/Trans", "Inst Own/Trans",
    "Short Float", "Short Interest", "Short Ratio", "Recom",
    "Target Price", "Index",
]


def main() -> None:
    ap = argparse.ArgumentParser(description="Build factors_latest from Parquet inputs")
    ap.add_argument("--data-dir", default=DEFAULT_DATA_DIR, help="Input data directory")
    ap.add_argument("--out", default=DEFAULT_OUT, help="Output base name (factors_latest)")
    ap.add_argument("--index-symbol", default=INDEX_SYMBOL, help="Index for membership (e.g. SP500)")
    args = ap.parse_args()

    data_dir = Path(args.data_dir)
    out_base = args.out
    index_symbol = args.index_symbol or INDEX_SYMBOL

    log.info("Loading parquets from %s", data_dir)
    prices = load_prices(data_dir)
    if prices.empty:
        log.warning("prices_eod empty or missing; no symbols to process")
        out_df = pd.DataFrame(columns=OUTPUT_COLUMNS)
    else:
        symbols = prices["symbol"].dropna().astype(str).str.strip().str.upper().unique().tolist()
        log.info("Symbols: %s", len(symbols))

        financials = load_financials(data_dir)
        dividends = load_dividends(data_dir)
        shares_df = load_shares(data_dir)
        index_df = load_index_membership(data_dir, index_symbol)
        targets_df = load_targets(data_dir)
        est_a = load_estimates_snapshot(data_dir)
        est_q = load_estimates_quarterly_snapshot(data_dir)

        eps_ttm_series_map = build_eps_ttm_series(financials)
        latest_fin, ttm_fin = latest_financials_and_ttm(financials)
        shares_latest = latest_shares_per_symbol(shares_df)
        target_series = latest_target_per_symbol(targets_df)
        index_member = latest_index_member(index_df)
        est_a_latest = latest_snapshot_per_symbol(est_a, ["epsThisY", "epsNextY", "epsNext5Y"])
        est_q_latest = latest_snapshot_per_symbol(est_q, ["epsNextQ"])
        est_a_map = est_a_latest.set_index("symbol").to_dict("index") if not est_a_latest.empty else {}
        est_q_map = est_q_latest.set_index("symbol").to_dict("index") if not est_q_latest.empty else {}

        latest_price_map = latest_price_date_per_symbol(prices)
        log.info("Computed latest price date map")

        # asOfDate = per-symbol price_date (계산 기준일)
        rows: List[Dict[str, Any]] = []
        g_fwd_list: List[float] = []
        g_hist_list: List[float] = []
        eps5y_list: List[float] = []
        peg_list: List[float] = []
        debug_eps_samples: List[Dict[str, Any]] = []
        debug_div_samples: List[Dict[str, Any]] = []
        for sym in symbols:
            price_date = latest_price_map.get(sym)
            if not price_date:
                continue
            series = get_price_series_for_symbol(prices, sym)
            if series.empty:
                continue
            as_of = price_date
            price_inds = compute_price_indicators(series, price_date)
            price = price_inds.get("Price", np.nan)
            if np.isnan(price):
                continue

            financials_date = np.nan
            if sym in latest_fin:
                financials_date = latest_fin[sym].get("fiscalDate", np.nan)
            row_latest = latest_fin.get(sym)
            row_ttm = ttm_fin.get(sym) if ttm_fin else None

            sh_row = shares_latest.loc[shares_latest["symbol"] == sym]
            if not sh_row.empty:
                shares_out = _float_or_nan(sh_row.iloc[0].get("sharesOutstanding"))
                shs_float = _float_or_nan(sh_row.iloc[0].get("sharesFloat"))
            else:
                shares_out = _float_or_nan(row_latest.get("sharesOutstanding")) if row_latest else np.nan
                shs_float = np.nan
            if np.isnan(shares_out) and row_latest:
                shares_out = _float_or_nan(row_latest.get("sharesOutstanding"))

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

            fin_inds = build_financial_indicators(row_latest, row_ttm, shares_out, price)
            target_p = target_series.get(sym, np.nan)
            if not isinstance(target_p, (int, float)):
                target_p = np.nan
            idx_val = index_member.get(sym, np.nan)

            # --- Estimates (Finviz-style): 4 separate EPS columns ---
            ea = est_a_map.get(sym, {}) if est_a_map else {}
            eq = est_q_map.get(sym, {}) if est_q_map else {}
            eps_this_y = _float_or_nan(ea.get("epsThisY"))
            eps_next_y = _float_or_nan(ea.get("epsNextY"))
            eps_next_q = _float_or_nan(eq.get("epsNextQ"))

            # Forward 1Y growth g_fwd from estimates
            g_fwd = np.nan
            if (
                eps_this_y is not None and not np.isnan(eps_this_y) and eps_this_y > 0
                and eps_next_y is not None and not np.isnan(eps_next_y) and eps_next_y > 0
            ):
                g_fwd = (float(eps_next_y) / float(eps_this_y)) - 1.0

            # Historical EPS CAGR (5Y with 3Y fallback) from EPS(TTM) time series
            g_hist = np.nan
            series_eps = eps_ttm_series_map.get(sym)
            latest_fd_dt = pd.to_datetime(financials_date, errors="coerce") if financials_date is not None else pd.NaT
            if series_eps is not None and not series_eps.empty and not pd.isna(latest_fd_dt):
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

            forward_pe = np.nan
            if eps_next_y is not None and not np.isnan(eps_next_y) and eps_next_y != 0:
                forward_pe = price / eps_next_y

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
                        "epsThisY": eps_this_y,
                        "epsNextY": eps_next_y,
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

            row_out = {
                "asOfDate": as_of,
                "symbol": sym,
                "price_date": price_date,
                "financials_date": financials_date if financials_date is not None else np.nan,
                **price_inds,
                "Dividend TTM": div_ttm,
                **fin_inds,
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
                "EPS This Y": eps_this_y if (eps_this_y is not None and not np.isnan(eps_this_y)) else np.nan,
                "EPS Next Y": eps_next_y if (eps_next_y is not None and not np.isnan(eps_next_y)) else np.nan,
                "EPS Next Q": eps_next_q if (eps_next_q is not None and not np.isnan(eps_next_q)) else np.nan,
                "EPS Next 5Y": eps_next_5y_pct,
                "Insider Own/Trans": np.nan,
                "Inst Own/Trans": np.nan,
                "Short Float": np.nan,
                "Short Interest": np.nan,
                "Short Ratio": np.nan,
                "Recom": np.nan,
            }
            rows.append(row_out)

        out_df = pd.DataFrame(rows)
        for c in OUTPUT_COLUMNS:
            if c not in out_df.columns:
                out_df[c] = np.nan
        out_df = out_df.reindex(columns=[c for c in OUTPUT_COLUMNS if c in out_df.columns])
        out_df = out_df.sort_values("symbol").reset_index(drop=True)
        for c in out_df.select_dtypes(include=[np.number]).columns:
            out_df[c] = out_df[c].astype(float, errors="ignore")
        for c in ["asOfDate", "price_date", "financials_date"]:
            if c in out_df.columns:
                out_df[c] = out_df[c].astype(str).replace("nan", "").str[:10]

    log.info("Output rows: %s", len(out_df))
    log.info("ROIC non-null: %s / %s", out_df["ROIC"].notna().sum(), len(out_df))
    eps_cols = ["EPS This Y", "EPS Next Y", "EPS Next Q", "EPS Next 5Y"]
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
                "EPS5Y sample %s: epsThisY=%s epsNextY=%s g_fwd=%s g_hist=%s epsNext5Y_pct=%s pe=%s peg=%s",
                sample.get("symbol"),
                sample.get("epsThisY"),
                sample.get("epsNextY"),
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

    out_path_pq = data_dir / f"{out_base}.parquet"
    out_path_csv = data_dir / f"{out_base}.csv"
    out_df.to_parquet(out_path_pq, index=False)
    out_df.to_csv(out_path_csv, index=False, date_format="%Y-%m-%d")
    log.info("Wrote %s and %s", out_path_pq, out_path_csv)


if __name__ == "__main__":
    main()
