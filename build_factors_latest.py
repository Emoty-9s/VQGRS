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
    df["exDate"] = pd.to_datetime(df["exDate"], errors="coerce").dt.strftime("%Y-%m-%d")
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
    """Returns (latest_quarter_by_symbol, ttm_4q_by_symbol). TTM only when symbol has >= 4 quarters."""
    if financials.empty or "symbol" not in financials.columns or "fiscalDate" not in financials.columns:
        return {}, {}
    fin = financials.sort_values(["symbol", "fiscalDate"], ascending=[True, False])
    latest = fin.groupby("symbol").first().reset_index()
    latest_d = latest.set_index("symbol").to_dict("index")

    numeric_cols = [
        c for c in fin.columns
        if c not in ("symbol", "fiscalDate", "period") and pd.api.types.is_numeric_dtype(fin[c])
    ]
    ttm_d: Dict[str, Dict] = {}
    for sym, g in fin.groupby("symbol"):
        if len(g) < 4:
            continue
        ttm_d[sym] = g.head(4)[numeric_cols].sum().to_dict()
    return latest_d, ttm_d


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
        wad_ttm = _float_or_nan(row_ttm.get("weightedAverageSharesDiluted"))
        fcf_ttm = _float_or_nan(row_ttm.get("freeCashFlow"))
    else:
        ni_ttm = rev_ttm = div_paid_ttm = ebitda_ttm = wad_ttm = fcf_ttm = np.nan

    out["Payout"] = np.nan
    if row_ttm and ni_ttm and ni_ttm != 0 and div_paid_ttm is not None and not np.isnan(div_paid_ttm):
        out["Payout"] = abs(div_paid_ttm) / abs(ni_ttm)

    out["Employees"] = np.nan
    out["IPO (Date)"] = np.nan

    out["EPS (ttm)"] = safe_div(ni_ttm, wad_ttm) if row_ttm else np.nan
    eps_ttm = out["EPS (ttm)"]
    out["P/E"] = safe_div(price, eps_ttm) if eps_ttm and not np.isnan(eps_ttm) else np.nan
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
    out["ROIC"] = np.nan

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
    "Earnings (Date)", "Forward P/E", "PEG", "Dividend Est", "Dividend Gr. 3/5Y", "Dividend Ex-Date",
    "EPS next Y/Q/this Y/next 5Y", "Insider Own/Trans", "Inst Own/Trans",
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

        latest_fin, ttm_fin = latest_financials_and_ttm(financials)
        shares_latest = latest_shares_per_symbol(shares_df)
        target_series = latest_target_per_symbol(targets_df)
        index_member = latest_index_member(index_df)

        latest_price_map = latest_price_date_per_symbol(prices)
        log.info("Computed latest price date map")

        # asOfDate = global latest price date across all symbols (or per-symbol; spec says "계산 기준일")
        # We use per-symbol: asOfDate = price_date for that symbol
        rows: List[Dict[str, Any]] = []
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

            fin_inds = build_financial_indicators(row_latest, row_ttm, shares_out, price)
            target_p = target_series.get(sym, np.nan)
            if not isinstance(target_p, (int, float)):
                target_p = np.nan
            idx_val = index_member.get(sym, np.nan)

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
                "Forward P/E": np.nan,
                "PEG": np.nan,
                "Dividend Est": np.nan,
                "Dividend Gr. 3/5Y": np.nan,
                "Dividend Ex-Date": np.nan,
                "EPS next Y/Q/this Y/next 5Y": np.nan,
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

    out_path_pq = data_dir / f"{out_base}.parquet"
    out_path_csv = data_dir / f"{out_base}.csv"
    out_df.to_parquet(out_path_pq, index=False)
    out_df.to_csv(out_path_csv, index=False, date_format="%Y-%m-%d")
    log.info("Wrote %s and %s", out_path_pq, out_path_csv)


if __name__ == "__main__":
    main()
