# -*- coding: utf-8 -*-
"""
Build factors_latest: one row per symbol, latest snapshot of computed indicators.
Reads Parquet from data dir, outputs factors_latest.parquet and .csv.
"""
from __future__ import annotations

import argparse
import logging
import sys
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
    """prices_eod.parquet. Finviz 근사: adjClose 있으면 close 대신 사용(Perf/Beta 등 조정종가 기준). Parquet만 읽음."""
    cols = ["symbol", "date", "open", "high", "low", "close", "volume"]
    path = data_dir / "prices_eod.parquet"
    # FIX: 파일 없으면 빈 DF 반환; main에서 path 존재 여부로 에러 로그 후 종료 처리
    if not path.exists():
        return pd.DataFrame(columns=cols)
    df = pd.read_parquet(path)
    use_cols = [c for c in cols + ["adjClose"] if c in df.columns]
    df = df[use_cols]
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    df = df.dropna(subset=["date"])
    if "adjClose" in df.columns:
        df["close"] = pd.to_numeric(df["adjClose"], errors="coerce").fillna(pd.to_numeric(df["close"], errors="coerce"))
    return df


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
# Finviz-style: company_facts (Employees, IPO Date), Insider Own/Trans
# -----------------------------------------------------------------------------


def build_company_facts_lookup(cf: pd.DataFrame) -> Dict[str, List[Dict[str, Any]]]:
    """symbol -> list of rows sorted by asOfDate desc (newest first). Used for asOfDate <= price_date lookup."""
    out: Dict[str, List[Dict[str, Any]]] = {}
    if cf.empty or "symbol" not in cf.columns or "asOfDate" not in cf.columns:
        return out
    cf = cf.dropna(subset=["symbol"]).sort_values(["symbol", "asOfDate"], ascending=[True, False])
    for sym, g in cf.groupby("symbol"):
        sym = str(sym).strip().upper() if sym else ""
        if not sym:
            continue
        rows = g.to_dict("records")
        out[sym] = [r for r in rows if r.get("asOfDate")]
    return out


def get_company_facts_at(
    sym: str,
    price_date: str,
    cf_lookup: Dict[str, List[Dict[str, Any]]],
    step_back_for_employees: int = 2,
) -> Dict[str, Any]:
    """Row with asOfDate <= price_date (latest). employees: step back up to step_back_for_employees if null."""
    out: Dict[str, Any] = {"employees": np.nan, "ipoDate": np.nan, "shares_out": np.nan}
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
    eligible = []
    for r in rows:
        asof = r.get("asOfDate")
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
    row_prev_quarter: Optional[Dict] = None,
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
        fcf_ttm = _float_or_nan(row_ttm.get("freeCashFlow"))
        # TTM diluted shares: mean(4q) from latest_financials_and_ttm (sum 금지 → P/E 4배 뻥튀기 방지)
        wad_ttm = _float_or_nan(row_ttm.get("weightedAverageSharesDiluted"))
        if (wad_ttm is None or np.isnan(wad_ttm)) and shares_out and not np.isnan(shares_out):
            wad_ttm = float(shares_out)
        if (wad_ttm is None or np.isnan(wad_ttm)) and so_l is not None and not np.isnan(so_l):
            wad_ttm = float(so_l)
    else:
        ni_ttm = rev_ttm = gp_ttm = oi_ttm = div_paid_ttm = ebitda_ttm = wad_ttm = fcf_ttm = np.nan

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

    # ROIC = NOPAT_TTM / InvestedCapital; IC = debt + equity - cash (현금 전액 차감 유지)
    # IC 분모: latest와 직전 분기 평균; tax_rate 안정화(IBT 규모/부호 이상 시 0.21 또는 clamp)
    out["ROIC"] = np.nan
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
        cash_for_ic = 0.0 if (cash_l is None or np.isnan(cash_l)) else float(cash_l)
        ic_latest = np.nan
        if debt_l is not None and not np.isnan(debt_l) and eq_l is not None and not np.isnan(eq_l):
            ic_latest = float(debt_l) + float(eq_l) - cash_for_ic
        # 직전 분기 IC 있으면 평균, 없으면 latest만 사용
        if row_prev_quarter is not None:
            debt_p = _float_or_nan(row_prev_quarter.get("totalDebt"))
            eq_p = _float_or_nan(row_prev_quarter.get("totalStockholdersEquity"))
            cash_p = _float_or_nan(row_prev_quarter.get("cashAndCashEquivalents"))
            cash_p_val = 0.0 if (cash_p is None or np.isnan(cash_p)) else float(cash_p)
            ic_prev = np.nan
            if debt_p is not None and not np.isnan(debt_p) and eq_p is not None and not np.isnan(eq_p):
                ic_prev = float(debt_p) + float(eq_p) - cash_p_val
            if ic_latest is not None and not np.isnan(ic_latest) and ic_prev is not None and not np.isnan(ic_prev):
                invested_cap = (float(ic_latest) + float(ic_prev)) / 2.0
            else:
                invested_cap = ic_latest
        else:
            invested_cap = ic_latest
        if invested_cap is not None and not np.isnan(invested_cap) and invested_cap != 0 and nopat_ttm is not None and not np.isnan(nopat_ttm):
            out["ROIC"] = float(nopat_ttm) / float(invested_cap)

    # Finviz: margins from TTM (grossProfit_ttm/revenue_ttm etc.); fallback to latest quarter
    out["Gross Margin"] = safe_div(gp_ttm, rev_ttm) if row_ttm else (safe_div(gp_l, rev_l) if rev_l else np.nan)
    out["Oper. Margin"] = safe_div(oi_ttm, rev_ttm) if row_ttm else (safe_div(oi_l, rev_l) if rev_l else np.nan)
    out["Profit Margin"] = safe_div(ni_ttm, rev_ttm) if row_ttm else (safe_div(ni_l, rev_l) if rev_l else np.nan)
    return out


# -----------------------------------------------------------------------------
# New factor helpers: Revenue YoY, EPS YoY, Share Dilution, Interest Coverage, OPM volatility
# -----------------------------------------------------------------------------


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
    "asOfDate", "symbol", "price_date", "financials_date",
    "Price", "Prev Close", "Change", "Volume", "Avg Volume", "Rel Volume",
    "SMA20", "SMA50", "SMA200",
    "52W High", "52W Low",
    "Volatility", "ATR(14)", "RSI(14)",
    "Perf Week", "Perf Month", "Perf Quarter", "Perf Half Y", "Perf Year",
    "Perf 3Y", "Perf 5Y", "Perf 10Y", "Perf YTD",
    "Beta",
    "Income (Net)", "Sales (Rev)", "Revenue YoY", "OCF YoY", "Book/sh", "Cash/sh",
    "Dividend TTM", "Payout", "Employees", "IPO (Date)",
    "EPS (ttm)", "EPS YoY", "P/E", "P/S", "P/B", "P/C", "P/FCF",
    "Market Cap", "Enterprise Value(EV)", "EV/EBITDA", "EV/Sales",
    "Quick Ratio", "Current Ratio", "Debt/Eq", "LT Debt/Eq", "Interest Coverage",
    "ROA", "ROE", "ROIC",
    "Gross Margin", "Oper. Margin", "Profit Margin", "OCF/NI", "OPM volatility",
    "Shs Outstand", "Share Dilution", "Shs Float",
    "Earnings (Date)", "Forward P/E", "PEG", "Dividend Est", "Dividend Gr. 3Y", "Dividend Gr. 5Y", "Dividend Ex-Date",
    "EPS This Y", "EPS Next Y", "EPS Next Q", "EPS Next 5Y",
    "Insider Own/Trans", "Inst Own/Trans",
    "Short Float", "Short Interest", "Short Ratio", "Recom",
    "Target Price", "Index",
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
    for c in ["asOfDate", "price_date", "financials_date"]:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce").dt.strftime("%Y-%m-%d")
            df[c] = df[c].astype(str).replace("nan", "").replace("<NA>", "").str[:10]
    return df


def upsert_factors(
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
    # FIX: prices_eod.parquet 없으면 명확한 에러 로그 후 종료 (Parquet 전용, CSV fallback 금지)
    if not (data_dir / "prices_eod.parquet").exists():
        log.error("prices_eod.parquet가 없습니다. Parquet 전용이므로 종료합니다. data 디렉터리와 파일을 확인하세요.")
        sys.exit(1)
    prices = load_prices(data_dir)
    if prices.empty:
        log.warning("prices_eod.parquet가 비어 있습니다. 처리할 심볼이 없습니다.")
        out_df = pd.DataFrame(columns=OUTPUT_COLUMNS)
    else:
        symbols = prices["symbol"].dropna().astype(str).str.strip().str.upper().unique().tolist()
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
        dividends = load_dividends(data_dir)
        shares_df = load_shares(data_dir)
        index_df = load_index_membership(data_dir, index_symbol)
        targets_df = load_targets(data_dir)
        company_facts = load_company_facts(data_dir)
        insider_holdings_df = load_insider_holdings(data_dir)
        insider_transactions_df = load_insider_transactions(data_dir)
        est_a = load_estimates_snapshot(data_dir)
        est_q = load_estimates_quarterly_snapshot(data_dir)

        cf_lookup = build_company_facts_lookup(company_facts)
        holdings_by_sym = build_holdings_dedupe_and_totals(insider_holdings_df)
        tx_group = build_transactions_group(insider_transactions_df)

        eps_ttm_series_map = build_eps_ttm_series(financials)
        latest_fin, prev_fin, ttm_fin = latest_financials_and_ttm(financials)
        shares_latest = latest_shares_per_symbol(shares_df)
        target_series = latest_target_per_symbol(targets_df)
        index_member = latest_index_member(index_df)
        est_a_latest = latest_snapshot_per_symbol(est_a, ["epsThisY", "epsNextY", "epsNext5Y"])
        est_q_latest = latest_snapshot_per_symbol(est_q, ["epsNextQ"])
        est_a_map = est_a_latest.set_index("symbol").to_dict("index") if not est_a_latest.empty else {}
        est_q_map = est_q_latest.set_index("symbol").to_dict("index") if not est_q_latest.empty else {}

        latest_price_map = latest_price_date_per_symbol(prices)
        log.info("Computed latest price date map")

        revenue_yoy_map = build_revenue_yoy_map(financials)
        ocf_yoy_map = build_ocf_yoy_map(financials)
        ocf_ni_map = build_ocf_ni_map(financials)
        eps_yoy_map = build_eps_yoy_map(financials)
        share_dilution_map = build_share_dilution_map(shares_df, latest_price_map)
        interest_coverage_map = build_interest_coverage_map(financials)
        opm_volatility_map = build_opm_volatility_map(financials)

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
            # Beta: sp500_prices(^GSPC 또는 대체 심볼) 시장 수익률로 Finviz 근사 (5Y 월간 → 252일 일간 fallback)
            beta_val = np.nan
            try:
                if sp500_df is not None and not sp500_df.empty:
                    stock_for_beta = series[["date", "close"]].copy()
                    mkt_for_beta = sp500_df[["date", "close"]].copy()
                    beta_val = beta_finviz_style(stock_for_beta, mkt_for_beta, price_date)
            except Exception as e:
                log.warning("Beta 계산 실패 sym=%s price_date=%s: %s", sym, price_date, e)
                beta_val = np.nan
            price_inds["Beta"] = beta_val

            price = price_inds.get("Price", np.nan)
            if np.isnan(price):
                continue

            financials_date = np.nan
            if sym in latest_fin:
                financials_date = latest_fin[sym].get("fiscalDate", np.nan)
            row_latest = latest_fin.get(sym)
            row_ttm = ttm_fin.get(sym) if ttm_fin else None
            row_prev = prev_fin.get(sym) if prev_fin else None

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

            fin_inds = build_financial_indicators(row_latest, row_ttm, shares_out, price, row_prev_quarter=row_prev)
            # Finviz-style: Employees, IPO (Date) from company_facts (asOfDate <= price_date, step back for employees)
            employees_val = get_employees_at(sym, price_date, cf_lookup)
            ipo_date_val = get_ipo_date_at(sym, price_date, cf_lookup)
            fin_inds["Employees"] = employees_val
            fin_inds["IPO (Date)"] = ipo_date_val

            # Finviz-style: Insider Own % (holdings_now/shares_out), Insider Trans % (holdings diff or net trans 90d)
            cf_at = get_company_facts_at(sym, price_date, cf_lookup)
            shares_out_insider = cf_at.get("shares_out")
            if shares_out_insider is None or (isinstance(shares_out_insider, (int, float)) and (np.isnan(shares_out_insider) or shares_out_insider <= 0)):
                shares_out_insider = float(shares_out) if (shares_out is not None and not np.isnan(shares_out) and shares_out > 0) else None
            holdings_now = holdings_total_at(sym, price_date, holdings_by_sym)
            holdings_prev = holdings_prev_90d(sym, price_date, holdings_by_sym)
            net_trans = net_trans_shares_90d(sym, price_date, tx_group)
            own_pct = insider_own_pct_finviz(holdings_now, shares_out_insider)
            trans_pct = insider_trans_pct_finviz(holdings_now, holdings_prev, net_trans, shares_out_insider, prefer_holdings_diff=True)
            insider_own_trans_str = format_insider_own_trans(own_pct, trans_pct)

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
                "Revenue YoY": revenue_yoy_map.get(sym, np.nan),
                "OCF YoY": ocf_yoy_map.get(sym, np.nan),
                "OCF/NI": ocf_ni_map.get(sym, np.nan),
                "EPS YoY": eps_yoy_map.get(sym, np.nan),
                "Share Dilution": share_dilution_map.get(sym, np.nan),
                "Interest Coverage": interest_coverage_map.get(sym, np.nan),
                "OPM volatility": opm_volatility_map.get(sym, np.nan),
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
                "Insider Own/Trans": insider_own_trans_str,
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

    log.info("New snapshot rows: %s", len(out_df))
    log.info("ROIC non-null: %s / %s", out_df["ROIC"].notna().sum(), len(out_df))
    log.info("Revenue YoY non-null: %s / %s", out_df["Revenue YoY"].notna().sum(), len(out_df))
    log.info("OCF YoY non-null: %s / %s", out_df["OCF YoY"].notna().sum(), len(out_df))
    log.info("OCF/NI non-null: %s / %s", out_df["OCF/NI"].notna().sum(), len(out_df))
    log.info("EPS YoY non-null: %s / %s", out_df["EPS YoY"].notna().sum(), len(out_df))
    log.info("Share Dilution non-null: %s / %s", out_df["Share Dilution"].notna().sum(), len(out_df))
    log.info("Interest Coverage non-null: %s / %s", out_df["Interest Coverage"].notna().sum(), len(out_df))
    log.info("OPM volatility non-null: %s / %s", out_df["OPM volatility"].notna().sum(), len(out_df))
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

    # Snapshot accumulation (upsert): do not overwrite entire file; merge today's out_df with existing.
    # key_cols=['asOfDate','symbol']: each row is one snapshot per symbol per date; same (date, symbol) in a
    # later run must replace the old row (update), and new (date, symbol) pairs are appended (insert).
    existing_df = load_existing_factors(data_dir, out_base, OUTPUT_COLUMNS)
    merged_df = upsert_factors(existing_df, out_df, FACTORS_KEY_COLS, OUTPUT_COLUMNS)
    log.info("Merged total rows: %s", len(merged_df))
    log.info("Unique keys: %s", merged_df[FACTORS_KEY_COLS].drop_duplicates().shape[0])

    for c in OUTPUT_COLUMNS:
        if c not in merged_df.columns:
            merged_df[c] = np.nan
    merged_df = merged_df.reindex(columns=[c for c in OUTPUT_COLUMNS if c in merged_df.columns])
    for c in merged_df.select_dtypes(include=[np.number]).columns:
        merged_df[c] = merged_df[c].astype(float, errors="ignore")
    for c in ["asOfDate", "price_date", "financials_date"]:
        if c in merged_df.columns:
            merged_df[c] = merged_df[c].astype(str).replace("nan", "").str[:10]

    merged_df = merged_df.sort_values(FACTORS_KEY_COLS, ascending=[True, True]).reset_index(drop=True)

    out_path_pq = data_dir / f"{out_base}.parquet"
    out_path_csv = data_dir / f"{out_base}.csv"
    merged_df.to_parquet(out_path_pq, index=False)
    merged_df.to_csv(out_path_csv, index=False, date_format="%Y-%m-%d")
    log.info("Wrote %s and %s (upserted)", out_path_pq, out_path_csv)


if __name__ == "__main__":
    main()
