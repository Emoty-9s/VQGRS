# -*- coding: utf-8 -*-
r"""
FMP Premium: 8개 테이블만 수집 (prices_eod, financials_quarterly, dividends_events, earnings_events,
estimates_snapshot, targets_snapshot, shares_snapshot, index_membership).
API Key: 환경변수 FMP_API_KEY (로그/예외/출력 노출 금지). 결측 NaN 유지. 저장은 PK 기준 Upsert.

실행 예:
  python fmp_universe_fetch.py --universe ./data/universe_list.csv --outdir ./data --mode backfill --from 2020-01-01
  python fmp_universe_fetch.py --universe ./data/universe_list.csv --outdir ./data --mode daily
"""
from __future__ import annotations

import argparse
import logging
import os
import threading
import time
import zlib
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# -----------------------------------------------------------------------------
# Config
# -----------------------------------------------------------------------------
BASE_URL = "https://financialmodelingprep.com"
API_KEY_ENV = "FMP_API_KEY"
CALLS_PER_MIN = 600
MIN_INTERVAL = 60.0 / CALLS_PER_MIN

PATH_EOD_BULK = "/stable/eod-bulk"
PATH_EOD_FULL = "/stable/historical-price-eod/full"
PATH_INCOME = "/stable/income-statement"
PATH_BALANCE = "/stable/balance-sheet-statement"
PATH_CASHFLOW = "/stable/cash-flow-statement"
PATH_DIVIDENDS = "/stable/dividends"
PATH_EARNINGS = "/stable/earnings-company"
PATH_ANALYST_ESTIMATES = "/stable/analyst-estimates"
PATH_PRICE_TARGET_CONSENSUS = "/stable/price-target-consensus"
PATH_SHARES_FLOAT_ALL = "/stable/shares-float-all"

# Table schemas (column order = final output)
PRICES_EOD_COLUMNS = ["symbol", "date", "open", "high", "low", "close", "adjClose", "volume"]
FINANCIALS_QUARTERLY_COLUMNS = [
    "symbol", "fiscalDate", "period",
    "revenue", "netIncome", "grossProfit", "operatingIncome", "EBITDA",
    "incomeBeforeTax", "incomeTaxExpense",
    "cashAndCashEquivalents", "receivables", "shortTermInvestments",
    "currentAssets", "currentLiabilities", "totalAssets",
    "totalStockholdersEquity", "totalDebt", "longTermDebt",
    "freeCashFlow", "dividendsPaid",
    "weightedAverageSharesDiluted", "sharesOutstanding",
]
DIVIDENDS_EVENTS_COLUMNS = [
    "symbol", "exDate", "dividend", "adjDividend", "recordDate", "paymentDate",
    "declarationDate", "frequency", "yield",
]
EARNINGS_EVENTS_COLUMNS = [
    "symbol", "earningsDate", "epsActual", "epsEstimated", "revenueActual", "revenueEstimated", "fiscalDate",
]
ESTIMATES_SNAPSHOT_COLUMNS = ["symbol", "asOfDate", "epsNextY", "epsNextQ", "epsThisY"]
TARGETS_SNAPSHOT_COLUMNS = ["symbol", "asOfDate", "targetPrice"]
SHARES_SNAPSHOT_COLUMNS = ["symbol", "asOfDate", "sharesOutstanding", "sharesFloat"]
INDEX_MEMBERSHIP_COLUMNS = ["indexSymbol", "asOfDate", "memberSymbol", "isMember"]

CUTOFF_DATE = "2020-01-01"


def _safe_params(params: Dict[str, Any]) -> Dict[str, Any]:
    return {k: ("***" if k.lower() == "apikey" else v) for k, v in params.items()}


class RateLimiter:
    def __init__(self, min_interval: float):
        self.min_interval = min_interval
        self._last = 0.0
        self._lock = threading.Lock()

    def wait(self) -> None:
        with self._lock:
            now = time.time()
            if now - self._last < self.min_interval:
                time.sleep(self.min_interval - (now - self._last))
            self._last = time.time()


def make_session() -> requests.Session:
    s = requests.Session()
    retries = Retry(
        total=5,
        backoff_factor=1.0,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.headers.update({"User-Agent": "VQGRS-FMP/1.0"})
    return s


def fmp_get(
    session: requests.Session,
    rl: RateLimiter,
    path: str,
    params: Dict[str, Any],
    api_key: str,
    call_counter: Optional[Dict[str, Any]] = None,
    allow_404_empty: bool = False,
) -> Any:
    rl.wait()
    url = f"{BASE_URL}{path}"
    p = dict(params)
    p["apikey"] = api_key
    r = session.get(url, params=p, timeout=60)
    if call_counter is not None:
        lock = call_counter.get("lock")
        if lock is not None:
            with lock:
                call_counter["count"] = call_counter.get("count", 0) + 1
        else:
            call_counter["count"] = call_counter.get("count", 0) + 1
    if r.status_code == 404 and allow_404_empty:
        return []
    if r.status_code >= 400:
        raise RuntimeError(f"HTTP {r.status_code}: {path} params={_safe_params(p)}")
    data = r.json()
    if isinstance(data, dict) and data.get("Error Message"):
        raise RuntimeError(f"FMP Error: {data.get('Error Message')} path={path}")
    return data


def read_universe(path: str) -> List[str]:
    df = pd.read_csv(path)
    cols_lower = {str(c).lower(): c for c in df.columns}
    for name in ["symbol", "ticker_fixed", "ticker"]:
        if name in cols_lower:
            col = cols_lower[name]
            syms = df[col].astype(str).str.strip().str.upper()
            return syms[syms != ""].dropna().unique().tolist()
    raise ValueError("universe_list.csv에 symbol/ticker_fixed/ticker 컬럼이 없습니다.")


def filter_by_shard(symbols: List[str], num_shards: int, shard_id: int) -> List[str]:
    """Stable sharding by symbol: zlib.crc32(sym) % num_shards == shard_id. num_shards==1이면 전체 반환."""
    if num_shards <= 1 or shard_id < 0 or shard_id >= num_shards:
        return list(symbols)
    result = []
    for sym in symbols:
        h = zlib.crc32(sym.encode("utf-8")) & 0xFFFFFFFF
        if (h % num_shards) == shard_id:
            result.append(sym)
    return result


def pick(d: Dict[str, Any], keys: List[str], coerce_float: bool = True) -> Optional[Any]:
    for k in keys:
        v = d.get(k)
        if v is None:
            continue
        if isinstance(v, str) and not v.strip():
            continue
        if coerce_float and isinstance(v, (int, float)):
            return float(v)
        if coerce_float and isinstance(v, str):
            try:
                return float(v)
            except ValueError:
                return str(v)
        return v
    return None


def pick_str(d: Dict[str, Any], keys: List[str]) -> Optional[str]:
    for k in keys:
        v = d.get(k)
        if v is None:
            continue
        s = str(v).strip()
        if s:
            return s[:10] if len(s) > 10 else s
    return None


# -----------------------------------------------------------------------------
# Fetch functions
# -----------------------------------------------------------------------------
def fetch_eod_bulk(
    session: requests.Session,
    rl: RateLimiter,
    api_key: str,
    trade_date: str,
    call_counter: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    data = fmp_get(
        session, rl, PATH_EOD_BULK,
        {"date": trade_date},
        api_key,
        call_counter=call_counter,
        allow_404_empty=True,
    )
    return data if isinstance(data, list) else []


def fetch_eod_full(
    session: requests.Session,
    rl: RateLimiter,
    api_key: str,
    symbol: str,
    from_date: str,
    to_date: str,
    call_counter: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    data = fmp_get(
        session, rl, PATH_EOD_FULL,
        {"symbol": symbol, "from": from_date, "to": to_date},
        api_key,
        call_counter=call_counter,
        allow_404_empty=True,
    )
    return data if isinstance(data, list) else []


def fetch_income(
    session: requests.Session,
    rl: RateLimiter,
    api_key: str,
    symbol: str,
    limit: int,
    call_counter: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    data = fmp_get(
        session, rl, PATH_INCOME,
        {"symbol": symbol, "period": "quarter", "limit": limit},
        api_key,
        call_counter=call_counter,
        allow_404_empty=True,
    )
    return data if isinstance(data, list) else []


def fetch_balance(
    session: requests.Session,
    rl: RateLimiter,
    api_key: str,
    symbol: str,
    limit: int,
    call_counter: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    data = fmp_get(
        session, rl, PATH_BALANCE,
        {"symbol": symbol, "period": "quarter", "limit": limit},
        api_key,
        call_counter=call_counter,
        allow_404_empty=True,
    )
    return data if isinstance(data, list) else []


def fetch_cashflow(
    session: requests.Session,
    rl: RateLimiter,
    api_key: str,
    symbol: str,
    limit: int,
    call_counter: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    data = fmp_get(
        session, rl, PATH_CASHFLOW,
        {"symbol": symbol, "period": "quarter", "limit": limit},
        api_key,
        call_counter=call_counter,
        allow_404_empty=True,
    )
    return data if isinstance(data, list) else []


def fetch_dividends(
    session: requests.Session,
    rl: RateLimiter,
    api_key: str,
    symbol: str,
    call_counter: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    data = fmp_get(
        session, rl, PATH_DIVIDENDS,
        {"symbol": symbol},
        api_key,
        call_counter=call_counter,
        allow_404_empty=True,
    )
    return data if isinstance(data, list) else []


def fetch_earnings(
    session: requests.Session,
    rl: RateLimiter,
    api_key: str,
    symbol: str,
    limit: int,
    call_counter: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    data = fmp_get(
        session, rl, PATH_EARNINGS,
        {"symbol": symbol, "limit": limit},
        api_key,
        call_counter=call_counter,
        allow_404_empty=True,
    )
    return data if isinstance(data, list) else []


def fetch_analyst_estimates(
    session: requests.Session,
    rl: RateLimiter,
    api_key: str,
    symbol: str,
    call_counter: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    data = fmp_get(
        session, rl, PATH_ANALYST_ESTIMATES,
        {"symbol": symbol},
        api_key,
        call_counter=call_counter,
        allow_404_empty=True,
    )
    return data if isinstance(data, list) else []


def fetch_price_target_consensus(
    session: requests.Session,
    rl: RateLimiter,
    api_key: str,
    symbol: str,
    call_counter: Optional[Dict[str, Any]] = None,
) -> Any:
    data = fmp_get(
        session, rl, PATH_PRICE_TARGET_CONSENSUS,
        {"symbol": symbol},
        api_key,
        call_counter=call_counter,
        allow_404_empty=True,
    )
    return data


def fetch_shares_float_all(
    session: requests.Session,
    rl: RateLimiter,
    api_key: str,
    page: int,
    limit: int = 1000,
    call_counter: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    data = fmp_get(
        session, rl, PATH_SHARES_FLOAT_ALL,
        {"page": page, "limit": limit},
        api_key,
        call_counter=call_counter,
        allow_404_empty=True,
    )
    return data if isinstance(data, list) else []


# -----------------------------------------------------------------------------
# Build DataFrames from API responses
# -----------------------------------------------------------------------------
def build_prices_eod_from_bulk(
    bulk_rows: List[Dict[str, Any]],
    universe: Set[str],
    trade_date: str,
) -> pd.DataFrame:
    rows = []
    for r in bulk_rows:
        sym = (r.get("symbol") or r.get("code") or "").strip().upper()
        if not sym or (universe and sym not in universe):
            continue
        rows.append({
            "symbol": sym,
            "date": trade_date,
            "open": pick(r, ["open", "Open"]),
            "high": pick(r, ["high", "High"]),
            "low": pick(r, ["low", "Low"]),
            "close": pick(r, ["close", "Close"]),
            "adjClose": pick(r, ["adjClose", "adjustedClose", "adj_close"]),
            "volume": pick(r, ["volume", "Volume"]),
        })
    if not rows:
        return pd.DataFrame(columns=PRICES_EOD_COLUMNS)
    df = pd.DataFrame(rows)
    return df.reindex(columns=PRICES_EOD_COLUMNS)


def build_prices_eod_from_full(
    full_rows: List[Dict[str, Any]],
    symbol: str,
) -> pd.DataFrame:
    sym = symbol.strip().upper()
    rows = []
    for r in full_rows:
        d = pick_str(r, ["date"])
        if d is None:
            continue
        rows.append({
            "symbol": sym,
            "date": d,
            "open": pick(r, ["open", "Open"]),
            "high": pick(r, ["high", "High"]),
            "low": pick(r, ["low", "Low"]),
            "close": pick(r, ["close", "Close"]),
            "adjClose": pick(r, ["adjClose", "adjustedClose", "adj_close"]),
            "volume": pick(r, ["volume", "Volume"]),
        })
    if not rows:
        return pd.DataFrame(columns=PRICES_EOD_COLUMNS)
    return pd.DataFrame(rows).reindex(columns=PRICES_EOD_COLUMNS)


def build_financials_quarterly(
    income: List[Dict[str, Any]],
    balance: List[Dict[str, Any]],
    cashflow: List[Dict[str, Any]],
    symbol: str,
    cutoff: str,
) -> pd.DataFrame:
    sym = symbol.strip().upper()
    by_key: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for r in income:
        d = pick_str(r, ["date", "fiscalDateEnding", "fillingDate"])
        per = pick_str(r, ["period"])
        if d is None or d < cutoff:
            continue
        key = (d, per if per is not None else "")
        if key not in by_key:
            by_key[key] = {"symbol": sym, "fiscalDate": d, "period": per if per is not None else pd.NA}
        by_key[key].update({
            "revenue": pick(r, ["revenue", "Revenue"]),
            "netIncome": pick(r, ["netIncome", "NetIncome"]),
            "grossProfit": pick(r, ["grossProfit", "GrossProfit"]),
            "operatingIncome": pick(r, ["operatingIncome", "OperatingIncome"]),
            "EBITDA": pick(r, ["ebitda", "EBITDA"]),
            "incomeBeforeTax": pick(r, ["incomeBeforeTax", "incomeBeforeTax"]),
            "incomeTaxExpense": pick(r, ["incomeTaxExpense", "incomeTaxExpense"]),
        })
    for r in balance:
        d = pick_str(r, ["date", "fiscalDateEnding", "fillingDate"])
        per = pick_str(r, ["period"])
        if d is None or d < cutoff:
            continue
        key = (d, per if per is not None else "")
        if key not in by_key:
            by_key[key] = {"symbol": sym, "fiscalDate": d, "period": per if per is not None else pd.NA}
        by_key[key].update({
            "cashAndCashEquivalents": pick(r, ["cashAndCashEquivalents"]),
            "receivables": pick(r, ["netReceivables", "receivables"]),
            "shortTermInvestments": pick(r, ["shortTermInvestments"]),
            "currentAssets": pick(r, ["totalCurrentAssets", "currentAssets"]),
            "currentLiabilities": pick(r, ["totalCurrentLiabilities", "currentLiabilities"]),
            "totalAssets": pick(r, ["totalAssets", "TotalAssets"]),
            "totalStockholdersEquity": pick(r, ["totalStockholdersEquity", "totalEquity", "stockholdersEquity"]),
            "totalDebt": pick(r, ["totalDebt"]),
            "longTermDebt": pick(r, ["longTermDebt"]),
            "sharesOutstanding": pick(r, ["commonStockSharesOutstanding", "sharesOutstanding"]),
        })
    for r in cashflow:
        d = pick_str(r, ["date", "fiscalDateEnding", "fillingDate"])
        per = pick_str(r, ["period"])
        if d is None or d < cutoff:
            continue
        key = (d, per if per is not None else "")
        if key not in by_key:
            by_key[key] = {"symbol": sym, "fiscalDate": d, "period": per if per is not None else pd.NA}
        by_key[key].update({
            "freeCashFlow": pick(r, ["freeCashFlow", "freeCashFlow"]),
            # dividendsPaid: raw 저장(부호 유지). payout 등 현금유출 총액 계산 시 abs(dividendsPaid) 사용.
            "dividendsPaid": pick(r, ["dividendsPaid"]),
            "weightedAverageSharesDiluted": pick(r, ["weightedAverageShsOutDil", "weightedAverageSharesDiluted"]),
        })
    if not by_key:
        return pd.DataFrame(columns=FINANCIALS_QUARTERLY_COLUMNS)
    df = pd.DataFrame(list(by_key.values()))
    for c in FINANCIALS_QUARTERLY_COLUMNS:
        if c not in df.columns:
            df[c] = pd.NA
    return df.reindex(columns=FINANCIALS_QUARTERLY_COLUMNS)


def build_dividends_events(
    rows: List[Dict[str, Any]],
    symbol: str,
    cutoff: str,
) -> pd.DataFrame:
    sym = symbol.strip().upper()
    out = []
    for r in rows:
        ex_d = pick_str(r, ["date", "exDividendDate", "exDate"])
        if ex_d is None or ex_d < cutoff:
            continue
        freq = pick_str(r, ["frequency"])
        if freq is None and r.get("frequency") is not None:
            freq = str(r["frequency"])
        out.append({
            "symbol": sym,
            "exDate": ex_d,
            "dividend": pick(r, ["dividend", "dividend"]),
            "adjDividend": pick(r, ["adjDividend", "adjustedDividend"]),
            "recordDate": pick_str(r, ["recordDate", "recordDate"]),
            "paymentDate": pick_str(r, ["paymentDate", "paymentDate"]),
            "declarationDate": pick_str(r, ["declarationDate"]),
            "frequency": freq,
            "yield": pick(r, ["yield", "dividendYield"]) if any(k in r for k in ("yield", "dividendYield")) else None,
        })
    if not out:
        return pd.DataFrame(columns=DIVIDENDS_EVENTS_COLUMNS)
    df = pd.DataFrame(out)
    return df.reindex(columns=DIVIDENDS_EVENTS_COLUMNS)


def build_earnings_events(
    rows: List[Dict[str, Any]],
    symbol: str,
    cutoff: str,
) -> pd.DataFrame:
    sym = symbol.strip().upper()
    out = []
    for r in rows:
        d = pick_str(r, ["date", "fiscalDateEnding", "earningsDate"])
        if d is None or d < cutoff:
            continue
        out.append({
            "symbol": sym,
            "earningsDate": d,
            "epsActual": pick(r, ["epsActual", "eps", "reportedEps"]),
            "epsEstimated": pick(r, ["epsEstimated", "estimatedEps"]),
            "revenueActual": pick(r, ["revenue", "revenueActual"]),
            "revenueEstimated": pick(r, ["revenueEstimated", "estimatedRevenue"]),
            "fiscalDate": pick_str(r, ["fiscalDateEnding", "fiscalDate"]),
        })
    if not out:
        return pd.DataFrame(columns=EARNINGS_EVENTS_COLUMNS)
    return pd.DataFrame(out).reindex(columns=EARNINGS_EVENTS_COLUMNS)


def build_estimates_snapshot(rows: List[Dict[str, Any]], symbol: str, as_of: str) -> pd.DataFrame:
    sym = symbol.strip().upper()
    r = (rows[0] if rows else {}) or {}
    df = pd.DataFrame([{
        "symbol": sym,
        "asOfDate": as_of,
        "epsNextY": pick(r, ["epsNextY", "epsNextYear", "estimatedEpsNextYear"]),
        "epsNextQ": pick(r, ["epsNextQ", "epsNextQuarter", "estimatedEpsNextQuarter"]),
        "epsThisY": pick(r, ["epsThisY", "epsThisYear", "estimatedEpsThisYear"]),
    }])
    return df.reindex(columns=ESTIMATES_SNAPSHOT_COLUMNS)


def build_targets_snapshot(data: Any, symbol: str, as_of: str) -> pd.DataFrame:
    sym = symbol.strip().upper()
    if isinstance(data, list) and data:
        r = data[0]
    elif isinstance(data, dict):
        r = data
    else:
        r = {}
    df = pd.DataFrame([{
        "symbol": sym,
        "asOfDate": as_of,
        "targetPrice": pick(r, ["targetConsensus", "consensus", "priceTarget", "targetPrice", "median"]),
    }])
    return df.reindex(columns=TARGETS_SNAPSHOT_COLUMNS)


def build_shares_snapshot_row(r: Dict[str, Any], as_of: str) -> Dict[str, Any]:
    sym = (r.get("symbol") or r.get("ticker") or "").strip().upper()
    return {
        "symbol": sym,
        "asOfDate": as_of,
        "sharesOutstanding": pick(r, ["outstandingShares", "sharesOutstanding", "commonStockSharesOutstanding"]),
        "sharesFloat": pick(r, ["floatShares", "sharesFloat", "float"]),
    }


# -----------------------------------------------------------------------------
# Upsert: load existing, concat new, dedupe by PK, save
# -----------------------------------------------------------------------------
def upsert_table(
    outdir: Path,
    name: str,
    new_df: pd.DataFrame,
    pk: List[str],
    columns: List[str],
) -> None:
    path_pq = outdir / f"{name}.parquet"
    path_csv = outdir / f"{name}.csv"
    existing = pd.DataFrame()
    if path_pq.exists():
        try:
            existing = pd.read_parquet(path_pq)
        except Exception:
            pass
    if new_df.empty and existing.empty:
        return
    combined = pd.concat([existing, new_df], ignore_index=True)
    for c in columns:
        if c not in combined.columns:
            combined[c] = pd.NA
    combined = combined.reindex(columns=columns)
    pk_avail = [c for c in pk if c in combined.columns]
    if pk_avail:
        combined["_pk"] = combined[pk_avail].astype(str).agg("|".join, axis=1)
        combined = combined.drop_duplicates(subset=["_pk"], keep="last")
        combined = combined.drop(columns=["_pk"])
    sort_cols = [c for c in pk_avail if c in combined.columns]
    if sort_cols:
        combined = combined.sort_values(sort_cols)
    outdir.mkdir(parents=True, exist_ok=True)
    combined.to_parquet(path_pq, index=False, engine="pyarrow")
    combined.to_csv(path_csv, index=False)
    log.info("%s: %s rows", name, len(combined))


# -----------------------------------------------------------------------------
# Mode: backfill / daily
# backfill EOD: per-symbol only (bulk 미사용). full-range 1회 시도 후 실패 시 2년→1년 chunk.
# daily EOD: bulk만 사용, 휴장일 역탐색(daily_lookback).
# -----------------------------------------------------------------------------
def _eod_backfill_symbol(
    sess: requests.Session,
    rl: RateLimiter,
    api_key: str,
    sym: str,
    from_date: str,
    to_date: str,
    call_counter: Dict[str, Any],
) -> pd.DataFrame:
    """심볼별 EOD backfill: 1) full-range 1회 시도 2) 실패 시 2년 chunk → 일부 실패하면 1년 chunk 재시도. concat 후 (symbol,date) dedupe."""
    try:
        rows = fetch_eod_full(sess, rl, api_key, sym, from_date, to_date, call_counter)
        if isinstance(rows, list) and len(rows) > 0:
            return build_prices_eod_from_full(rows, sym)
    except Exception:
        pass

    try:
        from_d = date.fromisoformat(from_date)
        to_d = date.fromisoformat(to_date)
    except ValueError:
        return pd.DataFrame(columns=PRICES_EOD_COLUMNS)

    dfs: List[pd.DataFrame] = []
    # 2-year chunks (729 days inclusive)
    current = from_d
    while current <= to_d:
        chunk_end = min(current + timedelta(days=729), to_d)
        c_from = current.isoformat()
        c_to = chunk_end.isoformat()
        try:
            rows = fetch_eod_full(sess, rl, api_key, sym, c_from, c_to, call_counter)
            if isinstance(rows, list) and len(rows) > 0:
                dfs.append(build_prices_eod_from_full(rows, sym))
        except Exception:
            # Retry this chunk as 1-year sub-chunks
            sub_current = current
            while sub_current <= chunk_end:
                sub_end = min(sub_current + timedelta(days=364), chunk_end)
                try:
                    rows = fetch_eod_full(sess, rl, api_key, sym, sub_current.isoformat(), sub_end.isoformat(), call_counter)
                    if isinstance(rows, list) and len(rows) > 0:
                        dfs.append(build_prices_eod_from_full(rows, sym))
                except Exception:
                    pass
                sub_current = sub_end + timedelta(days=1)
        current = chunk_end + timedelta(days=1)

    if not dfs:
        return pd.DataFrame(columns=PRICES_EOD_COLUMNS)
    out = pd.concat(dfs, ignore_index=True)
    out = out.drop_duplicates(subset=["symbol", "date"], keep="last")
    out = out.reindex(columns=PRICES_EOD_COLUMNS)
    return out


def run_backfill(
    session: requests.Session,
    rl: RateLimiter,
    api_key: str,
    outdir: Path,
    universe: List[str],
    from_date: str,
    to_date: str,
    use_bulk_eod: bool,
    max_workers: int,
    only_symbol: Optional[str],
    call_counter: Dict[str, Any],
) -> None:
    sym_set = set(universe)
    if only_symbol:
        want = {s.strip().upper() for s in only_symbol.split(",") if s.strip()}
        sym_set = sym_set & want
        universe = [s for s in universe if s in sym_set]

    today = date.today().isoformat()

    # (1) prices_eod — backfill에서는 bulk 미사용, 심볼별 full → adaptive chunk만 사용.
    def _eod_one(sym: str) -> pd.DataFrame:
        sess = make_session()
        return _eod_backfill_symbol(sess, rl, api_key, sym, from_date, to_date, call_counter)

    eod_dfs: List[pd.DataFrame] = []
    if max_workers <= 1:
        for sym in universe:
            try:
                eod_dfs.append(_eod_one(sym))
            except Exception as e:
                log.debug("%s eod: %s", sym, e)
                eod_dfs.append(pd.DataFrame(columns=PRICES_EOD_COLUMNS))
    else:
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            for df in ex.map(_eod_one, universe):
                eod_dfs.append(df)
    non_empty_eod = [d for d in eod_dfs if not d.empty]
    if non_empty_eod:
        new_eod = pd.concat(non_empty_eod, ignore_index=True)
        new_eod = new_eod.drop_duplicates(subset=["symbol", "date"], keep="last")
        new_eod = new_eod.reindex(columns=PRICES_EOD_COLUMNS)
        upsert_table(outdir, "prices_eod", new_eod, ["symbol", "date"], PRICES_EOD_COLUMNS)

    # (2) financials_quarterly
    def _fin_one(sym: str) -> pd.DataFrame:
        sess = make_session()
        try:
            inc = fetch_income(sess, rl, api_key, sym, 80, call_counter)
            bal = fetch_balance(sess, rl, api_key, sym, 80, call_counter)
            cf = fetch_cashflow(sess, rl, api_key, sym, 80, call_counter)
            return build_financials_quarterly(inc, bal, cf, sym, CUTOFF_DATE)
        except Exception as e:
            log.debug("%s financials: %s", sym, e)
            return pd.DataFrame(columns=FINANCIALS_QUARTERLY_COLUMNS)

    if max_workers <= 1:
        fin_dfs = [_fin_one(s) for s in universe]
    else:
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            fin_dfs = list(ex.map(_fin_one, universe))
    non_empty_fin = [d for d in fin_dfs if not d.empty]
    if non_empty_fin:
        fin_combined = pd.concat(non_empty_fin, ignore_index=True)
        upsert_table(outdir, "financials_quarterly", fin_combined, ["symbol", "fiscalDate", "period"], FINANCIALS_QUARTERLY_COLUMNS)

    # (3) dividends_events
    def _div_one(sym: str) -> pd.DataFrame:
        sess = make_session()
        try:
            rows = fetch_dividends(sess, rl, api_key, sym, call_counter)
            return build_dividends_events(rows, sym, CUTOFF_DATE)
        except Exception as e:
            log.debug("%s dividends: %s", sym, e)
            return pd.DataFrame(columns=DIVIDENDS_EVENTS_COLUMNS)

    if max_workers <= 1:
        div_dfs = [_div_one(s) for s in universe]
    else:
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            div_dfs = list(ex.map(_div_one, universe))
    non_empty_div = [d for d in div_dfs if not d.empty]
    if non_empty_div:
        div_combined = pd.concat(non_empty_div, ignore_index=True)
        div_combined["_dedupe"] = div_combined["paymentDate"].astype(str)
        div_combined = div_combined.drop_duplicates(subset=["symbol", "exDate", "_dedupe", "dividend"], keep="last")
        div_combined = div_combined.drop(columns=["_dedupe"], errors="ignore")
        upsert_table(outdir, "dividends_events", div_combined, ["symbol", "exDate", "paymentDate", "dividend"], DIVIDENDS_EVENTS_COLUMNS)

    # (4) earnings_events
    def _earn_one(sym: str) -> pd.DataFrame:
        sess = make_session()
        try:
            rows = fetch_earnings(sess, rl, api_key, sym, 400, call_counter)
            return build_earnings_events(rows, sym, CUTOFF_DATE)
        except Exception as e:
            log.debug("%s earnings: %s", sym, e)
            return pd.DataFrame(columns=EARNINGS_EVENTS_COLUMNS)

    if max_workers <= 1:
        earn_dfs = [_earn_one(s) for s in universe]
    else:
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            earn_dfs = list(ex.map(_earn_one, universe))
    non_empty_earn = [d for d in earn_dfs if not d.empty]
    if non_empty_earn:
        earn_combined = pd.concat(non_empty_earn, ignore_index=True)
        upsert_table(outdir, "earnings_events", earn_combined, ["symbol", "earningsDate"], EARNINGS_EVENTS_COLUMNS)

    # (5)(6)(7) snapshots: current run only
    as_of = today
    est_rows, tgt_rows, sh_rows = [], [], []

    for sym in universe:
        try:
            ae = fetch_analyst_estimates(session, rl, api_key, sym, call_counter)
            est_rows.append(build_estimates_snapshot(ae, sym, as_of))
        except Exception:
            pass
        try:
            pt = fetch_price_target_consensus(session, rl, api_key, sym, call_counter)
            tgt_rows.append(build_targets_snapshot(pt, sym, as_of))
        except Exception:
            pass

    if est_rows:
        est_df = pd.concat(est_rows, ignore_index=True)
        upsert_table(outdir, "estimates_snapshot", est_df, ["symbol", "asOfDate"], ESTIMATES_SNAPSHOT_COLUMNS)
    if tgt_rows:
        tgt_df = pd.concat(tgt_rows, ignore_index=True)
        upsert_table(outdir, "targets_snapshot", tgt_df, ["symbol", "asOfDate"], TARGETS_SNAPSHOT_COLUMNS)

    page = 0
    while True:
        try:
            sh_list = fetch_shares_float_all(session, rl, api_key, page, 1000, call_counter)
            if not sh_list:
                break
            for r in sh_list:
                sym = (r.get("symbol") or r.get("ticker") or "").strip().upper()
                if sym not in sym_set:
                    continue
                sh_rows.append(build_shares_snapshot_row(r, as_of))
            if len(sh_list) < 1000:
                break
            page += 1
        except Exception as e:
            log.warning("shares_float_all page %s: %s", page, e)
            break
    if sh_rows:
        sh_df = pd.DataFrame(sh_rows).reindex(columns=SHARES_SNAPSHOT_COLUMNS)
        upsert_table(outdir, "shares_snapshot", sh_df, ["symbol", "asOfDate"], SHARES_SNAPSHOT_COLUMNS)

    # (8) index_membership: TODO if FMP stable index-constituents available
    # Placeholder: empty table with correct schema
    idx_df = pd.DataFrame(columns=INDEX_MEMBERSHIP_COLUMNS)
    if not (outdir / "index_membership.parquet").exists():
        outdir.mkdir(parents=True, exist_ok=True)
        idx_df.to_parquet(outdir / "index_membership.parquet", index=False)
        idx_df.to_csv(outdir / "index_membership.csv", index=False)


def run_daily(
    session: requests.Session,
    rl: RateLimiter,
    api_key: str,
    outdir: Path,
    universe: List[str],
    from_date: str,
    to_date: str,
    use_bulk_eod: bool,
    max_workers: int,
    only_symbol: Optional[str],
    call_counter: Dict[str, Any],
    daily_lookback: int = 7,
) -> None:
    """Daily 모드: EOD는 bulk만 사용. to_date(기본 yesterday)부터 daily_lookback일 역탐색해 non-empty 첫 거래일로 upsert."""

    def find_latest_trade_date_with_bulk(
        sess: requests.Session,
        rate_limiter: RateLimiter,
        key: str,
        start_date: str,
        lookback_days: int,
        counter: Dict[str, Any],
    ) -> Tuple[Optional[str], Optional[List[Dict[str, Any]]]]:
        """start_date 포함 최대 lookback_days일 내려가며 eod-bulk가 non-empty인 첫 (날짜, bulk) 반환. 전부 실패 시 (None, None)."""
        try:
            current = date.fromisoformat(start_date)
        except ValueError:
            return (None, None)
        for _ in range(lookback_days):
            d_str = current.isoformat()
            try:
                bulk_rows = fetch_eod_bulk(sess, rate_limiter, key, d_str, counter)
                if isinstance(bulk_rows, list) and len(bulk_rows) > 0:
                    return (d_str, bulk_rows)
            except Exception:
                pass
            current -= timedelta(days=1)
        return (None, None)

    sym_set = set(universe)
    if only_symbol:
        want = {s.strip().upper() for s in only_symbol.split(",") if s.strip()}
        sym_set = sym_set & want
        universe = [s for s in universe if s in sym_set]

    today = date.today().isoformat()
    # EOD: daily에서는 bulk만 사용. to_date가 휴장일이면 daily_lookback일 역탐색.
    if use_bulk_eod:
        trade_date, bulk = find_latest_trade_date_with_bulk(
            session, rl, api_key, to_date, daily_lookback, call_counter
        )
        if trade_date is None:
            log.warning(
                "daily prices_eod: no bulk data found within %s days back from %s; skipping prices_eod update",
                daily_lookback,
                to_date,
            )
        else:
            try:
                new_eod = build_prices_eod_from_bulk(bulk or [], sym_set, trade_date)
                if not new_eod.empty:
                    upsert_table(outdir, "prices_eod", new_eod, ["symbol", "date"], PRICES_EOD_COLUMNS)
                log.info(
                    "daily prices_eod: using trade_date=%s (requested to_date=%s)",
                    trade_date,
                    to_date,
                )
            except Exception as e:
                log.warning("eod-bulk %s: %s", trade_date, e)
    else:
        eod_dfs = []
        for sym in universe:
            try:
                rows = fetch_eod_full(session, rl, api_key, sym, to_date, to_date, call_counter)
                eod_dfs.append(build_prices_eod_from_full(rows, sym))
            except Exception:
                pass
        non_empty_eod = [d for d in eod_dfs if not d.empty]
        if non_empty_eod:
            new_eod = pd.concat(non_empty_eod, ignore_index=True)
            upsert_table(outdir, "prices_eod", new_eod, ["symbol", "date"], PRICES_EOD_COLUMNS)

    as_of = today
    est_rows, tgt_rows = [], []
    for sym in universe:
        try:
            ae = fetch_analyst_estimates(session, rl, api_key, sym, call_counter)
            est_rows.append(build_estimates_snapshot(ae, sym, as_of))
        except Exception:
            pass
        try:
            pt = fetch_price_target_consensus(session, rl, api_key, sym, call_counter)
            tgt_rows.append(build_targets_snapshot(pt, sym, as_of))
        except Exception:
            pass
    if est_rows:
        upsert_table(outdir, "estimates_snapshot", pd.concat(est_rows, ignore_index=True), ["symbol", "asOfDate"], ESTIMATES_SNAPSHOT_COLUMNS)
    if tgt_rows:
        upsert_table(outdir, "targets_snapshot", pd.concat(tgt_rows, ignore_index=True), ["symbol", "asOfDate"], TARGETS_SNAPSHOT_COLUMNS)

    page = 0
    sh_rows = []
    while True:
        try:
            sh_list = fetch_shares_float_all(session, rl, api_key, page, 1000, call_counter)
            if not sh_list:
                break
            for r in sh_list:
                sym = (r.get("symbol") or r.get("ticker") or "").strip().upper()
                if sym not in sym_set:
                    continue
                sh_rows.append(build_shares_snapshot_row(r, as_of))
            if len(sh_list) < 1000:
                break
            page += 1
        except Exception:
            break
    if sh_rows:
        sh_df = pd.DataFrame(sh_rows).reindex(columns=SHARES_SNAPSHOT_COLUMNS)
        upsert_table(outdir, "shares_snapshot", sh_df, ["symbol", "asOfDate"], SHARES_SNAPSHOT_COLUMNS)


def main() -> None:
    ap = argparse.ArgumentParser(description="FMP Premium: 8 tables (prices_eod, financials_quarterly, ...)")
    ap.add_argument("--universe", required=True, help="universe_list.csv path")
    ap.add_argument("--outdir", required=True, help="Output directory")
    ap.add_argument("--from", dest="from_date", default="2020-01-01", help="Start date YYYY-MM-DD")
    ap.add_argument("--to", dest="to_date", default="", help="End date YYYY-MM-DD (backfill: today; daily: yesterday)")
    ap.add_argument("--max-workers", type=int, default=4, help="ThreadPool workers")
    ap.add_argument("--mode", choices=["backfill", "daily"], default="backfill")
    ap.add_argument("--only-symbol", default="", help="Optional: AAPL,MSFT")
    ap.add_argument("--use-bulk-eod", action="store_true", default=True, help="Use EOD bulk only in daily mode (backfill always uses per-symbol)")
    ap.add_argument("--no-bulk-eod", action="store_false", dest="use_bulk_eod")
    ap.add_argument("--daily-lookback", type=int, default=7, help="Daily mode: max days back from --to to find a trading day with EOD bulk (default 7)")
    ap.add_argument("--num-shards", type=int, default=1, help="Shard universe into N parts (default 1 = no sharding)")
    ap.add_argument("--shard-id", type=int, default=0, help="Which shard to process (0..num_shards-1)")
    ap.add_argument("--save-raw", action="store_true", help="Optional: save raw JSON")
    args = ap.parse_args()

    api_key = (os.environ.get(API_KEY_ENV) or "").strip()
    if not api_key:
        raise RuntimeError("FMP_API_KEY environment variable is not set.")

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    from_date = (getattr(args, "from_date") or "2020-01-01").strip()[:10]
    to_date = (getattr(args, "to_date") or "").strip()[:10]
    if not to_date:
        to_date = date.today().isoformat() if args.mode == "backfill" else (date.today() - timedelta(days=1)).isoformat()

    universe = read_universe(args.universe)
    num_shards = getattr(args, "num_shards", 1) or 1
    shard_id = getattr(args, "shard_id", 0)
    universe = filter_by_shard(universe, num_shards, shard_id)
    if num_shards > 1:
        log.info("Universe: %s symbols (shard %s/%s)", len(universe), shard_id, num_shards)
    else:
        log.info("Universe: %s symbols", len(universe))

    rl = RateLimiter(MIN_INTERVAL)
    call_counter: Dict[str, Any] = {"count": 0, "lock": threading.Lock()}
    session = make_session()

    if args.mode == "backfill":
        # Backfill: EOD는 항상 심볼별 full/chunk만 사용(bulk 미사용).
        run_backfill(
            session, rl, api_key, outdir, universe,
            from_date, to_date,
            use_bulk_eod=False,
            max_workers=args.max_workers,
            only_symbol=args.only_symbol or None,
            call_counter=call_counter,
        )
    else:
        # Daily: EOD는 bulk 1회 + 휴장일 역탐색만 사용.
        run_daily(
            session, rl, api_key, outdir, universe,
            from_date, to_date,
            getattr(args, "use_bulk_eod", True),
            args.max_workers,
            args.only_symbol or None,
            call_counter,
            daily_lookback=getattr(args, "daily_lookback", 7),
        )

    log.info("API calls: %s", call_counter.get("count", 0))


if __name__ == "__main__":
    main()
