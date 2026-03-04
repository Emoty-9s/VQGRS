# -*- coding: utf-8 -*-
r"""
FMP Premium: 11개 테이블 수집 (prices_eod, financials_quarterly, dividends_events, earnings_events,
estimates_snapshot, targets_snapshot, company_facts_snapshot, insider_*, index_membership).
API Key: 환경변수 FMP_API_KEY (로그/예외/출력 노출 금지). 결측 NaN 유지. 저장은 PK 기준 Upsert.
prices_eod의 adjClose는 dividend-adjusted API 또는 배당/스플릿 이벤트 기반 계산으로 채운다.

모드별 실행 테이블 (스케줄 정책):
  | 모드      | prices_eod              | estimates/targets | company_facts / index / insider     | 트리거(earnings/dividends)   |
  |-----------|-------------------------|-------------------|-------------------------------------|------------------------------|
  | backfill  | O (per-symbol full)     | O                 | O (항상: index_membership, company_facts_snapshot, insider_*) | O (전체 full fetch)          |
  | daily     | O (per-symbol 증분만)   | X                 | X                                   | X                            |
  | weekly    | X                       | O (스냅샷만)      | X                                   | X                            |
  | monthly   | X                       | X                 | O (항상: index_membership, company_facts_snapshot, insider_*) | X                    |
  | trigger   | X                       | X                 | X                                   | O (dividends+earnings+financials; 1~5일 보험) |

트리거(trigger 모드): (A) earnings-calendar → earnings_events upsert + earnings_hit_symbols 수집 (B) dividends-calendar → dividends_events upsert (C) financials_quarterly: earnings_hit_symbols만 갱신. 단 매월 1~5일(현지)에는 보험으로 shard universe 전체 갱신(자동, 옵션 없음).
backfill/monthly: index_membership, company_facts_snapshot, insider_transactions, insider_holdings_snapshot는 옵션 없이 항상 실행. 제한(402/403) 시 해당 소스만 스킵·가능 범위만 저장; 빈 결과로 기존 파일 덮어쓰지 않음.

실행 예:
  python fmp_universe_fetch.py --universe ./data/universe_list.csv --outdir ./data --mode daily
  python fmp_universe_fetch.py --universe ./data/universe_list.csv --outdir ./data --mode weekly
  python fmp_universe_fetch.py --universe ./data/universe_list.csv --outdir ./data --mode monthly --index-symbols SP500,NASDAQ
  python fmp_universe_fetch.py --universe ./data/universe_list.csv --outdir ./data --mode backfill --from 2020-01-01
  python fmp_universe_fetch.py --universe ./data/universe_list.csv --outdir ./data --mode trigger
  python fmp_universe_fetch.py --universe ./data/universe_list.csv --outdir ./data --mode trigger --earnings-use-lastupdated
  python fmp_universe_fetch.py --universe ./data/universe_list.csv --outdir ./data --mode trigger --verify-dividends-calendar-exdate AAPL
"""
from __future__ import annotations

import argparse
import hashlib
import logging
import os
import threading
import time
import zlib
from concurrent.futures import ThreadPoolExecutor
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

PATH_EOD_FULL = "/stable/historical-price-eod/full"
PATH_PROFILE = "/stable/profile"
PATH_EOD_DIVIDEND_ADJUSTED = "/stable/historical-price-eod/dividend-adjusted"
PATH_SPLITS = "/stable/splits"
PATH_INCOME = "/stable/income-statement"
PATH_BALANCE = "/stable/balance-sheet-statement"
PATH_CASHFLOW = "/stable/cash-flow-statement"
PATH_CASHFLOW_AS_REPORTED = "/stable/cash-flow-statement-as-reported"
PATH_BALANCE_AS_REPORTED = "/stable/balance-sheet-statement-as-reported"
PATH_DIVIDENDS = "/stable/dividends"
PATH_DIVIDENDS_CALENDAR = "/stable/dividends-calendar"
PATH_EARNINGS = "/stable/earnings-company"
PATH_EARNINGS_CALENDAR = "/stable/earnings-calendar"
PATH_ANALYST_ESTIMATES = "/stable/analyst-estimates"
PATH_PRICE_TARGET_CONSENSUS = "/stable/price-target-consensus"
PATH_SHARES_FLOAT_ALL = "/stable/shares-float-all"
PATH_INSIDER_TRADING_SEARCH = "/stable/insider-trading/search"
PATH_SP500 = "/stable/sp500-constituent"
PATH_NASDAQ = "/stable/nasdaq-constituent"
PATH_DOWJONES = "/stable/dowjones-constituent"

# Table schemas (column order = final output)
PRICES_EOD_COLUMNS = ["symbol", "date", "open", "high", "low", "close", "adjClose", "volume"]
ADJCLOSE_COLUMNS = ["symbol", "date", "adjClose"]

# adjClose fill: trigger dividend-adjusted fetch when missing >= 5%; fallback when still > 80%
ADJCLOSE_MISSING_RATE_THRESHOLD = 0.05
ADJCLOSE_FALLBACK_MISSING_RATE = 0.80
ADJCLOSE_WARN_MISSING_RATE = 0.20
ADJCLOSE_TOP_SYMBOLS_WARN = 20
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
FINANCIALS_PK = ["symbol", "fiscalDate", "period"]
FINANCIALS_COMPARE_COLUMNS = [
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
EARNINGS_EVENTS_COLUMNS_WITH_LASTUPDATED = EARNINGS_EVENTS_COLUMNS + ["lastUpdated"]
ESTIMATES_SNAPSHOT_COLUMNS = ["symbol", "asOfDate", "epsNextY", "epsNextQ", "epsThisY", "epsNext5Y"]
ESTIMATES_QUARTERLY_SNAPSHOT_COLUMNS = ["symbol", "asOfDate", "epsNextQ"]
TARGETS_SNAPSHOT_COLUMNS = ["symbol", "asOfDate", "targetPrice"]
SHARES_SNAPSHOT_COLUMNS = ["symbol", "asOfDate", "sharesOutstanding", "sharesFloat"]
INDEX_MEMBERSHIP_COLUMNS = ["indexSymbol", "asOfDate", "memberSymbol", "isMember"]
COMPANY_FACTS_SNAPSHOT_COLUMNS = [
    "symbol", "asOfDate", "sector", "industry", "employees", "ipoDate",
    "sharesOutstanding_profile", "sharesOutstanding_shares", "sharesFloat",
]
INSIDER_TRANSACTIONS_COLUMNS = [
    "symbol", "transactionDate", "reportingCik", "reportingName", "transactionType",
    "securitiesTransacted", "price", "value", "securitiesOwned",
    "securityName", "formType", "acquisitionOrDisposition", "link",
]
INSIDER_HOLDINGS_SNAPSHOT_COLUMNS = [
    "symbol", "asOfDate", "reportingCik", "reportingName", "securitiesOwned", "securityName", "lastTransactionDate",
]

CUTOFF_DATE = "2020-01-01"

def load_latest_prices_date_map(outdir: Path) -> Dict[str, str]:
    """prices_eod.parquet이 있으면 symbol별 max(date)를 반환. 없으면 빈 dict."""
    path = outdir / "prices_eod.parquet"
    if not path.exists():
        return {}
    try:
        df = pd.read_parquet(path)
        if df.empty or "symbol" not in df.columns or "date" not in df.columns:
            return {}
        return df.groupby("symbol", as_index=False)["date"].max().set_index("symbol")["date"].astype(str).to_dict()
    except Exception:
        return {}


def compute_target_trade_date(today: date) -> str:
    """토/일 → 직전 금요일, 월 → 직전 금요일, 화~금 → 어제."""
    w = today.weekday()  # Mon=0 .. Sun=6
    if w >= 5:  # Sat=5, Sun=6 -> Friday = today - 1 or 2
        delta = today.weekday() - 4
        target = today - timedelta(days=delta)
    elif w == 0:  # Monday -> previous Friday
        target = today - timedelta(days=3)
    else:  # Tue=1 .. Fri=4 -> yesterday
        target = today - timedelta(days=1)
    return target.isoformat()


def _safe_log_message(ex: Exception) -> str:
    """Return a short message for logging without risking API key exposure."""
    return type(ex).__name__ + (f": {str(ex)[:80]}" if str(ex) and "apikey" not in str(ex).lower() and "api_key" not in str(ex).lower() else "")


def make_insider_id(
    reporting_cik: Optional[str],
    reporting_name: Optional[str],
    link: Optional[str],
    *,
    symbol: Optional[str] = None,
    transaction_date: Optional[str] = None,
    transaction_type: Optional[str] = None,
    securities_transacted: Optional[Any] = None,
    price: Optional[Any] = None,
    value: Optional[Any] = None,
    security_name: Optional[str] = None,
) -> str:
    """Stable PK: use CIK if present; else hash of name|link. If all empty, hash of transaction fields to avoid PK collision."""
    if reporting_cik and str(reporting_cik).strip():
        return str(reporting_cik).strip()
    base = (reporting_name or "") + "|" + (link or "")
    if not base.strip():
        fallback = "|".join(
            str(x) for x in (
                symbol,
                transaction_date,
                transaction_type,
                securities_transacted,
                price,
                value,
                security_name,
            )
        )
        h = hashlib.sha1(fallback.encode("utf-8")).hexdigest()[:16]
        return "R_" + h
    h = hashlib.sha1(base.encode("utf-8")).hexdigest()[:16]
    return "H_" + h


def _sanitize_snippet(text: str, max_len: int = 120) -> str:
    """응답 바디 앞부분만 추출. apikey/URL 쿼리 노출 시 ***로 치환. 로그용."""
    if not text:
        return ""
    s = text[:max_len].replace("\n", " ").replace("\r", " ").strip()
    if "apikey" in s.lower() or "api_key" in s.lower():
        return "***"
    return s.replace('"', "'")


def _rate_limit_headers(headers: Any) -> Dict[str, str]:
    """Rate-limit 관련 헤더만 추출. 키는 소문자, 값은 문자열. apikey 포함 시 노출 안 함."""
    out: Dict[str, str] = {}
    if not headers:
        return {}
    try:
        h = dict(headers) if not isinstance(headers, dict) else headers
    except Exception:
        return {}
    for name in ["Retry-After", "X-RateLimit-Remaining", "X-RateLimit-Reset", "X-RateLimit-Limit", "Limit"]:
        for k, v in h.items():
            if k and v is not None and name.lower() == str(k).lower():
                out[name] = str(v).strip()
                break
    return out


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
    return_status: bool = False,
) -> Any:
    """GET 요청. return_status=False면 기존처럼 data만 반환(4xx/5xx 시 예외). return_status=True면 (status_code, data_or_text, headers) 반환하고 예외 없음."""
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

    if return_status:
        headers = dict(r.headers) if r.headers else {}
        if r.status_code == 404 and allow_404_empty:
            return (404, [], headers)
        if r.status_code >= 400:
            return (r.status_code, (r.text if r.text is not None else ""), headers)
        try:
            data = r.json()
        except Exception:
            data = r.text if r.text is not None else ""
        return (r.status_code, data, headers)

    if r.status_code == 404 and allow_404_empty:
        return []
    if r.status_code >= 400:
        raise RuntimeError(f"HTTP {r.status_code}: {path}")
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


def pick_date10(d: Dict[str, Any], keys: List[str]) -> Optional[str]:
    """Date only: return first 10 chars (YYYY-MM-DD). Use for date fields only."""
    for k in keys:
        v = d.get(k)
        if v is None:
            continue
        s = str(v).strip()
        if s:
            return s[:10]
    return None


def pick_text(d: Dict[str, Any], keys: List[str]) -> Optional[str]:
    """Full string, no truncation. Use for names, URLs, descriptions."""
    for k in keys:
        v = d.get(k)
        if v is None:
            continue
        s = str(v).strip()
        if s:
            return s
    return None


# -----------------------------------------------------------------------------
# Fetch functions
# -----------------------------------------------------------------------------
def fetch_profile_symbol(
    session: requests.Session,
    rl: RateLimiter,
    api_key: str,
    symbol: str,
    call_counter: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """Per-symbol profile (employees, ipoDate, sharesOutstanding). 403/restricted 시 호출부에서 비활성화."""
    data = fmp_get(
        session, rl, PATH_PROFILE,
        {"symbol": symbol},
        api_key,
        call_counter=call_counter,
        allow_404_empty=True,
    )
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        return [data]
    return []


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


def fetch_eod_dividend_adjusted(
    session: requests.Session,
    rl: RateLimiter,
    api_key: str,
    symbol: str,
    from_date: str,
    to_date: str,
    call_counter: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    data = fmp_get(
        session, rl, PATH_EOD_DIVIDEND_ADJUSTED,
        {"symbol": symbol, "from": from_date, "to": to_date},
        api_key,
        call_counter=call_counter,
        allow_404_empty=True,
    )
    return data if isinstance(data, list) else []


def fetch_splits(
    session: requests.Session,
    rl: RateLimiter,
    api_key: str,
    symbol: str,
    call_counter: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    data = fmp_get(
        session, rl, PATH_SPLITS,
        {"symbol": symbol},
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


def fetch_cashflow_as_reported(
    session: requests.Session,
    rl: RateLimiter,
    api_key: str,
    symbol: str,
    limit: int,
    call_counter: Optional[Dict[str, Any]] = None,
) -> Tuple[Optional[List[Dict[str, Any]]], bool]:
    """Returns (data_list_or_None, restricted). restricted=True if 402/403 so caller should disable fallback."""
    status, data, _ = fmp_get(
        session, rl, PATH_CASHFLOW_AS_REPORTED,
        {"symbol": symbol, "period": "quarter", "limit": limit},
        api_key,
        call_counter=call_counter,
        allow_404_empty=True,
        return_status=True,
    )
    if status in (402, 403):
        return (None, True)
    if status >= 400:
        return (None, False)
    return (data if isinstance(data, list) else [], False)


def fetch_balance_as_reported(
    session: requests.Session,
    rl: RateLimiter,
    api_key: str,
    symbol: str,
    limit: int,
    call_counter: Optional[Dict[str, Any]] = None,
) -> Tuple[Optional[List[Dict[str, Any]]], bool]:
    """Returns (data_list_or_None, restricted). restricted=True if 402/403 so caller should disable fallback."""
    status, data, _ = fmp_get(
        session, rl, PATH_BALANCE_AS_REPORTED,
        {"symbol": symbol, "period": "quarter", "limit": limit},
        api_key,
        call_counter=call_counter,
        allow_404_empty=True,
        return_status=True,
    )
    if status in (402, 403):
        return (None, True)
    if status >= 400:
        return (None, False)
    return (data if isinstance(data, list) else [], False)


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


def fetch_dividends_calendar(
    session: requests.Session,
    rl: RateLimiter,
    api_key: str,
    from_date: str,
    to_date: str,
    call_counter: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """GET /stable/dividends-calendar?from=&to=. Returns list of rows (no symbol filter)."""
    data = fmp_get(
        session, rl, PATH_DIVIDENDS_CALENDAR,
        {"from": from_date[:10], "to": to_date[:10]},
        api_key,
        call_counter=call_counter,
        allow_404_empty=True,
    )
    return data if isinstance(data, list) else []


def fetch_earnings_calendar(
    session: requests.Session,
    rl: RateLimiter,
    api_key: str,
    from_date: str,
    to_date: str,
    call_counter: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """GET /stable/earnings-calendar?from=&to=. Returns list of rows (no symbol filter)."""
    data = fmp_get(
        session, rl, PATH_EARNINGS_CALENDAR,
        {"from": from_date[:10], "to": to_date[:10]},
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
    *,
    period: str = "annual",
    page: int = 0,
    limit: int = 10,
) -> List[Dict[str, Any]]:
    """GET /stable/analyst-estimates. period=annual|quarter, page, limit required to avoid 400."""
    data = fmp_get(
        session, rl, PATH_ANALYST_ESTIMATES,
        {"symbol": symbol, "period": period, "page": page, "limit": limit},
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


def fetch_insider_trading_stable(
    session: requests.Session,
    rl: RateLimiter,
    api_key: str,
    symbol: str,
    page: int = 0,
    limit: int = 100,
    call_counter: Optional[Dict[str, Any]] = None,
    return_status: bool = False,
) -> Any:
    """Stable insider-trading/search by symbol. return_status=False: return list or raise. return_status=True: return (rows, None) on success or (None, (code, body_text, headers)) on 4xx/5xx (no raise)."""
    params: Dict[str, Any] = {"symbol": symbol, "page": page, "limit": limit}
    if return_status:
        code, body, headers = fmp_get(
            session, rl, PATH_INSIDER_TRADING_SEARCH,
            params, api_key,
            call_counter=call_counter,
            allow_404_empty=True,
            return_status=True,
        )
        if code >= 400:
            return (None, (code, body, headers))
        if isinstance(body, list):
            return (body, None)
        return ([], None)
    data = fmp_get(
        session, rl, PATH_INSIDER_TRADING_SEARCH,
        params, api_key,
        call_counter=call_counter,
        allow_404_empty=True,
    )
    return data if isinstance(data, list) else []


def fetch_index_constituents(
    session: requests.Session,
    rl: RateLimiter,
    api_key: str,
    index_symbol: str,
    call_counter: Optional[Dict[str, Any]] = None,
) -> Set[str]:
    path_map = {
        "SP500": PATH_SP500,
        "NASDAQ": PATH_NASDAQ,
        "DOWJONES": PATH_DOWJONES,
    }
    path = path_map.get(index_symbol.upper())
    if not path:
        return set()
    data = fmp_get(
        session, rl, path,
        {},
        api_key,
        call_counter=call_counter,
        allow_404_empty=True,
    )
    if not isinstance(data, list):
        return set()
    members = set()
    for row in data:
        sym = (row.get("symbol") or row.get("memberSymbol") or row.get("ticker") or "").strip().upper()
        if sym:
            members.add(sym)
    return members


# -----------------------------------------------------------------------------
# Build DataFrames from API responses
# -----------------------------------------------------------------------------
def build_prices_eod_from_full(
    full_rows: List[Dict[str, Any]],
    symbol: str,
) -> pd.DataFrame:
    sym = symbol.strip().upper()
    rows = []
    for r in full_rows:
        d = pick_date10(r, ["date"])
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


def build_adjclose_from_dividend_adjusted(
    rows: List[Dict[str, Any]],
    symbol: str,
    cutoff: str,
    to_date: str,
) -> pd.DataFrame:
    sym = symbol.strip().upper()
    out = []
    for r in rows:
        d = pick_date10(r, ["date"])
        if d is None or d < cutoff or d > to_date:
            continue
        adj = pick(r, ["adjClose", "adjustedClose", "adj_close"])
        if adj is None:
            continue
        out.append({"symbol": sym, "date": d, "adjClose": adj})
    if not out:
        return pd.DataFrame(columns=ADJCLOSE_COLUMNS)
    return pd.DataFrame(out).reindex(columns=ADJCLOSE_COLUMNS)


def _parse_split_ratio(row: Dict[str, Any]) -> Tuple[Optional[str], Optional[float]]:
    """Return (date_str, denominator/numerator) for backward adj. None if invalid."""
    d = pick_date10(row, ["date", "splitDate"])
    if not d:
        return (None, None)
    num = pick(row, ["numerator"])
    denom = pick(row, ["denominator"])
    if num is not None and denom is not None and float(denom) != 0:
        return (d, float(denom) / float(num))
    ratio_str = row.get("splitRatio") or row.get("ratio")
    if isinstance(ratio_str, str) and "/" in ratio_str:
        parts = ratio_str.strip().split("/")
        if len(parts) == 2:
            try:
                n, d_val = float(parts[0].strip()), float(parts[1].strip())
                if n != 0:
                    return (d, d_val / n)
            except ValueError:
                pass
    return (d, None)


def compute_adjclose_from_events(
    price_df: pd.DataFrame,
    dividends_df: pd.DataFrame,
    splits_rows: List[Dict[str, Any]],
) -> pd.Series:
    """Backward adjustment: latest factor=1. Returns adjClose series (index = price_df.index). NaN on failure."""
    if price_df.empty or "date" not in price_df.columns or "close" not in price_df.columns:
        return pd.Series(dtype=float, index=price_df.index)
    df = price_df.sort_values("date")
    df = df.dropna(subset=["close"])
    if df.empty:
        return pd.Series(dtype=float, index=price_df.index)

    split_by_date: Dict[str, float] = {}
    for r in splits_rows:
        d, ratio = _parse_split_ratio(r)
        if d and ratio is not None and ratio > 0:
            split_by_date[d] = ratio

    div_by_date: Dict[str, float] = {}
    if not dividends_df.empty and "exDate" in dividends_df.columns and "dividend" in dividends_df.columns:
        for _, row in dividends_df.iterrows():
            ex_d = row.get("exDate")
            div = row.get("dividend")
            if pd.notna(ex_d) and pd.notna(div):
                try:
                    div_by_date[str(ex_d)[:10]] = float(div)
                except (TypeError, ValueError):
                    pass
    out = pd.Series(index=df.index, dtype=float)
    factor = 1.0
    for i in range(len(df) - 1, -1, -1):
        idx = df.index[i]
        row = df.loc[idx]
        d = str(row["date"])[:10]
        close = float(row["close"])
        adj = close * factor
        out.loc[idx] = adj
        if d in split_by_date:
            factor *= split_by_date[d]
        if d in div_by_date:
            D = div_by_date[d]
            # Use previous trading day close for dividend adjustment (more accurate than exDate same-day)
            P_prev = float(df.iloc[i - 1]["close"]) if i > 0 else close
            if P_prev > 0:
                factor *= (P_prev - D) / P_prev
    return out


def _norm_period(x: Any) -> str:
    """period 정규화: 매칭 깨짐 방지 (None/NA/<NA>/nan/None → "")."""
    if x is None or pd.isna(x):
        return ""
    s = str(x).strip()
    return "" if s in ("<NA>", "nan", "None") else s


def _to_float(v: Any) -> Optional[float]:
    try:
        if v is None:
            return None
        if isinstance(v, str):
            s = v.strip().replace(",", "")
            if not s:
                return None
            return float(s)
        if isinstance(v, (int, float)):
            return float(v)
        return None
    except Exception:
        return None


def _iter_as_reported_line_items(row: Dict[str, Any]) -> List[Tuple[str, float]]:
    data = row.get("data")
    out: List[Tuple[str, float]] = []

    # 실제 응답: dict 케이스 (paymentsofdividends, commonstocksharesoutstanding 등)
    if isinstance(data, dict):
        for k, v in data.items():
            name = str(k).strip()
            if not name:
                continue
            vv = _to_float(v)
            if vv is None:
                continue
            out.append((name, vv))
        return out

    # list 케이스 (다른 심볼/엔드포인트에서 list로 올 때)
    if isinstance(data, list):
        name_keys = ("name", "label", "account", "lineItem", "title", "concept", "tag")
        value_keys = ("value", "amount", "val", "number", "raw", "usd", "valueUSD")
        for it in data:
            if not isinstance(it, dict):
                continue
            nm = None
            for nk in name_keys:
                if nk in it and it[nk] is not None and str(it[nk]).strip():
                    nm = str(it[nk]).strip()
                    break
            if not nm:
                continue
            vv = None
            for vk in value_keys:
                if vk in it:
                    vv = _to_float(it.get(vk))
                    if vv is not None:
                        break
            if vv is None and isinstance(it.get("value"), dict):
                vv = _to_float(it["value"].get("raw")) or _to_float(it["value"].get("value"))
            if vv is not None:
                out.append((nm, vv))
        return out

    return []


def _extract_dividends_paid_from_as_reported(rows: List[Dict[str, Any]]) -> Dict[Tuple[str, str], float]:
    result: Dict[Tuple[str, str], float] = {}

    for r in rows:
        d = pick_date10(r, ["date", "fiscalDateEnding", "fillingDate", "filingDate"])
        if not d:
            continue
        per = _norm_period(pick_text(r, ["period"]))

        # 1순위: paymentsofdividends 정확키
        val_map = r.get("data") if isinstance(r.get("data"), dict) else None
        if isinstance(val_map, dict) and "paymentsofdividends" in val_map:
            v = _to_float(val_map.get("paymentsofdividends"))
            if v is not None:
                result[(d, per)] = v
                if per:
                    result[(d, "")] = v
                continue

        # 2순위: 라인아이템 스캔
        candidates: List[Tuple[str, float]] = []
        for name, val in _iter_as_reported_line_items(r):
            nl = name.lower()
            if "dividend" in nl and ("paid" in nl or "payment" in nl or "payments" in nl):
                candidates.append((name, val))

        if candidates:
            best = min(candidates, key=lambda x: (0 if x[1] < 0 else 1, -abs(x[1])))
            result[(d, per)] = best[1]
            if per:
                result[(d, "")] = best[1]

    return result


def _extract_shares_outstanding_from_as_reported(rows: List[Dict[str, Any]]) -> Dict[Tuple[str, str], float]:
    result: Dict[Tuple[str, str], float] = {}

    for r in rows:
        d = pick_date10(r, ["date", "fiscalDateEnding", "fillingDate", "filingDate"])
        if not d:
            continue
        per = _norm_period(pick_text(r, ["period"]))

        # 1순위: commonstocksharesoutstanding 정확키
        val_map = r.get("data") if isinstance(r.get("data"), dict) else None
        if isinstance(val_map, dict) and "commonstocksharesoutstanding" in val_map:
            v = _to_float(val_map.get("commonstocksharesoutstanding"))
            if v is not None and v > 0:
                result[(d, per)] = v
                if per:
                    result[(d, "")] = v
                continue

        # 2순위: 라인아이템 스캔
        candidates: List[float] = []
        for name, val in _iter_as_reported_line_items(r):
            nl = name.lower().replace(" ", "").replace("_", "")
            if "commonstocksharesoutstanding" in nl or ("shares" in nl and "outstanding" in nl):
                if val is not None and val > 0:
                    candidates.append(val)

        if candidates:
            best = max(candidates)
            result[(d, per)] = best
            if per:
                result[(d, "")] = best

    return result


def build_financials_quarterly(
    income: List[Dict[str, Any]],
    balance: List[Dict[str, Any]],
    cashflow: List[Dict[str, Any]],
    symbol: str,
    cutoff: str,
) -> pd.DataFrame:
    sym = symbol.strip().upper()
    by_key: Dict[Tuple[str, str], Dict[str, Any]] = {}
    # A) weightedAverageSharesDiluted: Income에 존재. cashflow는 fallback만 (현재 100% NaN 원인: income 미파싱, cashflow 키명 상이)
    for r in income:
        d = pick_date10(r, ["date", "fiscalDateEnding", "fillingDate"])
        per = pick_text(r, ["period"])
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
            "weightedAverageSharesDiluted": pick(r, ["weightedAverageShsOutDil", "weightedAverageShsOutDiluted", "weightedAverageSharesDiluted"]),
        })
    # B) sharesOutstanding: balance 후보 키 확대 (commonStockSharesOutstanding 100% 비어있음 대응)
    for r in balance:
        d = pick_date10(r, ["date", "fiscalDateEnding", "fillingDate"])
        per = pick_text(r, ["period"])
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
            "sharesOutstanding": pick(r, ["commonStockSharesOutstanding", "commonStockSharesIssued", "sharesOutstanding", "ordinarySharesNumber"]),
        })
    # C) dividendsPaid: cashflow 후보 키 확대; weightedAverageSharesDiluted는 income 없을 때만 fallback (현재 100% NaN: cashflow에 dividendsPaid/weighted 키 없음)
    for r in cashflow:
        d = pick_date10(r, ["date", "fiscalDateEnding", "fillingDate"])
        per = pick_text(r, ["period"])
        if d is None or d < cutoff:
            continue
        key = (d, per if per is not None else "")
        if key not in by_key:
            by_key[key] = {"symbol": sym, "fiscalDate": d, "period": per if per is not None else pd.NA}
        cf_update: Dict[str, Any] = {
            "freeCashFlow": pick(r, ["freeCashFlow", "freeCashFlow"]),
            "dividendsPaid": pick(r, ["dividendsPaid", "cashDividendsPaid", "dividendsPaidCommonStock", "dividendPaid", "paymentsOfDividends"]),
        }
        existing_dil = by_key[key].get("weightedAverageSharesDiluted")
        if existing_dil is None or (isinstance(existing_dil, float) and pd.isna(existing_dil)):
            cf_update["weightedAverageSharesDiluted"] = pick(r, ["weightedAverageShsOutDil", "weightedAverageSharesDiluted"])
        by_key[key].update(cf_update)
    if not by_key:
        return pd.DataFrame(columns=FINANCIALS_QUARTERLY_COLUMNS)
    df = pd.DataFrame(list(by_key.values()))
    for c in FINANCIALS_QUARTERLY_COLUMNS:
        if c not in df.columns:
            df[c] = pd.NA
    return df.reindex(columns=FINANCIALS_QUARTERLY_COLUMNS)


def enrich_financials_quarterly_from_as_reported(
    df: pd.DataFrame,
    symbol: str,
    session: requests.Session,
    rl: RateLimiter,
    api_key: str,
    call_counter: Dict[str, Any],
    as_reported_state: Dict[str, Any],
    limit: int = 80,
) -> pd.DataFrame:
    """dividendsPaid/sharesOutstanding 결측 시 as-reported API로 보강. 402/403이면 1회 경고 후 fallback 비활성화."""
    if df.empty or "fiscalDate" not in df.columns:
        return df
    lock = as_reported_state.get("lock")
    if lock is None:
        return df

    need_div = df["dividendsPaid"].isna().any() if "dividendsPaid" in df.columns else False
    need_shares = df["sharesOutstanding"].isna().any() if "sharesOutstanding" in df.columns else False

    if need_div and not as_reported_state.get("disabled_cashflow", False):
        cf_ar, restricted = fetch_cashflow_as_reported(session, rl, api_key, symbol, limit, call_counter)
        if restricted:
            with lock:
                as_reported_state["disabled_cashflow"] = True
                if not as_reported_state.get("warned_cashflow", False):
                    log.warning("cash-flow-statement-as-reported 402/403; dividendsPaid as-reported fallback disabled for this run")
                    as_reported_state["warned_cashflow"] = True
        elif cf_ar:
            div_map = _extract_dividends_paid_from_as_reported(cf_ar)
            for idx, row in df.iterrows():
                fd = str(row.get("fiscalDate", ""))[:10]
                per = _norm_period(row.get("period"))
                key = (fd, per)
                val = div_map.get(key)
                if val is None and per:
                    val = div_map.get((fd, ""))
                if val is not None and pd.isna(row.get("dividendsPaid")):
                    df.at[idx, "dividendsPaid"] = val

    if need_shares and not as_reported_state.get("disabled_balance", False):
        bal_ar, restricted = fetch_balance_as_reported(session, rl, api_key, symbol, limit, call_counter)
        if restricted:
            with lock:
                as_reported_state["disabled_balance"] = True
                if not as_reported_state.get("warned_balance", False):
                    log.warning("balance-sheet-statement-as-reported 402/403; sharesOutstanding as-reported fallback disabled for this run")
                    as_reported_state["warned_balance"] = True
        elif bal_ar:
            shares_map = _extract_shares_outstanding_from_as_reported(bal_ar)
            for idx, row in df.iterrows():
                fd = str(row.get("fiscalDate", ""))[:10]
                per = _norm_period(row.get("period"))
                key = (fd, per)
                val = shares_map.get(key)
                if val is None and per:
                    val = shares_map.get((fd, ""))
                if val is not None and pd.isna(row.get("sharesOutstanding")):
                    df.at[idx, "sharesOutstanding"] = val

    return df


def build_dividends_events(
    rows: List[Dict[str, Any]],
    symbol: str,
    cutoff: str,
) -> pd.DataFrame:
    sym = symbol.strip().upper()
    out = []
    for r in rows:
        ex_d = pick_date10(r, ["date", "exDividendDate", "exDate"])
        if ex_d is None or ex_d < cutoff:
            continue
        freq = pick_text(r, ["frequency"])
        if freq is None and r.get("frequency") is not None:
            freq = str(r["frequency"])
        out.append({
            "symbol": sym,
            "exDate": ex_d,
            "dividend": pick(r, ["dividend", "dividend"]),
            "adjDividend": pick(r, ["adjDividend", "adjustedDividend"]),
            "recordDate": pick_date10(r, ["recordDate", "recordDate"]),
            "paymentDate": pick_date10(r, ["paymentDate", "paymentDate"]),
            "declarationDate": pick_date10(r, ["declarationDate"]),
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
        d = pick_date10(r, ["date", "fiscalDateEnding", "earningsDate"])
        if d is None or d < cutoff:
            continue
        out.append({
            "symbol": sym,
            "earningsDate": d,
            "epsActual": pick(r, ["epsActual", "eps", "reportedEps"]),
            "epsEstimated": pick(r, ["epsEstimated", "estimatedEps"]),
            "revenueActual": pick(r, ["revenue", "revenueActual"]),
            "revenueEstimated": pick(r, ["revenueEstimated", "estimatedRevenue"]),
            "fiscalDate": pick_date10(r, ["fiscalDateEnding", "fiscalDate"]),
        })
    if not out:
        return pd.DataFrame(columns=EARNINGS_EVENTS_COLUMNS)
    return pd.DataFrame(out).reindex(columns=EARNINGS_EVENTS_COLUMNS)


def build_dividends_events_from_calendar(
    rows: List[Dict[str, Any]],
    universe_set: Set[str],
) -> pd.DataFrame:
    """Calendar response: filter by universe_set, map date -> exDate and other fields to DIVIDENDS_EVENTS_COLUMNS."""
    out: List[Dict[str, Any]] = []
    for r in rows:
        sym = (r.get("symbol") or r.get("ticker") or "").strip().upper()
        if not sym or sym not in universe_set:
            continue
        ex_d = pick_date10(r, ["date", "exDate", "exDividendDate"])
        if not ex_d:
            continue
        freq = pick_text(r, ["frequency"])
        if freq is None and r.get("frequency") is not None:
            freq = str(r["frequency"])
        out.append({
            "symbol": sym,
            "exDate": ex_d,
            "dividend": pick(r, ["dividend"]),
            "adjDividend": pick(r, ["adjDividend", "adjustedDividend"]),
            "recordDate": pick_date10(r, ["recordDate"]),
            "paymentDate": pick_date10(r, ["paymentDate"]),
            "declarationDate": pick_date10(r, ["declarationDate"]),
            "frequency": freq,
            "yield": pick(r, ["yield", "dividendYield"]) if any(k in r for k in ("yield", "dividendYield")) else None,
        })
    if not out:
        return pd.DataFrame(columns=DIVIDENDS_EVENTS_COLUMNS)
    df = pd.DataFrame(out)
    return df.reindex(columns=DIVIDENDS_EVENTS_COLUMNS)


def build_earnings_events_from_calendar(
    rows: List[Dict[str, Any]],
    universe_set: Set[str],
    include_last_updated: bool = False,
) -> pd.DataFrame:
    """Calendar response: filter by universe_set, map to EARNINGS_EVENTS_COLUMNS; optionally add lastUpdated."""
    out: List[Dict[str, Any]] = []
    for r in rows:
        sym = (r.get("symbol") or r.get("ticker") or "").strip().upper()
        if not sym or sym not in universe_set:
            continue
        d = pick_date10(r, ["date", "earningsDate", "fiscalDateEnding"])
        if not d:
            continue
        row: Dict[str, Any] = {
            "symbol": sym,
            "earningsDate": d,
            "epsActual": pick(r, ["epsActual", "eps", "reportedEps"]),
            "epsEstimated": pick(r, ["epsEstimated", "estimatedEps"]),
            "revenueActual": pick(r, ["revenue", "revenueActual"]),
            "revenueEstimated": pick(r, ["revenueEstimated", "estimatedRevenue"]),
            "fiscalDate": pick_date10(r, ["fiscalDateEnding", "fiscalDate"]),
        }
        if include_last_updated:
            lu = r.get("updated") or r.get("lastUpdated")
            if lu is not None:
                row["lastUpdated"] = str(lu).strip()[:28]
            else:
                row["lastUpdated"] = pd.NA
        out.append(row)
    if not out:
        cols = EARNINGS_EVENTS_COLUMNS_WITH_LASTUPDATED if include_last_updated else EARNINGS_EVENTS_COLUMNS
        return pd.DataFrame(columns=cols)
    df = pd.DataFrame(out)
    cols = EARNINGS_EVENTS_COLUMNS_WITH_LASTUPDATED if include_last_updated else EARNINGS_EVENTS_COLUMNS
    return df.reindex(columns=cols)


def build_estimates_snapshot(rows: List[Dict[str, Any]], symbol: str, as_of: str) -> pd.DataFrame:
    """Legacy: single-call snapshot (kept for any callers). Prefer build_estimates_snapshot_annual/quarter."""
    sym = symbol.strip().upper()
    r = (rows[0] if rows else {}) or {}
    df = pd.DataFrame([{
        "symbol": sym,
        "asOfDate": as_of,
        "epsNextY": pick(r, ["epsNextY", "epsNextYear", "estimatedEpsNextYear"]),
        "epsNextQ": pick(r, ["epsNextQ", "epsNextQuarter", "estimatedEpsNextQuarter"]),
        "epsThisY": pick(r, ["epsThisY", "epsThisYear", "estimatedEpsThisYear"]),
        "epsNext5Y": pd.NA,
    }])
    return df.reindex(columns=ESTIMATES_SNAPSHOT_COLUMNS)


def _estimates_eps_from_row(r: Dict[str, Any]) -> Optional[float]:
    """Extract EPS value from analyst-estimates row; API returns epsAvg/epsLow/epsHigh."""
    v = pick(r, ["epsAvg", "estimatedEpsAvg", "eps", "epsMean"])
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _estimates_rows_on_or_after(rows: List[Dict[str, Any]], as_of: str) -> List[Dict[str, Any]]:
    """Filter rows with date >= as_of (YYYY-MM-DD), sort by date ascending. Drops None/empty dates."""
    as_of_10 = str(as_of).strip()[:10] if as_of else ""
    if not as_of_10:
        return list(rows) if rows else []
    dated: List[Tuple[str, Dict[str, Any]]] = []
    for r in rows:
        d = pick_date10(r, ["date", "fiscalDateEnding", "fillingDate"])
        if not d:
            continue
        if d >= as_of_10:
            dated.append((d, r))
    dated.sort(key=lambda x: x[0])
    return [r for _, r in dated]


def _estimates_rows_after(rows: List[Dict[str, Any]], as_of: str) -> List[Dict[str, Any]]:
    """Filter rows with date > as_of (strictly after today), sort by date ascending. Finviz-style this/next."""
    as_of_10 = str(as_of).strip()[:10] if as_of else ""
    if not as_of_10:
        return list(rows) if rows else []
    dated: List[Tuple[str, Dict[str, Any]]] = []
    for r in rows:
        d = pick_date10(r, ["date", "fiscalDateEnding", "fillingDate"])
        if not d:
            continue
        if d > as_of_10:
            dated.append((d, r))
    dated.sort(key=lambda x: x[0])
    return [r for _, r in dated]


def build_estimates_snapshot_annual(rows: List[Dict[str, Any]], symbol: str, as_of: str) -> pd.DataFrame:
    """Finviz-style annual: date>as_of 중 가장 가까운 1개 epsAvg->epsThisY, 2번째->epsNextY; 5번째로 epsNext5Y(CAGR)."""
    sym = symbol.strip().upper()
    filtered = _estimates_rows_after(rows, as_of)
    eps_this = None
    eps_next = None
    eps_next_5y: Optional[float] = None
    if len(filtered) >= 1:
        eps_this = _estimates_eps_from_row(filtered[0])
    if len(filtered) >= 2:
        eps_next = _estimates_eps_from_row(filtered[1])
    if len(filtered) >= 5:
        eps_t0 = _estimates_eps_from_row(filtered[0])
        eps_t5 = _estimates_eps_from_row(filtered[4])
        if eps_t0 is not None and eps_t5 is not None and eps_t0 > 0:
            try:
                growth = (float(eps_t5) / float(eps_t0)) ** (1.0 / 5.0) - 1.0
                eps_next_5y = growth
            except (ZeroDivisionError, ValueError):
                pass
    df = pd.DataFrame([{
        "symbol": sym,
        "asOfDate": as_of,
        "epsNextY": eps_next,
        "epsNextQ": pd.NA,
        "epsThisY": eps_this,
        "epsNext5Y": eps_next_5y,
    }])
    return df.reindex(columns=ESTIMATES_SNAPSHOT_COLUMNS)


def build_estimates_snapshot_quarter(rows: List[Dict[str, Any]], symbol: str, as_of: str) -> pd.DataFrame:
    """Quarter estimates: date>as_of 중 가장 가까운 1개 epsAvg->epsNextQ."""
    sym = symbol.strip().upper()
    filtered = _estimates_rows_after(rows, as_of)
    eps_next_q = _estimates_eps_from_row(filtered[0]) if filtered else None
    df = pd.DataFrame([{
        "symbol": sym,
        "asOfDate": as_of,
        "epsNextQ": eps_next_q,
    }])
    return df.reindex(columns=ESTIMATES_QUARTERLY_SNAPSHOT_COLUMNS)


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


def build_company_facts_from_shares_row(r: Dict[str, Any], as_of: str) -> Dict[str, Any]:
    """One row for company_facts_snapshot from shares-float-all; profile fields NA."""
    sym = (r.get("symbol") or r.get("ticker") or "").strip().upper()
    return {
        "symbol": sym,
        "asOfDate": as_of,
        "sector": pd.NA,
        "industry": pd.NA,
        "employees": pd.NA,
        "ipoDate": pd.NA,
        "sharesOutstanding_profile": pd.NA,
        "sharesOutstanding_shares": pick(r, ["outstandingShares", "sharesOutstanding", "commonStockSharesOutstanding"]),
        "sharesFloat": pick(r, ["floatShares", "sharesFloat", "float"]),
    }


def build_company_facts_from_profile(
    rows: List[Dict[str, Any]],
    sym_set: Set[str],
    as_of: str,
) -> pd.DataFrame:
    """Company_facts_snapshot DF from profile (per-symbol); shares cols NA."""
    out = []
    for r in rows:
        sym = (r.get("symbol") or r.get("ticker") or "").strip().upper()
        if not sym or (sym_set and sym not in sym_set):
            continue
        sector = pick_text(r, ["sector"])
        industry = pick_text(r, ["industry"])
        emp = pick(r, ["fullTimeEmployees", "employees", "employeeCount"])
        if emp is not None and isinstance(emp, (int, float)):
            emp = float(emp)
        out.append({
            "symbol": sym,
            "asOfDate": as_of,
            "sector": sector if sector is not None else pd.NA,
            "industry": industry if industry is not None else pd.NA,
            "employees": emp,
            "ipoDate": pick_date10(r, ["ipoDate", "ipoDate", "date"]),
            "sharesOutstanding_profile": pick(r, ["sharesOutstanding", "outstandingShares", "commonStockSharesOutstanding"]),
            "sharesOutstanding_shares": pd.NA,
            "sharesFloat": pd.NA,
        })
    if not out:
        return pd.DataFrame(columns=COMPANY_FACTS_SNAPSHOT_COLUMNS)
    return pd.DataFrame(out).reindex(columns=COMPANY_FACTS_SNAPSHOT_COLUMNS)


def build_insider_transactions(rows: List[Dict[str, Any]], symbol: str) -> pd.DataFrame:
    sym = symbol.strip().upper()
    out = []
    for r in rows:
        trans_d = pick_date10(r, ["transactionDate", "date"])
        if trans_d is None:
            continue
        sec_trans = pick(r, ["securitiesTransacted", "shares", "quantity"])
        if isinstance(sec_trans, float) and sec_trans == int(sec_trans):
            sec_trans = int(sec_trans)
        price = pick(r, ["price"])
        value = pick(r, ["value"])
        if value is None and price is not None and sec_trans is not None:
            try:
                value = float(price) * float(sec_trans)
            except (TypeError, ValueError):
                pass
        cik_raw = pick_text(r, ["reportingCik", "cik"])
        reporting_name = pick_text(r, ["reportingName", "reportingName"])
        link_val = pick_text(r, ["link", "url", "filingUrl"])
        transaction_type = pick_text(r, ["transactionType", "type"])
        security_name = pick_text(r, ["securityName", "securityName"])
        out.append({
            "symbol": sym,
            "transactionDate": trans_d,
            "reportingCik": make_insider_id(
                cik_raw, reporting_name, link_val,
                symbol=sym, transaction_date=trans_d, transaction_type=transaction_type,
                securities_transacted=sec_trans, price=price, value=value, security_name=security_name,
            ),
            "reportingName": reporting_name,
            "transactionType": transaction_type,
            "securitiesTransacted": sec_trans,
            "price": price,
            "value": value,
            "securitiesOwned": pick(r, ["securitiesOwned"]),
            "securityName": security_name,
            "formType": pick_text(r, ["formType", "form"]),
            "acquisitionOrDisposition": pick_text(r, ["acquisitionOrDisposition", "acquisitionOrDisposition"]),
            "link": link_val,
        })
    if not out:
        return pd.DataFrame(columns=INSIDER_TRANSACTIONS_COLUMNS)
    return pd.DataFrame(out).reindex(columns=INSIDER_TRANSACTIONS_COLUMNS)


def build_insider_holdings_snapshot(
    transactions_df: pd.DataFrame,
    as_of: str,
    common_stock_only: bool = True,
) -> pd.DataFrame:
    if transactions_df.empty or "securitiesOwned" not in transactions_df.columns:
        return pd.DataFrame(columns=INSIDER_HOLDINGS_SNAPSHOT_COLUMNS)
    df = transactions_df.dropna(subset=["securitiesOwned"])
    if df.empty:
        return pd.DataFrame(columns=INSIDER_HOLDINGS_SNAPSHOT_COLUMNS)
    if common_stock_only:
        mask = df["securityName"].astype(str).str.upper().str.contains("COMMON STOCK", na=False)
        df = df.loc[mask]
    if df.empty:
        return pd.DataFrame(columns=INSIDER_HOLDINGS_SNAPSHOT_COLUMNS)
    key_cols = ["symbol", "reportingCik", "securityName"]
    for c in key_cols:
        if c not in df.columns:
            return pd.DataFrame(columns=INSIDER_HOLDINGS_SNAPSHOT_COLUMNS)
    if "transactionDate" not in df.columns:
        return pd.DataFrame(columns=INSIDER_HOLDINGS_SNAPSHOT_COLUMNS)
    df = df.sort_values("transactionDate", ascending=False)
    latest = df.drop_duplicates(subset=key_cols, keep="first")
    out = []
    for _, row in latest.iterrows():
        cik = row["reportingCik"]
        if pd.notna(cik):
            cik = str(cik).strip() if not isinstance(cik, str) else cik
        else:
            cik = pd.NA
        out.append({
            "symbol": row["symbol"],
            "asOfDate": as_of,
            "reportingCik": cik,
            "reportingName": row["reportingName"] if pd.notna(row["reportingName"]) else pd.NA,
            "securitiesOwned": row["securitiesOwned"],
            "securityName": row["securityName"] if pd.notna(row["securityName"]) else pd.NA,
            "lastTransactionDate": row["transactionDate"],
        })
    if not out:
        return pd.DataFrame(columns=INSIDER_HOLDINGS_SNAPSHOT_COLUMNS)
    return pd.DataFrame(out).reindex(columns=INSIDER_HOLDINGS_SNAPSHOT_COLUMNS)


def build_index_membership(
    sym_set: Set[str],
    index_symbol: str,
    members_set: Set[str],
    as_of: str,
) -> pd.DataFrame:
    norm_index = index_symbol.upper()
    if norm_index not in ("SP500", "NASDAQ", "DOWJONES"):
        norm_index = index_symbol
    rows = []
    for member_symbol in sorted(sym_set):
        rows.append({
            "indexSymbol": norm_index,
            "asOfDate": as_of,
            "memberSymbol": member_symbol,
            "isMember": member_symbol in members_set,
        })
    if not rows:
        return pd.DataFrame(columns=INDEX_MEMBERSHIP_COLUMNS)
    return pd.DataFrame(rows).reindex(columns=INDEX_MEMBERSHIP_COLUMNS)


# -----------------------------------------------------------------------------
# Upsert: load existing, concat new, dedupe by PK, save
# -----------------------------------------------------------------------------
def upsert_table(
    outdir: Path,
    name: str,
    new_df: pd.DataFrame,
    pk: List[str],
    columns: List[str],
    *,
    allow_empty_overwrite: bool = True,
) -> None:
    """Concat existing + new_df, dedupe by pk, save. When allow_empty_overwrite=False and new_df is empty, skip write (keep existing); log only."""
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
    if not allow_empty_overwrite and new_df.empty and not existing.empty:
        log.info("%s: skip save (new_df empty, allow_empty_overwrite=False; existing kept)", name)
        return
    if not allow_empty_overwrite and new_df.empty:
        log.info("%s: skip save (new_df empty, no existing file; not creating empty)", name)
        return
    combined = pd.concat([existing, new_df], ignore_index=True)
    for c in columns:
        if c not in combined.columns:
            combined[c] = pd.NA
    combined = combined.reindex(columns=columns)
    pk_avail = [c for c in pk if c in combined.columns]
    if pk_avail:
        tmp = combined[pk_avail].copy()
        for c in pk_avail:
            tmp[c] = tmp[c].astype("string").fillna("<NA>")
        combined["_pk"] = tmp[pk_avail[0]]
        for c in pk_avail[1:]:
            combined["_pk"] = combined["_pk"] + "|" + tmp[c]
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
# backfill EOD: per-symbol full-range 1회 시도 후 실패 시 2년→1년 chunk.
# daily EOD: per-symbol 증분 (저장된 최신 날짜+1 ~ target_trade_date).
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


def fill_adjclose_backfill(
    new_eod: pd.DataFrame,
    from_date: str,
    to_date: str,
    session: requests.Session,
    rl: RateLimiter,
    api_key: str,
    call_counter: Dict[str, Any],
    outdir: Path,
    dividends_df: Optional[pd.DataFrame] = None,
    max_workers: int = 4,
) -> pd.DataFrame:
    """Fill adjClose: (1) dividend-adjusted API when missing >= 5% (parallel fetches); (2) fallback compute_adjclose_from_events when still > 80%."""
    if new_eod.empty:
        return new_eod
    out = new_eod.copy()
    symbols_needing_adj: List[str] = []
    for sym in out["symbol"].dropna().unique().tolist():
        sym_str = str(sym).strip().upper()
        mask = out["symbol"] == sym_str
        slice_df = out.loc[mask]
        if slice_df.empty:
            continue
        if slice_df["adjClose"].isna().mean() >= ADJCLOSE_MISSING_RATE_THRESHOLD:
            symbols_needing_adj.append(sym_str)

    def _fetch_adj(sym_str: str) -> Tuple[str, Optional[pd.DataFrame]]:
        sess = make_session()
        try:
            rows_adj = fetch_eod_dividend_adjusted(sess, rl, api_key, sym_str, from_date, to_date, call_counter)
            adj_df = build_adjclose_from_dividend_adjusted(rows_adj, sym_str, CUTOFF_DATE, to_date)
            return (sym_str, adj_df if not adj_df.empty else None)
        except Exception as e:
            log.debug("%s dividend-adjusted adjClose: %s", sym_str, e)
            return (sym_str, None)

    adj_by_sym: Dict[str, pd.DataFrame] = {}
    if symbols_needing_adj:
        workers = min(max_workers, len(symbols_needing_adj))
        if workers <= 1:
            for sym_str in symbols_needing_adj:
                _, adj_df = _fetch_adj(sym_str)
                if adj_df is not None:
                    adj_by_sym[sym_str] = adj_df
        else:
            with ThreadPoolExecutor(max_workers=workers) as ex:
                for sym_str, adj_df in ex.map(_fetch_adj, symbols_needing_adj):
                    if adj_df is not None:
                        adj_by_sym[sym_str] = adj_df

    for sym_str, adj_df in adj_by_sym.items():
        mask = out["symbol"] == sym_str
        slice_df = out.loc[mask]
        merged = slice_df.merge(adj_df[["date", "adjClose"]], on="date", how="left", suffixes=("", "_adj"))
        filled = merged["adjClose"].combine_first(merged["adjClose_adj"])
        out.loc[mask, "adjClose"] = filled.values

    for sym in out["symbol"].dropna().unique().tolist():
        sym_str = str(sym).strip().upper()
        mask = out["symbol"] == sym_str
        slice_df = out.loc[mask]
        if slice_df.empty:
            continue
        missing_rate_after = slice_df["adjClose"].isna().mean()
        if missing_rate_after <= ADJCLOSE_FALLBACK_MISSING_RATE:
            continue
        try:
            splits_rows = fetch_splits(session, rl, api_key, sym_str, call_counter)
            div_df = pd.DataFrame(columns=DIVIDENDS_EVENTS_COLUMNS)
            if dividends_df is not None and not dividends_df.empty and sym_str in dividends_df["symbol"].values:
                div_df = dividends_df.loc[dividends_df["symbol"] == sym_str].copy()
            if div_df.empty and (outdir / "dividends_events.parquet").exists():
                try:
                    existing = pd.read_parquet(outdir / "dividends_events.parquet")
                    if not existing.empty and "symbol" in existing.columns:
                        div_df = existing.loc[existing["symbol"] == sym_str].copy()
                except Exception:
                    pass
            if div_df.empty:
                try:
                    div_rows = fetch_dividends(session, rl, api_key, sym_str, call_counter)
                    div_df = build_dividends_events(div_rows, sym_str, CUTOFF_DATE)
                except Exception:
                    pass
            price_slice = out.loc[mask].copy()
            if price_slice.empty or price_slice["close"].isna().all():
                continue
            adj_series = compute_adjclose_from_events(price_slice, div_df, splits_rows)
            if not adj_series.empty:
                out.loc[mask, "adjClose"] = out.loc[mask, "adjClose"].combine_first(adj_series).values
        except Exception as e:
            log.debug("%s adjClose fallback: %s", sym_str, e)
    return out


def log_prices_eod_adjclose_observability(outdir: Path, df: Optional[pd.DataFrame] = None) -> None:
    """Log adjClose missing rate from in-memory df (avoid reading full parquet). If df is None, skip."""
    if df is None or df.empty or "adjClose" not in df.columns:
        return
    missing = df["adjClose"].isna()
    rate = missing.mean()
    log.info("prices_eod adjClose missing rate: %.2f%%", rate * 100.0)
    if rate > ADJCLOSE_WARN_MISSING_RATE:
        by_sym = df.groupby("symbol")["adjClose"].apply(lambda s: s.isna().mean())
        top = by_sym.nlargest(ADJCLOSE_TOP_SYMBOLS_WARN)
        log.warning(
            "prices_eod adjClose missing rate > 20%%; top %s symbols by missing rate: %s",
            ADJCLOSE_TOP_SYMBOLS_WARN,
            top.index.tolist(),
        )


# -----------------------------------------------------------------------------
# 증분 감지 공통: 저장된 최신 날짜 맵, 날짜 범위 필터 (trigger 등에서 사용)
# -----------------------------------------------------------------------------


def load_existing_table(outdir: Path, table_name: str) -> pd.DataFrame:
    """Parquet 테이블이 있으면 로드, 없으면 빈 DataFrame (columns는 호출부에서 맞춤)."""
    path = outdir / f"{table_name}.parquet"
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_parquet(path)
    except Exception:
        return pd.DataFrame()


def last_saved_date_map(
    df: pd.DataFrame,
    key_column: str,
    date_column: str,
    default_date: str,
) -> Dict[str, str]:
    """테이블에서 key_column별 max(date_column) 맵. 빈 df면 빈 dict(호출부에서 default 적용)."""
    if df.empty or key_column not in df.columns or date_column not in df.columns:
        return {}
    df = df.dropna(subset=[date_column])
    if df.empty:
        return {}
    g = df.groupby(key_column, as_index=False)[date_column].max()
    g[date_column] = g[date_column].astype(str).str[:10]
    return g.set_index(key_column)[date_column].to_dict()


def filter_rows_by_date_range(
    rows: List[Dict[str, Any]],
    date_keys: List[str],
    from_date: str,
    to_date: str,
) -> List[Dict[str, Any]]:
    """rows 중 date_keys로 추출한 날짜가 [from_date, to_date] 안에 있는 것만 반환. 재사용: earnings/dividends 등."""
    out: List[Dict[str, Any]] = []
    for r in rows:
        d = pick_date10(r, date_keys)
        if d is None:
            continue
        if from_date <= d <= to_date:
            out.append(r)
    return out


def run_backfill(
    session: requests.Session,
    rl: RateLimiter,
    api_key: str,
    outdir: Path,
    universe: List[str],
    from_date: str,
    to_date: str,
    max_workers: int,
    only_symbol: Optional[str],
    call_counter: Dict[str, Any],
    insider_limit: int = 200,
    insider_common_stock_only: bool = True,
    index_symbols: Optional[List[str]] = None,
    include_company_profile: bool = False,
    include_shares_snapshot: bool = False,
) -> None:
    sym_set = set(universe)
    if only_symbol:
        want = {s.strip().upper() for s in only_symbol.split(",") if s.strip()}
        sym_set = sym_set & want
        universe = [s for s in universe if s in sym_set]

    today = date.today().isoformat()

    # (1) dividends_events — run before prices_eod so fill_adjclose_backfill can reuse for fallback
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
    div_combined = pd.DataFrame(columns=DIVIDENDS_EVENTS_COLUMNS)
    non_empty_div = [d for d in div_dfs if not d.empty]
    if non_empty_div:
        div_combined = pd.concat(non_empty_div, ignore_index=True)
        div_combined["_dedupe"] = div_combined["paymentDate"].astype(str)
        div_combined = div_combined.drop_duplicates(subset=["symbol", "exDate", "_dedupe", "dividend"], keep="last")
        div_combined = div_combined.drop(columns=["_dedupe"], errors="ignore")
        upsert_table(outdir, "dividends_events", div_combined, ["symbol", "exDate", "paymentDate", "dividend"], DIVIDENDS_EVENTS_COLUMNS)

    # (2) prices_eod — backfill에서는 bulk 미사용, 심볼별 full → adaptive chunk. adjClose는 dividend-adjusted/fallback으로 채움.
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
        new_eod = fill_adjclose_backfill(new_eod, from_date, to_date, session, rl, api_key, call_counter, outdir, dividends_df=div_combined, max_workers=max_workers)
        upsert_table(outdir, "prices_eod", new_eod, ["symbol", "date"], PRICES_EOD_COLUMNS)
        log_prices_eod_adjclose_observability(outdir, df=new_eod)

    # (3) financials_quarterly (dividendsPaid/sharesOutstanding as-reported fallback 공유 상태)
    as_reported_state: Dict[str, Any] = {
        "disabled_cashflow": False,
        "disabled_balance": False,
        "warned_cashflow": False,
        "warned_balance": False,
        "lock": threading.Lock(),
    }

    def _fin_one(sym: str) -> pd.DataFrame:
        sess = make_session()
        try:
            inc = fetch_income(sess, rl, api_key, sym, 80, call_counter)
            bal = fetch_balance(sess, rl, api_key, sym, 80, call_counter)
            cf = fetch_cashflow(sess, rl, api_key, sym, 80, call_counter)
            df = build_financials_quarterly(inc, bal, cf, sym, CUTOFF_DATE)
            df = enrich_financials_quarterly_from_as_reported(df, sym, sess, rl, api_key, call_counter, as_reported_state, 80)
            return df
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

    # (5)(6)(7) snapshots: annual + quarter estimates, targets
    as_of = today
    est_annual_rows: List[pd.DataFrame] = []
    est_quarter_rows: List[pd.DataFrame] = []
    tgt_rows: List[pd.DataFrame] = []

    for sym in universe:
        try:
            ae_a = fetch_analyst_estimates(session, rl, api_key, sym, call_counter, period="annual", page=0, limit=10)
            est_annual_rows.append(build_estimates_snapshot_annual(ae_a, sym, as_of))
        except Exception as e:
            log.warning("analyst-estimates(annual) failed for %s: %s", sym, _safe_log_message(e))
        try:
            ae_q = fetch_analyst_estimates(session, rl, api_key, sym, call_counter, period="quarter", page=0, limit=10)
            est_quarter_rows.append(build_estimates_snapshot_quarter(ae_q, sym, as_of))
        except Exception as e:
            log.warning("analyst-estimates(quarter) failed for %s: %s", sym, _safe_log_message(e))
        try:
            pt = fetch_price_target_consensus(session, rl, api_key, sym, call_counter)
            tgt_rows.append(build_targets_snapshot(pt, sym, as_of))
        except Exception:
            pass

    if est_annual_rows:
        est_df = pd.concat(est_annual_rows, ignore_index=True)
        upsert_table(outdir, "estimates_snapshot", est_df, ["symbol", "asOfDate"], ESTIMATES_SNAPSHOT_COLUMNS)
    if est_quarter_rows:
        est_q_df = pd.concat(est_quarter_rows, ignore_index=True)
        upsert_table(outdir, "estimates_quarterly_snapshot", est_q_df, ["symbol", "asOfDate"], ESTIMATES_QUARTERLY_SNAPSHOT_COLUMNS)
    if tgt_rows:
        tgt_df = pd.concat(tgt_rows, ignore_index=True)
        upsert_table(outdir, "targets_snapshot", tgt_df, ["symbol", "asOfDate"], TARGETS_SNAPSHOT_COLUMNS)

    # company_facts_snapshot: 항상 실행. profile(per-symbol) + shares-float-all; 제한 시 해당 소스만 스킵.
    facts_profile_df: Optional[pd.DataFrame] = None
    facts_shares_df: Optional[pd.DataFrame] = None

    sh_rows: List[Dict[str, Any]] = []
    shares_restricted = False
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
                sh_rows.append(build_company_facts_from_shares_row(r, as_of))
            if len(sh_list) < 1000:
                break
            page += 1
        except RuntimeError as e:
            msg = str(e).lower()
            if "403" in msg or "402" in msg or "restricted" in msg or "subscription" in msg:
                if not shares_restricted:
                    log.warning("shares (company_facts): endpoint restricted; skipping shares source only")
                    shares_restricted = True
                break
        except Exception as e:
            log.debug("shares_float_all page %s: %s", page, e)
            break
    if sh_rows and not shares_restricted:
        facts_shares_df = pd.DataFrame(sh_rows).reindex(columns=COMPANY_FACTS_SNAPSHOT_COLUMNS)

    # profile: per-symbol only (profile-bulk 제거)
    profile_rows_list: List[Dict[str, Any]] = []
    profile_restricted = False

    for sym in universe:
        if profile_restricted:
            break
        try:
            raw = fetch_profile_symbol(session, rl, api_key, sym.strip().upper(), call_counter)
            if raw:
                profile_rows_list.extend(raw)
        except RuntimeError as e:
            msg = str(e).lower()
            if "403" in msg or "402" in msg or "restricted" in msg or "subscription" in msg:
                if not profile_restricted:
                    log.warning("profile (company_facts): endpoint restricted; skipping profile source only")
                    profile_restricted = True
                break
        except Exception:
            pass

    if profile_rows_list and not profile_restricted:
        facts_profile_df = build_company_facts_from_profile(profile_rows_list, sym_set, today)

    frames = []
    if facts_profile_df is not None and not facts_profile_df.empty:
        frames.append(facts_profile_df)
    if facts_shares_df is not None and not facts_shares_df.empty:
        frames.append(facts_shares_df)
    if frames:
        merged = frames[0]
        for f in frames[1:]:
            merged = merged.merge(f, on=["symbol", "asOfDate"], how="outer", suffixes=("_p", "_s"))
        for c in COMPANY_FACTS_SNAPSHOT_COLUMNS:
            if c + "_p" in merged.columns and c + "_s" in merged.columns:
                merged[c] = merged[c + "_p"].fillna(merged[c + "_s"])
            elif c + "_p" in merged.columns:
                merged[c] = merged[c + "_p"]
            elif c + "_s" in merged.columns:
                merged[c] = merged[c + "_s"]
        for c in COMPANY_FACTS_SNAPSHOT_COLUMNS:
            if c not in merged.columns:
                merged[c] = pd.NA
        merged = merged.reindex(columns=COMPANY_FACTS_SNAPSHOT_COLUMNS)
    else:
        merged = pd.DataFrame(columns=COMPANY_FACTS_SNAPSHOT_COLUMNS)
    upsert_table(outdir, "company_facts_snapshot", merged, ["symbol", "asOfDate"], COMPANY_FACTS_SNAPSHOT_COLUMNS, allow_empty_overwrite=False)

    # insider_transactions + insider_holdings_snapshot: 항상 실행. 402/403 시 해당 심볼만 스킵, 나머지 수집 후 upsert; 빈 결과면 기존 파일 유지.
    def _insider_one(sym: str) -> pd.DataFrame:
        sess = make_session()
        result = fetch_insider_trading_stable(sess, rl, api_key, sym, page=0, limit=insider_limit, call_counter=call_counter, return_status=True)
        if isinstance(result, tuple) and len(result) == 2 and result[1] is not None:
            code, body, _ = result[1]
            if code in (402, 403):
                log.debug("%s insider: restricted (status=%s), skipping symbol", sym, code)
                return pd.DataFrame(columns=INSIDER_TRANSACTIONS_COLUMNS)
            if code >= 400:
                body_str = (body if isinstance(body, str) else str(body)).lower()
                if "restricted" in body_str or "subscription" in body_str or "not available" in body_str:
                    log.debug("%s insider: restricted, skipping symbol", sym)
                    return pd.DataFrame(columns=INSIDER_TRANSACTIONS_COLUMNS)
        rows = result[0] if isinstance(result, tuple) and result[0] is not None else []
        return build_insider_transactions(rows, sym)

    all_insider_dfs: List[pd.DataFrame] = []
    if max_workers <= 1:
        for sym in universe:
            all_insider_dfs.append(_insider_one(sym))
    else:
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            for df in ex.map(_insider_one, universe):
                all_insider_dfs.append(df)
    non_empty_insider = [d for d in all_insider_dfs if not d.empty]
    if non_empty_insider:
        insider_combined = pd.concat(non_empty_insider, ignore_index=True)
        upsert_table(outdir, "insider_transactions", insider_combined, ["symbol", "transactionDate", "reportingCik", "transactionType", "securitiesTransacted", "securityName"], INSIDER_TRANSACTIONS_COLUMNS, allow_empty_overwrite=False)
        holdings_df = build_insider_holdings_snapshot(insider_combined, today, common_stock_only=insider_common_stock_only)
        if not holdings_df.empty:
            upsert_table(outdir, "insider_holdings_snapshot", holdings_df, ["symbol", "asOfDate", "reportingCik", "securityName"], INSIDER_HOLDINGS_SNAPSHOT_COLUMNS, allow_empty_overwrite=False)
    else:
        upsert_table(outdir, "insider_transactions", pd.DataFrame(columns=INSIDER_TRANSACTIONS_COLUMNS), ["symbol", "transactionDate", "reportingCik", "transactionType", "securitiesTransacted", "securityName"], INSIDER_TRANSACTIONS_COLUMNS, allow_empty_overwrite=False)
        upsert_table(outdir, "insider_holdings_snapshot", pd.DataFrame(columns=INSIDER_HOLDINGS_SNAPSHOT_COLUMNS), ["symbol", "asOfDate", "reportingCik", "securityName"], INSIDER_HOLDINGS_SNAPSHOT_COLUMNS, allow_empty_overwrite=False)

    # index_membership: 항상 실행 (기본 SP500). 실패한 인덱스만 로그 후 continue, run 정상 종료.
    index_list = index_symbols if index_symbols is not None and len(index_symbols) > 0 else ["SP500"]
    for idx_sym in index_list:
        idx_sym = idx_sym.strip().upper()
        if not idx_sym:
            continue
        try:
            members_set = fetch_index_constituents(session, rl, api_key, idx_sym, call_counter)
            idx_df = build_index_membership(sym_set, idx_sym, members_set, today)
            if not idx_df.empty:
                upsert_table(outdir, "index_membership", idx_df, ["indexSymbol", "asOfDate", "memberSymbol"], INDEX_MEMBERSHIP_COLUMNS)
        except Exception as e:
            log.warning("index_membership %s: %s (skipping index, run continues)", idx_sym, _safe_log_message(e))


def run_debug_financials_fields(
    symbol: str,
    session: requests.Session,
    rl: RateLimiter,
    api_key: str,
    call_counter: Dict[str, Any],
) -> None:
    """--debug-financials-fields: 1개 심볼에 대해 income/balance/cashflow 키 목록 및 financials_quarterly 3필드 non-null count 로깅. API key 미노출."""
    sym = symbol.strip().upper()
    try:
        inc = fetch_income(session, rl, api_key, sym, 5, call_counter)
        bal = fetch_balance(session, rl, api_key, sym, 5, call_counter)
        cf = fetch_cashflow(session, rl, api_key, sym, 5, call_counter)
    except Exception as e:
        log.info("[debug-financials-fields] fetch failed for %s: %s", sym, _safe_log_message(e))
        return
    for name, rows in [("income", inc), ("balance", bal), ("cashflow", cf)]:
        keys = list(rows[0].keys()) if isinstance(rows, list) and rows else []
        keys_safe = [k if "apikey" not in k.lower() else "***" for k in keys]
        log.info("[debug-financials-fields] %s first_row keys: %s", name, keys_safe)
    df = build_financials_quarterly(inc or [], bal or [], cf or [], sym, CUTOFF_DATE)
    as_reported_state: Dict[str, Any] = {"disabled_cashflow": False, "disabled_balance": False, "warned_cashflow": False, "warned_balance": False, "lock": threading.Lock()}
    df = enrich_financials_quarterly_from_as_reported(df, sym, session, rl, api_key, call_counter, as_reported_state, 5)
    for col in ["weightedAverageSharesDiluted", "dividendsPaid", "sharesOutstanding"]:
        if col in df.columns:
            n = int(df[col].notna().sum())
            log.info("[debug-financials-fields] financials_quarterly %s non-null count: %s", col, n)
        else:
            log.info("[debug-financials-fields] financials_quarterly %s: column missing", col)


def run_daily(
    session: requests.Session,
    rl: RateLimiter,
    api_key: str,
    outdir: Path,
    universe: List[str],
    from_date: str,
    to_date: str,
    max_workers: int,
    only_symbol: Optional[str],
    call_counter: Dict[str, Any],
) -> None:
    """Daily 모드: prices_eod(per-symbol 증분)만 실행. 다른 테이블은 weekly에서 실행."""

    sym_set = set(universe)
    if only_symbol:
        want = {s.strip().upper() for s in only_symbol.split(",") if s.strip()}
        sym_set = sym_set & want
        universe = [s for s in universe if s in sym_set]

    today_dt = date.today()
    today = today_dt.isoformat()
    target_trade_date = compute_target_trade_date(today_dt)
    latest_by_symbol = load_latest_prices_date_map(outdir)

    eod_dfs: List[pd.DataFrame] = []
    cutoff_dt = date.fromisoformat(CUTOFF_DATE[:10])
    to_dt = date.fromisoformat(target_trade_date[:10])
    for sym in universe:
        sym_u = sym.strip().upper()
        latest = latest_by_symbol.get(sym_u)
        if latest:
            try:
                from_dt = date.fromisoformat(latest[:10]) + timedelta(days=1)
            except ValueError:
                from_dt = cutoff_dt
        else:
            from_dt = cutoff_dt
        from_dt = max(from_dt, cutoff_dt)
        if from_dt > to_dt:
            continue
        from_d = from_dt.isoformat()
        try:
            rows = fetch_eod_full(session, rl, api_key, sym_u, from_d, target_trade_date, call_counter)
            df = build_prices_eod_from_full(rows, sym_u)
            if not df.empty:
                eod_dfs.append(df)
        except Exception as e:
            log.debug("daily eod %s: %s", sym_u, _safe_log_message(e))
    if eod_dfs:
        new_eod = pd.concat(eod_dfs, ignore_index=True)
        new_eod = new_eod.drop_duplicates(subset=["symbol", "date"], keep="last")
        new_eod = new_eod.reindex(columns=PRICES_EOD_COLUMNS)
        if new_eod["adjClose"].isna().any():
            new_eod = new_eod.copy()
            new_eod.loc[new_eod["adjClose"].isna(), "adjClose"] = new_eod.loc[new_eod["adjClose"].isna(), "close"]
        upsert_table(outdir, "prices_eod", new_eod, ["symbol", "date"], PRICES_EOD_COLUMNS)
        log_prices_eod_adjclose_observability(outdir, df=new_eod)
        log.info(
            "daily prices_eod: target_trade_date=%s rows=%s (per-symbol incremental)",
            target_trade_date, len(new_eod),
        )
    else:
        log.info("daily prices_eod: target_trade_date=%s no new rows (per-symbol incremental)", target_trade_date)


def run_weekly(
    session: requests.Session,
    rl: RateLimiter,
    api_key: str,
    outdir: Path,
    universe: List[str],
    call_counter: Dict[str, Any],
    max_workers: int,
    only_symbol: Optional[str] = None,
) -> None:
    """Weekly 모드: estimates_snapshot, targets_snapshot만 생성/업데이트. (company_facts/index/insider/watermark 미실행)"""
    sym_set = set(universe)
    if only_symbol:
        only_set = {s.strip().upper() for s in only_symbol.split(",") if s.strip()}
        if only_set:
            sym_set = sym_set & only_set
            universe = [s for s in universe if s in sym_set]
    today = date.today().isoformat()
    log.info("mode=weekly universe_size=%s", len(universe))

    as_of = today
    est_annual_rows: List[pd.DataFrame] = []
    est_quarter_rows: List[pd.DataFrame] = []
    tgt_rows: List[pd.DataFrame] = []
    for sym in universe:
        try:
            ae_a = fetch_analyst_estimates(session, rl, api_key, sym, call_counter, period="annual", page=0, limit=10)
            est_annual_rows.append(build_estimates_snapshot_annual(ae_a, sym, as_of))
        except Exception as e:
            log.warning("analyst-estimates(annual) failed for %s: %s", sym, _safe_log_message(e))
        try:
            ae_q = fetch_analyst_estimates(session, rl, api_key, sym, call_counter, period="quarter", page=0, limit=10)
            est_quarter_rows.append(build_estimates_snapshot_quarter(ae_q, sym, as_of))
        except Exception as e:
            log.warning("analyst-estimates(quarter) failed for %s: %s", sym, _safe_log_message(e))
        try:
            pt = fetch_price_target_consensus(session, rl, api_key, sym, call_counter)
            tgt_rows.append(build_targets_snapshot(pt, sym, as_of))
        except Exception:
            pass
    if est_annual_rows:
        upsert_table(outdir, "estimates_snapshot", pd.concat(est_annual_rows, ignore_index=True), ["symbol", "asOfDate"], ESTIMATES_SNAPSHOT_COLUMNS)
    if est_quarter_rows:
        upsert_table(outdir, "estimates_quarterly_snapshot", pd.concat(est_quarter_rows, ignore_index=True), ["symbol", "asOfDate"], ESTIMATES_QUARTERLY_SNAPSHOT_COLUMNS)
    if tgt_rows:
        upsert_table(outdir, "targets_snapshot", pd.concat(tgt_rows, ignore_index=True), ["symbol", "asOfDate"], TARGETS_SNAPSHOT_COLUMNS)


def run_monthly(
    session: requests.Session,
    rl: RateLimiter,
    api_key: str,
    outdir: Path,
    universe: List[str],
    call_counter: Dict[str, Any],
    max_workers: int,
    index_symbols: Optional[List[str]],
    include_company_profile: bool,
    include_shares_snapshot: bool,
    include_insider: bool = True,
    insider_limit: int = 200,
    insider_common_stock_only: bool = True,
    only_symbol: Optional[str] = None,
) -> None:
    """Monthly 모드: index_membership, company_facts_snapshot, insider_transactions, insider_holdings_snapshot 항상 실행. 전체 심볼(현재 shard 기준). asOfDate=today."""
    today = date.today().isoformat()
    sym_set = set(universe)
    if only_symbol:
        only_set = {s.strip().upper() for s in only_symbol.split(",") if s.strip()}
        if only_set:
            sym_set = sym_set & only_set
            universe = [s for s in universe if s in sym_set]
    log.info("mode=monthly universe_size=%s", len(universe))

    as_of = today

    # (1) company_facts_snapshot: 항상 실행 (profile + shares; 제한 시 해당 소스만 스킵)
    facts_profile_df = None
    facts_shares_df = None
    sh_rows: List[Dict[str, Any]] = []
    shares_restricted = False
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
                sh_rows.append(build_company_facts_from_shares_row(r, as_of))
            if len(sh_list) < 1000:
                break
            page += 1
        except RuntimeError as e:
            msg = str(e).lower()
            if "403" in msg or "402" in msg or "restricted" in msg or "subscription" in msg:
                if not shares_restricted:
                    log.warning("shares (company_facts): endpoint restricted; skipping shares source only")
                    shares_restricted = True
                break
        except Exception:
            break
    if sh_rows and not shares_restricted:
        facts_shares_df = pd.DataFrame(sh_rows).reindex(columns=COMPANY_FACTS_SNAPSHOT_COLUMNS)

    # profile: per-symbol only (profile-bulk 제거)
    profile_rows: List[Dict[str, Any]] = []
    profile_restricted = False

    for sym in universe:
        if profile_restricted:
            break
        try:
            raw = fetch_profile_symbol(session, rl, api_key, sym.strip().upper(), call_counter)
            if raw:
                profile_rows.extend(raw)
        except RuntimeError as e:
            msg = str(e).lower()
            if "403" in msg or "402" in msg or "restricted" in msg or "subscription" in msg:
                if not profile_restricted:
                    log.warning("profile (company_facts): endpoint restricted; skipping profile source only")
                    profile_restricted = True
                break
        except Exception:
            pass

    if profile_rows and not profile_restricted:
        facts_profile_df = build_company_facts_from_profile(profile_rows, sym_set, as_of)

    frames = []
    if facts_profile_df is not None and not facts_profile_df.empty:
        frames.append(facts_profile_df)
    if facts_shares_df is not None and not facts_shares_df.empty:
        frames.append(facts_shares_df)
    if frames:
        merged = frames[0]
        for f in frames[1:]:
            merged = merged.merge(f, on=["symbol", "asOfDate"], how="outer", suffixes=("_p", "_s"))
        for c in COMPANY_FACTS_SNAPSHOT_COLUMNS:
            if c + "_p" in merged.columns and c + "_s" in merged.columns:
                merged[c] = merged[c + "_p"].fillna(merged[c + "_s"])
            elif c + "_p" in merged.columns:
                merged[c] = merged[c + "_p"]
            elif c + "_s" in merged.columns:
                merged[c] = merged[c + "_s"]
        for c in COMPANY_FACTS_SNAPSHOT_COLUMNS:
            if c not in merged.columns:
                merged[c] = pd.NA
        merged = merged.reindex(columns=COMPANY_FACTS_SNAPSHOT_COLUMNS)
    else:
        merged = pd.DataFrame(columns=COMPANY_FACTS_SNAPSHOT_COLUMNS)
    upsert_table(outdir, "company_facts_snapshot", merged, ["symbol", "asOfDate"], COMPANY_FACTS_SNAPSHOT_COLUMNS, allow_empty_overwrite=False)

    # (2) index_membership: 항상 실행 (기본 SP500)
    index_list = index_symbols if index_symbols and len(index_symbols) > 0 else ["SP500"]
    for idx_sym in index_list:
        idx_sym = idx_sym.strip().upper()
        if not idx_sym:
            continue
        try:
            members_set = fetch_index_constituents(session, rl, api_key, idx_sym, call_counter)
            idx_df = build_index_membership(sym_set, idx_sym, members_set, as_of)
            if not idx_df.empty:
                upsert_table(outdir, "index_membership", idx_df, ["indexSymbol", "asOfDate", "memberSymbol"], INDEX_MEMBERSHIP_COLUMNS)
        except Exception as e:
            log.warning("index_membership %s: %s (skipping index, run continues)", idx_sym, _safe_log_message(e))

    # (3) insider_transactions + insider_holdings_snapshot: 항상 실행. 402/403 시 해당 심볼만 스킵.
    def _insider_one(sym: str) -> pd.DataFrame:
        sess = make_session()
        result = fetch_insider_trading_stable(sess, rl, api_key, sym, page=0, limit=insider_limit, call_counter=call_counter, return_status=True)
        if isinstance(result, tuple) and len(result) == 2 and result[1] is not None:
            code, body, _ = result[1]
            if code in (402, 403):
                log.debug("%s insider: restricted (status=%s), skipping symbol", sym, code)
                return pd.DataFrame(columns=INSIDER_TRANSACTIONS_COLUMNS)
            if code >= 400:
                body_str = (body if isinstance(body, str) else str(body)).lower()
                if "restricted" in body_str or "subscription" in body_str or "not available" in body_str:
                    log.debug("%s insider: restricted, skipping symbol", sym)
                    return pd.DataFrame(columns=INSIDER_TRANSACTIONS_COLUMNS)
        rows = result[0] if isinstance(result, tuple) and result[0] is not None else []
        return build_insider_transactions(rows, sym)

    all_insider_dfs: List[pd.DataFrame] = []
    if max_workers <= 1:
        for sym in universe:
            all_insider_dfs.append(_insider_one(sym))
    else:
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            all_insider_dfs = list(ex.map(_insider_one, universe))
    non_empty_insider = [d for d in all_insider_dfs if not d.empty]
    if non_empty_insider:
        insider_combined = pd.concat(non_empty_insider, ignore_index=True)
        upsert_table(outdir, "insider_transactions", insider_combined, ["symbol", "transactionDate", "reportingCik", "transactionType", "securitiesTransacted", "securityName"], INSIDER_TRANSACTIONS_COLUMNS, allow_empty_overwrite=False)
        holdings_df = build_insider_holdings_snapshot(insider_combined, today, common_stock_only=insider_common_stock_only)
        if not holdings_df.empty:
            upsert_table(outdir, "insider_holdings_snapshot", holdings_df, ["symbol", "asOfDate", "reportingCik", "securityName"], INSIDER_HOLDINGS_SNAPSHOT_COLUMNS, allow_empty_overwrite=False)
    else:
        upsert_table(outdir, "insider_transactions", pd.DataFrame(columns=INSIDER_TRANSACTIONS_COLUMNS), ["symbol", "transactionDate", "reportingCik", "transactionType", "securitiesTransacted", "securityName"], INSIDER_TRANSACTIONS_COLUMNS, allow_empty_overwrite=False)
        upsert_table(outdir, "insider_holdings_snapshot", pd.DataFrame(columns=INSIDER_HOLDINGS_SNAPSHOT_COLUMNS), ["symbol", "asOfDate", "reportingCik", "securityName"], INSIDER_HOLDINGS_SNAPSHOT_COLUMNS, allow_empty_overwrite=False)


# -----------------------------------------------------------------------------
# Trigger mode: calendar-based dividends_events + earnings_events (no financials)
# -----------------------------------------------------------------------------


def run_trigger_dividends_calendar(
    session: requests.Session,
    rl: RateLimiter,
    api_key: str,
    outdir: Path,
    universe_set: Set[str],
    from_date: str,
    to_date: str,
    call_counter: Dict[str, Any],
) -> None:
    """Fetch dividends-calendar for [from_date, to_date], filter by universe_set, upsert dividends_events. PK: symbol, exDate, paymentDate, dividend."""
    rows = fetch_dividends_calendar(session, rl, api_key, from_date, to_date, call_counter)
    if not rows:
        log.info("trigger dividends_calendar: no rows in range %s..%s", from_date, to_date)
        return
    df = build_dividends_events_from_calendar(rows, universe_set)
    if df.empty:
        log.info("trigger dividends_calendar: no rows after universe filter")
        return
    upsert_table(outdir, "dividends_events", df, ["symbol", "exDate", "paymentDate", "dividend"], DIVIDENDS_EVENTS_COLUMNS)
    log.info("trigger dividends_events upserted rows=%s", len(df))


def run_trigger_earnings_calendar(
    session: requests.Session,
    rl: RateLimiter,
    api_key: str,
    outdir: Path,
    universe_set: Set[str],
    from_date: str,
    to_date: str,
    call_counter: Dict[str, Any],
    use_lastupdated: bool = False,
) -> Set[str]:
    """Fetch earnings-calendar for [from_date, to_date], filter by universe_set; optionally filter by lastUpdated. Upsert earnings_events. Returns earnings_hit_symbols (symbols in calendar and in universe_set)."""
    rows = fetch_earnings_calendar(session, rl, api_key, from_date, to_date, call_counter)
    earnings_hit_symbols: Set[str] = {
        (r.get("symbol") or r.get("ticker") or "").strip().upper()
        for r in rows
        if (r.get("symbol") or r.get("ticker") or "").strip().upper() in universe_set
    }
    if not rows:
        log.info("trigger earnings_calendar: no rows in range %s..%s", from_date, to_date)
        return set()
    df = build_earnings_events_from_calendar(rows, universe_set, include_last_updated=use_lastupdated)
    if df.empty:
        log.info("trigger earnings_calendar: no rows after universe filter")
        return earnings_hit_symbols
    if use_lastupdated and "lastUpdated" in df.columns:
        existing = load_existing_table(outdir, "earnings_events")
        for c in EARNINGS_EVENTS_COLUMNS_WITH_LASTUPDATED:
            if c not in existing.columns:
                existing[c] = pd.NA
        if not existing.empty and "symbol" in existing.columns and "earningsDate" in existing.columns and "lastUpdated" in existing.columns:
            existing_lu = existing.set_index(["symbol", "earningsDate"])["lastUpdated"].to_dict()
            def keep_row(row: Any) -> bool:
                sym = row.get("symbol")
                ed = row.get("earningsDate")
                if sym is None or ed is None:
                    return True
                key = (str(sym).strip().upper(), str(ed).strip()[:10])
                existing_val = existing_lu.get(key)
                if pd.isna(existing_val) or existing_val is None or str(existing_val).strip() == "":
                    return True
                new_val = row.get("lastUpdated")
                if pd.isna(new_val) or new_val is None:
                    return True
                return str(new_val).strip() > str(existing_val).strip()
            mask = df.apply(keep_row, axis=1)
            df = df.loc[mask].copy()
        if df.empty:
            log.info("trigger earnings_calendar: no rows after lastUpdated filter")
            return earnings_hit_symbols
    columns = EARNINGS_EVENTS_COLUMNS_WITH_LASTUPDATED if use_lastupdated else EARNINGS_EVENTS_COLUMNS
    upsert_table(outdir, "earnings_events", df, ["symbol", "earningsDate"], columns)
    log.info("trigger earnings_events upserted rows=%s", len(df))
    return earnings_hit_symbols


def fetch_and_build_financials_for_symbols(
    symbols: List[str],
    outdir: Path,
    to_date: str,
    session: requests.Session,
    rl: RateLimiter,
    api_key: str,
    call_counter: Dict[str, Any],
    max_workers: int,
) -> List[pd.DataFrame]:
    """Per-symbol: load existing last_saved, fetch income/balance/cashflow (from last_saved-7d), build_financials_quarterly, enrich. Returns list of DataFrames (one per symbol; empty on fetch failure). ThreadPool with per-worker session; shared rl."""
    existing_fin = load_existing_table(outdir, "financials_quarterly")
    if not existing_fin.empty and "fiscalDate" in existing_fin.columns:
        existing_fin = existing_fin.copy()
        existing_fin["fiscalDate"] = pd.to_datetime(existing_fin["fiscalDate"], errors="coerce").dt.strftime("%Y-%m-%d")
    last_saved_fin = last_saved_date_map(existing_fin, "symbol", "fiscalDate", CUTOFF_DATE)
    cutoff_dt = date.fromisoformat(CUTOFF_DATE[:10])
    fin_date_keys = ["date", "fiscalDateEnding", "fillingDate"]
    as_reported_state: Dict[str, Any] = {
        "disabled_cashflow": False,
        "disabled_balance": False,
        "warned_cashflow": False,
        "warned_balance": False,
        "lock": threading.Lock(),
    }

    def _one(sym: str) -> pd.DataFrame:
        sess = make_session()
        try:
            last_saved = last_saved_fin.get(sym, CUTOFF_DATE)
            try:
                last_dt = date.fromisoformat(str(last_saved)[:10])
            except ValueError:
                last_dt = cutoff_dt
            from_dt = max(last_dt - timedelta(days=7), cutoff_dt)
            from_date_fin = from_dt.isoformat()
            inc = fetch_income(sess, rl, api_key, sym, 200, call_counter)
            bal = fetch_balance(sess, rl, api_key, sym, 200, call_counter)
            cf = fetch_cashflow(sess, rl, api_key, sym, 200, call_counter)
            inc_f = filter_rows_by_date_range(inc or [], fin_date_keys, from_date_fin, to_date)
            bal_f = filter_rows_by_date_range(bal or [], fin_date_keys, from_date_fin, to_date)
            cf_f = filter_rows_by_date_range(cf or [], fin_date_keys, from_date_fin, to_date)
            new_df = build_financials_quarterly(inc_f, bal_f, cf_f, sym, from_date_fin)
            new_df = enrich_financials_quarterly_from_as_reported(new_df, sym, sess, rl, api_key, call_counter, as_reported_state, 200)
            return new_df
        except Exception as e:
            log.debug("financials fetch %s: %s", sym, _safe_log_message(e))
            return pd.DataFrame(columns=FINANCIALS_QUARTERLY_COLUMNS)

    if not symbols:
        return []
    if max_workers <= 1:
        return [_one(s) for s in symbols]
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        return list(ex.map(_one, symbols))


def run_trigger_financials_quarterly(
    session: requests.Session,
    rl: RateLimiter,
    api_key: str,
    outdir: Path,
    earnings_hit_symbols: Set[str],
    universe_list: List[str],
    insurance_enabled: bool,
    to_date: str,
    call_counter: Dict[str, Any],
    max_workers: int,
) -> None:
    """Trigger financials_quarterly: earnings_hit_symbols only, or full shard (universe_list) when insurance_enabled (1~5일). Fetch+build per symbol, upsert."""
    today_str = to_date[:10] if to_date else date.today().isoformat()
    log.info("trigger financials_quarterly: insurance_enabled=%s today=%s earnings_hit_symbols=%s", insurance_enabled, today_str, len(earnings_hit_symbols))
    if insurance_enabled:
        target_symbols = {s.strip().upper() for s in universe_list if s and str(s).strip()}
        log.info("trigger financials_quarterly: insurance run, target symbols (shard universe)=%s", len(target_symbols))
    else:
        target_symbols = set(earnings_hit_symbols)
    if not target_symbols:
        log.info("trigger financials_quarterly: no target symbols, skip")
        return
    symbols_list = sorted(target_symbols)
    dfs = fetch_and_build_financials_for_symbols(
        symbols_list, outdir, to_date, session, rl, api_key, call_counter, max_workers,
    )
    non_empty = [d for d in dfs if not d.empty]
    if not non_empty:
        log.info("trigger financials_quarterly: no rows to upsert")
        return
    combined = pd.concat(non_empty, ignore_index=True)
    upsert_table(outdir, "financials_quarterly", combined, FINANCIALS_PK, FINANCIALS_QUARTERLY_COLUMNS)
    n_symbols = combined["symbol"].nunique() if "symbol" in combined.columns else 0
    log.info("trigger financials_quarterly upserted rows=%s symbols=%s", len(combined), n_symbols)


def verify_dividends_calendar_date_matches_exdate(
    session: requests.Session,
    rl: RateLimiter,
    api_key: str,
    symbol: str,
    call_counter: Dict[str, Any],
    from_date: str,
    to_date: str,
) -> None:
    """Compare dividends-calendar 'date' with /dividends?symbol=XXX exDate for one symbol. Log MATCH or MISMATCH (no data change)."""
    sym = symbol.strip().upper()
    if not sym:
        log.info("verify-dividends-calendar-exdate: empty symbol, skip")
        return
    cal_rows = fetch_dividends_calendar(session, rl, api_key, from_date, to_date, call_counter)
    cal_for_sym = [r for r in cal_rows if (r.get("symbol") or r.get("ticker") or "").strip().upper() == sym]
    if not cal_for_sym:
        log.info("verify-dividends-calendar-exdate: %s no calendar row in range %s..%s", sym, from_date, to_date)
        return
    cal_date = pick_date10(cal_for_sym[0], ["date", "exDate", "exDividendDate"])
    div_rows = fetch_dividends(session, rl, api_key, sym, call_counter)
    ex_from_div = None
    for r in div_rows:
        d = pick_date10(r, ["exDate", "exDividendDate", "date"])
        if d:
            ex_from_div = d
            break
    if cal_date is None:
        log.info("verify-dividends-calendar-exdate: %s calendar date missing → MISMATCH (no calendar date)", sym)
        return
    if ex_from_div is None:
        log.info("verify-dividends-calendar-exdate: %s dividends exDate missing → MISMATCH (no exDate)", sym)
        return
    if cal_date == ex_from_div:
        log.info("verify-dividends-calendar-exdate: %s calendar date=%s vs exDate=%s → MATCH", sym, cal_date, ex_from_div)
    else:
        log.info("verify-dividends-calendar-exdate: %s calendar date=%s vs exDate=%s → MISMATCH", sym, cal_date, ex_from_div)


def main() -> None:
    ap = argparse.ArgumentParser(description="FMP Premium: 11 tables (prices_eod, financials_quarterly, ..., company_facts_snapshot, insider_*, index_membership)")
    ap.add_argument("--universe", required=True, help="universe_list.csv path")
    ap.add_argument("--outdir", required=True, help="Output directory")
    ap.add_argument("--from", dest="from_date", default="2020-01-01", help="Start date YYYY-MM-DD")
    ap.add_argument("--to", dest="to_date", default="", help="End date YYYY-MM-DD (backfill: today; daily: yesterday)")
    ap.add_argument("--max-workers", type=int, default=4, help="ThreadPool workers")
    ap.add_argument("--mode", choices=["backfill", "daily", "weekly", "monthly", "trigger"], default="backfill", help="backfill=full; daily=prices_eod only; weekly=estimates/targets only; monthly=index_membership, company_facts, insider; trigger=calendar-based dividends_events+earnings_events")
    ap.add_argument("--only-symbol", default="", help="Optional: AAPL,MSFT")
    ap.add_argument("--verify-dividends-calendar-exdate", default="", metavar="SYMBOL", help="Trigger only: verify dividends-calendar date vs exDate for SYMBOL (log MATCH/MISMATCH, no data change)")
    ap.add_argument("--earnings-use-lastupdated", action="store_true", help="Trigger only: add lastUpdated to earnings_events and skip rows not newer than existing")
    ap.add_argument("--include-company-profile", action="store_true", help="(Deprecated) Backfill/monthly always run company_facts; flag ignored")
    ap.add_argument("--include-shares-snapshot", action="store_true", help="(Deprecated) Backfill/monthly always run company_facts; flag ignored")
    ap.add_argument("--monthly-skip-insider", action="store_true", help="(Deprecated) Monthly always runs insider; flag ignored")
    ap.add_argument("--num-shards", type=int, default=1, help="Shard universe into N parts (default 1 = no sharding)")
    ap.add_argument("--shard-id", type=int, default=0, help="Which shard to process (0..num_shards-1)")
    ap.add_argument("--insider-limit", type=int, default=200, help="Max insider transactions per symbol (default 200)")
    ap.add_argument("--insider-common-stock-only", action="store_true", default=True, help="Insider holdings: only Common Stock (default True)")
    ap.add_argument("--no-insider-common-stock-only", action="store_false", dest="insider_common_stock_only")
    ap.add_argument("--index-symbols", type=str, default="SP500", help="Index constituents (backfill/monthly), comma-separated (default SP500)")
    ap.add_argument("--debug-financials-fields", action="store_true", help="Log income/balance/cashflow top-level keys and financials_quarterly 3-field non-null counts for one symbol (API key not logged)")
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
    if args.mode == "trigger":
        from_date = (date.today() - timedelta(days=7)).isoformat()
        to_date = date.today().isoformat()

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

    if getattr(args, "debug_financials_fields", False) and universe:
        debug_sym = (getattr(args, "only_symbol", "") or "").strip()
        if debug_sym:
            debug_sym = debug_sym.split(",")[0].strip().upper()
        if not debug_sym:
            debug_sym = universe[0]
        run_debug_financials_fields(debug_sym, session, rl, api_key, call_counter)

    index_symbols_list: Optional[List[str]] = None
    if getattr(args, "index_symbols", ""):
        index_symbols_list = [s.strip() for s in args.index_symbols.split(",") if s.strip()]

    if args.mode == "backfill":
        if getattr(args, "include_company_profile", False) or getattr(args, "include_shares_snapshot", False):
            log.info("backfill: --include-company-profile / --include-shares-snapshot are deprecated; company_facts_snapshot always runs")
        run_backfill(
            session, rl, api_key, outdir, universe,
            from_date, to_date,
            max_workers=args.max_workers,
            only_symbol=args.only_symbol or None,
            call_counter=call_counter,
            insider_limit=getattr(args, "insider_limit", 200),
            insider_common_stock_only=getattr(args, "insider_common_stock_only", True),
            index_symbols=index_symbols_list,
            include_company_profile=True,
            include_shares_snapshot=True,
        )
    elif args.mode == "weekly":
        run_weekly(
            session, rl, api_key, outdir, universe, call_counter,
            max_workers=args.max_workers,
            only_symbol=args.only_symbol or None,
        )
    elif args.mode == "monthly":
        if getattr(args, "include_company_profile", False) or getattr(args, "include_shares_snapshot", False) or getattr(args, "monthly_skip_insider", False):
            log.info("monthly: --include-company-profile / --include-shares-snapshot / --monthly-skip-insider are deprecated; index, company_facts, insider always run")
        run_monthly(
            session, rl, api_key, outdir, universe, call_counter,
            max_workers=args.max_workers,
            index_symbols=index_symbols_list,
            include_company_profile=True,
            include_shares_snapshot=True,
            include_insider=True,
            insider_limit=getattr(args, "insider_limit", 200),
            insider_common_stock_only=getattr(args, "insider_common_stock_only", True),
            only_symbol=args.only_symbol or None,
        )
    elif args.mode == "trigger":
        universe_set = {s.strip().upper() for s in universe if s and str(s).strip()}
        verify_sym = (getattr(args, "verify_dividends_calendar_exdate", "") or "").strip()
        if verify_sym:
            verify_dividends_calendar_date_matches_exdate(
                session, rl, api_key, verify_sym, call_counter, from_date, to_date,
            )
        # A) earnings-calendar → earnings_events upsert + earnings_hit_symbols
        earnings_hit_symbols = run_trigger_earnings_calendar(
            session, rl, api_key, outdir, universe_set, from_date, to_date, call_counter,
            use_lastupdated=getattr(args, "earnings_use_lastupdated", False),
        )
        # B) dividends-calendar → dividends_events upsert
        run_trigger_dividends_calendar(session, rl, api_key, outdir, universe_set, from_date, to_date, call_counter)
        # C) financials_quarterly: earnings_hit_symbols + (1~5일이면 shard 전체 보험)
        today_dt = date.today()
        insurance_enabled = today_dt.day in (1, 2, 3, 4, 5)
        run_trigger_financials_quarterly(
            session, rl, api_key, outdir,
            earnings_hit_symbols, universe, insurance_enabled,
            to_date, call_counter, args.max_workers,
        )
    else:
        run_daily(
            session, rl, api_key, outdir, universe,
            from_date, to_date,
            args.max_workers,
            args.only_symbol or None,
            call_counter,
        )

    log.info("API calls: %s", call_counter.get("count", 0))


if __name__ == "__main__":
    main()
