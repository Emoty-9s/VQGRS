# -*- coding: utf-8 -*-
r"""
FMP Premium: 11개 테이블 수집 (prices_eod, financials_quarterly, dividends_events, earnings_events,
estimates_snapshot, targets_snapshot, shares_snapshot, company_profile_snapshot, insider_*, index_membership).
API Key: 환경변수 FMP_API_KEY (로그/예외/출력 노출 금지). 결측 NaN 유지. 저장은 PK 기준 Upsert.
prices_eod의 adjClose는 dividend-adjusted API 또는 배당/스플릿 이벤트 기반 계산으로 채운다.

모드별 실행 테이블:
  | 모드      | prices_eod              | estimates/targets | profile/shares      | earnings/dividends/financials/insider (워터마크) |
  |-----------|-------------------------|-------------------|---------------------|---------------------------------------------------|
  | backfill  | O (per-symbol full)     | O                 | opt-in (--include-*) | O (전체 full fetch)                               |
  | daily     | O (per-symbol 증분)     | O                 | opt-in (--include-*) | O (감지→변경시만; dividends daily 생략)             |
  | weekly    | X                       | X                 | X                   | O (감지→변경시만; dividends 감지 O; insider는 --weekly-include-insider 시에만) |

profile/shares: Premium 제한으로 bulk 엔드포인트 비사용. company_profile_snapshot은 --include-company-profile 시 per-symbol profile 시도; shares_snapshot은 --include-shares-snapshot 시에만. 제한 시 1회 로그 후 비활성화.
weekly dividends: API limit 지원 시 limit=2 light 감지; 미지원 시 기본 skip, --weekly-dividends-full-refresh 시 full refresh.
financials 월 1일: 현재 shard만 갱신; --monthly-financials-cap N 으로 심볼 수 제한 가능.

실행 예:
  python fmp_universe_fetch.py --universe ./data/universe_list.csv --outdir ./data --mode backfill --from 2020-01-01
  python fmp_universe_fetch.py --universe ./data/universe_list.csv --outdir ./data --mode daily
  python fmp_universe_fetch.py --universe ./data/universe_list.csv --outdir ./data --mode daily --only-symbol AAPL,MSFT
  python fmp_universe_fetch.py --universe ./data/universe_list.csv --outdir ./data --mode backfill --index-symbols SP500,NASDAQ,DOWJONES
  python fmp_universe_fetch.py --universe ./data/universe_list.csv --outdir ./data --mode daily --daily-include-insider
  python fmp_universe_fetch.py --universe ./data/universe_list.csv --outdir ./data --mode daily --daily-include-index --index-symbols SP500
  python fmp_universe_fetch.py --universe ./data/universe_list.csv --outdir ./data --mode daily --include-company-profile --include-shares-snapshot
"""
from __future__ import annotations

import argparse
import hashlib
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

PATH_EOD_FULL = "/stable/historical-price-eod/full"
PATH_PROFILE = "/stable/profile"
PATH_EOD_DIVIDEND_ADJUSTED = "/stable/historical-price-eod/dividend-adjusted"
PATH_SPLITS = "/stable/splits"
PATH_INCOME = "/stable/income-statement"
PATH_BALANCE = "/stable/balance-sheet-statement"
PATH_CASHFLOW = "/stable/cash-flow-statement"
PATH_DIVIDENDS = "/stable/dividends"
PATH_EARNINGS = "/stable/earnings-company"
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
ESTIMATES_SNAPSHOT_COLUMNS = ["symbol", "asOfDate", "epsNextY", "epsNextQ", "epsThisY"]
TARGETS_SNAPSHOT_COLUMNS = ["symbol", "asOfDate", "targetPrice"]
SHARES_SNAPSHOT_COLUMNS = ["symbol", "asOfDate", "sharesOutstanding", "sharesFloat"]
INDEX_MEMBERSHIP_COLUMNS = ["indexSymbol", "asOfDate", "memberSymbol", "isMember"]
COMPANY_PROFILE_SNAPSHOT_COLUMNS = ["symbol", "asOfDate", "employees", "ipoDate", "sharesOutstanding"]
INSIDER_TRANSACTIONS_COLUMNS = [
    "symbol", "transactionDate", "reportingCik", "reportingName", "transactionType",
    "securitiesTransacted", "price", "value", "securitiesOwned",
    "securityName", "formType", "acquisitionOrDisposition", "link",
]
INSIDER_HOLDINGS_SNAPSHOT_COLUMNS = [
    "symbol", "asOfDate", "reportingCik", "reportingName", "securitiesOwned", "securityName", "lastTransactionDate",
]

CUTOFF_DATE = "2020-01-01"
MAX_PROFILE_BULK_PARTS = 5000
PROFILE_BULK_STAGNATION_PARTS = 50

WATERMARKS_COLUMNS = ["table_name", "symbol", "last_date", "last_checked_at", "last_status"]
WATERMARKS_PK = ["table_name", "symbol"]
DIVIDENDS_LIGHT_LIMIT = 2
DIVIDENDS_LIGHT_MAX_RESPONSE_ROWS = 10  # if response has more rows, consider limit not supported


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


def load_watermarks(outdir: Path) -> pd.DataFrame:
    path = outdir / "watermarks.parquet"
    if not path.exists():
        return pd.DataFrame(columns=WATERMARKS_COLUMNS)
    try:
        df = pd.read_parquet(path)
        for c in WATERMARKS_COLUMNS:
            if c not in df.columns:
                df[c] = pd.NA
        return df.reindex(columns=WATERMARKS_COLUMNS)
    except Exception:
        return pd.DataFrame(columns=WATERMARKS_COLUMNS)


def _safe_log_message(ex: Exception) -> str:
    """Return a short message for logging without risking API key exposure."""
    return type(ex).__name__ + (f": {str(ex)[:80]}" if str(ex) and "apikey" not in str(ex).lower() and "api_key" not in str(ex).lower() else "")


def get_watermark(df: pd.DataFrame, table_name: str, symbol: str) -> Optional[str]:
    if df.empty or "table_name" not in df.columns or "symbol" not in df.columns:
        return None
    m = (df["table_name"] == table_name) & (df["symbol"] == symbol)
    rows = df.loc[m]
    if rows.empty or "last_date" not in rows.columns:
        return None
    val = rows["last_date"].iloc[0]
    if pd.isna(val) or not str(val).strip():
        return None
    return str(val).strip()[:10]


def touch_watermark(
    df: pd.DataFrame,
    table_name: str,
    symbol: str,
    today: str,
    status: str = "ok",
) -> pd.DataFrame:
    """Update last_checked_at and last_status (for every detection attempt). status: 'ok' | 'fail'. Returns DataFrame (caller assign back)."""
    today = str(today).strip()[:10]
    status = "ok" if str(status).strip().lower() == "ok" else "fail"
    for c in WATERMARKS_COLUMNS:
        if c not in df.columns:
            df[c] = pd.NA
    out = df.copy()
    m = (out["table_name"].astype(str) == table_name) & (out["symbol"].astype(str) == symbol)
    if m.any():
        out.loc[m, "last_checked_at"] = today
        out.loc[m, "last_status"] = status
    else:
        new_row = pd.DataFrame([{
            "table_name": table_name,
            "symbol": symbol,
            "last_date": pd.NA,
            "last_checked_at": today,
            "last_status": status,
        }])
        out = pd.concat([out, new_row], ignore_index=True)
    return out


def set_watermark_date(
    df: pd.DataFrame,
    table_name: str,
    symbol: str,
    new_date: str,
    today: str,
) -> pd.DataFrame:
    """Update last_date, last_checked_at and last_status=ok (on successful data update). Returns DataFrame (caller assign back)."""
    new_date = str(new_date).strip()[:10]
    today = str(today).strip()[:10]
    for c in WATERMARKS_COLUMNS:
        if c not in df.columns:
            df[c] = pd.NA
    out = df.copy()
    m = (out["table_name"].astype(str) == table_name) & (out["symbol"].astype(str) == symbol)
    if m.any():
        out.loc[m, "last_date"] = new_date
        out.loc[m, "last_checked_at"] = today
        out.loc[m, "last_status"] = "ok"
    else:
        new_row = pd.DataFrame([{
            "table_name": table_name,
            "symbol": symbol,
            "last_date": new_date,
            "last_checked_at": today,
            "last_status": "ok",
        }])
        out = pd.concat([out, new_row], ignore_index=True)
    return out


def save_watermarks(outdir: Path, df: pd.DataFrame) -> None:
    """Save watermarks.parquet; dedupe by (table_name, symbol) keep last."""
    if df.empty:
        return
    outdir.mkdir(parents=True, exist_ok=True)
    path = outdir / "watermarks.parquet"
    for c in WATERMARKS_COLUMNS:
        if c not in df.columns:
            df[c] = pd.NA
    out = df.reindex(columns=WATERMARKS_COLUMNS).copy()
    pk_avail = [c for c in WATERMARKS_PK if c in out.columns]
    if pk_avail:
        tmp = out[pk_avail].copy()
        for c in pk_avail:
            tmp[c] = tmp[c].astype("string").fillna("<NA>")
        out["_pk"] = tmp[pk_avail[0]]
        for c in pk_avail[1:]:
            out["_pk"] = out["_pk"].astype(str) + "|" + tmp[c].astype(str)
        out = out.drop_duplicates(subset=["_pk"], keep="last").drop(columns=["_pk"])
    out = out.reindex(columns=WATERMARKS_COLUMNS)
    out.to_parquet(path, index=False, engine="pyarrow")


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


def _safe_params(params: Dict[str, Any]) -> Dict[str, Any]:
    return {k: ("***" if k.lower() == "apikey" else v) for k, v in params.items()}


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


def fetch_dividends_light(
    session: requests.Session,
    rl: RateLimiter,
    api_key: str,
    symbol: str,
    limit: int = DIVIDENDS_LIGHT_LIMIT,
    call_counter: Optional[Dict[str, Any]] = None,
) -> Tuple[List[Dict[str, Any]], bool]:
    """Try dividends with limit for detection. Returns (rows, light_ok). light_ok=False if API errors or returns too many rows (limit ignored)."""
    try:
        data = fmp_get(
            session, rl, PATH_DIVIDENDS,
            {"symbol": symbol, "limit": limit},
            api_key,
            call_counter=call_counter,
            allow_404_empty=True,
        )
    except Exception:
        return ([], False)
    if not isinstance(data, list):
        return ([], False)
    if len(data) > DIVIDENDS_LIGHT_MAX_RESPONSE_ROWS:
        return ([], False)
    return (data, True)


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
        })
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
            "sharesOutstanding": pick(r, ["commonStockSharesOutstanding", "sharesOutstanding"]),
        })
    for r in cashflow:
        d = pick_date10(r, ["date", "fiscalDateEnding", "fillingDate"])
        per = pick_text(r, ["period"])
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


def build_company_profile_snapshot_from_bulk(
    rows: List[Dict[str, Any]],
    sym_set: Set[str],
    as_of: str,
) -> pd.DataFrame:
    out = []
    for r in rows:
        sym = (r.get("symbol") or r.get("ticker") or "").strip().upper()
        if not sym or (sym_set and sym not in sym_set):
            continue
        emp = pick(r, ["fullTimeEmployees", "employees", "employeeCount"])
        if emp is not None and isinstance(emp, (int, float)):
            emp = float(emp)
        out.append({
            "symbol": sym,
            "asOfDate": as_of,
            "employees": emp,
            "ipoDate": pick_date10(r, ["ipoDate", "ipoDate", "date"]),
            "sharesOutstanding": pick(r, ["sharesOutstanding", "outstandingShares", "commonStockSharesOutstanding"]),
        })
    if not out:
        return pd.DataFrame(columns=COMPANY_PROFILE_SNAPSHOT_COLUMNS)
    return pd.DataFrame(out).reindex(columns=COMPANY_PROFILE_SNAPSHOT_COLUMNS)


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


def _max_date_from_rows(rows: List[Dict[str, Any]], keys: List[str]) -> Optional[str]:
    """Extract max YYYY-MM-DD from list of dicts using pick_date10 on keys."""
    dates: List[str] = []
    for r in rows:
        d = pick_date10(r, keys)
        if d:
            dates.append(d)
    return max(dates) if dates else None


# -----------------------------------------------------------------------------
# 증분 감지 공통: 저장된 최신 날짜 맵, 날짜 범위 필터, PK/컬럼 비교 (earnings/dividends 확장 가능)
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


def _value_equals(a: Any, b: Any) -> bool:
    """NaN==NaN 동일, float는 동일 또는 abs(diff)<1e-9."""
    if pd.isna(a) and pd.isna(b):
        return True
    if pd.isna(a) or pd.isna(b):
        return False
    try:
        fa, fb = float(a), float(b)
        if fa == fb:
            return True
        return abs(fa - fb) < 1e-9
    except (TypeError, ValueError):
        return a == b


def detect_changes(
    new_df: pd.DataFrame,
    existing_df: pd.DataFrame,
    pk_columns: List[str],
    compare_columns: List[str],
) -> bool:
    """PK가 새로 있거나, 기존 PK인데 compare_columns 중 하나라도 값이 다르면 True. earnings/dividends 등 재사용 가능."""
    if new_df.empty:
        return False
    if existing_df.empty:
        return True
    if not all(p in existing_df.columns for p in pk_columns):
        return True
    existing_sub = existing_df[[c for c in pk_columns + compare_columns if c in existing_df.columns]].copy()
    new_sub = new_df[[c for c in pk_columns + compare_columns if c in new_df.columns]].copy()
    existing_by_pk = existing_sub.set_index(pk_columns)
    for _, new_row in new_sub.iterrows():
        pk_tuple = tuple(new_row.get(p) for p in pk_columns)
        if pk_tuple not in existing_by_pk.index:
            return True
        old_row = existing_by_pk.loc[pk_tuple]
        if isinstance(old_row, pd.DataFrame):
            old_row = old_row.iloc[0]
        for col in compare_columns:
            if col not in new_sub.columns or col not in existing_sub.columns:
                continue
            if not _value_equals(new_row.get(col), old_row.get(col)):
                return True
    return False


def run_watermark_tables(
    session: requests.Session,
    rl: RateLimiter,
    api_key: str,
    outdir: Path,
    universe: List[str],
    call_counter: Dict[str, Any],
    today: str,
    max_workers: int,
    insider_limit: int = 200,
    insider_common_stock_only: bool = True,
    include_insider: bool = False,
    run_dividends_detection: bool = False,
    weekly_dividends_full_refresh: bool = False,
    num_shards: int = 1,
    shard_id: int = 0,
    monthly_financials_cap: Optional[int] = None,
    log_start: bool = False,
) -> None:
    """Watermark-based change detection. earnings → trigger financials. last_checked_at/last_status on every attempt; last_date only on update.
    run_dividends_detection: daily=False; weekly=True. weekly_dividends_full_refresh: when limit unsupported, allow full refresh in weekly (opt-in)."""
    watermarks_df = load_watermarks(outdir)
    today_dt = date.today()
    force_financials_monthly = today_dt.day == 1
    if log_start:
        log.info(
            "run_watermark_tables: today=%s monthly_safeguard=%s run_dividends_detection=%s weekly_dividends_full_refresh=%s universe_size=%s include_insider=%s",
            today, force_financials_monthly, run_dividends_detection, weekly_dividends_full_refresh, len(universe), include_insider,
        )

    # --- earnings_events (limit=2 lightweight detection) ---
    symbols_earnings: List[str] = []
    for sym in universe:
        try:
            rows = fetch_earnings(session, rl, api_key, sym, 2, call_counter)
            watermarks_df = touch_watermark(watermarks_df, "earnings_events", sym, today, status="ok")
            max_d = _max_date_from_rows(rows, ["date", "fiscalDateEnding", "earningsDate"])
            w = get_watermark(watermarks_df, "earnings_events", sym)
            if max_d and (w is None or max_d > w):
                symbols_earnings.append(sym)
                log.info("[earnings] %s → update detected", sym)
            else:
                log.info("[earnings] %s → no update", sym)
        except Exception as e:
            log.info("[earnings] %s detection failed: %s", sym, _safe_log_message(e))
            watermarks_df = touch_watermark(watermarks_df, "earnings_events", sym, today, status="fail")
    if symbols_earnings:
        def _earn_full(s: str) -> pd.DataFrame:
            sess = make_session()
            try:
                r = fetch_earnings(sess, rl, api_key, s, 400, call_counter)
                df = build_earnings_events(r, s, CUTOFF_DATE)
                return df
            except Exception as e:
                log.debug("%s earnings full: %s", s, e)
                return pd.DataFrame(columns=EARNINGS_EVENTS_COLUMNS)
        if max_workers <= 1:
            earn_dfs = [_earn_full(s) for s in symbols_earnings]
        else:
            with ThreadPoolExecutor(max_workers=max_workers) as ex:
                earn_dfs = list(ex.map(_earn_full, symbols_earnings))
        non_empty = [d for d in earn_dfs if not d.empty]
        if non_empty:
            combined = pd.concat(non_empty, ignore_index=True)
            upsert_table(outdir, "earnings_events", combined, ["symbol", "earningsDate"], EARNINGS_EVENTS_COLUMNS)
            for s in symbols_earnings:
                log.info("[earnings] %s → updated", s)
            if "earningsDate" in combined.columns:
                for s in symbols_earnings:
                    sub = combined.loc[combined["symbol"] == s, "earningsDate"].dropna().astype(str)
                    if not sub.empty:
                        max_d = sub.str[:10].max()
                        watermarks_df = set_watermark_date(watermarks_df, "earnings_events", s, max_d, today)
    # --- dividends_events: light detection (limit=2) when API supports it; else weekly full refresh only if --weekly-dividends-full-refresh ---
    symbols_div: List[str] = []
    dividends_light_available: Optional[bool] = None
    if run_dividends_detection:
        if universe:
            probe_rows, dividends_light_available = fetch_dividends_light(
                session, rl, api_key, universe[0], DIVIDENDS_LIGHT_LIMIT, call_counter
            )
            if dividends_light_available is False and not weekly_dividends_full_refresh:
                log.info(
                    "dividends: LIMIT unsupported; weekly dividends skipped (use --weekly-dividends-full-refresh to enable full refresh)"
                )
            elif dividends_light_available is False and weekly_dividends_full_refresh:
                log.info("dividends: LIMIT unsupported; performing weekly dividends full refresh (heavy)")
        if dividends_light_available:
            for sym in universe:
                try:
                    rows, light_ok = fetch_dividends_light(session, rl, api_key, sym, DIVIDENDS_LIGHT_LIMIT, call_counter)
                    watermarks_df = touch_watermark(watermarks_df, "dividends_events", sym, today, status="ok")
                    if not light_ok:
                        log.info("[dividends] %s → light failed, skip", sym)
                        continue
                    max_d = _max_date_from_rows(rows, ["date", "exDividendDate", "exDate"])
                    w = get_watermark(watermarks_df, "dividends_events", sym)
                    if max_d and (w is None or max_d > w):
                        symbols_div.append(sym)
                        log.info("[dividends] %s → update detected (light)", sym)
                    else:
                        log.info("[dividends] %s → no update", sym)
                except Exception as e:
                    log.info("[dividends] %s detection failed: %s", sym, _safe_log_message(e))
                    watermarks_df = touch_watermark(watermarks_df, "dividends_events", sym, today, status="fail")
        elif dividends_light_available is False and weekly_dividends_full_refresh:
            for sym in universe:
                try:
                    rows = fetch_dividends(session, rl, api_key, sym, call_counter)
                    watermarks_df = touch_watermark(watermarks_df, "dividends_events", sym, today, status="ok")
                    max_d = _max_date_from_rows(rows, ["date", "exDividendDate", "exDate"])
                    w = get_watermark(watermarks_df, "dividends_events", sym)
                    if max_d and (w is None or max_d > w):
                        symbols_div.append(sym)
                        log.info("[dividends] %s → update detected (full)", sym)
                    else:
                        log.info("[dividends] %s → skipped", sym)
                except Exception as e:
                    log.info("[dividends] %s detection failed: %s", sym, _safe_log_message(e))
                    watermarks_df = touch_watermark(watermarks_df, "dividends_events", sym, today, status="fail")
    if symbols_div:
        def _div_full(s: str) -> pd.DataFrame:
            sess = make_session()
            try:
                r = fetch_dividends(sess, rl, api_key, s, call_counter)
                return build_dividends_events(r, s, CUTOFF_DATE)
            except Exception as e:
                log.debug("%s dividends full: %s", s, e)
                return pd.DataFrame(columns=DIVIDENDS_EVENTS_COLUMNS)
        if max_workers <= 1:
            div_dfs = [_div_full(s) for s in symbols_div]
        else:
            with ThreadPoolExecutor(max_workers=max_workers) as ex:
                div_dfs = list(ex.map(_div_full, symbols_div))
        non_empty = [d for d in div_dfs if not d.empty]
        if non_empty:
            combined = pd.concat(non_empty, ignore_index=True)
            combined["_d"] = combined["paymentDate"].astype(str)
            combined = combined.drop_duplicates(subset=["symbol", "exDate", "_d", "dividend"], keep="last")
            combined = combined.drop(columns=["_d"], errors="ignore")
            upsert_table(outdir, "dividends_events", combined, ["symbol", "exDate", "paymentDate", "dividend"], DIVIDENDS_EVENTS_COLUMNS)
            for s in symbols_div:
                log.info("[dividends] %s → updated", s)
            if "exDate" in combined.columns:
                for s in symbols_div:
                    sub = combined.loc[combined["symbol"] == s, "exDate"].dropna().astype(str)
                    if not sub.empty:
                        max_d = sub.str[:10].max()
                        watermarks_df = set_watermark_date(watermarks_df, "dividends_events", s, max_d, today)
    # --- financials_quarterly: 저장된 최신 날짜 기반 증분 감지 → 변경된 심볼만 upsert (earnings 트리거 제거) ---
    existing_fin = load_existing_table(outdir, "financials_quarterly")
    if not existing_fin.empty and "fiscalDate" in existing_fin.columns:
        existing_fin = existing_fin.copy()
        existing_fin["fiscalDate"] = pd.to_datetime(existing_fin["fiscalDate"], errors="coerce").dt.strftime("%Y-%m-%d")
    last_saved_fin = last_saved_date_map(existing_fin, "symbol", "fiscalDate", CUTOFF_DATE)
    symbols_fin = list(universe)
    if force_financials_monthly and monthly_financials_cap is not None and monthly_financials_cap >= 1:
        symbols_fin = symbols_fin[:monthly_financials_cap]
        log.info("monthly safeguard: financials capped to %s symbols", len(symbols_fin))
    to_date_fin = today
    cutoff_dt = date.fromisoformat(CUTOFF_DATE[:10])
    fin_date_keys = ["date", "fiscalDateEnding", "fillingDate"]

    collected_fin: List[pd.DataFrame] = []
    succeeded_fin: List[str] = []
    for sym in symbols_fin:
        last_saved = last_saved_fin.get(sym, CUTOFF_DATE)
        try:
            last_dt = date.fromisoformat(str(last_saved)[:10])
        except ValueError:
            last_dt = cutoff_dt
        from_dt = max(last_dt - timedelta(days=7), cutoff_dt)
        from_date_fin = from_dt.isoformat()
        try:
            inc = fetch_income(session, rl, api_key, sym, 200, call_counter)
            bal = fetch_balance(session, rl, api_key, sym, 200, call_counter)
            cf = fetch_cashflow(session, rl, api_key, sym, 200, call_counter)
        except Exception as e:
            log.debug("financials fetch %s: %s", sym, _safe_log_message(e))
            watermarks_df = touch_watermark(watermarks_df, "financials_quarterly", sym, today, status="fail")
            continue
        inc_f = filter_rows_by_date_range(inc or [], fin_date_keys, from_date_fin, to_date_fin)
        bal_f = filter_rows_by_date_range(bal or [], fin_date_keys, from_date_fin, to_date_fin)
        cf_f = filter_rows_by_date_range(cf or [], fin_date_keys, from_date_fin, to_date_fin)
        new_df = build_financials_quarterly(inc_f, bal_f, cf_f, sym, from_date_fin)
        existing_slice = existing_fin.loc[existing_fin["symbol"] == sym] if not existing_fin.empty else pd.DataFrame()
        changed = detect_changes(new_df, existing_slice, FINANCIALS_PK, FINANCIALS_COMPARE_COLUMNS)
        new_max_date = None
        if not new_df.empty and "fiscalDate" in new_df.columns:
            new_max_date = new_df["fiscalDate"].max()
            if hasattr(new_max_date, "strftime"):
                new_max_date = new_max_date.strftime("%Y-%m-%d")[:10]
            else:
                new_max_date = str(new_max_date)[:10] if new_max_date is not None else None
        log.info(
            "[financials] symbol=%s last_saved_date=%s new_max_date=%s n_new_rows=%s changed=%s",
            sym, last_saved, new_max_date, len(new_df), changed,
        )
        if new_df.empty:
            watermarks_df = touch_watermark(watermarks_df, "financials_quarterly", sym, today, status="fail")
            continue
        if changed:
            collected_fin.append(new_df)
            succeeded_fin.append(sym)
    if collected_fin:
        combined_fin = pd.concat(collected_fin, ignore_index=True)
        upsert_table(outdir, "financials_quarterly", combined_fin, FINANCIALS_PK, FINANCIALS_QUARTERLY_COLUMNS)
        for s in succeeded_fin:
            watermarks_df = touch_watermark(watermarks_df, "financials_quarterly", s, today, status="ok")
            sub = combined_fin.loc[combined_fin["symbol"] == s, "fiscalDate"].dropna().astype(str)
            if not sub.empty:
                max_d = sub.str[:10].max()
                watermarks_df = set_watermark_date(watermarks_df, "financials_quarterly", s, max_d, today)
        log.info("financials upserted rows=%s symbols=%s", len(combined_fin), len(succeeded_fin))
    # --- insider_transactions (stable /stable/insider-trading/search); 403/restricted → one-time disable ---
    insider_disabled = False
    insider_warning_logged = False

    def _insider_restricted(code: int, body: str) -> bool:
        if code in (402, 403):
            return True
        if code >= 400 and body and ("restricted" in body.lower() or "subscription" in body.lower() or "not available" in body.lower()):
            return True
        return False

    if include_insider:
        symbols_insider: List[str] = []
        first_insider_probe = True
        for sym in universe:
            if insider_disabled:
                break
            if first_insider_probe:
                first_insider_probe = False
                result = fetch_insider_trading_stable(session, rl, api_key, sym, page=0, limit=2, call_counter=call_counter, return_status=True)
                if isinstance(result, tuple) and len(result) == 2 and result[1] is not None:
                    code, body, headers = result[1]
                    snippet = _sanitize_snippet(body if isinstance(body, str) else str(body))
                    rl_h = _rate_limit_headers(headers)
                    retry_after = rl_h.get("Retry-After", "<none>") or "<none>"
                    rl_remaining = rl_h.get("X-RateLimit-Remaining", "")
                    log.info(
                        "insider probe: symbol=%s status=%s snippet=\"%s\" retry_after=\"%s\" rl_remaining=\"%s\"",
                        sym, code, snippet, retry_after, rl_remaining,
                    )
                    if _insider_restricted(code, body if isinstance(body, str) else str(body)):
                        if not insider_warning_logged:
                            log.warning("insider_snapshot/transactions disabled due to subscription restriction")
                            insider_warning_logged = True
                        insider_disabled = True
                        watermarks_df = touch_watermark(watermarks_df, "insider_transactions", sym, today, status="fail")
                        break
                else:
                    rows = result[0] if isinstance(result, tuple) and result[0] is not None else []
                    watermarks_df = touch_watermark(watermarks_df, "insider_transactions", sym, today, status="ok")
                    max_d = _max_date_from_rows(rows, ["transactionDate", "date"])
                    w = get_watermark(watermarks_df, "insider_transactions", sym)
                    if max_d and (w is None or max_d > w):
                        symbols_insider.append(sym)
                        log.info("[insider] %s → update detected", sym)
                    else:
                        log.info("[insider] %s → skipped", sym)
                continue
            try:
                rows = fetch_insider_trading_stable(session, rl, api_key, sym, page=0, limit=2, call_counter=call_counter)
                watermarks_df = touch_watermark(watermarks_df, "insider_transactions", sym, today, status="ok")
                max_d = _max_date_from_rows(rows, ["transactionDate", "date"])
                w = get_watermark(watermarks_df, "insider_transactions", sym)
                if max_d and (w is None or max_d > w):
                    symbols_insider.append(sym)
                    log.info("[insider] %s → update detected", sym)
                else:
                    log.info("[insider] %s → skipped", sym)
            except RuntimeError as e:
                msg = str(e).lower()
                if "403" in msg or "402" in msg or "restricted" in msg:
                    if not insider_warning_logged:
                        log.warning("insider_snapshot/transactions disabled due to subscription restriction")
                        insider_warning_logged = True
                    insider_disabled = True
                    watermarks_df = touch_watermark(watermarks_df, "insider_transactions", sym, today, status="fail")
                    break
            except Exception as e:
                log.info("[insider] %s detection failed: %s", sym, _safe_log_message(e))
                watermarks_df = touch_watermark(watermarks_df, "insider_transactions", sym, today, status="fail")
        if symbols_insider and not insider_disabled:
            def _insider_full(s: str) -> pd.DataFrame:
                sess = make_session()
                try:
                    r = fetch_insider_trading_stable(sess, rl, api_key, s, page=0, limit=insider_limit, call_counter=call_counter)
                    return build_insider_transactions(r, s)
                except Exception as e:
                    log.debug("%s insider full: %s", s, e)
                    return pd.DataFrame(columns=INSIDER_TRANSACTIONS_COLUMNS)
            if max_workers <= 1:
                insider_dfs = [_insider_full(s) for s in symbols_insider]
            else:
                with ThreadPoolExecutor(max_workers=max_workers) as ex:
                    insider_dfs = list(ex.map(_insider_full, symbols_insider))
            non_empty = [d for d in insider_dfs if not d.empty]
            if non_empty:
                combined = pd.concat(non_empty, ignore_index=True)
                upsert_table(outdir, "insider_transactions", combined, ["symbol", "transactionDate", "reportingCik", "transactionType", "securitiesTransacted", "securityName"], INSIDER_TRANSACTIONS_COLUMNS)
                holdings_df = build_insider_holdings_snapshot(combined, today, common_stock_only=insider_common_stock_only)
                if not holdings_df.empty:
                    upsert_table(outdir, "insider_holdings_snapshot", holdings_df, ["symbol", "asOfDate", "reportingCik", "securityName"], INSIDER_HOLDINGS_SNAPSHOT_COLUMNS)
                for s in symbols_insider:
                    log.info("[insider] %s → updated", s)
                if "transactionDate" in combined.columns:
                    for s in symbols_insider:
                        sub = combined.loc[combined["symbol"] == s, "transactionDate"].dropna().astype(str)
                        if not sub.empty:
                            max_d = sub.str[:10].max()
                            watermarks_df = set_watermark_date(watermarks_df, "insider_transactions", s, max_d, today)
    save_watermarks(outdir, watermarks_df)


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

    # (3) financials_quarterly
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
        est_df = pd.concat(est_rows, ignore_index=True)
        upsert_table(outdir, "estimates_snapshot", est_df, ["symbol", "asOfDate"], ESTIMATES_SNAPSHOT_COLUMNS)
    if tgt_rows:
        tgt_df = pd.concat(tgt_rows, ignore_index=True)
        upsert_table(outdir, "targets_snapshot", tgt_df, ["symbol", "asOfDate"], TARGETS_SNAPSHOT_COLUMNS)

    if include_shares_snapshot:
        sh_rows: List[Dict[str, Any]] = []
        shares_disabled = False
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
            except RuntimeError as e:
                msg = str(e).lower()
                if "403" in msg or "402" in msg or "restricted" in msg or "subscription" in msg:
                    if not shares_disabled:
                        log.warning("shares_snapshot disabled: restricted/unsupported endpoint")
                        shares_disabled = True
                    break
            except Exception as e:
                log.debug("shares_float_all page %s: %s", page, e)
                break
        if sh_rows and not shares_disabled:
            sh_df = pd.DataFrame(sh_rows).reindex(columns=SHARES_SNAPSHOT_COLUMNS)
            upsert_table(outdir, "shares_snapshot", sh_df, ["symbol", "asOfDate"], SHARES_SNAPSHOT_COLUMNS)

    if include_company_profile:
        profile_disabled = False
        profile_rows: List[Dict[str, Any]] = []
        for sym in universe:
            if profile_disabled:
                break
            try:
                raw = fetch_profile_symbol(session, rl, api_key, sym.strip().upper(), call_counter)
                if raw:
                    profile_rows.extend(raw)
            except RuntimeError as e:
                msg = str(e).lower()
                if "403" in msg or "402" in msg or "restricted" in msg or "subscription" in msg:
                    if not profile_disabled:
                        log.warning("profile disabled: restricted by subscription")
                        profile_disabled = True
                    break
            except Exception:
                pass
        if profile_rows and not profile_disabled:
            profile_df = build_company_profile_snapshot_from_bulk(profile_rows, sym_set, today)
            if not profile_df.empty:
                upsert_table(outdir, "company_profile_snapshot", profile_df, ["symbol", "asOfDate"], COMPANY_PROFILE_SNAPSHOT_COLUMNS)

    # insider_transactions: stable /stable/insider-trading/search; 403 → skip entire block, one-time warning
    all_insider_dfs: List[pd.DataFrame] = []
    insider_backfill_disabled = False
    if universe:
        first_sym = universe[0]
        result = fetch_insider_trading_stable(session, rl, api_key, first_sym, page=0, limit=2, call_counter=call_counter, return_status=True)
        if isinstance(result, tuple) and len(result) == 2 and result[1] is not None:
            code, body, headers = result[1]
            snippet = _sanitize_snippet(body if isinstance(body, str) else str(body))
            rl_h = _rate_limit_headers(headers)
            log.info(
                "insider probe: symbol=%s status=%s snippet=\"%s\" retry_after=\"%s\" rl_remaining=\"%s\"",
                first_sym, code, snippet, rl_h.get("Retry-After", "<none>") or "<none>", rl_h.get("X-RateLimit-Remaining", ""),
            )
            body_str = (body if isinstance(body, str) else str(body)).lower()
            if code in (402, 403) or (code >= 400 and ("restricted" in body_str or "subscription" in body_str)):
                log.warning("insider_snapshot/transactions disabled due to subscription restriction")
                insider_backfill_disabled = True

    if not insider_backfill_disabled:

        def _insider_one(sym: str) -> pd.DataFrame:
            sess = make_session()
            try:
                rows = fetch_insider_trading_stable(sess, rl, api_key, sym, page=0, limit=insider_limit, call_counter=call_counter)
                return build_insider_transactions(rows, sym)
            except Exception as e:
                log.debug("%s insider_trading: %s", sym, e)
                return pd.DataFrame(columns=INSIDER_TRANSACTIONS_COLUMNS)

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
        upsert_table(outdir, "insider_transactions", insider_combined, ["symbol", "transactionDate", "reportingCik", "transactionType", "securitiesTransacted", "securityName"], INSIDER_TRANSACTIONS_COLUMNS)
        holdings_df = build_insider_holdings_snapshot(insider_combined, today, common_stock_only=insider_common_stock_only)
        if not holdings_df.empty:
            upsert_table(outdir, "insider_holdings_snapshot", holdings_df, ["symbol", "asOfDate", "reportingCik", "securityName"], INSIDER_HOLDINGS_SNAPSHOT_COLUMNS)

    # index_membership: real fetch per index_symbol, build rows for sym_set only
    index_list = index_symbols or ["SP500"]
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
            log.debug("index_membership %s: %s", idx_sym, e)


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
    insider_limit: int = 200,
    insider_common_stock_only: bool = True,
    index_symbols: Optional[List[str]] = None,
    daily_include_insider: bool = False,
    daily_include_index: bool = False,
    include_company_profile: bool = False,
    include_shares_snapshot: bool = False,
) -> None:
    """Daily 모드: prices_eod(per-symbol 증분), estimates/targets 갱신. profile/shares는 opt-in. insider/index는 --daily-include-* 시에만."""

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

    if include_shares_snapshot:
        sh_rows: List[Dict[str, Any]] = []
        shares_disabled = False
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
            except RuntimeError as e:
                msg = str(e).lower()
                if "403" in msg or "402" in msg or "restricted" in msg or "subscription" in msg:
                    if not shares_disabled:
                        log.warning("shares_snapshot disabled: restricted/unsupported endpoint")
                        shares_disabled = True
                    break
            except Exception:
                break
        if sh_rows and not shares_disabled:
            sh_df = pd.DataFrame(sh_rows).reindex(columns=SHARES_SNAPSHOT_COLUMNS)
            upsert_table(outdir, "shares_snapshot", sh_df, ["symbol", "asOfDate"], SHARES_SNAPSHOT_COLUMNS)

    if include_company_profile:
        profile_disabled = False
        profile_rows_d: List[Dict[str, Any]] = []
        for sym in universe:
            if profile_disabled:
                break
            try:
                raw = fetch_profile_symbol(session, rl, api_key, sym.strip().upper(), call_counter)
                if raw:
                    profile_rows_d.extend(raw)
            except RuntimeError as e:
                msg = str(e).lower()
                if "403" in msg or "402" in msg or "restricted" in msg or "subscription" in msg:
                    if not profile_disabled:
                        log.warning("profile disabled: restricted by subscription")
                        profile_disabled = True
                    break
            except Exception:
                pass
        if profile_rows_d and not profile_disabled:
            profile_df_d = build_company_profile_snapshot_from_bulk(profile_rows_d, sym_set, as_of)
            if not profile_df_d.empty:
                upsert_table(outdir, "company_profile_snapshot", profile_df_d, ["symbol", "asOfDate"], COMPANY_PROFILE_SNAPSHOT_COLUMNS)

    # Watermark-based detection (daily: dividends 감지 생략; insider는 daily_include_insider 시에만)
    run_watermark_tables(
        session, rl, api_key, outdir, universe, call_counter, as_of,
        max_workers=max_workers,
        insider_limit=insider_limit,
        insider_common_stock_only=insider_common_stock_only,
        include_insider=daily_include_insider,
        run_dividends_detection=False,
        log_start=True,
    )

    if daily_include_index:
        # index_membership: real fetch per index_symbol (daily opt-in)
        index_list_d = index_symbols or ["SP500"]
        for idx_sym_d in index_list_d:
            idx_sym_d = idx_sym_d.strip().upper()
            if not idx_sym_d:
                continue
            try:
                members_set_d = fetch_index_constituents(session, rl, api_key, idx_sym_d, call_counter)
                idx_df_d = build_index_membership(sym_set, idx_sym_d, members_set_d, as_of)
                if not idx_df_d.empty:
                    upsert_table(outdir, "index_membership", idx_df_d, ["indexSymbol", "asOfDate", "memberSymbol"], INDEX_MEMBERSHIP_COLUMNS)
            except Exception as e:
                log.debug("index_membership %s: %s", idx_sym_d, e)


def run_weekly(
    session: requests.Session,
    rl: RateLimiter,
    api_key: str,
    outdir: Path,
    universe: List[str],
    call_counter: Dict[str, Any],
    max_workers: int,
    insider_limit: int = 200,
    insider_common_stock_only: bool = True,
    include_insider: bool = False,
    weekly_dividends_full_refresh: bool = False,
    num_shards: int = 1,
    shard_id: int = 0,
    monthly_financials_cap: Optional[int] = None,
) -> None:
    """Weekly 모드: prices_eod, estimates/targets/shares/company_profile 미실행. 워터마크 테이블만(earnings/dividends/financials/insider) 감지→변경시 full fetch. dividends: limit 지원 시 light 감지; 미지원 시 --weekly-dividends-full-refresh 시에만 full refresh."""
    today = date.today().isoformat()
    today_dt = date.today()
    monthly_safeguard = today_dt.day == 1
    run_dividends_detection = True  # weekly always attempts dividends (light or full per flag)
    log.info(
        "mode=weekly universe_size=%s num_shards=%s shard_id=%s include_insider=%s run_dividends_detection=%s weekly_dividends_full_refresh=%s today=%s monthly_safeguard=%s",
        len(universe), num_shards, shard_id, include_insider, run_dividends_detection, weekly_dividends_full_refresh, today, monthly_safeguard,
    )
    run_watermark_tables(
        session, rl, api_key, outdir, universe, call_counter, today,
        max_workers=max_workers,
        insider_limit=insider_limit,
        insider_common_stock_only=insider_common_stock_only,
        include_insider=include_insider,
        run_dividends_detection=run_dividends_detection,
        weekly_dividends_full_refresh=weekly_dividends_full_refresh,
        num_shards=num_shards,
        shard_id=shard_id,
        monthly_financials_cap=monthly_financials_cap,
        log_start=True,
    )


def main() -> None:
    # Example: daily 기본 (insider/index 미실행): ... --mode daily
    # Example: daily + insider: ... --mode daily --daily-include-insider
    # Example: daily + index: ... --mode daily --daily-include-index --index-symbols SP500
    # Example: backfill with all index constituents: ... --mode backfill --index-symbols SP500,NASDAQ,DOWJONES
    ap = argparse.ArgumentParser(description="FMP Premium: 11 tables (prices_eod, financials_quarterly, ..., company_profile_snapshot, insider_*, index_membership)")
    ap.add_argument("--universe", required=True, help="universe_list.csv path")
    ap.add_argument("--outdir", required=True, help="Output directory")
    ap.add_argument("--from", dest="from_date", default="2020-01-01", help="Start date YYYY-MM-DD")
    ap.add_argument("--to", dest="to_date", default="", help="End date YYYY-MM-DD (backfill: today; daily: yesterday)")
    ap.add_argument("--max-workers", type=int, default=4, help="ThreadPool workers")
    ap.add_argument("--mode", choices=["backfill", "daily", "weekly"], default="backfill", help="backfill=full fetch; daily/weekly=watermark detection for earnings/dividends/financials/insider")
    ap.add_argument("--only-symbol", default="", help="Optional: AAPL,MSFT")
    ap.add_argument("--include-company-profile", action="store_true", help="Daily/backfill: fetch company_profile_snapshot (per-symbol profile; may be restricted by subscription)")
    ap.add_argument("--include-shares-snapshot", action="store_true", help="Daily/backfill: fetch shares_snapshot (shares-float-all; may be restricted/unsupported)")
    ap.add_argument("--num-shards", type=int, default=1, help="Shard universe into N parts (default 1 = no sharding)")
    ap.add_argument("--shard-id", type=int, default=0, help="Which shard to process (0..num_shards-1)")
    ap.add_argument("--insider-limit", type=int, default=200, help="Max insider transactions per symbol (default 200)")
    ap.add_argument("--insider-common-stock-only", action="store_true", default=True, help="Insider holdings: only Common Stock (default True)")
    ap.add_argument("--no-insider-common-stock-only", action="store_false", dest="insider_common_stock_only")
    ap.add_argument("--index-symbols", type=str, default="SP500", help="Index constituents to fetch, comma-separated (default SP500)")
    ap.add_argument("--daily-include-insider", action="store_true", help="Daily mode: also run insider_transactions + insider_holdings_snapshot (default off)")
    ap.add_argument("--daily-include-index", action="store_true", help="Daily mode: also run index_membership fetch (default off)")
    ap.add_argument("--weekly-include-insider", action="store_true", help="Weekly mode: run insider_transactions + insider_holdings_snapshot (default off)")
    ap.add_argument("--weekly-skip-insider", action="store_true", help="Weekly mode: do not run insider (no-op when default is already off)")
    ap.add_argument("--weekly-dividends-full-refresh", action="store_true", help="Weekly mode: when dividends API limit is unsupported, perform full refresh (default: skip dividends in that case)")
    ap.add_argument("--monthly-financials-cap", type=int, default=None, metavar="N", help="Cap symbols for monthly (day=1) financials refresh (default: no cap, current shard only)")
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

    index_symbols_list: Optional[List[str]] = None
    if getattr(args, "index_symbols", ""):
        index_symbols_list = [s.strip() for s in args.index_symbols.split(",") if s.strip()]

    if args.mode == "backfill":
        run_backfill(
            session, rl, api_key, outdir, universe,
            from_date, to_date,
            max_workers=args.max_workers,
            only_symbol=args.only_symbol or None,
            call_counter=call_counter,
            insider_limit=getattr(args, "insider_limit", 200),
            insider_common_stock_only=getattr(args, "insider_common_stock_only", True),
            index_symbols=index_symbols_list,
            include_company_profile=getattr(args, "include_company_profile", False),
            include_shares_snapshot=getattr(args, "include_shares_snapshot", False),
        )
    elif args.mode == "weekly":
        run_weekly(
            session, rl, api_key, outdir, universe, call_counter,
            max_workers=args.max_workers,
            insider_limit=getattr(args, "insider_limit", 200),
            insider_common_stock_only=getattr(args, "insider_common_stock_only", True),
            include_insider=getattr(args, "weekly_include_insider", False) and not getattr(args, "weekly_skip_insider", False),
            weekly_dividends_full_refresh=getattr(args, "weekly_dividends_full_refresh", False),
            num_shards=num_shards,
            shard_id=shard_id,
            monthly_financials_cap=getattr(args, "monthly_financials_cap", None),
        )
    else:
        run_daily(
            session, rl, api_key, outdir, universe,
            from_date, to_date,
            args.max_workers,
            args.only_symbol or None,
            call_counter,
            insider_limit=getattr(args, "insider_limit", 200),
            insider_common_stock_only=getattr(args, "insider_common_stock_only", True),
            index_symbols=index_symbols_list,
            daily_include_insider=getattr(args, "daily_include_insider", False),
            daily_include_index=getattr(args, "daily_include_index", False),
            include_company_profile=getattr(args, "include_company_profile", False),
            include_shares_snapshot=getattr(args, "include_shares_snapshot", False),
        )

    log.info("API calls: %s", call_counter.get("count", 0))


if __name__ == "__main__":
    main()
