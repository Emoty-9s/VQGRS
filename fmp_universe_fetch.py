# -*- coding: utf-8 -*-
r"""
FMP Premium: universe_list.csv 기반 최근 5년(분기 20개) 재무 + 시장/기술 지표 수집.

- 입력: universe_list.csv (symbol/ticker 컬럼)
- 출력: fundamentals_quarterly.parquet/csv (fiscal_quarter 기준 anchor부터 연속 20회계분기), market_snapshot.parquet/csv
- 결측은 0으로 치환하지 않음 (NaN 유지).
- API Key: 환경변수 FMP_API_KEY (코드/로그 노출 금지).
- 회계분기: FMP fiscalYear(우선)+period(Q1~Q4) → fiscal_quarter. 종목별 최신 분기(anchor)부터 과거 20개만 저장.

실행 예:
  python fmp_universe_fetch.py --universe .\data\universe_list.csv --outdir .\data --period quarter --limit 20 --max-workers 10
  python fmp_universe_fetch.py --universe universe_list.csv --outdir .\data --only-symbol AAPL --period quarter --limit 20 --max-workers 1 --skip-tech

Corporate actions만 수집 (유니버스 티커, 2020-01-01~오늘 earnings/dividends/splits):
  python fmp_universe_fetch.py --universe universe_list.csv --outdir .\data --actions-only
  python fmp_universe_fetch.py --universe universe_list.csv --outdir .\data --fetch-actions --actions-from 2020-01-01 --actions-to 2025-12-31
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# -----------------------------------------------------------------------------
# 상수 (엔드포인트 경로 변경 시 여기만 수정)
# -----------------------------------------------------------------------------
BASE_URL = "https://financialmodelingprep.com"
API_KEY_ENV = "FMP_API_KEY"
# Premium 750 calls/min → 여유 있게 600/min
CALLS_PER_MIN = 600
MIN_INTERVAL = 60.0 / CALLS_PER_MIN  # 초당 간격

# Stable 재무/시장
PATH_INCOME = "/stable/income-statement"
PATH_BALANCE = "/stable/balance-sheet-statement"
PATH_CASHFLOW = "/stable/cash-flow-statement"  # FMP 문서: cash-flow-statement (하이픈)
PATH_QUOTE = "/stable/quote"
PATH_PROFILE = "/stable/profile"  # stable 사용 (v3 대신)

# 기술 지표: legacy (stable 경로로 바꾸려면 여기만 수정)
PATH_TECH_DAILY_LEGACY = "/api/v3/technical_indicator/daily"

# Stable EOD / Ownership / Analyst / Earnings / Calendars / Macro
PATH_EOD = "/stable/historical-price-eod/full"
PATH_SHARES_FLOAT = "/stable/shares-float"
PATH_POSITIONS_SUMMARY = "/stable/positions-summary"
PATH_ANALYST_ESTIMATES = "/stable/analyst-estimates"
PATH_PRICE_TARGET_SUMMARY = "/stable/price-target-summary"
PATH_PRICE_TARGET_CONSENSUS = "/stable/price-target-consensus"
PATH_GRADES_SUMMARY = "/stable/grades-summary"
PATH_EARNINGS_COMPANY = "/stable/earnings-company"
PATH_EARNINGS_CALENDAR = "/stable/earnings-calendar"
PATH_DIVIDENDS_COMPANY = "/stable/dividends"
PATH_DIVIDENDS_CALENDAR = "/stable/dividends-calendar"
PATH_SPLITS_COMPANY = "/stable/splits"
PATH_IPOS_CALENDAR = "/stable/ipos-calendar"
PATH_SPLITS_CALENDAR = "/stable/splits-calendar"
PATH_TREASURY_RATES = "/stable/treasury-rates"
PATH_ECONOMIC_INDICATORS = "/stable/economic-indicators"
PATH_ECONOMIC_CALENDAR = "/stable/economic-calendar"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# -----------------------------------------------------------------------------
# 필드 매핑 테이블 (FMP 키 후보 → 우리 출력 컬럼). 키 없으면 NaN.
# -----------------------------------------------------------------------------
INCOME_FIELDS: Dict[str, List[str]] = {
    "revenue": ["revenue", "Revenue"],
    "operatingIncome": ["operatingIncome", "OperatingIncome"],
    "netIncome": ["netIncome", "NetIncome"],
    "interestExpense": ["interestExpense", "InterestExpense", "interestExpenseNet"],
}
BS_FIELDS: Dict[str, List[str]] = {
    "totalAssets": ["totalAssets", "TotalAssets"],
    "totalLiabilities": ["totalLiabilities", "TotalLiabilities"],
    "totalStockholdersEquity": ["totalStockholdersEquity", "totalEquity", "stockholdersEquity"],
    "totalCurrentAssets": ["totalCurrentAssets", "currentAssets"],
    "totalCurrentLiabilities": ["totalCurrentLiabilities", "currentLiabilities"],
    "investedCapital": ["investedCapital"],
    "sharesOutstanding": [
        "commonStockSharesOutstanding",
        "sharesOutstanding",
        "weightedAverageShsOut",
        "weightedAverageShsOutDil",
    ],
}
CF_FIELDS: Dict[str, List[str]] = {
    "operatingCashFlow": ["operatingCashFlow", "netCashProvidedByOperatingActivities"],
    "capitalExpenditure": ["capitalExpenditure", "capex"],
}

QUARTERLY_COLUMNS = [
    "ticker_fixed", "ticker_raw", "name_raw", "match_type",
    "fiscal_year", "fiscal_period", "fiscal_quarter", "end_date",
    "revenue", "operatingIncome", "netIncome", "interestExpense",
    "totalAssets", "totalLiabilities", "totalStockholdersEquity",
    "totalCurrentAssets", "totalCurrentLiabilities", "investedCapital", "sharesOutstanding",
    "operatingCashFlow", "capitalExpenditure",
]
SNAPSHOT_COLUMNS = [
    "ticker_fixed", "ticker_raw", "name_raw", "asof",
    "price", "marketCap", "beta", "sector", "industry",
    "volAvg20",
    "rsi14", "sma20", "sma50", "sma200",
]
PRICE_EOD_COLUMNS = ["ticker_fixed", "date", "open", "high", "low", "close", "volume"]
OWNERSHIP_COLUMNS = ["ticker_fixed", "asof", "floatShares", "positionsSummaryCount"]
ANALYST_ESTIMATES_COLUMNS = ["ticker_fixed", "asof", "epsNextQ", "epsNextY", "targetPrice", "recomScore"]
EARNINGS_COLUMNS = [
    "ticker_fixed", "date", "epsEstimated", "epsActual", "epsSurprise", "epsSurprisePct",
    "revenue", "revenueEstimated", "time",
]
CALENDARS_COLUMNS = ["calendar_type", "date", "symbol"]
MACRO_COLUMNS = ["series_type", "name", "date", "value"]

# economic-indicators 기본 name 리스트 (--econ-names 미지정 시)
DEFAULT_ECON_NAMES = ["CPI", "GDP", "unemploymentRate"]


def make_fiscal_quarter_grid(anchor_fq: str, n: int = 20) -> List[str]:
    """anchor_fq(예: 2025Q3)부터 과거로 연속 n개 회계분기 리스트. [2025Q3, 2025Q2, ...]."""
    if not anchor_fq or len(anchor_fq) < 6:
        return []
    try:
        y = int(anchor_fq[:4])
        q_str = anchor_fq[4:].upper()
        if not q_str.startswith("Q") or len(q_str) < 2:
            return []
        q = int(q_str[1])
        if q < 1 or q > 4:
            return []
    except (ValueError, IndexError):
        return []
    out: List[str] = []
    for _ in range(n):
        out.append(f"{y}Q{q}")
        q -= 1
        if q < 1:
            q = 4
            y -= 1
    return out


def make_fiscal_quarter_range(anchor_fq: str, stop_fq: str, max_iter: int = 36) -> List[str]:
    """anchor_fq부터 과거로 연속 fiscal_quarter 리스트 생성. stop_fq 포함되면 종료. 무한루프 방지로 max_iter 제한."""
    if not anchor_fq or not stop_fq:
        return []
    try:
        y = int(anchor_fq[:4])
        q_str = anchor_fq[4:].upper()
        if not q_str.startswith("Q") or len(q_str) < 2:
            return []
        q = int(q_str[1])
        if q < 1 or q > 4:
            return []
    except (ValueError, IndexError):
        return []
    out: List[str] = []
    for _ in range(max_iter):
        fq = f"{y}Q{q}"
        out.append(fq)
        if fq == stop_fq:
            break
        q -= 1
        if q < 1:
            q = 4
            y -= 1
    return out


def pick_value(d: Dict[str, Any], candidates: List[str]) -> Optional[float]:
    for k in candidates:
        if k in d and d[k] is not None:
            try:
                return float(d[k])
            except (TypeError, ValueError):
                pass
    return None


def pick_str(d: Dict[str, Any], candidates: List[str]) -> Optional[str]:
    for k in candidates:
        v = d.get(k)
        if v is not None and str(v).strip() != "":
            return str(v)
    return None


class RateLimiter:
    def __init__(self, min_interval: float):
        self.min_interval = min_interval
        self._last = 0.0
        self._lock = threading.Lock()

    def wait(self) -> None:
        with self._lock:
            now = time.time()
            elapsed = now - self._last
            if elapsed < self.min_interval:
                time.sleep(self.min_interval - elapsed)
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


def _safe_params_for_log(params: Dict[str, Any]) -> Dict[str, Any]:
    """API 키 제거 후 로그/예외용."""
    return {k: ("***" if k.lower() == "apikey" else v) for k, v in params.items()}


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
    r = session.get(url, params=p, timeout=30)
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
        raise RuntimeError(
            f"HTTP {r.status_code}: {path} params={_safe_params_for_log(p)} body={r.text[:200]}"
        )
    try:
        data = r.json()
    except Exception:
        data = None
    if isinstance(data, dict) and data.get("Error Message"):
        raise RuntimeError(f"FMP Error Message: {data.get('Error Message')} path={path}")
    return data


def fetch_income(
    session: requests.Session,
    rl: RateLimiter,
    api_key: str,
    symbol: str,
    period: str,
    limit: int,
    call_counter: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    data = fmp_get(
        session, rl, PATH_INCOME,
        {"symbol": symbol, "period": period, "limit": limit},
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
    period: str,
    limit: int,
    call_counter: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    data = fmp_get(
        session, rl, PATH_BALANCE,
        {"symbol": symbol, "period": period, "limit": limit},
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
    period: str,
    limit: int,
    call_counter: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    data = fmp_get(
        session, rl, PATH_CASHFLOW,
        {"symbol": symbol, "period": period, "limit": limit},
        api_key,
        call_counter=call_counter,
        allow_404_empty=True,
    )
    return data if isinstance(data, list) else []


def fetch_quote(
    session: requests.Session,
    rl: RateLimiter,
    api_key: str,
    symbol: str,
    call_counter: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    data = fmp_get(
        session, rl, PATH_QUOTE, {"symbol": symbol}, api_key,
        call_counter=call_counter, allow_404_empty=True,
    )
    return data if isinstance(data, list) else []


def fetch_profile(
    session: requests.Session,
    rl: RateLimiter,
    api_key: str,
    symbol: str,
    call_counter: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    data = fmp_get(
        session, rl, PATH_PROFILE, {"symbol": symbol}, api_key,
        call_counter=call_counter, allow_404_empty=True,
    )
    return data if isinstance(data, list) else []


def fetch_technical_daily(
    session: requests.Session,
    rl: RateLimiter,
    api_key: str,
    symbol: str,
    indicator_type: str,
    period: int,
    call_counter: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """Legacy: /api/v3/technical_indicator/daily/{symbol}?type=rsi&period=14 (apikey는 fmp_get에서 추가)"""
    path = f"{PATH_TECH_DAILY_LEGACY}/{symbol}"
    data = fmp_get(
        session, rl, path,
        {"type": indicator_type, "period": period},
        api_key,
        call_counter=call_counter,
        allow_404_empty=True,
    )
    return data if isinstance(data, list) else []


def fetch_eod(
    session: requests.Session,
    rl: RateLimiter,
    api_key: str,
    symbol: str,
    from_date: str,
    to_date: str,
    call_counter: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """EOD OHLCV. /stable/historical-price-eod/full?from=...&to=... (경로/쿼리는 상수만 수정)."""
    data = fmp_get(
        session, rl, PATH_EOD,
        {"symbol": symbol, "from": from_date, "to": to_date},
        api_key,
        call_counter=call_counter,
        allow_404_empty=True,
    )
    return data if isinstance(data, list) else []


def fetch_shares_float(
    session: requests.Session,
    rl: RateLimiter,
    api_key: str,
    symbol: str,
    call_counter: Optional[Dict[str, Any]] = None,
) -> Any:
    data = fmp_get(
        session, rl, PATH_SHARES_FLOAT, {"symbol": symbol}, api_key,
        call_counter=call_counter, allow_404_empty=True,
    )
    return data


def fetch_positions_summary(
    session: requests.Session,
    rl: RateLimiter,
    api_key: str,
    symbol: str,
    call_counter: Optional[Dict[str, Any]] = None,
) -> Any:
    data = fmp_get(
        session, rl, PATH_POSITIONS_SUMMARY, {"symbol": symbol}, api_key,
        call_counter=call_counter, allow_404_empty=True,
    )
    return data


def fetch_analyst_estimates(
    session: requests.Session,
    rl: RateLimiter,
    api_key: str,
    symbol: str,
    call_counter: Optional[Dict[str, Any]] = None,
) -> Any:
    data = fmp_get(
        session, rl, PATH_ANALYST_ESTIMATES, {"symbol": symbol}, api_key,
        call_counter=call_counter, allow_404_empty=True,
    )
    return data


def fetch_price_target_summary(
    session: requests.Session,
    rl: RateLimiter,
    api_key: str,
    symbol: str,
    call_counter: Optional[Dict[str, Any]] = None,
) -> Any:
    data = fmp_get(
        session, rl, PATH_PRICE_TARGET_SUMMARY, {"symbol": symbol}, api_key,
        call_counter=call_counter, allow_404_empty=True,
    )
    return data


def fetch_price_target_consensus(
    session: requests.Session,
    rl: RateLimiter,
    api_key: str,
    symbol: str,
    call_counter: Optional[Dict[str, Any]] = None,
) -> Any:
    data = fmp_get(
        session, rl, PATH_PRICE_TARGET_CONSENSUS, {"symbol": symbol}, api_key,
        call_counter=call_counter, allow_404_empty=True,
    )
    return data


def fetch_grades_summary(
    session: requests.Session,
    rl: RateLimiter,
    api_key: str,
    symbol: str,
    call_counter: Optional[Dict[str, Any]] = None,
) -> Any:
    data = fmp_get(
        session, rl, PATH_GRADES_SUMMARY, {"symbol": symbol}, api_key,
        call_counter=call_counter, allow_404_empty=True,
    )
    return data


def fetch_earnings_company(
    session: requests.Session,
    rl: RateLimiter,
    api_key: str,
    symbol: str,
    limit: int,
    call_counter: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    data = fmp_get(
        session, rl, PATH_EARNINGS_COMPANY,
        {"symbol": symbol, "limit": limit},
        api_key,
        call_counter=call_counter,
        allow_404_empty=True,
    )
    return data if isinstance(data, list) else []


def fetch_dividends_company(
    session: requests.Session,
    rl: RateLimiter,
    api_key: str,
    symbol: str,
    call_counter: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """회사별 배당. /stable/dividends?symbol=..."""
    data = fmp_get(
        session, rl, PATH_DIVIDENDS_COMPANY,
        {"symbol": symbol},
        api_key,
        call_counter=call_counter,
        allow_404_empty=True,
    )
    return data if isinstance(data, list) else []


def fetch_splits_company(
    session: requests.Session,
    rl: RateLimiter,
    api_key: str,
    symbol: str,
    call_counter: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """회사별 주식 분할. /stable/splits?symbol=..."""
    data = fmp_get(
        session, rl, PATH_SPLITS_COMPANY,
        {"symbol": symbol},
        api_key,
        call_counter=call_counter,
        allow_404_empty=True,
    )
    return data if isinstance(data, list) else []


def fetch_calendars_once(
    session: requests.Session,
    rl: RateLimiter,
    api_key: str,
    from_date: str,
    to_date: str,
    call_counter: Optional[Dict[str, Any]] = None,
) -> Dict[str, List[Dict[str, Any]]]:
    """시장 전체 캘린더 1회 수집. earnings, dividends, ipos, splits."""
    out: Dict[str, List[Dict[str, Any]]] = {}
    for name, path in [
        ("earnings", PATH_EARNINGS_CALENDAR),
        ("dividends", PATH_DIVIDENDS_CALENDAR),
        ("ipos", PATH_IPOS_CALENDAR),
        ("splits", PATH_SPLITS_CALENDAR),
    ]:
        try:
            data = fmp_get(
                session, rl, path,
                {"from": from_date, "to": to_date},
                api_key,
                call_counter=call_counter,
                allow_404_empty=True,
            )
            out[name] = data if isinstance(data, list) else []
        except Exception as e:
            log.debug("calendars %s: %s", name, e)
            out[name] = []
    return out


def fetch_macro_once(
    session: requests.Session,
    rl: RateLimiter,
    api_key: str,
    from_date: str,
    to_date: str,
    economic_names: Optional[List[str]] = None,
    call_counter: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """시장 전체 매크로 1회 수집. treasury-rates, economic-indicators, economic-calendar."""
    out: Dict[str, Any] = {"treasury": [], "economic_indicators": [], "economic_calendar": []}
    try:
        data = fmp_get(
            session, rl, PATH_TREASURY_RATES,
            {"from": from_date, "to": to_date},
            api_key,
            call_counter=call_counter,
            allow_404_empty=True,
        )
        out["treasury"] = data if isinstance(data, list) else []
    except Exception as e:
        log.debug("treasury-rates: %s", e)
    for nm in (economic_names or DEFAULT_ECON_NAMES)[:15]:
        try:
            data = fmp_get(
                session, rl, PATH_ECONOMIC_INDICATORS,
                {"name": nm, "from": from_date, "to": to_date},
                api_key,
                call_counter=call_counter,
                allow_404_empty=True,
            )
            if isinstance(data, list) and data:
                for r in data:
                    r["_indicator_name"] = nm
                out["economic_indicators"].extend(data)
        except Exception as e:
            log.debug("economic %s: %s", nm, e)
    try:
        data = fmp_get(
            session, rl, PATH_ECONOMIC_CALENDAR,
            {"from": from_date, "to": to_date},
            api_key,
            call_counter=call_counter,
            allow_404_empty=True,
        )
        out["economic_calendar"] = data if isinstance(data, list) else []
    except Exception as e:
        log.debug("economic-calendar: %s", e)
    return out


def _rows_to_df(
    ticker_fixed: str,
    ticker_raw: str,
    name_raw: str,
    match_type: str,
    rows: List[Dict[str, Any]],
    fields_map: Dict[str, List[str]],
) -> pd.DataFrame:
    """FMP row → fiscal_year(fiscalYear 우선, 없으면 calendarYear), fiscal_period(Q1~Q4), fiscal_quarter, end_date. (ticker_fixed, fiscal_quarter) 기준 end_date 최신 1건만 유지."""
    required_cols = [
        "ticker_fixed", "ticker_raw", "name_raw", "match_type",
        "fiscal_year", "fiscal_period", "fiscal_quarter", "end_date",
    ] + list(fields_map.keys())
    out: List[Dict[str, Any]] = []
    for r in rows or []:
        end_date = str(r.get("date") or "")[:10]
        if not end_date or end_date == "None":
            continue
        fy_raw = r.get("fiscalYear") if r.get("fiscalYear") is not None else r.get("calendarYear")
        fy_num = pd.to_numeric(str(fy_raw).strip(), errors="coerce") if fy_raw is not None else pd.NA
        if pd.isna(fy_num):
            continue
        fiscal_year = int(fy_num)
        per = str(r.get("period") or "").strip().upper()
        if per not in ("Q1", "Q2", "Q3", "Q4"):
            continue
        fiscal_quarter = f"{fiscal_year}{per}"
        row: Dict[str, Any] = {
            "ticker_fixed": ticker_fixed,
            "ticker_raw": ticker_raw,
            "name_raw": name_raw,
            "match_type": match_type,
            "fiscal_year": fiscal_year,
            "fiscal_period": per,
            "fiscal_quarter": fiscal_quarter,
            "end_date": end_date,
        }
        for out_key, candidates in fields_map.items():
            row[out_key] = pick_value(r, candidates)
        out.append(row)
    if not out:
        return pd.DataFrame(columns=required_cols)
    df = pd.DataFrame(out)
    df = df.reindex(columns=required_cols)
    df = df.sort_values("end_date", ascending=False)
    df = df.drop_duplicates(subset=["ticker_fixed", "fiscal_quarter"], keep="first")
    return df


def merge_quarterly(
    row_meta: Dict[str, Any],
    income: List[Dict[str, Any]],
    bs: List[Dict[str, Any]],
    cf: List[Dict[str, Any]],
    years_back: int,
) -> pd.DataFrame:
    """boundary 기반: anchor(최신 end_date)의 연도 - years_back 을 1월 1일로 boundary. end_date>=boundary 전부 + end_date<boundary 1개 포함, 연속 fiscal_quarter 그리드."""
    tf = str(row_meta.get("ticker_fixed") or "").strip().upper()
    tr = str(row_meta.get("ticker_raw") or "").strip()
    nr = str(row_meta.get("name_raw") or "").strip()
    mt = str(row_meta.get("match_type") or "").strip()
    df_i = _rows_to_df(tf, tr, nr, mt, income, INCOME_FIELDS)
    df_b = _rows_to_df(tf, tr, nr, mt, bs, BS_FIELDS)
    df_c = _rows_to_df(tf, tr, nr, mt, cf, CF_FIELDS)
    if df_i.empty and df_b.empty and df_c.empty:
        return pd.DataFrame(columns=QUARTERLY_COLUMNS)
    df = df_i.merge(df_b, on=["ticker_fixed", "fiscal_quarter"], how="outer", suffixes=("", "_b"))
    if "end_date_b" in df.columns:
        df["end_date"] = df["end_date"].fillna(df["end_date_b"])
    df = df.drop(columns=[c for c in df.columns if c.endswith("_b")], errors="ignore")
    df = df.merge(df_c, on=["ticker_fixed", "fiscal_quarter"], how="outer", suffixes=("", "_c"))
    if "end_date_c" in df.columns:
        df["end_date"] = df["end_date"].fillna(df["end_date_c"])
    df = df.drop(columns=[c for c in df.columns if c.endswith("_c")], errors="ignore")
    if "fiscal_quarter" not in df.columns:
        df["fiscal_quarter"] = pd.NA
    if "end_date" not in df.columns:
        df["end_date"] = pd.NA
    df["_end_dt"] = pd.to_datetime(df["end_date"], errors="coerce")
    valid = df.dropna(subset=["_end_dt", "fiscal_quarter"])
    if valid.empty:
        return pd.DataFrame(columns=QUARTERLY_COLUMNS)
    anchor_row = valid.loc[valid["_end_dt"].idxmax()]
    anchor_dt = anchor_row["_end_dt"]
    anchor_fq = str(anchor_row["fiscal_quarter"])
    boundary_dt = datetime(anchor_dt.year - years_back, 1, 1)
    keep_main = df[df["_end_dt"] >= boundary_dt].copy()
    pre = df[df["_end_dt"] < boundary_dt]
    if pre.empty:
        keep = keep_main
    else:
        pre_one = pre.loc[pre["_end_dt"].idxmax()]
        keep = pd.concat([keep_main, pd.DataFrame([pre_one])], ignore_index=True)
    keep = keep.dropna(subset=["fiscal_quarter"])
    keep = keep.sort_values("_end_dt", ascending=False).drop_duplicates(subset=["ticker_fixed", "fiscal_quarter"], keep="first")
    if keep.empty:
        return pd.DataFrame(columns=QUARTERLY_COLUMNS)
    stop_row = keep.loc[keep["_end_dt"].idxmin()]
    stop_fq = str(stop_row["fiscal_quarter"])
    max_iter = years_back * 4 + 16
    grid = make_fiscal_quarter_range(anchor_fq, stop_fq, max_iter=max_iter)
    if not grid:
        return pd.DataFrame(columns=QUARTERLY_COLUMNS)
    keep_clean = keep.drop(columns=["_end_dt"], errors="ignore")
    base = pd.DataFrame({"ticker_fixed": [tf] * len(grid), "fiscal_quarter": grid})
    merged = base.merge(keep_clean, on=["ticker_fixed", "fiscal_quarter"], how="left")
    merged["fiscal_year"] = pd.to_numeric(merged["fiscal_quarter"].astype(str).str[:4], errors="coerce")
    merged["fiscal_period"] = merged["fiscal_quarter"].astype(str).str[4:]
    merged["ticker_raw"] = tr
    merged["name_raw"] = nr
    merged["match_type"] = mt
    if "end_date" not in merged.columns:
        merged["end_date"] = pd.NA
    merged["_ord"] = merged["fiscal_quarter"].map({s: i for i, s in enumerate(grid)})
    merged = merged.sort_values("_ord").drop(columns=["_ord"])
    return merged


def read_universe(path: str | Path) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Universe CSV 로드. (fetch할 row 리스트, 스킵된 row 리스트). ticker_fixed 있으면 사용, 없으면 symbol/ticker. REJECT/AMBIGUOUS는 스킵."""
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Universe file not found: {path}")
    df = pd.read_csv(path)
    cols_lower = {str(c).lower(): c for c in df.columns}
    ticker_col = None
    for cand in ["ticker_fixed", "symbol", "ticker", "tick", "sym"]:
        if cand in cols_lower:
            ticker_col = cols_lower[cand]
            break
    if ticker_col is None:
        raise ValueError("universe_list.csv에 ticker_fixed/symbol/ticker 컬럼이 없습니다.")
    ticker_raw_col = cols_lower.get("ticker_raw")
    name_raw_col = cols_lower.get("name_raw") or cols_lower.get("name")
    match_type_col = cols_lower.get("match_type")
    weight_col = cols_lower.get("weight")

    rows_to_fetch: List[Dict[str, Any]] = []
    skipped: List[Dict[str, Any]] = []
    for _, r in df.iterrows():
        ticker_fixed = str(r.get(ticker_col, "") or "").strip().upper()
        if not ticker_fixed:
            continue
        match_type = str(r.get(match_type_col, "") or "").strip().upper() if match_type_col else ""
        if match_type in ("REJECT", "AMBIGUOUS"):
            skipped.append({
                "ticker_raw": str(r.get(ticker_raw_col, "") or ticker_fixed).strip() if ticker_raw_col else ticker_fixed,
                "name_raw": str(r.get(name_raw_col, "") or "").strip() if name_raw_col else "",
                "match_type": match_type,
                "reason": match_type,
            })
            continue
        row: Dict[str, Any] = {"ticker_fixed": ticker_fixed}
        if ticker_raw_col:
            row["ticker_raw"] = str(r.get(ticker_raw_col, "") or "").strip()
        else:
            row["ticker_raw"] = ticker_fixed
        if name_raw_col:
            row["name_raw"] = str(r.get(name_raw_col, "") or "").strip()
        else:
            row["name_raw"] = ""
        if match_type_col:
            row["match_type"] = str(r.get(match_type_col, "") or "").strip()
        else:
            row["match_type"] = ""
        if weight_col and weight_col in r.index:
            row["weight"] = r.get(weight_col)
        rows_to_fetch.append(row)
    return (rows_to_fetch, skipped)


def process_symbol(
    row: Dict[str, Any],
    session: requests.Session,
    rl: RateLimiter,
    api_key: str,
    period: str,
    limit: int,
    years_back: int,
    skip_market: bool,
    skip_tech: bool,
    call_counter: Dict[str, Any],
    save_raw_dir: Optional[Path],
    from_date: str = "",
    to_date: str = "",
    fetch_eod_flag: bool = True,
    fetch_ownership_flag: bool = True,
    fetch_analyst_flag: bool = True,
    fetch_earnings_flag: bool = True,
    earnings_limit: int = 60,
    skip_financials: bool = False,
    fetch_actions_flag: bool = False,
    actions_from: str = "",
    actions_to: str = "",
) -> Tuple[Optional[pd.DataFrame], Optional[Dict[str, Any]], Optional[pd.DataFrame], Optional[Dict[str, Any]], Optional[Dict[str, Any]], Optional[pd.DataFrame], Optional[pd.DataFrame], Optional[pd.DataFrame], Optional[pd.DataFrame], Optional[Dict[str, Any]]]:
    """한 종목 재무 + 시장/기술 + EOD/소유/애널/실적 수집. (qdf, market_row, eod_df, ownership_row, analyst_row, earnings_df, earnings_actions_df, dividends_df, splits_df, raw_dict)."""
    sym = str(row.get("ticker_fixed") or "").strip().upper()
    if not sym:
        raise RuntimeError("row has no ticker_fixed")
    fetch_limit = limit + 8
    raw: Dict[str, Any] = {} if save_raw_dir else {}
    qdf: Optional[pd.DataFrame] = None
    if not skip_financials:
        income: List[Dict[str, Any]] = []
        bs: List[Dict[str, Any]] = []
        cf: List[Dict[str, Any]] = []
        try:
            income = fetch_income(session, rl, api_key, sym, period, fetch_limit, call_counter)
        except Exception as e:
            log.debug("%s income: %s", sym, e)
        try:
            bs = fetch_balance(session, rl, api_key, sym, period, fetch_limit, call_counter)
        except Exception as e:
            log.debug("%s balance: %s", sym, e)
        try:
            cf = fetch_cashflow(session, rl, api_key, sym, period, fetch_limit, call_counter)
        except Exception as e:
            log.debug("%s cashflow: %s", sym, e)
        if not income and not bs:
            raise RuntimeError("no_financials: income and balance both empty")
        if save_raw_dir:
            raw["income"] = income
            raw["balance"] = bs
            raw["cashflow"] = cf
        qdf = merge_quarterly(row, income, bs, cf, years_back)

    eod_df: Optional[pd.DataFrame] = None
    if not skip_market and fetch_eod_flag and from_date and to_date:
        try:
            eod_raw = fetch_eod(session, rl, api_key, sym, from_date, to_date, call_counter)
            if save_raw_dir:
                raw["eod"] = eod_raw
            if eod_raw:
                rows_eod = []
                for r in eod_raw:
                    d = str(r.get("date") or "")[:10]
                    if not d:
                        continue
                    rows_eod.append({
                        "ticker_fixed": sym,
                        "date": d,
                        "open": pick_value(r, ["open", "Open"]),
                        "high": pick_value(r, ["high", "High"]),
                        "low": pick_value(r, ["low", "Low"]),
                        "close": pick_value(r, ["close", "Close"]),
                        "volume": pick_value(r, ["volume", "Volume"]),
                    })
                if rows_eod:
                    eod_df = pd.DataFrame(rows_eod)
                    eod_df = eod_df.drop_duplicates(subset=["ticker_fixed", "date"], keep="first")
        except Exception as e:
            log.debug("%s eod: %s", sym, e)

    ownership_row: Optional[Dict[str, Any]] = None
    if fetch_ownership_flag:
        try:
            sf = fetch_shares_float(session, rl, api_key, sym, call_counter)
            if save_raw_dir:
                raw["shares_float"] = sf
            if sf and isinstance(sf, list) and sf:
                r0 = sf[0]
                ownership_row = {
                    "ticker_fixed": sym,
                    "asof": str(r0.get("date") or r0.get("asof") or "")[:10] or time.strftime("%Y-%m-%d"),
                    "floatShares": pick_value(r0, ["floatShares", "sharesFloat", "float", "float_shares"]),
                    "positionsSummaryCount": pick_value(r0, ["positionsSummaryCount", "count"]),
                }
            ps = fetch_positions_summary(session, rl, api_key, sym, call_counter)
            if save_raw_dir:
                raw["positions_summary"] = ps
            if ownership_row is None and ps and isinstance(ps, list) and ps:
                ownership_row = {
                    "ticker_fixed": sym,
                    "asof": time.strftime("%Y-%m-%d"),
                    "floatShares": None,
                    "positionsSummaryCount": float(len(ps)) if ps else None,
                }
        except Exception as e:
            log.debug("%s ownership: %s", sym, e)

    analyst_row: Optional[Dict[str, Any]] = None
    if fetch_analyst_flag:
        try:
            ae = fetch_analyst_estimates(session, rl, api_key, sym, call_counter)
            if save_raw_dir:
                raw["analyst_estimates"] = ae
            pt_cons = fetch_price_target_consensus(session, rl, api_key, sym, call_counter)
            gr = fetch_grades_summary(session, rl, api_key, sym, call_counter)
            if save_raw_dir:
                raw["price_target_consensus"] = pt_cons
                raw["grades_summary"] = gr
            d = (ae[0] if isinstance(ae, list) and ae else {}) or {}
            eps_next_q = pick_value(d, ["epsNextQ", "epsNextQuarter", "eps", "estimatedEpsNextQuarter", "consensusEpsNextQuarter", "nextQuarterEps"])
            eps_next_y = pick_value(d, ["epsNextY", "epsNextYear", "epsNextAnnual", "estimatedEpsNextYear", "consensusEpsNextYear", "nextYearEps"])
            pt_d = (pt_cons[0] if isinstance(pt_cons, list) and pt_cons else pt_cons) if isinstance(pt_cons, (list, dict)) else {}
            target_price = pick_value(pt_d, ["targetConsensus", "consensus", "priceTarget", "targetPrice", "median"]) if isinstance(pt_d, dict) else None
            gr_d = (gr[0] if isinstance(gr, list) and gr else gr) if isinstance(gr, (list, dict)) else {}
            recom_score = pick_value(gr_d, ["score"]) or pick_str(gr_d, ["consensus", "rating", "recommendation"]) if isinstance(gr_d, dict) else None
            analyst_row = {
                "ticker_fixed": sym,
                "asof": time.strftime("%Y-%m-%d %H:%M:%S"),
                "epsNextQ": eps_next_q,
                "epsNextY": eps_next_y,
                "targetPrice": target_price,
                "recomScore": recom_score,
            }
        except Exception as e:
            log.debug("%s analyst: %s", sym, e)

    earnings_df: Optional[pd.DataFrame] = None
    earnings_actions_df: Optional[pd.DataFrame] = None
    dividends_df: Optional[pd.DataFrame] = None
    splits_df: Optional[pd.DataFrame] = None
    _need_ec = fetch_earnings_flag or fetch_actions_flag
    if _need_ec:
        try:
            ec = fetch_earnings_company(session, rl, api_key, sym, earnings_limit, call_counter)
            if save_raw_dir:
                raw["earnings_company"] = ec
            if ec:
                if fetch_earnings_flag:
                    rows_earn = []
                    for r in ec:
                        d = str(r.get("date") or r.get("fiscalDateEnding") or "")[:10]
                        eps_est = pick_value(r, ["epsEstimated", "estimatedEps", "epsEstimate", "estimate"])
                        eps_act = pick_value(r, ["epsActual", "eps", "actualEps", "reportedEps"])
                        surprise = None
                        surprise_pct = None
                        if eps_est is not None and eps_act is not None:
                            surprise = eps_act - eps_est
                            surprise_pct = (surprise / abs(eps_est) * 100) if eps_est != 0 else None
                        rows_earn.append({
                            "ticker_fixed": sym,
                            "date": d or "",
                            "epsEstimated": eps_est,
                            "epsActual": eps_act,
                            "epsSurprise": surprise,
                            "epsSurprisePct": surprise_pct,
                            "revenue": pick_value(r, ["revenue", "Revenue"]),
                            "revenueEstimated": pick_value(r, ["revenueEstimated", "estimatedRevenue"]),
                            "time": pick_str(r, ["time", "period"]),
                        })
                    if rows_earn:
                        earnings_df = pd.DataFrame(rows_earn)
                if fetch_actions_flag and actions_from and actions_to:
                    rows_act = []
                    for r in ec:
                        d = str(r.get("date") or r.get("fiscalDateEnding") or "")[:10]
                        if not d or d < actions_from or d > actions_to:
                            continue
                        rows_act.append({
                            "ticker_fixed": sym,
                            "date": d,
                            "epsEstimated": pick_value(r, ["epsEstimated", "estimatedEps", "epsEstimate", "estimate"]),
                            "epsActual": pick_value(r, ["epsActual", "eps", "actualEps", "reportedEps"]),
                            "revenueEstimated": pick_value(r, ["revenueEstimated", "estimatedRevenue"]),
                            "revenueActual": pick_value(r, ["revenue", "Revenue"]),
                            "lastUpdated": r.get("updated") or r.get("lastUpdated") or pd.NA,
                        })
                    if rows_act:
                        earnings_actions_df = pd.DataFrame(rows_act)
        except Exception as e:
            log.debug("%s earnings: %s", sym, e)

    if fetch_actions_flag and actions_from and actions_to:
        try:
            div_raw = fetch_dividends_company(session, rl, api_key, sym, call_counter)
            if save_raw_dir and div_raw:
                raw["dividends_company"] = div_raw
            if div_raw:
                rows_div = []
                for r in div_raw:
                    d = str(r.get("date") or r.get("exDividendDate") or r.get("declarationDate") or "")[:10]
                    if not d or d < actions_from or d > actions_to:
                        continue
                    rows_div.append({
                        "ticker_fixed": sym,
                        "date": d,
                        "dividend": r.get("dividend") if "dividend" in r else pick_value(r, ["dividend"]),
                        "adjDividend": r.get("adjDividend") if "adjDividend" in r else pick_value(r, ["adjDividend"]),
                        "yield": r.get("yield") if "yield" in r else pick_value(r, ["dividendYield", "yield"]),
                        "frequency": pick_str(r, ["frequency"]),
                        "declarationDate": str(r.get("declarationDate") or "")[:10] if r.get("declarationDate") else pd.NA,
                        "recordDate": str(r.get("recordDate") or "")[:10] if r.get("recordDate") else pd.NA,
                        "paymentDate": str(r.get("paymentDate") or "")[:10] if r.get("paymentDate") else pd.NA,
                    })
                if rows_div:
                    dividends_df = pd.DataFrame(rows_div)
        except Exception as e:
            log.debug("%s dividends: %s", sym, e)
        try:
            spl_raw = fetch_splits_company(session, rl, api_key, sym, call_counter)
            if save_raw_dir and spl_raw:
                raw["splits_company"] = spl_raw
            if spl_raw:
                rows_spl = []
                for r in spl_raw:
                    d = str(r.get("date") or "")[:10]
                    if not d or d < actions_from or d > actions_to:
                        continue
                    rows_spl.append({
                        "ticker_fixed": sym,
                        "date": d,
                        "numerator": pick_value(r, ["numerator", "newToOld"]),
                        "denominator": pick_value(r, ["denominator", "oldToNew"]),
                        "splitType": pick_str(r, ["splitType", "type"]),
                    })
                if rows_spl:
                    splits_df = pd.DataFrame(rows_spl)
        except Exception as e:
            log.debug("%s splits: %s", sym, e)

    market_row: Optional[Dict[str, Any]] = None
    if not skip_market:
        quote: List[Dict[str, Any]] = []
        prof: List[Dict[str, Any]] = []
        try:
            quote = fetch_quote(session, rl, api_key, sym, call_counter)
        except Exception as e:
            log.debug("%s quote: %s", sym, e)
        try:
            prof = fetch_profile(session, rl, api_key, sym, call_counter)
        except Exception as e:
            log.debug("%s profile: %s", sym, e)
        if save_raw_dir:
            raw["quote"] = quote
            raw["profile"] = prof
        q0 = (quote[0] if isinstance(quote, list) and quote else {}) or {}
        p0 = (prof[0] if isinstance(prof, list) and prof else {}) or {}
        price = pick_value(q0, ["price"])
        mcap = pick_value(q0, ["marketCap", "marketcap"])
        beta = pick_value(p0, ["beta"]) or pick_value(q0, ["beta"])
        sector = pick_str(p0, ["sector"])
        industry = pick_str(p0, ["industry"])
        vol_avg_20 = None
        if eod_df is not None and not eod_df.empty and "volume" in eod_df.columns and "date" in eod_df.columns:
            ed = eod_df.sort_values("date", ascending=False)
            vol = ed["volume"].dropna()
            if len(vol) >= 20:
                vol_avg_20 = float(vol.iloc[:20].mean())
            elif len(vol) > 0:
                vol_avg_20 = float(vol.mean())
        rsi14 = sma20 = sma50 = sma200 = None
        if not skip_tech:
            try:
                rsi = fetch_technical_daily(session, rl, api_key, sym, "rsi", 14, call_counter)
                if isinstance(rsi, list) and rsi:
                    rsi14 = pick_value(rsi[0], ["rsi"])
                if save_raw_dir:
                    raw["rsi"] = rsi
            except Exception:
                pass
            for period_val, key in [(20, "sma20"), (50, "sma50"), (200, "sma200")]:
                try:
                    sma = fetch_technical_daily(session, rl, api_key, sym, "sma", period_val, call_counter)
                    if isinstance(sma, list) and sma:
                        val = pick_value(sma[0], ["sma"])
                        if key == "sma20":
                            sma20 = val
                        elif key == "sma50":
                            sma50 = val
                        else:
                            sma200 = val
                    if save_raw_dir:
                        raw[key] = sma
                except Exception:
                    pass
        market_row = {
            "ticker_fixed": sym,
            "ticker_raw": row.get("ticker_raw", sym),
            "name_raw": row.get("name_raw", ""),
            "asof": time.strftime("%Y-%m-%d %H:%M:%S"),
            "price": price,
            "marketCap": mcap,
            "beta": beta,
            "sector": sector,
            "industry": industry,
            "volAvg20": vol_avg_20,
            "rsi14": rsi14,
            "sma20": sma20,
            "sma50": sma50,
            "sma200": sma200,
        }

    if save_raw_dir and raw:
        (save_raw_dir / f"{sym}.raw.json").write_text(
            json.dumps(raw, default=str), encoding="utf-8"
        )
    return (qdf, market_row, eod_df, ownership_row, analyst_row, earnings_df, earnings_actions_df, dividends_df, splits_df, raw if save_raw_dir else None)


def load_cached_symbols(outdir: Path) -> Tuple[Set[str], Set[str]]:
    """기존 parquet에서 ticker_fixed 집합 반환. ticker_fixed 없으면 symbol 컬럼 호환."""
    q_path = outdir / "fundamentals_quarterly.parquet"
    s_path = outdir / "market_snapshot.parquet"
    q_syms: Set[str] = set()
    s_syms: Set[str] = set()
    if q_path.exists():
        try:
            full = pd.read_parquet(q_path)
            col = "ticker_fixed" if "ticker_fixed" in full.columns else "symbol"
            q_syms = set(full[col].astype(str).str.upper())
        except Exception:
            pass
    if s_path.exists():
        try:
            full = pd.read_parquet(s_path)
            col = "ticker_fixed" if "ticker_fixed" in full.columns else "symbol"
            s_syms = set(full[col].astype(str).str.upper())
        except Exception:
            pass
    return (q_syms, s_syms)


def load_cached_data_for_symbols(
    outdir: Path,
    ticker_fixed_set: Set[str],
    skip_market: bool,
) -> Tuple[pd.DataFrame, List[Dict[str, Any]]]:
    """캐시된 parquet에서 지정 ticker_fixed 행만 반환. (quarterly_df, market_rows)"""
    q_path = outdir / "fundamentals_quarterly.parquet"
    s_path = outdir / "market_snapshot.parquet"
    syms_upper = {s.upper() for s in ticker_fixed_set}
    q_df = pd.DataFrame()
    m_rows: List[Dict[str, Any]] = []
    if q_path.exists():
        try:
            full = pd.read_parquet(q_path)
            col = "ticker_fixed" if "ticker_fixed" in full.columns else "symbol"
            q_df = full[full[col].astype(str).str.upper().isin(syms_upper)].copy()
            if "ticker_fixed" not in q_df.columns and "symbol" in q_df.columns:
                q_df["ticker_fixed"] = q_df["symbol"]
                q_df["ticker_raw"] = q_df["symbol"]
                q_df["name_raw"] = ""
                q_df["match_type"] = ""
        except Exception:
            pass
    if not skip_market and s_path.exists():
        try:
            full = pd.read_parquet(s_path)
            col = "ticker_fixed" if "ticker_fixed" in full.columns else "symbol"
            sub = full[full[col].astype(str).str.upper().isin(syms_upper)].copy()
            if "ticker_fixed" not in sub.columns and "symbol" in sub.columns:
                sub["ticker_fixed"] = sub["symbol"]
                sub["ticker_raw"] = sub["symbol"]
                sub["name_raw"] = ""
            m_rows = sub.to_dict("records")
        except Exception:
            pass
    return (q_df, m_rows)


def main() -> None:
    ap = argparse.ArgumentParser(description="FMP Premium universe 재무/시장 수집")
    ap.add_argument("--universe", required=True, help="universe_list.csv 경로")
    ap.add_argument("--outdir", required=True, help="출력 디렉터리")
    ap.add_argument("--period", default="quarter", help="period=quarter")
    ap.add_argument("--limit", type=int, default=20, help="FMP fetch 시 가져올 최소 분기 수 (내부적으로 limit+8 사용)")
    ap.add_argument("--years-back", type=int, default=5, help="anchor 연도 기준 boundary (years_back년 전 1월 1일). end_date>=boundary 전부 + 이전 1개 포함")
    ap.add_argument("--max-workers", type=int, default=1, help="동시 스레드 수 (1=순차, 20~50 권장)")
    ap.add_argument("--only-symbol", default="", help="테스트용: AAPL,MSFT 처럼 일부만")
    ap.add_argument("--skip-market", action="store_true", help="시장/기술/EOD 수집 생략")
    ap.add_argument("--skip-tech", action="store_true", help="RSI/SMA 수집 생략")
    ap.add_argument("--use-cache", action="store_true", help="이미 저장된 symbol은 재호출 스킵 (quarterly는 fiscal_quarter 스키마일 때만 병합)")
    ap.add_argument("--save-raw", action="store_true", help="raw JSON 응답 저장 (outdir/raw/)")
    ap.add_argument("--fetch-eod", action="store_true", default=True, help="EOD OHLCV 수집 (기본 on, --skip-market이면 무시)")
    ap.add_argument("--skip-eod", action="store_false", dest="fetch_eod", help="EOD 수집 생략")
    ap.add_argument("--fetch-ownership", action="store_true", default=True, help="소유/수급 수집 (기본 on)")
    ap.add_argument("--fetch-analyst", action="store_true", default=True, help="애널 추정/가격목표/등급 수집 (기본 on)")
    ap.add_argument("--fetch-earnings", action="store_true", default=True, help="실적 시계열 수집 (기본 on)")
    ap.add_argument("--fetch-calendars", action="store_true", default=False, help="시장 캘린더 1회 수집 (기본 off)")
    ap.add_argument("--fetch-macro", action="store_true", default=False, help="매크로 1회 수집 (기본 off)")
    ap.add_argument("--macro-from", default="", help="매크로 기간 시작 YYYY-MM-DD (미지정 시 years_back 기준)")
    ap.add_argument("--macro-to", default="", help="매크로 기간 종료 YYYY-MM-DD (미지정 시 오늘)")
    ap.add_argument("--econ-names", default="", help="economic-indicators name 리스트 (쉼표 구분, 예: CPI,GDP,unemploymentRate)")
    ap.add_argument("--earnings-limit", type=int, default=60, help="실적 수집 limit")
    ap.add_argument("--fetch-actions", action="store_true", default=False, help="Corporate actions(earnings/dividends/splits) 수집 및 저장")
    ap.add_argument("--actions-from", default="2020-01-01", help="actions 기간 시작 YYYY-MM-DD")
    ap.add_argument("--actions-to", default="", help="actions 기간 종료 YYYY-MM-DD (미지정 시 오늘)")
    ap.add_argument("--actions-only", action="store_true", default=False, help="actions만 수집 (재무/시장/EOD/ownership/analyst 스킵)")
    args = ap.parse_args()

    api_key = os.environ.get(API_KEY_ENV) or ""
    if not api_key.strip():
        raise RuntimeError("환경변수 FMP_API_KEY가 설정되지 않았습니다.")

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    save_raw_dir: Optional[Path] = outdir / "raw" if args.save_raw else None
    if save_raw_dir:
        save_raw_dir.mkdir(parents=True, exist_ok=True)

    today = date.today()
    to_date = today.isoformat()
    from_d = today - timedelta(days=args.years_back * 365)
    from_date = from_d.isoformat()
    actions_to = (getattr(args, "actions_to", "") or "").strip()[:10] or to_date
    actions_from = (getattr(args, "actions_from", "") or "2020-01-01").strip()[:10]
    if getattr(args, "actions_only", False):
        args.skip_market = True
        args.skip_tech = True
        setattr(args, "fetch_eod", False)
        setattr(args, "fetch_ownership", False)
        setattr(args, "fetch_analyst", False)
        setattr(args, "fetch_earnings", True)
        if not getattr(args, "fetch_actions", False):
            setattr(args, "fetch_actions", True)
    macro_from = (args.macro_from.strip()[:10]) if getattr(args, "macro_from", "") else from_date
    macro_to = (args.macro_to.strip()[:10]) if getattr(args, "macro_to", "") else to_date
    econ_names_raw = getattr(args, "econ_names", "") or ""
    econ_names_list: List[str] = [s.strip() for s in econ_names_raw.split(",") if s.strip()] if econ_names_raw else DEFAULT_ECON_NAMES

    rows, skipped_rows = read_universe(args.universe)
    if skipped_rows:
        skip_path = outdir / "universe_skipped.csv"
        pd.DataFrame(skipped_rows).to_csv(skip_path, index=False)
        log.info("universe_skipped.csv: %s (rows=%s)", skip_path, len(skipped_rows))

    if args.only_symbol:
        want = {s.strip().upper() for s in args.only_symbol.split(",") if s.strip()}
        rows = [r for r in rows if (r.get("ticker_fixed") or "").upper() in want]
        if not rows:
            log.warning("--only-symbol에 매칭되는 종목 없음: %s", args.only_symbol)

    cached_q, cached_s = set(), set()
    existing_q_df = pd.DataFrame()
    existing_m_rows: List[Dict[str, Any]] = []
    if args.use_cache:
        cached_q, cached_s = load_cached_symbols(outdir)
        log.info("캐시 사용: 이미 quarterly %s개, snapshot %s개 ticker_fixed 있음", len(cached_q), len(cached_s))

    rl = RateLimiter(MIN_INTERVAL)
    call_counter: Dict[str, Any] = {"count": 0, "lock": threading.Lock()}

    all_quarterly: List[pd.DataFrame] = []
    market_rows: List[Dict[str, Any]] = []
    eod_dfs: List[pd.DataFrame] = []
    ownership_rows: List[Dict[str, Any]] = []
    analyst_rows: List[Dict[str, Any]] = []
    earnings_dfs: List[pd.DataFrame] = []
    earnings_actions_dfs: List[pd.DataFrame] = []
    dividends_actions_dfs: List[pd.DataFrame] = []
    splits_actions_dfs: List[pd.DataFrame] = []
    fetch_failures: List[Dict[str, Any]] = []
    ok = 0
    fail = 0
    quarter_counts: List[int] = []
    start_time = time.time()
    skipped_ticker_fixed: Set[str] = set()

    actions_only = getattr(args, "actions_only", False)
    to_fetch = [
        r for r in rows
        if actions_only
        or not (args.use_cache and (r.get("ticker_fixed") or "").upper() in cached_q and (args.skip_market or (r.get("ticker_fixed") or "").upper() in cached_s))
    ]
    for r in rows:
        tf = (r.get("ticker_fixed") or "").upper()
        if not actions_only and args.use_cache and tf in cached_q and (args.skip_market or tf in cached_s):
            skipped_ticker_fixed.add(tf)

    def fetch_one(row: Dict[str, Any]):
        session = make_session()
        try:
            qdf, mrow, eod_df, ownership_row, analyst_row, earnings_df, earnings_actions_df, dividends_df, splits_df, _ = process_symbol(
                row, session, rl, api_key,
                args.period, args.limit,
                args.years_back,
                args.skip_market, args.skip_tech,
                call_counter, save_raw_dir,
                from_date, to_date,
                fetch_eod_flag=getattr(args, "fetch_eod", True),
                fetch_ownership_flag=getattr(args, "fetch_ownership", True),
                fetch_analyst_flag=getattr(args, "fetch_analyst", True),
                fetch_earnings_flag=getattr(args, "fetch_earnings", True),
                earnings_limit=getattr(args, "earnings_limit", 60),
                skip_financials=actions_only,
                fetch_actions_flag=getattr(args, "fetch_actions", False),
                actions_from=actions_from,
                actions_to=actions_to,
            )
            return (row, qdf, mrow, eod_df, ownership_row, analyst_row, earnings_df, earnings_actions_df, dividends_df, splits_df, None)
        except Exception as e:
            return (row, None, None, None, None, None, None, None, None, None, e)

    def _collect(r: Dict[str, Any], qdf: Any, mrow: Any, eod_df: Any, ownership_row: Any, analyst_row: Any, earnings_df: Any, earnings_actions_df: Any, dividends_df: Any, splits_df: Any, err: Any) -> None:
        nonlocal ok, fail
        tf = (r.get("ticker_fixed") or "").upper()
        if err is not None:
            fail += 1
            fetch_failures.append({
                "ticker_fixed": tf,
                "ticker_raw": r.get("ticker_raw", tf),
                "error": str(err),
                "ts": time.strftime("%Y-%m-%d %H:%M:%S"),
            })
            log.warning("[FAIL] %s err=%s", tf, err)
        else:
            if qdf is not None and not qdf.empty:
                all_quarterly.append(qdf)
                quarter_counts.append(len(qdf))
            if mrow is not None:
                market_rows.append(mrow)
            if eod_df is not None and not eod_df.empty:
                eod_dfs.append(eod_df)
            if ownership_row is not None:
                ownership_rows.append(ownership_row)
            if analyst_row is not None:
                analyst_rows.append(analyst_row)
            if earnings_df is not None and not earnings_df.empty:
                earnings_dfs.append(earnings_df)
            if earnings_actions_df is not None and not earnings_actions_df.empty:
                earnings_actions_dfs.append(earnings_actions_df)
            if dividends_df is not None and not dividends_df.empty:
                dividends_actions_dfs.append(dividends_df)
            if splits_df is not None and not splits_df.empty:
                splits_actions_dfs.append(splits_df)
            ok += 1

    if args.max_workers <= 1:
        for idx, row in enumerate(to_fetch, 1):
            row, qdf, mrow, eod_df, ownership_row, analyst_row, earnings_df, earnings_actions_df, dividends_df, splits_df, err = fetch_one(row)
            _collect(row, qdf, mrow, eod_df, ownership_row, analyst_row, earnings_df, earnings_actions_df, dividends_df, splits_df, err)
            if idx % 50 == 0:
                log.info("[progress] %s/%s ok=%s fail=%s calls=%s", idx, len(to_fetch), ok, fail, call_counter.get("count", 0))
    else:
        completed = 0
        with ThreadPoolExecutor(max_workers=args.max_workers) as executor:
            futures = {executor.submit(fetch_one, r): r for r in to_fetch}
            for fut in as_completed(futures):
                completed += 1
                row, qdf, mrow, eod_df, ownership_row, analyst_row, earnings_df, earnings_actions_df, dividends_df, splits_df, err = fut.result()
                _collect(row, qdf, mrow, eod_df, ownership_row, analyst_row, earnings_df, earnings_actions_df, dividends_df, splits_df, err)
                if completed % 50 == 0:
                    log.info("[progress] %s/%s ok=%s fail=%s calls=%s", completed, len(to_fetch), ok, fail, call_counter.get("count", 0))
    ok += len(skipped_ticker_fixed)

    if fetch_failures:
        fail_path = outdir / "fetch_failures.csv"
        pd.DataFrame(fetch_failures).to_csv(fail_path, index=False)
        log.info("fetch_failures.csv: %s (rows=%s)", fail_path, len(fetch_failures))

    if skipped_ticker_fixed:
        existing_q_df, existing_m_rows = load_cached_data_for_symbols(outdir, skipped_ticker_fixed, args.skip_market)
        if not existing_q_df.empty and "fiscal_quarter" in existing_q_df.columns:
            all_quarterly.append(existing_q_df)
            key_col = "ticker_fixed" if "ticker_fixed" in existing_q_df.columns else "symbol"
            quarter_counts.extend(existing_q_df.groupby(key_col).size().tolist())
        elif not existing_q_df.empty:
            log.warning("캐시된 quarterly는 이전 스키마(end 기준)라 사용하지 않습니다. 해당 종목은 재수집하세요.")
        market_rows = existing_m_rows + market_rows

    elapsed = time.time() - start_time

    # 저장 (결측은 0으로 채우지 않음. NaN 유지). (ticker_fixed, fiscal_quarter) 기준 end_date 최신 1건만 유지. NA인 fiscal_quarter는 dedupe 제외
    if all_quarterly:
        dfq = pd.concat(all_quarterly, ignore_index=True)
        dfq = dfq.sort_values(["ticker_fixed", "end_date"], ascending=[True, False], na_position="last")
        has_fq = dfq["fiscal_quarter"].notna() if "fiscal_quarter" in dfq.columns else pd.Series(False, index=dfq.index)
        dfq1 = dfq[has_fq].drop_duplicates(subset=["ticker_fixed", "fiscal_quarter"], keep="first")
        dfq2 = dfq[~has_fq]
        dfq = pd.concat([dfq1, dfq2], ignore_index=True)
        for c in ("fiscal_year", "fiscal_period", "fiscal_quarter", "end_date"):
            if c not in dfq.columns:
                dfq[c] = pd.NA
        dfq = dfq.reindex(columns=QUARTERLY_COLUMNS)
        outdir.mkdir(parents=True, exist_ok=True)
        dfq.to_parquet(outdir / "fundamentals_quarterly.parquet", index=False)
        dfq.to_csv(outdir / "fundamentals_quarterly.csv", index=False)
        if not dfq.empty and "ticker_fixed" in dfq.columns:
            per_ticker = dfq.groupby("ticker_fixed").size()
            min_r, max_r = int(per_ticker.min()), int(per_ticker.max())
            log.info("fundamentals_quarterly: ticker_fixed별 행 수 분포 min=%s max=%s", min_r, max_r)
            aapl = dfq[dfq["ticker_fixed"] == "AAPL"]
            if not aapl.empty:
                log.info(
                    "AAPL 상위 6개 fiscal_quarter/end_date: %s",
                    aapl[["fiscal_quarter", "end_date"]].head(6).to_dict("records"),
                )
    if market_rows:
        dfm = pd.DataFrame(market_rows)
        dfm = dfm.reindex(columns=SNAPSHOT_COLUMNS)
        dfm.to_parquet(outdir / "market_snapshot.parquet", index=False)
        dfm.to_csv(outdir / "market_snapshot.csv", index=False)

    if eod_dfs:
        dfe = pd.concat(eod_dfs, ignore_index=True)
        dfe = dfe.drop_duplicates(subset=["ticker_fixed", "date"], keep="first")
        dfe = dfe.reindex(columns=PRICE_EOD_COLUMNS)
        dfe.to_parquet(outdir / "price_eod.parquet", index=False)
        dfe.to_csv(outdir / "price_eod.csv", index=False)
        log.info("price_eod: %s rows", len(dfe))
    if ownership_rows:
        dfo = pd.DataFrame(ownership_rows).reindex(columns=OWNERSHIP_COLUMNS)
        dfo.to_parquet(outdir / "ownership.parquet", index=False)
        dfo.to_csv(outdir / "ownership.csv", index=False)
        log.info("ownership: %s rows", len(dfo))
    if analyst_rows:
        dfa = pd.DataFrame(analyst_rows).reindex(columns=ANALYST_ESTIMATES_COLUMNS)
        dfa.to_parquet(outdir / "analyst_estimates.parquet", index=False)
        dfa.to_csv(outdir / "analyst_estimates.csv", index=False)
        log.info("analyst_estimates: %s rows", len(dfa))
    if earnings_dfs:
        dfearn = pd.concat(earnings_dfs, ignore_index=True)
        dfearn = dfearn.reindex(columns=EARNINGS_COLUMNS)
        dfearn.to_parquet(outdir / "earnings.parquet", index=False)
        dfearn.to_csv(outdir / "earnings.csv", index=False)
        log.info("earnings: %s rows", len(dfearn))

    if getattr(args, "fetch_actions", False):
        EARNINGS_ACTIONS_COLUMNS = ["ticker_fixed", "date", "epsEstimated", "epsActual", "revenueEstimated", "revenueActual", "lastUpdated"]
        DIVIDENDS_ACTIONS_COLUMNS = ["ticker_fixed", "date", "dividend", "adjDividend", "yield", "frequency", "declarationDate", "recordDate", "paymentDate"]
        SPLITS_ACTIONS_COLUMNS = ["ticker_fixed", "date", "numerator", "denominator", "splitType"]
        if earnings_actions_dfs:
            dea = pd.concat(earnings_actions_dfs, ignore_index=True)
            dea = dea.drop_duplicates(subset=["ticker_fixed", "date"], keep="first")
            dea = dea.reindex(columns=EARNINGS_ACTIONS_COLUMNS)
            dea.to_parquet(outdir / "earnings_actions.parquet", index=False)
            dea.to_csv(outdir / "earnings_actions.csv", index=False)
            log.info("earnings_actions: %s rows", len(dea))
        if dividends_actions_dfs:
            dda = pd.concat(dividends_actions_dfs, ignore_index=True)
            dda = dda.drop_duplicates(subset=["ticker_fixed", "date"], keep="first")
            dda = dda.reindex(columns=DIVIDENDS_ACTIONS_COLUMNS)
            dda.to_parquet(outdir / "dividends_actions.parquet", index=False)
            dda.to_csv(outdir / "dividends_actions.csv", index=False)
            log.info("dividends_actions: %s rows", len(dda))
        if splits_actions_dfs:
            dsa = pd.concat(splits_actions_dfs, ignore_index=True)
            dsa = dsa.drop_duplicates(subset=["ticker_fixed", "date", "numerator", "denominator"], keep="first")
            dsa = dsa.reindex(columns=SPLITS_ACTIONS_COLUMNS)
            dsa.to_parquet(outdir / "splits_actions.parquet", index=False)
            dsa.to_csv(outdir / "splits_actions.csv", index=False)
            log.info("splits_actions: %s rows", len(dsa))

    if getattr(args, "fetch_calendars", False):
        session_cal = make_session()
        cal_data = fetch_calendars_once(session_cal, rl, api_key, from_date, to_date, call_counter)
        cal_rows: List[Dict[str, Any]] = []
        for cal_type, lst in cal_data.items():
            for r in lst or []:
                row_cal = {"calendar_type": cal_type, "date": str(r.get("date") or "")[:10], "symbol": r.get("symbol")}
                for k, v in r.items():
                    if k not in row_cal and v is not None:
                        row_cal[k] = v
                cal_rows.append(row_cal)
        if cal_rows:
            dfcal = pd.DataFrame(cal_rows)
            for c in CALENDARS_COLUMNS:
                if c not in dfcal.columns:
                    dfcal[c] = pd.NA
            dfcal.to_parquet(outdir / "calendars.parquet", index=False)
            dfcal.to_csv(outdir / "calendars.csv", index=False)
            log.info("calendars: %s rows", len(dfcal))

    if getattr(args, "fetch_macro", False):
        session_macro = make_session()
        macro_data = fetch_macro_once(session_macro, rl, api_key, macro_from, macro_to, economic_names=econ_names_list, call_counter=call_counter)
        macro_rows: List[Dict[str, Any]] = []
        for r in macro_data.get("treasury") or []:
            macro_rows.append({
                "series_type": "treasury",
                "name": r.get("name") or r.get("symbol") or "",
                "date": str(r.get("date") or "")[:10],
                "value": pick_value(r, ["value", "close", "rate", "yield"]),
            })
        for r in macro_data.get("economic_indicators") or []:
            macro_rows.append({
                "series_type": "econ_indicator",
                "name": r.get("_indicator_name") or r.get("name") or "",
                "date": str(r.get("date") or "")[:10],
                "value": pick_value(r, ["value", "close", "actual"]),
            })
        for r in macro_data.get("economic_calendar") or []:
            macro_rows.append({
                "series_type": "econ_calendar",
                "name": r.get("name") or r.get("event") or "",
                "date": str(r.get("date") or "")[:10],
                "value": pick_value(r, ["value", "actual", "previous"]),
            })
        if macro_rows:
            dfmacro = pd.DataFrame(macro_rows).reindex(columns=MACRO_COLUMNS)
            dfmacro.to_parquet(outdir / "macro.parquet", index=False)
            dfmacro.to_csv(outdir / "macro.csv", index=False)
            log.info("macro: %s rows", len(dfmacro))

    # 요약
    total_calls = call_counter.get("count", 0)
    calls_per_min = total_calls / (elapsed / 60.0) if elapsed > 0 else 0
    print("\n==== 실행 결과 요약 ====")
    print(f"총 종목 수(fetch 대상): {len(rows)}, 스킵(REJECT/AMBIGUOUS): {len(skipped_rows)}")
    print(f"성공: {ok}, 실패: {fail}")
    if quarter_counts:
        from collections import Counter
        dist = Counter(quarter_counts)
        print("종목별 재무 분기 개수 분포:")
        for k in sorted(dist.keys(), reverse=True):
            print(f"  {k}개: {dist[k]} 종목")
    print(f"API 호출 수: 총 {total_calls}회, 추정 분당 {calls_per_min:.0f} calls/min")
    print(f"소요 시간: {elapsed:.1f}초")


if __name__ == "__main__":
    main()
