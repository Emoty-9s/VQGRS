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
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
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
# 예: stable RSI = /stable/technical-indicators/rsi?symbol=AAPL&periodLength=10&timeframe=1day
PATH_TECH_DAILY_LEGACY = "/api/v3/technical_indicator/daily"  # /daily/{symbol}?type=rsi&period=14

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
    "rsi14", "sma20", "sma50", "sma200",
]


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
) -> Tuple[Optional[pd.DataFrame], Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    """한 종목 재무 + 시장/기술 수집. row["ticker_fixed"]로 FMP 호출. (qdf, market_row, raw_dict)."""
    sym = str(row.get("ticker_fixed") or "").strip().upper()
    if not sym:
        raise RuntimeError("row has no ticker_fixed")
    fetch_limit = limit + 8
    raw: Dict[str, Any] = {} if save_raw_dir else {}
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
            "rsi14": rsi14,
            "sma20": sma20,
            "sma50": sma50,
            "sma200": sma200,
        }

    if save_raw_dir and raw:
        (save_raw_dir / f"{sym}.raw.json").write_text(
            json.dumps(raw, default=str), encoding="utf-8"
        )
    return (qdf, market_row, raw if save_raw_dir else None)


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
    ap.add_argument("--skip-market", action="store_true", help="시장/기술 수집 생략")
    ap.add_argument("--skip-tech", action="store_true", help="RSI/SMA 수집 생략")
    ap.add_argument("--use-cache", action="store_true", help="이미 저장된 symbol은 재호출 스킵 (quarterly는 fiscal_quarter 스키마일 때만 병합)")
    ap.add_argument("--save-raw", action="store_true", help="raw JSON 응답 저장 (outdir/raw/)")
    args = ap.parse_args()

    api_key = os.environ.get(API_KEY_ENV) or ""
    if not api_key.strip():
        raise RuntimeError("환경변수 FMP_API_KEY가 설정되지 않았습니다.")

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    save_raw_dir: Optional[Path] = outdir / "raw" if args.save_raw else None
    if save_raw_dir:
        save_raw_dir.mkdir(parents=True, exist_ok=True)

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
    fetch_failures: List[Dict[str, Any]] = []
    ok = 0
    fail = 0
    quarter_counts: List[int] = []
    start_time = time.time()
    skipped_ticker_fixed: Set[str] = set()

    to_fetch = [
        r for r in rows
        if not (args.use_cache and (r.get("ticker_fixed") or "").upper() in cached_q and (args.skip_market or (r.get("ticker_fixed") or "").upper() in cached_s))
    ]
    for r in rows:
        tf = (r.get("ticker_fixed") or "").upper()
        if args.use_cache and tf in cached_q and (args.skip_market or tf in cached_s):
            skipped_ticker_fixed.add(tf)

    def fetch_one(row: Dict[str, Any]) -> Tuple[Dict[str, Any], Optional[pd.DataFrame], Optional[Dict[str, Any]], Optional[Exception]]:
        session = make_session()
        try:
            qdf, mrow, _ = process_symbol(
                row, session, rl, api_key,
                args.period, args.limit,
                args.years_back,
                args.skip_market, args.skip_tech,
                call_counter, save_raw_dir,
            )
            return (row, qdf, mrow, None)
        except Exception as e:
            return (row, None, None, e)

    if args.max_workers <= 1:
        for idx, row in enumerate(to_fetch, 1):
            row, qdf, market_row, err = fetch_one(row)
            tf = (row.get("ticker_fixed") or "").upper()
            if err is not None:
                fail += 1
                fetch_failures.append({
                    "ticker_fixed": tf,
                    "ticker_raw": row.get("ticker_raw", tf),
                    "error": str(err),
                    "ts": time.strftime("%Y-%m-%d %H:%M:%S"),
                })
                log.warning("[FAIL] %s err=%s", tf, err)
            else:
                if qdf is not None and not qdf.empty:
                    all_quarterly.append(qdf)
                    quarter_counts.append(len(qdf))
                if market_row is not None:
                    market_rows.append(market_row)
                ok += 1
            if idx % 50 == 0:
                log.info("[progress] %s/%s ok=%s fail=%s calls=%s", idx, len(to_fetch), ok, fail, call_counter.get("count", 0))
    else:
        completed = 0
        with ThreadPoolExecutor(max_workers=args.max_workers) as executor:
            futures = {executor.submit(fetch_one, r): r for r in to_fetch}
            for fut in as_completed(futures):
                completed += 1
                row, qdf, market_row, err = fut.result()
                tf = (row.get("ticker_fixed") or "").upper()
                if err is not None:
                    fail += 1
                    fetch_failures.append({
                        "ticker_fixed": tf,
                        "ticker_raw": row.get("ticker_raw", tf),
                        "error": str(err),
                        "ts": time.strftime("%Y-%m-%d %H:%M:%S"),
                    })
                    log.warning("[FAIL] %s err=%s", tf, err)
                else:
                    if qdf is not None and not qdf.empty:
                        all_quarterly.append(qdf)
                        quarter_counts.append(len(qdf))
                    if market_row is not None:
                        market_rows.append(market_row)
                    ok += 1
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
