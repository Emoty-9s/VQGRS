# -*- coding: utf-8 -*-
r"""
FMP Company Screener 기반 미국 주식 유니버스 생성 및 관리.

FMP /stable/company-screener 엔드포인트로 미국 주요 거래소의
common stock 종목을 수집하고, 시가총액(기본 100M USD 이상)·종목유형 필터를 적용해
유니버스를 CSV + Parquet로 저장한다.

모드:
  snapshot   — screener 결과 전체로 유니버스를 완전 재생성
  scheduled  — 매월 1일 실행 통합 운영 모드
      * 1/1, 7/1: rebalance (시총 100M 미만 등 기준 미달 종목 inactive 처리)
      * 그 외 월의 1일: add-only 누적

환경변수:
  FMP_API_KEY  (필수)

실행 예시:
  python build_universe_fmp.py --data-dir ./data --mode snapshot
  python build_universe_fmp.py --data-dir ./data --mode scheduled --run-date 2026-02-01
  python build_universe_fmp.py --data-dir ./data --mode scheduled --run-date 2026-07-01
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
BASE_URL = "https://financialmodelingprep.com"
API_KEY_ENV = "FMP_API_KEY"
PATH_SCREENER = "/stable/company-screener"
PATH_PROFILE = "/stable/profile"

SCREENER_EXCHANGES = ["NYSE", "NASDAQ", "AMEX"]

FINAL_COLUMNS = [
    "symbol", "company_name", "sector", "industry",
    "status", "history", "change_date",
]

EXCLUDE_TYPES_LOWER = {
    "etf", "etn", "fund", "mutual fund", "closed-end fund",
    "trust", "preferred", "warrant", "unit", "right", "adr",
}

EXCLUDE_NAME_KEYWORDS_UPPER = [
    " ETF", " ETN", " FUND", " MUTUAL FUND",
    " PREFERRED", " WARRANT", " UNIT", " RIGHT",
    " LP UNITS", " DEPOSITARY", " ACQUISITION CORP",
    " TRUST", " INDEX FUND", " INVERSE", " ULTRA",
    " 2X", " 3X", " BULL", " BEAR",
]

EXCLUDE_SYMBOL_SUFFIXES = ["-P", "-W", "-U", "-R", ".WS", ".RT", ".UN"]

REBALANCE_ALLOWED_MMDD = {"01-01", "07-01"}

CALLS_PER_MIN = 300
MIN_INTERVAL = 60.0 / CALLS_PER_MIN
_last_call = 0.0


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------
def _make_session() -> requests.Session:
    s = requests.Session()
    retries = Retry(
        total=5,
        backoff_factor=1.0,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.headers.update({"User-Agent": "VQGRS-Universe/1.0"})
    return s


def _rate_wait() -> None:
    global _last_call
    gap = MIN_INTERVAL - (time.time() - _last_call)
    if gap > 0:
        time.sleep(gap)
    _last_call = time.time()


def _fmp_get(sess: requests.Session, path: str, params: Dict[str, Any], api_key: str) -> Any:
    _rate_wait()
    url = f"{BASE_URL}{path}"
    p = {**params, "apikey": api_key}
    r = sess.get(url, params=p, timeout=120)
    if r.status_code >= 400:
        raise RuntimeError(f"FMP API {r.status_code}: {r.text[:500]}")
    return r.json()


def normalize_bool(value: Any) -> Optional[bool]:
    """Convert API value to bool safely. True/False, 'true'/'false', 1/0, None supported."""
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        if value == 1:
            return True
        if value == 0:
            return False
        return None
    s = str(value).strip().lower()
    if s in ("true", "1", "yes"):
        return True
    if s in ("false", "0", "no"):
        return False
    return None


def fetch_profile_flags_for_symbols(
    api_key: str,
    symbols: List[str],
    sess: Optional[requests.Session] = None,
) -> pd.DataFrame:
    """Fetch isEtf, isFund, isAdr, isActivelyTrading from FMP /stable/profile for each symbol.
    Uses same rate limit as screener. On per-symbol failure: log warning and continue (no row for that symbol).
    Empty/missing profile response: row with NaN flags (not auto-excluded).
    """
    if not symbols:
        return pd.DataFrame(columns=["symbol", "isEtf", "isFund", "isAdr", "isActivelyTrading"])
    use_sess = sess if sess is not None else _make_session()
    close_sess = sess is None
    rows: List[Dict[str, Any]] = []
    success_count = 0
    fail_count = 0

    log.info("Profile fetch target symbols: %d", len(symbols))

    for sym in symbols:
        try:
            data = _fmp_get(use_sess, PATH_PROFILE, {"symbol": sym}, api_key)
        except Exception as e:
            log.warning("Profile fetch failed for %s: %s", sym, e)
            fail_count += 1
            continue

        # Response can be list with one object or single object
        if isinstance(data, list):
            if not data:
                fail_count += 1
                continue
            obj = data[0]
        elif isinstance(data, dict):
            obj = data
        else:
            fail_count += 1
            continue

        is_etf = normalize_bool(obj.get("isEtf"))
        is_fund = normalize_bool(obj.get("isFund"))
        is_adr = normalize_bool(obj.get("isAdr"))
        is_actively = normalize_bool(obj.get("isActivelyTrading"))

        rows.append({
            "symbol": sym,
            "isEtf": is_etf,
            "isFund": is_fund,
            "isAdr": is_adr,
            "isActivelyTrading": is_actively,
        })
        success_count += 1

    if close_sess:
        use_sess.close()

    log.info("Profile fetch success: %d, fail: %d", success_count, fail_count)

    if not rows:
        return pd.DataFrame(columns=["symbol", "isEtf", "isFund", "isAdr", "isActivelyTrading"])
    return pd.DataFrame(rows)


def apply_profile_filters(df: pd.DataFrame) -> pd.DataFrame:
    """Apply 2nd filter: exclude isEtf==True, isFund==True, isAdr==True, isActivelyTrading==False.
    Profile columns (isEtf, isFund, isAdr, isActivelyTrading) are only used for filtering;
    missing/NaN profile does not auto-exclude (exclude only when explicitly True/False as above).
    """
    if df.empty:
        return df
    if "isEtf" not in df.columns:
        log.info("No profile columns — skipping profile 2nd filter")
        return df.copy()

    out = df.copy()
    n_before = len(out)

    ex_etf = (out["isEtf"] == True) if "isEtf" in out.columns else pd.Series(False, index=out.index)
    ex_fund = (out["isFund"] == True) if "isFund" in out.columns else pd.Series(False, index=out.index)
    ex_adr = (out["isAdr"] == True) if "isAdr" in out.columns else pd.Series(False, index=out.index)
    ex_inactive = (out["isActivelyTrading"] == False) if "isActivelyTrading" in out.columns else pd.Series(False, index=out.index)

    n_etf = ex_etf.sum()
    n_fund = ex_fund.sum()
    n_adr = ex_adr.sum()
    n_inactive = ex_inactive.sum()

    log.info("Excluded by profile isEtf: %d", n_etf)
    log.info("Excluded by profile isFund: %d", n_fund)
    log.info("Excluded by profile isAdr: %d", n_adr)
    log.info("Excluded by profile inactive: %d", n_inactive)

    drop_mask = ex_etf | ex_fund | ex_adr | ex_inactive
    out = out[~drop_mask].reset_index(drop=True)
    n_excluded = n_before - len(out)
    log.info("Profile 2nd filter total excluded: %d", n_excluded)

    return out


# ---------------------------------------------------------------------------
# 1. Fetch
# ---------------------------------------------------------------------------
def fetch_screener(
    api_key: str,
    min_market_cap: int,
    country: str,
    sess: Optional[requests.Session] = None,
) -> pd.DataFrame:
    """FMP company-screener를 거래소별로 호출하여 후보 종목 DataFrame을 반환한다."""
    use_sess = sess if sess is not None else _make_session()
    all_rows: List[Dict[str, Any]] = []

    for exch in SCREENER_EXCHANGES:
        try:
            data = _fmp_get(use_sess, PATH_SCREENER, {
                "marketCapMoreThan": min_market_cap,
                "country": country,
                "exchange": exch,
                "isActivelyTrading": "true",
                "limit": 10000,
            }, api_key)
        except Exception as e:
            log.error("Screener %s failed: %s", exch, e)
            continue

        if isinstance(data, list):
            all_rows.extend(data)
            log.info("Screener %s: %d rows", exch, len(data))
        else:
            log.warning("Screener %s: unexpected response type %s", exch, type(data).__name__)

    if not all_rows:
        return pd.DataFrame()

    records = []
    for r in all_rows:
        sym = (r.get("symbol") or "").strip().upper()
        if not sym:
            continue
        records.append({
            "symbol": sym,
            "company_name": (r.get("companyName") or r.get("name") or "").strip(),
            "market_cap": r.get("marketCap") or r.get("mktCap"),
            "exchange": (r.get("exchangeShortName") or r.get("exchange") or "").strip().upper(),
            "sector": (r.get("sector") or "").strip(),
            "industry": (r.get("industry") or "").strip(),
            "asset_type": (r.get("type") or "").strip(),
        })

    df = pd.DataFrame(records)
    log.info("Screener raw total: %d rows", len(df))
    df = df.drop_duplicates(subset=["symbol"], keep="first").reset_index(drop=True)
    log.info("After dedup: %d rows", len(df))
    return df


# ---------------------------------------------------------------------------
# 2. Filter
# ---------------------------------------------------------------------------
def filter_universe(df: pd.DataFrame, min_market_cap: int) -> pd.DataFrame:
    """ETF·워런트 등 비 common stock 제외, 시가총액 필터 적용."""
    if df.empty:
        return df

    df = df.copy()

    if "asset_type" in df.columns:
        type_mask = df["asset_type"].str.strip().str.lower().isin(EXCLUDE_TYPES_LOWER)
        log.info("Excluded by type: %d", type_mask.sum())
        df = df[~type_mask]

    suffix_mask = pd.Series(False, index=df.index)
    for sfx in EXCLUDE_SYMBOL_SUFFIXES:
        suffix_mask |= df["symbol"].str.endswith(sfx)
    log.info("Excluded by symbol suffix: %d", suffix_mask.sum())
    df = df[~suffix_mask]

    name_upper = df["company_name"].str.upper()
    kw_mask = pd.Series(False, index=df.index)
    for kw in EXCLUDE_NAME_KEYWORDS_UPPER:
        kw_mask |= name_upper.str.contains(kw, na=False)
    log.info("Excluded by name keyword: %d", kw_mask.sum())
    df = df[~kw_mask]

    df["market_cap"] = pd.to_numeric(df["market_cap"], errors="coerce")
    before = len(df)
    df = df[df["market_cap"] >= min_market_cap]
    log.info("Excluded by market cap < %s: %d", f"{min_market_cap:,}", before - len(df))

    return df.reset_index(drop=True)


# ---------------------------------------------------------------------------
# 3. Snapshot finalize
# ---------------------------------------------------------------------------
def finalize_snapshot(df: pd.DataFrame, run_date: str) -> pd.DataFrame:
    """snapshot 모드: 전체를 active/NEW/run_date로 완전 재생성."""
    if df.empty:
        return pd.DataFrame(columns=FINAL_COLUMNS)

    out = df[["symbol", "company_name", "sector", "industry"]].copy()
    out["status"] = "active"
    out["history"] = "NEW"
    out["change_date"] = run_date
    return out.sort_values("symbol").reset_index(drop=True)[FINAL_COLUMNS]


# ---------------------------------------------------------------------------
# 4. Load existing universe
# ---------------------------------------------------------------------------
def load_existing_universe(csv_path: Path, parquet_path: Path) -> pd.DataFrame:
    """기존 universe_current 파일을 읽는다. CSV 우선, 없으면 parquet, 둘 다 없으면 빈 DataFrame."""
    df = pd.DataFrame(columns=FINAL_COLUMNS)

    if csv_path.exists():
        try:
            df = pd.read_csv(csv_path, dtype=str)
            log.info("Loaded existing CSV: %s (%d rows)", csv_path, len(df))
        except Exception as e:
            log.warning("CSV 읽기 실패 (%s): %s", csv_path, e)
            df = pd.DataFrame(columns=FINAL_COLUMNS)
    elif parquet_path.exists():
        try:
            df = pd.read_parquet(parquet_path)
            for c in df.columns:
                df[c] = df[c].astype(str)
            log.info("Loaded existing Parquet: %s (%d rows)", parquet_path, len(df))
        except Exception as e:
            log.warning("Parquet 읽기 실패 (%s): %s", parquet_path, e)
            df = pd.DataFrame(columns=FINAL_COLUMNS)
    else:
        log.info("No existing universe file found — will treat as initial creation")

    for col in FINAL_COLUMNS:
        if col not in df.columns:
            df[col] = ""

    if not df.empty:
        df["symbol"] = df["symbol"].str.strip().str.upper()

    return df


# ---------------------------------------------------------------------------
# 5. Add-mode merge
# ---------------------------------------------------------------------------
def merge_add_mode(
    existing_df: pd.DataFrame,
    new_df: pd.DataFrame,
    run_date: str,
) -> pd.DataFrame:
    """add 모드: 기존 active는 STAY, inactive 재등장은 REENTRY, 완전 신규는 NEW. 삭제 없음."""
    if existing_df.empty:
        log.info("No existing data — treating all %d symbols as NEW", len(new_df))
        return finalize_snapshot(new_df, run_date)

    new_candidates = new_df[["symbol", "company_name", "sector", "industry"]].copy()
    new_candidates["symbol"] = new_candidates["symbol"].str.strip().str.upper()
    new_candidates = new_candidates.drop_duplicates(subset=["symbol"], keep="first")
    new_syms: Set[str] = set(new_candidates["symbol"].unique())

    existing_map: Dict[str, pd.Series] = {}
    for _, row in existing_df.iterrows():
        existing_map[row["symbol"]] = row
    existing_syms: Set[str] = set(existing_map.keys())

    inactive_syms: Set[str] = {
        s for s, r in existing_map.items()
        if str(r.get("status", "")).strip().lower() == "inactive"
    }

    reentry_syms = inactive_syms & new_syms
    stay_syms = (existing_syms - inactive_syms) & new_syms
    new_only_syms = new_syms - existing_syms

    rows: List[Dict[str, Any]] = []

    for sym in existing_syms:
        prev = existing_map[sym]
        if sym in reentry_syms:
            rows.append({
                "symbol": sym,
                "company_name": prev.get("company_name", ""),
                "sector": prev.get("sector", ""),
                "industry": prev.get("industry", ""),
                "status": "active",
                "history": "REENTRY",
                "change_date": prev.get("change_date", ""),
            })
        elif sym in stay_syms:
            rows.append({
                "symbol": sym,
                "company_name": prev.get("company_name", ""),
                "sector": prev.get("sector", ""),
                "industry": prev.get("industry", ""),
                "status": "active",
                "history": "STAY",
                "change_date": prev.get("change_date", ""),
            })
        else:
            rows.append({
                "symbol": sym,
                "company_name": prev.get("company_name", ""),
                "sector": prev.get("sector", ""),
                "industry": prev.get("industry", ""),
                "status": prev.get("status", "active"),
                "history": prev.get("history", ""),
                "change_date": prev.get("change_date", ""),
            })

    new_lookup = new_candidates.set_index("symbol")
    for sym in sorted(new_only_syms):
        info = new_lookup.loc[sym]
        rows.append({
            "symbol": sym,
            "company_name": info.get("company_name", ""),
            "sector": info.get("sector", ""),
            "industry": info.get("industry", ""),
            "status": "active",
            "history": "NEW",
            "change_date": run_date,
        })

    result = pd.DataFrame(rows)
    result = result.drop_duplicates(subset=["symbol"], keep="first")
    result = result.sort_values("symbol").reset_index(drop=True)

    n_stay = (result["history"] == "STAY").sum()
    n_reentry = (result["history"] == "REENTRY").sum()
    n_new = (result["history"] == "NEW").sum()
    log.info("Add mode — STAY: %d, REENTRY: %d, NEW: %d", n_stay, n_reentry, n_new)

    return result[FINAL_COLUMNS]


# ---------------------------------------------------------------------------
# 6. Rebalance-mode merge
# ---------------------------------------------------------------------------
def merge_rebalance_mode(
    existing_df: pd.DataFrame,
    new_df: pd.DataFrame,
    run_date: str,
) -> pd.DataFrame:
    """rebalance 모드: stay/reentry/new/exit를 동시 처리한다.

    - existing active  ∩ new  → active  / STAY    / 기존 change_date
    - existing inactive ∩ new → active  / REENTRY / 기존 change_date
    - new − existing          → active  / NEW     / run_date
    - existing − new          → inactive / EXIT   / run_date
    """
    if existing_df.empty:
        log.info("No existing data — treating all %d symbols as NEW", len(new_df))
        return finalize_snapshot(new_df, run_date)

    new_candidates = new_df[["symbol", "company_name", "sector", "industry"]].copy()
    new_candidates["symbol"] = new_candidates["symbol"].str.strip().str.upper()
    new_candidates = new_candidates.drop_duplicates(subset=["symbol"], keep="first")
    new_lookup = new_candidates.set_index("symbol")
    new_syms: Set[str] = set(new_candidates["symbol"].unique())

    existing_map: Dict[str, pd.Series] = {}
    for _, row in existing_df.iterrows():
        existing_map[row["symbol"]] = row
    existing_syms: Set[str] = set(existing_map.keys())

    inactive_syms: Set[str] = {
        s for s, r in existing_map.items()
        if str(r.get("status", "")).strip().lower() == "inactive"
    }
    active_syms: Set[str] = existing_syms - inactive_syms

    stay_syms = active_syms & new_syms
    reentry_syms = inactive_syms & new_syms
    new_only_syms = new_syms - existing_syms
    exit_syms = existing_syms - new_syms

    rows: List[Dict[str, Any]] = []

    for sym in stay_syms:
        prev = existing_map[sym]
        info = new_lookup.loc[sym] if sym in new_lookup.index else prev
        rows.append({
            "symbol": sym,
            "company_name": info.get("company_name", ""),
            "sector": info.get("sector", ""),
            "industry": info.get("industry", ""),
            "status": "active",
            "history": "STAY",
            "change_date": prev.get("change_date", ""),
        })

    for sym in reentry_syms:
        prev = existing_map[sym]
        info = new_lookup.loc[sym] if sym in new_lookup.index else prev
        rows.append({
            "symbol": sym,
            "company_name": info.get("company_name", ""),
            "sector": info.get("sector", ""),
            "industry": info.get("industry", ""),
            "status": "active",
            "history": "REENTRY",
            "change_date": prev.get("change_date", ""),
        })

    for sym in sorted(new_only_syms):
        info = new_lookup.loc[sym]
        rows.append({
            "symbol": sym,
            "company_name": info.get("company_name", ""),
            "sector": info.get("sector", ""),
            "industry": info.get("industry", ""),
            "status": "active",
            "history": "NEW",
            "change_date": run_date,
        })

    for sym in exit_syms:
        prev = existing_map[sym]
        rows.append({
            "symbol": sym,
            "company_name": prev.get("company_name", ""),
            "sector": prev.get("sector", ""),
            "industry": prev.get("industry", ""),
            "status": "inactive",
            "history": "EXIT",
            "change_date": run_date,
        })

    result = pd.DataFrame(rows)
    result = result.drop_duplicates(subset=["symbol"], keep="first")
    result = result.sort_values("symbol").reset_index(drop=True)

    n_stay = (result["history"] == "STAY").sum()
    n_reentry = (result["history"] == "REENTRY").sum()
    n_new = (result["history"] == "NEW").sum()
    n_exit = (result["history"] == "EXIT").sum()
    log.info("Rebalance — STAY: %d, REENTRY: %d, NEW: %d, EXIT: %d",
             n_stay, n_reentry, n_new, n_exit)

    return result[FINAL_COLUMNS]


# ---------------------------------------------------------------------------
# 7. Scheduled date validation
# ---------------------------------------------------------------------------
def get_scheduled_run_type(run_date: str) -> str:
    """scheduled 모드에서 run_date(YYYY-MM-DD)에 따른 실행 타입을 반환한다.

    - 매월 1일이 아니면 "invalid"
    - 01-01, 07-01이면 "rebalance"
    - 그 외 월의 1일이면 "add"
    """
    if not run_date or len(run_date) < 10:
        return "invalid"
    try:
        day = run_date[8:10]
        mmdd = run_date[5:10]
    except (IndexError, TypeError):
        return "invalid"
    if day != "01":
        return "invalid"
    if mmdd in REBALANCE_ALLOWED_MMDD:
        return "rebalance"
    return "add"


# ---------------------------------------------------------------------------
# 8–9. Save
# ---------------------------------------------------------------------------
def save_csv(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)
    log.info("CSV saved: %s (%d rows)", path, len(df))


def save_parquet(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        df.to_parquet(path, index=False, engine="pyarrow")
    except ImportError:
        try:
            df.to_parquet(path, index=False, engine="fastparquet")
        except ImportError:
            log.error(
                "Parquet 저장 실패: pyarrow 또는 fastparquet가 설치되어 있지 않습니다. "
                "pip install pyarrow 또는 pip install fastparquet 를 실행하세요."
            )
            return
    except Exception as e:
        log.error("Parquet 저장 실패: %s", e)
        return
    log.info("Parquet saved: %s (%d rows)", path, len(df))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    ap = argparse.ArgumentParser(description="FMP Screener 기반 유니버스 생성 및 관리")
    ap.add_argument("--data-dir", type=str, default="./data")
    ap.add_argument("--min-market-cap", type=int, default=100_000_000,
                    help="최소 시가총액 USD (기본 100,000,000 = 100M)")
    ap.add_argument("--country", type=str, default="US")
    ap.add_argument("--out-current-csv", type=str, default="universe_current.csv")
    ap.add_argument("--out-current-parquet", type=str, default="universe_current.parquet")
    ap.add_argument("--mode", type=str, default="snapshot",
                    choices=["snapshot", "scheduled"],
                    help="snapshot=완전 재생성, scheduled=매월 1일 통합 운영(add/rebalance 자동 분기)")
    ap.add_argument("--run-date", type=str, default="",
                    help="실행일 YYYY-MM-DD (미입력 시 오늘; scheduled 모드는 매월 1일만 허용)")
    args = ap.parse_args()

    api_key = (os.environ.get(API_KEY_ENV) or "").strip()
    if not api_key:
        log.error("환경변수 %s 가 설정되지 않았습니다.", API_KEY_ENV)
        sys.exit(1)

    data_dir = Path(args.data_dir)
    csv_path = data_dir / args.out_current_csv
    pq_path = data_dir / args.out_current_parquet
    run_date = args.run_date.strip() if args.run_date.strip() else date.today().isoformat()
    scheduled_run_type: str = ""

    log.info("=== build_universe_fmp START ===")
    log.info("mode=%s  run_date=%s  min_market_cap=%s  country=%s",
             args.mode, run_date, f"{args.min_market_cap:,}", args.country)

    # --- scheduled 모드: 매월 1일만 허용 ---
    if args.mode == "scheduled":
        scheduled_run_type = get_scheduled_run_type(run_date)
        if scheduled_run_type == "invalid":
            log.error(
                "scheduled 모드는 매월 1일만 실행할 수 있습니다. run_date=%s 는 허용되지 않습니다.",
                run_date,
            )
            sys.exit(1)
        log.info("scheduled(%s) for run_date=%s", scheduled_run_type, run_date)

    # --- fetch & filter (1st: screener type/name/suffix/market cap) ---
    sess = _make_session()
    raw = fetch_screener(api_key, args.min_market_cap, args.country, sess=sess)
    if raw.empty:
        log.error("Screener에서 데이터를 받지 못했습니다. API 키와 네트워크를 확인하세요.")
        sys.exit(1)

    filtered = filter_universe(raw, args.min_market_cap)
    log.info("After 1st filter (type/name/suffix/market cap): %d symbols", len(filtered))

    if filtered.empty:
        log.warning("1차 필터 후 유니버스가 0건입니다. 빈 파일을 저장하지 않고 종료합니다.")
        sys.exit(1)

    # --- profile 2nd filter ---
    profile_df = fetch_profile_flags_for_symbols(api_key, filtered["symbol"].tolist(), sess=sess)
    merged = filtered.merge(profile_df, on="symbol", how="left")
    after_profile = apply_profile_filters(merged)
    log.info("After profile 2nd filter: %d symbols", len(after_profile))

    if after_profile.empty:
        log.warning("2차 필터 후 유니버스가 0건입니다. 빈 파일을 저장하지 않고 종료합니다.")
        sys.exit(1)

    # --- mode dispatch (use only columns needed; FINAL_COLUMNS does not include profile flags) ---
    if args.mode == "snapshot":
        result = finalize_snapshot(after_profile, run_date)

    else:  # scheduled
        existing = load_existing_universe(csv_path, pq_path)
        if scheduled_run_type == "rebalance":
            result = merge_rebalance_mode(existing, after_profile, run_date)
        else:
            result = merge_add_mode(existing, after_profile, run_date)

    log.info("Final universe: %d rows", len(result))

    save_csv(result, csv_path)
    save_parquet(result, pq_path)

    log.info("=== build_universe_fmp DONE ===")


if __name__ == "__main__":
    main()
