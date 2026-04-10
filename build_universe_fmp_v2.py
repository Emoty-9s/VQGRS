# -*- coding: utf-8 -*-
r"""
FMP 유니버스 빌더 v2 — company-screener 후보에서 **순차 제외**로 유니버스를 만든다.

데이터 소스
-----------
* **company-screener**: 미국·허용 거래소·시총·활성 등 API 필터로 **기본 후보 풀**.
* **stock-list**: 심볼 기준으로 ``type`` 을 붙일 수 있을 때만 참조해 **추가 제외**(SPAC·채권류 등).

정책
----
* **정밀 분류 엔진이 아님** — “제외군을 순서대로 걷어낸 뒤 남은 것”이 유니버스다.
* ADR·보통주 **분리 판정 없음**. ADR 전용 컬럼 없음.
* 회사명·티커·suffix **휴리스틱 금지**.
* **stock-list 는 필수 통과문이 아님** — 심볼이 목록에 없거나 ``type`` 이 비어도 후보는 스크리너 게이트만 통과하면 다음 단계로 진행.
* 동일 심볼에 stock-list 행이 여러 개면, 부착된 ``type_norm`` 토큰 **어느 하나라도** 제외 토큰이면 ``EXCLUDED_TYPE``.
* 스크리너 ``isFund`` / ``isEtf`` 로 먼저 펀드·ETF 제거 후, 나머지 비보통주성 상품은 ``stock-list.type`` **정확 일치** 제외 토큰으로만 제거.
* **REIT** 는 제외 집합에 없음.

파이프라인
----------
1. fetch company-screener
2. normalize screener candidates
3. fetch / normalize stock-list (선택적 참조)
4. symbol 기준으로 stock-list ``type`` 부착 (없으면 빈 값)
5. 순차 제외 qualify
6. ``finalize`` 후 **해당 시점** 유니버스 CSV/Parquet 저장
7. write debug reports (옵션)

**상태 관리 없음** — ``status``/``history``/``change_date``/merge 는 사용하지 않는다.

환경 변수: ``FMP_API_KEY`` (필수).
"""
from __future__ import annotations

import argparse
import logging
import os
import re
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

import numpy as np
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

log = logging.getLogger(__name__)

# =============================================================================
# 0. Logging bootstrap (minimal; CLI에서 level 조정 가능)
# =============================================================================
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# =============================================================================
# 1. API / runtime constants
# =============================================================================
BASE_URL = "https://financialmodelingprep.com"
API_KEY_ENV = "FMP_API_KEY"
PATH_SCREENER = "/stable/company-screener"
PATH_STOCK_LIST = "/stable/stock-list"

SCREENER_EXCHANGES: List[str] = ["NYSE", "NASDAQ", "AMEX"]

DEFAULT_MIN_MARKET_CAP: int = 100_000_000

# =============================================================================
# 2. Saved output schemas (Parquet/CSV)
# =============================================================================
# 최종 저장: **5개 핵심 컬럼** (+선택 디버그). ADR 전용 컬럼 없음.
FINAL_COLUMNS: List[str] = [
    "symbol",
    "company_name",
    "sector",
    "industry",
    "is_actively_trading",
]

# ``--keep-debug-columns`` 등으로만 저장에 덧붙임.
UNIVERSE_DEBUG_COLUMNS: List[str] = [
    "qualifies",
    "reason_code",
]

# 저장 시 bool 로 강제할 컬럼.
UNIVERSE_BOOL_STORAGE_COLUMNS: frozenset[str] = frozenset({"qualifies", "is_actively_trading"})

# 이전 스키마·레거시 열(최종 출력에 남기지 않음).
LEGACY_UNIVERSE_COLUMNS_DROP: Tuple[str, ...] = (
    "is_adr",
    "exchange",
    "market_cap",
    "security_type",
    "status",
    "history",
    "change_date",
)

# ``write_debug_reports`` 기본 파일명(``data_dir`` 기준).
DEFAULT_QUALIFY_REASON_SUMMARY_CSV: str = "qualify_reason_summary.csv"
DEFAULT_STOCK_LIST_TYPE_DISTRIBUTION_CSV: str = "stock_list_type_distribution.csv"
DEFAULT_UNKNOWN_NONEXCLUDED_TYPE_SYMBOLS_CSV: str = "unknown_nonexcluded_type_symbols.csv"
DEFAULT_MISSING_TYPE_SYMBOLS_CSV: str = "missing_type_symbols.csv"
DEFAULT_JOIN_CONFLICT_SYMBOLS_CSV: str = "join_conflict_symbols.csv"

# ``--debug-qualify-report`` 인자 생략 시 ``data-dir`` 아래 기본 qualify 요약 파일명.
DEBUG_QUALIFY_REPORT_DEFAULT: str = "__DEFAULT__"

# =============================================================================
# 3. Internal table schemas (normalize / join / qualify 입력)
# =============================================================================
# 1) Screener 후보 (조인 전). 거래소는 원문 + 조인 키용 정규화 열을 분리.
SCREENER_CANDIDATE_COLUMNS: List[str] = [
    "symbol",
    "company_name",
    "exchange_raw",
    "exchange_canonical",
    "sector",
    "industry",
    "market_cap",
    "is_etf_flag",
    "is_fund_flag",
    "is_actively_trading_flag",
]

# 2) Stock-list 타입 참조 (조인 전).
STOCK_LIST_TYPE_COLUMNS: List[str] = [
    "symbol",
    "exchange_raw",
    "exchange_canonical",
    "type_raw",
    "type_norm",
]

# 3) stock-list 타입 부착 후 qualify 입력(``join_status`` 없음 — 목록 미존재는 탈락 사유 아님).
JOINED_QUALIFY_INPUT_COLUMNS: List[str] = [
    "symbol",
    "company_name",
    "exchange_canonical",
    "sector",
    "industry",
    "market_cap",
    "is_etf_flag",
    "is_fund_flag",
    "is_actively_trading_flag",
    "type_raw",
    "type_norm",
]

# 동일 심볼에 stock-list ``type_norm`` 이 여러 개일 때 셀에 합칠 구분자(FMP 타입 문자열에 등장하지 않도록 ``|`` 사용).
STOCK_LIST_TYPE_NORM_SEP: str = "|"

# =============================================================================
# 4. Type tokens — exclusion only (REIT 미포함)
# =============================================================================
# 부착된 ``type_norm`` 토큰 각각과 **정확 일치**할 때만 제외. 이름/티커/suffix 와 무관.
TYPE_EXCLUSION_TOKENS: frozenset[str] = frozenset({
    "spac",
    "preferred",
    "preferred stock",
    "warrant",
    "unit",
    "right",
    "bond",
    "note",
    "trust",
    "mutual fund",
    "closed-end fund",
    "fund",
    "etf",
    "etn",
    "etp",
})

# 행 단위 qualify 사유 코드 — 유니버스 빌드에 쓰는 것만 아래 7개.
REASON_PASS: str = "PASS"
REASON_NOT_US_LISTED: str = "NOT_US_LISTED"
REASON_BELOW_MARKET_CAP: str = "BELOW_MARKET_CAP"
REASON_INACTIVE: str = "INACTIVE"
REASON_FUND: str = "FUND"
REASON_ETF: str = "ETF"
REASON_EXCLUDED_TYPE: str = "EXCLUDED_TYPE"

# 레거시·외부 스크립트 호환용(``universe_row_reason_code`` 는 반환하지 않음).
REASON_MISSING_TYPE_FROM_STOCK_LIST: str = "MISSING_TYPE_FROM_STOCK_LIST"
REASON_TYPE_CONFLICT: str = "TYPE_CONFLICT"
REASON_UNKNOWN_TYPE_EMPTY: str = "UNKNOWN_TYPE_EMPTY"

# FMP 호출 간 최소 간격(분당 요청 상한의 역수).
CALLS_PER_MINUTE: int = 300
_MIN_INTERVAL_SEC: float = 60.0 / float(CALLS_PER_MINUTE)
_last_fmp_call_mono: float = 0.0

# =============================================================================
# 5. HTTP session, rate limit, low-level GET
# =============================================================================
#
# 이 블록은 **전송 전용**이다. 유니버스 자격·``type`` 제외 등 비즈니스 규칙은 넣지 않는다.
# ---------------------------------------------------------------------------
# company-screener: API가 허용하는 필터(국가·거래소·시총·활성 등)로 **후보 풀** JSON 만 가져온다.
#   → 응답 행의 ``type`` 필드가 있어도 fetch 에서 해석·필터하지 않는다(후속 normalize/qualify).
# stock-list: **전역 심볼 목록** JSON 을 한 번에 가져온다. ``type`` 은 나중에 조인·qualify 에서만 사용.
# ---------------------------------------------------------------------------


def _make_session() -> requests.Session:
    """Retry/backoff 이 붙은 ``requests.Session`` (GET 전용)."""
    s = requests.Session()
    retries = Retry(
        total=5,
        backoff_factor=1.0,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.headers.update({"User-Agent": "VQGRS-Universe-v2/1.0"})
    return s


def _rate_wait() -> None:
    """모듈 전역 시계로 FMP 호출 간 최소 간격을 둔다."""
    global _last_fmp_call_mono
    gap = _MIN_INTERVAL_SEC - (time.monotonic() - _last_fmp_call_mono)
    if gap > 0:
        time.sleep(gap)
    _last_fmp_call_mono = time.monotonic()


def _fmp_get(
    path: str,
    params: Dict[str, Any],
    api_key: str,
    sess: requests.Session,
) -> Any:
    """
    단일 GET → 파싱된 JSON(``list`` / ``dict`` 등 원형).

    * API 키는 쿼리 ``apikey`` 로만 붙인다.
    * HTTP 4xx/5xx 는 ``RuntimeError`` (본문 일부 포함).
    * 비즈니스 판정 없음.
    """
    _rate_wait()
    url = f"{BASE_URL}{path}"
    merged = {**params, "apikey": api_key}
    log.debug("FMP GET %s params_keys=%s", path, sorted(k for k in merged.keys() if k != "apikey"))
    r = sess.get(url, params=merged, timeout=120)
    if r.status_code >= 400:
        raise RuntimeError(f"FMP API {r.status_code}: {path} — {r.text[:500]}")
    return r.json()


# =============================================================================
# 6. Fetch — external API (원본 JSON row list 반환)
# =============================================================================


def fetch_company_screener_rows(
    api_key: str,
    min_market_cap: int,
    country: str,
    exchanges: Sequence[str],
    sess: requests.Session,
) -> List[Dict[str, Any]]:
    """
    **company-screener** — 거래소마다 한 번씩 호출해 응답 **dict 행**을 이어붙인다.

    역할: 시총·국가·거래소·``isActivelyTrading`` 등 API 파라미터로 좁힌 **상장 후보** 목록만 수집.
    ``type`` 기반 필터·판정은 하지 않는다.

    쿼리: ``country``, ``exchange``, ``marketCapMoreThan``, ``isActivelyTrading=true``, ``limit=10000``.
    """
    all_rows: List[Dict[str, Any]] = []
    for exch in exchanges:
        ex = str(exch).strip()
        try:
            payload = _fmp_get(
                PATH_SCREENER,
                {
                    "country": country,
                    "exchange": ex,
                    "marketCapMoreThan": min_market_cap,
                    "isActivelyTrading": "true",
                    "limit": 10000,
                },
                api_key,
                sess,
            )
        except Exception as e:
            log.error(
                "company-screener fetch failed | exchange=%s country=%s min_mcap=%s | %s",
                ex,
                country,
                min_market_cap,
                e,
            )
            continue

        if isinstance(payload, list):
            n = len(payload)
            log.info(
                "company-screener ok | exchange=%s country=%s min_mcap=%s | json_rows=%d",
                ex,
                country,
                min_market_cap,
                n,
            )
            for item in payload:
                if isinstance(item, dict):
                    all_rows.append(item)
                else:
                    log.warning(
                        "company-screener skip non-dict row | exchange=%s type=%s",
                        ex,
                        type(item).__name__,
                    )
        else:
            log.warning(
                "company-screener unexpected response | exchange=%s json_type=%s",
                ex,
                type(payload).__name__,
            )

    log.info(
        "company-screener aggregate | exchanges_requested=%d dict_rows_total=%d",
        len(exchanges),
        len(all_rows),
    )
    return all_rows


def fetch_stock_list_rows(api_key: str, sess: requests.Session) -> List[Dict[str, Any]]:
    """
    **stock-list** — 단일 GET 으로 전역 목록을 받아 **dict 행** 리스트로 돌려준다.

    역할: 심볼·거래소·``type`` 등 **참조용 원본 필드** 수집. 제외/자격 판정은 fetch 에서 하지 않는다.
    """
    try:
        payload = _fmp_get(PATH_STOCK_LIST, {}, api_key, sess)
    except Exception as e:
        log.error("stock-list fetch failed | %s", e)
        return []

    if not isinstance(payload, list):
        log.warning("stock-list unexpected response | json_type=%s", type(payload).__name__)
        return []

    out: List[Dict[str, Any]] = []
    for item in payload:
        if isinstance(item, dict):
            out.append(item)
        else:
            log.warning("stock-list skip non-dict row | type=%s", type(item).__name__)

    log.info("stock-list ok | json_rows=%d dict_rows=%d", len(payload), len(out))
    return out


# =============================================================================
# 7. Normalize — exchange & type tokens, tabular shapes
# =============================================================================
#
# 이 단계는 **형태 정리만** 수행한다. qualify·제외 타입 판정은 하지 않는다.
#
# 중복 제거 정책 (screener·stock-list 공통):
#   키: (symbol, exchange_canonical) — 정규화된 심볼(대문자)·정규화 거래소 코드 기준.
#   동일 키가 여러 번 나오면 **첫 행만 유지**(keep="first"), 나머지는 버린다.
# ---------------------------------------------------------------------------
# ``exchangeShortName`` 과 ``exchange`` 는 호출부에서 동일하게 ``raw`` 로 넘기기 전에
# ``exchangeShortName`` 우선·없으면 ``exchange`` 를 쓰도록 흡수한다(FMP 필드 차이).
# ---------------------------------------------------------------------------

# ``normalize_exchange_code`` 가 키를 만든 뒤 조회. 키는 trim·구두점 제거·하이픈/슬래시→공백·대문자·공백 압축.
_EXCHANGE_SYNONYM_TO_CANONICAL: Dict[str, str] = {
    # 기본 세 거래소
    "NYSE": "NYSE",
    "NASDAQ": "NASDAQ",
    "AMEX": "AMEX",
    # NYSE 풀네임 / 변형
    "NEW YORK STOCK EXCHANGE": "NYSE",
    "NEW YORK STOCK EXCHANGE INC": "NYSE",
    "NEW YORK STOCK EXCHANGE LLC": "NYSE",
    # Arca
    "NYSE ARCA": "NYSE",
    "NYSEARCA": "NYSE",
    "ARCA": "NYSE",
    # AMEX / NYSE American
    "NYSE MKT": "AMEX",
    "NYSE AMERICAN": "AMEX",
    "NYSE AMERICAN EQUITIES": "AMEX",
    "NYSE AMERICAN EQUITIES LLC": "AMEX",
    # NASDAQ
    "NASDAQ STOCK MARKET": "NASDAQ",
    "NASDAQ CAPITAL MARKET": "NASDAQ",
    "NASDAQ GLOBAL MARKET": "NASDAQ",
    "NASDAQ GLOBAL SELECT": "NASDAQ",
    "NASDAQ GLOBAL SELECT MARKET": "NASDAQ",
    "NASDAQCM": "NASDAQ",
    "NASDAQGS": "NASDAQ",
    "NMS": "NASDAQ",
    "NGM": "NASDAQ",
    "NCM": "NASDAQ",
    "NASDAQ NMS": "NASDAQ",
    "NASDAQ NGM": "NASDAQ",
    "NASDAQ NCM": "NASDAQ",
}

DEFAULT_SCREENER_EXCHANGE_DISTRIBUTION_CSV: str = "screener_exchange_distribution.csv"
DEFAULT_STOCK_LIST_EXCHANGE_DISTRIBUTION_CSV: str = "stock_list_exchange_distribution.csv"
DEFAULT_JOIN_NO_MATCH_SAMPLES_CSV: str = "join_no_match_samples.csv"
DEFAULT_JOIN_SYMBOL_CANDIDATES_CSV: str = "join_symbol_candidates_in_stock_list.csv"

_JOIN_DIAGNOSTIC_TOP_N: int = 30
_JOIN_NO_MATCH_LOG_SAMPLES: int = 20
_JOIN_NO_MATCH_CSV_SAMPLES: int = 200


def _json_bool_flag(value: Any) -> bool:
    """스크리너 ``isEtf`` / ``isFund`` 등: 명시 True 만 True, 누락·애매하면 False."""
    if value is True:
        return True
    if value is False:
        return False
    if isinstance(value, str):
        return value.strip().lower() in ("true", "1", "yes")
    if isinstance(value, (int, np.integer)) and not isinstance(value, bool):
        return bool(value)
    if isinstance(value, float) and not isinstance(value, bool):
        if pd.isna(value):
            return False
        return bool(value)
    return False


def _json_optional_bool(value: Any) -> Optional[bool]:
    """``isActivelyTrading`` 등: True / False / None(미제공·해석 불가)."""
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        s = value.strip().lower()
        if s in ("true", "1", "yes"):
            return True
        if s in ("false", "0", "no"):
            return False
        return None
    if isinstance(value, (int, np.integer)) and not isinstance(value, bool):
        return bool(value)
    if isinstance(value, float) and not isinstance(value, bool):
        if pd.isna(value):
            return None
        return bool(value)
    return None


def _normalize_exchange_lookup_key(raw: Any) -> str:
    """
    동의어 매핑용 키: trim → `.,` 제거 → `-`/`/` → 공백 → 공백 압축 → UPPER.

    처음 보는 값은 매핑 실패 후 그대로 canonical 후보로 쓰이며, 분포 CSV에 남긴다.
    """
    if raw is None or (isinstance(raw, float) and pd.isna(raw)):
        return ""
    s = str(raw).strip()
    if not s:
        return ""
    s = s.replace(".", "").replace(",", "")
    s = s.replace("-", " ").replace("/", " ")
    s = re.sub(r"\s+", " ", s).strip().upper()
    return s


def normalize_exchange_code(raw: Any) -> str:
    """
    거래소 문자열을 **canonical exchange code** 로 맞춘다.

    * 입력은 ``exchangeShortName`` 또는 ``exchange`` 원문.
    * 표준화: trim, `.,` 제거, `-`/`/` → 공백, 연속 공백 1칸, UPPER.
    * 알려진 동의어는 NYSE / NASDAQ / AMEX 로 매핑.
    * 매핑 없으면 표준화된 문자열 그대로 반환(OTC 등 보존, 디버그 분포에 잡힘).

    qualify·거래소 화이트리스트는 이 함수 밖에서 처리한다.
    """
    key = _normalize_exchange_lookup_key(raw)
    if not key:
        return ""
    return _EXCHANGE_SYNONYM_TO_CANONICAL.get(key, key)


def exchange_distribution_dataframe(
    df: pd.DataFrame,
    raw_col: str,
    canonical_col: str,
) -> pd.DataFrame:
    """``exchange_raw`` / ``exchange_canonical`` 조합별 건수(리포트·로그용)."""
    cols_out = ["exchange_raw", "exchange_canonical", "count"]
    if df.empty or raw_col not in df.columns or canonical_col not in df.columns:
        return pd.DataFrame(columns=cols_out)
    g = (
        df.groupby([raw_col, canonical_col], dropna=False)
        .size()
        .reset_index(name="count")
        .sort_values("count", ascending=False)
        .reset_index(drop=True)
    )
    g = g.rename(columns={raw_col: "exchange_raw", canonical_col: "exchange_canonical"})
    return g[cols_out]


def normalize_type_token(raw: Any) -> Optional[str]:
    """
    stock-list API ``type`` 필드 전용 정규화.

    * 소문자, 앞뒤 공백 제거, 연속 공백은 단일 공백.
    * 부분 문자열 매칭·회사명·티커·suffix 추정 **금지**.

    비어 있으면 ``None`` (호출부에서 ``type_norm`` 을 \"\" 로 저장 가능).
    """
    if raw is None or (isinstance(raw, float) and pd.isna(raw)):
        return None
    s = str(raw).strip()
    if not s:
        return None
    s = s.lower()
    s = re.sub(r"\s+", " ", s)
    return s if s else None


def normalize_screener_rows(rows: List[Dict[str, Any]]) -> pd.DataFrame:
    """
    company-screener 원본 JSON 행 → ``SCREENER_CANDIDATE_COLUMNS`` DataFrame.

    * **type 컬럼을 넣지 않는다** (분류는 stock-list 만).
    * ``exchangeShortName`` 이 있으면 우선, 없으면 ``exchange`` 를 ``exchange_raw`` 로 사용.
    * 중복: ``(symbol, exchange_canonical)`` 첫 행만 유지.
    """
    if not rows:
        return pd.DataFrame(columns=SCREENER_CANDIDATE_COLUMNS)

    records: List[Dict[str, Any]] = []
    for r in rows:
        if not isinstance(r, dict):
            continue
        sym = (r.get("symbol") or "").strip().upper()
        if not sym:
            continue
        ex_short = r.get("exchangeShortName")
        ex_alt = r.get("exchange")
        if ex_short is not None and str(ex_short).strip() != "":
            exchange_raw = str(ex_short).strip()
        elif ex_alt is not None:
            exchange_raw = str(ex_alt).strip()
        else:
            exchange_raw = ""
        exchange_canonical = normalize_exchange_code(exchange_raw)
        name = r.get("companyName") if r.get("companyName") is not None else r.get("name")
        company_name = str(name).strip() if name is not None else ""
        mcap = r.get("marketCap")
        if mcap is None:
            mcap = r.get("mktCap")
        records.append({
            "symbol": sym,
            "company_name": company_name,
            "exchange_raw": exchange_raw,
            "exchange_canonical": exchange_canonical,
            "sector": str(r.get("sector") or "").strip(),
            "industry": str(r.get("industry") or "").strip(),
            "market_cap": mcap,
            "is_etf_flag": _json_bool_flag(r.get("isEtf")),
            "is_fund_flag": _json_bool_flag(r.get("isFund")),
            "is_actively_trading_flag": _json_optional_bool(r.get("isActivelyTrading")),
        })

    if not records:
        return pd.DataFrame(columns=SCREENER_CANDIDATE_COLUMNS)

    df = pd.DataFrame.from_records(records, columns=SCREENER_CANDIDATE_COLUMNS)
    n_before = len(df)
    df = df.drop_duplicates(subset=["symbol", "exchange_canonical"], keep="first").reset_index(drop=True)
    log.info(
        "normalize_screener_rows | input_dict_rows=%d parsed_rows=%d output_rows=%d "
        "| dedup=symbol+exchange_canonical keep=first dropped=%d",
        len(rows),
        n_before,
        len(df),
        n_before - len(df),
    )
    return df


def normalize_stock_list_rows(rows: List[Dict[str, Any]]) -> pd.DataFrame:
    """
    stock-list 원본 JSON 행 → ``STOCK_LIST_TYPE_COLUMNS`` DataFrame.

    * 여기서만 ``type_raw`` / ``type_norm`` 을 둔다.
    * ``exchangeShortName`` 우선, 없으면 ``exchange`` → ``exchange_raw`` / canonical.
    * 중복: ``(symbol, exchange_canonical)`` 첫 행만 유지.
    """
    if not rows:
        return pd.DataFrame(columns=STOCK_LIST_TYPE_COLUMNS)

    records: List[Dict[str, Any]] = []
    for r in rows:
        if not isinstance(r, dict):
            continue
        sym = (r.get("symbol") or "").strip().upper()
        if not sym:
            continue
        ex_short = r.get("exchangeShortName")
        ex_alt = r.get("exchange")
        if ex_short is not None and str(ex_short).strip() != "":
            exchange_raw = str(ex_short).strip()
        elif ex_alt is not None:
            exchange_raw = str(ex_alt).strip()
        else:
            exchange_raw = ""
        exchange_canonical = normalize_exchange_code(exchange_raw)
        tv = r.get("type")
        if tv is None or (isinstance(tv, float) and pd.isna(tv)):
            type_raw = ""
        else:
            type_raw = str(tv).strip()
        tnorm = normalize_type_token(tv)
        type_norm = tnorm if tnorm is not None else ""
        records.append({
            "symbol": sym,
            "exchange_raw": exchange_raw,
            "exchange_canonical": exchange_canonical,
            "type_raw": type_raw,
            "type_norm": type_norm,
        })

    if not records:
        return pd.DataFrame(columns=STOCK_LIST_TYPE_COLUMNS)

    df = pd.DataFrame.from_records(records, columns=STOCK_LIST_TYPE_COLUMNS)
    n_before = len(df)
    df = df.drop_duplicates(subset=["symbol", "exchange_canonical"], keep="first").reset_index(drop=True)
    log.info(
        "normalize_stock_list_rows | input_dict_rows=%d parsed_rows=%d output_rows=%d "
        "| dedup=symbol+exchange_canonical keep=first dropped=%d",
        len(rows),
        n_before,
        len(df),
        n_before - len(df),
    )
    return df


# =============================================================================
# 8. Stock-list lookup — symbol only (필수 통과문 아님)
# =============================================================================
#
# * 심볼이 stock-list 에 없거나 ``type`` 이 비면 ``type_*`` 는 빈 문자열 — **자동 탈락 없음**.
# * 동일 심볼에 행이 여러 개면 고유 ``type_norm`` / ``type_raw`` 를 정렬 후 ``STOCK_LIST_TYPE_NORM_SEP`` 로 합친다.
# * qualify 에서 토큰 **하나라도** ``TYPE_EXCLUSION_TOKENS`` 이면 ``EXCLUDED_TYPE``.


def _stock_list_nonempty_strings(s: pd.Series) -> Set[str]:
    out: Set[str] = set()
    for x in s:
        if x is None or (isinstance(x, float) and pd.isna(x)):
            continue
        t = str(x).strip()
        if t and t.lower() != "nan":
            out.add(t)
    return out


def _aggregate_stock_list_types_by_symbol(stock_list_df: pd.DataFrame) -> pd.DataFrame:
    """``symbol`` 당 고유 ``type_norm`` / ``type_raw`` 를 합친 1행."""
    sl = stock_list_df.copy()
    sl["symbol"] = sl["symbol"].astype(str).str.strip().str.upper()
    sep = STOCK_LIST_TYPE_NORM_SEP
    rows: List[Dict[str, str]] = []
    for sym, g in sl.groupby("symbol", sort=False):
        norms = _stock_list_nonempty_strings(g["type_norm"])
        raws = _stock_list_nonempty_strings(g["type_raw"])
        rows.append({
            "symbol": str(sym).strip().upper(),
            "type_norm": sep.join(sorted(norms)),
            "type_raw": sep.join(sorted(raws)),
        })
    return pd.DataFrame(rows)


def attach_stock_list_types_by_symbol(
    candidates_df: pd.DataFrame,
    stock_list_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Screener 후보에 stock-list ``type`` 을 **심볼 기준**으로만 붙인다.

    Returns:
        ``JOINED_QUALIFY_INPUT_COLUMNS`` 순서의 DataFrame.
    """
    out_cols = JOINED_QUALIFY_INPUT_COLUMNS

    if candidates_df.empty:
        return pd.DataFrame(columns=out_cols)

    missing_c = set(SCREENER_CANDIDATE_COLUMNS) - set(candidates_df.columns)
    if missing_c:
        raise ValueError(f"candidates_df missing columns: {sorted(missing_c)}")

    left = candidates_df[list(SCREENER_CANDIDATE_COLUMNS)].reset_index(drop=True).copy()

    if stock_list_df.empty:
        out = left.copy()
        out["type_raw"] = ""
        out["type_norm"] = ""
        log.info(
            "attach_stock_list_types_by_symbol | candidates=%d stock_list=empty (types left blank)",
            len(out),
        )
        return out[out_cols]

    missing_s = {"symbol", "type_raw", "type_norm"} - set(stock_list_df.columns)
    if missing_s:
        raise ValueError(f"stock_list_df missing columns: {sorted(missing_s)}")

    agg = _aggregate_stock_list_types_by_symbol(stock_list_df)
    out = left.merge(agg, on="symbol", how="left")
    out["type_raw"] = out["type_raw"].fillna("").map(
        lambda v: "" if str(v).strip().lower() == "nan" else str(v).strip(),
    )
    out["type_norm"] = out["type_norm"].fillna("").map(
        lambda v: "" if str(v).strip().lower() == "nan" else str(v).strip(),
    )

    sl_sym = set(agg["symbol"].astype(str).str.strip().str.upper())
    sym_u = out["symbol"].astype(str).str.strip().str.upper()
    n_hit = int(sym_u.isin(sl_sym).sum())
    esc = re.escape(STOCK_LIST_TYPE_NORM_SEP)
    n_multi = int(out["type_norm"].str.contains(esc, regex=True).sum())
    log.info(
        "attach_stock_list_types_by_symbol | candidates=%d symbols_with_stock_list_row=%d "
        "candidates_with_nonempty_type=%d candidates_with_multi_type_token=%d",
        len(out),
        n_hit,
        int((out["type_norm"].str.strip() != "").sum()),
        n_multi,
    )
    return out[out_cols].reset_index(drop=True)


def join_candidates_with_type_ref(
    candidates_df: pd.DataFrame,
    stock_list_df: pd.DataFrame,
) -> pd.DataFrame:
    """:func:`attach_stock_list_types_by_symbol` 별칭(호환)."""
    return attach_stock_list_types_by_symbol(candidates_df, stock_list_df)


# =============================================================================
# 9. Qualify — 순차 제외(스크리너 게이트 → 선택적 stock-list 타입 제외)
# =============================================================================
#
# * stock-list 없음·빈 ``type`` 은 **통과**(앞 단계 게이트만 만족하면).
# * ``TYPE_EXCLUSION_TOKENS`` 에 **토큰 정확 일치**할 때만 ``EXCLUDED_TYPE``.
# * REIT 는 제외 집합에 없음. ADR 별도 처리 없음.


def _qualify_row_get(row: Any, key: str, default: Any = None) -> Any:
    if isinstance(row, pd.Series):
        return row[key] if key in row.index else default
    if isinstance(row, dict):
        return row.get(key, default)
    return getattr(row, key, default)


def _row_market_cap_float(row: Any) -> float:
    v = _qualify_row_get(row, "market_cap")
    try:
        return float(v)
    except (TypeError, ValueError):
        return float("nan")


def _is_strict_true(v: Any) -> bool:
    """스크리너 ``is_actively_trading`` 등: Python ``True`` / ``numpy.bool_`` 참 만 활성."""
    if v is True:
        return True
    if isinstance(v, np.bool_):
        return bool(v)
    return False


def _is_etf_or_fund_true(v: Any) -> bool:
    """``is_etf_flag`` / ``is_fund_flag`` 가 명시적으로 참인지."""
    return _is_strict_true(v)


def is_us_listed(row: Any, allowed_exchanges: Sequence[str]) -> bool:
    """
    ``exchange_canonical`` 이 ``allowed_exchanges``(대소문자 무시·trim) 중 하나인지.
    """
    allowed = {str(x).strip().upper() for x in allowed_exchanges if str(x).strip()}
    ex = str(_qualify_row_get(row, "exchange_canonical", "") or "").strip().upper()
    return bool(ex) and ex in allowed


def is_excluded_type(type_norm: str) -> bool:
    """
    ``type_norm`` 이 비어 있지 않고 ``TYPE_EXCLUSION_TOKENS`` 에 **exact** 로 들어있는지.

    회사명·티커·suffix 는 사용하지 않는다.
    """
    t = (type_norm or "").strip()
    return bool(t) and t in TYPE_EXCLUSION_TOKENS


def type_norm_cell_tokens(cell: Any) -> List[str]:
    """부착된 ``type_norm`` 셀을 ``STOCK_LIST_TYPE_NORM_SEP`` 기준 토큰 리스트로 분해."""
    s = str(cell or "").strip()
    if not s or s.lower() == "nan":
        return []
    return [p.strip() for p in s.split(STOCK_LIST_TYPE_NORM_SEP) if p.strip()]


def any_attached_type_is_excluded(type_norm_cell: Any) -> bool:
    """합쳐진 ``type_norm`` 중 **어느 하나라도** 제외 토큰이면 True."""
    return any(is_excluded_type(t) for t in type_norm_cell_tokens(type_norm_cell))


def universe_row_reason_code(
    row: Any,
    min_market_cap: int,
    allowed_exchanges: Sequence[str],
) -> str:
    """
    한 행에 대한 PASS 또는 제외 사유 코드(순차 제외).

    입력: ``attach_stock_list_types_by_symbol`` / ``join_candidates_with_type_ref`` 출력.
    stock-list 미존재·빈 ``type`` 은 탈락 사유가 **아님**.

    순서: 거래소 → 활성 → 시총 → FUND → ETF → (type 있으면) 제외 토큰 → PASS.
    """
    if not is_us_listed(row, allowed_exchanges):
        return REASON_NOT_US_LISTED

    if not _is_strict_true(_qualify_row_get(row, "is_actively_trading_flag", None)):
        return REASON_INACTIVE

    mcap = _row_market_cap_float(row)
    if pd.isna(mcap) or mcap < min_market_cap:
        return REASON_BELOW_MARKET_CAP

    if _is_etf_or_fund_true(_qualify_row_get(row, "is_fund_flag", False)):
        return REASON_FUND

    if _is_etf_or_fund_true(_qualify_row_get(row, "is_etf_flag", False)):
        return REASON_ETF

    tnorm_cell = _qualify_row_get(row, "type_norm", "")
    tokens = type_norm_cell_tokens(tnorm_cell)
    if tokens:
        if any(is_excluded_type(t) for t in tokens):
            return REASON_EXCLUDED_TYPE

    return REASON_PASS


def _qualify_build_summary(reason_codes: pd.Series, n_total: int) -> pd.DataFrame:
    vc = reason_codes.value_counts().sort_values(ascending=False)
    summary = vc.reset_index()
    summary.columns = ["reason_code", "count"]
    summary["share_pct"] = (summary["count"] / n_total * 100).round(4)
    return summary


def compute_qualify_reason_codes(
    joined_df: pd.DataFrame,
    min_market_cap: int,
    allowed_exchanges: Sequence[str],
) -> pd.Series:
    """``joined_df`` 각 행에 ``universe_row_reason_code`` 적용(인덱스 0..n-1)."""
    if joined_df.empty:
        return pd.Series(dtype=object)
    missing = set(JOINED_QUALIFY_INPUT_COLUMNS) - set(joined_df.columns)
    if missing:
        raise ValueError(f"joined_df missing columns: {sorted(missing)}")
    work = joined_df[list(JOINED_QUALIFY_INPUT_COLUMNS)].copy().reset_index(drop=True)
    allowed_seq = tuple(allowed_exchanges)
    return work.apply(
        lambda r: universe_row_reason_code(r, min_market_cap, allowed_seq),
        axis=1,
    )


def universe_save_column_order(include_debug: bool) -> List[str]:
    """디스크 저장·``finalize_universe_for_save`` 열 순서(핵심 + 선택 디버그). ``is_adr`` 없음."""
    if include_debug:
        return list(FINAL_COLUMNS) + list(UNIVERSE_DEBUG_COLUMNS)
    return list(FINAL_COLUMNS)


def materialize_qualified_output_df(
    passed_df: pd.DataFrame,
    *,
    include_debug: bool = False,
) -> pd.DataFrame:
    """
    qualify PASS 행 → ``FINAL_COLUMNS``(+선택 디버그) 5개 핵심 필드만 물질화.

    ``is_actively_trading`` 는 유니버스에 포함된 행이므로 항상 True 로 둔다.
    """
    cols = universe_save_column_order(include_debug)
    if passed_df.empty:
        return pd.DataFrame(columns=cols)

    out = pd.DataFrame()
    out["symbol"] = passed_df["symbol"].astype(str).str.strip().str.upper()
    out["company_name"] = passed_df["company_name"].fillna("").astype(str)
    out["sector"] = passed_df["sector"].fillna("").astype(str) if "sector" in passed_df.columns else ""
    out["industry"] = passed_df["industry"].fillna("").astype(str) if "industry" in passed_df.columns else ""
    out["is_actively_trading"] = True

    if include_debug:
        out["qualifies"] = True
        out["reason_code"] = REASON_PASS

    return out[cols].reset_index(drop=True)


def _coerce_cell_to_bool(v: Any) -> bool:
    if v is True or v is False:
        return bool(v)
    if isinstance(v, (np.bool_,)):
        return bool(v)
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return False
    s = str(v).strip().lower()
    if s in ("true", "1", "yes", "t"):
        return True
    return False


def finalize_universe_for_save(df: pd.DataFrame, *, include_debug: bool) -> pd.DataFrame:
    """
    저장 직전: 허용 열만 남기고 dtype 정리.

    * 문자열: symbol, company_name, sector, industry, reason_code
    * bool: is_actively_trading(통과 행은 True), qualifies(디버그)
    """
    order = universe_save_column_order(include_debug)
    if df.empty:
        return pd.DataFrame(columns=order)

    out = df.copy()
    drop_legacy = [c for c in LEGACY_UNIVERSE_COLUMNS_DROP if c in out.columns]
    if drop_legacy:
        out = out.drop(columns=drop_legacy)
        log.info("finalize_universe_for_save: dropped legacy columns: %s", ", ".join(drop_legacy))

    allowed = set(order)
    extra = [c for c in out.columns if c not in allowed]
    if extra:
        out = out.drop(columns=extra)

    for c in order:
        if c not in out.columns:
            if c == "is_actively_trading":
                out[c] = True
            elif c == "qualifies":
                out[c] = False
            else:
                out[c] = ""

    str_cols = ("symbol", "company_name", "sector", "industry", "reason_code")
    for sc in str_cols:
        if sc in out.columns:
            out[sc] = out[sc].fillna("").astype(str)

    out["symbol"] = out["symbol"].astype(str).str.strip().str.upper()

    if "is_actively_trading" in out.columns:
        out["is_actively_trading"] = True

    if "qualifies" in out.columns:
        out["qualifies"] = out["qualifies"].map(_coerce_cell_to_bool).astype(bool)

    return out[order].reset_index(drop=True)


def qualify_universe(
    joined_df: pd.DataFrame,
    min_market_cap: int,
    allowed_exchanges: Sequence[str],
    *,
    include_debug: bool = False,
    reason_codes: Optional[pd.Series] = None,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    exclusion-based qualify. 화이트리스트로 타입을 통과시키지 않는다.

    ``reason_codes`` 가 있으면 동일 길이의 사전 계산 시리즈를 사용(``compute_qualify_reason_codes``).

    Returns:
        (qualified_output_df, summary_df). summary: reason_code, count, share_pct.
    """
    empty_summary = pd.DataFrame(columns=["reason_code", "count", "share_pct"])
    if joined_df.empty:
        return materialize_qualified_output_df(pd.DataFrame(), include_debug=include_debug), empty_summary

    missing = set(JOINED_QUALIFY_INPUT_COLUMNS) - set(joined_df.columns)
    if missing:
        raise ValueError(f"joined_df missing columns: {sorted(missing)}")

    work = joined_df[list(JOINED_QUALIFY_INPUT_COLUMNS)].copy().reset_index(drop=True)
    if reason_codes is None:
        codes = compute_qualify_reason_codes(joined_df, min_market_cap, allowed_exchanges)
    else:
        codes = reason_codes.reset_index(drop=True)
        if len(codes) != len(work):
            raise ValueError("reason_codes length must match joined_df row count")

    n_total = len(work)
    passed_mask = codes == REASON_PASS
    summary_df = _qualify_build_summary(codes, n_total)

    qualified_df = materialize_qualified_output_df(
        work.loc[passed_mask].reset_index(drop=True),
        include_debug=include_debug,
    )
    return qualified_df, summary_df


# =============================================================================
# 10. Merge / snapshot / scheduled — 제거됨
# =============================================================================
# qualify 통과 결과를 ``finalize_universe_for_save`` 후 즉시 CSV/Parquet 저장.


# =============================================================================
# 11. Debug reports
# =============================================================================


def _debug_reports_ensure_dir(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def _write_qualify_reason_summary_csv(path: Path, summary_df: pd.DataFrame) -> int:
    """``reason_code``, ``count``, ``share_pct``. 빈 입력도 헤더만 저장."""
    cols = ["reason_code", "count", "share_pct"]
    _debug_reports_ensure_dir(path)
    if summary_df is None or summary_df.empty:
        pd.DataFrame(columns=cols).to_csv(path, index=False)
        return 0
    out = summary_df.copy()
    for c in cols:
        if c not in out.columns:
            raise ValueError(f"qualify summary missing column {c!r}")
    out[cols].to_csv(path, index=False)
    return len(out)


def _write_stock_list_type_distribution_csv(path: Path, stock_list_df: pd.DataFrame) -> int:
    """정규화 stock-list 기준 ``type_norm``/``type_raw`` 조합 분포."""
    cols = ["type_norm", "type_raw", "count", "share_pct"]
    _debug_reports_ensure_dir(path)
    if stock_list_df.empty:
        pd.DataFrame(columns=cols).to_csv(path, index=False)
        return 0
    need = {"type_norm", "type_raw"}
    if not need.issubset(stock_list_df.columns):
        raise ValueError(f"stock_list_df missing columns: {sorted(need - set(stock_list_df.columns))}")
    n = len(stock_list_df)
    g = (
        stock_list_df.groupby(["type_norm", "type_raw"], dropna=False)
        .size()
        .reset_index(name="count")
        .sort_values("count", ascending=False)
    )
    g["share_pct"] = (g["count"] / n * 100).round(4)
    g.to_csv(path, index=False)
    return len(g)


def _compute_reason_codes_if_needed(
    joined_df: pd.DataFrame,
    min_market_cap: int,
    allowed_exchanges: Sequence[str],
    reason_codes: Optional[pd.Series],
) -> pd.Series:
    if joined_df.empty:
        return pd.Series(dtype=object)
    if reason_codes is not None:
        rc = reason_codes.reset_index(drop=True)
        if len(rc) != len(joined_df):
            raise ValueError("reason_codes length must match joined_df row count")
        return rc
    allowed_seq = tuple(allowed_exchanges)
    work = joined_df.reset_index(drop=True)
    return work.apply(
        lambda r: universe_row_reason_code(r, min_market_cap, allowed_seq),
        axis=1,
    )


def _debug_joined_with_reasons(joined_df: pd.DataFrame, reason_codes: pd.Series) -> pd.DataFrame:
    """조인 출력 + ``reason_code`` + 리포트용 ``exchange``(= canonical) 열."""
    if len(reason_codes) != len(joined_df):
        raise ValueError("reason_codes length must match joined_df")
    work = joined_df.reset_index(drop=True).copy()
    work["reason_code"] = reason_codes.astype(str).values
    work["exchange"] = work["exchange_canonical"].fillna("").astype(str).str.strip().str.upper()
    work["security_type"] = work["type_norm"].fillna("").astype(str).str.strip()
    return work


def _write_unknown_nonexcluded_type_symbols_csv(path: Path, work: pd.DataFrame) -> int:
    """
    ``PASS`` 이면서 stock-list 에서 부착된 ``type_norm`` 토큰이 있고, **어느 토큰도** 제외 집합이 아닌 행.

    운영 감사용(미등록 타입 문자열 포함).
    """
    cols = ["symbol", "exchange", "company_name", "security_type", "reason_code"]
    _debug_reports_ensure_dir(path)
    if work.empty:
        pd.DataFrame(columns=cols).to_csv(path, index=False)
        return 0

    def _pass_nonempty_no_exclusion(row: pd.Series) -> bool:
        if str(row.get("reason_code", "")).strip() != REASON_PASS:
            return False
        toks = type_norm_cell_tokens(row.get("type_norm", ""))
        if not toks:
            return False
        return not any(is_excluded_type(t) for t in toks)

    mask = work.apply(_pass_nonempty_no_exclusion, axis=1)
    sub = work.loc[mask, cols].copy()
    sub = sub.sort_values(["security_type", "symbol", "exchange"]).reset_index(drop=True)
    sub.to_csv(path, index=False)
    return len(sub)


def _write_missing_type_symbols_csv(
    path: Path,
    work: pd.DataFrame,
    stock_list_df: pd.DataFrame,
) -> int:
    """스크리너 후보 심볼이 stock-list 에 **한 행도 없는** 경우(타입 부착 불가)."""
    cols = ["symbol", "exchange", "company_name", "reason_code"]
    _debug_reports_ensure_dir(path)
    if work.empty:
        pd.DataFrame(columns=cols).to_csv(path, index=False)
        return 0
    if stock_list_df.empty:
        sub = work[cols].copy()
        sub.to_csv(path, index=False)
        return len(sub)
    sl_syms = set(stock_list_df["symbol"].astype(str).str.strip().str.upper())
    sym_u = work["symbol"].astype(str).str.strip().str.upper()
    sub = work.loc[~sym_u.isin(sl_syms), cols].copy()
    sub = sub.sort_values(["symbol", "exchange"]).reset_index(drop=True)
    sub.to_csv(path, index=False)
    return len(sub)


def _write_join_conflict_symbols_csv(path: Path, work: pd.DataFrame) -> int:
    """stock-list 에서 여러 ``type_norm`` 토큰이 합쳐진 후보(``STOCK_LIST_TYPE_NORM_SEP`` 포함)."""
    cols = ["symbol", "exchange", "company_name", "security_type", "reason_code"]
    _debug_reports_ensure_dir(path)
    if work.empty:
        pd.DataFrame(columns=cols).to_csv(path, index=False)
        return 0
    esc = re.escape(STOCK_LIST_TYPE_NORM_SEP)
    mask = work["type_norm"].fillna("").astype(str).str.contains(esc, regex=True)
    sub = work.loc[mask, cols].copy()
    sub = sub.sort_values(["symbol", "exchange"]).reset_index(drop=True)
    sub.to_csv(path, index=False)
    return len(sub)


def _build_candidates_no_stock_list_symbol_sample_df(
    joined_df: pd.DataFrame,
    screener_df: pd.DataFrame,
    stock_list_df: pd.DataFrame,
    limit: int,
) -> pd.DataFrame:
    """stock-list 에 심볼 자체가 없는 후보 샘플(진단용)."""
    cols = ["symbol", "company_name", "screener_exchange_raw", "screener_exchange_canonical"]
    if joined_df.empty or stock_list_df.empty:
        return pd.DataFrame(columns=cols)
    sl_syms = set(stock_list_df["symbol"].astype(str).str.strip().str.upper())
    sym_u = joined_df["symbol"].astype(str).str.strip().str.upper()
    miss = joined_df.loc[~sym_u.isin(sl_syms)].head(limit)
    if miss.empty:
        return pd.DataFrame(columns=cols)
    base = miss[["symbol", "company_name", "exchange_canonical"]].copy()
    skey = screener_df[["symbol", "exchange_canonical", "exchange_raw"]].drop_duplicates(
        subset=["symbol", "exchange_canonical"],
        keep="first",
    )
    merged = base.merge(
        skey.rename(columns={"exchange_raw": "screener_exchange_raw"}),
        on=["symbol", "exchange_canonical"],
        how="left",
    )
    merged["screener_exchange_raw"] = merged["screener_exchange_raw"].fillna("").astype(str)
    merged = merged.rename(columns={"exchange_canonical": "screener_exchange_canonical"})
    return merged[cols].reset_index(drop=True)


def _build_excluded_type_symbol_stock_list_rows_df(
    stock_list_df: pd.DataFrame,
    joined_df: pd.DataFrame,
    max_symbols: int,
) -> pd.DataFrame:
    """``EXCLUDED_TYPE`` 로 떨어진 심볼 일부에 대한 stock-list 원시 행(참조)."""
    cols = ["symbol", "stock_list_exchange_raw", "stock_list_exchange_canonical", "type_raw", "type_norm"]
    if stock_list_df.empty or joined_df.empty:
        return pd.DataFrame(columns=cols)
    ex_mask = joined_df.apply(
        lambda r: any_attached_type_is_excluded(r.get("type_norm", "")),
        axis=1,
    )
    u = (
        joined_df.loc[ex_mask, "symbol"].astype(str).str.strip().str.upper().drop_duplicates().head(max_symbols).tolist()
    )
    if not u:
        return pd.DataFrame(columns=cols)
    sym_set = set(u)
    sub = stock_list_df[
        stock_list_df["symbol"].astype(str).str.strip().str.upper().isin(sym_set)
    ].copy()
    sub = sub.rename(columns={
        "exchange_raw": "stock_list_exchange_raw",
        "exchange_canonical": "stock_list_exchange_canonical",
    })
    return sub[cols].sort_values(["symbol", "stock_list_exchange_canonical"]).reset_index(drop=True)


def write_join_and_exchange_diagnostic_csvs(
    data_dir: Path,
    *,
    screener_df: pd.DataFrame,
    stock_list_df: pd.DataFrame,
    joined_df: pd.DataFrame,
) -> None:
    """
    거래소 분포 + stock-list 에 심볼 없는 후보 샘플 + 타입 제외 심볼의 stock-list 행 샘플.

    조인 실패로 PASS 가 0이 되는 구조는 없음 — 참고용 진단.
    """
    data_dir = Path(data_dir)
    data_dir.mkdir(parents=True, exist_ok=True)

    p_se = data_dir / DEFAULT_SCREENER_EXCHANGE_DISTRIBUTION_CSV
    d_se = exchange_distribution_dataframe(screener_df, "exchange_raw", "exchange_canonical")
    _debug_reports_ensure_dir(p_se)
    d_se.to_csv(p_se, index=False)
    log.info("debug-report: wrote %s (%d rows)", p_se.name, len(d_se))

    p_sle = data_dir / DEFAULT_STOCK_LIST_EXCHANGE_DISTRIBUTION_CSV
    d_sle = exchange_distribution_dataframe(stock_list_df, "exchange_raw", "exchange_canonical")
    _debug_reports_ensure_dir(p_sle)
    d_sle.to_csv(p_sle, index=False)
    log.info("debug-report: wrote %s (%d rows)", p_sle.name, len(d_sle))

    p_nm = data_dir / DEFAULT_JOIN_NO_MATCH_SAMPLES_CSV
    d_nm = _build_candidates_no_stock_list_symbol_sample_df(
        joined_df, screener_df, stock_list_df, _JOIN_NO_MATCH_CSV_SAMPLES,
    )
    _debug_reports_ensure_dir(p_nm)
    d_nm.to_csv(p_nm, index=False)
    log.info("debug-report: wrote %s (%d rows)", p_nm.name, len(d_nm))

    p_cand = data_dir / DEFAULT_JOIN_SYMBOL_CANDIDATES_CSV
    d_cand = _build_excluded_type_symbol_stock_list_rows_df(
        stock_list_df, joined_df, max_symbols=max(100, _JOIN_NO_MATCH_CSV_SAMPLES),
    )
    _debug_reports_ensure_dir(p_cand)
    d_cand.to_csv(p_cand, index=False)
    log.info("debug-report: wrote %s (%d rows)", p_cand.name, len(d_cand))


def log_qualify_reason_breakdown(reason_codes: pd.Series) -> None:
    """PASS 0 등 원인 파악용 reason_code 건수 요약 로그(핵심 7코드)."""
    if reason_codes is None or len(reason_codes) == 0:
        log.info("qualify breakdown | (no candidate rows)")
        return
    vc = reason_codes.astype(str).value_counts()

    def _cnt(code: str) -> int:
        return int(vc.get(code, 0))

    log.info(
        "qualify breakdown | total=%d PASS=%d FUND=%d ETF=%d EXCLUDED_TYPE=%d "
        "NOT_US_LISTED=%d BELOW_MARKET_CAP=%d INACTIVE=%d",
        len(reason_codes),
        _cnt(REASON_PASS),
        _cnt(REASON_FUND),
        _cnt(REASON_ETF),
        _cnt(REASON_EXCLUDED_TYPE),
        _cnt(REASON_NOT_US_LISTED),
        _cnt(REASON_BELOW_MARKET_CAP),
        _cnt(REASON_INACTIVE),
    )


def write_pass_zero_forced_diagnostic_csvs(
    data_dir: Path,
    *,
    screener_df: pd.DataFrame,
    stock_list_df: pd.DataFrame,
    joined_df: pd.DataFrame,
    reason_summary_df: pd.DataFrame,
) -> None:
    """
    ``--debug-type-report`` 가 꺼져 있어도 PASS 0 시 조인·거래소 원인 분석용 CSV 를 강제 저장.

    ``write_debug_reports`` 가 이미 호출된 경우(전체 리포트)에는 중복 호출하지 않는다.
    """
    data_dir = Path(data_dir)
    data_dir.mkdir(parents=True, exist_ok=True)
    write_join_and_exchange_diagnostic_csvs(
        data_dir,
        screener_df=screener_df,
        stock_list_df=stock_list_df,
        joined_df=joined_df,
    )
    p_summary = data_dir / DEFAULT_QUALIFY_REASON_SUMMARY_CSV
    n_sum = _write_qualify_reason_summary_csv(p_summary, reason_summary_df)
    log.info(
        "pass-zero forced diagnostics: wrote join/exchange CSVs + %s (%d rows)",
        p_summary.name,
        n_sum,
    )


def write_debug_reports(
    data_dir: Path,
    *,
    screener_df: pd.DataFrame,
    stock_list_df: pd.DataFrame,
    joined_df: pd.DataFrame,
    reason_summary_df: pd.DataFrame,
    min_market_cap: int,
    allowed_exchanges: Sequence[str],
    reason_codes: Optional[pd.Series] = None,
) -> None:
    """``--debug-type-report``: 거래소·조인·타입·qualify 진단 CSV 전체."""
    data_dir = Path(data_dir)
    data_dir.mkdir(parents=True, exist_ok=True)

    write_join_and_exchange_diagnostic_csvs(
        data_dir,
        screener_df=screener_df,
        stock_list_df=stock_list_df,
        joined_df=joined_df,
    )

    p_summary = data_dir / DEFAULT_QUALIFY_REASON_SUMMARY_CSV
    n_sum = _write_qualify_reason_summary_csv(p_summary, reason_summary_df)
    log.info("debug-report: wrote %s (%d rows)", p_summary.name, n_sum)

    p_dist = data_dir / DEFAULT_STOCK_LIST_TYPE_DISTRIBUTION_CSV
    n_dist = _write_stock_list_type_distribution_csv(p_dist, stock_list_df)
    log.info("debug-report: wrote %s (%d rows)", p_dist.name, n_dist)

    codes = _compute_reason_codes_if_needed(
        joined_df, min_market_cap, allowed_exchanges, reason_codes,
    )
    if joined_df.empty:
        work = pd.DataFrame(columns=list(JOINED_QUALIFY_INPUT_COLUMNS) + ["reason_code", "exchange", "security_type"])
    else:
        work = _debug_joined_with_reasons(joined_df, codes)

    p_unk = data_dir / DEFAULT_UNKNOWN_NONEXCLUDED_TYPE_SYMBOLS_CSV
    n_unk = _write_unknown_nonexcluded_type_symbols_csv(p_unk, work)
    log.info("debug-report: wrote %s (%d rows)", p_unk.name, n_unk)

    p_miss = data_dir / DEFAULT_MISSING_TYPE_SYMBOLS_CSV
    n_miss = _write_missing_type_symbols_csv(p_miss, work, stock_list_df)
    log.info("debug-report: wrote %s (%d rows)", p_miss.name, n_miss)

    p_conf = data_dir / DEFAULT_JOIN_CONFLICT_SYMBOLS_CSV
    n_conf = _write_join_conflict_symbols_csv(p_conf, work)
    log.info("debug-report: wrote %s (%d rows)", p_conf.name, n_conf)



# =============================================================================
# 12. Save
# =============================================================================


def save_csv(df: pd.DataFrame, path: Path) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)
    log.info("save_csv: %s rows=%d", path, len(df))


def save_parquet(df: pd.DataFrame, path: Path) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        df.to_parquet(path, index=False, engine="pyarrow")
    except ImportError:
        try:
            df.to_parquet(path, index=False, engine="fastparquet")
        except ImportError:
            log.error(
                "save_parquet: pyarrow 또는 fastparquet 필요. pip install pyarrow 권장."
            )
            return
    except Exception as e:
        log.error("save_parquet failed: %s", e)
        return
    log.info("save_parquet: %s rows=%d", path, len(df))


def save_universe_to_disk(df: pd.DataFrame, csv_path: Path, parquet_path: Path) -> None:
    save_csv(df, csv_path)
    save_parquet(df, parquet_path)


# =============================================================================
# 13. Orchestration
# =============================================================================


def run_pipeline(args: argparse.Namespace) -> int:
    """
    fetch → normalize → optional stock-list type attach → qualify → (debug) → save.

    **해당 시점** 유니버스 스냅샷만 저장(merge·상태 이력 없음). 성공 0, PASS 0건 등 중단 시 1.
    """
    api_key = (os.environ.get(API_KEY_ENV) or "").strip()
    if not api_key:
        log.error("환경변수 %s 가 설정되지 않았습니다.", API_KEY_ENV)
        return 1

    data_dir = Path(args.data_dir)
    csv_path = data_dir / args.out_current_csv
    parquet_path = data_dir / args.out_current_parquet
    exchanges = list(SCREENER_EXCHANGES)

    log.info(
        "pipeline start | min_mcap=%s country=%s data_dir=%s",
        f"{args.min_market_cap:,}",
        args.country,
        data_dir,
    )

    sess = _make_session()
    try:
        log.info("step 1: fetch company-screener")
        raw_screener = fetch_company_screener_rows(
            api_key,
            args.min_market_cap,
            args.country,
            exchanges,
            sess,
        )
        log.info("screener raw json rows=%d", len(raw_screener))
        if not raw_screener:
            log.error("company-screener 응답이 비었습니다.")
            return 1

        log.info("step 2: normalize screener candidates")
        screener_df = normalize_screener_rows(raw_screener)
        log.info("screener normalized rows=%d", len(screener_df))
        if screener_df.empty:
            log.error("screener normalize 후 0건입니다.")
            return 1

        log.info("step 3: fetch stock-list")
        raw_sl = fetch_stock_list_rows(api_key, sess)
        log.info("stock-list raw json rows=%d", len(raw_sl))
        if not raw_sl:
            log.warning("stock-list 응답이 비었습니다 — type 부착 없이 스크리너 후보만 qualify 합니다.")
            stock_list_df = pd.DataFrame(columns=STOCK_LIST_TYPE_COLUMNS)
        else:
            log.info("step 4: normalize stock-list")
            stock_list_df = normalize_stock_list_rows(raw_sl)
            log.info("stock-list normalized rows=%d", len(stock_list_df))

        log.info("step 5: attach stock-list types by symbol (optional)")
        joined_df = attach_stock_list_types_by_symbol(screener_df, stock_list_df)
        log.info("candidates with types attached rows=%d", len(joined_df))

        log.info("step 6: compute qualify reason codes")
        reason_codes = compute_qualify_reason_codes(
            joined_df, args.min_market_cap, exchanges,
        )

        log.info("step 7: qualify")
        qualified_df, qualify_summary = qualify_universe(
            joined_df,
            args.min_market_cap,
            exchanges,
            include_debug=args.keep_debug_columns,
            reason_codes=reason_codes,
        )

        if args.debug_type_report:
            log.info("step 8a: debug-type-report (full diagnostic CSVs)")
            write_debug_reports(
                data_dir,
                screener_df=screener_df,
                stock_list_df=stock_list_df,
                joined_df=joined_df,
                reason_summary_df=qualify_summary,
                min_market_cap=args.min_market_cap,
                allowed_exchanges=exchanges,
                reason_codes=reason_codes,
            )

        if args.debug_qualify_report is not None:
            if args.debug_qualify_report == DEBUG_QUALIFY_REPORT_DEFAULT:
                qpath = data_dir / DEFAULT_QUALIFY_REASON_SUMMARY_CSV
            else:
                qpath = Path(args.debug_qualify_report)
            qpath.parent.mkdir(parents=True, exist_ok=True)
            qualify_summary.to_csv(qpath, index=False)
            log.info("step 8b: debug-qualify-report → %s (%d rows)", qpath, len(qualify_summary))

        if qualified_df.empty:

            def _rc0(code: str) -> int:
                return int((reason_codes == code).sum())

            n_tot0 = len(joined_df)
            log.info(
                "universe snapshot | total_screener_candidates=%d removed_FUND=%d removed_ETF=%d "
                "removed_EXCLUDED_TYPE=%d PASS=%d",
                n_tot0,
                _rc0(REASON_FUND),
                _rc0(REASON_ETF),
                _rc0(REASON_EXCLUDED_TYPE),
                _rc0(REASON_PASS),
            )
            log.info("universe saved | (skipped — PASS=0, no CSV/Parquet written)")
            log_qualify_reason_breakdown(reason_codes)
            log.warning(
                "PASS 0건 — 유니버스 CSV/Parquet 저장 생략. 원인은 스크리너 게이트 또는 "
                "EXCLUDED_TYPE 위주로 qualify breakdown·요약 CSV 를 확인하세요.",
            )
            if not args.debug_type_report:
                write_pass_zero_forced_diagnostic_csvs(
                    data_dir,
                    screener_df=screener_df,
                    stock_list_df=stock_list_df,
                    joined_df=joined_df,
                    reason_summary_df=qualify_summary,
                )
                log.info(
                    "PASS 0 강제 진단: ``--debug-type-report`` 가 꺼져 있어 "
                    "``screener_exchange_distribution.csv``, "
                    "``stock_list_exchange_distribution.csv``, "
                    "``join_no_match_samples.csv`` (stock-list 에 심볼 없는 후보 샘플), "
                    "``join_symbol_candidates_in_stock_list.csv`` (타입 제외 심볼의 목록 행 샘플), "
                    "``qualify_reason_summary.csv`` 를 data-dir 에 저장했습니다.",
                )
            else:
                log.info(
                    "PASS 0: ``--debug-type-report`` 로 전체 디버그 CSV 가 이미 data-dir 에 기록됨 "
                    "(join/거래소 CSV 중복 강제 저장 생략).",
                )
            return 1

        log.info("step 9: finalize and save universe list")
        result_df = finalize_universe_for_save(
            qualified_df,
            include_debug=args.keep_debug_columns,
        )
        save_universe_to_disk(result_df, csv_path, parquet_path)

        def _rc(code: str) -> int:
            return int((reason_codes == code).sum())

        n_tot = len(joined_df)
        log.info(
            "universe snapshot | total_screener_candidates=%d removed_FUND=%d removed_ETF=%d "
            "removed_EXCLUDED_TYPE=%d PASS=%d",
            n_tot,
            _rc(REASON_FUND),
            _rc(REASON_ETF),
            _rc(REASON_EXCLUDED_TYPE),
            int((reason_codes == REASON_PASS).sum()),
        )
        log.info(
            "universe saved | csv=%s parquet=%s rows=%d",
            csv_path,
            parquet_path,
            len(result_df),
        )
    finally:
        sess.close()

    return 0


def _parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description=(
            "FMP universe v2: point-in-time universe list (CSV/Parquet). "
            "No merge/status/history — screener pool + sequential exclusions + optional stock-list types."
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    ap.add_argument(
        "--data-dir",
        type=str,
        default="./data",
        help="출력 디렉터리(유니버스, 디버그 CSV).",
    )
    ap.add_argument(
        "--min-market-cap",
        type=int,
        default=DEFAULT_MIN_MARKET_CAP,
        help="최소 시가총액(USD). 스크리너·qualify 동일 기준.",
    )
    ap.add_argument(
        "--country",
        type=str,
        default="US",
        help="company-screener country 파라미터.",
    )
    ap.add_argument(
        "--out-current-csv",
        type=str,
        default="universe_current.csv",
        help="유니버스 CSV 파일명(--data-dir 기준 상대 경로).",
    )
    ap.add_argument(
        "--out-current-parquet",
        type=str,
        default="universe_current.parquet",
        help="유니버스 Parquet 파일명(--data-dir 기준 상대 경로).",
    )
    ap.add_argument(
        "--keep-debug-columns",
        action="store_true",
        help="유니버스에 qualifies, reason_code 포함.",
    )
    ap.add_argument(
        "--debug-qualify-report",
        nargs="?",
        const=DEBUG_QUALIFY_REPORT_DEFAULT,
        default=None,
        metavar="PATH",
        help=(
            "qualify 요약 CSV만 별도 저장. 인자 생략 시 "
            f"{DEFAULT_QUALIFY_REASON_SUMMARY_CSV} (--data-dir 기준)."
        ),
    )
    ap.add_argument(
        "--debug-type-report",
        action="store_true",
        help="data-dir 에 운영용 디버그 CSV 5종(qualify 요약, 타입 분포, 심볼 목록 등).",
    )
    return ap.parse_args(argv)


def main() -> None:
    sys.exit(run_pipeline(_parse_args()))


if __name__ == "__main__":
    main()
