# -*- coding: utf-8 -*-
"""
유니버스(universe_list.csv) 티커에 대해 SEC CompanyFacts를 일괄 추출하여 CSV 저장.

핵심 개선(배치 2500종목용):
1) BOM 안전 처리: utf-8-sig + 헤더 정규화(\\ufeff 제거)
2) 중복 CIK 처리 옵션: --duplicate-cik-strategy {skip,copy,allow}
   - copy(권장): CIK 1회 fetch/parse 후 중복 티커는 결과 복제 출력
3) 비기업(USD/Cash/Derivatives) 필터 강화
4) 진행률/재시도 로그 강화: 429/5xx backoff + Retry-After + 주기적 진행 로그

실행 예:
  python run_universe_extract.py --universe universe_list.csv --duplicate-cik-strategy copy --log-every 50

출력:
- output/universe_metrics_quarterly.csv
- output/universe_extraction_report.csv
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import re
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

try:
    import requests
except ImportError:
    requests = None

# --------------------------------------------------------------------------------------
# Config
# --------------------------------------------------------------------------------------
ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

SEC_COMPANY_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
SEC_COMPANYFACTS_URL_TEMPLATE = "https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"

# 반드시 연락처 포함 권장
USER_AGENT = "VQGRS batch (contact: imcooli9999@gmail.com)"

DEFAULT_RATE_LIMIT_REQ_PER_SEC = 10  # SEC 권장 상한 근처. 필요시 낮추세요(예: 5).
DEFAULT_MAX_RETRIES = 5
DEFAULT_TIMEOUT_SEC = 60

CACHE_DIR = ROOT / "cache"
CACHE_TICKERS_PATH = CACHE_DIR / "sec_company_tickers.json"
CACHE_TICKERS_TTL_SEC = 86400 * 7  # 7 days
CACHE_FACTS_DIR = CACHE_DIR / "companyfacts"
CACHE_FACTS_TTL_SEC = 86400 * 1  # 1 day

OUTPUT_DIR = ROOT / "output"
DEFAULT_UNIVERSE_CSV = ROOT / "universe_list.csv"

# Ticker normalization: exception map (유니버스 티커 → SEC 티커)
TICKER_EXCEPTION_MAP = {
    "BRKB": "BRK-B",
    "BFB": "BF-B",
}

# 비기업/통화 티커(필요하면 추가)
CURRENCY_LIKE_TICKERS = {
    "USD", "USDT", "USDC", "EUR", "GBP", "JPY", "CNY", "HKD", "AUD", "CAD", "CHF", "SGD",
}

# 섹터가 이렇게 오면 거의 비기업(업로드하신 파일에 존재)
NON_COMPANY_SECTOR_HINTS = {
    "Cash and/or Derivatives",
}

NON_COMPANY_NAME_PATTERNS = [
    r"\bCASH\b",
    r"\bCURRENCY\b",
    r"\bCOLLATERAL\b",
    r"\bDERIVATIVE\b",
    r"\bFUTURE(S)?\b",
    r"\bSWAP(S)?\b",
]


# --------------------------------------------------------------------------------------
# Logging
# --------------------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("universe_batch")


# --------------------------------------------------------------------------------------
# Utilities
# --------------------------------------------------------------------------------------
def _norm_header(s: str) -> str:
    """CSV 헤더 정규화: BOM 제거 + strip"""
    if s is None:
        return ""
    return str(s).replace("\ufeff", "").strip()


def _safe_strip(v: Any) -> str:
    if v is None:
        return ""
    return str(v).strip()


@dataclass
class RateLimiter:
    req_per_sec: float
    _min_interval: float = 0.0
    _last_ts: float = 0.0

    def __post_init__(self) -> None:
        self._min_interval = 1.0 / max(self.req_per_sec, 0.001)
        self._last_ts = 0.0

    def wait(self) -> None:
        now = time.time()
        if self._last_ts <= 0:
            self._last_ts = now
            return
        dt = now - self._last_ts
        if dt < self._min_interval:
            time.sleep(self._min_interval - dt)
        self._last_ts = time.time()


def read_universe_rows(universe_csv: Path) -> List[Dict[str, str]]:
    """
    BOM-safe CSV read.
    - utf-8-sig로 읽어서 BOM 제거
    - 헤더/값 strip
    """
    if not universe_csv.exists():
        raise FileNotFoundError(f"Universe CSV not found: {universe_csv}")

    rows: List[Dict[str, str]] = []
    with open(universe_csv, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f)
        try:
            raw_headers = next(reader)
        except StopIteration:
            return rows
        headers = [_norm_header(h) for h in raw_headers]

        for line_idx, cols in enumerate(reader, start=2):
            if not cols or all((c.strip() == "" for c in cols)):
                continue
            # 길이 불일치 방어
            if len(cols) < len(headers):
                cols = cols + [""] * (len(headers) - len(cols))
            rec = {headers[i]: _safe_strip(cols[i]) for i in range(len(headers))}
            rows.append(rec)
    return rows


def is_cvr_name(name: str) -> bool:
    """'... - CVR' 또는 끝이 ' CVR' 형태면 True."""
    if not name:
        return False
    n = name.strip()
    if n.endswith(" CVR") or n.endswith(" - CVR"):
        return True
    if re.search(r"\s+-\s*CVR\s*$", n):
        return True
    return False


def is_non_company_security(ticker: str, name: str, sector: str) -> Tuple[bool, str]:
    """
    비기업/현금/파생성 보유분 필터.
    - 통화형 티커(USD 등)
    - Sector 힌트(Cash and/or Derivatives)
    - Name 패턴(CASH/CURRENCY/DERIVATIVE/FUTURES 등)
    """
    t = (ticker or "").strip().upper()
    n = (name or "").strip()
    s = (sector or "").strip()

    if t in CURRENCY_LIKE_TICKERS:
        return True, "excluded_currency_ticker"

    if s in NON_COMPANY_SECTOR_HINTS:
        return True, "excluded_non_company_sector"

    up = n.upper()
    for pat in NON_COMPANY_NAME_PATTERNS:
        if re.search(pat, up):
            return True, "excluded_non_company_name"

    return False, ""


def ticker_to_cik_candidates(ticker: str, name: str) -> List[str]:
    """
    정규화된 티커 후보 리스트 (우선순위 순).
    - 예외맵 최우선
    - name에 CLASS/SERIES가 있고 ticker가 한 글자(A~Z)로 끝나면 base-cls, base.cls 후보 추가 (HEIA -> HEI-A, HEI.A)
    - '.' -> '-', Class B 추정 유지
    테스트 예: HEIA+"HEICO CORP CLASS A" -> HEI-A, HEI.A 포함; BRKB+Class B -> BRK-B(예외맵) 우선.
    """
    t = (ticker or "").strip().upper()
    if not t:
        return []
    candidates: List[str] = []

    # 1) 예외 맵 최우선
    if t in TICKER_EXCEPTION_MAP:
        candidates.append(TICKER_EXCEPTION_MAP[t].upper())

    # 2) 유니버스 티커가 클래스 문자를 끝에 붙인 경우(HEIA, HEIB 등) -> SEC 표기(HEI-A, HEI.A) 복원 후보
    #    name에 CLASS/SERIES가 있고, ticker가 A/B 한 글자로 끝날 때만 적용(GOOGL 등 오탐 방지)
    name_has_class_series = bool(
        name and re.search(r"\b(?:CLASS|SERIES)\s+[A-Z]\b", (name or "").strip(), re.I)
    )
    if name_has_class_series and len(t) >= 3:
        base = t[:-1]
        cls = t[-1]
        if len(base) >= 1 and cls in ("A", "B"):
            candidates.append(f"{base}-{cls}")
            candidates.append(f"{base}.{cls}")

    # 3) 원본 티커
    candidates.append(t)

    # 4) "." → "-" 변환
    t_dash = t.replace(".", "-")
    if t_dash != t:
        candidates.append(t_dash)

    # 5) Class B 추정(기존 로직)
    if len(t) <= 4 and name and re.search(r"class\s+[bB]\b", name, re.I):
        if "-" not in t:
            candidates.append(t + "-B")

    # 중복 제거(우선순위 유지)
    seen: set[str] = set()
    out: List[str] = []
    for c in candidates:
        if c not in seen:
            out.append(c)
            seen.add(c)
    return out


def _parse_company_tickers_payload(payload: Any) -> Dict[str, str]:
    """
    SEC company_tickers.json 포맷 변화 방어:
    - dict with numeric keys → values are dicts
    - list of dicts
    반환: ticker(대문자) -> CIK(10자리)
    """
    out: Dict[str, str] = {}

    def add_one(item: Dict[str, Any]) -> None:
        ticker = (item.get("ticker") or "").strip().upper()
        cik = item.get("cik_str") if "cik_str" in item else item.get("cik")
        if ticker and cik is not None:
            try:
                out[ticker] = str(int(cik)).zfill(10)
            except Exception:
                return

    if isinstance(payload, dict):
        vals = payload.values()
        for v in vals:
            if isinstance(v, dict):
                add_one(v)
    elif isinstance(payload, list):
        for v in payload:
            if isinstance(v, dict):
                add_one(v)

    return out


def load_sec_tickers_map(
    session: "requests.Session",
    force_refresh: bool = False,
    ttl_sec: int = CACHE_TICKERS_TTL_SEC,
) -> Dict[str, str]:
    """ticker(대문자) -> CIK(10자리). 캐시 사용."""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    now = time.time()

    if (not force_refresh) and CACHE_TICKERS_PATH.exists():
        try:
            if now - CACHE_TICKERS_PATH.stat().st_mtime < ttl_sec:
                with open(CACHE_TICKERS_PATH, "r", encoding="utf-8") as f:
                    payload = json.load(f)
                m = _parse_company_tickers_payload(payload)
                if m:
                    return m
        except Exception as e:
            log.warning("캐시 tickers 읽기 실패(무시): %s", e)

    # fetch
    log.info("SEC tickers 다운로드: %s", SEC_COMPANY_TICKERS_URL)
    r = session.get(
        SEC_COMPANY_TICKERS_URL,
        headers={"User-Agent": USER_AGENT},
        timeout=DEFAULT_TIMEOUT_SEC,
    )
    r.raise_for_status()
    payload = r.json()
    with open(CACHE_TICKERS_PATH, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False)
    m = _parse_company_tickers_payload(payload)
    return m


def _retry_sleep_seconds(resp: Optional["requests.Response"], attempt: int) -> float:
    """
    429/5xx 재시도 대기 시간:
    - Retry-After 있으면 우선
    - 없으면 지수 백오프(2^attempt) + 약간의 여유
    """
    if resp is not None:
        ra = resp.headers.get("Retry-After")
        if ra:
            try:
                return float(ra) + 0.5
            except Exception:
                pass
    # attempt: 1..N
    return min(60.0, (2.0 ** (attempt - 1)) * 2.0 + 0.5)


def fetch_companyfacts(
    session: "requests.Session",
    rate: RateLimiter,
    cik: str,
    force_refresh: bool = False,
    ttl_sec: int = CACHE_FACTS_TTL_SEC,
    max_retries: int = DEFAULT_MAX_RETRIES,
    timeout_sec: int = DEFAULT_TIMEOUT_SEC,
) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    """
    캐시 있으면 로드, 없으면 SEC 요청 (rate limit, backoff 재시도).
    반환: (data, fail_meta). 성공 시 (data, None). 실패 시 (None, {http_status, last_err, attempts_used}).
    """
    CACHE_FACTS_DIR.mkdir(parents=True, exist_ok=True)
    path = CACHE_FACTS_DIR / f"CIK{cik}.json"
    now = time.time()

    if (not force_refresh) and path.exists():
        try:
            if now - path.stat().st_mtime < ttl_sec:
                with open(path, "r", encoding="utf-8") as f:
                    return (json.load(f), None)
        except Exception as e:
            log.debug("캐시 companyfacts 읽기 실패 CIK=%s(무시): %s", cik, e)

    url = SEC_COMPANYFACTS_URL_TEMPLATE.format(cik=cik)
    last_err: Optional[str] = None
    last_http_status: Any = ""

    for attempt in range(1, max_retries + 1):
        rate.wait()
        try:
            r = session.get(url, headers={"User-Agent": USER_AGENT}, timeout=timeout_sec)
            last_http_status = getattr(r, "status_code", "")

            if r.status_code == 404:
                return (None, {"http_status": 404, "last_err": "404", "attempts_used": attempt})

            if r.status_code in (429, 500, 502, 503, 504):
                wait_s = _retry_sleep_seconds(r, attempt)
                log.warning(
                    "companyfacts 재시도 대상 status=%s CIK=%s attempt=%s/%s wait=%.1fs",
                    r.status_code,
                    cik,
                    attempt,
                    max_retries,
                    wait_s,
                )
                time.sleep(wait_s)
                continue

            r.raise_for_status()

            try:
                data = r.json()
            except Exception as je:
                last_err = f"json_decode_error: {je}"
                wait_s = _retry_sleep_seconds(r, attempt)
                log.warning("companyfacts JSON 디코드 실패 CIK=%s attempt=%s wait=%.1fs", cik, attempt, wait_s)
                time.sleep(wait_s)
                continue

            with open(path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=None)
            return (data, None)

        except requests.RequestException as e:
            last_err = str(e)
            last_http_status = getattr(getattr(e, "response", None), "status_code", "")
            wait_s = _retry_sleep_seconds(getattr(e, "response", None), attempt)
            log.warning(
                "companyfacts 요청 실패 CIK=%s attempt=%s/%s err=%s wait=%.1fs",
                cik,
                attempt,
                max_retries,
                e,
                wait_s,
            )
            time.sleep(wait_s)

    log.error("companyfacts 최종 실패 CIK=%s last_err=%s", cik, last_err)
    fail_meta = {
        "http_status": last_http_status if last_http_status != "" else "",
        "last_err": last_err or "",
        "attempts_used": max_retries,
    }
    return (None, fail_meta)


# --------------------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------------------
def run_universe_extract(
    universe_csv: Path,
    output_quarterly: Path,
    output_report: Path,
    limit_quarters: int,
    rate_limit: float,
    duplicate_cik_strategy: str,
    log_every: int,
    force_refresh_tickers: bool,
    force_refresh_facts: bool,
    post_retry_fetch_errors: bool = True,
    post_retry_rounds: int = 1,
    post_retry_rate_limit: Optional[float] = None,
    report_only: bool = False,  # PATCH: report만 생성 옵션
) -> None:
    """
    universe_list.csv 읽어 ticker→CIK 매칭 후 CompanyFacts 추출, CSV 저장.
    duplicate_cik_strategy:
      - skip: 중복 CIK 티커는 스킵
      - copy: 중복 CIK 티커도 결과를 복제하여 출력(권장)
      - allow: 중복 CIK도 "티커별 처리"로 간주(단, fetch는 캐시로 대부분 재사용됨)
    """
    if not requests:
        raise RuntimeError("requests 패키지가 필요합니다. `pip install requests`")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # 로컬 모듈 import
    from sec_companyfacts_extract import get_metrics_config, extract_company_metrics

    rows_universe = read_universe_rows(universe_csv)
    if not rows_universe:
        log.warning("유니버스 행이 없습니다: %s", universe_csv)
        return

    # 컬럼 후보(파일마다 다를 수 있어, 최소한의 자동 탐지)
    # 우선순위: Ticker/Name/Sector (대소문자, 공백 포함 변형 방어)
    def pick_col(candidates: List[str]) -> str:
        existing = {k.lower(): k for k in rows_universe[0].keys()}
        for c in candidates:
            if c.lower() in existing:
                return existing[c.lower()]
        return candidates[0]  # fallback

    ticker_col = pick_col(["Ticker", "ticker"])
    name_col = pick_col(["Name", "name"])
    sector_col = pick_col(["Sector", "sector"])

    session = requests.Session()
    rate = RateLimiter(req_per_sec=rate_limit)

    log.info("SEC ticker→CIK 매핑 로드 중...")
    ticker_to_cik = load_sec_tickers_map(session, force_refresh=force_refresh_tickers)
    log.info("ticker→CIK 항목 수: %s", len(ticker_to_cik))

    metrics_config = get_metrics_config()
    metric_names = list(metrics_config.keys())

    base_columns = ["ticker", "cik", "name", "sector", "end", "fy", "fp", "form", "filed", "accn"]
    quarter_columns = base_columns + [m for m in metric_names] + [f"{m}_Quarter" for m in metric_names] + ["balance_sheet_ok", "bs_diff_pct"]

    # CIK당 1회만 parse한 결과를 재사용하기 위한 캐시
    cik_to_extracted: Dict[str, Dict[str, Any]] = {}
    cik_to_facts_loaded: Dict[str, bool] = {}

    report_rows: List[Dict[str, str]] = []
    out_rows: List[Dict[str, Any]] = []

    ticker_to_sector: Dict[str, str] = {}
    for u in rows_universe:
        t = _safe_strip(u.get(ticker_col))
        if t:
            ticker_to_sector[t] = _safe_strip(u.get(sector_col))


    total = len(rows_universe)

    # 통계
    stats = {
        "ok": 0,
        "excluded_cvr": 0,
        "excluded_non_company": 0,
        "no_cik": 0,
        "fetch_error": 0,
        "parse_error": 0,
        "duplicate_cik_skip": 0,
        "duplicate_cik_copy": 0,
        "duplicate_cik_allow": 0,
        "ok_after_retry": 0,
        "fetch_error_final": 0,
    }

    start_ts = time.time()

    for idx, u in enumerate(rows_universe, start=1):
        ticker = _safe_strip(u.get(ticker_col))
        name = _safe_strip(u.get(name_col))
        sector = _safe_strip(u.get(sector_col))

        # 진행 로그
        if (idx == 1) or (log_every > 0 and idx % log_every == 0) or (idx == total):
            elapsed = time.time() - start_ts
            log.info("진행: %s/%s (%.1f%%) elapsed=%.1fs", idx, total, idx * 100.0 / total, elapsed)

        if is_cvr_name(name):
            stats["excluded_cvr"] += 1
            report_rows.append({"ticker": ticker, "name": name, "reason": "excluded_cvr", "cik": "", "error": "", "http_status": "", "last_err": "", "attempts_used": ""})
            continue

        nonco, nonco_reason = is_non_company_security(ticker, name, sector)
        if nonco:
            stats["excluded_non_company"] += 1
            report_rows.append({"ticker": ticker, "name": name, "reason": nonco_reason, "cik": "", "error": "", "http_status": "", "last_err": "", "attempts_used": ""})
            continue

        candidates = ticker_to_cik_candidates(ticker, name)
        cik = ""
        for c in candidates:
            cik = ticker_to_cik.get(c, "")
            if cik:
                break

        if not cik:
            stats["no_cik"] += 1
            report_rows.append({"ticker": ticker, "name": name, "reason": "no_cik", "cik": "", "error": "", "http_status": "", "last_err": "", "attempts_used": ""})
            continue

        # 중복 CIK 처리
        if cik in cik_to_extracted:
            if duplicate_cik_strategy == "skip":
                stats["duplicate_cik_skip"] += 1
                report_rows.append({"ticker": ticker, "name": name, "reason": "duplicate_cik_skip", "cik": cik, "error": "", "http_status": "", "last_err": "", "attempts_used": ""})
                continue

            if duplicate_cik_strategy == "copy":
                stats["duplicate_cik_copy"] += 1
                report_rows.append({"ticker": ticker, "name": name, "reason": "duplicate_cik_copy", "cik": cik, "error": "", "http_status": "", "last_err": "", "attempts_used": ""})
                extracted = cik_to_extracted[cik]
                # 결과 복제 출력
                for q in extracted.get("quarters") or []:
                    row = {
                        "ticker": ticker,
                        "cik": cik,
                        "name": name,
                        "sector": sector,
                        "end": q.get("end"),
                        "fy": q.get("fy"),
                        "fp": q.get("fp"),
                        "form": q.get("form"),
                        "filed": q.get("filed"),
                        "accn": q.get("accn"),
                    }
                    for m in metric_names:
                        row[m] = q.get(m)
                        row[f"{m}_Quarter"] = q.get(f"{m}_Quarter")
                    row["balance_sheet_ok"] = q.get("balance_sheet_ok")
                    row["bs_diff_pct"] = q.get("bs_diff_pct")
                    out_rows.append(row)
                continue

            # allow: 여기서는 "티커별 ok"로 처리하되, 사실상 extracted는 재사용해도 됨
            stats["duplicate_cik_allow"] += 1
            report_rows.append({"ticker": ticker, "name": name, "reason": "duplicate_cik_allow_reuse", "cik": cik, "error": "", "http_status": "", "last_err": "", "attempts_used": ""})
            extracted = cik_to_extracted[cik]
            for q in extracted.get("quarters") or []:
                row = {
                    "ticker": ticker,
                    "cik": cik,
                    "name": name,
                    "sector": sector,
                    "end": q.get("end"),
                    "fy": q.get("fy"),
                    "fp": q.get("fp"),
                    "form": q.get("form"),
                    "filed": q.get("filed"),
                    "accn": q.get("accn"),
                }
                for m in metric_names:
                    row[m] = q.get(m)
                    row[f"{m}_Quarter"] = q.get(f"{m}_Quarter")
                row["balance_sheet_ok"] = q.get("balance_sheet_ok")
                row["bs_diff_pct"] = q.get("bs_diff_pct")
                out_rows.append(row)
            continue

        # 최초 CIK 처리: fetch → extract
        cf, fail_meta = fetch_companyfacts(
            session=session,
            rate=rate,
            cik=cik,
            force_refresh=force_refresh_facts,
        )
        if not cf:
            stats["fetch_error"] += 1
            report_rows.append({
                "ticker": ticker,
                "name": name,
                "reason": "fetch_error",
                "cik": cik,
                "error": str((fail_meta or {}).get("last_err", "")),
                "http_status": str((fail_meta or {}).get("http_status", "")),
                "last_err": str((fail_meta or {}).get("last_err", "")),
                "attempts_used": str((fail_meta or {}).get("attempts_used", "")),
            })
            continue

        try:
            extracted = extract_company_metrics(cf, metrics_config=metrics_config, limit_quarters=limit_quarters)
            cik_to_extracted[cik] = extracted
            cik_to_facts_loaded[cik] = True
        except Exception as e:
            stats["parse_error"] += 1
            log.warning("extract_company_metrics 실패 ticker=%s cik=%s err=%s", ticker, cik, e)
            report_rows.append({"ticker": ticker, "name": name, "reason": "parse_error", "cik": cik, "error": str(e), "http_status": "", "last_err": "", "attempts_used": ""})
            continue

        stats["ok"] += 1
        report_rows.append({"ticker": ticker, "name": name, "reason": "ok", "cik": cik, "error": "", "http_status": "", "last_err": "", "attempts_used": ""})

        for q in extracted.get("quarters") or []:
            row = {
                "ticker": ticker,
                "cik": cik,
                "name": name,
                "sector": sector,
                "end": q.get("end"),
                "fy": q.get("fy"),
                "fp": q.get("fp"),
                "form": q.get("form"),
                "filed": q.get("filed"),
                "accn": q.get("accn"),
            }
            for m in metric_names:
                row[m] = q.get(m)
                row[f"{m}_Quarter"] = q.get(f"{m}_Quarter")
            row["balance_sheet_ok"] = q.get("balance_sheet_ok")
            row["bs_diff_pct"] = q.get("bs_diff_pct")
            out_rows.append(row)

    # ---------- Post-retry: fetch_error 항목만 CIK별 1회 재시도 (PATCH: 들여쓰기/구조 정리, 404 제외, 캐시 갱신, report_only 분기) ----------
    post_retry_rate = (min(rate_limit, 5.0) if post_retry_rate_limit is None else post_retry_rate_limit)
    if post_retry_fetch_errors and post_retry_rounds >= 1:
        for round_no in range(1, post_retry_rounds + 1):
            reason_filter = "fetch_error" if round_no == 1 else "fetch_error_final"
            # PATCH: http_status 404인 항목은 재시도 제외
            retry_indices = [
                i for i, r in enumerate(report_rows)
                if r.get("reason") == reason_filter and str(r.get("http_status", "")).strip() != "404"
            ]
            if not retry_indices:
                log.info("post-retry round %s: 재시도 대상 없음 (reason=%s)", round_no, reason_filter)
                continue

            cik_to_entries: Dict[str, List[tuple]] = {}
            for i in retry_indices:
                r = report_rows[i]
                cik = r.get("cik", "")
                if not cik:
                    continue
                ticker = r.get("ticker", "")
                name = r.get("name", "")
                sector = ticker_to_sector.get(ticker, "")
                cik_to_entries.setdefault(cik, []).append((ticker, name, sector, i))

            ciks = list(cik_to_entries.keys())
            total_retry_rows = sum(len(entries) for entries in cik_to_entries.values())
            log.info(
                "post-retry round %s 시작: 대상 rows=%s, CIKs=%s",
                round_no,
                total_retry_rows,
                len(ciks),
            )
            rate_retry = RateLimiter(req_per_sec=post_retry_rate)
            round_ok, round_fail = 0, 0
            for k, cik in enumerate(ciks):
                if log_every > 0 and (k + 1) % log_every == 0:
                    log.info("post-retry 진행: %s/%s CIKs", k + 1, len(ciks))
                entries = cik_to_entries[cik]
                cf, fail_meta = fetch_companyfacts(
                    session=session,
                    rate=rate_retry,
                    cik=cik,
                    force_refresh=True,
                )
                extracted = None
                if cf:
                    try:
                        extracted = extract_company_metrics(
                            cf,
                            metrics_config=metrics_config,
                            limit_quarters=limit_quarters,
                        )
                    except Exception as e:
                        fail_meta = {"http_status": "", "last_err": str(e), "attempts_used": ""}
                if cf and extracted:
                    # PATCH: 성공 시 캐시 갱신 (duplicate copy/allow에서 재사용)
                    cik_to_extracted[cik] = extracted
                    cik_to_facts_loaded[cik] = True
                    for (ticker, name, sector, report_idx) in entries:
                        if not report_only:
                            for q in extracted.get("quarters") or []:
                                row = {
                                    "ticker": ticker,
                                    "cik": cik,
                                    "name": name,
                                    "sector": sector,
                                    "end": q.get("end"),
                                    "fy": q.get("fy"),
                                    "fp": q.get("fp"),
                                    "form": q.get("form"),
                                    "filed": q.get("filed"),
                                    "accn": q.get("accn"),
                                }
                                for m in metric_names:
                                    row[m] = q.get(m)
                                    row[f"{m}_Quarter"] = q.get(f"{m}_Quarter")
                                row["balance_sheet_ok"] = q.get("balance_sheet_ok")
                                row["bs_diff_pct"] = q.get("bs_diff_pct")
                                out_rows.append(row)
                        r = report_rows[report_idx]
                        report_rows[report_idx] = {
                            "ticker": r["ticker"],
                            "name": r["name"],
                            "reason": "ok_after_retry",
                            "cik": r["cik"],
                            "error": "",
                            "http_status": "",
                            "last_err": "",
                            "attempts_used": "",
                        }
                    round_ok += len(entries)
                    stats["ok_after_retry"] += len(entries)
                else:
                    for (ticker, name, sector, report_idx) in entries:
                        r = report_rows[report_idx]
                        report_rows[report_idx] = {
                            "ticker": r["ticker"],
                            "name": r["name"],
                            "reason": "fetch_error_final",
                            "cik": r["cik"],
                            "error": str((fail_meta or {}).get("last_err", "")),
                            "http_status": str((fail_meta or {}).get("http_status", "")),
                            "last_err": str((fail_meta or {}).get("last_err", "")),
                            "attempts_used": str((fail_meta or {}).get("attempts_used", "")),
                        }
                    round_fail += len(entries)
            log.info("post-retry round %s 완료: 성공=%s, 실패=%s", round_no, round_ok, round_fail)
        stats["fetch_error_final"] = sum(1 for r in report_rows if r.get("reason") == "fetch_error_final")

    # 저장 (PATCH: report_only면 quarterly 미생성/미저장).
    # 결측→0 오염 방지: fillna(0)/na_rep=0 사용 금지. 결측은 NaN(빈칸) 유지.
    report_columns = ["ticker", "name", "reason", "cik", "error", "http_status", "last_err", "attempts_used"]
    if not report_only:
        from csv_export_guard import write_quarterly_csv_no_fillna0
        write_quarterly_csv_no_fillna0(
            out_rows,
            output_quarterly,
            quarter_columns,
            sanitize=True,
            diagnostic=True,
        )
    with open(output_report, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=report_columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(report_rows)
    log.info("저장: %s (rows=%s)", output_report, len(report_rows))

    # 요약
    log.info("==== 배치 요약 ====")
    for k, v in stats.items():
        log.info("%s: %s", k, v)


def build_argparser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Universe batch SEC CompanyFacts extractor")
    p.add_argument("--universe", type=str, default=str(DEFAULT_UNIVERSE_CSV), help="universe_list.csv path")
    p.add_argument("--out-quarterly", type=str, default=str(OUTPUT_DIR / "universe_metrics_quarterly.csv"))
    p.add_argument("--out-report", type=str, default=str(OUTPUT_DIR / "universe_extraction_report.csv"))
    p.add_argument("--limit-quarters", type=int, default=8)
    p.add_argument("--rate-limit", type=float, default=DEFAULT_RATE_LIMIT_REQ_PER_SEC, help="requests per second")
    p.add_argument(
        "--duplicate-cik-strategy",
        type=str,
        default="copy",
        choices=["skip", "copy", "allow"],
        help="how to handle tickers mapping to the same CIK",
    )
    p.add_argument("--log-every", type=int, default=50, help="progress log interval (#tickers)")
    p.add_argument("--force-refresh-tickers", action="store_true", help="ignore cached sec_company_tickers.json")
    p.add_argument("--force-refresh-facts", action="store_true", help="ignore cached companyfacts json")
    p.add_argument("--no-post-retry-fetch-errors", action="store_false", dest="post_retry_fetch_errors", help="disable post-retry of fetch_error (default: post-retry enabled)")
    p.add_argument("--post-retry-rounds", type=int, default=1, help="post-retry rounds (1 or 2, default 1)")
    p.add_argument("--post-retry-rate-limit", type=float, default=None, help="rate limit for post-retry (default min(rate_limit, 5.0))")
    # PATCH: report만 생성 옵션
    p.add_argument("--report-only", action="store_true", help="only write extraction report CSV, do not write quarterly metrics")
    return p


if __name__ == "__main__":
    args = build_argparser().parse_args()
    run_universe_extract(
        universe_csv=Path(args.universe),
        output_quarterly=Path(args.out_quarterly),
        output_report=Path(args.out_report),
        limit_quarters=args.limit_quarters,
        rate_limit=args.rate_limit,
        duplicate_cik_strategy=args.duplicate_cik_strategy,
        log_every=args.log_every,
        force_refresh_tickers=args.force_refresh_tickers,
        force_refresh_facts=args.force_refresh_facts,
        post_retry_fetch_errors=args.post_retry_fetch_errors,
        post_retry_rounds=args.post_retry_rounds,
        post_retry_rate_limit=getattr(args, "post_retry_rate_limit", None),
        report_only=getattr(args, "report_only", False),
    )
