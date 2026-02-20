# -*- coding: utf-8 -*-
"""
SEC EDGAR companyfacts API 기반 재무 시계열 추출 (end 슬롯 기반).
- VQGRS 3.3 / Finviz 멀티플용 원천값: Revenue, OP, NI, Interest, Pretax, Tax, OCF, CAPEX, Assets, Liabilities, Equity, Cash, Shares, EPS 등.
- YTD 차감·10-K Q4 차감·TTM 합산은 미구현 (다음 단계).
- 슬롯: 앵커 태그로 최신 N개 보고기간(end)을 확정한 뒤, 모든 항목을 동일 슬롯에 맞춰 추출. frame/fy/fp는 참고 메타만.
- 로컬 캐시(TTL 24h).

Interest 추출 파이프라인 (Step 0~3):
  Step 0: dump_interest_candidates — 0건 시 메시지, 태그별 units 로그. --debug-interest 로 강제 실행.
  Step 1: fetch_companyconcept / get_companyconcept_usd_records — companyconcept API로 InterestPaidNet/InterestPaid 등 USD 레코드 수집.
  Step 2: A accn PNL → B accn CFS → C end 완전일치 → D end ±3일 → E companyconcept 매칭. interest_source: PNL|CFS|CONCEPT|IXBRL.
  Step 3: --require-pnl-interest 시 arelle iXBRL 파싱 시도 (pip install arelle-release).

실행 예시:
  python sec_companyfacts_extract.py 320193
  python sec_companyfacts_extract.py 320193 --debug-interest
  python sec_companyfacts_extract.py 320193 --require-pnl-interest
"""
from __future__ import annotations

import json
import logging
import re
import time
from datetime import datetime, date
from pathlib import Path

try:
    import requests
except ImportError:
    requests = None

# -----------------------------------------------------------------------------
# 상수
# -----------------------------------------------------------------------------
SEC_COMPANYFACTS_BASE = "https://data.sec.gov/api/xbrl/companyfacts"
SEC_COMPANYCONCEPT_BASE = "https://data.sec.gov/api/xbrl/companyconcept"
USER_AGENT = "VQGRS imcooli9999@gmail.com"
CACHE_DIR = Path(__file__).resolve().parent / ".companyfacts_cache"
CACHE_TTL_HOURS = 24
DEFAULT_QUARTERS = 8
# OUTPUT은 최근 8분기만; FETCH는 8+3 backfill으로 분기값(YTD 차감) 계산용
N_OUT_DEFAULT = 8
BACKFILL_DEFAULT = 3
N_FETCH_DEFAULT = N_OUT_DEFAULT + BACKFILL_DEFAULT
# Interest end 매칭 완화: ±N일 내 가장 가까운 end 허용 (기본 ON)
INTEREST_END_TOLERANCE_DAYS = 3
# companyconcept 슬롯 매칭: ±7일 근접, 92일 초과 레코드 제외 (2분기 이상 오래된 것 제외)
CONCEPT_END_TOLERANCE_DAYS = 7
CONCEPT_MAX_AGE_DAYS = 92
# companyconcept Interest 확인 태그 우선순위 (Step 1)
CONCEPT_INTEREST_TAGS = [
    "InterestPaidNet", "InterestPaid",
    "InterestExpense", "InterestExpenseNonoperating", "InterestAndDebtExpense",
    "InterestExpenseBorrowings", "InterestExpenseDebt", "InterestCostsIncurred",
]

# 분기 프레임 패턴: CY2025Q2, FY2025Q2 (참고용)
FRAME_QUARTER_PATTERN = re.compile(r"^(?:CY|FY)(\d{4})Q([1-4])$", re.IGNORECASE)

# 슬롯 확정용 앵커 태그 후보 (최근 end 커버리지가 가장 많은 태그를 자동 선택)
ANCHOR_TAG_CANDIDATES = [
    "Revenues",
    "SalesRevenueNet",
    "NetIncomeLoss",
    "ProfitLoss",
    "Assets",
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


# -----------------------------------------------------------------------------
# A) TAG_MAP: 항목명 -> 후보 태그 리스트 (우선순위 순). us-gaap 기본, dei는 별도 처리.
# -----------------------------------------------------------------------------
TAG_MAP = {
    "Revenue": [
        "Revenues",
        "SalesRevenueNet",
        "RevenueFromContractWithCustomerExcludingAssessedTax",
        "RevenueFromContractWithCustomerIncludingAssessedTax",
    ],
    "OperatingIncome": [
        "OperatingIncomeLoss",
    ],
    "NetIncome": [
        "NetIncomeLoss",
        "ProfitLoss",
    ],
    "InterestExpense": [
        "InterestExpense",
        "InterestExpenseNonoperating",
        "InterestAndDebtExpense",
        "InterestExpenseBorrowings",
        "InterestExpenseDebt",
        "InterestCostsIncurred",
        "InterestPaidNet",
        "InterestPaid",
    ],
    "PretaxIncome": [
        "IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest",
        "IncomeLossFromContinuingOperationsBeforeIncomeTaxes",
    ],
    "IncomeTaxExpense": [
        "IncomeTaxExpenseBenefit",
    ],
    "OCF": [
        "NetCashProvidedByUsedInOperatingActivities",
    ],
    "CAPEX": [
        "PaymentsToAcquirePropertyPlantAndEquipment",
    ],
    "Assets": [
        "Assets",
    ],
    "Liabilities": [
        "Liabilities",
    ],
    "Equity": [
        "StockholdersEquity",
        "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest",
    ],
    "Cash": [
        "CashAndCashEquivalentsAtCarryingValue",
    ],
    "SharesOutstanding": [
        "EntityCommonStockSharesOutstanding",   # dei
        "CommonStockSharesOutstanding",         # us-gaap
    ],
    "WeightedAvgSharesDiluted": [
        "WeightedAverageNumberOfDilutedSharesOutstanding",
        "WeightedAverageNumberOfSharesOutstandingDiluted",
    ],
    "EPSDiluted": [
        "EarningsPerShareDiluted",
    ],
    # --- 확장: Income Statement FLOW, unit=USD (후보군 우선순위 = 리스트 순서, 그룹별 slot fallback) ---
    "CostOfRevenue": [["CostOfRevenue"], ["CostOfGoodsAndServicesSold"]],
    "GrossProfit": [["GrossProfit"]],  # derived fallback: Revenue - CostOfRevenue
    "OperatingExpenses": [["OperatingExpenses"]],  # derived fallback: SGnA + RD
    "SGnA": [["SellingGeneralAndAdministrativeExpense"], ["GeneralAndAdministrativeExpense"]],
    "RD": [["ResearchAndDevelopmentExpense"]],
    # Cashflow / Non-cash FLOW
    "DepreciationAmortization": [["DepreciationDepletionAndAmortization"], ["DepreciationAndAmortization"]],
    "ShareBasedCompensation": [["ShareBasedCompensation"]],
    # Balance Sheet INSTANT
    "AssetsCurrent": [["AssetsCurrent"]],
    "LiabilitiesCurrent": [["LiabilitiesCurrent"]],
    "LongTermDebt": [["LongTermDebt"], ["LongTermDebtNoncurrent"]],
    "LongTermDebtCurrent": [["LongTermDebtCurrent"], ["DebtCurrent"]],
    "AccountsReceivableNetCurrent": [["AccountsReceivableNetCurrent"], ["AccountsReceivableNet"]],
    "InventoryNet": [["InventoryNet"], ["InventoryFinishedGoods"]],
    "PPENet": [["PropertyPlantAndEquipmentNet"]],
    "Goodwill": [["Goodwill"]],
    "IntangiblesNetExGoodwill": [["IntangibleAssetsNetExcludingGoodwill"], ["IntangibleAssetsNetIncludingGoodwill"]],
    "RetainedEarnings": [["RetainedEarningsAccumulatedDeficit"]],
    # Shareholder return / Financing FLOW
    "DividendsPaid": [["PaymentsOfDividends"], ["PaymentsOfDividendsCommonStock"]],
    "ShareRepurchase": [["PaymentsForRepurchaseOfCommonStock"], ["PaymentsForRepurchaseOfEquity"]],
    "DebtIssued": [["ProceedsFromIssuanceOfLongTermDebt"], ["ProceedsFromIssuanceOfDebt"]],
    "DebtRepaid": [["RepaymentsOfLongTermDebt"], ["RepaymentsOfDebt"]],
    "AcquisitionsNet": [["PaymentsToAcquireBusinessesNetOfCashAcquired"], ["PaymentsToAcquireBusinesses"]],
}

# InterestExpense: PNL(손익) vs CFO(현금흐름) 태그 구분 — interest_source 메타용
INTEREST_PNL_TAGS = [
    "InterestExpense",
    "InterestExpenseNonoperating",
    "InterestAndDebtExpense",
    "InterestExpenseBorrowings",
    "InterestExpenseDebt",
    "InterestCostsIncurred",
]
INTEREST_CFO_TAGS = ["InterestPaidNet", "InterestPaid"]
INTEREST_ALL_TAGS = INTEREST_PNL_TAGS + INTEREST_CFO_TAGS

# FLOW(기간/손익) vs STOCK(시점) — accn 기반 추출 시 FLOW는 60~400일 기간 필터, STOCK은 미적용
FLOW_ITEMS = [
    "Revenue", "OperatingIncome", "NetIncome", "InterestExpense", "PretaxIncome",
    "IncomeTaxExpense", "OCF", "CAPEX", "WeightedAvgSharesDiluted", "EPSDiluted",
    "CostOfRevenue", "GrossProfit", "OperatingExpenses", "SGnA", "RD",
    "DepreciationAmortization", "ShareBasedCompensation",
    "DividendsPaid", "ShareRepurchase", "DebtIssued", "DebtRepaid", "AcquisitionsNet",
]
STOCK_ITEMS = [
    "Assets", "Liabilities", "Equity", "Cash", "SharesOutstanding",
    "AssetsCurrent", "LiabilitiesCurrent", "LongTermDebt", "LongTermDebtCurrent",
    "AccountsReceivableNetCurrent", "InventoryNet", "PPENet", "Goodwill",
    "IntangiblesNetExGoodwill", "RetainedEarnings",
]

# 항목별 단위 후보 (companyfacts units 키). USD 계열 우선.
UNIT_CANDIDATES = {
    "Revenue": ["USD"],
    "OperatingIncome": ["USD"],
    "NetIncome": ["USD"],
    "InterestExpense": ["USD"],
    "PretaxIncome": ["USD"],
    "IncomeTaxExpense": ["USD"],
    "OCF": ["USD"],
    "CAPEX": ["USD"],
    "Assets": ["USD"],
    "Liabilities": ["USD"],
    "Equity": ["USD"],
    "Cash": ["USD"],
    "SharesOutstanding": ["shares", "Shares"],
    "WeightedAvgSharesDiluted": ["shares", "Shares"],
    "EPSDiluted": ["USD/shares", "USDPerShare"],
    "CostOfRevenue": ["USD"],
    "GrossProfit": ["USD"],
    "OperatingExpenses": ["USD"],
    "SGnA": ["USD"],
    "RD": ["USD"],
    "DepreciationAmortization": ["USD"],
    "ShareBasedCompensation": ["USD"],
    "AssetsCurrent": ["USD"],
    "LiabilitiesCurrent": ["USD"],
    "LongTermDebt": ["USD"],
    "LongTermDebtCurrent": ["USD"],
    "AccountsReceivableNetCurrent": ["USD"],
    "InventoryNet": ["USD"],
    "PPENet": ["USD"],
    "Goodwill": ["USD"],
    "IntangiblesNetExGoodwill": ["USD"],
    "RetainedEarnings": ["USD"],
    "DividendsPaid": ["USD"],
    "ShareRepurchase": ["USD"],
    "DebtIssued": ["USD"],
    "DebtRepaid": ["USD"],
    "AcquisitionsNet": ["USD"],
}

# METRICS_CONFIG: 외부/유니버스 추출용. metric별 우선순위 tag 후보 + 단위. (TAG_MAP/UNIT_CANDIDATES와 동기화)
def get_metrics_config() -> dict:
    """TAG_MAP + UNIT_CANDIDATES 기반 METRICS_CONFIG 반환. 후보 tag는 순서대로 시도."""
    return {
        m: {"tags": TAG_MAP[m], "units": UNIT_CANDIDATES.get(m, ["USD"])}
        for m in TAG_MAP
    }


# -----------------------------------------------------------------------------
# B) fetch_companyfacts(cik) -> JSON. User-Agent 포함, disk cache (TTL).
# -----------------------------------------------------------------------------
def _normalize_cik(cik: str) -> str:
    """CIK를 10자리 0-padded 문자열로."""
    s = str(cik).strip()
    if s.isdigit():
        return s.zfill(10)
    return s


def _cache_path(cik: str) -> Path:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return CACHE_DIR / f"{_normalize_cik(cik)}.json"


def _cache_meta_path(cik: str) -> Path:
    return CACHE_DIR / f"{_normalize_cik(cik)}.meta.json"


def _concept_cache_path(cik: str, tag: str) -> Path:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return CACHE_DIR / f"concept_{_normalize_cik(cik)}_{tag}.json"


def _concept_cache_meta_path(cik: str, tag: str) -> Path:
    return CACHE_DIR / f"concept_{_normalize_cik(cik)}_{tag}.meta.json"


def _concept_cache_is_valid(cik: str, tag: str) -> bool:
    meta = _concept_cache_meta_path(cik, tag)
    if not meta.exists():
        return False
    try:
        with open(meta, "r", encoding="utf-8") as f:
            m = json.load(f)
        ts = m.get("cached_at")
        if ts is None:
            return False
        return (time.time() - float(ts)) < (CACHE_TTL_HOURS * 3600)
    except Exception:
        return False


def _cache_is_valid(cik: str) -> bool:
    meta = _cache_meta_path(cik)
    if not meta.exists():
        return False
    try:
        with open(meta, "r", encoding="utf-8") as f:
            m = json.load(f)
        ts = m.get("cached_at")
        if ts is None:
            return False
        try:
            cached_ts = float(ts)
        except (TypeError, ValueError):
            return False
        return (time.time() - cached_ts) < (CACHE_TTL_HOURS * 3600)
    except Exception:
        return False


def fetch_companyfacts(cik: str) -> dict | None:
    """
    CIK에 해당하는 companyfacts JSON 반환.
    User-Agent 헤더 포함, 로컬 캐시 사용(TTL 24h). 실패 시 None.
    """
    cik = _normalize_cik(cik)
    path = _cache_path(cik)
    meta_path = _cache_meta_path(cik)
    if path.exists() and _cache_is_valid(cik):
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict) and "facts" in data:
                return data
            log.debug("캐시 무효(facts 없음) CIK=%s, 재요청", cik)
        except Exception as e:
            log.warning("캐시 읽기 실패 CIK=%s: %s", cik, e)
    if requests is None:
        log.error("requests 미설치. pip install requests")
        return None
    url = f"{SEC_COMPANYFACTS_BASE}/CIK{cik}.json"
    try:
        r = requests.get(
            url,
            headers={"User-Agent": USER_AGENT},
            timeout=30,
        )
        r.raise_for_status()
        data = r.json()
        log.info("companyfacts keys=%s has_facts=%s", list(data.keys())[:8] if isinstance(data, dict) else [], "facts" in data if isinstance(data, dict) else False)
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=None)
        with open(meta_path, "w", encoding="utf-8") as f:
            json.dump({"cached_at": time.time()}, f)
        return data
    except requests.RequestException as e:
        log.warning("companyfacts 요청 실패 CIK=%s: %s", cik, e)
        return None
    except Exception as e:
        log.warning("companyfacts 파싱/저장 실패 CIK=%s: %s", cik, e)
        return None


# -----------------------------------------------------------------------------
# Step 1) fetch_companyconcept(cik, tag) — SEC companyconcept API, 캐시 24h
# -----------------------------------------------------------------------------
def fetch_companyconcept(cik: str, tag: str) -> dict | None:
    """
    SEC companyconcept API: CIK + us-gaap tag별 JSON.
    반환: {"tag":..., "units": {"USD": [{"end", "val", "accn", "fy", "fp", "start", "frame"}, ...]}} 형태로 통일.
    """
    cik = _normalize_cik(cik)
    path = _concept_cache_path(cik, tag)
    meta_path = _concept_cache_meta_path(cik, tag)
    if path.exists() and _concept_cache_is_valid(cik, tag):
        try:
            with open(path, "r", encoding="utf-8") as f:
                raw = json.load(f)
            return _normalize_concept_response(raw, tag)
        except Exception as e:
            log.debug("concept 캐시 읽기 실패 cik=%s tag=%s: %s", cik, tag, e)
    if requests is None:
        return None
    url = f"{SEC_COMPANYCONCEPT_BASE}/CIK{cik}/us-gaap/{tag}.json"
    try:
        r = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=30)
        r.raise_for_status()
        raw = r.json()
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(raw, f, ensure_ascii=False, indent=None)
        with open(meta_path, "w", encoding="utf-8") as f:
            json.dump({"cached_at": time.time()}, f)
        return _normalize_concept_response(raw, tag)
    except requests.RequestException as e:
        log.debug("companyconcept 요청 실패 cik=%s tag=%s: %s", cik, tag, e)
        return None
    except Exception as e:
        log.debug("companyconcept 파싱 실패 cik=%s tag=%s: %s", cik, tag, e)
        return None


def _normalize_concept_response(raw: dict, tag: str) -> dict:
    """companyconcept API 응답을 units/items 탐색 가능한 형태로 통일."""
    units = raw.get("units") or {}
    return {"tag": tag, "units": units}


def match_from_concept_records(
    slot_end_str: str,
    slot_accn: str | None,
    concept_records: list[dict],
) -> tuple[dict | None, str | None]:
    """
    concept_records에서 accn 일치 우선, 없으면 end 근접(절대일수 최소)로 1건 선택.
    반환: (row_dict 동일 형태, "CONCEPT") 또는 (None, None).
    """
    if not concept_records:
        return (None, None)
    target_d = _parse_end(slot_end_str)
    by_accn = []
    by_end = []
    for r in concept_records:
        rec_accn = (r.get("accn") or "").strip()
        rec_end_str = r.get("end_str") or ""
        rec_end_d = r.get("end")
        if slot_accn and rec_accn == str(slot_accn).strip():
            delta = abs((rec_end_d - target_d).days) if rec_end_d and target_d else 0
            by_accn.append((delta, r))
        if rec_end_d and target_d:
            delta = abs((rec_end_d - target_d).days)
            by_end.append((delta, r))
    if by_accn:
        by_accn.sort(key=lambda x: x[0])
        _, r = by_accn[0]
        row = {
            "start": r.get("start"),
            "end": r.get("end"),
            "fy": r.get("fy"),
            "fp": r.get("fp"),
            "frame": r.get("frame"),
            "filed": None,
            "form": None,
            "tag": r.get("tag"),
            "unit": "USD",
            "accn": r.get("accn"),
            "val": r.get("val"),
        }
        return (row, "CONCEPT")
    if by_end:
        by_end.sort(key=lambda x: x[0])
        _, r = by_end[0]
        row = {
            "start": r.get("start"),
            "end": r.get("end"),
            "fy": r.get("fy"),
            "fp": r.get("fp"),
            "frame": r.get("frame"),
            "filed": None,
            "form": None,
            "tag": r.get("tag"),
            "unit": "USD",
            "accn": r.get("accn"),
            "val": r.get("val"),
        }
        return (row, "CONCEPT")
    return (None, None)


def get_companyconcept_usd_records(cik: str, tags_priority: list[str] | None = None) -> list[dict]:
    """
    companyconcept에서 태그 순으로 USD 레코드 수집. end 내림차순.
    각 항목: end, accn, fy, fp, start, frame, val, tag (동일 형태 row).
    """
    tags_priority = tags_priority or CONCEPT_INTEREST_TAGS
    all_rows = []
    for tag in tags_priority:
        data = fetch_companyconcept(cik, tag)
        if not data or not isinstance(data.get("units"), dict):
            continue
        for unit_key, items in data["units"].items():
            if not _unit_matches(unit_key, ["USD"], reject_substrings=["shares"]):
                continue
            if not isinstance(items, list):
                continue
            for it in items:
                if not isinstance(it, dict) or it.get("val") is None:
                    continue
                try:
                    val_f = float(it["val"])
                except (TypeError, ValueError):
                    continue
                end_str = (it.get("end") or "")[:10]
                if len(end_str) < 10:
                    continue
                try:
                    fy_val = int(it["fy"]) if it.get("fy") is not None else None
                except (TypeError, ValueError, KeyError):
                    fy_val = None
                filed_str = str(it.get("filed")).strip()[:10] if it.get("filed") else None
                form_str = str(it.get("form")).strip() if it.get("form") else None
                all_rows.append({
                    "start": _start_to_str(it.get("start")),
                    "end": _parse_end(end_str),
                    "end_str": end_str,
                    "fy": fy_val,
                    "fp": str(it.get("fp")).strip() if it.get("fp") else None,
                    "frame": str(it.get("frame")).strip() if it.get("frame") else None,
                    "filed": filed_str,
                    "form": form_str,
                    "accn": it.get("accn"),
                    "tag": tag,
                    "unit": "USD",
                    "val": val_f,
                    "source": "CONCEPT",
                })
    all_rows.sort(key=lambda x: (x.get("end_str") or "", x.get("filed") or "", x.get("accn") or ""), reverse=True)
    return all_rows


def select_concept_record_for_slot(
    records: list[dict],
    slot_end_str: str,
    slot_meta: dict,
    tolerance_days: int = CONCEPT_END_TOLERANCE_DAYS,
    max_age_days: int = CONCEPT_MAX_AGE_DAYS,
) -> tuple[dict | None, str | None, str]:
    """
    슬롯(slot_end, slot_meta)에 맞는 companyconcept 레코드 1건 선택.
    (1) end 정확일치 우선 (2) ±tolerance_days 내 근접, max_age_days 초과 제외 (3) tie-break: accn→filed→form→tag.
    반환: (row_dict, interest_source "CONCEPT_PNL"|"CONCEPT_CFO", reason 문자열).
    """
    if not records or not slot_end_str or len(slot_end_str) < 10:
        return (None, None, "no_records_or_slot")
    slot_end_d = _parse_end(slot_end_str)
    if not slot_end_d:
        return (None, None, "slot_end_invalid")
    slot_accn = (slot_meta.get("accn") or "").strip()
    slot_filed = (slot_meta.get("filed") or "")[:10]
    slot_filed_d = _parse_end(slot_filed) if slot_filed and len(slot_filed) >= 10 else None

    candidates = []
    for r in records:
        rec_end_d = r.get("end")
        rec_end_str = r.get("end_str") or ""
        if not rec_end_d:
            continue
        delta_days = abs((rec_end_d - slot_end_d).days)
        if delta_days > max_age_days:
            continue
        if rec_end_str == slot_end_str:
            candidates.append((0, "exact_end", r))
            continue
        if delta_days <= tolerance_days:
            candidates.append((delta_days, "near_end", r))

    if not candidates:
        log.info("InterestExpense CONCEPT match: slot=%s no_candidate (max_age=%dd tolerance=%dd)", slot_end_str, max_age_days, tolerance_days)
        return (None, None, "no_candidate_in_range")

    def _tie_key(cand):
        _, reason, r = cand
        delta_days = abs((r.get("end") - slot_end_d).days) if r.get("end") else 9999
        accn_match = 0 if (slot_accn and (r.get("accn") or "").strip() == slot_accn) else 1
        rec_filed = (r.get("filed") or "")[:10]
        rec_filed_d = _parse_end(rec_filed) if len(rec_filed) >= 10 else None
        if slot_filed_d and rec_filed_d:
            filed_diff = abs((rec_filed_d - slot_filed_d).days)
            filed_prefer = 0 if rec_filed_d <= slot_filed_d else 1
        else:
            filed_diff = 9999
            filed_prefer = 1
        form_10q = 0 if (r.get("form") or "").upper() == "10-Q" else (0 if (r.get("form") or "").upper() == "10-K" else 1)
        tag_pnl = 0 if (r.get("tag") or "") in INTEREST_PNL_TAGS else 1
        return (delta_days, accn_match, filed_prefer, filed_diff, form_10q, tag_pnl)

    candidates.sort(key=_tie_key)
    _, reason, best = candidates[0]
    tag = best.get("tag") or ""
    concept_source = "CONCEPT_PNL" if tag in INTEREST_PNL_TAGS else "CONCEPT_CFO"
    row = {
        "start": best.get("start"),
        "end": best.get("end"),
        "fy": best.get("fy"),
        "fp": best.get("fp"),
        "frame": best.get("frame"),
        "filed": best.get("filed"),
        "form": best.get("form"),
        "tag": best.get("tag"),
        "unit": best.get("unit", "USD"),
        "accn": best.get("accn"),
        "val": best.get("val"),
    }
    log.info(
        "InterestExpense CONCEPT match: slot=%s picked_end=%s tag=%s accn=%s reason=%s",
        slot_end_str, best.get("end_str"), tag, best.get("accn"), reason,
    )
    return (row, concept_source, reason)


# -----------------------------------------------------------------------------
# C) extract_value_for_end(companyfacts, tags, unit_candidates, end_date) -> best_row | None
#    extract_series: 전체 시계열 추출 (기존 유지)
# -----------------------------------------------------------------------------
def _parse_end(s: str) -> date | None:
    if not s or len(s) < 10:
        return None
    try:
        return date(int(s[:4]), int(s[5:7]), int(s[8:10]))
    except (ValueError, IndexError):
        return None


def _end_to_str(d: date | str) -> str:
    """date 또는 YYYY-MM-DD 문자열을 YYYY-MM-DD 문자열로 통일."""
    if isinstance(d, date):
        return d.strftime("%Y-%m-%d")
    s = str(d).strip()
    return s[:10] if len(s) >= 10 else s


def _to_date(s: str | None) -> date | None:
    """SEC YYYY-MM-DD 문자열을 date로. 없거나 짧으면 None."""
    if not s or len(str(s).strip()) < 10:
        return None
    return _parse_end(str(s).strip()[:10])


def _days_between(start: date | None, end: date | None) -> int | None:
    """start~end 일수. 없으면 None."""
    if not start or not end:
        return None
    return (end - start).days


def infer_period_type(start: date | str | None, end: date | str | None) -> str:
    """
    (end - start) 일수로 period type 판정. fp/form/frame 의존 제거.
    - >=330일 -> FY
    - 150~320일 -> YTD (6M/9M)
    - 70~110일 -> QUARTER
    - start 없음 -> INSTANT
    """
    start_d = start if isinstance(start, date) else _to_date(str(start)[:10] if start else None)
    end_d = end if isinstance(end, date) else _to_date(str(end)[:10] if end else None)
    d = _days_between(start_d, end_d)
    if d is None:
        return "INSTANT"
    if d >= 330:
        return "FY"
    if 150 <= d <= 320:
        return "YTD"
    if 70 <= d <= 110:
        return "QUARTER"
    return "UNKNOWN"


def quarterize_by_slots(
    slot_ends: list[date],
    picked_by_end: dict[date, dict],
    *,
    logger: logging.Logger | None = None,
) -> dict[date, float | None]:
    """
    누적(YTD/FY)을 직전 동일 start 기준으로 차분해 분기값으로 변환.
    - QUARTER: q = val, last_value_by_start[start] = val
    - YTD/FY: q = val - last_value_by_start[start] (있으면), 없으면 None; last 갱신
    슬롯은 end 기준 오름차순으로 처리.
    """
    asc = sorted(slot_ends)
    last_value_by_start: dict[date, float] = {}
    out_q: dict[date, float | None] = {}

    for end in asc:
        r = picked_by_end.get(end)
        if not r or r.get("val") is None:
            out_q[end] = None
            continue
        start_raw = r.get("start")
        start_d = start_raw if isinstance(start_raw, date) else _to_date(str(start_raw)[:10] if start_raw else None)
        end_d = r.get("end")
        if not isinstance(end_d, date):
            end_d = _to_date(str(end_d)[:10] if end_d else None) or end
        val = float(r["val"])
        ptype = infer_period_type(start_d, end_d)

        qval: float | None = None
        if ptype == "QUARTER":
            qval = val
            if start_d is not None:
                last_value_by_start[start_d] = val
        elif ptype in ("YTD", "FY"):
            if start_d is not None and start_d in last_value_by_start:
                qval = val - last_value_by_start[start_d]
                last_value_by_start[start_d] = val
            else:
                qval = None
                if start_d is not None:
                    last_value_by_start[start_d] = val
        else:
            qval = None

        out_q[end] = qval
        if logger:
            logger.debug("quarterize end=%s ptype=%s start=%s val=%s q=%s", end, ptype, start_d, val, qval)
    return out_q


# -----------------------------------------------------------------------------
# 슬롯: 앵커 태그로 end 목록 확정
# -----------------------------------------------------------------------------
def _collect_ends_from_tag(companyfacts: dict, tag: str) -> set[str]:
    """해당 태그(us-gaap 또는 dei)에서 등장하는 모든 end(YYYY-MM-DD) 수집."""
    facts = companyfacts.get("facts") or {}
    out = set()
    for ns, block in [("us-gaap", facts.get("us-gaap") or {}), ("dei", facts.get("dei") or {})]:
        if not isinstance(block, dict) or tag not in block:
            continue
        unit_block = (block[tag] or {}).get("units") or {}
        for items in unit_block.values():
            if not isinstance(items, list):
                continue
            for it in items:
                if isinstance(it, dict) and it.get("end"):
                    out.add(it.get("end", "")[:10])
    return {e for e in out if len(e) >= 10}


def _choose_anchor(companyfacts: dict) -> tuple[str | None, list[str]]:
    """
    앵커 태그 후보 중 최근 end 커버리지가 가장 많은 태그 선택.
    반환: (선택된_태그명, end 문자열 리스트 내림차순).
    """
    best_tag = None
    best_ends: list[str] = []
    for tag in ANCHOR_TAG_CANDIDATES:
        ends = _collect_ends_from_tag(companyfacts, tag)
        if not ends:
            continue
        sorted_ends = sorted(ends, reverse=True)
        if len(sorted_ends) > len(best_ends):
            best_ends = sorted_ends
            best_tag = tag
    return (best_tag, best_ends)


def _collect_end_accn_from_tag(companyfacts: dict, tag: str) -> list[dict]:
    """
    앵커 태그에서 (end, accn, form, filed, fp, fy) 수집, end 내림차순.
    동일 end는 첫 번째 발생만 유지.
    """
    facts = companyfacts.get("facts") or {}
    rows = []
    for ns, block in [("us-gaap", facts.get("us-gaap") or {}), ("dei", facts.get("dei") or {})]:
        if not isinstance(block, dict) or tag not in block:
            continue
        unit_block = (block[tag] or {}).get("units") or {}
        for items in unit_block.values():
            if not isinstance(items, list):
                continue
            for it in items:
                if not isinstance(it, dict) or not it.get("end"):
                    continue
                end_str = (it.get("end") or "")[:10]
                if len(end_str) < 10:
                    continue
                try:
                    fy_val = int(it["fy"]) if it.get("fy") is not None else None
                except (TypeError, ValueError, KeyError):
                    fy_val = None
                rows.append({
                    "end": end_str,
                    "accn": it.get("accn"),
                    "form": str(it.get("form")).strip() if it.get("form") else None,
                    "filed": str(it.get("filed")).strip() if it.get("filed") else None,
                    "fp": str(it.get("fp")).strip() if it.get("fp") else None,
                    "fy": fy_val,
                })
    rows.sort(key=lambda x: x.get("end") or "", reverse=True)
    seen_end = set()
    unique = []
    for r in rows:
        e = r.get("end")
        if e in seen_end:
            continue
        seen_end.add(e)
        unique.append(r)
    return unique


def _build_slots(companyfacts: dict, N: int) -> tuple[list[date], list[str], str | None, list[dict]]:
    """
    companyfacts에서 앵커로 슬롯 확정. 슬롯별 accn/form/filed/fp/fy 메타 포함.
    반환: (슬롯_날짜_리스트, 슬롯_문자열_리스트, 앵커_태그명, slot_meta 리스트).
    """
    anchor, _ = _choose_anchor(companyfacts)
    if not anchor:
        return ([], [], anchor, [])
    end_accn_list = _collect_end_accn_from_tag(companyfacts, anchor)
    if not end_accn_list:
        return ([], [], anchor, [])
    slot_meta = end_accn_list[:N]
    unique_ends = [m["end"] for m in slot_meta]
    slot_dates = []
    for e in unique_ends:
        d = _parse_end(e)
        slot_dates.append(d if d else date(2099, 12, 31))
    return (slot_dates, unique_ends, anchor, slot_meta)


def _start_to_str(s) -> str | None:
    """start 필드를 YYYY-MM-DD 문자열로. 없거나 짧으면 None."""
    if s is None:
        return None
    s = str(s).strip()
    if len(s) >= 10:
        return s[:10]
    return None


def _unit_matches(unit_key: str, unit_candidates: list[str], reject_substrings: list[str] | None = None) -> bool:
    """unit_key가 후보에 매칭되는지(대소문자·공백 무시). reject_substrings에 포함되면 False(shares 등)."""
    if not unit_key:
        return False
    uk = (unit_key or "").strip().lower()
    if reject_substrings:
        for sub in reject_substrings:
            if sub and sub.lower() in uk:
                return False
    for u in unit_candidates:
        if (u or "").strip().lower() == uk:
            return True
    return False


def extract_value_for_end(
    companyfacts: dict,
    tags: list[str],
    unit_candidates: list[str],
    end_date: date | str,
    reject_unit_substrings: list[str] | None = None,
) -> dict | None:
    """
    주어진 보고기간(end)에 해당하는 값 1건 추출. 동일 end에 여러 건이면 accn 최신(문자열 내림) 1건.
    reject_unit_substrings: 해당 문자열이 unit_key에 포함되면 제외 (예: ["shares"]).
    반환: YTD 판별용 메타 포함 (start, end, fy, fp, frame, filed, form, tag, unit, accn, val) 또는 None.
    """
    end_str = _end_to_str(end_date)
    facts = companyfacts.get("facts") or {}
    candidates = []
    for tag in tags:
        for ns, block in [("us-gaap", facts.get("us-gaap") or {}), ("dei", facts.get("dei") or {})]:
            if not isinstance(block, dict) or tag not in block:
                continue
            unit_block = (block[tag] or {}).get("units") or {}
            for unit_key, items in unit_block.items():
                if not _unit_matches(unit_key, unit_candidates, reject_substrings=reject_unit_substrings):
                    continue
                if not isinstance(items, list):
                    continue
                for it in items:
                    if not isinstance(it, dict):
                        continue
                    it_end = (it.get("end") or "")[:10]
                    if it_end != end_str:
                        continue
                    val = it.get("val")
                    if val is None:
                        continue
                    try:
                        val_f = float(val)
                    except (TypeError, ValueError):
                        continue
                    end_d = _parse_end(it.get("end"))
                    try:
                        fy_val = int(it["fy"]) if it.get("fy") is not None else None
                    except (TypeError, ValueError, KeyError):
                        fy_val = None
                    fp_val = str(it.get("fp")).strip() if it.get("fp") else None
                    frame_val = str(it.get("frame")).strip() if it.get("frame") else None
                    filed_val = str(it.get("filed")).strip() if it.get("filed") else None
                    form_val = str(it.get("form")).strip() if it.get("form") else None
                    start_val = _start_to_str(it.get("start"))
                    candidates.append({
                        "start": start_val,
                        "end": end_d,
                        "fy": fy_val,
                        "fp": fp_val,
                        "frame": frame_val,
                        "filed": filed_val,
                        "form": form_val,
                        "tag": tag,
                        "unit": unit_key,
                        "accn": it.get("accn"),
                        "val": val_f,
                    })
    if not candidates:
        return None
    candidates.sort(key=lambda x: x.get("accn") or "", reverse=True)
    return candidates[0]


def extract_value_for_end_fuzzy(
    companyfacts: dict,
    tags: list[str],
    unit_candidates: list[str],
    end_date: date | str,
    tolerance_days: int = 3,
    reject_unit_substrings: list[str] | None = None,
) -> tuple[dict | None, str | None]:
    """
    end 완전일치 실패 시 ±tolerance_days 내 가장 가까운 end로 1건 추출.
    반환: (row_dict, matched_end_str). row_dict에 "matched_end" 키로 실제 선택된 end 문자열 추가.
    """
    target_str = _end_to_str(end_date)
    target_d = _parse_end(target_str)
    if not target_d:
        return (None, None)
    facts = companyfacts.get("facts") or {}
    candidates = []
    for tag in tags:
        for ns, block in [("us-gaap", facts.get("us-gaap") or {}), ("dei", facts.get("dei") or {})]:
            if not isinstance(block, dict) or tag not in block:
                continue
            unit_block = (block[tag] or {}).get("units") or {}
            for unit_key, items in unit_block.items():
                if not _unit_matches(unit_key, unit_candidates, reject_substrings=reject_unit_substrings):
                    continue
                if not isinstance(items, list):
                    continue
                for it in items:
                    if not isinstance(it, dict):
                        continue
                    it_end_str = (it.get("end") or "")[:10]
                    if len(it_end_str) < 10:
                        continue
                    it_end_d = _parse_end(it_end_str)
                    if not it_end_d:
                        continue
                    delta = abs((it_end_d - target_d).days)
                    if delta > tolerance_days:
                        continue
                    val = it.get("val")
                    if val is None:
                        continue
                    try:
                        val_f = float(val)
                    except (TypeError, ValueError):
                        continue
                    try:
                        fy_val = int(it["fy"]) if it.get("fy") is not None else None
                    except (TypeError, ValueError, KeyError):
                        fy_val = None
                    fp_val = str(it.get("fp")).strip() if it.get("fp") else None
                    frame_val = str(it.get("frame")).strip() if it.get("frame") else None
                    filed_val = str(it.get("filed")).strip() if it.get("filed") else None
                    form_val = str(it.get("form")).strip() if it.get("form") else None
                    start_val = _start_to_str(it.get("start"))
                    candidates.append({
                        "start": start_val,
                        "end": it_end_d,
                        "fy": fy_val,
                        "fp": fp_val,
                        "frame": frame_val,
                        "filed": filed_val,
                        "form": form_val,
                        "tag": tag,
                        "unit": unit_key,
                        "accn": it.get("accn"),
                        "val": val_f,
                        "_delta_days": delta,
                        "_end_str": it_end_str,
                    })
    if not candidates:
        return (None, None)
    candidates.sort(key=lambda x: (x["_delta_days"], -(len(x.get("accn") or ""))))
    best = candidates[0]
    matched_end = best.pop("_end_str", None)
    best.pop("_delta_days", None)
    best["matched_end"] = matched_end
    return (best, matched_end)


def _flow_period_days_ok(start_str: str | None, end_str: str | None, min_days: int = 60, max_days: int = 400) -> bool:
    """start/end가 있으면 기간 일수가 min_days~max_days 내인지. 없으면 True."""
    if not start_str or not end_str or len(str(start_str)) < 10 or len(str(end_str)) < 10:
        return True
    start_d = _parse_end(str(start_str)[:10])
    end_d = _parse_end(str(end_str)[:10])
    if not start_d or not end_d:
        return True
    delta = (end_d - start_d).days
    return min_days <= delta <= max_days


def extract_value_for_accn(
    companyfacts: dict,
    tags: list[str],
    unit_candidates: list[str],
    accn: str,
    reject_unit_substrings: list[str] | None = None,
    start_end_days_range: tuple[int, int] = (60, 400),
    enforce_flow_period: bool = True,
) -> dict | None:
    """
    동일 accn에서 태그 우선순위대로 값 1건 추출.
    enforce_flow_period=True(FLOW): start/end 기간 60~400일만 허용.
    enforce_flow_period=False(STOCK): 기간 필터 미적용.
    """
    if not accn:
        return None
    accn_s = str(accn).strip()
    facts = companyfacts.get("facts") or {}
    min_days, max_days = start_end_days_range
    for tag in tags:
        for ns, block in [("us-gaap", facts.get("us-gaap") or {}), ("dei", facts.get("dei") or {})]:
            if not isinstance(block, dict) or tag not in block:
                continue
            unit_block = (block[tag] or {}).get("units") or {}
            for unit_key, items in unit_block.items():
                if not _unit_matches(unit_key, unit_candidates, reject_substrings=reject_unit_substrings):
                    continue
                if not isinstance(items, list):
                    continue
                for it in items:
                    if not isinstance(it, dict):
                        continue
                    if (it.get("accn") or "").strip() != accn_s:
                        continue
                    val = it.get("val")
                    if val is None:
                        continue
                    try:
                        val_f = float(val)
                    except (TypeError, ValueError):
                        continue
                    start_val = _start_to_str(it.get("start"))
                    end_str = (it.get("end") or "")[:10]
                    if enforce_flow_period and not _flow_period_days_ok(start_val, end_str, min_days, max_days):
                        continue
                    end_d = _parse_end(it.get("end"))
                    try:
                        fy_val = int(it["fy"]) if it.get("fy") is not None else None
                    except (TypeError, ValueError, KeyError):
                        fy_val = None
                    fp_val = str(it.get("fp")).strip() if it.get("fp") else None
                    frame_val = str(it.get("frame")).strip() if it.get("frame") else None
                    filed_val = str(it.get("filed")).strip() if it.get("filed") else None
                    form_val = str(it.get("form")).strip() if it.get("form") else None
                    return {
                        "start": start_val,
                        "end": end_d,
                        "fy": fy_val,
                        "fp": fp_val,
                        "frame": frame_val,
                        "filed": filed_val,
                        "form": form_val,
                        "tag": tag,
                        "unit": unit_key,
                        "accn": it.get("accn"),
                        "val": val_f,
                    }
    return None


def extract_value_for_accn_and_end(
    companyfacts: dict,
    tags: list[str],
    unit_candidates: list[str],
    accn: str,
    target_end: date | str,
    end_tolerance_days: int = 0,
    reject_unit_substrings: list[str] | None = None,
    start_end_days_range: tuple[int, int] = (60, 400),
    enforce_flow_period: bool = True,
    slot_meta: dict | None = None,
) -> dict | None:
    """
    ✅ 핵심 버그 수정용:
      - 같은 accn 내에 '현재기간 end'와 '전년동기 end'가 동시에 존재하는 경우가 많음.
      - 기존 extract_value_for_accn()은 end를 보지 않고 먼저 만난 레코드를 반환해서
        슬롯(end)과 다른 값(전년동기 등)을 집어오는 버그가 발생함.

    이 함수는:
      1) accn 일치 AND end가 target_end와 (정확일치 또는 ±tolerance)인 레코드만 후보로 모음
      2) tie-break: end delta → tag 우선순위 → (slot_meta의 fp/fy 일치 선호) → filed 근접 → accn
    """
    if not accn:
        return None
    target_end_str = _end_to_str(target_end)
    target_end_d = _parse_end(target_end_str)
    if not target_end_d:
        return None

    accn_s = str(accn).strip()
    facts = companyfacts.get("facts") or {}
    min_days, max_days = start_end_days_range

    slot_fp = (slot_meta.get("fp") if isinstance(slot_meta, dict) else None)
    slot_fy = (slot_meta.get("fy") if isinstance(slot_meta, dict) else None)
    slot_filed = (slot_meta.get("filed") if isinstance(slot_meta, dict) else None)
    slot_filed_d = _parse_end(str(slot_filed)[:10]) if slot_filed else None

    candidates: list[dict] = []
    tag_rank = {t: i for i, t in enumerate(tags)}

    for tag in tags:
        for ns, block in [("us-gaap", facts.get("us-gaap") or {}), ("dei", facts.get("dei") or {})]:
            if not isinstance(block, dict) or tag not in block:
                continue
            unit_block = (block[tag] or {}).get("units") or {}
            for unit_key, items in unit_block.items():
                if not _unit_matches(unit_key, unit_candidates, reject_substrings=reject_unit_substrings):
                    continue
                if not isinstance(items, list):
                    continue
                for it in items:
                    if not isinstance(it, dict):
                        continue
                    if (it.get("accn") or "").strip() != accn_s:
                        continue
                    it_end_str = (it.get("end") or "")[:10]
                    if len(it_end_str) < 10:
                        continue
                    it_end_d = _parse_end(it_end_str)
                    if not it_end_d:
                        continue
                    delta = abs((it_end_d - target_end_d).days)
                    if delta > int(end_tolerance_days or 0):
                        continue
                    val = it.get("val")
                    if val is None:
                        continue
                    try:
                        val_f = float(val)
                    except (TypeError, ValueError):
                        continue

                    start_val = _start_to_str(it.get("start"))
                    if enforce_flow_period and not _flow_period_days_ok(start_val, it_end_str, min_days, max_days):
                        continue

                    try:
                        fy_val = int(it["fy"]) if it.get("fy") is not None else None
                    except (TypeError, ValueError, KeyError):
                        fy_val = None
                    fp_val = str(it.get("fp")).strip() if it.get("fp") else None
                    frame_val = str(it.get("frame")).strip() if it.get("frame") else None
                    filed_val = str(it.get("filed")).strip() if it.get("filed") else None
                    filed_d = _parse_end(filed_val[:10]) if filed_val and len(filed_val) >= 10 else None
                    form_val = str(it.get("form")).strip() if it.get("form") else None

                    candidates.append({
                        "start": start_val,
                        "end": it_end_d,
                        "fy": fy_val,
                        "fp": fp_val,
                        "frame": frame_val,
                        "filed": filed_val,
                        "form": form_val,
                        "tag": tag,
                        "unit": unit_key,
                        "accn": it.get("accn"),
                        "val": val_f,
                        "_delta": delta,
                        "_tag_rank": tag_rank.get(tag, 9999),
                        "_fp_match": 0 if (slot_fp and fp_val and str(fp_val).strip().upper() == str(slot_fp).strip().upper()) else 1,
                        "_fy_match": 0 if (slot_fy is not None and fy_val is not None and int(fy_val) == int(slot_fy)) else 1,
                        "_filed_diff": abs((filed_d - slot_filed_d).days) if (filed_d and slot_filed_d) else 9999,
                        "_filed_future": 1 if (filed_d and slot_filed_d and filed_d > slot_filed_d) else 0,
                    })

    if not candidates:
        return None

    candidates.sort(
        key=lambda x: (
            x["_delta"],
            x["_tag_rank"],
            x["_fy_match"],
            x["_fp_match"],
            x["_filed_future"],
            x["_filed_diff"],
            (x.get("accn") or ""),
        )
    )
    best = candidates[0]
    # 내부 키 제거
    for k in list(best.keys()):
        if k.startswith("_"):
            best.pop(k, None)
    return best


def _quarter_key_from_frame(frame: str) -> str | None:
    """CY2025Q2 / FY2025Q2 -> '2025Q2'. 매칭 실패 시 None."""
    if not frame:
        return None
    m = FRAME_QUARTER_PATTERN.match(str(frame).strip())
    if not m:
        return None
    return f"{m.group(1)}Q{m.group(2)}"


def _quarter_key_from_fy_fp(fy, fp) -> str | None:
    """fy(정수), fp('Q1'~'Q4') -> '2025Q2'. fp가 Q1~Q4가 아니면 None."""
    if fy is None or fp is None:
        return None
    try:
        y = int(fy)
    except (TypeError, ValueError):
        return None
    fp_s = (str(fp).strip().upper() if fp else "") or ""
    if fp_s not in ("Q1", "Q2", "Q3", "Q4"):
        return None
    return f"{y}{fp_s}"


def _qkey_sort_key(qkey: str) -> tuple:
    """'2025Q2' -> (2025, 2) for descending sort."""
    if not qkey or "Q" not in str(qkey):
        return (0, 0)
    parts = str(qkey).upper().split("Q")
    if len(parts) != 2:
        return (0, 0)
    try:
        return (int(parts[0]), int(parts[1]))
    except (ValueError, TypeError):
        return (0, 0)


def _frame_to_sort_key(frame: str) -> tuple:
    """CY2025Q2 -> (2025, 2) for descending sort."""
    q = _quarter_key_from_frame(frame)
    return _qkey_sort_key(q) if q else (0, 0)


def extract_series(
    companyfacts: dict,
    tags: list[str],
    unit_candidates: list[str],
    frame_regex: re.Pattern,
    limit: int = 8,
    allow_no_frame: bool = False,
) -> list[dict]:
    """
    companyfacts에서 주어진 태그·단위 조건에 맞는 시계열 추출.
    반환: list[dict], 각 dict는 end, frame, val, accn, tag, unit, qkey(YYYYQ# 또는 None).
    - qkey: frame(CY/FY####Q#)에서 추출 또는 fy+fp로 생성. 없으면 None.
    - allow_no_frame=False(FLOW): qkey가 있는 행만 포함, qkey 기준 정렬/중복제거/limit.
    - allow_no_frame=True(STOCK): frame 없어도 end만 있으면 포함, end 기준 최신 N개.
    """
    facts = companyfacts.get("facts") or {}
    us_gaap = facts.get("us-gaap") or {}
    dei = facts.get("dei") or {}
    sources = [("us-gaap", us_gaap), ("dei", dei)]
    rows = []
    for tag in tags:
        if rows:
            break
        for ns, block in sources:
            if not isinstance(block, dict) or tag not in block:
                continue
            unit_block = (block[tag] or {}).get("units") or {}
            if not unit_block:
                log.debug("결측(단위 없음): tag=%s namespace=%s", tag, ns)
                continue
            for unit_key, items in unit_block.items():
                u_ok = (unit_key in unit_candidates or
                        any((unit_key or "").lower() == (u or "").lower() for u in unit_candidates))
                if not u_ok:
                    continue
                if not isinstance(items, list):
                    continue
                for it in items:
                    if not isinstance(it, dict):
                        continue
                    end_s = it.get("end")
                    val = it.get("val")
                    if val is None:
                        continue
                    try:
                        val_f = float(val)
                    except (TypeError, ValueError):
                        continue
                    end_d = _parse_end(end_s) if end_s else None
                    frame_raw = it.get("frame")
                    frame_str = str(frame_raw).strip() if frame_raw else ""
                    qkey = _quarter_key_from_frame(frame_str)
                    if qkey is None:
                        qkey = _quarter_key_from_fy_fp(it.get("fy"), it.get("fp"))
                    if not allow_no_frame and qkey is None:
                        continue
                    rows.append({
                        "end": end_d,
                        "frame": frame_str or None,
                        "val": val_f,
                        "accn": it.get("accn"),
                        "tag": tag,
                        "unit": unit_key,
                        "qkey": qkey,
                    })
                if rows:
                    break
            if rows:
                break
    if allow_no_frame:
        # STOCK: end 기준 최신순, 동일 end면 accn 내림
        rows.sort(key=lambda x: (x.get("end") or date.min, x.get("accn") or ""), reverse=True)
        return rows[:limit]
    # FLOW: qkey 기준 내림차순, 동일 qkey면 accn 내림, qkey당 1건
    rows.sort(key=lambda x: (_qkey_sort_key(x.get("qkey") or ""), x.get("accn") or ""), reverse=True)
    seen_qkey = set()
    unique = []
    for r in rows:
        q = r.get("qkey")
        if q in seen_qkey:
            continue
        seen_qkey.add(q)
        unique.append(r)
        if len(unique) >= limit:
            break
    return unique


# -----------------------------------------------------------------------------
# D) get_quarterly_frames(item_series) -> qkey 있는 분기 레코드만 필터/정렬 (레거시)
# -----------------------------------------------------------------------------
def get_quarterly_frames(item_series: list[dict]) -> list[dict]:
    """
    qkey가 존재하는 분기 레코드만 남기고 qkey 기준 내림차순 정렬.
    반환: list[dict] (end, frame, val, accn, qkey 등), 최신순.
    """
    out = [r for r in item_series if r.get("qkey")]
    out.sort(key=lambda x: (_qkey_sort_key(x.get("qkey") or ""), x.get("accn") or ""), reverse=True)
    return out


# -----------------------------------------------------------------------------
# E) get_latest_slots(cik, N=8) -> 최신 N개 보고기간(end) 슬롯
# -----------------------------------------------------------------------------
def dump_interest_candidates(companyfacts: dict, max_rows: int = 40) -> int:
    """
    진단용: us-gaap에서 Interest 후보 태그별 unit=USD 레코드를 end 내림차순으로 태그당 최대 8건 출력.
    태그별 units 키 목록을 로그로 남김. 0건이면 NO USD INTEREST FACTS 메시지 출력.
    반환: 출력한 USD 행 개수.
    """
    facts = companyfacts.get("facts") or {}
    us_gaap = facts.get("us-gaap") or {}
    tags_to_scan = list(INTEREST_ALL_TAGS)
    for t in list(us_gaap.keys()):
        if "interest" in t.lower() and t not in tags_to_scan:
            tags_to_scan.append(t)
    tag_units: dict[str, list[str]] = {}
    for tag in tags_to_scan:
        if tag not in us_gaap:
            continue
        unit_block = (us_gaap[tag] or {}).get("units") or {}
        tag_units[tag] = list(unit_block.keys()) if isinstance(unit_block, dict) else []
    for tag, u_list in tag_units.items():
        log.info("Interest tag %s: units=%s", tag, u_list[:15])
    print("\n--- dump_interest_candidates (us-gaap, unit=USD, end 내림차순 태그당 최대 8건) ---")
    total = 0
    for tag in tags_to_scan:
        if tag not in us_gaap:
            continue
        unit_block = (us_gaap[tag] or {}).get("units") or {}
        usd_items = []
        for unit_key, items in unit_block.items():
            if not _unit_matches(unit_key, ["USD"], reject_substrings=["shares"]):
                continue
            if not isinstance(items, list):
                continue
            for it in items:
                if not isinstance(it, dict) or it.get("val") is None:
                    continue
                usd_items.append({
                    "tag": tag,
                    "unit": unit_key,
                    "val": it.get("val"),
                    "start": _start_to_str(it.get("start")),
                    "end": (it.get("end") or "")[:10],
                    "fp": str(it.get("fp")).strip() if it.get("fp") else None,
                    "fy": it.get("fy"),
                    "frame": str(it.get("frame")).strip() if it.get("frame") else None,
                    "accn": it.get("accn"),
                })
        usd_items.sort(key=lambda x: x.get("end") or "", reverse=True)
        for r in usd_items[:8]:
            total += 1
            if total > max_rows:
                break
            print(f"  tag={r['tag']} unit={r['unit']} val={r['val']} start={r['start']} end={r['end']} fp={r['fp']} fy={r['fy']} frame={r['frame']} accn={r['accn']}")
        if total > max_rows:
            break
    if total == 0:
        print("  NO USD INTEREST FACTS FOUND IN COMPANYFACTS")
    print("--- end dump_interest_candidates ---\n")
    return total


def _log_interest_debug(companyfacts: dict, cik: str) -> None:
    """InterestExpense가 전부 None일 때 CIK당 1회: Interest 관련 태그·unit 키 디버그 로그."""
    interest_tags = set(INTEREST_PNL_TAGS + INTEREST_CFO_TAGS)
    facts = companyfacts.get("facts") or {}
    ns_tags: dict[str, list[str]] = {}
    all_unit_keys: list[str] = []
    for ns in ("us-gaap", "dei"):
        block = facts.get(ns) or {}
        if not isinstance(block, dict):
            continue
        found = [t for t in interest_tags if t in block]
        for t in list(block.keys()):
            if "interest" in t.lower() and t not in found:
                found.append(t)
        if found:
            ns_tags[ns] = sorted(found)
        for tag in found:
            unit_block = (block.get(tag) or {}).get("units") or {}
            for uk in unit_block.keys():
                if uk and uk not in all_unit_keys:
                    all_unit_keys.append(uk)
                    if len(all_unit_keys) >= 10:
                        break
            if len(all_unit_keys) >= 10:
                break
        if len(all_unit_keys) >= 10:
            break
    log.info(
        "InterestExpense 결측 CIK=%s: namespace별 Interest 관련 태그=%s, unit 키(상위 10)=%s",
        _normalize_cik(cik),
        ns_tags or "없음",
        all_unit_keys[:10] if all_unit_keys else "없음",
    )


def get_latest_slots(cik: str, N: int = DEFAULT_QUARTERS) -> list[date]:
    """
    앵커 태그로 확정한 최신 N개 보고기간(end) 슬롯을 날짜 리스트로 반환.
    """
    cf = fetch_companyfacts(cik)
    if not cf:
        return []
    slot_dates, _, _, _ = _build_slots(cf, N)
    return slot_dates


# -----------------------------------------------------------------------------
# F) normalize_flow_to_quarter_only(result) — YTD → 분기 차감, *_Quarter, period_type
# -----------------------------------------------------------------------------
FLOW_ITEMS_FOR_QUARTER = [
    "Revenue", "OperatingIncome", "NetIncome", "PretaxIncome", "IncomeTaxExpense",
    "OCF", "CAPEX", "InterestExpense",
    "CostOfRevenue", "GrossProfit", "OperatingExpenses", "SGnA", "RD",
    "DepreciationAmortization", "ShareBasedCompensation",
    "DividendsPaid", "ShareRepurchase", "DebtIssued", "DebtRepaid", "AcquisitionsNet",
]


def _fp_order(fp: str) -> int:
    """Q1=1, Q2=2, Q3=3, Q4=4, FY=5 for ordering."""
    if not fp:
        return 0
    u = (fp or "").strip().upper()
    if u == "Q1": return 1
    if u == "Q2": return 2
    if u == "Q3": return 3
    if u == "Q4": return 4
    if u == "FY": return 5
    return 0


def restore_quarter_series(
    slot_dates_fetch: list[date],
    rows_by_metric: dict[str, list[dict]],
    flow_metrics_for_quarter: list[str],
    n_out: int,
    logger: logging.Logger | None = None,
) -> dict[str, list[float | None]]:
    """
    slot 단위 레코드를 (end-start) 일수 기반 QUARTER/YTD/FY 판정 후 분기값으로 복원.
    YTD/FY는 동일 start 직전 누적 차분. 반환: { "Revenue_Quarter": [v0, v1, ...], ... } (길이 n_out).
    rows_by_metric 내 row에 period_type을 설정한다(in-place).
    """
    N = len(slot_dates_fetch)
    if N == 0:
        return {}
    logger = logger or log
    asc = sorted(slot_dates_fetch)
    out_quarters: dict[str, list[float | None]] = {}
    for item in flow_metrics_for_quarter:
        rows = rows_by_metric.get(item)
        if not rows or len(rows) != N:
            continue
        picked_by_end: dict[date, dict] = {}
        for r in rows:
            end_val = r.get("end")
            if end_val is None:
                continue
            end_d = end_val if isinstance(end_val, date) else _to_date(str(end_val)[:10])
            if end_d is not None:
                picked_by_end[end_d] = r
        qmap = quarterize_by_slots(asc, picked_by_end, logger=logger)
        out_quarters[f"{item}_Quarter"] = [qmap.get(slot_dates_fetch[i]) for i in range(min(n_out, N))]
        for r in rows:
            start_raw, end_raw = r.get("start"), r.get("end")
            start_d = start_raw if isinstance(start_raw, date) else _to_date(str(start_raw)[:10] if start_raw else None)
            end_d = end_raw if isinstance(end_raw, date) else _to_date(str(end_raw)[:10] if end_raw else None)
            r["period_type"] = infer_period_type(start_d, end_d)
    return out_quarters


def normalize_flow_to_quarter_only(result: dict) -> None:
    """
    restore_quarter_series 호출 후 OUTPUT(8)만 저장하고 모든 항목을 n_out으로 trim.
    """
    slots_fetch = result.get("_slots_fetch") or []
    slot_dates_fetch = result.get("_slot_dates_fetch") or []
    n_out = result.get("_n_out", N_OUT_DEFAULT)
    N = len(slots_fetch)
    if N == 0:
        return
    rows_by_metric = {m: result[m] for m in FLOW_ITEMS_FOR_QUARTER if result.get(m) and len(result.get(m)) == N}
    quarter_series = restore_quarter_series(
        slot_dates_fetch, rows_by_metric, FLOW_ITEMS_FOR_QUARTER, n_out, logger=log
    )
    for k, v in quarter_series.items():
        result[k] = v
    # Trim to n_out
    for key in list(result.keys()):
        if key.startswith("_"):
            continue
        val = result[key]
        if isinstance(val, list) and len(val) > n_out:
            result[key] = val[:n_out]
    result["_slots"] = (result.get("_slots_fetch") or [])[:n_out]


def _float_val(r: dict | None, key: str = "val") -> float | None:
    """row['val'] 또는 *_Quarter 리스트 요소를 float으로. None이면 None."""
    if r is None:
        return None
    v = r.get(key) if isinstance(r, dict) else r
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def apply_derived_metrics(result: dict) -> None:
    """
    GrossProfit = Revenue - CostOfRevenue, OperatingExpenses = SGnA + RD (slot val 및 *_Quarter).
    derived 시 row에 derived=True 표시 및 로그.
    """
    n_out = result.get("_n_out", N_OUT_DEFAULT)
    slots = result.get("_slots") or []

    # GrossProfit: tag 없으면 Revenue - CostOfRevenue (slot + _Quarter)
    rev_rows = result.get("Revenue")
    cogs_rows = result.get("CostOfRevenue")
    gp_rows = result.get("GrossProfit")
    rev_q = result.get("Revenue_Quarter")
    cogs_q = result.get("CostOfRevenue_Quarter")
    gp_q_key = "GrossProfit_Quarter"
    if gp_rows is not None and rev_rows is not None and cogs_rows is not None and len(gp_rows) == len(rev_rows) == len(cogs_rows):
        for i in range(min(n_out, len(gp_rows))):
            r_rev = _float_val(rev_rows[i] if i < len(rev_rows) else None)
            r_cogs = _float_val(cogs_rows[i] if i < len(cogs_rows) else None)
            gp_val = _float_val(gp_rows[i] if i < len(gp_rows) else None)
            if gp_val is None and r_rev is not None and r_cogs is not None:
                derived = r_rev - r_cogs
                if gp_rows[i] is not None and isinstance(gp_rows[i], dict):
                    gp_rows[i]["val"] = derived
                    gp_rows[i]["derived"] = True
                    log.info("GrossProfit slot i=%s derived=Revenue-CostOfRevenue val=%s", i, derived)
        if rev_q is not None and cogs_q is not None and result.get(gp_q_key) is not None:
            qlist = result[gp_q_key]
            for i in range(min(n_out, len(rev_q), len(cogs_q), len(qlist))):
                rq = _float_val(rev_q[i] if i < len(rev_q) else None)
                cq = _float_val(cogs_q[i] if i < len(cogs_q) else None)
                gq = _float_val(qlist[i] if i < len(qlist) else None)
                if gq is None and rq is not None and cq is not None:
                    qlist[i] = rq - cq

    # OperatingExpenses: tag 없으면 SGnA + RD (slot + _Quarter)
    sgna_rows = result.get("SGnA")
    rd_rows = result.get("RD")
    opex_rows = result.get("OperatingExpenses")
    sgna_q = result.get("SGnA_Quarter")
    rd_q = result.get("RD_Quarter")
    opex_q_key = "OperatingExpenses_Quarter"
    if opex_rows is not None and sgna_rows is not None and rd_rows is not None:
        for i in range(min(n_out, len(opex_rows))):
            s_val = _float_val(sgna_rows[i] if i < len(sgna_rows) else None)
            r_val = _float_val(rd_rows[i] if i < len(rd_rows) else None)
            opex_val = _float_val(opex_rows[i] if i < len(opex_rows) else None)
            if opex_val is None and (s_val is not None or r_val is not None):
                derived = (s_val or 0) + (r_val or 0)
                if opex_rows[i] is not None and isinstance(opex_rows[i], dict):
                    opex_rows[i]["val"] = derived
                    opex_rows[i]["derived"] = True
                    log.info("OperatingExpenses slot i=%s derived=SGnA+RD val=%s", i, derived)
        if result.get(opex_q_key) is not None and (sgna_q is not None or rd_q is not None):
            qlist = result[opex_q_key]
            for i in range(min(n_out, len(qlist))):
                sq = _float_val(sgna_q[i] if sgna_q and i < len(sgna_q) else None)
                rq = _float_val(rd_q[i] if rd_q and i < len(rd_q) else None)
                oq = _float_val(qlist[i] if i < len(qlist) else None)
                if oq is None and (sq is not None or rq is not None):
                    qlist[i] = (sq or 0) + (rq or 0)

    # (선택) OperatingIncome cross-check: gross - opex vs OperatingIncome, debug만
    gp_rows = result.get("GrossProfit")
    opex_rows = result.get("OperatingExpenses")
    oi_rows = result.get("OperatingIncome")
    if gp_rows and opex_rows and oi_rows and len(gp_rows) == len(opex_rows) == len(oi_rows):
        for i in range(min(n_out, len(oi_rows))):
            g = _float_val(gp_rows[i] if i < len(gp_rows) else None)
            o = _float_val(opex_rows[i] if i < len(opex_rows) else None)
            oi = _float_val(oi_rows[i] if i < len(oi_rows) else None)
            if g is not None and o is not None and oi is not None:
                diff = (g - o) - oi
                if abs(diff) > 1e-3:
                    log.debug("OperatingIncome cross-check slot i=%s gross-opex=%s OperatingIncome=%s diff=%s", i, g - o, oi, diff)


def extract_company_metrics(
    companyfacts_json: dict,
    *,
    metrics_config: dict | None = None,
    match_basis: str = "accn_first",
    limit_quarters: int = 8,
) -> dict:
    """
    단일 기업 companyfacts JSON에서 metric별 분기 시리즈 추출. (유니버스/단일 기업 공통 진입점)
    - metrics_config: None이면 get_metrics_config() 사용 (TAG_MAP/UNIT_CANDIDATES 기반)
    - match_basis: "accn_first" 등 (현재는 accn 우선 후 end fallback만 사용)
    반환: { "cik": str, "chosen_tags": { metric: tag }, "quarters": [ { end, fy, fp, filed, accn, form, metric1, metric1_Quarter, ... }, ... ] }
    InterestExpense는 companyfacts만 사용(companyconcept 미호출).
    """
    cf = companyfacts_json
    cik = str(cf.get("cik", "")).zfill(10) if cf.get("cik") is not None else ""
    config = metrics_config or get_metrics_config()
    n_fetch = limit_quarters + BACKFILL_DEFAULT
    n_out = limit_quarters
    slot_dates, slot_strs, anchor, slot_meta = _build_slots(cf, n_fetch)
    if not slot_strs:
        return {"cik": cik, "chosen_tags": {}, "quarters": []}
    result = {
        "_slots": slot_strs[:n_out],
        "_slots_fetch": slot_strs,
        "_slot_dates_fetch": slot_dates,
        "_n_out": n_out,
    }
    for metric, mconfig in config.items():
        tags_raw = mconfig.get("tags", TAG_MAP.get(metric, []))
        units = mconfig.get("units", UNIT_CANDIDATES.get(metric, ["USD"]))
        tag_groups = [tags_raw] if (tags_raw and not isinstance(tags_raw[0], list)) else (tags_raw or [])
        row_list = []
        is_flow = metric in FLOW_ITEMS
        for i, end_str in enumerate(slot_strs):
            end_d = slot_dates[i] if i < len(slot_dates) else _parse_end(end_str)
            meta = slot_meta[i] if i < len(slot_meta) else {}
            accn = meta.get("accn") if meta else None
            row = None
            for tag_group in tag_groups:
                if accn:
                    row = extract_value_for_accn_and_end(
                        cf, tag_group, units, accn, end_str,
                        end_tolerance_days=0,
                        enforce_flow_period=is_flow,
                        start_end_days_range=(60, 400),
                        slot_meta=meta,
                    )
                    if row:
                        row["match_basis"] = "ACCN"
                        break
                if not row:
                    row = extract_value_for_end(cf, tag_group, units, end_str)
                    if row:
                        row["match_basis"] = "END"
                        break
            if row:
                row_list.append(row)
            else:
                row_list.append({
                    "end": end_d, "val": None, "tag": None, "unit": None, "accn": None,
                    "start": None, "fy": None, "fp": None, "frame": None, "filed": None, "form": None, "match_basis": None,
                })
        result[metric] = row_list
    normalize_flow_to_quarter_only(result)
    apply_derived_metrics(result)
    # chosen_tags: 가장 많이 채워진 tag (또는 첫 non-null tag)
    chosen_tags = {}
    for metric in config:
        rows = result.get(metric) or []
        tags_used = [r.get("tag") for r in rows if r.get("tag")]
        chosen_tags[metric] = (max(set(tags_used), key=tags_used.count) if tags_used else None)
    # quarters: 출력용 행 리스트 (end, fy, fp, filed, accn, form + 각 metric 값 및 _Quarter)
    slots_out = result.get("_slots") or slot_strs[:n_out]
    quarters = []
    for i in range(min(n_out, len(slots_out))):
        meta = slot_meta[i] if i < len(slot_meta) else {}
        q = {
            "end": slots_out[i],
            "fy": meta.get("fy"),
            "fp": meta.get("fp"),
            "filed": meta.get("filed"),
            "accn": meta.get("accn"),
            "form": meta.get("form"),
        }
        for metric in config:
            rows = result.get(metric)
            if rows and i < len(rows):
                q[metric] = rows[i].get("val") if isinstance(rows[i], dict) else None
            else:
                q[metric] = None
            qkey = f"{metric}_Quarter"
            qvals = result.get(qkey)
            if qvals is not None and i < len(qvals):
                q[qkey] = qvals[i]
            else:
                q[qkey] = None
        quarters.append(q)
    return {"cik": cik, "chosen_tags": chosen_tags, "quarters": quarters}


# -----------------------------------------------------------------------------
# G) get_latest_quarters(cik, N=8) -> 슬롯 기반으로 모든 항목 반환 (이름 유지)
# -----------------------------------------------------------------------------
def get_latest_quarters(cik: str, N: int = DEFAULT_QUARTERS) -> dict:
    """
    슬롯 기반: 앵커로 최신 N개 end 확정 후, 모든 항목을 동일 슬롯에 맞춰 반환.
    반환:
      _slots: [ "2025-12-27", "2025-09-27", ... ]  (최신순)
      _anchor: 선택된 앵커 태그명
      Revenue, NetIncome, ...: 각 [ { end, start, fy, fp, frame, filed, form, val, tag, unit, accn }, ... ]  (길이 N, 결측은 None)
    """
    cf = fetch_companyfacts(cik)
    if not cf:
        log.warning("companyfacts 없음: CIK=%s", cik)
        base = {"_slots": [], "_anchor": None}
        for k in TAG_MAP:
            base[k] = []
        return base
    # FETCH 11 slots for quarterization backfill; OUTPUT 8 only
    n_fetch = N_FETCH_DEFAULT
    n_out = N_OUT_DEFAULT
    slot_dates, slot_strs, anchor, slot_meta = _build_slots(cf, n_fetch)
    result = {
        "_slots": slot_strs[:n_out],
        "_anchor": anchor,
        "_slots_fetch": slot_strs,
        "_slot_dates_fetch": slot_dates,
        "_n_out": n_out,
    }
    if not slot_strs:
        for k in TAG_MAP:
            result[k] = []
        return result
    interest_debug_logged = False
    for item_name, tags in TAG_MAP.items():
        units = UNIT_CANDIDATES.get(item_name, ["USD", "shares", "Shares", "USD/shares", "USDPerShare"])
        row_list = []
        if item_name == "InterestExpense":
            # Step 2: A accn PNL → B accn CFS → C end exact → D end ±3일 → E companyconcept (interest_source: PNL|CFS|CONCEPT|IXBRL)
            reject_shares = ["shares"]
            log.info("Interest pipeline Step 1: fetching companyconcept for tags %s", CONCEPT_INTEREST_TAGS[:4])
            concept_records = get_companyconcept_usd_records(cik, CONCEPT_INTEREST_TAGS)
            log.info("Interest pipeline Step 1: companyconcept USD records=%d", len(concept_records))
            for i, end_str in enumerate(slot_strs):
                end_d = slot_dates[i] if i < len(slot_dates) else _parse_end(end_str)
                meta = slot_meta[i] if i < len(slot_meta) else {}
                accn = meta.get("accn") if meta else None
                row = None
                source = None
                match_basis = None
                # A) 같은 슬롯 accn에서 P&L
                if accn:
                    row = extract_value_for_accn_and_end(
                        cf, INTEREST_PNL_TAGS, units, accn, end_str,
                        end_tolerance_days=0,
                        reject_unit_substrings=reject_shares,
                        start_end_days_range=(60, 400),
                        enforce_flow_period=True,
                        slot_meta=meta,
                    )
                    if row:
                        source = "PNL"
                        match_basis = "ACCN"
                # B) 같은 accn에서 CFS(현금흐름표 이자지급)
                if not row and accn:
                    row = extract_value_for_accn_and_end(
                        cf, INTEREST_CFO_TAGS, units, accn, end_str,
                        end_tolerance_days=0,
                        reject_unit_substrings=reject_shares,
                        start_end_days_range=(60, 400),
                        enforce_flow_period=True,
                        slot_meta=meta,
                    )
                    if row:
                        source = "CFS"
                        match_basis = "ACCN"
                # C) end 완전일치
                if not row:
                    row = extract_value_for_end(cf, INTEREST_PNL_TAGS, units, end_str, reject_unit_substrings=reject_shares)
                    if row:
                        source = "PNL"
                        match_basis = "END"
                if not row:
                    row = extract_value_for_end(cf, INTEREST_CFO_TAGS, units, end_str, reject_unit_substrings=reject_shares)
                    if row:
                        source = "CFS"
                        match_basis = "END"
                # D) end ±3일
                if not row and INTEREST_END_TOLERANCE_DAYS:
                    r2, matched_end = extract_value_for_end_fuzzy(
                        cf, INTEREST_PNL_TAGS, units, end_str,
                        tolerance_days=INTEREST_END_TOLERANCE_DAYS,
                        reject_unit_substrings=reject_shares,
                    )
                    if r2:
                        row = r2
                        source = "PNL"
                        match_basis = "END"
                        log.info("InterestExpense slot end=%s: matched_end ±%dd → %s", end_str, INTEREST_END_TOLERANCE_DAYS, matched_end)
                if not row and INTEREST_END_TOLERANCE_DAYS:
                    r2, matched_end = extract_value_for_end_fuzzy(
                        cf, INTEREST_CFO_TAGS, units, end_str,
                        tolerance_days=INTEREST_END_TOLERANCE_DAYS,
                        reject_unit_substrings=reject_shares,
                    )
                    if r2:
                        row = r2
                        source = "CFS"
                        match_basis = "END"
                        log.info("InterestExpense slot end=%s: matched_end ±%dd → %s", end_str, INTEREST_END_TOLERANCE_DAYS, matched_end)
                # E) companyconcept — 슬롯별 end/근접 매칭 (select_concept_record_for_slot)
                if not row and concept_records:
                    row, concept_src, _reason = select_concept_record_for_slot(
                        concept_records, end_str, meta,
                        tolerance_days=CONCEPT_END_TOLERANCE_DAYS,
                        max_age_days=CONCEPT_MAX_AGE_DAYS,
                    )
                    if row and concept_src:
                        source = concept_src
                        row["match_basis"] = "CONCEPT"
                if row:
                    row["interest_source"] = source
                    row["match_basis"] = row.get("match_basis") or match_basis or ("ACCN" if accn else "END")
                    row_list.append(row)
                else:
                    log.info("InterestExpense CONCEPT match: slot=%s failed (no record selected)", end_str)
                    row_list.append({
                        "end": end_d,
                        "val": None,
                        "tag": None,
                        "unit": None,
                        "accn": None,
                        "start": None,
                        "fy": None,
                        "fp": None,
                        "frame": None,
                        "filed": None,
                        "form": None,
                        "interest_source": None,
                        "match_basis": None,
                    })
            result[item_name] = row_list
            filled = sum(1 for r in row_list if r.get("val") is not None)
            if filled > 0 and concept_records and not any(r.get("interest_source") == "PNL" for r in row_list if r.get("val") is not None):
                log.info("InterestExpense: companyfacts USD missing → companyconcept has InterestPaidNet/InterestPaid USD → using CFS/CONCEPT proxy")
            elif filled > 0:
                log.info("InterestExpense: filled %d slots (sources: %s)", filled, list({r.get("interest_source") for r in row_list if r.get("val") is not None}))
            else:
                log.info("InterestExpense: all slots None (companyfacts USD missing; concept_records=%d)", len(concept_records))
            if all(r.get("val") is None for r in row_list):
                if not interest_debug_logged:
                    _log_interest_debug(cf, cik)
                    dump_interest_candidates(cf, max_rows=40)
                    interest_debug_logged = True
            continue
        # tag_groups: 단일 리스트면 [tags]로, 이미 리스트의 리스트면 그대로 (slot별 우선순위 fallback)
        tags_raw = tags
        tag_groups = [tags_raw] if (tags_raw and not isinstance(tags_raw[0], list)) else (tags_raw or [])
        for i, end_str in enumerate(slot_strs):
            end_d = slot_dates[i] if i < len(slot_dates) else _parse_end(end_str)
            meta = slot_meta[i] if i < len(slot_meta) else {}
            accn = meta.get("accn") if meta else None
            row = None
            is_flow = item_name in FLOW_ITEMS
            for tag_group in tag_groups:
                if accn:
                    row = extract_value_for_accn_and_end(
                        cf, tag_group, units, accn, end_str,
                        end_tolerance_days=0,
                        enforce_flow_period=is_flow,
                        start_end_days_range=(60, 400),
                        slot_meta=meta,
                    )
                    if row:
                        row["match_basis"] = "ACCN"
                        log.debug("metric=%s slot=%s tag=%s accn=%s", item_name, end_str, row.get("tag"), row.get("accn"))
                        break
                if not row:
                    row = extract_value_for_end(cf, tag_group, units, end_str)
                    if row:
                        row["match_basis"] = "END"
                        log.debug("metric=%s slot=%s tag=%s (END)", item_name, end_str, row.get("tag"))
                        break
            if row:
                row_list.append(row)
            else:
                row_list.append({
                    "end": end_d,
                    "val": None,
                    "tag": None,
                    "unit": None,
                    "accn": None,
                    "start": None,
                    "fy": None,
                    "fp": None,
                    "frame": None,
                    "filed": None,
                    "form": None,
                    "match_basis": None,
                })
        result[item_name] = row_list
    normalize_flow_to_quarter_only(result)
    apply_derived_metrics(result)
    return result


# -----------------------------------------------------------------------------
# G) __main__: 단일 CIK 슬롯 기준 출력
# -----------------------------------------------------------------------------
def _print_quarters(data: dict, cik: str) -> None:
    """슬롯 기준: _anchor, _slots 먼저 출력. 항목별 end/val/... + match_basis, period_type. *_Quarter 항목 출력."""
    slots = data.get("_slots") or []
    slots_fetch = data.get("_slots_fetch") or slots
    anchor = data.get("_anchor")
    print(f"\n=== CIK {cik} (슬롯 기반, OUTPUT_N={len(slots)} / FETCH_N={len(slots_fetch)}) ===")
    print(f"  _anchor = {anchor}")
    print(f"  _slots  = {slots}\n")
    for item_name, rows in data.items():
        if item_name.startswith("_"):
            continue
        print(f"  [{item_name}]")
        if not rows:
            print("    (슬롯 없음)")
            continue
        if isinstance(rows[0], dict):
            for r in rows:
                end = r.get("end")
                start = r.get("start")
                fp = r.get("fp")
                fy = r.get("fy")
                frame = r.get("frame")
                form = r.get("form")
                filed = r.get("filed")
                val = r.get("val")
                tag = r.get("tag")
                unit = r.get("unit")
                accn = r.get("accn")
                extra = ""
                if r.get("match_basis") is not None:
                    extra += f"  match_basis={r.get('match_basis')}"
                if item_name == "InterestExpense" and r.get("interest_source") is not None:
                    extra += f"  interest_source={r.get('interest_source')}"
                if r.get("period_type") is not None:
                    extra += f"  period_type={r.get('period_type')}"
                if r.get("derived"):
                    extra += "  derived=1"
                print(f"    end={end}  start={start}  fp={fp}  fy={fy}  frame={frame}  form={form}  filed={filed}  val={val}  tag={tag}  unit={unit}  accn={accn}{extra}")
        else:
            for i, val in enumerate(rows):
                slot = slots[i] if i < len(slots) else ""
                print(f"    slot={slot}  val={val}")
        print()


# -----------------------------------------------------------------------------
# Step 3) arelle iXBRL 파싱 — P&L interest 필수 시 확장 태그 추출
# 설치: pip install arelle-release
# -----------------------------------------------------------------------------
def _try_ixbrl_interest_for_slots(cik: str, slot_meta: list[dict], result_interest_rows: list[dict]) -> bool:
    """
    --require-pnl-interest 시: arelle로 iXBRL에서 Interest P&L 추출 시도.
    성공 시 result_interest_rows를 갱신하고 True. 실패/미설치 시 False.
    """
    try:
        from arelle import Cntlr
        from arelle import ModelManager
    except ImportError:
        log.warning(
            "arelle 미설치. P&L interest를 iXBRL에서 추출하려면: pip install arelle-release"
        )
        print("\n  [Step 3] arelle 미설치. P&L interest iXBRL 추출을 위해: pip install arelle-release\n")
        return False
    log.info("Step 3: arelle iXBRL 파싱 시도 (--require-pnl-interest)")
    # TODO: submissions API로 primaryDocument 확보 후 iXBRL URL 로드, facts 추출, slot별 매칭
    # 여기서는 스텁만: 시도 메시지 출력
    print("  [Step 3] arelle iXBRL 단계는 스텁입니다. (submissions → primaryDocument → arelle 로드 → interest concept 매칭)")
    return False


if __name__ == "__main__":
    import sys
    argv = [a for a in sys.argv[1:] if a and not a.startswith("-")]
    flags = [a for a in sys.argv[1:] if a and a.startswith("--")]
    debug_interest = "--debug-interest" in flags
    require_pnl_interest = "--require-pnl-interest" in flags
    cik = argv[0] if argv else "320193"
    print(f"Fetching companyfacts for CIK={cik} (cache TTL={CACHE_TTL_HOURS}h)...")
    cf = fetch_companyfacts(cik)
    if debug_interest and cf:
        print("\n[--debug-interest] 강제 실행: dump_interest_candidates")
        dump_interest_candidates(cf, max_rows=40)
    data = get_latest_quarters(cik, N=DEFAULT_QUARTERS)
    if require_pnl_interest and data.get("InterestExpense"):
        non_pnl = [r for r in data["InterestExpense"] if r.get("val") is not None and r.get("interest_source") != "PNL"]
        if non_pnl:
            log.info("--require-pnl-interest: PNL이 아닌 슬롯 %d개 → Step 3 (iXBRL) 시도", len(non_pnl))
            slot_meta = []
            if cf:
                _, _, _, slot_meta = _build_slots(cf, DEFAULT_QUARTERS)
            _try_ixbrl_interest_for_slots(cik, slot_meta, data["InterestExpense"])
    _print_quarters(data, cik)
