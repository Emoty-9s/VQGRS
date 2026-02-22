# -*- coding: utf-8 -*-
"""
CSV 저장 시 "결측→0 오염" 방지 및 진단.
- 핵심 계정은 결측이면 NaN(빈칸)으로만 저장. 0은 원천 데이터가 0일 때만 허용.
- Revenue==0 이면서 GrossProfit>0 같은 불가능 케이스를 보수적으로 NaN으로 복원.
"""

from __future__ import annotations

import csv
import logging
from pathlib import Path
from typing import Any, List

log = logging.getLogger(__name__)

# 절대 "결측→0"으로 채우면 안 되는 핵심 계정 (0이면 오염 의심)
CORE_NONZERO_FIELDS = [
    "Revenue",
    "Assets", "Liabilities", "Equity", "Cash",
    "NetIncome", "OperatingIncome", "PretaxIncome", "IncomeTaxExpense",
    "OCF", "CAPEX",
    "SharesOutstanding", "WeightedAvgSharesDiluted", "EPSDiluted",
]


def _numeric_zero(v: Any) -> bool:
    """값이 숫자 0(또는 0.0)에 해당하는지."""
    if v is None:
        return False
    try:
        return float(v) == 0
    except (TypeError, ValueError):
        return False


def _numeric_val(v: Any) -> float | None:
    """숫자로 해석 가능하면 float, 아니면 None."""
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def sanitize_zero_contamination_rows(
    rows: List[dict],
    *,
    revenue_key: str = "Revenue",
    gross_profit_key: str = "GrossProfit",
) -> List[dict]:
    """
    목적: 결측이 0으로 오염된 케이스를 보수적으로 제거(=None으로 복원).
    - Revenue==0 AND GrossProfit>0 => Revenue는 오염 가능성 매우 큼 → Revenue를 None으로.
    - rows는 in-place 수정 후 동일 리스트 반환.
    """
    if not rows:
        return rows
    for r in rows:
        rev = r.get(revenue_key)
        gp = r.get(gross_profit_key)
        if _numeric_zero(rev) and _numeric_val(gp) is not None and _numeric_val(gp) > 0:
            r[revenue_key] = None
            log.debug("sanitize: Revenue=0 & GrossProfit>0 → Revenue set to missing (ticker=%s end=%s)", r.get("ticker"), r.get("end"))
    return rows


def _diagnostic_log(rows: List[dict], revenue_key: str = "Revenue", gross_profit_key: str = "GrossProfit") -> None:
    """Revenue==0 개수, Revenue NaN 개수, GrossProfit>Revenue 개수 및 worst 예시 로그."""
    n_rev_zero = sum(1 for r in rows if _numeric_zero(r.get(revenue_key)))
    n_rev_nan = sum(1 for r in rows if r.get(revenue_key) is None or r.get(revenue_key) == "")
    bad_gp_gt_rev = []
    for r in rows:
        rev = _numeric_val(r.get(revenue_key))
        gp = _numeric_val(r.get(gross_profit_key))
        if rev is not None and gp is not None and gp > rev:
            bad_gp_gt_rev.append((r.get("ticker"), r.get("end"), rev, gp))
    n_bad = len(bad_gp_gt_rev)
    log.info("[guard] Revenue==0 rows=%s, Revenue NaN rows=%s, GrossProfit>Revenue rows=%s", n_rev_zero, n_rev_nan, n_bad)
    if n_bad > 0:
        for (ticker, end, rev, gp) in bad_gp_gt_rev[:5]:
            log.info("[guard]   example: ticker=%s end=%s Revenue=%s GrossProfit=%s", ticker, end, rev, gp)
        if n_bad > 5:
            log.info("[guard]   ... and %s more", n_bad - 5)


def write_quarterly_csv_no_fillna0(
    rows: List[dict],
    path: Path | str,
    fieldnames: List[str],
    *,
    sanitize: bool = True,
    diagnostic: bool = True,
) -> None:
    """
    - 절대 fillna(0) 하지 않음. 결측은 None 유지 후 CSV에는 빈칸으로 저장.
    - sanitize: True면 Revenue=0 & GrossProfit>0 인 경우 Revenue를 None으로 복원.
    - diagnostic: True면 Revenue==0/NaN, GrossProfit>Revenue 요약 로그 출력.
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    out = list(rows)
    if sanitize:
        out = sanitize_zero_contamination_rows(out)
    if diagnostic:
        _diagnostic_log(out)
    # None/빈 값은 CSV에 빈칸으로 (문자열 "None" 또는 0 대체 금지)
    def _cell(v: Any) -> str:
        if v is None or v == "":
            return ""
        return str(v)

    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for r in out:
            writer.writerow({k: _cell(r.get(k)) for k in fieldnames})
    log.info("저장(가드 적용): %s (rows=%s)", path, len(out))
