# -*- coding: utf-8 -*-
"""
Donor-based auxiliary factor score estimation (factor-score dimension only).

Does not reconstruct raw fundamentals. Intended for use after group_factor_scores
long table exists; not wired into score_one_factor_one_group by default.
"""
from __future__ import annotations

from typing import Any, Mapping

import numpy as np
import pandas as pd

# Optional similarity columns (use only those present in base_df and target_row).
DONOR_SIMILARITY_COLS: tuple[str, ...] = (
    "Market Cap",
    "Revenue YoY",
    "Oper. Margin",
    "Debt/Eq",
    "Beta",
)


def _target_has_col(target_row: Mapping[str, Any] | Any, col: str) -> bool:
    if hasattr(target_row, "index") and col in getattr(target_row, "index", []):
        return True
    if isinstance(target_row, dict) and col in target_row:
        return True
    return False


def _normalize_gt_cat(val: Any) -> str:
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return ""
    s = str(val).strip()
    return s


def find_donor_candidates(
    base_df: pd.DataFrame,
    target_row: Mapping[str, Any] | Any,
    factor_name: str,
    max_donors: int = 20,
) -> pd.DataFrame:
    """
    Select donor rows (other symbols) with valid adjusted evidence/score for the same factor.

    Priority: same group_type, then same category, then mean absolute standardized
    difference on available similarity columns (lower is closer).
    """
    from score_primitives import _row_get, safe_to_float

    if base_df is None or base_df.empty or max_donors <= 0:
        return pd.DataFrame()

    if "factor_name" not in base_df.columns:
        return pd.DataFrame()
    if "symbol" not in base_df.columns:
        return pd.DataFrame()

    fn = str(factor_name)
    work = base_df[base_df["factor_name"].astype(str) == fn].copy()
    if work.empty:
        return pd.DataFrame()

    if "adjusted_evidence" in work.columns:
        work["adjusted_evidence"] = pd.to_numeric(work["adjusted_evidence"], errors="coerce")
        ok = work["adjusted_evidence"].notna()
    elif "adjusted_score" in work.columns:
        work["adjusted_score"] = pd.to_numeric(work["adjusted_score"], errors="coerce")
        ok = work["adjusted_score"].notna()
    else:
        return pd.DataFrame()
    if "is_valid_score" in work.columns:
        iv = work["is_valid_score"]
        if iv.dtype == object:
            ok = ok & iv.map(
                lambda x: str(x).strip().lower() in ("1", "true", "t", "yes")
                if pd.notna(x)
                else False
            )
        else:
            ok = ok & iv.fillna(0).astype(bool)
    work = work.loc[ok]
    if work.empty:
        return pd.DataFrame()

    tsym = _row_get(target_row, "symbol")
    if tsym is not None and str(tsym).strip() != "":
        work = work[work["symbol"].astype(str) != str(tsym).strip()]
    if work.empty:
        return pd.DataFrame()

    if "group_type" not in work.columns:
        work["group_type"] = "ALL"
    if "category" not in work.columns:
        work["category"] = ""

    tgt_gt = _normalize_gt_cat(_row_get(target_row, "group_type"))
    tgt_cat = _normalize_gt_cat(_row_get(target_row, "category"))

    work["_same_gt"] = work["group_type"].map(_normalize_gt_cat) == tgt_gt
    work["_same_cat"] = work["category"].fillna("").astype(str) == tgt_cat

    aux_cols = [c for c in DONOR_SIMILARITY_COLS if c in work.columns and _target_has_col(target_row, c)]
    n = len(work)
    dist = np.zeros(n, dtype=float)

    if aux_cols:
        used_cols = 0
        for c in aux_cols:
            col_vals = pd.to_numeric(work[c], errors="coerce")
            mu = float(col_vals.mean())
            sig = float(col_vals.std(ddof=0))
            if sig < 1e-12:
                continue
            t = safe_to_float(_row_get(target_row, c))
            if t is None:
                continue
            used_cols += 1
            z_t = (t - mu) / sig
            z_i = (col_vals - mu) / sig
            dist += np.abs(z_t - z_i.to_numpy(dtype=float))
        if used_cols > 0:
            dist /= float(used_cols)
    else:
        dist[:] = 0.0

    work["_dist"] = dist
    work = work.sort_values(
        by=["_same_gt", "_same_cat", "_dist"],
        ascending=[False, False, True],
        kind="mergesort",
    )
    out = work.head(int(max_donors)).copy()
    drop_cols = [x for x in out.columns if str(x).startswith("_")]
    out = out.drop(columns=drop_cols, errors="ignore")
    return out


def estimate_missing_factor_score_from_donors(
    base_df: pd.DataFrame,
    target_row: Mapping[str, Any] | Any,
    factor_name: str,
    factor_spec: Any,
    max_donors: int = 20,
) -> dict[str, Any]:
    """
    Median donor adjusted evidence (preferred) / score (fallback) + confidence (0~1).

    structural_missing: no donor math; donor_method='structural_skip'.
    """
    from score_primitives import _structural_missing_from_row, clip_value

    fn = str(getattr(factor_spec, "name", "") or factor_name)

    if not bool(getattr(factor_spec, "enabled", True)):
        return {
            "donor_score_estimate": None,
            "donor_evidence_estimate": None,
            "donor_count": 0,
            "donor_dispersion": None,
            "donor_confidence": 0.0,
            "donor_method": "disabled_skip",
            "donor_missing_reason": "disabled_factor",
        }

    if _structural_missing_from_row(target_row, factor_spec):
        return {
            "donor_score_estimate": None,
            "donor_evidence_estimate": None,
            "donor_count": 0,
            "donor_dispersion": None,
            "donor_confidence": 0.0,
            "donor_method": "structural_skip",
            "donor_missing_reason": "structural_missing",
        }

    donors = find_donor_candidates(base_df, target_row, fn, max_donors=max_donors)
    if donors.empty:
        return {
            "donor_score_estimate": None,
            "donor_evidence_estimate": None,
            "donor_count": 0,
            "donor_dispersion": None,
            "donor_confidence": 0.0,
            "donor_method": "median_donors",
            "donor_missing_reason": "no_donors",
        }

    evidences = pd.to_numeric(donors.get("adjusted_evidence"), errors="coerce").dropna() if "adjusted_evidence" in donors.columns else pd.Series(dtype=float)
    scores = pd.to_numeric(donors.get("adjusted_score"), errors="coerce").dropna() if "adjusted_score" in donors.columns else pd.Series(dtype=float)
    base = evidences if not evidences.empty else scores
    if base.empty:
        return {
            "donor_score_estimate": None,
            "donor_evidence_estimate": None,
            "donor_count": 0,
            "donor_dispersion": None,
            "donor_confidence": 0.0,
            "donor_method": "median_donors",
            "donor_missing_reason": "no_valid_donor_scores",
        }

    n = int(len(base))
    med = float(base.median())
    std = float(base.std(ddof=0)) if n > 1 else 0.0
    q25 = float(base.quantile(0.25))
    q75 = float(base.quantile(0.75))
    iqr = max(q75 - q25, 0.0)
    # Combine std and IQR scale (scores are ~0–100): higher spread lowers confidence.
    dispersion = float(max(std, iqr / 1.349)) if n > 1 else 0.0

    conf_n = min(1.0, n / float(max(1, max_donors)))
    # Penalize wide donor score spread; ~15 score points ~ moderate.
    spread_penalty = 1.0 / (1.0 + dispersion / 15.0)
    if n < 3:
        conf_n *= 0.65
    donor_confidence = float(clip_value(conf_n * spread_penalty, 0.0, 1.0))

    return {
        "donor_score_estimate": float(scores.median()) if not scores.empty else None,
        "donor_evidence_estimate": float(evidences.median()) if not evidences.empty else None,
        "donor_count": n,
        "donor_dispersion": dispersion,
        "donor_confidence": donor_confidence,
        "donor_method": "median_donors",
        "donor_missing_reason": None,
    }
