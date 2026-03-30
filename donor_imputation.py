# -*- coding: utf-8 -*-
"""
Hierarchical mean (median) imputation for missing factor evidence — VQGRS 3.3 transparency.

Sector → industry peer medians (min 3 peers each); no k-NN / similarity distance.
Does not reconstruct raw fundamentals. Used after group_factor_scores long table exists.
"""
from __future__ import annotations

from typing import Any, Mapping

import numpy as np
import pandas as pd

# Minimum peer count (excluding target symbol) to impute from sector or industry cohort.
MIN_PEER_COUNT = 3
# Imputed values are not observed; cap confidence so shrink stays near neutral prior.
IMPUTED_DONOR_CONFIDENCE_CAP = 0.1


def _normalize_cat(val: Any) -> str:
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return ""
    return str(val).strip()


def _sector_from_row(row: Mapping[str, Any] | Any) -> str:
    from score_primitives import _row_get

    for key in ("sector", "group_a_sector"):
        v = _row_get(row, key)
        s = _normalize_cat(v)
        if s:
            return s
    return ""


def _industry_from_row(row: Mapping[str, Any] | Any) -> str:
    from score_primitives import _row_get

    for key in ("industry", "group_a_industry"):
        v = _row_get(row, key)
        s = _normalize_cat(v)
        if s:
            return s
    return ""


def _sector_series(df: pd.DataFrame) -> pd.Series:
    if "sector" in df.columns:
        s = df["sector"]
    elif "group_a_sector" in df.columns:
        s = df["group_a_sector"]
    else:
        s = pd.Series(np.nan, index=df.index)
    return s.fillna("").astype(str).map(_normalize_cat)


def _industry_series(df: pd.DataFrame) -> pd.Series:
    if "industry" in df.columns:
        s = df["industry"]
    elif "group_a_industry" in df.columns:
        s = df["group_a_industry"]
    else:
        s = pd.Series(np.nan, index=df.index)
    return s.fillna("").astype(str).map(_normalize_cat)


def _prepare_peer_pool(
    base_df: pd.DataFrame,
    factor_name: str,
    target_row: Mapping[str, Any] | Any,
) -> pd.DataFrame:
    """Same factor, valid evidence or score, excluding target symbol."""
    from score_primitives import _row_get

    fn = str(factor_name)
    if base_df is None or base_df.empty or "factor_name" not in base_df.columns:
        return pd.DataFrame()
    if "symbol" not in base_df.columns:
        return pd.DataFrame()

    work = base_df[base_df["factor_name"].astype(str) == fn].copy()
    if work.empty:
        return pd.DataFrame()

    ev = (
        pd.to_numeric(work["adjusted_evidence"], errors="coerce")
        if "adjusted_evidence" in work.columns
        else pd.Series(np.nan, index=work.index)
    )
    sc = (
        pd.to_numeric(work["adjusted_score"], errors="coerce")
        if "adjusted_score" in work.columns
        else pd.Series(np.nan, index=work.index)
    )
    ok = ev.notna() | sc.notna()

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

    work = work.loc[ok].copy()
    if work.empty:
        return pd.DataFrame()

    tsym = str(_row_get(target_row, "symbol")).strip().upper()
    work = work[work["symbol"].astype(str).str.strip().str.upper() != tsym]
    if work.empty:
        return pd.DataFrame()

    work["_sec"] = _sector_series(work)
    work["_ind"] = _industry_series(work)
    return work


def _median_dispersion(vals: pd.Series) -> tuple[float, float]:
    v = pd.to_numeric(vals, errors="coerce").dropna()
    n = int(len(v))
    if n == 0:
        return float("nan"), 0.0
    med = float(v.median())
    disp = float(v.std(ddof=0)) if n > 1 else 0.0
    return med, disp


def _cohort_median_imputation(
    sub: pd.DataFrame,
    method: str,
) -> dict[str, Any] | None:
    """
    Prefer median(adjusted_evidence) with >= MIN_PEER_COUNT rows; else median(adjusted_score).
    """
    from score_primitives import evidence_to_score, score_to_evidence_approx

    if sub.empty:
        return None

    ev = (
        pd.to_numeric(sub["adjusted_evidence"], errors="coerce")
        if "adjusted_evidence" in sub.columns
        else pd.Series(np.nan, index=sub.index)
    )
    ce = sub.loc[ev.notna()]
    if len(ce) >= MIN_PEER_COUNT:
        med, disp = _median_dispersion(ce["adjusted_evidence"])
        if np.isnan(med):
            return None
        return {
            "donor_score_estimate": float(evidence_to_score(med)),
            "donor_evidence_estimate": float(med),
            "donor_count": int(len(ce)),
            "donor_dispersion": float(disp),
            "donor_confidence": float(IMPUTED_DONOR_CONFIDENCE_CAP),
            "donor_method": method,
            "donor_missing_reason": None,
        }

    sc = (
        pd.to_numeric(sub["adjusted_score"], errors="coerce")
        if "adjusted_score" in sub.columns
        else pd.Series(np.nan, index=sub.index)
    )
    cs = sub.loc[sc.notna()]
    if len(cs) >= MIN_PEER_COUNT:
        med, disp = _median_dispersion(cs["adjusted_score"])
        if np.isnan(med):
            return None
        med_ev = score_to_evidence_approx(med, beta=0.7)
        if med_ev is None:
            med_ev = 0.0
        return {
            "donor_score_estimate": float(med),
            "donor_evidence_estimate": float(med_ev),
            "donor_count": int(len(cs)),
            "donor_dispersion": float(disp),
            "donor_confidence": float(IMPUTED_DONOR_CONFIDENCE_CAP),
            "donor_method": method,
            "donor_missing_reason": None,
        }

    return None


def estimate_missing_factor_score_from_donors(
    base_df: pd.DataFrame,
    target_row: Mapping[str, Any] | Any,
    factor_name: str,
    factor_spec: Any,
    max_donors: int = 20,
) -> dict[str, Any]:
    """
    Hierarchical median imputation: same-sector peers, then same-industry, then prior fallback.

    Returns donor_evidence_estimate when imputation applies; else None so downstream uses prior.
    donor_confidence is capped at IMPUTED_DONOR_CONFIDENCE_CAP.

    structural_missing: no imputation; donor_method='structural_skip'.
    """
    from score_primitives import _structural_missing_from_row

    _ = max_donors  # API compatibility; hierarchical imputation does not use k-NN.

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

    work = _prepare_peer_pool(base_df, fn, target_row)
    if work.empty:
        return {
            "donor_score_estimate": None,
            "donor_evidence_estimate": None,
            "donor_count": 0,
            "donor_dispersion": None,
            "donor_confidence": 0.0,
            "donor_method": "no_valid_peer_avg",
            "donor_missing_reason": "no_peer_pool",
        }

    tgt_sec = _sector_from_row(target_row)
    tgt_ind = _industry_from_row(target_row)

    if tgt_sec:
        m = _cohort_median_imputation(work.loc[work["_sec"] == tgt_sec], "imputed_sector_avg")
        if m is not None:
            return m

    if tgt_ind:
        m = _cohort_median_imputation(work.loc[work["_ind"] == tgt_ind], "imputed_industry_avg")
        if m is not None:
            return m

    return {
        "donor_score_estimate": None,
        "donor_evidence_estimate": None,
        "donor_count": 0,
        "donor_dispersion": None,
        "donor_confidence": 0.0,
        "donor_method": "no_valid_peer_avg",
        "donor_missing_reason": "no_valid_peer_avg",
    }
