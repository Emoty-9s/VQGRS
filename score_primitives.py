# -*- coding: utf-8 -*-
from __future__ import annotations

import math
from typing import Any, Mapping


def get_rep_columns(factor_name: str) -> dict[str, str]:
    """
    Return representative column mapping for a given factor.

    Example:
      factor_name="ROIC" ->
        {"median": "rep__ROIC__median", "q25": "rep__ROIC__q25", ...}
    """
    return {
        "median": f"rep__{factor_name}__median",
        "q25": f"rep__{factor_name}__q25",
        "q75": f"rep__{factor_name}__q75",
        "iqr": f"rep__{factor_name}__iqr",
        "n_valid": f"rep__{factor_name}__n_valid",
    }


def safe_to_float(value: Any) -> float | None:
    """Coerce value to float safely; return None for NaN/unparseable."""
    if value is None:
        return None
    try:
        x = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(x) or math.isinf(x):
        return None
    return x


def clip_value(x: float, lo: float, hi: float) -> float:
    """Clip x into [lo, hi]."""
    if x < lo:
        return lo
    if x > hi:
        return hi
    return x


def compute_robust_z(
    raw_value: float | None,
    median_value: float | None,
    iqr_value: float | None,
    direction: str,
    eps: float = 1e-9,
) -> float | None:
    """
    Robust z = sign * (raw - median) / max(abs(iqr), eps)

    Notes:
      - If raw or median is missing -> None
      - If iqr is 0/None -> use eps in denominator
    """
    if raw_value is None or median_value is None:
        return None
    if direction == "higher_better":
        sign = 1.0
    elif direction == "lower_better":
        sign = -1.0
    else:
        return None

    iqr_abs = abs(iqr_value) if iqr_value is not None else 0.0
    denom = max(iqr_abs, eps)
    return sign * (raw_value - median_value) / denom


def robust_z_to_score(
    robust_z: float | None,
    clip_z: float = 3.0,
    slope: float = 0.7,
) -> float | None:
    """
    z_clip = clip(robust_z, -clip_z, clip_z)
    score  = 50 + 50 * tanh(slope * z_clip)
    score range: 0~100
    """
    if robust_z is None:
        return None
    z_clip = clip_value(robust_z, -clip_z, clip_z)
    score = 50.0 + 50.0 * math.tanh(slope * z_clip)
    # Defensive clamp in case of numeric drift
    return float(clip_value(score, 0.0, 100.0))


def map_peer_quality_to_multiplier(peer_quality: str | None) -> float:
    mapping = {
        "HIGH": 1.00,
        "MEDIUM": 0.85,
        "LOW": 0.65,
        "VERY_LOW": 0.45,
    }
    if peer_quality is None:
        return 0.75
    q = str(peer_quality).strip().upper()
    return mapping.get(q, 0.75)


def compute_group_confidence(
    n_valid: float | int | None,
    peer_quality: str | None,
) -> float:
    """
    conf_n = min(1.0, n_valid / 20.0)  # n_valid 없으면 0.0
    conf_quality = map_peer_quality_to_multiplier(peer_quality)
    conf_total = conf_n * conf_quality
    0~1 클리핑
    """
    if n_valid is None:
        conf_n = 0.0
    else:
        n = safe_to_float(n_valid)
        conf_n = 0.0 if n is None else min(1.0, n / 20.0)
    conf_quality = map_peer_quality_to_multiplier(peer_quality)
    conf_total = conf_n * conf_quality
    return float(clip_value(conf_total, 0.0, 1.0))


def shrink_score_to_neutral(
    raw_score: float | None,
    confidence: float | None,
    neutral: float = 50.0,
) -> float | None:
    """
    final = neutral + confidence * (raw_score - neutral)
    """
    if raw_score is None or confidence is None:
        return None
    return float(neutral + float(confidence) * (float(raw_score) - float(neutral)))


def _row_get(row: Mapping[str, Any] | Any, key: str) -> Any:
    """Support dict-like and pandas-row-like objects."""
    if hasattr(row, "get"):
        return row.get(key, None)
    try:
        return row[key]
    except Exception:
        return None


def score_one_factor_one_group(
    row: Mapping[str, Any] | Any,
    factor_spec: Any,
) -> dict[str, Any]:
    """
    Compute robust_z + score for a single (factor, group) row.
    This is a pure function: no I/O, no external state.
    """
    factor_name = str(getattr(factor_spec, "name", None) or "")
    category = str(getattr(factor_spec, "category", None) or "")
    direction = str(getattr(factor_spec, "direction", None) or "")
    enabled = bool(getattr(factor_spec, "enabled", True))

    rep_cols = get_rep_columns(factor_name) if factor_name else {}

    raw_value = safe_to_float(_row_get(row, factor_name)) if factor_name else None
    median_value = safe_to_float(_row_get(row, rep_cols.get("median", ""))) if rep_cols else None
    q25_value = safe_to_float(_row_get(row, rep_cols.get("q25", ""))) if rep_cols else None
    q75_value = safe_to_float(_row_get(row, rep_cols.get("q75", ""))) if rep_cols else None
    iqr_value = safe_to_float(_row_get(row, rep_cols.get("iqr", ""))) if rep_cols else None
    n_valid = safe_to_float(_row_get(row, rep_cols.get("n_valid", ""))) if rep_cols else None

    # Peer quality extraction priority
    peer_quality = _row_get(row, "b_peer_quality")
    if peer_quality is None:
        peer_quality = _row_get(row, "group_b_peer_quality")
    if peer_quality is None:
        peer_quality = _row_get(row, "peer_quality")
    if peer_quality is not None:
        peer_quality = str(peer_quality).strip().upper() or None

    robust_z = None
    raw_score = None
    confidence: float | None = None
    adjusted_score = None
    is_valid_score = False
    missing_reason: str | None = None

    if not enabled:
        missing_reason = "factor_disabled"
        is_valid_score = False
        return {
            "factor_name": factor_name,
            "category": category,
            "raw_value": raw_value,
            "median_value": median_value,
            "q25_value": q25_value,
            "q75_value": q75_value,
            "iqr_value": iqr_value,
            "n_valid": n_valid,
            "robust_z": None,
            "raw_score": None,
            "peer_quality": peer_quality,
            "confidence": None,
            "adjusted_score": None,
            "missing_reason": missing_reason,
            "is_valid_score": is_valid_score,
        }

    robust_z = compute_robust_z(
        raw_value=raw_value,
        median_value=median_value,
        iqr_value=iqr_value,
        direction=direction,
    )

    if robust_z is None:
        # Raw/median missing or direction invalid -> score cannot be computed.
        missing_reason = "missing_raw_or_median_or_invalid_direction"
        return {
            "factor_name": factor_name,
            "category": category,
            "raw_value": raw_value,
            "median_value": median_value,
            "q25_value": q25_value,
            "q75_value": q75_value,
            "iqr_value": iqr_value,
            "n_valid": n_valid,
            "robust_z": None,
            "raw_score": None,
            "peer_quality": peer_quality,
            "confidence": None,
            "adjusted_score": None,
            "missing_reason": missing_reason,
            "is_valid_score": False,
        }

    raw_score = robust_z_to_score(robust_z=robust_z)
    if raw_score is None:
        missing_reason = "raw_score_none"
        return {
            "factor_name": factor_name,
            "category": category,
            "raw_value": raw_value,
            "median_value": median_value,
            "q25_value": q25_value,
            "q75_value": q75_value,
            "iqr_value": iqr_value,
            "n_valid": n_valid,
            "robust_z": robust_z,
            "raw_score": None,
            "peer_quality": peer_quality,
            "confidence": None,
            "adjusted_score": None,
            "missing_reason": missing_reason,
            "is_valid_score": False,
        }

    confidence = compute_group_confidence(n_valid=n_valid, peer_quality=peer_quality)
    adjusted_score = shrink_score_to_neutral(raw_score=raw_score, confidence=confidence)

    is_valid_score = adjusted_score is not None
    return {
        "factor_name": factor_name,
        "category": category,
        "raw_value": raw_value,
        "median_value": median_value,
        "q25_value": q25_value,
        "q75_value": q75_value,
        "iqr_value": iqr_value,
        "n_valid": n_valid,
        "robust_z": robust_z,
        "raw_score": raw_score,
        "peer_quality": peer_quality,
        "confidence": confidence,
        "adjusted_score": adjusted_score,
        "missing_reason": None,
        "is_valid_score": is_valid_score,
    }

