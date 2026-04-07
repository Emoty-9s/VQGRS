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


def _signed_log1p(value: float | None) -> float | None:
    """
    sign(x) * log1p(|x|); x==0 -> 0. Returns None only for invalid inputs.
    sign-preserving log1p is used so negative values do not break scoring.
    """
    if value is None:
        return None
    try:
        x = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(x) or math.isinf(x):
        return None
    ax = abs(x)
    try:
        lg = math.log1p(ax)
    except (ValueError, OverflowError):
        return None
    if x == 0.0:
        return 0.0
    return float(math.copysign(lg, x))


def _prepare_relative_inputs_for_scoring(
    raw_value: float | None,
    median_value: float | None,
    q25_value: float | None,
    q75_value: float | None,
    iqr_value: float | None,
    use_log_scale: bool,
) -> tuple[float | None, float | None, float | None, float | None, float | None]:
    """
    Prepare inputs for compute_signed_evidence / relative robust z.

    log scale is applied only to relative scoring inputs, not to the stored raw output fields.
    When use_log_scale and both quartiles exist, IQR in transformed space is (T(q75)-T(q25));
    otherwise the peer IQR column is used unchanged as denominator fallback.
    """
    if not use_log_scale:
        return (raw_value, median_value, iqr_value, q25_value, q75_value)

    raw_s = _signed_log1p(raw_value)
    med_s = _signed_log1p(median_value)
    q25_s = _signed_log1p(q25_value)
    q75_s = _signed_log1p(q75_value)

    if q25_s is not None and q75_s is not None:
        iqr_s = q75_s - q25_s
    else:
        iqr_s = iqr_value

    return (raw_s, med_s, iqr_s, q25_s, q75_s)


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
    return compute_signed_evidence(
        raw_value=raw_value,
        median_value=median_value,
        iqr_value=iqr_value,
        direction=direction,
        eps=eps,
    )


def compute_signed_evidence(
    raw_value: float | None,
    median_value: float | None,
    iqr_value: float | None,
    direction: str,
    eps: float = 1e-9,
) -> float | None:
    """
    Signed robust evidence (neutral=0):
      evidence = sign * (raw - median) / max(abs(iqr), eps)
      sign = +1 for higher_better, -1 for lower_better
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


def compute_absolute_evidence(
    raw_value: float | None,
    direction: str,
    good: float | None,
    neutral: float | None,
    bad: float | None,
    cap: float,
    mode: str | None,
) -> float | None:
    """
    Piecewise-linear absolute (anchor) evidence with neutral=0, clipped to [-cap, +cap].

    - higher_better: requires bad < neutral < good (raw increases toward "good").
    - lower_better: requires good < neutral < bad (raw decreases toward "good").

    If anchors are missing or ordering is invalid, returns None.
    ``mode`` reserved for future variants; None / piecewise_linear / anchor_band use the same mapping.
    """
    if raw_value is None:
        return None
    if direction not in ("higher_better", "lower_better"):
        return None
    if good is None or neutral is None or bad is None:
        return None
    c = float(cap)
    if c <= 0.0 or math.isnan(c) or math.isinf(c):
        return None

    g, n, b = float(good), float(neutral), float(bad)
    if any(math.isnan(x) or math.isinf(x) for x in (g, n, b, float(raw_value))):
        return None

    _mode = (mode or "").strip().lower() if isinstance(mode, str) else ""
    if _mode not in ("", "piecewise_linear", "anchor_band"):
        return None

    x = float(raw_value)

    def _lin(x0: float, x1: float, y0: float, y1: float, t: float) -> float:
        if x1 == x0:
            return y0
        u = (t - x0) / (x1 - x0)
        return y0 + u * (y1 - y0)

    ev: float
    if direction == "higher_better":
        if not (b < n < g):
            return None
        if x <= b:
            ev = -c
        elif x < n:
            ev = _lin(b, n, -c, 0.0, x)
        elif x < g:
            ev = _lin(n, g, 0.0, c, x)
        else:
            ev = c
    else:
        # lower_better: good < neutral < bad
        if not (g < n < b):
            return None
        if x <= g:
            ev = c
        elif x < n:
            ev = _lin(g, n, c, 0.0, x)
        elif x < b:
            ev = _lin(n, b, 0.0, -c, x)
        else:
            ev = -c

    return float(clip_value(ev, -c, c))


def blend_evidences(
    relative_evidence: float | None,
    absolute_evidence: float | None,
    absolute_weight: float | None,
) -> tuple[float | None, str]:
    """
    Combine relative and absolute evidence. ``absolute_weight`` is clipped to [0, 1].

    Returns (blended_evidence_or_None, blend_method).
    """
    w = 0.0 if absolute_weight is None else float(absolute_weight)
    w = float(clip_value(w, 0.0, 1.0))
    rel = relative_evidence
    abs_e = absolute_evidence
    if rel is None and abs_e is None:
        return None, "none"
    if rel is None and abs_e is not None:
        return float(abs_e), "absolute_only"
    if rel is not None and abs_e is None:
        return float(rel), "relative_only"
    blended = (1.0 - w) * float(rel) + w * float(abs_e)
    return float(blended), "weighted_blend"


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
    return evidence_to_score(
        evidence=robust_z,
        beta=slope,
        clip_evidence=clip_z,
    )


def shrink_evidence_to_prior(
    evidence: float | None,
    confidence: float | None,
    prior_evidence: float = 0.0,
) -> float | None:
    """final = confidence * evidence + (1 - confidence) * prior_evidence"""
    if evidence is None or confidence is None:
        return None
    return float(float(confidence) * float(evidence) + (1.0 - float(confidence)) * float(prior_evidence))


def evidence_to_score(
    evidence: float | None,
    beta: float = 0.7,
    clip_evidence: float = 4.0,
) -> float | None:
    """
    e_clip = clip(evidence, -clip_evidence, clip_evidence)
    score = 50 + 50 * tanh(beta * e_clip)
    """
    if evidence is None:
        return None
    e_clip = clip_value(float(evidence), -float(clip_evidence), float(clip_evidence))
    score = 50.0 + 50.0 * math.tanh(float(beta) * e_clip)
    return float(clip_value(score, 0.0, 100.0))


def score_to_evidence_approx(
    score: float | None,
    beta: float = 0.7,
    eps: float = 1e-6,
) -> float | None:
    """
    Transitional compatibility inverse mapping of evidence_to_score using atanh.
    Use only when a legacy score must be approximately mapped back to evidence.
    """
    if score is None or beta == 0:
        return None
    s = float(score)
    y = (s - 50.0) / 50.0
    y = clip_value(y, -1.0 + eps, 1.0 - eps)
    return float(math.atanh(y) / float(beta))


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
    Confidence used for evidence shrink:
      adjusted_evidence = confidence * raw_evidence + (1-confidence) * prior_evidence

    conf_n = min(1.0, n_valid / 20.0)  # n_valid 없으면 0.0
    conf_quality = map_peer_quality_to_multiplier(peer_quality)
    conf_total = conf_n * conf_quality
    0~1 clipping
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


def _row_has_key(row: Mapping[str, Any] | Any, key: str) -> bool:
    """True if row exposes key (backward compat when diagnostic columns are absent)."""
    if row is None:
        return False
    if isinstance(row, Mapping):
        return key in row
    try:
        return key in row.index  # type: ignore[attr-defined]
    except Exception:
        return False


def _structural_missing_from_row(row: Mapping[str, Any] | Any, factor_spec: Any) -> bool:
    """
    Structural missing: value is not meaningfully defined; do not impute or score.
    Uses factor_spec.structural_missing_rule when set; dividend growth uses a conservative fallback.
    """
    rule = getattr(factor_spec, "structural_missing_rule", None)
    if isinstance(rule, str):
        rule = rule.strip() or None
    fn = str(getattr(factor_spec, "name", "") or "")

    if rule == "pe_nonpositive_eps":
        if fn != "P/E":
            return False
        eps = safe_to_float(_row_get(row, "EPS (ttm)"))
        return eps is None or eps <= 0.0

    if rule == "peg_invalid_growth":
        if fn != "PEG":
            return False
        g5 = safe_to_float(_row_get(row, "EPS Next 5Y"))
        return g5 is None or g5 <= 0.0

    if rule == "forward_pe_nonpositive_estimate":
        if fn != "Forward P/E":
            return False
        eps_ny = safe_to_float(_row_get(row, "EPS Next Y"))
        if eps_ny is None:
            eps_ny = safe_to_float(_row_get(row, "EPS Next Y Est Level"))
        fpe = safe_to_float(_row_get(row, "Forward P/E"))
        return (eps_ny is None or eps_ny <= 0.0) or (fpe is None or fpe <= 0.0)

    if rule == "pfcf_nonpositive_or_invalid":
        if fn != "P/FCF":
            return False
        pfcf = safe_to_float(_row_get(row, "P/FCF"))
        return pfcf is None or pfcf <= 0.0

    if rule == "ev_ebitda_nonpositive_or_invalid":
        if fn != "EV/EBITDA":
            return False
        ev_e = safe_to_float(_row_get(row, "EV/EBITDA"))
        return ev_e is None or ev_e <= 0.0

    if rule == "roic_invalid_invested_capital":
        if fn != "ROIC":
            return False
        if not _row_has_key(row, "ROIC IC Avg"):
            return False
        src = _row_get(row, "ROIC Calc Source")
        if src is not None:
            s = str(src).strip()
            if s in {"missing_ic", "nonpositive_ic"}:
                return True
        ic_avg = safe_to_float(_row_get(row, "ROIC IC Avg"))
        if ic_avg is None:
            return True
        if ic_avg <= 0.0:
            return True
        return False

    if rule == "eps_yoy_nonpositive_regime":
        if fn != "EPS YoY":
            return False
        if not _row_has_key(row, "EPS YoY Current TTM") and not _row_has_key(row, "EPS YoY Prior TTM"):
            return False
        cur = safe_to_float(_row_get(row, "EPS YoY Current TTM"))
        pri = safe_to_float(_row_get(row, "EPS YoY Prior TTM"))
        if cur is None or pri is None:
            return True
        if cur <= 0.0 or pri <= 0.0:
            return True
        src = _row_get(row, "EPS YoY Calc Source")
        if src is not None:
            s = str(src).strip()
            if s and s.lower() != "nan" and s != "positive_to_positive":
                return True
        return False

    if rule == "dividend_not_applicable":
        if fn not in {"Dividend Gr. 3Y", "Dividend Gr. 5Y"}:
            return False
        dt = safe_to_float(_row_get(row, "Dividend TTM"))
        return dt is None or dt <= 0.0

    if rule is None and fn in {"Dividend Gr. 3Y", "Dividend Gr. 5Y"}:
        dt = safe_to_float(_row_get(row, "Dividend TTM"))
        return dt is None or dt <= 0.0

    return False


def _base_score_dict(
    factor_name: str,
    category: str,
    raw_value: float | None,
    median_value: float | None,
    q25_value: float | None,
    q75_value: float | None,
    iqr_value: float | None,
    n_valid: float | None,
    peer_quality: str | None,
    robust_z: float | None,
    raw_score: float | None,
    confidence: float | None,
    adjusted_score: float | None,
    missing_reason: str | None,
    is_valid_score: bool,
    raw_evidence: float | None = None,
    prior_evidence: float | None = None,
    adjusted_evidence: float | None = None,
    evidence_source: str | None = None,
    evidence_beta: float | None = None,
    relative_evidence: float | None = None,
    absolute_evidence: float | None = None,
    absolute_weight: float | None = None,
    blend_method: str | None = None,
    absolute_enabled: bool | None = None,
) -> dict[str, Any]:
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
        "missing_reason": missing_reason,
        "is_valid_score": is_valid_score,
        "raw_evidence": raw_evidence,
        "prior_evidence": prior_evidence,
        "adjusted_evidence": adjusted_evidence,
        "evidence_source": evidence_source,
        "evidence_beta": evidence_beta,
        "relative_evidence": relative_evidence,
        "absolute_evidence": absolute_evidence,
        "absolute_weight": absolute_weight,
        "blend_method": blend_method,
        "absolute_enabled": absolute_enabled,
    }


def _reason_when_robust_z_missing(
    raw_value: float | None,
    median_value: float | None,
    n_valid: float | None,
    direction: str,
) -> str:
    if direction not in ("higher_better", "lower_better"):
        return "invalid_direction"
    if raw_value is None:
        return "missing_raw_value"
    if median_value is None:
        nv = safe_to_float(n_valid)
        if nv is None or nv <= 0.0:
            return "insufficient_peer_data"
        return "missing_group_median"
    return "missing_raw_value"


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
    absolute_enabled_flag = bool(getattr(factor_spec, "absolute_enabled", False))
    _aw_in = safe_to_float(getattr(factor_spec, "absolute_weight", None))
    absolute_weight_eff = clip_value(float(_aw_in if _aw_in is not None else 0.0), 0.0, 1.0)

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
    prior_evidence = safe_to_float(getattr(factor_spec, "evidence_prior", 0.0))
    if prior_evidence is None:
        prior_evidence = 0.0
    evidence_beta = safe_to_float(getattr(factor_spec, "evidence_beta", None))
    if evidence_beta is None:
        evidence_beta = 0.7
    raw_evidence: float | None = None
    adjusted_evidence: float | None = None
    evidence_source: str | None = None
    is_valid_score = False
    missing_reason: str | None = None

    if _structural_missing_from_row(row, factor_spec):
        return _base_score_dict(
            factor_name,
            category,
            raw_value,
            median_value,
            q25_value,
            q75_value,
            iqr_value,
            n_valid,
            peer_quality,
            None,
            None,
            None,
            None,
            "structural_missing",
            False,
            raw_evidence=None,
            prior_evidence=prior_evidence,
            adjusted_evidence=None,
            evidence_source="structural_missing",
            evidence_beta=evidence_beta,
            relative_evidence=None,
            absolute_evidence=None,
            absolute_weight=absolute_weight_eff,
            blend_method=None,
            absolute_enabled=absolute_enabled_flag,
        )

    if not enabled:
        return _base_score_dict(
            factor_name,
            category,
            raw_value,
            median_value,
            q25_value,
            q75_value,
            iqr_value,
            n_valid,
            peer_quality,
            None,
            None,
            None,
            None,
            "disabled_factor",
            False,
            raw_evidence=None,
            prior_evidence=prior_evidence,
            adjusted_evidence=None,
            evidence_source="disabled_factor",
            evidence_beta=evidence_beta,
            relative_evidence=None,
            absolute_evidence=None,
            absolute_weight=absolute_weight_eff,
            blend_method=None,
            absolute_enabled=absolute_enabled_flag,
        )

    if direction not in ("higher_better", "lower_better"):
        return _base_score_dict(
            factor_name,
            category,
            raw_value,
            median_value,
            q25_value,
            q75_value,
            iqr_value,
            n_valid,
            peer_quality,
            None,
            None,
            None,
            None,
            "invalid_direction",
            False,
            raw_evidence=None,
            prior_evidence=prior_evidence,
            adjusted_evidence=None,
            evidence_source="invalid_direction",
            evidence_beta=evidence_beta,
            relative_evidence=None,
            absolute_evidence=None,
            absolute_weight=absolute_weight_eff,
            blend_method=None,
            absolute_enabled=absolute_enabled_flag,
        )

    use_log_scale = bool(getattr(factor_spec, "use_log_scale", False))
    raw_rel, median_rel, iqr_rel, _q25_rel, _q75_rel = _prepare_relative_inputs_for_scoring(
        raw_value,
        median_value,
        q25_value,
        q75_value,
        iqr_value,
        use_log_scale,
    )
    relative_evidence = compute_signed_evidence(
        raw_value=raw_rel,
        median_value=median_rel,
        iqr_value=iqr_rel,
        direction=direction,
    )
    absolute_evidence: float | None = None
    if absolute_enabled_flag:
        _cap = safe_to_float(getattr(factor_spec, "absolute_cap", 3.0))
        if _cap is None or _cap <= 0.0:
            _cap = 3.0
        absolute_evidence = compute_absolute_evidence(
            raw_value=raw_value,
            direction=direction,
            good=safe_to_float(getattr(factor_spec, "absolute_good", None)),
            neutral=safe_to_float(getattr(factor_spec, "absolute_neutral", None)),
            bad=safe_to_float(getattr(factor_spec, "absolute_bad", None)),
            cap=float(_cap),
            mode=getattr(factor_spec, "absolute_mode", None),
        )

    raw_evidence, blend_method = blend_evidences(
        relative_evidence,
        absolute_evidence,
        absolute_weight_eff,
    )
    robust_z = raw_evidence

    if raw_evidence is None:
        missing_reason = _reason_when_robust_z_missing(
            raw_value, median_value, n_valid, direction
        )
        return _base_score_dict(
            factor_name,
            category,
            raw_value,
            median_value,
            q25_value,
            q75_value,
            iqr_value,
            n_valid,
            peer_quality,
            None,
            None,
            None,
            None,
            missing_reason,
            False,
            raw_evidence=None,
            prior_evidence=prior_evidence,
            adjusted_evidence=None,
            evidence_source="missing_evidence",
            evidence_beta=evidence_beta,
            relative_evidence=relative_evidence,
            absolute_evidence=absolute_evidence,
            absolute_weight=absolute_weight_eff,
            blend_method=blend_method,
            absolute_enabled=absolute_enabled_flag,
        )

    raw_score = evidence_to_score(evidence=raw_evidence, beta=evidence_beta, clip_evidence=4.0)
    if raw_score is None:
        return _base_score_dict(
            factor_name,
            category,
            raw_value,
            median_value,
            q25_value,
            q75_value,
            iqr_value,
            n_valid,
            peer_quality,
            robust_z,
            None,
            None,
            None,
            "missing_raw_value",
            False,
            raw_evidence=raw_evidence,
            prior_evidence=prior_evidence,
            adjusted_evidence=None,
            evidence_source="missing_evidence",
            evidence_beta=evidence_beta,
            relative_evidence=relative_evidence,
            absolute_evidence=absolute_evidence,
            absolute_weight=absolute_weight_eff,
            blend_method=blend_method,
            absolute_enabled=absolute_enabled_flag,
        )

    # No group-level confidence shrink: use blended raw evidence directly.
    confidence = 1.0
    adjusted_evidence = raw_evidence
    adjusted_score = raw_score
    evidence_source = "observed_evidence"

    is_valid_score = adjusted_score is not None
    if is_valid_score and iqr_value is None:
        missing_reason = "missing_group_iqr"
    else:
        missing_reason = None

    return _base_score_dict(
        factor_name,
        category,
        raw_value,
        median_value,
        q25_value,
        q75_value,
        iqr_value,
        n_valid,
        peer_quality,
        robust_z,
        raw_score,
        confidence,
        adjusted_score,
        missing_reason,
        is_valid_score,
        raw_evidence=raw_evidence,
        prior_evidence=prior_evidence,
        adjusted_evidence=adjusted_evidence,
        evidence_source=evidence_source,
        evidence_beta=evidence_beta,
        relative_evidence=relative_evidence,
        absolute_evidence=absolute_evidence,
        absolute_weight=absolute_weight_eff,
        blend_method=blend_method,
        absolute_enabled=absolute_enabled_flag,
    )


# Donor helpers (factor-score dimension; optional for downstream batch wiring).
from donor_imputation import estimate_missing_factor_score_from_donors  # noqa: E402

