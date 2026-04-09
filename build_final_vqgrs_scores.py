# -*- coding: utf-8 -*-
"""
Build final VQGRS scores from category-level inputs.

Input:
  - output/scoring/symbol_category_scores_latest.(parquet|csv)

Output:
  - output/scoring/final_vqgrs_scores_latest.(parquet|csv)

Category-level ``final_evidence_{V/Q/G/R/S}`` and ``final_evidence_track_*`` are retained for
diagnostics and backward compatibility only — **not used for the final numeric score decision**.

The final score is **only** a function of ``score_V``, ``score_Q``, ``score_G``, ``score_R``, and
``score_S`` (filled to 0.0 when absent in the row). Each ``final_score_track_*`` is a strict weighted
sum ``sum_c w_c * score_c`` with **no** renormalization over “available” categories; coverage and
missing-structure effects are assumed to be embedded in those category scores upstream.

``lti_pre_penalty`` picks the track-appropriate weighted sum; ``final_score`` equals ``lti_pre_penalty``
with no additional final-stage penalties.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from group_snapshot_utils import finalize_scoring_wide_input_df
from score_primitives import evidence_to_score


CORE_CATS = ["V", "Q", "G", "R", "S"]
FINAL_METHOD_LABEL = "track_weighted_category_scores_v1"


def _read_df(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    if path.suffix.lower() == ".csv":
        return pd.read_csv(path, low_memory=False)
    return pd.read_parquet(path)


def _load_track_inputs_from_factors_latest() -> pd.DataFrame:
    """
    Track raw inputs with safe fallback:
      - data/factors_latest.parquet
      - if missing: data/factors_latest.csv
      - if both missing/empty: return empty DataFrame with required columns
    """
    pq_path = Path("data") / "factors_latest.parquet"
    csv_path = Path("data") / "factors_latest.csv"

    fac: pd.DataFrame
    if pq_path.exists():
        fac = pd.read_parquet(pq_path)
    elif csv_path.exists():
        fac = pd.read_csv(csv_path, low_memory=False)
    else:
        print(f"WARNING: track inputs missing: neither {pq_path} nor {csv_path} found. Using empty track inputs.")
        return pd.DataFrame(
            columns=[
                "symbol",
                "as_of_date",
                "track_input_roic",
                "track_input_oper_margin",
                "track_input_ocf_ni",
                "track_input_revenue_yoy",
                "track_input_eps_yoy",
                "track_input_ocf_yoy",
                "track_input_debt_to_equity",
                "track_input_current_ratio",
                "track_input_interest_coverage",
                "track_input_beta",
                "track_input_pe",
                "track_input_ps",
                "track_input_ev_ebitda",
                "track_input_share_dilution",
            ]
        )
    if fac.empty:
        return pd.DataFrame(
            columns=[
                "symbol",
                "as_of_date",
                "track_input_roic",
                "track_input_oper_margin",
                "track_input_ocf_ni",
                "track_input_revenue_yoy",
                "track_input_eps_yoy",
                "track_input_ocf_yoy",
                "track_input_debt_to_equity",
                "track_input_current_ratio",
                "track_input_interest_coverage",
                "track_input_beta",
                "track_input_pe",
                "track_input_ps",
                "track_input_ev_ebitda",
                "track_input_share_dilution",
            ]
        )

    if "as_of_date" not in fac.columns:
        if "asOfDate" in fac.columns:
            fac = fac.copy()
            fac["as_of_date"] = fac["asOfDate"]
        else:
            return pd.DataFrame(
                columns=[
                    "symbol",
                    "as_of_date",
                    "track_input_roic",
                    "track_input_oper_margin",
                    "track_input_ocf_ni",
                    "track_input_revenue_yoy",
                    "track_input_eps_yoy",
                    "track_input_ocf_yoy",
                    "track_input_debt_to_equity",
                    "track_input_current_ratio",
                    "track_input_interest_coverage",
                    "track_input_beta",
                    "track_input_pe",
                    "track_input_ps",
                    "track_input_ev_ebitda",
                    "track_input_share_dilution",
                ]
            )
    if "symbol" not in fac.columns:
        return pd.DataFrame(
            columns=[
                "symbol",
                "as_of_date",
                "track_input_roic",
                "track_input_oper_margin",
                "track_input_ocf_ni",
                "track_input_revenue_yoy",
                "track_input_eps_yoy",
                "track_input_ocf_yoy",
                "track_input_debt_to_equity",
                "track_input_current_ratio",
                "track_input_interest_coverage",
                "track_input_beta",
                "track_input_pe",
                "track_input_ps",
                "track_input_ev_ebitda",
                "track_input_share_dilution",
            ]
        )

    needed = [
        "symbol",
        "as_of_date",
        "ROIC",
        "Oper. Margin",
        "OCF/NI",
        "Revenue YoY",
        "EPS YoY",
        "OCF YoY",
        "Debt/Eq",
        "Current Ratio",
        "Interest Coverage",
        "Beta",
        "P/E",
        "P/S",
        "EV/EBITDA",
        "Share Dilution",
    ]
    for c in needed:
        if c not in fac.columns:
            fac[c] = np.nan
    work = fac[needed].copy()
    work["symbol"] = work["symbol"].astype(str).str.strip().str.upper()
    work["as_of_date"] = pd.to_datetime(work["as_of_date"], errors="coerce")
    work = work.dropna(subset=["as_of_date"])
    if work.empty:
        return pd.DataFrame(columns=["symbol", "as_of_date"])

    latest_dt = work["as_of_date"].max()
    src = pq_path if pq_path.exists() else csv_path
    print(f"Track raw source: {src} | max(as_of_date)={latest_dt}")
    work = work.loc[work["as_of_date"] == latest_dt].copy()

    num_cols = [c for c in needed if c not in ("symbol", "as_of_date")]
    for c in num_cols:
        work[c] = pd.to_numeric(work[c], errors="coerce")
    work["as_of_date"] = work["as_of_date"].dt.strftime("%Y-%m-%d")

    work = (
        work.groupby(["symbol", "as_of_date"], as_index=False, dropna=False)
        .agg({c: "median" for c in num_cols})
    )

    work = work.rename(
        columns={
            "ROIC": "track_input_roic",
            "Oper. Margin": "track_input_oper_margin",
            "OCF/NI": "track_input_ocf_ni",
            "Revenue YoY": "track_input_revenue_yoy",
            "EPS YoY": "track_input_eps_yoy",
            "OCF YoY": "track_input_ocf_yoy",
            "Debt/Eq": "track_input_debt_to_equity",
            "Current Ratio": "track_input_current_ratio",
            "Interest Coverage": "track_input_interest_coverage",
            "Beta": "track_input_beta",
            "P/E": "track_input_pe",
            "P/S": "track_input_ps",
            "EV/EBITDA": "track_input_ev_ebitda",
            "Share Dilution": "track_input_share_dilution",
        }
    )
    return work


def _load_track_a_inputs_from_group_a_snapshot() -> pd.DataFrame:
    """
    Prepare raw/peer valuation inputs for upcoming Track A rule refresh.
    """
    pq_path = Path("output") / "group_a" / "group_a_snapshot_latest.parquet"
    csv_path = Path("output") / "group_a" / "group_a_snapshot_latest.csv"

    out_cols = [
        "symbol",
        "as_of_date",
        "track_a_raw_pe",
        "track_a_raw_ps",
        "track_a_raw_ev_ebitda",
        "track_a_peer_pe_median",
        "track_a_peer_ps_median",
        "track_a_peer_ev_ebitda_median",
        "track_a_peer_pe_n_valid",
        "track_a_peer_ps_n_valid",
        "track_a_peer_ev_ebitda_n_valid",
    ]

    snap = _read_df(pq_path)
    src = pq_path
    if snap.empty:
        snap = _read_df(csv_path)
        src = csv_path
    if snap.empty:
        return pd.DataFrame(columns=out_cols)

    if "symbol" not in snap.columns:
        return pd.DataFrame(columns=out_cols)
    if "as_of_date" not in snap.columns:
        if "asOfDate" in snap.columns:
            snap = snap.copy()
            snap["as_of_date"] = snap["asOfDate"]
        else:
            return pd.DataFrame(columns=out_cols)

    use_map = {
        "P/E": "track_a_raw_pe",
        "P/S": "track_a_raw_ps",
        "EV/EBITDA": "track_a_raw_ev_ebitda",
        "rep__P/E__median": "track_a_peer_pe_median",
        "rep__P/S__median": "track_a_peer_ps_median",
        "rep__EV/EBITDA__median": "track_a_peer_ev_ebitda_median",
        "rep__P/E__n_valid": "track_a_peer_pe_n_valid",
        "rep__P/S__n_valid": "track_a_peer_ps_n_valid",
        "rep__EV/EBITDA__n_valid": "track_a_peer_ev_ebitda_n_valid",
    }

    base_cols = ["symbol", "as_of_date"]
    present = [c for c in use_map.keys() if c in snap.columns]
    work = snap[base_cols + present].copy()
    work["symbol"] = work["symbol"].astype(str).str.strip().str.upper()
    work["as_of_date"] = pd.to_datetime(work["as_of_date"], errors="coerce")
    work = work.dropna(subset=["as_of_date"])
    if work.empty:
        return pd.DataFrame(columns=out_cols)
    work["as_of_date"] = work["as_of_date"].dt.strftime("%Y-%m-%d")

    for src_col in present:
        work[src_col] = pd.to_numeric(work[src_col], errors="coerce")

    work = work.rename(columns=use_map)
    work = (
        work.groupby(["symbol", "as_of_date"], as_index=False, dropna=False)
        .agg({c: "median" for c in work.columns if c not in ("symbol", "as_of_date")})
    )
    for c in out_cols:
        if c not in work.columns:
            work[c] = np.nan
    print(f"Track A raw source: {src} | rows={len(work)}")
    return work[out_cols].copy()


def _save_df(df: pd.DataFrame, parquet_path: Path, csv_path: Path) -> None:
    parquet_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        df.to_parquet(parquet_path, index=False)
    except Exception as e:
        print(f"WARNING: failed to save parquet: {parquet_path} ({e})")
    try:
        df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    except PermissionError as e:
        print(f"WARNING: failed to save CSV (file may be open): {csv_path} ({e})")
    except Exception as e:
        print(f"WARNING: failed to save CSV: {csv_path} ({e})")


def _simple_mean_category_evidences(df: pd.DataFrame) -> pd.Series:
    """Mean of ``final_evidence_V``…``final_evidence_S``; NaN if all five are missing."""
    cols = [f"final_evidence_{c}" for c in CORE_CATS]
    m = pd.concat([pd.to_numeric(df[c], errors="coerce") for c in cols], axis=1)
    m.columns = cols
    has_any = m.notna().any(axis=1)
    mean_v = m.mean(axis=1, skipna=True)
    return mean_v.where(has_any, np.nan).astype(float)


def _simple_mean_category_scores(df: pd.DataFrame) -> pd.Series:
    """Equal-weight mean of the five category scores: (V+Q+G+R+S)/5, with NaNs treated as 0."""
    cols = [f"score_{c}" for c in CORE_CATS]
    m = pd.concat([pd.to_numeric(df[c], errors="coerce").fillna(0.0) for c in cols], axis=1)
    m.columns = cols
    return (m.sum(axis=1) / 5.0).clip(lower=0.0, upper=100.0).astype(float)


def _compute_track_weighted_score(df: pd.DataFrame, weights: dict[str, float]) -> pd.Series:
    """
    Strict weighted sum of score_V..score_S (0–100 scale).

    final_score track columns must be a function of **all five** category scores, not only those
    that happen to be non-NaN in the row. Missing categories are represented as low scores upstream;
    here any remaining NaNs are coerced to 0.0 before applying weights. **No denominator
    renormalization** — weights always apply to the full five-vector.
    """
    score_cols = {c: f"score_{c}" for c in CORE_CATS}
    s = pd.DataFrame(
        {c: pd.to_numeric(df[col], errors="coerce").fillna(0.0).clip(0.0, 100.0) for c, col in score_cols.items()}
    )
    w = pd.Series({c: float(weights.get(c, 0.0)) for c in CORE_CATS}, dtype=float)
    out = (s.mul(w, axis=1)).sum(axis=1)
    return out.clip(lower=0.0, upper=100.0).astype(float)


def _compute_track_weighted_evidence(df: pd.DataFrame, weights: dict[str, float]) -> pd.Series:
    """
    Track-weighted blend of final category evidences (diagnostic / backward compatibility).

    **Not used for final score decision** — see ``_compute_track_weighted_score`` and ``final_score``.
    Retains missing-aware renormalization for evidence-only reporting.
    """
    ev_cols = {c: f"final_evidence_{c}" for c in CORE_CATS}
    s = pd.DataFrame({c: pd.to_numeric(df[col], errors="coerce") for c, col in ev_cols.items()})
    w = pd.Series({c: float(weights.get(c, 0.0)) for c in CORE_CATS}, dtype=float)
    valid = s.notna().astype(float)
    denom = valid.mul(w, axis=1).sum(axis=1)
    num = s.fillna(0.0).mul(w, axis=1).sum(axis=1)
    out = num / denom.replace(0.0, np.nan)
    return out.where(denom > 0.0, np.nan).astype(float)


def _compute_weighted_confidence_for_profile(
    df: pd.DataFrame, weights: dict[str, float], *, conf_col_prefix: str = "final_conf_"
) -> pd.Series:
    """Weighted mean of category confidence with missing-aware renormalization."""
    conf_cols = {c: f"{conf_col_prefix}{c}" for c in CORE_CATS}
    m = pd.DataFrame()
    for c, col in conf_cols.items():
        if col in df.columns:
            m[c] = pd.to_numeric(df[col], errors="coerce")
        else:
            # Backward-compatible fallback for older category files.
            m[c] = pd.to_numeric(df.get(f"final_conf_{c}"), errors="coerce")
    w = pd.Series({c: float(weights.get(c, 0.0)) for c in CORE_CATS}, dtype=float)
    valid = m.notna().astype(float)
    denom = valid.mul(w, axis=1).sum(axis=1)
    num = m.fillna(0.0).mul(w, axis=1).sum(axis=1)
    out = num / denom.replace(0.0, np.nan)
    return out.where(denom > 0.0, 0.0).clip(lower=0.0, upper=1.0).astype(float)


def _add_core_category_diagnostics(out: pd.DataFrame) -> None:
    """
    Core diagnostics from ``category_missing_class_{V..S}`` (one label per category column).

    Count rules per category column:
      observed_only -> valid_core_count += 1
      partial_observed -> valid_core_count += 1, partial_observed_core_count += 1
      structural_only -> structural_only_core_count += 1
      incidental_only -> incidental_only_core_count += 1
      mixed_missing -> mixed_missing_core_count += 1

    Downstream aliases (legacy names): structural_missing_core_count == structural_only_core_count,
    incidental_missing_core_count == incidental_only_core_count (mixed/partial not folded in).
    """
    idx = out.index
    valid_n = pd.Series(0, index=idx, dtype=np.int64)
    struct_only_n = pd.Series(0, index=idx, dtype=np.int64)
    inc_only_n = pd.Series(0, index=idx, dtype=np.int64)
    mixed_n = pd.Series(0, index=idx, dtype=np.int64)
    partial_n = pd.Series(0, index=idx, dtype=np.int64)
    for c in CORE_CATS:
        col = f"category_missing_class_{c}"
        if col not in out.columns:
            continue
        cls = out[col].astype(str).str.strip().str.lower().replace({"nan": "", "none": "", "<na>": ""})
        valid_n = valid_n + (cls.eq("observed_only") | cls.eq("partial_observed")).astype(np.int64)
        partial_n = partial_n + cls.eq("partial_observed").astype(np.int64)
        struct_only_n = struct_only_n + cls.eq("structural_only").astype(np.int64)
        inc_only_n = inc_only_n + cls.eq("incidental_only").astype(np.int64)
        mixed_n = mixed_n + cls.eq("mixed_missing").astype(np.int64)

    out["valid_core_count"] = valid_n.astype(int)
    out["structural_only_core_count"] = struct_only_n.astype(int)
    out["incidental_only_core_count"] = inc_only_n.astype(int)
    out["mixed_missing_core_count"] = mixed_n.astype(int)
    out["partial_observed_core_count"] = partial_n.astype(int)
    out["structural_missing_core_count"] = out["structural_only_core_count"]
    out["incidental_missing_core_count"] = out["incidental_only_core_count"]

    cov_parts: list[pd.Series] = []
    for c in CORE_CATS:
        sc = pd.to_numeric(out[f"score_{c}"], errors="coerce")
        mc = (
            pd.to_numeric(out[f"main_coverage_{c}"], errors="coerce")
            if f"main_coverage_{c}" in out.columns
            else pd.Series(np.nan, index=out.index, dtype=float)
        )
        cov_parts.append(mc.where(sc.notna(), np.nan))
    cov_df = pd.concat(cov_parts, axis=1)
    mcf = cov_df.min(axis=1, skipna=True)
    out["main_cov_floor"] = mcf.fillna(1.0).astype(float)


def _apply_penalties_and_hard_stop(df: pd.DataFrame) -> pd.DataFrame:
    """
    Final score equals track-selected category weighted sum only.

    Coverage / incidental / confidence effects are assumed embedded in category scores upstream;
    this stage does not subtract additional penalties. ``penalty_total`` is kept at 0 for legacy
    column compatibility; ``hard_stop_triggered`` defaults false (no NaN final_score from this step).
    """
    out = df.copy()
    lti = pd.to_numeric(out["lti_pre_penalty"], errors="coerce").fillna(0.0).clip(0.0, 100.0)
    out["final_score"] = lti.astype(float)
    out["penalty_total"] = 0.0
    out["hard_stop_triggered"] = False
    out["investment_warning"] = ""
    return out


def _evidence_series_to_score(ev: pd.Series) -> pd.Series:
    def _one(x: Any) -> float:
        try:
            if x is None or pd.isna(x):
                return np.nan
        except (TypeError, ValueError):
            return np.nan
        s = evidence_to_score(float(x))
        return np.nan if s is None else float(s)

    return ev.map(_one).astype(float)


def _assign_track_from_raw_inputs(out: pd.DataFrame) -> pd.DataFrame:
    def _series_or_nan(df: pd.DataFrame, col: str) -> pd.Series:
        if col in df.columns:
            return pd.to_numeric(df[col], errors="coerce")
        return pd.Series(np.nan, index=df.index, dtype=float)

    # ----------------------
    # Track A (value)
    # ----------------------
    raw_pe = _series_or_nan(out, "track_a_raw_pe")
    raw_ps = _series_or_nan(out, "track_a_raw_ps")
    raw_ev = _series_or_nan(out, "track_a_raw_ev_ebitda")
    med_pe = _series_or_nan(out, "track_a_peer_pe_median")
    med_ps = _series_or_nan(out, "track_a_peer_ps_median")
    med_ev = _series_or_nan(out, "track_a_peer_ev_ebitda_median")
    n_pe = _series_or_nan(out, "track_a_peer_pe_n_valid")
    n_ps = _series_or_nan(out, "track_a_peer_ps_n_valid")
    n_ev = _series_or_nan(out, "track_a_peer_ev_ebitda_n_valid")
    roic = _series_or_nan(out, "track_input_roic")
    icov = _series_or_nan(out, "track_input_interest_coverage")

    valid_pe = (raw_pe > 0) & (med_pe > 0) & (n_pe >= 5)
    valid_ps = (raw_ps > 0) & (med_ps > 0) & (n_ps >= 5)
    valid_ev = (raw_ev > 0) & (med_ev > 0) & (n_ev >= 5)

    disc_pe = pd.Series(np.nan, index=out.index, dtype=float)
    disc_ps = pd.Series(np.nan, index=out.index, dtype=float)
    disc_ev = pd.Series(np.nan, index=out.index, dtype=float)
    disc_pe.loc[valid_pe] = 1.0 - (raw_pe.loc[valid_pe] / med_pe.loc[valid_pe])
    disc_ps.loc[valid_ps] = 1.0 - (raw_ps.loc[valid_ps] / med_ps.loc[valid_ps])
    disc_ev.loc[valid_ev] = 1.0 - (raw_ev.loc[valid_ev] / med_ev.loc[valid_ev])

    cheap_pe = valid_pe & (disc_pe >= 0.15)
    cheap_ps = valid_ps & (disc_ps >= 0.15)
    cheap_ev = valid_ev & (disc_ev >= 0.15)

    out["track_A_discount_pe"] = disc_pe.astype(float)
    out["track_A_discount_ps"] = disc_ps.astype(float)
    out["track_A_discount_ev_ebitda"] = disc_ev.astype(float)
    out["track_A_valuation_valid_count"] = (valid_pe.astype(int) + valid_ps.astype(int) + valid_ev.astype(int)).astype(float)
    out["track_A_cheap_count"] = (cheap_pe.astype(int) + cheap_ps.astype(int) + cheap_ev.astype(int)).astype(float)
    out["track_A_quality_guard_count"] = ((roic >= 0.08).astype(int) + (icov >= 2.0).astype(int)).astype(float)
    out["is_track_A_candidate"] = (
        (out["track_A_valuation_valid_count"] >= 2.0)
        & (out["track_A_cheap_count"] >= 2.0)
        & (out["track_A_quality_guard_count"] >= 1.0)
    ).astype(bool)

    # ----------------------
    # Track B (quality)
    # ----------------------
    opm = _series_or_nan(out, "track_input_oper_margin")
    ocf_ni = _series_or_nan(out, "track_input_ocf_ni")
    debt = _series_or_nan(out, "track_input_debt_to_equity")
    current = _series_or_nan(out, "track_input_current_ratio")
    beta = _series_or_nan(out, "track_input_beta")

    b_quality_count = (roic >= 0.15).astype(int) + (opm >= 0.10).astype(int) + (ocf_ni >= 0.80).astype(int)
    b_risk_count = (debt <= 1.00).astype(int) + (current >= 1.50).astype(int) + (icov >= 3.0).astype(int)
    b_stability_count = (beta <= 1.20).astype(int)

    out["track_B_quality_count"] = b_quality_count.astype(float)
    out["track_B_risk_count"] = b_risk_count.astype(float)
    out["track_B_stability_count"] = b_stability_count.astype(float)
    out["is_track_B_candidate"] = (
        (roic >= 0.15)
        & (out["track_B_quality_count"] >= 2.0)
        & (out["track_B_risk_count"] >= 2.0)
        & (out["track_B_stability_count"] >= 1.0)
    ).astype(bool)

    # ----------------------
    # Track C (growth)
    # ----------------------
    rev = _series_or_nan(out, "track_input_revenue_yoy")
    eps_yoy = _series_or_nan(out, "track_input_eps_yoy")
    ocf_yoy = _series_or_nan(out, "track_input_ocf_yoy")
    dilution = _series_or_nan(out, "track_input_share_dilution")

    c_growth_count = (eps_yoy > 0).astype(int) + (ocf_yoy > 0).astype(int)
    c_quality_count = (ocf_ni >= 0.60).astype(int) + (opm >= 0.00).astype(int)
    c_dilution_soft_fail = (dilution >= 0.05).fillna(False)

    out["track_C_growth_count"] = c_growth_count.astype(float)
    out["track_C_quality_guard_count"] = c_quality_count.astype(float)
    out["track_C_dilution_soft_fail"] = c_dilution_soft_fail.astype(bool)
    out["is_track_C_candidate"] = (
        (rev >= 0.20)
        & (out["track_C_growth_count"] >= 1.0)
        & (out["track_C_quality_guard_count"] >= 1.0)
    ).astype(bool)

    out["track_conflict_count"] = (
        out["is_track_A_candidate"].astype(int)
        + out["is_track_B_candidate"].astype(int)
        + out["is_track_C_candidate"].astype(int)
    ).astype(float)

    # Priority: B > C > A > N
    out["assigned_track"] = np.select(
        [
            out["is_track_B_candidate"],
            out["is_track_C_candidate"],
            out["is_track_A_candidate"],
        ],
        ["B", "C", "A"],
        default="N",
    )

    reasons = np.full(len(out), "N: no track threshold met", dtype=object)
    b_mask = out["assigned_track"] == "B"
    c_mask = out["assigned_track"] == "C"
    a_mask = out["assigned_track"] == "A"
    reasons[b_mask] = (
        "B: quality="
        + out.loc[b_mask, "track_B_quality_count"].astype(int).astype(str)
        + " risk="
        + out.loc[b_mask, "track_B_risk_count"].astype(int).astype(str)
        + " stability="
        + out.loc[b_mask, "track_B_stability_count"].astype(int).astype(str)
    )
    reasons[c_mask] = (
        "C: revenue gate pass, growth="
        + out.loc[c_mask, "track_C_growth_count"].astype(int).astype(str)
        + " quality="
        + out.loc[c_mask, "track_C_quality_guard_count"].astype(int).astype(str)
    )
    reasons[a_mask] = (
        "A: valid="
        + out.loc[a_mask, "track_A_valuation_valid_count"].astype(int).astype(str)
        + " cheap="
        + out.loc[a_mask, "track_A_cheap_count"].astype(int).astype(str)
        + " quality_guard="
        + out.loc[a_mask, "track_A_quality_guard_count"].astype(int).astype(str)
    )
    out["track_reason"] = pd.Series(reasons, index=out.index, dtype=object)
    return out


def build_final_vqgrs_scores_df(df_cat: pd.DataFrame) -> pd.DataFrame:
    if df_cat is None or df_cat.empty:
        return pd.DataFrame()

    required = {"symbol", "as_of_date", *[f"final_evidence_{c}" for c in CORE_CATS]}
    missing = [c for c in required if c not in df_cat.columns]
    if missing:
        raise ValueError(f"Missing required input columns: {missing}")

    out = df_cat[["symbol", "as_of_date"]].copy()
    out["symbol"] = out["symbol"].astype(str).str.strip().str.upper()
    out["as_of_date"] = pd.to_datetime(out["as_of_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    for c in (
        "track_input_roic",
        "track_input_oper_margin",
        "track_input_ocf_ni",
        "track_input_revenue_yoy",
        "track_input_debt_to_equity",
        "track_input_current_ratio",
        "track_input_interest_coverage",
        "track_input_beta",
        "track_input_pe",
        "track_input_ps",
        "track_input_ev_ebitda",
        "track_input_eps_yoy",
        "track_input_ocf_yoy",
        "track_input_share_dilution",
    ):
        if c in df_cat.columns:
            out[c] = pd.to_numeric(df_cat[c], errors="coerce")

    for c in CORE_CATS:
        out[f"final_evidence_{c}"] = pd.to_numeric(df_cat[f"final_evidence_{c}"], errors="coerce")
        score_col = f"score_{c}"
        if score_col in df_cat.columns:
            out[score_col] = (
                pd.to_numeric(df_cat[score_col], errors="coerce").fillna(0.0).clip(lower=0.0, upper=100.0).astype(float)
            )
        else:
            out[score_col] = (
                pd.to_numeric(_evidence_series_to_score(out[f"final_evidence_{c}"]), errors="coerce")
                .fillna(0.0)
                .clip(0.0, 100.0)
            )

        # Debug passthroughs (when present).
        mc = f"main_coverage_{c}"
        ds = f"dominant_signal_{c}"
        if mc in df_cat.columns:
            out[mc] = pd.to_numeric(df_cat[mc], errors="coerce").fillna(0.0).astype(float)
        if ds in df_cat.columns:
            out[ds] = df_cat[ds].fillna("balanced").astype(object)
        fc = f"final_conf_{c}"
        if fc in df_cat.columns:
            out[fc] = pd.to_numeric(df_cat[fc], errors="coerce").fillna(0.0).clip(lower=0.0, upper=1.0).astype(float)
        else:
            out[fc] = pd.to_numeric(df_cat.get(f"category_confidence_{c}"), errors="coerce").fillna(0.0).clip(lower=0.0, upper=1.0).astype(float)
        cc = f"category_confidence_{c}"
        if cc in df_cat.columns:
            out[cc] = pd.to_numeric(df_cat[cc], errors="coerce").fillna(0.0).clip(lower=0.0, upper=1.0).astype(float)
        else:
            out[cc] = pd.to_numeric(df_cat.get(f"conf_{c}"), errors="coerce").fillna(0.0).clip(lower=0.0, upper=1.0).astype(float)
        cmc = f"category_missing_class_{c}"
        if cmc in df_cat.columns:
            out[cmc] = df_cat[cmc].astype(object)
        else:
            out[cmc] = pd.NA

    # Track inputs from factors_latest raw-source join, injected in main().
    for c in (
        "track_input_roic",
        "track_input_oper_margin",
        "track_input_ocf_ni",
        "track_input_revenue_yoy",
        "track_input_debt_to_equity",
        "track_input_current_ratio",
        "track_input_interest_coverage",
        "track_input_beta",
        "track_input_pe",
        "track_input_ps",
        "track_input_ev_ebitda",
        "track_input_eps_yoy",
        "track_input_ocf_yoy",
        "track_input_share_dilution",
    ):
        if c not in out.columns:
            out[c] = np.nan
    for c in (
        "track_a_raw_pe",
        "track_a_raw_ps",
        "track_a_raw_ev_ebitda",
        "track_a_peer_pe_median",
        "track_a_peer_ps_median",
        "track_a_peer_ev_ebitda_median",
        "track_a_peer_pe_n_valid",
        "track_a_peer_ps_n_valid",
        "track_a_peer_ev_ebitda_n_valid",
    ):
        if c in df_cat.columns:
            out[c] = pd.to_numeric(df_cat[c], errors="coerce")
        else:
            out[c] = np.nan

    # 1) Track candidate assignment
    out = _assign_track_from_raw_inputs(out)

    # 2) Track diagnostics (evidence blends) vs strict score tracks (used for final_score only)
    ev_agg = _simple_mean_category_evidences(out)
    out["final_evidence_equal"] = ev_agg
    out["final_evidence_track_A"] = _compute_track_weighted_evidence(out, {"V": 0.40, "Q": 0.20, "G": 0.10, "R": 0.20, "S": 0.10})
    out["final_evidence_track_B"] = _compute_track_weighted_evidence(out, {"V": 0.25, "Q": 0.30, "G": 0.10, "R": 0.15, "S": 0.20})
    out["final_evidence_track_C"] = _compute_track_weighted_evidence(out, {"V": 0.20, "Q": 0.20, "G": 0.35, "R": 0.15, "S": 0.10})
    out["final_evidence_track_N"] = _compute_track_weighted_evidence(out, {"V": 0.20, "Q": 0.20, "G": 0.20, "R": 0.20, "S": 0.20})
    out["final_evidence_track_method"] = "track_weighted_category_evidence_v1"

    # Equal-weight mean of five category scores (all NaNs -> 0); reporting only.
    score_agg = _simple_mean_category_scores(out)
    out["final_score_equal"] = score_agg
    w_a = {"V": 0.40, "Q": 0.20, "G": 0.10, "R": 0.20, "S": 0.10}
    w_b = {"V": 0.25, "Q": 0.30, "G": 0.10, "R": 0.15, "S": 0.20}
    w_c = {"V": 0.20, "Q": 0.20, "G": 0.35, "R": 0.15, "S": 0.10}
    w_n = {"V": 0.20, "Q": 0.20, "G": 0.20, "R": 0.20, "S": 0.20}
    # Strict weighted sums of score_V..score_S (no renormalization); sole input to lti_pre_penalty / final_score.
    out["final_score_track_A"] = _compute_track_weighted_score(out, w_a)
    out["final_score_track_B"] = _compute_track_weighted_score(out, w_b)
    out["final_score_track_C"] = _compute_track_weighted_score(out, w_c)
    out["final_score_track_N"] = _compute_track_weighted_score(out, w_n)
    out["final_score_transform_stage"] = "track_weighted_category_scores_v1"
    out["lti_confidence_track_A"] = _compute_weighted_confidence_for_profile(out, w_a, conf_col_prefix="final_conf_")
    out["lti_confidence_track_B"] = _compute_weighted_confidence_for_profile(out, w_b, conf_col_prefix="final_conf_")
    out["lti_confidence_track_C"] = _compute_weighted_confidence_for_profile(out, w_c, conf_col_prefix="final_conf_")
    out["lti_confidence_track_N"] = _compute_weighted_confidence_for_profile(out, w_n, conf_col_prefix="final_conf_")

    selected_profile = out["assigned_track"].where(out["assigned_track"].isin(["A", "B", "C"]), "N")
    out["selected_weight_profile"] = selected_profile.astype(str)
    out["lti_pre_penalty"] = np.select(
        [
            selected_profile == "A",
            selected_profile == "B",
            selected_profile == "C",
        ],
        [
            pd.to_numeric(out["final_score_track_A"], errors="coerce"),
            pd.to_numeric(out["final_score_track_B"], errors="coerce"),
            pd.to_numeric(out["final_score_track_C"], errors="coerce"),
        ],
        default=pd.to_numeric(out["final_score_track_N"], errors="coerce"),
    )
    out["lti_pre_penalty"] = pd.to_numeric(out["lti_pre_penalty"], errors="coerce").clip(lower=0.0, upper=100.0).astype(float)

    out["lti_confidence"] = np.select(
        [
            selected_profile == "A",
            selected_profile == "B",
            selected_profile == "C",
        ],
        [
            pd.to_numeric(out["lti_confidence_track_A"], errors="coerce"),
            pd.to_numeric(out["lti_confidence_track_B"], errors="coerce"),
            pd.to_numeric(out["lti_confidence_track_C"], errors="coerce"),
        ],
        default=pd.to_numeric(out["lti_confidence_track_N"], errors="coerce"),
    )
    out["lti_confidence"] = pd.to_numeric(out["lti_confidence"], errors="coerce").fillna(0.0).clip(lower=0.0, upper=1.0).astype(float)
    out["lti_confidence_bucket"] = np.select(
        [out["lti_confidence"] >= 0.75, out["lti_confidence"] >= 0.45],
        ["HIGH", "MEDIUM"],
        default="LOW",
    ).astype(object)

    _add_core_category_diagnostics(out)

    lti_pre_nan = pd.to_numeric(out["lti_pre_penalty"], errors="coerce").isna()
    missing_core = pd.DataFrame(
        {c: pd.to_numeric(out[f"final_evidence_{c}"], errors="coerce") for c in CORE_CATS}
    ).isna().sum(axis=1)
    io_cnt = pd.to_numeric(out["incidental_only_core_count"], errors="coerce").fillna(0.0)
    mm_cnt = pd.to_numeric(out["mixed_missing_core_count"], errors="coerce").fillna(0.0)
    po_cnt = pd.to_numeric(out["partial_observed_core_count"], errors="coerce").fillna(0.0)
    mcfloor = pd.to_numeric(out["main_cov_floor"], errors="coerce").fillna(1.0)
    low_main_cov = mcfloor < 0.67
    low_factor_conf = pd.to_numeric(out["lti_confidence"], errors="coerce").fillna(0.0) < 0.45

    reason_parts: list[str] = []
    for ix in out.index:
        parts: list[str] = []
        if bool(lti_pre_nan.loc[ix]):
            parts.append("insufficient_core_data")
        if int(missing_core.loc[ix]) > 0:
            parts.append("missing_core_categories")
        if float(io_cnt.loc[ix]) > 0.0:
            parts.append("incidental_missing_core")
        if float(mm_cnt.loc[ix]) > 0.0:
            parts.append("mixed_missing_core")
        if float(po_cnt.loc[ix]) > 0.0:
            parts.append("partial_observed_core")
        if bool(low_main_cov.loc[ix]):
            parts.append("low_main_coverage")
        if bool(low_factor_conf.loc[ix]):
            parts.append("low_factor_confidence")
        reason_parts.append("|".join(parts) if parts else "none")
    out["lti_uncertainty_reason"] = pd.Series(reason_parts, index=out.index, dtype=object)
    out["lti_confidence_model_version"] = "lti_conf_weighted_final_conf_v1"

    # 3) final_score := lti_pre_penalty (no additional final-stage penalty)
    out = _apply_penalties_and_hard_stop(out)
    out["final_score_method"] = FINAL_METHOD_LABEL

    out_cols = [
        "symbol",
        "as_of_date",
        "assigned_track",
        "final_score",
        "final_score_method",
        "selected_weight_profile",
        "final_evidence_equal",
        "final_score_equal",
        "final_evidence_track_A",
        "final_score_track_A",
        "final_evidence_track_B",
        "final_score_track_B",
        "final_evidence_track_C",
        "final_score_track_C",
        "final_evidence_track_N",
        "final_score_track_N",
        "final_evidence_track_method",
        "final_score_transform_stage",
        "lti_confidence_track_A",
        "lti_confidence_track_B",
        "lti_confidence_track_C",
        "lti_confidence_track_N",
        "lti_confidence",
        "lti_confidence_bucket",
        "lti_uncertainty_reason",
        "lti_confidence_model_version",
        "valid_core_count",
        "structural_only_core_count",
        "incidental_only_core_count",
        "mixed_missing_core_count",
        "partial_observed_core_count",
        "structural_missing_core_count",
        "incidental_missing_core_count",
        "main_cov_floor",
        "score_V",
        "score_Q",
        "score_G",
        "score_R",
        "score_S",
        "final_evidence_V",
        "final_evidence_Q",
        "final_evidence_G",
        "final_evidence_R",
        "final_evidence_S",
        "track_reason",
        "lti_pre_penalty",
        "penalty_total",
        "track_B_quality_count",
        "track_B_risk_count",
        "track_B_stability_count",
        "track_A_discount_pe",
        "track_A_discount_ps",
        "track_A_discount_ev_ebitda",
        "track_A_valuation_valid_count",
        "track_A_cheap_count",
        "track_A_quality_guard_count",
        "track_C_growth_count",
        "track_C_quality_guard_count",
        "track_C_dilution_soft_fail",
        "track_conflict_count",
        "is_track_A_candidate",
        "is_track_B_candidate",
        "is_track_C_candidate",
        "track_input_roic",
        "track_input_oper_margin",
        "track_input_ocf_ni",
        "track_input_revenue_yoy",
        "track_input_debt_to_equity",
        "track_input_current_ratio",
        "track_input_interest_coverage",
        "track_input_beta",
        "track_input_pe",
        "track_input_ps",
        "track_input_ev_ebitda",
        "track_input_eps_yoy",
        "track_input_ocf_yoy",
        "track_input_share_dilution",
        "track_a_raw_pe",
        "track_a_raw_ps",
        "track_a_raw_ev_ebitda",
        "track_a_peer_pe_median",
        "track_a_peer_ps_median",
        "track_a_peer_ev_ebitda_median",
        "track_a_peer_pe_n_valid",
        "track_a_peer_ps_n_valid",
        "track_a_peer_ev_ebitda_n_valid",
        "investment_warning",
        "hard_stop_triggered",
        "main_coverage_V",
        "main_coverage_Q",
        "main_coverage_G",
        "main_coverage_R",
        "main_coverage_S",
        "dominant_signal_V",
        "dominant_signal_Q",
        "dominant_signal_G",
        "dominant_signal_R",
        "dominant_signal_S",
    ]
    out_cols = [c for c in out_cols if c in out.columns]
    out_cols = list(dict.fromkeys(out_cols))
    return out[out_cols].copy()


def main(input_dir: str | Path = "output", output_dir: str | Path = "output/scoring") -> None:
    input_dir = Path(input_dir)
    output_dir = Path(output_dir)

    parquet_in = input_dir / "scoring" / "symbol_category_scores_latest.parquet"
    csv_in = input_dir / "scoring" / "symbol_category_scores_latest.csv"

    df = _read_df(parquet_in)
    if df.empty:
        df = _read_df(csv_in)
    if df.empty:
        print("No symbol_category_scores_latest input found.")
        return

    def _normalize_key_columns(x: pd.DataFrame) -> pd.DataFrame:
        if x is None or x.empty:
            return pd.DataFrame(columns=["symbol", "as_of_date"])
        y = x.copy()
        if "symbol" not in y.columns:
            y["symbol"] = ""
        y["symbol"] = y["symbol"].astype(str).str.strip().str.upper()
        if "as_of_date" not in y.columns:
            if "asOfDate" in y.columns:
                y["as_of_date"] = y["asOfDate"]
            else:
                y["as_of_date"] = np.nan
        y["as_of_date"] = pd.to_datetime(y["as_of_date"], errors="coerce").dt.strftime("%Y-%m-%d")
        return y

    def _dedupe_source_latest_by_symbol(src: pd.DataFrame, value_cols: list[str]) -> pd.DataFrame:
        if src is None or src.empty:
            return pd.DataFrame(columns=["symbol", "as_of_date", *value_cols])
        keep_cols = ["symbol", "as_of_date"] + [c for c in value_cols if c in src.columns]
        w = src[keep_cols].copy()
        w["symbol"] = w["symbol"].astype(str).str.strip().str.upper()
        w["as_of_date"] = pd.to_datetime(w["as_of_date"], errors="coerce").dt.strftime("%Y-%m-%d")
        w = w.dropna(subset=["symbol", "as_of_date"])
        if w.empty:
            return pd.DataFrame(columns=["symbol", "as_of_date", *value_cols])
        w = (
            w.groupby(["symbol", "as_of_date"], as_index=False, dropna=False)
            .agg({c: "median" for c in keep_cols if c not in ("symbol", "as_of_date")})
        )
        w = w.sort_values(["symbol", "as_of_date"], ascending=[True, True]).groupby("symbol", as_index=False).last()
        for c in value_cols:
            if c not in w.columns:
                w[c] = np.nan
        return w[["symbol", "as_of_date", *value_cols]].copy()

    def _merge_exact_then_symbol_fallback(
        base: pd.DataFrame,
        src: pd.DataFrame,
        value_cols: list[str],
        tag: str,
    ) -> tuple[pd.DataFrame, int, int]:
        if base.empty:
            return base, 0, 0
        out_df = base.copy()
        for c in value_cols:
            if c not in out_df.columns:
                out_df[c] = np.nan

        if src is None or src.empty:
            out_df[f"_{tag}_exact_hit"] = False
            out_df[f"_{tag}_fallback_hit"] = False
            return out_df, 0, 0

        src_norm = _normalize_key_columns(src)
        src_cols = ["symbol", "as_of_date"] + [c for c in value_cols if c in src_norm.columns]
        src_exact = src_norm[src_cols].copy()
        src_exact = (
            src_exact.groupby(["symbol", "as_of_date"], as_index=False, dropna=False)
            .agg({c: "median" for c in src_cols if c not in ("symbol", "as_of_date")})
        )
        src_exact[f"_{tag}_has_exact"] = True

        merged = out_df.merge(src_exact, on=["symbol", "as_of_date"], how="left", suffixes=("", "__exact"))

        exact_any = pd.Series(False, index=merged.index, dtype=bool)
        for c in value_cols:
            c_ex = f"{c}__exact"
            if c_ex in merged.columns:
                take_exact = merged[c].isna() & merged[c_ex].notna()
                if take_exact.any():
                    merged.loc[take_exact, c] = merged.loc[take_exact, c_ex]
                exact_any = exact_any | merged[c_ex].notna()
                merged = merged.drop(columns=[c_ex])
        merged[f"_{tag}_exact_hit"] = exact_any
        if f"_{tag}_has_exact" in merged.columns:
            merged = merged.drop(columns=[f"_{tag}_has_exact"])

        src_latest = _dedupe_source_latest_by_symbol(src_norm, value_cols=value_cols).set_index("symbol", drop=True)
        fallback_any = pd.Series(False, index=merged.index, dtype=bool)
        if not src_latest.empty:
            for c in value_cols:
                if c not in src_latest.columns:
                    continue
                fill_mask = merged[c].isna()
                if fill_mask.any():
                    mapped = merged.loc[fill_mask, "symbol"].map(src_latest[c].to_dict())
                    hit_mask = mapped.notna()
                    if hit_mask.any():
                        idx = mapped.index[hit_mask]
                        merged.loc[idx, c] = mapped.loc[idx]
                        fallback_any.loc[idx] = True
        merged[f"_{tag}_fallback_hit"] = fallback_any
        return merged, int(exact_any.sum()), int(fallback_any.sum())

    df = finalize_scoring_wide_input_df(df, label="build_final_vqgrs_scores")
    track_inputs = _load_track_inputs_from_factors_latest()
    track_a_inputs = _load_track_a_inputs_from_group_a_snapshot()
    print(f"Input category rows (base): {len(df)}")
    print(f"Track input rows from factors_latest: {len(track_inputs)}")
    print(f"Track A raw input rows from group_a snapshot: {len(track_a_inputs)}")

    df = _normalize_key_columns(df)

    track_cols = [c for c in track_inputs.columns if c.startswith("track_input_")] if not track_inputs.empty else []
    track_a_cols = [c for c in track_a_inputs.columns if c.startswith("track_a_")] if not track_a_inputs.empty else []

    df, exact_track_cnt, fallback_track_cnt = _merge_exact_then_symbol_fallback(
        df, track_inputs, track_cols, tag="track"
    )
    df, exact_a_cnt, fallback_a_cnt = _merge_exact_then_symbol_fallback(
        df, track_a_inputs, track_a_cols, tag="track_a"
    )

    exact_total = int(
        (df.get("_track_exact_hit", False).astype(bool) | df.get("_track_a_exact_hit", False).astype(bool)).sum()
    )
    fallback_total = int(
        (df.get("_track_fallback_hit", False).astype(bool) | df.get("_track_a_fallback_hit", False).astype(bool)).sum()
    )

    print(
        "Merge diagnostics: "
        f"track_exact={exact_track_cnt}, track_fallback={fallback_track_cnt}, "
        f"track_a_exact={exact_a_cnt}, track_a_fallback={fallback_a_cnt}, "
        f"exact_any={exact_total}, fallback_any={fallback_total}"
    )
    track_a_ready = 0
    if "track_a_raw_pe" in df.columns:
        track_a_ready = int(df["track_a_raw_pe"].notna().sum())
    print(f"Group A snapshot usable rows (track_a_raw_pe non-null): {track_a_ready}")

    # Guarantee one row per symbol before scoring:
    # priority = exact-hit rows > fallback-hit rows > latest as_of_date.
    dedupe_before = len(df)
    df["_merge_exact_any"] = (
        df.get("_track_exact_hit", False).astype(bool) | df.get("_track_a_exact_hit", False).astype(bool)
    ).astype(int)
    df["_merge_fallback_any"] = (
        df.get("_track_fallback_hit", False).astype(bool) | df.get("_track_a_fallback_hit", False).astype(bool)
    ).astype(int)
    df = df.sort_values(
        ["symbol", "_merge_exact_any", "_merge_fallback_any", "as_of_date"],
        ascending=[True, False, False, False],
    ).drop_duplicates(subset=["symbol"], keep="first").reset_index(drop=True)
    dedupe_after = len(df)
    if dedupe_after != dedupe_before:
        print(f"Dedupe applied: rows {dedupe_before} -> {dedupe_after} (1 symbol 1 row)")

    for c in ["_track_exact_hit", "_track_fallback_hit", "_track_a_exact_hit", "_track_a_fallback_hit", "_merge_exact_any", "_merge_fallback_any"]:
        if c in df.columns:
            df = df.drop(columns=[c])

    out = build_final_vqgrs_scores_df(df)
    print(f"Output final-score rows: {len(out)}")
    cat_cols = [f"score_{c}" for c in CORE_CATS if f"score_{c}" in out.columns]
    if cat_cols:
        sc_m = pd.concat([pd.to_numeric(out[c], errors="coerce") for c in cat_cols], axis=1)
        n_any_nan = int(sc_m.isna().any(axis=1).sum())
        print(f"Sanity: rows with any NaN in score_V..score_S (expect 0): {n_any_nan}")
        z = (sc_m.fillna(0.0) == 0.0)
        n_any_zero = int(z.any(axis=1).sum())
        n_ge2_zero = int((z.sum(axis=1) >= 2).sum())
        print(f"Sanity: rows with any category score == 0: {n_any_zero}")
        print(f"Sanity: rows with >=2 category scores == 0: {n_ge2_zero}")
    final_missing_rows = int(pd.to_numeric(out.get("final_score"), errors="coerce").isna().sum())
    print(f"Sanity: final_score missing rows (expect 0): {final_missing_rows}")
    if "assigned_track" in out.columns:
        dist = out["assigned_track"].astype(str).value_counts(dropna=False).to_dict()
        print(f"Sanity: A/B/C/N distribution={dist}")

    if len(out) > 0:
        print("Diagnostic — final layer (score-only; no final-stage penalty):")
        fs_num = pd.to_numeric(out.get("final_score"), errors="coerce")
        print(f"  final_score isna mean: {fs_num.isna().mean():.6f}")
        print(
            f"  final_score min/median/max: {fs_num.min():.4f} / {fs_num.median():.4f} / {fs_num.max():.4f}"
        )
        if cat_cols:
            sc_m = pd.concat([pd.to_numeric(out[c], errors="coerce").fillna(0.0) for c in cat_cols], axis=1)
            z = sc_m == 0.0
            giw = (z.sum(axis=1) >= 2) & fs_num.ge(70.0).fillna(False)
            print(f"  rows with >=2 zero category scores AND final_score>=70 (GIW-style check): {int(giw.sum())}")
        if "lti_uncertainty_reason" in out.columns:
            print("  lti_uncertainty_reason value_counts (top 12):")
            print(out["lti_uncertainty_reason"].astype(str).value_counts(dropna=False).head(12).to_string())
        def _diag_count_mean(name: str, series: pd.Series) -> None:
            s = pd.to_numeric(series, errors="coerce").fillna(0).astype(int)
            print(f"  {name} value_counts (top 16):")
            vc = s.value_counts().sort_index()
            print(vc.head(16).to_string())
            print(f"  {name} mean: {s.mean():.6f}")

        if "incidental_only_core_count" in out.columns:
            _diag_count_mean("incidental_only_core_count", out["incidental_only_core_count"])
        if "mixed_missing_core_count" in out.columns:
            _diag_count_mean("mixed_missing_core_count", out["mixed_missing_core_count"])
        if "partial_observed_core_count" in out.columns:
            _diag_count_mean("partial_observed_core_count", out["partial_observed_core_count"])

        if "penalty_total" in out.columns:
            pt = pd.to_numeric(out["penalty_total"], errors="coerce")
            print(f"  penalty_total (legacy diagnostic, fixed 0): min={pt.min():.4f} max={pt.max():.4f}")

    parquet_out = output_dir / "final_vqgrs_scores_latest.parquet"
    csv_out = output_dir / "final_vqgrs_scores_latest.csv"
    _save_df(out, parquet_out, csv_out)
    print(f"Saved: {parquet_out}")


if __name__ == "__main__":
    main()

