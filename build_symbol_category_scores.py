# -*- coding: utf-8 -*-
"""
Build per-symbol per-category scores (V/Q/G/R/S/STI) from symbol_factor_scores_latest.

Input:
  - output/scoring/symbol_factor_scores_latest.(parquet|csv)

Output:
  - output/scoring/symbol_category_scores_latest.(parquet|csv)

Evidence-first: category weights and main/aux splits use ``final_factor_evidence`` only (already
group→symbol aggregated, including hybrid blended factor evidence). Extra transparency columns on
the input, if present, are ignored here and do not affect groupby.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from group_snapshot_utils import finalize_scoring_long_input_df
from score_factor_config import CATEGORY_TO_FACTORS, FACTOR_SPECS, MAIN_FACTORS_BY_CATEGORY
from score_primitives import evidence_to_score


CAT_LIST = ["V", "Q", "G", "R", "S", "STI"]
CONF_DENOM_BY_CAT: dict[str, float] = {
    "V": 3.0,
    "Q": 3.0,
    "G": 3.0,
    "R": 3.0,
    "S": 3.0,
    "STI": 5.0,
}
_CATEGORY_EVIDENCE_PRIOR = 0.0


def _read_df(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    if path.suffix.lower() == ".csv":
        return pd.read_csv(path, low_memory=False)
    return pd.read_parquet(path)


def _save_df(df: pd.DataFrame, parquet_path: Path, csv_path: Path) -> None:
    parquet_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(parquet_path, index=False)
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")


def _compute_category_block(df_factor_scores: pd.DataFrame, category: str) -> pd.DataFrame:
    enabled_factors = [
        f for f in CATEGORY_TO_FACTORS.get(category, []) if FACTOR_SPECS.get(f) is not None and FACTOR_SPECS[f].enabled
    ]
    denom = CONF_DENOM_BY_CAT.get(category, 3.0)
    main_expected_count = len(MAIN_FACTORS_BY_CATEGORY.get(category, []))
    base_conf_col = f"base_conf_{category}"
    main_cov_col = f"main_coverage_{category}"

    keys = df_factor_scores[["symbol", "as_of_date"]].drop_duplicates()
    if not enabled_factors:
        out = keys.copy()
        out[f"raw_score_{category}"] = _CATEGORY_EVIDENCE_PRIOR  # compatibility alias
        out[f"raw_evidence_{category}"] = _CATEGORY_EVIDENCE_PRIOR
        out[f"raw_main_evidence_{category}"] = np.nan
        out[f"raw_aux_evidence_{category}"] = np.nan
        out[f"main_count_{category}"] = 0
        out[f"aux_count_{category}"] = 0
        out[f"main_weight_sum_{category}"] = 0.0
        out[f"aux_weight_sum_{category}"] = 0.0
        out[f"final_evidence_{category}"] = _CATEGORY_EVIDENCE_PRIOR
        out[f"score_{category}"] = 50.0
        out[f"count_{category}"] = 0
        out[f"weight_sum_{category}"] = 0.0
        out[f"conf_{category}"] = 0.0
        out[main_cov_col] = 1.0 if main_expected_count == 0 else 0.0
        out[base_conf_col] = 0.0
        out[f"observed_weight_{category}"] = 0.0
        out[f"observed_ratio_{category}"] = 0.0
        out[f"final_conf_{category}"] = 0.0
        # Very weak last-resort guardrail: score cap depends on main coverage.
        # Evidence shrink already happened earlier (via final_conf), so this cap is only a safety net.
        score_cap_applied_col = f"score_cap_applied_{category}"
        cap_reason_col = f"cap_reason_{category}"
        out[score_cap_applied_col] = 0
        out[cap_reason_col] = "no_cap"
        dominant_col = f"dominant_signal_{category}"
        out[dominant_col] = (
            "main_missing_shrunk" if float(out[main_cov_col].iloc[0]) < 0.67 else "balanced"
        )
        return out

    df_cat = df_factor_scores[df_factor_scores["category"] == category].copy()
    if df_cat.empty:
        out = keys.copy()
        out[f"raw_score_{category}"] = _CATEGORY_EVIDENCE_PRIOR  # compatibility alias
        out[f"raw_evidence_{category}"] = _CATEGORY_EVIDENCE_PRIOR
        out[f"raw_main_evidence_{category}"] = np.nan
        out[f"raw_aux_evidence_{category}"] = np.nan
        out[f"main_count_{category}"] = 0
        out[f"aux_count_{category}"] = 0
        out[f"main_weight_sum_{category}"] = 0.0
        out[f"aux_weight_sum_{category}"] = 0.0
        out[f"final_evidence_{category}"] = _CATEGORY_EVIDENCE_PRIOR
        out[f"score_{category}"] = 50.0
        out[f"count_{category}"] = 0
        out[f"weight_sum_{category}"] = 0.0
        out[f"conf_{category}"] = 0.0
        out[main_cov_col] = 1.0 if main_expected_count == 0 else 0.0
        out[base_conf_col] = 0.0
        out[f"observed_weight_{category}"] = 0.0
        out[f"observed_ratio_{category}"] = 0.0
        out[f"final_conf_{category}"] = 0.0
        score_cap_applied_col = f"score_cap_applied_{category}"
        cap_reason_col = f"cap_reason_{category}"
        out[score_cap_applied_col] = 0
        out[cap_reason_col] = "no_cap"
        dominant_col = f"dominant_signal_{category}"
        out[dominant_col] = (
            "main_missing_shrunk" if float(out[main_cov_col].iloc[0]) < 0.67 else "balanced"
        )
        return out

    # Keep only enabled factors for this category.
    df_cat = df_cat[df_cat["factor_name"].isin(enabled_factors)].copy()
    if df_cat.empty:
        out = keys.copy()
        out[f"raw_score_{category}"] = _CATEGORY_EVIDENCE_PRIOR  # compatibility alias
        out[f"raw_evidence_{category}"] = _CATEGORY_EVIDENCE_PRIOR
        out[f"raw_main_evidence_{category}"] = np.nan
        out[f"raw_aux_evidence_{category}"] = np.nan
        out[f"main_count_{category}"] = 0
        out[f"aux_count_{category}"] = 0
        out[f"main_weight_sum_{category}"] = 0.0
        out[f"aux_weight_sum_{category}"] = 0.0
        out[f"final_evidence_{category}"] = _CATEGORY_EVIDENCE_PRIOR
        out[f"score_{category}"] = 50.0
        out[f"count_{category}"] = 0
        out[f"weight_sum_{category}"] = 0.0
        out[f"conf_{category}"] = 0.0
        out[main_cov_col] = 1.0 if main_expected_count == 0 else 0.0
        out[base_conf_col] = 0.0
        out[f"observed_weight_{category}"] = 0.0
        out[f"observed_ratio_{category}"] = 0.0
        out[f"final_conf_{category}"] = 0.0
        score_cap_applied_col = f"score_cap_applied_{category}"
        cap_reason_col = f"cap_reason_{category}"
        out[score_cap_applied_col] = 0
        out[cap_reason_col] = "no_cap"
        dominant_col = f"dominant_signal_{category}"
        out[dominant_col] = (
            "main_missing_shrunk" if float(out[main_cov_col].iloc[0]) < 0.67 else "balanced"
        )
        return out

    if "factor_source" not in df_cat.columns:
        df_cat["factor_source"] = "observed"
    else:
        df_cat["factor_source"] = df_cat["factor_source"].fillna("unknown")

    df_cat["factor_weight"] = df_cat["factor_name"].map(lambda x: float(FACTOR_SPECS[x].weight))
    # Hybrid factor path is sealed into final_factor_evidence upstream; no separate relative/absolute here.
    df_cat["final_factor_evidence"] = pd.to_numeric(df_cat["final_factor_evidence"], errors="coerce")
    valid = df_cat["final_factor_evidence"].notna()
    # Only "observed" is treated as observed for coverage; "mixed" must not inflate coverage.
    is_observed = df_cat["factor_source"].astype(str).str.strip().str.lower() == "observed"

    df_cat["valid_factor"] = valid
    df_cat["weighted_evidence_term"] = df_cat["final_factor_evidence"] * df_cat["factor_weight"]
    df_cat["weighted_weight_term"] = df_cat["factor_weight"].where(valid, other=0.0)
    df_cat["weighted_evidence_term"] = df_cat["weighted_evidence_term"].where(valid, other=0.0)

    # Main/aux split (evidence space only).
    # - main evidence: weighted avg over main factors
    # - aux evidence: weighted avg over aux factors
    is_main_factor = df_cat["factor_name"].map(lambda x: bool(getattr(FACTOR_SPECS[x], "main_factor", False)))
    df_cat["main_valid_factor"] = valid & is_main_factor
    df_cat["aux_valid_factor"] = valid & (~is_main_factor)
    # For coverage: count only factors that are truly observed (not donor/prior shrunk),
    # so main_coverage drops when main evidence is scarce.
    df_cat["main_observed_factor"] = df_cat["main_valid_factor"] & is_observed
    df_cat["aux_observed_factor"] = df_cat["aux_valid_factor"] & is_observed
    df_cat["main_weight_term"] = df_cat["factor_weight"].where(df_cat["main_valid_factor"], other=0.0)
    df_cat["aux_weight_term"] = df_cat["factor_weight"].where(df_cat["aux_valid_factor"], other=0.0)
    df_cat["main_weighted_evidence_term"] = (
        (df_cat["final_factor_evidence"] * df_cat["factor_weight"]).where(df_cat["main_valid_factor"], other=0.0)
    )
    df_cat["aux_weighted_evidence_term"] = (
        (df_cat["final_factor_evidence"] * df_cat["factor_weight"]).where(df_cat["aux_valid_factor"], other=0.0)
    )

    df_cat["observed_weight_term"] = np.where(valid & is_observed, df_cat["factor_weight"], 0.0).astype(float)

    grouped = df_cat.groupby(["symbol", "as_of_date"], dropna=False).agg(
        final_weighted_sum=("weighted_evidence_term", "sum"),
        total_weight=("weighted_weight_term", "sum"),
        count_valid=("valid_factor", "sum"),
        observed_weight=("observed_weight_term", "sum"),
        raw_main_weighted_sum=("main_weighted_evidence_term", "sum"),
        main_total_weight=("main_weight_term", "sum"),
        main_count=("main_observed_factor", "sum"),
        raw_aux_weighted_sum=("aux_weighted_evidence_term", "sum"),
        aux_total_weight=("aux_weight_term", "sum"),
        aux_count=("aux_observed_factor", "sum"),
    )
    grouped = grouped.reset_index()

    score_col = f"score_{category}"
    raw_col = f"raw_evidence_{category}"
    raw_score_alias_col = f"raw_score_{category}"
    final_evd_col = f"final_evidence_{category}"
    raw_main_col = f"raw_main_evidence_{category}"
    raw_aux_col = f"raw_aux_evidence_{category}"
    main_count_col = f"main_count_{category}"
    aux_count_col = f"aux_count_{category}"
    main_weight_sum_col = f"main_weight_sum_{category}"
    aux_weight_sum_col = f"aux_weight_sum_{category}"

    has_weight = pd.to_numeric(grouped["total_weight"], errors="coerce").fillna(0.0) > 0

    # Compute raw main/aux evidence from main/aux weighted averages:
    # raw_category_evidence = 0.7 * raw_main + 0.3 * raw_aux
    main_tw = pd.to_numeric(grouped["main_total_weight"], errors="coerce").fillna(0.0)
    aux_tw = pd.to_numeric(grouped["aux_total_weight"], errors="coerce").fillna(0.0)
    main_ws = pd.to_numeric(grouped["raw_main_weighted_sum"], errors="coerce").fillna(0.0)
    aux_ws = pd.to_numeric(grouped["raw_aux_weighted_sum"], errors="coerce").fillna(0.0)

    main_evd = np.where(main_tw > 0.0, main_ws / main_tw, np.nan).astype(float)
    aux_evd = np.where(aux_tw > 0.0, aux_ws / aux_tw, np.nan).astype(float)

    has_main = main_tw > 0.0
    has_aux = aux_tw > 0.0

    raw_cat_evd = np.where(
        has_main & has_aux,
        0.7 * main_evd + 0.3 * aux_evd,
        np.where(has_main, main_evd, np.where(has_aux, aux_evd, _CATEGORY_EVIDENCE_PRIOR)),
    ).astype(float)

    grouped[raw_main_col] = main_evd
    grouped[raw_aux_col] = aux_evd
    grouped[raw_col] = raw_cat_evd
    grouped[main_count_col] = pd.to_numeric(grouped["main_count"], errors="coerce").fillna(0).astype(int)
    grouped[aux_count_col] = pd.to_numeric(grouped["aux_count"], errors="coerce").fillna(0).astype(int)
    grouped[main_weight_sum_col] = main_tw.astype(float)
    grouped[aux_weight_sum_col] = aux_tw.astype(float)

    tw = pd.to_numeric(grouped["total_weight"], errors="coerce").fillna(0.0)
    ow = pd.to_numeric(grouped["observed_weight"], errors="coerce").fillna(0.0)
    grouped[f"observed_weight_{category}"] = ow.astype(float)
    grouped[f"observed_ratio_{category}"] = np.where(tw > 0.0, ow / tw, 0.0).astype(float)

    grouped[f"count_{category}"] = grouped["count_valid"].astype(int)
    grouped[f"weight_sum_{category}"] = grouped["total_weight"].astype(float)
    grouped[f"conf_{category}"] = grouped[f"count_{category}"].astype(float) / denom
    grouped[f"conf_{category}"] = grouped[f"conf_{category}"].clip(upper=1.0, lower=0.0)

    base_conf_col = f"base_conf_{category}"
    main_cov_col = f"main_coverage_{category}"

    # base_conf: existing (count/denom) combined with observed_ratio.
    base_conf = pd.to_numeric(grouped[f"conf_{category}"], errors="coerce").fillna(0.0)
    obs_r = pd.to_numeric(grouped[f"observed_ratio_{category}"], errors="coerce").fillna(0.0)
    grouped[base_conf_col] = (base_conf * (0.5 + 0.5 * obs_r)).astype(float)
    grouped[base_conf_col] = grouped[base_conf_col].clip(upper=1.0, lower=0.0)

    # main_coverage: how many main factors are observed/valid vs expected.
    main_expected_count = len(MAIN_FACTORS_BY_CATEGORY.get(category, []))
    main_obs = pd.to_numeric(grouped[main_count_col], errors="coerce").fillna(0.0)
    if main_expected_count > 0:
        main_coverage = main_obs / float(main_expected_count)
    else:
        # No explicit main factors (e.g. STI): do not penalize.
        main_coverage = pd.Series(1.0, index=grouped.index, dtype=float)
    grouped[main_cov_col] = np.clip(main_coverage.astype(float), 0.0, 1.0)

    # final_conf: tighten confidence based on main coverage.
    grouped[f"final_conf_{category}"] = (
        pd.to_numeric(grouped[base_conf_col], errors="coerce").fillna(0.0)
        * (0.25 + 0.75 * pd.to_numeric(grouped[main_cov_col], errors="coerce").fillna(0.0))
    ).astype(float)
    grouped[f"final_conf_{category}"] = grouped[f"final_conf_{category}"].clip(upper=1.0, lower=0.0)

    # Debug label: summarize whether category signal is main-driven, aux-only, main-missing shrunk, or balanced.
    dominant_col = f"dominant_signal_{category}"
    mc = pd.to_numeric(grouped[main_count_col], errors="coerce").fillna(0.0)
    ac = pd.to_numeric(grouped[aux_count_col], errors="coerce").fillna(0.0)
    cov = pd.to_numeric(grouped[main_cov_col], errors="coerce").fillna(0.0)
    grouped[dominant_col] = np.select(
        [
            (mc > 0) & (ac <= 0),
            (mc <= 0) & (ac > 0),
            cov < 0.67,
        ],
        [
            "main_driven",
            "aux_only",
            "main_missing_shrunk",
        ],
        default="balanced",
    ).astype(object)

    final_conf_s = pd.to_numeric(grouped[f"final_conf_{category}"], errors="coerce").fillna(0.0)
    raw_evd_s = pd.to_numeric(grouped[raw_col], errors="coerce").fillna(_CATEGORY_EVIDENCE_PRIOR)
    grouped[final_evd_col] = np.where(
        has_weight,
        final_conf_s * raw_evd_s + (1.0 - final_conf_s) * _CATEGORY_EVIDENCE_PRIOR,
        _CATEGORY_EVIDENCE_PRIOR,
    ).astype(float)
    grouped[score_col] = grouped[final_evd_col].map(lambda x: evidence_to_score(float(x)) if pd.notna(x) else np.nan).astype(float)
    grouped[raw_score_alias_col] = pd.to_numeric(grouped[raw_col], errors="coerce").astype(float)

    # Left-join to ensure all symbols exist.
    keep_cols = [
        "symbol",
        "as_of_date",
        raw_col,
        raw_score_alias_col,
        raw_main_col,
        raw_aux_col,
        main_count_col,
        aux_count_col,
        main_weight_sum_col,
        aux_weight_sum_col,
        main_cov_col,
        base_conf_col,
        dominant_col,
        final_evd_col,
        score_col,
        f"count_{category}",
        f"weight_sum_{category}",
        f"conf_{category}",
        f"observed_weight_{category}",
        f"observed_ratio_{category}",
        f"final_conf_{category}",
    ]
    grouped = grouped[keep_cols]
    out = keys.merge(grouped, on=["symbol", "as_of_date"], how="left")

    out[raw_col] = out[raw_col].astype("float64")
    out[raw_score_alias_col] = out[raw_score_alias_col].astype("float64")
    out[raw_main_col] = out[raw_main_col].astype("float64")
    out[raw_aux_col] = out[raw_aux_col].astype("float64")
    if base_conf_col in out.columns:
        out[base_conf_col] = out[base_conf_col].fillna(0.0).astype(float)
    if main_cov_col in out.columns:
        out[main_cov_col] = out[main_cov_col].fillna(0.0).astype(float)
    out[final_evd_col] = out[final_evd_col].fillna(_CATEGORY_EVIDENCE_PRIOR).astype(float)
    out[score_col] = out[score_col].fillna(50.0)
    out[f"count_{category}"] = out[f"count_{category}"].fillna(0).astype(int)
    out[f"weight_sum_{category}"] = out[f"weight_sum_{category}"].fillna(0.0).astype(float)
    out[main_weight_sum_col] = out[main_weight_sum_col].fillna(0.0).astype(float)
    out[aux_weight_sum_col] = out[aux_weight_sum_col].fillna(0.0).astype(float)
    out[main_count_col] = out[main_count_col].fillna(0).astype(int)
    out[aux_count_col] = out[aux_count_col].fillna(0).astype(int)
    out[f"conf_{category}"] = out[f"conf_{category}"].fillna(0.0).astype(float)
    out[f"observed_weight_{category}"] = out[f"observed_weight_{category}"].fillna(0.0).astype(float)
    out[f"observed_ratio_{category}"] = out[f"observed_ratio_{category}"].fillna(0.0).astype(float)
    out[f"final_conf_{category}"] = out[f"final_conf_{category}"].fillna(0.0).astype(float)

    # Weak safety cap (score stage only) for rare main-evidence scarcity cases.
    # This uses already main_coverage-adjusted evidence (final_evidence -> final_conf -> score),
    # so it should have near-zero impact for items with sufficient observed main factors.
    main_cov = pd.to_numeric(out[main_cov_col], errors="coerce").fillna(0.0).astype(float)
    mask_very_low = main_cov < 0.34
    mask_partial = (main_cov >= 0.34) & (main_cov < 0.67)
    score_cap_applied_col = f"score_cap_applied_{category}"
    cap_reason_col = f"cap_reason_{category}"

    out[score_cap_applied_col] = (mask_very_low | mask_partial).astype(int)
    out[cap_reason_col] = np.select(
        [mask_very_low, mask_partial],
        ["main_coverage_lt_0.34_cap_68", "main_coverage_0.34_0.67_cap_82"],
        default="no_cap",
    ).astype(object)

    cap_68 = 68.0
    cap_82 = 82.0
    s = pd.to_numeric(out[score_col], errors="coerce").fillna(50.0).astype(float)
    s_cap = np.where(mask_very_low, np.minimum(s, cap_68), s)
    s_cap = np.where(mask_partial & ~mask_very_low, np.minimum(s_cap, cap_82), s_cap)
    out[score_col] = s_cap.astype(float)
    if dominant_col in out.columns:
        out[dominant_col] = out[dominant_col].fillna("balanced").astype(object)
    return out


def build_symbol_category_scores_df(df_factor_scores: pd.DataFrame) -> pd.DataFrame:
    if df_factor_scores is None or df_factor_scores.empty:
        return pd.DataFrame()

    required = {"symbol", "as_of_date", "category", "factor_name", "final_factor_evidence"}
    missing = [c for c in required if c not in df_factor_scores.columns]
    if missing:
        raise ValueError(
            "Missing required input columns for evidence-first category build: "
            f"{missing}. Expected 'final_factor_evidence' in symbol factor snapshot."
        )

    base_keys = df_factor_scores[["symbol", "as_of_date"]].drop_duplicates()

    blocks = []
    for cat in CAT_LIST:
        blocks.append(_compute_category_block(df_factor_scores, cat))

    out = base_keys.copy()
    for b in blocks:
        out = out.merge(
            b,
            on=["symbol", "as_of_date"],
            how="left",
        )

    # Ensure columns exist even if some categories are missing from input.
    for cat in CAT_LIST:
        if f"raw_score_{cat}" not in out.columns:
            out[f"raw_score_{cat}"] = _CATEGORY_EVIDENCE_PRIOR
        if f"raw_evidence_{cat}" not in out.columns:
            out[f"raw_evidence_{cat}"] = _CATEGORY_EVIDENCE_PRIOR
        if f"raw_main_evidence_{cat}" not in out.columns:
            out[f"raw_main_evidence_{cat}"] = np.nan
        if f"raw_aux_evidence_{cat}" not in out.columns:
            out[f"raw_aux_evidence_{cat}"] = np.nan
        if f"main_count_{cat}" not in out.columns:
            out[f"main_count_{cat}"] = 0
        if f"aux_count_{cat}" not in out.columns:
            out[f"aux_count_{cat}"] = 0
        if f"main_weight_sum_{cat}" not in out.columns:
            out[f"main_weight_sum_{cat}"] = 0.0
        if f"aux_weight_sum_{cat}" not in out.columns:
            out[f"aux_weight_sum_{cat}"] = 0.0
        if f"final_evidence_{cat}" not in out.columns:
            out[f"final_evidence_{cat}"] = _CATEGORY_EVIDENCE_PRIOR
        if f"score_{cat}" not in out.columns:
            out[f"score_{cat}"] = 50.0
        if f"count_{cat}" not in out.columns:
            out[f"count_{cat}"] = 0
        if f"weight_sum_{cat}" not in out.columns:
            out[f"weight_sum_{cat}"] = 0.0
        if f"conf_{cat}" not in out.columns:
            out[f"conf_{cat}"] = 0.0
        if f"observed_weight_{cat}" not in out.columns:
            out[f"observed_weight_{cat}"] = 0.0
        if f"observed_ratio_{cat}" not in out.columns:
            out[f"observed_ratio_{cat}"] = 0.0
        if f"final_conf_{cat}" not in out.columns:
            out[f"final_conf_{cat}"] = 0.0
        score_cap_applied_col = f"score_cap_applied_{cat}"
        cap_reason_col = f"cap_reason_{cat}"
        if score_cap_applied_col not in out.columns:
            out[score_cap_applied_col] = 0
        if cap_reason_col not in out.columns:
            out[cap_reason_col] = "no_cap"
        dominant_col = f"dominant_signal_{cat}"
        if dominant_col not in out.columns:
            out[dominant_col] = "balanced"
        base_conf_col = f"base_conf_{cat}"
        main_cov_col = f"main_coverage_{cat}"
        if base_conf_col not in out.columns:
            out[base_conf_col] = 0.0
        if main_cov_col not in out.columns:
            exp = len(MAIN_FACTORS_BY_CATEGORY.get(cat, []))
            out[main_cov_col] = 1.0 if exp == 0 else 0.0
    return out


def main(input_dir: str | Path = "output", output_dir: str | Path = "output/scoring") -> None:
    input_dir = Path(input_dir)
    output_dir = Path(output_dir)

    parquet_in = input_dir / "scoring" / "symbol_factor_scores_latest.parquet"
    csv_in = input_dir / "scoring" / "symbol_factor_scores_latest.csv"

    df = _read_df(parquet_in)
    if df.empty:
        df = _read_df(csv_in)
    if df.empty:
        print("No symbol_factor_scores_latest input found.")
        return

    df = finalize_scoring_long_input_df(df, label="build_symbol_category_scores")
    print(f"Input factor-score rows: {len(df)}")
    scores_df = build_symbol_category_scores_df(df)
    print(f"Output symbol-category rows: {len(scores_df)}")

    # Category coverage print (average confidence proxy).
    for cat in CAT_LIST:
        conf_col = f"conf_{cat}"
        if conf_col in scores_df.columns:
            avg_conf = float(pd.to_numeric(scores_df[conf_col], errors="coerce").fillna(0.0).mean())
            print(f"Avg coverage conf_{cat}: {avg_conf:.4f}")

    parquet_out = output_dir / "symbol_category_scores_latest.parquet"
    csv_out = output_dir / "symbol_category_scores_latest.csv"
    _save_df(scores_df, parquet_out, csv_out)
    print(f"Saved: {parquet_out}")


if __name__ == "__main__":
    main()

