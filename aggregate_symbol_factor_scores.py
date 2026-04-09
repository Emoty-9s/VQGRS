# -*- coding: utf-8 -*-
"""
Aggregate per-group factor scores into a single final score per:
  symbol x factor_name

Input:
  output/scoring/group_factor_scores_latest.(parquet|csv) only

Output:
  output/scoring/symbol_factor_scores_latest.(parquet|csv)
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from score_factor_config import GROUP_BASE_WEIGHTS, FACTOR_SPECS
from score_primitives import evidence_to_score

GROUP_TYPES = ["A", "B", "C", "D", "E"]
AGGREGATION_MODEL_VERSION = "factor_agg_conf_weight_v2"

# From group_factor_scores (score_one_factor_one_group); carried through aggregation when present.
HYBRID_TRANSPARENCY_COLS: tuple[str, ...] = (
    "relative_evidence",
    "absolute_evidence",
    "absolute_weight",
    "blend_method",
    "absolute_enabled",
)


def _row_is_valid_for_aggregate(series: pd.Series) -> bool:
    adj = pd.to_numeric(series.get("adjusted_evidence"), errors="coerce")
    if pd.isna(adj):
        return False
    conf = pd.to_numeric(series.get("confidence"), errors="coerce")
    if pd.isna(conf) or float(conf) <= 0.0:
        return False
    return True


def _structural_missing_mask(df: pd.DataFrame) -> pd.Series:
    """Row is structurally missing (no meaningful factor value); uses new columns or legacy fields."""
    idx = df.index
    base = pd.Series(False, index=idx)
    if "structural_missing_flag" in df.columns:
        col = df["structural_missing_flag"]
        if getattr(col.dtype, "name", str(col.dtype)) == "bool":
            base = base | col.fillna(False).astype(bool)
        else:
            v = pd.to_numeric(col, errors="coerce")
            base = base | v.fillna(0.0).astype(bool)
    if "missing_class" in df.columns:
        mc = df["missing_class"].astype(str).str.strip().str.lower()
        base = base | (mc == "structural")
    if "evidence_source" in df.columns:
        es = df["evidence_source"].astype(str).str.strip().str.lower()
        base = base | es.eq("structural_skip")
    if "missing_reason" in df.columns:
        mr = df["missing_reason"].astype(str).str.strip().str.lower()
        base = base | mr.eq("structural_missing")
    return base


def _incidental_missing_mask(df: pd.DataFrame, valid: pd.Series, structural: pd.Series) -> pd.Series:
    """Non-structural rows that do not contribute valid weighted evidence."""
    eligible = ~valid & ~structural
    if "incidental_missing_flag" in df.columns:
        inc_b = pd.to_numeric(df["incidental_missing_flag"], errors="coerce").fillna(1.0) != 0.0
        return eligible & inc_b
    if "missing_class" in df.columns:
        mc = df["missing_class"].astype(str).str.strip().str.lower()
        return eligible & mc.eq("incidental")
    return eligible


def _final_factor_source(og: pd.Series, sm: pd.Series, im: pd.Series) -> pd.Series:
    ogv = np.asarray(og, dtype=np.int64)
    smv = np.asarray(sm, dtype=np.int64)
    imv = np.asarray(im, dtype=np.int64)
    msum = smv + imv
    result = np.full(len(ogv), "missing_incidental", dtype=object)
    result[(ogv >= 1) & (msum == 0)] = "observed"
    result[(ogv >= 1) & (msum > 0)] = "observed_partial"
    result[(ogv == 0) & (smv > 0) & (imv == 0)] = "missing_structural"
    result[(ogv == 0) & (imv > 0)] = "missing_incidental"
    return pd.Series(result, index=og.index, dtype=object)


def _factor_missing_class(og: pd.Series, sm: pd.Series, im: pd.Series) -> pd.Series:
    ogv = np.asarray(og, dtype=np.int64)
    smv = np.asarray(sm, dtype=np.int64)
    imv = np.asarray(im, dtype=np.int64)
    msum = smv + imv
    result = np.full(len(ogv), "incidental_only", dtype=object)
    result[(ogv >= 1) & (msum == 0)] = "observed_only"
    result[(ogv >= 1) & (msum > 0)] = "partial_observed"
    result[(ogv == 0) & (smv > 0) & (imv == 0)] = "structural_only"
    result[(ogv == 0) & (imv > 0) & (smv == 0)] = "incidental_only"
    result[(ogv == 0) & (smv > 0) & (imv > 0)] = "mixed_missing"
    return pd.Series(result, index=og.index, dtype=object)


def _evidence_to_score_or_nan(x: Any) -> float:
    if x is None:
        return float(np.nan)
    try:
        if pd.isna(x):
            return float(np.nan)
    except (ValueError, TypeError):
        return float(np.nan)
    s = evidence_to_score(float(x))
    return float(np.nan) if s is None else float(s)


def _read_df(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    if path.suffix.lower() == ".csv":
        return pd.read_csv(path, low_memory=False)
    return pd.read_parquet(path)


def _infer_group_type_from_columns(df: pd.DataFrame) -> pd.DataFrame:
    if "group_type" in df.columns:
        return df
    if "group_tag" in df.columns:

        def _infer(x: Any) -> Any:
            if x is None or (isinstance(x, float) and pd.isna(x)):
                return None
            s = str(x).strip().lower()
            if "group_a" in s:
                return "A"
            if "group_b" in s:
                return "B"
            if "group_c" in s:
                return "C"
            if "group_d" in s:
                return "D"
            if "group_e" in s:
                return "E"
            return None

        out = df.copy()
        out["group_type"] = out["group_tag"].apply(_infer)
        return out
    return df


def _aggregate_symbol_factor_scores(df: pd.DataFrame) -> pd.DataFrame:
    df = _infer_group_type_from_columns(df)
    required = [
        "symbol",
        "as_of_date",
        "factor_name",
        "category",
        "group_type",
    ]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns in score input: {missing}")
    if "group_type" not in df.columns:
        raise ValueError("Cannot infer group_type; missing both group_type and group_tag.")

    df = df.copy()
    df["adjusted_evidence"] = pd.to_numeric(df["adjusted_evidence"], errors="coerce")
    if "adjusted_score" in df.columns:
        df["adjusted_score"] = pd.to_numeric(df["adjusted_score"], errors="coerce")
    df["confidence"] = pd.to_numeric(df["confidence"], errors="coerce")
    df["prior_evidence_factor"] = df["factor_name"].map(
        lambda x: float(getattr(FACTOR_SPECS.get(str(x)), "evidence_prior", 0.0)) if FACTOR_SPECS.get(str(x)) else 0.0
    )

    valid_mask = df.apply(_row_is_valid_for_aggregate, axis=1)
    raw_structural = _structural_missing_mask(df)
    incidental_mask = _incidental_missing_mask(df, valid_mask, raw_structural)
    structural_for_count = raw_structural & ~valid_mask
    uncounted_missing = ~valid_mask & ~structural_for_count & ~incidental_mask
    df["_observed_int"] = valid_mask.astype(np.int64)
    df["_structural_int"] = structural_for_count.astype(np.int64)
    df["_incidental_int"] = (incidental_mask | uncounted_missing).astype(np.int64)

    df["category_valid"] = df["category"].where(valid_mask, other=pd.NA)
    df["confidence_valid"] = df["confidence"].where(valid_mask, other=pd.NA)

    for g in GROUP_TYPES:
        col_evd = f"group_evidence_{g}"
        col_w = f"weight_{g}"  # backward-compatible alias to effective weight
        col_w_base = f"base_weight_{g}"
        col_w_eff = f"effective_weight_{g}"
        col_conf = f"group_confidence_{g}"
        w_base = float(GROUP_BASE_WEIGHTS.get(g, 1.0))
        is_g = df["group_type"].astype(str).str.strip().str.upper() == g
        df[col_evd] = df["adjusted_evidence"].where(valid_mask & is_g, other=pd.NA)
        conf_g = pd.to_numeric(df["confidence"], errors="coerce").fillna(0.0).clip(lower=0.0, upper=1.0)
        df[col_conf] = np.where(valid_mask & is_g, conf_g, 0.0).astype(float)
        df[col_w_base] = np.where(valid_mask & is_g, w_base, 0.0).astype(float)
        df[col_w_eff] = (df[col_w_base] * df[col_conf]).astype(float)
        df[col_w] = df[col_w_eff].astype(float)

    key_cols = ["symbol", "as_of_date", "factor_name"]

    agg_spec: dict[str, Any] = {
        "category_valid": "first",
        "confidence_valid": "mean",
        "prior_evidence_factor": "first",
        "_observed_int": "sum",
        "_structural_int": "sum",
        "_incidental_int": "sum",
    }
    for c in HYBRID_TRANSPARENCY_COLS:
        if c not in df.columns:
            continue
        if c in ("blend_method", "absolute_enabled"):
            agg_spec[c] = "first"
        else:
            agg_spec[c] = "mean"
    for g in GROUP_TYPES:
        agg_spec[f"group_evidence_{g}"] = "first"
        agg_spec[f"weight_{g}"] = "sum"
        agg_spec[f"base_weight_{g}"] = "sum"
        agg_spec[f"effective_weight_{g}"] = "sum"
        agg_spec[f"group_confidence_{g}"] = "mean"

    grouped = df.groupby(key_cols, dropna=False, as_index=False).agg(agg_spec)
    grouped = grouped.rename(
        columns={
            "_observed_int": "observed_group_count",
            "_structural_int": "structural_missing_group_count",
            "_incidental_int": "incidental_missing_group_count",
        }
    )

    weight_sum = None
    for g in GROUP_TYPES:
        wcol = f"weight_{g}"
        weight_sum = grouped[wcol].astype(float) if weight_sum is None else weight_sum + grouped[wcol].astype(float)
    grouped["total_effective_weight"] = weight_sum
    base_weight_sum = None
    for g in GROUP_TYPES:
        wb = f"base_weight_{g}"
        base_weight_sum = grouped[wb].astype(float) if base_weight_sum is None else base_weight_sum + grouped[wb].astype(float)
    grouped["total_base_weight"] = base_weight_sum

    w_gt0 = None
    for g in GROUP_TYPES:
        wcol = f"weight_{g}"
        bit = (grouped[wcol] > 0).astype(int)
        w_gt0 = bit if w_gt0 is None else w_gt0 + bit
    grouped["valid_group_count"] = w_gt0

    grouped["availability"] = grouped["valid_group_count"].astype(float) / 5.0

    num = None
    for g in GROUP_TYPES:
        s_col = f"group_evidence_{g}"
        w_col = f"weight_{g}"
        w_part = grouped[w_col].astype(float)
        s_part = grouped[s_col].astype(float)
        term = np.where(w_part > 0.0, s_part * w_part, 0.0).astype(float)
        num = term if num is None else (num + term)

    tw = grouped["total_effective_weight"].astype(float)
    has_weight = tw > 0
    safe_tw = tw.where(has_weight, np.nan)
    grouped["raw_observed_factor_evidence"] = (num.astype(float) / safe_tw).where(has_weight, np.nan)

    tw_base = pd.to_numeric(grouped["total_base_weight"], errors="coerce").fillna(0.0)
    grouped["factor_confidence"] = np.where(tw_base > 0.0, tw / tw_base, 0.0).astype(float)
    grouped["factor_confidence"] = grouped["factor_confidence"].clip(lower=0.0, upper=1.0)
    grouped["confidence_weighted_availability"] = grouped["factor_confidence"].astype(float)

    conf_frame = pd.DataFrame(index=grouped.index)
    for g in GROUP_TYPES:
        ccol = f"group_confidence_{g}"
        ecol = f"effective_weight_{g}"
        cvals = pd.to_numeric(grouped.get(ccol), errors="coerce")
        evals = pd.to_numeric(grouped.get(ecol), errors="coerce").fillna(0.0)
        conf_frame[g] = cvals.where(evals > 0.0, np.nan)
    grouped["confidence_mean"] = conf_frame.mean(axis=1, skipna=True).fillna(0.0).astype(float)
    grouped["confidence_max"] = conf_frame.max(axis=1, skipna=True).fillna(0.0).astype(float)
    grouped["confidence_min"] = conf_frame.min(axis=1, skipna=True).fillna(0.0).astype(float)

    grouped["prior_evidence"] = pd.to_numeric(grouped["prior_evidence_factor"], errors="coerce").fillna(0.0).astype(float)
    grouped["final_factor_evidence"] = grouped["raw_observed_factor_evidence"].where(has_weight, np.nan)
    grouped["final_factor_score"] = grouped["final_factor_evidence"].map(_evidence_to_score_or_nan).astype(float)

    og = pd.to_numeric(grouped["observed_group_count"], errors="coerce").fillna(0).astype(np.int64)
    sm = pd.to_numeric(grouped["structural_missing_group_count"], errors="coerce").fillna(0).astype(np.int64)
    im = pd.to_numeric(grouped["incidental_missing_group_count"], errors="coerce").fillna(0).astype(np.int64)
    grouped["factor_source"] = _final_factor_source(og, sm, im)
    grouped["factor_missing_class"] = _factor_missing_class(og, sm, im)

    grouped["category"] = grouped["category_valid"]
    grouped = grouped.drop(columns=["category_valid"], errors="ignore")

    nan_diag = np.nan
    for c in ("donor_evidence", "donor_count", "donor_confidence"):
        grouped[c] = nan_diag
    grouped["shrink_lambda"] = (1.0 - grouped["factor_confidence"]).astype(float)
    grouped["aggregation_model_version"] = AGGREGATION_MODEL_VERSION

    out_cols = [
        "symbol",
        "as_of_date",
        "factor_name",
        "category",
        "factor_source",
        "factor_missing_class",
        "observed_group_count",
        "structural_missing_group_count",
        "incidental_missing_group_count",
        "raw_observed_factor_evidence",
        "prior_evidence",
        "donor_evidence",
        "donor_count",
        "donor_confidence",
        "shrink_lambda",
        "relative_evidence",
        "absolute_evidence",
        "absolute_weight",
        "blend_method",
        "absolute_enabled",
        "final_factor_evidence",
        "final_factor_score",
        "valid_group_count",
        "total_effective_weight",
        "total_base_weight",
        "availability",
        "factor_confidence",
        "confidence_weighted_availability",
        "mean_confidence",
        "confidence_mean",
        "confidence_max",
        "confidence_min",
        "aggregation_model_version",
    ]
    grouped = grouped.rename(columns={"confidence_valid": "mean_confidence"})
    for g in GROUP_TYPES:
        out_cols.append(f"group_evidence_{g}")
    for g in GROUP_TYPES:
        out_cols.append(f"base_weight_{g}")
    for g in GROUP_TYPES:
        out_cols.append(f"effective_weight_{g}")
    for g in GROUP_TYPES:
        out_cols.append(f"group_confidence_{g}")
    for g in GROUP_TYPES:
        out_cols.append(f"weight_{g}")

    for c in out_cols:
        if c not in grouped.columns:
            grouped[c] = pd.NA

    return grouped[out_cols].copy()


def _save_df(df: pd.DataFrame, parquet_path: Path, csv_path: Path) -> None:
    parquet_path.parent.mkdir(parents=True, exist_ok=True)
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    try:
        df.to_parquet(parquet_path, index=False)
    except Exception as e:
        print(f"Warning: failed to save parquet ({parquet_path}): {e}. CSV saved successfully.")


def main(input_dir: str | Path = "output", output_dir: str | Path = "output/scoring") -> None:
    input_dir = Path(input_dir)
    out_dir = Path(output_dir)

    parquet_in = input_dir / "scoring" / "group_factor_scores_latest.parquet"
    csv_in = input_dir / "scoring" / "group_factor_scores_latest.csv"

    df = _read_df(parquet_in)
    if df.empty:
        df = _read_df(csv_in)

    if df.empty:
        print("No input factor score snapshot found; nothing to aggregate.")
        return

    print("donor/prior disabled")
    print("simple observed-only factor aggregation")

    if "adjusted_evidence" not in df.columns:
        df["adjusted_evidence"] = pd.NA
    if "confidence" not in df.columns:
        df["confidence"] = pd.NA

    scores_df = _aggregate_symbol_factor_scores(df)

    expected_factors = set(FACTOR_SPECS.keys())
    have_factors = set(scores_df["factor_name"].dropna().unique().tolist())
    factor_coverage = len(have_factors) / len(expected_factors) if expected_factors else 0.0

    print(f"Input rows: {len(df)} | Aggregated rows: {len(scores_df)}")
    print(f"Factor coverage (computed/expected): {len(have_factors)}/{len(expected_factors)} = {factor_coverage:.2%}")

    if len(scores_df) > 0 and "factor_source" in scores_df.columns:
        print("Missing / provenance summary — factor_source:")
        vc_src = scores_df["factor_source"].value_counts(dropna=False)
        for k, v in vc_src.items():
            print(f"  {k}: {int(v)}")
    if len(scores_df) > 0 and "factor_missing_class" in scores_df.columns:
        print("Missing / provenance summary — factor_missing_class:")
        vc_mc = scores_df["factor_missing_class"].value_counts(dropna=False)
        for k, v in vc_mc.items():
            print(f"  {k}: {int(v)}")

    if len(scores_df) > 0:
        print("Diagnostic — factor aggregation (missing must not become neutral score 50):")
        if "factor_source" in scores_df.columns:
            print("  factor_source value_counts:")
            print(scores_df["factor_source"].value_counts(dropna=False).to_string())
        ev = pd.to_numeric(scores_df.get("final_factor_evidence"), errors="coerce")
        fs = pd.to_numeric(scores_df.get("final_factor_score"), errors="coerce")
        print(f"  final_factor_evidence isna mean: {ev.isna().mean():.6f}")
        miss_ev = ev.isna()
        bad_50 = miss_ev & fs.notna() & (np.abs(fs - 50.0) < 1e-6)
        n_bad = int(bad_50.sum())
        if n_bad > 0:
            print(
                f"  WARNING: rows with missing final_factor_evidence but final_factor_score==50: {n_bad} "
                "(expected 0)"
            )
        else:
            print("  OK: no rows with missing final_factor_evidence and final_factor_score==50")

    parquet_out = out_dir / "symbol_factor_scores_latest.parquet"
    csv_out = out_dir / "symbol_factor_scores_latest.csv"
    _save_df(scores_df, parquet_out, csv_out)
    print(f"Saved: {parquet_out}")


if __name__ == "__main__":
    main()
