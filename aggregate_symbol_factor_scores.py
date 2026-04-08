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
AGGREGATION_MODEL_VERSION = "factor_agg_conf_weight_v1"

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
        term = grouped[s_col].fillna(0.0).astype(float) * grouped[w_col].astype(float)
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
    grouped["final_factor_evidence"] = np.where(
        has_weight,
        grouped["raw_observed_factor_evidence"].astype(float),
        0.0,
    ).astype(float)
    grouped["final_factor_score"] = grouped["final_factor_evidence"].map(
        lambda x: evidence_to_score(float(x)) if pd.notna(x) else 50.0
    )

    grouped["factor_source"] = np.where(grouped["valid_group_count"] >= 1, "observed", "neutral")

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

    parquet_out = out_dir / "symbol_factor_scores_latest.parquet"
    csv_out = out_dir / "symbol_factor_scores_latest.csv"
    _save_df(scores_df, parquet_out, csv_out)
    print(f"Saved: {parquet_out}")


if __name__ == "__main__":
    main()
