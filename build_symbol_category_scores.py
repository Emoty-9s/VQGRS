# -*- coding: utf-8 -*-
"""
Build per-symbol per-category scores (V/Q/G/R/S/STI) from symbol_factor_scores_latest.

Input:
  - output/scoring/symbol_factor_scores_latest.(parquet|csv)

Output:
  - output/scoring/symbol_category_scores_latest.(parquet|csv)
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from score_factor_config import CATEGORY_TO_FACTORS, FACTOR_SPECS
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

    keys = df_factor_scores[["symbol", "as_of_date"]].drop_duplicates()
    if not enabled_factors:
        out = keys.copy()
        out[f"raw_score_{category}"] = np.nan  # compatibility alias
        out[f"raw_evidence_{category}"] = np.nan
        out[f"final_evidence_{category}"] = _CATEGORY_EVIDENCE_PRIOR
        out[f"score_{category}"] = 50.0
        out[f"count_{category}"] = 0
        out[f"weight_sum_{category}"] = 0.0
        out[f"conf_{category}"] = 0.0
        out[f"observed_weight_{category}"] = 0.0
        out[f"observed_ratio_{category}"] = 0.0
        out[f"final_conf_{category}"] = 0.0
        return out

    df_cat = df_factor_scores[df_factor_scores["category"] == category].copy()
    if df_cat.empty:
        out = keys.copy()
        out[f"raw_score_{category}"] = np.nan
        out[f"raw_evidence_{category}"] = np.nan
        out[f"final_evidence_{category}"] = _CATEGORY_EVIDENCE_PRIOR
        out[f"score_{category}"] = 50.0
        out[f"count_{category}"] = 0
        out[f"weight_sum_{category}"] = 0.0
        out[f"conf_{category}"] = 0.0
        out[f"observed_weight_{category}"] = 0.0
        out[f"observed_ratio_{category}"] = 0.0
        out[f"final_conf_{category}"] = 0.0
        return out

    # Keep only enabled factors for this category.
    df_cat = df_cat[df_cat["factor_name"].isin(enabled_factors)].copy()
    if df_cat.empty:
        out = keys.copy()
        out[f"raw_score_{category}"] = np.nan
        out[f"raw_evidence_{category}"] = np.nan
        out[f"final_evidence_{category}"] = _CATEGORY_EVIDENCE_PRIOR
        out[f"score_{category}"] = 50.0
        out[f"count_{category}"] = 0
        out[f"weight_sum_{category}"] = 0.0
        out[f"conf_{category}"] = 0.0
        out[f"observed_weight_{category}"] = 0.0
        out[f"observed_ratio_{category}"] = 0.0
        out[f"final_conf_{category}"] = 0.0
        return out

    if "factor_source" not in df_cat.columns:
        df_cat["factor_source"] = "observed"
    else:
        df_cat["factor_source"] = df_cat["factor_source"].fillna("unknown")

    df_cat["factor_weight"] = df_cat["factor_name"].map(lambda x: float(FACTOR_SPECS[x].weight))
    df_cat["final_factor_evidence"] = pd.to_numeric(df_cat["final_factor_evidence"], errors="coerce")
    valid = df_cat["final_factor_evidence"].notna()
    is_observed = df_cat["factor_source"].astype(str).str.strip().str.lower() == "observed"

    df_cat["valid_factor"] = valid
    df_cat["weighted_evidence_term"] = df_cat["final_factor_evidence"] * df_cat["factor_weight"]
    df_cat["weighted_weight_term"] = df_cat["factor_weight"].where(valid, other=0.0)
    df_cat["weighted_evidence_term"] = df_cat["weighted_evidence_term"].where(valid, other=0.0)
    df_cat["observed_weight_term"] = np.where(valid & is_observed, df_cat["factor_weight"], 0.0).astype(float)

    grouped = df_cat.groupby(["symbol", "as_of_date"], dropna=False).agg(
        final_weighted_sum=("weighted_evidence_term", "sum"),
        total_weight=("weighted_weight_term", "sum"),
        count_valid=("valid_factor", "sum"),
        observed_weight=("observed_weight_term", "sum"),
    )
    grouped = grouped.reset_index()

    score_col = f"score_{category}"
    raw_col = f"raw_evidence_{category}"
    raw_score_alias_col = f"raw_score_{category}"
    final_evd_col = f"final_evidence_{category}"
    has_weight = pd.to_numeric(grouped["total_weight"], errors="coerce").fillna(0.0) > 0
    num = pd.to_numeric(grouped["final_weighted_sum"], errors="coerce")
    den = pd.to_numeric(grouped["total_weight"], errors="coerce").replace({0.0: np.nan})
    grouped[raw_col] = np.where(has_weight, num / den, np.nan).astype(float)

    tw = pd.to_numeric(grouped["total_weight"], errors="coerce").fillna(0.0)
    ow = pd.to_numeric(grouped["observed_weight"], errors="coerce").fillna(0.0)
    grouped[f"observed_weight_{category}"] = ow.astype(float)
    grouped[f"observed_ratio_{category}"] = np.where(tw > 0.0, ow / tw, 0.0).astype(float)

    grouped[f"count_{category}"] = grouped["count_valid"].astype(int)
    grouped[f"weight_sum_{category}"] = grouped["total_weight"].astype(float)
    grouped[f"conf_{category}"] = grouped[f"count_{category}"].astype(float) / denom
    grouped[f"conf_{category}"] = grouped[f"conf_{category}"].clip(upper=1.0, lower=0.0)

    base_conf = pd.to_numeric(grouped[f"conf_{category}"], errors="coerce").fillna(0.0)
    obs_r = pd.to_numeric(grouped[f"observed_ratio_{category}"], errors="coerce").fillna(0.0)
    grouped[f"final_conf_{category}"] = (base_conf * (0.5 + 0.5 * obs_r)).astype(float)
    grouped[f"final_conf_{category}"] = grouped[f"final_conf_{category}"].clip(upper=1.0, lower=0.0)

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
    out[final_evd_col] = out[final_evd_col].fillna(_CATEGORY_EVIDENCE_PRIOR).astype(float)
    out[score_col] = out[score_col].fillna(50.0)
    out[f"count_{category}"] = out[f"count_{category}"].fillna(0).astype(int)
    out[f"weight_sum_{category}"] = out[f"weight_sum_{category}"].fillna(0.0).astype(float)
    out[f"conf_{category}"] = out[f"conf_{category}"].fillna(0.0).astype(float)
    out[f"observed_weight_{category}"] = out[f"observed_weight_{category}"].fillna(0.0).astype(float)
    out[f"observed_ratio_{category}"] = out[f"observed_ratio_{category}"].fillna(0.0).astype(float)
    out[f"final_conf_{category}"] = out[f"final_conf_{category}"].fillna(0.0).astype(float)
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
            out[f"raw_score_{cat}"] = np.nan
        if f"raw_evidence_{cat}" not in out.columns:
            out[f"raw_evidence_{cat}"] = np.nan
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

