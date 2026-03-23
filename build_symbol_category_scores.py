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

import pandas as pd

from score_factor_config import CATEGORY_TO_FACTORS, FACTOR_SPECS


CAT_LIST = ["V", "Q", "G", "R", "S", "STI"]
CONF_DENOM_BY_CAT: dict[str, float] = {
    "V": 3.0,
    "Q": 3.0,
    "G": 3.0,
    "R": 3.0,
    "S": 3.0,
    "STI": 5.0,
}


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
        out[f"score_{category}"] = 50.0
        out[f"count_{category}"] = 0
        out[f"weight_sum_{category}"] = 0.0
        out[f"conf_{category}"] = 0.0
        return out

    df_cat = df_factor_scores[df_factor_scores["category"] == category].copy()
    if df_cat.empty:
        out = keys.copy()
        out[f"score_{category}"] = 50.0
        out[f"count_{category}"] = 0
        out[f"weight_sum_{category}"] = 0.0
        out[f"conf_{category}"] = 0.0
        return out

    # Keep only enabled factors for this category.
    df_cat = df_cat[df_cat["factor_name"].isin(enabled_factors)].copy()
    if df_cat.empty:
        out = keys.copy()
        out[f"score_{category}"] = 50.0
        out[f"count_{category}"] = 0
        out[f"weight_sum_{category}"] = 0.0
        out[f"conf_{category}"] = 0.0
        return out

    df_cat["factor_weight"] = df_cat["factor_name"].map(lambda x: float(FACTOR_SPECS[x].weight))
    df_cat["final_factor_score"] = pd.to_numeric(df_cat["final_factor_score"], errors="coerce")
    valid = df_cat["final_factor_score"].notna()

    df_cat["valid_factor"] = valid
    df_cat["weighted_score_term"] = df_cat["final_factor_score"] * df_cat["factor_weight"]
    df_cat["weighted_weight_term"] = df_cat["factor_weight"].where(valid, other=0.0)
    df_cat["weighted_score_term"] = df_cat["weighted_score_term"].where(valid, other=0.0)

    grouped = df_cat.groupby(["symbol", "as_of_date"], dropna=False).agg(
        final_weighted_sum=("weighted_score_term", "sum"),
        total_weight=("weighted_weight_term", "sum"),
        count_valid=("valid_factor", "sum"),
    )
    grouped = grouped.reset_index()

    score_col = f"score_{category}"
    grouped[score_col] = 50.0
    has_weight = grouped["total_weight"] > 0
    grouped.loc[has_weight, score_col] = grouped.loc[has_weight, "final_weighted_sum"] / grouped.loc[
        has_weight, "total_weight"
    ]

    grouped[f"count_{category}"] = grouped["count_valid"].astype(int)
    grouped[f"weight_sum_{category}"] = grouped["total_weight"].astype(float)
    grouped[f"conf_{category}"] = grouped[f"count_{category}"].astype(float) / denom
    grouped[f"conf_{category}"] = grouped[f"conf_{category}"].clip(upper=1.0, lower=0.0)

    # Left-join to ensure all symbols exist.
    keep_cols = ["symbol", "as_of_date", score_col, f"count_{category}", f"weight_sum_{category}", f"conf_{category}"]
    grouped = grouped[keep_cols]
    out = keys.merge(grouped, on=["symbol", "as_of_date"], how="left")

    out[score_col] = out[score_col].fillna(50.0)
    out[f"count_{category}"] = out[f"count_{category}"].fillna(0).astype(int)
    out[f"weight_sum_{category}"] = out[f"weight_sum_{category}"].fillna(0.0).astype(float)
    out[f"conf_{category}"] = out[f"conf_{category}"].fillna(0.0).astype(float)
    return out


def build_symbol_category_scores_df(df_factor_scores: pd.DataFrame) -> pd.DataFrame:
    if df_factor_scores is None or df_factor_scores.empty:
        return pd.DataFrame()

    required = {"symbol", "as_of_date", "category", "factor_name", "final_factor_score"}
    missing = [c for c in required if c not in df_factor_scores.columns]
    if missing:
        raise ValueError(f"Missing required input columns: {missing}")

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
        if f"score_{cat}" not in out.columns:
            out[f"score_{cat}"] = 50.0
        if f"count_{cat}" not in out.columns:
            out[f"count_{cat}"] = 0
        if f"weight_sum_{cat}" not in out.columns:
            out[f"weight_sum_{cat}"] = 0.0
        if f"conf_{cat}" not in out.columns:
            out[f"conf_{cat}"] = 0.0
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

