# -*- coding: utf-8 -*-
"""
Aggregate per-group factor scores into a single final score per:
  symbol x factor_name

Input:
  output/scoring/group_factor_scores_latest.(parquet|csv)

Output:
  output/scoring/symbol_factor_scores_latest.(parquet|csv)
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from score_factor_config import GROUP_BASE_WEIGHTS, FACTOR_SPECS


GROUP_TYPES = ["A", "B", "C", "D", "E"]


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
        # group_tag is usually: group_a / group_b / group_c / ...
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
    required = [
        "symbol",
        "as_of_date",
        "factor_name",
        "category",
        "group_type",
        "adjusted_score",
        "confidence",
    ]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns in score input: {missing}")

    df = _infer_group_type_from_columns(df)
    if "group_type" not in df.columns:
        raise ValueError("Cannot infer group_type; missing both group_type and group_tag.")

    # Numeric coercion (defensive). Non-coercible becomes NaN -> invalid.
    df = df.copy()
    df["adjusted_score"] = pd.to_numeric(df["adjusted_score"], errors="coerce")
    df["confidence"] = pd.to_numeric(df["confidence"], errors="coerce")

    # Validity filter for group contributions.
    valid_row = df["adjusted_score"].notna() & df["confidence"].notna() & (df["confidence"] > 0)

    df["category_valid"] = df["category"].where(valid_row, other=pd.NA)
    df["confidence_valid"] = df["confidence"].where(valid_row, other=pd.NA)

    # Precompute per-group columns.
    for g in GROUP_TYPES:
        col_score = f"group_score_{g}"
        col_w = f"weight_{g}"
        w_base = float(GROUP_BASE_WEIGHTS.get(g, 1.0))
        w = df["confidence"] * w_base
        df[col_score] = df["adjusted_score"].where(valid_row & (df["group_type"] == g), other=pd.NA)
        df[col_w] = w.where(valid_row & (df["group_type"] == g), other=0.0)

    key_cols = ["symbol", "as_of_date", "factor_name"]

    agg_spec: dict[str, Any] = {
        "category_valid": "first",
        "confidence_valid": "mean",
    }
    for g in GROUP_TYPES:
        agg_spec[f"group_score_{g}"] = "first"
        agg_spec[f"weight_{g}"] = "sum"

    grouped = df.groupby(key_cols, dropna=False, as_index=False).agg(agg_spec)

    # Compute coverage/availability and final scores.
    weight_sum = None
    for g in GROUP_TYPES:
        wcol = f"weight_{g}"
        if weight_sum is None:
            weight_sum = grouped[wcol].astype(float)
        else:
            weight_sum = weight_sum + grouped[wcol].astype(float)
    grouped["total_effective_weight"] = weight_sum

    # valid_group_count: number of groups with weight > 0
    w_gt0 = None
    for g in GROUP_TYPES:
        wcol = f"weight_{g}"
        if w_gt0 is None:
            w_gt0 = (grouped[wcol] > 0).astype(int)
        else:
            w_gt0 = w_gt0 + (grouped[wcol] > 0).astype(int)
    grouped["valid_group_count"] = w_gt0

    grouped["availability"] = grouped["valid_group_count"].clip(upper=3) / 3.0
    grouped["availability"] = grouped["availability"].clip(lower=0.0, upper=1.0)

    # weighted average (only among valid rows)
    num = None
    for g in GROUP_TYPES:
        s_col = f"group_score_{g}"
        w_col = f"weight_{g}"
        term = grouped[s_col].fillna(0.0).astype(float) * grouped[w_col].astype(float)
        num = term if num is None else (num + term)
    grouped["weighted_avg"] = num / grouped["total_effective_weight"].replace({0.0: pd.NA})

    # final_factor_score: if weight sum is 0 => 50
    grouped["final_factor_score"] = 50.0
    has_weight = grouped["total_effective_weight"] > 0
    grouped.loc[has_weight, "final_factor_score"] = 50.0 + grouped.loc[has_weight, "availability"] * (
        grouped.loc[has_weight, "weighted_avg"] - 50.0
    )

    grouped["category"] = grouped["category_valid"]
    grouped = grouped.drop(columns=["category_valid", "weighted_avg"], errors="ignore")

    # Ensure required output column order.
    out_cols = [
        "symbol",
        "as_of_date",
        "factor_name",
        "category",
        "final_factor_score",
        "valid_group_count",
        "total_effective_weight",
        "availability",
        "mean_confidence",
    ]
    # mean_confidence comes from confidence_valid mean.
    grouped = grouped.rename(columns={"confidence_valid": "mean_confidence"})
    for g in GROUP_TYPES:
        out_cols.append(f"group_score_{g}")
    for g in GROUP_TYPES:
        out_cols.append(f"weight_{g}")

    for c in out_cols:
        if c not in grouped.columns:
            grouped[c] = pd.NA

    return grouped[out_cols].copy()


def _save_df(df: pd.DataFrame, parquet_path: Path, csv_path: Path) -> None:
    parquet_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(parquet_path, index=False)
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")


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

    rows_in = len(df)
    scores_df = _aggregate_symbol_factor_scores(df)
    print(f"Input rows: {rows_in}")
    print(f"Aggregated rows: {len(scores_df)}")

    expected_factors = set(FACTOR_SPECS.keys())
    have_factors = set(scores_df["factor_name"].dropna().unique().tolist())
    factor_coverage = 0.0
    if expected_factors:
        factor_coverage = len(have_factors) / len(expected_factors)
    print(f"Factor coverage (computed/expected): {len(have_factors)}/{len(expected_factors)} = {factor_coverage:.2%}")

    parquet_out = out_dir / "symbol_factor_scores_latest.parquet"
    csv_out = out_dir / "symbol_factor_scores_latest.csv"
    _save_df(scores_df, parquet_out, csv_out)
    print(f"Saved: {parquet_out}")


if __name__ == "__main__":
    main()

