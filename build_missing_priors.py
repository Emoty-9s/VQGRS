# -*- coding: utf-8 -*-
"""
Build per-factor missing prior scores from group_factor_scores_latest.

Input:
  - output/scoring/group_factor_scores_latest.(parquet|csv)

Output:
  - output/scoring/missing_priors_latest.(parquet|csv)

This module only builds lookup tables; it does not change the scoring engine.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


PARQUET_IN = "group_factor_scores_latest.parquet"
CSV_IN = "group_factor_scores_latest.csv"
PARQUET_OUT = "missing_priors_latest.parquet"
CSV_OUT = "missing_priors_latest.csv"


def _read_df(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    if path.suffix.lower() == ".csv":
        return pd.read_csv(path, low_memory=False)
    return pd.read_parquet(path)


def _save_df(df: pd.DataFrame, parquet_path: Path, csv_path: Path) -> None:
    parquet_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    try:
        df.to_parquet(parquet_path, index=False)
    except Exception as exc:
        print(f"Warning: could not write parquet ({exc}); CSV was written.")


def _valid_score_mask(df: pd.DataFrame) -> pd.Series:
    """Rows with a usable adjusted_score for prior estimation."""
    if df.empty or "adjusted_score" not in df.columns:
        return pd.Series(False, index=df.index)
    adj = pd.to_numeric(df["adjusted_score"], errors="coerce")
    base = adj.notna()
    if "is_valid_score" in df.columns:
        iv = df["is_valid_score"]
        if iv.dtype == object:
            iv_bool = iv.map(
                lambda x: str(x).strip().lower() in ("1", "true", "t", "yes")
                if pd.notna(x)
                else False
            )
        else:
            iv_bool = iv.fillna(0).astype(bool)
        base = base & iv_bool
    return base


def _mean_and_count(sub: pd.DataFrame) -> tuple[float, int]:
    if sub is None or sub.empty:
        return float("nan"), 0
    adj = pd.to_numeric(sub["adjusted_score"], errors="coerce").dropna()
    if adj.empty:
        return float("nan"), 0
    return float(adj.mean()), int(len(adj))


def _normalize_group_type(val: Any) -> str:
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return "ALL"
    s = str(val).strip()
    return s if s else "ALL"


def build_missing_priors_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    For each (factor_name, category, group_type) key, compute prior_score using:
      1) mean valid adjusted_score in same group_type + category (+ factor)
      2) else mean valid in same category (+ factor), all group types
      3) else mean valid for factor_name (all rows)
      4) else 50.0

    structural_prior_* columns: category-level valid mean for the factor (reference for
    structural_missing handling; same pool as step 2 for that factor/category).
    """
    out_cols = [
        "factor_name",
        "category",
        "group_type",
        "prior_score",
        "support_count",
        "prior_level",
        "structural_prior_score",
        "structural_support_count",
        "structural_prior_level",
    ]
    if df is None or df.empty:
        return pd.DataFrame(columns=out_cols)

    work = df.copy()
    if "factor_name" not in work.columns:
        return pd.DataFrame(columns=out_cols)
    if "category" not in work.columns:
        work["category"] = ""
    if "group_type" not in work.columns:
        work["group_type"] = "ALL"

    work["factor_name"] = work["factor_name"].astype(str)
    work["category"] = work["category"].fillna("").astype(str)
    work["group_type"] = work["group_type"].map(_normalize_group_type)

    valid_m = _valid_score_mask(work)
    vf = work.loc[valid_m].copy()

    keys = work[["factor_name", "category", "group_type"]].drop_duplicates()

    # Category-level stats for structural reference: (factor_name, category)
    structural_by_fc: dict[tuple[str, str], tuple[float, int]] = {}
    if not vf.empty:
        for (fn, cat), sub in vf.groupby(["factor_name", "category"], dropna=False):
            m, c = _mean_and_count(sub)
            structural_by_fc[(str(fn), str(cat))] = (m, c)

    rows: list[dict[str, Any]] = []
    for _, kr in keys.iterrows():
        fn = str(kr["factor_name"])
        cat = str(kr["category"])
        gt = _normalize_group_type(kr["group_type"])

        sub_fcg = vf[
            (vf["factor_name"] == fn) & (vf["category"] == cat) & (vf["group_type"] == gt)
        ]
        m, c = _mean_and_count(sub_fcg)
        if c > 0 and not np.isnan(m):
            prior = m
            level = "group_type_category"
            sup = c
        else:
            sub_fc = vf[(vf["factor_name"] == fn) & (vf["category"] == cat)]
            m2, c2 = _mean_and_count(sub_fc)
            if c2 > 0 and not np.isnan(m2):
                prior = m2
                level = "category"
                sup = c2
            else:
                sub_f = vf[vf["factor_name"] == fn]
                m3, c3 = _mean_and_count(sub_f)
                if c3 > 0 and not np.isnan(m3):
                    prior = m3
                    level = "global"
                    sup = c3
                else:
                    prior = 50.0
                    level = "fallback_50"
                    sup = 0

        sp_m, sp_c = structural_by_fc.get((fn, cat), (float("nan"), 0))
        sp_level = "category_valid_mean" if sp_c > 0 and not np.isnan(sp_m) else "no_valid_in_category"

        rows.append(
            {
                "factor_name": fn,
                "category": cat,
                "group_type": gt,
                "prior_score": float(prior),
                "support_count": int(sup),
                "prior_level": level,
                "structural_prior_score": float(sp_m) if sp_c > 0 and not np.isnan(sp_m) else np.nan,
                "structural_support_count": int(sp_c),
                "structural_prior_level": sp_level,
            }
        )

    return pd.DataFrame(rows, columns=out_cols)


def main(input_dir: str | Path = "output", output_dir: str | Path = "output/scoring") -> None:
    input_dir = Path(input_dir)
    output_dir = Path(output_dir)
    scoring_dir = input_dir / "scoring"

    p_parquet = scoring_dir / PARQUET_IN
    p_csv = scoring_dir / CSV_IN

    df = _read_df(p_parquet)
    if df.empty:
        df = _read_df(p_csv)

    if df.empty:
        print("No group_factor_scores_latest input found; writing empty priors table.")
        out = build_missing_priors_df(df)
    else:
        print(f"Input group factor rows: {len(df)}")
        out = build_missing_priors_df(df)
        print(f"Prior rows: {len(out)}")

    parquet_out = output_dir / PARQUET_OUT
    csv_out = output_dir / CSV_OUT
    _save_df(out, parquet_out, csv_out)
    print(f"Saved: {parquet_out}")


if __name__ == "__main__":
    main()
