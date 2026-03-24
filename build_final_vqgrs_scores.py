# -*- coding: utf-8 -*-
"""
Build final VQGRS scores from category-level evidences.

Input:
  - output/scoring/symbol_category_scores_latest.(parquet|csv)

Output:
  - output/scoring/final_vqgrs_scores_latest.(parquet|csv)
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from score_primitives import evidence_to_score


CORE_CATS = ["V", "Q", "G", "R", "S"]

TRACK_WEIGHTS: dict[str, dict[str, float]] = {
    "equal": {"V": 0.20, "Q": 0.20, "G": 0.20, "R": 0.20, "S": 0.20},
    "track_A": {"V": 0.40, "Q": 0.20, "G": 0.10, "R": 0.20, "S": 0.10},
    "track_B": {"V": 0.25, "Q": 0.30, "G": 0.10, "R": 0.15, "S": 0.20},
    "track_C": {"V": 0.20, "Q": 0.20, "G": 0.35, "R": 0.15, "S": 0.10},
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


def _weighted_evidence(df: pd.DataFrame, weights: dict[str, float]) -> pd.Series:
    num = pd.Series(0.0, index=df.index, dtype=float)
    den = pd.Series(0.0, index=df.index, dtype=float)
    for cat, w in weights.items():
        col = f"final_evidence_{cat}"
        vals = pd.to_numeric(df[col], errors="coerce")
        valid = vals.notna()
        wf = float(w)
        num = num + vals.fillna(0.0) * wf
        den = den + np.where(valid, wf, 0.0)
    return pd.Series(np.where(den > 0.0, num / den, 0.0), index=df.index, dtype=float)


def build_final_vqgrs_scores_df(df_cat: pd.DataFrame) -> pd.DataFrame:
    if df_cat is None or df_cat.empty:
        return pd.DataFrame()

    required = {"symbol", "as_of_date", *[f"final_evidence_{c}" for c in CORE_CATS]}
    missing = [c for c in required if c not in df_cat.columns]
    if missing:
        raise ValueError(f"Missing required input columns: {missing}")

    out = df_cat[["symbol", "as_of_date"]].copy()

    for c in CORE_CATS:
        out[f"final_evidence_{c}"] = pd.to_numeric(df_cat[f"final_evidence_{c}"], errors="coerce").fillna(0.0).astype(float)
        score_col = f"score_{c}"
        if score_col in df_cat.columns:
            out[score_col] = pd.to_numeric(df_cat[score_col], errors="coerce").astype(float)
        else:
            out[score_col] = out[f"final_evidence_{c}"].map(lambda x: evidence_to_score(float(x)) if pd.notna(x) else np.nan).astype(float)

    out["final_evidence_equal"] = _weighted_evidence(out, TRACK_WEIGHTS["equal"])
    out["final_score_equal"] = out["final_evidence_equal"].map(lambda x: evidence_to_score(float(x)) if pd.notna(x) else np.nan).astype(float)

    out["final_evidence_track_A"] = _weighted_evidence(out, TRACK_WEIGHTS["track_A"])
    out["final_score_track_A"] = out["final_evidence_track_A"].map(lambda x: evidence_to_score(float(x)) if pd.notna(x) else np.nan).astype(float)

    out["final_evidence_track_B"] = _weighted_evidence(out, TRACK_WEIGHTS["track_B"])
    out["final_score_track_B"] = out["final_evidence_track_B"].map(lambda x: evidence_to_score(float(x)) if pd.notna(x) else np.nan).astype(float)

    out["final_evidence_track_C"] = _weighted_evidence(out, TRACK_WEIGHTS["track_C"])
    out["final_score_track_C"] = out["final_evidence_track_C"].map(lambda x: evidence_to_score(float(x)) if pd.notna(x) else np.nan).astype(float)

    out_cols = [
        "symbol",
        "as_of_date",
        "final_evidence_equal",
        "final_score_equal",
        "final_evidence_track_A",
        "final_score_track_A",
        "final_evidence_track_B",
        "final_score_track_B",
        "final_evidence_track_C",
        "final_score_track_C",
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
    ]
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

    print(f"Input category rows: {len(df)}")
    out = build_final_vqgrs_scores_df(df)
    print(f"Output final-score rows: {len(out)}")

    parquet_out = output_dir / "final_vqgrs_scores_latest.parquet"
    csv_out = output_dir / "final_vqgrs_scores_latest.csv"
    _save_df(out, parquet_out, csv_out)
    print(f"Saved: {parquet_out}")


if __name__ == "__main__":
    main()

