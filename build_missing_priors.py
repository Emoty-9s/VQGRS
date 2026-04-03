# -*- coding: utf-8 -*-
"""
Placeholder for legacy ``missing_priors_latest`` outputs.

The simple scoring pipeline no longer reads or applies missing priors; this script only writes
empty schema files so existing run orders and paths keep working.

Output (unchanged paths):
  - output/scoring/missing_priors_latest.parquet
  - output/scoring/missing_priors_latest.csv
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd

PARQUET_OUT = "missing_priors_latest.parquet"
CSV_OUT = "missing_priors_latest.csv"

PLACEHOLDER_COLUMNS: list[str] = [
    "factor_name",
    "category",
    "group_type",
    "prior_score",
    "support_count",
    "prior_level",
]


def _save_df(df: pd.DataFrame, parquet_path: Path, csv_path: Path) -> None:
    parquet_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    try:
        df.to_parquet(parquet_path, index=False)
    except Exception as exc:
        print(f"Warning: could not write parquet ({exc}); CSV was written.")


def build_missing_priors_df() -> pd.DataFrame:
    """Return an empty priors table with the standard column schema (no computation)."""
    return pd.DataFrame(columns=PLACEHOLDER_COLUMNS)


def main(input_dir: str | Path = "output", output_dir: str | Path = "output/scoring") -> None:
    _ = input_dir
    output_dir = Path(output_dir)

    print("missing priors disabled in simple scoring mode")

    out = build_missing_priors_df()
    parquet_out = output_dir / PARQUET_OUT
    csv_out = output_dir / CSV_OUT
    _save_df(out, parquet_out, csv_out)
    print(f"Saved: {parquet_out}")


if __name__ == "__main__":
    main()
