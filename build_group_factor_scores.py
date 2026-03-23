# -*- coding: utf-8 -*-
"""
Batch builder: convert latest group snapshots into long-format factor scores.

Input:
  - output/group_unified/group_unified_snapshot_latest.(parquet|csv) preferred
  - else fallback to per-group latest: output/group_{a,b,c,d,e}/group_{x}_snapshot_latest.(parquet|csv)

Output:
  - output/scoring/group_factor_scores_latest.(parquet|csv)

This module does NOT modify snapshot builders/history; it only reads existing latest snapshots.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from score_factor_config import CATEGORY_TO_FACTORS, FACTOR_SPECS
from score_primitives import score_one_factor_one_group


GROUP_TYPES = ["A", "B", "C", "D", "E"]


def _read_df(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    if path.suffix.lower() == ".csv":
        return pd.read_csv(path, low_memory=False)
    return pd.read_parquet(path)


def _read_snapshot_latest(input_dir: str | Path) -> pd.DataFrame:
    """
    Load latest snapshot DataFrame:
      - try unified first (new and legacy names)
      - else concat per-group latest files
    """
    input_dir = Path(input_dir)

    unified_candidates = [
        input_dir / "group_unified" / "group_unified_snapshot_latest.parquet",
        input_dir / "group_unified" / "group_unified_snapshot_latest.csv",
        input_dir / "group_cde" / "group_cde_snapshot_latest.parquet",
        input_dir / "group_cde" / "group_cde_snapshot_latest.csv",
    ]
    for p in unified_candidates:
        if p.exists():
            return _read_df(p)

    dfs: list[pd.DataFrame] = []
    for gt in GROUP_TYPES:
        low = gt.lower()
        base = input_dir / f"group_{low}"
        candidates = [
            base / f"group_{low}_snapshot_latest.parquet",
            base / f"group_{low}_snapshot_latest.csv",
        ]
        loaded = pd.DataFrame()
        for p in candidates:
            if p.exists():
                loaded = _read_df(p)
                break
        if loaded is not None and not loaded.empty:
            dfs.append(loaded)
    if not dfs:
        return pd.DataFrame()
    return pd.concat(dfs, ignore_index=True)


def _infer_group_type_and_tag(row: pd.Series) -> tuple[str | None, str | None]:
    group_type = None
    if "group_type" in row.index and pd.notna(row.get("group_type")):
        group_type = str(row.get("group_type")).strip().upper()
    if "group_tag" in row.index and pd.notna(row.get("group_tag")):
        group_tag = str(row.get("group_tag"))
    else:
        group_tag = None

    if group_type is None:
        # Heuristic fallback using presence of group_x columns.
        for gt in GROUP_TYPES:
            col = f"group_{gt.lower()}"
            if col in row.index and pd.notna(row.get(col)):
                group_type = gt
                break

    if group_tag is None and group_type is not None:
        col = f"group_{group_type.lower()}"
        if col in row.index and pd.notna(row.get(col)):
            group_tag = str(row.get(col)).strip()

    return group_type, group_tag


def _save_long_scores(df: pd.DataFrame, output_dir: str | Path) -> None:
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = output_dir / "group_factor_scores_latest.parquet"
    csv_path = output_dir / "group_factor_scores_latest.csv"

    df.to_parquet(parquet_path, index=False)
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")


def build_group_factor_scores_df(snapshot_df: pd.DataFrame) -> pd.DataFrame:
    if snapshot_df is None or snapshot_df.empty:
        return pd.DataFrame()

    needed_base_cols = ["symbol", "as_of_date"]
    for c in needed_base_cols:
        if c not in snapshot_df.columns:
            raise ValueError(f"Snapshot input must contain column: {c}")

    records: list[dict[str, Any]] = []

    # Pre-materialize factor specs per category to avoid repeated lookups.
    factors_by_category: dict[str, list[str]] = {}
    for cat, names in CATEGORY_TO_FACTORS.items():
        enabled = [n for n in names if FACTOR_SPECS.get(n) is not None and FACTOR_SPECS[n].enabled]
        factors_by_category[cat] = enabled

    for _, row in snapshot_df.iterrows():
        group_type, group_tag = _infer_group_type_and_tag(row)
        if group_type not in {"A", "B", "C", "D", "E"}:
            # Skip rows that cannot be identified.
            continue

        as_of_date = row.get("as_of_date")
        symbol = row.get("symbol")

        # Category selection: use factor's own category list (V/Q/G/R/S/STI),
        # and compute for enabled factors across all categories.
        # This keeps output consistent even if snapshot row uses a specific group type.
        for cat, factors in factors_by_category.items():
            for factor_name in factors:
                factor_spec = FACTOR_SPECS[factor_name]
                scored = score_one_factor_one_group(row, factor_spec)
                record = {
                    "symbol": symbol,
                    "as_of_date": as_of_date,
                    "group_type": group_type,
                    "group_tag": group_tag,
                    **scored,
                }
                records.append(record)

    if not records:
        return pd.DataFrame()
    return pd.DataFrame(records)


def main(input_dir: str | Path = "output", output_dir: str | Path = "output/scoring") -> None:
    snapshot_df = _read_snapshot_latest(input_dir=input_dir)
    if snapshot_df.empty:
        print("No snapshot input found; skipping scoring.")
        return

    print(f"Input snapshot rows: {len(snapshot_df)}")
    scores_df = build_group_factor_scores_df(snapshot_df)
    print(f"Scored rows (long format): {len(scores_df)}")

    if scores_df.empty:
        print("No scored rows produced.")
        return

    if "is_valid_score" in scores_df.columns:
        valid_cnt = int(pd.to_numeric(scores_df["is_valid_score"], errors="coerce").fillna(0).sum())
    else:
        valid_cnt = 0
    print(f"Valid score rows: {valid_cnt}")

    _save_long_scores(scores_df, output_dir=output_dir)
    print(f"Saved: {Path(output_dir) / 'group_factor_scores_latest.(parquet/csv)'}")


if __name__ == "__main__":
    main()

