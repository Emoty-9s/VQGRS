# -*- coding: utf-8 -*-
"""
Batch builder: convert latest group snapshots into long-format factor outputs.

Input:
  - output/group_unified/group_unified_snapshot_latest.(parquet|csv) preferred
  - else fallback to per-group latest: output/group_{a,b,c,d,e}/group_{x}_snapshot_latest.(parquet|csv)

Output:
  - output/scoring/group_factor_scores_latest.(parquet|csv)

This module reads snapshot *_latest files and enforces max(as_of_date) from file contents (not the filename).
It does not modify snapshot builders/history.
Core engine output is evidence-first (`adjusted_evidence`); score columns are kept for compatibility.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from group_snapshot_utils import finalize_snapshot_for_scoring
from score_factor_config import CATEGORY_TO_FACTORS, FACTOR_SPECS
from score_primitives import score_one_factor_one_group


GROUP_TYPES = ["A", "B", "C", "D", "E"]

# Long-table columns that downstream expects from score_one_factor_one_group.
REQUIRED_SCORE_DETAIL_COLS: tuple[str, ...] = (
    "raw_evidence",
    "prior_evidence",
    "adjusted_evidence",
    "evidence_source",
    "confidence",
    "raw_score",
    "adjusted_score",
    "missing_reason",
    "is_valid_score",
    "raw_value",
    "median_value",
    "iqr_value",
    "n_valid",
)


def _normalize_evidence_source(src: Any, missing_reason: Any) -> str:
    s = str(src).strip().lower() if src is not None else ""
    mr = str(missing_reason).strip().lower() if missing_reason is not None else ""
    if s in {"observed_evidence", "observed"}:
        return "observed"
    if s in {"structural_missing", "disabled_factor", "invalid_direction"}:
        return s
    if mr == "structural_missing":
        return "structural_missing"
    if mr == "missing_raw_value":
        return "missing_raw"
    if mr == "insufficient_peer_data":
        return "insufficient_peer_data"
    if mr:
        return mr
    return "observed"


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


def _per_group_snapshot_max_dates(input_dir: Path) -> dict[str, pd.Timestamp | None]:
    """Max as_of_date inside each group_*_snapshot_latest file (for stale-unified diagnostics)."""
    out: dict[str, pd.Timestamp | None] = {}
    for gt in GROUP_TYPES:
        low = gt.lower()
        base = input_dir / f"group_{low}"
        found = False
        for name in (f"group_{low}_snapshot_latest.parquet", f"group_{low}_snapshot_latest.csv"):
            p = base / name
            if not p.exists():
                continue
            df = _read_df(p)
            found = True
            if df.empty or "as_of_date" not in df.columns:
                out[gt] = None
            else:
                d = pd.to_datetime(df["as_of_date"], errors="coerce").dropna()
                out[gt] = pd.Timestamp(d.max()) if len(d) else None
            break
        if not found:
            out[gt] = None
    return out


def _log_snapshot_date_audit(input_dir: Path, snapshot_df: pd.DataFrame) -> None:
    per_g = _per_group_snapshot_max_dates(input_dir)
    umax: pd.Timestamp | None = None
    if not snapshot_df.empty and "as_of_date" in snapshot_df.columns:
        d = pd.to_datetime(snapshot_df["as_of_date"], errors="coerce").dropna()
        umax = pd.Timestamp(d.max()) if len(d) else None
    print("  [snapshot date audit] per-group snapshot max(as_of_date):")
    max_vals: list[pd.Timestamp] = []
    for gt, m in per_g.items():
        print(f"    group_{gt.lower()}: {m}")
        if m is not None:
            max_vals.append(m)
    print(f"  [snapshot date audit] loaded snapshot input max(as_of_date): {umax}")
    if max_vals and umax is not None:
        overall_max = max(max_vals)
        if pd.notna(overall_max) and pd.notna(umax) and umax < overall_max:
            print(
                "WARNING [snapshot date audit]: loaded unified snapshot is OLDER than at least one "
                f"per-group snapshot_latest (unified max={umax}, per-group max={overall_max}). "
                "Regenerate group_unified_snapshot_latest or rely on per-group concat path."
            )


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
                record["evidence_source"] = _normalize_evidence_source(
                    record.get("evidence_source"),
                    record.get("missing_reason"),
                )
                records.append(record)

    if not records:
        return pd.DataFrame()
    df = pd.DataFrame(records)
    for c in REQUIRED_SCORE_DETAIL_COLS:
        if c not in df.columns:
            df[c] = pd.NA
    preferred = [
        "symbol",
        "as_of_date",
        "group_type",
        "group_tag",
        "factor_name",
        "category",
        *REQUIRED_SCORE_DETAIL_COLS,
    ]
    rest = [c for c in df.columns if c not in preferred]
    return df[[c for c in preferred if c in df.columns] + rest]


def main(input_dir: str | Path = "output", output_dir: str | Path = "output/scoring") -> None:
    input_dir = Path(input_dir)
    snapshot_df = _read_snapshot_latest(input_dir=input_dir)
    if snapshot_df.empty:
        print("No snapshot input found; skipping scoring.")
        return

    _log_snapshot_date_audit(input_dir, snapshot_df)
    snapshot_df = finalize_snapshot_for_scoring(snapshot_df, label="build_group_factor_scores")

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

