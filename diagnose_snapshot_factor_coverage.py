# -*- coding: utf-8 -*-
"""
Diagnose whether each A/B/C/D/E snapshot retains all columns from data/factors_latest.

Usage:
  python diagnose_snapshot_factor_coverage.py

Outputs console summary and output/diagnostics/snapshot_factor_coverage_report.csv
"""
from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import pandas as pd

DEFAULT_DATA_DIR = Path("data")
DEFAULT_OUTPUT_DIR = Path("output")
DIAGNOSTICS_DIR_NAME = "diagnostics"
REPORT_CSV_NAME = "snapshot_factor_coverage_report.csv"

GROUP_KEYS = ("A", "B", "C", "D", "E")

# Snapshot-only columns (tags, peer meta, etc.): not expected to appear in factors_latest.
# Used only to label "extra" columns that are meta vs likely aliases; coverage uses factors_latest only.
_SNAPSHOT_META_LIKE: frozenset[str] = frozenset(
    {
        "as_of_date",
        "symbol",
        "group_type",
        "group_tag",
        "group_a",
        "group_b",
        "group_c",
        "group_d",
        "group_e",
        "group_a_mode",
        "group_a_sector",
        "group_a_industry",
        "group_a_sector_count",
        "group_a_industry_count",
        "sector",
        "industry",
        "a_peer_mode",
        "a_base_count",
        "a_added_count",
        "a_final_peer_count",
        "a_peer_shortfall_flag",
        "a_add_sector_value",
        "a_add_need_count",
        "a_add_candidate_count_raw",
        "a_add_candidate_count_after_valid",
        "a_add_selected_count",
        "a_add_similarity_factors_used",
        "a_add_debug_reason",
        "peer_method",
        "peer_quality",
        "final_peer_count",
        "tolerance_used",
        "b_peer_symbols",
        "group_b_market_cap_valid",
        "group_b_revenue_valid",
        "group_b_assets_valid",
        "group_b_mode",
        "group_b_size_score",
        "group_b_weight_mcap",
        "group_b_weight_revenue",
        "group_b_weight_assets",
        "group_b_valid_components",
        "group_b_full_peer_count",
        "group_b_adjusted_peer_count",
        "group_b_adjusted_peer_count_relaxed",
        "group_b_relaxed_applied",
        "group_b_relaxed_final_tolerance",
        "group_b_relaxed_target_met",
        "group_b_final_peer_count",
        "group_b_final_peer_method",
        "group_b_nearest_fill_added",
        "group_b_peer_quality",
        "group_b_no_mcap_peer_count",
        "group_b_no_mcap_peer_method",
        "group_b_no_mcap_peer_quality",
    }
)


def _is_rep_or_dev(name: str) -> bool:
    return name.startswith("rep__") or name.startswith("dev__")


def load_factors_latest(data_dir: Path | None = None) -> pd.DataFrame:
    """Load factors_latest: CSV preferred, else parquet."""
    root = Path(data_dir) if data_dir is not None else DEFAULT_DATA_DIR
    fcsv = root / "factors_latest.csv"
    fpq = root / "factors_latest.parquet"
    if fcsv.exists():
        return pd.read_csv(fcsv, low_memory=False)
    if fpq.exists():
        return pd.read_parquet(fpq)
    raise FileNotFoundError(f"factors_latest not found under {root} (expected .csv or .parquet).")


def resolve_snapshot_path(group: str, output_dir: Path | None = None) -> Path | None:
    """
    Return path to latest snapshot file: parquet preferred, else csv.
    Paths: output/group_{g}/group_{g}_snapshot_latest.parquet | .csv
    """
    g = group.strip().lower()
    base = Path(output_dir) if output_dir is not None else DEFAULT_OUTPUT_DIR
    sub = base / f"group_{g}"
    pq = sub / f"group_{g}_snapshot_latest.parquet"
    csv = sub / f"group_{g}_snapshot_latest.csv"
    if pq.exists():
        return pq
    if csv.exists():
        return csv
    return None


def load_snapshot(group: str, output_dir: Path | None = None) -> pd.DataFrame:
    p = resolve_snapshot_path(group, output_dir=output_dir)
    if p is None:
        g = group.strip().lower()
        raise FileNotFoundError(
            f"Snapshot for group {group} not found. Expected "
            f"output/group_{g}/group_{g}_snapshot_latest.parquet or .csv"
        )
    if p.suffix.lower() == ".csv":
        return pd.read_csv(p, low_memory=False)
    return pd.read_parquet(p)


def compare_coverage(
    factor_cols: list[str],
    snapshot_cols: set[str],
) -> dict[str, Any]:
    """
    Coverage is strictly: each column name from factors_latest must appear in snapshot.
    rep__/dev__ and snapshot meta are not part of the expected factor column list.
    """
    fl = list(factor_cols)
    present = [c for c in fl if c in snapshot_cols]
    missing = [c for c in fl if c not in snapshot_cols]
    snap_extra = snapshot_cols - set(fl)
    rep_dev = {c for c in snap_extra if _is_rep_or_dev(c)}
    extra_non_factor = snap_extra - rep_dev
    meta_extra = sorted(c for c in extra_non_factor if c in _SNAPSHOT_META_LIKE)
    alias_like = sorted(c for c in extra_non_factor if c not in _SNAPSHOT_META_LIKE)
    return {
        "total_factor_columns": len(fl),
        "present_factor_columns_count": len(present),
        "missing_factor_columns_count": len(missing),
        "missing_factor_columns": missing,
        "extra_rep_dev_count": len(rep_dev),
        "extra_alias_like_columns": alias_like,
        "extra_meta_like_columns": meta_extra,
    }


def run_diagnosis(
    data_dir: Path | None = None,
    output_dir: Path | None = None,
) -> pd.DataFrame:
    data_dir = Path(data_dir) if data_dir is not None else DEFAULT_DATA_DIR
    output_dir = Path(output_dir) if output_dir is not None else DEFAULT_OUTPUT_DIR

    factors = load_factors_latest(data_dir)
    factor_cols = list(factors.columns)

    rows: list[dict[str, Any]] = []
    for g in GROUP_KEYS:
        path = resolve_snapshot_path(g, output_dir=output_dir)
        if path is None:
            rows.append(
                {
                    "group": g,
                    "snapshot_path": "",
                    "total_factor_columns": len(factor_cols),
                    "present_factor_columns_count": "",
                    "missing_factor_columns_count": "",
                    "missing_factor_columns": "",
                    "extra_alias_like_columns": "",
                    "extra_meta_like_columns": "",
                    "extra_rep_dev_count": "",
                    "status": "missing_snapshot_file",
                }
            )
            print(f"[{g}] SKIP: snapshot file not found under output/group_{g.lower()}/")
            continue
        try:
            snap = load_snapshot(g, output_dir=output_dir)
        except Exception as e:
            rows.append(
                {
                    "group": g,
                    "snapshot_path": str(path),
                    "total_factor_columns": len(factor_cols),
                    "present_factor_columns_count": "",
                    "missing_factor_columns_count": "",
                    "missing_factor_columns": "",
                    "extra_alias_like_columns": "",
                    "extra_meta_like_columns": "",
                    "extra_rep_dev_count": "",
                    "status": f"error: {e}",
                }
            )
            print(f"[{g}] ERROR loading snapshot: {e}")
            continue

        snap_cols = set(snap.columns)
        r = compare_coverage(factor_cols, snap_cols)
        status = "ok" if r["missing_factor_columns_count"] == 0 else "missing_factors"
        rows.append(
            {
                "group": g,
                "snapshot_path": str(path),
                "total_factor_columns": r["total_factor_columns"],
                "present_factor_columns_count": r["present_factor_columns_count"],
                "missing_factor_columns_count": r["missing_factor_columns_count"],
                "missing_factor_columns": "; ".join(r["missing_factor_columns"]),
                "extra_alias_like_columns": "; ".join(r["extra_alias_like_columns"]),
                "extra_meta_like_columns": "; ".join(r["extra_meta_like_columns"]),
                "extra_rep_dev_count": r["extra_rep_dev_count"],
                "status": status,
            }
        )
        print(
            f"[{g}] path={path}\n"
            f"      total_factor_columns={r['total_factor_columns']} "
            f"present={r['present_factor_columns_count']} "
            f"missing={r['missing_factor_columns_count']} "
            f"rep_dev_cols={r['extra_rep_dev_count']} "
            f"status={status}"
        )
        if r["missing_factor_columns"]:
            print(f"      missing: {r['missing_factor_columns'][:20]}{' ...' if len(r['missing_factor_columns']) > 20 else ''}")

    return pd.DataFrame(rows)


def save_report(df: pd.DataFrame, output_dir: Path | None = None) -> Path:
    output_dir = Path(output_dir) if output_dir is not None else DEFAULT_OUTPUT_DIR
    out_dir = output_dir / DIAGNOSTICS_DIR_NAME
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / REPORT_CSV_NAME
    df.to_csv(out_path, index=False, encoding="utf-8-sig")
    return out_path


def main() -> None:
    data_dir = Path(DEFAULT_DATA_DIR)
    output_dir = Path(DEFAULT_OUTPUT_DIR)
    if len(sys.argv) >= 2:
        data_dir = Path(sys.argv[1])
    if len(sys.argv) >= 3:
        output_dir = Path(sys.argv[2])

    print(f"data_dir={data_dir.resolve()}")
    print(f"output_dir={output_dir.resolve()}")
    print("")

    try:
        load_factors_latest(data_dir)
    except FileNotFoundError as e:
        print(f"ERROR: {e}")
        sys.exit(1)

    df = run_diagnosis(data_dir=data_dir, output_dir=output_dir)
    out_path = save_report(df, output_dir=output_dir)
    print("")
    print(f"Report saved: {out_path.resolve()}")


if __name__ == "__main__":
    main()
