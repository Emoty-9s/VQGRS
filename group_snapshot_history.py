# -*- coding: utf-8 -*-
"""
Unified A/B/C/D/E snapshot storage: combine A, B, C, D, E snapshot DataFrames and save as
a single latest file and a single history file (parquet + csv).

Execution flow:
  1. python build_group_a_snapshot.py → A snapshot only; A latest saved; no unified history.
  2. python build_group_b_snapshot.py → B snapshot only; B latest saved; no unified history.
  3. python build_group_c_snapshot.py  → C snapshot only; C latest saved; no unified history.
  4. python build_group_d_snapshot.py  → D snapshot only; D latest saved; no unified history.
  5. python build_group_e_snapshot.py  → E snapshot only; E latest saved; no unified history.
  6. python group_snapshot_history.py (main) → A+B+C+D+E built, combined, unified latest + unified history saved.

Dedup key: (as_of_date, symbol, group_type). Only unified history file is written;
  no per-group history files. Per-group latest-only files are optional.

Unified outputs live under output/group_unified/ (historically group_cde; paths renamed for accuracy).
"""
from __future__ import annotations

import warnings
from pathlib import Path

import pandas as pd

# ---------------------------------------------------------------------------
# Path / config constants
# ---------------------------------------------------------------------------
DEFAULT_OUTPUT_DIR = "output"
# Combined A+B+C+D+E latest + rolling history (parquet + csv). Replaces legacy group_cde/* names.
UNIFIED_DIR_NAME = "group_unified"
UNIFIED_LATEST_NAME = "group_unified_snapshot_latest.parquet"
UNIFIED_HISTORY_NAME = "group_unified_snapshot_history.parquet"
SNAPSHOT_LATEST_SUFFIX = "_snapshot_latest.parquet"

# Dedup key for history: as_of_date + symbol + group_type (same symbol, different group_type = different row)
KEY_COLS = ["as_of_date", "symbol", "group_type"]
META_COLS = ["as_of_date", "symbol", "group_type", "group_tag"]
HISTORY_SORT_COLS = ["as_of_date", "symbol", "group_type"]


def _save_parquet_and_csv(df: pd.DataFrame, parquet_path: Path) -> None:
    """
    Save DataFrame to parquet and CSV side by side.
    CSV path is derived from parquet_path.with_suffix(".csv").
    """
    parquet_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        df.to_parquet(parquet_path, index=False)
    except Exception as e:
        warnings.warn(
            f"Failed to save parquet to {parquet_path}: {e}",
            UserWarning,
            stacklevel=2,
        )

    csv_path = parquet_path.with_suffix(".csv")
    try:
        df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    except Exception as e:
        warnings.warn(
            f"Failed to save CSV to {csv_path}: {e}",
            UserWarning,
            stacklevel=2,
        )


def combine_snapshots(snapshot_dfs: list[pd.DataFrame]) -> pd.DataFrame:
    """
    Combine multiple snapshot DataFrames (e.g. A, B, C, D, E) into one.
    Empty DataFrames are skipped. Meta columns (as_of_date, symbol, group_type, group_tag)
    are moved to the front. group_type must exist in each non-empty df.
    """
    non_empty = [df for df in snapshot_dfs if df is not None and not df.empty]
    if not non_empty:
        return pd.DataFrame()

    for df in non_empty:
        if "group_type" not in df.columns:
            raise ValueError("Each snapshot DataFrame must contain column: group_type")

    combined = pd.concat(non_empty, ignore_index=True)
    combined = combined.reset_index(drop=True)

    meta_present = [c for c in META_COLS if c in combined.columns]
    others = [c for c in combined.columns if c not in meta_present]
    return combined[meta_present + others].copy()


def _get_cde_paths(output_dir: str | Path) -> tuple[Path, Path]:
    """Return (latest_path, history_path) for unified A/B/C/D/E: output/group_unified/group_unified_snapshot_*.parquet."""
    output_dir = Path(output_dir).resolve()
    base = output_dir / UNIFIED_DIR_NAME
    latest = base / UNIFIED_LATEST_NAME
    history = base / UNIFIED_HISTORY_NAME
    return latest, history


def upsert_history_df(
    existing: pd.DataFrame,
    new: pd.DataFrame,
    key_cols: list[str] | None = None,
    sort_cols: list[str] | None = None,
) -> pd.DataFrame:
    """
    Pure in-memory upsert: remove from existing any rows whose key is in new,
    then concat existing + new, dedupe by key (keep last), sort.
    Returns combined DataFrame.
    """
    key_cols = key_cols or KEY_COLS
    sort_cols = sort_cols or HISTORY_SORT_COLS
    if new.empty:
        return existing.copy() if not existing.empty else pd.DataFrame()
    if existing.empty:
        out = new.copy()
    else:
        if not all(c in existing.columns for c in key_cols):
            out = new.copy()
        else:
            new_keys = new[key_cols].drop_duplicates()
            merged = existing.merge(new_keys, on=key_cols, how="left", indicator=True)
            existing_rest = merged[merged["_merge"] == "left_only"].drop(columns=["_merge"])
            out = pd.concat([existing_rest, new], ignore_index=True, join="outer")
    out = out.drop_duplicates(subset=key_cols, keep="last")
    if sort_cols and all(c in out.columns for c in sort_cols):
        out = out.sort_values(sort_cols).reset_index(drop=True)
    return out


def save_cde_latest(
    snapshot_df: pd.DataFrame,
    output_dir: str | Path = DEFAULT_OUTPUT_DIR,
) -> None:
    """Save unified A/B/C/D/E snapshot to output/group_unified/group_unified_snapshot_latest.(parquet,csv)."""
    for c in KEY_COLS:
        if c not in snapshot_df.columns:
            raise ValueError(f"snapshot_df must contain column: {c}")
    latest_path, _ = _get_cde_paths(output_dir)
    _save_parquet_and_csv(snapshot_df, latest_path)


def upsert_cde_history(
    snapshot_df: pd.DataFrame,
    output_dir: str | Path = DEFAULT_OUTPUT_DIR,
) -> None:
    """
    Upsert snapshot into output/group_unified/group_unified_snapshot_history.(parquet,csv).
    Dedup key: as_of_date + symbol + group_type. Same key is overwritten by new data.
    """
    for c in KEY_COLS:
        if c not in snapshot_df.columns:
            raise ValueError(f"snapshot_df must contain column: {c}")
    _, history_path = _get_cde_paths(output_dir)
    history_path.parent.mkdir(parents=True, exist_ok=True)

    existing = pd.DataFrame()
    if history_path.exists():
        try:
            existing = pd.read_parquet(history_path)
        except Exception as e:
            warnings.warn(
                f"Could not read unified snapshot history {history_path}: {e}. Creating new.",
                UserWarning,
                stacklevel=2,
            )

    combined = upsert_history_df(
        existing, snapshot_df.copy(),
        key_cols=KEY_COLS,
        sort_cols=HISTORY_SORT_COLS,
    )
    _save_parquet_and_csv(combined, history_path)


def save_cde_snapshot_and_history(
    snapshot_df: pd.DataFrame,
    output_dir: str | Path = DEFAULT_OUTPUT_DIR,
) -> None:
    """Save unified latest and upsert unified history (A/B/C/D/E; parquet + csv)."""
    save_cde_latest(snapshot_df, output_dir=output_dir)
    upsert_cde_history(snapshot_df, output_dir=output_dir)


def get_group_latest_path(
    group_type: str,
    output_dir: str | Path = DEFAULT_OUTPUT_DIR,
) -> Path:
    """Return path for optional per-group latest-only file (e.g. output/group_c/group_c_snapshot_latest.parquet)."""
    output_dir = Path(output_dir).resolve()
    group_lower = str(group_type).strip().lower()
    base = output_dir / f"group_{group_lower}"
    return base / f"group_{group_lower}{SNAPSHOT_LATEST_SUFFIX}"


def save_group_latest(
    snapshot_df: pd.DataFrame,
    group_type: str,
    output_dir: str | Path = DEFAULT_OUTPUT_DIR,
) -> None:
    """Save a single group's snapshot to its latest-only files (parquet + csv, no history). Optional use."""
    for c in KEY_COLS:
        if c not in snapshot_df.columns:
            raise ValueError(f"snapshot_df must contain column: {c}")
    path = get_group_latest_path(group_type, output_dir)
    _save_parquet_and_csv(snapshot_df, path)


def main(
    logic_dir: str | None = None,
    data_dir: str | None = None,
    output_dir: str | Path = DEFAULT_OUTPUT_DIR,
) -> None:
    """
    Build A, B, C, D, E snapshot DataFrames, combine, save unified latest + history (parquet + csv).
    Entry point for: python group_snapshot_history.py
    Uses lazy imports to avoid circular imports with build_group_*_snapshot modules.
    """
    from group_snapshot_utils import DEFAULT_DATA_DIR as _DD, DEFAULT_LOGIC_DIR as _LD
    from build_group_a_snapshot import build_group_a_snapshot_df
    from build_group_b_snapshot import build_group_b_snapshot_df
    from build_group_c_snapshot import build_group_c_snapshot_df
    from build_group_d_snapshot import build_group_d_snapshot_df
    from build_group_e_snapshot import build_group_e_snapshot_df

    logic_dir = logic_dir or str(_LD)
    data_dir = data_dir or str(_DD)

    print("Building Group A snapshot...")
    df_a = build_group_a_snapshot_df(logic_dir=logic_dir, data_dir=data_dir)
    print(f"  A rows: {len(df_a)}")

    print("Building Group B snapshot...")
    df_b = build_group_b_snapshot_df(logic_dir=logic_dir, data_dir=data_dir)
    print(f"  B rows: {len(df_b)}")

    print("Building Group C snapshot...")
    df_c = build_group_c_snapshot_df(logic_dir=logic_dir, data_dir=data_dir)
    print(f"  C rows: {len(df_c)}")

    print("Building Group D snapshot...")
    df_d = build_group_d_snapshot_df(logic_dir=logic_dir, data_dir=data_dir)
    print(f"  D rows: {len(df_d)}")

    print("Building Group E snapshot...")
    df_e = build_group_e_snapshot_df(logic_dir=logic_dir, data_dir=data_dir)
    print(f"  E rows: {len(df_e)}")

    combined = combine_snapshots([df_a, df_b, df_c, df_d, df_e])
    print(f"Combined rows: {len(combined)}")

    if combined.empty:
        print("Combined snapshot is empty; skip unified save.")
        return

    save_cde_snapshot_and_history(combined, output_dir=output_dir)
    print(f"Unified latest and history saved (output/{UNIFIED_DIR_NAME}/).")


if __name__ == "__main__":
    main()
