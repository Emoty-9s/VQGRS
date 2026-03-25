# -*- coding: utf-8 -*-
"""
Build Group D snapshot: merge latest tags + factors_latest, compute group_d
representatives and deviations. Returns D snapshot DataFrame; does not save history.
Optional: save D latest-only file. Independent runnable.
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd

from group_snapshot_history import DEFAULT_OUTPUT_DIR, save_group_latest
from group_snapshot_utils import (
    DEFAULT_DATA_DIR,
    DEFAULT_LOGIC_DIR,
    attach_representatives_and_deviations,
    compute_representatives,
    get_factor_columns,
    load_latest_tags_and_factors,
)

GROUP_COL = "group_d"
GROUP_TYPE = "D"


def build_group_d_snapshot_df(
    logic_dir: str | Path | None = None,
    data_dir: str | Path | None = None,
) -> pd.DataFrame:
    """
    Build Group D snapshot DataFrame: latest tags + factors merge, group_d reps and deviations.
    Returns DataFrame with as_of_date, symbol, group_type, group_tag, factor cols, rep cols, dev cols.

    The merged base retains all factors_latest columns; attach only appends rep__/dev__ columns.
    """
    from group_snapshot_utils import DEFAULT_DATA_DIR as _DD, DEFAULT_LOGIC_DIR as _LD
    logic_dir = Path(logic_dir) if logic_dir is not None else _LD
    data_dir = Path(data_dir) if data_dir is not None else _DD

    base, _as_of, _path = load_latest_tags_and_factors(logic_dir=logic_dir, data_dir=data_dir)
    factor_cols = get_factor_columns(base)
    reps = compute_representatives(base, GROUP_COL)
    snapshot_df = attach_representatives_and_deviations(base, reps, GROUP_COL, factor_cols)

    snapshot_df["group_type"] = GROUP_TYPE
    snapshot_df["group_tag"] = snapshot_df[GROUP_COL]

    meta = ["as_of_date", "symbol", "group_type", "group_tag"]
    others = [c for c in snapshot_df.columns if c not in meta]
    return snapshot_df[meta + others].copy()


def main() -> None:
    base, as_of_date_str, tags_path = load_latest_tags_and_factors(
        logic_dir=DEFAULT_LOGIC_DIR,
        data_dir=DEFAULT_DATA_DIR,
    )
    print(f"Tags source: {tags_path}")
    print(f"Latest as_of_date: {as_of_date_str}, base rows: {len(base)}")

    snapshot_df = build_group_d_snapshot_df(logic_dir=DEFAULT_LOGIC_DIR, data_dir=DEFAULT_DATA_DIR)

    save_group_latest(snapshot_df, GROUP_TYPE, output_dir=DEFAULT_OUTPUT_DIR)
    print(f"Group {GROUP_TYPE} snapshot (latest only) saved, rows: {len(snapshot_df)}")


if __name__ == "__main__":
    main()
