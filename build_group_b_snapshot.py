# -*- coding: utf-8 -*-
"""
Build Group B snapshot: merge latest tags (group_b) + factors_latest,
reconstruct per-symbol size-score peer sets, compute representatives and deviations
via B-only utils (not compute_representatives(..., group_b)).

Returns B snapshot DataFrame; does not save history. Optional: save B latest-only file.
Independent runnable.

Output paths (when saving via save_group_latest):
  - output/group_b/group_b_snapshot_latest.parquet
  - output/group_b/group_b_snapshot_latest.csv
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd

from group_snapshot_history import DEFAULT_OUTPUT_DIR, normalize_as_of_date_key, save_group_latest
from group_snapshot_utils import (
    DEFAULT_DATA_DIR,
    DEFAULT_LOGIC_DIR,
    attach_representatives_and_deviations_b,
    build_peer_sets_and_reps_b,
    get_factor_columns_for_b,
    load_latest_tags_and_factors_for_b,
    log_snapshot_as_of_date_sanity,
)

GROUP_COL = "group_b"
GROUP_TYPE = "B"


def build_group_b_snapshot_df(
    logic_dir: str | Path | None = None,
    data_dir: str | Path | None = None,
) -> pd.DataFrame:
    """
    Build Group B snapshot DataFrame: latest tags + factors merge, per-symbol peer sets
    (resolve_group_b_peer_set_for_symbol via build_peer_sets_and_reps_b), rep/dev via
    attach_representatives_and_deviations_b.

    Does not use compute_representatives(base, GROUP_COL) — peers are symbol-keyed.

    Returns DataFrame with as_of_date preserved, group_type, group_tag, group_b, factor cols,
    rep cols, dev cols, and peer meta from merge when present.
    Returns empty DataFrame if there are no rows after load.

    The merged base retains all factors_latest columns; attach only appends rep__/dev__ columns.
    """
    from group_snapshot_utils import DEFAULT_DATA_DIR as _DD, DEFAULT_LOGIC_DIR as _LD

    logic_dir = Path(logic_dir) if logic_dir is not None else _LD
    data_dir = Path(data_dir) if data_dir is not None else _DD

    base, _as_of, _path = load_latest_tags_and_factors_for_b(logic_dir=logic_dir, data_dir=data_dir)
    if base.empty:
        return pd.DataFrame()

    factor_cols = get_factor_columns_for_b(base)
    reps_by_symbol, meta_df = build_peer_sets_and_reps_b(base, factor_cols)
    snapshot_df = attach_representatives_and_deviations_b(
        base, reps_by_symbol, meta_df, factor_cols, group_tag_col=GROUP_COL
    )
    if GROUP_COL not in snapshot_df.columns:
        raise ValueError("build_group_b_snapshot_df: missing column 'group_b' after attach (tags merge required).")
    snapshot_df = snapshot_df.assign(
        group_type=GROUP_TYPE,
        group_tag=snapshot_df[GROUP_COL],
    )
    snapshot_df = normalize_as_of_date_key(snapshot_df, "as_of_date")

    snapshot_df = snapshot_df.assign(
        sector=snapshot_df["sector"] if "sector" in snapshot_df.columns else "",
        industry=snapshot_df["industry"] if "industry" in snapshot_df.columns else "",
    )

    meta = ["as_of_date", "symbol", "group_type", "group_tag", GROUP_COL]
    meta_present = [c for c in meta if c in snapshot_df.columns]
    others = [c for c in snapshot_df.columns if c not in meta_present]
    log_snapshot_as_of_date_sanity(snapshot_df, label="group_b_snapshot")
    return snapshot_df[meta_present + others].copy()


def main(
    logic_dir: str | Path | None = None,
    data_dir: str | Path | None = None,
    output_dir: str | Path = DEFAULT_OUTPUT_DIR,
) -> None:
    """
    Build Group B snapshot and save latest-only parquet + csv under output/group_b/.
    """
    logic_path = Path(logic_dir) if logic_dir is not None else DEFAULT_LOGIC_DIR
    data_path = Path(data_dir) if data_dir is not None else DEFAULT_DATA_DIR

    base, as_of_date_str, tags_path = load_latest_tags_and_factors_for_b(
        logic_dir=logic_path,
        data_dir=data_path,
    )
    print(f"Tags source: {tags_path}")
    print(f"Latest as_of_date: {as_of_date_str}, base rows: {len(base)}")

    snapshot_df = build_group_b_snapshot_df(logic_dir=logic_path, data_dir=data_path)

    if snapshot_df.empty:
        print(f"Group {GROUP_TYPE} snapshot is empty; skip save.")
        return

    save_group_latest(snapshot_df, GROUP_TYPE, output_dir=output_dir)
    print(f"Group {GROUP_TYPE} snapshot (latest only) saved, rows: {len(snapshot_df)}")


if __name__ == "__main__":
    main()
