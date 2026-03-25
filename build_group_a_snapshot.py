# -*- coding: utf-8 -*-
"""
Build Group A snapshot: merge latest tags (with group_a, group_a_mode) + factors_latest,
compute mode-based peer sets and representatives, then deviations.
Returns A snapshot DataFrame; does not save history. Optional: save A latest-only file.
Independent runnable.

Output paths (when saving via save_group_latest):
  - output/group_a/group_a_snapshot_latest.parquet
  - output/group_a/group_a_snapshot_latest.csv
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd

from group_snapshot_history import DEFAULT_OUTPUT_DIR, save_group_latest
from group_snapshot_utils import (
    DEFAULT_DATA_DIR,
    DEFAULT_LOGIC_DIR,
    attach_representatives_and_deviations_a,
    build_peer_sets_and_reps_a,
    get_factor_columns_for_a,
    load_latest_tags_and_factors_for_a,
    print_group_a_add_mode_debug_summary,
    print_group_a_similarity_factor_debug,
)

GROUP_TAG_COL = "group_a"
GROUP_TYPE = "A"


def build_group_a_snapshot_df(
    logic_dir: str | Path | None = None,
    data_dir: str | Path | None = None,
) -> pd.DataFrame:
    """
    Build Group A snapshot DataFrame: latest tags + factors merge, mode-based peer sets
    (resolve_group_a_peer_sets), representative table (build_group_a_representative_table),
    then deviations. Uses INDUSTRY_ONLY / INDUSTRY_ADD_SECTOR / TOTAL_MARKET.
    Snapshot includes a_peer_mode, a_base_count, a_added_count, a_final_peer_count,
    a_peer_shortfall_flag, and INDUSTRY_ADD_SECTOR debug columns (a_add_*).
    Returns DataFrame with as_of_date, symbol, group_type, group_tag, group_a, group_a_mode,
    sector, industry, a_peer_mode, a_base_count, a_added_count, a_final_peer_count,
    a_peer_shortfall_flag, a_add_sector_value, a_add_need_count, a_add_candidate_count_*,
    a_add_selected_count, a_add_similarity_factors_used, a_add_debug_reason,
    factor cols, rep cols, dev cols.

    The merged base retains all factors_latest columns; attach only appends rep__/dev__ columns.
    """
    from group_snapshot_utils import DEFAULT_DATA_DIR as _DD, DEFAULT_LOGIC_DIR as _LD

    logic_dir = Path(logic_dir) if logic_dir is not None else _LD
    data_dir = Path(data_dir) if data_dir is not None else _DD

    base, _as_of, _path = load_latest_tags_and_factors_for_a(logic_dir=logic_dir, data_dir=data_dir)
    factor_cols = get_factor_columns_for_a(base)
    reps_by_ga, meta_df = build_peer_sets_and_reps_a(base, factor_cols)
    snapshot_df = attach_representatives_and_deviations_a(
        base, reps_by_ga, meta_df, factor_cols, group_tag_col=GROUP_TAG_COL
    )
    # Ensure a_final_peer_count is carried to final output/CSV
    # by reusing already computed values from meta_df (no new calculation).
    if "a_final_peer_count" not in snapshot_df.columns:
        if not meta_df.empty and {"symbol", "a_final_peer_count"}.issubset(meta_df.columns):
            s2c = (
                meta_df[["symbol", "a_final_peer_count"]]
                .drop_duplicates(subset=["symbol"], keep="last")
                .set_index("symbol")["a_final_peer_count"]
            )
            snapshot_df["a_final_peer_count"] = snapshot_df["symbol"].map(s2c)
        else:
            snapshot_df["a_final_peer_count"] = pd.NA

    snapshot_df["group_type"] = GROUP_TYPE
    snapshot_df["group_tag"] = snapshot_df[GROUP_TAG_COL]

    if "sector" not in snapshot_df.columns:
        snapshot_df["sector"] = ""
    if "industry" not in snapshot_df.columns:
        snapshot_df["industry"] = ""

    print_group_a_similarity_factor_debug(base, snapshot_df)

    meta = [
        "as_of_date",
        "symbol",
        "group_type",
        "group_tag",
        "group_a",
        "group_a_mode",
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
    ]
    meta_present = [c for c in meta if c in snapshot_df.columns]
    others = [c for c in snapshot_df.columns if c not in meta_present]
    return snapshot_df[meta_present + others].copy()


def main() -> None:
    base, as_of_date_str, tags_path = load_latest_tags_and_factors_for_a(
        logic_dir=DEFAULT_LOGIC_DIR,
        data_dir=DEFAULT_DATA_DIR,
    )
    print(f"Tags source: {tags_path}")
    print(f"Latest as_of_date: {as_of_date_str}, base rows: {len(base)}")

    snapshot_df = build_group_a_snapshot_df(logic_dir=DEFAULT_LOGIC_DIR, data_dir=DEFAULT_DATA_DIR)

    save_group_latest(snapshot_df, GROUP_TYPE, output_dir=DEFAULT_OUTPUT_DIR)
    print(f"Group {GROUP_TYPE} snapshot (latest only) saved, rows: {len(snapshot_df)}")
    print("")
    print_group_a_add_mode_debug_summary(snapshot_df)


if __name__ == "__main__":
    main()
