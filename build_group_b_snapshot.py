# -*- coding: utf-8 -*-
"""
Build Group B snapshot by comparing every symbol to the S&P500 benchmark universe.

Loads latest ``factors_latest`` only (no tag merge), attaches benchmark representatives
and deviations via ``build_group_b_sp500_benchmark_reps`` and
``attach_representatives_and_deviations_b_benchmark``.

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
    attach_representatives_and_deviations_b_benchmark,
    build_group_b_sp500_benchmark_reps,
    get_factor_columns_for_b,
    load_factors_latest,
    log_snapshot_as_of_date_sanity,
)

GROUP_COL = "group_b"
GROUP_TYPE = "B"
GROUP_TAG_BENCHMARK = "B_SP500_BENCHMARK"

_BENCHMARK_META_COLS = (
    "group_b_benchmark_index",
    "group_b_benchmark_tag",
    "group_b_benchmark_membership_as_of_date",
    "group_b_benchmark_member_count_total",
    "group_b_benchmark_member_count_intersection",
    "group_b_benchmark_method",
)


def _factors_latest_max_as_of_date_str(base: pd.DataFrame) -> str:
    if base.empty or "as_of_date" not in base.columns:
        return "N/A"
    s = pd.to_datetime(base["as_of_date"], errors="coerce")
    mx = s.max()
    if pd.isna(mx):
        return "N/A"
    return pd.Timestamp(mx).strftime("%Y-%m-%d")


def build_group_b_snapshot_df(
    logic_dir: str | Path | None = None,
    data_dir: str | Path | None = None,
) -> pd.DataFrame:
    """
    Build Group B snapshot: latest ``factors_latest`` (one row per symbol), S&P500 benchmark
    reps via ``build_group_b_sp500_benchmark_reps``, then ``attach_representatives_and_deviations_b_benchmark``.

    ``logic_dir`` is accepted for API compatibility and is not used.

    Returns DataFrame with ``as_of_date``, identifiers, ``group_b`` / ``group_type`` / ``group_tag``,
    benchmark meta, factor columns, and ``rep__`` / ``dev__`` columns. Empty if factors load is empty.
    """
    _ = logic_dir
    from group_snapshot_utils import DEFAULT_DATA_DIR as _DD

    data_dir = Path(data_dir) if data_dir is not None else _DD

    base = load_factors_latest(data_dir)
    if base.empty:
        return pd.DataFrame()

    base = base.copy()
    base[GROUP_COL] = GROUP_TAG_BENCHMARK

    factor_cols = get_factor_columns_for_b(base)
    try:
        reps_df, benchmark_meta = build_group_b_sp500_benchmark_reps(base, factor_cols, data_dir=data_dir)
    except FileNotFoundError as e:
        raise FileNotFoundError(
            "Group B benchmark: need data/index_membership.parquet or data/index_membership.csv "
            f"(under {data_dir}). {e}"
        ) from e
    except ValueError as e:
        err = str(e).lower()
        if "empty intersection" in err:
            raise RuntimeError(
                "Group B S&P500 benchmark: benchmark intersection members is 0 "
                "(no overlap between factors_latest symbols and S&P500 membership)."
            ) from e
        raise ValueError(
            "Group B benchmark: index_membership is missing required columns, has no S&P500 rows, "
            f"or is otherwise invalid. {e}"
        ) from e

    n_ix = int(benchmark_meta["group_b_benchmark_member_count_intersection"])
    if n_ix < 1:
        raise RuntimeError(
            "Group B S&P500 benchmark: benchmark intersection members is 0 "
            "(expected at least one symbol in factors_latest ∩ S&P500 membership)."
        )

    print(f"[group_b_snapshot] factors_latest max(as_of_date)={_factors_latest_max_as_of_date_str(base)}")
    print(f"[group_b_snapshot] base rows={len(base)}")
    print(
        f"[group_b_snapshot] benchmark membership as_of_date="
        f"{benchmark_meta['group_b_benchmark_membership_as_of_date']}"
    )
    print(f"[group_b_snapshot] benchmark total members={benchmark_meta['group_b_benchmark_member_count_total']}")
    print(f"[group_b_snapshot] benchmark intersection members={n_ix}")
    snapshot_df = attach_representatives_and_deviations_b_benchmark(
        base, reps_df, benchmark_meta, factor_cols, group_tag_col=GROUP_COL
    )
    if GROUP_COL not in snapshot_df.columns:
        raise ValueError("build_group_b_snapshot_df: missing column 'group_b' after benchmark attach.")

    snapshot_df = snapshot_df.assign(
        group_type=GROUP_TYPE,
        group_tag=GROUP_TAG_BENCHMARK,
    )
    snapshot_df = normalize_as_of_date_key(snapshot_df, "as_of_date")

    if "sector" not in snapshot_df.columns:
        snapshot_df["sector"] = ""
    else:
        snapshot_df["sector"] = snapshot_df["sector"].fillna("").astype(str)
    if "industry" not in snapshot_df.columns:
        snapshot_df["industry"] = ""
    else:
        snapshot_df["industry"] = snapshot_df["industry"].fillna("").astype(str)

    meta_core = ["as_of_date", "symbol", "group_type", "group_tag", GROUP_COL]
    meta_benchmark = [c for c in _BENCHMARK_META_COLS if c in snapshot_df.columns]
    meta_present = [c for c in meta_core if c in snapshot_df.columns] + meta_benchmark
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
    _ = logic_dir
    data_path = Path(data_dir) if data_dir is not None else DEFAULT_DATA_DIR

    snapshot_df = build_group_b_snapshot_df(logic_dir=logic_dir, data_dir=data_path)

    if snapshot_df.empty:
        print("[group_b_snapshot] empty factors_latest; skip save.")
        return

    save_group_latest(snapshot_df, GROUP_TYPE, output_dir=output_dir)
    print(f"[group_b_snapshot] saved rows={len(snapshot_df)}")


if __name__ == "__main__":
    main()
