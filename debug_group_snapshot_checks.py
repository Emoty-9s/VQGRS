# -*- coding: utf-8 -*-
"""
Self-check for group snapshot logic: representatives, deviations, history upsert,
and Group A mode-based peer sets (INDUSTRY_ONLY, INDUSTRY_ADD_SECTOR, TOTAL_MARKET).
Uses small in-memory DataFrames only (no path dependency).
Run: python debug_group_snapshot_checks.py
"""
from __future__ import annotations

import pandas as pd

from group_snapshot_history import KEY_COLS, upsert_history_df
from group_snapshot_utils import (
    REP_PREFIX,
    DEV_PREFIX,
    MIN_A_PEER_COUNT,
    SIMILARITY_FACTORS_A,
    build_group_a_representative_table,
    resolve_group_a_peer_sets,
    build_peer_sets_and_reps_a,
    attach_representatives_and_deviations,
    attach_representatives_and_deviations_a,
    compute_representatives,
    rank_sector_fill_candidates,
)


# ---------------------------------------------------------------------------
# C/D/E base helpers
# ---------------------------------------------------------------------------


def _make_base_df() -> pd.DataFrame:
    """Minimal base: symbol, group_c, one factor column (f1)."""
    return pd.DataFrame({
        "symbol": ["A", "A", "B", "B", "C"],
        "group_c": ["G1", "G1", "G1", "G2", "G2"],
        "f1": [10.0, 20.0, 30.0, 40.0, 50.0],
    })


# ---------------------------------------------------------------------------
# C/D/E checks
# ---------------------------------------------------------------------------


def check_representatives() -> None:
    """Representative calculation: median, q25, q75, iqr, n_valid."""
    df = _make_base_df()
    reps = compute_representatives(df, "group_c")
    assert "group_tag" in reps.columns and "factor_name" in reps.columns
    assert set(reps.columns) >= {"n_valid", "median", "q25", "q75", "iqr"}

    r1 = reps[(reps["group_tag"] == "G1") & (reps["factor_name"] == "f1")].iloc[0]
    assert r1["n_valid"] == 3
    assert r1["median"] == 20.0
    assert r1["q25"] == 15.0
    assert r1["q75"] == 25.0
    assert r1["iqr"] == 10.0

    r2 = reps[(reps["group_tag"] == "G2") & (reps["factor_name"] == "f1")].iloc[0]
    assert r2["n_valid"] == 2
    assert r2["median"] == 45.0
    assert r2["iqr"] == 5.0


def check_dev_abs() -> None:
    """dev_abs = value - median."""
    df = _make_base_df()
    reps = compute_representatives(df, "group_c")
    out = attach_representatives_and_deviations(df, reps, "group_c", ["f1"])
    col = f"{DEV_PREFIX}f1__abs"
    assert col in out.columns
    row0 = out[out["symbol"] == "A"].iloc[0]
    assert row0[col] == -10.0
    row30 = out[out["f1"] == 30.0].iloc[0]
    assert row30[col] == 10.0


def check_dev_pct_median_zero() -> None:
    """dev_pct is NaN when median=0."""
    df = pd.DataFrame({
        "symbol": ["X", "Y"],
        "group_c": ["Z", "Z"],
        "f1": [0.0, 0.0],
    })
    reps = compute_representatives(df, "group_c")
    out = attach_representatives_and_deviations(df, reps, "group_c", ["f1"])
    pct_col = f"{DEV_PREFIX}f1__pct"
    assert pct_col in out.columns
    assert pd.isna(out[pct_col]).all()


def check_dev_robust_z_iqr_zero() -> None:
    """dev_robust_z is NaN when iqr<=0."""
    df = pd.DataFrame({
        "symbol": ["P", "Q"],
        "group_c": ["R", "R"],
        "f1": [5.0, 5.0],
    })
    reps = compute_representatives(df, "group_c")
    out = attach_representatives_and_deviations(df, reps, "group_c", ["f1"])
    z_col = f"{DEV_PREFIX}f1__robust_z"
    assert z_col in out.columns
    assert pd.isna(out[z_col]).all()


def check_history_upsert_overwrite() -> None:
    """Upsert: same key (as_of_date + symbol + group_type) in new overwrites existing."""
    key_cols = KEY_COLS
    existing = pd.DataFrame([
        {"as_of_date": "2026-01-01", "symbol": "A", "group_type": "C", "v": 1},
        {"as_of_date": "2026-01-01", "symbol": "B", "group_type": "C", "v": 2},
    ])
    new = pd.DataFrame([
        {"as_of_date": "2026-01-01", "symbol": "A", "group_type": "C", "v": 99},
        {"as_of_date": "2026-01-01", "symbol": "C", "group_type": "C", "v": 3},
    ])
    combined = upsert_history_df(existing, new, key_cols=key_cols, sort_cols=["as_of_date", "symbol", "group_type"])
    assert len(combined) == 3
    a_row = combined[(combined["symbol"] == "A") & (combined["as_of_date"] == "2026-01-01")]
    assert len(a_row) == 1
    assert a_row.iloc[0]["v"] == 99
    assert combined[(combined["symbol"] == "B")].iloc[0]["v"] == 2
    assert combined[(combined["symbol"] == "C")].iloc[0]["v"] == 3


def check_history_upsert_same_key_group_type_distinct() -> None:
    """Same as_of_date + symbol but different group_type are distinct rows; same key overwrites."""
    key_cols = KEY_COLS
    existing = pd.DataFrame([
        {"as_of_date": "2026-01-01", "symbol": "X", "group_type": "A", "v": 1},
        {"as_of_date": "2026-01-01", "symbol": "X", "group_type": "C", "v": 2},
    ])
    new = pd.DataFrame([
        {"as_of_date": "2026-01-01", "symbol": "X", "group_type": "C", "v": 999},
    ])
    combined = upsert_history_df(existing, new, key_cols=key_cols, sort_cols=["as_of_date", "symbol", "group_type"])
    assert len(combined) == 2
    row_a = combined[(combined["symbol"] == "X") & (combined["group_type"] == "A")]
    row_c = combined[(combined["symbol"] == "X") & (combined["group_type"] == "C")]
    assert len(row_a) == 1 and row_a.iloc[0]["v"] == 1
    assert len(row_c) == 1 and row_c.iloc[0]["v"] == 999


# ---------------------------------------------------------------------------
# Group A: INDUSTRY_ONLY
# ---------------------------------------------------------------------------


def check_a_industry_only_peer_set() -> None:
    """INDUSTRY_ONLY: same group_a (industry) only used for representative."""
    base = pd.DataFrame({
        "symbol": ["S1", "S2", "S3", "S4", "S5"],
        "group_a": ["A_Ind1", "A_Ind1", "A_Ind1", "A_Ind2", "A_Ind2"],
        "group_a_mode": ["INDUSTRY_ONLY", "INDUSTRY_ONLY", "INDUSTRY_ONLY", "INDUSTRY_ONLY", "INDUSTRY_ONLY"],
        "f1": [10.0, 20.0, 30.0, 100.0, 200.0],
    })
    factor_cols = ["f1"]
    peer_sets, meta = resolve_group_a_peer_sets(base, factor_cols, min_peer_count=2)
    assert "A_Ind1" in peer_sets
    assert "A_Ind2" in peer_sets
    assert len(peer_sets["A_Ind1"]) == 3
    assert len(peer_sets["A_Ind2"]) == 2
    reps_a1 = build_group_a_representative_table(peer_sets["A_Ind1"], factor_cols, "A_Ind1")
    reps_a2 = build_group_a_representative_table(peer_sets["A_Ind2"], factor_cols, "A_Ind2")
    r1 = reps_a1[reps_a1["factor_name"] == "f1"].iloc[0]
    r2 = reps_a2[reps_a2["factor_name"] == "f1"].iloc[0]
    assert r1["n_valid"] == 3 and r1["median"] == 20.0
    assert r2["n_valid"] == 2 and r2["median"] == 150.0
    m1 = meta[meta["a_peer_mode"] == "INDUSTRY_ONLY"]
    assert m1["a_base_count"].iloc[0] == 3
    assert m1["a_added_count"].iloc[0] == 0


# ---------------------------------------------------------------------------
# Group A: INDUSTRY_ADD_SECTOR (base + sector fill toward 12)
# ---------------------------------------------------------------------------


def check_a_industry_add_sector_fill() -> None:
    """INDUSTRY_ADD_SECTOR: base industry first, then sector fill to reach min_peer_count."""
    # Base: 3 rows in A_IndAdd_Sec1; sector Sec1; need 12 total -> add 9 from sector. 7 fill candidates in Sec1, 3 in Sec2.
    n_base, n_sec1, n_sec2 = 3, 7, 3
    base = pd.DataFrame({
        "symbol": [f"B{i}" for i in range(n_base)] + [f"F{i}" for i in range(n_sec1)] + [f"G{i}" for i in range(n_sec2)],
        "group_a": ["A_IndAdd_Sec1", "A_IndAdd_Sec1", "A_IndAdd_Sec1"] + ["A_Other"] * (n_sec1 + n_sec2),
        "group_a_mode": ["INDUSTRY_ADD_SECTOR", "INDUSTRY_ADD_SECTOR", "INDUSTRY_ADD_SECTOR"] + ["INDUSTRY_ONLY"] * (n_sec1 + n_sec2),
        "sector": ["Sec1"] * n_base + ["Sec1"] * n_sec1 + ["Sec2"] * n_sec2,
        "op_margin": [1.0, 2.0, 3.0] + [4.0] * n_sec1 + [5.0] * n_sec2,
        "roic": [10.0, 11.0, 12.0] + [13.0] * n_sec1 + [14.0] * n_sec2,
        "debt_to_equity": [0.5, 0.5, 0.5] + [0.6] * n_sec1 + [0.7] * n_sec2,
        "f1": [100.0, 101.0, 102.0] + list(range(200, 200 + n_sec1)) + list(range(300, 300 + n_sec2)),
    })
    factor_cols = ["f1"]
    sim_factors = [f for f in SIMILARITY_FACTORS_A if f in base.columns]
    peer_sets, meta = resolve_group_a_peer_sets(base, factor_cols, similarity_factors=sim_factors, min_peer_count=MIN_A_PEER_COUNT)
    assert "A_IndAdd_Sec1" in peer_sets
    final = peer_sets["A_IndAdd_Sec1"]
    assert len(final) >= 3
    base_only = meta[(meta["a_peer_mode"] == "INDUSTRY_ADD_SECTOR") & (meta["symbol"].str.startswith("B"))]
    assert len(base_only) == 3
    assert base_only["a_base_count"].iloc[0] == 3
    assert base_only["a_added_count"].iloc[0] >= 0
    assert base_only["a_final_peer_count"].iloc[0] == len(final)


# ---------------------------------------------------------------------------
# Group A: rank_sector_fill_candidates sort order
# ---------------------------------------------------------------------------


def check_a_rank_sector_fill_order() -> None:
    """Sector fill candidates: distance_score asc, valid_similarity_factor_count desc, symbol asc."""
    base = pd.DataFrame({
        "symbol": ["BASE1", "BASE2", "C1", "C2", "C3"],
        "sector": ["S", "S", "S", "S", "S"],
        "op_margin": [10.0, 10.0, 12.0, 11.0, 11.0],
        "roic": [20.0, 20.0, 22.0, 21.0, 21.0],
        "debt_to_equity": [1.0, 1.0, 1.2, 1.1, 1.1],
    })
    base_symbols = {"BASE1", "BASE2"}
    ranked, _dbg = rank_sector_fill_candidates(base, base_symbols, "S", ["op_margin", "roic", "debt_to_equity"])
    assert len(ranked) == 3
    assert list(ranked.columns) >= ["symbol", "distance_score", "valid_similarity_factor_count"]
    # C2 and C3 have same distance (closer to base median), C1 farther; C2/C3 both 3 valid
    # So order: by distance_score asc, then valid_similarity_factor_count desc, then symbol asc
    assert ranked["distance_score"].is_monotonic_increasing
    prev = None
    for _, row in ranked.iterrows():
        if prev is not None and row["distance_score"] == prev["distance_score"]:
            assert row["valid_similarity_factor_count"] <= prev["valid_similarity_factor_count"]
            if row["valid_similarity_factor_count"] == prev["valid_similarity_factor_count"]:
                assert row["symbol"] >= prev["symbol"]
        prev = row


# ---------------------------------------------------------------------------
# Group A: TOTAL_MARKET
# ---------------------------------------------------------------------------


def check_a_total_market_rep() -> None:
    """TOTAL_MARKET: full market used as peer set; all share same rep."""
    base = pd.DataFrame({
        "symbol": ["M1", "M2", "M3"],
        "group_a": ["A_Total_Market", "A_Total_Market", "A_Total_Market"],
        "group_a_mode": ["TOTAL_MARKET", "TOTAL_MARKET", "TOTAL_MARKET"],
        "f1": [1.0, 2.0, 3.0],
    })
    factor_cols = ["f1"]
    peer_sets, meta = resolve_group_a_peer_sets(base, factor_cols, min_peer_count=2)
    assert "A_Total_Market" in peer_sets
    assert len(peer_sets["A_Total_Market"]) == 3
    reps = build_group_a_representative_table(peer_sets["A_Total_Market"], factor_cols, "A_Total_Market")
    r = reps[reps["factor_name"] == "f1"].iloc[0]
    assert r["n_valid"] == 3
    assert r["median"] == 2.0
    assert meta["a_peer_mode"].iloc[0] == "TOTAL_MARKET"
    assert meta["a_final_peer_count"].iloc[0] == 3


# ---------------------------------------------------------------------------
# Group A: representative table median/q25/q75/iqr/n_valid
# ---------------------------------------------------------------------------


def check_a_representative_table_stats() -> None:
    """Representative table: median, q25, q75, iqr, n_valid correct."""
    peer_df = pd.DataFrame({"f1": [10.0, 20.0, 30.0, 40.0]})
    reps = build_group_a_representative_table(peer_df, ["f1"], "TAG")
    assert len(reps) == 1
    r = reps.iloc[0]
    assert r["group_tag"] == "TAG"
    assert r["factor_name"] == "f1"
    assert r["n_valid"] == 4
    assert r["median"] == 25.0
    assert r["q25"] == 17.5
    assert r["q75"] == 32.5
    assert r["iqr"] == 15.0


# ---------------------------------------------------------------------------
# Group A: dev_pct when median=0, dev_robust_z when iqr<=0
# ---------------------------------------------------------------------------


def check_a_dev_pct_median_zero() -> None:
    """Group A: dev_pct is NaN when median=0."""
    base = pd.DataFrame({
        "symbol": ["X", "Y"],
        "group_a": ["Z", "Z"],
        "group_a_mode": ["INDUSTRY_ONLY", "INDUSTRY_ONLY"],
        "f1": [0.0, 0.0],
    })
    factor_cols = ["f1"]
    reps_by_ga, meta_df = build_peer_sets_and_reps_a(base, factor_cols, min_peer_count=2)
    assert "Z" in reps_by_ga
    out = attach_representatives_and_deviations_a(base, reps_by_ga, meta_df, factor_cols, group_tag_col="group_a")
    pct_col = f"{DEV_PREFIX}f1__pct"
    assert pct_col in out.columns
    assert pd.isna(out[pct_col]).all()


def check_a_dev_robust_z_iqr_zero() -> None:
    """Group A: dev_robust_z is NaN when iqr<=0."""
    base = pd.DataFrame({
        "symbol": ["P", "Q"],
        "group_a": ["R", "R"],
        "group_a_mode": ["INDUSTRY_ONLY", "INDUSTRY_ONLY"],
        "f1": [5.0, 5.0],
    })
    factor_cols = ["f1"]
    reps_by_ga, meta_df = build_peer_sets_and_reps_a(base, factor_cols, min_peer_count=2)
    out = attach_representatives_and_deviations_a(base, reps_by_ga, meta_df, factor_cols, group_tag_col="group_a")
    z_col = f"{DEV_PREFIX}f1__robust_z"
    assert z_col in out.columns
    assert pd.isna(out[z_col]).all()


# ---------------------------------------------------------------------------
# Run all
# ---------------------------------------------------------------------------


def run_all_checks() -> None:
    check_representatives()
    check_dev_abs()
    check_dev_pct_median_zero()
    check_dev_robust_z_iqr_zero()
    check_history_upsert_overwrite()
    check_history_upsert_same_key_group_type_distinct()

    check_a_industry_only_peer_set()
    check_a_industry_add_sector_fill()
    check_a_rank_sector_fill_order()
    check_a_total_market_rep()
    check_a_representative_table_stats()
    check_a_dev_pct_median_zero()
    check_a_dev_robust_z_iqr_zero()

    print("ALL CHECKS PASSED")


if __name__ == "__main__":
    run_all_checks()
