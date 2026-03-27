# -*- coding: utf-8 -*-
"""
Group snapshot utilities: factor column identification, representative values,
and deviation computation. Used by build_group_*_snapshot.py scripts.
"""
from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from tag_and_save_groups import (
    GROUP_B_BASE_TOLERANCE,
    GROUP_B_RELAXED_TARGET_MIN_PEERS,
    compute_group_b_features,
    build_group_b_no_market_cap_peer_pool,
)

# ---------------------------------------------------------------------------
# as_of_date: content-based latest (not filename, not row order)
# ---------------------------------------------------------------------------


def normalize_as_of_date(df: pd.DataFrame, col: str = "as_of_date") -> pd.DataFrame:
    """
    Normalize `col` to datetime64 for comparisons. Idempotent for already-normalized columns.
    If `col` is missing, logs a warning and returns a copy unchanged.
    """
    out = df.copy()
    if col not in out.columns:
        print(f"WARNING [normalize_as_of_date]: column {col!r} missing; leaving DataFrame unchanged.")
        return out
    out[col] = pd.to_datetime(out[col], errors="coerce")
    return out


def filter_to_latest_as_of_date(
    df: pd.DataFrame,
    col: str = "as_of_date",
    *,
    label: str = "",
) -> pd.DataFrame:
    """
    Keep only rows where `col` equals the maximum non-null date in the frame.
    Does not use row order. Idempotent if already a single date.
    If `col` is missing or all values are NaT, logs a warning and returns a copy unchanged.
    """
    out = normalize_as_of_date(df, col=col)
    if col not in out.columns:
        return out
    valid = out[col].notna()
    if not valid.any():
        print(
            f"WARNING [filter_to_latest_as_of_date{(':' + label) if label else ''}]: "
            f"no valid {col}; leaving rows unchanged."
        )
        return out
    n_before = len(out)
    uniq = pd.unique(out.loc[valid, col])
    max_dt = out.loc[valid, col].max()
    if len(uniq) > 1:
        print(
            f"WARNING [filter_to_latest_as_of_date{(':' + label) if label else ''}]: "
            f"{col} has {len(uniq)} distinct values; keeping rows with max={max_dt} only "
            f"(rows {n_before} -> filter)."
        )
    out = out.loc[valid & (out[col] == max_dt)].copy()
    n_after = len(out)
    print(
        f"  [filter_to_latest_as_of_date{(':' + label) if label else ''}] "
        f"rows {n_before} -> {n_after} | max({col})={max_dt} | n_unique_before={len(uniq)}"
    )
    return out


def dedupe_by_symbol_keep_latest(
    df: pd.DataFrame,
    symbol_col: str = "symbol",
    date_col: str = "as_of_date",
) -> pd.DataFrame:
    """
    One row per symbol: prefer the latest `date_col`; ties on same date keep last row in original order.
    If `date_col` is missing, falls back to drop_duplicates(symbol, keep='last') with a warning.
    Idempotent when already unique per symbol (for a single date).
    """
    if df.empty:
        return df.copy()
    if symbol_col not in df.columns:
        raise ValueError(f"dedupe_by_symbol_keep_latest: missing column {symbol_col!r}")
    work = df.copy()
    if date_col not in work.columns:
        print(
            "WARNING [dedupe_by_symbol_keep_latest]: "
            f"{date_col!r} missing; using drop_duplicates({{symbol_col}}, keep='last') only."
        )
        n0 = len(work)
        out = work.drop_duplicates(subset=[symbol_col], keep="last").reset_index(drop=True)
        print(f"  [dedupe_by_symbol_keep_latest] rows {n0} -> {len(out)} (no date column)")
        return out
    work = normalize_as_of_date(work, col=date_col)
    work["_orig_idx"] = np.arange(len(work), dtype=np.int64)
    work = work.sort_values(
        [symbol_col, date_col, "_orig_idx"],
        ascending=[True, True, True],
        na_position="last",
    )
    n_before = len(work)
    sym_dup = work.duplicated(subset=[symbol_col], keep=False).sum()
    out = work.drop_duplicates(subset=[symbol_col], keep="last").drop(columns=["_orig_idx"]).reset_index(drop=True)
    print(
        f"  [dedupe_by_symbol_keep_latest] rows {n_before} -> {len(out)} | "
        f"rows_in_duplicate_symbol_groups={int(sym_dup)}"
    )
    return out


def finalize_factors_latest_by_as_of_date(factors: pd.DataFrame, *, label: str = "factors_latest") -> pd.DataFrame:
    """
    After loading factors_latest: normalize date, filter to global max(as_of_date), dedupe by symbol.
    If as_of_date is absent, logs a warning and keeps legacy drop_duplicates(symbol, keep='last').
    """
    if factors.empty:
        return factors.copy()
    if "as_of_date" not in factors.columns:
        print(
            f"WARNING [finalize_factors_latest_by_as_of_date:{label}]: "
            "as_of_date column missing; cannot filter by max date; "
            "using drop_duplicates(symbol, keep='last') (row-order legacy)."
        )
        out = factors.copy()
        n0 = len(out)
        out = out.drop_duplicates(subset=["symbol"], keep="last").reset_index(drop=True)
        print(f"  [{label}] legacy dedupe rows {n0} -> {len(out)}")
        return out
    out = normalize_as_of_date(factors, col="as_of_date")
    out = filter_to_latest_as_of_date(out, col="as_of_date", label=label)
    out = dedupe_by_symbol_keep_latest(out, symbol_col="symbol", date_col="as_of_date")
    return out


def ensure_merged_snapshot_as_of_date(base: pd.DataFrame, *, label: str) -> pd.DataFrame:
    """
    After tags+factors inner join: ensure a single data-date `as_of_date` (max in frame),
    one row per symbol. Does not use row order for recency.
    If only `as_of_date_tag` exists (left-only name), coalesce from it.
    """
    out = base.copy()
    if "as_of_date" not in out.columns and "as_of_date_tag" in out.columns:
        out["as_of_date"] = out["as_of_date_tag"]
        print(f"  [{label}] coalesced as_of_date from as_of_date_tag")
    if "as_of_date" not in out.columns:
        print(f"WARNING [{label}]: merged base has no as_of_date; cannot enforce latest-date filter.")
        return out
    out = normalize_as_of_date(out)
    out = filter_to_latest_as_of_date(out, label=label)
    out = dedupe_by_symbol_keep_latest(out, symbol_col="symbol", date_col="as_of_date")
    return out


def finalize_snapshot_for_scoring(df: pd.DataFrame, *, label: str) -> pd.DataFrame:
    """
    Snapshot input for build_group_factor_scores: may have multiple rows per symbol (e.g. A/B/C/D/E).
    Enforce max(as_of_date) only; do not collapse rows that differ by group_type.
    Drops duplicate rows for the same (symbol, group_type, as_of_date) if present.
    """
    if df.empty:
        return df.copy()
    if "as_of_date" not in df.columns:
        print(f"WARNING [finalize_snapshot_for_scoring:{label}]: missing as_of_date; leaving unchanged.")
        return df.copy()
    n0 = len(df)
    work = normalize_as_of_date(df)
    valid = work["as_of_date"].notna()
    if not valid.any():
        print(f"WARNING [finalize_snapshot_for_scoring:{label}]: all as_of_date NaT; leaving unchanged.")
        return work
    nu = int(work.loc[valid, "as_of_date"].nunique())
    mx = work.loc[valid, "as_of_date"].max()
    print(
        f"  [finalize_snapshot_for_scoring:{label}] rows={n0} unique_as_of_date={nu} max_as_of_date={mx}"
    )
    if nu > 1:
        print(
            f"WARNING [finalize_snapshot_for_scoring:{label}]: "
            "snapshot *_latest contains multiple as_of_date values; filtering to max."
        )
    work = filter_to_latest_as_of_date(work, label=label)
    if "group_type" in work.columns:
        subset = [c for c in ("symbol", "group_type", "as_of_date") if c in work.columns]
        dup = work.duplicated(subset=subset, keep=False)
        if dup.any():
            n1 = len(work)
            work = work.drop_duplicates(subset=subset, keep="last").reset_index(drop=True)
            print(
                f"  [finalize_snapshot_for_scoring:{label}] removed duplicate snapshot rows "
                f"{n1} -> {len(work)} on {subset}"
            )
    return work


def finalize_scoring_long_input_df(df: pd.DataFrame, *, label: str) -> pd.DataFrame:
    """
    Long-format scoring inputs (e.g. symbol_factor_scores): many rows per symbol.
    Enforce max(as_of_date) only; do not dedupe by symbol.
    """
    if df.empty:
        return df.copy()
    if "as_of_date" not in df.columns:
        print(f"WARNING [finalize_scoring_long_input_df:{label}]: missing as_of_date; leaving unchanged.")
        return df.copy()
    n0 = len(df)
    work = normalize_as_of_date(df)
    valid = work["as_of_date"].notna()
    if not valid.any():
        print(f"WARNING [finalize_scoring_long_input_df:{label}]: all as_of_date NaT; leaving unchanged.")
        return work
    nu = int(work.loc[valid, "as_of_date"].nunique())
    mx = work.loc[valid, "as_of_date"].max()
    print(
        f"  [finalize_scoring_long_input_df:{label}] rows={n0} unique_as_of_date={nu} max_as_of_date={mx}"
    )
    if nu > 1:
        print(
            f"WARNING [finalize_scoring_long_input_df:{label}]: "
            "multiple as_of_date values in input; filtering to max."
        )
    return filter_to_latest_as_of_date(work, label=label)


def log_snapshot_as_of_date_sanity(df: pd.DataFrame, *, label: str) -> None:
    """
    Log whether snapshot output has a single data-date; warn if multiple as_of_date values appear.
    """
    if df.empty:
        return
    if "as_of_date" not in df.columns:
        print(f"WARNING [log_snapshot_as_of_date_sanity:{label}]: snapshot has no as_of_date column.")
        return
    s = pd.to_datetime(df["as_of_date"], errors="coerce").dropna()
    if s.empty:
        print(f"WARNING [log_snapshot_as_of_date_sanity:{label}]: snapshot as_of_date all NaT.")
        return
    u = pd.unique(s.values)
    mx = s.max()
    if len(u) > 1:
        print(
            f"WARNING [log_snapshot_as_of_date_sanity:{label}]: "
            f"{len(u)} distinct as_of_date values (max={mx}); expected single date from loaders."
        )
    else:
        print(f"  [snapshot as_of_date] {label}: single data-date max={mx}")


def finalize_scoring_wide_input_df(df: pd.DataFrame, *, label: str) -> pd.DataFrame:
    """
    Wide per-symbol scoring inputs (e.g. symbol_category_scores): one row per symbol for a given date.
    Enforce max(as_of_date), then at most one row per symbol for that date.
    """
    if df.empty:
        return df.copy()
    if "as_of_date" not in df.columns:
        print(f"WARNING [finalize_scoring_wide_input_df:{label}]: missing as_of_date; leaving unchanged.")
        return df.copy()
    n0 = len(df)
    work = normalize_as_of_date(df)
    valid = work["as_of_date"].notna()
    if not valid.any():
        print(f"WARNING [finalize_scoring_wide_input_df:{label}]: all as_of_date NaT; leaving unchanged.")
        return work
    nu = int(work.loc[valid, "as_of_date"].nunique())
    mx = work.loc[valid, "as_of_date"].max()
    print(
        f"  [finalize_scoring_wide_input_df:{label}] rows={n0} unique_as_of_date={nu} max_as_of_date={mx}"
    )
    if nu > 1:
        print(
            f"WARNING [finalize_scoring_wide_input_df:{label}]: "
            "multiple as_of_date values in input; filtering to max."
        )
    work = filter_to_latest_as_of_date(work, label=label)
    return dedupe_by_symbol_keep_latest(work, symbol_col="symbol", date_col="as_of_date")


# ---------------------------------------------------------------------------
# Path / config constants
# ---------------------------------------------------------------------------
DEFAULT_LOGIC_DIR = Path("logic_data")
DEFAULT_DATA_DIR = Path("data")
TAGS_HISTORY_FILENAME = "group_tags_history.parquet"
TAGS_DATE_PATTERN = re.compile(r"group_tags_(\d{8})\.(parquet|csv)$", re.IGNORECASE)

# Columns to exclude from factor list (numeric-only selection).
EXCLUDE_COLUMNS = {
    "symbol",
    "as_of_date",
    "group_c",
    "group_d",
    "group_e",
    "group_type",
    "group_tag",
    "IPO (Date)",
    "Earnings (Date)",
    "Dividend Ex-Date",
}

# Group A: exclude meta/tag/identifier columns from factor list (same numeric factors as C/D/E).
EXCLUDE_COLUMNS_A = EXCLUDE_COLUMNS | {
    "sector",
    "industry",
    "group_a",
    "group_a_mode",
    "group_a_industry",
    "group_a_sector",
    "group_a_industry_count",
    "group_a_sector_count",
}

# Group B: exclude Group B tag/meta + peer-set reconstruction inputs from factor stats.
# (peer selection reconstruction logic itself is implemented in build_group_b_snapshot.py)
EXCLUDE_COLUMNS_B = EXCLUDE_COLUMNS | {
    "group_b",
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
    # Derived inputs used during B peer reconstruction; keep them out of rep__/dev__ factor stats.
    "market_cap",
    "revenue_ttm",
    "total_assets",
    "sector",
    "industry",
}

# Group A sector-add: similarity factors and minimum peer count.
SIMILARITY_FACTORS_A = ["op_margin", "roic", "debt_to_equity"]
MIN_A_PEER_COUNT = 12

# Cross-group factor canonical naming:
# (source display name -> canonical factor name)
# Applied to merged base for all groups so rep__/dev__ naming stays consistent.
FACTOR_CANONICAL_ALIASES: tuple[tuple[str, str], ...] = (
    ("Oper. Margin", "op_margin"),
    ("ROIC", "roic"),
    ("Debt/Eq", "debt_to_equity"),
)


def apply_factor_canonical_aliases(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure canonical factor columns exist from source display names (cross-group).
    Rules:
      - if canonical column already exists, keep it
      - else if source exists, copy source -> canonical
      - source columns are preserved (no drop)

    Idempotent: safe to call multiple times.
    """
    out = df.copy()
    for src, dst in FACTOR_CANONICAL_ALIASES:
        if dst in out.columns:
            continue
        if src not in out.columns:
            continue
        out[dst] = out[src]
    return out


def _apply_group_a_similarity_factor_aliases(df: pd.DataFrame) -> pd.DataFrame:
    """
    Backward-compat wrapper.
    Canonicalization is now cross-group via apply_factor_canonical_aliases().
    """
    return apply_factor_canonical_aliases(df)


def _dedupe_alias_factor_columns(cols: list[str], df: pd.DataFrame) -> list[str]:
    """
    If source+canonical alias pair both exist, keep canonical only in factor columns.
    Example: keep 'roic', drop 'ROIC' from factor_cols when both exist.
    """
    if not cols:
        return cols
    keep = set(cols)
    for src, dst in FACTOR_CANONICAL_ALIASES:
        if src in keep and dst in keep and dst in df.columns:
            keep.discard(src)
    return sorted(keep)

REP_PREFIX = "rep__"
DEV_PREFIX = "dev__"


def _resolve_latest_tags_path(logic_dir: Path) -> Path:
    """
    Resolve path to latest group tags data.
    1) If group_tags_history.parquet exists, use it (latest as_of_date chosen when loading).
    2) Else search logic_dir for group_tags_YYYYMMDD.parquet or .csv; pick latest by date in filename.
    """
    history_path = logic_dir / TAGS_HISTORY_FILENAME
    if history_path.exists():
        return history_path

    candidates: list[tuple[Path, str]] = []
    for p in logic_dir.iterdir():
        if not p.is_file():
            continue
        m = TAGS_DATE_PATTERN.search(p.name)
        if m:
            candidates.append((p, m.group(1)))

    if not candidates:
        raise FileNotFoundError(
            f"No group tags file found. Expected {history_path} or "
            f"{logic_dir}/group_tags_YYYYMMDD.parquet (or .csv)."
        )

    candidates.sort(key=lambda x: x[1], reverse=True)
    return candidates[0][0]


def _load_tags_from_path(path: Path) -> tuple[pd.DataFrame, str]:
    """Load tags from path; return (latest_only_df, as_of_date_str). If history, take max(as_of_date)."""
    need = ["symbol", "as_of_date", "group_c", "group_d", "group_e"]
    if path.suffix.lower() == ".csv":
        tags = pd.read_csv(path, low_memory=False)
    else:
        tags = pd.read_parquet(path)

    missing = [c for c in need if c not in tags.columns]
    if missing:
        raise ValueError(f"Tags file missing required columns: {missing}")

    tags = tags[need].copy()
    tags = normalize_as_of_date(tags)
    tags = tags.dropna(subset=["as_of_date"])
    if tags.empty:
        raise ValueError("No valid as_of_date in tags file.")

    tags = filter_to_latest_as_of_date(tags, label="_load_tags_from_path")
    tags = dedupe_by_symbol_keep_latest(tags)
    tags["symbol"] = _normalize_symbol_series(tags["symbol"])
    as_of_date_str = pd.Timestamp(tags["as_of_date"].iloc[0]).strftime("%Y-%m-%d")
    return tags, as_of_date_str


CANONICAL_GROUP_A_MODES = frozenset({"TOTAL_MARKET", "INDUSTRY_ONLY", "INDUSTRY_ADD_SECTOR"})


def _infer_group_a_mode(group_a: str) -> str:
    """Infer group_a_mode from group_a tag when group_a_mode column is missing."""
    if pd.isna(group_a) or str(group_a).strip() == "":
        return "TOTAL_MARKET"
    s = str(group_a).strip().upper()
    if s == "A_TOTAL_MARKET":
        return "TOTAL_MARKET"
    if "_ADD_" in s:
        return "INDUSTRY_ADD_SECTOR"
    return "INDUSTRY_ONLY"


def _normalize_or_infer_group_a_mode(raw_mode: object, group_a: object) -> str:
    """
    Canonical group_a_mode: TOTAL_MARKET | INDUSTRY_ONLY | INDUSTRY_ADD_SECTOR.
    Empty/invalid raw_mode -> infer from group_a; fuzzy strings -> map then infer if needed.
    """
    if raw_mode is None or (isinstance(raw_mode, float) and pd.isna(raw_mode)):
        return _infer_group_a_mode(group_a)
    s = str(raw_mode).strip()
    if s == "" or s.lower() in ("nan", "none", "null"):
        return _infer_group_a_mode(group_a)

    s_up = s.upper().strip()
    if s_up in CANONICAL_GROUP_A_MODES:
        return s_up

    compact = re.sub(r"[\s_]+", "", s_up)
    if compact in ("ATOTALMARKET", "TOTALMARKET"):
        return "TOTAL_MARKET"
    if compact in ("INDUSTRYONLY",):
        return "INDUSTRY_ONLY"
    if compact in ("INDUSTRYADDSECTOR", "INDUSTRYADDSECTOR"):
        return "INDUSTRY_ADD_SECTOR"
    if s_up.startswith("TOTAL") and "MARKET" in s_up:
        return "TOTAL_MARKET"
    if s_up.startswith("INDUSTRY") and "ONLY" in s_up and "ADD" not in s_up:
        return "INDUSTRY_ONLY"
    if s_up.startswith("INDUSTRY") and ("ADD" in s_up or "SECTOR" in s_up):
        return "INDUSTRY_ADD_SECTOR"
    if "ADD" in s_up:
        return "INDUSTRY_ADD_SECTOR"

    return _infer_group_a_mode(group_a)


def _normalize_group_a_sector_value(x: object) -> str:
    """Upper/strip sector for matching; NaN-like -> empty string."""
    if x is None:
        return ""
    try:
        if pd.isna(x):
            return ""
    except TypeError:
        pass
    if isinstance(x, float) and np.isnan(x):
        return ""
    s = str(x).strip()
    if s == "" or s.lower() in ("nan", "none", "null", "<na>"):
        return ""
    return s.upper()


def _infer_group_a_sector_from_tag(group_a: object) -> str:
    """Suffix after _ADD_ (case-insensitive) in group_a tag, e.g. ..._Add_TECHNOLOGY -> TECHNOLOGY."""
    if group_a is None:
        return ""
    try:
        if pd.isna(group_a):
            return ""
    except TypeError:
        pass
    s = str(group_a).strip()
    if s == "" or s.lower() in ("nan", "none", "null"):
        return ""
    m = re.search(r"(?i)_add_(.+)$", s)
    if not m:
        return ""
    return _normalize_group_a_sector_value(m.group(1))


def _effective_group_a_sector_series(df: pd.DataFrame) -> pd.Series:
    """
    Return effective sector series for Group A:
    group_a_sector -> sector -> group_a tag suffix after _ADD_.
    Values normalized (upper, empty if missing).
    """
    idx = df.index
    ga = (
        df["group_a_sector"]
        if "group_a_sector" in df.columns
        else pd.Series("", index=idx, dtype=object)
    )
    sec = (
        df["sector"]
        if "sector" in df.columns
        else pd.Series("", index=idx, dtype=object)
    )
    ga_n = ga.map(_normalize_group_a_sector_value)
    sec_n = sec.map(_normalize_group_a_sector_value)
    eff = ga_n.where(ga_n != "", sec_n)
    if "group_a" in df.columns:
        tag_n = df["group_a"].map(_infer_group_a_sector_from_tag)
        eff = eff.where(eff != "", tag_n)
    return eff.map(_normalize_group_a_sector_value)


def _first_valid_group_a_sector(df: pd.DataFrame) -> str:
    """First non-empty normalized effective sector in df, or empty string."""
    if df.empty:
        return ""
    eff = _effective_group_a_sector_series(df)
    eff = eff[eff != ""]
    return str(eff.iloc[0]) if not eff.empty else ""


def _load_tags_from_path_for_a(path: Path) -> tuple[pd.DataFrame, str]:
    """
    Load tags for Group A: require symbol, as_of_date, group_a.
    group_a_mode optional; if missing, inferred from group_a (A_Total_Market -> TOTAL_MARKET, _Add_ -> INDUSTRY_ADD_SECTOR, else INDUSTRY_ONLY).
    Keep optional group_a_sector, group_a_industry, sector, industry if present.
    Return (latest_only_df, as_of_date_str).
    """
    need = ["symbol", "as_of_date", "group_a"]
    if path.suffix.lower() == ".csv":
        tags = pd.read_csv(path, low_memory=False)
    else:
        tags = pd.read_parquet(path)

    missing = [c for c in need if c not in tags.columns]
    if missing:
        raise ValueError(f"Tags file missing required columns for Group A: {missing}")

    optional = ["group_a_mode", "group_a_sector", "group_a_industry", "sector", "industry"]
    keep = need + [c for c in optional if c in tags.columns]
    tags = tags[keep].copy()
    if "group_a_mode" not in tags.columns:
        tags["group_a_mode"] = tags["group_a"].map(_infer_group_a_mode)
    else:
        tags["group_a_mode"] = tags.apply(
            lambda r: _normalize_or_infer_group_a_mode(r["group_a_mode"], r["group_a"]),
            axis=1,
        )
    tags = normalize_as_of_date(tags)
    tags = tags.dropna(subset=["as_of_date"])
    if tags.empty:
        raise ValueError("No valid as_of_date in tags file.")

    tags = filter_to_latest_as_of_date(tags, label="_load_tags_from_path_for_a")
    tags = dedupe_by_symbol_keep_latest(tags)
    tags["symbol"] = _normalize_symbol_series(tags["symbol"])
    as_of_date_str = pd.Timestamp(tags["as_of_date"].iloc[0]).strftime("%Y-%m-%d")
    return tags, as_of_date_str


def _normalize_symbol_series(s: pd.Series) -> pd.Series:
    """Consistent join key: str, strip, upper (matches load_factors_latest and tag loaders)."""
    return s.astype(str).str.strip().str.upper()


def log_tags_factors_inner_join_diagnostics(
    latest_tags: pd.DataFrame,
    factors: pd.DataFrame,
    label: str,
    merged: pd.DataFrame | None = None,
) -> None:
    """
    Diagnostic: inner join on symbol keeps only intersection of tag and factor symbols.
    Log row counts before merge, set overlap, and optional post-merge row count.
    Does not change data; safe to call from load_* helpers.
    """
    if "symbol" not in latest_tags.columns or "symbol" not in factors.columns:
        print(f"[{label}] inner join diagnostics: missing symbol column on tags or factors")
        return
    t_syms = set(_normalize_symbol_series(latest_tags["symbol"]))
    f_syms = set(_normalize_symbol_series(factors["symbol"]))
    only_in_tags = t_syms - f_syms
    only_in_factors = f_syms - t_syms
    inter = t_syms & f_syms
    n_expect = len(inter)
    print(
        f"[{label}] inner join on symbol (how='inner'): "
        f"tags_rows={len(latest_tags)} factors_rows={len(factors)} "
        f"|unique_symbols_tags|={len(t_syms)} |unique_symbols_factors|={len(f_syms)} "
        f"|intersection|={len(inter)} "
        f"only_in_tags={len(only_in_tags)} only_in_factors={len(only_in_factors)} "
        f"expected_merged_rows={n_expect}"
    )
    if only_in_tags:
        sample = sorted(only_in_tags)[:12]
        more = " ..." if len(only_in_tags) > 12 else ""
        print(f"  [{label}] only_in_tags (sample): {sample}{more}")
    if only_in_factors:
        sample = sorted(only_in_factors)[:12]
        more = " ..." if len(only_in_factors) > 12 else ""
        print(f"  [{label}] only_in_factors (sample): {sample}{more}")
    if merged is not None:
        mr = len(merged)
        ok = mr == n_expect
        print(
            f"  [{label}] merged_rows={mr} "
            f"({'matches expected intersection' if ok else 'WARNING: differs from expected; check duplicate symbols'})"
        )


def load_factors_latest(data_dir: str | Path) -> pd.DataFrame:
    """
    Load factors_latest: CSV first, then parquet. Symbol normalized to str.upper().strip().
    If the file uses `asOfDate` but not `as_of_date`, coalesce to `as_of_date` for a single
    canonical data-date column, then filter to max(date) and dedupe by symbol (not row order).
    """
    data_dir = Path(data_dir)
    fcsv = data_dir / "factors_latest.csv"
    fpq = data_dir / "factors_latest.parquet"
    if fcsv.exists():
        factors = pd.read_csv(fcsv, low_memory=False)
    elif fpq.exists():
        factors = pd.read_parquet(fpq)
    else:
        raise FileNotFoundError(
            f"factors_latest not found. Expected {fcsv} or {fpq}."
        )
    if "symbol" not in factors.columns:
        raise ValueError("factors_latest must contain column: symbol")
    factors["symbol"] = factors["symbol"].astype(str).str.strip().str.upper()
    if "as_of_date" not in factors.columns and "asOfDate" in factors.columns:
        factors = factors.copy()
        factors["as_of_date"] = factors["asOfDate"]
        print(
            "[load_factors_latest] coalesced column as_of_date from asOfDate "
            "(canonical data-date for max-date filtering)."
        )
    return finalize_factors_latest_by_as_of_date(factors, label="load_factors_latest")


def load_latest_tags_and_factors(
    logic_dir: str | Path = DEFAULT_LOGIC_DIR,
    data_dir: str | Path = DEFAULT_DATA_DIR,
) -> tuple[pd.DataFrame, str]:
    """
    Load latest group tags (from history or fallback date-stamped file) and factors_latest;
    normalize symbol, inner join on symbol.
    Tags and factors are each reduced to max(as_of_date) before join (not row-order dedupe).
    factors_latest may use `asOfDate`; it is coalesced to `as_of_date` for filtering.
    Inner join drops symbols present in only one side; see log_tags_factors_inner_join_diagnostics output.
    Overlapping column names (other than symbol) keep factors_latest names on the right;
    tag-side duplicates are suffixed with _tag.
    Returns (merged_df, as_of_date_str, tags_path_used).
    """
    logic_dir = Path(logic_dir)
    data_dir = Path(data_dir)
    if not logic_dir.exists():
        raise FileNotFoundError(f"Logic dir not found: {logic_dir}")

    tags_path = _resolve_latest_tags_path(logic_dir)
    latest_tags, as_of_date_str = _load_tags_from_path(tags_path)

    factors = load_factors_latest(data_dir)
    latest_tags["symbol"] = latest_tags["symbol"].astype(str).str.strip().str.upper()

    # Preserve factors_latest column names when tags share the same name (e.g. sector, industry):
    # tag-side duplicates get _tag; right (factors) keeps unprefixed names.
    base = latest_tags.merge(factors, on="symbol", how="inner", suffixes=("_tag", ""))
    base = apply_factor_canonical_aliases(base)
    log_tags_factors_inner_join_diagnostics(
        latest_tags, factors, "load_latest_tags_and_factors", merged=base
    )
    base = ensure_merged_snapshot_as_of_date(base, label="load_latest_tags_and_factors")
    return base.sort_values("symbol").reset_index(drop=True), as_of_date_str, tags_path


def load_latest_tags_and_factors_for_a(
    logic_dir: str | Path = DEFAULT_LOGIC_DIR,
    data_dir: str | Path = DEFAULT_DATA_DIR,
) -> tuple[pd.DataFrame, str, Path]:
    """
    Load latest group tags (Group A columns: group_a, group_a_mode, etc.) and factors_latest;
    inner join on symbol. Symbol normalized. Inner join keeps only symbols in both tables.
    Overlapping names: tag columns get _tag suffix; factors_latest columns keep their original names.
    Returns (merged_df, as_of_date_str, tags_path).
    """
    logic_dir = Path(logic_dir)
    data_dir = Path(data_dir)
    if not logic_dir.exists():
        raise FileNotFoundError(f"Logic dir not found: {logic_dir}")

    tags_path = _resolve_latest_tags_path(logic_dir)
    latest_tags, as_of_date_str = _load_tags_from_path_for_a(tags_path)

    factors = load_factors_latest(data_dir)
    latest_tags["symbol"] = latest_tags["symbol"].astype(str).str.strip().str.upper()

    base = latest_tags.merge(factors, on="symbol", how="inner", suffixes=("_tag", ""))
    base = apply_factor_canonical_aliases(base)
    log_tags_factors_inner_join_diagnostics(
        latest_tags, factors, "load_latest_tags_and_factors_for_a", merged=base
    )
    base = ensure_merged_snapshot_as_of_date(base, label="load_latest_tags_and_factors_for_a")
    return base.sort_values("symbol").reset_index(drop=True), as_of_date_str, tags_path


def _load_tags_from_path_for_b(path: Path) -> tuple[pd.DataFrame, str]:
    """
    Load Group B tags from a path and return (latest_only_df, as_of_date_str).
    latest_only_df is filtered to max as_of_date and deduped to 1 row/symbol.
    """
    required = [
        "symbol",
        "as_of_date",
        "group_b",
        "group_b_adjusted_peer_count",
        "group_b_adjusted_peer_count_relaxed",
        "group_b_relaxed_final_tolerance",
        "group_b_final_peer_count",
        "group_b_final_peer_method",
        "group_b_nearest_fill_added",
        "group_b_peer_quality",
        "group_b_no_mcap_peer_count",
        "group_b_no_mcap_peer_method",
        "group_b_no_mcap_peer_quality",
    ]

    if path.suffix.lower() == ".csv":
        tags = pd.read_csv(path, low_memory=False)
    else:
        tags = pd.read_parquet(path)

    missing = [c for c in required if c not in tags.columns]
    if missing:
        raise ValueError(f"Tags file missing required Group B columns: {missing}")

    tags = tags[required].copy()
    tags = normalize_as_of_date(tags)
    tags = tags.dropna(subset=["as_of_date"])
    if tags.empty:
        raise ValueError("No valid as_of_date in Group B tags file.")

    tags = filter_to_latest_as_of_date(tags, label="_load_tags_from_path_for_b")
    tags = dedupe_by_symbol_keep_latest(tags)
    tags["symbol"] = _normalize_symbol_series(tags["symbol"])
    as_of_date_str = pd.Timestamp(tags["as_of_date"].iloc[0]).strftime("%Y-%m-%d")
    return tags, as_of_date_str


def load_latest_tags_and_factors_for_b(
    logic_dir: str | Path | None = None,
    data_dir: str | Path | None = None,
) -> tuple[pd.DataFrame, str, Path]:
    """
    Load latest Group B tags + factors_latest and inner-join on symbol.
    Inner join keeps only symbols present in both sides (see diagnostics log).
    Overlapping column names: tag-side _tag suffix; factors_latest names preserved on the right.
    Returns (base, as_of_date_str, tags_path).

    Minimal coercions for B peer reconstruction inputs:
      - market_cap (from existing `market_cap` or `Market Cap`)
      - revenue_ttm (from existing `revenue_ttm` or `Sales (Rev)`)
      - total_assets (from existing `total_assets` or `Total Assets`/`totalAssets` if present)
    """
    logic_dir = Path(logic_dir) if logic_dir is not None else DEFAULT_LOGIC_DIR
    data_dir = Path(data_dir) if data_dir is not None else DEFAULT_DATA_DIR
    if not logic_dir.exists():
        raise FileNotFoundError(f"Logic dir not found: {logic_dir}")

    tags_path = _resolve_latest_tags_path(logic_dir)
    latest_tags, as_of_date_str = _load_tags_from_path_for_b(tags_path)

    factors = load_factors_latest(data_dir)

    base = latest_tags.merge(factors, on="symbol", how="inner", suffixes=("_tag", ""))
    base = apply_factor_canonical_aliases(base)
    log_tags_factors_inner_join_diagnostics(
        latest_tags, factors, "load_latest_tags_and_factors_for_b", merged=base
    )
    n_after_merge = len(base)
    base = ensure_merged_snapshot_as_of_date(base, label="load_latest_tags_and_factors_for_b")
    if len(base) != n_after_merge:
        print(
            f"  [load_latest_tags_and_factors_for_b] NOTE: as-of-date merge cleanup "
            f"changed rows {n_after_merge} -> {len(base)}"
        )

    # Coerce/derive peer-selection input columns (snake_case) for later reuse.
    if "market_cap" not in base.columns and "Market Cap" in base.columns:
        base["market_cap"] = base["Market Cap"]
    if "market_cap" in base.columns:
        base["market_cap"] = pd.to_numeric(base["market_cap"], errors="coerce")
    else:
        base["market_cap"] = np.nan

    if "revenue_ttm" not in base.columns and "Sales (Rev)" in base.columns:
        base["revenue_ttm"] = base["Sales (Rev)"]
    if "revenue_ttm" in base.columns:
        base["revenue_ttm"] = pd.to_numeric(base["revenue_ttm"], errors="coerce")
    else:
        base["revenue_ttm"] = np.nan

    if "total_assets" not in base.columns:
        if "Total Assets" in base.columns:
            base["total_assets"] = base["Total Assets"]
        elif "totalAssets" in base.columns:
            base["total_assets"] = base["totalAssets"]
        else:
            base["total_assets"] = np.nan
    base["total_assets"] = pd.to_numeric(base["total_assets"], errors="coerce")

    return base.sort_values("symbol").reset_index(drop=True), as_of_date_str, tags_path


def get_factor_columns(
    df: pd.DataFrame,
    exclude_cols: set[str] | None = None,
) -> list[str]:
    """
    Return list of columns suitable for factor stats (numeric or coercible to numeric).
    Excludes EXCLUDE_COLUMNS (or provided set) and bool dtype.
    """
    exclude = exclude_cols if exclude_cols is not None else EXCLUDE_COLUMNS
    cols: list[str] = []
    for c in df.columns:
        if c in exclude:
            continue
        if pd.api.types.is_bool_dtype(df[c]):
            continue
        if pd.api.types.is_numeric_dtype(df[c]):
            cols.append(c)
        else:
            if pd.to_numeric(df[c], errors="coerce").notna().any():
                cols.append(c)
    return _dedupe_alias_factor_columns(cols, df)


def compute_representatives(df: pd.DataFrame, group_col: str) -> pd.DataFrame:
    """
    Compute per (group_tag, factor_name): n_valid, median, q25, q75, iqr.
    Long format: group_tag, factor_name, n_valid, median, q25, q75, iqr.
    """
    factor_cols = get_factor_columns(df)
    if not factor_cols:
        return pd.DataFrame(
            columns=["group_tag", "factor_name", "n_valid", "median", "q25", "q75", "iqr"]
        )

    long_df = df[[group_col] + factor_cols].melt(
        id_vars=[group_col], var_name="factor_name", value_name="value"
    )
    long_df["value"] = pd.to_numeric(long_df["value"], errors="coerce")
    long_df = long_df.dropna(subset=[group_col, "value"])
    if long_df.empty:
        return pd.DataFrame(
            columns=["group_tag", "factor_name", "n_valid", "median", "q25", "q75", "iqr"]
        )

    g = long_df.groupby([group_col, "factor_name"])["value"]
    out = g.agg(
        n_valid="size",
        median="median",
        q25=lambda s: float(s.quantile(0.25)),
        q75=lambda s: float(s.quantile(0.75)),
    ).reset_index()
    out["iqr"] = out["q75"] - out["q25"]
    out = out.rename(columns={group_col: "group_tag"})
    return out.sort_values(["group_tag", "factor_name"]).reset_index(drop=True)


def attach_representatives_and_deviations(
    base_df: pd.DataFrame,
    reps_df: pd.DataFrame,
    group_col: str,
    factor_cols: list[str],
) -> pd.DataFrame:
    """
    Attach representative stats and deviation columns to base_df (one row per symbol).
    Naming:
      - rep__{factor}__median, rep__{factor}__q25, rep__{factor}__q75, rep__{factor}__iqr, rep__{factor}__n_valid
      - dev__{factor}__abs, dev__{factor}__pct, dev__{factor}__robust_z
    """
    out = base_df.copy()
    reps = reps_df.set_index(["group_tag", "factor_name"])

    # Pre-allocate rep/dev columns in one concat to avoid DataFrame fragmentation.
    rep_dev_cols = [
        col
        for f in factor_cols
        for col in (
            f"{REP_PREFIX}{f}__median",
            f"{REP_PREFIX}{f}__q25",
            f"{REP_PREFIX}{f}__q75",
            f"{REP_PREFIX}{f}__iqr",
            f"{REP_PREFIX}{f}__n_valid",
            f"{DEV_PREFIX}{f}__abs",
            f"{DEV_PREFIX}{f}__pct",
            f"{DEV_PREFIX}{f}__robust_z",
        )
    ]
    if rep_dev_cols:
        extra = pd.DataFrame(index=out.index, data={c: np.nan for c in rep_dev_cols})
        out = pd.concat([out, extra], axis=1)

    for idx in out.index:
        tag = out.at[idx, group_col]
        if pd.isna(tag):
            continue
        for f in factor_cols:
            if (tag, f) not in reps.index:
                continue
            row = reps.loc[(tag, f)]
            med = float(row["median"])
            iqr_val = float(row["iqr"])
            val = out.at[idx, f]
            if pd.isna(val):
                continue
            val = float(pd.to_numeric(val, errors="coerce"))
            out.at[idx, f"{REP_PREFIX}{f}__median"] = med
            out.at[idx, f"{REP_PREFIX}{f}__q25"] = row["q25"]
            out.at[idx, f"{REP_PREFIX}{f}__q75"] = row["q75"]
            out.at[idx, f"{REP_PREFIX}{f}__iqr"] = iqr_val
            out.at[idx, f"{REP_PREFIX}{f}__n_valid"] = row["n_valid"]
            out.at[idx, f"{DEV_PREFIX}{f}__abs"] = val - med
            if med != 0:
                out.at[idx, f"{DEV_PREFIX}{f}__pct"] = (val - med) / abs(med)
            if iqr_val > 0:
                out.at[idx, f"{DEV_PREFIX}{f}__robust_z"] = (val - med) / iqr_val

    return out.copy()


# ---------------------------------------------------------------------------
# Group A: mode-based peer sets and representatives
# ---------------------------------------------------------------------------


def get_factor_columns_for_a(df: pd.DataFrame) -> list[str]:
    """Return factor columns for Group A (exclude EXCLUDE_COLUMNS_A). All numeric columns for rep/dev; distinct from similarity factors."""
    return get_factor_columns(df, exclude_cols=EXCLUDE_COLUMNS_A)


def get_factor_columns_for_b(df: pd.DataFrame) -> list[str]:
    """Return factor columns for Group B (exclude EXCLUDE_COLUMNS_B)."""
    return get_factor_columns(df, exclude_cols=EXCLUDE_COLUMNS_B)


def build_group_a_representative_table(
    peer_df: pd.DataFrame,
    factor_cols: list[str],
    group_tag: str,
) -> pd.DataFrame:
    """
    Compute representative stats (n_valid, median, q25, q75, iqr) for one peer set.
    Uses full factor_cols (all numeric); not to be confused with similarity factors.
    Returns long-format DataFrame: group_tag, factor_name, n_valid, median, q25, q75, iqr.
    """
    if not factor_cols or peer_df.empty:
        return pd.DataFrame(
            columns=["group_tag", "factor_name", "n_valid", "median", "q25", "q75", "iqr"]
        )
    present = [c for c in factor_cols if c in peer_df.columns]
    if not present:
        return pd.DataFrame(
            columns=["group_tag", "factor_name", "n_valid", "median", "q25", "q75", "iqr"]
        )
    long_df = peer_df[present].copy()
    long_df = long_df.melt(var_name="factor_name", value_name="value")
    long_df["value"] = pd.to_numeric(long_df["value"], errors="coerce")
    long_df = long_df.dropna(subset=["value"])
    if long_df.empty:
        return pd.DataFrame(
            columns=["group_tag", "factor_name", "n_valid", "median", "q25", "q75", "iqr"]
        )
    g = long_df.groupby("factor_name")["value"]
    out = g.agg(
        n_valid="size",
        median="median",
        q25=lambda s: float(s.quantile(0.25)),
        q75=lambda s: float(s.quantile(0.75)),
    ).reset_index()
    out["iqr"] = out["q75"] - out["q25"]
    out["group_tag"] = group_tag
    return out[["group_tag", "factor_name", "n_valid", "median", "q25", "q75", "iqr"]]


# ---------------------------------------------------------------------------
# Group B: per-symbol size-score peer sets (not group_b groupby)
# ---------------------------------------------------------------------------


def _ensure_group_b_features(base: pd.DataFrame) -> pd.DataFrame:
    """Ensure group_b_mode / size_score / validity columns exist (reuse tag_and_save_groups)."""
    df = base.copy()
    need = ("group_b_mode", "group_b_size_score", "group_b_market_cap_valid")
    if not all(c in df.columns for c in need):
        df = compute_group_b_features(df)
    return df


def resolve_group_b_peer_set_for_symbol(
    base: pd.DataFrame,
    symbol: str,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """
    Reconstruct the dynamic peer set for one symbol from Group B tags + size_score universe masks.
    Returns (peer_df subset of base, metadata dict).
    Does not use generic compute_representatives(..., group_col=\"group_b\").
    """
    work = _ensure_group_b_features(base).reset_index(drop=True)
    if "symbol" not in work.columns or "group_b" not in work.columns:
        raise ValueError("base must contain symbol and group_b")

    sym_u = str(symbol).strip().upper()
    pos = np.where(work["symbol"].astype(str).str.strip().str.upper().values == sym_u)[0]
    if len(pos) == 0:
        raise ValueError(f"symbol not in base: {symbol}")
    i = int(pos[0])
    row = work.iloc[i]

    size_score = pd.to_numeric(work["group_b_size_score"], errors="coerce").to_numpy(dtype=float)
    mcap_valid = work["group_b_market_cap_valid"].fillna(False).to_numpy(dtype=bool)
    group_b_mode = work["group_b_mode"].astype(str).to_numpy()
    symbols = work["symbol"].astype(str).str.strip().str.upper().to_numpy()

    full_universe_mask = (group_b_mode == "MCAP_REV_ASSETS") & mcap_valid & ~np.isnan(size_score)
    adjusted_universe_mask = (
        np.isin(group_b_mode, ["MCAP_REV_ASSETS", "MCAP_REV", "MCAP_ASSETS", "MCAP_ONLY"])
        & mcap_valid
        & ~np.isnan(size_score)
    )
    full_indices = np.where(full_universe_mask)[0]
    adjusted_indices = np.where(adjusted_universe_mask)[0]

    tag = str(row.get("group_b", "")).strip()
    score_i = float(size_score[i])
    target_peer_count = int(GROUP_B_RELAXED_TARGET_MIN_PEERS)

    meta: dict[str, Any] = {
        "peer_method": "",
        "peer_quality": "",
        "final_peer_count": 0,
        "tolerance_used": None,
        "b_peer_symbols": [],
    }

    if tag == "B_NO_MARKET_CAP":
        pool = build_group_b_no_market_cap_peer_pool(work)
        if isinstance(pool, set):
            peer_symbols = sorted(str(s).strip().upper() for s in pool if str(s).strip())
        else:
            peer_symbols = [
                str(s).strip().upper()
                for s in list(pool)
                if s is not None and str(s).strip()
            ]
        peer_df = work.loc[work["symbol"].isin(peer_symbols)].drop_duplicates(subset=["symbol"], keep="first")
        actual_count = int(len(peer_df))
        method_raw = row.get("group_b_no_mcap_peer_method", "")
        quality_raw = row.get("group_b_no_mcap_peer_quality", "")
        method_row = str(method_raw).strip() if pd.notna(method_raw) else ""
        quality_row = str(quality_raw).strip() if pd.notna(quality_raw) else ""
        meta["peer_method"] = method_row or "GLOBAL_SECTOR_3POINTS"
        if not quality_row:
            # tag_and_save_groups quality rule: count >= 15 => LOW, else VERY_LOW
            quality_row = "LOW" if actual_count >= 15 else "VERY_LOW"
        meta["peer_quality"] = quality_row
        meta["final_peer_count"] = actual_count
        meta["tolerance_used"] = None
        meta["b_peer_symbols"] = (
            peer_df["symbol"].astype(str).str.strip().str.upper().tolist()
            if not peer_df.empty
            else []
        )
        return peer_df, meta

    final_peer_method = str(row.get("group_b_final_peer_method", "BASE")).strip().upper()
    relaxed_tol = pd.to_numeric(row.get("group_b_relaxed_final_tolerance"), errors="coerce")
    tol_used = float(relaxed_tol) if pd.notna(relaxed_tol) else None
    meta["peer_quality"] = str(row.get("group_b_peer_quality", ""))

    if tag == "B_NORMAL":
        universe_indices = full_indices
        threshold = float(GROUP_B_BASE_TOLERANCE)
        cand_d = np.abs(size_score[universe_indices] - score_i)
        keep_indices = universe_indices[cand_d <= threshold]
        peer_symbols = sorted(set(symbols[keep_indices].tolist()))
        meta["peer_method"] = "BASE"
        meta["tolerance_used"] = threshold
    elif tag == "B_ADJUSTED":
        universe_indices = adjusted_indices
        threshold = float(GROUP_B_BASE_TOLERANCE)
        cand_d = np.abs(size_score[universe_indices] - score_i)
        keep_indices = universe_indices[cand_d <= threshold]
        peer_symbols = sorted(set(symbols[keep_indices].tolist()))
        meta["peer_method"] = "BASE"
        meta["tolerance_used"] = threshold
    elif tag in ("B_INSUFFICIENT", "B_RISK"):
        universe_indices = adjusted_indices
        method = final_peer_method
        if method == "BASE":
            threshold = float(GROUP_B_BASE_TOLERANCE)
            peer_method_for_meta = "BASE"
        elif method == "RELAXED":
            threshold = float(tol_used if tol_used is not None else GROUP_B_BASE_TOLERANCE)
            peer_method_for_meta = "RELAXED"
        elif method in ("NEAREST_FILL", "VERY_LOW"):
            threshold = float(tol_used if tol_used is not None else GROUP_B_BASE_TOLERANCE)
            peer_method_for_meta = method
        else:
            threshold = float(GROUP_B_BASE_TOLERANCE)
            peer_method_for_meta = "BASE"

        if tag in ("B_INSUFFICIENT", "B_RISK") and final_peer_method in ("NEAREST_FILL", "VERY_LOW"):
            cand_d = np.abs(size_score[universe_indices] - score_i)
            relaxed_mask = cand_d <= threshold
            relaxed_indices = universe_indices[relaxed_mask]
            relaxed_syms = symbols[relaxed_indices].tolist()
            need = max(0, target_peer_count - len(relaxed_syms))
            if need > 0:
                remaining_indices = universe_indices[~relaxed_mask]
                if len(remaining_indices) > 0:
                    rem_d = cand_d[~relaxed_mask]
                    rem_symbols = symbols[remaining_indices]
                    order = np.lexsort((rem_symbols, rem_d))
                    add_indices = remaining_indices[order][:need]
                else:
                    add_indices = np.array([], dtype=int)
            else:
                add_indices = np.array([], dtype=int)
            all_indices = np.concatenate([relaxed_indices, add_indices])
            peer_symbols = sorted(set(symbols[all_indices].tolist()))
        else:
            cand_d = np.abs(size_score[universe_indices] - score_i)
            keep_indices = universe_indices[cand_d <= threshold]
            peer_symbols = sorted(set(symbols[keep_indices].tolist()))

        tol_meta = float(threshold) if peer_method_for_meta in ("BASE", "RELAXED") else tol_used
        meta["tolerance_used"] = tol_meta
        meta["peer_method"] = peer_method_for_meta
    else:
        universe_indices = adjusted_indices
        threshold = float(GROUP_B_BASE_TOLERANCE)
        cand_d = np.abs(size_score[universe_indices] - score_i)
        keep_indices = universe_indices[cand_d <= threshold]
        peer_symbols = sorted(set(symbols[keep_indices].tolist()))
        meta["peer_method"] = "BASE"
        meta["tolerance_used"] = threshold

    peer_df = work.loc[work["symbol"].isin(peer_symbols)].drop_duplicates(subset=["symbol"], keep="first")
    try:
        meta["final_peer_count"] = int(row.get("group_b_final_peer_count", len(peer_symbols)))
    except (TypeError, ValueError):
        meta["final_peer_count"] = len(peer_symbols)
    meta["b_peer_symbols"] = peer_symbols
    return peer_df, meta


def build_peer_sets_and_reps_b(
    base: pd.DataFrame,
    factor_cols: list[str],
) -> tuple[dict[str, pd.DataFrame], pd.DataFrame]:
    """
    For each symbol, resolve peer set and compute long-format representative stats (per factor).
    Returns (reps_by_symbol: dict[symbol_str, reps_df], meta_df one row per symbol).
    """
    work = _ensure_group_b_features(base)
    reps_by_symbol: dict[str, pd.DataFrame] = {}
    meta_rows: list[dict[str, Any]] = []

    for sym in work["symbol"].astype(str).str.strip().str.upper().unique():
        peer_df, meta = resolve_group_b_peer_set_for_symbol(work, sym)
        reps = build_group_a_representative_table(peer_df, factor_cols, str(sym))
        reps_by_symbol[str(sym)] = reps
        meta_rows.append({"symbol": sym, **meta})

    meta_df = pd.DataFrame(meta_rows)
    if meta_df.empty:
        meta_df = pd.DataFrame(
            columns=[
                "symbol",
                "peer_method",
                "peer_quality",
                "final_peer_count",
                "tolerance_used",
                "b_peer_symbols",
            ]
        )
    return reps_by_symbol, meta_df


def attach_representatives_and_deviations_b(
    base_df: pd.DataFrame,
    reps_by_symbol: dict,
    meta_df: pd.DataFrame,
    factor_cols: list[str],
    group_tag_col: str = "group_b",
) -> pd.DataFrame:
    """
    Attach per-symbol peer-set representatives and deviations (symbol-keyed reps, not group_b groupby).
    Left-joins meta_df (peer reconstruction summary) on symbol when provided; rep/dev use reps_by_symbol.
    """
    _ = group_tag_col  # row still carries group_b; peer reps are keyed by symbol string

    out = base_df.copy()
    if "symbol" not in out.columns:
        raise ValueError("attach_representatives_and_deviations_b: base_df must contain column 'symbol'")
    if meta_df is not None and not meta_df.empty and "symbol" in meta_df.columns:
        m = meta_df.copy()
        m["symbol"] = m["symbol"].astype(str).str.strip().str.upper()
        out["symbol"] = out["symbol"].astype(str).str.strip().str.upper()
        out = out.merge(m, on="symbol", how="left", suffixes=("", "_b_peer_meta"))

    rep_dev_cols = [
        col
        for f in factor_cols
        for col in (
            f"{REP_PREFIX}{f}__median",
            f"{REP_PREFIX}{f}__q25",
            f"{REP_PREFIX}{f}__q75",
            f"{REP_PREFIX}{f}__iqr",
            f"{REP_PREFIX}{f}__n_valid",
            f"{DEV_PREFIX}{f}__abs",
            f"{DEV_PREFIX}{f}__pct",
            f"{DEV_PREFIX}{f}__robust_z",
        )
    ]
    if rep_dev_cols:
        extra = pd.DataFrame(index=out.index, data={c: np.nan for c in rep_dev_cols})
        out = pd.concat([out, extra], axis=1)

    for idx in out.index:
        sym = str(out.at[idx, "symbol"]).strip().upper()
        if sym not in reps_by_symbol:
            continue
        reps = reps_by_symbol[sym]
        if reps.empty or "factor_name" not in reps.columns:
            continue
        reps_ix = reps.set_index("factor_name")
        for f in factor_cols:
            if f not in reps_ix.index:
                continue
            if f not in out.columns:
                continue
            rrow = reps_ix.loc[f]
            if isinstance(rrow, pd.DataFrame):
                rrow = rrow.iloc[0]
            med = float(pd.to_numeric(rrow["median"], errors="coerce"))
            iqr_val = float(pd.to_numeric(rrow["iqr"], errors="coerce"))
            if pd.isna(med):
                continue
            val = out.at[idx, f]
            if pd.isna(val):
                continue
            val = float(pd.to_numeric(val, errors="coerce"))
            if pd.isna(val):
                continue
            q25 = pd.to_numeric(rrow.get("q25", np.nan), errors="coerce")
            q75 = pd.to_numeric(rrow.get("q75", np.nan), errors="coerce")
            n_valid = pd.to_numeric(rrow.get("n_valid", np.nan), errors="coerce")
            out.at[idx, f"{REP_PREFIX}{f}__median"] = med
            out.at[idx, f"{REP_PREFIX}{f}__q25"] = q25
            out.at[idx, f"{REP_PREFIX}{f}__q75"] = q75
            out.at[idx, f"{REP_PREFIX}{f}__iqr"] = iqr_val
            out.at[idx, f"{REP_PREFIX}{f}__n_valid"] = n_valid
            out.at[idx, f"{DEV_PREFIX}{f}__abs"] = val - med
            if med != 0:
                out.at[idx, f"{DEV_PREFIX}{f}__pct"] = (val - med) / abs(med)
            if not pd.isna(iqr_val) and iqr_val > 0:
                out.at[idx, f"{DEV_PREFIX}{f}__robust_z"] = (val - med) / iqr_val

    return out.copy()


def rank_sector_fill_candidates(
    base: pd.DataFrame,
    base_symbols: set[str],
    sector_value: str,
    similarity_factors: list[str],
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """
    Rank sector fill candidates: same normalized sector, not in base_symbols, >= 2 valid similarity factors.
    Returns (ranked_df with finite distance only, debug_info_dict).
    debug_info: sector_value_normalized, candidate_count_raw, candidate_count_after_valid,
    present_similarity_factors, sim_in_base, has_any_finite_distance.
    """
    empty_cols = ["symbol", "distance_score", "valid_similarity_factor_count"]
    empty_df = pd.DataFrame(columns=empty_cols)
    debug: dict[str, Any] = {
        "sector_value_normalized": "",
        "candidate_count_raw": 0,
        "candidate_count_after_valid": 0,
        "present_similarity_factors": [],
        "sim_in_base": [],
        "has_any_finite_distance": False,
    }

    sector_norm = _normalize_group_a_sector_value(sector_value)
    debug["sector_value_normalized"] = sector_norm
    if not sector_norm:
        return empty_df, debug

    eff_sector = _effective_group_a_sector_series(base)
    if eff_sector.empty:
        return empty_df, debug

    cand = base.loc[eff_sector == sector_norm].copy()
    cand = cand.loc[~cand["symbol"].isin(base_symbols)]
    debug["candidate_count_raw"] = int(len(cand))
    if cand.empty:
        return empty_df, debug

    present_sim = [f for f in similarity_factors if f in cand.columns]
    debug["present_similarity_factors"] = list(present_sim)
    cand = cand.copy()
    for f in present_sim:
        cand[f] = pd.to_numeric(cand[f], errors="coerce")
    valid_count = (
        cand[present_sim].notna().sum(axis=1)
        if present_sim
        else pd.Series(0, index=cand.index)
    )
    cand = cand.loc[valid_count >= 2].copy()
    debug["candidate_count_after_valid"] = int(len(cand))
    if cand.empty:
        return empty_df, debug

    base_sub = base.loc[base["symbol"].isin(base_symbols)]
    sim_in_base = [f for f in present_sim if f in base_sub.columns]
    debug["sim_in_base"] = list(sim_in_base)
    if not sim_in_base:
        return empty_df, debug

    base_medians = base_sub[sim_in_base].median()

    rows: list[dict[str, Any]] = []
    for _, row in cand.iterrows():
        dists: list[float] = []
        for f in sim_in_base:
            v, m = row.get(f), base_medians.get(f)
            if pd.isna(v) or pd.isna(m):
                continue
            try:
                dists.append(float(abs(float(v) - float(m))))
            except (TypeError, ValueError):
                continue
        if not dists:
            score = float("inf")
            n_valid = 0
        else:
            score = sum(dists) / len(dists)
            n_valid = len(dists)
        rows.append({
            "symbol": row["symbol"],
            "distance_score": score,
            "valid_similarity_factor_count": n_valid,
        })
    out = pd.DataFrame(rows)
    if out.empty:
        return empty_df, debug

    finite_mask = np.isfinite(out["distance_score"].astype(float))
    debug["has_any_finite_distance"] = bool(finite_mask.any())
    out = out.loc[finite_mask].copy()
    if out.empty:
        return empty_df, debug

    out = out.sort_values(
        ["distance_score", "valid_similarity_factor_count", "symbol"],
        ascending=[True, False, True],
    ).reset_index(drop=True)
    return out, debug


def _resolve_add_sector_debug_reason(
    need: int,
    sector_norm: str,
    ranked: pd.DataFrame,
    dbg: dict[str, Any],
    n_added: int,
) -> str:
    """Pick a_add_debug_reason for INDUSTRY_ADD_SECTOR rows."""
    if not sector_norm:
        return "NO_SECTOR_VALUE"
    if need == 0:
        return "BASE_ALREADY_SUFFICIENT"
    if dbg.get("candidate_count_raw", 0) == 0:
        return "NO_SECTOR_CANDIDATES"
    if dbg.get("candidate_count_after_valid", 0) == 0:
        return "NO_VALID_SIMILARITY_CANDIDATES"
    if ranked.empty:
        if dbg.get("candidate_count_after_valid", 0) > 0 and not dbg.get("has_any_finite_distance", False):
            return "NO_DISTANCE_AVAILABLE"
        return "NO_VALID_SIMILARITY_CANDIDATES"
    if n_added >= 1:
        return "ADDED_FROM_SECTOR"
    return "NO_DISTANCE_AVAILABLE"


def resolve_group_a_peer_sets(
    base: pd.DataFrame,
    factor_cols: list[str],
    similarity_factors: list[str] | None = None,
    min_peer_count: int = MIN_A_PEER_COUNT,
) -> tuple[dict[str, pd.DataFrame], pd.DataFrame]:
    """
    Resolve peer sets per mode (INDUSTRY_ONLY / INDUSTRY_ADD_SECTOR / TOTAL_MARKET).
    Same group_a tag shares the same peer set (cached per group_a for INDUSTRY_ADD_SECTOR).
    Returns (peer_sets_by_key, meta_df) with a_peer_mode, a_base_count, a_added_count,
    a_final_peer_count, a_peer_shortfall_flag, and a_add_* debug columns for sector fill.
    """
    similarity_factors = similarity_factors or [
        f for f in SIMILARITY_FACTORS_A if f in base.columns
    ]
    mode_col = "group_a_mode"
    tag_col = "group_a"
    if mode_col not in base.columns or tag_col not in base.columns:
        raise ValueError("base must contain group_a and group_a_mode")

    # Defensive: canonical modes even if caller skipped tag load (avoids NaN -> "nan" string masks).
    base = base.copy()
    base[mode_col] = base.apply(
        lambda r: _normalize_or_infer_group_a_mode(r.get(mode_col), r.get(tag_col)),
        axis=1,
    )

    peer_sets_by_key: dict[str, pd.DataFrame] = {}
    meta_rows: list[dict] = []

    mode_series = base[mode_col].astype(str).str.strip().str.upper()

    # TOTAL_MARKET: one shared peer set = full market; all A_Total_Market rows share same reps
    total_mask = mode_series == "TOTAL_MARKET"
    if total_mask.any():
        n_total = len(base)
        peer_sets_by_key["A_Total_Market"] = base.copy()
        for _, row in base.loc[total_mask].iterrows():
            meta_rows.append({
                "symbol": row["symbol"],
                "a_peer_mode": "TOTAL_MARKET",
                "a_base_count": n_total,
                "a_added_count": 0,
                "a_final_peer_count": n_total,
                "a_peer_shortfall_flag": False,
                "a_add_sector_value": pd.NA,
                "a_add_need_count": pd.NA,
                "a_add_candidate_count_raw": pd.NA,
                "a_add_candidate_count_after_valid": pd.NA,
                "a_add_selected_count": pd.NA,
                "a_add_similarity_factors_used": pd.NA,
                "a_add_debug_reason": "NOT_ADD_MODE",
            })

    # INDUSTRY_ONLY: peer set = same group_a (one set per tag)
    ind_only_mask = mode_series == "INDUSTRY_ONLY"
    if ind_only_mask.any():
        for ga, grp in base.loc[ind_only_mask].groupby(tag_col, sort=False):
            ga_str = str(ga).strip()
            peer_df = grp.copy()
            n = len(peer_df)
            peer_sets_by_key[ga_str] = peer_df
            shortfall = n < min_peer_count
            for _, row in peer_df.iterrows():
                meta_rows.append({
                    "symbol": row["symbol"],
                    "a_peer_mode": "INDUSTRY_ONLY",
                    "a_base_count": n,
                    "a_added_count": 0,
                    "a_final_peer_count": n,
                    "a_peer_shortfall_flag": shortfall,
                    "a_add_sector_value": pd.NA,
                    "a_add_need_count": pd.NA,
                    "a_add_candidate_count_raw": pd.NA,
                    "a_add_candidate_count_after_valid": pd.NA,
                    "a_add_selected_count": pd.NA,
                    "a_add_similarity_factors_used": pd.NA,
                    "a_add_debug_reason": "NOT_ADD_MODE",
                })

    # INDUSTRY_ADD_SECTOR: one peer set per group_a (reused for all symbols with same group_a)
    add_sec_mask = mode_series == "INDUSTRY_ADD_SECTOR"
    if add_sec_mask.any():
        for ga, grp in base.loc[add_sec_mask].groupby(tag_col, sort=False):
            ga_str = str(ga).strip()
            base_peers = grp.copy()
            base_sym_set = set(base_peers["symbol"].astype(str).str.strip().tolist())
            n_base = len(base_peers)

            sector_norm = _first_valid_group_a_sector(base_peers)
            need = max(0, min_peer_count - n_base)

            if not sector_norm:
                ranked = pd.DataFrame(columns=["symbol", "distance_score", "valid_similarity_factor_count"])
                ranked_dbg: dict[str, Any] = {
                    "sector_value_normalized": "",
                    "candidate_count_raw": 0,
                    "candidate_count_after_valid": 0,
                    "present_similarity_factors": [],
                    "sim_in_base": [],
                    "has_any_finite_distance": False,
                }
            else:
                ranked, ranked_dbg = rank_sector_fill_candidates(
                    base, base_sym_set, sector_norm, similarity_factors
                )

            n_added = 0
            final_peer_df = base_peers
            if need > 0 and sector_norm and not ranked.empty:
                added_symbols = ranked["symbol"].head(need).tolist()
                added_df = (
                    base.loc[base["symbol"].isin(added_symbols)]
                    .drop_duplicates(subset=["symbol"], keep="first")
                )
                final_peer_df = pd.concat([base_peers, added_df], ignore_index=True)
                n_added = len(added_df)

            sim_used = "|".join(ranked_dbg.get("sim_in_base", []) or [])
            reason = _resolve_add_sector_debug_reason(
                need, sector_norm, ranked, ranked_dbg, n_added
            )

            n_final = len(final_peer_df)
            shortfall = n_final < min_peer_count
            peer_sets_by_key[ga_str] = final_peer_df
            for _, row in base_peers.iterrows():
                meta_rows.append({
                    "symbol": row["symbol"],
                    "a_peer_mode": "INDUSTRY_ADD_SECTOR",
                    "a_base_count": n_base,
                    "a_added_count": n_added,
                    "a_final_peer_count": n_final,
                    "a_peer_shortfall_flag": shortfall,
                    "a_add_sector_value": sector_norm if sector_norm else pd.NA,
                    "a_add_need_count": need,
                    "a_add_candidate_count_raw": int(ranked_dbg.get("candidate_count_raw", 0)),
                    "a_add_candidate_count_after_valid": int(ranked_dbg.get("candidate_count_after_valid", 0)),
                    "a_add_selected_count": n_added,
                    "a_add_similarity_factors_used": sim_used if sim_used else pd.NA,
                    "a_add_debug_reason": reason,
                })

    meta_df = pd.DataFrame(meta_rows)
    if meta_df.empty:
        meta_df = pd.DataFrame(
            columns=[
                "symbol",
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
        )
    return peer_sets_by_key, meta_df


def build_peer_sets_and_reps_a(
    base: pd.DataFrame,
    factor_cols: list[str],
    similarity_factors: list[str] | None = None,
    min_peer_count: int = MIN_A_PEER_COUNT,
) -> tuple[dict[str, pd.DataFrame], pd.DataFrame]:
    """
    Build peer sets (resolve_group_a_peer_sets) then representative table per set.
    Returns (reps_by_group_a: dict[group_a_key, long_reps_df], meta_df with a_* columns).
    """
    peer_sets_by_key, meta_df = resolve_group_a_peer_sets(
        base, factor_cols,
        similarity_factors=similarity_factors,
        min_peer_count=min_peer_count,
    )
    reps_by_group_a: dict[str, pd.DataFrame] = {}
    for key, peer_df in peer_sets_by_key.items():
        reps_by_group_a[key] = build_group_a_representative_table(
            peer_df, factor_cols, key
        )
    return reps_by_group_a, meta_df


def attach_representatives_and_deviations_a(
    base_df: pd.DataFrame,
    reps_by_group_a: dict[str, pd.DataFrame],
    meta_df: pd.DataFrame,
    factor_cols: list[str],
    group_tag_col: str = "group_a",
) -> pd.DataFrame:
    """
    Attach Group A representative and deviation columns and a_* meta to base_df.
    reps_by_group_a key = group_a tag; meta_df has symbol and a_peer_mode, a_base_count, etc.
    """
    out = base_df.copy()
    rep_dev_cols = [
        col
        for f in factor_cols
        for col in (
            f"{REP_PREFIX}{f}__median",
            f"{REP_PREFIX}{f}__q25",
            f"{REP_PREFIX}{f}__q75",
            f"{REP_PREFIX}{f}__iqr",
            f"{REP_PREFIX}{f}__n_valid",
            f"{DEV_PREFIX}{f}__abs",
            f"{DEV_PREFIX}{f}__pct",
            f"{DEV_PREFIX}{f}__robust_z",
        )
    ]
    if rep_dev_cols:
        extra = pd.DataFrame(index=out.index, data={c: np.nan for c in rep_dev_cols})
        out = pd.concat([out, extra], axis=1)

    if not meta_df.empty and "symbol" in meta_df.columns:
        meta_cols = [
            c
            for c in [
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
            if c in meta_df.columns
        ]
        if meta_cols:
            meta_extra = pd.DataFrame(index=out.index, data={c: pd.Series(index=out.index, dtype=object) for c in meta_cols})
            out = pd.concat([out, meta_extra], axis=1)
        sym_to_meta = meta_df.drop_duplicates(subset=["symbol"], keep="last").set_index("symbol")
        for idx in out.index:
            sym = out.at[idx, "symbol"]
            if sym in sym_to_meta.index:
                for c in meta_cols:
                    out.at[idx, c] = sym_to_meta.at[sym, c]

    for idx in out.index:
        tag = out.at[idx, group_tag_col]
        if pd.isna(tag):
            continue
        tag_str = str(tag).strip()
        if tag_str not in reps_by_group_a:
            continue
        reps = reps_by_group_a[tag_str].set_index("factor_name")
        for f in factor_cols:
            if f not in reps.index:
                continue
            row = reps.loc[f]
            med = float(row["median"])
            iqr_val = float(row["iqr"])
            val = out.at[idx, f]
            if pd.isna(val):
                continue
            val = float(pd.to_numeric(val, errors="coerce"))
            out.at[idx, f"{REP_PREFIX}{f}__median"] = med
            out.at[idx, f"{REP_PREFIX}{f}__q25"] = row["q25"]
            out.at[idx, f"{REP_PREFIX}{f}__q75"] = row["q75"]
            out.at[idx, f"{REP_PREFIX}{f}__iqr"] = iqr_val
            out.at[idx, f"{REP_PREFIX}{f}__n_valid"] = row["n_valid"]
            out.at[idx, f"{DEV_PREFIX}{f}__abs"] = val - med
            if med != 0:
                out.at[idx, f"{DEV_PREFIX}{f}__pct"] = (val - med) / abs(med)
            if iqr_val > 0:
                out.at[idx, f"{DEV_PREFIX}{f}__robust_z"] = (val - med) / iqr_val

    return out.copy()


def print_group_a_add_mode_debug_summary(df: pd.DataFrame) -> None:
    """Print counts for _ADD_ tag rows: modes, a_add_debug_reason, a_added_count > 0."""
    print("Group A add-mode debug summary:")
    if df.empty:
        print("  total rows: 0")
        return
    print(f"  total rows: {len(df)}")
    if "group_a" not in df.columns:
        print("  (no group_a column)")
        return
    sub = df.loc[df["group_a"].astype(str).str.contains("_ADD_", na=False)].copy()
    print(f"  add-tag rows: {len(sub)}")
    if sub.empty:
        return
    if "group_a_mode" in sub.columns:
        print("  canonical modes:")
        for m, c in sub["group_a_mode"].value_counts().sort_index().items():
            print(f"    {m}: {c}")
    if "a_add_debug_reason" in sub.columns:
        print("  debug reasons:")
        for r, c in sub["a_add_debug_reason"].value_counts(dropna=False).sort_index().items():
            print(f"    {r}: {c}")
    if "a_added_count" in sub.columns:
        ac = pd.to_numeric(sub["a_added_count"], errors="coerce").fillna(0)
        print(f"  rows with a_added_count > 0: {int((ac > 0).sum())}")


def print_group_a_similarity_factor_debug(base: pd.DataFrame, snapshot_df: pd.DataFrame) -> None:
    """Log which SIMILARITY_FACTORS_A columns exist on base; value_counts on add-tag rows."""
    present = [c for c in SIMILARITY_FACTORS_A if c in base.columns]
    print("Group A similarity factors present in base:", present)
    if "group_a" not in snapshot_df.columns:
        print("a_add_similarity_factors_used (add rows): (no group_a)")
        print("a_add_debug_reason (add rows): (no group_a)")
        return
    add_sub = snapshot_df.loc[snapshot_df["group_a"].astype(str).str.contains("_ADD_", na=False)]
    print(f"Group A add-tag rows for similarity debug: {len(add_sub)}")
    if "a_add_similarity_factors_used" in add_sub.columns:
        print("a_add_similarity_factors_used (add rows):")
        print(add_sub["a_add_similarity_factors_used"].value_counts(dropna=False).to_string())
    else:
        print("a_add_similarity_factors_used (add rows): (column missing)")
    if "a_add_debug_reason" in add_sub.columns:
        print("a_add_debug_reason (add rows):")
        print(add_sub["a_add_debug_reason"].value_counts(dropna=False).to_string())
    else:
        print("a_add_debug_reason (add rows): (column missing)")
