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

# Group A sector-add: similarity factors and minimum peer count.
SIMILARITY_FACTORS_A = ["op_margin", "roic", "debt_to_equity"]
MIN_A_PEER_COUNT = 12

# factors_latest display names -> SIMILARITY_FACTORS_A (applied once on merged base for Group A).
_GROUP_A_SIMILARITY_ALIASES: tuple[tuple[str, str], ...] = (
    ("Oper. Margin", "op_margin"),
    ("ROIC", "roic"),
    ("Debt/Eq", "debt_to_equity"),
)


def _apply_group_a_similarity_factor_aliases(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure op_margin / roic / debt_to_equity exist from factors_latest column names.
    If standard name already present, skip; else copy from source and drop source (rename).
    """
    out = df.copy()
    drop_src: list[str] = []
    for src, dst in _GROUP_A_SIMILARITY_ALIASES:
        if dst in out.columns:
            continue
        if src not in out.columns:
            continue
        out[dst] = out[src]
        drop_src.append(src)
    if drop_src:
        out = out.drop(columns=[c for c in drop_src if c in out.columns])
    return out

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
    tags["as_of_date"] = pd.to_datetime(tags["as_of_date"], errors="coerce")
    tags = tags.dropna(subset=["as_of_date"])
    if tags.empty:
        raise ValueError("No valid as_of_date in tags file.")

    latest_dt = tags["as_of_date"].max()
    latest = tags[tags["as_of_date"] == latest_dt].copy()
    latest["as_of_date"] = latest["as_of_date"].dt.strftime("%Y-%m-%d")
    as_of_date_str = latest["as_of_date"].iloc[0]
    return latest, as_of_date_str


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
    tags["as_of_date"] = pd.to_datetime(tags["as_of_date"], errors="coerce")
    tags = tags.dropna(subset=["as_of_date"])
    if tags.empty:
        raise ValueError("No valid as_of_date in tags file.")

    latest_dt = tags["as_of_date"].max()
    latest = tags[tags["as_of_date"] == latest_dt].copy()
    latest["as_of_date"] = latest["as_of_date"].dt.strftime("%Y-%m-%d")
    as_of_date_str = latest["as_of_date"].iloc[0]
    return latest, as_of_date_str


def load_factors_latest(data_dir: str | Path) -> pd.DataFrame:
    """Load factors_latest: CSV first, then parquet. Symbol normalized to str.upper().strip()."""
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
    return factors


def load_latest_tags_and_factors(
    logic_dir: str | Path = DEFAULT_LOGIC_DIR,
    data_dir: str | Path = DEFAULT_DATA_DIR,
) -> tuple[pd.DataFrame, str]:
    """
    Load latest group tags (from history or fallback date-stamped file) and factors_latest;
    normalize symbol, dedupe keep last, inner join.
    Returns (merged_df, as_of_date_str, tags_path_used).
    """
    logic_dir = Path(logic_dir)
    data_dir = Path(data_dir)
    if not logic_dir.exists():
        raise FileNotFoundError(f"Logic dir not found: {logic_dir}")

    tags_path = _resolve_latest_tags_path(logic_dir)
    latest_tags, as_of_date_str = _load_tags_from_path(tags_path)

    factors = load_factors_latest(data_dir)
    factors = factors.drop_duplicates(subset=["symbol"], keep="last")
    latest_tags["symbol"] = latest_tags["symbol"].astype(str).str.strip().str.upper()
    latest_tags = latest_tags.drop_duplicates(subset=["symbol"], keep="last")

    base = latest_tags.merge(factors, on="symbol", how="inner")
    return base.sort_values("symbol").reset_index(drop=True), as_of_date_str, tags_path


def load_latest_tags_and_factors_for_a(
    logic_dir: str | Path = DEFAULT_LOGIC_DIR,
    data_dir: str | Path = DEFAULT_DATA_DIR,
) -> tuple[pd.DataFrame, str, Path]:
    """
    Load latest group tags (Group A columns: group_a, group_a_mode, etc.) and factors_latest;
    inner join on symbol. Symbol normalized. Returns (merged_df, as_of_date_str, tags_path).
    """
    logic_dir = Path(logic_dir)
    data_dir = Path(data_dir)
    if not logic_dir.exists():
        raise FileNotFoundError(f"Logic dir not found: {logic_dir}")

    tags_path = _resolve_latest_tags_path(logic_dir)
    latest_tags, as_of_date_str = _load_tags_from_path_for_a(tags_path)

    factors = load_factors_latest(data_dir)
    factors = factors.drop_duplicates(subset=["symbol"], keep="last")
    latest_tags["symbol"] = latest_tags["symbol"].astype(str).str.strip().str.upper()
    latest_tags = latest_tags.drop_duplicates(subset=["symbol"], keep="last")

    base = latest_tags.merge(factors, on="symbol", how="inner")
    base = _apply_group_a_similarity_factor_aliases(base)
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
    return sorted(cols)


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

    for f in factor_cols:
        out[f"{REP_PREFIX}{f}__median"] = np.nan
        out[f"{REP_PREFIX}{f}__q25"] = np.nan
        out[f"{REP_PREFIX}{f}__q75"] = np.nan
        out[f"{REP_PREFIX}{f}__iqr"] = np.nan
        out[f"{REP_PREFIX}{f}__n_valid"] = np.nan
        out[f"{DEV_PREFIX}{f}__abs"] = np.nan
        out[f"{DEV_PREFIX}{f}__pct"] = np.nan
        out[f"{DEV_PREFIX}{f}__robust_z"] = np.nan

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

    return out


# ---------------------------------------------------------------------------
# Group A: mode-based peer sets and representatives
# ---------------------------------------------------------------------------


def get_factor_columns_for_a(df: pd.DataFrame) -> list[str]:
    """Return factor columns for Group A (exclude EXCLUDE_COLUMNS_A). All numeric columns for rep/dev; distinct from similarity factors."""
    return get_factor_columns(df, exclude_cols=EXCLUDE_COLUMNS_A)


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
    for f in factor_cols:
        out[f"{REP_PREFIX}{f}__median"] = np.nan
        out[f"{REP_PREFIX}{f}__q25"] = np.nan
        out[f"{REP_PREFIX}{f}__q75"] = np.nan
        out[f"{REP_PREFIX}{f}__iqr"] = np.nan
        out[f"{REP_PREFIX}{f}__n_valid"] = np.nan
        out[f"{DEV_PREFIX}{f}__abs"] = np.nan
        out[f"{DEV_PREFIX}{f}__pct"] = np.nan
        out[f"{DEV_PREFIX}{f}__robust_z"] = np.nan

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
        for c in meta_cols:
            out[c] = pd.Series(index=out.index, dtype=object)
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

    return out


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
