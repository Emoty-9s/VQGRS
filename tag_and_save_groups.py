"""
Tag universe_current with Group A/B (placeholder) and C/D/E; save to CSV and Parquet.

data/       = raw input (universe_current, factors_latest, prices_eod, financials_quarterly).
logic_data/ = logic outputs: merged_features_YYYYMMDD, group_tags_YYYYMMDD (CSV + Parquet).
"""
from __future__ import annotations

import warnings
from datetime import date
from pathlib import Path

import re
import numpy as np
import pandas as pd

# ─── Constants ─────────────────────────────────────────────────────────

GROUP_A_PLACEHOLDER = "A_PENDING"
GROUP_B_PLACEHOLDER = "B_PENDING"

# Group B peer-count thresholds and tolerances
GROUP_B_BASE_TOLERANCE = 0.40
GROUP_B_RISK_TOLERANCE_START = 0.40
GROUP_B_RISK_TOLERANCE_END = 0.65
GROUP_B_RISK_TOLERANCE_STEP = 0.05
GROUP_B_NORMAL_MIN_PEERS = 50
GROUP_B_RISK_MIN_PEERS = 20
GROUP_B_RELAXED_TARGET_MIN_PEERS = 15

C_HYPER_GROWTH_MIN = 20.0
C_PROFITABLE_GROWTH_MIN = 8.0
C_PROFITABLE_GROWTH_MAX = 20.0
C_STABLE_GROWTH_MAX = 8.0
C_OPM_GROWTH_MIN = 5.0
C_OPM_STABLE_MIN = 8.0

D_DEBT_STRESSED_HIGH = 200.0
D_DEBT_STRESSED_MED = 120.0
D_INTEREST_STRESSED = 1.5
D_LIQUIDITY_TIGHT = 1.0
D_FORTRESS_DEBT = 30.0
D_FORTRESS_CURRENT = 2.0
D_FORTRESS_COVERAGE = 8.0
D_STRONG_DEBT = 50.0
D_STRONG_CURRENT = 1.5
D_STRONG_COVERAGE = 5.0
D_LEVERAGED_DEBT_LOW = 120.0
D_LEVERAGED_DEBT_HIGH = 200.0
D_NORMAL_DEBT = 120.0
D_NORMAL_COVERAGE = 3.0
D_INTEREST_CAP = 50.0

E_VOL_LOW_PCT = 0.40
E_VOL_HIGH_PCT = 0.80
E_SPEC_BETA = 1.3
E_SPEC_RSI = 70.0
E_SPEC_VOL_RATIO = 1.5
E_HIGH_BETA = 1.2
E_HIGH_BETA_RSI = 55.0
E_DEFENSIVE_BETA = 0.85
E_DEFENSIVE_RSI = 45.0
E_WEAK_RSI = 45.0
E_UPTREND_RSI = 50.0
E_STABLE_BETA_LOW = 0.85
E_STABLE_BETA_HIGH = 1.2
E_STABLE_RSI_LOW = 45.0
E_STABLE_RSI_HIGH = 60.0
E_VALID_COUNT_MIN = 2

REQUIRED_COLS = ["symbol"]
GROUP_C_SOURCE_COLS = [
    "revenue_ttm", "revenue_prev_ttm", "operating_income_ttm", "ocf_ttm",
]
GROUP_D_SOURCE_COLS = [
    "total_debt", "total_equity", "current_assets", "current_liabilities",
    "ebit_ttm", "interest_expense_ttm", "ocf_ttm",
]
GROUP_E_SOURCE_COLS = [
    "beta", "daily_return_vol_252d", "momentum_3m", "rsi_14",
    "volume", "avg_volume_20d",
]
GROUP_B_SOURCE_COLS = [
    "market_cap", "revenue_ttm", "total_assets",
]
ALL_OPTIONAL_COLS = list(
    dict.fromkeys(
        GROUP_C_SOURCE_COLS
        + GROUP_D_SOURCE_COLS
        + GROUP_E_SOURCE_COLS
        + GROUP_B_SOURCE_COLS
    )
)

# Candidate original column names per standard name (first existing wins)
COLUMN_CANDIDATES = {
    "revenue_ttm": ["Sales (Rev)", "revenue_ttm", "revenue_ltm", "revenue"],
    "revenue_prev_ttm": ["revenue_prev_ttm", "revenue_ttm_prev", "sales_prev_ttm"],
    "operating_income_ttm": ["operating_income_ttm", "operatingIncome_ttm", "op_income_ttm"],
    "ocf_ttm": ["ocf_ttm", "operatingCashFlow_ttm", "operating_cash_flow_ttm", "operatingCashFlow"],
    "total_debt": ["totalDebt", "total_debt", "Total Debt", "total_debt_ttm"],
    "total_equity": ["totalStockholdersEquity", "total_equity", "Total Equity", "total_equity_ttm"],
    "debt_ratio": ["Debt/Eq", "debt_ratio", "debt_to_equity"],
    "current_assets": ["currentAssets", "current_assets", "Current Assets"],
    "current_liabilities": ["currentLiabilities", "current_liabilities", "Current Liabilities"],
    "ebit_ttm": ["ebit_ttm", "EBITDA", "incomeBeforeTax", "ebit_ltm"],
    "interest_expense_ttm": ["interest_expense_ttm", "interestExpense_ttm", "interest_expense"],
    "interest_coverage": ["Interest Coverage", "interest_coverage", "interest_coverage_ratio"],
    "beta": ["Beta", "beta"],
    "daily_return_vol_252d": ["Volatility", "daily_return_vol_252d", "price_volatility", "volatility_252d"],
    "momentum_3m": ["Perf Quarter", "momentum_3m", "Perf Quarter YTD", "momentum_3m_pct"],
    "rsi_14": ["RSI(14)", "rsi_14", "rsi"],
    "volume": ["Volume", "volume"],
    "avg_volume_20d": ["Avg Volume", "avg_volume_20d", "avg_volume", "avgVolume20d"],
    "market_cap": ["market_cap", "Market Cap", "marketCap", "market_capitalization"],
    "total_assets": ["total_assets", "totalAssets", "Total Assets"],
}

IMPORTANT_COLS = [
    "revenue_ttm", "revenue_prev_ttm", "operating_income_ttm", "ocf_ttm",
    "total_debt", "total_equity", "current_assets", "current_liabilities",
    "ebit_ttm", "interest_expense_ttm", "interest_coverage",
    "beta", "daily_return_vol_252d", "momentum_3m", "rsi_14",
    "volume", "avg_volume_20d",
]


def print_columns(name: str, df: pd.DataFrame) -> None:
    print(f"[{name}] columns: {df.columns.tolist()}")


def load_data_sources(data_dir: str = "data") -> dict[str, pd.DataFrame]:
    """Load universe_current, factors_latest, prices_eod, financials_quarterly from data_dir. Missing files yield empty DataFrame."""
    base = Path(data_dir)
    sources: dict[str, pd.DataFrame] = {}

    for key, candidates in [
        ("universe_current", ["universe_current.parquet", "universe_current.csv"]),
        ("factors_latest", ["factors_latest.csv", "factors_latest.parquet"]),
        ("prices_eod", ["prices_eod.csv", "prices_eod.parquet"]),
        ("financials_quarterly", ["financials_quarterly.csv", "financials_quarterly.parquet"]),
    ]:
        df = pd.DataFrame()
        for f in candidates:
            p = base / f
            if p.exists():
                if p.suffix.lower() == ".csv":
                    df = pd.read_csv(p, low_memory=False)
                else:
                    df = pd.read_parquet(p)
                break
        sources[key] = df
        if not df.empty:
            print_columns(key, df)

    return sources


def standardize_symbol_column(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize symbol to str, strip, upper. Require 'symbol' column."""
    out = df.copy()
    if "symbol" not in out.columns:
        return out
    out["symbol"] = out["symbol"].astype(str).str.strip().str.upper()
    return out


def extract_standard_columns(df: pd.DataFrame, source_name: str) -> pd.DataFrame:
    """From df, pick first existing candidate per standard name; return one row per symbol (last), with symbol + standard columns only."""
    if df.empty or "symbol" not in df.columns:
        return pd.DataFrame()
    out = standardize_symbol_column(df)
    found = {}
    for std_name, candidates in COLUMN_CANDIDATES.items():
        for c in candidates:
            if c in out.columns:
                found[std_name] = c
                break
    if not found:
        out = out.drop_duplicates(subset=["symbol"], keep="last").reset_index(drop=True)
        return out[["symbol"]].copy()
    result = out[["symbol"]].copy()
    for std_name, orig in found.items():
        result[std_name] = out[orig].values
    result = result.drop_duplicates(subset=["symbol"], keep="last").reset_index(drop=True)
    return result


def build_ttm_from_financials_quarterly(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate quarterly financials to one row per symbol: TTM for income/OCF, latest quarter for balance sheet."""
    if df.empty or "symbol" not in df.columns:
        return pd.DataFrame()
    out = standardize_symbol_column(df)
    if "fiscalDate" in out.columns:
        out["fiscalDate"] = pd.to_datetime(out["fiscalDate"], errors="coerce")
        out = out.sort_values(["symbol", "fiscalDate"])
    else:
        out = out.sort_values("symbol")
    rev_col = next((c for c in ["revenue", "revenue_ttm"] if c in out.columns), None)
    oi_col = next((c for c in ["operatingIncome", "operating_income_ttm"] if c in out.columns), None)
    ocf_col = next((c for c in ["operatingCashFlow", "ocf_ttm"] if c in out.columns), None)
    rows = []
    for symbol, g in out.groupby("symbol"):
        g = g.dropna(subset=["fiscalDate"]) if "fiscalDate" in g.columns else g
        if g.empty:
            rows.append({"symbol": symbol})
            continue
        g = g.sort_values("fiscalDate", ascending=True)
        r = {"symbol": symbol}
        if rev_col and len(g) >= 4:
            r["revenue_ttm"] = g[rev_col].iloc[-4:].sum()
            if len(g) >= 8:
                r["revenue_prev_ttm"] = g[rev_col].iloc[-8:-4].sum()
        if oi_col and len(g) >= 4:
            r["operating_income_ttm"] = g[oi_col].iloc[-4:].sum()
        if ocf_col and len(g) >= 4:
            r["ocf_ttm"] = g[ocf_col].iloc[-4:].sum()
        for std, col in [
            ("total_debt", "totalDebt"), ("total_equity", "totalStockholdersEquity"),
            ("current_assets", "currentAssets"), ("current_liabilities", "currentLiabilities"),
        ]:
            if col in g.columns:
                r[std] = g[col].iloc[-1]
        rows.append(r)
    return pd.DataFrame(rows)


def _left_merge_fill(merged: pd.DataFrame, right: pd.DataFrame, suffix: str) -> pd.DataFrame:
    """Left merge right into merged on symbol; for overlapping columns fill only where merged is NaN."""
    if right.empty or "symbol" not in right.columns:
        return merged
    cols_right = [c for c in right.columns if c != "symbol"]
    if not cols_right:
        return merged
    right = right[["symbol"] + cols_right].copy()
    merged = merged.merge(right, on="symbol", how="left", suffixes=("", f"_{suffix}"))
    for c in cols_right:
        c_suf = f"{c}_{suffix}"
        if c_suf in merged.columns:
            merged[c] = merged[c].fillna(merged[c_suf])
            merged = merged.drop(columns=[c_suf], errors="ignore")
    return merged


def merge_feature_sources(universe_current: pd.DataFrame, sources: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Left-join all sources onto universe_current on symbol. Standardize symbol; keep all universe rows."""
    merged = standardize_symbol_column(universe_current.copy())
    if merged.empty:
        return merged

    # 1) factors_latest (highest priority)
    factors = sources.get("factors_latest", pd.DataFrame())
    if not factors.empty:
        f = extract_standard_columns(factors, "factors_latest")
        if not f.empty:
            merged = _left_merge_fill(merged, f, "fl")

    # 2) financials_quarterly -> TTM
    fin = sources.get("financials_quarterly", pd.DataFrame())
    if not fin.empty:
        ft = build_ttm_from_financials_quarterly(fin)
        if not ft.empty:
            merged = _left_merge_fill(merged, ft, "fq")

    # 3) prices_eod (last row per symbol)
    prices = sources.get("prices_eod", pd.DataFrame())
    if not prices.empty and "symbol" in prices.columns:
        p = standardize_symbol_column(prices)
        date_col = "date" if "date" in p.columns else next((c for c in p.columns if "date" in c.lower()), None)
        if date_col:
            p[date_col] = pd.to_datetime(p[date_col], errors="coerce")
            p = p.sort_values(date_col).drop_duplicates(subset=["symbol"], keep="last")
        else:
            p = p.drop_duplicates(subset=["symbol"], keep="last")
        p_ext = extract_standard_columns(p, "prices_eod")
        if not p_ext.empty:
            merged = _left_merge_fill(merged, p_ext, "pe")

    # Derive revenue_prev_ttm from Revenue YoY + revenue_ttm if missing
    rev_yoy_col = next((c for c in merged.columns if "revenue" in c.lower() and "yoy" in c.lower()), None)
    if rev_yoy_col is not None and "revenue_ttm" in merged.columns:
        rev_ttm = pd.to_numeric(merged["revenue_ttm"], errors="coerce")
        yoy = pd.to_numeric(merged[rev_yoy_col], errors="coerce")
        prev = np.where(yoy.notna() & (yoy != -100), rev_ttm / (1 + yoy / 100.0), np.nan)
        if "revenue_prev_ttm" not in merged.columns:
            merged["revenue_prev_ttm"] = prev
        else:
            merged["revenue_prev_ttm"] = merged["revenue_prev_ttm"].fillna(pd.Series(prev, index=merged.index))
    if "operating_income_ttm" not in merged.columns and "revenue_ttm" in merged.columns:
        opm_col = next((c for c in merged.columns if "margin" in c.lower()), None)
        if opm_col is not None:
            merged["operating_income_ttm"] = pd.to_numeric(merged["revenue_ttm"], errors="coerce") * pd.to_numeric(merged[opm_col], errors="coerce") / 100.0

    # Debt/Eq from factors is typically decimal (e.g. 1.04); our debt_ratio expects percentage (104)
    if "debt_ratio" in merged.columns:
        dr = pd.to_numeric(merged["debt_ratio"], errors="coerce")
        if dr.notna().any() and dr.max() < 100:
            merged["debt_ratio"] = merged["debt_ratio"] * 100.0

    return merged


def ensure_columns(df: pd.DataFrame, required_cols: list[str]) -> pd.DataFrame:
    out = df.copy()
    all_cols = list(set(required_cols + ALL_OPTIONAL_COLS))
    for col in all_cols:
        if col not in out.columns:
            out[col] = np.nan
    return out


def coerce_numeric_columns(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    out = df.copy()
    for col in cols:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    return out


def compute_group_c_features(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    rev_ttm = out["revenue_ttm"]
    rev_prev = out["revenue_prev_ttm"]
    oi = out["operating_income_ttm"]
    rev_prev_valid = rev_prev.notna() & (rev_prev > 0)
    revenue_yoy = np.where(rev_prev_valid, ((rev_ttm - rev_prev) / rev_prev) * 100.0, np.nan)
    out["revenue_yoy"] = revenue_yoy
    rev_valid = rev_ttm.notna() & (rev_ttm > 0)
    opm = np.where(rev_valid, (oi / rev_ttm) * 100.0, np.nan)
    out["opm"] = opm
    return out


def compute_group_d_features(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    existing_debt_ratio = out.get("debt_ratio")
    existing_current_ratio = out.get("current_ratio")
    existing_interest_coverage = out.get("interest_coverage")
    td, te = out["total_debt"], out["total_equity"]
    ca, cl = out["current_assets"], out["current_liabilities"]
    ebit, ie = out["ebit_ttm"], out["interest_expense_ttm"]
    te_valid = te.notna() & (te > 0)
    debt_ratio = np.where(te_valid, (td / te) * 100.0, np.nan)
    te_nonpos = te.notna() & (te <= 0)
    debt_ratio = np.where(te_nonpos, np.inf, debt_ratio)
    out["debt_ratio"] = debt_ratio
    if existing_debt_ratio is not None:
        out["debt_ratio"] = out["debt_ratio"].fillna(pd.to_numeric(existing_debt_ratio, errors="coerce"))
    out["debt_ratio_inf_flag"] = te_nonpos
    cl_valid = cl.notna() & (cl > 0)
    current_ratio = np.where(cl_valid, ca / cl, np.nan)
    cl_nonpos = cl.notna() & (cl <= 0)
    current_ratio = np.where(cl_nonpos, np.inf, current_ratio)
    out["current_ratio"] = current_ratio
    if existing_current_ratio is not None:
        out["current_ratio"] = out["current_ratio"].fillna(pd.to_numeric(existing_current_ratio, errors="coerce"))
    out["current_ratio_inf_flag"] = cl_nonpos
    ie_valid = ie.notna() & (ie > 0)
    raw_cov = np.where(ie_valid, ebit / ie, np.nan)
    ie_zero_or_neg = ie.notna() & (ie <= 0)
    cov = np.where(ie_zero_or_neg, D_INTEREST_CAP, raw_cov)
    out["interest_coverage"] = np.where(np.isnan(cov), np.nan, np.minimum(cov, D_INTEREST_CAP))
    if existing_interest_coverage is not None:
        out["interest_coverage"] = out["interest_coverage"].fillna(pd.to_numeric(existing_interest_coverage, errors="coerce"))
    out["interest_coverage_capped_flag"] = ie_zero_or_neg | (ie_valid & (raw_cov >= D_INTEREST_CAP))
    return out


def compute_group_e_features(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    vol, avg_vol = out["volume"], out["avg_volume_20d"]
    avg_valid = avg_vol.notna() & (avg_vol > 0)
    out["volume_ratio"] = np.where(avg_valid, vol / avg_vol, np.nan)
    dvol = out["daily_return_vol_252d"]
    valid_vol_n = int(dvol.notna().sum())
    if valid_vol_n < 5:
        out["vol_pct_rank"] = np.nan
        out["vol_low"] = False
        out["vol_mid"] = False
        out["vol_high"] = False
        return out
    vol_pct_rank = dvol.rank(pct=True)
    out["vol_pct_rank"] = vol_pct_rank
    out["vol_low"] = vol_pct_rank <= E_VOL_LOW_PCT
    out["vol_high"] = vol_pct_rank >= E_VOL_HIGH_PCT
    out["vol_mid"] = (vol_pct_rank > E_VOL_LOW_PCT) & (vol_pct_rank < E_VOL_HIGH_PCT)
    return out


def normalize_group_a_text(value: object) -> str:
    """Normalize sector/industry text for Group A tags."""
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return ""
    text = str(value).strip()
    if not text:
        return ""
    text = text.upper()
    # Replace common separators and special chars with underscore
    text = re.sub(r"[\/&,\-\(\)]+", "_", text)
    # Normalize whitespace to single underscore
    text = re.sub(r"\s+", "_", text)
    # Collapse multiple underscores and trim
    text = re.sub(r"_+", "_", text).strip("_")
    return text


def compute_group_a_counts(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute per-row industry_count and sector_count for Group A.
    Counts are based on normalized non-empty industry/sector across the full universe.
    """
    out = df.copy()
    raw_industry = out.get("industry")
    raw_sector = out.get("sector")
    out["group_a_industry"] = (
        raw_industry.apply(normalize_group_a_text) if raw_industry is not None else ""
    )
    out["group_a_sector"] = (
        raw_sector.apply(normalize_group_a_text) if raw_sector is not None else ""
    )

    ind_non_empty = out["group_a_industry"] != ""
    sec_non_empty = out["group_a_sector"] != ""
    industry_counts = out.loc[ind_non_empty, "group_a_industry"].value_counts()
    sector_counts = out.loc[sec_non_empty, "group_a_sector"].value_counts()

    out["group_a_industry_count"] = (
        out["group_a_industry"].map(industry_counts).fillna(0).astype(int)
    )
    out["group_a_sector_count"] = (
        out["group_a_sector"].map(sector_counts).fillna(0).astype(int)
    )
    return out


def assign_group_a_tags(df: pd.DataFrame) -> pd.DataFrame:
    """
    Assign Group A tags using industry/sector availability and counts.

    Group A is tag-only at this stage (no benchmark/peer selection).
    Rules:
      - A_{INDUSTRY} when industry_count >= 12
      - A_{INDUSTRY}_Add_{SECTOR} when industry_count < 12 and sector_count >= 12
      - A_Total_Market otherwise (missing industry/sector or both counts < 12)
    """
    out = compute_group_a_counts(df)

    gi = out["group_a_industry"]
    gs = out["group_a_sector"]
    ic = out["group_a_industry_count"]
    sc = out["group_a_sector_count"]

    tags: list[str] = []
    modes: list[str] = []
    for ind, sec, icnt, scnt in zip(gi, gs, ic, sc):
        if not ind or not sec:
            tags.append("A_Total_Market")
            modes.append("TOTAL_MARKET")
            continue
        if icnt >= 12:
            tags.append(f"A_{ind}")
            modes.append("INDUSTRY_ONLY")
            continue
        if icnt < 12 and scnt >= 12:
            tags.append(f"A_{ind}_Add_{sec}")
            modes.append("INDUSTRY_ADD_SECTOR")
            continue
        tags.append("A_Total_Market")
        modes.append("TOTAL_MARKET")

    out["group_a"] = tags
    out["group_a_mode"] = modes
    return out


def compute_group_b_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute Group B validity flags, mode, composite size score,
    and component weights after re-normalization.
    market_cap is mandatory for any valid Group B calculation.
    """
    out = df.copy()

    mcap = pd.to_numeric(out.get("market_cap"), errors="coerce")
    rev = pd.to_numeric(out.get("revenue_ttm"), errors="coerce")
    assets = pd.to_numeric(out.get("total_assets"), errors="coerce")

    mcap_valid = mcap.notna() & (mcap > 0)
    rev_valid = rev.notna() & (rev > 0)
    assets_valid = assets.notna() & (assets > 0)

    out["group_b_market_cap_valid"] = mcap_valid
    out["group_b_revenue_valid"] = rev_valid
    out["group_b_assets_valid"] = assets_valid

    mode = np.full(len(out), "UNKNOWN", dtype=object)
    mode[mcap_valid & rev_valid & assets_valid] = "MCAP_REV_ASSETS"
    mode[mcap_valid & rev_valid & ~assets_valid] = "MCAP_REV"
    mode[mcap_valid & ~rev_valid & assets_valid] = "MCAP_ASSETS"
    mode[mcap_valid & ~rev_valid & ~assets_valid] = "MCAP_ONLY"
    out["group_b_mode"] = mode

    w_mcap_base = 0.5
    w_rev_base = 0.3
    w_assets_base = 0.2

    log_mcap = np.where(mcap_valid, np.log(mcap), np.nan)
    log_rev = np.where(rev_valid, np.log(rev), np.nan)
    log_assets = np.where(assets_valid, np.log(assets), np.nan)

    denom = (
        mcap_valid.astype(float) * w_mcap_base
        + rev_valid.astype(float) * w_rev_base
        + assets_valid.astype(float) * w_assets_base
    )
    denom_zero = denom <= 0

    num = (
        np.nan_to_num(log_mcap) * w_mcap_base
        + np.nan_to_num(log_rev) * w_rev_base
        + np.nan_to_num(log_assets) * w_assets_base
    )

    size_score = np.where(~denom_zero, num / denom, np.nan)
    out["group_b_size_score"] = size_score

    w_mcap = np.where(mcap_valid & ~denom_zero, w_mcap_base / denom, 0.0)
    w_rev = np.where(rev_valid & ~denom_zero, w_rev_base / denom, 0.0)
    w_assets = np.where(assets_valid & ~denom_zero, w_assets_base / denom, 0.0)

    out["group_b_weight_mcap"] = w_mcap
    out["group_b_weight_revenue"] = w_rev
    out["group_b_weight_assets"] = w_assets

    valid_components: list[str] = []
    for mv, rv, av in zip(mcap_valid, rev_valid, assets_valid):
        parts = []
        if mv:
            parts.append("MCAP")
        if rv:
            parts.append("REVENUE")
        if av:
            parts.append("ASSETS")
        valid_components.append(",".join(parts))
    out["group_b_valid_components"] = valid_components

    return out


def build_group_b_no_market_cap_peer_pool(df: pd.DataFrame) -> set[str]:
    """
    Build a common fallback peer pool for B_NO_MARKET_CAP:
    - Use only rows with valid market_cap (>0) and non-empty sector
    - For each sector, sort by market_cap asc and pick:
      - smallest (idx 0), median position, largest (idx -1)
      - unique within sector and across all sectors
    """
    if df.empty or "symbol" not in df.columns:
        return set()
    if "market_cap" not in df.columns or "sector" not in df.columns:
        return set()

    mcap = pd.to_numeric(df["market_cap"], errors="coerce")
    sec = df["sector"].astype(str).str.strip()
    valid = mcap.notna() & (mcap > 0) & (sec != "")
    if not valid.any():
        return set()

    base = df.loc[valid, ["symbol", "sector"]].copy()
    base["market_cap"] = mcap.loc[valid].values
    base["symbol"] = base["symbol"].astype(str).str.strip().str.upper()

    pool: set[str] = set()
    for sector, g in base.groupby("sector", sort=False):
        g = g.sort_values("market_cap", ascending=True).reset_index(drop=True)
        n = len(g)
        if n <= 0:
            continue
        idxs = {0, n - 1, n // 2}
        for j in sorted(idxs):
            if 0 <= j < n:
                sym = str(g.at[j, "symbol"]).strip().upper()
                if sym:
                    pool.add(sym)
    return pool


def assign_group_b_tags(df: pd.DataFrame) -> pd.DataFrame:
    """
    Assign Group B tags using full-data peers first, then adjusted peers.
    This function must operate on the full DataFrame, not row-only apply.

    Group B tag names must be standardized to English-only labels:
    B_NO_MARKET_CAP, B_NORMAL, B_ADJUSTED, B_INSUFFICIENT, B_RISK.
    Logic:
      - market_cap invalid          -> B_NO_MARKET_CAP
      - full-data peers (MCAP_REV_ASSETS only) count >= GROUP_B_NORMAL_MIN_PEERS -> B_NORMAL
      - adjusted peers count >= GROUP_B_NORMAL_MIN_PEERS  -> B_ADJUSTED
      - adjusted peers GROUP_B_RISK_MIN_PEERS~(GROUP_B_NORMAL_MIN_PEERS-1) -> B_INSUFFICIENT
      - adjusted peers < GROUP_B_RISK_MIN_PEERS -> B_RISK (with relaxed re-check for B_RISK candidates only)
    """
    out = df.copy()

    def _risk_relaxed_search(d_adj: np.ndarray) -> tuple[int, float, bool]:
        """
        For B_RISK candidates only: search tolerances from START to END (step),
        stopping at the first tolerance where adjusted peers >= GROUP_B_RELAXED_TARGET_MIN_PEERS.
        If never meets target, use END result. Returns (count, final_tolerance, target_met).
        """
        # Explicit list with rounding to avoid floating point drift
        tol_list: list[float] = []
        t = GROUP_B_RISK_TOLERANCE_START
        while t <= GROUP_B_RISK_TOLERANCE_END + 1e-9:
            tol_list.append(round(float(t), 2))
            t += GROUP_B_RISK_TOLERANCE_STEP

        final_tol = tol_list[-1]
        final_count = int((d_adj <= final_tol).sum())
        target_met = final_count >= GROUP_B_RELAXED_TARGET_MIN_PEERS
        for tol in tol_list:
            c = int((d_adj <= tol).sum())
            if c >= GROUP_B_RELAXED_TARGET_MIN_PEERS:
                return c, tol, True
            final_tol = tol
            final_count = c
        return final_count, final_tol, target_met

    score = pd.to_numeric(out.get("group_b_size_score"), errors="coerce")
    mode = out.get("group_b_mode").astype(str)
    mcap_valid = out.get("group_b_market_cap_valid").fillna(False).astype(bool)

    n = len(out)
    tags = np.full(n, "B_NO_MARKET_CAP", dtype=object)
    full_counts = np.zeros(n, dtype=int)
    adj_counts = np.zeros(n, dtype=int)
    adj_counts_relaxed = np.zeros(n, dtype=int)
    relaxed_applied = np.zeros(n, dtype=bool)
    relaxed_final_tol = np.full(n, GROUP_B_BASE_TOLERANCE, dtype=float)
    relaxed_target_met = np.zeros(n, dtype=bool)
    # Final peer-set summary for representative calculations (does not affect group_b tags)
    final_peer_count = np.zeros(n, dtype=int)
    final_peer_method = np.full(n, "VERY_LOW", dtype=object)
    nearest_fill_added = np.zeros(n, dtype=int)
    peer_quality = np.full(n, "VERY_LOW", dtype=object)

    full_mask = (
        (mode == "MCAP_REV_ASSETS")
        & mcap_valid
        & score.notna()
    )
    adj_mask = (
        mode.isin(["MCAP_REV_ASSETS", "MCAP_REV", "MCAP_ASSETS", "MCAP_ONLY"])
        & mcap_valid
        & score.notna()
    )

    score_vals = score.to_numpy()

    for i in range(n):
        if not mcap_valid.iat[i] or np.isnan(score_vals[i]):
            tags[i] = "B_NO_MARKET_CAP"
            full_counts[i] = 0
            adj_counts[i] = 0
            adj_counts_relaxed[i] = 0
            relaxed_applied[i] = False
            relaxed_final_tol[i] = GROUP_B_BASE_TOLERANCE
            relaxed_target_met[i] = False
            final_peer_count[i] = 0
            final_peer_method[i] = "VERY_LOW"
            nearest_fill_added[i] = 0
            peer_quality[i] = "VERY_LOW"
            continue

        s_i = score_vals[i]

        # full peers (self 포함 허용)
        full_count_i = 0
        if mode.iat[i] == "MCAP_REV_ASSETS":
            mask_full_i = full_mask.to_numpy()
            d_full = np.abs(score_vals[mask_full_i] - s_i)
            full_count_i = int((d_full <= GROUP_B_BASE_TOLERANCE).sum())
        full_counts[i] = full_count_i

        if mode.iat[i] == "MCAP_REV_ASSETS" and full_count_i >= GROUP_B_NORMAL_MIN_PEERS:
            tags[i] = "B_NORMAL"
            adj_counts[i] = full_count_i
            adj_counts_relaxed[i] = full_count_i
            relaxed_applied[i] = False
            relaxed_final_tol[i] = GROUP_B_BASE_TOLERANCE
            relaxed_target_met[i] = True
            # BASE peer-set summary
            final_peer_count[i] = int(full_count_i)
            final_peer_method[i] = "BASE"
            nearest_fill_added[i] = 0
            peer_quality[i] = "HIGH"
            continue

        # adjusted peers
        mask_adj_i = adj_mask.to_numpy()
        d_adj = np.abs(score_vals[mask_adj_i] - s_i)
        adj_count_i = int((d_adj <= GROUP_B_BASE_TOLERANCE).sum())
        adj_counts[i] = adj_count_i
        adj_counts_relaxed[i] = adj_count_i
        relaxed_applied[i] = False
        relaxed_final_tol[i] = GROUP_B_BASE_TOLERANCE
        relaxed_target_met[i] = adj_count_i >= GROUP_B_RELAXED_TARGET_MIN_PEERS

        if adj_count_i >= GROUP_B_NORMAL_MIN_PEERS:
            tags[i] = "B_ADJUSTED"
        elif adj_count_i >= GROUP_B_RISK_MIN_PEERS:
            tags[i] = "B_INSUFFICIENT"
        else:
            # Only for B_RISK candidates: stepwise relaxed tolerance search.
            adj_count_relaxed_i, tol_used, target_met = _risk_relaxed_search(d_adj)
            adj_counts_relaxed[i] = adj_count_relaxed_i
            relaxed_applied[i] = True
            relaxed_final_tol[i] = tol_used
            relaxed_target_met[i] = target_met
            if adj_count_relaxed_i >= GROUP_B_RISK_MIN_PEERS:
                tags[i] = "B_INSUFFICIENT"
            else:
                tags[i] = "B_RISK"

        # Final peer-set summary (BASE → RELAXED → NEAREST_FILL)
        base_cnt = int(adj_count_i)
        rel_cnt = int(adj_counts_relaxed[i])
        tol_used = float(relaxed_final_tol[i])

        # 1) HIGH
        if base_cnt >= GROUP_B_RISK_MIN_PEERS:
            peer_quality[i] = "HIGH"
            final_peer_count[i] = base_cnt
            final_peer_method[i] = "BASE"
            nearest_fill_added[i] = 0
            continue

        # 2) MEDIUM (relaxed >= 15)
        if rel_cnt >= GROUP_B_RELAXED_TARGET_MIN_PEERS:
            peer_quality[i] = "MEDIUM"
            final_peer_count[i] = rel_cnt
            final_peer_method[i] = "RELAXED"
            nearest_fill_added[i] = 0
            continue

        # 3) NEAREST_FILL to target 15 using size_score distance only (no sector/industry)
        # Build relaxed membership mask in the adjusted universe
        in_relaxed = d_adj <= tol_used
        remaining_dist = d_adj[~in_relaxed]
        need = max(0, GROUP_B_RELAXED_TARGET_MIN_PEERS - rel_cnt)
        if need > 0 and remaining_dist.size > 0:
            # take closest distances first
            added = int(min(need, remaining_dist.size))
        else:
            added = 0
        final_cnt = rel_cnt + added
        nearest_fill_added[i] = added
        final_peer_count[i] = final_cnt

        if final_cnt < 10:
            peer_quality[i] = "VERY_LOW"
            final_peer_method[i] = "VERY_LOW"
        else:
            peer_quality[i] = "LOW"
            final_peer_method[i] = "NEAREST_FILL"

    out["group_b"] = tags
    out["group_b_full_peer_count"] = full_counts
    out["group_b_adjusted_peer_count"] = adj_counts
    out["group_b_adjusted_peer_count_relaxed"] = adj_counts_relaxed
    out["group_b_relaxed_applied"] = relaxed_applied
    out["group_b_relaxed_final_tolerance"] = relaxed_final_tol
    out["group_b_relaxed_target_met"] = relaxed_target_met
    out["group_b_final_peer_count"] = final_peer_count
    out["group_b_final_peer_method"] = final_peer_method
    out["group_b_nearest_fill_added"] = nearest_fill_added
    out["group_b_peer_quality"] = peer_quality

    # B_NO_MARKET_CAP fallback peer pool metadata (sector 3-points)
    pool = build_group_b_no_market_cap_peer_pool(out)
    pool_count = int(len(pool))
    no_mcap_count = np.zeros(n, dtype=int)
    no_mcap_method = np.full(n, "", dtype=object)
    no_mcap_quality = np.full(n, "", dtype=object)
    is_no_mcap = out["group_b"] == "B_NO_MARKET_CAP"
    if is_no_mcap.any():
        no_mcap_count[is_no_mcap.to_numpy()] = pool_count
        no_mcap_method[is_no_mcap.to_numpy()] = "SECTOR_3POINTS"
        if pool_count >= 15:
            q = "LOW"
        else:
            q = "VERY_LOW"
        no_mcap_quality[is_no_mcap.to_numpy()] = q
    out["group_b_no_mcap_peer_count"] = no_mcap_count
    out["group_b_no_mcap_peer_method"] = no_mcap_method
    out["group_b_no_mcap_peer_quality"] = no_mcap_quality
    return out


def assign_group_c(row: pd.Series) -> str:
    revenue_yoy = row.get("revenue_yoy", np.nan)
    opm = row.get("opm", np.nan)
    ocf_ttm = row.get("ocf_ttm", np.nan)
    if pd.isna(revenue_yoy):
        return "C8_DATA_LIMITED"
    if pd.isna(opm) and pd.isna(ocf_ttm):
        return "C8_DATA_LIMITED"
    ry = float(revenue_yoy)
    om = float(opm) if not pd.isna(opm) else np.nan
    oc = float(ocf_ttm) if not pd.isna(ocf_ttm) else np.nan
    if ry < 0:
        return "C7_DECLINING_WEAK"
    if not pd.isna(om) and not pd.isna(oc) and om < 0 and oc < 0:
        return "C7_DECLINING_WEAK"
    if ry >= C_HYPER_GROWTH_MIN:
        if (not pd.isna(om) and om < 0) or (not pd.isna(oc) and oc <= 0):
            return "C1_HYPER_GROWTH_BURN"
        if not pd.isna(om) and not pd.isna(oc) and om >= 0 and oc > 0:
            return "C2_HYPER_GROWTH_QUALITY"
        return "C6_MIXED_TRANSITION"
    if C_PROFITABLE_GROWTH_MIN <= ry < C_PROFITABLE_GROWTH_MAX:
        if not pd.isna(om) and not pd.isna(oc) and om >= C_OPM_GROWTH_MIN and oc > 0:
            return "C3_PROFITABLE_GROWTH"
        return "C6_MIXED_TRANSITION"
    if 0 <= ry < C_STABLE_GROWTH_MAX:
        if not pd.isna(om) and not pd.isna(oc) and om >= C_OPM_STABLE_MIN and oc > 0:
            return "C4_STABLE_CASH_GENERATOR"
        if not pd.isna(om) and not pd.isna(oc) and om >= 0 and oc > 0:
            return "C5_MATURE_LOW_GROWTH"
        return "C6_MIXED_TRANSITION"
    return "C6_MIXED_TRANSITION"


def assign_group_d(row: pd.Series) -> str:
    debt_ratio = row.get("debt_ratio", np.nan)
    current_ratio = row.get("current_ratio", np.nan)
    interest_coverage = row.get("interest_coverage", np.nan)
    total_equity = row.get("total_equity", np.nan)
    ocf_ttm = row.get("ocf_ttm", np.nan)
    valid_count = sum(1 for x in (debt_ratio, current_ratio, interest_coverage) if not pd.isna(x))
    if valid_count < 2:
        return "D9_DATA_LIMITED"
    dr = float(debt_ratio) if not pd.isna(debt_ratio) else np.nan
    cr = float(current_ratio) if not pd.isna(current_ratio) else np.nan
    ic = float(interest_coverage) if not pd.isna(interest_coverage) else np.nan
    te = float(total_equity) if not pd.isna(total_equity) else np.nan
    oc = float(ocf_ttm) if not pd.isna(ocf_ttm) else np.nan
    if not pd.isna(dr) and dr >= D_DEBT_STRESSED_HIGH:
        return "D7_DEBT_STRESSED"
    if not pd.isna(te) and te <= 0:
        return "D7_DEBT_STRESSED"
    if not pd.isna(dr) and not pd.isna(oc) and dr >= D_DEBT_STRESSED_MED and oc < 0:
        return "D7_DEBT_STRESSED"
    if not pd.isna(ic) and ic < D_INTEREST_STRESSED:
        return "D6_INTEREST_STRESSED"
    if not pd.isna(cr) and not pd.isna(ic) and cr < D_LIQUIDITY_TIGHT and ic >= D_INTEREST_STRESSED:
        return "D5_LIQUIDITY_TIGHT"
    if not pd.isna(dr) and not pd.isna(cr) and not pd.isna(ic):
        if dr < D_FORTRESS_DEBT and cr >= D_FORTRESS_CURRENT and ic >= D_FORTRESS_COVERAGE:
            return "D1_FORTRESS"
        if dr < D_STRONG_DEBT and cr >= D_STRONG_CURRENT and ic >= D_STRONG_COVERAGE:
            return "D2_STRONG"
        if D_LEVERAGED_DEBT_LOW <= dr < D_LEVERAGED_DEBT_HIGH and cr >= D_LIQUIDITY_TIGHT and ic >= D_INTEREST_STRESSED:
            return "D4_LEVERAGED_MANAGEABLE"
        if dr < D_NORMAL_DEBT and cr >= D_LIQUIDITY_TIGHT and ic >= D_NORMAL_COVERAGE:
            return "D3_NORMAL"
    return "D8_MIXED_TRANSITION"


def assign_group_e(row: pd.Series) -> str:
    beta = row.get("beta", np.nan)
    momentum_3m = row.get("momentum_3m", np.nan)
    rsi_14 = row.get("rsi_14", np.nan)
    volume_ratio = row.get("volume_ratio", np.nan)
    vol_low = row.get("vol_low", False)
    vol_mid = row.get("vol_mid", False)
    vol_high = row.get("vol_high", False)
    valid_count = sum(
        1 for x in (beta, row.get("daily_return_vol_252d", np.nan), momentum_3m, rsi_14)
        if not pd.isna(x)
    )
    if valid_count < E_VALID_COUNT_MIN:
        return "E9_DATA_LIMITED"
    b = float(beta) if not pd.isna(beta) else np.nan
    mom = float(momentum_3m) if not pd.isna(momentum_3m) else np.nan
    rsi = float(rsi_14) if not pd.isna(rsi_14) else np.nan
    vr = float(volume_ratio) if not pd.isna(volume_ratio) else np.nan
    if not pd.isna(b) and b > E_SPEC_BETA and vol_high:
        if (not pd.isna(rsi) and rsi >= E_SPEC_RSI) or (not pd.isna(vr) and vr >= E_SPEC_VOL_RATIO):
            return "E5_SPECULATIVE_SURGE"
    if vol_high and not pd.isna(mom) and mom < 0:
        return "E8_HIGH_VOL_WEAK"
    if not pd.isna(b) and b > E_HIGH_BETA and not pd.isna(mom) and mom > 0 and not pd.isna(rsi) and rsi >= E_HIGH_BETA_RSI:
        return "E4_HIGH_BETA_UPTREND"
    if not pd.isna(b) and b < E_DEFENSIVE_BETA and vol_low and not pd.isna(rsi) and rsi >= E_DEFENSIVE_RSI:
        return "E1_DEFENSIVE_LOWVOL"
    if not pd.isna(mom) and mom < 0 and not pd.isna(rsi) and rsi < E_WEAK_RSI:
        return "E7_WEAK_TREND"
    if not pd.isna(mom) and mom > 0 and not pd.isna(rsi) and rsi >= E_UPTREND_RSI and (pd.isna(b) or b <= E_HIGH_BETA):
        return "E3_UPTREND_CORE"
    if not pd.isna(b) and E_STABLE_BETA_LOW <= b <= E_STABLE_BETA_HIGH and vol_mid:
        if not pd.isna(rsi) and E_STABLE_RSI_LOW <= rsi <= E_STABLE_RSI_HIGH:
            return "E2_STABLE_CORE"
    return "E6_RANGE_BOUND_NEUTRAL"


def build_group_tags(universe_current: pd.DataFrame, return_debug: bool = False) -> pd.DataFrame:
    if "symbol" not in universe_current.columns:
        raise ValueError(
            "Input DataFrame must contain a 'symbol' column. "
            "Found columns: " + str(list(universe_current.columns))
        )
    if universe_current["symbol"].duplicated().any():
        n_dup = int(universe_current["symbol"].duplicated().sum())
        warnings.warn(
            f"Duplicate symbol(s) in universe_current: {n_dup} duplicate row(s). "
            "Row-level tagging still runs.",
            UserWarning,
            stacklevel=2,
        )
    df = ensure_columns(universe_current, REQUIRED_COLS)
    df = coerce_numeric_columns(df, ALL_OPTIONAL_COLS)

    # Group A: tag-only, based on sector/industry counts across the universe
    df = assign_group_a_tags(df)

    # Group B: market_cap 필수 기반 size-score 태그
    df = compute_group_b_features(df)
    df = assign_group_b_tags(df)

    # Group C/D/E
    df = compute_group_c_features(df)
    df = compute_group_d_features(df)
    df = compute_group_e_features(df)
    today = date.today().isoformat()
    df = df.copy()
    df["as_of_date"] = today
    df["group_c"] = df.apply(assign_group_c, axis=1)
    df["group_d"] = df.apply(assign_group_d, axis=1)
    df["group_e"] = df.apply(assign_group_e, axis=1)
    # Base output columns (always saved to group_tags_history.parquet / .csv)
    # Promote Group B peer-count fields from debug-only to base output.
    out_cols = [
        "symbol",
        "as_of_date",
        "group_a",
        "group_b",
        "group_b_adjusted_peer_count",
        "group_b_adjusted_peer_count_relaxed",
        "group_b_relaxed_final_tolerance",
        "group_b_final_peer_count",
        "group_b_final_peer_method",
        "group_b_nearest_fill_added",
        "group_b_peer_quality",
        # B_NO_MARKET_CAP fallback peer pool metadata
        "group_b_no_mcap_peer_count",
        "group_b_no_mcap_peer_method",
        "group_b_no_mcap_peer_quality",
        "group_c",
        "group_d",
        "group_e",
    ]
    if return_debug:
        debug_cols = [
            # C/D/E
            "revenue_yoy", "opm", "debt_ratio", "current_ratio", "interest_coverage",
            "volume_ratio", "vol_pct_rank",
            "debt_ratio_inf_flag", "current_ratio_inf_flag", "interest_coverage_capped_flag",
            # A
            "group_a_industry", "group_a_sector",
            "group_a_industry_count", "group_a_sector_count", "group_a_mode",
            # B
            "group_b_weight_mcap", "group_b_weight_revenue", "group_b_weight_assets",
        ]
        out_cols = out_cols + [c for c in debug_cols if c in df.columns]
    return df[[c for c in out_cols if c in df.columns]].copy()


def resolve_default_input_path() -> str:
    """
    Return default input path, preferring:
    1) ./data/universe_current.parquet
    2) ./data/universe_current.csv
    3) ./universe_current.parquet
    4) ./universe_current.csv
    """
    candidates = [
        Path("data") / "universe_current.parquet",
        Path("data") / "universe_current.csv",
        Path("universe_current.parquet"),
        Path("universe_current.csv"),
    ]

    for path in candidates:
        if path.exists():
            return str(path)

    raise FileNotFoundError(
        "Default universe file not found. "
        "Expected one of: "
        "./data/universe_current.parquet, "
        "./data/universe_current.csv, "
        "./universe_current.parquet, "
        "./universe_current.csv"
    )


def load_universe(input_path: str) -> pd.DataFrame:
    path = Path(input_path)
    suffix = path.suffix.lower()

    if suffix == ".csv":
        return pd.read_csv(path)
    if suffix == ".parquet":
        return pd.read_parquet(path)

    raise ValueError(f"Unsupported file type: {suffix}")


# data/ = raw input; logic_data/ = logic outputs (merged_features, group_tags)


def ensure_as_of_date(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure DataFrame has an 'as_of_date' column.
    If missing, set to date.today().isoformat(); otherwise normalize to string date.
    """
    out = df.copy()
    if "as_of_date" not in out.columns:
        out["as_of_date"] = date.today().isoformat()
        return out
    # Normalize existing to string (e.g. datetime -> YYYY-MM-DD)
    raw = out["as_of_date"]
    today_str = date.today().isoformat()
    if pd.api.types.is_datetime64_any_dtype(raw):
        out["as_of_date"] = raw.dt.strftime("%Y-%m-%d").fillna(today_str)
    else:
        out["as_of_date"] = pd.to_datetime(raw, errors="coerce").dt.strftime("%Y-%m-%d")
        out["as_of_date"] = out["as_of_date"].fillna(today_str)
        out["as_of_date"] = out["as_of_date"].astype(str).str.strip()
    return out


def upsert_history_parquet(
    df: pd.DataFrame,
    history_path: Path,
    key_cols: list[str],
) -> str | None:
    """
    Upsert df into a history Parquet file by key_cols (e.g. as_of_date, symbol).
    Existing rows with same key are removed, then new data is appended; result is deduped and saved.
    Returns the resolved path string on success, None if input empty, key_cols missing, or save fails.
    """
    if df.empty:
        return None
    for c in key_cols:
        if c not in df.columns:
            warnings.warn(
                f"upsert_history_parquet: key column '{c}' missing in DataFrame. Skipping save.",
                UserWarning,
                stacklevel=2,
            )
            return None

    history_path = Path(history_path).resolve()
    history_path.parent.mkdir(parents=True, exist_ok=True)

    existing = pd.DataFrame()
    if history_path.exists():
        try:
            existing = pd.read_parquet(history_path)
        except Exception as e:
            warnings.warn(
                f"Could not read existing history {history_path}: {e}. Creating new file.",
                UserWarning,
                stacklevel=2,
            )

    # Remove from existing any rows whose key matches a key in df
    if not existing.empty and all(c in existing.columns for c in key_cols):
        new_keys = df[key_cols].drop_duplicates()
        # Merge anti-join: keep existing rows not in new_keys
        merged = existing.merge(
            new_keys,
            on=key_cols,
            how="left",
            indicator=True,
        )
        existing = merged[merged["_merge"] == "left_only"].drop(columns=["_merge"])
    elif not existing.empty and not all(c in existing.columns for c in key_cols):
        existing = pd.DataFrame()

    # Concat with outer join to allow differing columns
    combined = pd.concat([existing, df], ignore_index=True, join="outer")
    combined = combined.drop_duplicates(subset=key_cols, keep="last")
    if key_cols and all(c in combined.columns for c in key_cols):
        combined = combined.sort_values(key_cols).reset_index(drop=True)

    try:
        combined.to_parquet(history_path, index=False)
    except Exception as e:
        warnings.warn(
            f"Failed to save history Parquet to {history_path}: {e}",
            UserWarning,
            stacklevel=2,
        )
        return None
    return str(history_path)


def update_merged_features_history(df: pd.DataFrame, output_dir: str = "logic_data") -> str | None:
    """
    Ensure df has as_of_date, then upsert into logic_data/merged_features_history.parquet
    with key (as_of_date, symbol). Returns path on success, None otherwise.
    """
    df = ensure_as_of_date(df)
    history_path = Path(output_dir).resolve() / "merged_features_history.parquet"
    return upsert_history_parquet(df, history_path, key_cols=["as_of_date", "symbol"])


def update_group_tags_history(df: pd.DataFrame, output_dir: str = "logic_data") -> str | None:
    """
    Upsert group tags into logic_data/group_tags_history.parquet
    with key (as_of_date, symbol). Returns path on success, None otherwise.
    """
    history_path = Path(output_dir).resolve() / "group_tags_history.parquet"
    return upsert_history_parquet(df, history_path, key_cols=["as_of_date", "symbol"])


def save_merged_features(df: pd.DataFrame, output_dir: str = "logic_data") -> dict[str, str]:
    """Save merged feature DataFrame to CSV and Parquet under output_dir (default logic_data)."""
    out = Path(output_dir).resolve()
    out.mkdir(parents=True, exist_ok=True)
    date_str = date.today().strftime("%Y%m%d")
    csv_path = out / f"merged_features_{date_str}.csv"
    parquet_path = out / f"merged_features_{date_str}.parquet"
    df.to_csv(csv_path, index=False)
    parquet_path_str = str(parquet_path)
    try:
        df.to_parquet(parquet_path, index=False)
    except Exception as e:
        warnings.warn(
            f"Parquet save failed (merged_features): {e}. CSV was saved to {csv_path}.",
            UserWarning,
            stacklevel=2,
        )
        parquet_path_str = None
    return {"csv_path": str(csv_path), "parquet_path": parquet_path_str}


def save_group_tags(df: pd.DataFrame, output_dir: str = "logic_data") -> dict[str, str]:
    """Save final group tag DataFrame to CSV and Parquet under output_dir (default logic_data)."""
    out = Path(output_dir).resolve()
    out.mkdir(parents=True, exist_ok=True)
    date_str = date.today().strftime("%Y%m%d")
    csv_path = out / f"group_tags_{date_str}.csv"
    parquet_path = out / f"group_tags_{date_str}.parquet"
    df.to_csv(csv_path, index=False)
    parquet_path_str = str(parquet_path)
    try:
        df.to_parquet(parquet_path, index=False)
    except Exception as e:
        warnings.warn(
            f"Parquet save failed (group_tags): {e}. CSV was saved to {csv_path}.",
            UserWarning,
            stacklevel=2,
        )
        parquet_path_str = None
    return {"csv_path": str(csv_path), "parquet_path": parquet_path_str}


if __name__ == "__main__":
    data_dir = "data"
    output_dir = "logic_data"
    sources = load_data_sources(data_dir)
    universe_current = sources.get("universe_current")
    if universe_current is None or universe_current.empty:
        raise FileNotFoundError("universe_current not found or empty. Place universe_current.parquet or .csv in data/.")

    print(f"Input rows - universe_current: {len(universe_current)}")
    for k, v in sources.items():
        if k != "universe_current" and not v.empty:
            print(f"  {k}: {len(v)}")

    merged_df = merge_feature_sources(universe_current, sources)
    merged_df = ensure_as_of_date(merged_df)
    print(f"Merged rows: {len(merged_df)}")
    print("Merged non-null counts (before ensure_columns):")
    for col in IMPORTANT_COLS:
        if col in merged_df.columns:
            print(f"  {col}: {merged_df[col].notna().sum()}")

    merged_paths = save_merged_features(merged_df, output_dir=output_dir)
    print(f"Merged CSV saved: {merged_paths['csv_path']}")
    print(f"Merged Parquet saved: {merged_paths['parquet_path']}")

    merged_history_path = update_merged_features_history(merged_df, output_dir=output_dir)
    if merged_history_path:
        print(f"Merged History Parquet updated: {merged_history_path}")
    else:
        print("Merged History Parquet not updated (empty or save failed)")

    group_tagged_df = build_group_tags(merged_df, return_debug=False)

    if len(group_tagged_df) != len(universe_current):
        raise ValueError(
            f"Row count mismatch: input={len(universe_current)}, output={len(group_tagged_df)}"
        )

    tag_paths = save_group_tags(group_tagged_df, output_dir=output_dir)
    print(f"Output rows: {len(group_tagged_df)}")
    print(f"Tags CSV saved: {tag_paths['csv_path']}")
    print(f"Tags Parquet saved: {tag_paths['parquet_path']}")

    tags_history_path = update_group_tags_history(group_tagged_df, output_dir=output_dir)
    if tags_history_path:
        print(f"Tags History Parquet updated: {tags_history_path}")
    else:
        print("Tags History Parquet not updated (empty or save failed)")
    print("Tag counts - group_c:")
    print(group_tagged_df["group_c"].value_counts(dropna=False).to_string())
    print("Tag counts - group_d:")
    print(group_tagged_df["group_d"].value_counts(dropna=False).to_string())
    print("Tag counts - group_e:")
    print(group_tagged_df["group_e"].value_counts(dropna=False).to_string())
    print(group_tagged_df.head(10).to_string(index=False))
