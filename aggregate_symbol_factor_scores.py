# -*- coding: utf-8 -*-
"""
Aggregate per-group factor scores into a single final score per:
  symbol x factor_name

Input:
  output/scoring/group_factor_scores_latest.(parquet|csv)
  output/scoring/missing_priors_latest.(parquet|csv) optional; fallback prior evidence 0 if absent

Output:
  output/scoring/symbol_factor_scores_latest.(parquet|csv)
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from donor_imputation import estimate_missing_factor_score_from_donors
from score_factor_config import GROUP_BASE_WEIGHTS, FACTOR_SPECS
from score_primitives import evidence_to_score, score_to_evidence_approx


GROUP_TYPES = ["A", "B", "C", "D", "E"]

def _load_missing_priors(scoring_dir: Path) -> pd.DataFrame:
    """Prefer parquet; fallback to csv. Empty if neither exists."""
    p = scoring_dir / "missing_priors_latest.parquet"
    c = scoring_dir / "missing_priors_latest.csv"
    df = _read_df(p)
    if df.empty:
        df = _read_df(c)
    return df


def _lookup_prior_score(
    priors_df: pd.DataFrame,
    factor_name: str,
    category: str,
    group_type: Any,
) -> tuple[float, str]:
    """
    Returns prior evidence using score prior table with inverse mapping:
    same factor+category+group_type -> same category+ALL -> global -> fallback.
    """
    if priors_df is None or priors_df.empty:
        return 0.0, "fallback_0"
    fn = str(factor_name)
    cat = str(category) if category is not None else ""
    gt = str(group_type).strip() if group_type is not None and str(group_type).strip() else "ALL"
    if gt == "":
        gt = "ALL"

    sub = priors_df[priors_df["factor_name"].astype(str) == fn]
    if sub.empty:
        return 0.0, "fallback_0"

    m = sub[
        (sub["category"].fillna("").astype(str) == cat)
        & (sub["group_type"].map(lambda x: str(x).strip() if pd.notna(x) else "ALL") == gt)
    ]
    def _ps(row) -> float:
        v = float(pd.to_numeric(row["prior_score"], errors="coerce"))
        if np.isnan(v):
            return 0.0
        ev = score_to_evidence_approx(v, beta=0.7)
        return 0.0 if ev is None else float(ev)

    if not m.empty:
        return _ps(m.iloc[0]), str(m.iloc[0].get("prior_level", "group_type_category"))

    m = sub[
        (sub["category"].fillna("").astype(str) == cat)
        & (sub["group_type"].map(lambda x: str(x).strip() if pd.notna(x) else "") == "ALL")
    ]
    if not m.empty:
        return _ps(m.iloc[0]), str(m.iloc[0].get("prior_level", "category"))

    g = sub[sub["prior_level"].astype(str) == "global"]
    if not g.empty:
        return _ps(g.iloc[0]), "global"

    return _ps(sub.iloc[0]), str(sub.iloc[0].get("prior_level", "global"))


def _row_is_observed(series: pd.Series) -> bool:
    adj = pd.to_numeric(series.get("adjusted_evidence"), errors="coerce")
    if pd.isna(adj):
        return False
    if "is_valid_score" in series.index:
        iv = series["is_valid_score"]
        if isinstance(iv, str):
            ok = iv.strip().lower() in ("1", "true", "t", "yes")
        else:
            ok = bool(iv) if pd.notna(iv) else False
        if not ok:
            return False
    conf = pd.to_numeric(series.get("confidence"), errors="coerce")
    if pd.isna(conf) or float(conf) <= 0.0:
        return False
    return True


def _availability_component(series: pd.Series) -> float:
    nv = pd.to_numeric(series.get("n_valid"), errors="coerce")
    if nv is None or pd.isna(nv) or float(nv) <= 0.0:
        return 0.45
    return float(min(1.0, float(nv) / 20.0))


def _imputation_confidence(lambda_f: float, donor_conf: float) -> float:
    """Low weight for imputed rows; dtype-stable float in (0, 0.45]."""
    prior_w = 0.22
    mix = float(lambda_f) * float(donor_conf) + (1.0 - float(lambda_f)) * prior_w
    return float(np.clip(0.03 + 0.4 * mix, 0.02, 0.45))


def _enrich_imputed_scores(df: pd.DataFrame, priors_df: pd.DataFrame) -> pd.DataFrame:
    """
    Fill adjusted_evidence/confidence for missing factor rows using prior + donor shrink.
    Observed rows (valid adjusted_evidence + conf>0) are not changed.
    """
    out = df.copy()
    n = len(out)
    src: list[str | None] = [None] * n
    pp: list[float] = [np.nan] * n
    de: list[float] = [np.nan] * n
    dc: list[float] = [np.nan] * n
    dcf: list[float] = [np.nan] * n
    sl: list[float] = [np.nan] * n

    if "missing_reason" not in out.columns:
        out["missing_reason"] = pd.NA
    if "is_valid_score" not in out.columns:
        out["is_valid_score"] = pd.NA
    if "n_valid" not in out.columns:
        out["n_valid"] = pd.NA
    if "adjusted_evidence" not in out.columns:
        out["adjusted_evidence"] = pd.NA

    idx_list = list(out.index)
    for j, idx in enumerate(idx_list):
        row = out.loc[idx]
        if _row_is_observed(row):
            src[j] = "observed"
            continue

        fn = str(row.get("factor_name", "") or "")
        cat = str(row.get("category", "") or "")
        gt = row.get("group_type")
        spec = FACTOR_SPECS.get(fn)
        default_prior_evidence = float(getattr(spec, "evidence_prior", 0.0)) if spec is not None else 0.0
        if spec is None:
            src[j] = "prior_only"
            pe, _ = _lookup_prior_score(priors_df, fn, cat, gt)
            pe = default_prior_evidence if pe is None else float(pe)
            pp[j] = pe
            out.loc[idx, "adjusted_evidence"] = float(pe)
            out.loc[idx, "adjusted_score"] = evidence_to_score(float(pe))
            out.loc[idx, "confidence"] = 0.06
            out.loc[idx, "is_valid_score"] = True
            sl[j] = 0.0
            dcf[j] = 0.0
            dc[j] = 0.0
            continue

        pe, _pl = _lookup_prior_score(priors_df, fn, cat, gt)
        pe = default_prior_evidence if pe is None else float(pe)
        pp[j] = pe

        mr = row.get("missing_reason")
        is_structural = mr is not None and str(mr) == "structural_missing"

        dest = estimate_missing_factor_score_from_donors(out, row, fn, spec, max_donors=20)
        d_est = dest.get("donor_evidence_estimate")
        dconf = float(dest.get("donor_confidence") or 0.0)
        dcount = float(dest.get("donor_count") or 0)
        dc[j] = dcount
        dcf[j] = dconf

        avail = _availability_component(row)

        if is_structural or dest.get("donor_method") == "structural_skip":
            lambda_f = 0.0
            fe = float(pe)
            src[j] = "structural_prior"
            de[j] = np.nan
            out.loc[idx, "adjusted_evidence"] = float(fe)
            out.loc[idx, "adjusted_score"] = evidence_to_score(float(fe))
            out.loc[idx, "confidence"] = _imputation_confidence(0.0, 0.0)
            out.loc[idx, "is_valid_score"] = True
            sl[j] = lambda_f
            continue
        elif d_est is not None and not (isinstance(d_est, float) and np.isnan(d_est)):
            lambda_raw = float(np.clip(dconf * avail, 0.0, 1.0))
            lambda_f = float(min(lambda_raw, 0.25)) if is_structural else lambda_raw
            dv = float(d_est)
            de[j] = dv
            fe = lambda_f * dv + (1.0 - lambda_f) * float(pe)
            src[j] = "donor_shrink" if lambda_f > 0.02 else "prior_only"
        else:
            lambda_f = 0.0
            fe = float(pe)
            src[j] = "prior_only"
            de[j] = np.nan

        sl[j] = lambda_f
        out.loc[idx, "adjusted_evidence"] = float(fe)
        out.loc[idx, "adjusted_score"] = evidence_to_score(float(fe))
        out.loc[idx, "confidence"] = _imputation_confidence(lambda_f, dconf if d_est is not None else 0.0)
        out.loc[idx, "is_valid_score"] = True

    out["factor_source_row"] = src
    out["prior_evidence"] = pp
    out["donor_evidence"] = de
    out["donor_count"] = dc
    out["donor_confidence"] = dcf
    out["shrink_lambda"] = sl

    for j, idx in enumerate(idx_list):
        if src[j] == "observed":
            out.loc[idx, "prior_evidence"] = np.nan
            out.loc[idx, "donor_evidence"] = np.nan
            out.loc[idx, "donor_count"] = np.nan
            out.loc[idx, "donor_confidence"] = np.nan
            out.loc[idx, "shrink_lambda"] = np.nan

    return out


def _dominant_factor_source(s: pd.Series) -> str:
    u = set(pd.Series(s).dropna().astype(str).unique().tolist())
    if "observed" in u:
        return "observed"
    if "donor_shrink" in u:
        return "donor_shrink"
    if "structural_prior" in u:
        return "structural_prior"
    if "prior_only" in u:
        return "prior_only"
    return "prior_only"


def _read_df(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    if path.suffix.lower() == ".csv":
        return pd.read_csv(path, low_memory=False)
    return pd.read_parquet(path)


def _infer_group_type_from_columns(df: pd.DataFrame) -> pd.DataFrame:
    if "group_type" in df.columns:
        return df
    if "group_tag" in df.columns:
        # group_tag is usually: group_a / group_b / group_c / ...
        def _infer(x: Any) -> Any:
            if x is None or (isinstance(x, float) and pd.isna(x)):
                return None
            s = str(x).strip().lower()
            if "group_a" in s:
                return "A"
            if "group_b" in s:
                return "B"
            if "group_c" in s:
                return "C"
            if "group_d" in s:
                return "D"
            if "group_e" in s:
                return "E"
            return None

        out = df.copy()
        out["group_type"] = out["group_tag"].apply(_infer)
        return out
    return df


def _aggregate_symbol_factor_scores(df: pd.DataFrame) -> pd.DataFrame:
    required = [
        "symbol",
        "as_of_date",
        "factor_name",
        "category",
        "group_type",
    ]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns in score input: {missing}")

    df = _infer_group_type_from_columns(df)
    if "group_type" not in df.columns:
        raise ValueError("Cannot infer group_type; missing both group_type and group_tag.")

    # Numeric coercion (defensive). Non-coercible becomes NaN -> invalid.
    df = df.copy()
    df["adjusted_evidence"] = pd.to_numeric(df["adjusted_evidence"], errors="coerce")
    df["adjusted_score"] = pd.to_numeric(df["adjusted_score"], errors="coerce")
    df["confidence"] = pd.to_numeric(df["confidence"], errors="coerce")

    # Validity filter for group contributions (imputed rows filled upstream).
    valid_row = df["adjusted_evidence"].notna() & df["confidence"].notna() & (df["confidence"] > 0)

    df["category_valid"] = df["category"].where(valid_row, other=pd.NA)
    df["confidence_valid"] = df["confidence"].where(valid_row, other=pd.NA)

    # Precompute per-group columns.
    for g in GROUP_TYPES:
        col_evd = f"group_evidence_{g}"
        col_w = f"weight_{g}"
        w_base = float(GROUP_BASE_WEIGHTS.get(g, 1.0))
        w = df["confidence"] * w_base
        df[col_evd] = df["adjusted_evidence"].where(valid_row & (df["group_type"] == g), other=pd.NA)
        df[col_w] = w.where(valid_row & (df["group_type"] == g), other=0.0)

    key_cols = ["symbol", "as_of_date", "factor_name"]

    agg_spec: dict[str, Any] = {
        "category_valid": "first",
        "confidence_valid": "mean",
    }
    if "factor_source_row" in df.columns:
        agg_spec["factor_source_row"] = _dominant_factor_source
    if "prior_evidence" in df.columns:
        agg_spec["prior_evidence"] = "mean"
    if "donor_evidence" in df.columns:
        agg_spec["donor_evidence"] = "mean"
    if "donor_confidence" in df.columns:
        agg_spec["donor_confidence"] = "mean"
    if "shrink_lambda" in df.columns:
        agg_spec["shrink_lambda"] = "mean"
    if "donor_count" in df.columns:
        agg_spec["donor_count"] = "max"
    for g in GROUP_TYPES:
        agg_spec[f"group_evidence_{g}"] = "first"
        agg_spec[f"weight_{g}"] = "sum"

    grouped = df.groupby(key_cols, dropna=False, as_index=False).agg(agg_spec)

    # Compute coverage/availability and final scores.
    weight_sum = None
    for g in GROUP_TYPES:
        wcol = f"weight_{g}"
        if weight_sum is None:
            weight_sum = grouped[wcol].astype(float)
        else:
            weight_sum = weight_sum + grouped[wcol].astype(float)
    grouped["total_effective_weight"] = weight_sum

    # valid_group_count: number of groups with weight > 0
    w_gt0 = None
    for g in GROUP_TYPES:
        wcol = f"weight_{g}"
        if w_gt0 is None:
            w_gt0 = (grouped[wcol] > 0).astype(int)
        else:
            w_gt0 = w_gt0 + (grouped[wcol] > 0).astype(int)
    grouped["valid_group_count"] = w_gt0

    grouped["availability"] = grouped["valid_group_count"].clip(upper=3) / 3.0
    grouped["availability"] = grouped["availability"].clip(lower=0.0, upper=1.0)

    # weighted evidence average (only among valid rows)
    num = None
    for g in GROUP_TYPES:
        s_col = f"group_evidence_{g}"
        w_col = f"weight_{g}"
        term = grouped[s_col].fillna(0.0).astype(float) * grouped[w_col].astype(float)
        num = term if num is None else (num + term)
    grouped["raw_observed_factor_evidence"] = num / grouped["total_effective_weight"].replace({0.0: np.nan})

    has_weight = pd.to_numeric(grouped["total_effective_weight"], errors="coerce").fillna(0.0) > 0
    availability = pd.to_numeric(grouped["availability"], errors="coerce").fillna(0.0)
    obs_evd = pd.to_numeric(grouped["raw_observed_factor_evidence"], errors="coerce")
    prior_evd = pd.to_numeric(grouped.get("prior_evidence", 0.0), errors="coerce").fillna(0.0)
    grouped["final_factor_evidence"] = np.where(
        has_weight,
        availability * obs_evd + (1.0 - availability) * prior_evd,
        prior_evd,
    ).astype(float)
    grouped["final_factor_score"] = pd.to_numeric(
        grouped["final_factor_evidence"], errors="coerce"
    ).map(lambda x: evidence_to_score(float(x)) if pd.notna(x) else np.nan)
    grouped["final_factor_score"] = pd.to_numeric(grouped["final_factor_score"], errors="coerce").astype(float)

    if "factor_source_row" in grouped.columns:
        grouped["factor_source"] = grouped["factor_source_row"].astype(str)
        grouped = grouped.drop(columns=["factor_source_row"], errors="ignore")
    else:
        grouped["factor_source"] = "observed"

    grouped["category"] = grouped["category_valid"]
    grouped = grouped.drop(columns=["category_valid"], errors="ignore")

    # Ensure required output column order.
    out_cols = [
        "symbol",
        "as_of_date",
        "factor_name",
        "category",
        "factor_source",
        "raw_observed_factor_evidence",
        "prior_evidence",
        "donor_evidence",
        "donor_count",
        "donor_confidence",
        "shrink_lambda",
        "final_factor_evidence",
        "final_factor_score",
        "valid_group_count",
        "total_effective_weight",
        "availability",
        "mean_confidence",
    ]
    # mean_confidence comes from confidence_valid mean.
    grouped = grouped.rename(columns={"confidence_valid": "mean_confidence"})
    for g in GROUP_TYPES:
        out_cols.append(f"group_evidence_{g}")
    for g in GROUP_TYPES:
        out_cols.append(f"weight_{g}")

    for c in out_cols:
        if c not in grouped.columns:
            grouped[c] = pd.NA

    return grouped[out_cols].copy()


def _save_df(df: pd.DataFrame, parquet_path: Path, csv_path: Path) -> None:
    parquet_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(parquet_path, index=False)
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")


def main(input_dir: str | Path = "output", output_dir: str | Path = "output/scoring") -> None:
    input_dir = Path(input_dir)
    out_dir = Path(output_dir)

    parquet_in = input_dir / "scoring" / "group_factor_scores_latest.parquet"
    csv_in = input_dir / "scoring" / "group_factor_scores_latest.csv"

    df = _read_df(parquet_in)
    if df.empty:
        df = _read_df(csv_in)

    if df.empty:
        print("No input factor score snapshot found; nothing to aggregate.")
        return

    rows_in = len(df)
    scoring_dir = input_dir / "scoring"
    priors_df = _load_missing_priors(scoring_dir)
    if priors_df.empty:
        print("Note: missing_priors_latest not found; using fallback prior evidence 0.0.")
    if "adjusted_score" not in df.columns:
        df["adjusted_score"] = pd.NA
    if "adjusted_evidence" not in df.columns:
        df["adjusted_evidence"] = pd.NA
    if "confidence" not in df.columns:
        df["confidence"] = pd.NA
    df = _enrich_imputed_scores(df, priors_df)
    scores_df = _aggregate_symbol_factor_scores(df)
    print(f"Input rows: {rows_in}")
    print(f"Aggregated rows: {len(scores_df)}")

    expected_factors = set(FACTOR_SPECS.keys())
    have_factors = set(scores_df["factor_name"].dropna().unique().tolist())
    factor_coverage = 0.0
    if expected_factors:
        factor_coverage = len(have_factors) / len(expected_factors)
    print(f"Factor coverage (computed/expected): {len(have_factors)}/{len(expected_factors)} = {factor_coverage:.2%}")

    parquet_out = out_dir / "symbol_factor_scores_latest.parquet"
    csv_out = out_dir / "symbol_factor_scores_latest.csv"
    _save_df(scores_df, parquet_out, csv_out)
    print(f"Saved: {parquet_out}")


if __name__ == "__main__":
    main()

