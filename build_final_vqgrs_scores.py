# -*- coding: utf-8 -*-
"""
Build final VQGRS scores from category-level evidences.

Input:
  - output/scoring/symbol_category_scores_latest.(parquet|csv)

Output:
  - output/scoring/final_vqgrs_scores_latest.(parquet|csv)

Debug note:
  - A high category score with low main_coverage_* can indicate lower-confidence results.
    This pipeline is designed to be conservative when main indicators are missing.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from group_snapshot_utils import finalize_scoring_wide_input_df


CORE_CATS = ["V", "Q", "G", "R", "S"]

TRACK_WEIGHTS: dict[str, dict[str, float]] = {
    "equal": {"V": 0.20, "Q": 0.20, "G": 0.20, "R": 0.20, "S": 0.20},
    "track_A": {"V": 0.40, "Q": 0.20, "G": 0.10, "R": 0.20, "S": 0.10},
    "track_B": {"V": 0.25, "Q": 0.30, "G": 0.10, "R": 0.15, "S": 0.20},
    "track_C": {"V": 0.20, "Q": 0.20, "G": 0.35, "R": 0.15, "S": 0.10},
}


def _read_df(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    if path.suffix.lower() == ".csv":
        return pd.read_csv(path, low_memory=False)
    return pd.read_parquet(path)


def _load_track_inputs_from_factors_latest() -> pd.DataFrame:
    """
    Track raw input source (strict):
      - data/factors_latest.parquet only
      - no CSV fallback
      - no snapshot fallback
    """
    path = Path("data") / "factors_latest.parquet"
    if not path.exists():
        raise FileNotFoundError(f"Required track raw source missing: {path}")
    fac = pd.read_parquet(path)
    if fac.empty:
        return pd.DataFrame(
            columns=[
                "symbol",
                "as_of_date",
                "track_input_roic",
                "track_input_oper_margin",
                "track_input_ocf_ni",
                "track_input_revenue_yoy",
                "track_input_debt_to_equity",
                "track_input_current_ratio",
                "track_input_interest_coverage",
                "track_input_beta",
                "track_input_pe",
                "track_input_ps",
                "track_input_ev_ebitda",
                "track_input_rule_of_40_calc",
                "track_A_valuation_valid_count",
                "track_input_v_market_proxy",
            ]
        )

    if "as_of_date" not in fac.columns:
        if "asOfDate" in fac.columns:
            fac = fac.copy()
            fac["as_of_date"] = fac["asOfDate"]
        else:
            raise ValueError("factors_latest.parquet must contain as_of_date (or asOfDate).")
    if "symbol" not in fac.columns:
        raise ValueError("factors_latest.parquet must contain symbol.")

    needed = [
        "symbol",
        "as_of_date",
        "ROIC",
        "Oper. Margin",
        "OCF/NI",
        "Revenue YoY",
        "Debt/Eq",
        "Current Ratio",
        "Interest Coverage",
        "Beta",
        "P/E",
        "P/S",
        "EV/EBITDA",
    ]
    for c in needed:
        if c not in fac.columns:
            fac[c] = np.nan
    work = fac[needed].copy()
    work["symbol"] = work["symbol"].astype(str).str.strip().str.upper()
    work["as_of_date"] = pd.to_datetime(work["as_of_date"], errors="coerce")
    work = work.dropna(subset=["as_of_date"])
    if work.empty:
        return pd.DataFrame(columns=["symbol", "as_of_date"])

    latest_dt = work["as_of_date"].max()
    print(f"Track raw source: {path} | max(as_of_date)={latest_dt}")
    work = work.loc[work["as_of_date"] == latest_dt].copy()

    num_cols = [c for c in needed if c not in ("symbol", "as_of_date")]
    for c in num_cols:
        work[c] = pd.to_numeric(work[c], errors="coerce")
    work["as_of_date"] = work["as_of_date"].dt.strftime("%Y-%m-%d")

    work = (
        work.groupby(["symbol", "as_of_date"], as_index=False, dropna=False)
        .agg({c: "median" for c in num_cols})
    )

    work = work.rename(
        columns={
            "ROIC": "track_input_roic",
            "Oper. Margin": "track_input_oper_margin",
            "OCF/NI": "track_input_ocf_ni",
            "Revenue YoY": "track_input_revenue_yoy",
            "Debt/Eq": "track_input_debt_to_equity",
            "Current Ratio": "track_input_current_ratio",
            "Interest Coverage": "track_input_interest_coverage",
            "Beta": "track_input_beta",
            "P/E": "track_input_pe",
            "P/S": "track_input_ps",
            "EV/EBITDA": "track_input_ev_ebitda",
        }
    )

    work["track_input_rule_of_40_calc"] = np.where(
        pd.to_numeric(work["track_input_revenue_yoy"], errors="coerce").notna()
        & pd.to_numeric(work["track_input_oper_margin"], errors="coerce").notna(),
        pd.to_numeric(work["track_input_revenue_yoy"], errors="coerce")
        + pd.to_numeric(work["track_input_oper_margin"], errors="coerce"),
        np.nan,
    )

    def _lower_better_score(series: pd.Series) -> pd.Series:
        x = pd.to_numeric(series, errors="coerce")
        med = float(x.median(skipna=True))
        q25 = float(x.quantile(0.25))
        q75 = float(x.quantile(0.75))
        iqr = q75 - q25
        if not np.isfinite(iqr) or iqr <= 0:
            return pd.Series(np.nan, index=x.index, dtype=float)
        z = (x - med) / iqr
        return pd.Series(np.clip(50.0 - 20.0 * z, 0.0, 100.0), index=x.index, dtype=float)

    pe_s = _lower_better_score(work["track_input_pe"])
    ps_s = _lower_better_score(work["track_input_ps"])
    ev_s = _lower_better_score(work["track_input_ev_ebitda"])
    valid_pe = pe_s.notna()
    valid_ps = ps_s.notna()
    valid_ev = ev_s.notna()
    val_count = valid_pe.astype(int) + valid_ps.astype(int) + valid_ev.astype(int)
    work["track_A_valuation_valid_count"] = val_count.astype(float)

    den = valid_pe.astype(float) * 0.4 + valid_ps.astype(float) * 0.3 + valid_ev.astype(float) * 0.3
    num = pe_s.fillna(0.0) * 0.4 + ps_s.fillna(0.0) * 0.3 + ev_s.fillna(0.0) * 0.3
    work["track_input_v_market_proxy"] = np.where((val_count >= 2) & (den > 0), num / den, np.nan)
    return work


def _save_df(df: pd.DataFrame, parquet_path: Path, csv_path: Path) -> None:
    parquet_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        df.to_parquet(parquet_path, index=False)
    except Exception as e:
        print(f"WARNING: failed to save parquet: {parquet_path} ({e})")
    try:
        df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    except PermissionError as e:
        print(f"WARNING: failed to save CSV (file may be open): {csv_path} ({e})")
    except Exception as e:
        print(f"WARNING: failed to save CSV: {csv_path} ({e})")


def _weighted_score(df: pd.DataFrame, weights: dict[str, float]) -> pd.Series:
    num = pd.Series(0.0, index=df.index, dtype=float)
    den = pd.Series(0.0, index=df.index, dtype=float)
    for cat, w in weights.items():
        col = f"score_{cat}"
        vals = pd.to_numeric(df[col], errors="coerce")
        valid = vals.notna()
        wf = float(w)
        num = num + vals.fillna(0.0) * wf
        den = den + np.where(valid, wf, 0.0)
    out = pd.Series(np.where(den > 0.0, num / den, np.nan), index=df.index, dtype=float)
    return out.clip(lower=0.0, upper=100.0)


def _build_track_reason(df: pd.DataFrame) -> pd.Series:
    reasons: list[str] = []
    for _, r in df.iterrows():
        if bool(r.get("is_track_B_candidate", False)):
            reasons.append("B: raw quality/risk/stability pass")
            continue
        if bool(r.get("is_track_C_candidate", False)):
            reasons.append("C: Revenue YoY>=20 and rule_of_40_calc>=30")
            continue
        if bool(r.get("is_track_A_candidate", False)):
            reasons.append("A: v_market_proxy>=70")
            continue

        missing: list[str] = []
        for c in (
            "track_input_roic",
            "track_input_oper_margin",
            "track_input_ocf_ni",
            "track_input_revenue_yoy",
            "track_input_debt_to_equity",
            "track_input_current_ratio",
            "track_input_interest_coverage",
            "track_input_beta",
            "track_input_pe",
            "track_input_ps",
            "track_input_ev_ebitda",
            "track_input_rule_of_40_calc",
            "track_input_v_market_proxy",
        ):
            if pd.isna(pd.to_numeric(r.get(c), errors="coerce")):
                missing.append(c.replace("track_input_", ""))
        if missing:
            reasons.append("N: missing_inputs=" + ",".join(missing))
        else:
            reasons.append("N: no raw-track condition met")
    return pd.Series(reasons, index=df.index, dtype=object)


def build_final_vqgrs_scores_df(df_cat: pd.DataFrame) -> pd.DataFrame:
    if df_cat is None or df_cat.empty:
        return pd.DataFrame()

    required = {"symbol", "as_of_date", *[f"score_{c}" for c in CORE_CATS]}
    missing = [c for c in required if c not in df_cat.columns]
    if missing:
        raise ValueError(f"Missing required input columns: {missing}")

    out = df_cat[["symbol", "as_of_date"]].copy()
    out["symbol"] = out["symbol"].astype(str).str.strip().str.upper()
    out["as_of_date"] = pd.to_datetime(out["as_of_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    for c in (
        "track_input_roic",
        "track_input_oper_margin",
        "track_input_ocf_ni",
        "track_input_revenue_yoy",
        "track_input_debt_to_equity",
        "track_input_current_ratio",
        "track_input_interest_coverage",
        "track_input_beta",
        "track_input_pe",
        "track_input_ps",
        "track_input_ev_ebitda",
        "track_input_rule_of_40_calc",
        "track_A_valuation_valid_count",
        "track_input_v_market_proxy",
    ):
        if c in df_cat.columns:
            out[c] = pd.to_numeric(df_cat[c], errors="coerce")

    for c in CORE_CATS:
        out[f"final_evidence_{c}"] = pd.to_numeric(df_cat[f"final_evidence_{c}"], errors="coerce").fillna(0.0).astype(float)
        score_col = f"score_{c}"
        out[score_col] = pd.to_numeric(df_cat[score_col], errors="coerce").clip(lower=0.0, upper=100.0).astype(float)

        # Debug passthroughs (when present).
        mc = f"main_coverage_{c}"
        ds = f"dominant_signal_{c}"
        if mc in df_cat.columns:
            out[mc] = pd.to_numeric(df_cat[mc], errors="coerce").fillna(0.0).astype(float)
        if ds in df_cat.columns:
            out[ds] = df_cat[ds].fillna("balanced").astype(object)

    out["final_evidence_equal"] = np.nan
    out["final_score_equal"] = _weighted_score(out, TRACK_WEIGHTS["equal"]).astype(float)

    out["final_evidence_track_A"] = np.nan
    out["final_score_track_A"] = _weighted_score(out, TRACK_WEIGHTS["track_A"]).astype(float)

    out["final_evidence_track_B"] = np.nan
    out["final_score_track_B"] = _weighted_score(out, TRACK_WEIGHTS["track_B"]).astype(float)

    out["final_evidence_track_C"] = np.nan
    out["final_score_track_C"] = _weighted_score(out, TRACK_WEIGHTS["track_C"]).astype(float)

    # Track inputs from factors_latest raw-source join, injected in main().
    for c in (
        "track_input_roic",
        "track_input_oper_margin",
        "track_input_ocf_ni",
        "track_input_revenue_yoy",
        "track_input_debt_to_equity",
        "track_input_current_ratio",
        "track_input_interest_coverage",
        "track_input_beta",
        "track_input_pe",
        "track_input_ps",
        "track_input_ev_ebitda",
        "track_input_rule_of_40_calc",
        "track_A_valuation_valid_count",
        "track_input_v_market_proxy",
    ):
        if c not in out.columns:
            out[c] = np.nan

    roic = pd.to_numeric(out["track_input_roic"], errors="coerce")
    opm = pd.to_numeric(out["track_input_oper_margin"], errors="coerce")
    ocf_ni = pd.to_numeric(out["track_input_ocf_ni"], errors="coerce")
    rev = pd.to_numeric(out["track_input_revenue_yoy"], errors="coerce")
    debt = pd.to_numeric(out["track_input_debt_to_equity"], errors="coerce")
    current = pd.to_numeric(out["track_input_current_ratio"], errors="coerce")
    icov = pd.to_numeric(out["track_input_interest_coverage"], errors="coerce")
    beta = pd.to_numeric(out["track_input_beta"], errors="coerce")
    rule40 = pd.to_numeric(out["track_input_rule_of_40_calc"], errors="coerce")
    v_proxy = pd.to_numeric(out["track_input_v_market_proxy"], errors="coerce")
    val_count = pd.to_numeric(out["track_A_valuation_valid_count"], errors="coerce").fillna(0.0)

    quality_count = (roic >= 15.0).astype(int) + (opm >= 10.0).astype(int) + (ocf_ni >= 0.8).astype(int)
    risk_count = (debt <= 1.0).astype(int) + (current >= 1.5).astype(int) + (icov >= 3.0).astype(int)
    stability_pass = (beta <= 1.2)
    out["track_B_quality_pass"] = (quality_count >= 2).astype(bool)
    out["track_B_risk_pass"] = (risk_count >= 2).astype(bool)
    out["track_B_stability_pass"] = stability_pass.astype(bool)

    out["is_track_B_candidate"] = (
        out["track_B_quality_pass"]
        & out["track_B_risk_pass"]
        & out["track_B_stability_pass"]
        & (roic >= 15.0)
    ).astype(bool)
    out["is_track_C_candidate"] = ((rev >= 20.0) & (rule40 >= 30.0)).astype(bool)
    out["is_track_A_candidate"] = ((v_proxy >= 70.0) & (val_count >= 2.0)).astype(bool)

    out["assigned_track"] = np.select(
        [
            out["is_track_B_candidate"],
            out["is_track_C_candidate"],
            out["is_track_A_candidate"],
        ],
        ["B", "C", "A"],
        default="N",
    )
    out["track_reason"] = _build_track_reason(out)

    # Representative final score selection by assigned investment track.
    out["final_score"] = np.select(
        [
            out["assigned_track"] == "A",
            out["assigned_track"] == "B",
            out["assigned_track"] == "C",
            out["assigned_track"] == "N",
        ],
        [
            pd.to_numeric(out["final_score_track_A"], errors="coerce"),
            pd.to_numeric(out["final_score_track_B"], errors="coerce"),
            pd.to_numeric(out["final_score_track_C"], errors="coerce"),
            pd.to_numeric(out["final_score_equal"], errors="coerce"),
        ],
        default=pd.to_numeric(out["final_score_equal"], errors="coerce"),
    )
    out["final_score"] = pd.to_numeric(out["final_score"], errors="coerce").clip(lower=0.0, upper=100.0).astype(float)
    out["final_score_method"] = np.select(
        [
            out["assigned_track"] == "A",
            out["assigned_track"] == "B",
            out["assigned_track"] == "C",
            out["assigned_track"] == "N",
        ],
        [
            "track_A_weighted",
            "track_B_weighted",
            "track_C_weighted",
            "equal_weighted",
        ],
        default="equal_weighted",
    )
    out["selected_weight_profile"] = out["final_score_method"].astype(object)
    # Placeholder columns for later hard-stop / penalty wiring.
    out["investment_warning"] = ""
    out["hard_stop_triggered"] = False

    out_cols = [
        "symbol",
        "as_of_date",
        "assigned_track",
        "final_score",
        "final_score_method",
        "selected_weight_profile",
        "final_evidence_equal",
        "final_score_equal",
        "final_evidence_track_A",
        "final_score_track_A",
        "final_evidence_track_B",
        "final_score_track_B",
        "final_evidence_track_C",
        "final_score_track_C",
        "score_V",
        "score_Q",
        "score_G",
        "score_R",
        "score_S",
        "final_evidence_V",
        "final_evidence_Q",
        "final_evidence_G",
        "final_evidence_R",
        "final_evidence_S",
        "track_reason",
        "track_B_quality_pass",
        "track_B_risk_pass",
        "track_B_stability_pass",
        "track_A_valuation_valid_count",
        "is_track_A_candidate",
        "is_track_B_candidate",
        "is_track_C_candidate",
        "track_input_roic",
        "track_input_oper_margin",
        "track_input_ocf_ni",
        "track_input_revenue_yoy",
        "track_input_debt_to_equity",
        "track_input_current_ratio",
        "track_input_interest_coverage",
        "track_input_beta",
        "track_input_pe",
        "track_input_ps",
        "track_input_ev_ebitda",
        "track_input_rule_of_40_calc",
        "track_input_v_market_proxy",
        "investment_warning",
        "hard_stop_triggered",
        "main_coverage_V",
        "main_coverage_Q",
        "main_coverage_G",
        "main_coverage_R",
        "main_coverage_S",
        "dominant_signal_V",
        "dominant_signal_Q",
        "dominant_signal_G",
        "dominant_signal_R",
        "dominant_signal_S",
    ]
    out_cols = [c for c in out_cols if c in out.columns]
    return out[out_cols].copy()


def main(input_dir: str | Path = "output", output_dir: str | Path = "output/scoring") -> None:
    input_dir = Path(input_dir)
    output_dir = Path(output_dir)

    parquet_in = input_dir / "scoring" / "symbol_category_scores_latest.parquet"
    csv_in = input_dir / "scoring" / "symbol_category_scores_latest.csv"

    df = _read_df(parquet_in)
    if df.empty:
        df = _read_df(csv_in)
    if df.empty:
        print("No symbol_category_scores_latest input found.")
        return

    df = finalize_scoring_wide_input_df(df, label="build_final_vqgrs_scores")
    track_inputs = _load_track_inputs_from_factors_latest()
    print(f"Track input rows from factors_latest.parquet: {len(track_inputs)}")
    df = df.copy()
    df["symbol"] = df["symbol"].astype(str).str.strip().str.upper()
    df["as_of_date"] = pd.to_datetime(df["as_of_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    df = df.merge(track_inputs, on=["symbol", "as_of_date"], how="left")
    print(f"Input category rows: {len(df)}")
    out = build_final_vqgrs_scores_df(df)
    print(f"Output final-score rows: {len(out)}")

    parquet_out = output_dir / "final_vqgrs_scores_latest.parquet"
    csv_out = output_dir / "final_vqgrs_scores_latest.csv"
    _save_df(out, parquet_out, csv_out)
    print(f"Saved: {parquet_out}")


if __name__ == "__main__":
    main()

