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


def _load_track_inputs_from_snapshot(input_dir: Path) -> pd.DataFrame:
    """
    Load track assignment raw inputs from unified snapshot (or legacy group_cde path).
    Keeps one row per (symbol, as_of_date).
    """
    candidates = [
        input_dir / "group_unified" / "group_unified_snapshot_latest.parquet",
        input_dir / "group_unified" / "group_unified_snapshot_latest.csv",
        input_dir / "group_cde" / "group_cde_snapshot_latest.parquet",
        input_dir / "group_cde" / "group_cde_snapshot_latest.csv",
    ]
    snap = pd.DataFrame()
    for p in candidates:
        if p.exists():
            snap = _read_df(p)
            if not snap.empty:
                break
    if snap.empty or "symbol" not in snap.columns or "as_of_date" not in snap.columns:
        return pd.DataFrame(columns=["symbol", "as_of_date", "track_input_roic", "track_input_revenue_yoy", "track_input_rule_of_40", "track_input_v_market"])

    for c in ("ROIC", "Revenue YoY", "Rule of 40", "V_market"):
        if c not in snap.columns:
            snap[c] = np.nan

    out = snap[["symbol", "as_of_date", "ROIC", "Revenue YoY", "Rule of 40", "V_market"]].copy()
    out["symbol"] = out["symbol"].astype(str).str.strip().str.upper()
    out["as_of_date"] = pd.to_datetime(out["as_of_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    out["ROIC"] = pd.to_numeric(out["ROIC"], errors="coerce")
    out["Revenue YoY"] = pd.to_numeric(out["Revenue YoY"], errors="coerce")
    out["Rule of 40"] = pd.to_numeric(out["Rule of 40"], errors="coerce")
    out["V_market"] = pd.to_numeric(out["V_market"], errors="coerce")

    out = (
        out.groupby(["symbol", "as_of_date"], dropna=False, as_index=False)
        .agg(
            {
                "ROIC": "median",
                "Revenue YoY": "median",
                "Rule of 40": "median",
                "V_market": "median",
            }
        )
        .rename(
            columns={
                "ROIC": "track_input_roic",
                "Revenue YoY": "track_input_revenue_yoy",
                "Rule of 40": "track_input_rule_of_40",
                "V_market": "track_input_v_market",
            }
        )
    )
    return out


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
            reasons.append("B: Q>=80,R>=70,S>=70,ROIC>=15")
            continue
        if bool(r.get("is_track_C_candidate", False)):
            reasons.append("C: Revenue YoY>=20, Rule of 40>=30")
            continue
        if bool(r.get("is_track_A_candidate", False)):
            reasons.append("A: V>=70, V_market>=50")
            continue

        missing: list[str] = []
        for c in ("track_input_roic", "track_input_revenue_yoy", "track_input_rule_of_40", "track_input_v_market"):
            if pd.isna(pd.to_numeric(r.get(c), errors="coerce")):
                missing.append(c.replace("track_input_", ""))
        if missing:
            reasons.append("N: missing_inputs=" + ",".join(missing))
        else:
            reasons.append("N: thresholds_not_met")
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
    for c in ("track_input_roic", "track_input_revenue_yoy", "track_input_rule_of_40", "track_input_v_market"):
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

    # Track inputs from visible category scores.
    out["track_input_V"] = pd.to_numeric(out["score_V"], errors="coerce")
    out["track_input_Q"] = pd.to_numeric(out["score_Q"], errors="coerce")
    out["track_input_R"] = pd.to_numeric(out["score_R"], errors="coerce")
    out["track_input_S"] = pd.to_numeric(out["score_S"], errors="coerce")

    # Track inputs from external raw-source join (snapshot), injected in main().
    for c in ("track_input_roic", "track_input_revenue_yoy", "track_input_rule_of_40", "track_input_v_market"):
        if c not in out.columns:
            out[c] = np.nan

    q = pd.to_numeric(out["track_input_Q"], errors="coerce")
    r = pd.to_numeric(out["track_input_R"], errors="coerce")
    s = pd.to_numeric(out["track_input_S"], errors="coerce")
    v = pd.to_numeric(out["track_input_V"], errors="coerce")
    roic = pd.to_numeric(out["track_input_roic"], errors="coerce")
    rev = pd.to_numeric(out["track_input_revenue_yoy"], errors="coerce")
    rule40 = pd.to_numeric(out["track_input_rule_of_40"], errors="coerce")
    v_market = pd.to_numeric(out["track_input_v_market"], errors="coerce")

    out["is_track_B_candidate"] = ((q >= 80.0) & (r >= 70.0) & (s >= 70.0) & (roic >= 15.0)).astype(bool)
    out["is_track_C_candidate"] = ((rev >= 20.0) & (rule40 >= 30.0)).astype(bool)
    out["is_track_A_candidate"] = ((v >= 70.0) & (v_market >= 50.0)).astype(bool)

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
        "is_track_A_candidate",
        "is_track_B_candidate",
        "is_track_C_candidate",
        "track_input_V",
        "track_input_Q",
        "track_input_R",
        "track_input_S",
        "track_input_roic",
        "track_input_revenue_yoy",
        "track_input_rule_of_40",
        "track_input_v_market",
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
    track_inputs = _load_track_inputs_from_snapshot(input_dir)
    if not track_inputs.empty:
        print(f"Track input rows from snapshot: {len(track_inputs)}")
        df = df.copy()
        df["symbol"] = df["symbol"].astype(str).str.strip().str.upper()
        df["as_of_date"] = pd.to_datetime(df["as_of_date"], errors="coerce").dt.strftime("%Y-%m-%d")
        df = df.merge(track_inputs, on=["symbol", "as_of_date"], how="left")
    else:
        print("WARNING: No track raw inputs found in snapshot; C/A track conditions use conservative missing=False.")
    print(f"Input category rows: {len(df)}")
    out = build_final_vqgrs_scores_df(df)
    print(f"Output final-score rows: {len(out)}")

    parquet_out = output_dir / "final_vqgrs_scores_latest.parquet"
    csv_out = output_dir / "final_vqgrs_scores_latest.csv"
    _save_df(out, parquet_out, csv_out)
    print(f"Saved: {parquet_out}")


if __name__ == "__main__":
    main()

