# -*- coding: utf-8 -*-
"""One-off: liquidity ratio linear vs log relative_evidence. Delete after use."""
from __future__ import annotations

import math
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

from score_primitives import (  # noqa: E402
    _prepare_relative_inputs_for_scoring,
    _signed_log1p,
    compute_signed_evidence,
)

FACTORS = ["Quick Ratio", "Current Ratio", "Interest Coverage"]


def peer_stats(s: pd.Series) -> dict:
    x = pd.to_numeric(s, errors="coerce").replace([np.inf, -np.inf], np.nan).dropna()
    if len(x) < 20:
        return {}
    return {
        "median": float(x.median()),
        "q25": float(x.quantile(0.25)),
        "q75": float(x.quantile(0.75)),
        "iqr": float(x.quantile(0.75) - x.quantile(0.25)),
        "n": int(len(x)),
    }


def rel_linear(raw: float, st: dict) -> float | None:
    return compute_signed_evidence(
        raw_value=raw,
        median_value=st.get("median"),
        iqr_value=st.get("iqr"),
        direction="higher_better",
    )


def rel_log(raw: float, st: dict) -> float | None:
    rr, mm, iq, _, _ = _prepare_relative_inputs_for_scoring(
        raw,
        st.get("median"),
        st.get("q25"),
        st.get("q75"),
        st.get("iqr"),
        use_log_scale=True,
    )
    return compute_signed_evidence(
        raw_value=rr,
        median_value=mm,
        iqr_value=iq,
        direction="higher_better",
    )


def main() -> None:
    pq = ROOT / "data" / "factors_latest.parquet"
    fac = pd.read_parquet(pq)
    fac["symbol"] = fac["symbol"].astype(str).str.strip().str.upper()
    latest = pd.to_datetime(fac["asOfDate"], errors="coerce").max()
    snap = fac.loc[pd.to_datetime(fac["asOfDate"], errors="coerce") == latest].copy()
    print("=== Data ===", pq.name, "| rows", len(snap), "| asOfDate", latest)

    stats_by_f: dict[str, dict] = {}
    for f in FACTORS:
        stats_by_f[f] = peer_stats(snap[f])

    # Sample: NNE + top by max of z-scored quick/current/coverage
    m = pd.to_numeric(snap["Quick Ratio"], errors="coerce")
    c = pd.to_numeric(snap["Current Ratio"], errors="coerce")
    ic = pd.to_numeric(snap["Interest Coverage"], errors="coerce")
    ext = (m.fillna(0) + c.fillna(0) + ic.fillna(0)).rank(pct=True)
    pool = snap.loc[ext >= 0.97, "symbol"].tolist()
    picked = ["NNE"] if "NNE" in snap["symbol"].values else []
    for s in pool:
        if s not in picked and len(picked) < 10:
            picked.append(s)
    for s in snap["symbol"].tolist():
        if len(picked) >= 10:
            break
        if s not in picked:
            picked.append(s)

    print("\n=== C. Sanity: T(x)=sign(x)*log1p(|x|) for x>0 ===")
    for x in (2.0, 20.0, 134.0):
        print(f"  x={x:>3}  T(x)={_signed_log1p(x):.6f}")
    d20 = _signed_log1p(20.0) - _signed_log1p(2.0)
    d134_20 = _signed_log1p(134.0) - _signed_log1p(20.0)
    print(f"  T(20)-T(2)={d20:.4f}  T(134)-T(20)={d134_20:.4f}  ratio latter/former={(d134_20/d20):.3f}")

    print("\n=== Synthetic peer: median=10, q25=5, q75=20 (iqr=15) ===")
    st = {"median": 10.0, "q25": 5.0, "q75": 20.0, "iqr": 15.0}
    tq25, tq75 = _signed_log1p(5.0), _signed_log1p(20.0)
    print(f"  T(q25)={tq25:.4f} T(q75)={tq75:.4f} transformed_iqr={tq75-tq25:.4f}")
    for x in (2.0, 20.0, 134.0):
        lin = rel_linear(x, st)
        lg = rel_log(x, st)
        print(f"  x={x:>3}  rel_linear={lin:.4f}  rel_log={lg:.4f}  delta_log_minus_lin={(lg-lin) if lg is not None and lin is not None else None}")

    print("\n=== A. Factor level (universe peer @ latest; higher_better) ===")
    for sym in picked[:10]:
        row = snap.set_index("symbol").loc[sym]
        print(f"\n--- {sym} ---")
        for f in FACTORS:
            st = stats_by_f[f]
            if not st:
                continue
            raw = pd.to_numeric(row.get(f), errors="coerce")
            if pd.isna(raw):
                print(f"  {f}: raw=NaN")
                continue
            raw = float(raw)
            lin = rel_linear(raw, st)
            lg = rel_log(raw, st)
            tm = _signed_log1p(st["median"])
            tq25, tq75 = _signed_log1p(st["q25"]), _signed_log1p(st["q75"])
            tiqr = (tq75 - tq25) if tq25 is not None and tq75 is not None else None
            print(
                f"  {f}: raw={raw:.4g} T(raw)={_signed_log1p(raw):.4f} | "
                f"med={st['median']:.4g} T(med)={tm:.4f} | "
                f"iqr={st['iqr']:.4g} T_iqr={tiqr:.4f} | "
                f"rel_lin={lin} rel_log={lg}"
            )

    print("\n=== B. Category level ===")
    print(
        "Stored outputs are from whatever pipeline last wrote CSVs; "
        "backup '복사본' differs by track too — not a clean pre-log A/B."
    )
    cur = pd.read_csv(ROOT / "output" / "scoring" / "final_vqgrs_scores_latest.csv")
    cur["symbol"] = cur["symbol"].astype(str).str.upper()
    sub = cur.set_index("symbol").reindex(picked[:10])
    for sym in picked[:10]:
        if sym not in sub.index or not isinstance(sub.loc[sym], pd.Series):
            continue
        r = sub.loc[sym]
        print(
            f"  {sym}: score_R={r.get('score_R')} final_score={r.get('final_score')} track={r.get('assigned_track')}"
        )


if __name__ == "__main__":
    main()
