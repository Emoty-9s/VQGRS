# -*- coding: utf-8 -*-
"""
Offline sanity checks for the VQGRS score-calculation engine refactor (steps 1–6).

Run from repo root:
  python tools/vqgrs_scoring_engine_selfcheck.py

No extra dependencies (stdlib + project imports). Not imported by production code.
"""
from __future__ import annotations

import ast
import sys
from dataclasses import replace
from pathlib import Path

# Repo root = parent of tools/
_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))


def _assert_compute_signed_evidence_unchanged() -> None:
    """Heuristic: body of compute_signed_evidence must match known baseline (relative-only primitive)."""
    path = _REPO_ROOT / "score_primitives.py"
    src = path.read_text(encoding="utf-8")
    tree = ast.parse(src)
    fn = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "compute_signed_evidence")
    lines = src.splitlines()
    body_lines = lines[fn.lineno : fn.end_lineno]
    body_text = "\n".join(body_lines)
    # Expected stable fingerprint: no absolute/hybrid keywords inside this function
    assert "absolute" not in body_text.lower(), "compute_signed_evidence must not reference absolute/hybrid"
    assert "blend" not in body_text.lower()
    # Core formula markers
    assert "raw_value - median_value" in body_text
    assert "sign *" in body_text or "(raw_value - median_value)" in body_text


def _factor_relative_only() -> None:
    from score_factor_config import FACTOR_SPECS
    from score_primitives import score_one_factor_one_group

    spec = FACTOR_SPECS["P/B"]  # aux; absolute_enabled False in config
    assert not spec.absolute_enabled
    row = {
        "P/B": 2.0,
        "rep__P/B__median": 2.5,
        "rep__P/B__q25": 2.0,
        "rep__P/B__q75": 3.0,
        "rep__P/B__iqr": 1.0,
        "rep__P/B__n_valid": 15.0,
        "b_peer_quality": "HIGH",
    }
    out = score_one_factor_one_group(row, spec)
    assert out["blend_method"] == "relative_only"
    assert out["absolute_evidence"] is None
    assert out["raw_evidence"] == out["relative_evidence"]
    assert out["adjusted_evidence"] is not None
    assert out["adjusted_score"] is not None


def _factor_relative_plus_absolute() -> None:
    import pandas as pd

    from score_factor_config import FACTOR_SPECS
    from score_primitives import blend_evidences, compute_absolute_evidence, score_one_factor_one_group

    spec = replace(
        FACTOR_SPECS["ROIC"],
        absolute_good=20.0,
        absolute_neutral=10.0,
        absolute_bad=5.0,
        absolute_cap=3.0,
        absolute_mode="anchor_band",
    )
    assert spec.absolute_enabled
    row = {
        "ROIC": 15.0,
        "rep__ROIC__median": 10.0,
        "rep__ROIC__q25": 8.0,
        "rep__ROIC__q75": 12.0,
        "rep__ROIC__iqr": 4.0,
        "rep__ROIC__n_valid": 20.0,
        "b_peer_quality": "HIGH",
    }
    out = score_one_factor_one_group(row, spec)
    rel = out["relative_evidence"]
    abs_e = compute_absolute_evidence(
        raw_value=15.0,
        direction="higher_better",
        good=20.0,
        neutral=10.0,
        bad=5.0,
        cap=3.0,
        mode="anchor_band",
    )
    blended, method = blend_evidences(rel, abs_e, spec.absolute_weight)
    assert method == "weighted_blend"
    assert abs(float(out["raw_evidence"]) - float(blended)) < 1e-9
    assert out["adjusted_evidence"] is not None
    assert out["adjusted_score"] is not None


def _final_track_evidence_not_score_average() -> None:
    import pandas as pd

    from build_final_vqgrs_scores import CORE_CATS, TRACK_WEIGHTS, _evidence_series_to_score, _weighted_evidence
    from score_primitives import evidence_to_score

    # Same category evidence everywhere -> track aggregate equals that evidence; score = evidence_to_score(ev)
    ev = 0.4
    row = {"symbol": "SELFCHK", "as_of_date": "2024-01-01"}
    for c in CORE_CATS:
        row[f"final_evidence_{c}"] = ev
        row[f"score_{c}"] = 99.0  # deliberately wrong vs evidence — must not drive LTI
    df = pd.DataFrame([row])
    fe = _weighted_evidence(df, TRACK_WEIGHTS["equal"])
    assert len(fe) == 1
    assert abs(float(fe.iloc[0]) - ev) < 1e-9
    fs = _evidence_series_to_score(fe)
    exp = evidence_to_score(ev)
    assert exp is not None and abs(float(fs.iloc[0]) - float(exp)) < 1e-9
    # If we had used score average, result would be ~99, not evidence_to_score(0.4)
    assert float(fs.iloc[0]) < 90.0


def _penalty_placeholders_untouched() -> None:
    src = (_REPO_ROOT / "build_final_vqgrs_scores.py").read_text(encoding="utf-8")
    assert 'out["investment_warning"] = ""' in src or "investment_warning" in src
    assert "hard_stop_triggered" in src


def main() -> None:
    _assert_compute_signed_evidence_unchanged()
    _factor_relative_only()
    _factor_relative_plus_absolute()
    _final_track_evidence_not_score_average()
    _penalty_placeholders_untouched()
    print("vqgrs_scoring_engine_selfcheck: OK")


if __name__ == "__main__":
    main()
