# -*- coding: utf-8 -*-
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class FactorSpec:
    name: str
    category: str  # V/Q/G/R/S/STI
    direction: str  # "higher_better" / "lower_better"
    tier: str  # "core" / "aux"
    weight: float
    enabled: bool
    use_log_scale: bool
    neutral_on_missing: bool
    notes: str | None = None
    # Missing-value handling (policy only; scoring logic consumes these in later steps.)
    # missing_policy: structural_skip | donor_shrink | neutral_shrink | drop
    missing_policy: str = "donor_shrink"
    missing_prior_group: str | None = None  # e.g. same_category | same_factor | same_sector_size
    structural_missing_rule: str | None = None  # e.g. pe_nonpositive_eps | peg_invalid_growth
    min_donor_count: int = 5
    max_donor_count: int = 20
    missing_penalty_floor: float | None = None
    # Evidence-first pipeline knobs (score mapping only at final stage).
    use_evidence_pipeline: bool = True
    evidence_prior: float = 0.0
    evidence_beta: float | None = None
    category_weight: float | None = None
    allow_donor_in_evidence: bool = True
    structural_missing_to_prior_only: bool = True


def _spec(
    *,
    name: str,
    category: str,
    direction: str,
    tier: str,
    weight: float,
    enabled: bool = True,
    use_log_scale: bool = False,
    neutral_on_missing: bool = True,
    notes: str | None = None,
    missing_policy: str = "donor_shrink",
    missing_prior_group: str | None = None,
    structural_missing_rule: str | None = None,
    min_donor_count: int = 5,
    max_donor_count: int = 20,
    missing_penalty_floor: float | None = None,
    use_evidence_pipeline: bool = True,
    evidence_prior: float = 0.0,
    evidence_beta: float | None = None,
    category_weight: float | None = None,
    allow_donor_in_evidence: bool = True,
    structural_missing_to_prior_only: bool = True,
) -> FactorSpec:
    return FactorSpec(
        name=name,
        category=category,
        direction=direction,
        tier=tier,
        weight=weight,
        enabled=enabled,
        use_log_scale=use_log_scale,
        neutral_on_missing=neutral_on_missing,
        notes=notes,
        missing_policy=missing_policy,
        missing_prior_group=missing_prior_group,
        structural_missing_rule=structural_missing_rule,
        min_donor_count=min_donor_count,
        max_donor_count=max_donor_count,
        missing_penalty_floor=missing_penalty_floor,
        use_evidence_pipeline=use_evidence_pipeline,
        evidence_prior=evidence_prior,
        evidence_beta=evidence_beta,
        category_weight=category_weight,
        allow_donor_in_evidence=allow_donor_in_evidence,
        structural_missing_to_prior_only=structural_missing_to_prior_only,
    )


_CORE_W = 1.0
_AUX_W = 0.5
DEFAULT_EVIDENCE_PRIOR = 0.0
DEFAULT_SCORE_BETA = 0.7
CATEGORY_SCORE_BETA: dict[str, float] = {
    "V": 0.7,
    "Q": 0.7,
    "G": 0.7,
    "R": 0.7,
    "S": 0.7,
    "STI": 0.7,
}


# V
_V_FACTORS: list[str] = [
    "P/E",
    "P/S",
    "EV/EBITDA",
    "EV/Sales",
    "P/B",
    "P/FCF",
    "Forward P/E",
    "PEG",
]

# Q
_Q_FACTORS: list[str] = [
    "ROIC",
    "ROE",
    "ROA",
    "Gross Margin",
    "Oper. Margin",
    "Profit Margin",
    "OCF/NI",
    "Cash/sh",
    "EPS (ttm)",
]

# G
_G_FACTORS: list[str] = [
    "Revenue YoY",
    "EPS YoY",
    "OCF YoY",
    "EPS This Y",
    "EPS Next Y",
    "EPS Next Q",
    "EPS Next 5Y",
]

# R
_R_FACTORS: list[str] = [
    "Debt/Eq",
    "LT Debt/Eq",
    "Quick Ratio",
    "Current Ratio",
    "Interest Coverage",
    "Cash/sh",
    "Shs Outstand",
]

# S
_S_FACTORS: list[str] = [
    "Beta",
    "Volatility",
    "OPM volatility",
    "ATR(14)",
    "Perf 3Y",
    "Perf 5Y",
    "Dividend Gr. 3Y",
    "Dividend Gr. 5Y",
]

# STI
_STI_FACTORS: list[str] = [
    "Price",
    "Prev Close",
    "Change",
    "Volume",
    "Avg Volume",
    "Rel Volume",
    "SMA20",
    "SMA50",
    "SMA200",
    "52W High",
    "52W Low",
    "RSI(14)",
    "Perf Week",
    "Perf Month",
    "Perf Quarter",
    "Perf Half Y",
    "Perf Year",
    "Perf YTD",
    "Target Price",
    "Insider Trans",
]


FACTOR_SPECS: dict[str, FactorSpec] = {}

# V: all lower_better, core, log-scale
for n in _V_FACTORS:
    if n == "P/E":
        _mp, _smr = "structural_skip", "pe_nonpositive_eps"
    elif n == "PEG":
        _mp, _smr = "structural_skip", "peg_invalid_growth"
    else:
        _mp, _smr = "donor_shrink", None
    FACTOR_SPECS[n] = _spec(
        name=n,
        category="V",
        direction="lower_better",
        tier="core",
        weight=_CORE_W,
        enabled=True,
        use_log_scale=n in {"P/E", "P/S", "EV/EBITDA", "EV/Sales", "P/B", "P/FCF", "Forward P/E", "PEG"},
        neutral_on_missing=True,
        notes=None,
        missing_policy=_mp,
        structural_missing_rule=_smr,
    )

# Q
for n in _Q_FACTORS:
    if n in {"Cash/sh", "EPS (ttm)"}:
        _mp = "neutral_shrink" if n == "EPS (ttm)" else "donor_shrink"
        _mpg = "same_category" if n == "EPS (ttm)" else None
        FACTOR_SPECS[n] = _spec(
            name=n,
            category="Q",
            direction="higher_better",
            tier="aux",
            weight=_AUX_W,
            enabled=True,
            use_log_scale=False,
            neutral_on_missing=True,
            notes=None,
            missing_policy=_mp,
            missing_prior_group=_mpg,
        )
    else:
        FACTOR_SPECS[n] = _spec(
            name=n,
            category="Q",
            direction="higher_better",
            tier="core",
            weight=_CORE_W,
            enabled=True,
            use_log_scale=False,
            neutral_on_missing=True,
            notes=None,
        )

# G: all higher_better, core
for n in _G_FACTORS:
    FACTOR_SPECS[n] = _spec(
        name=n,
        category="G",
        direction="higher_better",
        tier="core",
        weight=_CORE_W,
        enabled=True,
        use_log_scale=False,
        neutral_on_missing=True,
        notes=None,
    )

# R: mixed directions, aux for Cash/sh + Shs Outstand
for n in _R_FACTORS:
    if n in {"Debt/Eq", "LT Debt/Eq"}:
        FACTOR_SPECS[n] = _spec(
            name=n,
            category="R",
            direction="lower_better",
            tier="core",
            weight=_CORE_W,
            enabled=True,
            use_log_scale=False,
            neutral_on_missing=True,
        )
    elif n in {"Quick Ratio", "Current Ratio", "Interest Coverage"}:
        FACTOR_SPECS[n] = _spec(
            name=n,
            category="R",
            direction="higher_better",
            tier="core",
            weight=_CORE_W,
            enabled=True,
            use_log_scale=False,
            neutral_on_missing=True,
        )
    elif n == "Cash/sh":
        # Cash/sh is also listed under Q; this spec matches the Q aux rule.
        # (category field remains Q as the canonical definition.)
        FACTOR_SPECS[n] = _spec(
            name=n,
            category=FACTOR_SPECS[n].category if n in FACTOR_SPECS else "R",
            direction="higher_better",
            tier="aux",
            weight=_AUX_W,
            enabled=True,
            use_log_scale=False,
            neutral_on_missing=True,
        )
    elif n == "Shs Outstand":
        FACTOR_SPECS[n] = _spec(
            name=n,
            category="R",
            direction="higher_better",
            tier="aux",
            weight=_AUX_W,
            enabled=False,
            use_log_scale=False,
            neutral_on_missing=True,
            notes="dilution rate 없이는 직접 점수화 보류",
            missing_policy="drop",
        )
    else:
        # Defensive fallback: should not happen with the declared list.
        FACTOR_SPECS[n] = _spec(
            name=n,
            category="R",
            direction="higher_better",
            tier="core",
            weight=_CORE_W,
            enabled=True,
            use_log_scale=False,
            neutral_on_missing=True,
            notes="Unclassified R factor (check config).",
        )

# S
for n in _S_FACTORS:
    if n in {"Beta", "Volatility", "OPM volatility"}:
        FACTOR_SPECS[n] = _spec(
            name=n,
            category="S",
            direction="lower_better",
            tier="core",
            weight=_CORE_W,
            enabled=True,
            use_log_scale=False,
            neutral_on_missing=True,
        )
    elif n == "ATR(14)":
        FACTOR_SPECS[n] = _spec(
            name=n,
            category="S",
            direction="lower_better",
            tier="aux",
            weight=_AUX_W,
            enabled=True,
            use_log_scale=False,
            neutral_on_missing=True,
        )
    elif n in {"Perf 3Y", "Perf 5Y", "Dividend Gr. 3Y", "Dividend Gr. 5Y"}:
        FACTOR_SPECS[n] = _spec(
            name=n,
            category="S",
            direction="higher_better",
            tier="aux",
            weight=_AUX_W,
            enabled=True,
            use_log_scale=False,
            neutral_on_missing=True,
        )
    else:
        # Defensive fallback: should not happen with the declared list.
        FACTOR_SPECS[n] = _spec(
            name=n,
            category="S",
            direction="higher_better",
            tier="core",
            weight=_CORE_W,
            enabled=True,
            use_log_scale=False,
            neutral_on_missing=True,
            notes="Unclassified S factor (check config).",
        )

# STI: disable by default, direction judged per item
for n in _STI_FACTORS:
    if n == "RSI(14)":
        FACTOR_SPECS[n] = _spec(
            name=n,
            category="STI",
            direction="higher_better",
            tier="core",
            weight=_CORE_W,
            enabled=False,
            use_log_scale=False,
            neutral_on_missing=True,
            notes="RSI(14): disabled by default (direction/threshold needs final scoring design)",
        )
    else:
        FACTOR_SPECS[n] = _spec(
            name=n,
            category="STI",
            direction="higher_better",
            tier="core",
            weight=_CORE_W,
            enabled=False,
            use_log_scale=False,
            neutral_on_missing=True,
            notes="STI factors: direction requires per-item judgment; disabled by default",
        )


FACTOR_SPECS = dict(FACTOR_SPECS)  # freeze mapping for import safety

MISSING_POLICY_FACTORS: dict[str, list[str]] = {
    "structural_skip": [],
    "donor_shrink": [],
    "neutral_shrink": [],
    "drop": [],
}
for _fname, _fspec in FACTOR_SPECS.items():
    MISSING_POLICY_FACTORS[_fspec.missing_policy].append(_fname)
for _k in MISSING_POLICY_FACTORS:
    MISSING_POLICY_FACTORS[_k].sort()

STRUCTURAL_MISSING_FACTORS: dict[str, str] = dict(
    sorted(
        (
            (_fname, _fspec.structural_missing_rule)
            for _fname, _fspec in FACTOR_SPECS.items()
            if _fspec.structural_missing_rule is not None
        ),
        key=lambda t: t[0],
    )
)

CATEGORY_TO_FACTORS: dict[str, list[str]] = {
    "V": list(_V_FACTORS),
    "Q": list(_Q_FACTORS),
    "G": list(_G_FACTORS),
    "R": list(_R_FACTORS),
    "S": list(_S_FACTORS),
    "STI": list(_STI_FACTORS),
}

CORE_FACTORS_BY_CATEGORY: dict[str, list[str]] = {}
AUX_FACTORS_BY_CATEGORY: dict[str, list[str]] = {}

for cat, names in CATEGORY_TO_FACTORS.items():
    core_names: list[str] = []
    aux_names: list[str] = []
    for n in names:
        spec = FACTOR_SPECS.get(n)
        if spec is None:
            # Should never happen; keep deterministic behavior.
            continue
        if spec.tier == "aux":
            aux_names.append(n)
        else:
            core_names.append(n)
    CORE_FACTORS_BY_CATEGORY[cat] = core_names
    AUX_FACTORS_BY_CATEGORY[cat] = aux_names


# Weight semantics:
# - FactorSpec.weight: aggregation weight inside each category (factor-level evidence aggregation).
# - GROUP_BASE_WEIGHTS: aggregation weights for group A/B/C/D/E contributions.
# - CATEGORY_BASE_WEIGHTS: final category aggregation weights across V/Q/G/R/S/STI.
#
# Pipeline note:
# - Factor step computes signed robust evidence (not final score).
# - Category score is obtained by mapping aggregated category evidence at the final stage (e.g. tanh beta mapping).
# - No additional cross-sectional re-normalization / min-max / percentile normalization between categories.
GROUP_BASE_WEIGHTS: dict[str, float] = {
    "A": 1.0,
    "B": 1.0,
    "C": 1.0,
    "D": 1.0,
    "E": 1.0,
}

CATEGORY_BASE_WEIGHTS: dict[str, float] = {
    "V": 1.0,
    "Q": 1.0,
    "G": 1.0,
    "R": 1.0,
    "S": 1.0,
    "STI": 1.0,
}

