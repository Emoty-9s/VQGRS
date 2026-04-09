# -*- coding: utf-8 -*-
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class FactorSpec:
    name: str
    category: str  # V/Q/G/R/S/STI
    direction: str  # "higher_better" / "lower_better"
    tier: str  # "core" / "aux"
    # Importance tier for downstream category aggregation logic:
    # - "main": category primary indicators (main-factor coverage is handled separately)
    # - "aux": secondary indicators
    # - "drop": disabled factors (enabled=False)
    importance_tier: str  # allowed: main | aux | drop
    # Convenience boolean for easier downstream reads.
    main_factor: bool
    weight: float
    enabled: bool
    use_log_scale: bool
    neutral_on_missing: bool
    notes: str | None = None
    # Missing-value handling (policy only; scoring logic consumes these in later steps.)
    # missing_policy: structural_skip | donor_shrink | neutral_shrink | drop
    missing_policy: str = "donor_shrink"
    missing_prior_group: str | None = None  # e.g. same_category | same_factor | same_sector_size
    structural_missing_rule: str | None = None  # e.g. pe_nonpositive_eps | forward_pe_nonpositive_estimate | ...
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
    # Absolute (anchor) evaluation — factor-owned; hybrid relative+absolute uses these at factor stage.
    absolute_enabled: bool = False
    absolute_weight: float = 0.0
    absolute_mode: str | None = None
    absolute_good: float | None = None
    absolute_neutral: float | None = None
    absolute_bad: float | None = None
    absolute_cap: float = 3.0


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
    absolute_enabled: bool | None = None,
    absolute_weight: float | None = None,
    absolute_mode: str | None = None,
    absolute_good: float | None = None,
    absolute_neutral: float | None = None,
    absolute_bad: float | None = None,
    absolute_cap: float | None = None,
) -> FactorSpec:
    # Main/aux importance tier + weight are derived from (category, name, enabled).
    # This enables downstream main-factor coverage handling without breaking existing config structure.
    is_main = name in MAIN_FACTORS_BY_CATEGORY.get(category, [])
    importance_tier = "main" if is_main else ("drop" if not enabled else "aux")
    main_factor = is_main
    computed_weight = (
        MAIN_AUX_WEIGHT_POLICY["main_weight"]
        if importance_tier == "main"
        else MAIN_AUX_WEIGHT_POLICY["aux_weight"]
    )
    dcat = ABSOLUTE_DEFAULTS_BY_CATEGORY.get(category)
    if is_main and dcat is not None:
        if absolute_enabled is None:
            absolute_enabled = True
        if absolute_weight is None:
            absolute_weight = float(dcat["absolute_weight"])
        if absolute_mode is None:
            absolute_mode = dcat.get("absolute_mode")
        if absolute_good is None:
            absolute_good = dcat.get("absolute_good")
        if absolute_neutral is None:
            absolute_neutral = dcat.get("absolute_neutral")
        if absolute_bad is None:
            absolute_bad = dcat.get("absolute_bad")
        if absolute_cap is None:
            absolute_cap = float(dcat.get("absolute_cap", 3.0))
    else:
        if absolute_enabled is None:
            absolute_enabled = False
        if absolute_weight is None:
            absolute_weight = 0.0
        if absolute_mode is None:
            absolute_mode = None
        if absolute_good is None:
            absolute_good = None
        if absolute_neutral is None:
            absolute_neutral = None
        if absolute_bad is None:
            absolute_bad = None
        if absolute_cap is None:
            absolute_cap = 3.0
    return FactorSpec(
        name=name,
        category=category,
        direction=direction,
        tier=tier,
        importance_tier=importance_tier,
        main_factor=main_factor,
        weight=computed_weight,
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
        absolute_enabled=absolute_enabled,
        absolute_weight=absolute_weight,
        absolute_mode=absolute_mode,
        absolute_good=absolute_good,
        absolute_neutral=absolute_neutral,
        absolute_bad=absolute_bad,
        absolute_cap=absolute_cap,
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


# ---------------------------------------------------------------------------
# Main/Aux factor importance tiers
# ---------------------------------------------------------------------------
# Philosophy: **main** factors are the minimal core that directly represent the category
# thesis; **aux** factors add explanatory power but are noisier or more supplementary.
# Category score machinery uses main vs aux for coverage and weighting (main_weight /
# aux_weight below); missing mains shrink evidence more conservatively.
# **S (risk / swing):** mains are Beta and Volatility—direct measures of how much the
# stock moves vs the market and in absolute terms. Long-horizon return (e.g. Perf 3Y)
# and operating-margin volatility stay aux as reinforcement, not the core S thesis.
#
# weight policy:
# - main factor weight = 1.5
# - aux factor weight = 0.75
#
MAIN_AUX_WEIGHT_POLICY = {
    "main_weight": 1.5,
    "aux_weight": 0.75,
}

# Canonical mains per category; all other factors in CATEGORY_TO_FACTORS for that category are aux.
# ROE and Debt/Eq remain listed as mains for layout / weighting, but when the issuer is in a structural-invalid
# regime (e.g. nonpositive equity), build_factors_latest leaves the raw factor as NaN and scoring treats that as
# structural_skip—not a neutral “missing donor” and not good/bad—because the ratio is not economically interpretable.
MAIN_FACTORS_BY_CATEGORY: dict[str, list[str]] = {
    "V": ["P/E", "EV/EBITDA", "P/S"],
    "Q": ["ROIC", "Oper. Margin", "ROE"],
    "G": ["Revenue YoY", "EPS YoY", "EPS This Y"],
    "R": ["Debt/Eq", "Current Ratio", "Interest Coverage"],
    "S": ["Beta", "Volatility"],
    "STI": [],
}

# Category-level defaults for absolute evaluation. Merged in _spec() for main factors; resolved values live on FactorSpec.
# Thresholds (good/neutral/bad) are placeholders until calibrated; scoring primitives consume FactorSpec fields only.
ABSOLUTE_DEFAULTS_BY_CATEGORY: dict[str, dict[str, float | str | None]] = {
    "V": {
        "absolute_weight": 0.5,
        "absolute_mode": "anchor_band",
        "absolute_good": None,
        "absolute_neutral": None,
        "absolute_bad": None,
        "absolute_cap": 3.0,
    },
    "Q": {
        "absolute_weight": 0.5,
        "absolute_mode": "anchor_band",
        "absolute_good": None,
        "absolute_neutral": None,
        "absolute_bad": None,
        "absolute_cap": 3.0,
    },
    "G": {
        "absolute_weight": 0.5,
        "absolute_mode": "anchor_band",
        "absolute_good": None,
        "absolute_neutral": None,
        "absolute_bad": None,
        "absolute_cap": 3.0,
    },
    "R": {
        "absolute_weight": 0.5,
        "absolute_mode": "anchor_band",
        "absolute_good": None,
        "absolute_neutral": None,
        "absolute_bad": None,
        "absolute_cap": 3.0,
    },
    "S": {
        "absolute_weight": 0.5,
        "absolute_mode": "anchor_band",
        "absolute_good": None,
        "absolute_neutral": None,
        "absolute_bad": None,
        "absolute_cap": 3.0,
    },
}


FACTOR_SPECS: dict[str, FactorSpec] = {}

# V: all lower_better, core, log-scale
for n in _V_FACTORS:
    _notes: str | None = None
    if n == "P/E":
        _mp, _smr = "structural_skip", "pe_nonpositive_eps"
    elif n == "PEG":
        _mp, _smr = "structural_skip", "peg_invalid_growth"
    elif n == "Forward P/E":
        _mp, _smr = "structural_skip", "forward_pe_nonpositive_estimate"
    elif n == "P/FCF":
        _mp, _smr = "structural_skip", "pfcf_nonpositive_or_invalid"
    elif n == "EV/EBITDA":
        _mp, _smr = "structural_skip", "ev_ebitda_nonpositive_or_invalid"
    elif n == "P/B":
        _mp, _smr = "structural_skip", "pb_nonpositive_book_value"
        _notes = "Negative or zero book value makes P/B economically non-interpretable."
    elif n == "P/S":
        _mp, _smr = "structural_skip", "ps_nonpositive_sales"
    elif n == "EV/Sales":
        _mp, _smr = "structural_skip", "ev_sales_nonpositive_sales"
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
        notes=_notes,
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
    elif n == "ROIC":
        FACTOR_SPECS[n] = _spec(
            name=n,
            category="Q",
            direction="higher_better",
            tier="core",
            weight=_CORE_W,
            enabled=True,
            use_log_scale=False,
            neutral_on_missing=True,
            notes=(
                "ROIC is scored only when operating invested capital is available and positive; "
                "cash-heavy denominator distortions are excluded at factor build and re-checked here."
            ),
            missing_policy="structural_skip",
            structural_missing_rule="roic_invalid_invested_capital",
        )
    elif n == "ROE":
        FACTOR_SPECS[n] = _spec(
            name=n,
            category="Q",
            direction="higher_better",
            tier="core",
            weight=_CORE_W,
            enabled=True,
            use_log_scale=False,
            neutral_on_missing=True,
            notes=(
                "ROE with nonpositive equity is structurally invalid; negative ROE with positive equity remains observed."
            ),
            missing_policy="structural_skip",
            structural_missing_rule="roe_nonpositive_equity",
        )
    elif n == "OCF/NI":
        FACTOR_SPECS[n] = _spec(
            name=n,
            category="Q",
            direction="higher_better",
            tier="core",
            weight=_CORE_W,
            enabled=True,
            use_log_scale=False,
            neutral_on_missing=True,
            notes="OCF/NI is only meaningful when denominator NI is positive.",
            missing_policy="structural_skip",
            structural_missing_rule="ocfni_nonpositive_net_income",
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
    if n == "EPS Next Y":
        FACTOR_SPECS[n] = _spec(
            name=n,
            category="G",
            direction="higher_better",
            tier="core",
            weight=_CORE_W,
            enabled=False,
            use_log_scale=False,
            neutral_on_missing=True,
            notes=(
                "Currently raw next-year analyst EPS estimate level, not a normalized growth metric; "
                "temporarily disabled until next-year growth definition is finalized."
            ),
            missing_policy="drop",
        )
    elif n == "EPS YoY":
        FACTOR_SPECS[n] = _spec(
            name=n,
            category="G",
            direction="higher_better",
            tier="core",
            weight=_CORE_W,
            enabled=True,
            use_log_scale=False,
            neutral_on_missing=True,
            notes=(
                "Valid only when current and prior EPS TTM are both positive; "
                "turnaround / loss regimes are excluded from standard YoY growth scoring."
            ),
            missing_policy="structural_skip",
            structural_missing_rule="eps_yoy_nonpositive_regime",
        )
    elif n == "EPS This Y":
        FACTOR_SPECS[n] = _spec(
            name=n,
            category="G",
            direction="higher_better",
            tier="core",
            weight=_CORE_W,
            enabled=True,
            use_log_scale=False,
            neutral_on_missing=True,
            notes="Growth rate requires positive base actual EPS.",
            missing_policy="structural_skip",
            structural_missing_rule="eps_this_y_nonpositive_base_actual",
        )
    else:
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
    if n == "Debt/Eq":
        FACTOR_SPECS[n] = _spec(
            name=n,
            category="R",
            direction="lower_better",
            tier="core",
            weight=_CORE_W,
            enabled=True,
            use_log_scale=False,
            neutral_on_missing=True,
            missing_policy="structural_skip",
            structural_missing_rule="de_ratio_nonpositive_equity",
        )
    elif n == "LT Debt/Eq":
        FACTOR_SPECS[n] = _spec(
            name=n,
            category="R",
            direction="lower_better",
            tier="core",
            weight=_CORE_W,
            enabled=True,
            use_log_scale=False,
            neutral_on_missing=True,
            missing_policy="structural_skip",
            structural_missing_rule="lt_de_ratio_nonpositive_equity",
        )
    elif n in {"Quick Ratio", "Current Ratio"}:
        # Liquidity / coverage ratios are highly skewed; log-scale relative scoring compresses extremes.
        # This reduces outsized advantage from cash-heavy pre-revenue names vs typical operating balance sheets.
        FACTOR_SPECS[n] = _spec(
            name=n,
            category="R",
            direction="higher_better",
            tier="core",
            weight=_CORE_W,
            enabled=True,
            use_log_scale=True,
            neutral_on_missing=True,
        )
    elif n == "Interest Coverage":
        FACTOR_SPECS[n] = _spec(
            name=n,
            category="R",
            direction="higher_better",
            tier="core",
            weight=_CORE_W,
            enabled=True,
            use_log_scale=True,
            neutral_on_missing=True,
            missing_policy="structural_skip",
            structural_missing_rule="interest_coverage_nonpositive_interest_expense",
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
        # Legacy export (kept for backward compatibility): based on spec.tier ("core"/"aux").
        if spec.tier != "aux":
            core_names.append(n)
        # New export: enabled non-main factors are "aux" by importance_tier.
        if spec.importance_tier == "aux" and spec.enabled:
            aux_names.append(n)
    CORE_FACTORS_BY_CATEGORY[cat] = core_names
    AUX_FACTORS_BY_CATEGORY[cat] = aux_names


def summarize_main_aux_layout() -> dict[str, dict[str, list[str]]]:
    """Per category: main / aux / drop factor names from resolved FactorSpec.importance_tier."""
    out: dict[str, dict[str, list[str]]] = {}
    for cat, names in CATEGORY_TO_FACTORS.items():
        main: list[str] = []
        aux: list[str] = []
        drop: list[str] = []
        for n in names:
            spec = FACTOR_SPECS[n]
            if spec.importance_tier == "main":
                main.append(n)
            elif spec.importance_tier == "aux":
                aux.append(n)
            else:
                drop.append(n)
        main_order = MAIN_FACTORS_BY_CATEGORY.get(cat, [])
        main_set = set(main)
        main_sorted = [m for m in main_order if m in main_set]
        for m in sorted(main_set - set(main_sorted)):
            main_sorted.append(m)
        out[cat] = {
            "main": main_sorted,
            "aux": sorted(aux),
            "drop": sorted(drop),
        }
    return out


def validate_main_aux_layout() -> None:
    """Sanity checks for MAIN_FACTORS_BY_CATEGORY vs resolved specs (run from __main__ only)."""
    layout = summarize_main_aux_layout()
    s_main = set(layout["S"]["main"])
    assert "Beta" in s_main
    assert "Volatility" in s_main
    assert "Perf 3Y" not in s_main
    assert "OPM volatility" not in s_main
    q_main = set(layout["Q"]["main"])
    assert "ROE" in q_main
    assert "OCF/NI" not in q_main
    g_main = set(layout["G"]["main"])
    assert "EPS This Y" in g_main
    assert "OCF YoY" not in g_main


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


if __name__ == "__main__":
    validate_main_aux_layout()
    _layout = summarize_main_aux_layout()
    _order = ("V", "Q", "G", "R", "S", "STI")
    for _cat in _order:
        if _cat not in _layout:
            continue
        _buckets = _layout[_cat]
        print(f"[{_cat}] main ({len(_buckets['main'])}): {_buckets['main']}")
        print(f"     aux ({len(_buckets['aux'])}): {_buckets['aux']}")
        print(f"     drop ({len(_buckets['drop'])}): {_buckets['drop']}")
        print()

