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
    )


_CORE_W = 1.0
_AUX_W = 0.5


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
    )

# Q
for n in _Q_FACTORS:
    if n in {"Cash/sh", "EPS (ttm)"}:
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


# Group base weights (A/B/C/D/E); factor weights are in FactorSpec.weight.
GROUP_BASE_WEIGHTS: dict[str, float] = {
    "A": 1.0,
    "B": 1.0,
    "C": 1.0,
    "D": 1.0,
    "E": 1.0,
}

