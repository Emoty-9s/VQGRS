# -*- coding: utf-8 -*-
"""
build_universe_fmp_v2 순차 제외 qualify 최소 검증.

실행:
  py -3 -m unittest tests.test_universe_fmp_v2_qualify -v
  py -3 -c "from tests.test_universe_fmp_v2_qualify import run_universe_fmp_v2_qualify_self_check; run_universe_fmp_v2_qualify_self_check()"
"""
from __future__ import annotations

import sys
import unittest
from pathlib import Path

import pandas as pd

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from build_universe_fmp_v2 import (  # noqa: E402
    DEFAULT_MIN_MARKET_CAP,
    JOINED_QUALIFY_INPUT_COLUMNS,
    REASON_BELOW_MARKET_CAP,
    REASON_ETF,
    REASON_EXCLUDED_TYPE,
    REASON_FUND,
    REASON_INACTIVE,
    REASON_NOT_US_LISTED,
    REASON_PASS,
    SCREENER_EXCHANGES,
    universe_row_reason_code,
)

MIN_MCAP = DEFAULT_MIN_MARKET_CAP
ALLOWED = tuple(SCREENER_EXCHANGES)


def _candidate_row(
    *,
    type_norm: str = "",
    type_raw: str = "",
    company_name: str = "Plain Name Inc",
    symbol: str = "TST",
    is_etf_flag: bool = False,
    is_fund_flag: bool = False,
    is_actively_trading_flag: bool = True,
    exchange_canonical: str = "NASDAQ",
    market_cap: float = 1e9,
) -> pd.Series:
    return pd.Series(
        {
            "symbol": symbol,
            "company_name": company_name,
            "exchange_canonical": exchange_canonical,
            "sector": "",
            "industry": "",
            "market_cap": market_cap,
            "is_etf_flag": is_etf_flag,
            "is_fund_flag": is_fund_flag,
            "is_actively_trading_flag": is_actively_trading_flag,
            "type_raw": type_raw if type_raw else type_norm,
            "type_norm": type_norm,
        },
        dtype=object,
    )


def _reason(row: pd.Series) -> str:
    return universe_row_reason_code(row, MIN_MCAP, ALLOWED)


class V2QualifySequentialExclusionTests(unittest.TestCase):
    """스크리너 게이트 → 선택적 stock-list 타입 제외. 이름/티커 비의존."""

    def test_type_empty_passes(self) -> None:
        """stock-list 타입 없음은 탈락 사유 아님."""
        self.assertEqual(_reason(_candidate_row(type_norm="")), REASON_PASS)

    def test_type_stock_pass(self) -> None:
        self.assertEqual(_reason(_candidate_row(type_norm="stock")), REASON_PASS)

    def test_type_common_stock_pass(self) -> None:
        self.assertEqual(_reason(_candidate_row(type_norm="common stock")), REASON_PASS)

    def test_type_adr_pass(self) -> None:
        self.assertEqual(_reason(_candidate_row(type_norm="adr")), REASON_PASS)

    def test_type_reit_pass(self) -> None:
        self.assertEqual(_reason(_candidate_row(type_norm="reit")), REASON_PASS)

    def test_type_etf_excluded_or_screener_etf(self) -> None:
        self.assertEqual(
            _reason(_candidate_row(type_norm="etf", is_etf_flag=False)),
            REASON_EXCLUDED_TYPE,
        )
        self.assertEqual(
            _reason(_candidate_row(type_norm="stock", is_etf_flag=True)),
            REASON_ETF,
        )

    def test_type_fund_excluded_or_screener_fund(self) -> None:
        self.assertEqual(
            _reason(_candidate_row(type_norm="fund", is_fund_flag=False)),
            REASON_EXCLUDED_TYPE,
        )
        self.assertEqual(
            _reason(_candidate_row(type_norm="stock", is_fund_flag=True)),
            REASON_FUND,
        )

    def test_screener_fund_before_etf(self) -> None:
        """동시에 True 면 FUND 가 먼저 평가."""
        self.assertEqual(
            _reason(_candidate_row(type_norm="stock", is_fund_flag=True, is_etf_flag=True)),
            REASON_FUND,
        )

    def test_multi_type_any_excluded(self) -> None:
        self.assertEqual(
            _reason(_candidate_row(type_norm="stock|warrant")),
            REASON_EXCLUDED_TYPE,
        )

    def test_type_preferred_stock_excluded(self) -> None:
        self.assertEqual(
            _reason(_candidate_row(type_norm="preferred stock")),
            REASON_EXCLUDED_TYPE,
        )

    def test_type_exclusion_tokens(self) -> None:
        for tok in (
            "warrant",
            "unit",
            "right",
            "spac",
            "bond",
            "note",
            "trust",
            "mutual fund",
            "closed-end fund",
            "etn",
            "etp",
            "preferred",
        ):
            with self.subTest(type_norm=tok):
                self.assertEqual(_reason(_candidate_row(type_norm=tok)), REASON_EXCLUDED_TYPE)

    def test_gates_before_type(self) -> None:
        self.assertEqual(
            _reason(_candidate_row(exchange_canonical="LSE", type_norm="warrant")),
            REASON_NOT_US_LISTED,
        )
        self.assertEqual(
            _reason(_candidate_row(market_cap=1.0, type_norm="warrant")),
            REASON_BELOW_MARKET_CAP,
        )
        self.assertEqual(
            _reason(_candidate_row(is_actively_trading_flag=False, type_norm="warrant")),
            REASON_INACTIVE,
        )

    def test_company_name_noise_does_not_change_pass(self) -> None:
        noisy = "MEGA ETF TRUST SPAC UNIT -WARRANT LP"
        plain = _reason(_candidate_row(type_norm="stock", company_name="Boring Inc"))
        loud = _reason(_candidate_row(type_norm="stock", company_name=noisy))
        self.assertEqual(plain, REASON_PASS)
        self.assertEqual(loud, REASON_PASS)
        self.assertEqual(plain, loud)

    def test_symbol_pattern_does_not_change_reason(self) -> None:
        r1 = _reason(_candidate_row(type_norm="stock", symbol="ABC"))
        r2 = _reason(_candidate_row(type_norm="stock", symbol="ETF-WEIRD"))
        self.assertEqual(r1, r2)
        self.assertEqual(r2, REASON_PASS)

    def test_unknown_nonempty_type_pass(self) -> None:
        self.assertEqual(
            _reason(_candidate_row(type_norm="widget depositary thing")),
            REASON_PASS,
        )


class V2QualifySchemaSmokeTests(unittest.TestCase):
    def test_joined_columns_documented(self) -> None:
        row = _candidate_row(type_norm="stock")
        for c in JOINED_QUALIFY_INPUT_COLUMNS:
            self.assertIn(c, row.index)


def run_universe_fmp_v2_qualify_self_check() -> int:
    """테스트 스위트 실행. CI/스크립트용. 성공 0, 실패 1."""
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    suite.addTests(loader.loadTestsFromTestCase(V2QualifySequentialExclusionTests))
    suite.addTests(loader.loadTestsFromTestCase(V2QualifySchemaSmokeTests))
    result = unittest.TextTestRunner(verbosity=1).run(suite)
    return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    raise SystemExit(run_universe_fmp_v2_qualify_self_check())
