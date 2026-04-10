# -*- coding: utf-8 -*-
"""
build_universe_fmp.universe_row_reason_code / qualify_universe 최소 검증
(company-screener 후보 + stock-list 조인 입력 스키마).

실행:  py -3 -m unittest tests.test_universe_qualify -v
"""
from __future__ import annotations

import sys
import unittest
from pathlib import Path

import pandas as pd

# 프로젝트 루트를 path에 추가
_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from build_universe_fmp import (  # noqa: E402
    DEFAULT_MIN_MARKET_CAP,
    SCREENER_EXCHANGES,
    STOCK_LIST_JOIN_MATCHED_EXCHANGE,
    STOCK_LIST_JOIN_NO_MATCH,
    compute_qualify_reason_codes,
    is_eligible_pass_type,
    is_excluded_type,
    is_unknown_type,
    qualify_universe,
    universe_row_reason_code,
)


MIN_MCAP = DEFAULT_MIN_MARKET_CAP


def _base_row() -> dict:
    """미국 상장·시총 충족·stock-list 조인 성공·security_type=stock."""
    return {
        "symbol": "EXM",
        "company_name": "Example Corp",
        "exchange": "NYSE",
        "sector": "Tech",
        "industry": "Software",
        "market_cap": 200_000_000,
        "screener_is_etf": False,
        "screener_is_fund": False,
        "screener_is_actively_trading": True,
        "stock_type_raw": "stock",
        "stock_type_norm": "stock",
        "security_type": "stock",
        "stock_list_join": STOCK_LIST_JOIN_MATCHED_EXCHANGE,
    }


def _series(**overrides) -> pd.Series:
    d = _base_row()
    d.update(overrides)
    return pd.Series(d)


def _row_reason_for_list_type(
    security_type: str,
    *,
    stock_list_join: str = STOCK_LIST_JOIN_MATCHED_EXCHANGE,
    company_name: str = "Example Corp",
    symbol: str = "SYM",
    screener_is_etf: bool = False,
    screener_is_fund: bool = False,
) -> str:
    """단일 행에 대한 qualify reason_code (stock-list type 중심)."""
    st = security_type
    r = _series(
        symbol=symbol,
        company_name=company_name,
        security_type=st,
        stock_type_norm=st,
        stock_type_raw=st or "",
        stock_list_join=stock_list_join,
        screener_is_etf=screener_is_etf,
        screener_is_fund=screener_is_fund,
    )
    return universe_row_reason_code(r, MIN_MCAP, SCREENER_EXCHANGES)


class StockListTypePolicyMinimumTests(unittest.TestCase):
    """
    stock-list ``type``(→ security_type) 정책 최소 검증.
    회사명·티커는 ``security_type`` 만 같으면 reason_code 가 동일해야 한다.
    """

    def test_stock_list_etf_excluded_reason_code(self) -> None:
        """list-type etf → 제외(구현: ``EXCLUDED_TYPE``; 스크리너 isEtf True 일 때만 ``ETF``)."""
        self.assertEqual(_row_reason_for_list_type("etf"), "EXCLUDED_TYPE")
        self.assertEqual(
            _row_reason_for_list_type("etf", screener_is_etf=True),
            "ETF",
        )

    def test_stock_list_fund_excluded_or_screener_fund(self) -> None:
        """list-type fund → ``EXCLUDED_TYPE``; 스크리너 isFund True → ``FUND``."""
        self.assertEqual(_row_reason_for_list_type("fund"), "EXCLUDED_TYPE")
        self.assertEqual(
            _row_reason_for_list_type("stock", screener_is_fund=True),
            "FUND",
        )

    def test_structural_exclusion_types_excluded_type(self) -> None:
        for tok in (
            "preferred stock",
            "warrant",
            "unit",
            "right",
            "spac",
            "bond",
        ):
            with self.subTest(security_type=tok):
                self.assertEqual(
                    _row_reason_for_list_type(tok),
                    "EXCLUDED_TYPE",
                    msg=f"expected EXCLUDED_TYPE for {tok!r}",
                )

    def test_reit_not_auto_excluded_pass(self) -> None:
        """reit 은 제외 집합에 없음 → ``PASS``."""
        self.assertEqual(_row_reason_for_list_type("reit"), "PASS")
        self.assertNotEqual(_row_reason_for_list_type("reit"), "EXCLUDED_TYPE")

    def test_adr_and_stock_pass(self) -> None:
        self.assertEqual(_row_reason_for_list_type("adr"), "PASS")
        self.assertEqual(_row_reason_for_list_type("stock"), "PASS")

    def test_empty_type_join_fail_vs_join_ok(self) -> None:
        """빈 type + 조인 실패 → ``MISSING_TYPE_FROM_STOCK_LIST``; 조인 성공 → ``UNKNOWN_TYPE_EMPTY``."""
        self.assertEqual(
            _row_reason_for_list_type("", stock_list_join=STOCK_LIST_JOIN_NO_MATCH),
            "MISSING_TYPE_FROM_STOCK_LIST",
        )
        self.assertEqual(
            _row_reason_for_list_type("", stock_list_join=STOCK_LIST_JOIN_MATCHED_EXCHANGE),
            "UNKNOWN_TYPE_EMPTY",
        )

    def test_company_name_and_symbol_do_not_change_reason_for_same_type(self) -> None:
        """회사명·티커에 ETF/Trust/특이 패턴이 있어도 ``security_type=stock`` 이면 ``PASS``."""
        noise = [
            ("MEGA ETF TRUST LP", "ETF-W"),
            ("Not A Fund Really Inc", "FUND-X"),
            ("UNIT -WARRANT TRUST", "ABCD-W.U"),
            ("Normal", "BRK-A"),
        ]
        for company_name, symbol in noise:
            with self.subTest(company_name=company_name, symbol=symbol):
                self.assertEqual(
                    _row_reason_for_list_type(
                        "stock",
                        company_name=company_name,
                        symbol=symbol,
                    ),
                    "PASS",
                )

    def test_dataframe_batch_reason_codes_stable_under_noise(self) -> None:
        """작은 DataFrame: 동일 type 행들이 이름/심볼만 다를 때 reason_code 동일."""
        base = _base_row()
        rows = []
        for i, (co, sy) in enumerate(
            [
                ("ETF Trust Co", "T1"),
                ("Plain Inc", "T2"),
                ("WARRANT-UNIT LP", "WEIRD-W"),
            ],
        ):
            rows.append({**base, "symbol": sy, "company_name": co, "security_type": "stock"})
        df = pd.DataFrame(rows)
        codes = compute_qualify_reason_codes(df, MIN_MCAP, SCREENER_EXCHANGES)
        self.assertTrue((codes == "PASS").all())


class TypeHelperTests(unittest.TestCase):
    """is_excluded_type / is_unknown_type exact membership."""

    def test_unknown_type_helper(self) -> None:
        self.assertTrue(is_unknown_type("custom_vendor_type_xyz"))
        self.assertFalse(is_unknown_type(""))
        self.assertFalse(is_unknown_type("etf"))
        self.assertFalse(is_unknown_type("stock"))

    def test_excluded_type_helper(self) -> None:
        self.assertTrue(is_excluded_type("spac"))
        self.assertFalse(is_excluded_type("reit"))

    def test_eligible_pass_type_helper(self) -> None:
        self.assertTrue(is_eligible_pass_type("adr"))
        self.assertFalse(is_eligible_pass_type("etf"))


class UniverseReasonCodeTests(unittest.TestCase):
    """각 케이스별 universe_row_reason_code 기대값."""

    def test_us_listed_common_stock_pass(self) -> None:
        """미국 상장 common stock (security_type=stock) → PASS."""
        r = _series()
        self.assertEqual(
            universe_row_reason_code(r, MIN_MCAP, SCREENER_EXCHANGES),
            "PASS",
        )

    def test_us_listed_adr_pass_via_token(self) -> None:
        """미국 상장 ADR (security_type=adr) → PASS."""
        r = _series(security_type="adr", stock_type_norm="adr", stock_type_raw="ADR")
        self.assertEqual(
            universe_row_reason_code(r, MIN_MCAP, SCREENER_EXCHANGES),
            "PASS",
        )

    def test_etf_excluded_by_type_token(self) -> None:
        """stock-list 분류 토큰 etf → EXCLUDED_TYPE."""
        r = _series(security_type="etf", stock_type_norm="etf", stock_type_raw="ETF")
        self.assertEqual(
            universe_row_reason_code(r, MIN_MCAP, SCREENER_EXCHANGES),
            "EXCLUDED_TYPE",
        )

    def test_fund_excluded_by_type_token(self) -> None:
        """stock-list 분류 토큰 fund → EXCLUDED_TYPE."""
        r = _series(security_type="fund", stock_type_norm="fund", stock_type_raw="Fund")
        self.assertEqual(
            universe_row_reason_code(r, MIN_MCAP, SCREENER_EXCHANGES),
            "EXCLUDED_TYPE",
        )

    def test_below_mcap_common_stock_excluded(self) -> None:
        """시총 미달 보통주 → BELOW_MARKET_CAP."""
        r = _series(market_cap=50_000_000)
        self.assertEqual(
            universe_row_reason_code(r, MIN_MCAP, SCREENER_EXCHANGES),
            "BELOW_MARKET_CAP",
        )

    def test_unknown_type_empty_when_joined(self) -> None:
        """조인 성공 후 빈 security_type → UNKNOWN_TYPE_EMPTY (보류/검토)."""
        r = _series(security_type="", stock_type_norm="", stock_type_raw="")
        self.assertEqual(
            universe_row_reason_code(r, MIN_MCAP, SCREENER_EXCHANGES),
            "UNKNOWN_TYPE_EMPTY",
        )

    def test_missing_type_from_stock_list(self) -> None:
        """stock-list 조인 실패 → MISSING_TYPE_FROM_STOCK_LIST."""
        r = _series(
            stock_list_join=STOCK_LIST_JOIN_NO_MATCH,
            security_type="",
            stock_type_norm="",
            stock_type_raw="",
        )
        self.assertEqual(
            universe_row_reason_code(r, MIN_MCAP, SCREENER_EXCHANGES),
            "MISSING_TYPE_FROM_STOCK_LIST",
        )

    def test_unknown_security_type_token_excluded(self) -> None:
        """알려지지 않은 비어 있지 않은 분류 토큰 → UNKNOWN_TYPE_NEW."""
        r = _series(
            security_type="custom_vendor_type_xyz",
            stock_type_norm="custom_vendor_type_xyz",
            stock_type_raw="custom_vendor_type_xyz",
        )
        self.assertEqual(
            universe_row_reason_code(r, MIN_MCAP, SCREENER_EXCHANGES),
            "UNKNOWN_TYPE_NEW",
        )

    def test_company_name_etf_trust_strings_do_not_affect_pass(self) -> None:
        """회사명에 ETF/TRUST 등이 있어도 security_type=stock이면 PASS."""
        r = _series(
            company_name="MEGA ETF TRUST -WARRANT UNIT LP",
            symbol="BAD-W",
        )
        self.assertEqual(
            universe_row_reason_code(r, MIN_MCAP, SCREENER_EXCHANGES),
            "PASS",
        )

    def test_symbol_suffix_w_does_not_affect_pass(self) -> None:
        """심볼 접미사 -W 등은 판정에 사용되지 않음 → PASS."""
        r = _series(symbol="MOCK-W")
        self.assertEqual(
            universe_row_reason_code(r, MIN_MCAP, SCREENER_EXCHANGES),
            "PASS",
        )

    def test_type_token_etf_not_bypassed_by_company_name(self) -> None:
        """회사명이 일반적이어도 security_type=etf면 제외 집합."""
        r = _series(
            company_name="Not An ETF Really Inc",
            security_type="etf",
            stock_type_norm="etf",
            stock_type_raw="etf",
        )
        self.assertEqual(
            universe_row_reason_code(r, MIN_MCAP, SCREENER_EXCHANGES),
            "EXCLUDED_TYPE",
        )

    def test_spac_excluded(self) -> None:
        """security_type=spac → EXCLUDED_TYPE."""
        r = _series(security_type="spac", stock_type_norm="spac", stock_type_raw="spac")
        self.assertEqual(
            universe_row_reason_code(r, MIN_MCAP, SCREENER_EXCHANGES),
            "EXCLUDED_TYPE",
        )

    def test_reit_token_eligible_pass(self) -> None:
        """reit 은 제외 집합에 없고 ELIGIBLE 에 포함 → PASS."""
        r = _series(security_type="reit", stock_type_norm="reit", stock_type_raw="REIT")
        self.assertEqual(
            universe_row_reason_code(r, MIN_MCAP, SCREENER_EXCHANGES),
            "PASS",
        )

    def test_reit_not_in_exclusion_set(self) -> None:
        """reit 은 EXCLUDED_TYPE 이 아님."""
        r = _series(security_type="reit", stock_type_norm="reit", stock_type_raw="REIT")
        code = universe_row_reason_code(r, MIN_MCAP, SCREENER_EXCHANGES)
        self.assertNotEqual(code, "EXCLUDED_TYPE")

    def test_reit_as_common_stock_type_passes(self) -> None:
        """FMP가 REIT를 stock 등 보통주 타입으로 주면 PASS."""
        r = _series(company_name="Some REIT Corp")
        self.assertEqual(
            universe_row_reason_code(r, MIN_MCAP, SCREENER_EXCHANGES),
            "PASS",
        )

    def test_screener_is_etf_flag_excludes_before_stock_type(self) -> None:
        """API isEtf 플래그 True 이면 security_type=stock 이라도 ETF."""
        r = _series(screener_is_etf=True)
        self.assertEqual(
            universe_row_reason_code(r, MIN_MCAP, SCREENER_EXCHANGES),
            "ETF",
        )

    def test_screener_is_fund_flag_excludes(self) -> None:
        r = _series(screener_is_fund=True)
        self.assertEqual(
            universe_row_reason_code(r, MIN_MCAP, SCREENER_EXCHANGES),
            "FUND",
        )

    def test_explicit_not_actively_trading(self) -> None:
        r = _series(screener_is_actively_trading=False)
        self.assertEqual(
            universe_row_reason_code(r, MIN_MCAP, SCREENER_EXCHANGES),
            "INACTIVE",
        )

    def test_inactive_when_actively_trading_unset(self) -> None:
        """PASS 는 isActivelyTrading == True 일 때만."""
        r = _series(screener_is_actively_trading=None)
        self.assertEqual(
            universe_row_reason_code(r, MIN_MCAP, SCREENER_EXCHANGES),
            "INACTIVE",
        )


class JoinBasedMandatoryQualifyTests(unittest.TestCase):
    """
    qualify 최소 매트릭스: security_type(+조인·플래그) → reason_code (이름·심볼 미사용).
    """

    def test_mandatory_security_type_reason_codes(self) -> None:
        cases: tuple[tuple[str, str], ...] = (
            ("stock", "PASS"),
            ("common stock", "PASS"),
            ("adr", "PASS"),
            ("etf", "EXCLUDED_TYPE"),
            ("etn", "EXCLUDED_TYPE"),
            ("etp", "EXCLUDED_TYPE"),
            ("fund", "EXCLUDED_TYPE"),
            ("preferred stock", "EXCLUDED_TYPE"),
            ("warrant", "EXCLUDED_TYPE"),
            ("spac", "EXCLUDED_TYPE"),
            ("bond", "EXCLUDED_TYPE"),
            ("reit", "PASS"),
        )
        for token, expected in cases:
            with self.subTest(security_type=token, expected=expected):
                r = _series(security_type=token, stock_type_norm=token, stock_type_raw=token)
                self.assertEqual(
                    universe_row_reason_code(r, MIN_MCAP, SCREENER_EXCHANGES),
                    expected,
                )

    def test_adr_depositary_receipt_pass(self) -> None:
        r = _series(
            security_type="american depositary receipt",
            stock_type_norm="american depositary receipt",
            stock_type_raw="American Depositary Receipt",
        )
        self.assertEqual(
            universe_row_reason_code(r, MIN_MCAP, SCREENER_EXCHANGES),
            "PASS",
        )

    def test_empty_security_type_policy_reason(self) -> None:
        """조인 성공 후 빈 분류 토큰 → UNKNOWN_TYPE_EMPTY."""
        r = _series(security_type="", stock_type_norm="", stock_type_raw="")
        self.assertEqual(
            universe_row_reason_code(r, MIN_MCAP, SCREENER_EXCHANGES),
            "UNKNOWN_TYPE_EMPTY",
        )

    def test_company_name_and_symbol_noise_invariant(self) -> None:
        """
        회사명/티커 문자열(ETF, Trust, -W 등)은 판정에 사용되지 않음.
        security_type만이 사유를 결정한다.
        """
        noisy = dict(
            company_name="MEGA ETF TRUST UNIT -WARRANT LP",
            symbol="TRST-W",
        )
        self.assertEqual(
            universe_row_reason_code(_series(**noisy), MIN_MCAP, SCREENER_EXCHANGES),
            "PASS",
        )
        self.assertEqual(
            universe_row_reason_code(
                _series(
                    company_name="Boring Name Inc",
                    symbol="BORE",
                    security_type="etf",
                    stock_type_norm="etf",
                    stock_type_raw="etf",
                ),
                MIN_MCAP,
                SCREENER_EXCHANGES,
            ),
            "EXCLUDED_TYPE",
        )
        self.assertEqual(
            universe_row_reason_code(
                _series(
                    company_name="Not A Fund Trust",
                    symbol="NAF-W",
                    security_type="fund",
                    stock_type_norm="fund",
                    stock_type_raw="fund",
                ),
                MIN_MCAP,
                SCREENER_EXCHANGES,
            ),
            "EXCLUDED_TYPE",
        )


class QualifyUniverseMandatoryBatchTests(unittest.TestCase):
    """동일 DataFrame에 필수 토큰 행을 넣고 qualify_universe 요약 count 검증."""

    def test_mandatory_tokens_summary_counts(self) -> None:
        specs: tuple[tuple[str, str, str], ...] = (
            ("S1", "stock", "PASS"),
            ("S2", "common stock", "PASS"),
            ("A1", "adr", "PASS"),
            ("E1", "etf", "EXCLUDED_TYPE"),
            ("F1", "fund", "EXCLUDED_TYPE"),
            ("P1", "preferred stock", "EXCLUDED_TYPE"),
            ("W1", "warrant", "EXCLUDED_TYPE"),
            ("SP1", "spac", "EXCLUDED_TYPE"),
            ("B1", "bond", "EXCLUDED_TYPE"),
            ("R1", "reit", "PASS"),
            ("M1", "", "UNKNOWN_TYPE_EMPTY"),
            ("M2", "__NO_MATCH__", "MISSING_TYPE_FROM_STOCK_LIST"),
        )
        rows = []
        for sym, token, _ in specs:
            base = {**_base_row(), "symbol": sym}
            if token == "__NO_MATCH__":
                base.update(
                    stock_list_join=STOCK_LIST_JOIN_NO_MATCH,
                    security_type="",
                    stock_type_norm="",
                    stock_type_raw="",
                )
            elif token:
                base.update(
                    security_type=token,
                    stock_type_norm=token,
                    stock_type_raw=token,
                )
            else:
                base.update(security_type="", stock_type_norm="", stock_type_raw="")
            rows.append(base)
        df = pd.DataFrame(rows)
        _, summary = qualify_universe(
            df, MIN_MCAP, SCREENER_EXCHANGES, keep_qualify_debug=False,
        )
        counts = dict(zip(summary["reason_code"], summary["count"]))
        self.assertEqual(counts.get("PASS"), 4)  # stock, common stock, adr, reit
        self.assertEqual(counts.get("EXCLUDED_TYPE"), 6)
        self.assertEqual(counts.get("UNKNOWN_TYPE_EMPTY"), 1)
        self.assertEqual(counts.get("MISSING_TYPE_FROM_STOCK_LIST"), 1)
        self.assertEqual(summary["count"].sum(), len(specs))


class QualifyUniverseDataFrameTests(unittest.TestCase):
    """작은 DataFrame으로 qualify_universe end-to-end (행 집계·출력 행 수)."""

    def test_qualify_universe_mixed_batch(self) -> None:
        """동일 배치에서 stock 통과 + etf 토큰 제외 + 요약 PASS/EXCLUDED_TYPE 카운트."""
        rows = [
            {**_base_row(), "symbol": "GOOD", "company_name": "Good Co"},
            {
                **_base_row(),
                "symbol": "BADETF",
                "company_name": "Bad ETF Name",
                "security_type": "etf",
                "stock_type_norm": "etf",
                "stock_type_raw": "etf",
            },
            {**_base_row(), "symbol": "LOW", "market_cap": 10_000_000},
        ]
        df = pd.DataFrame(rows)
        qualified, summary = qualify_universe(
            df, MIN_MCAP, SCREENER_EXCHANGES, keep_qualify_debug=False,
        )
        self.assertEqual(len(qualified), 1)
        self.assertEqual(qualified.iloc[0]["symbol"], "GOOD")
        codes = dict(zip(summary["reason_code"], summary["count"]))
        self.assertEqual(codes.get("PASS"), 1)
        self.assertEqual(codes.get("EXCLUDED_TYPE"), 1)
        self.assertEqual(codes.get("BELOW_MARKET_CAP"), 1)


def run_universe_qualify_self_check() -> int:
    """
    unittest 없이도 스크립트에서 호출 가능한 최소 self-check.
    Returns 0 if all pass, else 1.
    """
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    suite.addTests(loader.loadTestsFromTestCase(TypeHelperTests))
    suite.addTests(loader.loadTestsFromTestCase(StockListTypePolicyMinimumTests))
    suite.addTests(loader.loadTestsFromTestCase(UniverseReasonCodeTests))
    suite.addTests(loader.loadTestsFromTestCase(JoinBasedMandatoryQualifyTests))
    suite.addTests(loader.loadTestsFromTestCase(QualifyUniverseMandatoryBatchTests))
    suite.addTests(loader.loadTestsFromTestCase(QualifyUniverseDataFrameTests))
    runner = unittest.TextTestRunner(verbosity=1)
    result = runner.run(suite)
    return 0 if result.wasSuccessful() else 1


def run_stock_list_policy_minimum_self_check() -> int:
    """
    stock-list 타입 정책·이름/티커 비영향 최소 검증만 실행.
    Returns 0 if all pass, else 1.
    """
    loader = unittest.TestLoader()
    suite = loader.loadTestsFromTestCase(StockListTypePolicyMinimumTests)
    result = unittest.TextTestRunner(verbosity=1).run(suite)
    return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    raise SystemExit(run_universe_qualify_self_check())
