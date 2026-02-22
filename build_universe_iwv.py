# -*- coding: utf-8 -*-
"""
IWV (iShares Russell 3000 ETF) holdings → universe_list.csv 파이프라인.

1) Playwright로 IWV 페이지 접속 후 "Detailed Holdings and Analytics" CSV 다운로드
2) CSV 파싱 → ticker_raw/name_raw 기반 ticker_fixed 보정 (FMP 매핑 옵션)
3) universe_list.csv, universe_rejects.csv, universe_ambiguous.csv, ticker_map.json 저장

설치/실행:
  pip install playwright pandas rapidfuzz requests
  python -m playwright install chromium
  $env:FMP_API_KEY="..."   # --fmp-map 1 일 때만
  python build_universe_iwv.py --outdir ./data --headless 0 --fmp-map 1

PowerShell 단건 실행 예:
  python build_universe_iwv.py --outdir ./data --headless 0 --timeout-sec 60 --download-wait-sec 60 --fmp-map 1 --fmp-cache ./data/fmp_stock_list.json
"""
from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

try:
    from rapidfuzz import fuzz
except ImportError:
    fuzz = None  # type: ignore

# -----------------------------------------------------------------------------
# 상수
# -----------------------------------------------------------------------------
IWV_URL = "https://www.ishares.com/us/products/239714/ishares-russell-3000-etf"
FMP_BASE = "https://financialmodelingprep.com"
FMP_STOCK_LIST_PATH = "/stable/stock-list"
API_KEY_ENV = "FMP_API_KEY"

TICKER_COL_CANDIDATES = ["Ticker", "ticker", "Symbol", "symbol"]
NAME_COL_CANDIDATES = ["Name", "name", "Security Name", "Holding", "Description"]
ASSET_CLASS_CANDIDATES = ["Asset Class", "assetClass", "Asset Class ", "Type"]
WEIGHT_COL_CANDIDATES = ["Weight (%)", "Weight", "% Weight", "Market Value Weight", "Weight (%) ", "Market Value"]

REJECT_KEYWORDS = [
    "CASH", "USD", "US DOLLAR", "DOLLAR", "CURRENCY", "FX",
    "SWAP", "FUTURE", "FORWARD", "OPTION", "DERIVATIVE",
    "ETF", "FUND", "TRUST", "MONEY MARKET",
]
REJECT_ASSET_CLASSES = {"cash", "currency", "currencies", "derivatives", "money market", "commodity"}

# 헤더 탐지: 이 키워드가 같은 줄에 있으면 헤더로 간주 (Ticker/Symbol AND Name/Security/Holding)
HEADER_TICKER_KEYWORDS = ("Ticker", "Symbol")
HEADER_NAME_KEYWORDS = ("Name", "Security", "Holding")


def _detect_header_line(csv_path: Path, max_lines: int = 200) -> int:
    """파일 상단 max_lines줄을 읽어, 헤더가 있는 줄의 0-based index 반환. 없으면 -1."""
    text = csv_path.read_text(encoding="utf-8-sig", errors="replace")
    lines = text.splitlines()
    for i, line in enumerate(lines[:max_lines]):
        upper = line.upper()
        has_ticker = any(k.upper() in upper for k in HEADER_TICKER_KEYWORDS)
        has_name = any(k.upper() in upper for k in HEADER_NAME_KEYWORDS)
        if has_ticker and has_name:
            return i
    return -1


def _read_holdings_csv(csv_path: Path) -> pd.DataFrame:
    """메타 줄이 있는 IWV CSV를 헤더 자동 탐지 후 읽기. encoding/sep fallback 적용."""
    header_line = _detect_header_line(csv_path)
    if header_line < 0:
        text = csv_path.read_text(encoding="utf-8-sig", errors="replace")
        lines = text.splitlines()
        print("헤더 탐지 실패. CSV 상단 30줄 (디버깅용):")
        for i, line in enumerate(lines[:30]):
            print(f"  {i}: {line[:120]}")
        raise SystemExit("헤더 행을 찾지 못했습니다. (Ticker/Symbol AND Name/Security/Holding 포함 줄이 없음)")

    skip = list(range(header_line))
    for encoding in ("utf-8-sig", "latin1"):
        try:
            df = pd.read_csv(
                csv_path,
                engine="python",
                skiprows=skip,
                encoding=encoding,
                sep=",",
                on_bad_lines="skip",
            )
        except Exception:
            continue
        if len(df.columns) > 2:
            return df
        for sep in (";", "\t"):
            try:
                df = pd.read_csv(
                    csv_path,
                    engine="python",
                    skiprows=skip,
                    encoding=encoding,
                    sep=sep,
                    on_bad_lines="skip",
                )
                if len(df.columns) > 2:
                    return df
            except Exception:
                continue
    raise SystemExit("CSV 읽기 실패: encoding/sep 조합으로 유효한 테이블을 얻지 못했습니다.")


def _pick_col(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    cols_lower = {str(c).lower(): c for c in df.columns}
    for c in candidates:
        if c.lower() in cols_lower:
            return cols_lower[c.lower()]
    return None


def _normalize_key(ticker: str) -> str:
    return re.sub(r"[^A-Z0-9]", "", ticker.upper())


def _should_reject_by_keywords(
    name: str,
    asset_class: str,
    ticker_raw: str,
    in_fmp_symbols: bool,
) -> bool:
    """비종목 필터. assetClass가 Equity이고 FMP에 있으면 Trust 등 키워드로도 통과."""
    if asset_class:
        ac = str(asset_class).strip().lower()
        if ac in REJECT_ASSET_CLASSES:
            return True
        if ac in ("equity", "equities") and in_fmp_symbols and ticker_raw:
            return False
    if not name and not asset_class:
        return False
    combined = f" {str(name or '').upper()} {str(asset_class or '').upper()} "
    for kw in REJECT_KEYWORDS:
        if kw in combined:
            if ticker_raw and in_fmp_symbols and "equity" in str(asset_class or "").lower():
                continue
            return True
    return False


def load_fmp_stock_list(cache_path: Path, api_key: str, use_cache: bool) -> List[Dict[str, Any]]:
    """FMP stock list 로드: 캐시 있으면 사용, 없으면 API 호출 후 저장."""
    if use_cache and cache_path.exists():
        try:
            data = json.loads(cache_path.read_text(encoding="utf-8"))
            return data if isinstance(data, list) else []
        except Exception:
            pass
    try:
        import requests
    except ImportError:
        return []
    url = f"{FMP_BASE}{FMP_STOCK_LIST_PATH}"
    r = requests.get(url, params={"apikey": api_key}, timeout=120)
    if r.status_code != 200:
        return []
    data = r.json()
    if not isinstance(data, list):
        return []
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
    return data


def build_symbol_lookup(fmp_list: List[Dict[str, Any]]) -> Tuple[Dict[str, Dict], Dict[str, List[str]]]:
    """symbol -> {symbol, name, ...}, normalize_key -> [symbols]"""
    by_symbol: Dict[str, Dict[str, Any]] = {}
    by_normalized: Dict[str, List[str]] = {}
    for row in fmp_list or []:
        sym = (row.get("symbol") or row.get("Symbol") or "").strip().upper()
        name = (row.get("name") or row.get("Name") or row.get("companyName") or "").strip()
        if not sym:
            continue
        by_symbol[sym] = {"symbol": sym, "name": name, **row}
        nk = _normalize_key(sym)
        by_normalized.setdefault(nk, []).append(sym)
    for nk in by_normalized:
        by_normalized[nk] = sorted(set(by_normalized[nk]))
    return by_symbol, by_normalized


def resolve_ticker(
    ticker_raw: str,
    name_raw: str,
    by_symbol: Dict[str, Dict],
    by_normalized: Dict[str, List[str]],
) -> Tuple[str, str, Optional[float], int, Optional[str]]:
    """(ticker_fixed, match_type, match_score, candidate_count, reason)."""
    raw_upper = ticker_raw.strip().upper()
    if not raw_upper:
        return ("", "REJECT", None, 0, "empty_ticker")
    # EXACT
    if raw_upper in by_symbol:
        return (raw_upper, "EXACT", None, 1, None)
    nk = _normalize_key(ticker_raw)
    candidates = by_normalized.get(nk, [])
    if len(candidates) == 0:
        return ("", "REJECT", None, 0, "no_symbol_match")
    if len(candidates) == 1:
        return (candidates[0], "NORMALIZED", None, 1, None)
    # NAME_FUZZY
    if fuzz is None:
        return ("", "AMBIGUOUS", None, len(candidates), "rapidfuzz_not_installed")
    name_raw_n = (name_raw or "").strip()
    best_sym = None
    best_score = -1
    second_score = -1
    for sym in candidates:
        rec = by_symbol.get(sym, {})
        cand_name = (rec.get("name") or rec.get("companyName") or "").strip()
        score = fuzz.token_set_ratio(name_raw_n, cand_name) if name_raw_n and cand_name else 0
        if score > best_score:
            second_score = best_score
            best_score = score
            best_sym = sym
        elif score > second_score:
            second_score = score
    if best_sym is None:
        return ("", "AMBIGUOUS", None, len(candidates), "no_name_match")
    if best_score >= 90 and (best_score - second_score) >= 5:
        return (best_sym, "NAME_FUZZY", float(best_score), len(candidates), None)
    return ("", "AMBIGUOUS", float(best_score) if best_score >= 0 else None, len(candidates), "fuzzy_tie_or_low_score")


def download_iwv_holdings_playwright(
    outdir: Path,
    timeout_sec: int,
    download_wait_sec: int,
    headless: bool,
) -> Optional[Path]:
    """Playwright로 IWV 페이지에서 CSV 다운로드. 저장 경로 반환."""
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        print("Playwright 미설치: pip install playwright && python -m playwright install chromium")
        return None
    outdir.mkdir(parents=True, exist_ok=True)
    ts = time.strftime("%Y%m%d_%H%M%S")
    dest = outdir / f"iwv_holdings_{ts}.csv"
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()
        page.set_default_timeout(timeout_sec * 1000)
        try:
            page.goto(IWV_URL, wait_until="domcontentloaded")
            # 쿠키/동의 버튼
            for sel in [
                'button:has-text("Accept")',
                'button:has-text("Accept All")',
                '[data-testid="accept-cookies"]',
                'a:has-text("Accept")',
            ]:
                try:
                    btn = page.locator(sel).first
                    if btn.is_visible(timeout=3000):
                        btn.click()
                        time.sleep(1)
                        break
                except Exception:
                    pass
            # "Detailed Holdings and Analytics" 링크/버튼
            download_triggered = False
            for loc in [
                page.get_by_text("Detailed Holdings and Analytics", exact=False),
                page.locator('a[href*="download"]').filter(has_text="Holdings"),
                page.locator('a[href*=".csv"]'),
                page.get_by_role("link", name=re.compile(r"Detailed|Holdings|CSV", re.I)),
            ]:
                try:
                    if loc.count() > 0 and loc.first.is_visible(timeout=2000):
                        with page.expect_download(timeout=download_wait_sec * 1000) as dl_info:
                            loc.first.click()
                        download = dl_info.value
                        path = download.path()
                        if path and Path(path).exists():
                            shutil.copy(path, dest)
                            download_triggered = True
                            break
                except Exception:
                    continue
            if not download_triggered:
                # 스크롤 후 재시도
                page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                time.sleep(2)
                for loc in [
                    page.get_by_text("Detailed Holdings and Analytics", exact=False),
                    page.locator('a:has-text("CSV")'),
                ]:
                    try:
                        if loc.count() > 0:
                            with page.expect_download(timeout=download_wait_sec * 1000) as dl_info:
                                loc.first.click()
                            download = dl_info.value
                            path = download.path()
                            if path and Path(path).exists():
                                shutil.copy(path, dest)
                                download_triggered = True
                                break
                    except Exception:
                        continue
            if not download_triggered:
                print("다운로드 링크를 찾지 못했습니다. 수동으로 CSV를 저장한 뒤 --csv 경로를 사용하세요.")
                browser.close()
                return None
        finally:
            browser.close()
    return dest if dest.exists() else None


def parse_and_build_universe(
    csv_path: Path,
    outdir: Path,
    by_symbol: Dict[str, Dict],
    by_normalized: Dict[str, List[str]],
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, Dict[str, Dict]]:
    """CSV 파싱 → 보정 → universe_list, rejects, ambiguous, ticker_map."""
    df = _read_holdings_csv(csv_path)
    ticker_col = _pick_col(df, TICKER_COL_CANDIDATES)
    name_col = _pick_col(df, NAME_COL_CANDIDATES)
    asset_col = _pick_col(df, ASSET_CLASS_CANDIDATES)
    weight_col = _pick_col(df, WEIGHT_COL_CANDIDATES)
    if not ticker_col:
        raise ValueError("CSV에 ticker/symbol 컬럼을 찾을 수 없습니다.")
    name_col = name_col or "name"
    if name_col not in df.columns:
        df[name_col] = ""

    rows_accepted: List[Dict[str, Any]] = []
    rows_reject: List[Dict[str, Any]] = []
    rows_ambiguous: List[Dict[str, Any]] = []
    ticker_map: Dict[str, Dict[str, Any]] = {}

    for _, r in df.iterrows():
        ticker_raw = str(r.get(ticker_col, "") or "").strip().upper()
        name_raw = str(r.get(name_col, "") or "").strip()
        asset_class = str(r.get(asset_col, "") or "").strip() if asset_col else ""
        weight = r.get(weight_col) if weight_col and weight_col in r else None
        if not ticker_raw:
            rows_reject.append({
                "ticker_raw": "",
                "name_raw": name_raw,
                "ticker_fixed": "",
                "match_type": "REJECT",
                "match_score": None,
                "candidate_count": 0,
                "reason": "empty_ticker",
                "weight": weight,
            })
            continue
        in_fmp = ticker_raw in by_symbol
        if _should_reject_by_keywords(name_raw, asset_class, ticker_raw, in_fmp):
            rows_reject.append({
                "ticker_raw": ticker_raw,
                "name_raw": name_raw,
                "ticker_fixed": "",
                "match_type": "REJECT",
                "match_score": None,
                "candidate_count": 0,
                "reason": "non_equity",
                "weight": weight,
            })
            continue
        fixed, match_type, score, cand_count, reason = resolve_ticker(
            ticker_raw, name_raw, by_symbol, by_normalized
        )
        row_out = {
            "ticker_raw": ticker_raw,
            "name_raw": name_raw,
            "ticker_fixed": fixed,
            "match_type": match_type,
            "match_score": score,
            "candidate_count": cand_count,
            "reason": reason,
            "weight": weight,
        }
        ticker_map[ticker_raw] = {
            "ticker_fixed": fixed,
            "match_type": match_type,
            "score": score,
            "candidate_count": cand_count,
            "reason": reason,
        }
        if match_type == "REJECT":
            rows_reject.append(row_out)
        elif match_type == "AMBIGUOUS":
            rows_ambiguous.append(row_out)
        else:
            rows_accepted.append(row_out)

    out_cols = ["ticker_raw", "name_raw", "ticker_fixed", "match_type", "match_score", "candidate_count", "reason", "weight"]
    df_accepted = pd.DataFrame(rows_accepted).reindex(columns=out_cols, copy=False)
    df_reject = pd.DataFrame(rows_reject).reindex(columns=out_cols, copy=False)
    df_ambiguous = pd.DataFrame(rows_ambiguous).reindex(columns=out_cols, copy=False)
    df_universe = df_accepted  # 최종 universe = 티커 확정된 행만 (EXACT/NORMALIZED/NAME_FUZZY)
    return df_universe, df_reject, df_ambiguous, ticker_map


def main() -> None:
    ap = argparse.ArgumentParser(description="IWV holdings 다운로드 → universe_list.csv 생성")
    ap.add_argument("--outdir", type=str, default="./data", help="출력 디렉터리")
    ap.add_argument("--headless", type=int, default=0, choices=[0, 1], help="1=헤드리스")
    ap.add_argument("--timeout-sec", type=int, default=60)
    ap.add_argument("--download-wait-sec", type=int, default=60)
    ap.add_argument("--keep-browser-profile", type=int, default=0, choices=[0, 1], help="1=persistent context")
    ap.add_argument("--fmp-map", type=int, default=1, choices=[0, 1], help="1=FMP 심볼리스트로 ticker 보정")
    ap.add_argument("--fmp-cache", type=str, default="./data/fmp_stock_list.json", help="FMP stock list 캐시 경로")
    ap.add_argument("--csv", type=str, default="", help="이미 받은 CSV 경로 (지정 시 다운로드 스킵)")
    args = ap.parse_args()

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    fmp_cache = Path(args.fmp_cache)
    api_key = (os.environ.get(API_KEY_ENV) or "").strip() if args.fmp_map else ""
    use_fmp = bool(args.fmp_map)
    if use_fmp and not api_key:
        print("--fmp-map 1 사용 시 환경변수 FMP_API_KEY를 설정하세요.")
        use_fmp = False

    by_symbol: Dict[str, Dict] = {}
    by_normalized: Dict[str, List[str]] = {}
    if use_fmp and api_key:
        fmp_list = load_fmp_stock_list(fmp_cache, api_key, use_cache=True)
        by_symbol, by_normalized = build_symbol_lookup(fmp_list)
        print(f"FMP 심볼 수: {len(by_symbol)} (캐시: {fmp_cache})")
    else:
        print("FMP 매핑 비사용. EXACT만 적용되며 NORMALIZED/NAME_FUZZY는 비활성.")

    csv_path: Optional[Path] = None
    if args.csv and Path(args.csv).exists():
        csv_path = Path(args.csv)
        print(f"기존 CSV 사용: {csv_path}")
    else:
        csv_path = download_iwv_holdings_playwright(
            outdir, args.timeout_sec, args.download_wait_sec, headless=bool(args.headless)
        )
    if not csv_path or not csv_path.exists():
        print("CSV를 얻지 못했습니다. --csv 로 수동 파일을 지정하거나 브라우저 다운로드를 확인하세요.")
        return

    dest_copy = outdir / "iwv_holdings_downloaded.csv"
    shutil.copy(csv_path, dest_copy)
    print(f"원본 복사: {dest_copy}")

    df_universe, df_reject, df_ambiguous, ticker_map = parse_and_build_universe(
        csv_path, outdir, by_symbol, by_normalized
    )

    universe_path = outdir / "universe_list.csv"
    df_universe.to_csv(universe_path, index=False)
    print(f"universe_list.csv: {universe_path} (rows={len(df_universe)})")

    reject_path = outdir / "universe_rejects.csv"
    df_reject.to_csv(reject_path, index=False)
    print(f"universe_rejects.csv: {reject_path} (rows={len(df_reject)})")

    ambiguous_path = outdir / "universe_ambiguous.csv"
    df_ambiguous.to_csv(ambiguous_path, index=False)
    print(f"universe_ambiguous.csv: {ambiguous_path} (rows={len(df_ambiguous)})")

    map_path = outdir / "ticker_map.json"
    map_path.write_text(json.dumps(ticker_map, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"ticker_map.json: {map_path}")

    accepted = len(df_universe) - len(df_ambiguous)
    print("\n==== 요약 ====")
    print(f"총 행(원본): {len(df_universe) + len(df_reject)}")
    print(f"티커 확정(수락): {accepted}")
    print(f"AMBIGUOUS: {len(df_ambiguous)}")
    print(f"REJECT: {len(df_reject)}")
    if not df_reject.empty and "reason" in df_reject.columns:
        print("상위 REJECT reason:")
        for reason, cnt in df_reject["reason"].value_counts().head(10).items():
            print(f"  {reason}: {cnt}")


if __name__ == "__main__":
    main()
