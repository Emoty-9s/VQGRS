# -*- coding: utf-8 -*-
"""
get_data.py
- 날짜/요일 기준으로 fmp_universe_fetch.py를 trigger/daily/weekly/monthly로 자동 실행
- 실행 후 parquet/csv를 확인해서 "오늘(또는 최근 거래일)" 데이터가 들어왔는지 검사
- adjClose 결측률, 최신 날짜, 행 수 등 간단 요약 출력

실행 스케줄 (한국시간 기준):
  trigger: 매일
  daily:   화(1) 수(2) 목(3) 금(4) 토(5)
  weekly:  토(5) 일(6) 월(0)
  monthly: 매달 1일

실행 순서: trigger → daily → weekly → monthly

사용:
  py -3 get_data.py
  py -3 get_data.py --force trigger
  py -3 get_data.py --force daily
  py -3 get_data.py --force weekly
  py -3 get_data.py --force monthly
  py -3 get_data.py --force all
  py -3 get_data.py --dry-run
"""

from __future__ import annotations

import argparse
import os
import sys
import subprocess
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd


# -----------------------------
# Config (여기만 네 환경에 맞게)
# -----------------------------
PYTHON_EXE = "py"           # Windows: "py", 필요시 "python"
PYTHON_VER_FLAG = "-3"     # Windows py 런처에서 3.x 지정. python 쓰면 ""로.
FMP_SCRIPT = "fmp_universe_fetch.py"

UNIVERSE_CSV = Path("./data/universe_list.csv")
OUTDIR = Path("./data")

# 실행 모드별 arguments
TRIGGER_ARGS = [
    "--mode", "trigger",
    "--universe", str(UNIVERSE_CSV),
    "--outdir", str(OUTDIR),
]

DAILY_ARGS = [
    "--mode", "daily",
    "--universe", str(UNIVERSE_CSV),
    "--outdir", str(OUTDIR),
]

WEEKLY_ARGS = [
    "--mode", "weekly",
    "--universe", str(UNIVERSE_CSV),
    "--outdir", str(OUTDIR),
]

MONTHLY_ARGS = [
    "--mode", "monthly",
    "--universe", str(UNIVERSE_CSV),
    "--outdir", str(OUTDIR),
]

# 요일 기준 자동 정책 (0=월 ... 6=일)
RUN_DAILY_ON = {1, 2, 3, 4, 5}   # 화~토
RUN_WEEKLY_ON = {5, 6, 0}        # 토, 일, 월
# trigger: 매일 실행
# monthly: today.day == 1 일 때 실행

# 검증 기준
ADJCLOSE_WARN_RATE = 0.20
MAX_TRADEDATE_LAG_DAYS = 6   # 최신 가격 날짜가 오늘-6일보다 오래되면 경고 (휴일/연휴 감안)


def _python_cmd() -> list[str]:
    cmd = [PYTHON_EXE]
    if PYTHON_EXE == "py" and PYTHON_VER_FLAG:
        cmd.append(PYTHON_VER_FLAG)
    return cmd


def run_fetch(args_list: list[str], dry_run: bool = False) -> int:
    cmd = _python_cmd() + [FMP_SCRIPT] + args_list
    print("\n[RUN]", " ".join(cmd))
    if dry_run:
        print("[DRY-RUN] 실행은 생략했습니다.")
        return 0

    # 실시간 로그를 그대로 보여주면서 실행
    p = subprocess.run(cmd)
    return p.returncode


def read_prices_eod(outdir: Path) -> pd.DataFrame | None:
    pq = outdir / "prices_eod.parquet"
    if pq.exists():
        try:
            return pd.read_parquet(pq)
        except Exception as e:
            print(f"[ERROR] prices_eod.parquet 읽기 실패: {e}")
            return None

    csv = outdir / "prices_eod.csv"
    if csv.exists():
        try:
            return pd.read_csv(csv)
        except Exception as e:
            print(f"[ERROR] prices_eod.csv 읽기 실패: {e}")
            return None

    print("[WARN] prices_eod 파일이 없습니다.")
    return None


def read_snapshots(outdir: Path) -> dict[str, int]:
    """스냅샷 테이블 row 수만 빠르게 확인"""
    names = [
        "estimates_snapshot",
        "estimates_quarterly_snapshot",
        "targets_snapshot",
        "company_facts_snapshot",
        "earnings_events",
        "dividends_events",
        "financials_quarterly",
        "insider_transactions",
        "insider_holdings_snapshot",
        "index_membership",
    ]
    out: dict[str, int] = {}
    for n in names:
        pq = outdir / f"{n}.parquet"
        if pq.exists():
            try:
                df = pd.read_parquet(pq, columns=None)
                out[n] = len(df)
            except Exception:
                out[n] = -1
        else:
            out[n] = 0
    return out


def summarize_prices(df: pd.DataFrame | None, mode_run: str = "") -> None:
    if df is None or df.empty:
        print("[WARN] prices_eod 데이터가 비었습니다.")
        if mode_run:
            print(f"- 실행 모드: {mode_run}")
        return

    # 표준 컬럼이 아닐 수도 있어서 방어적으로 처리
    if "date" not in df.columns or "symbol" not in df.columns:
        print("[WARN] prices_eod에 date/symbol 컬럼이 없어 요약을 건너뜁니다.")
        return

    # 날짜 파싱
    dates = pd.to_datetime(df["date"], errors="coerce")
    max_date = dates.max()
    min_date = dates.min()

    # 최신 날짜 = 수집된 최신 거래일(휴장/주말 시 lookback으로 선택된 trade_date)
    latest_slice = df.loc[dates == max_date] if pd.notna(max_date) else df.iloc[0:0]
    latest_rows = len(latest_slice)

    # adjClose 결측률(전체/최신일)
    adj_missing_all = None
    adj_missing_latest = None
    if "adjClose" in df.columns:
        adj_missing_all = float(df["adjClose"].isna().mean())
        if latest_rows > 0:
            adj_missing_latest = float(latest_slice["adjClose"].isna().mean())

    print("\n=== prices_eod 요약 ===")
    if mode_run:
        print(f"- 실행 모드: {mode_run}")
    print(f"- 선택된 최신 거래일(trade_date): {max_date.date() if pd.notna(max_date) else 'NA'}")
    print(f"- 전체 행 수: {len(df):,}")
    print(f"- 날짜 범위: {min_date.date() if pd.notna(min_date) else 'NA'} ~ {max_date.date() if pd.notna(max_date) else 'NA'}")
    print(f"- 최신 날짜 행 수: {latest_rows:,}")

    if adj_missing_all is not None:
        print(f"- adjClose 결측률(전체): {adj_missing_all*100:.2f}%")
    if adj_missing_latest is not None:
        print(f"- adjClose 결측률(최신일): {adj_missing_latest*100:.2f}%")

    # 경고 조건
    today = datetime.now().date()
    if pd.notna(max_date):
        lag_days = (today - max_date.date()).days
        if lag_days > MAX_TRADEDATE_LAG_DAYS:
            print(f"[WARNING] 최신 가격 날짜가 오늘 기준 {lag_days}일 전입니다. (거래일/휴장/수집 실패 가능)")
    if adj_missing_latest is not None and adj_missing_latest > ADJCLOSE_WARN_RATE:
        # 결측률 상위 심볼 20개
        if latest_rows > 0:
            by_sym = latest_slice.groupby("symbol")["adjClose"].apply(lambda s: s.isna().mean()).sort_values(ascending=False)
            top = by_sym.head(20).index.tolist()
            print(f"[WARNING] 최신일 adjClose 결측률이 {ADJCLOSE_WARN_RATE*100:.0f}% 초과. 결측 상위 심볼(최대20): {top}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--force",
        choices=["trigger", "daily", "weekly", "monthly", "all", "none"],
        default="none",
        help="모드를 강제로 지정 (trigger/daily/weekly/monthly/all)",
    )
    ap.add_argument("--dry-run", action="store_true", help="실행 커맨드만 출력하고 실제 실행은 안 함")
    args = ap.parse_args()

    # 기본 검증: 파일 경로
    if not Path(FMP_SCRIPT).exists():
        print(f"[ERROR] {FMP_SCRIPT} 파일이 현재 경로에 없습니다.")
        sys.exit(2)
    if not UNIVERSE_CSV.exists():
        print(f"[ERROR] universe 파일이 없습니다: {UNIVERSE_CSV}")
        sys.exit(2)

    now = datetime.now()
    today = now.date()
    dow = now.weekday()  # 0=월 ... 6=일
    day = today.day

    run_trigger = False
    run_daily = False
    run_weekly = False
    run_monthly = False

    if args.force != "none":
        if args.force == "trigger":
            run_trigger = True
        elif args.force == "daily":
            run_daily = True
        elif args.force == "weekly":
            run_weekly = True
        elif args.force == "monthly":
            run_monthly = True
        elif args.force == "all":
            run_trigger = True
            run_daily = True
            run_weekly = True
            run_monthly = True
    else:
        run_trigger = True
        run_daily = dow in RUN_DAILY_ON
        run_weekly = dow in RUN_WEEKLY_ON
        run_monthly = day == 1

    mode_desc: list[str] = []
    if run_trigger:
        mode_desc.append("trigger")
    if run_daily:
        mode_desc.append("daily")
    if run_weekly:
        mode_desc.append("weekly")
    if run_monthly:
        mode_desc.append("monthly")
    mode_str = " → ".join(mode_desc) if mode_desc else "none"

    print(f"[INFO] now={now.strftime('%Y-%m-%d %H:%M:%S')} weekday={dow} day={day} -> run: {mode_str}")

    if not run_trigger and not run_daily and not run_weekly and not run_monthly:
        print("[INFO] 오늘은 자동 실행 정책상 수집을 수행하지 않습니다. (--force trigger/daily/weekly/monthly/all 로 강제 가능)")
        df = read_prices_eod(OUTDIR)
        summarize_prices(df if df is not None else pd.DataFrame(), mode_run="none")
        sys.exit(0)

    # 실행 순서: trigger → daily → weekly → monthly
    rc = 0
    if run_trigger:
        rc = run_fetch(TRIGGER_ARGS, dry_run=args.dry_run)
        if rc != 0:
            print(f"[ERROR] trigger 실행 실패. returncode={rc}")
            sys.exit(rc)
    if run_daily:
        rc = run_fetch(DAILY_ARGS, dry_run=args.dry_run)
        if rc != 0:
            print(f"[ERROR] daily 실행 실패. returncode={rc}")
            sys.exit(rc)
    if run_weekly:
        rc = run_fetch(WEEKLY_ARGS, dry_run=args.dry_run)
        if rc != 0:
            print(f"[ERROR] weekly 실행 실패. returncode={rc}")
            sys.exit(rc)
    if run_monthly:
        rc = run_fetch(MONTHLY_ARGS, dry_run=args.dry_run)
        if rc != 0:
            print(f"[ERROR] monthly 실행 실패. returncode={rc}")
            sys.exit(rc)

    if rc != 0:
        print(f"[ERROR] fmp_universe_fetch 실행 실패. returncode={rc}")
        sys.exit(rc)

    # 실행 후 검증/요약
    print("\n[INFO] 수집 완료. 결과 검증을 시작합니다...")

    df = read_prices_eod(OUTDIR)
    if df is not None:
        summarize_prices(df, mode_run=mode_str)
    else:
        print("[WARN] prices_eod를 읽지 못해 가격 검증을 건너뜁니다.")
        print(f"- 실행 모드: {mode_str}")

    counts = read_snapshots(OUTDIR)
    print("\n=== 테이블 행 수(존재하면) ===")
    for k, v in counts.items():
        if v == 0:
            continue
        if v == -1:
            print(f"- {k}: (읽기 실패)")
        else:
            print(f"- {k}: {v:,}")

    print("\n[OK] get_data.py 완료")


if __name__ == "__main__":
    main()
