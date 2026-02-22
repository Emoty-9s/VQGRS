# IWV ETF 홀딩스 → universe_list.csv

iShares Russell 3000 ETF(IWV) 종목 리스트를 **브라우저 자동화로 다운로드**한 뒤,  
**ticker_raw/name_raw 기반 ticker_fixed(FMP용) 보정**을 거쳐 `universe_list.csv` 등을 생성하는 파이프라인입니다.

## 필요한 것

- Python 3.8+
- playwright, pandas, rapidfuzz, requests

## 설치

```powershell
pip install playwright pandas rapidfuzz requests
python -m playwright install chromium
```

## 사용법

### 1) 자동 다운로드 + FMP 매핑 (권장)

FMP API Key를 설정한 뒤 실행합니다. (FMP 매핑으로 BRKB→BRK.B, 클래스주 등 보정)

```powershell
$env:FMP_API_KEY="your_key"
python build_universe_iwv.py --outdir ./data --headless 0 --fmp-map 1
```

### 2) 이미 받은 CSV로만 실행

다운로드는 건너뛰고, 기존 CSV만 파싱·보정합니다.

```powershell
python build_universe_iwv.py --outdir ./data --csv "C:\Users\사용자명\Downloads\IWV_holdings.csv" --fmp-map 1
```

### 3) 단건 실행 예 (PowerShell)

```powershell
python build_universe_iwv.py --outdir ./data --headless 0 --timeout-sec 60 --download-wait-sec 60 --fmp-map 1 --fmp-cache ./data/fmp_stock_list.json
```

## CLI 옵션

| 옵션 | 설명 | 기본값 |
|------|------|--------|
| `--outdir` | 출력 디렉터리 | `./data` |
| `--headless` | 1=헤드리스 브라우저 | 0 |
| `--timeout-sec` | 페이지 로드 타임아웃(초) | 60 |
| `--download-wait-sec` | 다운로드 대기(초) | 60 |
| `--fmp-map` | 1=FMP 심볼리스트로 ticker 보정 | 1 |
| `--fmp-cache` | FMP stock list 캐시 파일 경로 | `./data/fmp_stock_list.json` |
| `--csv` | 이미 받은 CSV 경로 (지정 시 다운로드 스킵) | (없음) |

## 출력 파일 (outdir)

- **iwv_holdings_downloaded.csv** – 다운로드 원본 복사
- **universe_list.csv** – 최종 유니버스 (ticker_fixed 확정된 행만)
- **universe_rejects.csv** – REJECT된 행 (CASH, ETF, 파생 등)
- **universe_ambiguous.csv** – AMBIGUOUS (사람 확인용)
- **ticker_map.json** – ticker_raw → ticker_fixed 매핑 + 메타

## match_type

- **EXACT** – ticker_raw가 FMP 심볼과 동일
- **NORMALIZED** – 축약/하이픈 제거 후 후보 1개
- **NAME_FUZZY** – 후보 2개 이상일 때 회사명 유사도로 선택 (rapidfuzz)
- **AMBIGUOUS** – 후보 복수·유사도 타이 → 수동 확인
- **REJECT** – 비종목(현금, ETF, 파생 등) 또는 빈 티커

결과는 같은 폴더에 위 파일들로 저장됩니다.
