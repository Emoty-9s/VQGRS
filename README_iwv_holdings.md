# IWV ETF 홀딩스 → universe_list.csv

iShares Russell 3000 ETF(IWV) 종목 리스트를 **Ticker, Name, Sector** 만 추려 `universe_list.csv`로 저장하는 스크립트입니다.

## 필요한 것

- Python 3.8+
- `requests`, `pandas` (아래 설치 방법 참고)

## 설치

```powershell
cd d:\VQGRS_Project
py -3 -m pip install -r requirements.txt
```

## 사용법

### 1) 공식 사이트에서 자동 다운로드 시도

```powershell
py -3 fetch_iwv_holdings.py
```

사이트 정책에 따라 CSV가 안 받아질 수 있습니다. 그럴 경우 아래 수동 방법을 사용하세요.

### 2) 수동 다운로드한 CSV로 실행 (권장)

1. 브라우저에서 [iShares Russell 3000 ETF (IWV)](https://www.ishares.com/us/products/239714/ishares-russell-3000-etf) 접속
2. **"Detailed Holdings and Analytics"** 링크로 CSV 다운로드
3. 다운로드한 파일 경로를 인자로 넣어 실행:

```powershell
py -3 fetch_iwv_holdings.py "C:\Users\사용자명\Downloads\IWV_holdings.csv"
```

결과는 같은 폴더에 **`universe_list.csv`** 로 저장됩니다. (종목 코드, 회사 이름, 섹터만 포함)
