# VQGRS Grouping Engine

규칙 기반 비교 그룹 생성 및 그룹별 대표값 산출 엔진.

## 기능 요약

| Group | 이름 | 방식 |
|-------|------|------|
| **A** | Sector/Industry Hybrid | 산업·섹터 대표값 블렌딩 (alpha 가중) |
| **B** | Size Nearest-Neighbour | log(market_cap) 거리 기반 상위 N 피어 |
| **C** | Lifecycle | 성장/수익화 단계 5-label 분류 |
| **D** | Financial Structure | 재무 체질 5-label 분류 |
| **E** | Market Behaviour | 시장 행동 5-label 분류 (percentile 기반) |

각 그룹별로 종목마다 comparable metric 대표값(median / trimmed mean / winsorised mean)을 산출합니다.

---

## 설치

```bash
pip install -r requirements.txt
```

필수 패키지: `pandas`, `numpy`, `pyarrow`, `pydantic`, `pyyaml`, `pytest`

---

## 입력 데이터

### Parquet (기본)

단일 파일 또는 여러 파일을 `ticker + as_of_date` 기준으로 자동 merge합니다.

#### 내부 표준 컬럼 (주요)

| 분류 | 컬럼 |
|------|------|
| 식별 | `ticker`, `as_of_date`, `sector`, `industry`, `market_cap` |
| Valuation | `pe`, `ps`, `ev_ebitda` |
| Profitability | `revenue_ttm`, `eps_ttm`, `ocf_ttm`, `opm`, `roic`, `ocf_ni` |
| Risk | `debt_ratio`, `current_ratio`, `interest_coverage` |
| Behaviour | `beta`, `price_volatility`, `momentum_3m`, `rsi_14`, `volume_ratio` |

원본 컬럼명이 다를 경우 **column mapping YAML** 로 매핑합니다.
`data/column_mapping_sample.yaml` 을 참고하세요.

---

## 실행

### 단일 Parquet

```bash
python -m src.main \
  --input-parquet data/universe.parquet \
  --mapping-config data/column_mapping_sample.yaml \
  --group-config src/config/default_group_config.yaml \
  --metric-config src/config/default_metric_config.yaml \
  --output-dir outputs/
```

### 여러 Parquet

```bash
python -m src.main \
  --input-parquet data/security_master.parquet data/fundamentals.parquet data/market.parquet \
  --mapping-config data/column_mapping_sample.yaml \
  --group-config src/config/default_group_config.yaml \
  --metric-config src/config/default_metric_config.yaml \
  --output-dir outputs/
```

### 폴더 입력

```bash
python -m src.main \
  --input-dir data/parquets/ \
  --mapping-config data/column_mapping_sample.yaml \
  --group-config src/config/default_group_config.yaml \
  --metric-config src/config/default_metric_config.yaml \
  --output-dir outputs/
```

### 옵션

| 플래그 | 설명 |
|--------|------|
| `--no-csv` | CSV 내보내기 생략 (Parquet만 저장) |

---

## 출력 파일

| 파일 | 형식 | 설명 |
|------|------|------|
| `group_membership.parquet` | 종목당 1행 | 각 그룹 라벨 + confidence |
| `group_representatives.parquet` | long format | ticker × group × metric별 대표값 |
| `invalid_logs.parquet` | 로그 | clip / invalid 처리 기록 |
| `debug.json` | JSON | rule_path, alpha, neighbor 등 상세 |

CSV 버전도 기본으로 함께 저장됩니다 (`--no-csv` 미사용 시).

---

## 설정 파일

### `default_group_config.yaml`

각 그룹의 분류 임계값 (alpha_base, n_size, LC/FS/MB 판정 기준 등).

### `default_metric_config.yaml`

- **comparable_metrics**: 비교 대상 metric 목록
- **aggregation_methods**: metric별 집계 방식 (median / trimmed_mean / winsorized_mean)
- **clip_rules**: metric별 outlier clip 규칙
- **confidence_thresholds**: 유효 표본 수 기반 confidence 판정
- **trimmed/winsorized_mean_rules**: 표본 크기별 절사/윈서화 비율

### `column_mapping_sample.yaml`

원본 컬럼명 → 내부 표준 컬럼명 매핑. 프로젝트에 맞게 수정하세요.

---

## 테스트

```bash
pytest tests/ -v
```

synthetic 데이터 기반 테스트:

- `test_loader.py` — Parquet/CSV 로드, merge, 날짜 파싱
- `test_preprocess.py` — 파생지표 계산
- `test_representative.py` — median, trimmed mean, winsorised mean
- `test_group_a.py` — alpha 공식, hybrid 대표값
- `test_group_b.py` — nearest-neighbour, 거리 통계
- `test_group_c.py` — lifecycle 분류 우선순위
- `test_group_d.py` — 재무 구조 분류 우선순위
- `test_group_e.py` — 시장 행동 percentile 분류

---

## 프로젝트 구조

```
src/
  config/
    default_group_config.yaml
    default_metric_config.yaml
  vqgrs_grouping/
    __init__.py
    models.py          # Pydantic config models + result dataclasses
    utils.py           # 로깅, 안전 나눗셈
    confidence.py      # confidence 레벨 산출
    loader.py          # Parquet/CSV 로드, merge
    mapper.py          # 컬럼 매핑 + 검증
    preprocess.py      # 파생지표 계산
    outlier.py         # clip / winsorise / invalid
    metrics.py         # metric config 로드 + 검증
    representative.py  # 대표값 집계 (median/trimmed/winsorised)
    grouping_a.py      # Sector/Industry Hybrid
    grouping_b.py      # Size Nearest-Neighbour
    grouping_c.py      # Lifecycle
    grouping_d.py      # Financial Structure
    grouping_e.py      # Market Behaviour
    pipeline.py        # 전체 파이프라인 오케스트레이션
    exporters.py       # Parquet/CSV/JSON 내보내기
  main.py              # CLI 진입점
tests/
  conftest.py          # 공유 fixtures
  test_*.py            # 모듈별 테스트
```

---

## 제약 사항

- 동일 `as_of_date` 단면에서만 그룹/대표값 계산 (미래 데이터 누수 방지)
- 완전 deterministic: 같은 입력 → 같은 출력
- 외부 API / DB / LLM 호출 없음
- V/Q/G/R/S 점수, LTI/STI, penalty, 매매 판단은 구현 범위 밖
