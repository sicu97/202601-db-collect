# 202601-db-collect

데이터베이스 수집 프로젝트

Python을 사용하여 다양한 데이터베이스에서 SQL 쿼리를 실행하고 결과를 SQLite에 수집하는 도구입니다.

## 아키텍처 구조

```
┌─────────────────────────────────────────────────────────────┐
│                      db_collector.py                        │
│                     (DBCollector Class)                     │
└─────────────────────────────────────────────────────────────┘
                              │
                              │
        ┌─────────────────────┼─────────────────────┐
        │                     │                     │
        ▼                     ▼                     ▼
┌──────────────┐      ┌──────────────┐     ┌──────────────┐
│  config.yaml │      │ sql_queries/ │     │   .env       │
│              │      │              │     │              │
│ - DB 연결정보 │      │ - *.sql 파일 │     │ - 환경변수    │
│ - SQLite 경로│      │              │     │              │
│ - 실행 옵션   │      │              │     │              │
└──────────────┘      └──────────────┘     └──────────────┘
        │                     │                     
        └─────────────────────┘                     
                    │                               
                    ▼                               
        ┌───────────────────────┐                   
        │   데이터 수집 프로세스   │                   
        └───────────────────────┘                   
                    │                               
        ┌───────────┴───────────┐                   
        │                       │                   
        ▼                       ▼                   
┌──────────────┐        ┌──────────────┐           
│  Source DB   │        │  SQLite DB   │           
│              │        │              │           
│ PostgreSQL   │───────▶│ data/*.db    │           
│ MySQL        │        │              │           
│ Oracle       │        │ - 수집된 데이터│           
│ MS SQL       │        │ - 테이블별 저장│           
└──────────────┘        └──────────────┘           
```

### 데이터 흐름

1. **설정 로드**: `config.yaml`과 `.env`에서 DB 연결 정보 및 옵션 로드
2. **SQL 파일 스캔**: `sql_queries/` 디렉터리의 모든 `.sql` 파일 읽기
3. **소스 DB 연결**: 설정된 데이터베이스에 연결
4. **쿼리 실행**: 각 SQL 파일의 쿼리를 소스 DB에서 실행
5. **결과 수집**: 쿼리 결과를 메모리로 가져오기
6. **SQLite 저장**: 배치 단위로 SQLite 데이터베이스에 저장
7. **로깅**: 각 단계별 실행 결과 및 에러 로깅

### 주요 컴포넌트

- **DBCollector**: 메인 클래스, 전체 수집 프로세스 관리
  - `_load_config()`: 설정 파일 로드
  - `connect_sqlite()`: SQLite 연결
  - `get_source_connection()`: 소스 DB 연결
  - `load_sql_files()`: SQL 파일 로드
  - `execute_and_collect()`: 쿼리 실행 및 수집
  - `_save_to_sqlite()`: SQLite에 데이터 저장
  - `run_all()`: 모든 SQL 파일 일괄 실행

## 기능

- 여러 데이터베이스 지원 (PostgreSQL, MySQL 등)
- SQL 파일 기반 쿼리 관리
- SQLite로 데이터 수집 및 저장
- 배치 처리 및 로깅

## 설치

```bash
pip install -r requirements.txt
```

## 설정

1. `.env.example`을 `.env`로 복사하고 데이터베이스 정보 입력
2. `config.yaml`에서 데이터베이스 연결 정보 설정
3. `sql_queries/` 디렉터리에 실행할 SQL 파일 추가

## 사용법

```bash
python db_collector.py
```

## 디렉터리 구조

```
.
├── config.yaml           # 설정 파일
├── db_collector.py       # 메인 실행 파일
├── sql_queries/          # SQL 쿼리 파일들
│   └── example_query.sql
├── data/                 # SQLite 데이터베이스 저장 위치
├── requirements.txt      # Python 패키지 의존성
├── .env                  # 환경 변수 (git 제외)
└── .env.example          # 환경 변수 예시

```

## 작업 히스토리

### 2025-01-16
- **프로젝트 초기 설정**
  - GitHub 리포지토리 생성 및 연결
  - Git 초기화 및 첫 커밋
  
- **기본 구조 구축**
  - `db_collector.py`: 메인 수집 모듈 작성
    - DBCollector 클래스 구현
    - PostgreSQL, MySQL 연결 지원
    - SQL 파일 로드 기능
    - SQLite 저장 기능
    - 배치 처리 및 로깅
  
  - `config.yaml`: 설정 파일 생성
    - 다중 데이터베이스 연결 설정
    - SQLite 출력 경로 설정
    - 실행 옵션 (배치 크기, 타임아웃, 로그 레벨)
  
  - `requirements.txt`: 의존성 패키지 정의
    - PyYAML, psycopg2-binary, pymysql
    - sqlalchemy, python-dotenv
  
  - `sql_queries/`: SQL 쿼리 디렉터리 생성
    - 예시 쿼리 파일 추가
  
  - `.gitignore`: Git 제외 파일 설정
    - Python 캐시, 가상환경
    - 데이터베이스 파일, 환경 변수
    - IDE 설정 파일
  
  - `README.md`: 프로젝트 문서화
    - 아키텍처 다이어그램
    - 데이터 흐름 설명
    - 사용법 및 설치 가이드

### 향후 계획
- [ ] 에러 핸들링 강화
- [ ] 쿼리 실행 스케줄링 기능
- [ ] 데이터 변환 및 검증 기능
- [ ] 실행 결과 리포트 생성
- [ ] CLI 인터페이스 개선
- [ ] 단위 테스트 작성
