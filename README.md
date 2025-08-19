# Database Assistant MCP Server

이 프로젝트는 **SQL 검증 및 성능분석 Amazon Q CLI MCP 서버**를 제공합니다:
DDL 검증, 데이터베이스 스키마 분석, 성능 모니터링을 하나의 통합 서버에서 제공

## 📋 목차

- [개요](#개요)
- [시스템 요구사항](#시스템-요구사항)
- [설치 및 설정](#설치-및-설정)
- [통합된 기능 소개](#통합된-기능-소개)
- [사용 예시](#사용-예시)
- [파일 구조](#파일-구조)
- [문제 해결](#문제-해결)

## 🎯 개요

### 🔄 **2025-01-19 업데이트**
**SQL 검증 및 성능분석 Amazon Q CLI MCP 서버** - DDL 검증, 데이터베이스 스키마 분석, 성능 모니터링을 하나의 서버에서 제공

### 🛠️ **통합된 DB Assistant MCP Server 기능**

#### 📋 **SQL 파일 관리 (4개 도구)**
- `list_sql_files`: SQL 파일 목록 조회
- `copy_sql_to_directory`: 외부 SQL 파일을 sql 디렉토리로 복사
- `validate_sql_file`: 특정 SQL 파일 검증 (로컬/완전 검증 선택)
- `validate_sql_with_database`: 데이터베이스 지정 완전 검증

#### 🔍 **DDL 검증 (6개 도구)**
- `validate_all_sql`: 모든 SQL 파일 일괄 검증 (최대 5개)
- `validate_selected_sql_files`: 선택한 SQL 파일들 검증 (최대 10개)
- `validate_multiple_sql_direct`: 여러 SQL 파일 직접 검증 (최대 15개)
- `check_ddl_conflicts`: DDL 실행 전 충돌 검사
- `create_execution_plan`: 작업 실행 계획 생성
- `confirm_and_execute`: 계획 확인 후 실행

#### 🗄️ **데이터베이스 연결 관리 (6개 도구)**
- `list_database_secrets`: AWS Secrets Manager 시크릿 목록 조회
- `test_database_connection`: 데이터베이스 연결 테스트
- `list_databases`: 데이터베이스 목록 조회
- `select_database`: 데이터베이스 선택 및 변경
- `list_aurora_mysql_clusters`: Aurora MySQL 클러스터 목록 조회
- `select_aurora_cluster`: Aurora 클러스터 선택

#### 📊 **스키마 분석 (3개 도구)**
- `get_schema_summary`: 스키마 요약 정보 조회
- `analyze_current_schema`: 현재 스키마 상세 분석
- `get_aurora_mysql_parameters`: Aurora MySQL 파라미터 조회

#### ⚡ **성능 모니터링 (9개 도구)**
- `get_performance_metrics`: 데이터베이스 성능 메트릭 조회
- `analyze_slow_queries`: 느린 쿼리 분석
- `get_table_io_stats`: 테이블별 I/O 통계
- `get_index_usage_stats`: 인덱스 사용 통계
- `get_connection_stats`: 연결 및 세션 통계
- `get_memory_usage`: 메모리 사용량 조회
- `get_lock_analysis`: 락 상태 분석
- `get_replication_status`: 복제 상태 조회

## 🔧 시스템 요구사항

- Python 3.8+
- AWS CLI 구성 (AWS 자격 증명)
- 필수 Python 패키지:
  ```bash
  pip install boto3 mysql-connector-python mcp
  ```

## ⚙️ 설치 및 설정

### 1. 저장소 클론
```bash
git clone <repository-url>
cd database-assistant-mcp
```

### 2. Python 환경 설정
```bash
python -m venv venv
source venv/bin/activate  # Linux/Mac
# 또는
venv\Scripts\activate     # Windows
```

### 3. 의존성 설치
```bash
pip install -r requirements.txt
```

### 4. MCP 서버 등록
`~/.kiro/settings/mcp.json` 또는 `.kiro/settings/mcp.json` 파일에 다음 설정을 추가:

```json
{
  "mcpServers": {
    "db-assistant": {
      "command": "/path/to/python",
      "args": [
        "/path/to/ddl_validation_qcli_mcp_server.py"
      ],
      "env": {
        "AWS_DEFAULT_REGION": "ap-northeast-2",
        "PYTHONPATH": "/path/to/project"
      },
      "disabled": false,
      "autoApprove": [
        "list_sql_files",
        "list_database_secrets",
        "get_schema_summary",
        "get_performance_metrics"
      ]
    }
  }
}
```

## 🔍 통합된 기능 소개

### 📋 SQL 파일 관리

#### 1. SQL 파일 목록 조회
```python
# 사용법
list_sql_files()
```

#### 2. 외부 SQL 파일 복사
```python
# 사용법
copy_sql_to_directory(source_path="/path/to/file.sql", target_name="new_file.sql")
```

### 🔍 DDL 검증

#### 1. 특정 파일 검증 (선택적 DB 연결)
```python
# 로컬 검증만
validate_sql_file(filename="test.sql")

# 완전 검증 (DB 연결)
validate_sql_file(filename="test.sql", database_secret="my-db-secret")
```

#### 2. 모든 SQL 파일 일괄 검증
```python
# 최대 5개 파일 검증
validate_all_sql(database_secret="my-db-secret")
```

#### 3. 선택한 파일들 검증
```python
# 최대 10개 파일 검증
validate_selected_sql_files(
    database_secret="my-db-secret",
    sql_files=["file1.sql", "file2.sql", "file3.sql"]
)
```

#### 4. 여러 파일 직접 검증
```python
# 계획 없이 바로 실행 (최대 15개)
validate_multiple_sql_direct(database_secret="my-db-secret", file_count=10)
```

### 🗄️ 데이터베이스 연결 관리

#### 1. 시크릿 목록 조회
```python
# 사용법
list_database_secrets(keyword="mysql")
```

#### 2. 연결 테스트
```python
# 사용법
test_database_connection(database_secret="my-db-secret")
```

#### 3. Aurora 클러스터 관리
```python
# 클러스터 목록 조회
list_aurora_mysql_clusters(region="ap-northeast-2")

# 클러스터 선택
select_aurora_cluster(cluster_selection="1", region="ap-northeast-2")
```

### 📊 스키마 분석

#### 1. 스키마 요약 정보
```python
# 사용법
get_schema_summary(database_secret="my-db-secret")
```

#### 2. 상세 스키마 분석
```python
# 사용법
analyze_current_schema(database_secret="my-db-secret")
```

#### 3. Aurora 파라미터 조회
```python
# 사용법
get_aurora_mysql_parameters(
    cluster_identifier="my-cluster",
    region="ap-northeast-2",
    filter_type="important",
    category="performance"
)
```

### ⚡ 성능 모니터링

#### 1. 성능 메트릭 조회
```python
# 사용법
get_performance_metrics(database_secret="my-db-secret", metric_type="all")
```

#### 2. 느린 쿼리 분석
```python
# 사용법
analyze_slow_queries(database_secret="my-db-secret", limit=10)
```

#### 3. I/O 통계
```python
# 사용법
get_table_io_stats(database_secret="my-db-secret", schema_name="mydb")
```

#### 4. 인덱스 사용 통계
```python
# 사용법
get_index_usage_stats(database_secret="my-db-secret", table_name="users")
```

## 🧪 사용 예시

### 기본 워크플로우

**1단계: SQL 파일 확인**
```
Q CLI에서 입력: "SQL 파일 목록을 보여주세요"

예상 응답:
SQL 파일 목록:
- sample_create_table.sql
- test_good_table.sql  
- test_bad_naming.sql
- test_syntax_error.sql
- sample_alter_table.sql
```

**2단계: 데이터베이스 시크릿 조회**
```
Q CLI에서 입력: "데이터베이스 시크릿 목록을 조회해주세요"

예상 응답:
데이터베이스 시크릿 목록:
- rds-mysql-dev-secret
- aurora-prod-secret
- test-db-credentials
```

**3단계: 연결 테스트**
```
Q CLI에서 입력: "rds-mysql-dev-secret으로 데이터베이스 연결을 테스트해주세요"

예상 응답:
✅ 데이터베이스 연결 성공!

**연결 정보:**
- 서버 버전: 8.0.35
- 현재 데이터베이스: testdb
- 연결 방식: SSH Tunnel

**데이터베이스 목록:**
   - testdb
   - userdb
   - productdb
```

**4단계: SQL 파일 검증**
```
Q CLI에서 입력: "test_good_table.sql 파일을 rds-mysql-dev-secret으로 검증해주세요"

예상 응답:
✅ 모든 검증을 통과했습니다.

📊 검증 결과:
• 문법 검증: ✅ 통과
• 데이터베이스 연결: ✅ 성공
• 스키마 검증: ✅ 통과
• 제약조건 검증: ✅ 통과
• Claude AI 검증: ✅ 통과

📄 상세 보고서가 저장되었습니다: output/validation_report_test_good_table.sql_20250119_143022.html
```

**5단계: 전체 파일 일괄 검증**
```
Q CLI에서 입력: "모든 SQL 파일을 rds-mysql-dev-secret으로 검증해주세요"

예상 응답:
📊 전체 SQL 파일 검증 완료

📋 요약:
• 총 파일: 5개
• 통과: 3개 (60.0%)
• 실패: 2개 (40.0%)

📄 종합 보고서: output/consolidated_validation_report_20250119_143045.html

📊 개별 결과:
✅ **sample_create_table.sql**: 통과
✅ **test_good_table.sql**: 통과
❌ **test_bad_naming.sql**: 실패 (2개 문제)
❌ **test_syntax_error.sql**: 실패 (1개 문제)
✅ **sample_alter_table.sql**: 통과
```

**6단계: 성능 분석**
```
Q CLI에서 입력: "데이터베이스 성능 메트릭을 조회해주세요"

예상 응답:
📊 **데이터베이스 성능 메트릭**

🔍 **느린 쿼리 TOP 5:**
1. SELECT * FROM users WHERE email LIKE ? ORDER BY created_at DESC
   - 실행횟수: 1,234, 평균시간: 2.456초, 최대시간: 5.123초

🔗 **연결 통계:**
- 총 연결: 15개
- 활성 연결: 3개
```

### 고급 사용 시나리오

**스키마 상세 분석**
```
Q CLI에서 입력: "현재 데이터베이스의 스키마를 상세 분석해주세요"

예상 응답:
✅ 스키마 분석 완료 (DB: testdb)

📊 **분석 결과:**
- 총 테이블 수: 3개

📋 **테이블 상세:**
🔹 **users** (InnoDB)
   - 컬럼: 5개, 인덱스: 2개, 외래키: 0개
   - 예상 행 수: 1,250

   📋 **컬럼 정보:**
      • id: INT(11) NOT NULL [PRI] AUTO_INCREMENT
      • email: VARCHAR(255) NOT NULL [UNI]
      • name: VARCHAR(100) NOT NULL
      • created_at: TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
      • updated_at: TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP

   🔍 **인덱스 정보:**
      • PRIMARY (id) [BTREE]
      • UNIQUE email_unique (email) [BTREE]

   🔗 **외래키 정보:**
      외래키가 없습니다.
```

**Aurora 파라미터 조회**
```
Q CLI에서 입력: "aurora-prod-cluster의 성능 관련 파라미터를 조회해주세요"

예상 응답:
📊 Aurora MySQL 파라미터 정보 (성능 최적화)

🔧 **클러스터 정보:**
- 클러스터 ID: aurora-prod-cluster
- 클러스터 파라미터 그룹: default.aurora-mysql8.0
- 엔진 버전: 8.0.mysql_aurora.3.02.0

🏗️ **클러스터 레벨 파라미터:**
⚡ 성능 최적화:
• innodb_thread_concurrency: 0 (Source: engine-default)
• innodb_read_io_threads: 4 (Source: engine-default)
• innodb_write_io_threads: 4 (Source: engine-default)
• thread_cache_size: 9 (Source: engine-default)
```

## 📁 파일 구조

### 핵심 서버 파일
```
├── ddl_validation_qcli_mcp_server.py    # 통합 DB Assistant MCP 서버 (메인)
└── README.md                            # 이 파일
```

### SQL 검증 관련 파일
```
├── sql/                                 # SQL 테스트 파일들
│   ├── sample_create_table.sql
│   ├── sample_alter_table.sql
│   ├── test_good_table.sql
│   ├── test_bad_naming.sql
│   ├── test_syntax_error.sql
│   └── ...
├── output/                              # 검증 보고서 출력
│   ├── validation_report_*.html
│   ├── consolidated_validation_report_*.html
│   └── validation_report_*.md
└── logs/                               # 로그 파일
    └── ddl_validation.log
```

### 지원 파일
```
├── ssh_tunnel.sh                       # SSH 터널 스크립트
├── requirements.txt                    # Python 의존성
└── backup files/                       # 백업 파일들
    ├── ddl_validation_qcli_mcp_server backup_20250729_1.py
    └── ddl_validation_qcli_mcp_server backup_20250729_2.py
```

## 🔧 문제 해결

### 일반적인 문제

#### 1. MySQL 연결 오류
```bash
pip install mysql-connector-python
```

#### 2. SSH 터널 설정 실패
- SSH 키 파일 경로 확인: `/Users/heungh/test.pem`
- SSH 서버 접근 권한 확인: `ec2-user@54.180.79.255`

#### 3. AWS 자격 증명 오류
```bash
aws configure
# 또는 환경 변수 설정
export AWS_ACCESS_KEY_ID=your_access_key
export AWS_SECRET_ACCESS_KEY=your_secret_key
export AWS_DEFAULT_REGION=ap-northeast-2
```

#### 4. Claude AI 호출 오류
- Bedrock 서비스 접근 권한 확인
- us-east-1 리전에서 Claude 모델 사용 가능 여부 확인
- 모델 ID 확인: `us.anthropic.claude-sonnet-4-20250514-v1:0`

#### 5. 패키지 의존성 오류
```bash
pip install --upgrade boto3 mysql-connector-python mcp
```

### 로그 확인
서버 실행 시 로그를 확인하여 문제를 진단할 수 있습니다:
```bash
python ddl_validation_qcli_mcp_server.py
```

로그 파일 위치: `logs/ddl_validation.log`

### 디렉토리 권한
출력 디렉토리에 대한 쓰기 권한이 있는지 확인:
```bash
chmod 755 output/
chmod 755 logs/
```

### 성능 최적화

#### 1. SSH 터널 연결 최적화
- SSH 연결 유지 시간 조정
- 터널 재사용을 위한 연결 풀링

#### 2. Claude AI 호출 최적화
- 토큰 수 제한으로 응답 시간 단축
- 모델 fallback 메커니즘 활용

#### 3. 데이터베이스 연결 최적화
- 연결 타임아웃 설정
- 연결 재사용 패턴 적용

## 📞 지원

문제가 발생하거나 기능 요청이 있으시면 이슈를 생성해 주세요.

## 📄 라이선스

이 프로젝트는 MIT 라이선스 하에 배포됩니다.

---

## 🆕 최신 업데이트 (2025-01-19)

### 새로운 기능
- **Claude AI 검증**: Claude Sonnet 4 모델을 활용한 고급 DDL 검증
- **실행 계획 시스템**: 작업 실행 전 계획 생성 및 확인
- **통합 HTML 보고서**: 클릭 가능한 링크가 포함된 종합 보고서
- **Aurora 파라미터 조회**: 카테고리별 파라미터 필터링 및 조회
- **성능 모니터링**: 9가지 성능 메트릭 도구 제공

### 개선사항
- **SSH 터널 안정성**: 연결 설정 및 정리 로직 개선
- **오류 처리**: 상세한 오류 메시지 및 디버그 정보 제공
- **로깅 시스템**: 파일 및 콘솔 동시 로깅 지원
- **HTML 보고서**: 반응형 디자인 및 사용자 경험 개선

### 호환성
- Python 3.8+ 지원
- MySQL 8.0+ 최적화
- Aurora MySQL 3.x 지원
- AWS Bedrock Claude 모델 통합
