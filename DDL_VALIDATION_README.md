# DDL Validation Q CLI MCP Server

DDL(Data Definition Language) 검증 및 데이터베이스 스키마 분석을 위한 MCP (Model Context Protocol) 서버입니다.

## 📋 목차

- [개요](#개요)
- [주요 기능](#주요-기능)
- [시스템 요구사항](#시스템-요구사항)
- [설치 및 설정](#설치-및-설정)
- [사용법](#사용법)
- [Amazon Q CLI 테스트 예시](#amazon-q-cli-테스트-예시)
- [검증 결과 예시](#검증-결과-예시)
- [파일 구조](#파일-구조)
- [문제 해결](#문제-해결)

## 🎯 개요

DDL Validation Q CLI MCP Server는 다음과 같은 기능을 제공합니다:

- **DDL 구문 검증**: CREATE, ALTER, DROP 등 DDL 문의 구문 및 논리적 검증
- **스키마 호환성 검증**: 실제 데이터베이스 스키마와 비교하여 충돌 여부 확인
- **데이터 타입 호환성 검증**: 컬럼 타입 변경 시 데이터 손실 가능성 검증
- **제약조건 검증**: 외래키, 인덱스 등 제약조건의 유효성 확인
- **Claude AI 통합**: 고급 DDL 분석 및 권장사항 제공
- **상세 보고서 생성**: HTML 형식의 시각적 검증 보고서

## 🔧 주요 기능

### 1. DDL 구문 유형별 검증

#### CREATE TABLE
- 테이블 존재 여부 확인
- 이미 존재하는 테이블 생성 시도 시 오류 발생

#### ALTER TABLE
- **ADD COLUMN**: 추가하려는 컬럼이 이미 존재하는지 확인
- **DROP COLUMN**: 삭제하려는 컬럼이 존재하는지 확인
- **MODIFY COLUMN**: 데이터 타입 호환성 및 길이 축소 검증
- **CHANGE COLUMN**: 컬럼명 변경 및 타입 호환성 검증

#### CREATE INDEX
- 대상 테이블 존재 여부 확인
- 인덱스 대상 컬럼 존재 여부 확인
- 동일한 인덱스명 중복 여부 확인

#### DROP TABLE/INDEX
- 삭제 대상 테이블/인덱스 존재 여부 확인

### 2. 데이터 타입 호환성 검증
- 문자열 ↔ 숫자 타입 변경 시 데이터 손실 경고
- VARCHAR/CHAR 길이 축소 시 경고
- DECIMAL 정밀도/스케일 축소 시 경고

### 3. 데이터베이스 연결
- AWS Secrets Manager를 통한 안전한 DB 연결 정보 관리
- SSH 터널을 통한 보안 연결 지원
- 다중 데이터베이스 환경 지원

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
cd ddl-validation-mcp
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
pip install boto3 mysql-connector-python mcp
```

### 4. MCP 서버 등록
`~/.kiro/settings/mcp.json` 또는 `.kiro/settings/mcp.json` 파일에 다음 설정을 추가:

```json
{
  "mcpServers": {
    "ddl-qcli-validator": {
      "command": "/path/to/python",
      "args": [
        "/path/to/ddl_validation_qcli_mcp_server.py"
      ],
      "env": {
        "AWS_DEFAULT_REGION": "us-east-1",
        "PYTHONPATH": "/path/to/project"
      },
      "disabled": false,
      "autoApprove": [
        "list_sql_files",
        "list_database_secrets"
      ]
    }
  }
}
```

## 📖 사용법

### 주요 도구 (Tools)

#### 1. SQL 파일 목록 조회
```python
list_sql_files()
```

#### 2. 데이터베이스 시크릿 목록 조회
```python
list_database_secrets(keyword="")
```

#### 3. SQL 파일 검증
```python
validate_sql_file(filename="sample_create_table.sql", database_secret="my-db-secret")
```

#### 4. 모든 SQL 파일 검증
```python
validate_all_sql(database_secret="my-db-secret")
```

#### 5. SQL 파일 복사
```python
copy_sql_to_directory(source_path="path/to/file.sql", target_name="new_name.sql")
```

#### 6. 데이터베이스 연결 테스트
```python
test_database_connection(database_secret="my-db-secret")
```

#### 7. 현재 스키마 분석
```python
analyze_current_schema(database_secret="my-db-secret")
```

## 🧪 Amazon Q CLI 테스트 예시

### 기본 테스트 시나리오

**1단계: SQL 파일 목록 확인**
```
Q CLI에서 입력: "DDL 검증 서버에서 사용 가능한 SQL 파일 목록을 보여주세요"

예상 응답:
📁 SQL 파일 목록:
- sample_create_table.sql
- test_good_table.sql  
- test_bad_naming.sql
- test_syntax_error.sql
- test_create_existing_table.sql
- test_alter_add_existing_column.sql
- test_alter_modify_incompatible_type.sql
- test_create_index_nonexistent_column.sql
- test_drop_nonexistent_table.sql
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

**3단계: 정상 DDL 검증 테스트**
```
Q CLI에서 입력: "sample_create_table.sql 파일을 rds-mysql-dev-secret으로 검증해주세요"

테스트할 SQL 내용:
CREATE TABLE users (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    username VARCHAR(50) NOT NULL,
    email VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

예상 응답:
✅ 모든 검증을 통과했습니다.
📄 상세 보고서가 저장되었습니다: output/validation_report_sample_create_table.sql_20250730_192506.html
```

**4단계: 문법 오류 DDL 검증 테스트**
```
Q CLI에서 입력: "test_syntax_error.sql 파일을 검증해주세요"

테스트할 SQL 내용 (세미콜론 누락):
CREATE TABLE shopping_carts (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    user_id BIGINT NOT NULL,
    session_id VARCHAR(128),
    cart_status ENUM('active', 'abandoned', 'converted') DEFAULT 'active',
    total_items INT DEFAULT 0,
    total_amount DECIMAL(12,2) DEFAULT 0.00,
    expires_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    CONSTRAINT fk_shopping_carts_users FOREIGN KEY (user_id) REFERENCES users(id),
    CONSTRAINT ck_shopping_carts_total_items CHECK (total_items >= 0)
    -- 세미콜론 누락

예상 응답:
❌ 발견된 문제: 1개
• 세미콜론이 누락되었습니다.
📄 상세 보고서가 저장되었습니다: output/validation_report_test_syntax_error.sql_20250730_192540.html
```

**5단계: 명명 규칙 위반 DDL 검증 테스트**
```
Q CLI에서 입력: "test_bad_naming.sql 파일을 검증해주세요"

테스트할 SQL 내용 (PascalCase 사용):
CREATE TABLE ProductCategory (
    CategoryID int AUTO_INCREMENT PRIMARY KEY,
    CategoryName varchar(100) NOT NULL,
    ParentCategoryID int,
    CategoryDesc text,
    IsActive tinyint DEFAULT 1,
    CreateDate datetime DEFAULT CURRENT_TIMESTAMP,
    UpdateDate datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

예상 응답:
❌ 발견된 문제: 2개
• Claude 검증: 테이블명과 컬럼명이 snake_case 명명 규칙을 따르지 않습니다...
• 스키마 검증: 권장되지 않는 명명 규칙이 사용되었습니다.
📄 상세 보고서가 저장되었습니다: output/validation_report_test_bad_naming.sql_20250730_192540.html
```

**6단계: 스키마 충돌 검증 테스트**
```
Q CLI에서 입력: "test_create_existing_table.sql 파일을 검증해주세요"

테스트할 SQL 내용 (이미 존재하는 테이블 생성):
CREATE TABLE users (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    username VARCHAR(50) NOT NULL,
    email VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

예상 응답:
❌ 발견된 문제: 1개
• 스키마 검증: 테이블 'users'이 이미 존재합니다.

🏗️ 스키마 검증 결과:
❌ CREATE_TABLE: users
• 테이블이 이미 존재하여 생성할 수 없습니다.

📄 상세 보고서가 저장되었습니다: output/validation_report_test_create_existing_table.sql_20250730_192540.html
```

**7단계: ALTER TABLE 검증 테스트**
```
Q CLI에서 입력: "test_alter_add_existing_column.sql 파일을 검증해주세요"

테스트할 SQL 내용 (이미 존재하는 컬럼 추가):
ALTER TABLE users ADD COLUMN username VARCHAR(100);

예상 응답:
❌ 발견된 문제: 1개
• 스키마 검증: 컬럼 'username'이 이미 존재합니다.

🏗️ 스키마 검증 결과:
❌ ALTER_TABLE (ADD_COLUMN): users
• 추가하려는 컬럼이 이미 존재합니다.
• 기존 컬럼: id, username, email, created_at, updated_at

📄 상세 보고서가 저장되었습니다: output/validation_report_test_alter_add_existing_column.sql_20250730_192603.html
```

**8단계: 데이터 타입 호환성 검증**
```
Q CLI에서 입력: "test_alter_modify_incompatible_type.sql 파일을 검증해주세요"

테스트할 SQL 내용 (호환되지 않는 타입 변경):
ALTER TABLE users MODIFY COLUMN email INT;

예상 응답:
❌ 발견된 문제: 1개
• 스키마 검증: 데이터 타입을 VARCHAR에서 INT로 변경하는 것은 데이터 손실을 야기할 수 있습니다.

🏗️ 스키마 검증 결과:
❌ ALTER_TABLE (MODIFY_COLUMN): users
• 호환되지 않는 데이터 타입 변경 시도
• 기존: email VARCHAR(255)
• 변경 시도: email INT

📄 상세 보고서가 저장되었습니다: output/validation_report_test_alter_modify_incompatible_type.sql_20250730_192615.html
```

**9단계: 인덱스 검증 테스트**
```
Q CLI에서 입력: "test_create_index_nonexistent_column.sql 파일을 검증해주세요"

테스트할 SQL 내용 (존재하지 않는 컬럼에 인덱스 생성):
CREATE INDEX idx_users_nonexistent ON users(nonexistent_column);

예상 응답:
❌ 발견된 문제: 1개
• 스키마 검증: 컬럼 'nonexistent_column'이 테이블 'users'에 존재하지 않습니다.

🏗️ 스키마 검증 결과:
❌ CREATE_INDEX: users
• 인덱스 대상 컬럼이 존재하지 않습니다.
• 인덱스명: idx_users_nonexistent
• 대상 컬럼: nonexistent_column

📄 상세 보고서가 저장되었습니다: output/validation_report_test_create_index_nonexistent_column.sql_20250730_192625.html
```

**10단계: 복합 검증 테스트**
```
Q CLI에서 입력: "test_good_table.sql 파일을 rds-mysql-dev-secret으로 검증해주세요"

테스트할 SQL 내용 (완전한 DDL):
CREATE TABLE user_profiles (
    id BIGINT AUTO_INCREMENT,
    user_id BIGINT NOT NULL,
    profile_image_url VARCHAR(500),
    bio_description TEXT,
    birth_date DATE,
    gender ENUM('M', 'F', 'O') DEFAULT 'O',
    country_code CHAR(2),
    timezone_offset INT DEFAULT 0,
    is_public BOOLEAN DEFAULT FALSE,
    last_login_at TIMESTAMP NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    CONSTRAINT pk_user_profiles PRIMARY KEY (id),
    CONSTRAINT fk_user_profiles_users FOREIGN KEY (user_id) REFERENCES users(id),
    CONSTRAINT uk_user_profiles_user_id UNIQUE (user_id)
) COMMENT '사용자 프로필 정보 테이블';

예상 응답:
✅ 모든 검증을 통과했습니다.

🔗 데이터베이스 연결 정보:
• 호스트: mysql-dev.cluster-xxx.ap-northeast-2.rds.amazonaws.com
• 연결 상태: ✅ 성공
• 서버 버전: 8.0.35

🏗️ 스키마 검증 결과:
✅ CREATE_TABLE: user_profiles
• 테이블이 존재하지 않아 생성 가능
• 참조 테이블 users 존재 확인
• 모든 제약조건 유효

📄 상세 보고서가 저장되었습니다: output/validation_report_test_good_table.sql_20250730_192603.html
```

### 고급 테스트 시나리오

**전체 SQL 파일 일괄 검증**
```
Q CLI에서 입력: "모든 SQL 파일을 rds-mysql-dev-secret으로 검증해주세요"

예상 응답:
📊 전체 SQL 파일 검증 결과:

✅ sample_create_table.sql - 통과
✅ test_good_table.sql - 통과  
❌ test_bad_naming.sql - 실패 (2개 문제)
❌ test_syntax_error.sql - 실패 (1개 문제)
❌ test_create_existing_table.sql - 실패 (1개 문제)
✅ sample_alter_table.sql - 통과

📋 요약:
• 총 파일: 6개
• 통과: 3개 (50%)
• 실패: 3개 (50%)

📄 종합 보고서가 저장되었습니다: output/all_validation_report_20250730_195320.html
```

**외부 SQL 파일 복사 및 검증**
```
Q CLI에서 입력: "외부 SQL 파일을 복사해서 검증하고 싶습니다"

1단계: 파일 복사
copy_sql_to_directory(source_path="/path/to/my_table.sql", target_name="my_test.sql")

2단계: 검증 실행
validate_sql_file(filename="my_test.sql", database_secret="rds-mysql-dev-secret")
```

## 📊 검증 결과 예시

### ✅ 성공 사례
```
✅ 모든 검증을 통과했습니다.

📊 검증 결과:
• 구문 검증: 통과
• 스키마 검증: 통과
• 제약조건 검증: 통과
• Claude AI 검증: 통과

📄 상세 보고서가 저장되었습니다: output/validation_report_sample_create_table.sql_20250730_192506.html
```

### ❌ 실패 사례
```
❌ 발견된 문제: 3개

📊 검증 결과:
• 세미콜론이 누락되었습니다.
• 스키마 검증: 컬럼 'invalid_column'이 존재하지 않습니다.
• 제약조건 검증: FOREIGN KEY 참조 테이블이 존재하지 않습니다.

📄 상세 보고서가 저장되었습니다: output/validation_report_test_bad_naming.sql_20250730_192540.html
```

### 생성되는 보고서

HTML 형식의 상세 보고서가 `output/` 디렉토리에 생성됩니다:
- 검증 상태 및 요약
- 데이터베이스 연결 정보
- 스키마 검증 결과 (DDL 유형별)
- 제약조건 검증 결과
- 발견된 문제 목록
- 원본 DDL 코드

## 📁 파일 구조

### 핵심 서버 파일
```
├── ddl_validation_qcli_mcp_server.py    # 메인 DDL 검증 MCP 서버
└── DDL_VALIDATION_README.md             # 이 파일
```

### SQL 테스트 파일들
```
├── sql/                                 # SQL 테스트 파일들
│   ├── sample_create_table.sql          # 샘플 테이블 생성
│   ├── sample_alter_table.sql           # 샘플 테이블 변경
│   ├── test_good_table.sql              # 올바른 테이블 생성
│   ├── test_good_alter.sql              # 올바른 테이블 변경
│   ├── demo_report.sql                  # 데모용 DDL
│   ├── test_bad_naming.sql              # 잘못된 네이밍 규칙
│   ├── test_syntax_error.sql            # 구문 오류 포함
│   ├── test_datatype_issues.sql         # 데이터 타입 문제
│   ├── test_create_existing_table.sql   # 기존 테이블 생성 시도
│   ├── test_alter_add_existing_column.sql # 기존 컬럼 추가 시도
│   ├── test_alter_add_new_column.sql    # 새 컬럼 추가
│   ├── test_alter_drop_nonexistent_column.sql # 존재하지 않는 컬럼 삭제
│   ├── test_alter_modify_incompatible_type.sql # 호환되지 않는 타입 변경
│   ├── test_alter_modify_reduce_length.sql # 컬럼 길이 축소
│   ├── test_create_index_existing.sql   # 기존 인덱스 생성 시도
│   ├── test_create_index_nonexistent_column.sql # 존재하지 않는 컬럼 인덱스
│   ├── test_drop_nonexistent_table.sql  # 존재하지 않는 테이블 삭제
│   ├── test_drop_nonexistent_index.sql  # 존재하지 않는 인덱스 삭제
│   └── ...
```

### 검증 보고서 출력
```
├── output/                              # 검증 보고서 출력
│   ├── validation_report_*.html         # HTML 형식 상세 보고서
│   ├── validation_report_*.md           # Markdown 형식 보고서
│   └── all_validation_report_*.html     # 전체 검증 종합 보고서
```

### 테스트 스크립트들
```
├── test_db_connection.py                # 데이터베이스 연결 테스트
├── test_db_connection_ssh.py            # SSH 터널 연결 테스트
├── test_schema_analysis.py              # 스키마 분석 테스트
├── test_detailed_schema.py              # 상세 스키마 검증 테스트
├── test_html_report.py                  # HTML 보고서 생성 테스트
└── test_mcp_connection.py               # MCP 서버 연결 테스트
```

### 지원 파일
```
├── ssh_tunnel.sh                        # SSH 터널 스크립트
├── ddl_validation_agent.py              # DDL 검증 에이전트
├── sql_validation_agent.py              # SQL 검증 에이전트
└── backup files/                        # 백업 파일들
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
export AWS_DEFAULT_REGION=us-east-1
```

#### 4. 패키지 의존성 오류
```bash
pip install --upgrade boto3 mysql-connector-python
```

### 로그 확인
서버 실행 시 로그를 확인하여 문제를 진단할 수 있습니다:
```bash
python ddl_validation_qcli_mcp_server.py
```

### 디렉토리 권한
출력 디렉토리에 대한 쓰기 권한이 있는지 확인:
```bash
chmod 755 output/
chmod 755 sql/
```

### 데이터베이스 연결 문제
1. AWS Secrets Manager에서 DB 연결 정보 확인
2. 네트워크 연결 상태 확인
3. SSH 터널 설정 확인 (필요한 경우)

## 📞 지원

문제가 발생하거나 기능 요청이 있으시면 이슈를 생성해 주세요.

## 📄 라이선스

이 프로젝트는 MIT 라이선스 하에 배포됩니다.