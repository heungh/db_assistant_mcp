"""
상수 정의 모듈

애플리케이션 전역에서 사용되는 상수 값들을 정의합니다.
"""

import os
from pathlib import Path

# ============================================================================
# 디렉토리 경로 상수
# ============================================================================

# 현재 디렉토리 기준 경로 설정
CURRENT_DIR = Path(__file__).parent.parent
OUTPUT_DIR = CURRENT_DIR / "output"
SQL_DIR = CURRENT_DIR / "sql"
DATA_DIR = CURRENT_DIR / "data"
LOGS_DIR = CURRENT_DIR / "logs"
BACKUP_DIR = CURRENT_DIR / "backup"

# 디렉토리 자동 생성
for directory in [OUTPUT_DIR, SQL_DIR, DATA_DIR, LOGS_DIR, BACKUP_DIR]:
    directory.mkdir(exist_ok=True)


# ============================================================================
# 성능 임계값 설정
# ============================================================================

PERFORMANCE_THRESHOLDS = {
    "max_rows_scan": 10_000_000,  # 1천만 행 이상 스캔 시 실패
    "table_scan_ratio": 0.1,  # 테이블의 10% 이상 스캔 시 경고
    "critical_rows_scan": 50_000_000,  # 5천만 행 이상 스캔 시 심각한 문제
}


# ============================================================================
# CloudWatch 메트릭 정의
# ============================================================================

DEFAULT_METRICS = [
    "CPUUtilization",
    "DatabaseConnections",
    "DBLoad",
    "DBLoadCPU",
    "DBLoadNonCPU",
    "FreeableMemory",
    "ReadIOPS",
    "WriteIOPS",
    "ReadLatency",
    "WriteLatency",
    "NetworkReceiveThroughput",
    "NetworkTransmitThroughput",
    "BufferCacheHitRatio",
]


# ============================================================================
# AWS 리전 설정
# ============================================================================

DEFAULT_REGION = os.getenv("AWS_DEFAULT_REGION", "ap-northeast-2")
BEDROCK_REGION = os.getenv("BEDROCK_REGION", "us-west-2")
KNOWLEDGE_BASE_REGION = os.getenv("KNOWLEDGE_BASE_REGION", "us-east-1")


# ============================================================================
# Knowledge Base 설정
# ============================================================================

KNOWLEDGE_BASE_ID = os.getenv("KNOWLEDGE_BASE_ID", "<your knowledgebase id>")
DATA_SOURCE_ID = os.getenv("DATA_SOURCE_ID", "<your data source id>")


# ============================================================================
# S3 버킷 설정
# ============================================================================

QUERY_RESULTS_BUCKET = os.getenv("QUERY_RESULTS_BUCKET", "db-assistant-query-results")
QUERY_RESULTS_DEV_BUCKET = os.getenv("QUERY_RESULTS_DEV_BUCKET", "db-assistant-query-results-dev")
BEDROCK_AGENT_BUCKET = os.getenv("BEDROCK_AGENT_BUCKET", "bedrockagent-hhs")


# ============================================================================
# SQL 파싱 관련 상수
# ============================================================================

# DDL 타입
DDL_TYPES = {
    "CREATE_TABLE": "CREATE TABLE",
    "ALTER_TABLE": "ALTER TABLE",
    "DROP_TABLE": "DROP TABLE",
    "CREATE_INDEX": "CREATE INDEX",
    "DROP_INDEX": "DROP INDEX",
    "TRUNCATE": "TRUNCATE",
}

# DML 타입
DML_TYPES = {
    "SELECT": "SELECT",
    "INSERT": "INSERT",
    "UPDATE": "UPDATE",
    "DELETE": "DELETE",
}


# ============================================================================
# 파일 확장자
# ============================================================================

SQL_EXTENSIONS = [".sql", ".ddl", ".dml"]
DATA_EXTENSIONS = [".csv", ".json", ".parquet"]
REPORT_EXTENSIONS = [".html", ".pdf", ".md"]


# ============================================================================
# 타임아웃 설정 (초)
# ============================================================================

DB_CONNECTION_TIMEOUT = 30
SSH_TUNNEL_TIMEOUT = 60
QUERY_EXECUTION_TIMEOUT = 300
API_REQUEST_TIMEOUT = 30


# ============================================================================
# 재시도 설정
# ============================================================================

MAX_RETRY_ATTEMPTS = 3
RETRY_DELAY_SECONDS = 2


# ============================================================================
# 로그 레벨
# ============================================================================

LOG_LEVELS = {
    "DEBUG": "DEBUG",
    "INFO": "INFO",
    "WARNING": "WARNING",
    "ERROR": "ERROR",
    "CRITICAL": "CRITICAL",
}


# ============================================================================
# 에러 메시지
# ============================================================================

ERROR_MESSAGES = {
    "DB_CONNECTION_FAILED": "데이터베이스 연결에 실패했습니다.",
    "SSH_TUNNEL_FAILED": "SSH 터널 설정에 실패했습니다.",
    "SQL_VALIDATION_FAILED": "SQL 검증에 실패했습니다.",
    "FILE_NOT_FOUND": "파일을 찾을 수 없습니다.",
    "INVALID_CREDENTIALS": "인증 정보가 올바르지 않습니다.",
}


# ============================================================================
# 성공 메시지
# ============================================================================

SUCCESS_MESSAGES = {
    "DB_CONNECTION_SUCCESS": "데이터베이스 연결에 성공했습니다.",
    "SSH_TUNNEL_SUCCESS": "SSH 터널이 성공적으로 설정되었습니다.",
    "SQL_VALIDATION_SUCCESS": "SQL 검증이 완료되었습니다.",
    "FILE_PROCESSED": "파일 처리가 완료되었습니다.",
}


# ============================================================================
# 버전 정보
# ============================================================================

VERSION = "2.0.0"
APP_NAME = "DB Assistant MCP Server"
