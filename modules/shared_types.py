"""
공유 타입 정의 모듈

모든 모듈에서 사용하는 공통 데이터 타입과 클래스를 정의합니다.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime
from enum import Enum


# ============================================================================
# Enumerations
# ============================================================================

class ValidationStatus(Enum):
    """검증 상태"""
    SUCCESS = "success"
    WARNING = "warning"
    ERROR = "error"
    PENDING = "pending"


class SQLType(Enum):
    """SQL 타입"""
    CREATE_TABLE = "CREATE_TABLE"
    ALTER_TABLE = "ALTER_TABLE"
    DROP_TABLE = "DROP_TABLE"
    CREATE_INDEX = "CREATE_INDEX"
    DROP_INDEX = "DROP_INDEX"
    SELECT = "SELECT"
    INSERT = "INSERT"
    UPDATE = "UPDATE"
    DELETE = "DELETE"
    TRUNCATE = "TRUNCATE"
    UNKNOWN = "UNKNOWN"


class ConnectionType(Enum):
    """연결 타입"""
    DIRECT = "direct"
    SSH_TUNNEL = "ssh_tunnel"


# ============================================================================
# Database Connection Types
# ============================================================================

@dataclass
class DBCredentials:
    """데이터베이스 인증 정보"""
    host: str
    port: int
    username: str
    password: str
    database: Optional[str] = None


@dataclass
class SSHTunnelConfig:
    """SSH 터널 설정"""
    bastion_host: str
    bastion_user: str
    bastion_key_path: Optional[str] = None
    local_bind_port: int = 3306
    remote_bind_host: str = "localhost"
    remote_bind_port: int = 3306


@dataclass
class ConnectionConfig:
    """데이터베이스 연결 설정"""
    credentials: DBCredentials
    connection_type: ConnectionType = ConnectionType.DIRECT
    ssh_config: Optional[SSHTunnelConfig] = None
    region: str = "ap-northeast-2"
    secret_name: Optional[str] = None


# ============================================================================
# Validation Types
# ============================================================================

@dataclass
class ValidationResult:
    """검증 결과"""
    status: ValidationStatus
    message: str
    details: Dict[str, Any] = field(default_factory=dict)
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    timestamp: datetime = field(default_factory=datetime.now)


@dataclass
class TableInfo:
    """테이블 정보"""
    schema: Optional[str]
    table_name: str
    columns: List[Dict[str, Any]] = field(default_factory=list)
    indexes: List[Dict[str, Any]] = field(default_factory=list)
    constraints: Dict[str, List[Dict]] = field(default_factory=dict)
    row_count: Optional[int] = None


@dataclass
class SQLValidationResult:
    """SQL 검증 결과"""
    sql_type: SQLType
    validation_status: ValidationStatus
    syntax_valid: bool
    semantic_valid: bool
    performance_issues: List[str] = field(default_factory=list)
    recommendations: List[str] = field(default_factory=list)
    affected_tables: List[str] = field(default_factory=list)
    estimated_rows_scanned: Optional[int] = None
    execution_plan: Optional[Dict[str, Any]] = None


# ============================================================================
# Performance Monitoring Types
# ============================================================================

@dataclass
class MetricData:
    """메트릭 데이터"""
    metric_name: str
    value: float
    timestamp: datetime
    unit: str = ""
    dimension: Optional[str] = None


@dataclass
class PerformanceMetrics:
    """성능 메트릭"""
    instance_id: str
    metrics: List[MetricData] = field(default_factory=list)
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None


@dataclass
class SlowQuery:
    """슬로우 쿼리 정보"""
    query_text: str
    execution_time: float
    rows_examined: int
    rows_sent: int
    timestamp: datetime
    user: Optional[str] = None
    database: Optional[str] = None


# ============================================================================
# Report Generation Types
# ============================================================================

@dataclass
class ReportConfig:
    """보고서 설정"""
    title: str
    output_path: str
    format: str = "html"  # html, pdf, markdown
    include_charts: bool = True
    include_recommendations: bool = True
    timestamp: datetime = field(default_factory=datetime.now)


@dataclass
class ChartData:
    """차트 데이터"""
    chart_type: str  # line, bar, pie, scatter
    title: str
    x_label: str
    y_label: str
    data: Dict[str, List[Any]]
    options: Dict[str, Any] = field(default_factory=dict)


# ============================================================================
# Error Analysis Types
# ============================================================================

@dataclass
class ErrorLogEntry:
    """에러 로그 엔트리"""
    timestamp: datetime
    level: str  # ERROR, WARNING, INFO
    message: str
    source: Optional[str] = None
    error_code: Optional[str] = None
    stack_trace: Optional[str] = None


@dataclass
class ErrorPattern:
    """에러 패턴"""
    pattern: str
    occurrences: int
    first_seen: datetime
    last_seen: datetime
    examples: List[ErrorLogEntry] = field(default_factory=list)
    severity: str = "medium"  # low, medium, high, critical


@dataclass
class ErrorAnalysisResult:
    """에러 분석 결과"""
    total_errors: int
    unique_patterns: int
    patterns: List[ErrorPattern] = field(default_factory=list)
    recommendations: List[str] = field(default_factory=list)
    analysis_period: Tuple[datetime, datetime] = field(default_factory=lambda: (datetime.now(), datetime.now()))


# ============================================================================
# AI Integration Types
# ============================================================================

@dataclass
class KnowledgeBaseQuery:
    """Knowledge Base 쿼리"""
    query_text: str
    sql_type: Optional[SQLType] = None
    max_results: int = 3
    filters: Dict[str, Any] = field(default_factory=dict)


@dataclass
class KnowledgeBaseResult:
    """Knowledge Base 결과"""
    content: str
    relevance_score: float
    source: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class AIRecommendation:
    """AI 추천"""
    category: str  # performance, security, best_practice
    severity: str  # low, medium, high
    title: str
    description: str
    code_example: Optional[str] = None
    impact: Optional[str] = None


# ============================================================================
# Session State Types
# ============================================================================

@dataclass
class SessionState:
    """세션 상태"""
    session_id: str
    selected_database: Optional[str] = None
    current_plan: Optional[str] = None
    created_at: datetime = field(default_factory=datetime.now)
    last_activity: datetime = field(default_factory=datetime.now)
    metadata: Dict[str, Any] = field(default_factory=dict)
