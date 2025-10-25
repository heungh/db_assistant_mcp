"""
인터페이스 및 프로토콜 정의 모듈

각 모듈이 구현해야 하는 인터페이스와 프로토콜을 정의합니다.
"""

from abc import ABC, abstractmethod
from typing import Protocol, Dict, List, Any, Optional
from pathlib import Path

from .shared_types import (
    ConnectionConfig,
    ValidationResult,
    SQLValidationResult,
    PerformanceMetrics,
    ReportConfig,
    ErrorAnalysisResult,
    KnowledgeBaseQuery,
    KnowledgeBaseResult,
    AIRecommendation,
)


# ============================================================================
# Database Connection Interface
# ============================================================================

class DatabaseConnectionInterface(Protocol):
    """데이터베이스 연결 인터페이스"""

    def connect(self, config: ConnectionConfig) -> bool:
        """데이터베이스 연결"""
        ...

    def disconnect(self) -> bool:
        """데이터베이스 연결 해제"""
        ...

    def execute_query(self, query: str) -> List[Dict[str, Any]]:
        """쿼리 실행"""
        ...

    def get_connection_status(self) -> Dict[str, Any]:
        """연결 상태 조회"""
        ...


# ============================================================================
# SQL Validator Interface
# ============================================================================

class SQLValidatorInterface(ABC):
    """SQL 검증 인터페이스"""

    @abstractmethod
    def validate_sql(self, sql_content: str, cursor: Any) -> SQLValidationResult:
        """SQL 검증"""
        pass

    @abstractmethod
    def validate_syntax(self, sql_content: str) -> ValidationResult:
        """SQL 구문 검증"""
        pass

    @abstractmethod
    def validate_semantics(self, sql_content: str, cursor: Any) -> ValidationResult:
        """SQL 의미 검증"""
        pass

    @abstractmethod
    def check_performance(self, sql_content: str, cursor: Any) -> List[str]:
        """성능 이슈 검사"""
        pass


# ============================================================================
# Performance Monitor Interface
# ============================================================================

class PerformanceMonitorInterface(Protocol):
    """성능 모니터링 인터페이스"""

    def collect_metrics(
        self,
        instance_id: str,
        start_time: Any,
        end_time: Any
    ) -> PerformanceMetrics:
        """메트릭 수집"""
        ...

    def analyze_metrics(self, metrics: PerformanceMetrics) -> Dict[str, Any]:
        """메트릭 분석"""
        ...

    def detect_anomalies(self, metrics: PerformanceMetrics) -> List[Dict[str, Any]]:
        """이상 탐지"""
        ...


# ============================================================================
# Report Generator Interface
# ============================================================================

class ReportGeneratorInterface(ABC):
    """보고서 생성 인터페이스"""

    @abstractmethod
    def generate_report(
        self,
        config: ReportConfig,
        data: Dict[str, Any]
    ) -> Path:
        """보고서 생성"""
        pass

    @abstractmethod
    def generate_html(self, data: Dict[str, Any]) -> str:
        """HTML 보고서 생성"""
        pass

    @abstractmethod
    def create_charts(self, data: Dict[str, Any]) -> List[str]:
        """차트 생성"""
        pass


# ============================================================================
# Error Analyzer Interface
# ============================================================================

class ErrorAnalyzerInterface(ABC):
    """에러 분석 인터페이스"""

    @abstractmethod
    def analyze_logs(
        self,
        log_content: str,
        start_time: Any = None,
        end_time: Any = None
    ) -> ErrorAnalysisResult:
        """로그 분석"""
        pass

    @abstractmethod
    def extract_patterns(self, log_entries: List[Any]) -> List[Any]:
        """패턴 추출"""
        pass

    @abstractmethod
    def categorize_errors(self, log_entries: List[Any]) -> Dict[str, List[Any]]:
        """에러 분류"""
        pass


# ============================================================================
# AI Integration Interface
# ============================================================================

class AIServiceInterface(Protocol):
    """AI 서비스 인터페이스"""

    def query_knowledge_base(self, query: KnowledgeBaseQuery) -> List[KnowledgeBaseResult]:
        """Knowledge Base 조회"""
        ...

    def generate_sql(self, natural_language: str, context: Dict[str, Any]) -> str:
        """자연어를 SQL로 변환"""
        ...

    def get_recommendations(self, sql_content: str, context: Dict[str, Any]) -> List[AIRecommendation]:
        """AI 추천 조회"""
        ...


# ============================================================================
# Session Manager Interface
# ============================================================================

class SessionManagerInterface(Protocol):
    """세션 관리 인터페이스"""

    def create_session(self, session_id: str) -> Dict[str, Any]:
        """세션 생성"""
        ...

    def get_session(self, session_id: str) -> Optional[Dict[str, Any]]:
        """세션 조회"""
        ...

    def update_session(self, session_id: str, data: Dict[str, Any]) -> bool:
        """세션 업데이트"""
        ...

    def delete_session(self, session_id: str) -> bool:
        """세션 삭제"""
        ...


# ============================================================================
# Logger Interface
# ============================================================================

class LoggerInterface(Protocol):
    """로거 인터페이스"""

    def info(self, message: str) -> None:
        """정보 로그"""
        ...

    def warning(self, message: str) -> None:
        """경고 로그"""
        ...

    def error(self, message: str) -> None:
        """에러 로그"""
        ...

    def debug(self, message: str) -> None:
        """디버그 로그"""
        ...
