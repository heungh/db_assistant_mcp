"""
에러 로그 분석 모듈

데이터베이스 에러 로그를 분석하고 패턴을 추출합니다.
"""

import re
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime
from collections import defaultdict
import logging

from .shared_types import (
    ErrorLogEntry,
    ErrorPattern,
    ErrorAnalysisResult,
)
from .interfaces import ErrorAnalyzerInterface

logger = logging.getLogger(__name__)


class ErrorAnalyzer(ErrorAnalyzerInterface):
    """에러 로그 분석 클래스"""

    def __init__(self):
        """초기화"""
        self.error_patterns = []
        self.error_categories = {
            "connection": ["connection", "connect", "timeout", "refused"],
            "authentication": ["auth", "access denied", "permission", "privilege"],
            "syntax": ["syntax error", "sql syntax", "parse error"],
            "deadlock": ["deadlock", "lock wait timeout"],
            "resource": ["out of memory", "disk full", "too many connections"],
            "replication": ["replication", "slave", "binlog"],
        }

    def analyze_logs(
        self,
        log_content: str,
        start_time: Any = None,
        end_time: Any = None
    ) -> ErrorAnalysisResult:
        """
        로그 분석

        Args:
            log_content: 로그 내용
            start_time: 시작 시간
            end_time: 종료 시간

        Returns:
            ErrorAnalysisResult
        """
        logger.info("에러 로그 분석 시작")

        # 로그 엔트리 파싱
        log_entries = self._parse_log_entries(log_content)

        # 시간 필터링
        if start_time or end_time:
            log_entries = self._filter_by_time(log_entries, start_time, end_time)

        # 에러만 추출
        error_entries = [e for e in log_entries if e.level in ["ERROR", "CRITICAL"]]

        # 패턴 추출
        patterns = self.extract_patterns(error_entries)

        # 분석 결과 생성
        analysis_period = (
            error_entries[0].timestamp if error_entries else datetime.now(),
            error_entries[-1].timestamp if error_entries else datetime.now()
        )

        result = ErrorAnalysisResult(
            total_errors=len(error_entries),
            unique_patterns=len(patterns),
            patterns=patterns,
            recommendations=self._generate_recommendations(patterns),
            analysis_period=analysis_period
        )

        logger.info(f"에러 로그 분석 완료: {result.total_errors}개 에러, {result.unique_patterns}개 패턴")
        return result

    def extract_patterns(self, log_entries: List[ErrorLogEntry]) -> List[ErrorPattern]:
        """
        패턴 추출

        Args:
            log_entries: 로그 엔트리 리스트

        Returns:
            ErrorPattern 리스트
        """
        pattern_map = defaultdict(list)

        for entry in log_entries:
            # 메시지에서 패턴 추출 (변수 부분 제거)
            pattern = self._extract_pattern_from_message(entry.message)
            pattern_map[pattern].append(entry)

        # ErrorPattern 객체 생성
        patterns = []
        for pattern_str, entries in pattern_map.items():
            severity = self._determine_severity(pattern_str, len(entries))

            pattern = ErrorPattern(
                pattern=pattern_str,
                occurrences=len(entries),
                first_seen=min(e.timestamp for e in entries),
                last_seen=max(e.timestamp for e in entries),
                examples=entries[:3],  # 최대 3개 예시
                severity=severity
            )
            patterns.append(pattern)

        # 발생 빈도순으로 정렬
        patterns.sort(key=lambda p: p.occurrences, reverse=True)

        return patterns

    def categorize_errors(self, log_entries: List[ErrorLogEntry]) -> Dict[str, List[ErrorLogEntry]]:
        """
        에러 분류

        Args:
            log_entries: 로그 엔트리 리스트

        Returns:
            카테고리별 에러 딕셔너리
        """
        categorized = defaultdict(list)

        for entry in log_entries:
            category = self._categorize_error(entry.message)
            categorized[category].append(entry)

        return dict(categorized)

    def _parse_log_entries(self, log_content: str) -> List[ErrorLogEntry]:
        """로그 엔트리 파싱"""
        entries = []
        lines = log_content.split('\n')

        for line in lines:
            if not line.strip():
                continue

            entry = self._parse_log_line(line)
            if entry:
                entries.append(entry)

        return entries

    def _parse_log_line(self, line: str) -> Optional[ErrorLogEntry]:
        """개별 로그 라인 파싱"""
        # MySQL/Aurora 로그 포맷: 2025-10-20 23:00:00 [ERROR] Message
        pattern = r'(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})\s+\[(\w+)\]\s+(.+)'
        match = re.match(pattern, line)

        if match:
            timestamp_str, level, message = match.groups()
            try:
                timestamp = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")
                return ErrorLogEntry(
                    timestamp=timestamp,
                    level=level.upper(),
                    message=message.strip()
                )
            except ValueError:
                pass

        # 다른 포맷도 시도
        # 간단한 포맷: ERROR: Message
        simple_pattern = r'(\w+):\s+(.+)'
        match = re.match(simple_pattern, line)
        if match:
            level, message = match.groups()
            if level.upper() in ["ERROR", "WARNING", "INFO", "CRITICAL"]:
                return ErrorLogEntry(
                    timestamp=datetime.now(),
                    level=level.upper(),
                    message=message.strip()
                )

        return None

    def _filter_by_time(
        self,
        entries: List[ErrorLogEntry],
        start_time: Optional[datetime],
        end_time: Optional[datetime]
    ) -> List[ErrorLogEntry]:
        """시간 범위로 필터링"""
        filtered = entries

        if start_time:
            filtered = [e for e in filtered if e.timestamp >= start_time]

        if end_time:
            filtered = [e for e in filtered if e.timestamp <= end_time]

        return filtered

    def _extract_pattern_from_message(self, message: str) -> str:
        """
        메시지에서 패턴 추출 (변수 부분을 제거하고 패턴만 남김)

        예:
        "Table 'mydb.users' doesn't exist" -> "Table '<table>' doesn't exist"
        """
        # 숫자를 <number>로 치환
        pattern = re.sub(r'\b\d+\b', '<number>', message)

        # 큰따옴표나 작은따옴표로 둘러싸인 값을 <value>로 치환
        pattern = re.sub(r"'[^']*'", '<value>', pattern)
        pattern = re.sub(r'"[^"]*"', '<value>', pattern)

        # 테이블명 패턴 (db.table)
        pattern = re.sub(r'\b\w+\.\w+\b', '<table>', pattern)

        # IP 주소
        pattern = re.sub(r'\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b', '<ip>', pattern)

        # 경로
        pattern = re.sub(r'/[\w/.-]+', '<path>', pattern)

        return pattern

    def _categorize_error(self, message: str) -> str:
        """에러 메시지를 카테고리로 분류"""
        message_lower = message.lower()

        for category, keywords in self.error_categories.items():
            for keyword in keywords:
                if keyword in message_lower:
                    return category

        return "other"

    def _determine_severity(self, pattern: str, occurrences: int) -> str:
        """심각도 결정"""
        pattern_lower = pattern.lower()

        # 크리티컬 키워드
        critical_keywords = ["crash", "fatal", "corruption", "data loss"]
        if any(keyword in pattern_lower for keyword in critical_keywords):
            return "critical"

        # 높음 키워드
        high_keywords = ["deadlock", "out of memory", "replication"]
        if any(keyword in pattern_lower for keyword in high_keywords):
            return "high"

        # 발생 빈도 기반
        if occurrences > 100:
            return "high"
        elif occurrences > 10:
            return "medium"
        else:
            return "low"

    def _generate_recommendations(self, patterns: List[ErrorPattern]) -> List[str]:
        """패턴 기반 권장사항 생성"""
        recommendations = []

        for pattern in patterns[:5]:  # 상위 5개 패턴만
            pattern_lower = pattern.pattern.lower()

            if "connection" in pattern_lower or "timeout" in pattern_lower:
                recommendations.append(
                    f"연결 타임아웃 문제 ({pattern.occurrences}회): "
                    "connection_timeout 설정 확인 및 네트워크 상태 점검 필요"
                )
            elif "deadlock" in pattern_lower:
                recommendations.append(
                    f"데드락 발생 ({pattern.occurrences}회): "
                    "트랜잭션 로직 검토 및 인덱스 최적화 필요"
                )
            elif "memory" in pattern_lower:
                recommendations.append(
                    f"메모리 부족 ({pattern.occurrences}회): "
                    "innodb_buffer_pool_size 증설 검토 필요"
                )
            elif "replication" in pattern_lower:
                recommendations.append(
                    f"복제 문제 ({pattern.occurrences}회): "
                    "복제 지연 및 바이너리 로그 상태 확인 필요"
                )
            elif "syntax" in pattern_lower:
                recommendations.append(
                    f"SQL 구문 오류 ({pattern.occurrences}회): "
                    "애플리케이션 코드의 SQL 쿼리 검토 필요"
                )

        if not recommendations:
            recommendations.append("특별한 권장사항 없음. 일반적인 모니터링 유지 권장")

        return recommendations

    def split_log_content(self, log_lines: List[str], max_chars: int = 100000) -> List[str]:
        """
        로그 내용을 최대 문자수로 분할

        Args:
            log_lines: 로그 라인 리스트
            max_chars: 최대 문자수

        Returns:
            분할된 로그 내용 리스트
        """
        chunks = []
        current_chunk = []
        current_size = 0

        for line in log_lines:
            line_size = len(line)

            if current_size + line_size > max_chars and current_chunk:
                # 현재 청크 저장
                chunks.append('\n'.join(current_chunk))
                current_chunk = []
                current_size = 0

            current_chunk.append(line)
            current_size += line_size

        # 마지막 청크
        if current_chunk:
            chunks.append('\n'.join(current_chunk))

        return chunks
