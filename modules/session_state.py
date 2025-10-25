"""
세션 상태 관리 모듈

MCP 서버의 세션 상태를 관리합니다.
"""

from typing import Dict, Any, Optional
from datetime import datetime
import logging

from .shared_types import SessionState

logger = logging.getLogger(__name__)


class SessionManager:
    """세션 관리 클래스"""

    def __init__(self):
        """초기화"""
        self.sessions: Dict[str, SessionState] = {}
        self.current_session_id: Optional[str] = None

    def create_session(self, session_id: str) -> SessionState:
        """
        세션 생성

        Args:
            session_id: 세션 ID

        Returns:
            SessionState 객체
        """
        logger.info(f"세션 생성: {session_id}")

        session = SessionState(session_id=session_id)
        self.sessions[session_id] = session
        self.current_session_id = session_id

        return session

    def get_session(self, session_id: str) -> Optional[SessionState]:
        """
        세션 조회

        Args:
            session_id: 세션 ID

        Returns:
            SessionState 객체 또는 None
        """
        return self.sessions.get(session_id)

    def update_session(self, session_id: str, **kwargs) -> bool:
        """
        세션 업데이트

        Args:
            session_id: 세션 ID
            **kwargs: 업데이트할 필드

        Returns:
            성공 여부
        """
        session = self.sessions.get(session_id)
        if not session:
            logger.warning(f"세션을 찾을 수 없음: {session_id}")
            return False

        # 업데이트 가능한 필드만 업데이트
        if "selected_database" in kwargs:
            session.selected_database = kwargs["selected_database"]

        if "current_plan" in kwargs:
            session.current_plan = kwargs["current_plan"]

        if "metadata" in kwargs:
            session.metadata.update(kwargs["metadata"])

        session.last_activity = datetime.now()
        logger.debug(f"세션 업데이트: {session_id}")

        return True

    def delete_session(self, session_id: str) -> bool:
        """
        세션 삭제

        Args:
            session_id: 세션 ID

        Returns:
            성공 여부
        """
        if session_id in self.sessions:
            del self.sessions[session_id]
            logger.info(f"세션 삭제: {session_id}")

            if self.current_session_id == session_id:
                self.current_session_id = None

            return True

        return False

    def get_current_session(self) -> Optional[SessionState]:
        """
        현재 세션 조회

        Returns:
            현재 SessionState 객체 또는 None
        """
        if self.current_session_id:
            return self.sessions.get(self.current_session_id)
        return None

    def set_current_session(self, session_id: str) -> bool:
        """
        현재 세션 설정

        Args:
            session_id: 세션 ID

        Returns:
            성공 여부
        """
        if session_id in self.sessions:
            self.current_session_id = session_id
            return True
        return False

    def get_selected_database(self) -> Optional[str]:
        """
        현재 세션의 선택된 데이터베이스 조회

        Returns:
            데이터베이스 이름 또는 None
        """
        session = self.get_current_session()
        return session.selected_database if session else None

    def set_selected_database(self, database: str) -> bool:
        """
        현재 세션의 데이터베이스 설정

        Args:
            database: 데이터베이스 이름

        Returns:
            성공 여부
        """
        if self.current_session_id:
            return self.update_session(
                self.current_session_id,
                selected_database=database
            )
        return False

    def cleanup_inactive_sessions(self, max_age_seconds: int = 3600) -> int:
        """
        비활성 세션 정리

        Args:
            max_age_seconds: 최대 비활성 시간 (초)

        Returns:
            삭제된 세션 수
        """
        now = datetime.now()
        to_delete = []

        for session_id, session in self.sessions.items():
            age = (now - session.last_activity).total_seconds()
            if age > max_age_seconds:
                to_delete.append(session_id)

        for session_id in to_delete:
            self.delete_session(session_id)

        if to_delete:
            logger.info(f"{len(to_delete)}개의 비활성 세션 삭제")

        return len(to_delete)
