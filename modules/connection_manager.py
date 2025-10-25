"""
데이터베이스 연결 관리 모듈

데이터베이스 연결과 SSH 터널을 관리합니다.
"""

from typing import Optional, Any, Dict
import subprocess
import logging

from .shared_types import ConnectionConfig, ConnectionType, SSHTunnelConfig

logger = logging.getLogger(__name__)


class ConnectionManager:
    """데이터베이스 연결 관리 클래스"""

    def __init__(self):
        """초기화"""
        self.connection: Optional[Any] = None
        self.cursor: Optional[Any] = None
        self.ssh_tunnel_process: Optional[subprocess.Popen] = None
        self.ssh_tunnel_port: Optional[int] = None
        self.connection_config: Optional[ConnectionConfig] = None
        self.is_connected: bool = False

    def set_connection(self, connection: Any, cursor: Any = None) -> None:
        """
        연결 설정

        Args:
            connection: 데이터베이스 연결 객체
            cursor: 커서 객체 (선택사항)
        """
        self.connection = connection
        self.cursor = cursor
        self.is_connected = True
        logger.info("데이터베이스 연결 설정 완료")

    def get_connection(self) -> Optional[Any]:
        """
        연결 조회

        Returns:
            데이터베이스 연결 객체 또는 None
        """
        return self.connection

    def get_cursor(self) -> Optional[Any]:
        """
        커서 조회

        Returns:
            커서 객체 또는 None
        """
        if self.cursor:
            return self.cursor

        if self.connection:
            try:
                self.cursor = self.connection.cursor()
                return self.cursor
            except Exception as e:
                logger.error(f"커서 생성 실패: {e}")
                return None

        return None

    def close_connection(self) -> bool:
        """
        연결 종료

        Returns:
            성공 여부
        """
        success = True

        try:
            if self.cursor:
                self.cursor.close()
                logger.debug("커서 종료")
                self.cursor = None

            if self.connection:
                self.connection.close()
                logger.info("데이터베이스 연결 종료")
                self.connection = None

            self.is_connected = False

        except Exception as e:
            logger.error(f"연결 종료 실패: {e}")
            success = False

        return success

    def set_ssh_tunnel(
        self,
        process: subprocess.Popen,
        local_port: int,
        config: SSHTunnelConfig
    ) -> None:
        """
        SSH 터널 설정

        Args:
            process: SSH 터널 프로세스
            local_port: 로컬 바인드 포트
            config: SSH 터널 설정
        """
        self.ssh_tunnel_process = process
        self.ssh_tunnel_port = local_port
        logger.info(f"SSH 터널 설정: localhost:{local_port}")

    def get_ssh_tunnel_port(self) -> Optional[int]:
        """
        SSH 터널 포트 조회

        Returns:
            로컬 바인드 포트 또는 None
        """
        return self.ssh_tunnel_port

    def is_ssh_tunnel_active(self) -> bool:
        """
        SSH 터널 활성 상태 확인

        Returns:
            활성 여부
        """
        if not self.ssh_tunnel_process:
            return False

        # 프로세스가 실행 중인지 확인
        return self.ssh_tunnel_process.poll() is None

    def cleanup_ssh_tunnel(self) -> bool:
        """
        SSH 터널 정리

        Returns:
            성공 여부
        """
        if not self.ssh_tunnel_process:
            return True

        try:
            if self.ssh_tunnel_process.poll() is None:
                # 프로세스가 실행 중이면 종료
                self.ssh_tunnel_process.terminate()
                try:
                    self.ssh_tunnel_process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    # 강제 종료
                    self.ssh_tunnel_process.kill()
                    self.ssh_tunnel_process.wait()

            logger.info("SSH 터널 정리 완료")
            self.ssh_tunnel_process = None
            self.ssh_tunnel_port = None
            return True

        except Exception as e:
            logger.error(f"SSH 터널 정리 실패: {e}")
            return False

    def cleanup_all(self) -> bool:
        """
        모든 리소스 정리

        Returns:
            성공 여부
        """
        success = True

        # 데이터베이스 연결 종료
        if not self.close_connection():
            success = False

        # SSH 터널 정리
        if not self.cleanup_ssh_tunnel():
            success = False

        if success:
            logger.info("모든 연결 리소스 정리 완료")
        else:
            logger.warning("일부 리소스 정리 실패")

        return success

    def get_connection_info(self) -> Dict[str, Any]:
        """
        연결 정보 조회

        Returns:
            연결 정보 딕셔너리
        """
        return {
            "is_connected": self.is_connected,
            "has_ssh_tunnel": self.ssh_tunnel_process is not None,
            "ssh_tunnel_active": self.is_ssh_tunnel_active(),
            "ssh_tunnel_port": self.ssh_tunnel_port,
            "has_cursor": self.cursor is not None,
        }

    def reconnect(self) -> bool:
        """
        재연결 시도

        Returns:
            성공 여부

        Note:
            실제 연결 로직은 database.py 모듈에서 처리
            이 메서드는 상태만 초기화
        """
        self.cleanup_all()
        return True
