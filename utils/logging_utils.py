"""
로깅 유틸리티 모듈

작업별 세션 로그 및 디버그 로그 기능을 제공합니다.
"""

import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Tuple, Callable

# 로깅 설정 - MCP 서버에서는 반드시 stderr로 로그를 출력해야 함
# stdout은 MCP 프로토콜(JSON-RPC)용으로 예약되어 있음
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stderr)],  # stderr로 변경
)

# 로거 인스턴스
logger = logging.getLogger(__name__)


def create_session_log(operation_name: str = "operation", logs_dir: Path = None) -> Tuple[Callable, str]:
    """
    작업별 세션 로그 파일 생성 및 로그 함수 반환

    Args:
        operation_name: 작업 이름
        logs_dir: 로그 디렉토리 경로 (기본값: logs/)

    Returns:
        (log_message 함수, 로그 파일 경로) 튜플

    Example:
        >>> log_func, log_path = create_session_log("validation")
        >>> log_func("INFO", "Starting validation")
    """
    if logs_dir is None:
        logs_dir = Path("logs")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = f"ddl_validation_{operation_name}_{timestamp}.log"
    log_path = logs_dir / log_filename

    # logs 디렉토리 생성
    log_path.parent.mkdir(exist_ok=True)

    def log_message(level: str, message: str):
        """
        세션 로그 파일에 메시지 작성

        Args:
            level: 로그 레벨 (INFO, WARNING, ERROR 등)
            message: 로그 메시지
        """
        timestamp_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_entry = f"{timestamp_str} - {operation_name} - {level} - {message}\n"

        with open(log_path, "a", encoding="utf-8") as f:
            f.write(log_entry)

        # 콘솔에도 출력 (MCP 서버에서는 stderr로 출력해야 함)
        print(f"[{level}] {message}", file=sys.stderr)

    # 초기 로그 작성
    log_message("INFO", f"새 작업 세션 시작: {operation_name} - 로그 파일: {log_path}")

    return log_message, str(log_path)


def debug_log(message: str, logs_dir: Path = None):
    """
    전역 디버그 로그 함수

    Args:
        message: 디버그 메시지
        logs_dir: 로그 디렉토리 경로 (기본값: logs/)

    Note:
        로그 실패 시 예외를 발생시키지 않고 무시합니다.
    """
    if logs_dir is None:
        logs_dir = Path("logs")

    debug_log_path = logs_dir / f"debug_{datetime.now().strftime('%Y%m%d')}.log"
    try:
        # 디렉토리가 없으면 생성
        debug_log_path.parent.mkdir(exist_ok=True)

        with open(debug_log_path, "a", encoding="utf-8") as f:
            f.write(f"{datetime.now().strftime('%H:%M:%S')} - {message}\n")
    except Exception:
        pass  # 로그 실패 시 무시


def get_logger(name: str = __name__) -> logging.Logger:
    """
    로거 인스턴스 반환

    Args:
        name: 로거 이름

    Returns:
        logging.Logger 인스턴스
    """
    return logging.getLogger(name)
