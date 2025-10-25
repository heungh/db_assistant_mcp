"""
포맷팅 유틸리티 모듈

HTML, 텍스트 등의 포맷팅 유틸리티 함수들을 제공합니다.
"""

from typing import Dict, Any, List
from datetime import datetime


def format_bytes(bytes_value: int) -> str:
    """
    바이트 크기를 읽기 쉬운 형태로 변환

    Args:
        bytes_value: 바이트 크기

    Returns:
        포맷된 문자열 (예: "1.5 GB")

    Example:
        >>> format_bytes(1536)
        '1.5 KB'
        >>> format_bytes(1073741824)
        '1.0 GB'
    """
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if bytes_value < 1024.0:
            return f"{bytes_value:.1f} {unit}"
        bytes_value /= 1024.0
    return f"{bytes_value:.1f} PB"


def format_number(number: float, decimals: int = 2) -> str:
    """
    숫자를 천 단위 구분자와 함께 포맷

    Args:
        number: 숫자
        decimals: 소수점 자리수

    Returns:
        포맷된 문자열

    Example:
        >>> format_number(1234567.89)
        '1,234,567.89'
    """
    return f"{number:,.{decimals}f}"


def format_percentage(value: float, decimals: int = 2) -> str:
    """
    퍼센트 값을 포맷

    Args:
        value: 0-1 사이의 값 또는 0-100 사이의 값
        decimals: 소수점 자리수

    Returns:
        포맷된 퍼센트 문자열

    Example:
        >>> format_percentage(0.856)
        '85.60%'
        >>> format_percentage(85.6)
        '85.60%'
    """
    if value <= 1:
        value *= 100
    return f"{value:.{decimals}f}%"


def format_duration(seconds: float) -> str:
    """
    초 단위 시간을 읽기 쉬운 형태로 변환

    Args:
        seconds: 초 단위 시간

    Returns:
        포맷된 시간 문자열

    Example:
        >>> format_duration(3665)
        '1h 1m 5s'
        >>> format_duration(125)
        '2m 5s'
    """
    if seconds < 60:
        return f"{seconds:.1f}s"

    minutes = int(seconds // 60)
    remaining_seconds = int(seconds % 60)

    if minutes < 60:
        return f"{minutes}m {remaining_seconds}s"

    hours = int(minutes // 60)
    remaining_minutes = int(minutes % 60)

    return f"{hours}h {remaining_minutes}m {remaining_seconds}s"


def format_timestamp(timestamp: datetime, format_str: str = "%Y-%m-%d %H:%M:%S") -> str:
    """
    datetime 객체를 문자열로 포맷

    Args:
        timestamp: datetime 객체
        format_str: 포맷 문자열

    Returns:
        포맷된 날짜/시간 문자열

    Example:
        >>> dt = datetime(2025, 10, 20, 22, 30, 0)
        >>> format_timestamp(dt)
        '2025-10-20 22:30:00'
    """
    return timestamp.strftime(format_str)


def format_sql_for_display(sql: str, max_length: int = 100) -> str:
    """
    SQL 문을 표시용으로 포맷 (너무 길면 자르기)

    Args:
        sql: SQL 문자열
        max_length: 최대 길이

    Returns:
        포맷된 SQL 문자열

    Example:
        >>> format_sql_for_display("SELECT * FROM very_long_table_name WHERE condition = 1", 30)
        'SELECT * FROM very_long_tab...'
    """
    # 여러 줄을 한 줄로
    sql = " ".join(sql.split())

    if len(sql) <= max_length:
        return sql

    return sql[: max_length - 3] + "..."


def format_table_row_html(data: Dict[str, Any], header: bool = False) -> str:
    """
    딕셔너리 데이터를 HTML 테이블 행으로 변환

    Args:
        data: 행 데이터 딕셔너리
        header: 헤더 행 여부

    Returns:
        HTML 테이블 행 문자열

    Example:
        >>> format_table_row_html({"Name": "John", "Age": 30})
        '<tr><td>John</td><td>30</td></tr>'
    """
    tag = "th" if header else "td"
    cells = "".join([f"<{tag}>{value}</{tag}>" for value in data.values()])
    return f"<tr>{cells}</tr>"


def format_list_html(items: List[str], ordered: bool = False) -> str:
    """
    리스트를 HTML 목록으로 변환

    Args:
        items: 항목 리스트
        ordered: 순서 있는 목록 여부

    Returns:
        HTML 목록 문자열

    Example:
        >>> format_list_html(["Apple", "Banana", "Cherry"])
        '<ul><li>Apple</li><li>Banana</li><li>Cherry</li></ul>'
    """
    list_tag = "ol" if ordered else "ul"
    items_html = "".join([f"<li>{item}</li>" for item in items])
    return f"<{list_tag}>{items_html}</{list_tag}>"


def format_code_block(code: str, language: str = "sql") -> str:
    """
    코드를 HTML 코드 블록으로 변환

    Args:
        code: 코드 문자열
        language: 프로그래밍 언어

    Returns:
        HTML 코드 블록 문자열

    Example:
        >>> format_code_block("SELECT * FROM users", "sql")
        '<pre><code class="language-sql">SELECT * FROM users</code></pre>'
    """
    return f'<pre><code class="language-{language}">{code}</code></pre>'


def format_status_badge(status: str) -> str:
    """
    상태를 HTML 배지로 변환

    Args:
        status: 상태 문자열 (success, warning, error, info)

    Returns:
        HTML 배지 문자열

    Example:
        >>> format_status_badge("success")
        '<span class="badge badge-success">success</span>'
    """
    status_colors = {
        "success": "success",
        "warning": "warning",
        "error": "danger",
        "info": "info",
        "pending": "secondary",
    }

    color = status_colors.get(status.lower(), "secondary")
    return f'<span class="badge badge-{color}">{status}</span>'


def escape_html(text: str) -> str:
    """
    HTML 특수 문자를 이스케이프

    Args:
        text: 텍스트 문자열

    Returns:
        이스케이프된 문자열

    Example:
        >>> escape_html("<script>alert('xss')</script>")
        '&lt;script&gt;alert(&#x27;xss&#x27;)&lt;/script&gt;'
    """
    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&#x27;")
    )


def format_metric_value(value: Any, metric_name: str) -> str:
    """
    메트릭 값을 적절한 형식으로 포맷

    Args:
        value: 메트릭 값
        metric_name: 메트릭 이름

    Returns:
        포맷된 문자열

    Example:
        >>> format_metric_value(0.856, "CPUUtilization")
        '85.60%'
        >>> format_metric_value(1073741824, "FreeableMemory")
        '1.0 GB'
    """
    if "Percent" in metric_name or "Utilization" in metric_name or "Ratio" in metric_name:
        return format_percentage(value)
    elif "Memory" in metric_name or "Bytes" in metric_name:
        return format_bytes(int(value))
    elif "Latency" in metric_name or "Duration" in metric_name:
        return format_duration(value)
    else:
        return format_number(value)
