"""
파싱 유틸리티 모듈

SQL, DDL, 데이터 타입 등을 파싱하는 유틸리티 함수들을 제공합니다.
"""

import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, Tuple, Optional, List


def parse_table_name(full_table_name: str) -> Tuple[Optional[str], str]:
    """
    테이블명에서 스키마와 테이블명을 분리

    Args:
        full_table_name: 전체 테이블명 (예: "schema.table" 또는 "table")

    Returns:
        (스키마명, 테이블명) 튜플. 스키마가 없으면 (None, 테이블명) 반환

    Examples:
        >>> parse_table_name("myschema.mytable")
        ('myschema', 'mytable')
        >>> parse_table_name("mytable")
        (None, 'mytable')
        >>> parse_table_name("`myschema`.`mytable`")
        ('myschema', 'mytable')
    """
    if "." in full_table_name:
        schema, table = full_table_name.split(".", 1)
        return schema.strip("`"), table.strip("`")
    return None, full_table_name.strip("`")


def format_file_link(file_path: str, display_name: str = None) -> str:
    """
    파일 경로를 HTML 링크로 변환

    Args:
        file_path: 파일 경로
        display_name: 표시할 이름 (없으면 파일명 사용)

    Returns:
        HTML 링크 문자열

    Example:
        >>> format_file_link("/path/to/file.sql")
        '<a href="file:///path/to/file.sql" class="file-link" target="_blank">file.sql</a>'
    """
    if not display_name:
        display_name = Path(file_path).name
    return f'<a href="file://{file_path}" class="file-link" target="_blank">{display_name}</a>'


def convert_kst_to_utc(kst_time_str: str) -> datetime:
    """
    KST 시간 문자열을 UTC datetime 객체로 변환

    Args:
        kst_time_str: KST 시간 문자열 (형식: "YYYY-MM-DD HH:MM:SS")

    Returns:
        UTC datetime 객체

    Raises:
        ValueError: 시간 형식이 올바르지 않을 경우

    Example:
        >>> convert_kst_to_utc("2025-10-20 22:00:00")
        datetime.datetime(2025, 10, 20, 13, 0, 0)
    """
    try:
        kst_dt = datetime.strptime(kst_time_str, "%Y-%m-%d %H:%M:%S")
        utc_dt = kst_dt - timedelta(hours=9)
        return utc_dt
    except ValueError as e:
        raise ValueError(
            f"시간 형식 오류. YYYY-MM-DD HH:MM:SS 형식으로 입력하세요: {e}"
        )


def convert_utc_to_local(utc_dt: datetime, region_name: str = "ap-northeast-2") -> datetime:
    """
    UTC datetime 객체를 지정된 AWS 리전의 로컬 시간으로 변환

    Args:
        utc_dt: UTC datetime 객체
        region_name: AWS 리전명 (기본값: ap-northeast-2)

    Returns:
        로컬 시간 datetime 객체

    Example:
        >>> utc_time = datetime(2025, 10, 20, 13, 0, 0)
        >>> convert_utc_to_local(utc_time, "ap-northeast-2")
        datetime.datetime(2025, 10, 20, 22, 0, 0)
    """
    # AWS 리전별 시간대 오프셋 (UTC 기준)
    region_timezone_offsets = {
        # 아시아-태평양
        "ap-northeast-1": 9,  # 도쿄 (JST)
        "ap-northeast-2": 9,  # 서울 (KST)
        "ap-northeast-3": 9,  # 오사카 (JST)
        "ap-south-1": 5.5,  # 뭄바이 (IST)
        "ap-southeast-1": 8,  # 싱가포르 (SGT)
        "ap-southeast-2": 10,  # 시드니 (AEST) - 표준시 기준
        "ap-east-1": 8,  # 홍콩 (HKT)
        # 유럽
        "eu-west-1": 0,  # 아일랜드 (GMT/UTC)
        "eu-west-2": 0,  # 런던 (GMT/UTC)
        "eu-west-3": 1,  # 파리 (CET)
        "eu-central-1": 1,  # 프랑크푸르트 (CET)
        "eu-north-1": 1,  # 스톡홀름 (CET)
        # 북미
        "us-east-1": -5,  # 버지니아 (EST)
        "us-east-2": -5,  # 오하이오 (EST)
        "us-west-1": -8,  # 캘리포니아 (PST)
        "us-west-2": -8,  # 오레곤 (PST)
        "ca-central-1": -5,  # 캐나다 중부 (EST)
        # 남미
        "sa-east-1": -3,  # 상파울루 (BRT)
        # 중동/아프리카
        "me-south-1": 3,  # 바레인 (AST)
        "af-south-1": 2,  # 케이프타운 (SAST)
    }

    offset_hours = region_timezone_offsets.get(region_name, 0)

    # 소수점이 있는 경우 (예: 인도 +5.5시간)
    if isinstance(offset_hours, float):
        hours = int(offset_hours)
        minutes = int((offset_hours - hours) * 60)
        return utc_dt + timedelta(hours=hours, minutes=minutes)
    else:
        return utc_dt + timedelta(hours=offset_hours)


def parse_data_type(data_type_str: str) -> Dict[str, Any]:
    """
    데이터 타입 문자열을 파싱하여 타입과 길이 정보 추출

    Args:
        data_type_str: 데이터 타입 문자열 (예: "VARCHAR(255)", "DECIMAL(10,2)")

    Returns:
        딕셔너리 형태의 파싱 결과
        - type: 기본 데이터 타입
        - length: 길이 (VARCHAR 등)
        - precision: 정밀도 (DECIMAL 등)
        - scale: 스케일 (DECIMAL 등)

    Examples:
        >>> parse_data_type("VARCHAR(255)")
        {'type': 'VARCHAR', 'length': 255, 'precision': None, 'scale': None}
        >>> parse_data_type("DECIMAL(10,2)")
        {'type': 'DECIMAL', 'length': None, 'precision': 10, 'scale': 2}
        >>> parse_data_type("INT")
        {'type': 'INT', 'length': None, 'precision': None, 'scale': None}
    """
    # VARCHAR(255), INT(11), DECIMAL(10,2) 등을 파싱
    type_match = re.match(r"(\w+)(?:\(([^)]+)\))?", data_type_str.upper())
    if not type_match:
        return {
            "type": data_type_str.upper(),
            "length": None,
            "precision": None,
            "scale": None,
        }

    base_type = type_match.group(1)
    params = type_match.group(2)

    result = {"type": base_type, "length": None, "precision": None, "scale": None}

    if params:
        if "," in params:
            # DECIMAL(10,2) 형태
            parts = [p.strip() for p in params.split(",")]
            result["precision"] = int(parts[0]) if parts[0].isdigit() else None
            result["scale"] = (
                int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else None
            )
        else:
            # VARCHAR(255), INT(11) 형태
            result["length"] = int(params) if params.isdigit() else None

    return result


def extract_sql_type(sql_content: str) -> str:
    """
    SQL 내용에서 SQL 타입 추출

    Args:
        sql_content: SQL 문자열

    Returns:
        SQL 타입 (CREATE_TABLE, ALTER_TABLE, SELECT 등)

    Example:
        >>> extract_sql_type("CREATE TABLE users (id INT)")
        'CREATE_TABLE'
        >>> extract_sql_type("SELECT * FROM users")
        'SELECT'
    """
    sql_upper = sql_content.strip().upper()

    # DDL 타입
    if sql_upper.startswith("CREATE TABLE"):
        return "CREATE_TABLE"
    elif sql_upper.startswith("ALTER TABLE"):
        return "ALTER_TABLE"
    elif sql_upper.startswith("DROP TABLE"):
        return "DROP_TABLE"
    elif sql_upper.startswith("CREATE INDEX"):
        return "CREATE_INDEX"
    elif sql_upper.startswith("DROP INDEX"):
        return "DROP_INDEX"
    elif sql_upper.startswith("TRUNCATE"):
        return "TRUNCATE"

    # DML 타입
    elif sql_upper.startswith("SELECT"):
        return "SELECT"
    elif sql_upper.startswith("INSERT"):
        return "INSERT"
    elif sql_upper.startswith("UPDATE"):
        return "UPDATE"
    elif sql_upper.startswith("DELETE"):
        return "DELETE"

    else:
        return "UNKNOWN"


def sanitize_sql(sql_content: str) -> str:
    """
    SQL 내용에서 주석과 불필요한 공백 제거

    Args:
        sql_content: SQL 문자열

    Returns:
        정리된 SQL 문자열

    Example:
        >>> sanitize_sql("SELECT * FROM users; -- comment")
        'SELECT * FROM users;'
    """
    # 한 줄 주석 제거 (-- 로 시작)
    sql_content = re.sub(r"--[^\n]*", "", sql_content)

    # 여러 줄 주석 제거 (/* ... */)
    sql_content = re.sub(r"/\*.*?\*/", "", sql_content, flags=re.DOTALL)

    # 여러 개의 공백을 하나로
    sql_content = re.sub(r"\s+", " ", sql_content)

    return sql_content.strip()


def is_valid_sql_identifier(identifier: str) -> bool:
    """
    SQL 식별자(테이블명, 컬럼명 등)가 유효한지 검사

    Args:
        identifier: SQL 식별자

    Returns:
        유효하면 True, 아니면 False

    Example:
        >>> is_valid_sql_identifier("my_table")
        True
        >>> is_valid_sql_identifier("123table")
        False
    """
    # SQL 식별자는 문자나 언더스코어로 시작하고, 문자, 숫자, 언더스코어로 구성
    pattern = r"^[a-zA-Z_][a-zA-Z0-9_]*$"
    return re.match(pattern, identifier) is not None


# ============================================================================
# SQL 테이블/인덱스 추출 함수들
# ============================================================================

def extract_table_name_from_alter(ddl_content: str) -> str:
    """
    ALTER TABLE 구문에서 테이블명 추출

    Args:
        ddl_content: DDL 내용

    Returns:
        테이블명 (찾지 못하면 None)

    Example:
        >>> extract_table_name_from_alter("ALTER TABLE users ADD COLUMN age INT")
        'users'
    """
    # 주석 제거
    sql_clean = re.sub(r"--.*$", "", ddl_content, flags=re.MULTILINE)
    sql_clean = re.sub(r"/\*.*?\*/", "", sql_clean, flags=re.DOTALL)

    # ALTER TABLE 패턴
    alter_pattern = r"ALTER\s+TABLE\s+`?([a-zA-Z_][a-zA-Z0-9_]*)`?\s+"
    match = re.search(alter_pattern, sql_clean, re.IGNORECASE)

    if match:
        return match.group(1)
    return None


def extract_created_tables(sql_content: str) -> List[str]:
    """
    현재 SQL에서 생성되는 테이블명 추출

    Args:
        sql_content: SQL 내용

    Returns:
        생성되는 테이블명 리스트

    Example:
        >>> extract_created_tables("CREATE TABLE users (id INT); CREATE TABLE posts (id INT)")
        ['users', 'posts']
    """
    tables = set()

    # 주석 제거
    sql_clean = re.sub(r"--.*$", "", sql_content, flags=re.MULTILINE)
    sql_clean = re.sub(r"/\*.*?\*/", "", sql_clean, flags=re.DOTALL)

    # CREATE TABLE 패턴 - 더 정확한 매칭
    create_pattern = r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?`?([a-zA-Z_][a-zA-Z0-9_]*)`?\s*\("
    create_matches = re.findall(create_pattern, sql_clean, re.IGNORECASE)

    # 유효한 테이블명만 필터링 (SQL 키워드 제외)
    sql_keywords = {
        "and",
        "or",
        "not",
        "in",
        "on",
        "as",
        "is",
        "if",
        "by",
        "to",
        "from",
        "where",
        "select",
        "insert",
        "update",
        "delete",
    }
    for table in create_matches:
        if table.lower() not in sql_keywords and len(table) > 1:
            tables.add(table)

    return list(tables)


def extract_created_indexes(sql_content: str) -> List[str]:
    """
    현재 SQL에서 생성되는 인덱스명 추출

    Args:
        sql_content: SQL 내용

    Returns:
        생성되는 인덱스명 리스트

    Example:
        >>> extract_created_indexes("CREATE INDEX idx_name ON users(name)")
        ['idx_name']
    """
    indexes = set()

    # 주석 제거
    sql_clean = re.sub(r"--.*$", "", sql_content, flags=re.MULTILINE)
    sql_clean = re.sub(r"/\*.*?\*/", "", sql_clean, flags=re.DOTALL)

    # CREATE INDEX 패턴
    index_pattern = (
        r"CREATE\s+(?:UNIQUE\s+)?INDEX\s+`?([a-zA-Z_][a-zA-Z0-9_]*)`?\s+ON"
    )
    index_matches = re.findall(index_pattern, sql_clean, re.IGNORECASE)
    indexes.update(index_matches)

    return list(indexes)


def extract_cte_tables(sql_content: str) -> List[str]:
    """
    WITH절의 CTE(Common Table Expression) 테이블명 추출

    Args:
        sql_content: SQL 내용

    Returns:
        CTE 테이블명 리스트

    Example:
        >>> extract_cte_tables("WITH temp AS (SELECT * FROM users) SELECT * FROM temp")
        ['temp']
    """
    cte_tables = set()

    # 주석 제거
    sql_clean = re.sub(r"--.*$", "", sql_content, flags=re.MULTILINE)
    sql_clean = re.sub(r"/\*.*?\*/", "", sql_clean, flags=re.DOTALL)

    # WITH RECURSIVE 패턴 (가장 일반적)
    recursive_with_pattern = (
        r"WITH\s+(?:RECURSIVE\s+)?([a-zA-Z_][a-zA-Z0-9_]*)\s+AS\s*\("
    )
    recursive_matches = re.findall(recursive_with_pattern, sql_clean, re.IGNORECASE)
    cte_tables.update(recursive_matches)

    # 추가 CTE 테이블들 (쉼표 후)
    additional_cte_pattern = r",\s*([a-zA-Z_][a-zA-Z0-9_]*)\s+AS\s*\("
    additional_matches = re.findall(
        additional_cte_pattern, sql_clean, re.IGNORECASE
    )
    cte_tables.update(additional_matches)

    return list(cte_tables)


def extract_foreign_keys(ddl_content: str) -> List[Dict[str, str]]:
    """
    DDL에서 외래키 정보 추출

    Args:
        ddl_content: DDL 내용

    Returns:
        외래키 정보 딕셔너리 리스트 (column, referenced_table, referenced_column)

    Example:
        >>> extract_foreign_keys("CREATE TABLE posts (user_id INT, FOREIGN KEY (user_id) REFERENCES users(id))")
        [{'column': 'user_id', 'referenced_table': 'users', 'referenced_column': 'id'}]
    """
    foreign_keys = []

    # 주석 제거
    ddl_clean = re.sub(r"--.*$", "", ddl_content, flags=re.MULTILINE)
    ddl_clean = re.sub(r"/\*.*?\*/", "", ddl_clean, flags=re.DOTALL)

    # FOREIGN KEY 패턴 매칭
    fk_pattern = r"FOREIGN\s+KEY\s*\(\s*([^)]+)\s*\)\s*REFERENCES\s+([^\s(]+)\s*\(\s*([^)]+)\s*\)"
    matches = re.finditer(fk_pattern, ddl_clean, re.IGNORECASE)

    for match in matches:
        column = match.group(1).strip().strip("`")
        ref_table = match.group(2).strip().strip("`")
        ref_column = match.group(3).strip().strip("`")

        foreign_keys.append(
            {
                "column": column,
                "referenced_table": ref_table,
                "referenced_column": ref_column,
            }
        )

    return foreign_keys


def extract_table_names(sql_content: str) -> List[str]:
    """
    SQL에서 테이블명 추출 (WITH절 CTE 테이블 제외)

    Args:
        sql_content: SQL 내용

    Returns:
        테이블명 리스트 (스키마 포함 가능)

    Example:
        >>> extract_table_names("SELECT * FROM users JOIN posts ON users.id = posts.user_id")
        ['users', 'posts']
    """
    tables = set()

    # 주석 제거
    sql_clean = re.sub(r"--.*$", "", sql_content, flags=re.MULTILINE)
    sql_clean = re.sub(r"/\*.*?\*/", "", sql_clean, flags=re.DOTALL)

    # WITH절의 CTE 테이블들 추출
    cte_tables = set(extract_cte_tables(sql_content))

    # MySQL 키워드들 (테이블명이 아닌 것들)
    mysql_keywords = {
        "CURRENT_TIMESTAMP",
        "NOW",
        "NULL",
        "TRUE",
        "FALSE",
        "DEFAULT",
        "AUTO_INCREMENT",
        "PRIMARY",
        "KEY",
        "UNIQUE",
        "INDEX",
        "FOREIGN",
        "REFERENCES",
        "ON",
        "DELETE",
        "UPDATE",
        "CASCADE",
        "SET",
        "RESTRICT",
        "NO",
        "ACTION",
        "CHECK",
        "CONSTRAINT",
        "ENUM",
        "VARCHAR",
        "INT",
        "DECIMAL",
        "DATETIME",
        "TIMESTAMP",
        "TEXT",
        "BOOLEAN",
        "TINYINT",
        "SMALLINT",
        "MEDIUMINT",
        "BIGINT",
        "FLOAT",
        "DOUBLE",
        "CHAR",
        "BINARY",
        "VARBINARY",
        "BLOB",
        "TINYBLOB",
        "MEDIUMBLOB",
        "LONGBLOB",
        "TINYTEXT",
        "MEDIUMTEXT",
        "LONGTEXT",
        "DATE",
        "TIME",
        "YEAR",
    }

    # CREATE TABLE 패턴 - 스키마 정보 포함 처리
    create_pattern = r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?`?(?:([a-zA-Z_][a-zA-Z0-9_]*)\.)??`?([a-zA-Z_][a-zA-Z0-9_]*)`?\s*\("
    create_matches = re.findall(create_pattern, sql_clean, re.IGNORECASE)
    for schema, table in create_matches:
        full_table_name = f"{schema}.{table}" if schema else table
        if table.upper() not in mysql_keywords:
            tables.add(full_table_name)

    # ALTER TABLE 패턴 - 스키마 정보 포함 처리
    alter_pattern = r"ALTER\s+TABLE\s+`?(?:([a-zA-Z_][a-zA-Z0-9_]*)\.)??`?([a-zA-Z_][a-zA-Z0-9_]*)`?\s+"
    alter_matches = re.findall(alter_pattern, sql_clean, re.IGNORECASE)
    for schema, table in alter_matches:
        full_table_name = f"{schema}.{table}" if schema else table
        if table.upper() not in mysql_keywords:
            tables.add(full_table_name)

    # DROP TABLE 패턴 - 스키마 정보 포함 처리
    drop_pattern = r"DROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?`?(?:([a-zA-Z_][a-zA-Z0-9_]*)\.)??`?([a-zA-Z_][a-zA-Z0-9_]*)`?"
    drop_matches = re.findall(drop_pattern, sql_clean, re.IGNORECASE)
    for schema, table in drop_matches:
        full_table_name = f"{schema}.{table}" if schema else table
        if table.upper() not in mysql_keywords:
            tables.add(full_table_name)

    # FROM 패턴 (SELECT, DELETE) - 스키마 정보 포함 처리
    from_pattern = r"\bFROM\s+`?(?:([a-zA-Z_][a-zA-Z0-9_]*)\.)??`?([a-zA-Z_][a-zA-Z0-9_]*)`?(?:\s+(?:AS\s+)?[a-zA-Z_][a-zA-Z0-9_]*)?(?:\s|$|,|;|\)|WHERE|ORDER|GROUP|LIMIT|JOIN|INNER|LEFT|RIGHT|FULL|CROSS)"
    from_matches = re.findall(from_pattern, sql_clean, re.IGNORECASE)
    for schema, table in from_matches:
        full_table_name = f"{schema}.{table}" if schema else table
        if table not in cte_tables and table.upper() not in mysql_keywords:
            tables.add(full_table_name)

    # JOIN 패턴 - 스키마 정보 포함 처리
    join_pattern = r"\b(?:INNER\s+|LEFT\s+|RIGHT\s+|FULL\s+|CROSS\s+)?JOIN\s+`?(?:([a-zA-Z_][a-zA-Z0-9_]*)\.)??`?([a-zA-Z_][a-zA-Z0-9_]*)`?(?:\s+(?:AS\s+)?[a-zA-Z_][a-zA-Z0-9_]*)?(?:\s|$|,|;|\)|ON)"
    join_matches = re.findall(join_pattern, sql_clean, re.IGNORECASE)
    for schema, table in join_matches:
        full_table_name = f"{schema}.{table}" if schema else table
        if table not in cte_tables and table.upper() not in mysql_keywords:
            tables.add(full_table_name)

    # UPDATE 패턴 - 스키마 정보 포함 처리
    update_pattern = r"\bUPDATE\s+`?(?:([a-zA-Z_][a-zA-Z0-9_]*)\.)??`?([a-zA-Z_][a-zA-Z0-9_]*)`?(?:\s|$|,|;|\)|SET)"
    update_matches = re.findall(update_pattern, sql_clean, re.IGNORECASE)
    for schema, table in update_matches:
        full_table_name = f"{schema}.{table}" if schema else table
        if table not in cte_tables and table.upper() not in mysql_keywords:
            tables.add(full_table_name)

    # INSERT INTO 패턴 - 스키마 정보 포함 처리
    insert_pattern = r"\bINSERT\s+INTO\s+`?(?:([a-zA-Z_][a-zA-Z0-9_]*)\.)??`?([a-zA-Z_][a-zA-Z0-9_]*)`?(?:\s|$|,|;|\)|\()"
    insert_matches = re.findall(insert_pattern, sql_clean, re.IGNORECASE)
    for schema, table in insert_matches:
        full_table_name = f"{schema}.{table}" if schema else table
        if table not in cte_tables and table.upper() not in mysql_keywords:
            tables.add(full_table_name)

    return list(tables)


# ============================================================================
# SQL 타입 감지 함수들
# ============================================================================

def detect_ddl_type(ddl_content: str) -> str:
    """
    DDL 타입 감지 (단일 구문)

    Args:
        ddl_content: DDL 내용

    Returns:
        SQL 타입 (CREATE_TABLE, ALTER_TABLE, SELECT 등)

    Example:
        >>> detect_ddl_type("CREATE TABLE users (id INT)")
        'CREATE_TABLE'
    """
    ddl_upper = ddl_content.upper().strip()

    if ddl_upper.startswith("CREATE TABLE"):
        return "CREATE_TABLE"
    elif ddl_upper.startswith("ALTER TABLE"):
        return "ALTER_TABLE"
    elif ddl_upper.startswith("DROP TABLE"):
        return "DROP_TABLE"
    elif ddl_upper.startswith("CREATE INDEX"):
        return "CREATE_INDEX"
    elif ddl_upper.startswith("DROP INDEX"):
        return "DROP_INDEX"
    elif ddl_upper.startswith("INSERT"):
        return "INSERT"
    elif ddl_upper.startswith("UPDATE"):
        return "UPDATE"
    elif ddl_upper.startswith("DELETE"):
        return "DELETE"
    elif ddl_upper.startswith("SELECT"):
        return "SELECT"
    else:
        return "UNKNOWN"


def extract_ddl_type(ddl_content: str, debug_log=None) -> str:
    """
    혼합 SQL 파일 타입 추출 - SELECT 쿼리가 많으면 MIXED_SELECT로 분류

    Args:
        ddl_content: DDL 내용
        debug_log: 디버그 로그 함수 (선택사항)

    Returns:
        SQL 타입 (CREATE_TABLE, MIXED_SELECT 등)

    Example:
        >>> extract_ddl_type("CREATE TABLE users (id INT); SELECT * FROM users;")
        'CREATE_TABLE'
    """
    # 주석과 빈 줄을 제거하고 실제 구문만 추출
    # 먼저 /* */ 스타일 주석을 전체적으로 제거
    ddl_content = re.sub(r"/\*.*?\*/", "", ddl_content, flags=re.DOTALL)

    lines = ddl_content.strip().split("\n")
    ddl_lines = []

    for line in lines:
        line = line.strip()
        # 주석 라인이나 빈 라인 건너뛰기
        if line and not line.startswith("--") and not line.startswith("#"):
            ddl_lines.append(line)

    if not ddl_lines:
        return "UNKNOWN"

    # 전체 내용을 분석하여 구문 타입별 개수 계산
    full_content = " ".join(ddl_lines).upper()

    # 개별 구문들을 분석
    statements = []
    for line in ddl_lines:
        line_upper = line.upper().strip()
        if line_upper and not line_upper.startswith("/*"):
            statements.append(line_upper)

    # 구문 타입별 개수 계산
    type_counts = {
        "SELECT": 0,
        "INSERT": 0,
        "UPDATE": 0,
        "DELETE": 0,
        "CREATE_TABLE": 0,
        "ALTER_TABLE": 0,
        "CREATE_INDEX": 0,
        "DROP_TABLE": 0,
        "DROP_INDEX": 0,
        "RENAME": 0,
    }

    # 각 구문 분석 - 세미콜론으로 분리된 실제 구문 단위로 계산
    sql_statements = [
        stmt.strip() for stmt in ddl_content.split(";") if stmt.strip()
    ]

    for stmt in sql_statements:
        stmt_upper = stmt.upper().strip()

        # /* */ 스타일 주석 제거
        stmt_upper = re.sub(r"/\*.*?\*/", "", stmt_upper, flags=re.DOTALL)

        # -- 스타일 주석 제거
        stmt_lines = [
            line.strip()
            for line in stmt_upper.split("\n")
            if line.strip() and not line.strip().startswith("--")
        ]
        if not stmt_lines:
            continue

        stmt_clean = " ".join(stmt_lines).strip()

        if stmt_clean.startswith("SELECT"):
            type_counts["SELECT"] += 1
        elif stmt_clean.startswith("INSERT"):
            type_counts["INSERT"] += 1
        elif stmt_clean.startswith("UPDATE"):
            type_counts["UPDATE"] += 1
        elif stmt_clean.startswith("DELETE"):
            type_counts["DELETE"] += 1
        elif stmt_clean.startswith("CREATE TABLE"):
            type_counts["CREATE_TABLE"] += 1
        elif stmt_clean.startswith("ALTER TABLE") or re.search(
            r"\bALTER\s+TABLE\b", stmt_clean
        ):
            type_counts["ALTER_TABLE"] += 1
        elif stmt_clean.startswith("CREATE INDEX"):
            type_counts["CREATE_INDEX"] += 1
        elif stmt_clean.startswith("DROP TABLE"):
            type_counts["DROP_TABLE"] += 1
        elif stmt_clean.startswith("DROP INDEX"):
            type_counts["DROP_INDEX"] += 1
        elif stmt_clean.startswith("RENAME TABLE") or re.search(
            r"\bRENAME\s+TABLE\b", stmt_clean
        ):
            type_counts["RENAME"] += 1

    # 총 구문 수
    total_statements = sum(type_counts.values())

    # 디버그 로그 추가
    if debug_log:
        debug_log(f"DEBUG - 구문 개수: {type_counts}")
        debug_log(f"DEBUG - 총 구문: {total_statements}")

    # SELECT 쿼리가 50% 이상이면 MIXED_SELECT로 분류
    if (
        type_counts["SELECT"] > 0
        and type_counts["SELECT"] >= total_statements * 0.5
    ):
        if debug_log:
            debug_log("DEBUG - MIXED_SELECT로 분류됨")
        return "MIXED_SELECT"

    # 기존 우선순위 로직 (DDL 우선)
    if type_counts["CREATE_TABLE"] > 0:
        return "CREATE_TABLE"
    elif type_counts["ALTER_TABLE"] > 0 or type_counts["RENAME"] > 0:
        return "ALTER_TABLE"
    elif type_counts["CREATE_INDEX"] > 0:
        return "CREATE_INDEX"
    elif type_counts["DROP_TABLE"] > 0:
        return "DROP_TABLE"
    elif type_counts["DROP_INDEX"] > 0:
        return "DROP_INDEX"
    elif type_counts["SELECT"] > 0:
        return "SELECT"
    elif type_counts["INSERT"] > 0:
        return "INSERT"
    elif type_counts["UPDATE"] > 0:
        return "UPDATE"
    elif type_counts["DELETE"] > 0:
        return "DELETE"

    # 기타 구문 처리
    if any(stmt.startswith("SHOW ") for stmt in statements):
        return "SHOW"
    elif any(stmt.startswith("SET ") for stmt in statements):
        return "SET"
    elif any(stmt.startswith("USE ") for stmt in statements):
        return "USE"
    else:
        return "UNKNOWN"


# ============================================================================
# SQL 검증 함수들
# ============================================================================

def validate_semicolon_usage(ddl_content: str) -> bool:
    """
    개선된 세미콜론 검증 - 독립적인 문장은 세미콜론 없어도 허용

    Args:
        ddl_content: DDL 내용

    Returns:
        유효하면 True, 아니면 False

    Example:
        >>> validate_semicolon_usage("CREATE TABLE users (id INT);")
        True
        >>> validate_semicolon_usage("CREATE TABLE users (id INT)")
        True
        >>> validate_semicolon_usage("CREATE TABLE users (id INT); CREATE TABLE posts (id INT)")
        False
    """
    content = ddl_content.strip()

    # 빈 내용은 통과
    if not content:
        return True

    # 주석 제거하고 실제 SQL 구문만 추출
    lines = content.split("\n")
    sql_lines = []
    for line in lines:
        line = line.strip()
        if line and not line.startswith("--") and not line.startswith("/*"):
            sql_lines.append(line)

    if not sql_lines:
        return True

    # 실제 SQL 구문 결합
    actual_sql = " ".join(sql_lines).strip()

    # 여러 문장이 있는 경우 (세미콜론으로 구분)
    statements = [stmt.strip() for stmt in actual_sql.split(";") if stmt.strip()]

    # 마지막 문장이 독립적인 단일 구문인지 확인
    if len(statements) == 1:
        # 단일 구문인 경우 세미콜론 없어도 허용
        single_stmt = statements[0].upper().strip()

        # SET, USE, SHOW 등 독립적인 구문들은 세미콜론 없어도 허용
        independent_keywords = [
            "SET SESSION",
            "SET GLOBAL",
            "SET @",
            "SET @@",
            "USE ",
            "SHOW ",
            "DESCRIBE ",
            "DESC ",
            "EXPLAIN ",
            "SELECT ",
            "INSERT ",
            "UPDATE ",
            "DELETE ",
            "CREATE TABLE",
            "CREATE INDEX",
            "ALTER TABLE",
            "DROP TABLE",
        ]

        for keyword in independent_keywords:
            if single_stmt.startswith(keyword):
                return True

    # 여러 문장이 있는 경우 마지막을 제외하고는 모두 세미콜론이 있어야 함
    return content.endswith(";")
