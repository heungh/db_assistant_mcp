"""
SQL Parser Module

SQL 파싱 및 타입 추출 전용 모듈
"""

import re
import logging
from typing import List, Dict, Any

logger = logging.getLogger(__name__)


class SQLParser:
    """SQL 파싱 클래스"""

    def __init__(self):
        """SQLParser 초기화"""
        logger.info("SQLParser 초기화 완료")

    def parse_ddl_statements(self, sql_content: str) -> List[Dict[str, Any]]:
        """DDL 구문을 파싱하여 개별 구문으로 분리"""
        statements = []

        # CREATE TABLE 파싱
        create_table_pattern = (
            r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?`?(\w+)`?\s*\("
        )
        for match in re.finditer(create_table_pattern, sql_content, re.IGNORECASE):
            statements.append({"type": "CREATE_TABLE", "table": match.group(1)})

        # CREATE INDEX 파싱
        create_index_pattern = r"CREATE\s+(?:UNIQUE\s+)?INDEX\s+`?(\w+)`?\s+ON\s+`?(\w+)`?\s*\(\s*`?(\w+)`?"
        for match in re.finditer(create_index_pattern, sql_content, re.IGNORECASE):
            statements.append(
                {
                    "type": "CREATE_INDEX",
                    "index_name": match.group(1),
                    "table": match.group(2),
                    "columns": match.group(3),
                }
            )

        return statements

    def extract_ddl_type(self, ddl_content: str, debug_log=None) -> str:
        """혼합 SQL 파일 타입 추출 - SELECT 쿼리가 많으면 MIXED_SELECT로 분류"""
        import re

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

        # DDL and DML count calculation
        ddl_count = (
            type_counts["CREATE_TABLE"]
            + type_counts["ALTER_TABLE"]
            + type_counts["CREATE_INDEX"]
            + type_counts["DROP_TABLE"]
            + type_counts["DROP_INDEX"]
            + type_counts["RENAME"]
        )

        dml_count = (
            type_counts["SELECT"]
            + type_counts["INSERT"]
            + type_counts["UPDATE"]
            + type_counts["DELETE"]
        )

        # Return MIXED_SELECT if both DDL and DML are present
        if ddl_count > 0 and dml_count > 0:
            if debug_log:
                debug_log(f"DEBUG - DDL({ddl_count}) and DML({dml_count}) mixed, MIXED_SELECT")
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

    def parse_ddl_detailed(self, ddl_content: str) -> List[Dict[str, Any]]:
        """DDL 구문을 상세하게 파싱하여 구문 유형별 정보 추출"""
        ddl_statements = []

        # CREATE TABLE 파싱
        create_table_pattern = r'CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?`?(\w+)`?\s*\((.*?)\)(?:\s*ENGINE\s*=\s*\w+)?(?:\s*COMMENT\s*=\s*[\'"][^\'"]*[\'"])?'
        create_matches = re.findall(
            create_table_pattern, ddl_content, re.DOTALL | re.IGNORECASE
        )

        for table_name, columns_def in create_matches:
            columns_info = self.parse_create_table_columns(columns_def)
            ddl_statements.append(
                {
                    "type": "CREATE_TABLE",
                    "table": table_name.lower(),
                    "columns": columns_info["columns"],
                    "constraints": columns_info["constraints"],
                }
            )

        # ALTER TABLE 파싱
        alter_patterns = [
            # ADD COLUMN
            (
                r"ALTER\s+TABLE\s+`?(\w+)`?\s+ADD\s+(?:COLUMN\s+)?`?(\w+)`?\s+([^,;]+)",
                "ADD_COLUMN",
            ),
            # DROP COLUMN
            (
                r"ALTER\s+TABLE\s+`?(\w+)`?\s+DROP\s+(?:COLUMN\s+)?`?(\w+)`?",
                "DROP_COLUMN",
            ),
            # MODIFY COLUMN
            (
                r"ALTER\s+TABLE\s+`?(\w+)`?\s+MODIFY\s+(?:COLUMN\s+)?`?(\w+)`?\s+([^,;]+)",
                "MODIFY_COLUMN",
            ),
            # CHANGE COLUMN
            (
                r"ALTER\s+TABLE\s+`?(\w+)`?\s+CHANGE\s+(?:COLUMN\s+)?`?(\w+)`?\s+`?(\w+)`?\s+([^,;]+)",
                "CHANGE_COLUMN",
            ),
        ]

        for pattern, alter_type in alter_patterns:
            matches = re.findall(pattern, ddl_content, re.IGNORECASE)
            for match in matches:
                if alter_type == "CHANGE_COLUMN":
                    table_name, old_column, new_column, column_def = match
                    ddl_statements.append(
                        {
                            "type": "ALTER_TABLE",
                            "table": table_name.lower(),
                            "alter_type": alter_type,
                            "old_column": old_column.lower(),
                            "new_column": new_column.lower(),
                            "column_definition": column_def.strip(),
                        }
                    )
                else:
                    table_name, column_name = match[:2]
                    column_def = match[2] if len(match) > 2 else None
                    ddl_statements.append(
                        {
                            "type": "ALTER_TABLE",
                            "table": table_name.lower(),
                            "alter_type": alter_type,
                            "column": column_name.lower(),
                            "column_definition": (
                                column_def.strip() if column_def else None
                            ),
                        }
                    )

        # CREATE INDEX 파싱
        create_index_pattern = (
            r"CREATE\s+(?:UNIQUE\s+)?INDEX\s+`?(\w+)`?\s+ON\s+`?(\w+)`?\s*\(([^)]+)\)"
        )
        index_matches = re.findall(create_index_pattern, ddl_content, re.IGNORECASE)

        for index_name, table_name, columns in index_matches:
            ddl_statements.append(
                {
                    "type": "CREATE_INDEX",
                    "table": table_name.lower(),
                    "index_name": index_name.lower(),
                    "columns": [
                        col.strip().strip("`").lower() for col in columns.split(",")
                    ],
                }
            )

        # DROP TABLE 파싱
        drop_table_pattern = r"DROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?`?(\w+)`?"
        drop_table_matches = re.findall(drop_table_pattern, ddl_content, re.IGNORECASE)

        for table_name in drop_table_matches:
            ddl_statements.append({"type": "DROP_TABLE", "table": table_name.lower()})

        # DROP INDEX 파싱
        drop_index_pattern = r"DROP\s+INDEX\s+`?(\w+)`?\s+ON\s+`?(\w+)`?"
        drop_index_matches = re.findall(drop_index_pattern, ddl_content, re.IGNORECASE)

        print(f"[DEBUG] DROP INDEX 파싱 - 패턴: {drop_index_pattern}")
        print(f"[DEBUG] DROP INDEX 파싱 - 입력: {repr(ddl_content)}")
        print(f"[DEBUG] DROP INDEX 파싱 - 결과: {drop_index_matches}")

        for index_name, table_name in drop_index_matches:
            ddl_statements.append(
                {
                    "type": "DROP_INDEX",
                    "table": table_name.lower(),
                    "index_name": index_name.lower(),
                }
            )
            print(f"[DEBUG] DROP INDEX 구문 추가됨: {index_name} on {table_name}")

        print(f"[DEBUG] 전체 파싱 결과: {len(ddl_statements)}개 구문")
        for i, stmt in enumerate(ddl_statements):
            print(f"[DEBUG]   [{i}] {stmt['type']}: {stmt}")

        return ddl_statements

    def parse_create_table_columns(self, columns_def: str) -> Dict[str, Any]:
        """CREATE TABLE의 컬럼 정의 파싱"""
        columns = []
        constraints = []

        # 컬럼 정의와 제약조건을 분리
        lines = [line.strip() for line in columns_def.split(",")]

        for line in lines:
            line = line.strip()
            if not line:
                continue

            # 제약조건 확인
            if re.match(
                r"(?:CONSTRAINT|PRIMARY\s+KEY|FOREIGN\s+KEY|UNIQUE|INDEX|KEY)",
                line,
                re.IGNORECASE,
            ):
                constraints.append(line)
            else:
                # 컬럼 정의 파싱
                column_match = re.match(
                    r"`?(\w+)`?\s+([^,\s]+)(?:\s+(.*))?", line, re.IGNORECASE
                )
                if column_match:
                    column_name = column_match.group(1).lower()
                    data_type = column_match.group(2).upper()
                    attributes = column_match.group(3) or ""

                    columns.append(
                        {
                            "name": column_name,
                            "data_type": data_type,
                            "attributes": attributes.strip(),
                        }
                    )

        return {"columns": columns, "constraints": constraints}

    def parse_ddl_constraints(self, ddl_content: str) -> Dict[str, List[Dict]]:
        """DDL에서 제약조건 정보 추출"""
        constraints = {"foreign_keys": [], "indexes": [], "primary_keys": []}

        # 외래키 패턴 매칭
        fk_pattern = (
            r"FOREIGN\s+KEY\s*\(`?(\w+)`?\)\s*REFERENCES\s+`?(\w+)`?\s*\(`?(\w+)`?\)"
        )
        fk_matches = re.findall(fk_pattern, ddl_content, re.IGNORECASE)

        for column, ref_table, ref_column in fk_matches:
            constraints["foreign_keys"].append(
                {
                    "column": column,
                    "referenced_table": ref_table,
                    "referenced_column": ref_column,
                }
            )

        return constraints
