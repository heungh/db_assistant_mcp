#!/usr/bin/env python3
"""
DB Assistant Amazon Q CLI MCP 서버
"""

import asyncio
import json
import os
import re
import subprocess
import time
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
from pathlib import Path
import sys

import boto3
from botocore.exceptions import ClientError

try:
    import mysql.connector
    from mysql.connector import Error as MySQLError
except ImportError:
    mysql = None
    MySQLError = Exception

# 분석 관련 라이브러리
ANALYSIS_AVAILABLE = False
try:
    import pandas as pd
    import numpy as np
    import sqlparse
    import matplotlib

    matplotlib.use("Agg")  # GUI 없는 환경에서 matplotlib 사용
    import matplotlib.pyplot as plt
    from sklearn.model_selection import train_test_split
    from sklearn.preprocessing import PolynomialFeatures
    from sklearn.linear_model import LinearRegression
    from sklearn.metrics import mean_squared_error, r2_score
    from sklearn.impute import SimpleImputer

    ANALYSIS_AVAILABLE = True
except ImportError:
    sqlparse = None

from mcp.server.models import InitializationOptions
import mcp.types as types
from mcp.server import NotificationOptions, Server
import mcp.server.stdio
import logging

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 현재 디렉토리 기준 경로 설정
CURRENT_DIR = Path(__file__).parent
OUTPUT_DIR = CURRENT_DIR / "output"
SQL_DIR = CURRENT_DIR / "sql"
DATA_DIR = CURRENT_DIR / "data"

# 디렉토리 생성
OUTPUT_DIR.mkdir(exist_ok=True)
SQL_DIR.mkdir(exist_ok=True)
DATA_DIR.mkdir(exist_ok=True)


class DBAssistantMCPServer:
    def __init__(self):
        self.bedrock_client = boto3.client(
            "bedrock-runtime", region_name="us-west-2", verify=False
        )
        self.knowledge_base_id = "0WQUBRHVR8"
        self.selected_database = None
        self.current_plan = None

        # 공용 DB 연결 변수 (연결 재사용을 위해)
        self.shared_connection = None
        self.shared_cursor = None
        self.tunnel_used = False

        # 성능 임계값 설정
        self.PERFORMANCE_THRESHOLDS = {
            "max_rows_scan": 10_000_000,  # 1천만 행 이상 스캔 시 실패
            "table_scan_ratio": 0.1,      # 테이블의 10% 이상 스캔 시 경고
            "critical_rows_scan": 50_000_000,  # 5천만 행 이상 스캔 시 심각한 문제
        }

        # 분석 관련 초기화
        self.cloudwatch = None
        self.default_metrics = [
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

        # Knowledge Base 클라이언트 초기화
        self.bedrock_agent_client = boto3.client(
            "bedrock-agent-runtime", region_name="us-east-1", verify=False
        )

    def parse_table_name(self, full_table_name: str) -> tuple:
        """테이블명에서 스키마와 테이블명을 분리"""
        if "." in full_table_name:
            schema, table = full_table_name.split(".", 1)
            return schema.strip("`"), table.strip("`")
        return None, full_table_name.strip("`")

    async def query_knowledge_base(self, query: str, sql_type: str) -> str:
        """Knowledge Base에서 관련 정보 조회"""
        try:
            # SQL 타입에 따른 쿼리 조정
            ddl_types = [
                "CREATE_TABLE",
                "ALTER_TABLE",
                "CREATE_INDEX",
                "DROP_TABLE",
                "DROP_INDEX",
            ]
            dql_types = ["SELECT", "UPDATE", "DELETE", "INSERT"]

            if sql_type in ddl_types:
                # DDL인 경우 데이터베이스 도메인 관리 규칙 조회
                kb_query = f"데이터베이스 도메인 관리 규칙 {query}"
            elif sql_type in dql_types:
                # DQL인 경우 Aurora MySQL 최적화 가이드 조회
                kb_query = f"Aurora MySQL 최적화 가이드 {query}"
            else:
                # 기본적으로 도메인 관리 규칙 조회
                kb_query = f"데이터베이스 도메인 관리 규칙 {query}"

            response = self.bedrock_agent_client.retrieve(
                knowledgeBaseId=self.knowledge_base_id,
                retrievalQuery={"text": kb_query},
                retrievalConfiguration={
                    "vectorSearchConfiguration": {"numberOfResults": 3}
                },
            )

            # 검색 결과에서 텍스트 추출
            knowledge_content = []
            for result in response.get("retrievalResults", []):
                content = result.get("content", {}).get("text", "")
                if content:
                    knowledge_content.append(content)

            if knowledge_content:
                return "\n\n".join(knowledge_content)
            else:
                return "관련 정보를 찾을 수 없습니다."

        except Exception as e:
            logger.warning(f"Knowledge Base 조회 실패: {e}")
            return "Knowledge Base 조회 중 오류가 발생했습니다."

    def get_secret(self, secret_name):
        """Secrets Manager에서 DB 연결 정보 가져오기"""
        try:
            session = boto3.session.Session()
            client = session.client(
                service_name="secretsmanager",
                region_name="ap-northeast-2",
                verify=False,
            )
            get_secret_value_response = client.get_secret_value(SecretId=secret_name)
            secret = get_secret_value_response["SecretString"]
            return json.loads(secret)
        except Exception as e:
            logger.error(f"Secret 조회 실패: {e}")
            raise e

    def get_secrets_by_keyword(self, keyword=""):
        """키워드로 Secret 목록 가져오기"""
        try:
            secrets_manager = boto3.client(
                service_name="secretsmanager",
                region_name="ap-northeast-2",
                verify=False,
            )

            all_secrets = []
            next_token = None

            # 페이지네이션 처리
            while True:
                if next_token:
                    response = secrets_manager.list_secrets(NextToken=next_token)
                else:
                    response = secrets_manager.list_secrets()

                all_secrets.extend(
                    [secret["Name"] for secret in response["SecretList"]]
                )

                if "NextToken" not in response:
                    break
                next_token = response["NextToken"]

            # 키워드 필터링
            if keyword:
                filtered_secrets = [
                    secret
                    for secret in all_secrets
                    if keyword.lower() in secret.lower()
                ]
                return filtered_secrets
            else:
                return all_secrets
        except Exception as e:
            logger.error(f"Secret 목록 조회 실패: {e}")
            return []

    def setup_ssh_tunnel(self, db_host: str, region: str = "ap-northeast-2") -> bool:
        """SSH 터널 설정"""
        try:
            import subprocess
            import time

            # 기존 터널 종료
            subprocess.run(["pkill", "-f", "ssh.*54.180.79.255"], capture_output=True)

            # SSH 터널 시작
            ssh_command = [
                "ssh",
                "-F",
                "/dev/null",
                "-o",
                "UserKnownHostsFile=/dev/null",
                "-o",
                "StrictHostKeyChecking=no",
                "-i",
                "/Users/heungh/test.pem",
                "-f",
                "-N",
                "-L",
                f"3307:{db_host}:3306",
                "ec2-user@54.180.79.255",
            ]

            logger.info(f"SSH 터널 설정 중: {db_host} -> localhost:3307")

            process = subprocess.run(ssh_command, capture_output=True, text=True)

            # 터널이 설정될 때까지 잠시 대기
            time.sleep(3)

            if process.returncode == 0:
                logger.info("SSH 터널이 설정되었습니다.")
                return True
            else:
                logger.error(f"SSH 터널 설정 실패: {process.stderr}")
                return False

        except Exception as e:
            logger.error(f"SSH 터널 설정 오류: {str(e)}")
            return False

    def cleanup_ssh_tunnel(self):
        """SSH 터널 정리"""
        try:
            import subprocess

            subprocess.run(["pkill", "-f", "ssh.*54.180.79.255"], capture_output=True)
            logger.info("SSH 터널이 정리되었습니다.")
        except Exception as e:
            logger.error(f"SSH 터널 정리 중 오류: {e}")

    async def validate_individual_ddl_statements(
        self, sql_content: str, cursor, debug_log, cte_tables: List[str]
    ):
        """개별 DDL 구문 검증 - CREATE TABLE/INDEX 각각 검증"""
        debug_log("🔥🔥🔥 validate_individual_ddl_statements 함수 시작 🔥🔥🔥")
        result = {"issues": []}

        try:
            # DDL 구문 파싱
            ddl_statements = self.parse_ddl_statements(sql_content)
            debug_log(f"파싱된 DDL 구문 수: {len(ddl_statements)}")

            # 파일 내에서 생성되는 테이블 목록 추출
            tables_created_in_file = set()
            for ddl in ddl_statements:
                if ddl["type"] == "CREATE_TABLE" and ddl["table"] not in cte_tables:
                    tables_created_in_file.add(ddl["table"])
            debug_log(f"파일 내 생성 테이블: {list(tables_created_in_file)}")

            for i, ddl in enumerate(ddl_statements):
                debug_log(
                    f"DDL [{i}] 검증 시작: {ddl['type']} - {ddl.get('table', ddl.get('index_name', 'unknown'))}"
                )

                if ddl["type"] == "CREATE_TABLE":
                    table_name = ddl["table"]

                    # CTE alias는 스킵
                    if table_name in cte_tables:
                        debug_log(
                            f"CREATE TABLE [{i}] - {table_name}은 CTE alias이므로 스킵"
                        )
                        continue

                    # 테이블 존재 여부 확인 (스키마 정보 포함 처리)
                    schema, actual_table = self.parse_table_name(table_name)
                    if schema:
                        cursor.execute(
                            """
                            SELECT COUNT(*) FROM information_schema.tables 
                            WHERE table_schema = %s AND table_name = %s
                        """,
                            (schema, actual_table),
                        )
                    else:
                        cursor.execute(
                            """
                            SELECT COUNT(*) FROM information_schema.tables 
                            WHERE table_schema = DATABASE() AND table_name = %s
                        """,
                            (actual_table,),
                        )
                    exists = cursor.fetchone()[0] > 0

                    if exists:
                        issue = f"CREATE TABLE 실패: 테이블 '{table_name}'이 이미 존재합니다."
                        result["issues"].append(issue)
                        debug_log(f"CREATE TABLE [{i}] - {table_name} 실패: 이미 존재")
                    else:
                        debug_log(
                            f"CREATE TABLE [{i}] - {table_name} 성공: 존재하지 않음"
                        )

                elif ddl["type"] == "CREATE_INDEX":
                    table_name = ddl["table"]
                    index_name = ddl["index_name"]
                    columns = ddl["columns"]

                    # CTE alias 테이블은 스킵
                    if table_name in cte_tables:
                        debug_log(
                            f"CREATE INDEX [{i}] - {table_name}은 CTE alias이므로 스킵"
                        )
                        continue

                    # 테이블 존재 여부 확인 (DB + 파일 내 생성) - 스키마 정보 포함 처리
                    schema, actual_table = self.parse_table_name(table_name)
                    if schema:
                        cursor.execute(
                            """
                            SELECT COUNT(*) FROM information_schema.tables 
                            WHERE table_schema = %s AND table_name = %s
                        """,
                            (schema, actual_table),
                        )
                    else:
                        cursor.execute(
                            """
                            SELECT COUNT(*) FROM information_schema.tables 
                            WHERE table_schema = DATABASE() AND table_name = %s
                        """,
                            (actual_table,),
                        )
                    table_exists_in_db = cursor.fetchone()[0] > 0
                    table_created_in_file = actual_table in tables_created_in_file

                    if not table_exists_in_db and not table_created_in_file:
                        issue = f"CREATE INDEX 실패: 테이블 '{table_name}'이 존재하지 않습니다."
                        result["issues"].append(issue)
                        debug_log(
                            f"CREATE INDEX [{i}] - {table_name}.{index_name} 실패: 테이블 없음"
                        )
                        continue

                    # 테이블이 파일 내에서 생성되는 경우, 인덱스 중복 검사 스킵
                    if table_created_in_file:
                        debug_log(
                            f"CREATE INDEX [{i}] - {table_name}.{index_name} 성공: 파일 내 생성 테이블"
                        )
                        continue

                    # 기존 테이블의 중복 인덱스 확인
                    cursor.execute(f"SHOW INDEX FROM `{table_name}`")
                    existing_indexes = cursor.fetchall()

                    # 동일한 컬럼에 대한 인덱스가 이미 있는지 확인
                    duplicate_found = False
                    for existing_idx in existing_indexes:
                        if existing_idx[4] == columns:  # Column_name
                            issue = f"CREATE INDEX 실패: 테이블 '{table_name}'의 컬럼 '{columns}'에 이미 인덱스 '{existing_idx[2]}'가 존재합니다."
                            result["issues"].append(issue)
                            debug_log(
                                f"CREATE INDEX [{i}] - {table_name}.{index_name} 실패: 중복 인덱스"
                            )
                            duplicate_found = True
                            break

                    if not duplicate_found:
                        debug_log(
                            f"CREATE INDEX [{i}] - {table_name}.{index_name} 성공: 중복 없음"
                        )

            debug_log(
                f"🔥🔥🔥 validate_individual_ddl_statements 완료: {len(result['issues'])}개 이슈 🔥🔥🔥"
            )
            return result

        except Exception as e:
            debug_log(f"개별 DDL 검증 오류: {e}")
            result["issues"].append(f"DDL 검증 오류: {str(e)}")
            return result

    def extract_successful_created_tables(
        self, sql_content: str, issues: List[str]
    ) -> List[str]:
        """성공한 CREATE TABLE만 추출 (실패한 것은 제외)"""
        created_tables = self.extract_created_tables(sql_content)
        successful_tables = []

        for table in created_tables:
            # issues에서 해당 테이블의 CREATE TABLE 실패 메시지가 있는지 확인
            table_failed = any(
                f"테이블 '{table}'이 이미 존재합니다" in issue for issue in issues
            )
            if not table_failed:
                successful_tables.append(table)

        return successful_tables

    def validate_dml_columns_with_context(
        self, sql_content: str, cursor, debug_log, available_tables: List[str]
    ):
        """컨텍스트를 고려한 DML 컬럼 검증"""
        # 기존 validate_dml_columns 로직을 사용하되, available_tables를 고려
        # 이 함수는 기존 함수를 확장한 버전입니다
        return self.validate_dml_columns(sql_content, cursor, debug_log)

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

    async def execute_explain_with_cursor(self, sql_content: str, cursor, debug_log):
        """EXPLAIN 실행 (커서 사용)"""
        result = {"issues": [], "explain_data": None}

        try:
            if cursor is None:
                debug_log("커서가 None입니다")
                result["issues"].append("데이터베이스 커서가 없습니다.")
                return result

            # EXPLAIN 실행
            explain_query = f"EXPLAIN {sql_content.strip().rstrip(';')}"
            debug_log(f"EXPLAIN 쿼리: {explain_query}")

            cursor.execute(explain_query)
            explain_data = cursor.fetchall()
            result["explain_data"] = explain_data

            # EXPLAIN 결과는 문자열로만 저장

            debug_log("EXPLAIN 실행 완료")
            return result

        except Exception as e:
            debug_log(f"EXPLAIN 실행 예외: {e}")
            result["issues"].append(f"EXPLAIN 실행 오류: {str(e)}")
            return result

    def check_performance_issues(self, explain_data, query_content, debug_log):
        """EXPLAIN 결과에서 성능 문제 검사"""
        debug_log("🔍🔍🔍 check_performance_issues 함수 시작 🔍🔍🔍")
        performance_issues = []
        
        # 승인된 대용량 배치 쿼리 체크
        batch_approval_patterns = [
            r"대용량\s*배치.*승인",
            r"배치.*승인.*받음",
            r"승인.*대용량",
            r"approved.*batch",
            r"batch.*approved"
        ]
        
        is_approved_batch = False
        for pattern in batch_approval_patterns:
            if re.search(pattern, query_content, re.IGNORECASE):
                is_approved_batch = True
                debug_log(f"승인된 대용량 배치 쿼리로 인식: {pattern}")
                break
        
        debug_log(f"EXPLAIN 데이터 행 수: {len(explain_data)}")
        for idx, row in enumerate(explain_data):
            debug_log(f"EXPLAIN 행 {idx}: {row}")
            if len(row) >= 10:  # EXPLAIN 결과 구조 확인
                rows_examined = row[9] if row[9] is not None else 0
                debug_log(f"검사할 행 수: {rows_examined}")
                
                if rows_examined >= self.PERFORMANCE_THRESHOLDS["critical_rows_scan"]:
                    if is_approved_batch:
                        issue = f"⚠️ 경고: 대용량 테이블 스캔 ({rows_examined:,}행) - 승인된 배치 작업"
                        performance_issues.append(issue)
                        debug_log(f"승인된 배치 - 경고 추가: {issue}")
                    else:
                        issue = f"❌ 실패: 심각한 성능 문제 - 대용량 테이블 전체 스캔 ({rows_examined:,}행)"
                        performance_issues.append(issue)
                        debug_log(f"심각한 성능 문제 - 실패 추가: {issue}")
                        
                elif rows_examined >= self.PERFORMANCE_THRESHOLDS["max_rows_scan"]:
                    if is_approved_batch:
                        issue = f"⚠️ 경고: 대용량 테이블 스캔 ({rows_examined:,}행) - 승인된 배치 작업"
                        performance_issues.append(issue)
                        debug_log(f"승인된 배치 - 경고 추가: {issue}")
                    else:
                        issue = f"❌ 실패: 성능 문제 - 대용량 테이블 스캔 ({rows_examined:,}행)"
                        performance_issues.append(issue)
                        debug_log(f"성능 문제 - 실패 추가: {issue}")
        
        debug_log(f"🔍🔍🔍 check_performance_issues 완료 - 이슈: {performance_issues}, 승인: {is_approved_batch} 🔍🔍🔍")
        return performance_issues, is_approved_batch

    async def execute_explain_individual_queries(
        self, sql_content: str, cursor, debug_log
    ):
        """개별 쿼리로 분리하여 EXPLAIN 실행 - CREATE 구문 고려"""
        result = {"issues": [], "explain_data": [], "performance_issues": []}

        try:
            if cursor is None:
                debug_log("커서가 None입니다")
                result["issues"].append("데이터베이스 커서가 없습니다.")
                return result

            # 현재 SQL에서 생성되는 테이블 추출
            created_tables = self.extract_created_tables(sql_content)
            debug_log(f"현재 SQL에서 생성되는 테이블: {created_tables}")

            # SQL 파일을 개별 쿼리로 분리
            if sqlparse:
                statements = sqlparse.split(sql_content)
            else:
                # sqlparse가 없으면 세미콜론으로 분리
                statements = [
                    stmt.strip() for stmt in sql_content.split(";") if stmt.strip()
                ]

            debug_log(f"총 {len(statements)}개의 개별 쿼리로 분리")

            for i, stmt in enumerate(statements):
                if not stmt.strip():
                    continue

                # 주석 제거 (라인 주석과 블록 주석 모두)
                cleaned_stmt = re.sub(r"--.*$", "", stmt, flags=re.MULTILINE)  # 라인 주석 제거
                cleaned_stmt = re.sub(r"/\*.*?\*/", "", cleaned_stmt, flags=re.DOTALL)  # 블록 주석 제거
                cleaned_stmt = cleaned_stmt.strip()
                debug_log(f"쿼리 {i+1} 정리 후: {repr(cleaned_stmt[:100])}")  # 디버그 로그 추가
                if not cleaned_stmt:
                    continue

                # DDL/DML 구문은 EXPLAIN 스킵
                ddl_pattern = re.match(
                    r"^\s*(CREATE|ALTER|DROP|RENAME)",
                    cleaned_stmt,
                    re.IGNORECASE,
                )
                dml_pattern = re.match(
                    r"^\s*(INSERT|UPDATE|DELETE)",
                    cleaned_stmt,
                    re.IGNORECASE,
                )

                if ddl_pattern:
                    debug_log(
                        f"🔥🔥🔥 쿼리 {i+1}: DDL 구문이므로 EXPLAIN 스킵 ({ddl_pattern.group(1).upper()}) 🔥🔥🔥"
                    )
                    continue
                elif dml_pattern:
                    debug_log(
                        f"🔥🔥🔥 쿼리 {i+1}: DML 구문이므로 EXPLAIN 스킵 ({dml_pattern.group(1).upper()}) 🔥🔥🔥"
                    )
                    continue

                # 쿼리에서 참조하는 테이블 추출
                query_tables = self.extract_table_names(cleaned_stmt)

                # 새로 생성되는 테이블을 참조하는지 확인
                references_new_table = any(
                    table in created_tables for table in query_tables
                )

                if references_new_table:
                    debug_log(
                        f"쿼리 {i+1}: 새로 생성되는 테이블을 참조하므로 EXPLAIN 스킵 - 테이블: {[t for t in query_tables if t in created_tables]}"
                    )
                    continue

                try:
                    # 각 쿼리에 대해 EXPLAIN 실행
                    explain_query = f"EXPLAIN {cleaned_stmt}"
                    debug_log(f"개별 쿼리 {i+1} EXPLAIN: {cleaned_stmt[:100]}...")
                    debug_log(f"🚨🚨🚨 성능 검사 코드 버전 확인 - 임계값: {self.PERFORMANCE_THRESHOLDS} 🚨🚨🚨")

                    cursor.execute(explain_query)
                    explain_data = cursor.fetchall()
                    
                    # 성능 문제 검사
                    debug_log(f"🔍 성능 검사 시작 - 쿼리 {i+1}, EXPLAIN 행 수: {len(explain_data)}")
                    perf_issues, is_approved = self.check_performance_issues(
                        explain_data, cleaned_stmt, debug_log
                    )
                    debug_log(f"🔍 성능 검사 완료 - 이슈: {perf_issues}, 승인됨: {is_approved}")
                    
                    if perf_issues:
                        result["performance_issues"].extend(perf_issues)
                        debug_log(f"⚠️ 성능 이슈 추가됨: {perf_issues}")
                        # 승인되지 않은 대용량 스캔은 오류로 처리
                        if not is_approved and any("❌ 실패" in issue for issue in perf_issues):
                            result["issues"].extend(perf_issues)
                            debug_log(f"❌ 성능 이슈를 오류로 처리: {perf_issues}")
                    
                    result["explain_data"].append(
                        {
                            "query_index": i + 1,
                            "query": (
                                cleaned_stmt[:200] + "..."
                                if len(cleaned_stmt) > 200
                                else cleaned_stmt
                            ),
                            "explain_result": explain_data,
                        }
                    )
                    debug_log(f"개별 쿼리 {i+1} EXPLAIN 성공")

                except Exception as e:
                    error_msg = f"쿼리 {i+1} EXPLAIN 오류: {str(e)}"
                    debug_log(error_msg)
                    result["issues"].append(error_msg)

                    # 컬럼 존재하지 않음 오류인 경우 상세 정보 추가
                    if "Unknown column" in str(e):
                        result["issues"].append(
                            f"쿼리 {i+1}에서 존재하지 않는 컬럼을 참조합니다: {cleaned_stmt[:100]}..."
                        )

            debug_log(
                f"개별 쿼리 EXPLAIN 완료: {len(result['explain_data'])}개 성공, {len(result['issues'])}개 오류"
            )
            return result

        except Exception as e:
            debug_log(f"개별 쿼리 EXPLAIN 전체 예외: {e}")
            result["issues"].append(f"개별 쿼리 EXPLAIN 실행 오류: {str(e)}")
            return result

    async def test_individual_query_validation(
        self, database_secret: str, filename: str
    ) -> str:
        """개별 쿼리 검증 테스트 함수"""
        try:
            # SQL 파일 읽기
            sql_file_path = os.path.join("sql", filename)
            if not os.path.exists(sql_file_path):
                return f"❌ SQL 파일을 찾을 수 없습니다: {filename}"

            with open(sql_file_path, "r", encoding="utf-8") as f:
                sql_content = f.read()

            # 데이터베이스 연결
            connection, tunnel_used = self.get_db_connection(
                database_secret, self.selected_database
            )
            if not connection:
                return "❌ 데이터베이스 연결 실패"

            cursor = connection.cursor()

            # 개별 쿼리 검증 실행
            result = await self.execute_explain_individual_queries(
                sql_content, cursor, print
            )

            # 연결 정리
            cursor.close()
            connection.close()
            if tunnel_used:
                self.cleanup_ssh_tunnel()

            return f"✅ 개별 쿼리 검증 완료\n성공: {len(result['explain_data'])}개\n오류: {len(result['issues'])}개\n상세: {result}"

        except Exception as e:
            return f"❌ 검증 중 오류: {str(e)}"

    async def validate_schema_with_cursor(self, ddl_content: str, cursor):
        """스키마 검증 (커서 사용) - 다중 CREATE 구문 고려"""
        result = {"valid": True, "issues": []}

        try:
            if cursor is None:
                result["valid"] = False
                result["issues"].append("데이터베이스 커서가 없습니다.")
                return result

            # 현재 SQL에서 생성되는 테이블들 추출
            created_tables = self.extract_created_tables(ddl_content)

            # DDL 타입에 따른 검증
            ddl_type = self.extract_ddl_type(ddl_content)

            if ddl_type == "CREATE_TABLE":
                # 각 CREATE TABLE 구문에 대해 검증
                for table_name in created_tables:
                    cursor.execute("SHOW TABLES LIKE %s", (table_name,))
                    if cursor.fetchone():
                        result["valid"] = False
                        result["issues"].append(
                            f"테이블 '{table_name}'이 이미 존재합니다."
                        )

            elif ddl_type == "ALTER_TABLE":
                # 테이블 변경 검증
                table_name = self.extract_table_name_from_alter(ddl_content)
                if table_name:
                    cursor.execute("SHOW TABLES LIKE %s", (table_name,))
                    if not cursor.fetchone():
                        result["valid"] = False
                        result["issues"].append(
                            f"테이블 '{table_name}'이 존재하지 않습니다."
                        )

            return result

        except Exception as e:
            result["valid"] = False
            result["issues"].append(f"스키마 검증 오류: {str(e)}")
            return result

    async def validate_constraints_with_cursor(self, ddl_content: str, cursor):
        """제약조건 검증 (커서 사용)"""
        result = {"valid": True, "issues": []}

        try:
            if cursor is None:
                result["valid"] = False
                result["issues"].append("데이터베이스 커서가 없습니다.")
                return result

            # 외래키 검증
            foreign_keys = self.extract_foreign_keys(ddl_content)
            for fk in foreign_keys:
                ref_table = fk.get("referenced_table")
                if ref_table:
                    cursor.execute("SHOW TABLES LIKE %s", (ref_table,))
                    if not cursor.fetchone():
                        result["valid"] = False
                        result["issues"].append(
                            f"참조 테이블 '{ref_table}'이 존재하지 않습니다."
                        )

            return result

        except Exception as e:
            result["valid"] = False
            result["issues"].append(f"제약조건 검증 오류: {str(e)}")
            return result

    async def validate_table_existence(self, sql_content: str, connection, debug_log):
        """테이블 존재성 검증"""
        result = {"issues": [], "tables_checked": []}

        try:
            if connection is None:
                debug_log("데이터베이스 연결이 None입니다")
                result["issues"].append("데이터베이스 연결이 없습니다.")
                return result

            cursor = connection.cursor()
            if cursor is None:
                debug_log("커서 생성 실패")
                result["issues"].append("데이터베이스 커서 생성 실패")
                return result

            # SQL에서 테이블명 추출
            tables = self.extract_table_names(sql_content)
            debug_log(f"추출된 테이블명: {tables}")

            for table in tables:
                result["tables_checked"].append(table)

                try:
                    # 테이블 존재 여부 확인
                    cursor.execute("SHOW TABLES LIKE %s", (table,))
                    exists = cursor.fetchone()

                    if not exists:
                        issue = f"테이블 '{table}'이 존재하지 않습니다."
                        result["issues"].append(issue)
                        debug_log(f"테이블 존재성 검증 실패: {table}")
                    else:
                        debug_log(f"테이블 존재성 검증 통과: {table}")
                except Exception as table_check_error:
                    debug_log(f"테이블 {table} 검증 중 오류: {table_check_error}")
                    result["issues"].append(
                        f"테이블 '{table}' 검증 오류: {str(table_check_error)}"
                    )

            cursor.close()

        except Exception as e:
            debug_log(f"테이블 존재성 검증 오류: {e}")
            result["issues"].append(f"테이블 존재성 검증 오류: {str(e)}")

        return result

    def extract_table_name_from_alter(self, ddl_content: str) -> str:
        """ALTER TABLE 구문에서 테이블명 추출"""
        # 주석 제거
        sql_clean = re.sub(r"--.*$", "", ddl_content, flags=re.MULTILINE)
        sql_clean = re.sub(r"/\*.*?\*/", "", sql_clean, flags=re.DOTALL)

        # ALTER TABLE 패턴
        alter_pattern = r"ALTER\s+TABLE\s+`?([a-zA-Z_][a-zA-Z0-9_]*)`?\s+"
        match = re.search(alter_pattern, sql_clean, re.IGNORECASE)

        if match:
            return match.group(1)
        return None

    def extract_created_tables(self, sql_content: str) -> List[str]:
        """현재 SQL에서 생성되는 테이블명 추출"""
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

    def extract_created_indexes(self, sql_content: str) -> List[str]:
        """현재 SQL에서 생성되는 인덱스명 추출"""
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

    def extract_cte_tables(self, sql_content: str) -> List[str]:
        """WITH절의 CTE(Common Table Expression) 테이블명 추출"""
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

    def extract_foreign_keys(self, ddl_content: str) -> List[Dict[str, str]]:
        """DDL에서 외래키 정보 추출"""
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

    def extract_table_names(self, sql_content: str) -> List[str]:
        """SQL에서 테이블명 추출 (WITH절 CTE 테이블 제외)"""
        tables = set()

        # 주석 제거
        sql_clean = re.sub(r"--.*$", "", sql_content, flags=re.MULTILINE)
        sql_clean = re.sub(r"/\*.*?\*/", "", sql_clean, flags=re.DOTALL)

        # WITH절의 CTE 테이블들 추출
        cte_tables = set(self.extract_cte_tables(sql_content))

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

    async def execute_explain(self, sql_content: str, connection, debug_log):
        """EXPLAIN 실행 및 분석"""
        result = {"issues": [], "explain_data": None}

        try:
            if connection is None:
                debug_log("데이터베이스 연결이 None입니다")
                result["issues"].append("데이터베이스 연결이 없습니다.")
                return result

            cursor = connection.cursor(dictionary=True)
            if cursor is None:
                debug_log("커서 생성 실패")
                result["issues"].append("데이터베이스 커서 생성 실패")
                return result

            # 주석 제거하고 실제 SQL만 추출
            sql_clean = re.sub(r"--.*$", "", sql_content, flags=re.MULTILINE)
            sql_clean = re.sub(r"/\*.*?\*/", "", sql_clean, flags=re.DOTALL)
            sql_clean = sql_clean.strip()

            if sql_clean.endswith(";"):
                sql_clean = sql_clean[:-1]

            explain_sql = f"EXPLAIN {sql_clean}"
            debug_log(f"EXPLAIN 실행: {explain_sql}")

            cursor.execute(explain_sql)
            explain_result = cursor.fetchall()
            result["explain_data"] = explain_result

            debug_log(f"EXPLAIN 결과: {explain_result}")

            # EXPLAIN 결과는 문자열로만 저장
            cursor.close()

        except Exception as e:
            debug_log(f"EXPLAIN 실행 오류: {e}")
            result["issues"].append(f"EXPLAIN 실행 오류: {str(e)}")

        return result

    def get_db_connection(
        self,
        database_secret: str,
        selected_database: str = None,
        use_ssh_tunnel: bool = True,
    ):
        """공통 DB 연결 함수"""
        if mysql is None:
            raise Exception(
                "mysql-connector-python이 설치되지 않았습니다. pip install mysql-connector-python을 실행해주세요."
            )

        # Secret에서 DB 연결 정보 가져오기
        session = boto3.session.Session()
        client = session.client(
            service_name="secretsmanager",
            region_name="ap-northeast-2",
            verify=False,
        )
        get_secret_value_response = client.get_secret_value(SecretId=database_secret)
        secret = get_secret_value_response["SecretString"]
        db_config = json.loads(secret)

        connection_config = None
        tunnel_used = False

        # 선택된 데이터베이스가 있으면 사용, 없으면 기본값 사용
        database_name = selected_database or db_config.get(
            "dbname", db_config.get("database")
        )
        # database_name이 None이 아닌 경우에만 문자열로 변환
        if database_name is not None:
            database_name = str(database_name)

        if use_ssh_tunnel:
            if self.setup_ssh_tunnel(db_config.get("host")):
                connection_config = {
                    "host": "localhost",
                    "port": 3307,
                    "user": db_config.get("username"),
                    "password": db_config.get("password"),
                    "database": database_name,
                    "connection_timeout": 10,
                }
                tunnel_used = True

        if not connection_config:
            connection_config = {
                "host": db_config.get("host"),
                "port": db_config.get("port", 3306),
                "user": db_config.get("username"),
                "password": db_config.get("password"),
                "database": database_name,
                "connection_timeout": 10,
            }

        connection = mysql.connector.connect(**connection_config)
        return connection, tunnel_used

    def setup_shared_connection(
        self,
        database_secret: str,
        selected_database: str = None,
        use_ssh_tunnel: bool = True,
    ):
        """공용 DB 연결 설정 (한 번만 호출)"""
        try:
            if self.shared_connection and self.shared_connection.is_connected():
                logger.info("이미 활성화된 공용 연결이 있습니다.")
                return True

            self.shared_connection, self.tunnel_used = self.get_db_connection(
                database_secret, selected_database, use_ssh_tunnel
            )

            if self.shared_connection and self.shared_connection.is_connected():
                self.shared_cursor = self.shared_connection.cursor()
                logger.info(f"공용 DB 연결 설정 완료 (터널: {self.tunnel_used})")
                return True
            else:
                logger.error("공용 DB 연결 실패")
                return False

        except Exception as e:
            logger.error(f"공용 DB 연결 설정 오류: {e}")
            return False

    def cleanup_shared_connection(self):
        """공용 DB 연결 정리"""
        try:
            if self.shared_cursor:
                self.shared_cursor.close()
                self.shared_cursor = None
                logger.info("공용 커서 닫기 완료")

            if self.shared_connection and self.shared_connection.is_connected():
                self.shared_connection.close()
                self.shared_connection = None
                logger.info("공용 DB 연결 닫기 완료")

            if self.tunnel_used:
                self.cleanup_ssh_tunnel()
                self.tunnel_used = False
                logger.info("SSH 터널 정리 완료")

        except Exception as e:
            logger.error(f"공용 연결 정리 중 오류: {e}")

    def get_shared_cursor(self):
        """공용 커서 반환"""
        if self.shared_cursor is None:
            logger.error(
                "공용 커서가 설정되지 않았습니다. setup_shared_connection()을 먼저 호출하세요."
            )
            return None
        return self.shared_cursor

    async def list_sql_files(self) -> str:
        """SQL 파일 목록 조회"""
        try:
            sql_files = list(SQL_DIR.glob("*.sql"))
            if not sql_files:
                return "sql 디렉토리에 SQL 파일이 없습니다."

            file_list = "\n".join([f"- {f.name}" for f in sql_files])
            return f"SQL 파일 목록:\n{file_list}"
        except Exception as e:
            return f"SQL 파일 목록 조회 실패: {str(e)}"

    async def list_database_secrets(self, keyword: str = "") -> str:
        """데이터베이스 시크릿 목록 조회"""
        try:
            secrets = self.get_secrets_by_keyword(keyword)
            if not secrets:
                return (
                    f"'{keyword}' 키워드로 찾은 시크릿이 없습니다."
                    if keyword
                    else "시크릿이 없습니다."
                )

            secret_list = "\n".join([f"- {secret}" for secret in secrets])
            return f"데이터베이스 시크릿 목록:\n{secret_list}"
        except Exception as e:
            return f"시크릿 목록 조회 실패: {str(e)}"

    async def test_database_connection(
        self, database_secret: str, use_ssh_tunnel: bool = True
    ) -> str:
        """데이터베이스 연결 테스트"""
        try:
            connection, tunnel_used = self.get_db_connection(
                database_secret, self.selected_database, use_ssh_tunnel
            )

            if connection.is_connected():
                db_info = connection.get_server_info()
                cursor = connection.cursor()
                cursor.execute("SELECT DATABASE()")
                current_db = cursor.fetchone()[0]

                # SHOW DATABASES 실행
                cursor.execute("SHOW DATABASES")
                databases = [db[0] for db in cursor.fetchall()]

                # 현재 DB의 테이블 목록
                tables = []
                if current_db:
                    cursor.execute("SHOW TABLES")
                    tables = [table[0] for table in cursor.fetchall()]

                cursor.close()
                connection.close()

                result = f"""✅ 데이터베이스 연결 성공!

**연결 정보:**
- 서버 버전: {db_info}
- 현재 데이터베이스: {current_db}
- 연결 방식: {'SSH Tunnel' if tunnel_used else 'Direct'}

**데이터베이스 목록:**"""
                for db in databases:
                    if db not in [
                        "information_schema",
                        "performance_schema",
                        "mysql",
                        "sys",
                    ]:
                        result += f"\n   - {db}"

                if tables:
                    result += f"\n\n**현재 DB 테이블 목록:**"
                    for table in tables:
                        result += f"\n   - {table}"

                # SSH 터널 정리
                if tunnel_used:
                    self.cleanup_ssh_tunnel()

                return result
            else:
                if tunnel_used:
                    self.cleanup_ssh_tunnel()
                return "❌ 데이터베이스 연결에 실패했습니다."

        except MySQLError as e:
            if use_ssh_tunnel:
                self.cleanup_ssh_tunnel()
            return f"❌ MySQL 오류: {str(e)}"
        except Exception as e:
            if use_ssh_tunnel:
                self.cleanup_ssh_tunnel()
            return f"❌ 연결 테스트 오류: {str(e)}"

    async def list_databases(
        self, database_secret: str, use_ssh_tunnel: bool = True
    ) -> str:
        """데이터베이스 목록 조회"""
        try:
            if mysql is None:
                raise Exception("mysql-connector-python이 설치되지 않았습니다.")

            # Secret에서 DB 연결 정보 가져오기
            session = boto3.session.Session()
            client = session.client(
                service_name="secretsmanager",
                region_name="ap-northeast-2",
                verify=False,
            )
            get_secret_value_response = client.get_secret_value(
                SecretId=database_secret
            )
            secret = get_secret_value_response["SecretString"]
            db_config = json.loads(secret)

            connection_config = None
            tunnel_used = False

            if use_ssh_tunnel:
                if self.setup_ssh_tunnel(db_config.get("host")):
                    connection_config = {
                        "host": "localhost",
                        "port": 3307,
                        "user": db_config.get("username"),
                        "password": db_config.get("password"),
                        "connection_timeout": 10,
                    }
                    tunnel_used = True

            if not connection_config:
                connection_config = {
                    "host": db_config.get("host"),
                    "port": db_config.get("port", 3306),
                    "user": db_config.get("username"),
                    "password": db_config.get("password"),
                    "connection_timeout": 10,
                }

            # 데이터베이스 없이 연결
            connection = mysql.connector.connect(**connection_config)
            cursor = connection.cursor()

            # 데이터베이스 목록 조회
            cursor.execute("SHOW DATABASES")
            databases = [
                db[0]
                for db in cursor.fetchall()
                if db[0]
                not in ["information_schema", "performance_schema", "mysql", "sys"]
            ]

            cursor.close()
            connection.close()

            if tunnel_used:
                self.cleanup_ssh_tunnel()

            result = "📋 사용 가능한 데이터베이스 목록:\n\n"
            for i, db in enumerate(databases, 1):
                result += f"{i}. {db}\n"
            result += f"\n총 {len(databases)}개의 데이터베이스가 있습니다."
            result += "\n\n💡 특정 데이터베이스를 선택하려면 번호나 이름을 사용하세요."

            return result

        except Exception as e:
            return f"❌ 데이터베이스 목록 조회 실패: {str(e)}"

    async def select_database(
        self, database_secret: str, database_selection: str, use_ssh_tunnel: bool = True
    ) -> str:
        """데이터베이스 선택 (USE 명령어 실행)"""
        try:
            # 먼저 데이터베이스 목록을 가져와서 유효성 검증
            db_list_result = await self.list_databases(database_secret, use_ssh_tunnel)

            # 데이터베이스 목록에서 실제 DB 이름들 추출
            lines = db_list_result.split("\n")
            databases = []
            for line in lines:
                if line.strip() and line[0].isdigit() and ". " in line:
                    db_name = line.split(". ", 1)[1]
                    databases.append(db_name)

            selected_db = None

            # 번호로 선택한 경우
            if database_selection.isdigit():
                index = int(database_selection) - 1
                if 0 <= index < len(databases):
                    selected_db = databases[index]
                else:
                    return f"❌ 잘못된 번호입니다. 1-{len(databases)} 범위에서 선택해주세요.\n\n{db_list_result}"
            else:
                # 이름으로 선택한 경우
                if database_selection in databases:
                    selected_db = database_selection
                else:
                    return f"❌ '{database_selection}' 데이터베이스를 찾을 수 없습니다.\n\n{db_list_result}"

            # 선택된 데이터베이스로 USE 명령어 실행
            connection, tunnel_used = self.get_db_connection(
                database_secret, None, use_ssh_tunnel
            )

            if connection.is_connected():
                cursor = connection.cursor()

                # USE 명령어 실행
                cursor.execute(f"USE `{selected_db}`")

                # 현재 데이터베이스 확인
                cursor.execute("SELECT DATABASE()")
                current_db = cursor.fetchone()[0]

                cursor.close()
                connection.close()

                # 선택된 데이터베이스 저장
                self.selected_database = selected_db

                result = f"✅ 데이터베이스 '{selected_db}' 선택 완료!\n\n"
                result += f"🔗 현재 활성 데이터베이스: {current_db}\n"
                result += f"💡 이제 이 데이터베이스에 대해 스키마 분석이나 SQL 검증을 수행할 수 있습니다."

                if tunnel_used:
                    self.cleanup_ssh_tunnel()

                return result
            else:
                return f"❌ 데이터베이스 연결 실패"

        except Exception as e:
            logger.error(f"데이터베이스 선택 오류: {e}")
            return f"❌ 오류 발생: {str(e)}"

    async def get_schema_summary(self, database_secret: str) -> str:
        """현재 스키마 요약 정보 반환"""
        try:
            # 직접 데이터베이스 연결해서 스키마 정보 조회
            connection, tunnel_used = self.get_db_connection(
                database_secret, self.selected_database
            )
            cursor = connection.cursor()

            # 현재 데이터베이스 확인
            cursor.execute("SELECT DATABASE()")
            current_db = cursor.fetchone()[0]

            # 테이블 정보 수집
            cursor.execute(
                """
                SELECT table_name, table_type, engine, table_rows, 
                       data_length, index_length, table_comment
                FROM information_schema.tables 
                WHERE table_schema = DATABASE()
                ORDER BY table_name
            """
            )

            tables_info = cursor.fetchall()

            summary = f"""📊 데이터베이스 스키마 요약 (DB: {current_db})

📋 **테이블 목록** ({len(tables_info)}개):"""

            for table_info in tables_info:
                table_name = table_info[0]
                table_type = table_info[1]
                engine = table_info[2]
                rows = table_info[3] or 0
                comment = table_info[6] or ""

                # 컬럼 수 조회
                cursor.execute(
                    """
                    SELECT COUNT(*) FROM information_schema.columns 
                    WHERE table_schema = DATABASE() AND table_name = %s
                """,
                    (table_name,),
                )
                column_count = cursor.fetchone()[0]

                # 인덱스 수 조회
                cursor.execute(
                    """
                    SELECT COUNT(DISTINCT index_name) FROM information_schema.statistics 
                    WHERE table_schema = DATABASE() AND table_name = %s
                """,
                    (table_name,),
                )
                index_count = cursor.fetchone()[0]

                summary += f"""
  🔹 **{table_name}** ({engine})
     - 컬럼: {column_count}개, 인덱스: {index_count}개
     - 예상 행 수: {rows:,}"""

                if comment:
                    summary += f"\n     - 설명: {comment}"

            cursor.close()
            connection.close()

            if tunnel_used:
                self.cleanup_ssh_tunnel()

            return summary

        except Exception as e:
            return f"❌ 스키마 요약 생성 실패: {str(e)}"

    async def get_table_schema(self, database_secret: str, table_name: str) -> str:
        """특정 테이블의 상세 스키마 정보 조회"""
        try:
            connection, tunnel_used = self.get_db_connection(
                database_secret, self.selected_database
            )
            cursor = connection.cursor()

            # 테이블 존재 확인
            cursor.execute(
                """
                SELECT COUNT(*) FROM information_schema.tables 
                WHERE table_schema = DATABASE() AND table_name = %s
            """,
                (table_name,),
            )

            if cursor.fetchone()[0] == 0:
                return f"❌ 테이블 '{table_name}'을 찾을 수 없습니다."

            # 컬럼 정보 조회
            cursor.execute(
                """
                SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT, 
                       COLUMN_COMMENT, COLUMN_KEY, EXTRA
                FROM information_schema.columns 
                WHERE table_schema = DATABASE() AND table_name = %s
                ORDER BY ORDINAL_POSITION
            """,
                (table_name,),
            )

            columns = cursor.fetchall()

            result = f"📋 **테이블 '{table_name}' 스키마 정보**\n\n"
            result += f"📊 **컬럼 목록** ({len(columns)}개):\n"

            for col in columns:
                col_name, data_type, is_nullable, default_val, comment, key, extra = col

                result += f"\n🔹 **{col_name}**\n"
                result += f"   - 타입: {data_type}\n"
                result += (
                    f"   - NULL 허용: {'예' if is_nullable == 'YES' else '아니오'}\n"
                )

                if default_val is not None:
                    result += f"   - 기본값: {default_val}\n"

                if key:
                    key_type = {"PRI": "기본키", "UNI": "고유키", "MUL": "인덱스"}.get(
                        key, key
                    )
                    result += f"   - 키 타입: {key_type}\n"

                if extra:
                    result += f"   - 추가 속성: {extra}\n"

                if comment:
                    result += f"   - 설명: {comment}\n"

            cursor.close()
            connection.close()

            if tunnel_used:
                self.cleanup_ssh_tunnel()

            return result

        except Exception as e:
            return f"❌ 테이블 스키마 조회 실패: {str(e)}"

    async def get_table_index(self, database_secret: str, table_name: str) -> str:
        """특정 테이블의 인덱스 정보 조회"""
        try:
            connection, tunnel_used = self.get_db_connection(
                database_secret, self.selected_database
            )
            cursor = connection.cursor()

            # 테이블 존재 확인
            cursor.execute(
                """
                SELECT COUNT(*) FROM information_schema.tables 
                WHERE table_schema = DATABASE() AND table_name = %s
            """,
                (table_name,),
            )

            if cursor.fetchone()[0] == 0:
                return f"❌ 테이블 '{table_name}'을 찾을 수 없습니다."

            # 인덱스 정보 조회
            cursor.execute(
                """
                SELECT INDEX_NAME, COLUMN_NAME, SEQ_IN_INDEX, NON_UNIQUE, 
                       INDEX_TYPE, CARDINALITY, NULLABLE, INDEX_COMMENT
                FROM information_schema.statistics 
                WHERE table_schema = DATABASE() AND table_name = %s
                ORDER BY INDEX_NAME, SEQ_IN_INDEX
            """,
                (table_name,),
            )

            indexes = cursor.fetchall()

            if not indexes:
                result = (
                    f"📋 **테이블 '{table_name}' 인덱스 정보**\n\n❌ 인덱스가 없습니다."
                )
            else:
                result = f"📋 **테이블 '{table_name}' 인덱스 정보**\n\n"

                # 인덱스별로 그룹화
                index_groups = {}
                for idx in indexes:
                    idx_name = idx[0]
                    if idx_name not in index_groups:
                        index_groups[idx_name] = []
                    index_groups[idx_name].append(idx)

                result += f"📊 **인덱스 목록** ({len(index_groups)}개):\n"

                for idx_name, idx_cols in index_groups.items():
                    first_col = idx_cols[0]
                    is_unique = "고유" if first_col[3] == 0 else "일반"
                    idx_type = first_col[4]
                    comment = first_col[7] or ""

                    result += f"\n🔹 **{idx_name}** ({is_unique} 인덱스)\n"
                    result += f"   - 타입: {idx_type}\n"

                    # 컬럼 목록
                    columns = [f"{col[1]}" for col in idx_cols]
                    result += f"   - 컬럼: {', '.join(columns)}\n"

                    if comment:
                        result += f"   - 설명: {comment}\n"

            cursor.close()
            connection.close()

            if tunnel_used:
                self.cleanup_ssh_tunnel()

            return result

        except Exception as e:
            return f"❌ 테이블 인덱스 조회 실패: {str(e)}"

    async def get_performance_metrics(
        self, database_secret: str, metric_type: str = "all"
    ) -> str:
        """데이터베이스 성능 메트릭 조회"""
        try:
            connection, tunnel_used = self.get_db_connection(
                database_secret, self.selected_database
            )
            cursor = connection.cursor()

            result = f"📊 **데이터베이스 성능 메트릭**\n\n"

            if metric_type in ["all", "query"]:
                # 쿼리 성능 통계
                cursor.execute(
                    """
                    SELECT 
                        DIGEST_TEXT as query_pattern,
                        COUNT_STAR as exec_count,
                        ROUND(AVG_TIMER_WAIT/1000000000000, 6) as avg_time_sec,
                        ROUND(MAX_TIMER_WAIT/1000000000000, 6) as max_time_sec,
                        ROUND(SUM_TIMER_WAIT/1000000000000, 6) as total_time_sec
                    FROM performance_schema.events_statements_summary_by_digest 
                    WHERE DIGEST_TEXT IS NOT NULL
                    ORDER BY AVG_TIMER_WAIT DESC 
                    LIMIT 5
                """
                )

                query_stats = cursor.fetchall()
                if query_stats:
                    result += "🔍 **느린 쿼리 TOP 5:**\n"
                    for i, (
                        pattern,
                        count,
                        avg_time,
                        max_time,
                        total_time,
                    ) in enumerate(query_stats, 1):
                        pattern_short = (
                            (pattern[:60] + "...") if len(pattern) > 60 else pattern
                        )
                        result += f"{i}. {pattern_short}\n"
                        result += f"   - 실행횟수: {count:,}, 평균시간: {avg_time:.3f}초, 최대시간: {max_time:.3f}초\n\n"

            if metric_type in ["all", "connection"]:
                # 연결 통계
                cursor.execute(
                    """
                    SELECT 
                        COUNT(*) as total_connections,
                        SUM(CASE WHEN COMMAND != 'Sleep' THEN 1 ELSE 0 END) as active_connections
                    FROM information_schema.processlist
                """
                )

                conn_stats = cursor.fetchone()
                if conn_stats:
                    result += f"🔗 **연결 통계:**\n"
                    result += f"- 총 연결: {conn_stats[0]}개\n"
                    result += f"- 활성 연결: {conn_stats[1]}개\n\n"

            cursor.close()
            connection.close()

            if tunnel_used:
                self.cleanup_ssh_tunnel()

            return result

        except Exception as e:
            return f"❌ 성능 메트릭 조회 실패: {str(e)}"

    # === DDL 검증 관련 메서드 ===

    async def validate_sql_file(
        self, filename: str, database_secret: Optional[str] = None
    ) -> str:
        """특정 SQL 파일 검증"""
        try:
            sql_file_path = SQL_DIR / filename
            if not sql_file_path.exists():
                return f"SQL 파일을 찾을 수 없습니다: {filename}"

            with open(sql_file_path, "r", encoding="utf-8") as f:
                ddl_content = f.read()

            result = await self.validate_ddl(ddl_content, database_secret, filename)
            return result
        except Exception as e:
            return f"SQL 파일 검증 실패: {str(e)}"

    async def validate_ddl(
        self, ddl_content: str, database_secret: Optional[str], filename: str
    ) -> str:
        """DDL/DML 검증 실행 (연결 재사용 패턴 적용)"""
        try:
            # 디버그 로그 파일 생성
            debug_log_path = (
                OUTPUT_DIR
                / f"debug_log_{filename}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
            )

            def debug_log(message):
                with open(debug_log_path, "a", encoding="utf-8") as f:
                    f.write(f"{datetime.now().strftime('%H:%M:%S')} - {message}\n")
                    f.flush()

            debug_log(f"validate_ddl 시작 - 파일: {filename}")
            debug_log(f"SQL 내용: {ddl_content.strip()}")

            issues = []
            db_connection_info = None
            schema_validation = None
            claude_analysis_result = None  # Claude 분석 결과 저장용
            constraint_validation = None
            explain_result = None

            # 변수 초기화
            dml_column_issues = []

            # 1. 기본 문법 검증 - 개선된 세미콜론 검증
            semicolon_valid = self.validate_semicolon_usage(ddl_content)
            if not semicolon_valid:
                issues.append("세미콜론이 누락되었습니다.")
                debug_log("세미콜론 검증 실패")
            else:
                debug_log("세미콜론 검증 통과")

            # 2. SQL 타입 확인
            sql_type = self.extract_ddl_type(ddl_content, debug_log)
            debug_log(f"SQL 타입: {sql_type}")

            # 3. SQL 타입에 따른 검증 분기
            ddl_types = [
                "CREATE_TABLE",
                "ALTER_TABLE",
                "CREATE_INDEX",
                "DROP_TABLE",
                "DROP_INDEX",
            ]
            dql_types = ["SELECT", "UPDATE", "DELETE", "INSERT", "MIXED_SELECT"]
            skip_types = ["SHOW", "SET", "USE"]  # 스킵할 SQL 타입

            if database_secret:
                try:
                    debug_log("공용 데이터베이스 연결 설정 시작")

                    # 공용 연결이 없으면 설정
                    if (
                        not self.shared_connection
                        or not self.shared_connection.is_connected()
                    ):
                        if not self.setup_shared_connection(
                            database_secret, self.selected_database
                        ):
                            debug_log("공용 데이터베이스 연결 실패")
                            issues.append("데이터베이스 연결에 실패했습니다.")
                        else:
                            debug_log("공용 데이터베이스 연결 성공")

                    # 공용 커서 사용
                    cursor = self.get_shared_cursor()
                    if cursor:
                        debug_log("공용 커서 사용 시작")

                        # WITH절 확인
                        cte_tables = self.extract_cte_tables(ddl_content)
                        has_with_clause = len(cte_tables) > 0
                        debug_log(f"WITH절 CTE 테이블: {cte_tables}")
                        debug_log(f"WITH절 존재 여부: {has_with_clause}")

                        # SQL 타입별 검증 분기
                        if sql_type in skip_types:
                            debug_log(f"SQL 타입 스킵: {sql_type} (SHOW/SET/USE 구문은 검증하지 않음)")
                        
                        # DDL 검증
                        elif sql_type in ddl_types:
                            debug_log(f"DDL 검증 수행: {sql_type}")
                            debug_log("=== 새로운 개별 DDL 검증 로직 시작 ===")

                            # 개별 DDL 구문 검증 (WITH절과 관계없이 실행)
                            ddl_validation = (
                                await self.validate_individual_ddl_statements(
                                    ddl_content, cursor, debug_log, cte_tables
                                )
                            )
                            debug_log(
                                f"개별 DDL 검증 완료: {len(ddl_validation['issues'])}개 이슈"
                            )
                            if ddl_validation["issues"]:
                                issues.extend(ddl_validation["issues"])

                        # DQL(DML) 검증 - MIXED_SELECT 포함
                        elif sql_type in dql_types:
                            debug_log(f"DQL 검증 수행: {sql_type}")

                            # MIXED_SELECT인 경우 DDL과 DML 모두 검증
                            if sql_type == "MIXED_SELECT":
                                debug_log("=== 혼합 SQL 파일 검증 시작 ===")
                                
                                # 1. DDL 구문 검증
                                debug_log("혼합 파일 내 DDL 구문 검증 시작")
                                ddl_validation = (
                                    await self.validate_individual_ddl_statements(
                                        ddl_content, cursor, debug_log, cte_tables
                                    )
                                )
                                debug_log(
                                    f"혼합 파일 DDL 검증 완료: {len(ddl_validation['issues'])}개 이슈"
                                )
                                if ddl_validation["issues"]:
                                    issues.extend(ddl_validation["issues"])

                            # DML 검증 (CTE alias는 스킵하되, 실제 테이블은 검증)
                            debug_log("개별 쿼리 EXPLAIN 함수 호출 시작")
                            explain_result = (
                                await self.execute_explain_individual_queries(
                                    ddl_content, cursor, debug_log
                                )
                            )
                            debug_log(
                                f"개별 쿼리 EXPLAIN 함수 호출 완료: {explain_result}"
                            )
                            if explain_result["issues"]:
                                issues.extend(explain_result["issues"])
                            
                            # 성능 이슈 처리
                            if "performance_issues" in explain_result and explain_result["performance_issues"]:
                                debug_log(f"성능 이슈 발견: {explain_result['performance_issues']}")
                                # 성능 이슈가 있으면 전체 검증 실패로 처리
                                for perf_issue in explain_result["performance_issues"]:
                                    if "❌ 실패" in perf_issue:
                                        issues.append(perf_issue)

                        else:
                            debug_log(f"알 수 없는 SQL 타입: {sql_type}")

                    else:
                        debug_log("공용 커서를 가져올 수 없습니다")
                        issues.append("데이터베이스 커서를 가져올 수 없습니다.")

                except Exception as e:
                    debug_log(f"데이터베이스 검증 오류: {e}")
                    issues.append(f"데이터베이스 검증 오류: {str(e)}")

            # 3.5. DML 쿼리의 컬럼 존재 여부 검증
            dml_column_issues = []
            if database_secret:  # sql_type 조건 제거하여 모든 경우에 실행
                try:
                    debug_log("DML 컬럼 검증 시작")
                    cursor = self.get_shared_cursor()
                    if cursor:
                        # 성공한 CREATE TABLE만 추출 (실패한 것은 제외)
                        successful_created_tables = (
                            self.extract_successful_created_tables(ddl_content, issues)
                        )
                        debug_log(f"성공한 CREATE TABLE: {successful_created_tables}")

                        # CTE alias 추출
                        cte_tables = self.extract_cte_tables(ddl_content)
                        debug_log(f"CTE alias 테이블: {cte_tables}")

                        # DML 컬럼 검증 (성공한 테이블과 CTE alias는 스킵)
                        available_tables = successful_created_tables + cte_tables
                        dml_validation_result = self.validate_dml_columns_with_context(
                            ddl_content, cursor, debug_log, available_tables
                        )
                        debug_log(f"DML 검증 결과: {dml_validation_result}")
                        if dml_validation_result["queries_with_issues"] > 0:
                            for query_result in dml_validation_result["results"]:
                                for issue in query_result["issues"]:
                                    dml_column_issues.append(
                                        {
                                            "type": "COLUMN_NOT_EXISTS",
                                            "severity": "ERROR",
                                            "message": issue["message"],
                                            "table": issue["table"],
                                            "column": issue["column"],
                                            "query_type": sql_type,
                                        }
                                    )
                                    debug_log(f"DML 컬럼 오류 발견: {issue['message']}")

                        debug_log(
                            f"DML 컬럼 검증 완료: {len(dml_column_issues)}개 이슈 발견"
                        )
                    else:
                        debug_log("DML 컬럼 검증 건너뜀: 공용 커서 없음")
                except Exception as e:
                    debug_log(f"DML 컬럼 검증 오류: {e}")
                    import traceback

                    debug_log(f"DML 컬럼 검증 스택 트레이스: {traceback.format_exc()}")

            # 4. Claude를 통한 검증
            try:
                debug_log("Claude 검증 시작")
                # 스키마 정보 추출
                relevant_schema_info = None
                explain_info_str = None

                if database_secret:
                    try:
                        relevant_schema_info = await self.extract_relevant_schema_info(
                            ddl_content, database_secret, debug_log
                        )
                        debug_log(f"관련 스키마 정보 추출 완료: {relevant_schema_info}")

                        # EXPLAIN 결과를 문자열로 변환하여 별도 처리
                        if explain_result:
                            explain_info_str = (
                                f"EXPLAIN 분석 결과: {str(explain_result)}"
                            )
                            debug_log(
                                f"EXPLAIN 정보 문자열 변환 완료: {explain_info_str}"
                            )

                    except Exception as e:
                        debug_log(f"스키마 정보 추출 실패: {e}")

                # 스키마 검증 결과 요약 생성
                schema_validation_summary = self.create_schema_validation_summary(
                    issues, dml_column_issues
                )
                debug_log(f"스키마 검증 요약 생성: {schema_validation_summary}")

                claude_result = await self.validate_with_claude(
                    ddl_content,
                    database_secret,
                    relevant_schema_info,
                    explain_info_str,
                    sql_type,
                    schema_validation_summary,
                )
                debug_log(f"Claude 검증 결과: {claude_result}")
                debug_log(f"스키마 정보 전달됨: {relevant_schema_info is not None}")

                # Claude 결과를 항상 저장 (성공/실패 상관없이)
                claude_analysis_result = claude_result

                # Claude 응답 분석 - 더 엄격한 검증
                if "오류:" in claude_result or "존재하지 않" in claude_result:
                    issues.append(f"Claude 검증: {claude_result}")
                    debug_log("Claude 검증에서 오류 발견")
                elif claude_result.startswith("검증 통과"):
                    debug_log("Claude 검증 통과")
                else:
                    debug_log("Claude 검증 완료")

            except Exception as e:
                logger.error(f"Claude 검증 오류: {e}")
                issues.append(f"Claude 검증 중 오류 발생: {str(e)}")
                debug_log(f"Claude 검증 예외: {e}")
                # 예외 발생 시에도 Claude 결과 설정
                claude_analysis_result = f"Claude 검증 중 오류 발생: {str(e)}"

            # DML 컬럼 이슈를 기존 이슈 목록에 추가
            if dml_column_issues:
                for dml_issue in dml_column_issues:
                    issues.append(dml_issue["message"])
                debug_log(
                    f"DML 컬럼 이슈 {len(dml_column_issues)}개를 최종 이슈 목록에 추가"
                )

            # 검증 완료
            debug_log(f"최종 이슈 개수: {len(issues)}")
            debug_log(f"이슈 목록: {issues}")

            # Claude 검증 결과를 기반으로 최종 상태 결정
            claude_success = (
                claude_analysis_result
                and claude_analysis_result.startswith("검증 통과")
            )

            # 결과 생성 - Claude 검증이 성공이면 우선적으로 PASS 처리
            if claude_success and not any(
                "오류:" in issue or "실패" in issue or "존재하지 않" in issue
                for issue in issues
            ):
                summary = "✅ 모든 검증을 통과했습니다."
                status = "PASS"
                debug_log("Claude 검증 성공으로 최종 상태를 PASS로 설정")
            elif not issues:
                summary = "✅ 모든 검증을 통과했습니다."
                status = "PASS"
            else:
                summary = f"❌ 발견된 문제: {len(issues)}개"
                status = "FAIL"

            debug_log(f"최종 상태: {status}, 요약: {summary}")

            # 보고서 생성 (HTML 형식)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            report_path = OUTPUT_DIR / f"validation_report_{filename}_{timestamp}.html"

            # HTML 보고서 생성
            debug_log("HTML 보고서 생성 시작")
            debug_log(f"dml_column_issues 값: {dml_column_issues}")
            debug_log(f"report_path 값: {report_path}")
            try:
                await self.generate_html_report(
                    report_path,
                    filename,
                    ddl_content,
                    sql_type,
                    status,
                    summary,
                    issues,
                    db_connection_info,
                    schema_validation,
                    constraint_validation,
                    database_secret,
                    explain_result,
                    claude_analysis_result,  # Claude 분석 결과 추가
                    dml_column_issues,  # DML 컬럼 이슈 추가
                )
                debug_log("HTML 보고서 생성 완료")
            except Exception as html_error:
                debug_log(f"HTML 보고서 생성 실패: {html_error}")
                import traceback

                debug_log(f"HTML 오류 상세: {traceback.format_exc()}")

            # 공용 연결 정리 (Claude 검증 완료 후)
            debug_log("공용 연결 정리 시작")
            self.cleanup_shared_connection()
            debug_log("공용 연결 정리 완료")

            return f"{summary}\n\n📄 상세 보고서가 저장되었습니다: {report_path}\n🔍 디버그 로그: {debug_log_path}"

        except Exception as e:
            # 예외 발생 시에도 연결 정리
            try:
                self.cleanup_shared_connection()
            except:
                pass
            return f"SQL 검증 중 오류 발생: {str(e)}"

    def validate_semicolon_usage(self, ddl_content: str) -> bool:
        """개선된 세미콜론 검증 - 독립적인 문장은 세미콜론 없어도 허용"""
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

    def extract_ddl_type(self, ddl_content: str, debug_log=None) -> str:
        """혼합 SQL 파일 타입 추출 - SELECT 쿼리가 많으면 MIXED_SELECT로 분류"""
        import re
        
        # 주석과 빈 줄을 제거하고 실제 구문만 추출
        # 먼저 /* */ 스타일 주석을 전체적으로 제거
        ddl_content = re.sub(r'/\*.*?\*/', '', ddl_content, flags=re.DOTALL)
        
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
            "RENAME": 0
        }
        
        # 각 구문 분석 - 세미콜론으로 분리된 실제 구문 단위로 계산
        sql_statements = [stmt.strip() for stmt in ddl_content.split(';') if stmt.strip()]
        
        for stmt in sql_statements:
            stmt_upper = stmt.upper().strip()
            
            # /* */ 스타일 주석 제거
            stmt_upper = re.sub(r'/\*.*?\*/', '', stmt_upper, flags=re.DOTALL)
            
            # -- 스타일 주석 제거
            stmt_lines = [line.strip() for line in stmt_upper.split('\n') 
                         if line.strip() and not line.strip().startswith('--')]
            if not stmt_lines:
                continue
                
            stmt_clean = ' '.join(stmt_lines).strip()
            
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
            elif stmt_clean.startswith("ALTER TABLE") or re.search(r'\bALTER\s+TABLE\b', stmt_clean):
                type_counts["ALTER_TABLE"] += 1
            elif stmt_clean.startswith("CREATE INDEX"):
                type_counts["CREATE_INDEX"] += 1
            elif stmt_clean.startswith("DROP TABLE"):
                type_counts["DROP_TABLE"] += 1
            elif stmt_clean.startswith("DROP INDEX"):
                type_counts["DROP_INDEX"] += 1
            elif stmt_clean.startswith("RENAME TABLE") or re.search(r'\bRENAME\s+TABLE\b', stmt_clean):
                type_counts["RENAME"] += 1
        
        # 총 구문 수
        total_statements = sum(type_counts.values())
        
        # 디버그 로그 추가
        if debug_log:
            debug_log(f"DEBUG - 구문 개수: {type_counts}")
            debug_log(f"DEBUG - 총 구문: {total_statements}")
        
        # SELECT 쿼리가 50% 이상이면 MIXED_SELECT로 분류
        if type_counts["SELECT"] > 0 and type_counts["SELECT"] >= total_statements * 0.5:
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

    def detect_ddl_type(self, ddl_content: str) -> str:
        """DDL 타입 감지"""
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

    def create_schema_validation_summary(
        self, issues: list, dml_column_issues: list
    ) -> str:
        """스키마 검증 결과를 요약하여 Claude에게 전달할 형태로 생성"""
        if not issues and not dml_column_issues:
            return "스키마 검증: 모든 검증 통과"

        summary_parts = []
        if issues:
            summary_parts.append(f"스키마 검증 문제점 ({len(issues)}개):")
            for i, issue in enumerate(issues, 1):  # 모든 문제 표시
                summary_parts.append(f"  {i}. {issue}")

        if dml_column_issues:
            summary_parts.append(f"컬럼 검증 문제점 ({len(dml_column_issues)}개):")
            for i, issue in enumerate(dml_column_issues, 1):  # 모든 문제 표시
                summary_parts.append(f"  {i}. {issue}")

        return "\n".join(summary_parts)

    async def validate_with_claude(
        self,
        ddl_content: str,
        database_secret: str = None,
        schema_info: dict = None,
        explain_info: str = None,
        sql_type: str = None,
        schema_validation_summary: str = None,
    ) -> str:
        """
        Claude cross-region 프로파일을 활용한 DDL 검증 (실제 스키마 정보 포함)
        """
        # 스키마 정보가 제공되지 않았고 database_secret이 있으면 스키마 정보 추출
        if schema_info is None and database_secret:
            try:
                schema_info = await self.extract_current_schema_info(database_secret)
            except Exception as e:
                logger.warning(f"스키마 정보 추출 실패: {e}")
                schema_info = {}

        # 관련 스키마 정보를 포함한 프롬프트 생성 (순서 고려)
        schema_context = ""
        if schema_info:
            schema_details = []

            # 순서대로 정렬하여 처리
            sorted_items = sorted(
                schema_info.items(), key=lambda x: x[1].get("order", 0)
            )

            for key, info in sorted_items:
                order = info.get("order", 0)

                if info["type"] == "table":
                    table_name = info.get("table_name", key)

                    if "columns" in info:
                        # ALTER TABLE 케이스
                        columns_info = [
                            f"{col['name']}({col['data_type']})"
                            for col in info["columns"]
                        ]
                        schema_details.append(f"[{order}] ALTER TABLE '{table_name}':")
                        schema_details.append(f"  - DB에 존재: {info['exists']}")
                        if info.get("created_in_file"):
                            schema_details.append(
                                f"  - 파일 내 생성됨: {info['created_in_file']}"
                            )
                        schema_details.append(
                            f"  - 유효 존재: {info.get('effective_exists', info['exists'])}"
                        )

                        if info["exists"] and columns_info:
                            schema_details.append(
                                f"  - 기존 컬럼: {', '.join(columns_info)}"
                            )

                        if info.get("alter_type") and info.get("target_column"):
                            schema_details.append(
                                f"  - ALTER 작업: {info['alter_type']} {info['target_column']}"
                            )
                    else:
                        # CREATE/DROP TABLE 케이스
                        action = (
                            "CREATE"
                            if "CREATE" in key or info.get("exists") == False
                            else "DROP"
                        )
                        schema_details.append(
                            f"[{order}] {action} TABLE '{table_name}': DB존재={info['exists']}"
                        )

                elif info["type"] == "index":
                    table_name = info.get("table_name", key.split(".")[0])
                    index_name = info["index_name"]

                    if "duplicate_column_indexes" in info:
                        # CREATE INDEX 케이스
                        schema_details.append(
                            f"[{order}] CREATE INDEX '{index_name}' on '{table_name}':"
                        )
                        schema_details.append(
                            f"  - 테이블 DB존재: {info['table_exists']}"
                        )
                        if info.get("created_in_file"):
                            schema_details.append(
                                f"  - 테이블 파일내생성: {info['created_in_file']}"
                            )
                        schema_details.append(
                            f"  - 테이블 유효존재: {info.get('effective_exists', info['table_exists'])}"
                        )
                        schema_details.append(
                            f"  - 생성할 컬럼: {', '.join(info['target_columns'])}"
                        )

                        # DB의 중복 인덱스
                        if info["duplicate_column_indexes"]:
                            schema_details.append("  - DB의 동일 컬럼 구성 인덱스:")
                            for dup_idx in info["duplicate_column_indexes"]:
                                schema_details.append(
                                    f"    * {dup_idx['name']} ({dup_idx['columns']}) - {'UNIQUE' if dup_idx['unique'] else 'NON-UNIQUE'}"
                                )

                        # 파일 내 중복 인덱스
                        if info.get("file_duplicate_indexes"):
                            schema_details.append("  - 파일 내 동일 컬럼 구성 인덱스:")
                            for dup_idx in info["file_duplicate_indexes"]:
                                schema_details.append(
                                    f"    * [{dup_idx['order']}] {dup_idx['name']} ({','.join(dup_idx['columns'])})"
                                )

                        if not info["duplicate_column_indexes"] and not info.get(
                            "file_duplicate_indexes"
                        ):
                            schema_details.append("  - 중복 컬럼 구성 인덱스 없음")
                    else:
                        # DROP INDEX 케이스
                        schema_details.append(
                            f"[{order}] DROP INDEX '{index_name}' on '{table_name}':"
                        )
                        schema_details.append(
                            f"  - 테이블 존재: {info['table_exists']}"
                        )
                        schema_details.append(
                            f"  - 인덱스 존재: {info['index_exists']}"
                        )

            if schema_details:
                schema_context = f"""
관련 스키마 정보 (실행 순서별):
{chr(10).join(schema_details)}

위 정보를 바탕으로 DDL의 적절성을 판단해주세요.
특히 다음 사항을 확인해주세요:
1. 파일 내에서 먼저 생성된 테이블은 이후 ALTER/INDEX 작업에서 존재하는 것으로 간주
2. 동일한 컬럼 구성의 인덱스 중복 여부
3. 존재하지 않는 테이블/인덱스에 대한 DROP 시도
4. 실행 순서상 논리적 오류
"""
            else:
                schema_context = """
관련 스키마 정보를 찾을 수 없습니다.
기본적인 문법 검증만 수행합니다.
"""
        else:
            schema_context = """
스키마 정보를 가져올 수 없습니다.
기본적인 문법 검증만 수행합니다.
"""

        # EXPLAIN 정보 컨텍스트 추가
        explain_context = ""
        if explain_info:
            explain_context = f"""
EXPLAIN 분석 결과:
{explain_info}

위 EXPLAIN 결과를 참고하여 성능상 문제가 있는지도 함께 분석해주세요.
참고: DDL 구문(CREATE, ALTER, DROP 등)에 대해서는 EXPLAIN을 실행하지 않으며, 
SELECT, UPDATE, DELETE, INSERT 등의 DML 구문에 대해서만 EXPLAIN 분석을 수행합니다.
"""

        # 스키마 검증 결과 컨텍스트 추가
        schema_validation_context = ""
        schema_has_errors = False
        if schema_validation_summary:
            # 스키마 검증에서 오류가 있는지 확인
            schema_has_errors = (
                "오류" in schema_validation_summary
                or "실패" in schema_validation_summary
                or "존재하지 않" in schema_validation_summary
                or "이미 존재" in schema_validation_summary
            )

            schema_validation_context = f"""
기존 스키마 검증 결과:
{schema_validation_summary}

위 스키마 검증 결과를 참고하여 종합적인 판단을 해주세요.
스키마 검증에서 문제가 발견된 경우, 해당 문제점들을 고려하여 검증해주세요.
"""

        # Knowledge Base에서 관련 정보 조회
        knowledge_context = ""
        try:
            knowledge_info = await self.query_knowledge_base(ddl_content, sql_type)
            if knowledge_info and knowledge_info != "관련 정보를 찾을 수 없습니다.":
                knowledge_context = f"""
Knowledge Base 참고 정보:
{knowledge_info}

위 정보를 참고하여 검증을 수행해주세요.
"""
        except Exception as e:
            logger.warning(f"Knowledge Base 조회 중 오류: {e}")

        # DDL과 DQL에 따른 프롬프트 구분
        ddl_types = [
            "CREATE_TABLE",
            "ALTER_TABLE",
            "CREATE_INDEX",
            "DROP_TABLE",
            "DROP_INDEX",
        ]
        dql_types = ["SELECT", "UPDATE", "DELETE", "INSERT"]

        if sql_type in ddl_types:
            # DDL 검증 프롬프트
            prompt = f"""
        다음 DDL 문을 Aurora MySQL 문법으로 검증해주세요:

        {ddl_content}

        {schema_context}

        {schema_validation_context}

        {knowledge_context}

        **검증 기준:**
        Aurora MySQL 8.0에서 문법적으로 올바르고 실행 가능한지만 확인하세요.

        **중요: 스키마 검증 실패 처리**
        {"스키마 검증에서 오류가 발견되었습니다. 테이블이 이미 존재하거나, 인덱스가 존재하거나, 기타 스키마 관련 문제가 있으면 반드시 실패로 평가해주세요." if schema_has_errors else ""}

        **응답 규칙:**
        1. {"스키마 검증에서 오류가 발견된 경우 반드시 '오류:'로 시작하여 실패로 평가하세요" if schema_has_errors else "DDL이 Aurora MySQL에서 실행 가능하면 반드시 '검증 통과'로 시작하세요"}
        2. 성능 개선이나 모범 사례는 "검증 통과 (권장사항: ...)"로 표시하세요  
        3. 실행을 막는 심각한 문법 오류만 "오류:"로 시작하세요

        **예시:**
        - 실행 가능한 경우: "검증 통과"
        - 개선점 있는 경우: "검증 통과 (권장사항: NULL 속성을 명시하면 더 명확합니다)"
        - 실행 불가능한 경우: "오류: 존재하지 않는 테이블을 참조합니다"

        반드시 위 형식으로만 응답하세요.
        """
        elif sql_type in dql_types:
            # DQL 검증 프롬프트
            prompt = f"""
        다음 DQL(DML) 쿼리를 Aurora MySQL에서 검증해주세요:

        {ddl_content}

        {explain_context}

        {schema_validation_context}

        {knowledge_context}

        **검증 기준:**
        1. Aurora MySQL 8.0에서 문법적으로 올바른지 확인
        2. 성능상 문제가 있는지 분석
        3. 인덱스 사용 효율성 검토
        4. **중요: 심각한 성능 문제가 있으면 검증 실패로 처리**

        **중요: 스키마 검증 실패 처리**
        {"스키마 검증에서 오류가 발견되었습니다. 테이블이 이미 존재하거나, 인덱스가 존재하거나, 기타 스키마 관련 문제가 있으면 반드시 실패로 평가해주세요." if schema_has_errors else ""}

        **성능 문제 실패 기준:**
        다음과 같은 심각한 성능 문제가 발견되면 반드시 "오류:"로 시작하여 실패로 평가하세요:
        - 1천만 행 이상의 대용량 테이블에 대한 전체 테이블 스캔 (Full Table Scan)
        - WHERE 절 없는 대용량 테이블 UPDATE/DELETE
        - 인덱스 없는 대용량 테이블 JOIN
        - 카디널리티가 매우 높은 GROUP BY나 ORDER BY 작업
        - 임시 테이블을 사용하는 복잡한 서브쿼리

        **응답 규칙:**
        1. {"스키마 검증에서 오류가 발견된 경우 반드시 '오류:'로 시작하여 실패로 평가하세요" if schema_has_errors else "쿼리가 실행 가능하고 심각한 성능 문제가 없으면 '검증 통과'로 시작하세요"}
        2. 심각한 성능 문제가 있으면 "오류: 심각한 성능 문제 - ..."로 시작하여 실패로 평가하세요
        3. 경미한 성능 개선점만 있으면 "검증 통과 (성능 권장사항: ...)"로 표시하세요
        4. 실행 불가능한 경우는 "오류:"로 시작하세요

        **예시:**
        - 실행 가능하고 성능 문제 없음: "검증 통과"
        - 경미한 성능 개선점: "검증 통과 (성능 권장사항: 인덱스 추가를 고려하세요)"
        - 심각한 성능 문제: "오류: 심각한 성능 문제 - 1천만 행 이상 테이블의 전체 스캔으로 운영 환경에서 사용 불가"
        - 실행 불가능한 경우: "오류: 존재하지 않는 테이블을 참조합니다"

        반드시 위 형식으로만 응답하세요.
        """
        else:
            # 기본 프롬프트
            prompt = f"""
        다음 SQL 문을 Aurora MySQL 문법으로 검증해주세요: 

        {ddl_content}

        {schema_context}

        {explain_context}

        {schema_validation_context}

        {knowledge_context}

        **검증 기준:**
        Aurora MySQL 8.0에서 문법적으로 올바르고 실행 가능한지만 확인하세요. 

        **중요: 스키마 검증 실패 처리**
        {"스키마 검증에서 오류가 발견되었습니다. 테이블이 이미 존재하거나, 인덱스가 존재하거나, 기타 스키마 관련 문제가 있으면 반드시 실패로 평가해주세요." if schema_has_errors else ""}

        **응답 규칙:**
        1. {"스키마 검증에서 오류가 발견된 경우 반드시 '오류:'로 시작하여 실패로 평가하세요" if schema_has_errors else "SQL이 Aurora MySQL에서 실행 가능하면 반드시 '검증 통과'로 시작하세요"}
        2. 성능 개선이나 모범 사례는 "검증 통과 (권장사항: ...)"로 표시하세요  
        3. 실행을 막는 심각한 문법 오류만 "오류:"로 시작하세요
        4. 권장사항을 제안할때, Aurora MySQL 8.0 이 아닌 기능이나 확인되지 않은 내용은 권장하지 마세요. 예를 들어 쿼리캐시 기능은 8.0부터 사용되지 않습니다. 구 mysql기능을 권장하거나 거론하지 마세요.

        반드시 위 형식으로만 응답하세요.
        """

        claude_input = json.dumps(
            {
                "anthropic_version": "bedrock-2023-05-31",
                "max_tokens": 4096,  # 토큰 수를 4배로 증가
                "messages": [
                    {"role": "user", "content": [{"type": "text", "text": prompt}]}
                ],
                "temperature": 0.3,
            }
        )

        sonnet_4_model_id = "us.anthropic.claude-sonnet-4-20250514-v1:0"
        sonnet_3_7_model_id = "us.anthropic.claude-3-7-sonnet-20250219-v1:0"

        # Claude Sonnet 4 inference profile 호출
        try:
            response = self.bedrock_client.invoke_model(
                modelId=sonnet_4_model_id, body=claude_input
            )
            response_body = json.loads(response.get("body").read())
            return response_body.get("content", [{}])[0].get("text", "")
        except Exception as e:
            logger.warning(
                f"Claude Sonnet 4 호출 실패 → Claude 3.7 Sonnet cross-region profile로 fallback: {e}"
            )
            # Claude 3.7 Sonnet inference profile 호출 (fallback)
            try:
                response = self.bedrock_client.invoke_model(
                    modelId=sonnet_3_7_model_id, body=claude_input
                )
                response_body = json.loads(response.get("body").read())
                return response_body.get("content", [{}])[0].get("text", "")
            except Exception as e:
                logger.error(f"Claude 3.7 Sonnet 호출 오류: {e}")
                return f"Claude 호출 중 오류 발생: {str(e)}"

    async def extract_current_schema_info(
        self, database_secret: str, use_ssh_tunnel: bool = True
    ) -> Dict[str, Any]:
        """현재 데이터베이스의 스키마 정보 추출"""
        try:
            logger.info(f"스키마 정보 추출 시작: database_secret={database_secret}")
            connection, tunnel_used = self.get_db_connection(
                database_secret, self.selected_database, use_ssh_tunnel
            )
            cursor = connection.cursor()

            # 현재 데이터베이스 확인
            cursor.execute("SELECT DATABASE()")
            current_db = cursor.fetchone()[0]
            logger.info(f"현재 데이터베이스: {current_db}")

            schema_info = {"tables": [], "columns": {}, "indexes": {}}

            # 테이블 목록 조회
            cursor.execute(
                """
                SELECT table_name FROM information_schema.tables 
                WHERE table_schema = DATABASE() AND table_type = 'BASE TABLE'
                ORDER BY table_name
            """
            )
            tables = [row[0] for row in cursor.fetchall()]
            schema_info["tables"] = tables
            logger.info(f"발견된 테이블: {tables}")

            # 각 테이블의 컬럼 정보 조회
            for table in tables:
                cursor.execute(
                    """
                    SELECT column_name, data_type, character_maximum_length, 
                           numeric_precision, numeric_scale, is_nullable, column_default
                    FROM information_schema.columns 
                    WHERE table_schema = DATABASE() AND table_name = %s
                    ORDER BY ordinal_position
                """,
                    (table,),
                )

                columns = []
                for col_row in cursor.fetchall():
                    col_info = {
                        "name": col_row[0],
                        "data_type": col_row[1],
                        "max_length": col_row[2],
                        "precision": col_row[3],
                        "scale": col_row[4],
                        "is_nullable": col_row[5],
                        "default_value": col_row[6],
                    }
                    columns.append(col_info)

                schema_info["columns"][table] = columns

                # 인덱스 정보 조회
                cursor.execute(
                    """
                    SELECT index_name, column_name, non_unique, seq_in_index
                    FROM information_schema.statistics 
                    WHERE table_schema = DATABASE() AND table_name = %s
                    ORDER BY index_name, seq_in_index
                """,
                    (table,),
                )

                indexes = {}
                for idx_row in cursor.fetchall():
                    idx_name = idx_row[0]
                    col_name = idx_row[1]
                    is_unique = idx_row[2] == 0

                    if idx_name not in indexes:
                        indexes[idx_name] = {"columns": [], "unique": is_unique}
                    indexes[idx_name]["columns"].append(col_name)

                schema_info["indexes"][table] = indexes

            cursor.close()
            connection.close()

            if tunnel_used:
                self.cleanup_ssh_tunnel()

            logger.info(
                f"스키마 정보 추출 완료: {len(schema_info['tables'])}개 테이블, {len(schema_info['columns'])}개 테이블의 컬럼 정보"
            )
            return schema_info

        except Exception as e:
            logger.error(f"스키마 정보 추출 오류: {e}")
            if "tunnel_used" in locals() and tunnel_used:
                self.cleanup_ssh_tunnel()
            return {}

    async def extract_relevant_schema_info(
        self,
        ddl_content: str,
        database_secret: str,
        debug_log,
        use_ssh_tunnel: bool = True,
    ) -> Dict[str, Any]:
        """DDL 유형에 따라 관련된 스키마 정보만 추출 (공용 커서 사용)"""
        try:
            # DDL 유형 파싱
            ddl_info = self.parse_ddl_detailed_with_debug(ddl_content, debug_log)
            if not ddl_info:
                return {}

            # 공용 커서 사용
            cursor = self.get_shared_cursor()
            if cursor is None:
                debug_log("공용 커서를 가져올 수 없음 - 스키마 정보 추출 중단")
                return {}

            relevant_info = {}

            # 파일 내에서 생성되는 테이블들을 추적
            created_tables_in_file = set()
            created_indexes_in_file = {}  # table_name -> [index_info]

            debug_log(f"총 {len(ddl_info)}개의 DDL 구문 처리 시작")

            for i, ddl_statement in enumerate(ddl_info):
                ddl_type = ddl_statement["type"]
                table_name = ddl_statement["table"]

                debug_log(f"관련 스키마 정보 추출: {ddl_type} for {table_name}")

                if ddl_type == "CREATE_TABLE":
                    # CREATE TABLE: 테이블 존재 여부 확인
                    try:
                        cursor.execute(
                            """
                            SELECT COUNT(*) FROM information_schema.tables 
                            WHERE table_schema = DATABASE() AND table_name = %s
                        """,
                            (table_name,),
                        )
                        result = cursor.fetchone()
                        table_exists = result[0] > 0 if result else False

                        relevant_info[f"{i}_{table_name}"] = {
                            "exists": table_exists,
                            "type": "table",
                            "order": i,
                            "table_name": table_name,
                        }

                        # 파일 내에서 생성되는 테이블로 추가
                        created_tables_in_file.add(table_name)
                        debug_log(
                            f"CREATE TABLE [{i}] - {table_name} 존재: {table_exists}, 파일 내 생성 추가"
                        )
                    except Exception as e:
                        debug_log(f"CREATE TABLE 정보 조회 오류: {e}")
                        table_exists = False

                elif ddl_type == "ALTER_TABLE":
                    # ALTER TABLE: 테이블 존재 여부 + 컬럼 정보 (파일 내 생성 고려)
                    cursor.execute(
                        """
                        SELECT COUNT(*) FROM information_schema.tables 
                        WHERE table_schema = DATABASE() AND table_name = %s
                    """,
                        (table_name,),
                    )
                    table_exists = cursor.fetchone()[0] > 0

                    # 파일 내에서 이미 생성된 테이블인지 확인
                    created_in_file = table_name in created_tables_in_file
                    effective_exists = table_exists or created_in_file

                    columns_info = []
                    if table_exists:  # 실제 DB에 존재하는 경우만 컬럼 정보 조회
                        cursor.execute(
                            """
                            SELECT column_name, data_type, character_maximum_length, 
                                   numeric_precision, numeric_scale, is_nullable
                            FROM information_schema.columns 
                            WHERE table_schema = DATABASE() AND table_name = %s
                            ORDER BY ordinal_position
                        """,
                            (table_name,),
                        )

                        for col_row in cursor.fetchall():
                            columns_info.append(
                                {
                                    "name": col_row[0],
                                    "data_type": col_row[1],
                                    "max_length": col_row[2],
                                    "precision": col_row[3],
                                    "scale": col_row[4],
                                    "is_nullable": col_row[5],
                                }
                            )

                    relevant_info[f"{i}_{table_name}"] = {
                        "exists": table_exists,
                        "created_in_file": created_in_file,
                        "effective_exists": effective_exists,
                        "type": "table",
                        "columns": columns_info,
                        "alter_type": ddl_statement.get("alter_type", "GENERAL"),
                        "target_column": ddl_statement.get("column"),
                        "order": i,
                        "table_name": table_name,
                    }
                    debug_log(
                        f"ALTER TABLE [{i}] - {table_name} DB존재: {table_exists}, 파일내생성: {created_in_file}, 유효존재: {effective_exists}"
                    )

                elif ddl_type == "CREATE_INDEX":
                    # CREATE INDEX: 테이블 존재 + 인덱스 정보 + 동일 컬럼 구성 인덱스 확인 (파일 내 생성 고려)
                    index_name = ddl_statement.get("index_name")
                    columns_str = ddl_statement.get("columns", "")
                    target_columns = [
                        col.strip().strip("`").lower() for col in columns_str.split(",")
                    ]

                    cursor.execute(
                        """
                        SELECT COUNT(*) FROM information_schema.tables 
                        WHERE table_schema = DATABASE() AND table_name = %s
                    """,
                        (table_name,),
                    )
                    table_exists = cursor.fetchone()[0] > 0

                    # 파일 내에서 이미 생성된 테이블인지 확인
                    created_in_file = table_name in created_tables_in_file
                    effective_exists = table_exists or created_in_file

                    existing_indexes = []
                    duplicate_column_indexes = []
                    file_duplicate_indexes = []

                    # DB에 실제 존재하는 인덱스 확인
                    if table_exists:
                        cursor.execute(
                            """
                            SELECT index_name, GROUP_CONCAT(column_name ORDER BY seq_in_index) as columns,
                                   non_unique
                            FROM information_schema.statistics 
                            WHERE table_schema = DATABASE() AND table_name = %s
                            GROUP BY index_name, non_unique
                        """,
                            (table_name,),
                        )

                        target_columns_sorted = ",".join(sorted(target_columns))

                        for idx_row in cursor.fetchall():
                            idx_name = idx_row[0]
                            idx_columns = idx_row[1].lower()
                            is_unique = idx_row[2] == 0

                            existing_indexes.append(
                                {
                                    "name": idx_name,
                                    "columns": idx_columns,
                                    "unique": is_unique,
                                }
                            )

                            # 동일한 컬럼 구성 확인
                            if idx_columns == target_columns_sorted:
                                duplicate_column_indexes.append(
                                    {
                                        "name": idx_name,
                                        "columns": idx_columns,
                                        "unique": is_unique,
                                    }
                                )

                    # 파일 내에서 이미 생성된 인덱스와 중복 확인
                    target_columns_sorted = ",".join(sorted(target_columns))
                    if table_name in created_indexes_in_file:
                        for file_idx in created_indexes_in_file[table_name]:
                            if file_idx["columns_sorted"] == target_columns_sorted:
                                file_duplicate_indexes.append(file_idx)

                    # 파일 내 생성 인덱스로 추가
                    if table_name not in created_indexes_in_file:
                        created_indexes_in_file[table_name] = []
                    created_indexes_in_file[table_name].append(
                        {
                            "name": index_name,
                            "columns": target_columns,
                            "columns_sorted": target_columns_sorted,
                            "order": i,
                        }
                    )

                    relevant_info[f"{i}_{table_name}.{index_name}"] = {
                        "type": "index",
                        "table_exists": table_exists,
                        "created_in_file": created_in_file,
                        "effective_exists": effective_exists,
                        "index_name": index_name,
                        "target_columns": target_columns,
                        "existing_indexes": existing_indexes,
                        "duplicate_column_indexes": duplicate_column_indexes,
                        "file_duplicate_indexes": file_duplicate_indexes,
                        "order": i,
                        "table_name": table_name,
                    }
                    debug_log(
                        f"CREATE INDEX [{i}] - {table_name}.{index_name}, DB중복: {len(duplicate_column_indexes)}개, 파일중복: {len(file_duplicate_indexes)}개"
                    )

                elif ddl_type == "DROP_TABLE":
                    # DROP TABLE: 테이블 존재 여부 확인 (파일 내 생성은 고려하지 않음 - DROP은 실제 존재해야 함)
                    cursor.execute(
                        """
                        SELECT COUNT(*) FROM information_schema.tables 
                        WHERE table_schema = DATABASE() AND table_name = %s
                    """,
                        (table_name,),
                    )
                    table_exists = cursor.fetchone()[0] > 0

                    relevant_info[f"{i}_{table_name}"] = {
                        "exists": table_exists,
                        "type": "table",
                        "order": i,
                        "table_name": table_name,
                    }
                    debug_log(f"DROP TABLE [{i}] - {table_name} 존재: {table_exists}")

                elif ddl_type == "DROP_INDEX":
                    # DROP INDEX: 테이블 존재 + 인덱스 존재 여부 (파일 내 생성은 고려하지 않음)
                    index_name = ddl_statement.get("index_name")

                    cursor.execute(
                        """
                        SELECT COUNT(*) FROM information_schema.tables 
                        WHERE table_schema = DATABASE() AND table_name = %s
                    """,
                        (table_name,),
                    )
                    table_exists = cursor.fetchone()[0] > 0

                    index_exists = False
                    if table_exists:
                        cursor.execute(
                            """
                            SELECT COUNT(*) FROM information_schema.statistics 
                            WHERE table_schema = DATABASE() AND table_name = %s AND index_name = %s
                        """,
                            (table_name, index_name),
                        )
                        index_exists = cursor.fetchone()[0] > 0

                    relevant_info[f"{i}_{table_name}.{index_name}"] = {
                        "type": "index",
                        "table_exists": table_exists,
                        "index_name": index_name,
                        "index_exists": index_exists,
                        "order": i,
                        "table_name": table_name,
                    }
                    debug_log(
                        f"DROP INDEX [{i}] - {table_name}.{index_name} 존재: {index_exists}"
                    )

            debug_log(f"관련 스키마 정보 추출 완료: {len(relevant_info)}개 항목")
            return relevant_info

        except Exception as e:
            logger.error(f"관련 스키마 정보 추출 오류: {e}")
            debug_log(f"관련 스키마 정보 추출 예외: {e}")
            return {}

    def validate_dml_columns(self, sql_content: str, cursor, debug_log) -> dict:
        """DML 쿼리의 컬럼 존재 여부 검증 - CREATE 구문 고려"""
        try:
            debug_log("=== DML 컬럼 검증 시작 ===")
            if not sqlparse:
                debug_log("sqlparse 모듈이 없어 DML 컬럼 검증을 건너뜁니다")
                return {"total_queries": 0, "queries_with_issues": 0, "results": []}

            # 현재 SQL에서 생성되는 테이블 추출
            created_tables = self.extract_created_tables(sql_content)
            debug_log(f"DML 검증에서 생성되는 테이블: {created_tables}")

            # 스키마 캐시
            schema_cache = {}

            def get_table_columns(table_name: str) -> set:
                """테이블의 컬럼 목록 조회 (스키마 정보 포함 처리)"""
                if table_name in schema_cache:
                    return schema_cache[table_name]

                try:
                    schema, actual_table = parse_table_name(table_name)

                    if schema:
                        cursor.execute(
                            """
                            SELECT column_name 
                            FROM information_schema.columns 
                            WHERE table_schema = %s AND table_name = %s
                        """,
                            (schema, actual_table),
                        )
                    else:
                        cursor.execute(
                            """
                            SELECT column_name 
                            FROM information_schema.columns 
                            WHERE table_schema = DATABASE() AND table_name = %s
                        """,
                            (actual_table,),
                        )

                    columns = {row[0].lower() for row in cursor.fetchall()}
                    schema_cache[table_name] = columns
                    debug_log(f"테이블 '{table_name}' 컬럼 조회: {columns}")
                    return columns
                except Exception as e:
                    debug_log(f"테이블 '{table_name}' 컬럼 조회 실패: {e}")
                    return set()

            def parse_table_name(full_table_name: str) -> tuple:
                """테이블명에서 스키마와 테이블명을 분리"""
                if "." in full_table_name:
                    schema, table = full_table_name.split(".", 1)
                    return schema.strip("`"), table.strip("`")
                return None, full_table_name.strip("`")

            def table_exists(table_name: str) -> bool:
                """테이블 존재 여부 확인 (스키마 정보 포함 처리)"""
                try:
                    schema, actual_table = parse_table_name(table_name)

                    if schema:
                        # 스키마가 명시된 경우
                        cursor.execute(
                            """
                            SELECT COUNT(*) FROM information_schema.tables 
                            WHERE table_schema = %s AND table_name = %s
                        """,
                            (schema, actual_table),
                        )
                    else:
                        # 스키마가 없는 경우 현재 데이터베이스에서 검색
                        cursor.execute(
                            """
                            SELECT COUNT(*) FROM information_schema.tables 
                            WHERE table_schema = DATABASE() AND table_name = %s
                        """,
                            (actual_table,),
                        )
                    return cursor.fetchone()[0] > 0
                except:
                    return False

            # SQL 문에서 사용된 컬럼들 추출
            validation_results = []

            # 주석 제거
            cleaned_sql = re.sub(r"--.*$", "", sql_content, flags=re.MULTILINE)

            # 각 쿼리별로 검증
            statements = sqlparse.split(cleaned_sql)
            debug_log(f"총 {len(statements)}개의 구문으로 분리됨")

            for i, stmt in enumerate(statements):
                if not stmt.strip():
                    continue

                stmt_lower = stmt.lower()
                issues = []
                debug_log(f"구문 {i+1} 검증 시작: {stmt_lower[:50]}...")

                # SELECT, UPDATE, DELETE 쿼리에서 컬럼 추출
                if any(
                    keyword in stmt_lower for keyword in ["select", "update", "delete"]
                ):
                    debug_log(f"구문 {i+1}은 DML 쿼리입니다")
                    # FROM 절에서 테이블 추출
                    # FROM과 JOIN에서 테이블명 추출 (스키마 정보 포함 처리)
                    debug_log(f"구문 {i+1} 원본: {stmt}")
                    from_pattern = r"from\s+(?:(\w+)\.)?(\w+)(?:\s+(?:as\s+)?(\w+))?(?=\s+(?:where|order|group|limit|join|inner|left|right|full|cross|$|;))"
                    from_tables = re.findall(from_pattern, stmt_lower)
                    debug_log(f"구문 {i+1} FROM 패턴 결과: {from_tables}")

                    join_pattern = r"join\s+(?:(\w+)\.)?(\w+)(?:\s+(?:as\s+)?(\w+))?(?=\s+(?:on|$|;))"
                    join_tables = re.findall(join_pattern, stmt_lower)
                    debug_log(f"구문 {i+1} JOIN 패턴 결과: {join_tables}")

                    # 테이블 별칭 매핑
                    table_aliases = {}
                    all_tables = set()

                    for schema, table, alias in from_tables + join_tables:
                        full_table_name = f"{schema}.{table}" if schema else table
                        all_tables.add(full_table_name)
                        debug_log(
                            f"구문 {i+1} 테이블 추가: schema={schema}, table={table}, full_name={full_table_name}"
                        )
                        if alias and alias not in [
                            "where",
                            "order",
                            "group",
                            "limit",
                            "join",
                            "inner",
                            "left",
                            "right",
                            "full",
                            "cross",
                            "on",
                        ]:
                            table_aliases[alias] = full_table_name

                    debug_log(f"구문 {i+1}에서 참조하는 테이블: {all_tables}")

                    # 새로 생성되는 테이블을 참조하는지 확인
                    references_new_table = any(
                        table in created_tables for table in all_tables
                    )
                    if references_new_table:
                        debug_log(
                            f"구문 {i+1}: 새로 생성되는 테이블을 참조하므로 DML 컬럼 검증 스킵: {[t for t in all_tables if t in created_tables]}"
                        )
                        continue

                    # 테이블.컬럼 형태의 컬럼 참조 찾기 (FROM/JOIN에서 이미 추출된 테이블만 고려)
                    column_refs = []
                    for table_or_alias in all_tables | set(table_aliases.keys()):
                        # 각 테이블/별칭에 대해 컬럼 참조 찾기
                        pattern = rf"\b{re.escape(table_or_alias)}\.(\w+)"
                        matches = re.findall(pattern, stmt_lower)
                        for column in matches:
                            column_refs.append((table_or_alias, column))

                    debug_log(f"구문 {i+1}에서 발견된 컬럼 참조: {column_refs}")

                    # 컬럼 존재 여부 검증
                    for table_ref, column_name in column_refs:
                        # 실제 테이블명 해결
                        actual_table = table_aliases.get(table_ref, table_ref)

                        # 새로 생성되는 테이블이면 스킵 (이중 체크)
                        if actual_table in created_tables:
                            debug_log(
                                f"테이블 '{actual_table}'은 새로 생성되므로 컬럼 검증 스킵"
                            )
                            continue

                        if (
                            actual_table in all_tables
                            or actual_table in schema_cache
                            or table_exists(actual_table)
                        ):
                            existing_columns = get_table_columns(actual_table)

                            if (
                                column_name not in existing_columns
                                and column_name != "*"
                            ):
                                issues.append(
                                    {
                                        "type": "MISSING_COLUMN",
                                        "table": actual_table,
                                        "column": column_name,
                                        "message": f"컬럼 '{column_name}'이 테이블 '{actual_table}'에 존재하지 않습니다",
                                    }
                                )
                else:
                    debug_log(f"구문 {i+1}은 DML 쿼리가 아닙니다")

                if issues:
                    validation_results.append(
                        {
                            "sql": (
                                stmt.strip()[:100] + "..."
                                if len(stmt.strip()) > 100
                                else stmt.strip()
                            ),
                            "issues": issues,
                        }
                    )

            debug_log(
                f"=== DML 컬럼 검증 완료: {len(validation_results)}개 쿼리에서 이슈 발견 ==="
            )
            return {
                "total_queries": len([s for s in statements if s.strip()]),
                "queries_with_issues": len(validation_results),
                "results": validation_results,
            }

        except Exception as e:
            debug_log(f"DML 컬럼 검증 예외: {e}")
            return {"total_queries": 0, "queries_with_issues": 0, "results": []}

    async def test_database_connection_for_validation(
        self, database_secret: str, use_ssh_tunnel: bool = True
    ) -> Dict[str, Any]:
        """검증용 데이터베이스 연결 테스트"""
        try:
            connection, tunnel_used = await self.get_db_connection(
                database_secret, self.selected_database, use_ssh_tunnel
            )

            if connection.is_connected():
                db_info = connection.get_server_info()
                cursor = connection.cursor()
                cursor.execute("SELECT DATABASE()")
                current_db = cursor.fetchone()[0]

                cursor.close()
                connection.close()

                result = {
                    "success": True,
                    "server_version": db_info,
                    "current_database": current_db,
                    "connection_method": "SSH Tunnel" if tunnel_used else "Direct",
                    "host": "localhost" if tunnel_used else "remote",
                    "port": 3307 if tunnel_used else 3306,
                }

                # SSH 터널 정리
                if tunnel_used:
                    self.cleanup_ssh_tunnel()

                return result
            else:
                if tunnel_used:
                    self.cleanup_ssh_tunnel()
                return {"success": False, "error": "데이터베이스 연결에 실패했습니다."}

        except MySQLError as e:
            if use_ssh_tunnel:
                self.cleanup_ssh_tunnel()
            return {"success": False, "error": f"MySQL 오류: {str(e)}"}
        except Exception as e:
            if use_ssh_tunnel:
                self.cleanup_ssh_tunnel()
            return {"success": False, "error": f"연결 테스트 오류: {str(e)}"}

    async def validate_schema_with_debug(
        self,
        ddl_content: str,
        database_secret: str,
        debug_log,
        use_ssh_tunnel: bool = True,
    ) -> Dict[str, Any]:
        """DDL 구문 유형에 따른 스키마 검증 (디버그 버전)"""
        try:
            debug_log("validate_schema 시작")
            # DDL 구문 유형 및 상세 정보 파싱
            ddl_info = self.parse_ddl_detailed_with_debug(ddl_content, debug_log)
            debug_log(f"파싱된 DDL 정보: {ddl_info}")

            if not ddl_info:
                debug_log("DDL 파싱 실패")
                return {
                    "success": False,
                    "error": "DDL에서 구문 정보를 추출할 수 없습니다.",
                }

            connection, tunnel_used = await self.get_db_connection(
                database_secret, self.selected_database, use_ssh_tunnel
            )
            cursor = connection.cursor()
            debug_log(f"DB 연결 성공, 터널 사용: {tunnel_used}")

            validation_results = []

            # DDL 구문 유형별 검증
            for ddl_statement in ddl_info:
                ddl_type = ddl_statement["type"]
                table_name = ddl_statement["table"]
                debug_log(f"검증 중: {ddl_type} on {table_name}")

                if ddl_type == "CREATE_TABLE":
                    debug_log("CREATE_TABLE 검증 호출")
                    result = await self.validate_create_table_with_debug(
                        cursor, ddl_statement, debug_log
                    )
                    debug_log(f"CREATE_TABLE 검증 결과: {result}")
                elif ddl_type == "ALTER_TABLE":
                    debug_log("ALTER_TABLE 검증 호출")
                    result = await self.validate_alter_table_with_debug(
                        cursor, ddl_statement, debug_log
                    )
                    debug_log(f"ALTER_TABLE 검증 결과: {result}")
                elif ddl_type == "CREATE_INDEX":
                    debug_log("CREATE_INDEX 검증 호출")
                    result = await self.validate_create_index_with_debug(
                        cursor, ddl_statement, debug_log
                    )
                    debug_log(f"CREATE_INDEX 검증 결과: {result}")
                elif ddl_type == "DROP_TABLE":
                    debug_log("DROP_TABLE 검증 호출")
                    result = await self.validate_drop_table_with_debug(
                        cursor, ddl_statement, debug_log
                    )
                    debug_log(f"DROP_TABLE 검증 결과: {result}")
                elif ddl_type == "DROP_INDEX":
                    debug_log("DROP_INDEX 검증 호출")
                    result = await self.validate_drop_index_with_debug(
                        cursor, ddl_statement, debug_log
                    )
                    debug_log(f"DROP_INDEX 검증 결과: {result}")
                else:
                    result = {
                        "table": table_name,
                        "ddl_type": ddl_type,
                        "valid": False,
                        "issues": [f"지원하지 않는 DDL 구문 유형: {ddl_type}"],
                    }

                validation_results.append(result)
                debug_log(f"검증 결과 추가됨: {result}")

            cursor.close()
            connection.close()

            # SSH 터널 정리
            if tunnel_used:
                self.cleanup_ssh_tunnel()

            debug_log(f"validate_schema 완료, 결과 개수: {len(validation_results)}")
            return {"success": True, "validation_results": validation_results}

        except Exception as e:
            debug_log(f"validate_schema 예외: {e}")
            if use_ssh_tunnel:
                self.cleanup_ssh_tunnel()
            return {"success": False, "error": f"스키마 검증 오류: {str(e)}"}

    def parse_ddl_detailed_with_debug(
        self, ddl_content: str, debug_log
    ) -> List[Dict[str, Any]]:
        """DDL 구문을 상세하게 파싱하여 구문 유형별 정보 추출 (디버그 버전)"""
        ddl_statements = []

        debug_log(f"DDL 파싱 시작: {repr(ddl_content)}")

        # CREATE TABLE 파싱
        create_table_pattern = r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?`?(\w+)`?"
        create_table_matches = re.findall(
            create_table_pattern, ddl_content, re.IGNORECASE
        )

        debug_log(f"CREATE TABLE 파싱 - 결과: {create_table_matches}")

        for table_name in create_table_matches:
            ddl_statements.append({"type": "CREATE_TABLE", "table": table_name.lower()})
            debug_log(f"CREATE TABLE 구문 추가됨: {table_name}")

        # ALTER TABLE 파싱 (상세)
        # ADD COLUMN - 스키마 정보 포함 처리
        alter_add_pattern = r"ALTER\s+TABLE\s+`?(?:(\w+)\.)?(\w+)`?\s+ADD\s+(?:COLUMN\s+)?`?(\w+)`?\s+([^,;]+)"
        alter_add_matches = re.findall(alter_add_pattern, ddl_content, re.IGNORECASE)
        debug_log(f"ALTER TABLE ADD COLUMN 파싱 - 결과: {alter_add_matches}")

        for schema, table_name, column_name, column_def in alter_add_matches:
            full_table_name = f"{schema}.{table_name}" if schema else table_name
            ddl_statements.append(
                {
                    "type": "ALTER_TABLE",
                    "table": full_table_name.lower(),
                    "alter_type": "ADD_COLUMN",
                    "column": column_name.lower(),
                    "column_definition": column_def.strip(),
                }
            )
            debug_log(
                f"ALTER TABLE ADD COLUMN 구문 추가됨: {full_table_name}.{column_name}"
            )

        # MODIFY COLUMN
        alter_modify_pattern = (
            r"ALTER\s+TABLE\s+`?(\w+)`?\s+MODIFY\s+(?:COLUMN\s+)?`?(\w+)`?\s+([^,;]+)"
        )
        alter_modify_matches = re.findall(
            alter_modify_pattern, ddl_content, re.IGNORECASE
        )
        debug_log(f"ALTER TABLE MODIFY COLUMN 파싱 - 결과: {alter_modify_matches}")

        for table_name, column_name, column_def in alter_modify_matches:
            ddl_statements.append(
                {
                    "type": "ALTER_TABLE",
                    "table": table_name.lower(),
                    "alter_type": "MODIFY_COLUMN",
                    "column": column_name.lower(),
                    "column_definition": column_def.strip(),
                }
            )
            debug_log(
                f"ALTER TABLE MODIFY COLUMN 구문 추가됨: {table_name}.{column_name}"
            )

        # DROP COLUMN
        alter_drop_pattern = (
            r"ALTER\s+TABLE\s+`?(\w+)`?\s+DROP\s+(?:COLUMN\s+)?`?(\w+)`?"
        )
        alter_drop_matches = re.findall(alter_drop_pattern, ddl_content, re.IGNORECASE)
        debug_log(f"ALTER TABLE DROP COLUMN 파싱 - 결과: {alter_drop_matches}")

        for table_name, column_name in alter_drop_matches:
            ddl_statements.append(
                {
                    "type": "ALTER_TABLE",
                    "table": table_name.lower(),
                    "alter_type": "DROP_COLUMN",
                    "column": column_name.lower(),
                }
            )
            debug_log(
                f"ALTER TABLE DROP COLUMN 구문 추가됨: {table_name}.{column_name}"
            )

        # 일반 ALTER TABLE (위의 패턴에 매치되지 않는 경우)
        # 주석을 완전히 제거한 후 실제 SQL 구문만 매치
        lines = ddl_content.split("\n")
        sql_lines = []
        for line in lines:
            # 주석 제거
            if "--" in line:
                line = line[: line.index("--")]
            line = line.strip()
            if line:
                sql_lines.append(line)

        clean_sql = " ".join(sql_lines)

        # ALTER TABLE로 시작하는 실제 구문만 매치
        alter_table_pattern = r"\bALTER\s+TABLE\s+`?(\w+)`?\s"
        alter_table_matches = re.findall(alter_table_pattern, clean_sql, re.IGNORECASE)

        # 이미 처리된 테이블들 제외 - 일반 ALTER TABLE 파싱 비활성화
        # (상세 파싱으로 충분하므로 일반 파싱은 건너뜀)
        debug_log("일반 ALTER TABLE 파싱 건너뜀 - 상세 파싱으로 충분")

        # CREATE INDEX 파싱 - 스키마 정보 포함 처리
        create_index_pattern = r"CREATE\s+(?:UNIQUE\s+)?INDEX\s+`?(\w+)`?\s+ON\s+`?(?:(\w+)\.)?(\w+)`?\s*\((.*?)\)"
        create_index_matches = re.findall(
            create_index_pattern, ddl_content, re.IGNORECASE
        )

        debug_log(f"CREATE INDEX 파싱 - 결과: {create_index_matches}")

        for index_name, schema, table_name, columns in create_index_matches:
            full_table_name = f"{schema}.{table_name}" if schema else table_name
            ddl_statements.append(
                {
                    "type": "CREATE_INDEX",
                    "table": full_table_name.lower(),
                    "index_name": index_name.lower(),
                    "columns": columns.strip(),
                }
            )
            debug_log(
                f"CREATE INDEX 구문 추가됨: {index_name} on {full_table_name}({columns})"
            )

        # DROP INDEX 파싱 - 스키마 정보 포함 처리
        drop_index_pattern = r"DROP\s+INDEX\s+`?(\w+)`?\s+ON\s+`?(?:(\w+)\.)?(\w+)`?"
        drop_index_matches = re.findall(drop_index_pattern, ddl_content, re.IGNORECASE)

        debug_log(f"DROP INDEX 파싱 - 패턴: {drop_index_pattern}")
        debug_log(f"DROP INDEX 파싱 - 결과: {drop_index_matches}")

        for index_name, schema, table_name in drop_index_matches:
            full_table_name = f"{schema}.{table_name}" if schema else table_name
            ddl_statements.append(
                {
                    "type": "DROP_INDEX",
                    "table": full_table_name.lower(),
                    "index_name": index_name.lower(),
                }
            )
            debug_log(f"DROP INDEX 구문 추가됨: {index_name} on {full_table_name}")

        debug_log(f"전체 파싱 결과: {len(ddl_statements)}개 구문")
        for i, stmt in enumerate(ddl_statements):
            debug_log(f"  [{i}] {stmt['type']}: {stmt}")

        return ddl_statements

    async def validate_create_index_with_debug(
        self, cursor, ddl_statement: Dict[str, Any], debug_log
    ) -> Dict[str, Any]:
        """CREATE INDEX 구문 검증 (디버그 버전)"""
        table_name = ddl_statement["table"]
        index_name = ddl_statement["index_name"]
        columns = ddl_statement["columns"]

        debug_log(
            f"CREATE INDEX 검증 시작: table={table_name}, index={index_name}, columns={columns}"
        )

        issues = []

        # 1. 테이블 존재 여부 확인 (스키마 정보 포함 처리)
        schema, actual_table = self.parse_table_name(table_name)
        if schema:
            cursor.execute(
                """
                SELECT COUNT(*) FROM information_schema.tables 
                WHERE table_schema = %s AND table_name = %s
            """,
                (schema, actual_table),
            )
        else:
            cursor.execute(
                """
                SELECT COUNT(*) FROM information_schema.tables 
                WHERE table_schema = DATABASE() AND table_name = %s
            """,
                (actual_table,),
            )

        table_exists = cursor.fetchone()[0] > 0
        debug_log(f"테이블 존재 여부: {table_exists}")

        if not table_exists:
            issues.append(
                f"테이블 '{table_name}'이 존재하지 않습니다. CREATE INDEX를 실행할 수 없습니다."
            )
            debug_log(f"오류: 테이블 '{table_name}' 존재하지 않음")
        else:
            # 2. 인덱스 이름 중복 확인
            cursor.execute(
                """
                SELECT COUNT(*) FROM information_schema.statistics 
                WHERE table_schema = DATABASE() AND table_name = %s AND index_name = %s
            """,
                (table_name, index_name),
            )

            index_exists = cursor.fetchone()[0] > 0
            debug_log(f"인덱스 '{index_name}' 존재 여부: {index_exists}")

            if index_exists:
                issues.append(f"인덱스 '{index_name}'이 이미 존재합니다.")
                debug_log(f"오류: 인덱스 '{index_name}' 이미 존재")

            # 3. 컬럼 존재 여부 확인
            cursor.execute(
                """
                SELECT column_name FROM information_schema.columns 
                WHERE table_schema = DATABASE() AND table_name = %s
            """,
                (table_name,),
            )

            existing_columns = {row[0].lower() for row in cursor.fetchall()}
            debug_log(f"기존 컬럼: {existing_columns}")

            # 인덱스 컬럼 파싱
            index_columns = [
                col.strip().strip("`").lower() for col in columns.split(",")
            ]
            debug_log(f"인덱스 대상 컬럼: {index_columns}")

            for col in index_columns:
                if col not in existing_columns:
                    issues.append(
                        f"컬럼 '{col}'이 테이블 '{table_name}'에 존재하지 않습니다."
                    )
                    debug_log(f"오류: 컬럼 '{col}' 존재하지 않음")

            # 4. 동일한 컬럼 구성의 인덱스 존재 여부 확인
            if not issues:  # 기본 검증을 통과한 경우에만
                cursor.execute(
                    """
                    SELECT index_name, GROUP_CONCAT(column_name ORDER BY seq_in_index) as columns
                    FROM information_schema.statistics 
                    WHERE table_schema = DATABASE() AND table_name = %s
                    GROUP BY index_name
                """,
                    (table_name,),
                )

                existing_index_columns = {}
                for row in cursor.fetchall():
                    existing_index_columns[row[0]] = row[1].lower()

                target_columns = ",".join(sorted(index_columns))
                debug_log(f"대상 컬럼 구성: {target_columns}")
                debug_log(f"기존 인덱스 컬럼 구성: {existing_index_columns}")

                for idx_name, idx_columns in existing_index_columns.items():
                    if idx_columns == target_columns:
                        issues.append(
                            f"동일한 컬럼 구성의 인덱스 '{idx_name}'이 이미 존재합니다."
                        )
                        debug_log(f"오류: 동일한 컬럼 구성의 인덱스 '{idx_name}' 존재")
                        break

        result = {
            "table": table_name,
            "ddl_type": "CREATE_INDEX",
            "valid": len(issues) == 0,
            "issues": issues,
            "details": {
                "table_exists": table_exists,
                "index_name": index_name,
                "columns": index_columns if "index_columns" in locals() else [],
            },
        }

        debug_log(
            f"CREATE INDEX 검증 완료: issues={len(issues)}, valid={len(issues) == 0}"
        )
        debug_log(f"최종 결과: {result}")

        return result

    async def validate_drop_index_with_debug(
        self, cursor, ddl_statement: Dict[str, Any], debug_log
    ) -> Dict[str, Any]:
        """DROP INDEX 구문 검증 (디버그 버전)"""
        table_name = ddl_statement["table"]
        index_name = ddl_statement["index_name"]

        debug_log(f"DROP INDEX 검증 시작: table={table_name}, index={index_name}")

        issues = []

        # 테이블 존재 여부 확인 (스키마 정보 포함 처리)
        schema, actual_table = self.parse_table_name(table_name)
        if schema:
            cursor.execute(
                """
                SELECT COUNT(*) FROM information_schema.tables 
                WHERE table_schema = %s AND table_name = %s
            """,
                (schema, actual_table),
            )
        else:
            cursor.execute(
                """
                SELECT COUNT(*) FROM information_schema.tables 
                WHERE table_schema = DATABASE() AND table_name = %s
            """,
                (actual_table,),
            )

        table_exists = cursor.fetchone()[0] > 0
        debug_log(f"테이블 '{table_name}' 존재 여부: {table_exists}")

        if not table_exists:
            issues.append(
                f"테이블 '{table_name}'이 존재하지 않습니다. DROP INDEX를 실행할 수 없습니다."
            )
            debug_log(f"오류: 테이블 '{table_name}' 존재하지 않음")
        else:
            # 인덱스 존재 여부 확인
            cursor.execute(
                """
                SELECT COUNT(*) FROM information_schema.statistics 
                WHERE table_schema = DATABASE() AND table_name = %s AND index_name = %s
            """,
                (table_name, index_name),
            )

            index_exists = cursor.fetchone()[0] > 0
            debug_log(f"인덱스 '{index_name}' 존재 여부: {index_exists}")

            if not index_exists:
                issues.append(
                    f"인덱스 '{index_name}'이 테이블 '{table_name}'에 존재하지 않습니다."
                )
                debug_log(f"오류: 인덱스 '{index_name}' 존재하지 않음")
            else:
                debug_log(f"인덱스 '{index_name}' 존재함 - DROP INDEX 가능")

        result = {
            "table": table_name,
            "ddl_type": "DROP_INDEX",
            "valid": len(issues) == 0,
            "issues": issues,
            "details": {
                "table_exists": table_exists,
                "index_name": index_name,
                "index_exists": index_exists if "index_exists" in locals() else False,
            },
        }

        debug_log(
            f"DROP INDEX 검증 완료: issues={len(issues)}, valid={len(issues) == 0}"
        )
        debug_log(f"최종 결과: {result}")

        return result

    async def validate_create_table_with_debug(
        self, cursor, ddl_statement: Dict[str, Any], debug_log
    ) -> Dict[str, Any]:
        """CREATE TABLE 구문 검증 (디버그 버전)"""
        table_name = ddl_statement["table"]

        debug_log(f"CREATE TABLE 검증 시작: table={table_name}")

        issues = []

        # 테이블 존재 여부 확인
        cursor.execute(
            """
            SELECT COUNT(*) FROM information_schema.tables 
            WHERE table_schema = DATABASE() AND table_name = %s
        """,
            (table_name,),
        )

        table_exists = cursor.fetchone()[0] > 0
        debug_log(f"테이블 존재 여부: {table_exists}")

        if table_exists:
            issues.append(
                f"테이블 '{table_name}'이 이미 존재합니다. CREATE TABLE은 실행할 수 없습니다."
            )
            debug_log(f"오류: 테이블 '{table_name}' 이미 존재")
        else:
            debug_log(f"테이블 '{table_name}' 존재하지 않음 - CREATE TABLE 가능")

        result = {
            "table": table_name,
            "ddl_type": "CREATE_TABLE",
            "valid": len(issues) == 0,
            "issues": issues,
            "details": {"table_exists": table_exists, "can_create": not table_exists},
        }

        debug_log(
            f"CREATE TABLE 검증 완료: issues={len(issues)}, valid={len(issues) == 0}"
        )
        debug_log(f"최종 결과: {result}")

        return result

    async def validate_alter_table_with_debug(
        self, cursor, ddl_statement: Dict[str, Any], debug_log
    ) -> Dict[str, Any]:
        """ALTER TABLE 구문 검증 (디버그 버전)"""
        table_name = ddl_statement["table"]

        debug_log(f"ALTER TABLE 검증 시작: table={table_name}")

        issues = []

        # 테이블 존재 여부 확인
        cursor.execute(
            """
            SELECT COUNT(*) FROM information_schema.tables 
            WHERE table_schema = DATABASE() AND table_name = %s
        """,
            (table_name,),
        )

        table_exists = cursor.fetchone()[0] > 0
        debug_log(f"테이블 존재 여부: {table_exists}")

        if not table_exists:
            issues.append(
                f"테이블 '{table_name}'이 존재하지 않습니다. ALTER TABLE을 실행할 수 없습니다."
            )
            debug_log(f"오류: 테이블 '{table_name}' 존재하지 않음")

            result = {
                "table": table_name,
                "ddl_type": "ALTER_TABLE",
                "valid": False,
                "issues": issues,
                "details": {"table_exists": table_exists},
            }
            return result

        # 테이블이 존재하는 경우, 현재 컬럼 정보 조회
        cursor.execute(
            """
            SELECT column_name, data_type, character_maximum_length, 
                   numeric_precision, numeric_scale, is_nullable, column_default
            FROM information_schema.columns 
            WHERE table_schema = DATABASE() AND table_name = %s
            ORDER BY ordinal_position
        """,
            (table_name,),
        )

        existing_columns = {}
        for col_row in cursor.fetchall():
            col_name = col_row[0].lower()
            existing_columns[col_name] = {
                "data_type": col_row[1].upper(),
                "max_length": col_row[2],
                "precision": col_row[3],
                "scale": col_row[4],
                "is_nullable": col_row[5],
                "default_value": col_row[6],
            }

        debug_log(f"기존 컬럼 목록: {list(existing_columns.keys())}")

        # ALTER 타입별 검증
        alter_type = ddl_statement.get("alter_type", "GENERAL")
        debug_log(f"ALTER 타입: {alter_type}")

        if alter_type == "ADD_COLUMN":
            column_name = ddl_statement.get("column")
            if column_name and column_name in existing_columns:
                issues.append(
                    f"컬럼 '{column_name}'이 이미 존재합니다. ADD COLUMN을 실행할 수 없습니다."
                )
                debug_log(f"오류: 컬럼 '{column_name}' 이미 존재")
            else:
                debug_log(f"컬럼 '{column_name}' 존재하지 않음 - ADD COLUMN 가능")

        elif alter_type == "DROP_COLUMN":
            column_name = ddl_statement.get("column")
            if column_name and column_name not in existing_columns:
                issues.append(
                    f"컬럼 '{column_name}'이 존재하지 않습니다. DROP COLUMN을 실행할 수 없습니다."
                )
                debug_log(f"오류: 컬럼 '{column_name}' 존재하지 않음")
            else:
                debug_log(f"컬럼 '{column_name}' 존재함 - DROP COLUMN 가능")

        elif alter_type == "MODIFY_COLUMN":
            column_name = ddl_statement.get("column")
            column_definition = ddl_statement.get("column_definition", "")

            if column_name and column_name not in existing_columns:
                issues.append(
                    f"컬럼 '{column_name}'이 존재하지 않습니다. MODIFY COLUMN을 실행할 수 없습니다."
                )
                debug_log(f"오류: 컬럼 '{column_name}' 존재하지 않음")
            else:
                debug_log(f"컬럼 '{column_name}' 존재함 - MODIFY COLUMN 가능")

                # 데이터 타입 호환성 검증
                if column_name in existing_columns:
                    existing_col = existing_columns[column_name]
                    compatibility_result = self.validate_column_type_compatibility(
                        existing_col, column_definition, debug_log
                    )
                    if not compatibility_result["compatible"]:
                        issues.extend(compatibility_result["issues"])

        result = {
            "table": table_name,
            "ddl_type": "ALTER_TABLE",
            "alter_type": alter_type,
            "valid": len(issues) == 0,
            "issues": issues,
            "details": {
                "table_exists": table_exists,
                "existing_columns": list(existing_columns.keys()),
                "column_count": len(existing_columns),
                "target_column": ddl_statement.get("column"),
            },
        }

        debug_log(
            f"ALTER TABLE 검증 완료: issues={len(issues)}, valid={len(issues) == 0}"
        )
        debug_log(f"최종 결과: {result}")

        return result

    async def validate_constraints(
        self, ddl_content: str, database_secret: str, use_ssh_tunnel: bool = True
    ) -> Dict[str, Any]:
        """제약조건 검증 - FK, 인덱스, 제약조건 확인"""
        try:
            # DDL에서 제약조건 정보 추출
            constraints_info = self.parse_ddl_constraints(ddl_content)

            connection, tunnel_used = await self.get_db_connection(
                database_secret, self.selected_database, use_ssh_tunnel
            )
            cursor = connection.cursor()

            constraint_results = []

            # 외래키 제약조건 검증
            if constraints_info.get("foreign_keys"):
                for fk in constraints_info["foreign_keys"]:
                    # 참조 테이블 존재 여부 확인
                    cursor.execute(
                        """
                        SELECT COUNT(*) FROM information_schema.tables 
                        WHERE table_schema = DATABASE() AND table_name = %s
                    """,
                        (fk["referenced_table"],),
                    )

                    ref_table_exists = cursor.fetchone()[0] > 0

                    if ref_table_exists:
                        # 참조 컬럼 존재 여부 확인
                        cursor.execute(
                            """
                            SELECT COUNT(*) FROM information_schema.columns 
                            WHERE table_schema = DATABASE() 
                            AND table_name = %s AND column_name = %s
                        """,
                            (fk["referenced_table"], fk["referenced_column"]),
                        )

                        ref_column_exists = cursor.fetchone()[0] > 0

                        constraint_results.append(
                            {
                                "type": "FOREIGN_KEY",
                                "constraint": f"{fk['column']} -> {fk['referenced_table']}.{fk['referenced_column']}",
                                "valid": ref_column_exists,
                                "issue": (
                                    None
                                    if ref_column_exists
                                    else f"참조 컬럼 '{fk['referenced_table']}.{fk['referenced_column']}'이 존재하지 않습니다."
                                ),
                            }
                        )
                    else:
                        constraint_results.append(
                            {
                                "type": "FOREIGN_KEY",
                                "constraint": f"{fk['column']} -> {fk['referenced_table']}.{fk['referenced_column']}",
                                "valid": False,
                                "issue": f"참조 테이블 '{fk['referenced_table']}'이 존재하지 않습니다.",
                            }
                        )

            cursor.close()
            connection.close()

            # SSH 터널 정리
            if tunnel_used:
                self.cleanup_ssh_tunnel()

            return {"success": True, "constraint_results": constraint_results}

        except Exception as e:
            if use_ssh_tunnel:
                self.cleanup_ssh_tunnel()
            return {"success": False, "error": f"제약조건 검증 오류: {str(e)}"}

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

    async def validate_create_table(
        self, cursor, ddl_statement: Dict[str, Any]
    ) -> Dict[str, Any]:
        """CREATE TABLE 구문 검증"""
        table_name = ddl_statement["table"]
        columns = ddl_statement["columns"]

        # 테이블 존재 여부 확인
        cursor.execute(
            """
            SELECT COUNT(*) FROM information_schema.tables 
            WHERE table_schema = DATABASE() AND table_name = %s
        """,
            (table_name,),
        )

        table_exists = cursor.fetchone()[0] > 0
        issues = []

        if table_exists:
            issues.append(f"테이블 '{table_name}'이 이미 존재합니다.")

        return {
            "table": table_name,
            "ddl_type": "CREATE_TABLE",
            "valid": not table_exists,
            "issues": issues,
            "details": {"table_exists": table_exists, "columns_count": len(columns)},
        }

    async def validate_alter_table(
        self, cursor, ddl_statement: Dict[str, Any]
    ) -> Dict[str, Any]:
        """ALTER TABLE 구문 검증"""
        table_name = ddl_statement["table"]
        alter_type = ddl_statement["alter_type"]

        # 테이블 존재 여부 확인
        cursor.execute(
            """
            SELECT COUNT(*) FROM information_schema.tables 
            WHERE table_schema = DATABASE() AND table_name = %s
        """,
            (table_name,),
        )

        table_exists = cursor.fetchone()[0] > 0
        issues = []

        if not table_exists:
            issues.append(f"테이블 '{table_name}'이 존재하지 않습니다.")
            return {
                "table": table_name,
                "ddl_type": "ALTER_TABLE",
                "alter_type": alter_type,
                "valid": False,
                "issues": issues,
            }

        # 현재 테이블의 컬럼 정보 조회
        cursor.execute(
            """
            SELECT column_name, data_type, character_maximum_length, numeric_precision, numeric_scale, is_nullable
            FROM information_schema.columns 
            WHERE table_schema = DATABASE() AND table_name = %s
        """,
            (table_name,),
        )

        existing_columns = {
            row[0].lower(): {
                "data_type": row[1].upper(),
                "max_length": row[2],
                "precision": row[3],
                "scale": row[4],
                "is_nullable": row[5],
            }
            for row in cursor.fetchall()
        }

        # ALTER 유형별 검증
        if alter_type == "ADD_COLUMN":
            column_name = ddl_statement["column"]
            if column_name in existing_columns:
                issues.append(f"컬럼 '{column_name}'이 이미 존재합니다.")

        elif alter_type == "DROP_COLUMN":
            column_name = ddl_statement["column"]
            if column_name not in existing_columns:
                issues.append(f"컬럼 '{column_name}'이 존재하지 않습니다.")

        elif alter_type == "MODIFY_COLUMN":
            column_name = ddl_statement["column"]
            new_definition = ddl_statement["column_definition"]

            if column_name not in existing_columns:
                issues.append(f"컬럼 '{column_name}'이 존재하지 않습니다.")
            else:
                # 데이터 타입 변경 가능성 검증
                validation_result = self.validate_column_type_change(
                    existing_columns[column_name], new_definition
                )
                if not validation_result["valid"]:
                    issues.extend(validation_result["issues"])

        elif alter_type == "CHANGE_COLUMN":
            old_column = ddl_statement["old_column"]
            new_column = ddl_statement["new_column"]
            new_definition = ddl_statement["column_definition"]

            if old_column not in existing_columns:
                issues.append(f"기존 컬럼 '{old_column}'이 존재하지 않습니다.")
            elif new_column != old_column and new_column in existing_columns:
                issues.append(f"새 컬럼명 '{new_column}'이 이미 존재합니다.")
            else:
                # 데이터 타입 변경 가능성 검증
                validation_result = self.validate_column_type_change(
                    existing_columns[old_column], new_definition
                )
                if not validation_result["valid"]:
                    issues.extend(validation_result["issues"])

        return {
            "table": table_name,
            "ddl_type": "ALTER_TABLE",
            "alter_type": alter_type,
            "valid": len(issues) == 0,
            "issues": issues,
            "details": {"existing_columns": list(existing_columns.keys())},
        }

    async def validate_create_index(
        self, cursor, ddl_statement: Dict[str, Any]
    ) -> Dict[str, Any]:
        """CREATE INDEX 구문 검증"""
        table_name = ddl_statement["table"]
        index_name = ddl_statement["index_name"]
        columns = ddl_statement["columns"]

        issues = []

        # 테이블 존재 여부 확인
        cursor.execute(
            """
            SELECT COUNT(*) FROM information_schema.tables 
            WHERE table_schema = DATABASE() AND table_name = %s
        """,
            (table_name,),
        )

        table_exists = cursor.fetchone()[0] > 0

        if not table_exists:
            issues.append(f"테이블 '{table_name}'이 존재하지 않습니다.")
        else:
            # 인덱스 존재 여부 확인
            cursor.execute(
                """
                SELECT COUNT(*) FROM information_schema.statistics 
                WHERE table_schema = DATABASE() AND table_name = %s AND index_name = %s
            """,
                (table_name, index_name),
            )

            index_exists = cursor.fetchone()[0] > 0

            if index_exists:
                issues.append(f"인덱스 '{index_name}'이 이미 존재합니다.")

            # 컬럼 존재 여부 확인
            cursor.execute(
                """
                SELECT column_name FROM information_schema.columns 
                WHERE table_schema = DATABASE() AND table_name = %s
            """,
                (table_name,),
            )

            existing_columns = {row[0].lower() for row in cursor.fetchall()}

            for column in columns:
                if column not in existing_columns:
                    issues.append(
                        f"컬럼 '{column}'이 테이블 '{table_name}'에 존재하지 않습니다."
                    )

        return {
            "table": table_name,
            "ddl_type": "CREATE_INDEX",
            "valid": len(issues) == 0,
            "issues": issues,
            "details": {"index_name": index_name, "columns": columns},
        }

    async def validate_drop_table_with_debug(
        self, cursor, ddl_statement: Dict[str, Any], debug_log
    ) -> Dict[str, Any]:
        """DROP TABLE 구문 검증 (디버그 버전)"""
        table_name = ddl_statement["table"]

        debug_log(f"DROP TABLE 검증 시작: table={table_name}")

        issues = []

        # 테이블 존재 여부 확인
        cursor.execute(
            """
            SELECT COUNT(*) FROM information_schema.tables 
            WHERE table_schema = DATABASE() AND table_name = %s
        """,
            (table_name,),
        )

        table_exists = cursor.fetchone()[0] > 0
        debug_log(f"테이블 '{table_name}' 존재 여부: {table_exists}")

        if not table_exists:
            issues.append(
                f"테이블 '{table_name}'이 존재하지 않습니다. DROP TABLE을 실행할 수 없습니다."
            )
            debug_log(f"오류: 테이블 '{table_name}' 존재하지 않음")
        else:
            debug_log(f"테이블 '{table_name}' 존재함 - DROP TABLE 가능")

        result = {
            "table": table_name,
            "ddl_type": "DROP_TABLE",
            "valid": len(issues) == 0,
            "issues": issues,
            "details": {"table_exists": table_exists, "can_drop": table_exists},
        }

        debug_log(
            f"DROP TABLE 검증 완료: issues={len(issues)}, valid={len(issues) == 0}"
        )
        debug_log(f"최종 결과: {result}")

        return result

    async def validate_drop_table(
        self, cursor, ddl_statement: Dict[str, Any]
    ) -> Dict[str, Any]:
        """DROP TABLE 구문 검증 (호환성 유지용)"""
        table_name = ddl_statement["table"]

        # 테이블 존재 여부 확인
        cursor.execute(
            """
            SELECT COUNT(*) FROM information_schema.tables 
            WHERE table_schema = DATABASE() AND table_name = %s
        """,
            (table_name,),
        )

        table_exists = cursor.fetchone()[0] > 0
        issues = []

        if not table_exists:
            issues.append(f"테이블 '{table_name}'이 존재하지 않습니다.")

        return {
            "table": table_name,
            "ddl_type": "DROP_TABLE",
            "valid": table_exists,
            "issues": issues,
            "details": {"table_exists": table_exists},
        }

    async def validate_drop_index(
        self, cursor, ddl_statement: Dict[str, Any]
    ) -> Dict[str, Any]:
        """DROP INDEX 구문 검증"""
        table_name = ddl_statement["table"]
        index_name = ddl_statement["index_name"]

        print(f"[DEBUG] DROP INDEX 검증 시작: table={table_name}, index={index_name}")

        issues = []

        # 테이블 존재 여부 확인
        cursor.execute(
            """
            SELECT COUNT(*) FROM information_schema.tables 
            WHERE table_schema = DATABASE() AND table_name = %s
        """,
            (table_name,),
        )

        table_exists = cursor.fetchone()[0] > 0
        print(f"[DEBUG] 테이블 '{table_name}' 존재 여부: {table_exists}")

        if not table_exists:
            issues.append(f"테이블 '{table_name}'이 존재하지 않습니다.")
            print(f"[DEBUG] 테이블 '{table_name}'이 존재하지 않음 - 이슈 추가")
        else:
            # 인덱스 존재 여부 확인
            cursor.execute(
                """
                SELECT COUNT(*) FROM information_schema.statistics 
                WHERE table_schema = DATABASE() AND table_name = %s AND index_name = %s
            """,
                (table_name, index_name),
            )

            index_exists = cursor.fetchone()[0] > 0
            print(f"[DEBUG] 인덱스 '{index_name}' 존재 여부: {index_exists}")

            if not index_exists:
                issues.append(
                    f"인덱스 '{index_name}'이 테이블 '{table_name}'에 존재하지 않습니다."
                )
                print(f"[DEBUG] 인덱스 '{index_name}'이 존재하지 않음 - 이슈 추가")

        result = {
            "table": table_name,
            "ddl_type": "DROP_INDEX",
            "valid": len(issues) == 0,
            "issues": issues,
            "details": {"index_name": index_name, "table_exists": table_exists},
        }

        print(
            f"[DEBUG] DROP INDEX 검증 완료: issues={len(issues)}, valid={len(issues) == 0}"
        )
        print(f"[DEBUG] 최종 결과: {result}")

        return result

    def validate_column_type_change(
        self, existing_column: Dict[str, Any], new_definition: str
    ) -> Dict[str, Any]:
        """컬럼 데이터 타입 변경 가능성 검증"""
        issues = []

        # 새로운 데이터 타입 파싱
        new_type_info = self.parse_data_type(new_definition.split()[0])
        existing_type = existing_column["data_type"]

        # 호환되지 않는 타입 변경 검사
        incompatible_changes = [
            # 문자열 -> 숫자
            (
                ["VARCHAR", "CHAR", "TEXT"],
                ["INT", "BIGINT", "DECIMAL", "FLOAT", "DOUBLE"],
            ),
            # 숫자 -> 문자열 (일반적으로 안전하지만 데이터 손실 가능)
            (["INT", "BIGINT", "DECIMAL", "FLOAT", "DOUBLE"], ["VARCHAR", "CHAR"]),
            # 날짜/시간 타입 변경
            (["DATE", "DATETIME", "TIMESTAMP"], ["INT", "VARCHAR", "CHAR"]),
        ]

        for from_types, to_types in incompatible_changes:
            if existing_type in from_types and new_type_info["type"] in to_types:
                issues.append(
                    f"데이터 타입을 {existing_type}에서 {new_type_info['type']}로 변경하는 것은 데이터 손실을 야기할 수 있습니다."
                )

        # 길이 축소 검사
        if existing_type in ["VARCHAR", "CHAR"] and new_type_info["type"] in [
            "VARCHAR",
            "CHAR",
        ]:
            existing_length = existing_column["max_length"]
            new_length = new_type_info["length"]

            if existing_length and new_length and new_length < existing_length:
                issues.append(
                    f"컬럼 길이를 {existing_length}에서 {new_length}로 축소하는 것은 데이터 손실을 야기할 수 있습니다."
                )

        # 정밀도 축소 검사 (DECIMAL)
        if existing_type == "DECIMAL" and new_type_info["type"] == "DECIMAL":
            existing_precision = existing_column["precision"]
            existing_scale = existing_column["scale"]
            new_precision = new_type_info["precision"]
            new_scale = new_type_info["scale"]

            if (
                existing_precision
                and new_precision
                and new_precision < existing_precision
            ) or (existing_scale and new_scale and new_scale < existing_scale):
                issues.append(
                    f"DECIMAL 정밀도를 ({existing_precision},{existing_scale})에서 ({new_precision},{new_scale})로 축소하는 것은 데이터 손실을 야기할 수 있습니다."
                )

        return {"valid": len(issues) == 0, "issues": issues}

    def parse_data_type(self, data_type_str: str) -> Dict[str, Any]:
        """데이터 타입 문자열을 파싱하여 타입과 길이 정보 추출"""
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

    async def generate_html_report(
        self,
        report_path: Path,
        filename: str,
        ddl_content: str,
        sql_type: str,
        status: str,
        summary: str,
        issues: List[str],
        db_connection_info: Optional[Dict],
        schema_validation: Optional[Dict],
        constraint_validation: Optional[Dict],
        database_secret: Optional[str],
        explain_result: Optional[Dict] = None,
        claude_analysis_result: Optional[str] = None,  # Claude 분석 결과 추가
        dml_column_issues: List[str] = None,  # DML 컬럼 이슈 추가
    ):
        """HTML 보고서 생성"""
        # dml_column_issues 초기화
        if dml_column_issues is None:
            dml_column_issues = []

        # 상세 디버그 로그 추가
        try:
            with open(
                "/Users/heungh/Documents/SA/05.Project/01.Infra-Assistant/01.DB-Assistant/output/html_debug.txt",
                "a",
                encoding="utf-8",
            ) as f:
                f.write(f"=== HTML 생성 함수 시작 ===\n")
                f.write(f"report_path: {report_path}\n")
                f.write(f"filename: {filename}\n")
                f.write(f"sql_type: {sql_type}\n")
                f.write(f"status: {status}\n")
                f.write(f"issues 개수: {len(issues)}\n")
                f.flush()
        except Exception as debug_e:
            logger.error(f"디버그 로그 작성 오류: {debug_e}")

        # 디버그 로그 추가
        try:
            with open(
                "/Users/heungh/Documents/SA/05.Project/01.Infra-Assistant/01.DB-Assistant/output/html_debug.txt",
                "a",
                encoding="utf-8",
            ) as f:
                f.write(
                    f"HTML 생성 함수 호출됨 - claude_analysis_result: {claude_analysis_result}\n"
                )
                f.flush()
        except:
            pass
        try:
            # Claude 검증 결과를 기반으로 상태 재평가
            claude_success = (
                claude_analysis_result
                and claude_analysis_result.startswith("검증 통과")
            )

            # 상태에 따른 색상 및 아이콘 - Claude 검증 결과 우선 반영
            if (
                claude_success
                and status == "FAIL"
                and not any(
                    "오류:" in issue or "실패" in issue or "존재하지 않" in issue
                    for issue in issues
                )
            ):
                # Claude가 성공이고 심각한 오류가 없으면 PASS로 변경
                status = "PASS"
                summary = "✅ 모든 검증을 통과했습니다."

            status_color = "#28a745" if status == "PASS" else "#dc3545"
            status_icon = "✅" if status == "PASS" else "❌"

            # DB 연결 정보 섹션 제거 (요청사항에 따라)
            db_info_section = ""

            # 발견된 문제 섹션 - Claude 검증과 기타 검증 분리
            claude_issues = []
            other_issues = []

            for issue in issues:
                if issue.startswith("Claude 검증:"):
                    claude_issues.append(issue[12:].strip())  # "Claude 검증:" 제거
                else:
                    other_issues.append(issue)

            # 기타 검증 문제 섹션 제거 (중복 방지)

            # Claude 검증 결과 내용 준비 (스키마 검증 결과 포함)
            schema_validation_summary = self.create_schema_validation_summary(
                issues, dml_column_issues
            )

            # Claude 검증과 스키마 검증을 통합한 내용 생성
            combined_validation_content = ""

            # Claude AI 검증 결과 추가 (스키마 검증 결과는 숨김)
            claude_content = (
                claude_analysis_result
                if claude_analysis_result
                else "Claude 검증 결과를 사용할 수 없습니다."
            )
            combined_validation_content += f"""
<div class="validation-subsection">
    <h4>📋 SQL검증결과</h4>
    <pre class="validation-text">{claude_content}</pre>
</div>
"""

            # 전체 문제가 없는 경우
            success_section = ""
            if not issues:
                success_section = """
                <div class="issues-section success" style="display: none;">
                    <h3>✅ 검증 결과</h3>
                    <p class="no-issues">모든 검증을 통과했습니다.</p>
                </div>
                """

            # HTML 보고서 내용
            report_content = f"""<!DOCTYPE html>
<html lang="ko">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>SQL 검증보고서 - {filename}</title>
    <style>
        body {{
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            line-height: 1.6;
            margin: 0;
            padding: 20px;
            background-color: #f5f5f5;
        }}
        .container {{
            max-width: 1200px;
            margin: 0 auto;
            background: white;
            border-radius: 10px;
            box-shadow: 0 0 20px rgba(0,0,0,0.1);
            overflow: hidden;
        }}
        .header {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 30px;
            text-align: center;
        }}
        .header h1 {{
            margin: 0;
            font-size: 2.5em;
            font-weight: 300;
        }}
        .status-badge {{
            display: inline-block;
            padding: 8px 16px;
            border-radius: 20px;
            font-weight: bold;
            margin-top: 10px;
            background-color: {status_color};
        }}
        .content {{
            padding: 30px;
        }}
        .summary-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }}
        .summary-item {{
            background: #f8f9fa;
            padding: 20px;
            border-radius: 8px;
            border-left: 4px solid #667eea;
        }}
        .summary-item h4 {{
            margin: 0 0 10px 0;
            color: #333;
        }}
        .summary-item p {{
            margin: 0;
            font-size: 1.1em;
            font-weight: 500;
        }}
        .info-section, .issues-section {{
            margin: 30px 0;
            padding: 20px;
            border-radius: 8px;
            border: 1px solid #e9ecef;
        }}
        .info-section h3, .issues-section h3 {{
            margin-top: 0;
            color: #495057;
            border-bottom: 2px solid #e9ecef;
            padding-bottom: 10px;
        }}
        .info-table {{
            width: 100%;
            border-collapse: collapse;
        }}
        .info-table td {{
            padding: 10px;
            border-bottom: 1px solid #e9ecef;
        }}
        .info-table td:first-child {{
            width: 150px;
            background: #f8f9fa;
        }}
        .issues-list {{
            margin: 10px 0;
            padding-left: 20px;
        }}
        .issues-list li {{
            margin: 5px 0;
        }}
        .status-success {{
            color: #28a745;
            font-weight: bold;
        }}
        .status-error {{
            color: #dc3545;
            font-weight: bold;
        }}
        .no-issues {{
            color: #28a745;
            font-weight: 500;
        }}
        .issues-section.success {{
            background: #d4edda;
            border-color: #28a745;
        }}
        .sql-code {{
            background: #f8f9fa;
            border: 1px solid #e9ecef;
            border-radius: 6px;
            padding: 20px;
            margin: 20px 0;
            font-family: 'Courier New', monospace;
            overflow-x: auto;
            overflow-y: auto;
            white-space: pre-wrap;
            word-wrap: break-word;
            max-height: 300px;
        }}
        .claude-section {{
            margin: 30px 0;
            padding: 25px;
            border-radius: 8px;
            border: 2px solid #667eea;
            background: #f8f9ff;
            box-shadow: 0 2px 10px rgba(102, 126, 234, 0.1);
        }}
        .claude-section h3 {{
            margin-top: 0;
            color: #495057;
            border-bottom: 2px solid #667eea;
            padding-bottom: 10px;
            font-size: 1.3em;
        }}
        .claude-result {{
            margin: 20px 0;
            padding: 0;
            background: white;
            border-radius: 8px;
            border: 1px solid #e9ecef;
            box-shadow: 0 1px 5px rgba(0,0,0,0.05);
        }}
        .claude-text {{
            background: white;
            border: 1px solid #e9ecef;
            border-radius: 4px;
            padding: 20px;
            margin: 0;
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            font-size: 14px;
            line-height: 1.8;
            white-space: pre-wrap;
            word-wrap: break-word;
            overflow-x: auto;
            max-height: 800px;  /* 400px에서 800px로 증가 */
            overflow-y: auto;
            min-height: 100px;
            resize: vertical;  /* 사용자가 수직으로 크기 조절 가능 */
        }}
        .validation-subsection {{
            margin: 15px 0;
            padding: 15px;
            border-radius: 6px;
            border-left: 4px solid #28a745;
            background: #f8fff9;
        }}
        .validation-subsection h4 {{
            margin: 0 0 10px 0;
            color: #495057;
            font-size: 1.1em;
        }}
        .validation-text {{
            background: white;
            border: 1px solid #e9ecef;
            border-radius: 4px;
            padding: 15px;
            margin: 0;
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            font-size: 13px;
            line-height: 1.6;
            white-space: pre-wrap;
            word-wrap: break-word;
            overflow-x: auto;
            max-height: 300px;
            overflow-y: auto;
        }}
        .success-text {{
            color: #28a745;
            font-weight: 500;
            margin: 0;
        }}
        .footer {{
            background: #f8f9fa;
            padding: 20px;
            text-align: center;
            color: #6c757d;
            border-top: 1px solid #e9ecef;
        }}
        @media (max-width: 768px) {{
            .summary-grid {{
                grid-template-columns: 1fr;
            }}
            .container {{
                margin: 10px;
            }}
            body {{
                padding: 10px;
            }}
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>{status_icon} SQL 검증보고서</h1>
            <div class="status-badge">{status}</div>
        </div>
        
        <div class="content">
            <div class="summary-grid">
                <div class="summary-item">
                    <h4>📄 파일명</h4>
                    <p>{filename}</p>
                </div>
                <div class="summary-item">
                    <h4>🕒 검증 일시</h4>
                    <p>{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
                </div>
                <div class="summary-item">
                    <h4>🔧 DDL 타입</h4>
                    <p>{sql_type}</p>
                </div>
                <div class="summary-item">
                    <h4>🗄️ 데이터베이스</h4>
                    <p>{database_secret or 'N/A'}</p>
                </div>
            </div>
            
            {db_info_section}
            
            <div class="info-section">
                <h3>📝 원본 DDL</h3>
                <div class="sql-code">{ddl_content}</div>
            </div>
            
            <div class="claude-section">
                <h3>🔍 통합 검증 결과 (스키마 + 쿼리성능)</h3>
                <div class="claude-result">
                    {combined_validation_content}
                </div>
            </div>
            
        </div>
        
        <div class="footer">
            <p>Generated by DB Assistant MCP Server</p>
            <p>Report generated at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        </div>
    </div>
</body>
</html>"""

            with open(report_path, "w", encoding="utf-8") as f:
                f.write(report_content)

            # 검증 결과 섹션 제거
            try:
                with open(report_path, "r", encoding="utf-8") as f:
                    html_content = f.read()
                
                # 검증 결과 섹션 제거 - 더 정확한 방법
                lines = html_content.split('\n')
                new_lines = []
                i = 0
                
                while i < len(lines):
                    line = lines[i]
                    
                    # 검증 결과 섹션 시작 감지
                    if '<div class="info-section" style="display: none;">' in line:
                        # 다음 라인들을 확인하여 검증 결과 섹션인지 판단
                        if i + 1 < len(lines) and '📊 검증 결과' in lines[i + 1]:
                            # 검증 결과 섹션이므로 </div>까지 스킵
                            i += 1  # 현재 div 라인 스킵
                            while i < len(lines):
                                if '</div>' in lines[i]:
                                    i += 1  # </div> 라인도 스킵
                                    break
                                i += 1
                            continue
                    
                    new_lines.append(line)
                    i += 1
                
                html_content = '\n'.join(new_lines)
                
                with open(report_path, "w", encoding="utf-8") as f:
                    f.write(html_content)
            except Exception as e:
                pass

            # 파일 생성 확인 디버그
            try:
                with open(
                    "/Users/heungh/Documents/SA/05.Project/01.Infra-Assistant/01.DB-Assistant/output/html_debug.txt",
                    "a",
                    encoding="utf-8",
                ) as f:
                    f.write(f"HTML 파일 생성 완료: {report_path}\n")
                    f.write(f"파일 존재 여부: {report_path.exists()}\n")
                    f.flush()
            except:
                pass

        except Exception as e:
            logger.error(f"HTML 보고서 생성 오류: {e}")
            # 상세 오류 정보를 디버그 파일에 기록
            try:
                with open(
                    "/Users/heungh/Documents/SA/05.Project/01.Infra-Assistant/01.DB-Assistant/output/html_debug.txt",
                    "a",
                    encoding="utf-8",
                ) as f:
                    import traceback

                    f.write(f"HTML 생성 오류: {e}\n")
                    f.write(f"상세 오류: {traceback.format_exc()}\n")
                    f.flush()
            except:
                pass

    async def generate_consolidated_html_report(
        self, validation_results: List[Dict], database_secret: str
    ) -> str:
        """여러 SQL 파일의 통합 HTML 보고서 생성"""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            report_filename = f"consolidated_validation_report_{timestamp}.html"
            report_path = OUTPUT_DIR / report_filename

            # 전체 통계 계산
            total_files = len(validation_results)
            passed_files = sum(1 for r in validation_results if r["status"] == "PASS")
            failed_files = total_files - passed_files

            # 파일별 결과 섹션 생성
            file_sections = ""
            for i, result in enumerate(validation_results, 1):
                status_icon = "✅" if result["status"] == "PASS" else "❌"
                status_class = "success" if result["status"] == "PASS" else "error"

                issues_html = ""
                if result["issues"]:
                    issues_html = "<ul class='issues-list'>"
                    for issue in result["issues"]:
                        issues_html += f"<li>{issue}</li>"
                    issues_html += "</ul>"
                else:
                    issues_html = "<p class='no-issues'>문제가 발견되지 않았습니다.</p>"

                # 개별 파일 검증에서만 상세 보고서가 생성되므로 링크 없이 파일명만 표시
                filename_display = result["filename"]

                file_sections += f"""
                <div class="file-section {status_class}">
                    <h3>{status_icon} {i}. {filename_display}</h3>
                    <div class="file-details">
                        <div class="file-info">
                            <span><strong>DDL 타입:</strong> {result['ddl_type']}</span>
                            <span><strong>상태:</strong> {result['status']}</span>
                            <span><strong>문제 수:</strong> {len(result['issues'])}개</span>
                        </div>
                        <div class="sql-code">
{result['ddl_content']}
                        </div>
                        {f'<div class="issues-section">{issues_html}</div>' if result['issues'] else ''}
                    </div>
                </div>
                """

            # HTML 보고서 내용
            report_content = f"""<!DOCTYPE html>
<html lang="ko">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>통합 SQL 검증보고서</title>
    <style>
        body {{
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            line-height: 1.6;
            margin: 0;
            padding: 20px;
            background-color: #f5f5f5;
        }}
        .container {{
            max-width: 1200px;
            margin: 0 auto;
            background: white;
            border-radius: 10px;
            box-shadow: 0 0 20px rgba(0,0,0,0.1);
            overflow: hidden;
        }}
        .header {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 30px;
            text-align: center;
        }}
        .header h1 {{
            margin: 0;
            font-size: 2.5em;
            font-weight: 300;
        }}
        .summary-stats {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
            gap: 20px;
            margin: 30px;
        }}
        .stat-item {{
            background: #f8f9fa;
            padding: 20px;
            border-radius: 8px;
            text-align: center;
            border-left: 4px solid #667eea;
        }}
        .stat-number {{
            font-size: 2em;
            font-weight: bold;
            color: #333;
        }}
        .stat-label {{
            color: #666;
            margin-top: 5px;
        }}
        .file-section {{
            margin: 20px 30px;
            border: 1px solid #e9ecef;
            border-radius: 8px;
            overflow: hidden;
        }}
        .file-section.success {{
            border-left: 4px solid #28a745;
        }}
        .file-section.error {{
            border-left: 4px solid #dc3545;
        }}
        .file-section h3 {{
            margin: 0;
            padding: 15px 20px;
            background: #f8f9fa;
            border-bottom: 1px solid #e9ecef;
        }}
        .file-section h3 a {{
            color: #495057;
            text-decoration: none;
        }}
        .file-section h3 a:hover {{
            color: #007bff;
            text-decoration: underline;
        }}
        .file-details {{
            padding: 20px;
        }}
        .file-info {{
            display: flex;
            gap: 20px;
            margin-bottom: 15px;
            flex-wrap: wrap;
        }}
        .file-info span {{
            background: #e9ecef;
            padding: 5px 10px;
            border-radius: 4px;
            font-size: 0.9em;
        }}
        .sql-code {{
            background: #f8f9fa;
            border: 1px solid #e9ecef;
            border-radius: 6px;
            padding: 15px;
            margin: 15px 0;
            font-family: 'Courier New', monospace;
            overflow-x: auto;
            overflow-y: auto;
            white-space: pre-wrap;
            word-wrap: break-word;
            max-height: 300px;
            font-size: 0.9em;
        }}
        .issues-section {{
            margin-top: 15px;
            padding: 15px;
            background: #fff5f5;
            border: 1px solid #fed7d7;
            border-radius: 6px;
        }}
        .issues-section h4 {{
            margin: 0 0 10px 0;
            color: #c53030;
        }}
        .issues-list {{
            margin: 0;
            padding-left: 20px;
        }}
        .issues-list li {{
            margin: 5px 0;
            color: #c53030;
        }}
        .no-issues {{
            color: #38a169;
            margin: 0;
            font-weight: 500;
        }}
        .footer {{
            background: #f8f9fa;
            padding: 20px;
            text-align: center;
            color: #6c757d;
            border-top: 1px solid #e9ecef;
        }}
        @media (max-width: 768px) {{
            .summary-stats {{
                grid-template-columns: 1fr;
                margin: 20px;
            }}
            .file-section {{
                margin: 20px 15px;
            }}
            .file-info {{
                flex-direction: column;
                gap: 10px;
            }}
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>📊 통합 SQL 검증보고서</h1>
            <p>데이터베이스: {database_secret}</p>
            <p>검증 일시: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}</p>
        </div>
        
        <div class="summary-stats">
            <div class="stat-item">
                <div class="stat-number">{total_files}</div>
                <div class="stat-label">총 파일 수</div>
            </div>
            <div class="stat-item">
                <div class="stat-number" style="color: #28a745;">{passed_files}</div>
                <div class="stat-label">통과</div>
            </div>
            <div class="stat-item">
                <div class="stat-number" style="color: #dc3545;">{failed_files}</div>
                <div class="stat-label">실패</div>
            </div>
            <div class="stat-item">
                <div class="stat-number">{round(passed_files/total_files*100) if total_files > 0 else 0}%</div>
                <div class="stat-label">성공률</div>
            </div>
        </div>
        
        {file_sections}
        
        <div class="footer">
            <p>Generated by DB Assistant MCP Server</p>
            <p>Report generated at {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}</p>
        </div>
    </div>
</body>
</html>"""

            # 파일 저장
            with open(report_path, "w", encoding="utf-8") as f:
                f.write(report_content)

            return str(report_path)

        except Exception as e:
            logger.error(f"통합 HTML 보고서 생성 오류: {e}")
            return f"보고서 생성 실패: {str(e)}"

    async def generate_consolidated_report(
        self, keyword: Optional[str] = None, report_files: Optional[List[str]] = None, 
        date_filter: Optional[str] = None, latest_count: Optional[int] = None
    ) -> str:
        """기존 HTML 보고서들을 기반으로 통합 보고서 생성"""
        try:
            # 보고서 파일 수집
            if report_files:
                # 특정 파일들 지정된 경우
                html_files = [OUTPUT_DIR / f for f in report_files if (OUTPUT_DIR / f).exists()]
            else:
                # validation_report로 시작하는 HTML 파일만 (debug_log 제외)
                html_files = list(OUTPUT_DIR.glob("validation_report_*.html"))
                
                # 키워드 필터링
                if keyword:
                    html_files = [f for f in html_files if keyword in f.name]
                
                # 날짜 필터링 (YYYYMMDD 형식)
                if date_filter:
                    html_files = [f for f in html_files if date_filter in f.name]
                
                # 최신 파일 개수 제한
                if latest_count:
                    # 파일명의 타임스탬프로 정렬 (최신순)
                    html_files.sort(key=lambda x: x.stat().st_mtime, reverse=True)
                    html_files = html_files[:latest_count]
            
            if not html_files:
                return f"조건에 맞는 HTML 보고서를 찾을 수 없습니다. (키워드: {keyword}, 날짜: {date_filter}, 개수: {latest_count})"

            # 각 보고서에서 정보 추출
            report_data = []
            for html_file in html_files:
                try:
                    content = html_file.read_text(encoding='utf-8')
                    
                    # 파일명에서 원본 SQL 파일명 추출
                    sql_filename = html_file.name.replace('validation_report_', '').replace('.html', '')
                    if '_2025' in sql_filename:
                        sql_filename = sql_filename.split('_2025')[0] + '.sql'
                    
                    # 검증 결과 추출 - HTML 구조 기반으로 정확히 추출
                    if 'status-badge">PASS' in content or '✅ SQL 검증보고서' in content:
                        status = 'PASS'
                        status_icon = '✅'
                    elif 'status-badge">FAIL' in content or '❌ SQL 검증보고서' in content:
                        status = 'FAIL'
                        status_icon = '❌'
                    else:
                        # 기본값으로 FAIL 처리
                        status = 'FAIL'
                        status_icon = '❌'
                    
                    # SQL 내용 일부 추출 (HTML 파일에서만)
                    sql_preview = "SQL 내용을 찾을 수 없습니다"
                    if 'sql-code' in content:
                        import re
                        sql_match = re.search(r'<div class="sql-code"[^>]*>(.*?)</div>', content, re.DOTALL)
                        if sql_match:
                            sql_preview = sql_match.group(1).strip()[:100] + "..."
                    
                    # 요약 정보 추출
                    summary = "상세 내용은 개별 보고서 참조"
                    if 'Claude AI 분석' in content:
                        summary = "AI 분석 완료"
                    
                    report_data.append({
                        'filename': sql_filename,
                        'html_file': html_file.name,
                        'status': status,
                        'status_icon': status_icon,
                        'sql_preview': sql_preview,
                        'summary': summary
                    })
                    
                except Exception as e:
                    logger.error(f"보고서 파싱 오류 {html_file}: {e}")
                    continue

            if not report_data:
                return "유효한 보고서 데이터를 찾을 수 없습니다."

            # 통합 보고서 HTML 생성
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            report_filename = f"consolidated_report_{timestamp}.html"
            report_path = OUTPUT_DIR / report_filename

            # 통계 계산
            total_reports = len(report_data)
            passed_reports = sum(1 for r in report_data if r['status'] == 'PASS')
            failed_reports = total_reports - passed_reports

            # 테이블 행 생성
            table_rows = ""
            for i, data in enumerate(report_data, 1):
                table_rows += f"""
                <tr onclick="openReport('{data['html_file']}')" style="cursor: pointer;">
                    <td>{i}</td>
                    <td>{data['status_icon']} {data['filename']}</td>
                    <td><code>{data['sql_preview']}</code></td>
                    <td><span class="status-badge {data['status'].lower()}">{data['status']}</span></td>
                    <td>{data['summary']}</td>
                </tr>
                """

            html_content = f"""<!DOCTYPE html>
<html lang="ko">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>통합 검증 보고서</title>
    <style>
        body {{ font-family: 'Segoe UI', sans-serif; margin: 0; padding: 20px; background: #f5f5f5; }}
        .container {{ max-width: 1400px; margin: 0 auto; background: white; border-radius: 10px; box-shadow: 0 0 20px rgba(0,0,0,0.1); }}
        .header {{ background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 30px; text-align: center; }}
        .header h1 {{ margin: 0; font-size: 2.5em; font-weight: 300; }}
        .stats {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 20px; margin: 30px; }}
        .stat-card {{ background: #f8f9fa; padding: 20px; border-radius: 8px; text-align: center; border-left: 4px solid #667eea; }}
        .stat-number {{ font-size: 2em; font-weight: bold; color: #333; }}
        .stat-label {{ color: #666; margin-top: 5px; }}
        .table-container {{ margin: 30px; overflow-x: auto; }}
        table {{ width: 100%; border-collapse: collapse; background: white; }}
        th, td {{ padding: 12px; text-align: left; border-bottom: 1px solid #ddd; }}
        th {{ background: #f8f9fa; font-weight: 600; }}
        tr:hover {{ background: #f8f9fa; }}
        .status-badge {{ padding: 4px 8px; border-radius: 4px; font-size: 0.8em; font-weight: bold; }}
        .status-badge.pass {{ background: #d4edda; color: #155724; }}
        .status-badge.fail {{ background: #f8d7da; color: #721c24; }}
        code {{ background: #f1f3f4; padding: 2px 4px; border-radius: 3px; font-size: 0.9em; }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>📊 통합 검증 보고서</h1>
            <p>생성일시: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        </div>
        
        <div class="stats">
            <div class="stat-card">
                <div class="stat-number">{total_reports}</div>
                <div class="stat-label">총 보고서</div>
            </div>
            <div class="stat-card">
                <div class="stat-number">{passed_reports}</div>
                <div class="stat-label">검증 통과</div>
            </div>
            <div class="stat-card">
                <div class="stat-number">{failed_reports}</div>
                <div class="stat-label">검증 실패</div>
            </div>
            <div class="stat-card">
                <div class="stat-number">{round(passed_reports/total_reports*100) if total_reports > 0 else 0}%</div>
                <div class="stat-label">성공률</div>
            </div>
        </div>
        
        <div class="table-container">
            <h2>📋 보고서 목록 (클릭하여 상세 보기)</h2>
            <table>
                <thead>
                    <tr>
                        <th>#</th>
                        <th>파일명</th>
                        <th>SQL 미리보기</th>
                        <th>검증 결과</th>
                        <th>요약</th>
                    </tr>
                </thead>
                <tbody>
                    {table_rows}
                </tbody>
            </table>
        </div>
    </div>
    
    <script>
        function openReport(filename) {{
            window.open(filename, '_blank');
        }}
    </script>
</body>
</html>"""

            report_path.write_text(html_content, encoding='utf-8')
            
            return f"""📊 통합 보고서 생성 완료

📈 요약:
• 총 보고서: {total_reports}개
• 검증 통과: {passed_reports}개 ({round(passed_reports/total_reports*100)}%)
• 검증 실패: {failed_reports}개 ({round(failed_reports/total_reports*100)}%)

📄 통합 보고서: {report_path}

💡 사용법: 테이블의 각 행을 클릭하면 해당 상세 보고서가 새 창에서 열립니다."""

        except Exception as e:
            return f"통합 보고서 생성 실패: {str(e)}"

    async def copy_sql_file(
        self, source_path: str, target_name: Optional[str] = None
    ) -> str:
        """SQL 파일을 sql 디렉토리로 복사"""
        try:
            source = Path(source_path)
            if not source.exists():
                return f"소스 파일을 찾을 수 없습니다: {source_path}"

            if not source.suffix.lower() == ".sql":
                return f"SQL 파일이 아닙니다: {source_path}"

            # 대상 파일명 결정
            if target_name:
                if not target_name.endswith(".sql"):
                    target_name += ".sql"
                target_path = SQL_DIR / target_name
            else:
                target_path = SQL_DIR / source.name

            # 파일 복사
            import shutil

            shutil.copy2(source, target_path)

            return f"✅ SQL 파일이 복사되었습니다: {source.name} -> {target_path.name}"

        except Exception as e:
            return f"SQL 파일 복사 실패: {str(e)}"

    # === 분석 관련 메서드 ===

    def setup_cloudwatch_client(self, region_name: str = "us-east-1"):
        """CloudWatch 클라이언트 설정"""
        try:
            self.cloudwatch = boto3.client("cloudwatch", region_name=region_name)
            return True
        except Exception as e:
            logger.error(f"CloudWatch 클라이언트 설정 실패: {e}")
            return False

    async def collect_db_metrics(
        self,
        db_instance_identifier: str,
        hours: int = 24,
        metrics: Optional[List[str]] = None,
        region: str = "us-east-1",
    ) -> str:
        """CloudWatch에서 데이터베이스 메트릭 수집"""
        if not ANALYSIS_AVAILABLE:
            return "❌ 분석 라이브러리가 설치되지 않았습니다. pip install pandas numpy matplotlib scikit-learn을 실행해주세요."

        try:
            if not self.setup_cloudwatch_client(region):
                return "CloudWatch 클라이언트 설정에 실패했습니다."

            if not metrics:
                metrics = self.default_metrics

            # 시간 범위 설정
            end_time = datetime.utcnow()
            start_time = end_time - timedelta(hours=hours)

            # 데이터를 저장할 리스트
            data = []

            # 각 메트릭에 대해 데이터 수집
            for metric in metrics:
                try:
                    response = self.cloudwatch.get_metric_statistics(
                        Namespace="AWS/RDS",
                        MetricName=metric,
                        Dimensions=[
                            {
                                "Name": "DBInstanceIdentifier",
                                "Value": db_instance_identifier,
                            },
                        ],
                        StartTime=start_time,
                        EndTime=end_time,
                        Period=300,  # 5분 간격
                        Statistics=["Average"],
                    )

                    # 응답에서 데이터 추출
                    for point in response["Datapoints"]:
                        data.append(
                            {
                                "Timestamp": point["Timestamp"].replace(tzinfo=None),
                                "Metric": metric,
                                "Value": point["Average"],
                            }
                        )
                except Exception as e:
                    logger.error(f"메트릭 {metric} 수집 실패: {str(e)}")

            if not data:
                return "수집된 데이터가 없습니다."

            # 데이터프레임 생성
            df = pd.DataFrame(data)
            df = df.sort_values("Timestamp")

            # 피벗 테이블 생성
            pivot_df = df.pivot(index="Timestamp", columns="Metric", values="Value")

            # CSV 파일로 저장
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            csv_file = (
                DATA_DIR / f"database_metrics_{db_instance_identifier}_{timestamp}.csv"
            )
            pivot_df.to_csv(csv_file)

            return f"✅ 메트릭 수집 완료\n📊 수집된 메트릭: {len(metrics)}개\n📈 데이터 포인트: {len(data)}개\n💾 저장 위치: {csv_file}"

        except Exception as e:
            return f"메트릭 수집 중 오류 발생: {str(e)}"

    async def analyze_metric_correlation(
        self, csv_file: str, target_metric: str = "CPUUtilization", top_n: int = 10
    ) -> str:
        """메트릭 간 상관관계 분석"""
        if not ANALYSIS_AVAILABLE:
            return "❌ 분석 라이브러리가 설치되지 않았습니다."

        try:
            # CSV 파일 경로 처리
            if not csv_file.startswith("/"):
                csv_path = DATA_DIR / csv_file
            else:
                csv_path = Path(csv_file)

            if not csv_path.exists():
                return f"CSV 파일을 찾을 수 없습니다: {csv_path}"

            # 데이터 읽기
            df = pd.read_csv(csv_path, index_col="Timestamp", parse_dates=True)
            df = df.dropna()

            if target_metric not in df.columns:
                return f"타겟 메트릭 '{target_metric}'이 데이터에 없습니다.\n사용 가능한 메트릭: {list(df.columns)}"

            # 상관 분석
            correlation_matrix = df.corr()
            target_correlations = correlation_matrix[target_metric].abs()
            target_correlations = target_correlations.drop(
                target_metric, errors="ignore"
            )
            top_correlations = target_correlations.nlargest(top_n)

            # 결과 문자열 생성
            result = f"📊 {target_metric}과 상관관계가 높은 상위 {top_n}개 메트릭:\n\n"
            for metric, correlation in top_correlations.items():
                result += f"• {metric}: {correlation:.4f}\n"

            # 시각화
            plt.figure(figsize=(12, 6))
            top_correlations.plot(kind="bar")
            plt.title(f"Top {top_n} Metrics Correlated with {target_metric}")
            plt.xlabel("Metrics")
            plt.ylabel("Correlation Coefficient")
            plt.xticks(rotation=45, ha="right")
            plt.tight_layout()

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            graph_file = (
                OUTPUT_DIR / f"correlation_analysis_{target_metric}_{timestamp}.png"
            )
            plt.savefig(graph_file, dpi=300, bbox_inches="tight")
            plt.close()

            result += f"\n📈 상관관계 그래프가 저장되었습니다: {graph_file}"
            return result

        except Exception as e:
            return f"상관관계 분석 중 오류 발생: {str(e)}"

    async def detect_metric_outliers(
        self, csv_file: str, std_threshold: float = 2.0
    ) -> str:
        """아웃라이어 탐지"""
        if not ANALYSIS_AVAILABLE:
            return "❌ 분석 라이브러리가 설치되지 않았습니다."

        try:
            # CSV 파일 경로 처리
            if not csv_file.startswith("/"):
                csv_path = DATA_DIR / csv_file
            else:
                csv_path = Path(csv_file)

            if not csv_path.exists():
                return f"CSV 파일을 찾을 수 없습니다: {csv_path}"

            # 데이터 읽기
            df = pd.read_csv(csv_path, index_col="Timestamp", parse_dates=True)
            df = df.dropna()

            result = f"🚨 아웃라이어 탐지 결과 (임계값: ±{std_threshold}σ):\n\n"

            outlier_summary = []

            # 각 메트릭에 대해 아웃라이어 탐지
            for column in df.columns:
                series = df[column]
                mean = series.mean()
                std = series.std()
                lower_bound = mean - std_threshold * std
                upper_bound = mean + std_threshold * std

                outliers = series[(series < lower_bound) | (series > upper_bound)]

                if not outliers.empty:
                    result += f"⚠️ {column} 메트릭의 아웃라이어 ({len(outliers)}개):\n"
                    result += f"   정상 범위: {lower_bound:.2f} ~ {upper_bound:.2f}\n"

                    # 최대 5개까지만 표시
                    for i, (timestamp, value) in enumerate(outliers.items()):
                        if i >= 5:
                            result += f"   ... 및 {len(outliers) - 5}개 더\n"
                            break
                        result += f"   • {timestamp}: {value:.2f}\n"
                    result += "\n"

                    outlier_summary.append(
                        {
                            "metric": column,
                            "count": len(outliers),
                            "percentage": (len(outliers) / len(series)) * 100,
                        }
                    )
                else:
                    result += f"✅ {column}: 아웃라이어 없음\n"

            # 요약 정보
            if outlier_summary:
                result += "\n📋 아웃라이어 요약:\n"
                for summary in outlier_summary:
                    result += f"• {summary['metric']}: {summary['count']}개 ({summary['percentage']:.1f}%)\n"

            return result

        except Exception as e:
            return f"아웃라이어 탐지 중 오류 발생: {str(e)}"

    async def perform_regression_analysis(
        self,
        csv_file: str,
        predictor_metric: str,
        target_metric: str = "CPUUtilization",
    ) -> str:
        """회귀 분석 수행"""
        if not ANALYSIS_AVAILABLE:
            return "❌ 분석 라이브러리가 설치되지 않았습니다."

        try:
            # CSV 파일 경로 처리
            if not csv_file.startswith("/"):
                csv_path = DATA_DIR / csv_file
            else:
                csv_path = Path(csv_file)

            if not csv_path.exists():
                return f"CSV 파일을 찾을 수 없습니다: {csv_path}"

            # 데이터 읽기
            df = pd.read_csv(csv_path, index_col="Timestamp", parse_dates=True)

            # 필요한 메트릭 확인
            if predictor_metric not in df.columns or target_metric not in df.columns:
                return f"필요한 메트릭이 데이터에 없습니다.\n사용 가능한 메트릭: {list(df.columns)}"

            # 데이터 준비
            X = df[predictor_metric].values.reshape(-1, 1)
            y = df[target_metric].values

            # NaN 값 처리
            imputer = SimpleImputer(strategy="mean")
            X = imputer.fit_transform(X)
            y = imputer.fit_transform(y.reshape(-1, 1)).ravel()

            # 데이터 분할
            X_train, X_test, y_train, y_test = train_test_split(
                X, y, test_size=0.2, random_state=42
            )

            # 다항 회귀 모델 생성 (2차)
            poly_features = PolynomialFeatures(degree=2, include_bias=False)
            X_poly_train = poly_features.fit_transform(X_train)
            X_poly_test = poly_features.transform(X_test)

            # 모델 학습
            model = LinearRegression()
            model.fit(X_poly_train, y_train)

            # 예측
            y_pred = model.predict(X_poly_test)

            # 모델 평가
            mse = mean_squared_error(y_test, y_pred)
            r2 = r2_score(y_test, y_pred)

            # 계수 출력
            coefficients = model.coef_
            intercept = model.intercept_

            result = f"📈 회귀 분석 결과 ({predictor_metric} → {target_metric}):\n\n"
            result += f"📊 모델 성능:\n"
            result += f"• Mean Squared Error: {mse:.4f}\n"
            result += f"• R-squared Score: {r2:.4f}\n\n"

            result += f"🔢 다항 회귀 모델 (2차):\n"
            result += f"y = {coefficients[1]:.4f}x² + {coefficients[0]:.4f}x + {intercept:.4f}\n\n"

            # 해석 추가
            result += "해석:\n"
            if r2 > 0.7:
                result += f"• 모델 설명력: {r2*100:.1f}% (높은 예측 정확도)\n"
            elif r2 > 0.5:
                result += f"• 모델 설명력: {r2*100:.1f}% (중간 예측 정확도)\n"
            else:
                result += f"• 모델 설명력: {r2*100:.1f}% (낮은 예측 정확도)\n"

            # 그래프 그리기
            plt.figure(figsize=(12, 8))

            # 산점도
            plt.subplot(2, 1, 1)
            plt.scatter(X_test, y_test, color="blue", alpha=0.6, label="실제 데이터")

            # 예측 곡선을 위한 정렬된 데이터
            X_plot = np.linspace(X_test.min(), X_test.max(), 100).reshape(-1, 1)
            X_plot_poly = poly_features.transform(X_plot)
            y_plot_pred = model.predict(X_plot_poly)

            plt.plot(X_plot, y_plot_pred, color="red", linewidth=2, label="예측 모델")
            plt.title(f"{predictor_metric} vs {target_metric} 회귀 분석")
            plt.xlabel(predictor_metric)
            plt.ylabel(target_metric)
            plt.legend()
            plt.grid(True, alpha=0.3)

            # 잔차 플롯
            plt.subplot(2, 1, 2)
            residuals = y_test - y_pred
            plt.scatter(y_pred, residuals, color="green", alpha=0.6)
            plt.axhline(y=0, color="red", linestyle="--")
            plt.title("잔차 플롯 (Residual Plot)")
            plt.xlabel("예측값")
            plt.ylabel("잔차")
            plt.grid(True, alpha=0.3)

            plt.tight_layout()

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            graph_file = (
                OUTPUT_DIR
                / f"regression_analysis_{predictor_metric}_{target_metric}_{timestamp}.png"
            )
            plt.savefig(graph_file, dpi=300, bbox_inches="tight")
            plt.close()

            result += f"📈 회귀 분석 그래프가 저장되었습니다: {graph_file}"
            return result

        except Exception as e:
            return f"회귀 분석 중 오류 발생: {str(e)}"

    async def list_data_files(self) -> str:
        """데이터 파일 목록 조회"""
        try:
            csv_files = list(DATA_DIR.glob("*.csv"))
            if not csv_files:
                return "data 디렉토리에 CSV 파일이 없습니다."

            result = "📁 데이터 파일 목록:\n\n"
            for file in csv_files:
                file_size = file.stat().st_size
                modified_time = datetime.fromtimestamp(file.stat().st_mtime)
                result += f"• {file.name}\n"
                result += f"  크기: {file_size:,} bytes\n"
                result += f"  수정일: {modified_time.strftime('%Y-%m-%d %H:%M:%S')}\n\n"

            return result
        except Exception as e:
            return f"데이터 파일 목록 조회 실패: {str(e)}"

    async def get_metric_summary(self, csv_file: str) -> str:
        """메트릭 요약 정보 조회"""
        if not ANALYSIS_AVAILABLE:
            return "❌ 분석 라이브러리가 설치되지 않았습니다."

        try:
            # CSV 파일 경로 처리
            if not csv_file.startswith("/"):
                csv_path = DATA_DIR / csv_file
            else:
                csv_path = Path(csv_file)

            if not csv_path.exists():
                return f"CSV 파일을 찾을 수 없습니다: {csv_path}"

            # 데이터 읽기
            df = pd.read_csv(csv_path, index_col="Timestamp", parse_dates=True)

            result = f"📊 메트릭 요약 정보 ({csv_file}):\n\n"
            result += f"📅 데이터 기간: {df.index.min()} ~ {df.index.max()}\n"
            result += f"📈 데이터 포인트: {len(df)}개\n"
            result += f"📋 메트릭 수: {len(df.columns)}개\n\n"

            result += "📊 메트릭 목록:\n"
            for i, column in enumerate(df.columns, 1):
                non_null_count = df[column].count()
                result += f"{i:2d}. {column} ({non_null_count}개 데이터)\n"

            # 기본 통계
            result += f"\n📈 기본 통계:\n"
            stats = df.describe()
            result += stats.to_string()

            return result

        except Exception as e:
            return f"메트릭 요약 정보 조회 중 오류 발생: {str(e)}"

    def validate_column_type_compatibility(
        self, existing_column: Dict[str, Any], new_definition: str, debug_log
    ) -> Dict[str, Any]:
        """컬럼 데이터 타입 호환성 검증"""
        debug_log(
            f"타입 호환성 검증 시작: 기존={existing_column['data_type']}, 새로운={new_definition}"
        )

        issues = []

        # 새로운 데이터 타입 파싱
        new_type_info = self.parse_data_type(new_definition.split()[0])
        existing_type = existing_column["data_type"]

        debug_log(f"파싱된 새 타입: {new_type_info}")

        # 호환되지 않는 타입 변경 검사
        incompatible_changes = [
            # 문자열 -> 숫자
            (
                ["VARCHAR", "CHAR", "TEXT", "LONGTEXT", "MEDIUMTEXT"],
                ["INT", "BIGINT", "DECIMAL", "FLOAT", "DOUBLE"],
            ),
            # 숫자 -> 문자열 (데이터 손실 가능)
            (["INT", "BIGINT", "DECIMAL", "FLOAT", "DOUBLE"], ["VARCHAR", "CHAR"]),
            # 날짜/시간 타입 변경
            (["DATE", "DATETIME", "TIMESTAMP"], ["INT", "VARCHAR", "CHAR"]),
        ]

        for from_types, to_types in incompatible_changes:
            if existing_type in from_types and new_type_info["type"] in to_types:
                issues.append(
                    f"데이터 타입을 {existing_type}에서 {new_type_info['type']}로 변경하는 것은 데이터 손실을 야기할 수 있습니다."
                )
                debug_log(f"호환성 문제: {existing_type} -> {new_type_info['type']}")

        # 길이 축소 검사
        if existing_type in ["VARCHAR", "CHAR"] and new_type_info["type"] in [
            "VARCHAR",
            "CHAR",
        ]:
            existing_length = existing_column["max_length"]
            new_length = new_type_info["length"]

            if existing_length and new_length and new_length < existing_length:
                issues.append(
                    f"컬럼 길이를 {existing_length}에서 {new_length}로 축소하는 것은 데이터 손실을 야기할 수 있습니다."
                )
                debug_log(f"길이 축소 문제: {existing_length} -> {new_length}")

        # 정밀도 축소 검사 (DECIMAL)
        if existing_type == "DECIMAL" and new_type_info["type"] == "DECIMAL":
            existing_precision = existing_column["precision"]
            existing_scale = existing_column["scale"]
            new_precision = new_type_info["precision"]
            new_scale = new_type_info["scale"]

            if (
                existing_precision
                and new_precision
                and new_precision < existing_precision
            ) or (existing_scale and new_scale and new_scale < existing_scale):
                issues.append(
                    f"DECIMAL 정밀도를 ({existing_precision},{existing_scale})에서 ({new_precision},{new_scale})로 축소하는 것은 데이터 손실을 야기할 수 있습니다."
                )
                debug_log(
                    f"정밀도 축소 문제: ({existing_precision},{existing_scale}) -> ({new_precision},{new_scale})"
                )

        result = {"compatible": len(issues) == 0, "issues": issues}

        debug_log(
            f"타입 호환성 검증 완료: compatible={result['compatible']}, issues={len(issues)}"
        )
        return result

    def parse_data_type(self, data_type_str: str) -> Dict[str, Any]:
        """데이터 타입 문자열을 파싱하여 타입과 길이 정보 추출"""
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

    async def collect_slow_queries(self, database_secret: str) -> str:
        """느린 쿼리 수집 및 SQL 파일 생성"""
        try:
            if not self.setup_shared_connection(database_secret):
                return "❌ 데이터베이스 연결 실패"

            cursor = self.shared_cursor

            # 현재 날짜로 파일명 생성
            current_date = datetime.now().strftime("%Y%m%d")
            filename = f"slow_queries_{current_date}.sql"
            file_path = SQL_DIR / filename

            collected_queries = set()  # 중복 제거용

            # 1. performance_schema에서 느린 쿼리 수집 시도
            try:
                cursor.execute(
                    """
                    SELECT QUERY_SAMPLE_TEXT 
                    FROM performance_schema.events_statements_summary_by_digest 
                    WHERE DIGEST_TEXT IS NOT NULL 
                        AND AVG_TIMER_WAIT >= 1000000000000
                        AND DIGEST_TEXT NOT LIKE '%performance_schema%'
                        AND DIGEST_TEXT NOT LIKE '%information_schema%'
                        AND DIGEST_TEXT NOT LIKE 'EXPLAIN%'
                    ORDER BY AVG_TIMER_WAIT DESC 
                    LIMIT 10
                """
                )

                for (query,) in cursor.fetchall():
                    if query and query.strip():
                        query_clean = query.strip()
                        # EXPLAIN으로 시작하는 쿼리 제외
                        if not query_clean.upper().startswith('EXPLAIN'):
                            # performance_schema, information_schema 관련 쿼리 제외
                            if ('performance_schema' not in query_clean.lower() and 
                                'information_schema' not in query_clean.lower()):
                                collected_queries.add(query_clean)

            except Exception as e:
                print(f"performance_schema 접근 실패: {e}")

            # 2. information_schema.PROCESSLIST에서 현재 실행 중인 느린 쿼리 수집
            try:
                cursor.execute(
                    """
                    SELECT INFO 
                    FROM information_schema.PROCESSLIST 
                    WHERE COMMAND = 'Query' 
                        AND TIME >= 1
                        AND INFO IS NOT NULL
                        AND INFO NOT LIKE '%PROCESSLIST%'
                        AND INFO NOT LIKE 'EXPLAIN%'
                    ORDER BY TIME DESC
                    LIMIT 10
                """
                )

                for (query,) in cursor.fetchall():
                    if query and query.strip():
                        query_clean = query.strip()
                        # EXPLAIN으로 시작하는 쿼리 제외
                        if not query_clean.upper().startswith('EXPLAIN'):
                            # performance_schema, information_schema 관련 쿼리 제외
                            if ('performance_schema' not in query_clean.lower() and 
                                'information_schema' not in query_clean.lower()):
                                collected_queries.add(query_clean)

            except Exception as e:
                print(f"PROCESSLIST 접근 실패: {e}")

            # 쿼리가 있을 때만 파일 생성
            if collected_queries:
                with open(file_path, "w", encoding="utf-8") as f:
                    f.write(f"-- 느린 쿼리 모음 (수집일시: {datetime.now()})\n")
                    f.write(f"-- 총 {len(collected_queries)}개의 쿼리\n\n")

                    for i, query in enumerate(collected_queries, 1):
                        f.write(f"-- 느린 쿼리 #{i}\n")
                        f.write(f"{query};\n\n")

                return f"✅ 느린 쿼리 {len(collected_queries)}개 수집 완료: {filename}"
            else:
                return f"✅ 느린 쿼리가 발견되지 않았습니다 (파일 생성하지 않음)"

        except Exception as e:
            return f"❌ 느린 쿼리 수집 실패: {str(e)}"

    async def collect_memory_intensive_queries(self, database_secret: str) -> str:
        """메모리 집약적 쿼리 수집 및 SQL 파일 생성"""
        try:
            if not self.setup_shared_connection(database_secret):
                return "❌ 데이터베이스 연결 실패"

            cursor = self.shared_cursor

            # 현재 날짜로 파일명 생성
            current_date = datetime.now().strftime("%Y%m%d")
            filename = f"memory_intensive_queries_{current_date}.sql"
            file_path = SQL_DIR / filename

            collected_queries = set()  # 중복 제거용

            # 1. performance_schema에서 메모리 집약적 쿼리 수집 시도
            try:
                cursor.execute(
                    """
                    SELECT QUERY_SAMPLE_TEXT 
                    FROM performance_schema.events_statements_summary_by_digest 
                    WHERE DIGEST_TEXT IS NOT NULL 
                        AND MAX_MEMORY_USED > 100*1024*1024
                        AND DIGEST_TEXT NOT LIKE '%performance_schema%'
                        AND DIGEST_TEXT NOT LIKE '%information_schema%'
                        AND DIGEST_TEXT NOT LIKE 'EXPLAIN%'
                    ORDER BY MAX_MEMORY_USED DESC 
                    LIMIT 10
                """
                )

                for (query,) in cursor.fetchall():
                    if query and query.strip():
                        query_clean = query.strip()
                        # EXPLAIN으로 시작하는 쿼리 제외
                        if not query_clean.upper().startswith('EXPLAIN'):
                            # performance_schema, information_schema 관련 쿼리 제외
                            if ('performance_schema' not in query_clean.lower() and 
                                'information_schema' not in query_clean.lower()):
                                collected_queries.add(query_clean)

            except Exception as e:
                print(f"performance_schema 메모리 쿼리 접근 실패: {e}")

            # 쿼리가 있을 때만 파일 생성
            if collected_queries:
                with open(file_path, "w", encoding="utf-8") as f:
                    f.write(
                        f"-- 메모리 집약적 쿼리 모음 (수집일시: {datetime.now()})\n"
                    )
                    f.write(f"-- 총 {len(collected_queries)}개의 쿼리\n\n")

                    for i, query in enumerate(collected_queries, 1):
                        f.write(f"-- 메모리 집약적 쿼리 #{i}\n")
                        f.write(f"{query};\n\n")

                return f"✅ 메모리 집약적 쿼리 {len(collected_queries)}개 수집 완료: {filename}"
            else:
                return (
                    f"✅ 메모리 집약적 쿼리가 발견되지 않았습니다 (파일 생성하지 않음)"
                )

        except Exception as e:
            return f"❌ 메모리 집약적 쿼리 수집 실패: {str(e)}"

    async def collect_cpu_intensive_queries(self, database_secret: str) -> str:
        """CPU 집약적 쿼리 수집 및 SQL 파일 생성"""
        try:
            if not self.setup_shared_connection(database_secret):
                return "❌ 데이터베이스 연결 실패"

            cursor = self.shared_cursor

            # 현재 날짜로 파일명 생성
            current_date = datetime.now().strftime("%Y%m%d")
            filename = f"cpu_intensive_queries_{current_date}.sql"
            file_path = SQL_DIR / filename

            collected_queries = set()  # 중복 제거용

            # 1. performance_schema에서 CPU 집약적 쿼리 수집 시도
            try:
                cursor.execute(
                    """
                    SELECT QUERY_SAMPLE_TEXT 
                    FROM performance_schema.events_statements_summary_by_digest 
                    WHERE DIGEST_TEXT IS NOT NULL 
                        AND SUM_TIMER_WAIT > 10000000000000
                        AND DIGEST_TEXT NOT LIKE '%performance_schema%'
                        AND DIGEST_TEXT NOT LIKE '%information_schema%'
                        AND DIGEST_TEXT NOT LIKE 'EXPLAIN%'
                    ORDER BY SUM_TIMER_WAIT DESC 
                    LIMIT 10
                """
                )

                for (query,) in cursor.fetchall():
                    if query and query.strip():
                        query_clean = query.strip()
                        # EXPLAIN으로 시작하는 쿼리 제외
                        if not query_clean.upper().startswith('EXPLAIN'):
                            # performance_schema, information_schema 관련 쿼리 제외
                            if ('performance_schema' not in query_clean.lower() and 
                                'information_schema' not in query_clean.lower()):
                                collected_queries.add(query_clean)

            except Exception as e:
                print(f"performance_schema CPU 쿼리 접근 실패: {e}")

            # 2. information_schema에서 현재 CPU 사용 중인 쿼리 수집
            try:
                cursor.execute(
                    """
                    SELECT INFO 
                    FROM information_schema.PROCESSLIST 
                    WHERE COMMAND = 'Query' 
                        AND STATE IN ('Sending data', 'Sorting result', 'Creating sort index', 'Copying to tmp table')
                        AND INFO IS NOT NULL
                        AND INFO NOT LIKE '%PROCESSLIST%'
                        AND INFO NOT LIKE 'EXPLAIN%'
                    ORDER BY TIME DESC
                    LIMIT 10
                """
                )

                for (query,) in cursor.fetchall():
                    if query and query.strip():
                        query_clean = query.strip()
                        # EXPLAIN으로 시작하는 쿼리 제외
                        if not query_clean.upper().startswith('EXPLAIN'):
                            # performance_schema, information_schema 관련 쿼리 제외
                            if ('performance_schema' not in query_clean.lower() and 
                                'information_schema' not in query_clean.lower()):
                                collected_queries.add(query_clean)

            except Exception as e:
                print(f"PROCESSLIST CPU 쿼리 접근 실패: {e}")

            # 쿼리가 있을 때만 파일 생성
            if collected_queries:
                with open(file_path, "w", encoding="utf-8") as f:
                    f.write(f"-- CPU 집약적 쿼리 모음 (수집일시: {datetime.now()})\n")
                    f.write(f"-- 총 {len(collected_queries)}개의 쿼리\n\n")

                    for i, query in enumerate(collected_queries, 1):
                        f.write(f"-- CPU 집약적 쿼리 #{i}\n")
                        f.write(f"{query};\n\n")

                return f"✅ CPU 집약적 쿼리 {len(collected_queries)}개 수집 완료: {filename}"
            else:
                return f"✅ CPU 집약적 쿼리가 발견되지 않았습니다 (파일 생성하지 않음)"

        except Exception as e:
            return f"❌ CPU 집약적 쿼리 수집 실패: {str(e)}"

    async def collect_temp_space_intensive_queries(self, database_secret: str) -> str:
        """임시 공간 집약적 쿼리 수집 및 SQL 파일 생성"""
        try:
            if not self.setup_shared_connection(database_secret):
                return "❌ 데이터베이스 연결 실패"

            cursor = self.shared_cursor

            # 현재 날짜로 파일명 생성
            current_date = datetime.now().strftime("%Y%m%d")
            filename = f"temp_space_intensive_queries_{current_date}.sql"
            file_path = SQL_DIR / filename

            collected_queries = set()  # 중복 제거용

            # 1. performance_schema에서 임시 공간 집약적 쿼리 수집 시도
            try:
                cursor.execute(
                    """
                    SELECT QUERY_SAMPLE_TEXT 
                    FROM performance_schema.events_statements_summary_by_digest 
                    WHERE DIGEST_TEXT IS NOT NULL 
                        AND (SUM_CREATED_TMP_TABLES > 0 OR SUM_CREATED_TMP_DISK_TABLES > 0 OR SUM_SORT_MERGE_PASSES > 0)
                        AND DIGEST_TEXT NOT LIKE '%performance_schema%'
                        AND DIGEST_TEXT NOT LIKE '%information_schema%'
                        AND DIGEST_TEXT NOT LIKE 'EXPLAIN%'
                    ORDER BY (SUM_CREATED_TMP_DISK_TABLES + SUM_SORT_MERGE_PASSES) DESC 
                    LIMIT 10
                """
                )

                for (query,) in cursor.fetchall():
                    if query and query.strip():
                        query_clean = query.strip()
                        # EXPLAIN으로 시작하는 쿼리 제외
                        if not query_clean.upper().startswith('EXPLAIN'):
                            # performance_schema, information_schema 관련 쿼리 제외
                            if ('performance_schema' not in query_clean.lower() and 
                                'information_schema' not in query_clean.lower()):
                                collected_queries.add(query_clean)

            except Exception as e:
                print(f"performance_schema 임시공간 쿼리 접근 실패: {e}")

            # 2. information_schema에서 현재 임시 테이블 사용 중인 쿼리 수집
            try:
                cursor.execute(
                    """
                    SELECT INFO 
                    FROM information_schema.PROCESSLIST 
                    WHERE COMMAND = 'Query' 
                        AND STATE IN ('Copying to tmp table', 'Sorting for group', 'Sorting for order', 'Creating sort index')
                        AND INFO IS NOT NULL
                        AND INFO NOT LIKE '%PROCESSLIST%'
                        AND INFO NOT LIKE 'EXPLAIN%'
                    ORDER BY TIME DESC
                    LIMIT 10
                """
                )

                for (query,) in cursor.fetchall():
                    if query and query.strip():
                        query_clean = query.strip()
                        # EXPLAIN으로 시작하는 쿼리 제외
                        if not query_clean.upper().startswith('EXPLAIN'):
                            # performance_schema, information_schema 관련 쿼리 제외
                            if ('performance_schema' not in query_clean.lower() and 
                                'information_schema' not in query_clean.lower()):
                                collected_queries.add(query_clean)

            except Exception as e:
                print(f"PROCESSLIST 임시공간 쿼리 접근 실패: {e}")

            # 쿼리가 있을 때만 파일 생성
            if collected_queries:
                with open(file_path, "w", encoding="utf-8") as f:
                    f.write(
                        f"-- 임시 공간 집약적 쿼리 모음 (수집일시: {datetime.now()})\n"
                    )
                    f.write(f"-- 총 {len(collected_queries)}개의 쿼리\n\n")

                    for i, query in enumerate(collected_queries, 1):
                        f.write(f"-- 임시 공간 집약적 쿼리 #{i}\n")
                        f.write(f"{query};\n\n")

                return f"✅ 임시 공간 집약적 쿼리 {len(collected_queries)}개 수집 완료: {filename}"
            else:
                return f"✅ 임시 공간 집약적 쿼리가 발견되지 않았습니다 (파일 생성하지 않음)"

        except Exception as e:
            return f"❌ 임시 공간 집약적 쿼리 수집 실패: {str(e)}"


# MCP 서버 설정
server = Server("db-assistant-mcp-server")
db_assistant = DBAssistantMCPServer()


@server.list_tools()
async def handle_list_tools() -> list[types.Tool]:
    """사용 가능한 도구 목록 반환"""
    return [
        types.Tool(
            name="list_sql_files",
            description="sql 디렉토리의 SQL 파일 목록을 조회합니다",
            inputSchema={"type": "object", "properties": {}},
        ),
        types.Tool(
            name="list_database_secrets",
            description="AWS Secrets Manager의 데이터베이스 시크릿 목록을 조회합니다",
            inputSchema={
                "type": "object",
                "properties": {
                    "keyword": {
                        "type": "string",
                        "description": "검색할 키워드 (선택사항)",
                    }
                },
            },
        ),
        types.Tool(
            name="test_database_connection",
            description="데이터베이스 연결을 테스트합니다",
            inputSchema={
                "type": "object",
                "properties": {
                    "database_secret": {
                        "type": "string",
                        "description": "데이터베이스 시크릿 이름",
                    }
                },
                "required": ["database_secret"],
            },
        ),
        types.Tool(
            name="list_databases",
            description="데이터베이스 목록을 조회하고 선택할 수 있습니다",
            inputSchema={
                "type": "object",
                "properties": {
                    "database_secret": {
                        "type": "string",
                        "description": "데이터베이스 시크릿 이름",
                    }
                },
                "required": ["database_secret"],
            },
        ),
        types.Tool(
            name="select_database",
            description="특정 데이터베이스를 선택합니다 (USE 명령어 실행). 먼저 list_databases로 목록을 확인한 후 번호나 이름으로 선택하세요.",
            inputSchema={
                "type": "object",
                "properties": {
                    "database_secret": {
                        "type": "string",
                        "description": "데이터베이스 시크릿 이름",
                    },
                    "database_selection": {
                        "type": "string",
                        "description": "선택할 데이터베이스 (번호 또는 이름)",
                    },
                },
                "required": ["database_secret", "database_selection"],
            },
        ),
        types.Tool(
            name="get_schema_summary",
            description="현재 데이터베이스 스키마의 요약 정보를 제공합니다",
            inputSchema={
                "type": "object",
                "properties": {
                    "database_secret": {
                        "type": "string",
                        "description": "데이터베이스 시크릿 이름",
                    }
                },
                "required": ["database_secret"],
            },
        ),
        types.Tool(
            name="get_table_schema",
            description="특정 테이블의 상세 스키마 정보를 조회합니다",
            inputSchema={
                "type": "object",
                "properties": {
                    "database_secret": {
                        "type": "string",
                        "description": "데이터베이스 시크릿 이름",
                    },
                    "table_name": {
                        "type": "string",
                        "description": "조회할 테이블 이름",
                    },
                },
                "required": ["database_secret", "table_name"],
            },
        ),
        types.Tool(
            name="get_table_index",
            description="특정 테이블의 인덱스 정보를 조회합니다",
            inputSchema={
                "type": "object",
                "properties": {
                    "database_secret": {
                        "type": "string",
                        "description": "데이터베이스 시크릿 이름",
                    },
                    "table_name": {
                        "type": "string",
                        "description": "조회할 테이블 이름",
                    },
                },
                "required": ["database_secret", "table_name"],
            },
        ),
        types.Tool(
            name="get_performance_metrics",
            description="데이터베이스 성능 메트릭을 조회합니다",
            inputSchema={
                "type": "object",
                "properties": {
                    "database_secret": {
                        "type": "string",
                        "description": "데이터베이스 시크릿 이름",
                    },
                    "metric_type": {
                        "type": "string",
                        "description": "메트릭 타입 (all, query, io, memory, connection)",
                        "enum": ["all", "query", "io", "memory", "connection"],
                        "default": "all",
                    },
                },
                "required": ["database_secret"],
            },
        ),
        types.Tool(
            name="collect_db_metrics",
            description="CloudWatch에서 데이터베이스 메트릭을 수집합니다",
            inputSchema={
                "type": "object",
                "properties": {
                    "db_instance_identifier": {
                        "type": "string",
                        "description": "데이터베이스 인스턴스 식별자",
                    },
                    "hours": {
                        "type": "integer",
                        "description": "수집할 시간 범위 (시간 단위, 기본값: 24)",
                        "default": 24,
                    },
                    "metrics": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "수집할 메트릭 목록 (선택사항)",
                    },
                    "region": {
                        "type": "string",
                        "description": "AWS 리전 (기본값: us-east-1)",
                        "default": "us-east-1",
                    },
                },
                "required": ["db_instance_identifier"],
            },
        ),
        types.Tool(
            name="analyze_metric_correlation",
            description="메트릭 간 상관관계를 분석합니다",
            inputSchema={
                "type": "object",
                "properties": {
                    "csv_file": {"type": "string", "description": "분석할 CSV 파일명"},
                    "target_metric": {
                        "type": "string",
                        "description": "타겟 메트릭 (기본값: CPUUtilization)",
                        "default": "CPUUtilization",
                    },
                    "top_n": {
                        "type": "integer",
                        "description": "상위 N개 메트릭 (기본값: 10)",
                        "default": 10,
                    },
                },
                "required": ["csv_file"],
            },
        ),
        types.Tool(
            name="detect_metric_outliers",
            description="메트릭 데이터에서 아웃라이어를 탐지합니다",
            inputSchema={
                "type": "object",
                "properties": {
                    "csv_file": {"type": "string", "description": "분석할 CSV 파일명"},
                    "std_threshold": {
                        "type": "number",
                        "description": "표준편차 임계값 (기본값: 2.0)",
                        "default": 2.0,
                    },
                },
                "required": ["csv_file"],
            },
        ),
        types.Tool(
            name="perform_regression_analysis",
            description="메트릭 간 회귀 분석을 수행합니다",
            inputSchema={
                "type": "object",
                "properties": {
                    "csv_file": {"type": "string", "description": "분석할 CSV 파일명"},
                    "predictor_metric": {
                        "type": "string",
                        "description": "예측 변수 메트릭",
                    },
                    "target_metric": {
                        "type": "string",
                        "description": "타겟 메트릭 (기본값: CPUUtilization)",
                        "default": "CPUUtilization",
                    },
                },
                "required": ["csv_file", "predictor_metric"],
            },
        ),
        types.Tool(
            name="list_data_files",
            description="데이터 디렉토리의 CSV 파일 목록을 조회합니다",
            inputSchema={"type": "object", "properties": {}},
        ),
        types.Tool(
            name="validate_sql_file",
            description="특정 SQL 파일을 검증합니다",
            inputSchema={
                "type": "object",
                "properties": {
                    "filename": {"type": "string", "description": "검증할 SQL 파일명"},
                    "database_secret": {
                        "type": "string",
                        "description": "데이터베이스 시크릿 이름 (선택사항)",
                    },
                },
                "required": ["filename"],
            },
        ),
        types.Tool(
            name="copy_sql_to_directory",
            description="SQL 파일을 sql 디렉토리로 복사합니다",
            inputSchema={
                "type": "object",
                "properties": {
                    "source_path": {
                        "type": "string",
                        "description": "복사할 SQL 파일의 경로",
                    },
                    "target_name": {
                        "type": "string",
                        "description": "대상 파일명 (선택사항, 기본값은 원본 파일명)",
                    },
                },
                "required": ["source_path"],
            },
        ),
        types.Tool(
            name="get_metric_summary",
            description="CSV 파일의 메트릭 요약 정보를 조회합니다",
            inputSchema={
                "type": "object",
                "properties": {
                    "csv_file": {"type": "string", "description": "요약할 CSV 파일명"}
                },
                "required": ["csv_file"],
            },
        ),
        types.Tool(
            name="collect_slow_queries",
            description="느린 쿼리를 수집하는 SQL을 생성합니다",
            inputSchema={
                "type": "object",
                "properties": {
                    "database_secret": {
                        "type": "string",
                        "description": "데이터베이스 시크릿 이름",
                    }
                },
                "required": ["database_secret"],
            },
        ),
        types.Tool(
            name="collect_memory_intensive_queries",
            description="메모리 집약적 쿼리를 수집하는 SQL을 생성합니다",
            inputSchema={
                "type": "object",
                "properties": {
                    "database_secret": {
                        "type": "string",
                        "description": "데이터베이스 시크릿 이름",
                    }
                },
                "required": ["database_secret"],
            },
        ),
        types.Tool(
            name="collect_cpu_intensive_queries",
            description="CPU 집약적 쿼리를 수집하는 SQL을 생성합니다",
            inputSchema={
                "type": "object",
                "properties": {
                    "database_secret": {
                        "type": "string",
                        "description": "데이터베이스 시크릿 이름",
                    }
                },
                "required": ["database_secret"],
            },
        ),
        types.Tool(
            name="collect_temp_space_intensive_queries",
            description="임시 공간 집약적 쿼리를 수집하는 SQL을 생성합니다",
            inputSchema={
                "type": "object",
                "properties": {
                    "database_secret": {
                        "type": "string",
                        "description": "데이터베이스 시크릿 이름",
                    }
                },
                "required": ["database_secret"],
            },
        ),
        types.Tool(
            name="test_individual_query_validation",
            description="개별 쿼리 검증 테스트 (디버그용)",
            inputSchema={
                "type": "object",
                "properties": {
                    "database_secret": {
                        "type": "string",
                        "description": "데이터베이스 시크릿 이름",
                    },
                    "filename": {"type": "string", "description": "검증할 SQL 파일명"},
                },
                "required": ["database_secret", "filename"],
            },
        ),
        types.Tool(
            name="generate_consolidated_report",
            description="기존 HTML 보고서들을 기반으로 통합 보고서 생성",
            inputSchema={
                "type": "object",
                "properties": {
                    "keyword": {
                        "type": "string",
                        "description": "필터링할 키워드 (선택사항)",
                    },
                    "report_files": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "특정 보고서 파일명 목록 (선택사항)",
                    },
                    "date_filter": {
                        "type": "string",
                        "description": "날짜 필터 (YYYYMMDD 형식, 선택사항)",
                    },
                    "latest_count": {
                        "type": "integer",
                        "description": "최신 파일 개수 제한 (선택사항)",
                    }
                },
            },
        ),
    ]


@server.call_tool()
async def handle_call_tool(name: str, arguments: dict) -> list[types.TextContent]:
    """도구 호출 처리"""
    try:
        if name == "list_sql_files":
            result = await db_assistant.list_sql_files()
        elif name == "list_database_secrets":
            result = await db_assistant.list_database_secrets(
                arguments.get("keyword", "")
            )
        elif name == "test_database_connection":
            result = await db_assistant.test_database_connection(
                arguments["database_secret"]
            )
        elif name == "list_databases":
            result = await db_assistant.list_databases(arguments["database_secret"])
        elif name == "select_database":
            result = await db_assistant.select_database(
                arguments["database_secret"], arguments["database_selection"]
            )
        elif name == "get_schema_summary":
            result = await db_assistant.get_schema_summary(arguments["database_secret"])
        elif name == "get_table_schema":
            result = await db_assistant.get_table_schema(
                arguments["database_secret"], arguments["table_name"]
            )
        elif name == "get_table_index":
            result = await db_assistant.get_table_index(
                arguments["database_secret"], arguments["table_name"]
            )
        elif name == "get_performance_metrics":
            result = await db_assistant.get_performance_metrics(
                arguments["database_secret"], arguments.get("metric_type", "all")
            )
        elif name == "collect_db_metrics":
            result = await db_assistant.collect_db_metrics(
                arguments["db_instance_identifier"],
                arguments.get("hours", 24),
                arguments.get("metrics"),
                arguments.get("region", "us-east-1"),
            )
        elif name == "analyze_metric_correlation":
            result = await db_assistant.analyze_metric_correlation(
                arguments["csv_file"],
                arguments.get("target_metric", "CPUUtilization"),
                arguments.get("top_n", 10),
            )
        elif name == "detect_metric_outliers":
            result = await db_assistant.detect_metric_outliers(
                arguments["csv_file"], arguments.get("std_threshold", 2.0)
            )
        elif name == "perform_regression_analysis":
            result = await db_assistant.perform_regression_analysis(
                arguments["csv_file"],
                arguments["predictor_metric"],
                arguments.get("target_metric", "CPUUtilization"),
            )
        elif name == "list_data_files":
            result = await db_assistant.list_data_files()
        elif name == "validate_sql_file":
            result = await db_assistant.validate_sql_file(
                arguments["filename"], arguments.get("database_secret")
            )
        elif name == "copy_sql_to_directory":
            result = await db_assistant.copy_sql_file(
                arguments["source_path"], arguments.get("target_name")
            )
        elif name == "get_metric_summary":
            result = await db_assistant.get_metric_summary(arguments["csv_file"])
        elif name == "collect_slow_queries":
            result = await db_assistant.collect_slow_queries(
                arguments["database_secret"]
            )
        elif name == "collect_memory_intensive_queries":
            result = await db_assistant.collect_memory_intensive_queries(
                arguments["database_secret"]
            )
        elif name == "collect_cpu_intensive_queries":
            result = await db_assistant.collect_cpu_intensive_queries(
                arguments["database_secret"]
            )
        elif name == "collect_temp_space_intensive_queries":
            result = await db_assistant.collect_temp_space_intensive_queries(
                arguments["database_secret"]
            )
        elif name == "test_individual_query_validation":
            result = await db_assistant.test_individual_query_validation(
                arguments["database_secret"], arguments["filename"]
            )
        elif name == "generate_consolidated_report":
            result = await db_assistant.generate_consolidated_report(
                arguments.get("keyword"), 
                arguments.get("report_files"),
                arguments.get("date_filter"),
                arguments.get("latest_count")
            )
        else:
            result = f"알 수 없는 도구: {name}"

        return [types.TextContent(type="text", text=result)]

    except Exception as e:
        logger.error(f"도구 실행 오류: {e}")
        return [types.TextContent(type="text", text=f"오류: {str(e)}")]


async def main():
    """메인 함수"""
    try:
        async with mcp.server.stdio.stdio_server() as (read_stream, write_stream):
            await server.run(
                read_stream,
                write_stream,
                InitializationOptions(
                    server_name="db-assistant-mcp-server",
                    server_version="1.0.0",
                    capabilities=server.get_capabilities(
                        notification_options=NotificationOptions(),
                        experimental_capabilities={},
                    ),
                ),
            )
    except Exception as e:
        logger.error(f"서버 실행 오류: {e}")
        raise e


if __name__ == "__main__":
    asyncio.run(main())
