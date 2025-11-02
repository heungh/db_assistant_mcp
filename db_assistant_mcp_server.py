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
CHART_AVAILABLE = False
try:
    import pandas as pd
    import numpy as np
    import sqlparse
    import matplotlib

    matplotlib.use("Agg")  # GUI 없는 환경에서 사용
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates
    from sklearn.model_selection import train_test_split
    from sklearn.preprocessing import PolynomialFeatures
    from sklearn.linear_model import LinearRegression
    from sklearn.metrics import mean_squared_error, r2_score
    from sklearn.impute import SimpleImputer

    ANALYSIS_AVAILABLE = True
    CHART_AVAILABLE = True
except ImportError:
    sqlparse = None

from mcp.server.models import InitializationOptions
import mcp.types as types
from mcp.server import NotificationOptions, Server
import mcp.server.stdio
import logging

# 유틸리티 모듈 import
from utils.logging_utils import create_session_log, debug_log, logger
from utils.constants import (
    CURRENT_DIR,
    OUTPUT_DIR,
    SQL_DIR,
    DATA_DIR,
    LOGS_DIR,
    BACKUP_DIR,
    PERFORMANCE_THRESHOLDS,
    DEFAULT_METRICS,
    DEFAULT_REGION,
    BEDROCK_REGION,
    KNOWLEDGE_BASE_REGION,
    KNOWLEDGE_BASE_ID,
    DATA_SOURCE_ID,
    QUERY_RESULTS_BUCKET,
    QUERY_RESULTS_DEV_BUCKET,
    BEDROCK_AGENT_BUCKET,
)
from utils.parsers import (
    parse_table_name,
    format_file_link,
    convert_kst_to_utc,
    convert_utc_to_local,
    parse_data_type,
    extract_sql_type,
    sanitize_sql,
    is_valid_sql_identifier,
)
from utils.formatters import (
    format_bytes,
    format_number,
    format_percentage,
    format_duration,
    format_timestamp,
    format_sql_for_display,
    format_metric_value,
)

# 모듈 import (리팩토링)
from modules.lambda_client import LambdaClient  # Week 1
from modules.cloudwatch_manager import CloudWatchManager  # Week 2
from modules.report_generator import ReportGenerator  # Week 3
from modules.sql_parser import SQLParser  # Week 4 Phase 2

# 로깅 설정 및 전역 변수는 utils 모듈에서 import됨
# create_session_log, debug_log 함수는 utils/logging_utils.py에서 제공
# 디렉토리 경로 상수들은 utils/constants.py에서 제공


class DBAssistantMCPServer:
    def __init__(self):
        try:
            logger.info("Bedrock 클라이언트 초기화 시작")
            self.bedrock_client = boto3.client(
                "bedrock-runtime", region_name="us-west-2", verify=False
            )
            logger.info("Bedrock 클라이언트 초기화 성공 - 리전: us-west-2")

            # Bedrock 접근 권한 테스트
            try:
                # 간단한 모델 목록 조회로 권한 테스트
                bedrock_control = boto3.client(
                    "bedrock", region_name="us-west-2", verify=False
                )
                logger.info("Bedrock 서비스 접근 권한 확인 중...")
                # 실제 권한 테스트는 모델 호출 시 수행
                logger.info("Bedrock 클라이언트 설정 완료")
            except Exception as perm_e:
                logger.warning(
                    f"Bedrock 권한 사전 확인 실패 (모델 호출 시 재시도): {perm_e}"
                )

        except Exception as e:
            logger.error(f"Bedrock 클라이언트 초기화 실패: {e}")
            raise

        # Knowledge Base ID (utils/constants.py에서 가져옴)
        self.knowledge_base_id = KNOWLEDGE_BASE_ID
        self.selected_database = None
        self.current_plan = None

        # 공용 DB 연결 변수 (연결 재사용을 위해)
        self.shared_connection = None
        self.shared_cursor = None
        self.tunnel_used = False

        # 성능 임계값 설정 (utils/constants.py에서 가져옴)
        self.PERFORMANCE_THRESHOLDS = PERFORMANCE_THRESHOLDS

        # 분석 관련 초기화
        self.cloudwatch = None
        # CloudWatch 메트릭 설정 (utils/constants.py에서 가져옴)
        self.default_metrics = DEFAULT_METRICS

        # 기본 리전 설정 (utils/constants.py에서 가져옴)
        self.default_region = self.get_default_region()

        # Knowledge Base 클라이언트 초기화 (리전은 utils/constants.py에서 가져옴)
        self.bedrock_agent_client = boto3.client(
            "bedrock-agent-runtime", region_name=KNOWLEDGE_BASE_REGION, verify=False
        )

        # Lambda 클라이언트 초기화 (하이브리드 아키텍처용)
        # 리팩토링: Week 1 - LambdaClient 모듈 사용
        self.lambda_client = LambdaClient(region=self.default_region)
        logger.info(f"Lambda 클라이언트 초기화 완료 - 리전: {self.default_region}")

        # CloudWatch Manager 초기화
        # 리팩토링: Week 2 - CloudWatchManager 모듈 사용
        self.cloudwatch_manager = CloudWatchManager(
            region=self.default_region,
            lambda_client=self.lambda_client
        )
        logger.info(f"CloudWatch Manager 초기화 완료 - 리전: {self.default_region}")

        # Report Generator 초기화
        # 리팩토링: Week 3 - ReportGenerator 모듈 사용
        self.report_generator = ReportGenerator()
        logger.info("Report Generator 초기화 완료")

        # SQL Parser 초기화
        # 리팩토링: Week 4 Phase 2 - SQLParser 모듈 사용
        self.sql_parser = SQLParser()
        logger.info("SQL Parser 초기화 완료")

    def get_default_region(self) -> str:
        """현재 AWS 프로파일의 기본 리전 가져오기"""
        try:
            session = boto3.Session()
            return session.region_name or DEFAULT_REGION
        except Exception:
            return DEFAULT_REGION

    async def _call_lambda(self, function_name: str, payload: dict) -> dict:
        """
        Lambda 함수 호출 헬퍼 (하이브리드 아키텍처용)

        리팩토링: Week 1 - LambdaClient 모듈로 위임
        하위 호환성을 위해 메서드 유지
        """
        return await self.lambda_client._call_lambda(function_name, payload)

    def parse_table_name(self, full_table_name: str) -> tuple:
        """테이블명에서 스키마와 테이블명을 분리 (utils/parsers.py 위임)"""
        return parse_table_name(full_table_name)

    def format_file_link(self, file_path: str, display_name: str = None) -> str:
        """파일 경로를 HTML 링크로 변환 (utils/parsers.py 위임)"""
        return format_file_link(file_path, display_name)

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

    def convert_kst_to_utc(self, kst_time_str: str) -> datetime:
        """KST 시간 문자열을 UTC datetime 객체로 변환 (utils/parsers.py 위임)"""
        return convert_kst_to_utc(kst_time_str)

    def convert_utc(self, utc_dt: datetime, region_name: str = None) -> datetime:
        """UTC datetime 객체를 지정된 리전의 로컬 시간으로 변환 (utils/parsers.py 위임)"""
        if region_name is None:
            region_name = self.default_region
        return convert_utc_to_local(utc_dt, region_name)

    def set_default_region(self, region_name: str) -> str:
        """기본 AWS 리전을 변경합니다"""
        # 지원하는 리전 목록
        supported_regions = {
            "ap-northeast-1",
            "ap-northeast-2",
            "ap-northeast-3",
            "ap-south-1",
            "ap-southeast-1",
            "ap-southeast-2",
            "ap-east-1",
            "eu-west-1",
            "eu-west-2",
            "eu-west-3",
            "eu-central-1",
            "eu-north-1",
            "us-east-1",
            "us-east-2",
            "us-west-1",
            "us-west-2",
            "ca-central-1",
            "sa-east-1",
            "me-south-1",
            "af-south-1",
        }

        if region_name not in supported_regions:
            return (
                f"❌ 지원하지 않는 리전입니다: {region_name}\n\n✅ 지원하는 리전:\n"
                + "\n".join([f"• {region}" for region in sorted(supported_regions)])
            )

        old_region = self.default_region
        self.default_region = region_name

        # 환경 변수도 업데이트
        os.environ["AWS_DEFAULT_REGION"] = region_name

        return f"✅ 기본 리전이 변경되었습니다!\n\n이전: {old_region}\n현재: {self.default_region}\n\n💡 이제 모든 AWS 서비스 호출과 시간 변환이 새 리전 기준으로 작동합니다."

    async def get_secret(self, secret_name):
        """Secrets Manager에서 DB 연결 정보 가져오기 (Lambda 사용)"""
        try:
            # Lambda 함수 호출
            result = await self._call_lambda('get-secret', {
                'secret_name': secret_name,
                'region': 'ap-northeast-2'
            })

            if result.get('success'):
                return result['secret']
            else:
                error_msg = result.get('error', 'Unknown error')
                logger.error(f"Secret 조회 실패 (Lambda): {error_msg}")
                raise Exception(error_msg)

        except Exception as e:
            logger.error(f"Secret 조회 실패: {e}")
            raise e

    async def get_secrets_by_keyword(self, keyword=""):
        """키워드로 Secret 목록 가져오기 (Lambda 사용)"""
        try:
            # Lambda 함수 호출
            result = await self._call_lambda('list-secrets', {
                'keyword': keyword,
                'region': 'ap-northeast-2'
            })

            if result.get('success'):
                return result.get('secrets', [])
            else:
                error_msg = result.get('error', 'Unknown error')
                logger.error(f"Secret 목록 조회 실패 (Lambda): {error_msg}")
                return []

        except Exception as e:
            logger.error(f"Secret 목록 조회 실패: {e}")
            return []

    def setup_ssh_tunnel(self, db_host: str, region: str = "ap-northeast-2") -> bool:
        """SSH 터널 설정 (EC2에서는 사용 안 함 - VPC 직접 연결)"""
        logger.info("EC2 VPC 환경: SSH 터널링 건너뛰기")
        return False  # 항상 False 반환하여 직접 연결 사용

    def cleanup_ssh_tunnel(self):
        """SSH 터널 정리 (EC2에서는 사용 안 함)"""
        pass  # 아무 작업도 하지 않음

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
            r"batch.*approved",
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

        debug_log(
            f"🔍🔍🔍 check_performance_issues 완료 - 이슈: {performance_issues}, 승인: {is_approved_batch} 🔍🔍🔍"
        )
        return performance_issues, is_approved_batch

    async def test_individual_query_validation(
        self, database_secret: str, filename: str
    ) -> str:
        """개별 쿼리 검증 테스트 함수 (Lambda 기반)"""
        try:
            # SQL 파일 읽기
            sql_file_path = os.path.join("sql", filename)
            if not os.path.exists(sql_file_path):
                return f"❌ SQL 파일을 찾을 수 없습니다: {filename}"

            with open(sql_file_path, "r", encoding="utf-8") as f:
                sql_content = f.read()

            logger.info(f"개별 쿼리 검증 시작 (Lambda 기반): {filename}")

            # SQL을 개별 쿼리로 분리
            if sqlparse:
                statements = sqlparse.split(sql_content)
            else:
                statements = [stmt.strip() for stmt in sql_content.split(";") if stmt.strip()]

            logger.info(f"총 {len(statements)}개의 쿼리로 분리")

            # 결과 저장
            success_count = 0
            error_count = 0
            performance_issues = []
            all_issues = []

            # 각 SELECT 쿼리에 대해 Lambda EXPLAIN 실행
            for i, stmt in enumerate(statements):
                if not stmt.strip():
                    continue

                # 주석 제거
                cleaned_stmt = re.sub(r"--.*$", "", stmt, flags=re.MULTILINE)
                cleaned_stmt = re.sub(r"/\*.*?\*/", "", cleaned_stmt, flags=re.DOTALL)
                cleaned_stmt = cleaned_stmt.strip()

                if not cleaned_stmt:
                    continue

                # DML 쿼리만 EXPLAIN 실행 (SELECT, UPDATE, DELETE, INSERT, REPLACE)
                if not re.match(r"^\s*(SELECT|UPDATE|DELETE|INSERT|REPLACE)", cleaned_stmt, re.IGNORECASE):
                    logger.info(f"쿼리 {i+1}: DML 쿼리가 아니므로 EXPLAIN 스킵")
                    continue

                logger.info(f"쿼리 {i+1}: Lambda EXPLAIN 실행 중...")

                # Lambda EXPLAIN 호출
                explain_result = await self.explain_query_lambda(
                    database_secret,
                    self.selected_database,
                    cleaned_stmt
                )

                # 결과 처리
                if explain_result.get('success'):
                    success_count += 1
                    logger.info(f"쿼리 {i+1}: 성공")

                    # 성능 이슈 확인
                    if explain_result.get('performance_issues'):
                        for perf_issue in explain_result['performance_issues']:
                            issue_desc = perf_issue.get('description', str(perf_issue))
                            performance_issues.append(f"쿼리 {i+1}: {issue_desc}")
                else:
                    error_count += 1
                    error_msg = explain_result.get('error', 'Lambda EXPLAIN 실패')
                    all_issues.append(f"쿼리 {i+1}: {error_msg}")
                    logger.error(f"쿼리 {i+1}: 오류 - {error_msg}")

            # 결과 요약
            result_summary = f"✅ 개별 쿼리 검증 완료 (Lambda 기반)\n"
            result_summary += f"총 쿼리: {len(statements)}개\n"
            result_summary += f"성공: {success_count}개\n"
            result_summary += f"오류: {error_count}개\n"

            if performance_issues:
                result_summary += f"\n⚠️ 성능 이슈 ({len(performance_issues)}개):\n"
                for issue in performance_issues[:5]:  # 최대 5개만 표시
                    result_summary += f"  - {issue}\n"
                if len(performance_issues) > 5:
                    result_summary += f"  ... 외 {len(performance_issues) - 5}개\n"

            if all_issues:
                result_summary += f"\n❌ 오류 목록:\n"
                for issue in all_issues[:5]:  # 최대 5개만 표시
                    result_summary += f"  - {issue}\n"
                if len(all_issues) > 5:
                    result_summary += f"  ... 외 {len(all_issues) - 5}개\n"

            return result_summary

        except Exception as e:
            logger.error(f"검증 중 오류: {e}")
            return f"❌ 검증 중 오류: {str(e)}"




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
        use_ssh_tunnel: bool = False,  # EC2에서는 VPC 직접 연결
        db_instance_identifier: str = None,
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

        # db_instance_identifier가 제공되면 해당 인스턴스 엔드포인트 사용
        host = db_config.get("host")
        if db_instance_identifier:
            # 클러스터 엔드포인트를 인스턴스 엔드포인트로 변경
            if ".cluster-" in host:
                # aurora-cluster.cluster-xxx.region.rds.amazonaws.com -> instance-id.xxx.region.rds.amazonaws.com
                host_parts = host.split(".")
                if len(host_parts) >= 4:
                    # cluster- 부분을 제거하고 인스턴스 ID로 교체
                    host_parts[1] = host_parts[1].replace("cluster-", "")
                    host = f"{db_instance_identifier}.{'.'.join(host_parts[1:])}"
            else:
                # 단일 인스턴스인 경우 인스턴스 ID로 교체
                host_parts = host.split(".")
                if len(host_parts) >= 4:
                    host = f"{db_instance_identifier}.{'.'.join(host_parts[1:])}"

        if use_ssh_tunnel:
            if self.setup_ssh_tunnel(host):
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
                "host": host,
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
        use_ssh_tunnel: bool = False,  # EC2에서는 VPC 직접 연결
        db_instance_identifier: str = None,
    ):
        """공용 DB 연결 설정 (한 번만 호출)"""
        try:
            if self.shared_connection and self.shared_connection.is_connected():
                logger.info("이미 활성화된 공용 연결이 있습니다.")
                return True

            self.shared_connection, self.tunnel_used = self.get_db_connection(
                database_secret,
                selected_database,
                use_ssh_tunnel,
                db_instance_identifier,
            )

            if self.shared_connection and self.shared_connection.is_connected():
                self.shared_cursor = self.shared_connection.cursor()
                # 연결된 호스트 정보 로깅
                host_info = (
                    f"인스턴스: {db_instance_identifier}"
                    if db_instance_identifier
                    else "클러스터 엔드포인트"
                )
                logger.info(
                    f"공용 DB 연결 설정 완료 - {host_info} (터널: {self.tunnel_used})"
                )
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
            secrets = await self.get_secrets_by_keyword(keyword)
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
        self, database_secret: str, use_ssh_tunnel: bool = False  # EC2에서는 VPC 직접 연결
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
        self, database_secret: str, use_ssh_tunnel: bool = False  # EC2에서는 VPC 직접 연결
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
        self, database_secret: str, database_selection: str, use_ssh_tunnel: bool = False  # EC2에서는 VPC 직접 연결
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

    async def text_to_sql(
        self, database_secret: str, natural_language_query: str
    ) -> str:
        """자연어 쿼리를 SQL로 변환하고 실행"""
        try:
            # 데이터베이스 연결
            connection, tunnel_used = self.get_db_connection(
                database_secret, self.selected_database
            )
            cursor = connection.cursor()

            # 현재 데이터베이스의 모든 테이블 스키마 정보 수집
            cursor.execute("SHOW TABLES")
            tables = [table[0] for table in cursor.fetchall()]

            schema_info = {}
            for table in tables:
                cursor.execute(
                    f"""
                    SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_KEY, EXTRA, COLUMN_COMMENT
                    FROM information_schema.columns 
                    WHERE table_schema = DATABASE() AND table_name = '{table}'
                    ORDER BY ORDINAL_POSITION
                """
                )
                columns = cursor.fetchall()
                schema_info[table] = columns

            # Claude에게 SQL 생성 요청
            sql_query = await self.generate_sql_with_claude(
                natural_language_query, schema_info
            )

            if not sql_query or sql_query.startswith("❌"):
                return sql_query

            # 생성된 SQL 실행
            cursor.execute(sql_query)

            if sql_query.strip().upper().startswith("SELECT"):
                results = cursor.fetchall()
                column_names = [desc[0] for desc in cursor.description]

                result = f"✅ SQL 생성 및 실행 완료!\n\n"
                result += f"🔍 **생성된 SQL:**\n```sql\n{sql_query}\n```\n\n"
                result += f"📊 **실행 결과:** ({len(results)}행)\n"

                if results:
                    # 테이블 형태로 결과 표시 (최대 10행)
                    result += f"| {' | '.join(column_names)} |\n"
                    result += f"|{'|'.join(['---'] * len(column_names))}|\n"

                    for i, row in enumerate(results[:10]):
                        result += f"| {' | '.join(str(val) if val is not None else 'NULL' for val in row)} |\n"

                    if len(results) > 10:
                        result += f"\n... 및 {len(results) - 10}행 더"
                else:
                    result += "결과가 없습니다."
            else:
                connection.commit()
                result = f"✅ SQL 생성 및 실행 완료!\n\n"
                result += f"🔍 **생성된 SQL:**\n```sql\n{sql_query}\n```\n\n"
                result += f"📊 **실행 결과:** 쿼리가 성공적으로 실행되었습니다."

            cursor.close()
            connection.close()

            if tunnel_used:
                self.cleanup_ssh_tunnel()

            return result

        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            if error_code == "ExpiredTokenException":
                return f"❌ AWS 토큰이 만료되었습니다. 'aws configure'로 자격 증명을 다시 설정해주세요."
            elif error_code == "UnauthorizedOperation":
                return f"❌ AWS 권한이 부족합니다. Secrets Manager 및 Bedrock 접근 권한을 확인해주세요."
            else:
                return f"❌ AWS 서비스 오류: {str(e)}"
        except MySQLError as e:
            if tunnel_used:
                self.cleanup_ssh_tunnel()
            return f"❌ 데이터베이스 연결 오류: {str(e)}"
        except Exception as e:
            if "tunnel_used" in locals() and tunnel_used:
                self.cleanup_ssh_tunnel()
            return f"❌ Text-to-SQL 실행 실패: {str(e)}"

    async def generate_sql_with_claude(
        self, natural_query: str, schema_info: dict
    ) -> str:
        """Claude를 사용하여 자연어를 SQL로 변환"""
        try:
            # 스키마 정보를 문자열로 변환
            schema_text = "데이터베이스 스키마 정보:\n\n"
            for table_name, columns in schema_info.items():
                schema_text += f"테이블: {table_name}\n"
                for col in columns:
                    col_name, data_type, is_nullable, key, extra, comment = col
                    key_info = f" [{key}]" if key else ""
                    extra_info = f" {extra}" if extra else ""
                    comment_info = f" -- {comment}" if comment else ""
                    schema_text += f"  - {col_name}: {data_type}{key_info}{extra_info}{comment_info}\n"
                schema_text += "\n"

            prompt = f"""당신은 Aurora MySQL 8.0 전문가입니다. 주어진 스키마 정보를 바탕으로 자연어 질문을 정확한 SQL 쿼리로 변환해주세요.

{schema_text}

자연어 질문: {natural_query}

요구사항:
1. Aurora MySQL 8.0 문법을 사용하세요
2. 주어진 스키마의 테이블과 컬럼만 사용하세요
3. 적절한 JOIN, WHERE, ORDER BY 등을 사용하세요
4. SQL 쿼리만 반환하고 다른 설명은 포함하지 마세요
5. 세미콜론(;)으로 끝내세요

SQL 쿼리:"""

            # Claude 호출
            bedrock_client = boto3.client("bedrock-runtime", region_name="us-west-2")

            body = {
                "anthropic_version": "bedrock-2023-05-31",
                "max_tokens": 1000,
                "messages": [{"role": "user", "content": prompt}],
            }

            response = bedrock_client.invoke_model(
                modelId="us.anthropic.claude-sonnet-4-20250514-v1:0",
                body=json.dumps(body),
            )

            response_body = json.loads(response["body"].read())
            sql_query = response_body["content"][0]["text"].strip()

            # SQL 쿼리 정리 (코드 블록 제거 등)
            if sql_query.startswith("```sql"):
                sql_query = sql_query[6:]
            if sql_query.endswith("```"):
                sql_query = sql_query[:-3]
            sql_query = sql_query.strip()

            return sql_query

        except Exception as e:
            return f"❌ SQL 생성 실패: {str(e)}"

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

    async def validate_sql_files(
        self, filenames: List[str], database_secret: Optional[str] = None
    ) -> str:
        """복수 SQL 파일 검증 및 통합 보고서 생성"""
        try:
            if not filenames:
                return "검증할 SQL 파일이 없습니다."

            # 개별 파일 검증
            results = []
            for filename in filenames:
                logger.info(f"검증 중: {filename}")
                result = await self.validate_sql_file(filename, database_secret)
                results.append(f"✅ {filename}: {result.split(chr(10))[0]}")  # 첫 줄만

            # 2개 이상이면 통합 보고서 생성
            if len(filenames) >= 2:
                consolidated_report = await self.auto_generate_consolidated_report()
                results.append("\n" + consolidated_report)

            return "\n".join(results)
        except Exception as e:
            return f"SQL 파일들 검증 실패: {str(e)}"

    async def auto_generate_consolidated_report(self) -> str:
        """최근 생성된 개별 보고서들을 수집해서 통합 보고서 생성"""
        try:
            import re
            from datetime import timedelta

            # 최근 5분 내에 생성된 validation_report 찾기
            now = datetime.now()
            recent_reports = []

            for html_file in OUTPUT_DIR.glob('validation_report_*.html'):
                mtime = datetime.fromtimestamp(html_file.stat().st_mtime)
                if now - mtime < timedelta(minutes=5):
                    recent_reports.append(html_file)

            if len(recent_reports) < 2:
                return "통합 보고서 생성 조건 미달 (최근 보고서 2개 미만)"

            recent_reports.sort(key=lambda x: x.stat().st_mtime)

            # 통계 계산
            passed_count = 0
            failed_count = 0
            report_rows = ''

            for i, report_file in enumerate(recent_reports, 1):
                with open(report_file, 'r', encoding='utf-8') as f:
                    content = f.read()

                # 파일명 추출
                filename = report_file.name.replace('validation_report_', '').replace('.html', '')
                filename_match = re.match(r'(.+?)_\d{8}_\d{6}$', filename)
                if filename_match:
                    sql_filename = filename_match.group(1)
                else:
                    sql_filename = filename

                # 상태 확인
                if 'FAIL' in content and '<div class="status-badge">' in content:
                    status = 'FAIL'
                    status_icon = '❌'
                    status_color = '#dc3545'
                    failed_count += 1
                else:
                    status = 'PASS'
                    status_icon = '✅'
                    status_color = '#28a745'
                    passed_count += 1

                # SQL 타입 추출
                sql_type_match = re.search(r'<h4>🔧 SQL 타입</h4>\s*<p>([^<]+)</p>', content)
                sql_type = sql_type_match.group(1) if sql_type_match else 'UNKNOWN'

                report_rows += f'''
    <tr>
        <td>{i}</td>
        <td><a href="{report_file.name}" target="_blank">{sql_filename}</a></td>
        <td>{sql_type}</td>
        <td style="color: {status_color}; font-weight: bold;">{status_icon} {status}</td>
    </tr>
    '''

            total_files = len(recent_reports)
            pass_rate = (passed_count / total_files * 100 if total_files > 0 else 0)

            # 통합 보고서 HTML 생성
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            consolidated_file = OUTPUT_DIR / f'consolidated_validation_report_{timestamp}.html'

            html_content = f'''<!DOCTYPE html>
<html lang="ko">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>SQL 통합 검증 보고서</title>
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
            text-align: center;
        }}
        .summary-item h4 {{
            margin: 0 0 10px 0;
            color: #333;
        }}
        .summary-item p {{
            margin: 0;
            font-size: 2em;
            font-weight: bold;
        }}
        .pass {{ color: #28a745; }}
        .fail {{ color: #dc3545; }}
        table {{
            width: 100%;
            border-collapse: collapse;
            margin: 20px 0;
        }}
        th, td {{
            padding: 12px;
            text-align: left;
            border-bottom: 1px solid #e9ecef;
        }}
        th {{
            background-color: #667eea;
            color: white;
        }}
        tr:hover {{
            background-color: #f8f9fa;
        }}
        a {{
            color: #667eea;
            text-decoration: none;
        }}
        a:hover {{
            text-decoration: underline;
        }}
        .footer {{
            background: #f8f9fa;
            padding: 20px;
            text-align: center;
            color: #6c757d;
            border-top: 1px solid #e9ecef;
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>📊 SQL 통합 검증 보고서</h1>
            <p>생성 일시: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        </div>

        <div class="content">
            <div class="summary-grid">
                <div class="summary-item">
                    <h4>📁 총 파일 수</h4>
                    <p>{total_files}</p>
                </div>
                <div class="summary-item">
                    <h4 class="pass">✅ 통과</h4>
                    <p class="pass">{passed_count}</p>
                </div>
                <div class="summary-item">
                    <h4 class="fail">❌ 실패</h4>
                    <p class="fail">{failed_count}</p>
                </div>
                <div class="summary-item">
                    <h4>📈 통과율</h4>
                    <p>{pass_rate:.1f}%</p>
                </div>
            </div>

            <h2>📋 검증 결과 상세</h2>
            <table>
                <thead>
                    <tr>
                        <th>#</th>
                        <th>파일명</th>
                        <th>SQL 타입</th>
                        <th>검증 결과</th>
                    </tr>
                </thead>
                <tbody>
                    {report_rows}
                </tbody>
            </table>

            <p style="color: #6c757d; font-size: 0.9em;">
                💡 파일명을 클릭하면 상세 검증 보고서를 확인할 수 있습니다.
            </p>
        </div>

        <div class="footer">
            <p>Generated by DB Assistant MCP Server</p>
            <p>Report generated at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        </div>
    </div>
</body>
</html>
'''

            # 파일 저장
            with open(consolidated_file, 'w', encoding='utf-8') as f:
                f.write(html_content)

            logger.info(f"통합 보고서 생성 완료: {consolidated_file}")

            return f"📊 통합 검증 보고서가 생성되었습니다: {consolidated_file}\n   총 {total_files}개 파일, 통과: {passed_count}, 실패: {failed_count}, 통과율: {pass_rate:.1f}%"

        except Exception as e:
            logger.error(f"통합 보고서 생성 오류: {e}")
            return f"통합 보고서 생성 실패: {str(e)}"

    async def validate_ddl(
        self, ddl_content: str, database_secret: Optional[str], filename: str
    ) -> str:
        """DDL/DML 검증 실행 (연결 재사용 패턴 적용)"""
        try:
            # filename에서 basename만 추출 (전체 경로가 올 수 있음)
            import os
            base_filename = os.path.basename(filename)

            # 디버그 로그 파일 생성
            debug_log_path = (
                LOGS_DIR
                / f"debug_log_{base_filename}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
            )

            debug_log(f"validate_ddl 시작 - 파일: {base_filename}")
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
            sql_type = self.sql_parser.extract_ddl_type(ddl_content, debug_log)
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
                    debug_log("Lambda 기반 검증 시작 (로컬 DB 연결 없음)")

                    # SQL 타입별 검증 분기
                    if sql_type in skip_types:
                        debug_log(
                            f"SQL 타입 스킵: {sql_type} (SHOW/SET/USE 구문은 검증하지 않음)"
                        )

                    # DDL 검증
                    elif sql_type in ddl_types:
                        debug_log(f"DDL 검증 수행: {sql_type}")
                        debug_log("=== Lambda 스키마 검증 로직 시작 ===")

                        # Lambda 스키마 검증으로 변경
                        ddl_validation = await self.validate_schema_lambda(
                            database_secret,
                            self.selected_database,
                            ddl_content
                        )
                        debug_log(
                            f"Lambda 스키마 검증 완료: Success={ddl_validation.get('success')}, Valid={ddl_validation.get('valid')}, Issues={len(ddl_validation.get('issues', []))}"
                        )

                        # Lambda 결과 처리
                        if ddl_validation.get('success'):
                            if ddl_validation.get('issues'):
                                issues.extend(ddl_validation['issues'])
                            if ddl_validation.get('warnings'):
                                # 경고는 issues에 추가하지 않고 로그만
                                debug_log(f"경고: {ddl_validation['warnings']}")
                        else:
                            # Lambda 호출 실패
                            error_msg = ddl_validation.get('error', 'Lambda 스키마 검증 실패')
                            issues.append(f"스키마 검증 오류: {error_msg}")
                            debug_log(f"Lambda 스키마 검증 오류: {error_msg}")

                    # DQL(DML) 검증 - MIXED_SELECT 포함
                    elif sql_type in dql_types:
                        debug_log(f"DQL 검증 수행: {sql_type}")

                        # MIXED_SELECT인 경우 DDL과 DML 모두 검증
                        if sql_type == "MIXED_SELECT":
                            debug_log("=== 혼합 SQL 파일 검증 시작 ===")

                            # 1. DDL 구문 검증 (Lambda 사용)
                            debug_log("혼합 파일 내 Lambda 스키마 검증 시작")
                            ddl_validation = await self.validate_schema_lambda(
                                database_secret,
                                self.selected_database,
                                ddl_content
                            )

                            # 타입 체크: ddl_validation이 딕셔너리가 아닌 경우 처리
                            if not isinstance(ddl_validation, dict):
                                logger.error(f"ddl_validation이 딕셔너리가 아님: {type(ddl_validation)}, 내용: {str(ddl_validation)[:200]}")
                                issues.append(f"스키마 검증 오류: Lambda 응답 형식 오류 (타입: {type(ddl_validation).__name__})")
                                ddl_validation = {'success': False, 'error': f'응답 타입 오류: {type(ddl_validation).__name__}'}

                            debug_log(
                                f"혼합 파일 Lambda 스키마 검증 완료: Success={ddl_validation.get('success')}, Valid={ddl_validation.get('valid')}, Issues={len(ddl_validation.get('issues', []))}"
                            )

                            # Lambda 결과 처리
                            if ddl_validation.get('success'):
                                if ddl_validation.get('issues'):
                                    issues.extend(ddl_validation['issues'])
                                if ddl_validation.get('warnings'):
                                    debug_log(f"경고: {ddl_validation['warnings']}")
                            else:
                                error_msg = ddl_validation.get('error', 'Lambda 스키마 검증 실패')
                                issues.append(f"스키마 검증 오류: {error_msg}")
                                debug_log(f"Lambda 스키마 검증 오류: {error_msg}")

                        # DML 검증 (Lambda EXPLAIN 사용)
                        debug_log("=== Lambda EXPLAIN 검증 시작 ===")

                        # 파일 내 생성된 테이블 목록 추출 (임시 스키마 시뮬레이션)
                        created_tables = set()
                        skipped_queries = []  # 스킵된 쿼리 정보 (Claude 분석용)

                        try:
                            ddl_statements = self.sql_parser.parse_ddl_detailed(ddl_content)
                            for stmt in ddl_statements:
                                if stmt.get('type') == 'CREATE_TABLE':
                                    table_name = stmt.get('table', '').lower()
                                    if table_name:
                                        created_tables.add(table_name)
                            debug_log(f"파일 내 생성된 테이블 목록: {created_tables}")
                        except Exception as parse_error:
                            debug_log(f"DDL 파싱 오류 (무시하고 계속): {parse_error}")

                        # SQL을 개별 쿼리로 분리
                        if sqlparse:
                            statements = sqlparse.split(ddl_content)
                        else:
                            statements = [
                                stmt.strip() for stmt in ddl_content.split(";") if stmt.strip()
                            ]
                        debug_log(f"총 {len(statements)}개의 개별 쿼리로 분리")

                        # 각 SELECT 쿼리에 대해 Lambda EXPLAIN 실행
                        for i, stmt in enumerate(statements):
                            if not stmt.strip():
                                continue

                            # 주석 제거
                            cleaned_stmt = re.sub(r"--.*$", "", stmt, flags=re.MULTILINE)
                            cleaned_stmt = re.sub(r"/\*.*?\*/", "", cleaned_stmt, flags=re.DOTALL)
                            cleaned_stmt = cleaned_stmt.strip()

                            if not cleaned_stmt:
                                continue

                            # DML 쿼리만 EXPLAIN 실행 (SELECT, UPDATE, DELETE, INSERT, REPLACE)
                            dml_pattern = re.match(r"^\s*(SELECT|UPDATE|DELETE|INSERT|REPLACE)", cleaned_stmt, re.IGNORECASE)
                            if not dml_pattern:
                                debug_log(f"쿼리 {i+1}: DML 쿼리가 아니므로 EXPLAIN 스킵")
                                continue

                            # 파일 내 생성된 테이블을 참조하는지 확인
                            references_new_table = False
                            referenced_tables = []
                            for table in created_tables:
                                # FROM, JOIN, INTO 절에서 테이블 참조 확인
                                if re.search(rf'\b(FROM|JOIN|INTO)\s+`?{table}`?\b', cleaned_stmt, re.IGNORECASE):
                                    references_new_table = True
                                    referenced_tables.append(table)

                            # 새 테이블 참조 시 EXPLAIN 스킵
                            if references_new_table:
                                debug_log(f"쿼리 {i+1}: 파일 내 생성된 테이블 참조 ({', '.join(referenced_tables)}) - EXPLAIN 스킵")
                                # 스킵된 쿼리 정보 기록 (Claude 분석용)
                                skipped_queries.append({
                                    'query_num': i+1,
                                    'query': cleaned_stmt[:100] + ('...' if len(cleaned_stmt) > 100 else ''),
                                    'tables': referenced_tables
                                })
                                continue

                            debug_log(f"쿼리 {i+1}: Lambda EXPLAIN 실행 중...")
                            explain_result = await self.explain_query_lambda(
                                database_secret,
                                self.selected_database,
                                cleaned_stmt
                            )

                            # 타입 체크: explain_result가 딕셔너리가 아닌 경우 처리
                            if not isinstance(explain_result, dict):
                                logger.error(f"쿼리 {i+1} explain_result가 딕셔너리가 아님: {type(explain_result)}, 내용: {str(explain_result)[:200]}")
                                issues.append(f"쿼리 {i+1} EXPLAIN 오류: Lambda 응답 형식 오류 (타입: {type(explain_result).__name__})")
                                continue

                            # Lambda 결과 처리
                            if explain_result.get('success'):
                                debug_log(f"쿼리 {i+1}: Lambda EXPLAIN 성공 - {len(explain_result.get('performance_issues', []))}개 성능 이슈")

                                # 성능 이슈가 있으면 issues에 추가
                                if explain_result.get('performance_issues'):
                                    for perf_issue in explain_result['performance_issues']:
                                        if "❌" in perf_issue or "실패" in perf_issue:
                                            issues.append(f"쿼리 {i+1}: {perf_issue}")
                                            debug_log(f"성능 이슈 추가: {perf_issue}")
                            else:
                                error_msg = explain_result.get('error', 'Lambda EXPLAIN 실패')
                                debug_log(f"쿼리 {i+1}: Lambda EXPLAIN 오류 - {error_msg}")
                                issues.append(f"쿼리 {i+1} EXPLAIN 오류: {error_msg}")

                        debug_log("=== Lambda EXPLAIN 검증 완료 ===")

                    else:
                        debug_log(f"알 수 없는 SQL 타입: {sql_type}")

                except Exception as e:
                    debug_log(f"데이터베이스 검증 오류: {e}")
                    issues.append(f"데이터베이스 검증 오류: {str(e)}")

            # Lambda 검증 결과를 기반으로 Claude 분석
            dml_column_issues = []  # 빈 리스트 유지 (기존 코드 호환성)

            # 4. Claude를 통한 검증
            try:
                debug_log("Claude 검증 시작 (Lambda 결과 기반)")

                # 스키마 검증 결과 요약 생성
                schema_validation_summary = self.create_schema_validation_summary(
                    issues, dml_column_issues
                )
                debug_log(f"스키마 검증 요약 생성: {schema_validation_summary}")

                # 스킵된 쿼리 정보 확인 (DML 검증에서 정의되었을 경우)
                skipped_info = locals().get('skipped_queries', [])
                debug_log(f"스킵된 쿼리 개수: {len(skipped_info)}")

                # Claude 검증 (스키마 정보는 Lambda에서 이미 확인했으므로 불필요)
                claude_result = await self.validate_with_claude(
                    ddl_content,
                    database_secret,
                    None,  # relevant_schema_info 제거
                    None,  # explain_info_str 제거
                    sql_type,
                    schema_validation_summary,
                    skipped_queries=skipped_info,  # 스킵된 쿼리 정보 전달
                )
                debug_log(f"Claude 검증 결과: {claude_result}")

                # Claude 결과를 항상 저장 (성공/실패 상관없이)
                claude_analysis_result = claude_result

                # Claude 응답 분석 - 더 엄격한 검증
                if "오류:" in claude_result or "존재하지 않" in claude_result:
                    issues.append(f"Claude 검증: {claude_result}")
                    debug_log("Claude 검증에서 오류 발견")
                elif "검증 통과" in claude_result:
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
                # 성능 문제와 기타 문제 분류
                performance_issues = [
                    issue for issue in issues if "심각한 성능 문제" in str(issue)
                ]
                claude_issues = [
                    issue for issue in issues if "Claude 검증:" in str(issue)
                ]
                other_issues = [
                    issue
                    for issue in issues
                    if issue not in performance_issues and issue not in claude_issues
                ]

                # 문제 요약 생성
                problem_parts = []
                if performance_issues:
                    unique_performance = len(
                        set(str(issue) for issue in performance_issues)
                    )
                    if unique_performance == 1:
                        problem_parts.append("성능 문제")
                    else:
                        problem_parts.append(f"성능 문제 {unique_performance}건")

                if claude_issues:
                    problem_parts.append("AI 분석 문제")

                if other_issues:
                    problem_parts.append(f"기타 문제 {len(other_issues)}건")

                if (
                    len(problem_parts) == 1
                    and "성능 문제" in problem_parts[0]
                    and not other_issues
                    and not claude_issues
                ):
                    summary = "❌ 심각한 성능 문제 발견"
                else:
                    summary = f"❌ 발견된 문제: {', '.join(problem_parts)}"

                status = "FAIL"

            debug_log(f"최종 상태: {status}, 요약: {summary}")

            # 보고서 생성 (HTML 형식)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            # filename에서 basename만 추출 (전체 경로가 올 수 있음)
            import os
            base_filename = os.path.basename(filename)
            report_path = OUTPUT_DIR / f"validation_report_{base_filename}_{timestamp}.html"

            # HTML 보고서 생성
            debug_log("HTML 보고서 생성 시작")
            debug_log(f"dml_column_issues 값: {dml_column_issues}")
            debug_log(f"report_path 값: {report_path}")

            # output 디렉토리 생성 (없을 경우)
            OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

            try:
                await self.generate_html_report(
                    report_path,
                    base_filename,  # 전체 경로가 아닌 basename만 전달
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

            return f"{summary}\n\n📄 상세 보고서가 저장되었습니다: {report_path}\n🔍 디버그 로그: {debug_log_path}"

        except Exception as e:
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
        skipped_queries: list = None,
    ) -> str:
        """
        Claude cross-region 프로파일을 활용한 DDL 검증 (실제 스키마 정보 포함)

        Args:
            skipped_queries: 파일 내 생성된 테이블을 참조하여 EXPLAIN이 스킵된 쿼리 목록
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
            # 타입 체크: schema_info의 값이 딕셔너리인지 확인
            try:
                sorted_items = sorted(
                    schema_info.items(),
                    key=lambda x: x[1].get("order", 0) if isinstance(x[1], dict) else 0
                )
            except Exception as sort_error:
                logger.error(f"schema_info 정렬 오류: {sort_error}, schema_info 타입: {type(schema_info)}")
                # 정렬 실패 시 그대로 사용
                sorted_items = list(schema_info.items()) if isinstance(schema_info, dict) else []

            for key, info in sorted_items:
                # 타입 체크: info가 딕셔너리가 아닌 경우 스킵
                if not isinstance(info, dict):
                    logger.warning(f"schema_info[{key}]가 딕셔너리가 아님: {type(info)}")
                    continue

                order = info.get("order", 0)

                if info.get("type") == "table":
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
                            f"[{order}] {action} TABLE '{table_name}': DB존재={info.get('exists', 'Unknown')}"
                        )

                elif info.get("type") == "index":
                    table_name = info.get("table_name", key.split(".")[0])
                    index_name = info.get("index_name", "Unknown")

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
스키마 검증에서 문제가 발견된 경우, 실패로 결론을 내리고 왜 문제가 나왔는지도 설명하면서 검증해주세요.
"""

        # 스킵된 쿼리 정보 컨텍스트 추가 (임시 스키마 시뮬레이션)
        skipped_queries_context = ""
        if skipped_queries and len(skipped_queries) > 0:
            skipped_details = []
            for sq in skipped_queries:
                query_summary = sq.get('query', '')
                tables = ', '.join(sq.get('tables', []))
                skipped_details.append(f"  - 쿼리 {sq.get('query_num')}: {query_summary}")
                skipped_details.append(f"    참조 테이블: {tables}")

            skipped_queries_context = f"""
**중요: 성능 검증이 스킵된 쿼리**

다음 쿼리들은 파일 내에서 생성된 테이블을 참조하므로, 실제 데이터베이스에 테이블이 존재하지 않아 EXPLAIN 성능 분석을 수행하지 못했습니다:

{chr(10).join(skipped_details)}

**검증 지침:**
1. 위 쿼리들은 **문법 검증만 수행**하고, 성능 검증은 수행하지 않았습니다.
2. 테이블이 파일 내에서 생성되므로 "테이블 존재 여부"는 문제가 아닙니다.
3. 성능 분석 결과가 없으므로, **성능 문제로 인한 실패 판정은 하지 마세요**.
4. 문법적으로 올바르다면 "검증 통과 (성능 분석 미실행: 파일 내 생성 테이블 참조)"로 표시하세요.
5. 사용자에게 "실제 테이블 생성 후 별도의 성능 검증이 필요합니다"라고 안내하세요.
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

        {skipped_queries_context}

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

        {skipped_queries_context}

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

        {skipped_queries_context}

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

            # response_body가 딕셔너리인지 확인
            if not isinstance(response_body, dict):
                logger.error(f"Claude 응답이 딕셔너리가 아님: {type(response_body)}")
                return f"Claude 응답 형식 오류: 응답이 딕셔너리가 아닙니다 (타입: {type(response_body).__name__})"

            # 안전한 타입 체크 및 데이터 추출
            content = response_body.get("content", [])
            if isinstance(content, list) and len(content) > 0:
                first_content = content[0]
                if isinstance(first_content, dict):
                    text_result = first_content.get("text", "")
                    if text_result:
                        return text_result
                    else:
                        logger.error("Claude 응답의 text 필드가 비어있음")
                        return "Claude 응답 형식 오류: text 필드가 비어있음"
                else:
                    logger.error(f"Claude 응답의 content[0]이 딕셔너리가 아님: {type(first_content)}")
                    return f"Claude 응답 형식 오류: content[0]의 타입이 {type(first_content).__name__}입니다"
            else:
                logger.error(f"Claude 응답의 content가 비어있거나 리스트가 아님: {type(content)}")
                return f"Claude 응답 형식 오류: content가 비어있거나 리스트가 아닙니다"

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

                # response_body가 딕셔너리인지 확인
                if not isinstance(response_body, dict):
                    logger.error(f"Claude 3.7 응답이 딕셔너리가 아님: {type(response_body)}")
                    return f"Claude 응답 형식 오류: 응답이 딕셔너리가 아닙니다 (타입: {type(response_body).__name__})"

                # 안전한 타입 체크 및 데이터 추출
                content = response_body.get("content", [])
                if isinstance(content, list) and len(content) > 0:
                    first_content = content[0]
                    if isinstance(first_content, dict):
                        text_result = first_content.get("text", "")
                        if text_result:
                            return text_result
                        else:
                            logger.error("Claude 3.7 응답의 text 필드가 비어있음")
                            return "Claude 응답 형식 오류: text 필드가 비어있음"
                    else:
                        logger.error(f"Claude 3.7 응답의 content[0]이 딕셔너리가 아님: {type(first_content)}")
                        return f"Claude 응답 형식 오류: content[0]의 타입이 {type(first_content).__name__}입니다"
                else:
                    logger.error(f"Claude 3.7 응답의 content가 비어있거나 리스트가 아님: {type(content)}")
                    return f"Claude 응답 형식 오류: content가 비어있거나 리스트가 아닙니다"

            except Exception as e:
                logger.error(f"Claude 3.7 Sonnet 호출 오류: {e}")
                return f"Claude 호출 중 오류 발생: {str(e)}"

    async def generate_performance_recommendations_with_claude(
        self,
        metrics_summary: str,
        correlation_analysis: str,
        outliers_analysis: str,
        slow_queries: str,
        cpu_queries: str,
        temp_queries: str,
        database_secret: str = None,
    ) -> Dict[str, Any]:
        """
        Claude를 활용하여 성능 메트릭과 쿼리 분석을 기반으로 동적 권장사항 생성
        """
        try:
            # Knowledge Base에서 성능 최적화 가이드 조회
            knowledge_context = ""
            try:
                knowledge_info = await self.query_knowledge_base(
                    "database performance optimization recommendations", "PERFORMANCE"
                )
                if knowledge_info and knowledge_info != "관련 정보를 찾을 수 없습니다.":
                    knowledge_context = f"""
Knowledge Base 성능 최적화 가이드:
{knowledge_info}

위 가이드를 참고하여 권장사항을 생성해주세요.
"""
            except Exception as e:
                logger.warning(f"Knowledge Base 조회 중 오류: {e}")

            prompt = f"""
다음 데이터베이스 성능 분석 결과를 바탕으로 구체적이고 실행 가능한 최적화 권장사항과 액션 아이템을 생성해주세요:

**메트릭 요약:**
{metrics_summary}

**상관관계 분석:**
{correlation_analysis}

**이상 징후 분석:**
{outliers_analysis}

**느린 쿼리 분석:**
{slow_queries}

**CPU 집약적 쿼리:**
{cpu_queries}

**임시 공간 집약적 쿼리:**
{temp_queries}

{knowledge_context}

**요구사항:**
1. 위 분석 결과를 종합하여 실제 데이터에 기반한 구체적인 권장사항을 제시하세요
2. 우선순위별로 분류하여 즉시 적용 가능한 개선사항을 제안하세요
3. 각 권장사항에 대해 예상 효과와 구현 난이도를 포함하세요
4. 액션 아이템은 담당자, 예상 소요시간, 우선순위를 포함하여 구체적으로 작성하세요

**응답 형식 (JSON):**
{{
    "immediate_improvements": [
        {{
            "category": "모니터링/성능/용량계획",
            "title": "구체적인 개선사항 제목",
            "description": "상세 설명",
            "items": ["구체적인 실행 항목1", "구체적인 실행 항목2"],
            "expected_impact": "예상 효과",
            "difficulty": "낮음/중간/높음"
        }}
    ],
    "action_items": [
        {{
            "priority": "높음/중간/낮음",
            "item": "구체적인 액션 아이템",
            "estimated_time": "예상 소요시간",
            "assignee": "담당자 역할",
            "rationale": "이 액션이 필요한 이유"
        }}
    ]
}}

반드시 위 JSON 형식으로만 응답하세요. 분석 결과에서 실제 발견된 문제점을 기반으로 구체적인 권장사항을 제시하세요.
"""

            claude_input = json.dumps(
                {
                    "anthropic_version": "bedrock-2023-05-31",
                    "max_tokens": 4096,
                    "messages": [
                        {"role": "user", "content": [{"type": "text", "text": prompt}]}
                    ],
                    "temperature": 0.3,
                }
            )

            sonnet_4_model_id = "us.anthropic.claude-sonnet-4-20250514-v1:0"
            sonnet_3_7_model_id = "us.anthropic.claude-3-7-sonnet-20250219-v1:0"

            # Claude Sonnet 4 호출
            try:
                logger.info(f"Claude Sonnet 4 호출 시작 - 모델ID: {sonnet_4_model_id}")
                logger.debug(f"입력 데이터 크기: {len(claude_input)} bytes")

                response = self.bedrock_client.invoke_model(
                    modelId=sonnet_4_model_id, body=claude_input
                )
                logger.info("Claude Sonnet 4 응답 수신 완료")

                response_body = json.loads(response.get("body").read())
                logger.debug(f"응답 본문 파싱 완료: {list(response_body.keys())}")

                claude_response = response_body.get("content", [{}])[0].get("text", "")
                logger.info(
                    f"Claude 응답 텍스트 길이: {len(claude_response)} characters"
                )
                logger.debug(f"Claude 응답 미리보기: {claude_response[:200]}...")

                # JSON 파싱 시도 - 먼저 마크다운 코드 블록 확인
                try:
                    # 마크다운 코드 블록에서 JSON 추출 시도
                    import re

                    markdown_pattern = r"```(?:json)?\s*(.*?)\s*```"
                    markdown_match = re.search(
                        markdown_pattern, claude_response, re.DOTALL | re.IGNORECASE
                    )

                    if markdown_match:
                        json_content = markdown_match.group(1).strip()
                        logger.info(
                            f"마크다운 블록에서 JSON 추출, 길이: {len(json_content)}"
                        )
                        parsed_result = json.loads(json_content)
                    else:
                        # 마크다운 블록이 없으면 직접 파싱 시도
                        logger.info("마크다운 블록 없음, 직접 JSON 파싱 시도")
                        parsed_result = json.loads(claude_response)

                    logger.info("Claude 응답 JSON 파싱 성공")
                    logger.debug(f"파싱된 결과 키: {list(parsed_result.keys())}")

                    # 필요한 키가 있는지 확인
                    if isinstance(parsed_result, dict) and (
                        "immediate_improvements" in parsed_result
                        or "action_items" in parsed_result
                    ):
                        improvements_count = len(
                            parsed_result.get("immediate_improvements", [])
                        )
                        actions_count = len(parsed_result.get("action_items", []))
                        logger.info(
                            f"유효한 권장사항 파싱 완료: {improvements_count}개 개선사항, {actions_count}개 액션아이템"
                        )
                        return parsed_result
                    else:
                        logger.warning("파싱된 JSON에 필요한 키가 없음")
                        return self._get_default_recommendations()

                except json.JSONDecodeError as json_err:
                    logger.error(f"Claude Sonnet 4 응답 JSON 파싱 실패: {json_err}")
                    logger.info("텍스트 파싱 시도")
                    # 텍스트에서 JSON 추출 시도
                    parsed_result = self._parse_claude_text_response(claude_response)
                    if parsed_result:
                        logger.info("텍스트 파싱 성공")
                        return parsed_result
                    logger.error(f"파싱 실패한 응답 내용: {claude_response}")
                    return self._get_default_recommendations()

            except Exception as e:
                logger.error(
                    f"Claude Sonnet 4 호출 실패 - 에러 타입: {type(e).__name__}"
                )
                logger.error(f"Claude Sonnet 4 호출 실패 - 에러 메시지: {str(e)}")
                if hasattr(e, "response"):
                    logger.error(
                        f"AWS 응답 코드: {e.response.get('Error', {}).get('Code', 'Unknown')}"
                    )
                    logger.error(
                        f"AWS 응답 메시지: {e.response.get('Error', {}).get('Message', 'Unknown')}"
                    )

                # Claude 3.7 Sonnet 호출 (fallback)
                try:
                    logger.info(
                        f"Claude 3.7 Sonnet fallback 시작 - 모델ID: {sonnet_3_7_model_id}"
                    )

                    response = self.bedrock_client.invoke_model(
                        modelId=sonnet_3_7_model_id, body=claude_input
                    )
                    logger.info("Claude 3.7 Sonnet 응답 수신 완료")

                    response_body = json.loads(response.get("body").read())
                    claude_response = response_body.get("content", [{}])[0].get(
                        "text", ""
                    )
                    logger.info(
                        f"Claude 3.7 응답 텍스트 길이: {len(claude_response)} characters"
                    )

                    try:
                        # 마크다운 코드 블록에서 JSON 추출 시도
                        import re

                        markdown_pattern = r"```(?:json)?\s*(.*?)\s*```"
                        markdown_match = re.search(
                            markdown_pattern, claude_response, re.DOTALL | re.IGNORECASE
                        )

                        if markdown_match:
                            json_content = markdown_match.group(1).strip()
                            logger.info(
                                f"Claude 3.7 마크다운 블록에서 JSON 추출, 길이: {len(json_content)}"
                            )
                            parsed_result = json.loads(json_content)
                        else:
                            # 마크다운 블록이 없으면 직접 파싱 시도
                            logger.info(
                                "Claude 3.7 마크다운 블록 없음, 직접 JSON 파싱 시도"
                            )
                            parsed_result = json.loads(claude_response)

                        logger.info("Claude 3.7 응답 JSON 파싱 성공")

                        # 필요한 키가 있는지 확인
                        if isinstance(parsed_result, dict) and (
                            "immediate_improvements" in parsed_result
                            or "action_items" in parsed_result
                        ):
                            improvements_count = len(
                                parsed_result.get("immediate_improvements", [])
                            )
                            actions_count = len(parsed_result.get("action_items", []))
                            logger.info(
                                f"Claude 3.7 유효한 권장사항 파싱 완료: {improvements_count}개 개선사항, {actions_count}개 액션아이템"
                            )
                            return parsed_result
                        else:
                            logger.warning("Claude 3.7 파싱된 JSON에 필요한 키가 없음")
                            return self._get_default_recommendations()

                    except json.JSONDecodeError as json_err:
                        logger.error(f"Claude 3.7 응답 JSON 파싱 실패: {json_err}")
                        logger.info("Claude 3.7 텍스트 파싱 시도")
                        # 텍스트에서 JSON 추출 시도
                        parsed_result = self._parse_claude_text_response(
                            claude_response
                        )
                        if parsed_result:
                            logger.info("Claude 3.7 텍스트 파싱 성공")
                            return parsed_result
                        logger.error(f"파싱 실패한 응답 내용: {claude_response}")
                        return self._get_default_recommendations()

                except Exception as fallback_e:
                    logger.error(
                        f"Claude 3.7 Sonnet fallback 실패 - 에러 타입: {type(fallback_e).__name__}"
                    )
                    logger.error(
                        f"Claude 3.7 Sonnet fallback 실패 - 에러 메시지: {str(fallback_e)}"
                    )
                    if hasattr(fallback_e, "response"):
                        logger.error(
                            f"AWS fallback 응답 코드: {fallback_e.response.get('Error', {}).get('Code', 'Unknown')}"
                        )
                        logger.error(
                            f"AWS fallback 응답 메시지: {fallback_e.response.get('Error', {}).get('Message', 'Unknown')}"
                        )
                    return self._get_default_recommendations()

        except Exception as e:
            logger.error(
                f"성능 권장사항 생성 중 전체 오류 - 에러 타입: {type(e).__name__}"
            )
            logger.error(f"성능 권장사항 생성 중 전체 오류 - 에러 메시지: {str(e)}")
            logger.error(
                f"성능 권장사항 생성 중 전체 오류 - 스택 트레이스:", exc_info=True
            )
            return self._get_default_recommendations()

    def _parse_claude_text_response(
        self, text_response: str
    ) -> Optional[Dict[str, Any]]:
        """Claude 텍스트 응답에서 JSON 추출 및 파싱"""
        try:
            import re

            logger.info(f"Claude 응답 파싱 시작, 응답 길이: {len(text_response)}")
            logger.debug(f"응답 시작 부분: {text_response[:200]}")

            # 먼저 마크다운 코드 블록에서 JSON 추출
            markdown_pattern = r"```(?:json)?\s*(.*?)\s*```"
            markdown_match = re.search(
                markdown_pattern, text_response, re.DOTALL | re.IGNORECASE
            )

            if markdown_match:
                json_content = markdown_match.group(1).strip()
                logger.info(
                    f"마크다운 블록에서 JSON 추출 성공, 길이: {len(json_content)}"
                )
                logger.debug(f"추출된 JSON 시작 부분: {json_content[:200]}")
                try:
                    parsed = json.loads(json_content)
                    if isinstance(parsed, dict) and (
                        "immediate_improvements" in parsed or "action_items" in parsed
                    ):
                        logger.info(
                            f"마크다운 블록 JSON 파싱 성공 - 개선사항: {len(parsed.get('immediate_improvements', []))}개, 액션아이템: {len(parsed.get('action_items', []))}개"
                        )
                        return parsed
                    else:
                        logger.warning("파싱된 JSON에 필요한 키가 없음")
                except json.JSONDecodeError as e:
                    logger.error(f"마크다운 블록 JSON 파싱 실패: {e}")
                    logger.debug(
                        f"파싱 실패한 JSON 내용 (처음 500자): {json_content[:500]}"
                    )
            else:
                logger.warning("마크다운 코드 블록을 찾을 수 없음")

            # 마크다운 블록이 없거나 파싱 실패시 다른 패턴들 시도
            logger.info("대체 JSON 패턴 매칭 시도")
            json_patterns = [
                r'(\{[^{}]*"immediate_improvements"[^{}]*\})',
                r'(\{.*?"immediate_improvements".*?\})',
                r'(\{.*?"action_items".*?\})',
                r"(\{.*?\})",
            ]

            for i, pattern in enumerate(json_patterns):
                logger.debug(f"패턴 {i+1} 시도: {pattern}")
                matches = re.findall(pattern, text_response, re.DOTALL | re.IGNORECASE)
                logger.debug(f"패턴 {i+1}에서 {len(matches)}개 매치 발견")
                for j, match in enumerate(matches):
                    try:
                        parsed = json.loads(match)
                        if isinstance(parsed, dict) and (
                            "immediate_improvements" in parsed
                            or "action_items" in parsed
                        ):
                            logger.info(f"JSON 패턴 매칭 성공: 패턴 {i+1}, 매치 {j+1}")
                            return parsed
                    except json.JSONDecodeError as e:
                        logger.debug(f"패턴 {i+1}, 매치 {j+1} JSON 파싱 실패: {e}")
                        continue

            # 구조화된 텍스트에서 정보 추출
            logger.info("구조화된 텍스트 파싱 시도")
            return self._extract_from_structured_text(text_response)

        except Exception as e:
            logger.error(f"텍스트 파싱 중 오류: {e}")
            return None

    def _extract_from_structured_text(self, text: str) -> Optional[Dict[str, Any]]:
        """구조화된 텍스트에서 권장사항 추출"""
        try:
            import re

            result = {"immediate_improvements": [], "action_items": []}

            # 개선사항 섹션 찾기
            improvements_pattern = r'"immediate_improvements":\s*\[(.*?)\]'
            improvements_match = re.search(improvements_pattern, text, re.DOTALL)

            if improvements_match:
                improvements_text = improvements_match.group(1)
                # 각 개선사항 파싱
                item_pattern = r"\{([^{}]+)\}"
                for item_match in re.finditer(item_pattern, improvements_text):
                    item_text = item_match.group(1)
                    improvement = self._parse_improvement_item(item_text)
                    if improvement:
                        result["immediate_improvements"].append(improvement)

            # 액션 아이템 섹션 찾기
            actions_pattern = r'"action_items":\s*\[(.*?)\]'
            actions_match = re.search(actions_pattern, text, re.DOTALL)

            if actions_match:
                actions_text = actions_match.group(1)
                # 각 액션 아이템 파싱
                for item_match in re.finditer(item_pattern, actions_text):
                    item_text = item_match.group(1)
                    action = self._parse_action_item(item_text)
                    if action:
                        result["action_items"].append(action)

            if result["immediate_improvements"] or result["action_items"]:
                return result

            return None

        except Exception as e:
            logger.error(f"구조화된 텍스트 추출 중 오류: {e}")
            return None

    def _parse_improvement_item(self, item_text: str) -> Optional[Dict[str, Any]]:
        """개선사항 아이템 파싱"""
        try:
            import re

            improvement = {}

            # 각 필드 추출
            fields = {
                "category": r'"category":\s*"([^"]+)"',
                "title": r'"title":\s*"([^"]+)"',
                "description": r'"description":\s*"([^"]+)"',
                "expected_impact": r'"expected_impact":\s*"([^"]+)"',
                "difficulty": r'"difficulty":\s*"([^"]+)"',
            }

            for field, pattern in fields.items():
                match = re.search(pattern, item_text)
                if match:
                    improvement[field] = match.group(1)

            # items 배열 추출
            items_pattern = r'"items":\s*\[(.*?)\]'
            items_match = re.search(items_pattern, item_text, re.DOTALL)
            if items_match:
                items_text = items_match.group(1)
                items = re.findall(r'"([^"]+)"', items_text)
                improvement["items"] = items

            return improvement if improvement else None

        except Exception as e:
            logger.error(f"개선사항 파싱 중 오류: {e}")
            return None

    def _parse_action_item(self, item_text: str) -> Optional[Dict[str, Any]]:
        """액션 아이템 파싱"""
        try:
            import re

            action = {}

            # 각 필드 추출
            fields = {
                "priority": r'"priority":\s*"([^"]+)"',
                "item": r'"item":\s*"([^"]+)"',
                "estimated_time": r'"estimated_time":\s*"([^"]+)"',
                "assignee": r'"assignee":\s*"([^"]+)"',
                "rationale": r'"rationale":\s*"([^"]+)"',
            }

            for field, pattern in fields.items():
                match = re.search(pattern, item_text)
                if match:
                    action[field] = match.group(1)

            return action if action else None

        except Exception as e:
            logger.error(f"액션 아이템 파싱 중 오류: {e}")
            return None

    def _generate_recommendations_html(self, recommendations: Dict[str, Any]) -> str:
        """Claude 권장사항을 HTML로 변환"""
        try:
            html_parts = []

            # 즉시 적용 가능한 개선사항
            html_parts.append("<h4>🚀 즉시 적용 가능한 개선사항</h4>")

            improvements = recommendations.get("immediate_improvements", [])
            if not improvements:
                html_parts.append(
                    '<div class="info-box">현재 분석된 데이터에서는 즉시 적용 가능한 개선사항이 발견되지 않았습니다.</div>'
                )
            else:
                for improvement in improvements:
                    category = improvement.get("category", "기타")
                    title = improvement.get("title", "개선사항")
                    description = improvement.get("description", "")
                    items = improvement.get("items", [])
                    expected_impact = improvement.get("expected_impact", "")
                    difficulty = improvement.get("difficulty", "중간")

                    html_parts.append(
                        f"""
                    <div class="recommendation">
                        <strong>{category}: {title}</strong>
                        <p style="margin: 10px 0; color: #666;">{description}</p>
                        <ul style="margin-top: 10px; margin-left: 20px;">
                    """
                    )

                    for item in items:
                        html_parts.append(f"<li>{item}</li>")

                    html_parts.append(
                        f"""
                        </ul>
                        <div style="margin-top: 10px; font-size: 0.9em;">
                            <span style="color: #27ae60;"><strong>예상 효과:</strong> {expected_impact}</span> | 
                            <span style="color: #3498db;"><strong>구현 난이도:</strong> {difficulty}</span>
                        </div>
                    </div>
                    """
                    )

            # 액션 아이템 테이블
            html_parts.append("<h4>📋 액션 아이템</h4>")

            actions = recommendations.get("action_items", [])
            if not actions:
                html_parts.append(
                    '<div class="info-box">현재 분석된 데이터에서는 특별한 액션 아이템이 필요하지 않습니다.</div>'
                )
            else:
                html_parts.append(
                    """
                    <table class="table">
                        <thead>
                            <tr>
                                <th>우선순위</th>
                                <th>항목</th>
                                <th>예상 소요시간</th>
                                <th>담당자</th>
                                <th>근거</th>
                            </tr>
                        </thead>
                        <tbody>
                """
                )

                for action in actions:
                    priority = action.get("priority", "중간")
                    item = action.get("item", "액션 아이템")
                    estimated_time = action.get("estimated_time", "미정")
                    assignee = action.get("assignee", "담당자")
                    rationale = action.get("rationale", "")

                    # 우선순위에 따른 스타일 클래스
                    priority_class = {
                        "높음": "status-critical",
                        "중간": "status-warning",
                        "낮음": "status-good",
                    }.get(priority, "status-warning")

                    html_parts.append(
                        f"""
                            <tr>
                                <td><span class="{priority_class}">{priority}</span></td>
                                <td>{item}</td>
                                <td>{estimated_time}</td>
                                <td>{assignee}</td>
                                <td style="font-size: 0.9em; color: #666;">{rationale}</td>
                            </tr>
                    """
                    )

                html_parts.append(
                    """
                        </tbody>
                    </table>
                """
                )

            return "".join(html_parts)

        except Exception as e:
            logger.error(f"HTML 권장사항 생성 중 오류: {e}")
            return f'<div class="issue">권장사항 생성 중 오류가 발생했습니다: {str(e)}</div>'

    def _get_default_recommendations(self) -> Dict[str, Any]:
        """Claude 호출 실패 시 기본 권장사항 반환"""
        return {
            "immediate_improvements": [
                {
                    "category": "모니터링",
                    "title": "기본 모니터링 강화",
                    "description": "기본적인 성능 모니터링 체계 구축",
                    "items": [
                        "CloudWatch 알람 설정",
                        "Performance Insights 활성화",
                        "슬로우 쿼리 로그 정기 분석",
                    ],
                    "expected_impact": "성능 문제 조기 발견",
                    "difficulty": "낮음",
                }
            ],
            "action_items": [
                {
                    "priority": "높음",
                    "item": "CloudWatch 알람 설정",
                    "estimated_time": "1일",
                    "assignee": "DBA",
                    "rationale": "성능 문제 조기 감지를 위해 필요",
                }
            ],
        }

    async def extract_current_schema_info(
        self, database_secret: str, use_ssh_tunnel: bool = False  # EC2에서는 VPC 직접 연결
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
        """데이터 타입 문자열을 파싱하여 타입과 길이 정보 추출 (utils/parsers.py 위임)"""
        return parse_data_type(data_type_str)

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
                OUTPUT_DIR / "html_debug.txt",
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
                OUTPUT_DIR / "html_debug.txt",
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
                    <h4>🔧 SQL 타입</h4>
                    <p>{sql_type}</p>
                </div>
                <div class="summary-item">
                    <h4>🗄️ 데이터베이스</h4>
                    <p>{database_secret or 'N/A'}</p>
                </div>
            </div>
            
            {db_info_section}
            
            <div class="info-section">
                <h3>📝 원본 SQL</h3>
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
                lines = html_content.split("\n")
                new_lines = []
                i = 0

                while i < len(lines):
                    line = lines[i]

                    # 검증 결과 섹션 시작 감지
                    if '<div class="info-section" style="display: none;">' in line:
                        # 다음 라인들을 확인하여 검증 결과 섹션인지 판단
                        if i + 1 < len(lines) and "📊 검증 결과" in lines[i + 1]:
                            # 검증 결과 섹션이므로 </div>까지 스킵
                            i += 1  # 현재 div 라인 스킵
                            while i < len(lines):
                                if "</div>" in lines[i]:
                                    i += 1  # </div> 라인도 스킵
                                    break
                                i += 1
                            continue

                    new_lines.append(line)
                    i += 1

                html_content = "\n".join(new_lines)

                with open(report_path, "w", encoding="utf-8") as f:
                    f.write(html_content)
            except Exception as e:
                pass

            # 파일 생성 확인 디버그
            try:
                with open(
                    OUTPUT_DIR / "html_debug.txt",
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
                    OUTPUT_DIR / "html_debug.txt",
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
        self,
        keyword: Optional[str] = None,
        report_files: Optional[List[str]] = None,
        date_filter: Optional[str] = None,
        latest_count: Optional[int] = None,
    ) -> str:
        """기존 HTML 보고서들을 기반으로 통합 보고서 생성"""
        try:
            # 보고서 파일 수집
            if report_files:
                # 특정 파일들 지정된 경우
                html_files = [
                    OUTPUT_DIR / f for f in report_files if (OUTPUT_DIR / f).exists()
                ]
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
                    content = html_file.read_text(encoding="utf-8")

                    # 파일명에서 원본 SQL 파일명 추출
                    sql_filename = html_file.name.replace(
                        "validation_report_", ""
                    ).replace(".html", "")
                    if "_2025" in sql_filename:
                        sql_filename = sql_filename.split("_2025")[0] + ".sql"

                    # 검증 결과 추출 - HTML 구조 기반으로 정확히 추출
                    if (
                        'status-badge">PASS' in content
                        or "✅ SQL 검증보고서" in content
                    ):
                        status = "PASS"
                        status_icon = "✅"
                    elif (
                        'status-badge">FAIL' in content
                        or "❌ SQL 검증보고서" in content
                    ):
                        status = "FAIL"
                        status_icon = "❌"
                    else:
                        # 기본값으로 FAIL 처리
                        status = "FAIL"
                        status_icon = "❌"

                    # SQL 내용 일부 추출 (HTML 파일에서만)
                    sql_preview = "SQL 내용을 찾을 수 없습니다"
                    if "sql-code" in content:
                        import re

                        sql_match = re.search(
                            r'<div class="sql-code"[^>]*>(.*?)</div>',
                            content,
                            re.DOTALL,
                        )
                        if sql_match:
                            sql_preview = sql_match.group(1).strip()[:100] + "..."

                    # 요약 정보 추출
                    summary = "상세 내용은 개별 보고서 참조"
                    if "Claude AI 분석" in content:
                        summary = "AI 분석 완료"

                    report_data.append(
                        {
                            "filename": sql_filename,
                            "html_file": html_file.name,
                            "status": status,
                            "status_icon": status_icon,
                            "sql_preview": sql_preview,
                            "summary": summary,
                        }
                    )

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
            passed_reports = sum(1 for r in report_data if r["status"] == "PASS")
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

            report_path.write_text(html_content, encoding="utf-8")

            return f"""📊 통합 보고서 생성 완료

📈 요약:
• 총 보고서: {total_reports}개
• 검증 통과: {passed_reports}개 ({round(passed_reports/total_reports*100)}%)
• 검증 실패: {failed_reports}개 ({round(failed_reports/total_reports*100)}%)

📄 통합 보고서: {report_path}

💡 사용법: 테이블의 각 행을 클릭하면 해당 상세 보고서가 새 창에서 열립니다."""

        except Exception as e:
            return f"통합 보고서 생성 실패: {str(e)}"

    def extract_key_metrics_from_csv(self, csv_filename: str) -> dict:
        """CSV 파일에서 직접 핵심 메트릭을 계산하여 반환"""
        import csv
        import statistics

        metrics = {}

        try:
            # CSV 파일 읽기
            csv_path = DATA_DIR / csv_filename

            with open(csv_path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                data = list(reader)

            if not data:
                raise ValueError("CSV 파일이 비어있습니다")

            # 각 메트릭별로 통계 계산
            def calculate_stats(column_name):
                values = []
                for row in data:
                    if column_name in row and row[column_name]:
                        try:
                            values.append(float(row[column_name]))
                        except ValueError:
                            continue

                if values:
                    return {
                        "mean": statistics.mean(values),
                        "min": min(values),
                        "max": max(values),
                    }
                return {"mean": 0.0, "min": 0.0, "max": 0.0}

            # CPU 사용률
            cpu_stats = calculate_stats("CPUUtilization")
            metrics["cpu_mean"] = cpu_stats["mean"]
            metrics["cpu_min"] = cpu_stats["min"]
            metrics["cpu_max"] = cpu_stats["max"]

            # DB Load
            dbload_stats = calculate_stats("DBLoad")
            metrics["dbload_mean"] = dbload_stats["mean"]
            metrics["dbload_min"] = dbload_stats["min"]
            metrics["dbload_max"] = dbload_stats["max"]

            # 연결 수
            conn_stats = calculate_stats("DatabaseConnections")
            metrics["connections_mean"] = conn_stats["mean"]
            metrics["connections_min"] = conn_stats["min"]
            metrics["connections_max"] = conn_stats["max"]

            # Read IOPS
            read_stats = calculate_stats("ReadIOPS")
            metrics["read_iops_mean"] = read_stats["mean"]
            metrics["read_iops_min"] = read_stats["min"]
            metrics["read_iops_max"] = read_stats["max"]

            # Write IOPS
            write_stats = calculate_stats("WriteIOPS")
            metrics["write_iops_mean"] = write_stats["mean"]
            metrics["write_iops_min"] = write_stats["min"]
            metrics["write_iops_max"] = write_stats["max"]

            # 메모리 사용률 계산
            memory_stats = calculate_stats("FreeableMemory")
            if memory_stats["mean"] > 0:
                total_memory = 8 * 1024 * 1024 * 1024  # 8GB
                metrics["memory_usage_mean"] = (
                    (total_memory - memory_stats["mean"]) / total_memory * 100
                )
                metrics["memory_usage_min"] = (
                    (total_memory - memory_stats["max"]) / total_memory * 100
                )
                metrics["memory_usage_max"] = (
                    (total_memory - memory_stats["min"]) / total_memory * 100
                )
            else:
                metrics["memory_usage_mean"] = 0.0
                metrics["memory_usage_min"] = 0.0
                metrics["memory_usage_max"] = 0.0

            print(f"DEBUG: Extracted metrics from CSV: {metrics}", file=sys.stderr)

        except Exception as e:
            print(f"Error reading CSV file {csv_filename}: {e}", file=sys.stderr)
            # 기본값 설정
            for key in [
                "cpu_mean",
                "cpu_min",
                "cpu_max",
                "dbload_mean",
                "dbload_min",
                "dbload_max",
                "connections_mean",
                "connections_min",
                "connections_max",
                "read_iops_mean",
                "read_iops_min",
                "read_iops_max",
                "write_iops_mean",
                "write_iops_min",
                "write_iops_max",
                "memory_usage_mean",
                "memory_usage_min",
                "memory_usage_max",
            ]:
                metrics[key] = 0.0

        return metrics

    def extract_key_metrics(self, summary_text: str) -> dict:
        """핵심 메트릭만 추출하여 딕셔너리로 반환"""
        import re

        # 통계 테이블에서 핵심 메트릭 추출
        metrics = {}

        # 실제 pandas describe() 출력에서 직접 값 추출
        lines = summary_text.split("\n")

        # mean, min, max 라인을 찾아서 값들을 추출
        for line in lines:
            # mean 라인 처리
            if line.strip().startswith("mean") and "CPUUtilization" in summary_text:
                # 공백으로 분할하여 숫자 값들만 추출
                parts = line.split()
                if len(parts) >= 13:
                    try:
                        metrics["cpu_mean"] = float(parts[2])  # CPUUtilization
                        metrics["dbload_mean"] = float(parts[3])  # DBLoad
                        metrics["connections_mean"] = float(
                            parts[6]
                        )  # DatabaseConnections
                        freeable_memory_mean = float(
                            parts[7]
                        )  # FreeableMemory (scientific notation)
                        metrics["read_iops_mean"] = float(parts[10])  # ReadIOPS
                        metrics["write_iops_mean"] = float(parts[12])  # WriteIOPS

                        # 메모리 사용률 계산 (8GB 가정)
                        total_memory = 8 * 1024 * 1024 * 1024
                        metrics["memory_usage_mean"] = (
                            (total_memory - freeable_memory_mean) / total_memory * 100
                        )
                    except (ValueError, IndexError) as e:
                        print(f"Error parsing mean line: {e}", file=sys.stderr)

            # min 라인 처리
            elif line.strip().startswith("min") and "CPUUtilization" in summary_text:
                parts = line.split()
                if len(parts) >= 13:
                    try:
                        metrics["cpu_min"] = float(parts[2])
                        metrics["dbload_min"] = float(parts[3])
                        metrics["connections_min"] = float(parts[6])
                        freeable_memory_max = float(
                            parts[7]
                        )  # min freeable = max usage
                        metrics["read_iops_min"] = float(parts[10])
                        metrics["write_iops_min"] = float(parts[12])

                        # 메모리 사용률 최대값 계산
                        total_memory = 8 * 1024 * 1024 * 1024
                        metrics["memory_usage_max"] = (
                            (total_memory - freeable_memory_max) / total_memory * 100
                        )
                    except (ValueError, IndexError) as e:
                        print(f"Error parsing min line: {e}", file=sys.stderr)

            # max 라인 처리
            elif line.strip().startswith("max") and "CPUUtilization" in summary_text:
                parts = line.split()
                if len(parts) >= 13:
                    try:
                        metrics["cpu_max"] = float(parts[2])
                        metrics["dbload_max"] = float(parts[3])
                        metrics["connections_max"] = float(parts[6])
                        freeable_memory_min = float(
                            parts[7]
                        )  # max freeable = min usage
                        metrics["read_iops_max"] = float(parts[10])
                        metrics["write_iops_max"] = float(parts[12])

                        # 메모리 사용률 최소값 계산
                        total_memory = 8 * 1024 * 1024 * 1024
                        metrics["memory_usage_min"] = (
                            (total_memory - freeable_memory_min) / total_memory * 100
                        )
                    except (ValueError, IndexError) as e:
                        print(f"Error parsing max line: {e}", file=sys.stderr)

        return metrics

    def format_metrics_as_html(self, metrics: dict) -> str:
        """메트릭 딕셔너리를 HTML로 포맷 (Week 3: ReportGenerator로 위임)"""
        return self.report_generator.format_metrics_as_html(metrics)

    def convert_urls_to_html_links(self, text: str) -> str:
        """텍스트 내의 URL을 HTML 링크로 변환하고 파일명을 링크로 만듦 (Week 3: ReportGenerator로 위임)"""
        return self.report_generator.convert_urls_to_html_links(text)

    async def generate_comprehensive_performance_report(
        self,
        database_secret: str,
        db_instance_identifier: str,
        region: Optional[str] = None,
        hours: int = 24,
    ) -> str:
        """종합 성능 진단 보고서 생성"""
        # 디버그 로그 파일 생성
        debug_log_path = (
            LOGS_DIR
            / f"debug_log_performance_{db_instance_identifier}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        )

        debug_log(
            f"종합 성능 진단 보고서 생성 시작 - 인스턴스: {db_instance_identifier}"
        )

        try:
            # region이 제공되지 않으면 현재 프로파일의 기본 리전 사용
            if region is None:
                region = self.default_region

            # 클러스터 엔드포인트 확인 및 인스턴스 identifier 변환
            original_identifier = db_instance_identifier
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            debug_log(f"리전 설정: {region}, 타임스탬프: {timestamp}")

            # 1. 메트릭 수집
            debug_log("메트릭 수집 시작")
            metrics_result = await self.collect_db_metrics(
                db_instance_identifier, hours, None, region
            )
            if "오류" in metrics_result:
                debug_log(f"메트릭 수집 실패: {metrics_result}")
                return f"❌ 메트릭 수집 실패: {metrics_result}"

            debug_log("메트릭 수집 완료")

            # CSV 파일명 및 Pre-signed URL 추출
            csv_file = None
            presigned_url = None
            for line in metrics_result.split("\n"):
                if "S3 저장 위치:" in line:
                    csv_file = line.split(": ")[-1]
                elif "다운로드 URL" in line and "https://" in line:
                    presigned_url = line.split(": ", 1)[-1]

            if not csv_file:
                debug_log("메트릭 CSV 파일을 찾을 수 없음")
                return "❌ 메트릭 CSV 파일을 찾을 수 없습니다"

            csv_filename = Path(csv_file).name
            debug_log(f"CSV 파일명: {csv_filename}")
            if presigned_url:
                debug_log(f"Pre-signed URL 추출 성공")

            # 2. 성능 쿼리 수집 및 파일 추적
            debug_log("성능 쿼리 수집 시작")
            generated_files = []  # 생성된 파일들 추적

            slow_queries = await self.collect_slow_queries(database_secret)
            cpu_queries = await self.collect_cpu_intensive_queries(
                database_secret, db_instance_identifier, None, None
            )
            temp_queries = await self.collect_temp_space_intensive_queries(
                database_secret, db_instance_identifier, None, None
            )

            # URL을 HTML 링크로 변환
            slow_queries = self.convert_urls_to_html_links(slow_queries)
            cpu_queries = self.convert_urls_to_html_links(cpu_queries)
            temp_queries = self.convert_urls_to_html_links(temp_queries)

            # SQL 파일들이 생성되었는지 확인하고 추적
            sql_files = list(Path("sql").glob("*.sql")) if Path("sql").exists() else []
            for sql_file in sql_files:
                if any(
                    keyword in sql_file.name.lower()
                    for keyword in ["slow", "cpu", "memory", "temp"]
                ):
                    generated_files.append(
                        {
                            "name": sql_file.name,
                            "type": "SQL 쿼리",
                            "path": f"../sql/{sql_file.name}",
                        }
                    )

            debug_log("성능 쿼리 수집 완료")

            # 3. 메트릭 분석 및 파일 추적
            debug_log("메트릭 분석 시작")
            summary = await self.get_metric_summary(csv_filename)

            # 핵심 메트릭 추출 - CSV 파일에서 직접 계산
            key_metrics_dict = self.extract_key_metrics_from_csv(csv_filename)
            key_metrics_html = self.format_metrics_as_html(key_metrics_dict)

            correlation = await self.analyze_metric_correlation(
                csv_filename, "CPUUtilization", 10
            )
            outliers = await self.detect_metric_outliers(
                csv_filename, 2.0, skip_html_report=True
            )

            # 아웃라이어 결과에서 링크 부분 제거 (종합보고서용)
            if "📄 상세 보고서:" in outliers:
                outliers_lines = outliers.split("\n")
                filtered_lines = []
                skip_next = False
                for line in outliers_lines:
                    if "📄 상세 보고서:" in line:
                        break  # 이 라인부터는 모두 제외
                    filtered_lines.append(line)
                outliers = "\n".join(filtered_lines).strip()

            # 임계값 모달 HTML 생성
            metric_thresholds = self.load_metric_thresholds()
            threshold_modal_html = self.generate_threshold_html(metric_thresholds)

            # 분석 결과 파일들 추적
            data_files = (
                list(Path("data").glob("*.csv")) if Path("data").exists() else []
            )
            for data_file in data_files:
                if db_instance_identifier in data_file.name:
                    generated_files.append(
                        {
                            "name": data_file.name,
                            "type": "분석 데이터",
                            "path": f"../data/{data_file.name}",
                        }
                    )

            debug_log("메트릭 분석 완료")

            # 4. 상관관계 분석 (기존 함수 사용)
            correlation_analysis = await self.analyze_metric_correlation(
                csv_filename, "CPUUtilization", 10
            )

            # 5. Claude 기반 성능 권장사항 생성
            debug_log("Claude 기반 성능 권장사항 생성 시작")
            logger.info("Claude 기반 성능 권장사항 생성 시작")
            try:
                claude_recommendations = (
                    await self.generate_performance_recommendations_with_claude(
                        summary,
                        correlation_analysis,
                        outliers,
                        slow_queries,
                        cpu_queries,
                        temp_queries,
                        database_secret,
                    )
                )
                debug_log("Claude 권장사항 생성 완료")
                logger.info(
                    f"Claude 권장사항 생성 완료: {len(claude_recommendations.get('immediate_improvements', []))}개 개선사항, {len(claude_recommendations.get('action_items', []))}개 액션아이템"
                )
            except Exception as e:
                debug_log(f"Claude 권장사항 생성 실패: {e}")
                logger.error(f"Claude 권장사항 생성 실패, 기본 권장사항 사용: {e}")
                claude_recommendations = self._get_default_recommendations()

            # 6. HTML 보고서 생성
            debug_log("HTML 보고서 생성 시작")
            report_path = (
                OUTPUT_DIR
                / f"comprehensive_performance_report_{db_instance_identifier}_{timestamp}.html"
            )

            html_content = f"""<!DOCTYPE html>
<html lang="ko">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>종합 성능 진단 보고서 - {db_instance_identifier}</title>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{ font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; line-height: 1.6; color: #333; background: #f5f5f5; }}
        .container {{ max-width: 1200px; margin: 0 auto; padding: 20px; }}
        .header {{ background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 30px; border-radius: 10px; margin-bottom: 30px; text-align: center; }}
        .header h1 {{ font-size: 2.5em; margin-bottom: 10px; }}
        .header .subtitle {{ font-size: 1.2em; opacity: 0.9; }}
        .section {{ background: white; margin-bottom: 30px; border-radius: 10px; box-shadow: 0 4px 6px rgba(0,0,0,0.1); overflow: hidden; }}
        .section-header {{ background: #2c3e50; color: white; padding: 20px; font-size: 1.3em; font-weight: bold; }}
        .section-content {{ padding: 25px; }}
        .metric-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(250px, 1fr)); gap: 20px; margin-bottom: 25px; }}
        .metric-card {{ background: #f8f9fa; border-left: 4px solid #3498db; padding: 20px; border-radius: 5px; }}
        .metric-card.warning {{ border-left-color: #f39c12; }}
        .metric-card.danger {{ border-left-color: #e74c3c; }}
        .metric-card.success {{ border-left-color: #27ae60; }}
        .metric-title {{ font-weight: bold; color: #2c3e50; margin-bottom: 10px; }}
        .metric-value {{ font-size: 1.8em; font-weight: bold; color: #3498db; }}
        .metric-unit {{ font-size: 0.9em; color: #7f8c8d; }}
        .query-box {{ background: #2c3e50; color: #ecf0f1; padding: 15px; border-radius: 5px; margin: 10px 0; font-family: 'Courier New', monospace; font-size: 0.9em; overflow-x: auto; }}
        .status-good {{ color: #27ae60; font-weight: bold; }}
        .status-warning {{ color: #f39c12; font-weight: bold; }}
        .status-critical {{ color: #e74c3c; font-weight: bold; }}
        .table {{ width: 100%; border-collapse: collapse; margin: 20px 0; }}
        .table th, .table td {{ padding: 12px; text-align: left; border-bottom: 1px solid #ddd; }}
        .table th {{ background: #34495e; color: white; }}
        .table tr:hover {{ background: #f5f5f5; }}
        .recommendation {{ background: #e8f5e9; border-left: 4px solid #4caf50; padding: 15px; margin: 10px 0; border-radius: 5px; }}
        .issue {{ background: #ffebee; border-left: 4px solid #f44336; padding: 15px; margin: 10px 0; border-radius: 5px; }}
        .info-box {{ background: #e3f2fd; border-left: 4px solid #2196f3; padding: 15px; margin: 10px 0; border-radius: 5px; }}
        .file-link {{ color: #007bff; text-decoration: none; font-weight: bold; }}
        .file-link:hover {{ text-decoration: underline; color: #0056b3; }}
        .toc {{ background: #f8f9fa; padding: 20px; border-radius: 5px; margin-bottom: 30px; }}
        .toc ul {{ list-style: none; }}
        .toc li {{ margin: 5px 0; }}
        .toc a {{ color: #3498db; text-decoration: none; }}
        .toc a:hover {{ text-decoration: underline; }}
        .btn {{ background: #007bff; color: white; padding: 10px 20px; border: none; border-radius: 5px; cursor: pointer; font-size: 14px; }}
        .btn:hover {{ background: #0056b3; }}
        .modal {{ display: none; position: fixed; z-index: 1000; left: 0; top: 0; width: 100%; height: 100%; overflow: auto; background-color: rgba(0,0,0,0.4); }}
        .modal-content {{ background-color: #fefefe; margin: 5% auto; padding: 20px; border: none; border-radius: 10px; width: 80%; max-width: 800px; box-shadow: 0 4px 20px rgba(0,0,0,0.3); }}
        .close {{ color: #aaa; float: right; font-size: 28px; font-weight: bold; cursor: pointer; }}
        .close:hover, .close:focus {{ color: black; text-decoration: none; }}
        .threshold-table {{ width: 100%; border-collapse: collapse; margin: 20px 0; }}
        .threshold-table th, .threshold-table td {{ padding: 12px; text-align: left; border-bottom: 1px solid #ddd; }}
        .threshold-table th {{ background: #f8f9fa; font-weight: bold; }}
        .threshold-table tr:hover {{ background: #f5f5f5; }}
        @media (max-width: 768px) {{
            .container {{ padding: 10px; }}
            .header {{ padding: 20px; }}
            .header h1 {{ font-size: 2em; }}
            .metric-grid {{ grid-template-columns: 1fr; }}
            .modal-content {{ width: 95%; margin: 10% auto; }}
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🗄️ 종합 성능 진단 보고서</h1>
            <div class="subtitle">데이터베이스 성능 분석</div>
            <div style="margin-top: 15px; font-size: 1em;">
                <strong>인스턴스:</strong> {db_instance_identifier} | 
                <strong>리전:</strong> {region} | 
                <strong>분석 기간:</strong> {hours}시간 | 
                <strong>생성일시:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
            </div>
        </div>

        <div class="toc">
            <h3>📋 목차</h3>
            <ul>
                <li><a href="#executive-summary">1. 요약 정보</a></li>
                <li><a href="#performance-metrics">2. 성능 메트릭 분석</a></li>
                <li><a href="#correlation-analysis">3. 상관관계 분석</a></li>
                <li><a href="#outlier-analysis">4. 이상 징후 분석</a></li>
                <li><a href="#slow-queries">5. 느린 쿼리 분석</a></li>
                <li><a href="#resource-intensive">6. 리소스 집약적 쿼리</a></li>
                <li><a href="#recommendations">7. 최적화 권장사항</a></li>
            </ul>
        </div>

        <div class="section" id="executive-summary">
            <div class="section-header">📊 1. 요약 정보 (Executive Summary)</div>
            <div class="section-content">
                <div class="info-box">
                    <strong>분석 개요:</strong> {hours}시간 동안의 성능 데이터를 기반으로 한 종합 진단 결과입니다.
                </div>
                
                <h4>📊 인스턴스 정보</h4>
                <div class="metric-grid">
                    <div class="metric-card">
                        <div class="metric-title">🗄️ 인스턴스 ID</div>
                        <div class="metric-value">{db_instance_identifier}</div>
                        <div class="metric-unit">{region}</div>
                    </div>
                    <div class="metric-card">
                        <div class="metric-title">📅 분석 기간</div>
                        <div class="metric-value">{hours}</div>
                        <div class="metric-unit">시간</div>
                    </div>
                    <div class="metric-card">
                        <div class="metric-title">📈 데이터 포인트</div>
                        <div class="metric-value">288</div>
                        <div class="metric-unit">개</div>
                    </div>
                </div>
                
                <h4>📈 핵심 성능 통계</h4>
                {key_metrics_html}
            </div>
        </div>

        <div class="section" id="performance-metrics">
            <div class="section-header">📈 2. 성능 메트릭 분석</div>
            <div class="section-content">
                <h4>🎯 핵심 성능 지표</h4>
                <div class="metric-grid">
                    <div class="metric-card success">
                        <div class="metric-title">📊 데이터 수집 상태</div>
                        <div class="metric-value">완료</div>
                        <div class="metric-unit">메트릭 수집 성공</div>
                    </div>
                    <div class="metric-card">
                        <div class="metric-title">📅 분석 기간</div>
                        <div class="metric-value">{hours}</div>
                        <div class="metric-unit">시간</div>
                    </div>
                    <div class="metric-card">
                        <div class="metric-title">🗄️ 인스턴스</div>
                        <div class="metric-value">{db_instance_identifier}</div>
                        <div class="metric-unit">{region}</div>
                    </div>
                </div>
                
                <h4>📊 상세 메트릭 정보</h4>
                <div class="info-box">
                    {"메트릭 데이터는 <strong><a href='" + presigned_url + "' target='_blank'>" + csv_filename + "</a></strong> 파일에 저장되었습니다 (다운로드 링크는 7일간 유효)." if presigned_url else "메트릭 데이터는 <strong>" + csv_filename + "</strong> 파일에 저장되었습니다."}
                </div>
            </div>
        </div>

        <div class="section" id="correlation-analysis">
            <div class="section-header">🔗 3. 상관관계 분석</div>
            <div class="section-content">
                <h4>📈 메트릭 간 상관관계</h4>
                <pre style="background: #f8f9fa; padding: 15px; border-radius: 5px; overflow-x: auto;">{correlation}</pre>
                
                <div class="recommendation">
                    <strong>💡 상관관계 인사이트:</strong>
                    <ul style="margin-top: 10px; margin-left: 20px;">
                        <li>높은 상관관계(r > 0.7)를 보이는 메트릭들은 함께 모니터링해야 합니다</li>
                        <li>CPU 사용률과 강한 상관관계를 보이는 메트릭들을 우선적으로 최적화하세요</li>
                        <li>네트워크 트래픽과 I/O 메트릭의 관계를 주의 깊게 관찰하세요</li>
                    </ul>
                </div>
            </div>
        </div>

        <div class="section" id="outlier-analysis">
            <div class="section-header">🚨 4. 이상 징후 분석 (Outlier Detection)</div>
            <div class="section-content">
                <h4>⚠️ 발견된 아웃라이어</h4>
                <pre style="background: #f8f9fa; padding: 15px; border-radius: 5px; overflow-x: auto;">{outliers}</pre>
                
                <div style="margin: 15px 0;">
                    <button class="btn" onclick="document.getElementById('thresholdModal').style.display='block'">
                        📊 임계값 설정 보기
                    </button>
                </div>
                
                <div class="issue">
                    <strong>🔍 주의사항:</strong> 아웃라이어가 발견된 시점의 애플리케이션 로그와 시스템 이벤트를 함께 분석하여 근본 원인을 파악하세요.
                </div>
            </div>
        </div>

        <div class="section" id="slow-queries">
            <div class="section-header">🐌 5. Slow 쿼리 분석 (Slow Query Analysis)</div>
            <div class="section-content">
                <h4>📊 수집 결과</h4>
                <div class="query-box">{slow_queries}</div>
                
                <div class="recommendation">
                    <strong>💡 Slow 쿼리 최적화 가이드:</strong>
                    <ul style="margin-top: 10px; margin-left: 20px;">
                        <li>인덱스 추가 또는 기존 인덱스 최적화</li>
                        <li>WHERE 절 조건 순서 최적화</li>
                        <li>JOIN 조건 및 순서 검토</li>
                        <li>쿼리 실행 계획(EXPLAIN) 분석</li>
                    </ul>
                </div>
            </div>
        </div>

        <div class="section" id="resource-intensive">
            <div class="section-header">💾 6. 리소스 소모가 큰 쿼리 분석</div>
            <div class="section-content">
                <h4>⚡ CPU 소비가 많은 쿼리</h4>
                <div class="query-box">{cpu_queries}</div>
                
                <h4>💿 임시 공간을 많이 소비하는 쿼리</h4>
                <div class="query-box">{temp_queries}</div>
                
                <div class="recommendation">
                    <strong>💡 리소스 최적화 전략:</strong>
                    <ul style="margin-top: 10px; margin-left: 20px;">
                        <li><strong>CPU:</strong> 복잡한 연산 최적화, 함수 사용 최소화</li>
                        <li><strong>임시 공간:</strong> GROUP BY, ORDER BY 최적화, 인덱스 활용</li>
                    </ul>
                </div>
            </div>
        </div>

        <div class="section" id="recommendations">
            <div class="section-header">🎯 7. 최적화 권장사항</div>
            <div class="section-content">
                <div class="info-box" style="margin-bottom: 20px;">
                    <strong>🤖 AI 기반 분석:</strong> 이 권장사항들은 Claude AI가 실제 성능 메트릭, 상관관계 분석, 느린 쿼리 데이터를 종합 분석하여 생성한 맞춤형 제안입니다.
                </div>
                {self._generate_recommendations_html(claude_recommendations)}
            </div>
        </div>

        <div class="section">
            <div class="section-content">
                <div style="text-align: center; margin-top: 30px; padding: 30px; color: #7f8c8d; font-size: 0.9em;">
                    <em>이 보고서는 {db_instance_identifier} 인스턴스의 {hours}시간 동안의 성능 데이터를 기반으로 생성되었습니다.<br>
                    정확한 성능 분석을 위해서는 최소 1주일 이상의 데이터 수집을 권장합니다.</em>
                </div>
            </div>
        </div>
    </div>
    
    <!-- 임계값 설정 모달 -->
    {threshold_modal_html}
    
    <script>
        // 모달 제어 JavaScript
        var modal = document.getElementById('thresholdModal');
        var span = document.getElementsByClassName('close')[0];
        
        span.onclick = function() {{
            modal.style.display = 'none';
        }}
        
        window.onclick = function(event) {{
            if (event.target == modal) {{
                modal.style.display = 'none';
            }}
        }}
    </script>
</body>
</html>"""

            # HTML 파일 저장
            with open(report_path, "w", encoding="utf-8") as f:
                f.write(html_content)

            debug_log(f"HTML 보고서 저장 완료: {report_path}")

            return f"""🗄️ 종합 성능 진단 보고서 생성 완료

📊 **데이터베이스 성능 보고서**
• 인스턴스: {db_instance_identifier}
• 리전: {region}
• 분석 기간: {hours}시간
• 생성 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

📁 **생성된 파일들:**
• 종합 보고서: {report_path.name}
• 메트릭 데이터: <a href="file://{DATA_DIR / csv_filename}" target="_blank">{csv_filename}</a>
• 상관관계 분석: 포함됨

📈 **포함된 분석:**
✅ 성능 메트릭 요약 및 통계
✅ 메트릭 간 상관관계 분석
✅ 이상 징후(아웃라이어) 탐지
✅ 느린 쿼리 수집 및 분석
✅ 리소스 집약적 쿼리 분석 (CPU, 메모리, 임시공간)
✅ 최적화 권장사항 및 액션 아이템
✅ 반응형 HTML 디자인

💡 **주요 특징:**
• 모바일 최적화된 반응형 디자인
• 상세한 메트릭 분석 및 시각화
• 실행 가능한 최적화 권장사항
• 우선순위별 액션 아이템 제공

🔍 보고서를 브라우저에서 열어 상세 분석 결과를 확인하세요.
📄 디버그 로그: {debug_log_path}"""

        except Exception as e:
            debug_log(f"종합 성능 진단 보고서 생성 오류: {e}")
            logger.error(f"종합 성능 진단 보고서 생성 오류: {e}")
            return (
                f"❌ 종합 보고서 생성 실패: {str(e)}\n📄 디버그 로그: {debug_log_path}"
            )

    async def generate_cluster_performance_report(
        self,
        database_secret: str,
        db_cluster_identifier: str,
        hours: int = 24,
        region: str = "ap-northeast-2",
    ) -> str:
        """
        Aurora 클러스터 전용 성능 보고서 생성
        - 클러스터 레벨 분석 (부하 분산, 레플리케이션 지연, HLL 등)
        - 각 인스턴스별 상세 보고서 링크 제공
        - Writer/Reader 역할별 비교 분석
        """
        # 디버그 로그 파일 생성
        debug_log_path = (
            LOGS_DIR
            / f"debug_log_cluster_{db_cluster_identifier}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        )

        try:
            debug_log(f"클러스터 성능 보고서 생성 시작: {db_cluster_identifier}")

            # 1. database_secret에서 실제 클러스터 정보 찾기
            debug_log("RDS 클라이언트 초기화")
            rds_client = boto3.client("rds", region_name=region)

            # Secret에서 호스트 정보 가져오기
            debug_log("Secret 정보 조회")
            secrets_client = boto3.client(
                "secretsmanager", region_name=region, verify=False
            )
            get_secret_value_response = secrets_client.get_secret_value(
                SecretId=database_secret
            )
            secret = get_secret_value_response["SecretString"]
            secret_info = json.loads(secret)
            host = secret_info.get("host", "")
            debug_log(f"호스트 정보: {host}")

            # 호스트 정보로 실제 클러스터 찾기
            actual_cluster_id = None
            if host:
                # 모든 클러스터 조회해서 엔드포인트 매칭
                debug_log("클러스터 목록 조회 및 매칭")
                clusters = rds_client.describe_db_clusters()["DBClusters"]
                for cluster in clusters:
                    if cluster.get("Endpoint", "") in host or host in cluster.get(
                        "Endpoint", ""
                    ):
                        actual_cluster_id = cluster["DBClusterIdentifier"]
                        break

            # 실제 클러스터 ID가 없으면 파라미터로 받은 값 사용
            if not actual_cluster_id:
                actual_cluster_id = db_cluster_identifier

            debug_log(f"실제 클러스터 ID: {actual_cluster_id}")

            cluster_info = rds_client.describe_db_clusters(
                DBClusterIdentifier=actual_cluster_id
            )["DBClusters"][0]

            cluster_members = cluster_info["DBClusterMembers"]
            writer_instance = next(
                (m for m in cluster_members if m["IsClusterWriter"]), None
            )
            reader_instances = [m for m in cluster_members if not m["IsClusterWriter"]]

            debug_log(f"클러스터 구성: Writer 1개, Reader {len(reader_instances)}개")

            # 2. 각 인스턴스별 메트릭 수집 및 상세 보고서 생성
            instance_reports = {}
            cluster_metrics = {}

            # Writer 인스턴스 처리
            if writer_instance:
                writer_id = writer_instance["DBInstanceIdentifier"]
                debug_log(f"Writer 인스턴스 처리: {writer_id}")

                # 상세 보고서 생성
                writer_report = await self.generate_comprehensive_performance_report(
                    database_secret, writer_id, region, hours
                )
                instance_reports[writer_id] = {
                    "role": "Writer",
                    "report": writer_report,
                    "is_writer": True,
                }
                debug_log(
                    f"Writer 인스턴스 상세보고서 생성: {writer_id},{writer_report}"
                )

                # 메트릭 수집
                metrics_result = await self.collect_db_metrics(writer_id, region, hours)
                # 실제 생성된 파일명 추출
                if "저장 위치:" in metrics_result:
                    csv_path = metrics_result.split("저장 위치: ")[-1].strip()
                    cluster_metrics[writer_id] = csv_path.split("/")[
                        -1
                    ]  # 파일명만 추출
                else:
                    cluster_metrics[writer_id] = (
                        f"database_metrics_{writer_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
                    )
            else:
                debug_log("Writer 인스턴스를 찾을 수 없음")

            # Reader 인스턴스들 처리
            for reader in reader_instances:
                reader_id = reader["DBInstanceIdentifier"]
                debug_log(f"Reader 인스턴스 처리: {reader_id}")

                # 상세 보고서 생성
                reader_report = await self.generate_comprehensive_performance_report(
                    database_secret, reader_id, region, hours
                )
                instance_reports[reader_id] = {
                    "role": "Reader",
                    "report": reader_report,
                    "is_writer": False,
                }

                # 메트릭 수집
                metrics_result = await self.collect_db_metrics(reader_id, region, hours)
                # 실제 생성된 파일명 추출
                if "저장 위치:" in metrics_result:
                    csv_path = metrics_result.split("저장 위치: ")[-1].strip()
                    cluster_metrics[reader_id] = csv_path.split("/")[
                        -1
                    ]  # 파일명만 추출
                else:
                    cluster_metrics[reader_id] = (
                        f"database_metrics_{reader_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
                    )
            debug_log("클러스터 레벨 분석 시작")
            # 3. 클러스터 레벨 메트릭 수집
            cluster_level_metrics = await self._collect_cluster_level_metrics(
                actual_cluster_id, region, hours
            )

            # 4. 클러스터 이벤트 수집 (최근 7일)
            cluster_events = await self._collect_cluster_events(
                actual_cluster_id, region, 7 * 24  # 7일
            )

            # 5. 클러스터 레벨 분석
            cluster_analysis = await self._analyze_cluster_metrics(
                cluster_metrics, cluster_info, cluster_level_metrics, cluster_events
            )

            # 4. 클러스터 통합 보고서 생성
            debug_log("클러스터 통합 보고서 생성 시작")
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            report_filename = (
                f"cluster_performance_report_{actual_cluster_id}_{timestamp}.html"
            )
            report_path = Path.cwd() / "output" / report_filename

            html_content = await self._generate_cluster_html_report(
                cluster_info,
                instance_reports,
                cluster_analysis,
                timestamp,
                cluster_level_metrics,
                cluster_events,
            )

            # 보고서 저장
            report_path.parent.mkdir(exist_ok=True)
            with open(report_path, "w", encoding="utf-8") as f:
                f.write(html_content)

            debug_log(f"클러스터 보고서 생성 완료: {report_path}")

            return f"""✅ Aurora 클러스터 성능 보고서 생성 완료

🏗️ 클러스터 정보:
• 클러스터 ID: {actual_cluster_id}
• 엔진: {cluster_info.get('Engine', 'N/A')} {cluster_info.get('EngineVersion', 'N/A')}
• 인스턴스 수: {len(cluster_members)}개 (Writer: 1개, Reader: {len(reader_instances)}개)
• 분석 기간: 최근 {hours}시간

📊 생성된 보고서:
• 🎯 클러스터 통합 보고서: file://{report_path}
• 📋 개별 인스턴스 상세 보고서: {len(instance_reports)}개

🔍 주요 분석 내용:
• 클러스터 부하 분산 상태
• Writer/Reader 성능 비교
• 레플리케이션 지연 분석
• 인스턴스 간 리소스 사용률 비교
• 클러스터 최적화 권장사항

📄 클러스터 보고서를 브라우저에서 열어 전체 분석 결과를 확인하고,
   각 인스턴스별 상세 분석은 링크를 통해 접근하세요.
📄 디버그 로그: {debug_log_path}"""

        except Exception as e:
            debug_log(f"클러스터 성능 보고서 생성 오류: {e}")
            return f"❌ 클러스터 보고서 생성 실패: {str(e)}\n📄 디버그 로그: {debug_log_path}"

    async def _collect_cluster_level_metrics(
        self, cluster_id: str, region: str, hours: int
    ) -> Dict:
        """클러스터 레벨 메트릭 수집 (Lambda 사용)"""
        try:
            # Lambda 함수 호출
            result = await self._call_lambda('collect-cluster-metrics', {
                'cluster_id': cluster_id,
                'region': region,
                'hours': hours
            })

            if result.get('success'):
                # Lambda 응답에서 metrics 데이터 추출
                # Lambda는 ISO 형식 문자열로 Timestamp를 반환하므로 datetime 객체로 변환
                metrics_data = result.get('metrics', {})

                # Timestamp를 datetime 객체로 변환
                for metric_name, datapoints in metrics_data.items():
                    for dp in datapoints:
                        if 'Timestamp' in dp and isinstance(dp['Timestamp'], str):
                            from dateutil import parser
                            dp['Timestamp'] = parser.parse(dp['Timestamp'])

                return metrics_data
            else:
                error_msg = result.get('error', 'Unknown error')
                logger.error(f"클러스터 메트릭 수집 실패 (Lambda): {error_msg}")
                return {}

        except Exception as e:
            logger.error(f"클러스터 메트릭 수집 오류: {e}")
            return {}

    async def _collect_cluster_events(
        self, cluster_id: str, region: str, hours: int
    ) -> List[Dict]:
        """클러스터 이벤트 수집 (Lambda 사용)"""
        try:
            # Lambda 함수 호출
            result = await self._call_lambda('collect-cluster-events', {
                'cluster_id': cluster_id,
                'region': region,
                'hours': hours
            })

            if result.get('success'):
                return result.get('events', [])
            else:
                error_msg = result.get('error', 'Unknown error')
                logger.error(f"클러스터 이벤트 수집 실패 (Lambda): {error_msg}")
                return []

        except Exception as e:
            logger.error(f"클러스터 이벤트 수집 오류: {e}")
            return []

    def _categorize_event_severity(self, message: str) -> str:
        """이벤트 메시지 기반 심각도 분류"""
        message_lower = message.lower()

        if any(
            keyword in message_lower
            for keyword in [
                "failed",
                "error",
                "critical",
                "fatal",
                "crash",
                "corruption",
            ]
        ):
            return "HIGH"
        elif any(
            keyword in message_lower
            for keyword in ["warning", "slow", "timeout", "retry", "restart", "reboot"]
        ):
            return "MEDIUM"
        else:
            return "LOW"

    async def _analyze_cluster_metrics(
        self,
        cluster_metrics: Dict,
        cluster_info: Dict,
        cluster_level_metrics: Dict = None,
        cluster_events: List = None,
    ) -> Dict:
        """클러스터 레벨 메트릭 분석"""
        try:
            if not ANALYSIS_AVAILABLE:
                logger.warning("분석 라이브러리 없음 - 기본 분석만 수행")
                return {"error": "분석 라이브러리가 필요합니다"}

            analysis = {
                "load_distribution": {},
                "replication_lag": {},
                "resource_comparison": {},
                "recommendations": [],
            }

            logger.info(f"클러스터 메트릭 분석 시작: {cluster_metrics}")

            # 각 인스턴스의 메트릭 로드 및 비교 - data 폴더에서 직접 찾기
            metrics_data = {}

            # 클러스터 멤버에서 인스턴스 ID 가져오기
            for member in cluster_info["DBClusterMembers"]:
                instance_id = member["DBInstanceIdentifier"]

                # data 폴더에서 해당 인스턴스의 최신 CSV 파일 찾기
                data_dir = Path("data")
                csv_files = list(data_dir.glob(f"database_metrics_{instance_id}_*.csv"))

                if csv_files:
                    # 가장 최신 파일 선택 (파일명의 타임스탬프 기준)
                    latest_csv = max(csv_files, key=lambda x: x.name.split("_")[-1])
                    logger.info(f"메트릭 파일 발견: {instance_id} -> {latest_csv}")

                    try:
                        df = pd.read_csv(latest_csv)
                        metrics_data[instance_id] = df
                        logger.info(
                            f"메트릭 파일 로드 성공: {instance_id} ({len(df)} 행)"
                        )
                    except Exception as e:
                        logger.warning(f"메트릭 파일 로드 실패 {latest_csv}: {e}")
                else:
                    logger.warning(f"메트릭 파일 없음: {instance_id}")

            logger.info(f"로드된 메트릭 데이터: {len(metrics_data)}개 인스턴스")

            if len(metrics_data) >= 2:
                # Writer vs Reader 비교
                writer_data = None
                reader_data = []

                for instance_id, df in metrics_data.items():
                    # 클러스터 멤버 정보에서 역할 확인
                    is_writer = any(
                        m["DBInstanceIdentifier"] == instance_id
                        and m["IsClusterWriter"]
                        for m in cluster_info["DBClusterMembers"]
                    )

                    logger.info(
                        f"인스턴스 역할 확인: {instance_id} -> {'Writer' if is_writer else 'Reader'}"
                    )

                    if is_writer:
                        writer_data = df
                    else:
                        reader_data.append((instance_id, df))

                # 부하 분산 분석
                if writer_data is not None and reader_data:
                    logger.info("부하 분산 분석 시작")
                    try:
                        analysis["load_distribution"] = self._analyze_load_distribution(
                            writer_data, reader_data
                        )
                        logger.info("부하 분산 분석 완료")
                    except Exception as e:
                        logger.error(f"부하 분산 분석 오류: {e}")
                        analysis["load_distribution"] = {}
                else:
                    logger.warning(
                        f"부하 분산 분석 불가: writer_data={writer_data is not None}, reader_data={len(reader_data)}"
                    )

                # 리소스 사용률 비교
                try:
                    logger.info("리소스 사용률 비교 시작")
                    analysis["resource_comparison"] = self._compare_resource_usage(
                        metrics_data
                    )
                    logger.info("리소스 사용률 비교 완료")
                except Exception as e:
                    logger.error(f"리소스 사용률 비교 오류: {e}")
                    analysis["resource_comparison"] = {}
            else:
                logger.warning(
                    f"분석을 위한 메트릭 데이터 부족: {len(metrics_data)}개 (최소 2개 필요)"
                )

            return analysis

        except Exception as e:
            logger.error(f"클러스터 메트릭 분석 오류: {e}")
            return {"error": str(e)}

    def _analyze_load_distribution(self, writer_df, reader_data):
        """부하 분산 상태 분석"""
        try:
            writer_cpu = (
                writer_df["CPUUtilization"].mean()
                if "CPUUtilization" in writer_df.columns
                else 0
            )
            writer_connections = (
                writer_df["DatabaseConnections"].mean()
                if "DatabaseConnections" in writer_df.columns
                else 0
            )

            reader_stats = []
            for reader_id, reader_df in reader_data:
                reader_cpu = (
                    reader_df["CPUUtilization"].mean()
                    if "CPUUtilization" in reader_df.columns
                    else 0
                )
                reader_connections = (
                    reader_df["DatabaseConnections"].mean()
                    if "DatabaseConnections" in reader_df.columns
                    else 0
                )
                reader_stats.append(
                    {
                        "instance_id": reader_id,
                        "cpu": reader_cpu,
                        "connections": reader_connections,
                    }
                )

            return {
                "writer": {"cpu": writer_cpu, "connections": writer_connections},
                "readers": reader_stats,
                "balance_score": self._calculate_balance_score(
                    writer_cpu, [r["cpu"] for r in reader_stats]
                ),
            }
        except Exception as e:
            logger.error(f"부하 분산 분석 오류: {e}")
            return {}

    def _calculate_balance_score(self, writer_cpu, reader_cpus):
        """부하 분산 점수 계산 (0-100)"""
        if not reader_cpus:
            return 0

        total_cpu = writer_cpu + sum(reader_cpus)
        if total_cpu == 0:
            return 100

        # 이상적인 분산: Writer가 전체 부하의 60-70% 담당
        writer_ratio = writer_cpu / total_cpu
        ideal_ratio = 0.65

        deviation = abs(writer_ratio - ideal_ratio)
        score = max(0, 100 - (deviation * 200))  # 편차가 클수록 점수 감소

        return round(score, 1)

    def _compare_resource_usage(self, metrics_data):
        """인스턴스 간 리소스 사용률 비교"""
        try:
            comparison = {}

            for instance_id, df in metrics_data.items():
                # 메모리 사용률 계산 (FreeableMemory 기반)
                memory_usage_percent = 0
                if "FreeableMemory" in df.columns and not df["FreeableMemory"].empty:
                    freeable_memory = df["FreeableMemory"].mean()
                    # 가정: 총 메모리 16GB (16 * 1024 * 1024 * 1024 bytes)
                    total_memory = 16 * 1024 * 1024 * 1024
                    memory_usage_percent = (
                        (total_memory - freeable_memory) / total_memory
                    ) * 100

                comparison[instance_id] = {
                    "cpu_avg": (
                        df["CPUUtilization"].mean()
                        if "CPUUtilization" in df.columns
                        else 0
                    ),
                    "memory_usage_percent": memory_usage_percent,
                    "connections_avg": (
                        df["DatabaseConnections"].mean()
                        if "DatabaseConnections" in df.columns
                        else 0
                    ),
                    "read_iops": (
                        df["ReadIOPS"].mean() if "ReadIOPS" in df.columns else 0
                    ),
                    "write_iops": (
                        df["WriteIOPS"].mean() if "WriteIOPS" in df.columns else 0
                    ),
                }

            return comparison
        except Exception as e:
            logger.error(f"리소스 사용률 비교 오류: {e}")
            return {}

    async def _generate_cluster_html_report(
        self,
        cluster_info,
        instance_reports,
        cluster_analysis,
        timestamp,
        cluster_level_metrics=None,
        cluster_events=None,
    ):
        """클러스터 통합 HTML 보고서 생성 - 기존 형태로 단순화"""

        cluster_id = cluster_info["DBClusterIdentifier"]
        engine_info = f"{cluster_info.get('Engine', 'N/A')} {cluster_info.get('EngineVersion', 'N/A')}"

        # 인스턴스 링크 생성
        instance_links = []
        for instance_id, report_info in instance_reports.items():
            role = report_info["role"]
            report_text = report_info["report"]
            if "comprehensive_performance_report_" in report_text:
                import re

                match = re.search(
                    r"comprehensive_performance_report_[^.]+\.html", report_text
                )
                if match:
                    report_filename = match.group(0)
                    instance_links.append(
                        f"""
                    <tr>
                        <td><span class="role-badge {'writer' if report_info['is_writer'] else 'reader'}">{role}</span></td>
                        <td>{instance_id}</td>
                        <td><a href="{report_filename}" target="_blank" class="detail-link">📊 상세 보고서 보기</a></td>
                    </tr>
                    """
                    )

        # 부하 분산 분석 HTML 생성 (제거)
        load_analysis_html = ""

        # 리소스 비교 HTML 생성
        resource_comparison_html = self._generate_resource_comparison_html(
            cluster_analysis.get("resource_comparison", {})
        )

        return f"""<!DOCTYPE html>
<html lang="ko">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Aurora 클러스터 성능 보고서 - {cluster_id}</title>
    <style>
        body {{ font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; margin: 0; padding: 20px; background: #f5f7fa; }}
        .container {{ max-width: 1200px; margin: 0 auto; background: white; border-radius: 10px; box-shadow: 0 4px 6px rgba(0,0,0,0.1); }}
        .header {{ background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 30px; border-radius: 10px 10px 0 0; }}
        .header h1 {{ margin: 0; font-size: 2.2em; }}
        .header .subtitle {{ margin-top: 10px; opacity: 0.9; font-size: 1.1em; }}
        
        .summary-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(250px, 1fr)); gap: 20px; padding: 30px; }}
        .summary-card {{ background: #f8f9fa; border-radius: 8px; padding: 20px; border-left: 4px solid #007bff; }}
        .summary-card h3 {{ margin: 0 0 10px 0; color: #333; }}
        .summary-card .value {{ font-size: 1.8em; font-weight: bold; color: #007bff; }}
        
        .section {{ margin: 20px 30px; }}
        .section-header {{ background: #e9ecef; padding: 15px; border-radius: 8px; font-weight: bold; font-size: 1.2em; color: #495057; }}
        .section-content {{ padding: 20px 0; }}
        
        .instance-table {{ width: 100%; border-collapse: collapse; margin: 20px 0; }}
        .instance-table th, .instance-table td {{ padding: 12px; text-align: left; border-bottom: 1px solid #dee2e6; }}
        .instance-table th {{ background: #f8f9fa; font-weight: bold; }}
        
        .role-badge {{ padding: 4px 8px; border-radius: 4px; font-size: 0.8em; font-weight: bold; }}
        .role-badge.writer {{ background: #d4edda; color: #155724; }}
        .role-badge.reader {{ background: #d1ecf1; color: #0c5460; }}
        
        .detail-link {{ color: #007bff; text-decoration: none; font-weight: bold; }}
        .detail-link:hover {{ text-decoration: underline; }}
        
        .recommendation {{ background: #fff3cd; border: 1px solid #ffeaa7; border-radius: 8px; padding: 15px; margin: 15px 0; }}
        .recommendation h4 {{ margin: 0 0 10px 0; color: #856404; }}
        
        .resource-grid {{ display: grid; grid-template-columns: repeat(2, 1fr); gap: 20px; margin: 20px 0; }}
        .resource-card {{ background: #f8f9fa; border-radius: 8px; padding: 15px; }}
        .resource-card h4 {{ margin: 0 0 15px 0; color: #495057; }}
        .metric-bar {{ background: #e9ecef; height: 20px; border-radius: 10px; margin: 5px 0; position: relative; }}
        .metric-fill {{ height: 100%; border-radius: 10px; }}
        .metric-label {{ font-size: 0.9em; color: #6c757d; }}
        
        @media (max-width: 768px) {{
            .summary-grid {{ grid-template-columns: 1fr; }}
            .resource-grid {{ grid-template-columns: 1fr; }}
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🏗️ Aurora 클러스터 성능 보고서</h1>
            <div class="subtitle">클러스터: {cluster_id} | 엔진: {engine_info} | 생성일시: {timestamp}</div>
        </div>
        
        <div class="summary-grid">
            <div class="summary-card">
                <h3>📊 클러스터 구성</h3>
                <div class="value">{len(cluster_info['DBClusterMembers'])}개 인스턴스</div>
                <div>Writer: 1개, Reader: {len([m for m in cluster_info['DBClusterMembers'] if not m['IsClusterWriter']])}개</div>
            </div>
            <div class="summary-card">
                <h3>🔄 클러스터 상태</h3>
                <div class="value" style="color: #28a745">{cluster_info.get('Status', 'AVAILABLE')}</div>
                <div>Multi-AZ: {'Yes' if cluster_info.get('MultiAZ', False) else 'No'}</div>
            </div>
            <div class="summary-card">
                <h3>🔐 보안 설정</h3>
                <div class="value">🔒</div>
                <div>암호화: {'활성화' if cluster_info.get('StorageEncrypted', False) else '비활성화'}</div>
            </div>
            <div class="summary-card">
                <h3>💾 백업 설정</h3>
                <div class="value">{cluster_info.get('BackupRetentionPeriod', 0)}일</div>
                <div>자동 백업: {'활성화' if cluster_info.get('BackupRetentionPeriod', 0) > 0 else '비활성화'}</div>
            </div>
        </div>
        
        <div class="section">
            <div class="section-header">📋 인스턴스별 상세 보고서</div>
            <div class="section-content">
                <table class="instance-table">
                    <thead>
                        <tr>
                            <th>역할</th>
                            <th>인스턴스 ID</th>
                            <th>상세 분석</th>
                        </tr>
                    </thead>
                    <tbody>
                        {''.join(instance_links)}
                    </tbody>
                </table>
                <div class="recommendation">
                    <h4>💡 사용 가이드</h4>
                    <p>각 인스턴스의 "📊 상세 보고서 보기" 링크를 클릭하면 해당 인스턴스의 상세한 성능 분석 결과를 확인할 수 있습니다.</p>
                </div>
            </div>
        </div>
        
        {load_analysis_html}
        
        {resource_comparison_html}
    </div>
</body>
</html>"""

    def _generate_load_analysis_html(self, load_distribution: Dict) -> str:
        """부하 분산 분석 HTML 생성 (비활성화)"""
        return ""

    def _generate_resource_comparison_html(self, resource_comparison: Dict) -> str:
        """리소스 비교 HTML 생성"""
        if not resource_comparison:
            return ""

        cards_html = ""
        for instance_id, metrics in resource_comparison.items():
            cpu_usage = metrics.get("cpu_avg", 0)
            memory_usage = metrics.get("memory_usage_percent", 0)
            connections_avg = metrics.get("connections_avg", 0)
            read_iops = metrics.get("read_iops", 0)
            write_iops = metrics.get("write_iops", 0)

            cards_html += f"""
            <div class="resource-card">
                <h4>{instance_id}</h4>
                <div class="metric-label">CPU 사용률: {cpu_usage:.1f}%</div>
                <div class="metric-bar">
                    <div class="metric-fill" style="width: {min(cpu_usage, 100)}%; background: #007bff;"></div>
                </div>
                
                <div class="metric-label">메모리 사용률: {memory_usage:.1f}%</div>
                <div class="metric-bar">
                    <div class="metric-fill" style="width: {min(memory_usage, 100)}%; background: #28a745;"></div>
                </div>
                
                <div class="metric-label">평균 연결 수: {connections_avg:.1f}</div>
                <div class="metric-label">Read IOPS: {read_iops:.1f}</div>
                <div class="metric-label">Write IOPS: {write_iops:.1f}</div>
            </div>"""

        return f"""
        <div class="section">
            <div class="section-header">📊 인스턴스별 리소스 사용률 비교</div>
            <div class="section-content">
                <div class="resource-grid">
                    {cards_html}
                </div>
            </div>
        </div>"""

    def _generate_cluster_metrics_table(self, cluster_metrics: Dict) -> str:
        """클러스터 메트릭 표 HTML 생성"""
        if not cluster_metrics:
            return '<div class="no-data">클러스터 메트릭 데이터가 없습니다.</div>'

        # 주요 메트릭들에 대한 통계 계산
        important_metrics = [
            ("CPUUtilization", "CPU 사용률", "%"),
            ("FreeableMemory", "사용 가능한 메모리", "MB"),
            ("ReadIOPS", "읽기 IOPS", "IOPS"),
            ("WriteIOPS", "쓰기 IOPS", "IOPS"),
            ("DatabaseConnections", "데이터베이스 연결 수", "개"),
            ("AuroraReplicaLag", "Aurora 복제 지연", "ms"),
        ]

        table_html = """
        <table class="metrics-table">
            <thead>
                <tr>
                    <th>메트릭</th>
                    <th>평균</th>
                    <th>최대값</th>
                    <th>최소값</th>
                    <th>단위</th>
                </tr>
            </thead>
            <tbody>
        """

        for metric_name, display_name, unit in important_metrics:
            if metric_name in cluster_metrics:
                datapoints = cluster_metrics[metric_name]

                if datapoints:
                    avg_values = [float(point["Average"]) for point in datapoints]
                    max_values = [float(point["Maximum"]) for point in datapoints]
                    min_values = [float(point["Minimum"]) for point in datapoints]

                    overall_avg = sum(avg_values) / len(avg_values)
                    overall_max = max(max_values)
                    overall_min = min(min_values)

                    # 메모리는 MB 단위로 변환
                    if metric_name == "FreeableMemory":
                        overall_avg = overall_avg / (1024 * 1024)
                        overall_max = overall_max / (1024 * 1024)
                        overall_min = overall_min / (1024 * 1024)

                    table_html += f"""
                    <tr>
                        <td class="metric-name">{display_name}</td>
                        <td class="metric-value">{overall_avg:.2f}</td>
                        <td class="metric-value">{overall_max:.2f}</td>
                        <td class="metric-value">{overall_min:.2f}</td>
                        <td>{unit}</td>
                    </tr>
                    """

        table_html += "</tbody></table>"

        if len([m for m, _, _ in important_metrics if m in cluster_metrics]) == 0:
            return '<div class="no-data">표시할 메트릭 데이터가 없습니다.</div>'

        return table_html

    def _generate_events_table(self, events: List[Dict]) -> str:
        """이벤트 테이블 HTML 생성"""
        if not events:
            return '<div class="no-data">최근 7일간 이벤트가 없습니다.</div>'

        # 최근 20개 이벤트만 표시
        recent_events = events[:20]

        table_html = """
        <table class="events-table">
            <thead>
                <tr>
                    <th>일시</th>
                    <th>심각도</th>
                    <th>소스</th>
                    <th>메시지</th>
                    <th>카테고리</th>
                </tr>
            </thead>
            <tbody>
        """

        for event in recent_events:
            severity_class = event["severity"].lower()
            categories = ", ".join(event.get("event_categories", []))

            table_html += f"""
            <tr>
                <td>{event['date']}</td>
                <td><span class="severity-badge {event['severity']}">{event['severity']}</span></td>
                <td>{event.get('source_id', 'N/A')}</td>
                <td>{event['message']}</td>
                <td>{categories}</td>
            </tr>
            """

        table_html += "</tbody></table>"

        if len(events) > 20:
            table_html += f'<div style="text-align: center; margin-top: 10px; color: #6c757d;">총 {len(events)}개 이벤트 중 최근 20개만 표시</div>'

        return table_html

    def _generate_chart_scripts(self, cluster_metrics: Dict) -> str:
        """차트 생성 비활성화 - 간단한 메트릭 표시만"""
        return ""

    def _get_balance_color(self, score):
        """부하 분산 점수에 따른 색상 반환"""
        if score >= 80:
            return "#28a745"  # 녹색
        elif score >= 60:
            return "#ffc107"  # 노란색
        else:
            return "#dc3545"  # 빨간색

    def _get_balance_status(self, score):
        """부하 분산 점수에 따른 상태 메시지"""
        if score >= 80:
            return "우수한 분산"
        elif score >= 60:
            return "보통 분산"
        else:
            return "개선 필요"

    def _generate_load_distribution_html(self, load_analysis):
        """부하 분산 분석 HTML 생성"""
        if not load_analysis:
            return """
            <div class="recommendation">
                <h4 style="color: #dc3545;">⚠️ 부하 분산 데이터 분석 불가</h4>
                <p><strong>원인:</strong> 클러스터 메트릭 데이터가 충분하지 않거나 분석 중 오류가 발생했습니다.</p>
                <p><strong>확인사항:</strong></p>
                <ul>
                    <li>각 인스턴스의 CloudWatch 메트릭이 정상적으로 수집되고 있는지 확인</li>
                    <li>Writer와 Reader 인스턴스가 모두 활성 상태인지 확인</li>
                    <li>메트릭 수집 기간 동안 충분한 데이터가 있는지 확인</li>
                </ul>
                <p><strong>권장조치:</strong> 개별 인스턴스 상세 보고서를 확인하여 각 인스턴스의 성능 상태를 점검하세요.</p>
            </div>
            """

        writer = load_analysis.get("writer", {})
        readers = load_analysis.get("readers", [])

        html = f"""
        <div style="margin: 10px 0; padding: 10px; background: #e3f2fd; border-radius: 5px;">
            <h4>Writer 인스턴스</h4>
            <p>CPU: {writer.get('cpu', 0):.1f}% | 연결 수: {writer.get('connections', 0):.1f}</p>
        </div>
        """

        if readers:
            html += "<h4>Reader 인스턴스들</h4>"
            for reader in readers:
                html += f"""
                <div style="margin: 10px 0; padding: 10px; background: #f8f9fa; border-radius: 5px;">
                    <strong>{reader['instance_id']}</strong><br>
                    CPU: {reader['cpu']:.1f}% | 연결 수: {reader['connections']:.1f}
                </div>
                """

        return html

    def _generate_cluster_recommendations(self, cluster_analysis, cluster_info):
        """클러스터 최적화 권장사항 생성"""
        recommendations = []

        # 부하 분산 기반 권장사항
        load_dist = cluster_analysis.get("load_distribution", {})
        balance_score = load_dist.get("balance_score", 0)

        if balance_score < 60:
            recommendations.append(
                {
                    "priority": "높음",
                    "title": "부하 분산 개선 필요",
                    "description": "Writer와 Reader 간 부하 분산이 불균형합니다. 읽기 쿼리를 Reader 엔드포인트로 분산하세요.",
                    "action": "애플리케이션에서 읽기 전용 쿼리를 Reader 엔드포인트로 라우팅",
                }
            )

        # 암호화 권장사항
        if not cluster_info.get("StorageEncrypted"):
            recommendations.append(
                {
                    "priority": "중간",
                    "title": "스토리지 암호화 활성화",
                    "description": "데이터 보안을 위해 스토리지 암호화를 활성화하는 것을 권장합니다.",
                    "action": "새 클러스터 생성 시 암호화 옵션 활성화",
                }
            )

        # 백업 설정 권장사항
        backup_retention = cluster_info.get("BackupRetentionPeriod", 0)
        if backup_retention < 7:
            recommendations.append(
                {
                    "priority": "중간",
                    "title": "백업 보존 기간 연장",
                    "description": f"현재 백업 보존 기간이 {backup_retention}일입니다. 최소 7일 이상 권장합니다.",
                    "action": "백업 보존 기간을 7-35일로 설정",
                }
            )

        # HTML 생성
        if not recommendations:
            return "<div class='recommendation'><h4>✅ 우수한 클러스터 설정</h4><p>현재 클러스터 설정이 모범 사례를 잘 따르고 있습니다.</p></div>"

        html = ""
        for rec in recommendations:
            priority_color = {
                "높음": "#dc3545",
                "중간": "#ffc107",
                "낮음": "#28a745",
            }.get(rec["priority"], "#6c757d")
            html += f"""
            <div class="recommendation">
                <h4 style="color: {priority_color};">🎯 {rec['title']} (우선순위: {rec['priority']})</h4>
                <p><strong>설명:</strong> {rec['description']}</p>
                <p><strong>권장 조치:</strong> {rec['action']}</p>
            </div>
            """

        return html

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
        """CloudWatch 클라이언트 설정

        리팩토링: Week 2 - CloudWatchManager 모듈로 위임
        하위 호환성을 위해 메서드 유지
        """
        result = self.cloudwatch_manager.setup_cloudwatch_client(region_name)
        # 메인 서버의 cloudwatch 속성도 업데이트
        self.cloudwatch = self.cloudwatch_manager.cloudwatch
        return result

    async def collect_db_metrics(
        self,
        db_instance_identifier: str,
        hours: int = 24,
        metrics: Optional[List[str]] = None,
        region: str = "us-east-1",
    ) -> str:
        """CloudWatch에서 데이터베이스 메트릭 수집

        리팩토링: Week 2 - CloudWatchManager 모듈로 위임
        하위 호환성을 위해 메서드 유지
        """
        result = await self.cloudwatch_manager.collect_db_metrics(
            db_instance_identifier, hours, metrics, region
        )
        # current_instance_class 동기화
        self.current_instance_class = self.cloudwatch_manager.current_instance_class
        return result

    async def analyze_metric_correlation(
        self, csv_file: str, target_metric: str = "CPUUtilization", top_n: int = 10
    ) -> str:
        """메트릭 간 상관관계 분석

        리팩토링: Week 2 - CloudWatchManager 모듈로 위임
        하위 호환성을 위해 메서드 유지
        """
        return await self.cloudwatch_manager.analyze_metric_correlation(
            csv_file, target_metric, top_n
        )

    def load_metric_thresholds(self) -> dict:
        """input 폴더에서 최신 임계값 설정 파일 로드"""
        try:
            input_dir = Path(__file__).parent / "input"
            if not input_dir.exists():
                input_dir.mkdir(exist_ok=True)

            # metric_thresholds_*.txt 파일 중 최신 파일 찾기
            threshold_files = list(input_dir.glob("metric_thresholds_*.txt"))
            if not threshold_files:
                return self.get_default_thresholds()

            latest_file = max(threshold_files, key=lambda f: f.stat().st_mtime)

            thresholds = {}
            current_metric = None

            with open(latest_file, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith("#"):
                        continue

                    if line.startswith("[") and line.endswith("]"):
                        current_metric = line[1:-1]
                        thresholds[current_metric] = {}
                    elif "=" in line and current_metric:
                        key, value = line.split("=", 1)
                        key = key.strip()
                        value = value.strip()

                        # 숫자 변환
                        if key in [
                            "min",
                            "max",
                            "high_threshold",
                            "low_threshold",
                            "spike_factor",
                        ]:
                            try:
                                thresholds[current_metric][key] = (
                                    float(value) if value != "None" else None
                                )
                            except ValueError:
                                thresholds[current_metric][key] = None
                        else:
                            thresholds[current_metric][key] = value

            return thresholds

        except Exception as e:
            debug_log(f"임계값 파일 로드 실패: {e}")
            return self.get_default_thresholds()

    def get_default_thresholds(self) -> dict:
        """기본 임계값 반환"""
        return {
            "CPUUtilization": {
                "min": 0,
                "max": 100,
                "high_threshold": 80,
                "method": "absolute",
            },
            "DatabaseConnections": {
                "min": 0,
                "max": None,
                "spike_factor": 3.0,
                "method": "spike",
            },
            "FreeableMemory": {
                "min": 0,
                "max": None,
                "low_threshold": 0.1,
                "method": "percentage",
            },
            "ReadLatency": {
                "min": 0,
                "max": None,
                "high_threshold": 0.01,
                "method": "absolute",
            },
            "WriteLatency": {
                "min": 0,
                "max": None,
                "high_threshold": 0.01,
                "method": "absolute",
            },
            "ReadIOPS": {"min": 0, "max": None, "spike_factor": 5.0, "method": "spike"},
            "WriteIOPS": {
                "min": 0,
                "max": None,
                "spike_factor": 5.0,
                "method": "spike",
            },
            "NetworkReceiveThroughput": {
                "min": 0,
                "max": None,
                "spike_factor": 3.0,
                "method": "spike",
            },
            "NetworkTransmitThroughput": {
                "min": 0,
                "max": None,
                "spike_factor": 3.0,
                "method": "spike",
            },
            "DBLoad": {
                "min": 0,
                "max": None,
                "high_threshold": 2.0,
                "method": "dynamic",
            },
            "DBLoadCPU": {
                "min": 0,
                "max": None,
                "high_threshold": 2.0,
                "method": "dynamic",
            },
            "DBLoadNonCPU": {
                "min": 0,
                "max": None,
                "high_threshold": 1.0,
                "method": "dynamic",
            },
            "BufferCacheHitRatio": {
                "min": 0,
                "max": 100,
                "low_threshold": 80,
                "method": "percentage",
            },
        }

    def get_dynamic_dbload_threshold(self, instance_class: str) -> float:
        """인스턴스 클래스별 DBLoad 임계값 반환"""
        # vCPU 수 기반 임계값 설정
        vcpu_mapping = {
            # t3/t4g 시리즈
            "t3.micro": 2,
            "t3.small": 2,
            "t3.medium": 2,
            "t3.large": 2,
            "t3.xlarge": 4,
            "t3.2xlarge": 8,
            "t4g.micro": 2,
            "t4g.small": 2,
            "t4g.medium": 2,
            "t4g.large": 2,
            "t4g.xlarge": 4,
            "t4g.2xlarge": 8,
            # r5/r6i 시리즈
            "r5.large": 2,
            "r5.xlarge": 4,
            "r5.2xlarge": 8,
            "r5.4xlarge": 16,
            "r5.8xlarge": 32,
            "r5.12xlarge": 48,
            "r5.16xlarge": 64,
            "r5.24xlarge": 96,
            "r6i.large": 2,
            "r6i.xlarge": 4,
            "r6i.2xlarge": 8,
            "r6i.4xlarge": 16,
            "r6i.8xlarge": 32,
            "r6i.12xlarge": 48,
            "r6i.16xlarge": 64,
            "r6i.24xlarge": 96,
            "r6i.32xlarge": 128,
            # m5/m6i 시리즈
            "m5.large": 2,
            "m5.xlarge": 4,
            "m5.2xlarge": 8,
            "m5.4xlarge": 16,
            "m5.8xlarge": 32,
            "m5.12xlarge": 48,
            "m5.16xlarge": 64,
            "m5.24xlarge": 96,
            "m6i.large": 2,
            "m6i.xlarge": 4,
            "m6i.2xlarge": 8,
            "m6i.4xlarge": 16,
            "m6i.8xlarge": 32,
            "m6i.12xlarge": 48,
            "m6i.16xlarge": 64,
            "m6i.24xlarge": 96,
            "m6i.32xlarge": 128,
        }

        vcpu_count = vcpu_mapping.get(instance_class, 2)  # 기본값 2 vCPU
        # DBLoad 임계값 = vCPU 수 * 0.8 (80% 활용률 기준)
        return vcpu_count * 0.8

    async def detect_metric_outliers(
        self, csv_file: str, std_threshold: float = 3.0, skip_html_report: bool = False
    ) -> str:
        """개선된 아웃라이어 탐지 - 메트릭별 맞춤 기준 적용"""
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

            # 임계값 파일에서 로드
            metric_thresholds = self.load_metric_thresholds()

            result = f"🔍 개선된 아웃라이어 탐지 결과:\n\n"
            outlier_summary = []
            critical_issues = []

            # 각 메트릭에 대해 맞춤 아웃라이어 탐지
            for column in df.columns:
                series = df[column]
                config = metric_thresholds.get(column, {"method": "iqr"})
                outliers = pd.Series(dtype=float)

                if config["method"] == "dynamic":
                    # 동적 임계값 기준 (DBLoad 등)
                    if column in ["DBLoad", "DBLoadCPU", "DBLoadNonCPU"]:
                        instance_class = getattr(
                            self, "current_instance_class", "r5.large"
                        )
                        dynamic_threshold = self.get_dynamic_dbload_threshold(
                            instance_class
                        )
                        outliers = series[series > dynamic_threshold]
                    else:
                        # 다른 메트릭은 기본 임계값 사용
                        if "high_threshold" in config:
                            outliers = series[series > config["high_threshold"]]

                elif config["method"] == "absolute":
                    # 절대값 기준 (CPU, Latency 등)
                    if "high_threshold" in config:
                        outliers = series[series > config["high_threshold"]]
                    if "low_threshold" in config:
                        low_outliers = series[series < config["low_threshold"]]
                        outliers = pd.concat([outliers, low_outliers])

                elif config["method"] == "spike":
                    # 급격한 변화 탐지 (Connections, IOPS, Network 등)
                    median = series.median()
                    mad = (series - median).abs().median()
                    threshold = median + config.get("spike_factor", 3.0) * mad
                    outliers = series[series > threshold]

                elif config["method"] == "percentage":
                    # 백분율 기준 (Memory, Cache Hit Ratio 등)
                    if "low_threshold" in config:
                        outliers = series[series < config["low_threshold"]]

                else:
                    # IQR 방식 (기본값)
                    Q1 = series.quantile(0.25)
                    Q3 = series.quantile(0.75)
                    IQR = Q3 - Q1
                    lower_bound = Q1 - 1.5 * IQR
                    upper_bound = Q3 + 1.5 * IQR
                    outliers = series[(series < lower_bound) | (series > upper_bound)]

                # 물리적 제약 적용
                if config.get("min") is not None:
                    outliers = outliers[outliers >= config["min"]]
                if config.get("max") is not None:
                    outliers = outliers[outliers <= config["max"]]

                if not outliers.empty:
                    severity = "🔥" if len(outliers) > len(series) * 0.1 else "⚠️"
                    result += f"{severity} {column} 이상 탐지 ({len(outliers)}개):\n"

                    # 심각도 판정
                    if column == "CPUUtilization" and outliers.max() > 90:
                        critical_issues.append(
                            f"CPU 사용률 위험 수준: {outliers.max():.1f}%"
                        )
                    elif (
                        column in ["ReadLatency", "WriteLatency"]
                        and outliers.max() > 0.1
                    ):
                        critical_issues.append(
                            f"{column} 지연시간 급증: {outliers.max():.3f}초"
                        )
                    elif column in ["DBLoad", "DBLoadCPU", "DBLoadNonCPU"]:
                        # 동적 임계값 기반 판정
                        instance_class = getattr(
                            self, "current_instance_class", "r5.large"
                        )
                        dynamic_threshold = self.get_dynamic_dbload_threshold(
                            instance_class
                        )
                        if (
                            outliers.max() > dynamic_threshold * 1.5
                        ):  # 임계값의 150% 초과 시 심각
                            critical_issues.append(
                                f"{column} 부하 과다 (인스턴스: {instance_class}): {outliers.max():.1f} (임계값: {dynamic_threshold:.1f})"
                            )

                    # 상위 3개 이상값만 표시
                    top_outliers = outliers.nlargest(3)
                    for timestamp, value in top_outliers.items():
                        result += f"   • {timestamp}: {value:.2f}\n"

                    if len(outliers) > 3:
                        result += f"   ... 및 {len(outliers) - 3}개 더\n"
                    result += "\n"

                    outlier_summary.append(
                        {
                            "metric": column,
                            "count": len(outliers),
                            "max_value": outliers.max(),
                            "severity": (
                                "Critical"
                                if len(outliers) > len(series) * 0.1
                                else "Warning"
                            ),
                        }
                    )
                else:
                    result += f"✅ {column}: 정상 범위\n"

            # 심각한 문제 요약
            if critical_issues:
                result += "\n🚨 즉시 조치 필요:\n"
                for issue in critical_issues:
                    result += f"• {issue}\n"

            # 전체 요약
            if outlier_summary:
                result += "\n📊 탐지 요약:\n"
                critical_count = sum(
                    1 for s in outlier_summary if s["severity"] == "Critical"
                )
                warning_count = len(outlier_summary) - critical_count
                result += f"• 심각: {critical_count}개 메트릭\n"
                result += f"• 경고: {warning_count}개 메트릭\n"
            else:
                result += "\n✅ 모든 메트릭이 정상 범위 내에 있습니다.\n"

            # 임계값 정보 HTML 생성
            threshold_html = self.generate_threshold_html(metric_thresholds)

            # HTML 보고서 생성 (선택적)
            debug_log(f"skip_html_report: {skip_html_report}")
            if not skip_html_report:
                debug_log("HTML 보고서 생성 중...")
                html_report_path = (
                    OUTPUT_DIR
                    / f"outlier_analysis_{csv_file.replace('.csv', '')}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
                )
                self.save_outlier_html_report(result, threshold_html, html_report_path)
                result += f"\n📄 상세 보고서: {self.format_file_link(str(html_report_path), '아웃라이어 분석 보고서 열기')}\n"
                result += "💡 보고서에서 '임계값 설정' 버튼을 클릭하여 상세 설정을 확인하세요.\n"
            else:
                debug_log("HTML 보고서 생성 건너뜀")

            return result

        except Exception as e:
            return f"아웃라이어 탐지 중 오류 발생: {str(e)}"

    def generate_threshold_html(self, thresholds: dict) -> str:
        """임계값 설정을 HTML 테이블로 생성 (Week 3: ReportGenerator로 위임)"""
        return self.report_generator.generate_threshold_html(thresholds)

    def save_outlier_html_report(
        self, result: str, threshold_html: str, report_path: Path
    ):
        """아웃라이어 분석 HTML 보고서 저장 (Week 3: ReportGenerator로 위임)"""
        self.report_generator.save_outlier_html_report(result, threshold_html, report_path)

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

            # 그래프 생성 제거됨 - 텍스트 결과만 반환
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

    async def debug_cloudwatch_collection(
        self, database_secret: str, start_time: str, end_time: str
    ) -> str:
        """CloudWatch 수집 디버그 함수"""
        try:
            # 시간 변환 (KST -> UTC)
            start_dt = self.convert_kst_to_utc(start_time)
            end_dt = self.convert_kst_to_utc(end_time)

            # 시크릿에서 DB 정보 가져오기
            secrets_client = boto3.client(
                "secretsmanager", region_name="ap-northeast-2"
            )
            secret_response = secrets_client.get_secret_value(SecretId=database_secret)
            secret_data = json.loads(secret_response["SecretString"])

            # DB 클러스터 식별자 추출
            db_host = secret_data.get("host", "")
            if ".cluster-" in db_host:
                cluster_identifier = db_host.split(".cluster-")[0]
            else:
                return "❌ Aurora 클러스터를 찾을 수 없습니다"

            # CloudWatch 수집 시도
            logs_client = boto3.client("logs", region_name="ap-northeast-2")
            log_group_name = f"/aws/rds/cluster/{cluster_identifier}/slowquery"

            start_time_ms = int(start_dt.timestamp() * 1000)
            end_time_ms = int(end_dt.timestamp() * 1000)

            print(f"DEBUG: 클러스터 ID: {cluster_identifier}", file=sys.stderr)
            print(f"DEBUG: 로그 그룹: {log_group_name}", file=sys.stderr)
            print(f"DEBUG: 시간 범위: {start_dt} ~ {end_dt} (UTC)", file=sys.stderr)
            print(f"DEBUG: 타임스탬프: {start_time_ms} ~ {end_time_ms}", file=sys.stderr)

            response = logs_client.filter_log_events(
                logGroupName=log_group_name,
                startTime=start_time_ms,
                endTime=end_time_ms,
            )

            events_count = len(response.get("events", []))
            print(f"DEBUG: 검색된 이벤트 수: {events_count}", file=sys.stderr)

            if events_count > 0:
                # 첫 번째 이벤트 확인
                first_event = response["events"][0]
                print(f"DEBUG: 첫 번째 이벤트 타임스탬프: {first_event['timestamp']}", file=sys.stderr)
                print(
                    f"DEBUG: 첫 번째 이벤트 메시지 미리보기: {first_event['message'][:100]}...", file=sys.stderr
                )

                # 파싱 테스트
                message = first_event["message"].replace("\\n", "\n")
                print(f"DEBUG: Query_time 패턴 존재? {'# Query_time: ' in message}", file=sys.stderr)

                if "# Query_time: " in message:
                    lines = message.split("\n")
                    print(f"DEBUG: 분할된 라인 수: {len(lines)}", file=sys.stderr)
                    for i, line in enumerate(lines[:5]):  # 처음 5줄만
                        print(f"DEBUG: Line {i}: {repr(line)}", file=sys.stderr)

            return f"✅ 디버그 완료: {events_count}개 이벤트 발견"

        except Exception as e:
            import traceback

            return f"❌ 디버그 실패: {str(e)}\n{traceback.format_exc()}"

    async def collect_slow_queries(
        self, database_secret: str, start_time: str = None, end_time: str = None
    ) -> str:
        """슬로우 쿼리 수집 (CloudWatch → 로컬파일 → Performance Schema 순서)"""
        try:
            # 시간대 처리 (KST -> UTC)
            if start_time and end_time:
                start_dt = self.convert_kst_to_utc(start_time)
                end_dt = self.convert_kst_to_utc(end_time)
            else:
                # 기본값: 24시간 전부터 현재까지 (UTC 기준)
                end_dt = datetime.utcnow()
                start_dt = end_dt - timedelta(hours=24)

            # 시크릿에서 DB 정보 가져오기
            secrets_client = boto3.client(
                "secretsmanager", region_name=self.default_region
            )
            secret_response = secrets_client.get_secret_value(SecretId=database_secret)
            secret_data = json.loads(secret_response["SecretString"])

            # DB 클러스터 식별자 추출
            db_host = secret_data.get("host", "")
            if ".cluster-" in db_host:
                cluster_identifier = db_host.split(".cluster-")[0]
            else:
                return "❌ Aurora 클러스터를 찾을 수 없습니다"

            # 1단계: CloudWatch Logs에서 슬로우 쿼리 수집 시도
            cloudwatch_result = await self._collect_from_cloudwatch(
                cluster_identifier, start_dt, end_dt
            )

            if cloudwatch_result["success"]:
                return cloudwatch_result["message"]

            # 2단계: 로컬 파일에서 수집 시도
            local_file_result = await self._collect_from_local_file(
                database_secret, start_dt, end_dt
            )

            if local_file_result["success"]:
                return local_file_result["message"]

            # 3단계: Performance Schema에서 수집 시도
            performance_result = await self._collect_from_performance_schema(
                database_secret, start_dt, end_dt
            )

            if performance_result["success"]:
                return performance_result["message"]

            # 4단계: Log exports 설정 제안
            return await self._suggest_log_exports_setup(
                cluster_identifier, cloudwatch_result["message"]
            )

        except ValueError as ve:
            return f"❌ {str(ve)}"
        except Exception as e:
            import traceback

            return f"❌ 슬로우 쿼리 수집 실패: {str(e)}\n{traceback.format_exc()}"

    async def _collect_from_local_file(
        self, database_secret: str, start_dt: datetime, end_dt: datetime
    ) -> dict:
        """로컬 슬로우 쿼리 파일에서 수집"""
        try:
            # 기존 연결 정리
            if self.shared_connection or self.shared_cursor:
                self.cleanup_shared_connection()

            # 데이터베이스 연결
            if not self.setup_shared_connection(database_secret, None, True):
                return {"success": False, "message": "데이터베이스 연결 실패"}

            cursor = self.shared_cursor

            # 슬로우 쿼리 로그 파일 경로 확인
            cursor.execute("SHOW VARIABLES LIKE 'slow_query_log_file'")
            result = cursor.fetchone()
            if not result:
                return {
                    "success": False,
                    "message": "슬로우 쿼리 로그 파일 경로를 찾을 수 없음",
                }

            log_file_path = result[1]

            # 파일 내용 읽기 시도 (LOAD_FILE 함수 사용)
            try:
                cursor.execute(f"SELECT LOAD_FILE('{log_file_path}')")
                file_content = cursor.fetchone()

                if not file_content or not file_content[0]:
                    return {
                        "success": False,
                        "message": f"슬로우 쿼리 로그 파일을 읽을 수 없음: {log_file_path}",
                    }

                content = (
                    file_content[0].decode("utf-8")
                    if isinstance(file_content[0], bytes)
                    else str(file_content[0])
                )

                # 시간 범위 필터링을 위한 로그 파싱
                slow_queries = self._parse_slow_query_log(content, start_dt, end_dt)

                if slow_queries:
                    # 파일 생성
                    current_date = datetime.now().strftime("%Y%m%d_%H%M%S")
                    filename = f"slow_queries_local_file_{current_date}.sql"
                    file_path = SQL_DIR / filename

                    with open(file_path, "w", encoding="utf-8") as f:
                        f.write(f"-- 로컬 슬로우 쿼리 파일 수집 결과\n")
                        f.write(f"-- 파일 경로: {log_file_path}\n")
                        f.write(
                            f"-- 수집 기간: {self.convert_utc(start_dt)} ~ {self.convert_utc(end_dt)} (KST)\n"
                        )
                        f.write(f"-- 총 {len(slow_queries)}개의 쿼리\n\n")

                        for i, query in enumerate(slow_queries, 1):
                            f.write(f"-- 슬로우 쿼리 #{i}\n")
                            if "time" in query:
                                f.write(f"-- {query['time']}\n")
                            if "query_time" in query:
                                f.write(f"-- {query['query_time']}\n")
                            if "user_host" in query:
                                f.write(f"-- {query['user_host']}\n")
                            f.write(f"{query['sql']};\n\n")

                    # S3에 업로드 및 Pre-signed URL 생성
                    try:
                        import boto3
                        s3_client = boto3.client('s3', region_name=self.default_region)
                        s3_bucket = QUERY_RESULTS_DEV_BUCKET
                        s3_key = f"sql-files/slow-queries/{filename}"

                        s3_client.upload_file(str(file_path), s3_bucket, s3_key)
                        logger.info(f"SQL 파일 S3 업로드 완료: s3://{s3_bucket}/{s3_key}")

                        presigned_url = s3_client.generate_presigned_url(
                            'get_object',
                            Params={'Bucket': s3_bucket, 'Key': s3_key},
                            ExpiresIn=604800  # 7일
                        )

                        return {
                            "success": True,
                            "message": f"✅ 로컬 파일에서 슬로우 쿼리 {len(slow_queries)}개 수집 완료: {filename}\n파일 경로: {log_file_path}\n🔗 다운로드 (7일 유효): {presigned_url}",
                        }
                    except Exception as s3_error:
                        logger.error(f"S3 업로드 실패: {s3_error}")
                        return {
                            "success": True,
                            "message": f"✅ 로컬 파일에서 슬로우 쿼리 {len(slow_queries)}개 수집 완료: {filename}\n파일 경로: {log_file_path}",
                        }
                else:
                    return {
                        "success": False,
                        "message": f"로컬 파일에서 해당 시간 범위의 슬로우 쿼리를 찾을 수 없음",
                    }

            except Exception as e:
                return {"success": False, "message": f"로컬 파일 읽기 실패: {str(e)}"}

        except Exception as e:
            return {"success": False, "message": f"로컬 파일 수집 실패: {str(e)}"}

    def _parse_slow_query_log(
        self, content: str, start_dt: datetime, end_dt: datetime
    ) -> list:
        """슬로우 쿼리 로그 내용 파싱"""
        slow_queries = []
        lines = content.split("\n")
        current_query = {}
        sql_lines = []

        for line in lines:
            line = line.strip()

            if line.startswith("# Time:"):
                # 이전 쿼리 저장
                if current_query and sql_lines:
                    current_query["sql"] = " ".join(sql_lines)

                    # 시간 범위 체크
                    if self._is_within_time_range(
                        current_query.get("time", ""), start_dt, end_dt
                    ):
                        slow_queries.append(current_query.copy())

                # 새 쿼리 시작
                current_query = {"time": line}
                sql_lines = []

            elif line.startswith("# Query_time:"):
                current_query["query_time"] = line
            elif line.startswith("# User@Host:"):
                current_query["user_host"] = line
            elif (
                not line.startswith("#")
                and line
                and not line.startswith("SET timestamp")
            ):
                if not line.startswith("use "):
                    sql_lines.append(line)

        # 마지막 쿼리 처리
        if current_query and sql_lines:
            current_query["sql"] = " ".join(sql_lines)
            if self._is_within_time_range(
                current_query.get("time", ""), start_dt, end_dt
            ):
                slow_queries.append(current_query)

        return slow_queries

    def _is_within_time_range(
        self, time_str: str, start_dt: datetime, end_dt: datetime
    ) -> bool:
        """시간 문자열이 범위 내에 있는지 확인"""
        try:
            # # Time: 2025-09-05T14:30:45.123456Z 형식 파싱
            if "Time:" in time_str:
                time_part = time_str.split("Time:")[1].strip()
                # Z를 제거하고 마이크로초 부분 처리
                if "Z" in time_part:
                    time_part = time_part.replace("Z", "")
                if "." in time_part:
                    time_part = time_part.split(".")[0]

                query_time = datetime.strptime(time_part, "%Y-%m-%dT%H:%M:%S")
                return start_dt <= query_time <= end_dt
        except:
            pass
        return True  # 파싱 실패 시 포함

    async def _collect_from_cloudwatch(
        self, cluster_identifier: str, start_dt: datetime, end_dt: datetime
    ) -> dict:
        """CloudWatch Logs에서 인스턴스별로 슬로우 쿼리 수집 (Lambda 사용)"""
        try:
            logger.info(f"Lambda로 Slow Query 수집: {cluster_identifier}")

            # Lambda 호출
            lambda_result = await self._call_lambda('collect-slow-queries-cloudwatch', {
                'cluster_identifier': cluster_identifier,
                'start_time': start_dt.isoformat(),
                'end_time': end_dt.isoformat(),
                'region': self.default_region
            })

            if not lambda_result.get('success'):
                return {
                    "success": False,
                    "message": lambda_result.get('message', lambda_result.get('error', 'Lambda 호출 실패'))
                }

            # Lambda에서 받은 데이터로 파일 생성 (로컬 처리)
            instances_data = lambda_result.get('instances', {})
            instance_files = []
            total_queries = 0

            for instance_id, slow_queries in instances_data.items():
                if slow_queries:
                    # 인스턴스별 파일 생성
                    current_date = datetime.now().strftime("%Y%m%d_%H%M%S")
                    filename = f"slow_queries_{instance_id}_{current_date}.sql"
                    file_path = SQL_DIR / filename

                    with open(file_path, "w", encoding="utf-8") as f:
                        f.write(
                            f"-- CloudWatch Logs 슬로우 쿼리 수집 결과 (인스턴스: {instance_id})\n"
                        )
                        f.write(
                            f"-- 수집 기간: {self.convert_utc(start_dt)} ~ {self.convert_utc(end_dt)} (Local)\n"
                        )
                        f.write(f"-- 총 {len(slow_queries)}개의 쿼리\n\n")

                        for i, query in enumerate(slow_queries, 1):
                            f.write(
                                f"-- 슬로우 쿼리 #{i} (인스턴스: {instance_id})\n"
                            )
                            if "time" in query:
                                f.write(f"-- {query['time']}\n")
                            if "query_time" in query:
                                f.write(f"-- {query['query_time']}\n")
                            if "user_host" in query:
                                f.write(f"-- {query['user_host']}\n")
                            f.write(f"{query['sql']};\n\n")

                    # S3에 업로드 및 Pre-signed URL 생성
                    try:
                        import boto3
                        s3_client = boto3.client('s3', region_name=self.default_region)
                        s3_bucket = QUERY_RESULTS_DEV_BUCKET
                        s3_key = f"sql-files/slow-queries/{filename}"

                        s3_client.upload_file(str(file_path), s3_bucket, s3_key)
                        logger.info(f"SQL 파일 S3 업로드 완료: s3://{s3_bucket}/{s3_key}")

                        presigned_url = s3_client.generate_presigned_url(
                            'get_object',
                            Params={'Bucket': s3_bucket, 'Key': s3_key},
                            ExpiresIn=604800  # 7일
                        )

                        instance_files.append(
                            f"{instance_id}: {filename} ({len(slow_queries)}개)\n  🔗 {presigned_url}"
                        )
                    except Exception as s3_error:
                        logger.error(f"S3 업로드 실패: {s3_error}")
                        instance_files.append(
                            f"{instance_id}: {self.format_file_link(str(file_path), filename)} ({len(slow_queries)}개)"
                        )
                    total_queries += len(slow_queries)

            if instance_files:
                return {
                    "success": True,
                    "message": f"✅ CloudWatch에서 인스턴스별 슬로우 쿼리 수집 완료 (총 {total_queries}개)\n"
                    + "\n".join([f"• {file_info}" for file_info in instance_files]),
                }
            else:
                return {
                    "success": False,
                    "message": f"CloudWatch Logs에서 슬로우 쿼리를 찾을 수 없음",
                }

        except Exception as e:
            logger.error(f"Slow Query 수집 실패: {str(e)}")
            return {"success": False, "message": f"CloudWatch Logs 조회 실패: {str(e)}"}

    async def _collect_from_performance_schema(
        self, database_secret: str, start_dt: datetime, end_dt: datetime
    ) -> dict:
        """Performance Schema에서 슬로우 쿼리 수집"""
        try:
            # 기존 연결 정리
            if self.shared_connection or self.shared_cursor:
                self.cleanup_shared_connection()

            # 데이터베이스 연결
            if not self.setup_shared_connection(database_secret, None, True):
                return {"success": False, "message": "데이터베이스 연결 실패"}

            cursor = self.shared_cursor

            # Performance Schema에서 느린 쿼리 조회
            query = """
            SELECT 
                DIGEST_TEXT as sql_text,
                COUNT_STAR as exec_count,
                AVG_TIMER_WAIT/1000000000000 as avg_time_sec,
                MAX_TIMER_WAIT/1000000000000 as max_time_sec,
                SUM_TIMER_WAIT/1000000000000 as total_time_sec,
                FIRST_SEEN,
                LAST_SEEN
            FROM performance_schema.events_statements_summary_by_digest 
            WHERE AVG_TIMER_WAIT/1000000000000 > 1.0
            AND LAST_SEEN >= %s
            ORDER BY AVG_TIMER_WAIT DESC 
            LIMIT 50
            """

            cursor.execute(query, (start_dt,))
            results = cursor.fetchall()

            if results:
                # 파일 생성
                current_date = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"slow_queries_performance_schema_{current_date}.sql"
                file_path = SQL_DIR / filename

                with open(file_path, "w", encoding="utf-8") as f:
                    f.write(f"-- Performance Schema 슬로우 쿼리 수집 결과\n")
                    f.write(
                        f"-- 수집 기간: {self.convert_utc(start_dt)} ~ {self.convert_utc(end_dt)} (Local)\n"
                    )
                    f.write(f"-- 총 {len(results)}개의 쿼리\n\n")

                    for i, row in enumerate(results, 1):
                        (
                            sql_text,
                            exec_count,
                            avg_time,
                            max_time,
                            total_time,
                            first_seen,
                            last_seen,
                        ) = row
                        f.write(f"-- 슬로우 쿼리 #{i}\n")
                        f.write(
                            f"-- 실행횟수: {exec_count}, 평균시간: {avg_time:.3f}초, 최대시간: {max_time:.3f}초\n"
                        )
                        f.write(
                            f"-- 총 시간: {total_time:.3f}초, 마지막 실행: {last_seen}\n"
                        )
                        f.write(f"{sql_text};\n\n")

                # S3에 업로드 및 Pre-signed URL 생성
                try:
                    import boto3
                    s3_client = boto3.client('s3', region_name=self.default_region)
                    s3_bucket = QUERY_RESULTS_DEV_BUCKET
                    s3_key = f"sql-files/slow-queries/{filename}"

                    s3_client.upload_file(str(file_path), s3_bucket, s3_key)
                    logger.info(f"SQL 파일 S3 업로드 완료: s3://{s3_bucket}/{s3_key}")

                    presigned_url = s3_client.generate_presigned_url(
                        'get_object',
                        Params={'Bucket': s3_bucket, 'Key': s3_key},
                        ExpiresIn=604800  # 7일
                    )

                    return {
                        "success": True,
                        "message": f"✅ Performance Schema에서 슬로우 쿼리 {len(results)}개 수집 완료: {filename}\n검색 기간: {start_dt} ~ {end_dt} (UTC)\n🔗 다운로드 (7일 유효): {presigned_url}",
                    }
                except Exception as s3_error:
                    logger.error(f"S3 업로드 실패: {s3_error}")
                    return {
                        "success": True,
                        "message": f"✅ Performance Schema에서 슬로우 쿼리 {len(results)}개 수집 완료: {filename}\n검색 기간: {start_dt} ~ {end_dt} (UTC)",
                    }
            else:
                return {
                    "success": False,
                    "message": f"Performance Schema에서도 슬로우 쿼리를 찾을 수 없음",
                }

        except Exception as e:
            return {
                "success": False,
                "message": f"Performance Schema 조회 실패: {str(e)}",
            }

    async def _suggest_log_exports_setup(
        self, cluster_identifier: str, cloudwatch_error: str
    ) -> str:
        """Log exports 설정 제안 및 자동 설정"""
        try:
            # RDS 클라이언트로 현재 설정 확인
            rds_client = boto3.client("rds", region_name="ap-northeast-2")

            try:
                response = rds_client.describe_db_clusters(
                    DBClusterIdentifier=cluster_identifier
                )
                cluster = response["DBClusters"][0]
                enabled_logs = cluster.get("EnabledCloudwatchLogsExports", [])

                result_msg = f"🔍 **슬로우 쿼리 수집 결과**\n\n"
                result_msg += f"❌ CloudWatch Logs: {cloudwatch_error}\n"
                result_msg += f"❌ Performance Schema: 슬로우 쿼리 없음\n\n"
                result_msg += f"📊 **현재 Log Exports 설정**\n"
                result_msg += f"클러스터: {cluster_identifier}\n"
                result_msg += (
                    f"활성화된 로그: {enabled_logs if enabled_logs else '없음'}\n\n"
                )

                if "slowquery" not in enabled_logs:
                    result_msg += f"💡 **해결 방안**\n"
                    result_msg += f"Aurora 클러스터에서 SlowQuery 로그를 CloudWatch로 전송하도록 설정이 필요합니다.\n\n"
                    result_msg += f"**자동 설정을 진행하시겠습니까?**\n"
                    result_msg += f"다음 명령이 실행됩니다:\n"
                    result_msg += f"```\n"
                    result_msg += f"aws rds modify-db-cluster \\\n"
                    result_msg += f"  --db-cluster-identifier {cluster_identifier} \\\n"
                    result_msg += f"  --cloudwatch-logs-configuration 'EnableLogTypes=slowquery'\n"
                    result_msg += f"```\n\n"
                    result_msg += f"설정 후 약 5-10분 후부터 CloudWatch Logs에서 슬로우 쿼리를 확인할 수 있습니다.\n\n"
                    result_msg += f"**설정하시겠습니까? (y/n)**"

                    # 사용자 입력 대기는 MCP에서 지원하지 않으므로, 별도 함수로 분리
                    return result_msg
                else:
                    result_msg += (
                        f"✅ SlowQuery 로그 전송이 이미 활성화되어 있습니다.\n"
                    )
                    result_msg += f"로그가 나타나지 않는 이유:\n"
                    result_msg += f"1. 실제로 1초 이상 실행되는 쿼리가 없음\n"
                    result_msg += f"2. 로그 전송에 지연이 있음 (최대 10분)\n"
                    result_msg += f"3. 로그 보존 정책으로 인한 삭제\n"
                    return result_msg

            except Exception as e:
                return f"❌ 클러스터 정보 조회 실패: {str(e)}"

        except Exception as e:
            return f"❌ Log exports 설정 확인 실패: {str(e)}"

    async def enable_slow_query_log_exports(self, cluster_identifier: str) -> str:
        """Aurora 클러스터의 SlowQuery 로그 CloudWatch 전송 활성화"""
        try:
            rds_client = boto3.client("rds", region_name="ap-northeast-2")

            response = rds_client.modify_db_cluster(
                DBClusterIdentifier=cluster_identifier,
                CloudwatchLogsConfiguration={"EnableLogTypes": ["slowquery"]},
                ApplyImmediately=True,
            )

            return (
                f"✅ SlowQuery 로그 CloudWatch 전송이 활성화되었습니다.\n"
                f"클러스터: {cluster_identifier}\n"
                f"상태: {response['DBCluster']['Status']}\n"
                f"약 5-10분 후부터 CloudWatch Logs에서 슬로우 쿼리를 확인할 수 있습니다."
            )

        except Exception as e:
            return f"❌ SlowQuery 로그 활성화 실패: {str(e)}"

    async def collect_cpu_intensive_queries(
        self,
        database_secret: str,
        db_instance_identifier: str = None,
        start_time: str = None,
        end_time: str = None,
    ) -> str:
        """CPU 집약적 쿼리 수집 및 SQL 파일 생성 (Lambda 사용)"""
        try:
            # 리팩토링: Week 1 - LambdaClient 모듈로 위임
            lambda_result = await self.lambda_client.collect_cpu_intensive_queries(
                database_secret, db_instance_identifier, start_time, end_time
            )

            if not lambda_result.get('success'):
                error_msg = lambda_result.get('error', 'Lambda 호출 실패')
                return f"❌ CPU 집약적 쿼리 수집 실패: {error_msg}"

            # Lambda에서 받은 쿼리 데이터로 파일 생성 (로컬 처리)
            queries = lambda_result.get('queries', [])

            if queries:
                # 현재 날짜와 인스턴스 ID로 파일명 생성
                current_date = datetime.now().strftime("%Y%m%d")
                instance_suffix = (
                    f"_{db_instance_identifier}" if db_instance_identifier else ""
                )
                filename = f"cpu_intensive_queries{instance_suffix}_{current_date}.sql"
                file_path = SQL_DIR / filename

                with open(file_path, "w", encoding="utf-8") as f:
                    f.write(f"-- CPU 집약적 쿼리 모음 (수집일시: {datetime.now()})\n")
                    f.write(f"-- 총 {len(queries)}개의 쿼리\n\n")

                    for i, query_info in enumerate(queries, 1):
                        sql = query_info.get('sql', '')
                        source = query_info.get('source', 'unknown')
                        exec_count = query_info.get('exec_count', 0)
                        avg_time = query_info.get('avg_time', 0.0)

                        f.write(f"-- CPU 집약적 쿼리 #{i} (출처: {source})\n")
                        if exec_count:
                            f.write(f"-- 실행 횟수: {exec_count}, 평균 시간: {avg_time:.3f}초\n")
                        f.write(f"{sql};\n\n")

                # S3에 업로드 및 Pre-signed URL 생성
                try:
                    import boto3
                    s3_client = boto3.client('s3', region_name=self.default_region)
                    s3_bucket = QUERY_RESULTS_DEV_BUCKET
                    s3_key = f"sql-files/cpu-intensive/{filename}"

                    s3_client.upload_file(str(file_path), s3_bucket, s3_key)
                    logger.info(f"SQL 파일 S3 업로드 완료: s3://{s3_bucket}/{s3_key}")

                    presigned_url = s3_client.generate_presigned_url(
                        'get_object',
                        Params={'Bucket': s3_bucket, 'Key': s3_key},
                        ExpiresIn=604800
                    )

                    return f"✅ CPU 집약적 쿼리 {len(queries)}개 수집 완료: {filename}\n🔗 다운로드 (7일 유효): {presigned_url}"
                except Exception as s3_error:
                    logger.error(f"S3 업로드 실패: {s3_error}")
                    return f"✅ CPU 집약적 쿼리 {len(queries)}개 수집 완료: {self.format_file_link(str(file_path), filename)}"
            else:
                return f"✅ CPU 집약적 쿼리가 발견되지 않았습니다"

        except Exception as e:
            logger.error(f"CPU 집약 쿼리 수집 실패: {str(e)}")
            return f"❌ CPU 집약적 쿼리 수집 실패: {str(e)}"

    async def collect_temp_space_intensive_queries(
        self,
        database_secret: str,
        db_instance_identifier: str = None,
        start_time: str = None,
        end_time: str = None,
    ) -> str:
        """임시 공간 집약적 쿼리 수집 및 SQL 파일 생성 (Lambda 사용)"""
        try:
            # 리팩토링: Week 1 - LambdaClient 모듈로 위임
            lambda_result = await self.lambda_client.collect_temp_space_intensive_queries(
                database_secret, db_instance_identifier, start_time, end_time
            )

            if not lambda_result.get('success'):
                error_msg = lambda_result.get('error', 'Lambda 호출 실패')
                return f"❌ 임시 공간 집약적 쿼리 수집 실패: {error_msg}"

            # Lambda에서 받은 쿼리 데이터로 파일 생성 (로컬 처리)
            queries = lambda_result.get('queries', [])

            if queries:
                # 현재 날짜와 인스턴스 ID로 파일명 생성
                current_date = datetime.now().strftime("%Y%m%d")
                instance_suffix = (
                    f"_{db_instance_identifier}" if db_instance_identifier else ""
                )
                filename = (
                    f"temp_space_intensive_queries{instance_suffix}_{current_date}.sql"
                )
                file_path = SQL_DIR / filename

                with open(file_path, "w", encoding="utf-8") as f:
                    f.write(
                        f"-- 임시 공간 집약적 쿼리 모음 (수집일시: {datetime.now()})\n"
                    )
                    f.write(f"-- 총 {len(queries)}개의 쿼리\n\n")

                    for i, query_info in enumerate(queries, 1):
                        sql = query_info.get('sql', '')
                        temp_tables = query_info.get('temp_tables', 0)
                        temp_disk_tables = query_info.get('temp_disk_tables', 0)
                        sort_rows = query_info.get('sort_rows', 0)

                        f.write(f"-- 임시 공간 집약적 쿼리 #{i}\n")
                        if temp_tables or temp_disk_tables:
                            f.write(f"-- 임시 테이블: {temp_tables}개, 디스크 임시 테이블: {temp_disk_tables}개, 정렬 행: {sort_rows}개\n")
                        f.write(f"{sql};\n\n")

                # S3에 업로드 및 Pre-signed URL 생성
                try:
                    import boto3
                    s3_client = boto3.client('s3', region_name=self.default_region)
                    s3_bucket = QUERY_RESULTS_DEV_BUCKET
                    s3_key = f"sql-files/temp-intensive/{filename}"

                    s3_client.upload_file(str(file_path), s3_bucket, s3_key)
                    logger.info(f"SQL 파일 S3 업로드 완료: s3://{s3_bucket}/{s3_key}")

                    presigned_url = s3_client.generate_presigned_url(
                        'get_object',
                        Params={'Bucket': s3_bucket, 'Key': s3_key},
                        ExpiresIn=604800  # 7일
                    )

                    return f"✅ 임시 공간 집약적 쿼리 {len(queries)}개 수집 완료: {filename}\n🔗 다운로드 (7일 유효): {presigned_url}"
                except Exception as s3_error:
                    logger.error(f"S3 업로드 실패: {s3_error}")
                    return f"✅ 임시 공간 집약적 쿼리 {len(queries)}개 수집 완료: {self.format_file_link(str(file_path), filename)}"
            else:
                return f"✅ 임시 공간 집약적 쿼리가 발견되지 않았습니다"

        except Exception as e:
            logger.error(f"Temp 공간 집약 쿼리 수집 실패: {str(e)}")
            return f"❌ 임시 공간 집약적 쿼리 수집 실패: {str(e)}"

    async def validate_schema_lambda(
        self,
        database_secret: str,
        database: str,
        ddl_content: str,
        region: str = "ap-northeast-2"
    ) -> dict:
        """DDL 스키마 검증 (Lambda 사용)

        리팩토링: Week 1 - LambdaClient 모듈로 위임
        하위 호환성을 위해 메서드 유지
        """
        return await self.lambda_client.validate_schema(
            database_secret, database, ddl_content, region
        )

    async def explain_query_lambda(
        self,
        database_secret: str,
        database: str,
        query: str,
        region: str = "ap-northeast-2"
    ) -> dict:
        """쿼리 실행 계획 분석 (Lambda 사용)

        리팩토링: Week 1 - LambdaClient 모듈로 위임
        하위 호환성을 위해 메서드 유지
        """
        return await self.lambda_client.explain_query(
            database_secret, database, query, region
        )

    async def analyze_aurora_mysql_error_logs(
        self, keyword: str, start_datetime_str: str, end_datetime_str: str
    ) -> str:
        """Aurora MySQL 에러 로그 분석"""
        try:
            # 시간 변환 (KST -> UTC)
            start_time_utc = self.convert_kst_to_utc(start_datetime_str)
            end_time_utc = self.convert_kst_to_utc(end_datetime_str)

            logger.info(f"에러 로그 분석 시작: {start_time_utc} ~ {end_time_utc} (UTC)")

            # AWS 클라이언트 초기화
            rds_client = boto3.client("rds", region_name=self.default_region)

            # 키워드로 시크릿 리스트 가져오기
            secret_lists = await self.get_secrets_by_keyword(keyword)
            if not secret_lists:
                return f"❌ '{keyword}' 키워드로 찾은 시크릿이 없습니다."

            results = []
            processed_instances = []

            for secret_name in secret_lists:
                try:
                    # 시크릿에서 DB 정보 가져오기
                    secret_data = await self.get_secret(secret_name)
                    db_host = secret_data.get("host", "")
                    
                    # 호스트명에서 인스턴스/클러스터 식별자 추출
                    # 예: mysql-instance.cluster-xxx.region.rds.amazonaws.com
                    if not db_host:
                        logger.warning(f"시크릿 {secret_name}에 host 정보가 없습니다.")
                        continue
                    
                    # 클러스터 엔드포인트인 경우 클러스터의 모든 인스턴스 조회
                    if ".cluster-" in db_host:
                        cluster_id = db_host.split(".")[0]
                        try:
                            response = rds_client.describe_db_clusters(
                                DBClusterIdentifier=cluster_id
                            )
                            instances = [
                                member["DBInstanceIdentifier"]
                                for member in response["DBClusters"][0]["DBClusterMembers"]
                            ]
                        except Exception as e:
                            logger.warning(f"클러스터 {cluster_id} 조회 실패: {e}")
                            continue
                    else:
                        # 단일 인스턴스인 경우
                        instance_id = db_host.split(".")[0]
                        instances = [instance_id]

                    for instance in instances:
                        # 중복 처리 방지
                        if instance in processed_instances:
                            continue
                        processed_instances.append(instance)
                        
                        log_content = []

                        # 에러 로그 파일 목록 가져오기
                        try:
                            log_file_list = rds_client.describe_db_log_files(
                                DBInstanceIdentifier=instance, FilenameContains="error"
                            )
                        except Exception as e:
                            logger.error(f"인스턴스 {instance} 로그 파일 목록 조회 실패: {e}")
                            results.append(
                                f"<{instance}>\n로그 파일 목록 조회 실패: {str(e)}\n</{instance}>"
                            )
                            continue

                        for log_file_info in log_file_list["DescribeDBLogFiles"]:
                            log_filename = log_file_info["LogFileName"]
                            last_written = datetime.fromtimestamp(
                                log_file_info["LastWritten"] / 1000
                            )

                            if start_time_utc <= last_written <= end_time_utc:
                                # 로그 파일 내용 다운로드
                                try:
                                    response = rds_client.download_db_log_file_portion(
                                        DBInstanceIdentifier=instance,
                                        LogFileName=log_filename,
                                        Marker="0",
                                    )

                                    log_data = response.get("LogFileData", "")
                                    lines = log_data.splitlines()

                                    # 중요한 에러 로그 항목 필터링
                                    error_keywords = [
                                        "error",
                                        "warning",
                                        "critical",
                                        "failed",
                                        "crash",
                                        "exception",
                                        "fatal",
                                        "corruption",
                                    ]

                                    for line in lines:
                                        if any(kw in line.lower() for kw in error_keywords):
                                            log_content.append(line)
                                except Exception as e:
                                    logger.error(f"로그 파일 {log_filename} 다운로드 실패: {e}")
                                    continue

                        # 로그 내용이 있으면 결과에 추가
                        if log_content:
                            # 적절한 크기로 분할 (최대 5000자)
                            content_chunks = self._split_log_content(log_content, 5000)
                            for i, chunk in enumerate(content_chunks):
                                chunk_header = (
                                    f"<{instance}_chunk_{i+1}>"
                                    if len(content_chunks) > 1
                                    else f"<{instance}>"
                                )
                                chunk_footer = (
                                    f"</{instance}_chunk_{i+1}>"
                                    if len(content_chunks) > 1
                                    else f"</{instance}>"
                                )
                                results.append(
                                    f"{chunk_header}\n{chunk}\n{chunk_footer}"
                                )
                        else:
                            results.append(
                                f"<{instance}>\n해당 기간에 에러 로그가 없습니다.\n</{instance}>"
                            )

                except Exception as e:
                    logger.error(f"시크릿 {secret_name} 처리 중 오류: {e}")
                    results.append(
                        f"<{secret_name}>\n로그 수집 중 오류 발생: {str(e)}\n</{secret_name}>"
                    )

            if not results:
                return "❌ 분석할 에러 로그를 찾을 수 없습니다."

            # Claude를 통한 에러 로그 분석
            analysis_result = await self._analyze_error_logs_with_claude(results)

            # 결과 저장
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_path = Path("output") / f"error_log_analysis_{timestamp}.html"

            # HTML 보고서 생성
            html_report = await self._generate_error_log_html_report(
                results,
                analysis_result,
                keyword,
                start_datetime_str,
                end_datetime_str,
                output_path,
            )

            return f"""✅ Aurora MySQL 에러 로그 분석 완료

📊 분석 요약:
• 분석 기간: {start_datetime_str} ~ {end_datetime_str}
• 대상 키워드: {keyword}
• 찾은 시크릿: {len(secret_lists)}개
• 분석된 인스턴스: {len(processed_instances)}개
• 수집된 로그 청크: {len(results)}개

🤖 Claude AI 분석 결과:
{analysis_result}

📄 상세 보고서: {html_report}
"""

        except Exception as e:
            logger.error(f"에러 로그 분석 중 오류: {e}")
            return f"❌ 에러 로그 분석 실패: {str(e)}"

    def _split_log_content(self, log_lines: List[str], max_chars: int) -> List[str]:
        """로그 내용을 적절한 크기로 분할"""
        chunks = []
        current_chunk = []
        current_size = 0

        for line in log_lines:
            line_size = len(line) + 1  # +1 for newline

            if current_size + line_size > max_chars and current_chunk:
                chunks.append("\n".join(current_chunk))
                current_chunk = [line]
                current_size = line_size
            else:
                current_chunk.append(line)
                current_size += line_size

        if current_chunk:
            chunks.append("\n".join(current_chunk))

        return chunks

    async def _analyze_error_logs_with_claude(self, log_results: List[str]) -> str:
        """Claude를 통한 에러 로그 분석"""
        try:
            # 로그 내용 결합
            combined_logs = "\n".join(log_results)

            prompt = f"""아래는 Aurora MySQL 3.5 인스턴스의 에러 로그입니다. 각 인스턴스에 대한 에러로그를 분석하고 다음 사항에 대한 요약을 제공해주세요:

<instance명>과 </instance명> 사이에 있는 로그는 해당 인스턴스의 error log입니다.

어떤 키워드의 에러가 가장 많이 나타났는지, 에러카테고리별로 집계도 부탁합니다.
아래와 같은 포맷으로 각 인스턴스별로 에러 카테고리별로 집계하고, 분석한 내용을 넣어주세요.

예를 들어 aborted connection이 몇건있었고, 그것은 어떤 영향을 가지는지 설명해주세요.
분석할때 어느 인스턴스에 있는 어떤 내용을 근거로 했는지 명확하게 하고, 그렇지 않으면 모르겠다고 합니다.

**분석 결과:**

1. **전체 요약**
   - 총 에러 건수: X건
   - 심각도별 분류: 높음/중간/낮음
   - 주요 에러 패턴: 

2. **인스턴스별 상세 분석**

3. **권장 조치사항**

<context>
심각한 에러 키워드:
1. "Fatal error" - 영향도: 매우 높음 (데이터베이스 서버 중지/재시작 가능)
2. "Out of memory" - 영향도: 높음 (메모리 부족으로 성능 저하/쿼리 실패)
3. "Disk full" - 영향도: 높음 (디스크 공간 부족으로 쓰기 작업 실패)
4. "Connection refused" - 영향도: 중간~높음 (클라이언트 연결 문제)
5. "InnoDB: Corruption" - 영향도: 높음 (데이터 무결성 문제, 데이터 손실 가능)

주의가 필요한 에러 키워드:
6. "Slow query" - 영향도: 중간 (성능 저하)
7. "Lock wait timeout exceeded" - 영향도: 중간 (동시성 문제)
8. "Warning" - 영향도: 낮음~중간 (잠재적 문제)
9. "Table is full" - 영향도: 중간 (테이블 용량 초과)
10. "Deadlock found" - 영향도: 중간 (트랜잭션 충돌)
</context>

{combined_logs}
"""

            claude_input = json.dumps(
                {
                    "anthropic_version": "bedrock-2023-05-31",
                    "max_tokens": 4096,
                    "messages": [
                        {"role": "user", "content": [{"type": "text", "text": prompt}]}
                    ],
                    "temperature": 0.3,
                }
            )

            sonnet_4_model_id = "us.anthropic.claude-sonnet-4-20250514-v1:0"
            sonnet_3_7_model_id = "us.anthropic.claude-3-7-sonnet-20250219-v1:0"

            # Claude Sonnet 4 호출 시도
            try:
                response = self.bedrock_client.invoke_model(
                    modelId=sonnet_4_model_id, body=claude_input
                )
                response_body = json.loads(response.get("body").read())
                claude_response = response_body.get("content", [{}])[0].get("text", "")
                logger.info("Claude Sonnet 4로 에러 로그 분석 완료")
                return claude_response

            except Exception as e:
                logger.warning(f"Claude Sonnet 4 호출 실패, fallback 시도: {e}")

                # Claude 3.7 Sonnet 호출 (fallback)
                try:
                    response = self.bedrock_client.invoke_model(
                        modelId=sonnet_3_7_model_id, body=claude_input
                    )
                    response_body = json.loads(response.get("body").read())
                    claude_response = response_body.get("content", [{}])[0].get(
                        "text", ""
                    )
                    logger.info("Claude 3.7 Sonnet으로 에러 로그 분석 완료")
                    return claude_response

                except Exception as e2:
                    logger.error(f"Claude 호출 완전 실패: {e2}")
                    return f"Claude 분석 실패: {str(e2)}"

        except Exception as e:
            logger.error(f"에러 로그 Claude 분석 중 오류: {e}")
            return f"분석 중 오류 발생: {str(e)}"

    async def _generate_error_log_html_report(
        self,
        log_results: List[str],
        analysis_result: str,
        keyword: str,
        start_time: str,
        end_time: str,
        output_path: Path,
    ) -> str:
        """에러 로그 분석 HTML 보고서 생성"""
        try:
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            html_content = f"""<!DOCTYPE html>
<html lang="ko">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Aurora MySQL 에러 로그 분석 보고서</title>
    <style>
        body {{ font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; margin: 0; padding: 20px; background-color: #f5f5f5; }}
        .container {{ max-width: 1200px; margin: 0 auto; background: white; border-radius: 10px; box-shadow: 0 0 20px rgba(0,0,0,0.1); }}
        .header {{ background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 30px; border-radius: 10px 10px 0 0; }}
        .header h1 {{ margin: 0; font-size: 2.5em; }}
        .header .subtitle {{ margin-top: 10px; opacity: 0.9; }}
        .content {{ padding: 30px; }}
        .summary-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(250px, 1fr)); gap: 20px; margin-bottom: 30px; }}
        .summary-card {{ background: #f8f9fa; border-left: 4px solid #007bff; padding: 20px; border-radius: 5px; }}
        .summary-card h3 {{ margin: 0 0 10px 0; color: #333; }}
        .summary-card .value {{ font-size: 1.5em; font-weight: bold; color: #007bff; }}
        .analysis-section {{ margin-bottom: 30px; }}
        .analysis-section h2 {{ color: #333; border-bottom: 2px solid #007bff; padding-bottom: 10px; }}
        .log-content {{ background: #f8f9fa; border: 1px solid #dee2e6; border-radius: 5px; padding: 15px; margin: 10px 0; font-family: monospace; font-size: 0.9em; max-height: 400px; overflow-y: auto; }}
        .error-high {{ color: #dc3545; font-weight: bold; }}
        .error-medium {{ color: #fd7e14; }}
        .error-low {{ color: #6c757d; }}
        .footer {{ text-align: center; padding: 20px; color: #6c757d; border-top: 1px solid #dee2e6; }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🔍 Aurora MySQL 에러 로그 분석</h1>
            <div class="subtitle">생성일시: {timestamp}</div>
        </div>
        
        <div class="content">
            <div class="summary-grid">
                <div class="summary-card">
                    <h3>분석 기간</h3>
                    <div class="value">{start_time}<br>~<br>{end_time}</div>
                </div>
                <div class="summary-card">
                    <h3>대상 키워드</h3>
                    <div class="value">{keyword}</div>
                </div>
                <div class="summary-card">
                    <h3>로그 청크 수</h3>
                    <div class="value">{len(log_results)}개</div>
                </div>
            </div>
            
            <div class="analysis-section">
                <h2>🤖 Claude AI 분석 결과</h2>
                <div class="log-content">
                    {analysis_result.replace(chr(10), '<br>')}
                </div>
            </div>
            
            <div class="analysis-section">
                <h2>📋 원본 로그 데이터</h2>
                {''.join([f'<div class="log-content">{log.replace(chr(10), "<br>")}</div>' for log in log_results])}
            </div>
        </div>
        
        <div class="footer">
            <p>DB Assistant MCP Server - Aurora MySQL 에러 로그 분석 보고서</p>
        </div>
    </div>
</body>
</html>"""

            # 출력 디렉토리 생성
            output_path.parent.mkdir(parents=True, exist_ok=True)

            # HTML 파일 저장
            with open(output_path, "w", encoding="utf-8") as f:
                f.write(html_content)

            return str(output_path)

        except Exception as e:
            logger.error(f"HTML 보고서 생성 중 오류: {e}")
            return f"보고서 생성 실패: {str(e)}"

    async def save_to_vector_store(
        self,
        content: str,
        topic: str,
        category: str = "examples",
        tags: list = None,
        force_save: bool = False,
        auto_summarize: bool = True,
    ) -> str:
        """대화 내용을 벡터 저장소에 저장 (자동 요약, 중복 및 상충 검사 포함)"""
        try:
            import os
            from datetime import datetime
            import re

            # 0. 중요도 및 길이 기반 저장 방식 결정
            original_content = content
            s3_full_content_path = None
            importance_score = self._calculate_importance_score(content)

            # 중요도가 높으면 길이와 상관없이 전체 저장, 낮으면 길이에 따라 처리
            if importance_score >= 0.7:  # 높은 중요도
                # 전체 내용을 벡터 저장소에 저장
                pass
            elif len(content) > 1000:
                if importance_score >= 0.4:  # 중간 중요도
                    # 요약해서 벡터 저장소에 저장
                    if auto_summarize:
                        content = await self._summarize_content(
                            content, topic, category
                        )
                else:  # 낮은 중요도
                    # 원본을 S3에 저장하고 메타정보만 벡터 저장소에
                    s3_full_content_path = await self._save_full_content_to_s3(
                        content, topic, category
                    )
                    content = await self._create_metadata_summary(
                        content, topic, s3_full_content_path
                    )

            # 1. 강제 저장이 아닌 경우에만 중복/상충 검사
            if not force_save:
                duplicate_check = await self._check_content_similarity(
                    content, category
                )

                if duplicate_check["is_duplicate"]:
                    return f"""⚠️ 중복된 내용이 발견되었습니다!

🔍 유사한 기존 문서:
📄 파일: {duplicate_check["similar_file"]}
📊 유사도: {duplicate_check["similarity_score"]:.1%}

💡 다음 중 선택하세요:
1. 'update_vector_content' 도구로 기존 문서 업데이트
2. 'save_to_vector_store'에 force_save=true로 강제 저장
3. 저장 취소"""

                if duplicate_check["has_conflict"]:
                    return f"""🚨 기존 내용과 상충되는 정보가 발견되었습니다!

⚠️ 상충 내용:
{duplicate_check["conflict_details"]}

🤔 다음 중 선택하세요:
1. 새로운 정보가 맞다면 'update_vector_content'로 기존 문서 교체
2. 기존 정보가 맞다면 저장 취소
3. 둘 다 맞다면 'save_to_vector_store'에 force_save=true로 별도 저장"""

            # 2. 중복/상충이 없으면 정상 저장 진행
            date_str = datetime.now().strftime("%Y%m%d")
            clean_topic = re.sub(r"[^a-zA-Z0-9]", "", topic.lower())[:10]
            if not clean_topic:
                clean_topic = "content"

            filename = f"{date_str}_{clean_topic}.md"

            # vector 폴더 생성
            vector_dir = "vector"
            os.makedirs(vector_dir, exist_ok=True)

            # 메타데이터 생성
            if tags is None:
                tags = ["conversation", "analysis"]

            metadata_tags = tags + ["database", "optimization", "best-practices"]

            # YAML 헤더 생성
            yaml_header = f"""---
title: "{topic}"
category: "{category}"
tags: {metadata_tags}
version: "1.0"
last_updated: "{datetime.now().strftime('%Y-%m-%d')}"
author: "DB Assistant"
source: "conversation"
similarity_checked: true
---

"""

            # 파일 내용 생성
            file_content = yaml_header + content

            # 로컬 파일 저장
            local_path = os.path.join(vector_dir, filename)
            with open(local_path, "w", encoding="utf-8") as f:
                f.write(file_content)

            # S3 키 생성
            s3_key = f"{category}/{filename}"

            # 기존 파일 버전 확인
            version_info = await self._check_file_version_in_s3(s3_key)

            # 버전 업데이트
            if version_info["exists"]:
                current_version = version_info["version"]
                try:
                    major, minor = map(int, current_version.split("."))
                    new_version = f"{major}.{minor + 1}"
                except:
                    new_version = "1.1"
            else:
                new_version = "1.0"

            # 메타데이터 생성 (S3 경로 정보 포함)
            if tags is None:
                tags = ["conversation", "analysis"]

            metadata_tags = tags + ["database", "optimization", "best-practices"]

            # YAML 헤더 생성 (버전 정보 포함)
            yaml_header = f"""---
title: "{topic}"
category: "{category}"
tags: {metadata_tags}
version: "{new_version}"
last_updated: "{datetime.now().strftime('%Y-%m-%d')}"
author: "DB Assistant"
source: "conversation"
s3_path: "s3://{BEDROCK_AGENT_BUCKET}/{s3_key}"
similarity_checked: true
---

"""

            # 파일 내용 생성
            file_content = yaml_header + content

            # 로컬 파일 저장
            local_path = os.path.join(vector_dir, filename)
            with open(local_path, "w", encoding="utf-8") as f:
                f.write(file_content)

            # S3에 메타데이터와 함께 업로드
            s3_client = boto3.client("s3", region_name="us-east-1")

            s3_client.upload_file(
                local_path,
                BEDROCK_AGENT_BUCKET,
                s3_key,
                ExtraArgs={
                    "ContentType": "text/markdown",
                    "Metadata": {
                        "version": new_version,
                        "category": category,
                        "title": topic,
                        "author": "DB Assistant",
                        "tags": ",".join(metadata_tags),
                    },
                },
            )

            logger.info(f"벡터 저장소에 파일 저장 완료: {s3_key}")

            # 자동으로 Knowledge Base 동기화 실행
            sync_result = await self.sync_knowledge_base()

            # 중요도 및 저장 방식 정보 추가
            storage_info = f"\n🎯 중요도: {importance_score:.2f}"

            if importance_score >= 0.7:
                storage_info += " (높음 - 전체 저장)"
            elif len(original_content) > 1000:
                if importance_score >= 0.4:
                    storage_info += " (중간 - 요약 저장)"
                    if s3_full_content_path:
                        storage_info += f"\n📄 전체 내용: {s3_full_content_path}"
                    storage_info += (
                        f"\n📝 요약: {len(original_content)} → {len(content)} 문자"
                    )
                else:
                    storage_info += " (낮음 - 메타정보만)"
                    if s3_full_content_path:
                        storage_info += f"\n📄 전체 내용: {s3_full_content_path} ({len(original_content)} 문자)"
                        storage_info += (
                            f"\n📝 벡터 저장: 메타정보만 ({len(content)} 문자)"
                        )
            else:
                storage_info += " (전체 저장)"

            return f"""✅ 벡터 저장소에 저장 완료!

📁 로컬 저장: {local_path}
☁️ S3 저장: s3://{BEDROCK_AGENT_BUCKET}/{s3_key}
🏷️ 카테고리: {category}
📝 버전: {new_version} {"(업데이트)" if version_info["exists"] else "(신규)"}
🔖 태그: {', '.join(metadata_tags)}{storage_info}
✅ 중복/상충 검사: 통과

🔄 Knowledge Base 동기화 자동 실행:
{sync_result}"""

        except Exception as e:
            logger.error(f"벡터 저장소 저장 오류: {e}")
            return f"❌ 벡터 저장소 저장 중 오류가 발생했습니다: {str(e)}"

    async def _check_content_similarity(self, new_content: str, category: str) -> dict:
        """기존 내용과 중복/상충 검사"""
        try:
            # 1. Knowledge Base에서 유사한 내용 검색
            similar_docs = await self._search_similar_content(new_content, category)

            # 2. Claude AI로 중복/상충 분석
            if similar_docs:
                analysis = await self._analyze_content_conflicts(
                    new_content, similar_docs
                )
                return analysis

            return {
                "is_duplicate": False,
                "has_conflict": False,
                "similarity_score": 0.0,
                "similar_file": None,
                "conflict_details": None,
            }

        except Exception as e:
            logger.error(f"내용 유사성 검사 오류: {e}")
            return {
                "is_duplicate": False,
                "has_conflict": False,
                "similarity_score": 0.0,
                "similar_file": None,
                "conflict_details": None,
            }

    async def _search_similar_content(self, content: str, category: str) -> list:
        """Knowledge Base에서 유사한 내용 검색"""
        try:
            # 내용의 핵심 키워드 추출
            keywords = self._extract_keywords(content)
            search_query = " ".join(keywords[:5])  # 상위 5개 키워드 사용

            response = self.bedrock_agent_client.retrieve(
                knowledgeBaseId=self.knowledge_base_id,
                retrievalQuery={"text": search_query},
                retrievalConfiguration={
                    "vectorSearchConfiguration": {
                        "numberOfResults": 3,
                        "overrideSearchType": "SEMANTIC",
                    }
                },
            )

            similar_docs = []
            for result in response.get("retrievalResults", []):
                if result["score"] > 0.7:  # 70% 이상 유사도
                    similar_docs.append(
                        {
                            "content": result["content"]["text"],
                            "score": result["score"],
                            "source": result["location"]["s3Location"]["uri"],
                        }
                    )

            return similar_docs

        except Exception as e:
            logger.error(f"유사 내용 검색 오류: {e}")
            return []

    def _extract_keywords(self, content: str) -> list:
        """내용에서 핵심 키워드 추출"""
        import re

        # 기본적인 키워드 추출 (실제로는 더 정교한 NLP 기법 사용 가능)
        words = re.findall(r"\b[a-zA-Z가-힣]{3,}\b", content.lower())

        # 데이터베이스 관련 중요 키워드 우선순위
        db_keywords = [
            "mysql",
            "aurora",
            "index",
            "query",
            "performance",
            "optimization",
            "table",
            "database",
            "sql",
            "schema",
            "connection",
            "error",
            "log",
        ]

        # 중요 키워드 우선 정렬
        keywords = []
        for keyword in db_keywords:
            if keyword in words:
                keywords.append(keyword)

        # 나머지 키워드 추가 (빈도순)
        word_freq = {}
        for word in words:
            if word not in keywords and len(word) > 3:
                word_freq[word] = word_freq.get(word, 0) + 1

        sorted_words = sorted(word_freq.items(), key=lambda x: x[1], reverse=True)
        keywords.extend([word for word, freq in sorted_words[:10]])

        return keywords

    async def _analyze_content_conflicts(
        self, new_content: str, similar_docs: list
    ) -> dict:
        """Claude AI로 내용 중복/상충 분석"""
        try:
            similar_content = "\n\n".join(
                [
                    f"문서 {i+1}: {doc['content'][:500]}..."
                    for i, doc in enumerate(similar_docs)
                ]
            )

            prompt = f"""다음 새로운 내용과 기존 문서들을 비교하여 중복성과 상충성을 분석해주세요.

새로운 내용:
{new_content}

기존 문서들:
{similar_content}

다음 기준으로 분석해주세요:
1. 중복성: 새로운 내용이 기존 문서와 80% 이상 유사한가?
2. 상충성: 새로운 내용이 기존 문서와 모순되는 정보를 포함하는가?

응답 형식:
DUPLICATE: true/false
CONFLICT: true/false
SIMILARITY_SCORE: 0.0-1.0
SIMILAR_FILE: 가장 유사한 문서 번호
CONFLICT_DETAILS: 상충되는 내용 설명 (상충이 있을 경우만)"""

            response = self.bedrock_runtime.invoke_model(
                modelId="us.anthropic.claude-sonnet-4-20250514-v1:0",
                body=json.dumps(
                    {
                        "anthropic_version": "bedrock-2023-05-31",
                        "max_tokens": 1000,
                        "messages": [{"role": "user", "content": prompt}],
                    }
                ),
            )

            response_body = json.loads(response.get("body").read())
            analysis_text = response_body.get("content", [{}])[0].get("text", "")

            # 응답 파싱
            is_duplicate = "DUPLICATE: true" in analysis_text.lower()
            has_conflict = "CONFLICT: true" in analysis_text.lower()

            # 유사도 점수 추출
            similarity_score = 0.0
            if similar_docs:
                similarity_score = similar_docs[0]["score"]

            # 가장 유사한 파일 추출
            similar_file = None
            if similar_docs:
                similar_file = similar_docs[0]["source"].split("/")[-1]

            # 상충 내용 추출
            conflict_details = None
            if has_conflict:
                lines = analysis_text.split("\n")
                for line in lines:
                    if "CONFLICT_DETAILS:" in line:
                        conflict_details = line.split("CONFLICT_DETAILS:")[1].strip()
                        break

            return {
                "is_duplicate": is_duplicate,
                "has_conflict": has_conflict,
                "similarity_score": similarity_score,
                "similar_file": similar_file,
                "conflict_details": conflict_details,
            }

        except Exception as e:
            logger.error(f"내용 상충 분석 오류: {e}")
            return {
                "is_duplicate": False,
                "has_conflict": False,
                "similarity_score": 0.0,
                "similar_file": None,
                "conflict_details": None,
            }

    def _calculate_importance_score(self, content: str) -> float:
        """내용의 중요도 점수 계산 (0.0-1.0)"""
        import re

        content_lower = content.lower()
        score = 0.0

        # 1. 고중요도 키워드 (0.3점)
        high_importance_keywords = [
            "error",
            "critical",
            "urgent",
            "performance",
            "optimization",
            "security",
            "vulnerability",
            "bug",
            "issue",
            "problem",
            "solution",
            "fix",
            "troubleshooting",
            "best practice",
        ]
        for keyword in high_importance_keywords:
            if keyword in content_lower:
                score += 0.03

        # 2. 기술적 내용 (0.2점)
        technical_keywords = [
            "sql",
            "query",
            "index",
            "database",
            "mysql",
            "aurora",
            "configuration",
            "parameter",
            "schema",
            "table",
            "connection",
        ]
        for keyword in technical_keywords:
            if keyword in content_lower:
                score += 0.02

        # 3. 구체적 수치/명령어 포함 (0.2점)
        if re.search(r"\d+\.\d+|\d+%|[0-9]+\s*(mb|gb|ms|sec)", content_lower):
            score += 0.1
        if re.search(r"(select|insert|update|delete|create|alter|drop)", content_lower):
            score += 0.1

        # 4. 구조화된 내용 (0.2점)
        if content.count("\n") > 5:  # 여러 줄
            score += 0.05
        if content.count("```") >= 2:  # 코드 블록
            score += 0.1
        if re.search(r"^\s*[-*+]\s", content, re.MULTILINE):  # 리스트
            score += 0.05

        # 5. 길이 보정 (0.1점)
        if len(content) > 500:
            score += 0.05
        if len(content) > 2000:
            score += 0.05

        return min(score, 1.0)  # 최대 1.0으로 제한

    async def _save_full_content_to_s3(
        self, content: str, topic: str, category: str
    ) -> str:
        """긴 내용을 S3에 저장하고 경로 반환"""
        try:
            from datetime import datetime
            import re

            date_str = datetime.now().strftime("%Y%m%d_%H%M%S")
            clean_topic = re.sub(r"[^a-zA-Z0-9]", "", topic.lower())[:10]
            filename = f"full_content_{date_str}_{clean_topic}.md"
            s3_key = f"{category}/full_content/{filename}"

            s3_client = boto3.client("s3", region_name="us-east-1")
            s3_client.put_object(
                Bucket=BEDROCK_AGENT_BUCKET,
                Key=s3_key,
                Body=content.encode("utf-8"),
                ContentType="text/markdown",
            )

            s3_path = f"s3://{BEDROCK_AGENT_BUCKET}/{s3_key}"
            logger.info(f"전체 내용 S3 저장: {s3_path}")
            return s3_path

        except Exception as e:
            logger.error(f"S3 전체 내용 저장 오류: {e}")
            return None

    async def _create_metadata_summary(
        self, content: str, topic: str, s3_path: str
    ) -> str:
        """긴 내용의 메타정보 요약 생성"""
        try:
            prompt = f"""다음 긴 내용을 메타정보 형태로 요약해주세요. 전체 내용은 S3에 저장되어 있습니다.

주제: {topic}
전체 내용 위치: {s3_path}

원본 내용:
{content[:2000]}...

다음 형식으로 메타정보를 생성해주세요:
## {topic} - 메타정보

**📄 전체 내용**: {s3_path}
**📊 내용 크기**: {len(content)} 문자
**🔍 주요 키워드**: [핵심 키워드 5개]
**📝 요약**: [3-4줄 핵심 요약]
**🎯 주요 내용**:
- [핵심 포인트 1]
- [핵심 포인트 2]
- [핵심 포인트 3]

**💡 활용 방법**: 전체 내용이 필요한 경우 위 S3 경로에서 확인 가능"""

            response = self.bedrock_runtime.invoke_model(
                modelId="us.anthropic.claude-sonnet-4-20250514-v1:0",
                body=json.dumps(
                    {
                        "anthropic_version": "bedrock-2023-05-31",
                        "max_tokens": 1000,
                        "messages": [{"role": "user", "content": prompt}],
                    }
                ),
            )

            response_body = json.loads(response.get("body").read())
            metadata_summary = response_body.get("content", [{}])[0].get("text", "")

            logger.info(
                f"메타정보 요약 생성 완료: {len(content)} -> {len(metadata_summary)} 문자"
            )
            return metadata_summary

        except Exception as e:
            logger.error(f"메타정보 요약 생성 오류: {e}")
            return f"## {topic} - 메타정보\n\n**📄 전체 내용**: {s3_path}\n**📊 내용 크기**: {len(content)} 문자\n**💡 활용 방법**: 전체 내용이 필요한 경우 위 S3 경로에서 확인 가능"

    async def _summarize_content(self, content: str, topic: str, category: str) -> str:
        """내용을 자동으로 요약"""
        try:
            prompt = f"""다음 내용을 {category} 카테고리에 맞게 핵심 정보만 간결하게 요약해주세요.

주제: {topic}
카테고리: {category}

원본 내용:
{content}

요약 기준:
1. 핵심 기술 정보와 해결책 중심으로 요약
2. 구체적인 수치, 명령어, 설정값은 유지
3. 불필요한 설명이나 반복 내용 제거
4. 원본 길이의 30-50% 수준으로 압축
5. 마크다운 형식 유지

요약된 내용:"""

            response = self.bedrock_runtime.invoke_model(
                modelId="us.anthropic.claude-sonnet-4-20250514-v1:0",
                body=json.dumps(
                    {
                        "anthropic_version": "bedrock-2023-05-31",
                        "max_tokens": 2000,
                        "messages": [{"role": "user", "content": prompt}],
                    }
                ),
            )

            response_body = json.loads(response.get("body").read())
            summarized = response_body.get("content", [{}])[0].get("text", content)

            logger.info(f"내용 요약 완료: {len(content)} -> {len(summarized)} 문자")
            return summarized

        except Exception as e:
            logger.error(f"내용 요약 오류: {e}")
            return content  # 요약 실패시 원본 반환

    async def sync_knowledge_base(self) -> str:
        """Knowledge Base 데이터 소스 동기화"""
        try:
            bedrock_agent_client = boto3.client(
                "bedrock-agent", region_name="us-east-1"
            )

            response = bedrock_agent_client.start_ingestion_job(
                knowledgeBaseId=KNOWLEDGE_BASE_ID, dataSourceId=DATA_SOURCE_ID
            )

            job_id = response["ingestionJob"]["ingestionJobId"]
            status = response["ingestionJob"]["status"]

            logger.info(f"Knowledge Base 동기화 시작: {job_id}")

            return f"""✅ Knowledge Base 동기화 시작!

🔄 작업 ID: {job_id}
📊 상태: {status}
⏰ 시작 시간: {response['ingestionJob']['startedAt']}

💡 동기화가 완료되면 새로운 내용을 Knowledge Base에서 검색할 수 있습니다.
상태 확인: AWS 콘솔 > Bedrock > Knowledge Base > 데이터 소스"""

        except Exception as e:
            logger.error(f"Knowledge Base 동기화 오류: {e}")
            return f"❌ Knowledge Base 동기화 중 오류가 발생했습니다: {str(e)}"

    async def query_vector_store(self, query: str, max_results: int = 5) -> str:
        """벡터 저장소에서 내용을 검색합니다"""
        try:
            bedrock_agent_runtime = boto3.client(
                "bedrock-agent-runtime", region_name="us-east-1"
            )

            # Knowledge Base에서 검색
            response = bedrock_agent_runtime.retrieve(
                knowledgeBaseId=KNOWLEDGE_BASE_ID,
                retrievalQuery={"text": query},
                retrievalConfiguration={
                    "vectorSearchConfiguration": {"numberOfResults": max_results}
                },
            )

            if not response.get("retrievalResults"):
                return f"""🔍 검색 결과가 없습니다.

🔎 검색어: '{query}'
💡 다른 키워드로 시도해보세요.
📝 예시: 'HLL', 'lock', 'performance', 'SQL' 등"""

            results = []
            for i, result in enumerate(response["retrievalResults"], 1):
                content = result["content"]["text"]
                score = result.get("score", 0)

                # 메타데이터 추출 및 S3 경로 정보 포함
                metadata = result.get("metadata", {})
                s3_uri = metadata.get("x-amz-bedrock-kb-source-uri", "")
                source_file = metadata.get("source", "")

                # S3 URI에서 파일 정보 추출
                if s3_uri:
                    source = s3_uri
                    # S3에서 전체 파일 내용 가져오기
                    try:
                        full_content = await self._get_full_content_from_s3(s3_uri)
                        if full_content:
                            content = full_content
                    except Exception as e:
                        logger.warning(f"S3에서 전체 내용 가져오기 실패: {e}")
                else:
                    source = source_file if source_file else "알 수 없음"

                # 내용 길이 제한
                preview = content[:500] + "..." if len(content) > 500 else content

                results.append(
                    f"""📄 **결과 {i}** (관련도: {score:.2f})
📁 출처: {source}
📝 내용:
{preview}
"""
                )

            return f"""🔍 **벡터 저장소 검색 결과**

🔎 검색어: "{query}"
📊 총 {len(results)}개 결과 발견

{chr(10).join(results)}

💡 더 구체적인 검색을 원하시면 키워드를 세분화해보세요."""

        except Exception as e:
            logger.error(f"벡터 검색 실패: {str(e)}")
            return f"벡터 검색 실패: {str(e)}"

    async def _get_full_content_from_s3(self, s3_uri: str) -> str:
        """S3 URI에서 전체 파일 내용을 가져옵니다"""
        try:
            # S3 URI 파싱 (s3://bucket/key 형식)
            if not s3_uri.startswith("s3://"):
                return ""

            uri_parts = s3_uri[5:].split("/", 1)
            if len(uri_parts) != 2:
                return ""

            bucket_name = uri_parts[0]
            object_key = uri_parts[1]

            # S3 클라이언트로 파일 내용 가져오기
            s3_client = boto3.client("s3", region_name="us-east-1")
            response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
            content = response["Body"].read().decode("utf-8")

            logger.info(f"S3에서 전체 내용 가져오기 성공: {s3_uri}")
            return content

        except Exception as e:
            logger.error(f"S3에서 내용 가져오기 실패 {s3_uri}: {e}")
            return ""

    async def _check_file_version_in_s3(self, s3_key: str) -> dict:
        """S3에서 파일 버전 정보를 확인합니다"""
        try:
            s3_client = boto3.client("s3", region_name="us-east-1")

            # 파일 존재 여부 및 메타데이터 확인
            try:
                response = s3_client.head_object(Bucket=BEDROCK_AGENT_BUCKET, Key=s3_key)

                # 메타데이터에서 버전 정보 추출
                metadata = response.get("Metadata", {})
                current_version = metadata.get("version", "1.0")
                last_modified = response.get("LastModified")

                return {
                    "exists": True,
                    "version": current_version,
                    "last_modified": last_modified,
                    "etag": response.get("ETag", "").strip('"'),
                }

            except s3_client.exceptions.NoSuchKey:
                return {
                    "exists": False,
                    "version": "1.0",
                    "last_modified": None,
                    "etag": None,
                }

        except Exception as e:
            logger.error(f"S3 파일 버전 확인 실패 {s3_key}: {e}")
            return {
                "exists": False,
                "version": "1.0",
                "last_modified": None,
                "etag": None,
            }

    async def update_vector_content(
        self, filename: str, new_content: str, update_mode: str = "append"
    ) -> str:
        """기존 벡터 저장소 문서 업데이트"""
        try:
            import os
            from datetime import datetime

            # 로컬 파일 경로
            local_path = os.path.join("vector", filename)

            if not os.path.exists(local_path):
                return f"❌ 파일을 찾을 수 없습니다: {filename}"

            # 기존 파일 읽기
            with open(local_path, "r", encoding="utf-8") as f:
                existing_content = f.read()

            # YAML 헤더와 본문 분리
            if existing_content.startswith("---"):
                parts = existing_content.split("---", 2)
                if len(parts) >= 3:
                    yaml_header = f"---{parts[1]}---"
                    existing_body = parts[2].strip()
                else:
                    yaml_header = ""
                    existing_body = existing_content
            else:
                yaml_header = ""
                existing_body = existing_content

            # 업데이트 모드에 따른 내용 처리
            if update_mode == "replace":
                updated_body = new_content
            else:  # append
                updated_body = f"{existing_body}\n\n## 업데이트 ({datetime.now().strftime('%Y-%m-%d %H:%M')})\n\n{new_content}"

            # YAML 헤더 업데이트
            if yaml_header:
                # last_updated 필드 업데이트
                import re

                yaml_header = re.sub(
                    r'last_updated: "[^"]*"',
                    f'last_updated: "{datetime.now().strftime("%Y-%m-%d")}"',
                    yaml_header,
                )
                # version 업데이트
                version_match = re.search(r'version: "([^"]*)"', yaml_header)
                if version_match:
                    current_version = version_match.group(1)
                    try:
                        version_num = float(current_version) + 0.1
                        yaml_header = re.sub(
                            r'version: "[^"]*"',
                            f'version: "{version_num:.1f}"',
                            yaml_header,
                        )
                    except:
                        pass

            # 새로운 파일 내용 생성
            updated_content = (
                f"{yaml_header}\n\n{updated_body}" if yaml_header else updated_body
            )

            # 로컬 파일 업데이트
            with open(local_path, "w", encoding="utf-8") as f:
                f.write(updated_content)

            # S3 업데이트
            s3_client = boto3.client("s3", region_name="us-east-1")

            # 카테고리 추출 (파일명에서 또는 YAML에서)
            category = "examples"  # 기본값
            if yaml_header:
                category_match = re.search(r'category: "([^"]*)"', yaml_header)
                if category_match:
                    category = category_match.group(1)

            s3_key = f"{category}/{filename}"

            s3_client.upload_file(
                local_path,
                BEDROCK_AGENT_BUCKET,
                s3_key,
                ExtraArgs={"ContentType": "text/markdown"},
            )

            logger.info(f"벡터 저장소 파일 업데이트 완료: {s3_key}")

            # 자동으로 Knowledge Base 동기화 실행
            sync_result = await self.sync_knowledge_base()

            return f"""✅ 벡터 저장소 문서 업데이트 완료!

📁 로컬 파일: {local_path}
☁️ S3 파일: s3://{BEDROCK_AGENT_BUCKET}/{s3_key}
🔄 업데이트 모드: {update_mode}
📝 업데이트 시간: {datetime.now().strftime('%Y-%m-%d %H:%M')}

🔄 Knowledge Base 동기화 자동 실행:
{sync_result}"""

        except Exception as e:
            logger.error(f"벡터 저장소 업데이트 오류: {e}")
            return f"❌ 벡터 저장소 업데이트 중 오류가 발생했습니다: {str(e)}"


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
            name="text_to_sql",
            description="자연어 쿼리를 SQL로 변환하고 실행합니다",
            inputSchema={
                "type": "object",
                "properties": {
                    "database_secret": {
                        "type": "string",
                        "description": "데이터베이스 시크릿 이름",
                    },
                    "natural_language_query": {
                        "type": "string",
                        "description": "자연어로 작성된 쿼리 요청",
                    },
                },
                "required": ["database_secret", "natural_language_query"],
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
            description="개선된 아웃라이어 탐지 - 메트릭별 맞춤 임계값과 물리적 제약 적용",
            inputSchema={
                "type": "object",
                "properties": {
                    "csv_file": {"type": "string", "description": "분석할 CSV 파일명"},
                    "std_threshold": {
                        "type": "number",
                        "description": "IQR 방식용 임계값 (기본값: 3.0, 메트릭별 맞춤 기준 우선 적용)",
                        "default": 3.0,
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
            name="debug_cloudwatch_collection",
            description="CloudWatch 슬로우 쿼리 수집 디버그",
            inputSchema={
                "type": "object",
                "properties": {
                    "database_secret": {
                        "type": "string",
                        "description": "데이터베이스 시크릿 이름",
                    },
                    "start_time": {
                        "type": "string",
                        "description": "시작 시간 (YYYY-MM-DD HH:MM:SS, KST 기준)",
                    },
                    "end_time": {
                        "type": "string",
                        "description": "종료 시간 (YYYY-MM-DD HH:MM:SS, KST 기준)",
                    },
                },
                "required": ["database_secret", "start_time", "end_time"],
            },
        ),
        types.Tool(
            name="collect_slow_queries",
            description="슬로우 쿼리 로그에서 느린 쿼리를 수집합니다",
            inputSchema={
                "type": "object",
                "properties": {
                    "database_secret": {
                        "type": "string",
                        "description": "데이터베이스 시크릿 이름",
                    },
                    "start_time": {
                        "type": "string",
                        "description": "시작 시간 (YYYY-MM-DD HH:MM:SS, KST 기준, 선택사항)",
                    },
                    "end_time": {
                        "type": "string",
                        "description": "종료 시간 (YYYY-MM-DD HH:MM:SS, KST 기준, 선택사항)",
                    },
                },
                "required": ["database_secret"],
            },
        ),
        types.Tool(
            name="enable_slow_query_log_exports",
            description="Aurora 클러스터의 SlowQuery 로그 CloudWatch 전송을 활성화합니다",
            inputSchema={
                "type": "object",
                "properties": {
                    "cluster_identifier": {
                        "type": "string",
                        "description": "Aurora 클러스터 식별자",
                    },
                },
                "required": ["cluster_identifier"],
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
                    },
                    "db_instance_identifier": {
                        "type": "string",
                        "description": "특정 인스턴스 식별자 (선택사항)",
                    },
                    "start_time": {
                        "type": "string",
                        "description": "시작 시간 (YYYY-MM-DD HH:MM:SS 형식, 선택사항)",
                    },
                    "end_time": {
                        "type": "string",
                        "description": "종료 시간 (YYYY-MM-DD HH:MM:SS 형식, 선택사항)",
                    },
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
                    },
                    "db_instance_identifier": {
                        "type": "string",
                        "description": "특정 인스턴스 식별자 (선택사항)",
                    },
                    "start_time": {
                        "type": "string",
                        "description": "시작 시간 (YYYY-MM-DD HH:MM:SS 형식, 선택사항)",
                    },
                    "end_time": {
                        "type": "string",
                        "description": "종료 시간 (YYYY-MM-DD HH:MM:SS 형식, 선택사항)",
                    },
                },
                "required": ["database_secret"],
            },
        ),
        types.Tool(
            name="validate_schema_lambda",
            description="DDL 스키마를 검증합니다 (Lambda 사용) - CREATE TABLE, ALTER TABLE, DROP TABLE, CREATE INDEX 지원",
            inputSchema={
                "type": "object",
                "properties": {
                    "database_secret": {
                        "type": "string",
                        "description": "데이터베이스 시크릿 이름",
                    },
                    "database": {
                        "type": "string",
                        "description": "데이터베이스 이름",
                    },
                    "ddl_content": {
                        "type": "string",
                        "description": "검증할 DDL 구문",
                    },
                    "region": {
                        "type": "string",
                        "description": "AWS 리전 (기본값: ap-northeast-2)",
                    },
                },
                "required": ["database_secret", "database", "ddl_content"],
            },
        ),
        types.Tool(
            name="explain_query_lambda",
            description="SQL 쿼리 실행 계획을 분석합니다 (Lambda 사용) - 성능 이슈 자동 감지 및 개선 권장사항 제공",
            inputSchema={
                "type": "object",
                "properties": {
                    "database_secret": {
                        "type": "string",
                        "description": "데이터베이스 시크릿 이름",
                    },
                    "database": {
                        "type": "string",
                        "description": "데이터베이스 이름",
                    },
                    "query": {
                        "type": "string",
                        "description": "분석할 SQL 쿼리",
                    },
                    "region": {
                        "type": "string",
                        "description": "AWS 리전 (기본값: ap-northeast-2)",
                    },
                },
                "required": ["database_secret", "database", "query"],
            },
        ),
        types.Tool(
            name="analyze_aurora_mysql_error_logs",
            description="Aurora MySQL 에러 로그를 분석합니다",
            inputSchema={
                "type": "object",
                "properties": {
                    "keyword": {
                        "type": "string",
                        "description": "검색할 키워드 (시크릿 이름 필터링용)",
                    },
                    "start_datetime_str": {
                        "type": "string",
                        "description": "시작 시간 (YYYY-MM-DD HH:MM:SS 형식)",
                    },
                    "end_datetime_str": {
                        "type": "string",
                        "description": "종료 시간 (YYYY-MM-DD HH:MM:SS 형식)",
                    },
                },
                "required": ["keyword", "start_datetime_str", "end_datetime_str"],
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
                    },
                },
            },
        ),
        types.Tool(
            name="save_to_vector_store",
            description="대화 내용이나 분석 결과를 벡터 저장소(Knowledge Base)에 저장합니다 (중복/상충 검사 포함)",
            inputSchema={
                "type": "object",
                "properties": {
                    "content": {
                        "type": "string",
                        "description": "저장할 내용",
                    },
                    "topic": {
                        "type": "string",
                        "description": "주제명 (10자 이내 영문)",
                    },
                    "category": {
                        "type": "string",
                        "description": "카테고리 (database-standards, performance-optimization, troubleshooting, examples 중 선택)",
                        "enum": [
                            "database-standards",
                            "performance-optimization",
                            "troubleshooting",
                            "examples",
                        ],
                        "default": "examples",
                    },
                    "tags": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "태그 목록 (선택사항)",
                    },
                    "force_save": {
                        "type": "boolean",
                        "description": "중복/상충 검사 무시하고 강제 저장 (선택사항)",
                        "default": False,
                    },
                    "auto_summarize": {
                        "type": "boolean",
                        "description": "1000자 이상 내용 자동 요약 (선택사항)",
                        "default": True,
                    },
                },
                "required": ["content", "topic"],
            },
        ),
        types.Tool(
            name="update_vector_content",
            description="기존 벡터 저장소 문서를 업데이트합니다",
            inputSchema={
                "type": "object",
                "properties": {
                    "filename": {
                        "type": "string",
                        "description": "업데이트할 파일명",
                    },
                    "new_content": {
                        "type": "string",
                        "description": "새로운 내용 (기존 내용에 추가됨)",
                    },
                    "update_mode": {
                        "type": "string",
                        "description": "업데이트 모드 (append: 추가, replace: 교체)",
                        "enum": ["append", "replace"],
                        "default": "append",
                    },
                },
                "required": ["filename", "new_content"],
            },
        ),
        types.Tool(
            name="sync_knowledge_base",
            description="Knowledge Base 데이터 소스를 동기화합니다",
            inputSchema={
                "type": "object",
                "properties": {},
            },
        ),
        types.Tool(
            name="query_vector_store",
            description="벡터 저장소(Knowledge Base)에서 내용을 검색합니다",
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "검색할 키워드나 질문",
                    },
                    "max_results": {
                        "type": "integer",
                        "description": "최대 검색 결과 수 (기본값: 5)",
                        "default": 5,
                    },
                },
                "required": ["query"],
            },
        ),
        types.Tool(
            name="generate_comprehensive_performance_report",
            description="Oracle AWR 스타일의 종합 성능 진단 보고서 생성 (메트릭 분석, 상관관계, 느린 쿼리, 리소스 집약적 쿼리 포함)",
            inputSchema={
                "type": "object",
                "properties": {
                    "database_secret": {
                        "type": "string",
                        "description": "데이터베이스 시크릿 이름",
                    },
                    "db_instance_identifier": {
                        "type": "string",
                        "description": "데이터베이스 인스턴스 식별자",
                    },
                    "region": {
                        "type": "string",
                        "description": "AWS 리전 (기본값: ap-northeast-2)",
                        "default": "ap-northeast-2",
                    },
                    "hours": {
                        "type": "integer",
                        "description": "수집할 시간 범위 (시간 단위, 기본값: 24)",
                        "default": 24,
                    },
                },
                "required": ["database_secret", "db_instance_identifier"],
            },
        ),
        types.Tool(
            name="generate_cluster_performance_report",
            description="Aurora 클러스터 전용 성능 보고서 생성 (클러스터 레벨 분석 + 인스턴스별 상세 보고서 링크)",
            inputSchema={
                "type": "object",
                "properties": {
                    "database_secret": {
                        "type": "string",
                        "description": "데이터베이스 시크릿 이름",
                    },
                    "db_cluster_identifier": {
                        "type": "string",
                        "description": "Aurora 클러스터 식별자",
                    },
                    "region": {
                        "type": "string",
                        "description": "AWS 리전 (기본값: ap-northeast-2)",
                        "default": "ap-northeast-2",
                    },
                    "hours": {
                        "type": "integer",
                        "description": "수집할 시간 범위 (시간 단위, 기본값: 24)",
                        "default": 24,
                    },
                },
                "required": ["database_secret", "db_cluster_identifier"],
            },
        ),
        types.Tool(
            name="set_default_region",
            description="기본 AWS 리전을 변경합니다",
            inputSchema={
                "type": "object",
                "properties": {
                    "region_name": {
                        "type": "string",
                        "description": "설정할 AWS 리전 (예: ap-northeast-2, us-east-1, eu-west-1)",
                    }
                },
                "required": ["region_name"],
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
        elif name == "text_to_sql":
            result = await db_assistant.text_to_sql(
                arguments["database_secret"], arguments["natural_language_query"]
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
                arguments["csv_file"], arguments.get("std_threshold", 3.0)
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
        elif name == "debug_cloudwatch_collection":
            result = await db_assistant.debug_cloudwatch_collection(
                arguments["database_secret"],
                arguments["start_time"],
                arguments["end_time"],
            )
        elif name == "collect_slow_queries":
            result = await db_assistant.collect_slow_queries(
                arguments["database_secret"],
                arguments.get("start_time"),
                arguments.get("end_time"),
            )
        elif name == "enable_slow_query_log_exports":
            result = await db_assistant.enable_slow_query_log_exports(
                arguments["cluster_identifier"]
            )
        elif name == "collect_cpu_intensive_queries":
            result = await db_assistant.collect_cpu_intensive_queries(
                arguments["database_secret"],
                arguments.get("db_instance_identifier"),
                arguments.get("start_time"),
                arguments.get("end_time"),
            )
        elif name == "collect_temp_space_intensive_queries":
            result = await db_assistant.collect_temp_space_intensive_queries(
                arguments["database_secret"],
                arguments.get("db_instance_identifier"),
                arguments.get("start_time"),
                arguments.get("end_time"),
            )
        elif name == "validate_schema_lambda":
            validation_result = await db_assistant.validate_schema_lambda(
                arguments["database_secret"],
                arguments["database"],
                arguments["ddl_content"],
                arguments.get("region", "ap-northeast-2"),
            )
            # 결과를 문자열로 포맷팅
            if validation_result.get('success'):
                ddl_type = validation_result.get('ddl_type', 'UNKNOWN')
                table_name = validation_result.get('table_name', 'N/A')
                is_valid = validation_result.get('valid', False)
                issues = validation_result.get('issues', [])
                warnings = validation_result.get('warnings', [])
                s3_location = validation_result.get('s3_location', '')

                status = "✅ 검증 통과" if is_valid else "❌ 검증 실패"
                result = f"{status}\n\n"
                result += f"DDL 타입: {ddl_type}\n"
                result += f"테이블 이름: {table_name}\n\n"

                if issues:
                    result += f"문제점 ({len(issues)}개):\n"
                    for issue in issues:
                        result += f"  - {issue}\n"
                    result += "\n"

                if warnings:
                    result += f"경고 ({len(warnings)}개):\n"
                    for warning in warnings:
                        result += f"  - {warning}\n"
                    result += "\n"

                result += f"S3 저장 위치: {s3_location}"
            else:
                error_msg = validation_result.get('error', 'Unknown error')
                result = f"❌ 스키마 검증 실패: {error_msg}"
        elif name == "explain_query_lambda":
            explain_result = await db_assistant.explain_query_lambda(
                arguments["database_secret"],
                arguments["database"],
                arguments["query"],
                arguments.get("region", "ap-northeast-2"),
            )
            # 결과를 문자열로 포맷팅
            if explain_result.get('success'):
                query = explain_result.get('query', '')
                explain_data = explain_result.get('explain_data', [])
                performance_issues = explain_result.get('performance_issues', [])
                recommendations = explain_result.get('recommendations', [])
                s3_location = explain_result.get('s3_location', '')

                issue_count = explain_result.get('performance_issue_count', 0)
                status = "✅ 성능 이슈 없음" if issue_count == 0 else f"⚠️ 성능 이슈 {issue_count}개 발견"

                result = f"{status}\n\n"
                result += f"쿼리: {query[:100]}...\n\n" if len(query) > 100 else f"쿼리: {query}\n\n"

                if performance_issues:
                    result += f"성능 이슈 ({len(performance_issues)}개):\n"
                    for issue in performance_issues:
                        result += f"  - {issue}\n"
                    result += "\n"

                if recommendations:
                    result += f"개선 권장사항 ({len(recommendations)}개):\n"
                    for rec in recommendations:
                        result += f"  - {rec}\n"
                    result += "\n"

                result += f"S3 저장 위치: {s3_location}"
            else:
                error_msg = explain_result.get('error', 'Unknown error')
                result = f"❌ EXPLAIN 분석 실패: {error_msg}"
        elif name == "analyze_aurora_mysql_error_logs":
            result = await db_assistant.analyze_aurora_mysql_error_logs(
                arguments["keyword"],
                arguments["start_datetime_str"],
                arguments["end_datetime_str"],
            )
        elif name == "save_to_vector_store":
            result = await db_assistant.save_to_vector_store(
                arguments["content"],
                arguments["topic"],
                arguments.get("category", "examples"),
                arguments.get("tags"),
                arguments.get("force_save", False),
                arguments.get("auto_summarize", True),
            )
        elif name == "update_vector_content":
            result = await db_assistant.update_vector_content(
                arguments["filename"],
                arguments["new_content"],
                arguments.get("update_mode", "append"),
            )
        elif name == "sync_knowledge_base":
            result = await db_assistant.sync_knowledge_base()
        elif name == "query_vector_store":
            result = await db_assistant.query_vector_store(
                arguments["query"], arguments.get("max_results", 5)
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
                arguments.get("latest_count"),
            )
        elif name == "generate_comprehensive_performance_report":
            result = await db_assistant.generate_comprehensive_performance_report(
                arguments["database_secret"],
                arguments["db_instance_identifier"],
                arguments.get("region", "ap-northeast-2"),
                arguments.get("hours", 24),
            )
        elif name == "generate_cluster_performance_report":
            result = await db_assistant.generate_cluster_performance_report(
                arguments["database_secret"],
                arguments["db_cluster_identifier"],
                arguments.get("hours", 24),
                arguments.get("region", "ap-northeast-2"),
            )
        elif name == "set_default_region":
            result = db_assistant.set_default_region(arguments["region_name"])
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
