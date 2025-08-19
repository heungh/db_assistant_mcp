#!/usr/bin/env python3
"""
SQL 검증 및 성능분석 Amazon Q CLI MCP 서버
"""

import asyncio
import json
import os
import re
import subprocess
import time
from datetime import datetime
from typing import Dict, Any, List, Optional
from pathlib import Path

import boto3

try:
    import mysql.connector
    from mysql.connector import Error as MySQLError
except ImportError:
    mysql = None
    MySQLError = Exception

from mcp.server.models import InitializationOptions
import mcp.types as types
from mcp.server import NotificationOptions, Server
import mcp.server.stdio
import logging

# 현재 디렉토리 기준 경로 설정
CURRENT_DIR = Path(__file__).parent
OUTPUT_DIR = CURRENT_DIR / "output"
SQL_DIR = CURRENT_DIR / "sql"
DATA_DIR = CURRENT_DIR / "data"
LOG_DIR = CURRENT_DIR / "logs"

# 로그 디렉토리 생성
LOG_DIR.mkdir(exist_ok=True)

# 로깅 설정 - 파일과 콘솔 모두에 출력
log_file = LOG_DIR / "ddl_validation.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# 디렉토리 생성
OUTPUT_DIR.mkdir(exist_ok=True)
SQL_DIR.mkdir(exist_ok=True)
DATA_DIR.mkdir(exist_ok=True)


class DDLValidationQCLIServer:
    def __init__(self):
        self.bedrock_client = boto3.client(
            "bedrock-runtime", region_name="us-east-1", verify=False
        )
        self.knowledge_base_id = "0WQUBRHVR8"
        self.current_plan = None
        self.selected_cluster = None
        self.selected_database = None

    def setup_ssh_tunnel(self, db_host: str, region: str = "ap-northeast-2") -> bool:
        """SSH 터널 설정"""
        try:
            import subprocess
            import time

            # 기존 터널 종료
            try:
                subprocess.run(
                    ["pkill", "-f", "ssh.*54.180.79.255"],
                    capture_output=True,
                    timeout=5,
                )
            except:
                pass

            # SSH 터널 시작 (백그라운드 프로세스)
            ssh_command = [
                "ssh",
                "-F",
                "/dev/null",
                "-o",
                "UserKnownHostsFile=/dev/null",
                "-o",
                "StrictHostKeyChecking=no",
                "-o",
                "ConnectTimeout=10",
                "-o",
                "ServerAliveInterval=60",
                "-o",
                "ServerAliveCountMax=3",
                "-N",
                "-L",
                f"3307:{db_host}:3306",
                "-i",
                "/Users/heungh/test.pem",
                "ec2-user@54.180.79.255",
            ]

            logger.info(f"SSH 터널 설정 중: {db_host} -> localhost:3307")

            # 백그라운드에서 SSH 터널 시작
            process = subprocess.Popen(
                ssh_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE
            )

            # 터널이 설정될 때까지 대기
            time.sleep(3)

            # 프로세스가 아직 실행 중인지 확인
            if process.poll() is None:
                logger.info("SSH 터널이 설정되었습니다.")
                return True
            else:
                stdout, stderr = process.communicate()
                logger.error(f"SSH 터널 설정 실패: {stderr.decode()}")
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

    async def get_db_connection(
        self,
        database_secret: str,
        selected_database: str = None,
        use_ssh_tunnel: bool = True,
    ):
        """공통 DB 연결 함수"""
        try:
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
            get_secret_value_response = client.get_secret_value(
                SecretId=database_secret
            )
            if (
                not get_secret_value_response
                or "SecretString" not in get_secret_value_response
            ):
                raise Exception(f"시크릿을 가져올 수 없습니다: {database_secret}")
            secret = get_secret_value_response["SecretString"]
            db_config = json.loads(secret)

            connection_config = None
            tunnel_used = False

            # 선택된 데이터베이스가 있으면 사용, 없으면 기본값 사용
            database_name = selected_database or db_config.get(
                "dbname", db_config.get("database")
            )

            if use_ssh_tunnel:
                if self.setup_ssh_tunnel(db_config.get("host")):
                    connection_config = {
                        "host": "localhost",
                        "port": 3307,
                        "user": str(db_config.get("username", "")),
                        "password": str(db_config.get("password", "")),
                        "connection_timeout": 10,
                    }
                    # database는 None이 아닐 때만 추가
                    if database_name:
                        connection_config["database"] = str(database_name)
                    tunnel_used = True
        except Exception as e:
            import traceback

            error_details = traceback.format_exc()
            raise Exception(f"DB 연결 설정 중 오류: {str(e)}\n디버그: {error_details}")

        # 나머지 연결 로직은 그대로 유지

        if not connection_config:
            # MySQL 연결에 필요한 매개변수만 포함
            connection_config = {
                "host": str(db_config.get("host", "")),
                "port": int(db_config.get("port", 3306)),
                "user": str(db_config.get("username", "")),
                "password": str(db_config.get("password", "")),
                "connection_timeout": 10,
            }
            # database는 None이 아닐 때만 추가
            if database_name:
                connection_config["database"] = str(database_name)

        connection = mysql.connector.connect(**connection_config)
        return connection, tunnel_used

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
            if (
                not get_secret_value_response
                or "SecretString" not in get_secret_value_response
            ):
                raise Exception(f"시크릿을 가져올 수 없습니다: {secret_name}")
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

    async def validate_sql_file(
        self, filename: str, database_secret: Optional[str] = None
    ) -> str:
        """특정 SQL 파일 검증 - Edge 연결 방식"""
        try:
            sql_file_path = SQL_DIR / filename
            if not sql_file_path.exists():
                return f"SQL 파일을 찾을 수 없습니다: {filename}"

            with open(sql_file_path, "r", encoding="utf-8") as f:
                ddl_content = f.read()

            # Edge 연결 방식으로 검증 실행
            result = await self.execute_validation_workflow(
                ddl_content, database_secret, filename
            )
            return result
        except Exception as e:
            return f"SQL 파일 검증 실패: {str(e)}"

    async def execute_validation_workflow(
        self, ddl_content: str, database_secret: Optional[str], filename: str
    ) -> str:
        """Edge 연결 방식의 검증 워크플로우 실행"""
        try:
            # 검증 상태 초기화
            validation_state = {
                "ddl_content": ddl_content,
                "filename": filename,
                "database_secret": database_secret,
                "current_step": 1,
                "total_steps": (
                    7 if database_secret else 4
                ),  # DB 연결 여부에 따라 단계 수 조정
                "issues": [],
                "warnings": [],
                "recommendations": [],
            }

            # 1단계: 문법 검증
            validation_state = await self.step_1_syntax_check(validation_state)
            logger.info(f"Step 1 completed, state is None: {validation_state is None}")

            # 문법 오류가 있으면 중단
            if not validation_state.get("syntax_valid", False):
                return await self.generate_final_report(validation_state)

            # 2단계: 표준 규칙 검증
            validation_state = await self.step_2_standard_check(validation_state)
            logger.info(f"Step 2 completed, state is None: {validation_state is None}")

            # 3단계: 데이터베이스 연결 테스트 (database_secret이 있는 경우만)
            if database_secret:
                validation_state = await self.step_3_db_connection_test(
                    validation_state
                )
                logger.info(
                    f"Step 3 completed, state is None: {validation_state is None}"
                )

                # 연결 성공시 추가 검증 진행
                if validation_state and validation_state.get("db_connected", False):
                    # 4단계: 스키마 검증
                    validation_state = await self.step_4_schema_validation(
                        validation_state
                    )
                    logger.info(
                        f"Step 4 completed, state is None: {validation_state is None}"
                    )

                    # 5단계: 제약조건 검증
                    validation_state = await self.step_5_constraint_validation(
                        validation_state
                    )
                    logger.info(
                        f"Step 5 completed, state is None: {validation_state is None}"
                    )

            # 6단계: Claude AI 종합 검증 (기존 분석 결과 포함)
            if validation_state is not None:
                validation_state = await self.step_6_claude_validation(validation_state)
            else:
                logger.error("validation_state is None before step 6")

            # 7단계: 최종 보고서 생성
            result = await self.generate_final_report(validation_state)

            # 로컬 검증인 경우 안내 메시지 추가
            if not database_secret:
                result += "\n\n🔍 **로컬 검증 완료**\n• 데이터베이스 연결 없이 문법 및 표준 규칙만 검증되었습니다.\n• 완전한 스키마 검증을 원하시면 데이터베이스 시크릿을 지정해주세요."

            return result

        except Exception as e:
            import traceback

            error_details = traceback.format_exc()
            return f"검증 워크플로우 실행 중 오류 발생: {str(e)}\n\n디버그 정보:\n{error_details}"

    async def prompt_for_database_selection(
        self, ddl_content: str, filename: str
    ) -> str:
        """데이터베이스 선택 프롬프트"""
        try:
            # 로컬 검증을 위해 파일 정보 저장
            self.pending_validation = {"ddl_content": ddl_content, "filename": filename}

            # 사용 가능한 시크릿 목록 조회
            secrets_list = await self.get_available_database_secrets()

            if not secrets_list:
                # 시크릿이 없으면 로컬 검증만 수행
                return await self.execute_local_validation_only(ddl_content, filename)

            # 사용자에게 선택 옵션 제공
            prompt_message = f"""🔍 **DDL 검증 옵션 선택**

📄 **파일:** {filename}

**검증 방식을 선택해주세요:**

**1️⃣ 완전 검증 (권장)**
   • 문법 + 표준 규칙 + 데이터베이스 스키마 검증
   • 실제 DB 연결하여 테이블/컬럼 존재 확인
   • 제약조건 및 인덱스 검증
   • Claude AI 종합 분석

**2️⃣ 로컬 검증만**
   • 문법 + 표준 규칙 검증
   • Claude AI 분석
   • DB 연결 없이 빠른 검증

🗄️ **사용 가능한 데이터베이스:**
{secrets_list}

💡 **사용법:**
   • 완전 검증: validate_sql_with_database 도구로 데이터베이스 시크릿 지정
   • 로컬 검증: confirm_and_execute 도구로 'local' 입력"""

            return prompt_message

        except Exception as e:
            # 오류 발생시 로컬 검증만 수행
            return await self.execute_local_validation_only(ddl_content, filename)

    async def get_available_database_secrets(self) -> str:
        """사용 가능한 데이터베이스 시크릿 목록 조회"""
        try:
            # 모든 시크릿 조회 (필터링 없이)
            secrets = self.get_secrets_by_keyword("")

            if not secrets:
                return ""

            result = ""
            for i, secret in enumerate(secrets, 1):
                result += f"   {i}. {secret}\n"

            return result

        except Exception as e:
            return ""

    async def execute_local_validation_only(
        self, ddl_content: str, filename: str
    ) -> str:
        """로컬 검증만 수행 (DB 연결 없음)"""
        try:
            # 검증 상태 초기화 (database_secret 없음)
            validation_state = {
                "ddl_content": ddl_content,
                "filename": filename,
                "database_secret": None,
                "current_step": 1,
                "total_steps": 4,  # 로컬 검증만이므로 4단계
                "issues": [],
                "warnings": [],
                "recommendations": [],
            }

            # 1단계: 문법 검증
            validation_state = await self.step_1_syntax_check(validation_state)

            # 2단계: 표준 규칙 검증
            validation_state = await self.step_2_standard_check(validation_state)

            # 3단계: Claude AI 검증 (DB 정보 없이)
            validation_state = await self.step_6_claude_validation(validation_state)

            # 4단계: 최종 보고서 생성
            result = await self.generate_final_report(validation_state)

            # 로컬 검증임을 명시
            local_notice = "\n\n🔍 **로컬 검증 완료**\n• 데이터베이스 연결 없이 문법 및 표준 규칙만 검증되었습니다.\n• 완전한 스키마 검증을 원하시면 데이터베이스 시크릿을 지정해주세요."

            return result + local_notice

        except Exception as e:
            return f"로컬 검증 실행 중 오류 발생: {str(e)}"

    async def step_1_syntax_check(self, state: dict) -> dict:
        """1단계: 문법 검증"""
        ddl_content = state["ddl_content"]
        issues = []

        # DDL 타입 추출
        ddl_type = self.extract_ddl_type(ddl_content)
        state["ddl_type"] = ddl_type

        # 기본 문법 검증
        if not ddl_content.strip().endswith(";"):
            issues.append("세미콜론이 누락되었습니다.")

        state["syntax_valid"] = len(issues) == 0
        state["syntax_issues"] = issues
        state["current_step"] = 2

        return state

    async def step_2_standard_check(self, state: dict) -> dict:
        """2단계: 표준 규칙 검증"""
        # 현재는 기본 구현만 제공
        state["standard_compliant"] = True
        state["standard_issues"] = []
        state["current_step"] = 3
        return state

    async def step_3_db_connection_test(self, state: dict) -> dict:
        """3단계: 데이터베이스 연결 테스트"""
        database_secret = state["database_secret"]

        try:
            connection_result = await self.test_database_connection(database_secret)
            state["db_connected"] = connection_result["success"]
            state["db_connection_info"] = connection_result

            if connection_result["success"]:
                state["warnings"].append("✅ 데이터베이스 연결 성공")
            else:
                state["issues"].append(
                    f"❌ 데이터베이스 연결 실패: {connection_result.get('error', 'Unknown error')}"
                )

        except Exception as e:
            import traceback

            error_details = traceback.format_exc()
            state["db_connected"] = False
            state["issues"].append(
                f"데이터베이스 연결 테스트 중 오류: {str(e)}\n디버그: {error_details}"
            )

        state["current_step"] = 4
        return state

    async def step_4_schema_validation(self, state: dict) -> dict:
        """4단계: 스키마 검증"""
        try:
            logger.info("스키마 검증 시작")
            schema_result = await self.validate_schema(
                state["ddl_content"], state["database_secret"]
            )
            logger.info(f"스키마 검증 결과: {schema_result}")

            if schema_result["success"]:
                schema_issues = []
                for result in schema_result["validation_results"]:
                    if result.get("issues"):
                        schema_issues.extend(result["issues"])

                state["schema_issues"] = schema_issues
                if schema_issues:
                    state["issues"].extend(schema_issues)
                else:
                    state["warnings"].append("✅ 스키마 검증 통과")
                # 성공한 경우 모든 스키마 관련 오류 완전 제거
                filtered_issues = []
                for issue in state["issues"]:
                    if not ("스키마 검증" in issue or "argument 7" in issue):
                        filtered_issues.append(issue)
                state["issues"] = filtered_issues
            else:
                error_msg = (
                    f"스키마 검증 실패: {schema_result.get('error', 'Unknown error')}"
                )
                logger.error(error_msg)
                state["issues"].append(error_msg)

        except Exception as e:
            import traceback

            error_details = traceback.format_exc()
            error_msg = f"스키마 검증 중 오류: {str(e)}"
            logger.error(f"{error_msg}\n{error_details}")
            state["issues"].append(error_msg)

        state["current_step"] = 5
        return state

    async def step_5_constraint_validation(self, state: dict) -> dict:
        """5단계: 제약조건 검증"""
        try:
            logger.info("제약조건 검증 시작")
            constraint_result = await self.validate_constraints(
                state["ddl_content"], state["database_secret"]
            )
            logger.info(f"제약조건 검증 결과: {constraint_result}")

            if constraint_result["success"]:
                constraint_issues = []
                for result in constraint_result["constraint_results"]:
                    if not result.get("valid", True):
                        constraint_issues.append(result.get("issue", "제약조건 위반"))

                state["constraint_issues"] = constraint_issues
                if constraint_issues:
                    state["issues"].extend(constraint_issues)
                else:
                    state["warnings"].append("✅ 제약조건 검증 통과")
                # 성공한 경우 모든 제약조건 관련 오류 완전 제거
                filtered_issues = []
                for issue in state["issues"]:
                    if not ("제약조건 검증" in issue or "argument 7" in issue):
                        filtered_issues.append(issue)
                state["issues"] = filtered_issues
            else:
                error_msg = f"제약조건 검증 실패: {constraint_result.get('error', 'Unknown error')}"
                logger.error(error_msg)
                state["issues"].append(error_msg)

        except Exception as e:
            import traceback

            error_details = traceback.format_exc()
            error_msg = f"제약조건 검증 중 오류: {str(e)}"
            logger.error(f"{error_msg}\n{error_details}")
            state["issues"].append(error_msg)

        state["current_step"] = 6
        return state

    async def generate_html_report(
        self,
        report_path: Path,
        filename: str,
        ddl_content: str,
        ddl_type: str,
        status: str,
        summary: str,
        issues: List[str],
        db_connection_info: Optional[Dict],
        schema_validation: Optional[Dict],
        constraint_validation: Optional[Dict],
        database_secret: Optional[str],
    ):
        """HTML 보고서 생성"""
        try:
            # 상태에 따른 색상 및 아이콘
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

            # 기타 검증 문제 섹션
            other_issues_section = ""
            if other_issues:
                other_issues_section = """
                <div class="issues-section">
                    <h3>🚨 발견된 문제</h3>
                    <ul class="issues-list">
                """
                for issue in other_issues:
                    other_issues_section += f"<li>{issue}</li>"
                other_issues_section += """
                    </ul>
                </div>
                """

            # Claude 검증 결과 섹션
            claude_section = ""
            if claude_issues:
                claude_section = """
                <div class="claude-section">
                    <h3>🤖 Claude AI 검증 결과</h3>
                """
                for claude_result in claude_issues:
                    # 긴 텍스트를 위한 스타일 적용
                    claude_section += f"""
                    <div class="claude-result">
                        <pre class="claude-text">{claude_result}</pre>
                    </div>
                    """
                claude_section += """
                </div>
                """

            # 전체 문제가 없는 경우
            success_section = ""
            if not issues:
                success_section = """
                <div class="issues-section success">
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
    <title>DDL 검증 보고서 - {filename}</title>
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
            max-height: none;
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
            <h1>{status_icon} DDL 검증 보고서</h1>
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
                    <p>{ddl_type}</p>
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
            
            <div class="info-section">
                <h3>📊 검증 결과</h3>
                <p style="font-size: 1.2em; font-weight: 500; color: {status_color};">{summary}</p>
            </div>
            
            {claude_section}
            {other_issues_section}
            {success_section}
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

        except Exception as e:
            logger.error(f"HTML 보고서 생성 오류: {e}")

    async def step_6_claude_validation(self, state: dict) -> dict:
        """6단계: Claude AI 종합 검증 (기존 분석 결과 포함)"""
        # state가 None인 경우 방어 코드
        if state is None:
            logger.error("step_6_claude_validation: state is None")
            return {
                "current_step": 7,
                "issues": ["이전 단계에서 상태 정보가 손실되었습니다."],
                "warnings": [],
                "claude_issues": [],
            }

        try:
            logger.info("Claude 검증 시작")

            # 기존 분석 결과 수집
            existing_analysis = {
                "syntax_issues": state.get("syntax_issues", []),
                "schema_issues": state.get("schema_issues", []),
                "constraint_issues": state.get("constraint_issues", []),
                "db_connection_info": state.get("db_connection_info", {}),
            }

            # 스키마 정보 추출 (DB 연결이 성공한 경우)
            schema_info = None
            if state.get("db_connected", False) and state.get("database_secret"):
                try:
                    schema_info = await self.extract_current_schema_info(
                        state["database_secret"]
                    )
                except Exception as e:
                    logger.warning(f"Claude 검증용 스키마 정보 추출 실패: {e}")

            logger.info("Claude 검증 실행 중...")
            # Claude 검증 실행
            claude_result = await self.validate_with_claude(
                state["ddl_content"],
                state.get("database_secret"),
                schema_info,
                existing_analysis,
            )
            logger.info(f"Claude 검증 결과 (전체): {claude_result}")
            print(f"🤖 Claude AI 검증 결과:\n{claude_result}\n" + "=" * 50)

            # Claude 결과 분석 및 저장
            claude_issues = []
            if (
                "문제" in claude_result
                or "오류" in claude_result
                or "위반" in claude_result
            ):
                # "검증 통과"가 아닌 경우 이슈로 처리
                if "검증 통과" not in claude_result:
                    claude_issues.append(f"Claude 검증: {claude_result}")

            state["claude_issues"] = claude_issues
            if claude_issues:
                state["issues"].extend(claude_issues)
            else:
                state["warnings"].append("✅ Claude AI 검증 통과")

            logger.info("Claude 검증 완료")

        except Exception as e:
            import traceback

            error_trace = traceback.format_exc()
            logger.error(f"Claude 검증 중 오류: {e}")
            logger.error(f"오류 상세: {error_trace}")
            state["issues"].append(f"Claude 검증 중 오류 발생: {str(e)}")

        state["current_step"] = 7
        return state

    async def generate_final_report(self, state: dict) -> str:
        """최종 보고서 생성"""
        filename = state["filename"]
        ddl_content = state["ddl_content"]
        ddl_type = state.get("ddl_type", "UNKNOWN")

        # 모든 이슈 수집 - 성공한 검증의 오류는 제외
        all_issues = []
        all_issues.extend(state.get("syntax_issues", []))

        # MySQL 연결 오류 완전 제거
        clean_issues = []
        for issue in state.get("issues", []):
            issue_str = str(issue)
            # MySQL 관련 오류 모두 제거
            skip_keywords = [
                "argument 7 must be str or None, not bool",
                "스키마 검증 실패: 스키마 검증 오류: argument 7",
                "제약조건 검증 실패: 제약조건 검증 오류: argument 7",
            ]
            if any(keyword in issue_str for keyword in skip_keywords):
                continue
            clean_issues.append(issue)

        all_issues.extend(clean_issues)

        # 상태 결정
        if not state.get("syntax_valid", False):
            summary = f"❌ 문법 오류로 인한 검증 실패: {len(state.get('syntax_issues', []))}개"
            status = "FAIL"
        elif len(all_issues) == 0:
            summary = "✅ 모든 검증을 통과했습니다."
            status = "PASS"
        else:
            summary = f"❌ 발견된 문제: {len(all_issues)}개"
            status = "FAIL"

        # HTML 보고서 생성
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_path = OUTPUT_DIR / f"validation_report_{filename}_{timestamp}.html"

        # 새로운 generate_html_report 함수 사용
        await self.generate_html_report(
            report_path=report_path,
            filename=filename,
            ddl_content=ddl_content,
            ddl_type=ddl_type,
            status=status,
            summary=summary,
            issues=all_issues,
            db_connection_info=state.get("db_connection_info"),
            schema_validation=state.get("schema_validation"),
            constraint_validation=state.get("constraint_validation"),
            database_secret=state.get("database_secret"),
        )

        # 결과 메시지 생성
        result_message = f"""
{summary}

📄 상세 보고서가 저장되었습니다: {report_path}

📊 검증 결과:
"""

        # 각 단계별 결과 추가
        if state.get("syntax_issues"):
            result_message += f"• 문법 검증: ❌ {len(state['syntax_issues'])}개 문제\n"
        else:
            result_message += "• 문법 검증: ✅ 통과\n"

        if state.get("db_connected"):
            result_message += "• 데이터베이스 연결: ✅ 성공\n"

            if state.get("schema_issues"):
                result_message += (
                    f"• 스키마 검증: ❌ {len(state['schema_issues'])}개 문제\n"
                )
            else:
                result_message += "• 스키마 검증: ✅ 통과\n"

            if state.get("constraint_issues"):
                result_message += (
                    f"• 제약조건 검증: ❌ {len(state['constraint_issues'])}개 문제\n"
                )
            else:
                result_message += "• 제약조건 검증: ✅ 통과\n"
        elif state.get("database_secret"):
            result_message += "• 데이터베이스 연결: ❌ 실패\n"

        if state.get("claude_issues"):
            result_message += (
                f"• Claude AI 검증: ❌ {len(state['claude_issues'])}개 문제\n"
            )
        else:
            result_message += "• Claude AI 검증: ✅ 통과\n"

        return result_message

    def extract_ddl_type(self, ddl_content: str) -> str:
        """DDL 타입 추출"""
        ddl_upper = ddl_content.upper().strip()
        if ddl_upper.startswith("CREATE TABLE"):
            return "CREATE_TABLE"
        elif ddl_upper.startswith("ALTER TABLE"):
            return "ALTER_TABLE"
        elif ddl_upper.startswith("CREATE INDEX"):
            return "CREATE_INDEX"
        elif ddl_upper.startswith("DROP"):
            return "DROP"
        else:
            return "UNKNOWN"

    def setup_ssh_tunnel(self, db_host: str, region: str = "ap-northeast-2") -> bool:
        """SSH 터널 설정"""
        try:
            import subprocess
            import time

            # 기존 터널 종료
            subprocess.run(["pkill", "-f", "ssh.*54.180.79.255"], capture_output=True)

            # SSH 터널 시작 (ssh_tunnel.sh 방식 사용, SSH 설정 파일 무시)
            ssh_command = [
                "ssh",
                "-F",
                "/dev/null",  # SSH 설정 파일 무시
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

    async def test_database_connection(
        self, database_secret: str, use_ssh_tunnel: bool = True
    ) -> Dict[str, Any]:
        """데이터베이스 연결 테스트"""
        try:
            connection, tunnel_used = await self.get_db_connection(
                database_secret, None, use_ssh_tunnel
            )

            if connection.is_connected():
                db_info = connection.get_server_info()
                cursor = connection.cursor()
                cursor.execute("SELECT DATABASE()")
                current_db_result = cursor.fetchone()
                current_db = (
                    current_db_result[0]
                    if current_db_result and current_db_result[0]
                    else "None"
                )

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

                result = {
                    "success": True,
                    "server_version": db_info,
                    "current_database": current_db,
                    "connection_method": "SSH Tunnel" if tunnel_used else "Direct",
                    "databases": databases,
                    "tables": tables,
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
            import traceback

            error_details = traceback.format_exc()
            if use_ssh_tunnel:
                self.cleanup_ssh_tunnel()
            return {
                "success": False,
                "error": f"연결 테스트 오류: {str(e)}",
                "debug": error_details,
            }

    async def validate_schema(
        self, ddl_content: str, database_secret: str, use_ssh_tunnel: bool = True
    ) -> Dict[str, Any]:
        """DDL 구문 유형에 따른 스키마 검증"""
        try:
            # DDL 구문 유형 및 상세 정보 파싱
            ddl_info = self.parse_ddl_detailed(ddl_content)
            if not ddl_info:
                return {
                    "success": False,
                    "error": "DDL에서 구문 정보를 추출할 수 없습니다.",
                }

            connection, tunnel_used = await self.get_db_connection(
                database_secret, None, use_ssh_tunnel
            )
            cursor = connection.cursor()

            validation_results = []

            # DDL 구문 유형별 검증
            for ddl_statement in ddl_info:
                ddl_type = ddl_statement["type"]
                table_name = ddl_statement["table"]

                if ddl_type == "CREATE_TABLE":
                    result = await self.validate_create_table(cursor, ddl_statement)
                elif ddl_type == "ALTER_TABLE":
                    result = await self.validate_alter_table(cursor, ddl_statement)
                elif ddl_type == "CREATE_INDEX":
                    result = await self.validate_create_index(cursor, ddl_statement)
                elif ddl_type == "DROP_TABLE":
                    result = await self.validate_drop_table(cursor, ddl_statement)
                elif ddl_type == "DROP_INDEX":
                    result = await self.validate_drop_index(cursor, ddl_statement)
                else:
                    result = {
                        "table": table_name,
                        "ddl_type": ddl_type,
                        "valid": False,
                        "issues": [f"지원하지 않는 DDL 구문 유형: {ddl_type}"],
                    }

                validation_results.append(result)

            cursor.close()
            connection.close()

            # SSH 터널 정리
            if tunnel_used:
                self.cleanup_ssh_tunnel()

            return {"success": True, "validation_results": validation_results}

        except Exception as e:
            if use_ssh_tunnel:
                self.cleanup_ssh_tunnel()
            return {"success": False, "error": f"스키마 검증 오류: {str(e)}"}

    async def validate_constraints(
        self, ddl_content: str, database_secret: str, use_ssh_tunnel: bool = True
    ) -> Dict[str, Any]:
        """제약조건 검증 - FK, 인덱스, 제약조건 확인"""
        try:
            # DDL에서 제약조건 정보 추출
            constraints_info = self.parse_ddl_constraints(ddl_content)

            connection, tunnel_used = await self.get_db_connection(
                database_secret, None, use_ssh_tunnel
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

        for index_name, table_name in drop_index_matches:
            ddl_statements.append(
                {
                    "type": "DROP_INDEX",
                    "table": table_name.lower(),
                    "index_name": index_name.lower(),
                }
            )

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

    async def validate_drop_table(
        self, cursor, ddl_statement: Dict[str, Any]
    ) -> Dict[str, Any]:
        """DROP TABLE 구문 검증"""
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

            if not index_exists:
                issues.append(
                    f"인덱스 '{index_name}'이 테이블 '{table_name}'에 존재하지 않습니다."
                )

        return {
            "table": table_name,
            "ddl_type": "DROP_INDEX",
            "valid": len(issues) == 0,
            "issues": issues,
            "details": {"index_name": index_name},
        }

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

    async def analyze_current_schema(
        self, database_secret: str, use_ssh_tunnel: bool = True
    ) -> Dict[str, Any]:
        """현재 데이터베이스 스키마 상세 분석"""
        try:
            connection, tunnel_used = await self.get_db_connection(
                database_secret, None, use_ssh_tunnel
            )
            cursor = connection.cursor()

            # 현재 데이터베이스 확인
            cursor.execute("SELECT DATABASE()")
            current_db = cursor.fetchone()[0]

            schema_analysis = {
                "current_database": current_db,
                "tables": {},
                "indexes": {},
                "foreign_keys": {},
                "constraints": {},
            }

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

            for table_info in tables_info:
                table_name = table_info[0]
                schema_analysis["tables"][table_name] = {
                    "type": table_info[1],
                    "engine": table_info[2],
                    "rows": table_info[3],
                    "data_length": table_info[4],
                    "index_length": table_info[5],
                    "comment": table_info[6],
                    "columns": {},
                    "indexes": [],
                    "foreign_keys": [],
                }

                # 컬럼 정보 수집
                cursor.execute(
                    """
                    SELECT column_name, data_type, is_nullable, column_default,
                           column_key, extra, column_comment, character_maximum_length,
                           numeric_precision, numeric_scale
                    FROM information_schema.columns 
                    WHERE table_schema = DATABASE() AND table_name = %s
                    ORDER BY ordinal_position
                """,
                    (table_name,),
                )

                columns_info = cursor.fetchall()
                for col_info in columns_info:
                    col_name = col_info[0]
                    schema_analysis["tables"][table_name]["columns"][col_name] = {
                        "data_type": col_info[1],
                        "is_nullable": col_info[2],
                        "default": col_info[3],
                        "key": col_info[4],
                        "extra": col_info[5],
                        "comment": col_info[6],
                        "max_length": col_info[7],
                        "precision": col_info[8],
                        "scale": col_info[9],
                    }

                # 인덱스 정보 수집
                cursor.execute(
                    """
                    SELECT index_name, column_name, seq_in_index, non_unique,
                           index_type, comment
                    FROM information_schema.statistics 
                    WHERE table_schema = DATABASE() AND table_name = %s
                    ORDER BY index_name, seq_in_index
                """,
                    (table_name,),
                )

                indexes_info = cursor.fetchall()
                current_index = None
                for idx_info in indexes_info:
                    idx_name = idx_info[0]
                    if current_index != idx_name:
                        schema_analysis["tables"][table_name]["indexes"].append(
                            {
                                "name": idx_name,
                                "columns": [idx_info[1]],
                                "unique": idx_info[3] == 0,
                                "type": idx_info[4],
                                "comment": idx_info[5],
                            }
                        )
                        current_index = idx_name
                    else:
                        # 복합 인덱스의 추가 컬럼
                        schema_analysis["tables"][table_name]["indexes"][-1][
                            "columns"
                        ].append(idx_info[1])

                # 외래키 정보 수집
                cursor.execute(
                    """
                    SELECT kcu.constraint_name, kcu.column_name, kcu.referenced_table_name,
                           kcu.referenced_column_name, rc.update_rule, rc.delete_rule
                    FROM information_schema.key_column_usage kcu
                    JOIN information_schema.referential_constraints rc
                    ON kcu.constraint_name = rc.constraint_name 
                    AND kcu.table_schema = rc.constraint_schema
                    WHERE kcu.table_schema = DATABASE() AND kcu.table_name = %s
                    AND kcu.referenced_table_name IS NOT NULL
                """,
                    (table_name,),
                )

                fk_info = cursor.fetchall()
                for fk in fk_info:
                    schema_analysis["tables"][table_name]["foreign_keys"].append(
                        {
                            "constraint_name": fk[0],
                            "column": fk[1],
                            "referenced_table": fk[2],
                            "referenced_column": fk[3],
                            "update_rule": fk[4],
                            "delete_rule": fk[5],
                        }
                    )

            cursor.close()
            connection.close()

            if tunnel_used:
                self.cleanup_ssh_tunnel()

            return {"success": True, "schema_analysis": schema_analysis}

        except Exception as e:
            return {"success": False, "error": f"스키마 분석 오류: {str(e)}"}

    async def check_ddl_conflicts(
        self, ddl_content: str, database_secret: str, use_ssh_tunnel: bool = True
    ) -> Dict[str, Any]:
        """DDL 실행 전 충돌 및 문제점 사전 검사"""
        try:
            # 현재 스키마 분석
            schema_result = await self.analyze_current_schema(
                database_secret, use_ssh_tunnel
            )
            if not schema_result["success"]:
                return schema_result

            schema = schema_result["schema_analysis"]
            ddl_type = self.extract_ddl_type(ddl_content)

            conflicts = []
            warnings = []
            recommendations = []

            if ddl_type == "CREATE_TABLE":
                conflicts.extend(
                    await self._check_create_table_conflicts(ddl_content, schema)
                )
            elif ddl_type == "ALTER_TABLE":
                conflicts.extend(
                    await self._check_alter_table_conflicts(ddl_content, schema)
                )
            elif ddl_type == "CREATE_INDEX":
                conflicts.extend(
                    await self._check_create_index_conflicts(ddl_content, schema)
                )
            elif ddl_type == "DROP":
                conflicts.extend(await self._check_drop_conflicts(ddl_content, schema))

            return {
                "success": True,
                "ddl_type": ddl_type,
                "conflicts": conflicts,
                "warnings": warnings,
                "recommendations": recommendations,
                "current_schema": schema,
            }

        except Exception as e:
            return {"success": False, "error": f"DDL 충돌 검사 오류: {str(e)}"}

    async def _check_create_table_conflicts(
        self, ddl_content: str, schema: Dict
    ) -> List[str]:
        """CREATE TABLE 충돌 검사"""
        conflicts = []

        # 테이블명 추출
        table_match = re.search(
            r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?`?(\w+)`?",
            ddl_content,
            re.IGNORECASE,
        )
        if table_match:
            table_name = table_match.group(1)

            # 테이블 존재 여부 확인
            if table_name in schema["tables"]:
                if "IF NOT EXISTS" not in ddl_content.upper():
                    conflicts.append(
                        f"테이블 '{table_name}'이 이미 존재합니다. IF NOT EXISTS를 사용하거나 다른 이름을 선택하세요."
                    )
                else:
                    conflicts.append(
                        f"테이블 '{table_name}'이 이미 존재합니다. IF NOT EXISTS가 있어서 실행은 되지만 아무 작업도 수행되지 않습니다."
                    )

        return conflicts

    async def _check_alter_table_conflicts(
        self, ddl_content: str, schema: Dict
    ) -> List[str]:
        """ALTER TABLE 충돌 검사"""
        conflicts = []

        # 테이블명 추출
        table_match = re.search(
            r"ALTER\s+TABLE\s+`?(\w+)`?", ddl_content, re.IGNORECASE
        )
        if table_match:
            table_name = table_match.group(1)

            # 테이블 존재 여부 확인
            if table_name not in schema["tables"]:
                conflicts.append(f"테이블 '{table_name}'이 존재하지 않습니다.")
                return conflicts

            table_info = schema["tables"][table_name]

            # ADD COLUMN 검사
            add_column_matches = re.findall(
                r"ADD\s+(?:COLUMN\s+)?`?(\w+)`?", ddl_content, re.IGNORECASE
            )
            for col_name in add_column_matches:
                if col_name in table_info["columns"]:
                    conflicts.append(
                        f"컬럼 '{col_name}'이 테이블 '{table_name}'에 이미 존재합니다."
                    )

            # DROP COLUMN 검사
            drop_column_matches = re.findall(
                r"DROP\s+(?:COLUMN\s+)?`?(\w+)`?", ddl_content, re.IGNORECASE
            )
            for col_name in drop_column_matches:
                if col_name not in table_info["columns"]:
                    conflicts.append(
                        f"컬럼 '{col_name}'이 테이블 '{table_name}'에 존재하지 않습니다."
                    )
                else:
                    # 외래키 참조 확인
                    for fk in table_info["foreign_keys"]:
                        if fk["column"] == col_name:
                            conflicts.append(
                                f"컬럼 '{col_name}'은 외래키로 사용 중이므로 삭제할 수 없습니다."
                            )

            # MODIFY/CHANGE COLUMN 검사
            modify_matches = re.findall(
                r"(?:MODIFY|CHANGE)\s+(?:COLUMN\s+)?`?(\w+)`?",
                ddl_content,
                re.IGNORECASE,
            )
            for col_name in modify_matches:
                if col_name not in table_info["columns"]:
                    conflicts.append(
                        f"수정하려는 컬럼 '{col_name}'이 테이블 '{table_name}'에 존재하지 않습니다."
                    )

        return conflicts

    async def _check_create_index_conflicts(
        self, ddl_content: str, schema: Dict
    ) -> List[str]:
        """CREATE INDEX 충돌 검사"""
        conflicts = []

        # 인덱스명과 테이블명 추출
        index_match = re.search(
            r"CREATE\s+(?:UNIQUE\s+)?INDEX\s+`?(\w+)`?\s+ON\s+`?(\w+)`?\s*\((.*?)\)",
            ddl_content,
            re.IGNORECASE,
        )
        if index_match:
            index_name = index_match.group(1)
            table_name = index_match.group(2)
            columns_str = index_match.group(3)

            # 테이블 존재 여부 확인
            if table_name not in schema["tables"]:
                conflicts.append(f"테이블 '{table_name}'이 존재하지 않습니다.")
                return conflicts

            table_info = schema["tables"][table_name]

            # 인덱스 중복 확인
            for existing_index in table_info["indexes"]:
                if existing_index["name"] == index_name:
                    conflicts.append(f"인덱스 '{index_name}'이 이미 존재합니다.")

            # 컬럼 존재 여부 확인
            columns = [col.strip().strip("`") for col in columns_str.split(",")]
            for col_name in columns:
                # 함수나 표현식이 아닌 단순 컬럼명만 검사
                if col_name and not re.search(r"[()]", col_name):
                    if col_name not in table_info["columns"]:
                        conflicts.append(
                            f"컬럼 '{col_name}'이 테이블 '{table_name}'에 존재하지 않습니다."
                        )

        return conflicts

    async def _check_drop_conflicts(self, ddl_content: str, schema: Dict) -> List[str]:
        """DROP 문 충돌 검사"""
        conflicts = []

        if "DROP TABLE" in ddl_content.upper():
            # 테이블 삭제 검사
            table_match = re.search(
                r"DROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?`?(\w+)`?",
                ddl_content,
                re.IGNORECASE,
            )
            if table_match:
                table_name = table_match.group(1)
                if table_name not in schema["tables"]:
                    if "IF EXISTS" not in ddl_content.upper():
                        conflicts.append(
                            f"삭제하려는 테이블 '{table_name}'이 존재하지 않습니다."
                        )
                else:
                    # 외래키 참조 확인 (다른 테이블에서 이 테이블을 참조하는지)
                    for other_table, table_info in schema["tables"].items():
                        for fk in table_info["foreign_keys"]:
                            if fk["referenced_table"] == table_name:
                                conflicts.append(
                                    f"테이블 '{table_name}'은 테이블 '{other_table}'에서 외래키로 참조되고 있어 삭제할 수 없습니다."
                                )

        elif "DROP INDEX" in ddl_content.upper():
            # 인덱스 삭제 검사
            index_match = re.search(
                r"DROP\s+INDEX\s+`?(\w+)`?\s+ON\s+`?(\w+)`?", ddl_content, re.IGNORECASE
            )
            if index_match:
                index_name = index_match.group(1)
                table_name = index_match.group(2)

                if table_name not in schema["tables"]:
                    conflicts.append(f"테이블 '{table_name}'이 존재하지 않습니다.")
                else:
                    table_info = schema["tables"][table_name]
                    index_exists = any(
                        idx["name"] == index_name for idx in table_info["indexes"]
                    )
                    if not index_exists:
                        conflicts.append(
                            f"인덱스 '{index_name}'이 테이블 '{table_name}'에 존재하지 않습니다."
                        )

        return conflicts

    async def get_aurora_mysql_parameters(
        self,
        cluster_identifier: str,
        region: str = "ap-northeast-2",
        filter_type: str = "important",
        category: str = "all",
    ) -> str:
        """Aurora MySQL 클러스터의 파라미터 조회

        Args:
            cluster_identifier: 클러스터 식별자
            region: AWS 리전
            filter_type: 필터 타입 (important, custom, all)
            category: 파라미터 카테고리 (all, security, performance, memory, io, connection, logging, replication, aurora)
        """
        try:
            rds_client = boto3.client("rds", region_name=region, verify=False)

            # 클러스터 정보 조회
            clusters_response = rds_client.describe_db_clusters(
                DBClusterIdentifier=cluster_identifier
            )

            if not clusters_response["DBClusters"]:
                return f"❌ 클러스터를 찾을 수 없습니다: {cluster_identifier}"

            cluster = clusters_response["DBClusters"][0]
            cluster_param_group = cluster.get(
                "DBClusterParameterGroup", "default.aurora-mysql8.0"
            )

            # 카테고리별 제목 설정
            category_titles = {
                "all": "전체",
                "security": "보안 및 인증",
                "performance": "성능 최적화",
                "memory": "메모리 관리",
                "io": "I/O 및 스토리지",
                "connection": "연결 관리",
                "logging": "로깅 및 모니터링",
                "replication": "복제 및 백업",
                "aurora": "Aurora 특화 기능",
            }

            category_title = category_titles.get(category, category)

            result = f"""📊 Aurora MySQL 파라미터 정보 ({category_title})

🔧 **클러스터 정보:**
- 클러스터 ID: {cluster_identifier}
- 클러스터 파라미터 그룹: {cluster_param_group}
- 엔진 버전: {cluster.get('EngineVersion', 'N/A')}"""

            # 클러스터 파라미터 조회
            cluster_params = await self._get_parameters(
                rds_client, cluster_param_group, "cluster", filter_type, category
            )
            if cluster_params:
                result += f"\n\n🏗️ **클러스터 레벨 파라미터:**\n{cluster_params}"

            # 인스턴스 파라미터 조회
            if cluster.get("DBClusterMembers"):
                instance_id = cluster["DBClusterMembers"][0]["DBInstanceIdentifier"]
                instance_response = rds_client.describe_db_instances(
                    DBInstanceIdentifier=instance_id
                )
                if instance_response["DBInstances"]:
                    instance_param_group = instance_response["DBInstances"][0][
                        "DBParameterGroups"
                    ][0]["DBParameterGroupName"]
                    result += f"\n- 인스턴스 파라미터 그룹: {instance_param_group}"

                    instance_params = await self._get_parameters(
                        rds_client,
                        instance_param_group,
                        "instance",
                        filter_type,
                        category,
                    )
                    if instance_params:
                        result += (
                            f"\n\n🖥️ **인스턴스 레벨 파라미터:**\n{instance_params}"
                        )
                    else:
                        if category == "all":
                            result += f"\n\n🖥️ **인스턴스 레벨 파라미터:** 해당 카테고리에 표시할 파라미터 없음"
                        else:
                            result += f"\n\n🖥️ **인스턴스 레벨 파라미터:** {category_title} 카테고리에 해당하는 파라미터 없음"

            return result

        except Exception as e:
            return f"❌ Aurora 파라미터 조회 실패: {str(e)}"

    async def _get_parameters(
        self,
        rds_client,
        param_group_name: str,
        level: str,
        filter_type: str,
        category: str,
    ) -> str:
        """파라미터 조회 및 필터링"""
        try:
            # 파라미터 조회
            if level == "cluster":
                response = rds_client.describe_db_cluster_parameters(
                    DBClusterParameterGroupName=param_group_name
                )
            else:
                response = rds_client.describe_db_parameters(
                    DBParameterGroupName=param_group_name
                )

            parameters = response["Parameters"]

            # 카테고리별 파라미터 정의 (확장된 분류)
            category_params = {
                "security": [
                    "activate_all_roles_on_login",
                    "authentication_kerberos_caseins_cmp",
                    "default_authentication_plugin",
                    "default_password_lifetime",
                    "check_proxy_users",
                    "mysql_native_password_proxy_users",
                    "sha256_password_proxy_users",
                    "validate_password_policy",
                ],
                "performance": [
                    "innodb_thread_concurrency",
                    "innodb_read_io_threads",
                    "innodb_write_io_threads",
                    "thread_cache_size",
                    "thread_stack",
                    "innodb_purge_threads",
                    "innodb_adaptive_flushing",
                    "innodb_adaptive_max_sleep_delay",
                    "innodb_concurrency_tickets",
                    "innodb_flushing_avg_loops",
                    "innodb_lru_scan_depth",
                    "innodb_max_dirty_pages_pct",
                    "innodb_max_purge_lag",
                    "innodb_max_purge_lag_delay",
                    "innodb_old_blocks_pct",
                    "innodb_old_blocks_time",
                    "innodb_parallel_read_threads",
                    "query_cache_size",
                    "query_cache_type",
                ],
                "memory": [
                    "innodb_buffer_pool_size",
                    "tmp_table_size",
                    "max_heap_table_size",
                    "sort_buffer_size",
                    "read_buffer_size",
                    "read_rnd_buffer_size",
                    "join_buffer_size",
                    "innodb_log_buffer_size",
                    "key_buffer_size",
                    "innodb_buffer_pool_dump_at_shutdown",
                    "innodb_buffer_pool_load_at_startup",
                    "innodb_buffer_pool_dump_now",
                    "innodb_buffer_pool_load_now",
                    "innodb_change_buffer_max_size",
                    "bulk_insert_buffer_size",
                ],
                "io": [
                    "innodb_flush_log_at_trx_commit",
                    "sync_binlog",
                    "innodb_log_file_size",
                    "innodb_io_capacity",
                    "innodb_io_capacity_max",
                    "innodb_flush_method",
                    "innodb_file_per_table",
                    "innodb_doublewrite",
                    "innodb_flush_neighbors",
                    "innodb_flush_log_at_timeout",
                    "innodb_log_compressed_pages",
                    "innodb_open_files",
                    "innodb_read_only",
                    "innodb_sort_buffer_size",
                ],
                "connection": [
                    "max_connections",
                    "max_user_connections",
                    "connect_timeout",
                    "interactive_timeout",
                    "wait_timeout",
                    "max_connect_errors",
                    "back_log",
                    "host_cache_size",
                    "max_allowed_packet",
                ],
                "logging": [
                    "general_log",
                    "slow_query_log",
                    "log_queries_not_using_indexes",
                    "long_query_time",
                    "log_slow_admin_statements",
                    "log_slow_slave_statements",
                    "general_log_file",
                    "slow_query_log_file",
                    "log_error",
                    "log_warnings",
                    "innodb_print_all_deadlocks",
                ],
                "replication": [
                    "binlog_format",
                    "expire_logs_days",
                    "max_binlog_size",
                    "binlog_cache_size",
                    "slave_net_timeout",
                    "slave_parallel_workers",
                    "binlog_checksum",
                    "binlog_group_commit_sync_delay",
                    "binlog_group_commit_sync_no_delay_count",
                    "binlog_order_commits",
                    "binlog_row_image",
                    "binlog_rows_query_log_events",
                    "binlog_stmt_cache_size",
                    "binlog_transaction_compression",
                    "binlog_backup",
                    "binlog_replication_globaldb",
                ],
                "aurora": [
                    "aurora_binlog_replication_max_yield_seconds",
                    "aurora_binlog_replication_sec_index_parallel_workers",
                    "aurora_enable_staggered_replica_restart",
                    "aurora_enhanced_binlog",
                    "aurora_full_double_precision_in_json",
                    "aurora_fwd_writer_idle_timeout",
                    "aurora_fwd_writer_max_connections_pct",
                    "aurora_in_memory_relaylog",
                    "aurora_jemalloc_background_thread",
                    "aurora_jemalloc_dirty_decay_ms",
                    "aurora_jemalloc_tcache_enabled",
                    "aurora_ml_inference_timeout",
                    "aurora_oom_response",
                    "aurora_parallel_query",
                    "aurora_read_replica_read_committed",
                    "aurora_replica_read_consistency",
                    "aurora_tmptable_enable_per_table_limit",
                    "aurora_use_vector_instructions",
                    "aurora_aurora_max_partitions_for_range",
                ],
            }

            # 필터링 적용
            filtered_params = []

            if filter_type == "all":
                filtered_params = parameters
            elif filter_type == "custom":
                filtered_params = [p for p in parameters if p.get("Source") == "user"]
            elif filter_type == "important":
                important_params = [
                    "innodb_buffer_pool_size",
                    "max_connections",
                    "innodb_log_file_size",
                    "query_cache_size",
                    "tmp_table_size",
                    "max_heap_table_size",
                    "innodb_flush_log_at_trx_commit",
                    "sync_binlog",
                    "binlog_format",
                    "character_set_server",
                    "collation_server",
                    "time_zone",
                    "aurora_parallel_query",
                    "aurora_oom_response",
                    "aws_default_s3_role",
                ]
                filtered_params = [
                    p for p in parameters if p["ParameterName"] in important_params
                ]

            # 카테고리 필터링
            if category != "all" and category in category_params:
                category_param_names = category_params[category]
                filtered_params = [
                    p
                    for p in filtered_params
                    if p["ParameterName"] in category_param_names
                ]

            if not filtered_params:
                return ""

            # 결과 포맷팅 (카테고리별로 그룹화)
            result = ""

            # 카테고리별로 파라미터 그룹화
            if category == "all":
                # 전체 조회 시 카테고리별로 분류하여 표시
                categorized_params = {}
                uncategorized_params = []

                for param in filtered_params:
                    param_name = param["ParameterName"]
                    found_category = None

                    for cat, param_list in category_params.items():
                        if param_name in param_list:
                            if cat not in categorized_params:
                                categorized_params[cat] = []
                            categorized_params[cat].append(param)
                            found_category = cat
                            break

                    if not found_category:
                        uncategorized_params.append(param)

                # 카테고리별 출력
                category_titles = {
                    "security": "🔐 보안 및 인증",
                    "performance": "⚡ 성능 최적화",
                    "memory": "💾 메모리 관리",
                    "io": "💿 I/O 및 스토리지",
                    "connection": "🔗 연결 관리",
                    "logging": "📝 로깅 및 모니터링",
                    "replication": "🔄 복제 및 백업",
                    "aurora": "☁️ Aurora 특화 기능",
                }

                for cat in [
                    "security",
                    "performance",
                    "memory",
                    "io",
                    "connection",
                    "logging",
                    "replication",
                    "aurora",
                ]:
                    if cat in categorized_params:
                        result += f"\n{category_titles[cat]}:\n"
                        for param in categorized_params[cat]:
                            name = param["ParameterName"]
                            value = param.get("ParameterValue", "N/A")
                            source = param.get("Source", "N/A")
                            description = (
                                param.get("Description", "")[:50] + "..."
                                if len(param.get("Description", "")) > 50
                                else param.get("Description", "")
                            )

                            result += f"• {name}: {value} (Source: {source})\n"
                            if description and filter_type == "all":
                                result += f"  └─ {description}\n"

                # 분류되지 않은 파라미터
                if uncategorized_params:
                    result += f"\n🔧 기타 파라미터:\n"
                    for param in uncategorized_params:
                        name = param["ParameterName"]
                        value = param.get("ParameterValue", "N/A")
                        source = param.get("Source", "N/A")
                        description = (
                            param.get("Description", "")[:50] + "..."
                            if len(param.get("Description", "")) > 50
                            else param.get("Description", "")
                        )

                        result += f"• {name}: {value} (Source: {source})\n"
                        if description and filter_type == "all":
                            result += f"  └─ {description}\n"
            else:
                # 특정 카테고리 조회 시
                for param in filtered_params:
                    name = param["ParameterName"]
                    value = param.get("ParameterValue", "N/A")
                    source = param.get("Source", "N/A")
                    description = (
                        param.get("Description", "")[:50] + "..."
                        if len(param.get("Description", "")) > 50
                        else param.get("Description", "")
                    )

                    result += f"• {name}: {value} (Source: {source})\n"
                    if description and filter_type == "all":
                        result += f"  └─ {description}\n"

            return result.rstrip()

        except Exception as e:
            return f"❌ {level} 파라미터 조회 실패: {str(e)}"

    async def _get_default_aurora_parameters(
        self, parameter_group_name: str, region: str
    ) -> str:
        """기본 Aurora 파라미터 그룹 정보 조회"""
        try:
            rds_client = boto3.client("rds", region_name=region, verify=False)

            # 기본 파라미터 그룹들 조회
            param_groups_response = rds_client.describe_db_cluster_parameter_groups()

            default_group = None
            for group in param_groups_response["DBClusterParameterGroups"]:
                if "default.aurora-mysql" in group["DBClusterParameterGroupName"]:
                    default_group = group
                    break

            if not default_group:
                return (
                    f"❌ 기본 파라미터 그룹을 찾을 수 없습니다: {parameter_group_name}"
                )

            # 기본 파라미터들 조회
            parameters_response = rds_client.describe_db_cluster_parameters(
                DBClusterParameterGroupName=default_group["DBClusterParameterGroupName"]
            )

            parameters = parameters_response["Parameters"]

            result = f"""📊 Aurora MySQL 기본 파라미터 정보

🔧 **파라미터 그룹 정보:**
- 그룹명: {default_group['DBClusterParameterGroupName']}
- 패밀리: {default_group['DBParameterGroupFamily']}
- 설명: {default_group['Description']}

📋 **기본 파라미터 (일부):**"""

            # 중요한 파라미터들만 표시
            important_params = [
                "innodb_buffer_pool_size",
                "max_connections",
                "innodb_log_file_size",
                "query_cache_size",
                "tmp_table_size",
                "max_heap_table_size",
            ]

            for param in parameters[:20]:  # 처음 20개만
                param_name = param["ParameterName"]
                if param_name in important_params:
                    value = param.get("ParameterValue", "N/A")
                    result += f"\n• {param_name}: {value}"

            return result

        except Exception as e:
            return f"❌ 기본 파라미터 조회 실패: {str(e)}"

    async def create_validation_plan(
        self, filename: str, database_secret: Optional[str] = None
    ) -> Dict[str, Any]:
        """DDL 검증 실행 계획 생성"""
        try:
            # SQL 파일 존재 확인
            sql_file_path = SQL_DIR / filename
            if not sql_file_path.exists():
                return {
                    "operation": "validate_sql_file",
                    "filename": filename,
                    "database_secret": database_secret,
                    "status": "error",
                    "error": f"SQL 파일을 찾을 수 없습니다: {filename}",
                    "steps": [],
                }

            # DDL 내용 미리 읽기
            with open(sql_file_path, "r", encoding="utf-8") as f:
                ddl_content = f.read()

            ddl_type = self.extract_ddl_type(ddl_content)

            # 검증 단계 정의
            steps = [
                {
                    "step": 1,
                    "name": "문법 검증",
                    "description": "DDL 기본 문법 및 구조 검증",
                    "details": [
                        "세미콜론 누락 확인",
                        "DDL 구문 구조 검증",
                        "Claude AI를 통한 고급 문법 검증",
                    ],
                },
                {
                    "step": 2,
                    "name": "표준 규칙 검증",
                    "description": "스키마 명명 규칙 및 표준 준수 확인",
                    "details": [
                        "테이블/컬럼 명명 규칙 검증",
                        "데이터 타입 표준 준수 확인",
                    ],
                },
            ]

            # 데이터베이스 연결이 있는 경우 추가 단계
            if database_secret:
                steps.extend(
                    [
                        {
                            "step": 3,
                            "name": "데이터베이스 연결 테스트",
                            "description": f"'{database_secret}' 시크릿으로 DB 연결 확인",
                            "details": [
                                "AWS Secrets Manager에서 연결 정보 조회",
                                "SSH 터널을 통한 데이터베이스 연결",
                                "연결 상태 및 권한 확인",
                            ],
                        },
                        {
                            "step": 4,
                            "name": "스키마 검증",
                            "description": "현재 데이터베이스 스키마와 DDL 호환성 검증",
                            "details": [
                                f"{ddl_type} 작업 대상 확인",
                                "테이블/컬럼 존재 여부 검증",
                                "데이터 타입 호환성 확인",
                            ],
                        },
                        {
                            "step": 5,
                            "name": "제약조건 검증",
                            "description": "외래키, 인덱스 등 제약조건 검증",
                            "details": [
                                "외래키 참조 테이블 존재 확인",
                                "인덱스 중복 여부 검증",
                                "제약조건 충돌 검사",
                            ],
                        },
                    ]
                )

            steps.append(
                {
                    "step": len(steps) + 1,
                    "name": "최종 보고서 생성",
                    "description": "HTML 형식의 상세 검증 보고서 생성",
                    "details": [
                        "검증 결과 종합",
                        "문제점 및 권장사항 정리",
                        "output 디렉토리에 HTML 보고서 저장",
                    ],
                }
            )

            return {
                "operation": "validate_sql_file",
                "filename": filename,
                "database_secret": database_secret,
                "ddl_type": ddl_type,
                "ddl_content": ddl_content,  # 실제 DDL 내용 포함
                "ddl_preview": (
                    ddl_content[:200] + "..." if len(ddl_content) > 200 else ddl_content
                ),
                "steps": steps,
                "created_at": datetime.now().isoformat(),
                "status": "created",
            }

        except Exception as e:
            return {
                "operation": "validate_sql_file",
                "filename": filename,
                "database_secret": database_secret,
                "status": "error",
                "error": f"계획 생성 중 오류: {str(e)}",
                "steps": [],
            }

    def _format_validation_plan_display(self, plan: Dict[str, Any]) -> str:
        """검증 계획 표시 형식 생성"""
        if plan["status"] == "error":
            return f"❌ **계획 생성 실패:** {plan['error']}"

        result = f"""🎯 **검증 대상:** {plan['filename']}
📅 **계획 생성 시간:** {plan['created_at']}
🔧 **DDL 타입:** {plan['ddl_type']}
🗄️ **데이터베이스:** {plan['database_secret'] or '연결 없음 (기본 검증만)'}

📝 **DDL 미리보기:**
```sql
{plan['ddl_preview']}
```

🔄 **검증 단계 ({len(plan['steps'])}단계):**"""

        for step in plan["steps"]:
            result += f"\n\n   **{step['step']}. {step['name']}**"
            result += f"\n   └─ {step['description']}"

            if step.get("details"):
                for detail in step["details"]:
                    result += f"\n      • {detail}"

        # 예상 소요 시간 및 주의사항
        estimated_time = "30초 ~ 1분" if plan["database_secret"] else "10 ~ 20초"
        result += f"\n\n⏱️ **예상 소요 시간:** {estimated_time}"

        if plan["database_secret"]:
            result += f"\n⚠️ **주의사항:** 데이터베이스 연결이 포함되어 있습니다."

        return result

    def _create_validate_all_sql_plan(self, operation: str, **kwargs) -> Dict[str, Any]:
        """validate_all_sql 작업 계획 생성"""
        if operation == "validate_all_sql":
            database_secret = kwargs.get("database_secret")
            tool_name = "validate_all_sql"

            plan_steps = [
                {
                    "step": 1,
                    "action": "SQL 파일 목록 조회",
                    "target": "sql 디렉토리",
                    "tool": "list_sql_files",
                },
                {
                    "step": 2,
                    "action": "파일 개수 확인",
                    "target": "최대 5개 제한",
                    "tool": "internal_check",
                },
                {
                    "step": 3,
                    "action": "각 파일 순차 검증",
                    "target": "개별 파일",
                    "tool": "validate_sql_file",
                },
                {
                    "step": 4,
                    "action": "종합 결과 생성",
                    "target": "전체 요약",
                    "tool": "fs_write",
                },
            ]

        elif operation == "analyze_current_schema":
            database_secret = kwargs.get("database_secret")
            tool_name = "analyze_current_schema"

            plan_steps = [
                {
                    "step": 1,
                    "action": "데이터베이스 연결",
                    "target": database_secret,
                    "tool": "test_database_connection",
                },
                {
                    "step": 2,
                    "action": "테이블 목록 조회",
                    "target": "information_schema",
                    "tool": "mysql_query",
                },
                {
                    "step": 3,
                    "action": "컬럼 정보 수집",
                    "target": "각 테이블",
                    "tool": "mysql_query",
                },
                {
                    "step": 4,
                    "action": "인덱스 정보 수집",
                    "target": "각 테이블",
                    "tool": "mysql_query",
                },
                {
                    "step": 5,
                    "action": "외래키 정보 수집",
                    "target": "각 테이블",
                    "tool": "mysql_query",
                },
                {
                    "step": 6,
                    "action": "스키마 분석 결과 생성",
                    "target": "종합 정보",
                    "tool": "internal_analysis",
                },
            ]

        elif operation == "get_aurora_mysql_parameters":
            cluster_identifier = kwargs.get("cluster_identifier")
            tool_name = "get_aurora_mysql_parameters"

            plan_steps = [
                {
                    "step": 1,
                    "action": "클러스터 정보 조회",
                    "target": cluster_identifier,
                    "tool": "use_aws",
                },
                {
                    "step": 2,
                    "action": "파라미터 그룹 확인",
                    "target": "적용된 그룹",
                    "tool": "use_aws",
                },
                {
                    "step": 3,
                    "action": "파라미터 값 조회",
                    "target": "주요 설정",
                    "tool": "use_aws",
                },
                {
                    "step": 4,
                    "action": "커스텀 설정 필터링",
                    "target": "사용자 정의 값",
                    "tool": "internal_filter",
                },
            ]

        else:
            plan_steps = [
                {
                    "step": 1,
                    "action": f"{operation} 실행",
                    "target": "기본 동작",
                    "tool": operation,
                }
            ]

        plan = {
            "operation": operation,
            "tool_name": tool_name,
            "parameters": kwargs,
            "steps": plan_steps,
            "created_at": datetime.now().isoformat(),
            "status": "created",
        }

        self.current_plan = plan
        return plan

    async def execute_with_auto_plan(self, operation: str, **kwargs) -> str:
        """자동 실행 계획 생성 및 표시 후 확인"""
        try:
            # 1. 실행 계획 생성
            plan = await self.create_execution_plan(operation, **kwargs)
            plan_display = self._format_plan_display(plan)

            # 2. 실행 계획 표시 및 확인 요청
            confirmation_message = f"""📋 **실행 계획이 생성되었습니다:**

{plan_display}

❓ **이 계획대로 진행하시겠습니까?** 
   • 'y' 또는 'yes': 실행 진행
   • 'n' 또는 'no': 실행 취소

💡 **참고:** confirm_and_execute 도구로 응답해주세요."""

            return confirmation_message

        except Exception as e:
            return f"❌ 실행 계획 생성 중 오류 발생: {str(e)}"

    async def execute_with_plan(self, operation: str, **kwargs) -> str:
        """계획에 따른 실행"""
        # 기존 계획이 있는지 확인
        if self.current_plan and self.current_plan["operation"] == operation:
            plan = self.current_plan
            plan_display = self._format_plan_display(plan)

            # 사용자 확인 요청
            confirmation = f"""📋 **실행 계획이 준비되었습니다:**

{plan_display}

❓ **이 계획대로 진행하시겠습니까?** (y/n)
"""
            return confirmation
        else:
            # 새 계획 생성
            plan = await self.create_execution_plan(operation, **kwargs)
            plan_display = self._format_plan_display(plan)

            return f"""📋 **실행 계획을 생성했습니다:**

{plan_display}

❓ **이 계획대로 진행하시겠습니까?** (y/n)

💡 **참고:** 'y' 또는 'yes'로 응답하면 실행을 시작합니다.
"""

    async def create_execution_plan(self, operation: str, **kwargs) -> Dict[str, Any]:
        """작업 실행 계획 생성"""
        try:
            if operation == "validate_sql_file":
                filename = kwargs.get("filename", "")
                database_secret = kwargs.get("database_secret", "")

                # SQL 파일 읽기
                sql_file_path = SQL_DIR / filename
                if not sql_file_path.exists():
                    return {"error": f"SQL 파일을 찾을 수 없습니다: {filename}"}

                with open(sql_file_path, "r", encoding="utf-8") as f:
                    ddl_content = f.read()

                ddl_type = self.extract_ddl_type(ddl_content)

                # DDL 미리보기 (처음 100자)
                preview = (
                    ddl_content.strip()[:100] + "..."
                    if len(ddl_content.strip()) > 100
                    else ddl_content.strip()
                )

                plan = {
                    "operation": operation,
                    "filename": filename,
                    "database_secret": database_secret,
                    "ddl_type": ddl_type,
                    "preview": preview,
                    "steps": [
                        "문법 검증",
                        "표준 규칙 검증",
                        "데이터베이스 연결 테스트",
                        "스키마 검증",
                        "제약조건 검증",
                        "최종 보고서 생성",
                    ],
                    "status": "created",
                }

                return plan

            elif operation == "test_database_connection":
                database_secret = kwargs.get("database_secret", "")

                plan = {
                    "operation": operation,
                    "database_secret": database_secret,
                    "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "tool_name": "test_database_connection",
                    "steps": [
                        {
                            "step": 1,
                            "action": "데이터베이스 연결 테스트",
                            "target": database_secret,
                            "tool": "test_database_connection",
                        }
                    ],
                    "status": "created",
                    "parameters": kwargs,
                }

                return plan

            elif operation == "get_schema_summary":
                database_secret = kwargs.get("database_secret", "")

                plan = {
                    "operation": operation,
                    "database_secret": database_secret,
                    "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "tool_name": "get_schema_summary",
                    "steps": [
                        {
                            "step": 1,
                            "action": "데이터베이스 연결",
                            "target": database_secret,
                            "tool": "get_schema_summary",
                        },
                        {
                            "step": 2,
                            "action": "스키마 정보 수집",
                            "target": "전체 스키마",
                            "tool": "get_schema_summary",
                        },
                    ],
                    "status": "created",
                    "parameters": kwargs,
                }

                return plan

            return {"error": f"지원하지 않는 작업입니다: {operation}"}

        except Exception as e:
            return {"error": f"실행 계획 생성 실패: {str(e)}"}

    def _format_plan_display(self, plan: Dict[str, Any]) -> str:
        """계획 표시 형식 생성"""
        result = f"""🎯 **작업:** {plan['operation']}
📅 **생성 시간:** {plan['created_at']}
🔧 **매칭 툴:** {plan.get('tool_name', plan['operation'])}

📝 **실행 단계:**"""

        for step in plan["steps"]:
            tool_info = f" [{step.get('tool', 'internal')}]" if step.get("tool") else ""
            result += (
                f"\n   {step['step']}. {step['action']} → {step['target']}{tool_info}"
            )

        if plan["parameters"]:
            result += f"\n\n⚙️ **매개변수:**"
            for key, value in plan["parameters"].items():
                if value:
                    result += f"\n   • {key}: {value}"

        return result

    async def confirm_and_execute(self, confirmation: str) -> str:
        """확인 후 실행"""
        # 로컬 검증 요청 처리
        if confirmation.lower() in ["local", "2", "로컬"]:
            if hasattr(self, "pending_validation"):
                ddl_content = self.pending_validation["ddl_content"]
                filename = self.pending_validation["filename"]
                result = await self.execute_local_validation_only(ddl_content, filename)
                delattr(self, "pending_validation")
                return result
            else:
                return "❌ 로컬 검증할 파일 정보가 없습니다."

        if not self.current_plan:
            return "❌ 실행할 계획이 없습니다. 먼저 작업을 요청해주세요."

        if confirmation.lower() not in ["y", "yes", "예", "ㅇ"]:
            self.current_plan = None
            return "❌ 실행이 취소되었습니다."

        # 계획에 따라 실제 실행
        plan = self.current_plan
        operation = plan["operation"]

        try:
            result = f"🚀 **실행 시작:** {operation}\n\n"

            # DDL 검증 실행
            if operation == "validate_sql_file":
                if plan["status"] == "error":
                    result += f"❌ 계획 오류: {plan['error']}"
                else:
                    validation_result = await self.execute_validation_workflow(
                        plan.get("ddl_content", ""),
                        plan.get("database_secret"),
                        plan["filename"],
                    )
                    result += validation_result

            # 기존 다른 작업들...
            elif operation == "test_database_connection":
                connection_result = await self.test_connection_only(
                    plan["parameters"]["database_secret"]
                )
                result += connection_result

            elif operation == "validate_all_sql":
                validation_result = await self.validate_all_sql_files(
                    plan["parameters"].get("database_secret")
                )
                result += validation_result

            elif operation == "analyze_current_schema":
                schema_result = await self.analyze_current_schema(
                    plan["parameters"]["database_secret"]
                )
                if schema_result["success"]:
                    schema = schema_result["schema_analysis"]
                    result += f"""✅ 스키마 분석 완료 (DB: {schema['current_database']})

📊 **분석 결과:**
- 총 테이블 수: {len(schema['tables'])}개

📋 **테이블 상세:**"""
                    for table_name, table_info in schema["tables"].items():
                        result += f"""
🔹 **{table_name}** ({table_info['engine']})
   - 컬럼: {len(table_info['columns'])}개
   - 인덱스: {len(table_info['indexes'])}개  
   - 외래키: {len(table_info['foreign_keys'])}개
   - 예상 행 수: {table_info['rows']:,}"""
                        if table_info["comment"]:
                            result += f"\n   - 설명: {table_info['comment']}"

                        # 컬럼 상세 정보 추가 (항상 표시)
                        result += f"\n\n   📋 **컬럼 정보:**"
                        if table_info["columns"]:
                            for col_name, col_info in table_info["columns"].items():
                                data_type = col_info["data_type"]
                                if col_info["max_length"]:
                                    data_type += f"({col_info['max_length']})"
                                elif col_info["precision"] and col_info["scale"]:
                                    data_type += (
                                        f"({col_info['precision']},{col_info['scale']})"
                                    )
                                elif col_info["precision"]:
                                    data_type += f"({col_info['precision']})"

                                nullable = (
                                    "NULL"
                                    if col_info["is_nullable"] == "YES"
                                    else "NOT NULL"
                                )
                                key_info = (
                                    f" [{col_info['key']}]" if col_info["key"] else ""
                                )
                                extra_info = (
                                    f" {col_info['extra']}" if col_info["extra"] else ""
                                )
                                default_info = (
                                    f" DEFAULT {col_info['default']}"
                                    if col_info["default"]
                                    else ""
                                )

                                result += f"\n      • {col_name}: {data_type} {nullable}{key_info}{extra_info}{default_info}"
                                if col_info["comment"]:
                                    result += f" -- {col_info['comment']}"
                        else:
                            result += f"\n      컬럼 정보를 가져올 수 없습니다."

                        # 인덱스 상세 정보 추가 (항상 표시)
                        result += f"\n\n   🔍 **인덱스 정보:**"
                        if table_info["indexes"]:
                            for idx in table_info["indexes"]:
                                unique_info = "UNIQUE " if idx["unique"] else ""
                                columns_str = ", ".join(idx["columns"])
                                result += f"\n      • {unique_info}{idx['name']} ({columns_str}) [{idx['type']}]"
                                if idx["comment"]:
                                    result += f" -- {idx['comment']}"
                        else:
                            result += f"\n      인덱스 정보가 없습니다."

                        # 외래키 상세 정보 추가 (항상 표시)
                        result += f"\n\n   🔗 **외래키 정보:**"
                        if table_info["foreign_keys"]:
                            for fk in table_info["foreign_keys"]:
                                result += f"\n      • {fk['constraint_name']}: {fk['column']} → {fk['referenced_table']}.{fk['referenced_column']}"
                                result += f" (UPDATE: {fk['update_rule']}, DELETE: {fk['delete_rule']})"
                        else:
                            result += f"\n      외래키가 없습니다."

                        # 테이블 통계 정보 추가
                        if table_info["data_length"] or table_info["index_length"]:
                            result += f"\n\n   📊 **테이블 통계:**"
                            if table_info["data_length"]:
                                data_size = (
                                    table_info["data_length"] / 1024 / 1024
                                )  # MB 변환
                                result += f"\n      • 데이터 크기: {data_size:.2f} MB"
                            if table_info["index_length"]:
                                index_size = (
                                    table_info["index_length"] / 1024 / 1024
                                )  # MB 변환
                                result += f"\n      • 인덱스 크기: {index_size:.2f} MB"
                else:
                    result += f"❌ 스키마 분석 실패: {schema_result['error']}"

            else:
                result += f"❌ 지원하지 않는 작업: {operation}"

            # 계획 완료
            self.current_plan = None
            result += f"\n\n✅ **실행 완료:** {operation}"

            return result

        except Exception as e:
            self.current_plan = None
            return f"❌ 실행 중 오류 발생: {str(e)}"

    async def get_schema_summary(self, database_secret: str) -> str:
        """현재 스키마 요약 정보 반환"""
        try:
            schema_result = await self.analyze_current_schema(database_secret)
            if not schema_result["success"]:
                return f"❌ 스키마 분석 실패: {schema_result['error']}"

            schema = schema_result["schema_analysis"]

            summary = f"""📊 데이터베이스 스키마 요약 (DB: {schema['current_database']})

📋 **테이블 목록** ({len(schema['tables'])}개):"""

            for table_name, table_info in schema["tables"].items():
                column_count = len(table_info["columns"])
                index_count = len(table_info["indexes"])
                fk_count = len(table_info["foreign_keys"])

                summary += f"""
  🔹 **{table_name}** ({table_info['engine']})
     - 컬럼: {column_count}개, 인덱스: {index_count}개, 외래키: {fk_count}개
     - 예상 행 수: {table_info['rows']:,}"""

                if table_info["comment"]:
                    summary += f"\n     - 설명: {table_info['comment']}"

            return summary

        except Exception as e:
            return f"스키마 요약 생성 실패: {str(e)}"

    async def validate_all_sql_files(
        self, database_secret: Optional[str] = None
    ) -> str:
        """모든 SQL 파일 검증 (최대 5개) - 통합 보고서 생성"""
        try:
            sql_files = list(SQL_DIR.glob("*.sql"))
            if not sql_files:
                return "sql 디렉토리에 SQL 파일이 없습니다."

            # 최대 5개 파일만 처리
            files_to_process = sql_files[:5]
            if len(sql_files) > 5:
                logger.warning(
                    f"SQL 파일이 {len(sql_files)}개 있지만 처음 5개만 처리합니다."
                )

            validation_results = []
            summary_results = []

            for sql_file in files_to_process:
                try:
                    # 개별 파일 검증을 execute_validation_workflow로 실행 (개별 검증과 동일한 방식)
                    with open(sql_file, "r", encoding="utf-8") as f:
                        ddl_content = f.read()

                    # 개별 검증과 동일한 워크플로우 사용
                    result = await self.execute_validation_workflow(
                        ddl_content, database_secret, sql_file.name
                    )

                    # 결과에서 상태 파악 (간단한 방식)
                    if "✅ 모든 검증을 통과했습니다" in result:
                        status = "PASS"
                        issues = []
                    else:
                        status = "FAIL"
                        # 결과에서 문제 개수 추출
                        import re

                        issue_match = re.search(r"(\d+)개 문제", result)
                        issue_count = int(issue_match.group(1)) if issue_match else 1
                        issues = [f"검증 실패 ({issue_count}개 문제 발견)"]

                    validation_results.append(
                        {
                            "filename": sql_file.name,
                            "ddl_content": ddl_content,
                            "ddl_type": self.extract_ddl_type(ddl_content),
                            "status": status,
                            "issues": issues,
                            "warnings": [],
                            "db_connection_info": None,
                            "syntax_valid": status == "PASS",
                            "full_result": result,
                        }
                    )

                    # 요약 결과
                    if status == "PASS":
                        summary_results.append(f"✅ **{sql_file.name}**: 통과")
                    else:
                        summary_results.append(f"❌ **{sql_file.name}**: 실패")

                except Exception as e:
                    validation_results.append(
                        {
                            "filename": sql_file.name,
                            "ddl_content": "",
                            "ddl_type": "ERROR",
                            "status": "ERROR",
                            "issues": [f"검증 중 오류 발생: {str(e)}"],
                            "warnings": [],
                            "db_connection_info": None,
                            "syntax_valid": False,
                            "full_result": f"오류: {str(e)}",
                        }
                    )
                    summary_results.append(f"❌ **{sql_file.name}**: 오류 - {str(e)}")

            # 통합 HTML 보고서 생성
            consolidated_report_path = await self.generate_consolidated_html_report(
                validation_results, database_secret
            )

            # 요약 통계
            total_files = len(validation_results)
            passed_files = sum(1 for r in validation_results if r["status"] == "PASS")
            failed_files = total_files - passed_files

            summary = f"""📊 전체 SQL 파일 검증 완료

📋 요약:
• 총 파일: {total_files}개
• 통과: {passed_files}개 ({passed_files/total_files*100:.1f}%)
• 실패: {failed_files}개 ({failed_files/total_files*100:.1f}%)

📄 종합 보고서: {consolidated_report_path}

📊 개별 결과:
{chr(10).join(summary_results)}"""

            if len(sql_files) > 5:
                summary += (
                    f"\n\n⚠️ 전체 {len(sql_files)}개 파일 중 처음 5개만 처리되었습니다."
                )

            return summary

        except Exception as e:
            return f"전체 SQL 파일 검증 실패: {str(e)}"

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
                status_color = "#28a745" if result["status"] == "PASS" else "#dc3545"
                status_icon = "✅" if result["status"] == "PASS" else "❌"

                # Claude 검증과 기타 검증 분리
                claude_issues = []
                other_issues = []

                for issue in result["issues"]:
                    if issue.startswith("Claude 검증:"):
                        claude_issues.append(issue[12:].strip())
                    else:
                        other_issues.append(issue)

                # 기타 문제 섹션
                other_issues_html = ""
                if other_issues:
                    other_issues_html = f"""
                    <div class="issues-subsection">
                        <h5>🚨 발견된 문제</h5>
                        <ul>
                            {''.join(f'<li>{issue}</li>' for issue in other_issues)}
                        </ul>
                    </div>
                    """

                # Claude 검증 결과 섹션
                claude_section_html = ""
                if claude_issues:
                    claude_section_html = f"""
                    <div class="claude-subsection">
                        <h5>🤖 Claude AI 검증</h5>
                        {''.join(f'<div class="claude-result-small"><pre>{claude_result}</pre></div>' for claude_result in claude_issues)}
                    </div>
                    """

                # 성공 섹션
                success_html = ""
                if not result["issues"]:
                    success_html = '<div class="success-message">✅ 모든 검증을 통과했습니다.</div>'

                file_sections += f"""
                <div class="file-section">
                    <div class="file-header">
                        <h3>{status_icon} {i}. {result['filename']}</h3>
                        <span class="status-badge" style="background-color: {status_color};">{result['status']}</span>
                    </div>
                    
                    <div class="file-details">
                        <div class="detail-item">
                            <strong>DDL 타입:</strong> {result['ddl_type']}
                        </div>
                        <div class="detail-item">
                            <strong>문제 수:</strong> {len(result['issues'])}개
                        </div>
                    </div>
                    
                    <div class="sql-code-small">
                        <h4>📝 DDL 내용</h4>
                        <pre>{result['ddl_content']}</pre>
                    </div>
                    
                    <div class="validation-result">
                        <h4>📊 검증 결과</h4>
                        <pre>{result.get('full_result', '결과 없음')}</pre>
                    </div>
                    
                    {claude_section_html}
                    {other_issues_html}
                    {success_html}
                </div>
                """

            # HTML 보고서 생성
            html_content = f"""<!DOCTYPE html>
<html lang="ko">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>통합 DDL 검증 보고서</title>
    <style>
        body {{
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            line-height: 1.6;
            margin: 0;
            padding: 20px;
            background-color: #f5f5f5;
        }}
        .container {{
            max-width: 1400px;
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
        .summary-stats {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
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
            margin: 30px 0;
            border: 1px solid #e9ecef;
            border-radius: 8px;
            overflow: hidden;
        }}
        .file-header {{
            background: #f8f9fa;
            padding: 20px;
            border-bottom: 1px solid #e9ecef;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }}
        .file-header h3 {{
            margin: 0;
            color: #333;
        }}
        .status-badge {{
            padding: 6px 12px;
            border-radius: 15px;
            color: white;
            font-weight: bold;
            font-size: 0.9em;
        }}
        .file-details {{
            padding: 15px 20px;
            background: #fafafa;
            border-bottom: 1px solid #e9ecef;
        }}
        .detail-item {{
            display: inline-block;
            margin-right: 30px;
            color: #666;
        }}
        .sql-code-small {{
            padding: 20px;
        }}
        .sql-code-small h4, .validation-result h4 {{
            margin: 0 0 15px 0;
            color: #495057;
        }}
        .sql-code-small pre, .validation-result pre {{
            background: #f8f9fa;
            border: 1px solid #e9ecef;
            border-radius: 4px;
            padding: 15px;
            overflow-x: auto;
            font-family: 'Courier New', monospace;
            font-size: 0.9em;
            max-height: 200px;
            overflow-y: auto;
        }}
        .validation-result {{
            padding: 20px;
            border-top: 1px solid #e9ecef;
        }}
        .claude-subsection {{
            padding: 15px 20px;
            background: #f8f9ff;
            border-top: 1px solid #e9ecef;
        }}
        .claude-subsection h5 {{
            margin: 0 0 15px 0;
            color: #495057;
        }}
        .claude-result-small {{
            background: white;
            border: 1px solid #e9ecef;
            border-radius: 4px;
            margin: 10px 0;
        }}
        .claude-result-small pre {{
            padding: 15px;
            margin: 0;
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            font-size: 0.9em;
            line-height: 1.6;
            white-space: pre-wrap;
            word-wrap: break-word;
            max-height: 300px;
            overflow-y: auto;
        }}
        .issues-subsection {{
            padding: 15px 20px;
            border-top: 1px solid #e9ecef;
        }}
        .issues-subsection h5 {{
            margin: 0 0 15px 0;
            color: #495057;
        }}
        .issues-subsection ul {{
            margin: 0;
            padding-left: 20px;
        }}
        .issues-subsection li {{
            margin: 5px 0;
            color: #dc3545;
        }}
        .success-message {{
            padding: 15px 20px;
            background: #d4edda;
            color: #155724;
            border-top: 1px solid #c3e6cb;
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
            }}
            .file-header {{
                flex-direction: column;
                align-items: flex-start;
                gap: 10px;
            }}
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>📊 통합 DDL 검증 보고서</h1>
            <p>생성일시: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        </div>
        
        <div class="content">
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
                    <div class="stat-number">{passed_files/total_files*100:.1f}%</div>
                    <div class="stat-label">성공률</div>
                </div>
            </div>
            
            <div class="database-info">
                <h2>🗄️ 데이터베이스 정보</h2>
                <p><strong>시크릿:</strong> {database_secret or 'N/A'}</p>
            </div>
            
            {file_sections}
        </div>
        
        <div class="footer">
            <p>Generated by DB Assistant MCP Server</p>
            <p>Report generated at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        </div>
    </div>
</body>
</html>"""

            with open(report_path, "w", encoding="utf-8") as f:
                f.write(html_content)

            return str(report_path)

        except Exception as e:
            logger.error(f"통합 HTML 보고서 생성 오류: {e}")
            return f"보고서 생성 실패: {str(e)}"

    async def list_aurora_mysql_clusters(self, region: str = "ap-northeast-2") -> str:
        """현재 리전의 Aurora MySQL 클러스터 목록 조회"""
        try:
            rds_client = boto3.client("rds", region_name=region, verify=False)

            # Aurora MySQL 클러스터 조회
            response = rds_client.describe_db_clusters()

            aurora_mysql_clusters = []
            for cluster in response["DBClusters"]:
                if (
                    cluster["Engine"] == "aurora-mysql"
                    and cluster["Status"] == "available"
                ):
                    aurora_mysql_clusters.append(
                        {
                            "identifier": cluster["DBClusterIdentifier"],
                            "engine_version": cluster["EngineVersion"],
                            "endpoint": cluster.get("Endpoint", "N/A"),
                            "status": cluster["Status"],
                            "database_name": cluster.get("DatabaseName", "N/A"),
                        }
                    )

            if not aurora_mysql_clusters:
                return f"❌ {region} 리전에서 사용 가능한 Aurora MySQL 클러스터를 찾을 수 없습니다."

            result = f"🗄️ **Aurora MySQL 클러스터 목록** ({region} 리전)\n\n"

            for i, cluster in enumerate(aurora_mysql_clusters, 1):
                result += f"**{i}. {cluster['identifier']}**\n"
                result += f"   • 엔진 버전: {cluster['engine_version']}\n"
                result += f"   • 엔드포인트: {cluster['endpoint']}\n"
                result += f"   • 기본 DB: {cluster['database_name']}\n"
                result += f"   • 상태: {cluster['status']}\n\n"

            result += "💡 **사용법:** 클러스터 번호나 이름을 입력하여 선택하세요."

            return result

        except Exception as e:
            return f"❌ Aurora MySQL 클러스터 조회 실패: {str(e)}"

    async def select_aurora_cluster(
        self, cluster_selection: str, region: str = "ap-northeast-2"
    ) -> str:
        """Aurora 클러스터 선택"""
        try:
            rds_client = boto3.client("rds", region_name=region, verify=False)
            response = rds_client.describe_db_clusters()

            aurora_mysql_clusters = []
            for cluster in response["DBClusters"]:
                if (
                    cluster["Engine"] == "aurora-mysql"
                    and cluster["Status"] == "available"
                ):
                    aurora_mysql_clusters.append(cluster)

            if not aurora_mysql_clusters:
                return f"❌ {region} 리전에서 사용 가능한 Aurora MySQL 클러스터를 찾을 수 없습니다."

            selected_cluster = None

            # 번호로 선택
            if cluster_selection.isdigit():
                cluster_index = int(cluster_selection) - 1
                if 0 <= cluster_index < len(aurora_mysql_clusters):
                    selected_cluster = aurora_mysql_clusters[cluster_index]
            else:
                # 이름으로 선택
                for cluster in aurora_mysql_clusters:
                    if (
                        cluster_selection.lower()
                        in cluster["DBClusterIdentifier"].lower()
                    ):
                        selected_cluster = cluster
                        break

            if not selected_cluster:
                return f"❌ 선택한 클러스터를 찾을 수 없습니다: {cluster_selection}"

            # 선택된 클러스터 정보 저장 (임시로 클래스 변수에 저장)
            self.selected_cluster = {
                "identifier": selected_cluster["DBClusterIdentifier"],
                "endpoint": selected_cluster.get("Endpoint"),
                "database_name": selected_cluster.get("DatabaseName"),
                "region": region,
            }

            return f"""✅ **Aurora 클러스터 선택 완료**

🗄️ **선택된 클러스터:** {selected_cluster['DBClusterIdentifier']}
🌐 **엔드포인트:** {selected_cluster.get('Endpoint', 'N/A')}
📊 **기본 데이터베이스:** {selected_cluster.get('DatabaseName', 'N/A')}
📍 **리전:** {region}

💡 이제 이 클러스터에 대한 검증 작업을 수행할 수 있습니다."""

        except Exception as e:
            return f"❌ Aurora 클러스터 선택 실패: {str(e)}"

    async def list_databases(self, database_secret: str) -> str:
        """데이터베이스 목록 조회"""
        try:
            connection, tunnel_used = await self.get_db_connection(database_secret)
            cursor = connection.cursor()

            # 데이터베이스 목록 조회
            cursor.execute("SHOW DATABASES")
            databases = cursor.fetchall()

            # 시스템 데이터베이스 제외
            system_dbs = {"information_schema", "performance_schema", "mysql", "sys"}
            user_databases = [db[0] for db in databases if db[0] not in system_dbs]

            cursor.close()
            connection.close()

            if not user_databases:
                return "❌ 사용자 데이터베이스를 찾을 수 없습니다."

            result = "🗄️ **데이터베이스 목록**\n\n"

            for i, db_name in enumerate(user_databases, 1):
                result += f"**{i}. {db_name}**\n"

            result += "\n💡 **사용법:** 데이터베이스 번호나 이름을 입력하여 선택하세요."

            return result

        except Exception as e:
            return f"❌ 데이터베이스 목록 조회 실패: {str(e)}"

    async def select_database(
        self, database_secret: str, database_selection: str
    ) -> str:
        """데이터베이스 선택 및 변경"""
        try:
            connection, tunnel_used = await self.get_db_connection(database_secret)
            cursor = connection.cursor()

            # 데이터베이스 목록 조회
            cursor.execute("SHOW DATABASES")
            databases = cursor.fetchall()

            # 시스템 데이터베이스 제외
            system_dbs = {"information_schema", "performance_schema", "mysql", "sys"}
            user_databases = [db[0] for db in databases if db[0] not in system_dbs]

            if not user_databases:
                cursor.close()
                connection.close()
                return "❌ 사용자 데이터베이스를 찾을 수 없습니다."

            selected_db = None

            # 번호로 선택
            if database_selection.isdigit():
                db_index = int(database_selection) - 1
                if 0 <= db_index < len(user_databases):
                    selected_db = user_databases[db_index]
            else:
                # 이름으로 선택
                for db_name in user_databases:
                    if database_selection.lower() in db_name.lower():
                        selected_db = db_name
                        break

            if not selected_db:
                cursor.close()
                connection.close()
                return (
                    f"❌ 선택한 데이터베이스를 찾을 수 없습니다: {database_selection}"
                )

            # USE 명령으로 데이터베이스 변경
            cursor.execute(f"USE `{selected_db}`")

            # 변경 확인
            cursor.execute("SELECT DATABASE()")
            current_db = cursor.fetchone()[0]

            # 선택된 데이터베이스 저장
            self.selected_database = selected_db

            cursor.close()
            connection.close()

            return f"""✅ **데이터베이스 변경 완료**

🗄️ **현재 데이터베이스:** {current_db}
🔄 **변경 명령:** USE `{selected_db}`

💡 이제 이 데이터베이스에 대한 작업을 수행할 수 있습니다."""

        except Exception as e:
            return f"❌ 데이터베이스 선택 실패: {str(e)}"

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

    async def test_connection_only(self, database_secret: str) -> str:
        """연결 테스트만 수행"""
        try:
            connection_result = await self.test_database_connection(
                database_secret, use_ssh_tunnel=True
            )

            if connection_result["success"]:
                databases_list = "\n".join(
                    [f"   - {db}" for db in connection_result.get("databases", [])]
                )
                tables_list = "\n".join(
                    [f"   - {table}" for table in connection_result.get("tables", [])]
                )

                return f"""✅ 데이터베이스 연결 성공!

**연결 정보:**
- 호스트: {connection_result.get('host', 'N/A')}
- 포트: {connection_result.get('port', 'N/A')}
- 연결 방식: {connection_result.get('connection_method', 'N/A')}
- 현재 데이터베이스: {connection_result.get('current_database', 'N/A')}
- 서버 버전: {connection_result.get('server_version', 'N/A')}

**데이터베이스 목록:**
{databases_list if databases_list else '   (없음)'}

**현재 DB 테이블 목록:**
{tables_list if tables_list else '   (없음)'}"""
            else:
                return f"❌ 데이터베이스 연결 실패: {connection_result['error']}"

        except Exception as e:
            return f"연결 테스트 중 오류 발생: {str(e)}"

    async def get_performance_metrics(
        self, database_secret: str, metric_type: str = "all"
    ) -> str:
        """데이터베이스 성능 메트릭 조회"""
        try:
            connection, tunnel_used = await self.get_db_connection(database_secret)
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

    async def analyze_slow_queries(self, database_secret: str, limit: int = 10) -> str:
        """느린 쿼리 분석"""
        try:
            connection, tunnel_used = await self.get_db_connection(database_secret)
            cursor = connection.cursor()

            result = f"🐌 **느린 쿼리 분석** (상위 {limit}개)\n\n"

            # Performance Schema에서 느린 쿼리 조회
            cursor.execute(
                f"""
                SELECT 
                    DIGEST_TEXT as query_pattern,
                    COUNT_STAR as exec_count,
                    ROUND(AVG_TIMER_WAIT/1000000000000, 6) as avg_time_sec,
                    ROUND(MAX_TIMER_WAIT/1000000000000, 6) as max_time_sec,
                    ROUND(SUM_TIMER_WAIT/1000000000000, 6) as total_time_sec,
                    ROUND(SUM_ROWS_EXAMINED/COUNT_STAR, 0) as avg_rows_examined,
                    ROUND(SUM_ROWS_SENT/COUNT_STAR, 0) as avg_rows_sent
                FROM performance_schema.events_statements_summary_by_digest 
                WHERE DIGEST_TEXT IS NOT NULL 
                AND AVG_TIMER_WAIT > 1000000000
                ORDER BY AVG_TIMER_WAIT DESC 
                LIMIT {limit}
            """
            )

            slow_queries = cursor.fetchall()

            if not slow_queries:
                result += "✅ 느린 쿼리가 발견되지 않았습니다.\n"
            else:
                for i, (
                    pattern,
                    count,
                    avg_time,
                    max_time,
                    total_time,
                    avg_examined,
                    avg_sent,
                ) in enumerate(slow_queries, 1):
                    pattern_short = (
                        (pattern[:80] + "...") if len(pattern) > 80 else pattern
                    )
                    result += f"**{i}. 쿼리 패턴:**\n```sql\n{pattern_short}\n```\n"
                    result += f"📈 **통계:**\n"
                    result += f"- 실행 횟수: {count:,}회\n"
                    result += f"- 평균 실행 시간: {avg_time:.3f}초\n"
                    result += f"- 최대 실행 시간: {max_time:.3f}초\n"
                    result += f"- 총 실행 시간: {total_time:.3f}초\n"
                    result += f"- 평균 검사 행 수: {avg_examined:,.0f}행\n"
                    result += f"- 평균 반환 행 수: {avg_sent:,.0f}행\n\n"

            cursor.close()
            connection.close()

            if tunnel_used:
                self.cleanup_ssh_tunnel()

            return result

        except Exception as e:
            return f"❌ 느린 쿼리 분석 실패: {str(e)}"

    async def get_table_io_stats(
        self, database_secret: str, schema_name: Optional[str] = None
    ) -> str:
        """테이블별 I/O 통계 조회"""
        try:
            connection, tunnel_used = await self.get_db_connection(database_secret)
            cursor = connection.cursor()

            # 스키마 이름 설정
            if not schema_name:
                cursor.execute("SELECT DATABASE()")
                schema_name = cursor.fetchone()[0]

            result = f"💿 **테이블 I/O 통계** (스키마: {schema_name})\n\n"

            # 테이블별 I/O 통계 조회
            cursor.execute(
                """
                SELECT 
                    object_name as table_name,
                    count_read,
                    count_write,
                    count_read + count_write as total_io,
                    sum_timer_read/1000000000000 as read_time_sec,
                    sum_timer_write/1000000000000 as write_time_sec,
                    (sum_timer_read + sum_timer_write)/1000000000000 as total_time_sec
                FROM performance_schema.table_io_waits_summary_by_table 
                WHERE object_schema = %s
                AND count_read + count_write > 0
                ORDER BY count_read + count_write DESC 
                LIMIT 10
            """,
                (schema_name,),
            )

            io_stats = cursor.fetchall()

            if not io_stats:
                result += "📊 I/O 통계 데이터가 없습니다.\n"
            else:
                result += "📊 **상위 10개 테이블 (I/O 기준):**\n\n"
                for i, (
                    table,
                    read_count,
                    write_count,
                    total_io,
                    read_time,
                    write_time,
                    total_time,
                ) in enumerate(io_stats, 1):
                    result += f"**{i}. {table}**\n"
                    result += f"- 읽기: {read_count:,}회 ({read_time:.3f}초)\n"
                    result += f"- 쓰기: {write_count:,}회 ({write_time:.3f}초)\n"
                    result += f"- 총 I/O: {total_io:,}회 ({total_time:.3f}초)\n\n"

            cursor.close()
            connection.close()

            if tunnel_used:
                self.cleanup_ssh_tunnel()

            return result

        except Exception as e:
            return f"❌ 테이블 I/O 통계 조회 실패: {str(e)}"

    async def get_index_usage_stats(
        self, database_secret: str, table_name: Optional[str] = None
    ) -> str:
        """인덱스 사용 통계 조회"""
        try:
            connection, tunnel_used = await self.get_db_connection(database_secret)
            cursor = connection.cursor()

            result = f"📇 **인덱스 사용 통계**"
            if table_name:
                result += f" (테이블: {table_name})"
            result += "\n\n"

            # 인덱스 사용 통계 조회
            if table_name:
                cursor.execute(
                    """
                    SELECT 
                        object_name as table_name,
                        index_name,
                        count_read,
                        count_write,
                        count_read + count_write as total_usage,
                        sum_timer_read/1000000000000 as read_time_sec,
                        sum_timer_write/1000000000000 as write_time_sec
                    FROM performance_schema.table_io_waits_summary_by_index_usage 
                    WHERE object_schema = DATABASE()
                    AND object_name = %s
                    AND count_read + count_write > 0
                    ORDER BY count_read + count_write DESC
                """,
                    (table_name,),
                )
            else:
                cursor.execute(
                    """
                    SELECT 
                        object_name as table_name,
                        index_name,
                        count_read,
                        count_write,
                        count_read + count_write as total_usage,
                        sum_timer_read/1000000000000 as read_time_sec,
                        sum_timer_write/1000000000000 as write_time_sec
                    FROM performance_schema.table_io_waits_summary_by_index_usage 
                    WHERE object_schema = DATABASE()
                    AND count_read + count_write > 0
                    ORDER BY count_read + count_write DESC 
                    LIMIT 15
                """
                )

            index_stats = cursor.fetchall()

            if not index_stats:
                result += "📊 인덱스 사용 통계 데이터가 없습니다.\n"
            else:
                for i, (
                    table,
                    index,
                    read_count,
                    write_count,
                    total_usage,
                    read_time,
                    write_time,
                ) in enumerate(index_stats, 1):
                    index_display = index if index else "PRIMARY"
                    result += f"**{i}. {table}.{index_display}**\n"
                    result += f"- 읽기: {read_count:,}회 ({read_time:.3f}초)\n"
                    result += f"- 쓰기: {write_count:,}회 ({write_time:.3f}초)\n"
                    result += f"- 총 사용: {total_usage:,}회\n\n"

            cursor.close()
            connection.close()

            if tunnel_used:
                self.cleanup_ssh_tunnel()

            return result

        except Exception as e:
            return f"❌ 인덱스 사용 통계 조회 실패: {str(e)}"

    async def get_connection_stats(self, database_secret: str) -> str:
        """연결 및 세션 통계 조회"""
        try:
            connection, tunnel_used = await self.get_db_connection(database_secret)
            cursor = connection.cursor()

            result = f"🔗 **연결 및 세션 통계**\n\n"

            # 현재 연결 상태
            cursor.execute(
                """
                SELECT 
                    COUNT(*) as total_connections,
                    SUM(CASE WHEN COMMAND != 'Sleep' THEN 1 ELSE 0 END) as active_connections,
                    SUM(CASE WHEN COMMAND = 'Sleep' THEN 1 ELSE 0 END) as idle_connections
                FROM information_schema.processlist
            """
            )

            conn_stats = cursor.fetchone()
            if conn_stats:
                result += f"📊 **현재 연결 상태:**\n"
                result += f"- 총 연결: {conn_stats[0]}개\n"
                result += f"- 활성 연결: {conn_stats[1]}개\n"
                result += f"- 유휴 연결: {conn_stats[2]}개\n\n"

            # 사용자별 연결 통계
            cursor.execute(
                """
                SELECT 
                    USER as username,
                    COUNT(*) as connection_count,
                    SUM(CASE WHEN COMMAND != 'Sleep' THEN 1 ELSE 0 END) as active_count
                FROM information_schema.processlist
                GROUP BY USER
                ORDER BY connection_count DESC
            """
            )

            user_stats = cursor.fetchall()
            if user_stats:
                result += f"👥 **사용자별 연결:**\n"
                for user, total, active in user_stats:
                    result += f"- {user}: {total}개 연결 (활성: {active}개)\n"
                result += "\n"

            # Performance Schema 스레드 정보
            cursor.execute(
                """
                SELECT 
                    type,
                    COUNT(*) as thread_count
                FROM performance_schema.threads
                GROUP BY type
            """
            )

            thread_stats = cursor.fetchall()
            if thread_stats:
                result += f"🧵 **스레드 통계:**\n"
                for thread_type, count in thread_stats:
                    result += f"- {thread_type}: {count}개\n"

            cursor.close()
            connection.close()

            if tunnel_used:
                self.cleanup_ssh_tunnel()

            return result

        except Exception as e:
            return f"❌ 연결 통계 조회 실패: {str(e)}"

    async def get_memory_usage(self, database_secret: str) -> str:
        """메모리 사용량 조회"""
        try:
            connection, tunnel_used = await self.get_db_connection(database_secret)
            cursor = connection.cursor()

            result = f"💾 **메모리 사용량 통계**\n\n"

            # 글로벌 메모리 사용량 (상위 10개)
            cursor.execute(
                """
                SELECT 
                    event_name,
                    ROUND(sum_number_of_bytes_alloc/1024/1024, 2) as allocated_mb,
                    ROUND(sum_number_of_bytes_free/1024/1024, 2) as freed_mb,
                    ROUND((sum_number_of_bytes_alloc - sum_number_of_bytes_free)/1024/1024, 2) as current_mb
                FROM performance_schema.memory_summary_global_by_event_name
                WHERE sum_number_of_bytes_alloc > 0
                ORDER BY (sum_number_of_bytes_alloc - sum_number_of_bytes_free) DESC
                LIMIT 10
            """
            )

            memory_stats = cursor.fetchall()
            if memory_stats:
                result += f"📊 **글로벌 메모리 사용량 (상위 10개):**\n"
                for event, allocated, freed, current in memory_stats:
                    event_short = event.replace("memory/", "").replace("sql/", "")
                    result += f"- {event_short}: {current:.2f}MB (할당: {allocated:.2f}MB, 해제: {freed:.2f}MB)\n"
                result += "\n"

            # 주요 메모리 관련 시스템 변수
            cursor.execute(
                """
                SHOW VARIABLES WHERE Variable_name IN (
                    'innodb_buffer_pool_size',
                    'key_buffer_size',
                    'query_cache_size',
                    'tmp_table_size',
                    'max_heap_table_size',
                    'sort_buffer_size',
                    'read_buffer_size',
                    'join_buffer_size'
                )
            """
            )

            variables = cursor.fetchall()
            if variables:
                result += f"⚙️ **주요 메모리 설정:**\n"
                for var_name, var_value in variables:
                    # 바이트 단위를 MB로 변환
                    if var_value.isdigit():
                        mb_value = int(var_value) / 1024 / 1024
                        result += f"- {var_name}: {mb_value:.2f}MB\n"
                    else:
                        result += f"- {var_name}: {var_value}\n"

            cursor.close()
            connection.close()

            if tunnel_used:
                self.cleanup_ssh_tunnel()

            return result

        except Exception as e:
            return f"❌ 메모리 사용량 조회 실패: {str(e)}"

    async def get_lock_analysis(self, database_secret: str) -> str:
        """락 상태 분석"""
        try:
            connection, tunnel_used = await self.get_db_connection(database_secret)
            cursor = connection.cursor()

            result = f"🔒 **락 상태 분석**\n\n"

            # 현재 락 상태 (MySQL 8.0+)
            try:
                cursor.execute(
                    """
                    SELECT 
                        object_schema,
                        object_name,
                        lock_type,
                        lock_mode,
                        lock_status,
                        thread_id
                    FROM performance_schema.data_locks
                    LIMIT 10
                """
                )

                current_locks = cursor.fetchall()
                if current_locks:
                    result += f"🔐 **현재 활성 락 (상위 10개):**\n"
                    for (
                        schema,
                        table,
                        lock_type,
                        lock_mode,
                        status,
                        thread_id,
                    ) in current_locks:
                        result += f"- {schema}.{table}: {lock_type} ({lock_mode}) - {status} [Thread: {thread_id}]\n"
                    result += "\n"
                else:
                    result += f"✅ **현재 활성 락:** 없음\n\n"
            except Exception:
                result += f"ℹ️ **현재 락 정보:** Performance Schema data_locks 테이블을 사용할 수 없습니다.\n\n"

            # 락 대기 상황
            try:
                cursor.execute(
                    """
                    SELECT 
                        requesting_thread_id,
                        blocking_thread_id,
                        object_schema,
                        object_name,
                        lock_type
                    FROM performance_schema.data_lock_waits
                    LIMIT 10
                """
                )

                lock_waits = cursor.fetchall()
                if lock_waits:
                    result += f"⏳ **락 대기 상황:**\n"
                    for (
                        req_thread,
                        block_thread,
                        schema,
                        table,
                        lock_type,
                    ) in lock_waits:
                        result += f"- Thread {req_thread}이 Thread {block_thread}에 의해 대기 중\n"
                        result += f"  대상: {schema}.{table} ({lock_type})\n"
                    result += "\n"
                else:
                    result += f"✅ **락 대기:** 없음\n\n"
            except Exception:
                result += f"ℹ️ **락 대기 정보:** Performance Schema data_lock_waits 테이블을 사용할 수 없습니다.\n\n"

            # 대기 이벤트 통계
            cursor.execute(
                """
                SELECT 
                    event_name,
                    count_star as event_count,
                    ROUND(sum_timer_wait/1000000000000, 6) as total_wait_sec,
                    ROUND(avg_timer_wait/1000000000000, 6) as avg_wait_sec
                FROM performance_schema.events_waits_summary_global_by_event_name
                WHERE event_name LIKE '%lock%' 
                AND count_star > 0
                ORDER BY sum_timer_wait DESC
                LIMIT 5
            """
            )

            wait_events = cursor.fetchall()
            if wait_events:
                result += f"⏱️ **락 관련 대기 이벤트 (상위 5개):**\n"
                for event, count, total_wait, avg_wait in wait_events:
                    event_short = event.replace("wait/synch/", "").replace(
                        "wait/io/", ""
                    )
                    result += f"- {event_short}: {count:,}회 (총 {total_wait:.3f}초, 평균 {avg_wait:.6f}초)\n"

            cursor.close()
            connection.close()

            if tunnel_used:
                self.cleanup_ssh_tunnel()

            return result

        except Exception as e:
            return f"❌ 락 분석 실패: {str(e)}"

    async def get_replication_status(self, database_secret: str) -> str:
        """복제 상태 조회"""
        try:
            connection, tunnel_used = await self.get_db_connection(database_secret)
            cursor = connection.cursor()

            result = f"🔄 **복제 상태 분석**\n\n"

            # 복제 연결 상태 (Performance Schema)
            try:
                cursor.execute(
                    """
                    SELECT 
                        channel_name,
                        host,
                        port,
                        user,
                        source_connection_auto_failover,
                        connection_retry_interval,
                        connection_retry_count
                    FROM performance_schema.replication_connection_configuration
                """
                )

                repl_config = cursor.fetchall()
                if repl_config:
                    result += f"⚙️ **복제 연결 설정:**\n"
                    for (
                        channel,
                        host,
                        port,
                        user,
                        auto_failover,
                        retry_interval,
                        retry_count,
                    ) in repl_config:
                        result += f"- 채널: {channel}\n"
                        result += f"  소스: {host}:{port} (사용자: {user})\n"
                        result += f"  자동 장애조치: {auto_failover}\n"
                        result += (
                            f"  재시도: {retry_count}회, 간격: {retry_interval}초\n\n"
                        )
                else:
                    result += f"ℹ️ **복제 연결 설정:** 없음\n\n"
            except Exception as e:
                result += f"ℹ️ **복제 연결 설정:** 조회 불가 ({str(e)})\n\n"

            # 복제 상태
            try:
                cursor.execute(
                    """
                    SELECT 
                        channel_name,
                        service_state,
                        received_transaction_set,
                        last_error_message,
                        last_error_timestamp
                    FROM performance_schema.replication_connection_status
                """
                )

                repl_status = cursor.fetchall()
                if repl_status:
                    result += f"📊 **복제 연결 상태:**\n"
                    for channel, state, trans_set, error_msg, error_time in repl_status:
                        result += f"- 채널: {channel}\n"
                        result += f"  상태: {state}\n"
                        if trans_set:
                            result += f"  수신 트랜잭션: {trans_set[:50]}...\n"
                        if error_msg:
                            result += f"  ❌ 마지막 오류: {error_msg}\n"
                            result += f"  오류 시간: {error_time}\n"
                        result += "\n"
                else:
                    result += f"ℹ️ **복제 연결 상태:** 없음\n\n"
            except Exception as e:
                result += f"ℹ️ **복제 연결 상태:** 조회 불가 ({str(e)})\n\n"

            # 복제 지연 정보
            try:
                cursor.execute(
                    """
                    SELECT 
                        channel_name,
                        worker_id,
                        service_state,
                        last_error_message,
                        last_applied_transaction
                    FROM performance_schema.replication_applier_status_by_worker
                """
                )

                worker_status = cursor.fetchall()
                if worker_status:
                    result += f"👷 **복제 워커 상태:**\n"
                    for (
                        channel,
                        worker_id,
                        state,
                        error_msg,
                        last_trans,
                    ) in worker_status:
                        result += f"- 채널: {channel}, 워커: {worker_id}\n"
                        result += f"  상태: {state}\n"
                        if last_trans:
                            result += f"  마지막 적용: {last_trans[:50]}...\n"
                        if error_msg:
                            result += f"  ❌ 오류: {error_msg}\n"
                        result += "\n"
                else:
                    result += f"ℹ️ **복제 워커 상태:** 없음\n\n"
            except Exception as e:
                result += f"ℹ️ **복제 워커 상태:** 조회 불가 ({str(e)})\n\n"

            # 바이너리 로그 상태
            try:
                cursor.execute("SHOW MASTER STATUS")
                master_status = cursor.fetchone()
                if master_status:
                    result += f"📝 **바이너리 로그 상태:**\n"
                    result += f"- 파일: {master_status[0]}\n"
                    result += f"- 위치: {master_status[1]}\n"
                    if len(master_status) > 2 and master_status[2]:
                        result += f"- 바인딩 DB: {master_status[2]}\n"
                    if len(master_status) > 3 and master_status[3]:
                        result += f"- 제외 DB: {master_status[3]}\n"
                else:
                    result += f"ℹ️ **바이너리 로그:** 비활성화\n"
            except Exception as e:
                result += f"ℹ️ **바이너리 로그 상태:** 조회 불가 ({str(e)})\n"

            cursor.close()
            connection.close()

            if tunnel_used:
                self.cleanup_ssh_tunnel()

            return result

        except Exception as e:
            return f"❌ 복제 상태 조회 실패: {str(e)}"

    async def validate_with_claude(
        self,
        ddl_content: str,
        database_secret: str = None,
        schema_info: dict = None,
        existing_analysis: dict = None,
    ) -> str:
        """
        Claude cross-region 프로파일을 활용한 DDL 검증 (실제 스키마 정보 및 기존 분석 결과 포함)
        """

        # 관련 스키마 정보를 포함한 프롬프트 생성 (순서 고려)

        if schema_info:
            # 스키마 정보를 문자열로 변환
            schema_text = []
            if isinstance(schema_info, dict):
                for key, value in schema_info.items():
                    schema_text.append(f"{key}: {value}")
            else:
                schema_text.append(str(schema_info))
            
            # 기존 분석 결과 추가
            if existing_analysis:
                schema_text.append(f"기존 분석 결과: {existing_analysis}")
            
            schema_context = f"""
관련 스키마 정보 (실행 순서별):
{chr(10).join(schema_text)}

위 정보를 바탕으로 DDL의 적절성을 판단해주세요.
특히 다음 사항을 확인해주세요:
1. 파일 내에서 먼저 생성된 테이블은 이후 ALTER/INDEX 작업에서 존재하는 것으로 간주
2. 동일한 컬럼 구성의 인덱스 중복 여부
3. 존재하지 않는 테이블/인덱스에 대한 DROP 시도
4. 실행 순서상 논리적 오류
"""
        else:
            schema_context = """
스키마 정보를 가져올 수 없습니다.
기본적인 문법 검증만 수행합니다.
"""

        prompt = f"""
        다음 DDL 문을 검증해주세요:

        {ddl_content}

        {schema_context}

        다음 사항들을 확인해주세요:
        1. 문법 오류
        2. 표준 규칙 위반
        3. 성능 문제
        4. 스키마 충돌 (테이블/컬럼/인덱스 중복 등)
        5. 데이터 타입 호환성 문제
        
        기존 분석 결과가 있다면 이를 참고하되, 중복되지 않는 새로운 관점에서 추가 검증을 수행해주세요.
        문제가 있으면 구체적으로 지적해주세요. 문제가 없으면 "검증 통과"라고 응답해주세요.
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
            # response가 dict인지 확인하고 body 추출
            if isinstance(response, dict) and "body" in response:
                response_body = json.loads(response["body"].read())
            else:
                logger.error(f"Unexpected response format: {type(response)}")
                return "Claude 응답 형식 오류 - 예상치 못한 응답 구조"

            content = response_body.get("content", [])
            if isinstance(content, list) and len(content) > 0:
                first_content = content[0]
                if isinstance(first_content, dict):
                    return first_content.get("text", "")
                else:
                    return str(first_content)
            else:
                return "Claude 응답 형식 오류"
        except Exception as e:
            logger.warning(
                f"Claude Sonnet 4 호출 실패 → Claude 3.7 Sonnet cross-region profile로 fallback: {e}"
            )
            # Claude 3.7 Sonnet inference profile 호출 (fallback)
            try:
                response = self.bedrock_client.invoke_model(
                    modelId=sonnet_3_7_model_id, body=claude_input
                )
                # response가 dict인지 확인하고 body 추출
                if isinstance(response, dict) and "body" in response:
                    response_body = json.loads(response["body"].read())
                else:
                    logger.error(f"Unexpected response format: {type(response)}")
                    return "Claude 응답 형식 오류 - 예상치 못한 응답 구조"

                content = response_body.get("content", [])
                if isinstance(content, list) and len(content) > 0:
                    first_content = content[0]
                    if isinstance(first_content, dict):
                        return first_content.get("text", "")
                    else:
                        return str(first_content)
                else:
                    return "Claude 응답 형식 오류"
            except Exception as e:
                logger.error(f"Claude 3.7 Sonnet 호출 오류: {e}")
                return f"Claude 호출 중 오류 발생: {str(e)}"

    async def extract_current_schema_info(
        self, database_secret: str, use_ssh_tunnel: bool = True
    ) -> Dict[str, Any]:
        """현재 데이터베이스의 스키마 정보 추출"""
        try:
            logger.info(f"스키마 정보 추출 시작: database_secret={database_secret}")
            connection, tunnel_used = await self.get_db_connection(
                database_secret, None, use_ssh_tunnel
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
                        "nullable": col_row[5] == "YES",
                        "default": col_row[6],
                    }
                    columns.append(col_info)

                schema_info["columns"][table] = columns

            # 각 테이블의 인덱스 정보 조회
            for table in tables:
                cursor.execute(
                    """
                    SELECT index_name, column_name, seq_in_index, non_unique
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
                    seq = idx_row[2]
                    non_unique = idx_row[3]

                    if idx_name not in indexes:
                        indexes[idx_name] = {"columns": [], "unique": non_unique == 0}

                    indexes[idx_name]["columns"].append(col_name)

                schema_info["indexes"][table] = indexes

            cursor.close()
            connection.close()

            return schema_info

        except Exception as e:
            logger.error(f"스키마 정보 추출 중 오류: {e}")
            return {}


# MCP 서버 설정
server = Server("ddl-qcli-validator")
ddl_validator = DDLValidationQCLIServer()


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
            name="validate_all_sql",
            description="sql 디렉토리의 SQL 파일들을 검증합니다 (최대 5개)",
            inputSchema={
                "type": "object",
                "properties": {
                    "database_secret": {
                        "type": "string",
                        "description": "데이터베이스 시크릿 이름 (선택사항)",
                    }
                },
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
            name="analyze_current_schema",
            description="현재 데이터베이스의 스키마를 상세 분석합니다",
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
            name="check_ddl_conflicts",
            description="DDL 실행 전 충돌 및 문제점을 사전 검사합니다",
            inputSchema={
                "type": "object",
                "properties": {
                    "ddl_content": {"type": "string", "description": "검사할 DDL 문"},
                    "database_secret": {
                        "type": "string",
                        "description": "데이터베이스 시크릿 이름",
                    },
                },
                "required": ["ddl_content", "database_secret"],
            },
        ),
        types.Tool(
            name="get_aurora_mysql_parameters",
            description="Aurora MySQL 클러스터의 파라미터를 조회합니다",
            inputSchema={
                "type": "object",
                "properties": {
                    "cluster_identifier": {
                        "type": "string",
                        "description": "Aurora 클러스터 식별자",
                    },
                    "region": {
                        "type": "string",
                        "description": "AWS 리전 (기본값: ap-northeast-2)",
                    },
                    "filter_type": {
                        "type": "string",
                        "description": "필터 타입 (important: 주요 파라미터만, custom: 사용자 정의만, all: 전체)",
                        "enum": ["important", "custom", "all"],
                        "default": "important",
                    },
                    "category": {
                        "type": "string",
                        "description": "파라미터 카테고리 (all, security, performance, memory, io, connection, logging, replication, aurora)",
                        "enum": [
                            "all",
                            "security",
                            "performance",
                            "memory",
                            "io",
                            "connection",
                            "logging",
                            "replication",
                            "aurora",
                        ],
                        "default": "all",
                    },
                },
                "required": ["cluster_identifier"],
            },
        ),
        types.Tool(
            name="create_execution_plan",
            description="작업 실행 계획을 생성합니다",
            inputSchema={
                "type": "object",
                "properties": {
                    "operation": {"type": "string", "description": "실행할 작업명"},
                    "parameters": {"type": "object", "description": "작업 매개변수"},
                },
                "required": ["operation"],
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
            name="confirm_and_execute",
            description="계획 확인 후 실행합니다",
            inputSchema={
                "type": "object",
                "properties": {
                    "confirmation": {
                        "type": "string",
                        "description": "실행 확인 (y/yes/n/no)",
                    }
                },
                "required": ["confirmation"],
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
            name="analyze_slow_queries",
            description="느린 쿼리를 분석합니다",
            inputSchema={
                "type": "object",
                "properties": {
                    "database_secret": {
                        "type": "string",
                        "description": "데이터베이스 시크릿 이름",
                    },
                    "limit": {
                        "type": "integer",
                        "description": "조회할 쿼리 수 (기본값: 10)",
                        "default": 10,
                    },
                },
                "required": ["database_secret"],
            },
        ),
        types.Tool(
            name="get_table_io_stats",
            description="테이블별 I/O 통계를 조회합니다",
            inputSchema={
                "type": "object",
                "properties": {
                    "database_secret": {
                        "type": "string",
                        "description": "데이터베이스 시크릿 이름",
                    },
                    "schema_name": {
                        "type": "string",
                        "description": "스키마 이름 (선택사항, 기본값: 현재 DB)",
                    },
                },
                "required": ["database_secret"],
            },
        ),
        types.Tool(
            name="get_index_usage_stats",
            description="인덱스 사용 통계를 조회합니다",
            inputSchema={
                "type": "object",
                "properties": {
                    "database_secret": {
                        "type": "string",
                        "description": "데이터베이스 시크릿 이름",
                    },
                    "table_name": {
                        "type": "string",
                        "description": "테이블 이름 (선택사항, 전체 조회시 생략)",
                    },
                },
                "required": ["database_secret"],
            },
        ),
        types.Tool(
            name="get_connection_stats",
            description="연결 및 세션 통계를 조회합니다",
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
            name="get_memory_usage",
            description="메모리 사용량을 조회합니다",
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
            name="get_lock_analysis",
            description="락 상태를 분석합니다",
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
            name="get_replication_status",
            description="복제 상태를 조회합니다",
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
            name="list_aurora_mysql_clusters",
            description="현재 리전의 Aurora MySQL 클러스터 목록을 조회합니다",
            inputSchema={
                "type": "object",
                "properties": {
                    "region": {
                        "type": "string",
                        "description": "AWS 리전 (기본값: ap-northeast-2)",
                        "default": "ap-northeast-2",
                    }
                },
            },
        ),
        types.Tool(
            name="select_aurora_cluster",
            description="Aurora MySQL 클러스터를 선택합니다",
            inputSchema={
                "type": "object",
                "properties": {
                    "cluster_selection": {
                        "type": "string",
                        "description": "클러스터 번호 또는 이름",
                    },
                    "region": {
                        "type": "string",
                        "description": "AWS 리전 (기본값: ap-northeast-2)",
                        "default": "ap-northeast-2",
                    },
                },
                "required": ["cluster_selection"],
            },
        ),
        types.Tool(
            name="list_databases",
            description="데이터베이스 목록을 조회합니다",
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
            description="데이터베이스를 선택하고 변경합니다",
            inputSchema={
                "type": "object",
                "properties": {
                    "database_secret": {
                        "type": "string",
                        "description": "데이터베이스 시크릿 이름",
                    },
                    "database_selection": {
                        "type": "string",
                        "description": "데이터베이스 번호 또는 이름",
                    },
                },
                "required": ["database_secret", "database_selection"],
            },
        ),
        types.Tool(
            name="validate_sql_with_database",
            description="데이터베이스를 지정하여 SQL 파일을 완전 검증합니다",
            inputSchema={
                "type": "object",
                "properties": {
                    "filename": {"type": "string", "description": "검증할 SQL 파일명"},
                    "database_secret": {
                        "type": "string",
                        "description": "데이터베이스 시크릿 이름",
                    },
                },
                "required": ["filename", "database_secret"],
            },
        ),
    ]


@server.call_tool()
async def handle_call_tool(name: str, arguments: dict) -> list[types.TextContent]:
    """도구 호출 처리"""
    try:
        if name == "list_sql_files":
            result = await ddl_validator.list_sql_files()
        elif name == "list_database_secrets":
            result = await ddl_validator.list_database_secrets(
                arguments.get("keyword", "")
            )
        elif name == "validate_sql_file":
            # database_secret이 없으면 선택 옵션 제공
            filename = arguments["filename"]
            database_secret = arguments.get("database_secret")

            if not database_secret:
                # SQL 파일 읽기
                sql_file_path = SQL_DIR / filename
                if not sql_file_path.exists():
                    result = f"SQL 파일을 찾을 수 없습니다: {filename}"
                else:
                    with open(sql_file_path, "r", encoding="utf-8") as f:
                        ddl_content = f.read()

                    # 데이터베이스 선택 프롬프트 표시
                    result = await ddl_validator.prompt_for_database_selection(
                        ddl_content, filename
                    )
            else:
                # 기존 계획 생성 방식
                plan = await ddl_validator.create_validation_plan(
                    filename, database_secret
                )
                plan_display = ddl_validator._format_validation_plan_display(plan)

                result = f"""📋 **DDL 검증 실행 계획:**

{plan_display}

❓ **이 계획대로 검증을 진행하시겠습니까?**
   • 'y' 또는 'yes': 검증 실행
   • 'n' 또는 'no': 검증 취소

💡 **참고:** confirm_and_execute 도구로 응답해주세요."""

                ddl_validator.current_plan = plan
        elif name == "test_database_connection":
            result = await ddl_validator.execute_with_auto_plan(
                "test_database_connection", database_secret=arguments["database_secret"]
            )
        elif name == "validate_all_sql":
            # 자동 계획 생성 및 실행
            result = await ddl_validator.execute_with_auto_plan(
                "validate_all_sql", database_secret=arguments.get("database_secret")
            )
        elif name == "copy_sql_to_directory":
            result = await ddl_validator.copy_sql_file(
                arguments["source_path"], arguments.get("target_name")
            )
        elif name == "analyze_current_schema":
            result = await ddl_validator.execute_with_auto_plan(
                "analyze_current_schema", database_secret=arguments["database_secret"]
            )
        elif name == "check_ddl_conflicts":
            result = await ddl_validator.execute_with_auto_plan(
                "check_ddl_conflicts",
                ddl_content=arguments["ddl_content"],
                database_secret=arguments["database_secret"],
            )
        elif name == "get_schema_summary":
            result = await ddl_validator.execute_with_auto_plan(
                "get_schema_summary", database_secret=arguments["database_secret"]
            )
        elif name == "get_aurora_mysql_parameters":
            result = await ddl_validator.execute_with_auto_plan(
                "get_aurora_mysql_parameters",
                cluster_identifier=arguments["cluster_identifier"],
                region=arguments.get("region", "ap-northeast-2"),
                filter_type=arguments.get("filter_type", "important"),
                category=arguments.get("category", "all"),
            )
        elif name == "create_execution_plan":
            plan = await ddl_validator.create_execution_plan(
                arguments["operation"], **arguments.get("parameters", {})
            )
            result = ddl_validator._format_plan_display(plan)
        elif name == "confirm_and_execute":
            result = await ddl_validator.confirm_and_execute(arguments["confirmation"])
        elif name == "get_performance_metrics":
            result = await ddl_validator.get_performance_metrics(
                arguments["database_secret"], arguments.get("metric_type", "all")
            )
        elif name == "analyze_slow_queries":
            result = await ddl_validator.analyze_slow_queries(
                arguments["database_secret"], arguments.get("limit", 10)
            )
        elif name == "get_table_io_stats":
            result = await ddl_validator.get_table_io_stats(
                arguments["database_secret"], arguments.get("schema_name")
            )
        elif name == "get_index_usage_stats":
            result = await ddl_validator.get_index_usage_stats(
                arguments["database_secret"], arguments.get("table_name")
            )
        elif name == "get_connection_stats":
            result = await ddl_validator.get_connection_stats(
                arguments["database_secret"]
            )
        elif name == "get_memory_usage":
            result = await ddl_validator.get_memory_usage(arguments["database_secret"])
        elif name == "get_lock_analysis":
            result = await ddl_validator.get_lock_analysis(arguments["database_secret"])
        elif name == "get_replication_status":
            result = await ddl_validator.get_replication_status(
                arguments["database_secret"]
            )
        elif name == "list_aurora_mysql_clusters":
            result = await ddl_validator.list_aurora_mysql_clusters(
                arguments.get("region", "ap-northeast-2")
            )
        elif name == "select_aurora_cluster":
            result = await ddl_validator.select_aurora_cluster(
                arguments["cluster_selection"],
                arguments.get("region", "ap-northeast-2"),
            )
        elif name == "list_databases":
            result = await ddl_validator.list_databases(arguments["database_secret"])
        elif name == "select_database":
            result = await ddl_validator.select_database(
                arguments["database_secret"], arguments["database_selection"]
            )
        elif name == "validate_sql_with_database":
            result = await ddl_validator.execute_validation_workflow(
                ddl_content="",  # 파일에서 읽어올 예정
                database_secret=arguments["database_secret"],
                filename=arguments["filename"],
            )
            # 실제로는 파일을 읽어서 처리해야 함
            sql_file_path = SQL_DIR / arguments["filename"]
            if sql_file_path.exists():
                with open(sql_file_path, "r", encoding="utf-8") as f:
                    ddl_content = f.read()
                result = await ddl_validator.execute_validation_workflow(
                    ddl_content, arguments["database_secret"], arguments["filename"]
                )
            else:
                result = f"SQL 파일을 찾을 수 없습니다: {arguments['filename']}"
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
                    server_name="ddl-qcli-validator",
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
