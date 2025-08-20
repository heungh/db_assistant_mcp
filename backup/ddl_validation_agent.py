#!/usr/bin/env python3
"""
LangGraph 기반 DDL 검증 에이전트
"""

import streamlit as st
import streamlit.components.v1
import boto3
import json
import re

try:
    import mysql.connector
except ImportError:
    mysql = None
from datetime import datetime
from typing import Dict, Any, List, TypedDict
from langgraph.graph import StateGraph, END
import logging

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# 상태 정의
class ValidationState(TypedDict):
    ddl_content: str
    ddl_type: str
    syntax_valid: bool
    syntax_errors: List[str]
    standard_compliant: bool
    standard_issues: List[str]
    db_connection_info: Dict[str, Any]
    schema_info: Dict[str, Any]
    performance_issues: List[str]
    data_safety_issues: List[str]
    final_result: Dict[str, Any]
    recommendations: List[str]


# db-admin.py에서 가져온 함수들
def get_secret(secret_name):
    """Secrets Manager에서 DB 연결 정보 가져오기"""
    session = boto3.session.Session()
    client = session.client(
        service_name="secretsmanager",
        region_name="ap-northeast-2",
        verify=False,  # SSL 검증 우회
    )
    get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    secret = get_secret_value_response["SecretString"]
    return json.loads(secret)


def connect_to_db(secret_name):
    """Secret Manager 정보로 DB 연결 (외부 SSH 터널 사용)"""
    import subprocess
    import time

    secret_values = get_secret(secret_name)
    print("secret_values", secret_values)

    # SSH 설정을 세션 상태에서 가져오기
    ssh_config = st.session_state.get("ssh_config", {})
    use_ssh_tunnel = ssh_config.get("use_ssh_tunnel", True)

    # DB 연결 정보
    db_user = secret_values["username"]
    db_password = secret_values["password"]
    database = secret_values["dbname"]

    if use_ssh_tunnel:
        # 외부 SSH 터널 스크립트 실행
        try:
            script_path = "/Users/heungh/Documents/SA/05.Project/01.Infra-Assistant/01.DB-Assistant/ssh_tunnel.sh"
            result = subprocess.run([script_path], capture_output=True, text=True)
            print("SSH 터널 스크립트 실행 결과:", result.stdout)
            if result.stderr:
                print("SSH 터널 오류:", result.stderr)

            # 터널 안정화 대기
            time.sleep(3)

            # SSH 터널을 통한 연결 (로컬 포트 3307 사용)
            connection = mysql.connector.connect(
                host="127.0.0.1",
                port=3307,  # SSH 터널 포트
                user=db_user,
                password=db_password,
                database=database,
                connection_timeout=30,
                autocommit=True,
            )
            print("MySQL 연결 성공 (SSH 터널 사용)")

        except Exception as e:
            print(f"SSH 터널 연결 실패: {e}")
            raise e
    else:
        # 직접 연결
        db_host = secret_values["host"]
        db_port = int(secret_values["port"])

        connection = mysql.connector.connect(
            host=db_host,
            user=db_user,
            password=db_password,
            port=db_port,
            database=database,
            connection_timeout=30,
            autocommit=True,
        )
        print("MySQL 직접 연결 성공")

    return connection


def get_secrets_by_keyword(keyword):
    """키워드로 Secret 목록 가져오기"""
    secrets_manager = boto3.client(
        service_name="secretsmanager",
        region_name="ap-northeast-2",
        verify=False,  # SSL 검증 우회
    )
    response = secrets_manager.list_secrets(
        Filters=[{"Key": "name", "Values": [keyword]}]
    )
    return [secret["Name"] for secret in response["SecretList"]]


def get_database_info(secret_name):
    """데이터베이스 스키마 정보 가져오기"""
    connection = connect_to_db(secret_name)

    cursor = connection.cursor()
    cursor.execute("SELECT DATABASE();")
    database_name = cursor.fetchone()

    # 테이블 정보 가져오기
    cursor.execute(
        f""" SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, COLUMN_COMMENT 
             FROM INFORMATION_SCHEMA.COLUMNS 
             WHERE TABLE_SCHEMA = '{database_name[0]}' 
             ORDER BY TABLE_NAME, ORDINAL_POSITION """
    )
    table_info = cursor.fetchall()

    # 테이블 정보를 문자열로 변환
    table_info_str = "Current Database has following tables and columns: \n"
    current_table = None
    for row in table_info:
        table_name, column_name, data_type, column_comment = row
        if table_name != current_table:
            if current_table:
                table_info_str += "\n"
            table_info_str += f"{table_name} with columns:\n"
            current_table = table_name
        table_info_str += f"{column_name} {data_type} {column_comment}\n"

    # 인덱스 정보 가져오기
    cursor.execute(
        f"""SELECT TABLE_NAME, INDEX_NAME, COLUMN_NAME, NON_UNIQUE, INDEX_COMMENT 
            FROM INFORMATION_SCHEMA.STATISTICS 
            WHERE TABLE_SCHEMA = '{database_name[0]}'
            ORDER BY TABLE_NAME, INDEX_NAME"""
    )
    index_info = cursor.fetchall()

    index_info_str = "\nIndexes:\n"
    current_table = None
    for row in index_info:
        table_name, index_name, column_name, non_unique, index_comment = row
        if table_name != current_table:
            if current_table:
                index_info_str += "\n"
            index_info_str += f"{table_name}:\n"
            current_table = table_name
        index_info_str += f"  {index_name} ({column_name}) {'' if non_unique else 'UNIQUE'} {index_comment}\n"

    cursor.close()

    # SSH 터널이 있으면 터널도 닫기
    if hasattr(connection, "_ssh_tunnel"):
        connection._ssh_tunnel.stop()

    connection.close()

    return table_info_str + index_info_str


class DDLValidationAgent:
    def __init__(self):
        self.bedrock_client = boto3.client(
            "bedrock-runtime", region_name="us-east-1", verify=False  # SSL 검증 우회
        )
        self.bedrock_agent_client = boto3.client(
            "bedrock-agent-runtime",
            region_name="us-east-1",
            verify=False,  # SSL 검증 우회
        )
        self.knowledge_base_id = "0WQUBRHVR8"

    def create_workflow(self):
        """워크플로우 생성"""
        workflow = StateGraph(ValidationState)

        # 노드 추가
        workflow.add_node("syntax_check", self.syntax_check_node)
        workflow.add_node("standard_check", self.standard_check_node)
        workflow.add_node("db_connection_test", self.db_connection_test_node)
        workflow.add_node("db_schema_check", self.db_schema_check_node)
        workflow.add_node("performance_check", self.performance_check_node)
        workflow.add_node("data_safety_check", self.data_safety_check_node)
        workflow.add_node("final_report", self.final_report_node)

        # 엣지 정의
        workflow.set_entry_point("syntax_check")

        workflow.add_conditional_edges(
            "syntax_check",
            self.should_continue_after_syntax,
            {"continue": "standard_check", "stop": "final_report"},
        )

        workflow.add_edge("standard_check", "db_connection_test")
        workflow.add_edge("db_connection_test", "db_schema_check")
        workflow.add_edge("db_schema_check", "performance_check")
        workflow.add_edge("performance_check", "data_safety_check")
        workflow.add_edge("data_safety_check", "final_report")
        workflow.add_edge("final_report", END)

        return workflow.compile()

    def syntax_check_node(self, state: ValidationState) -> ValidationState:
        """1단계: 문법 검증 노드"""
        st.write("🔍 **1단계: SQL 문법 검증 중...**")

        ddl_content = state["ddl_content"]
        ddl_type = self.extract_ddl_type(ddl_content)

        syntax_errors = []
        syntax_valid = True

        # 기본 문법 검증
        basic_checks = [
            (r";$", "세미콜론이 누락되었습니다."),
            (
                r"CREATE\s+TABLE\s+\w+\s*\(.*\)",
                "CREATE TABLE 구문이 올바르지 않습니다.",
            ),
            (r"VARCHAR\(\d+\)", "VARCHAR 타입에 길이가 지정되지 않았습니다."),
        ]

        for pattern, error_msg in basic_checks:
            if (
                ddl_type == "CREATE_TABLE"
                and "VARCHAR(" in ddl_content.upper()
                and not re.search(r"VARCHAR\(\d+\)", ddl_content, re.IGNORECASE)
            ):
                syntax_errors.append("VARCHAR 타입에 길이가 지정되지 않았습니다.")
                syntax_valid = False

            if not ddl_content.strip().endswith(";"):
                syntax_errors.append("세미콜론이 누락되었습니다.")
                syntax_valid = False

        # Claude를 사용한 고급 문법 검증
        try:
            claude_result = self.validate_syntax_with_claude(ddl_content)
            if "문법 오류" in claude_result or "syntax error" in claude_result.lower():
                syntax_valid = False
                syntax_errors.append("Claude 검증: " + claude_result)
        except Exception as e:
            logger.error(f"Claude 문법 검증 오류: {e}")

        state.update(
            {
                "ddl_type": ddl_type,
                "syntax_valid": syntax_valid,
                "syntax_errors": syntax_errors,
            }
        )

        if syntax_valid:
            st.success("✅ 문법 검증 통과")
        else:
            st.error(f"❌ 문법 오류 발견: {', '.join(syntax_errors)}")

        return state

    def standard_check_node(self, state: ValidationState) -> ValidationState:
        """2단계: 표준 규칙 검증 노드"""
        st.write("📋 **2단계: 스키마 표준 규칙 검증 중...**")

        ddl_content = state["ddl_content"]
        standard_issues = []

        # Knowledge Base에서 표준 규칙 검색
        try:
            search_query = f"DDL {state['ddl_type']} 스키마 표준 규칙 명명규칙"
            schema_standards = self.query_knowledge_base(search_query)

            if schema_standards:
                claude_result = self.validate_standards_with_claude(
                    ddl_content, schema_standards
                )

                # 표준 위반 사항 파싱
                if "부적절" in claude_result or "위반" in claude_result:
                    standard_issues = self.parse_standard_issues(claude_result)

        except Exception as e:
            logger.error(f"표준 검증 오류: {e}")
            standard_issues.append(f"표준 검증 중 오류 발생: {str(e)}")

        state.update(
            {
                "standard_compliant": len(standard_issues) == 0,
                "standard_issues": standard_issues,
            }
        )

        if len(standard_issues) == 0:
            st.success("✅ 표준 규칙 준수")
        else:
            st.warning(f"⚠️ 표준 규칙 위반: {len(standard_issues)}개 발견")
            for issue in standard_issues:
                st.write(f"  - {issue}")

        return state

    def db_connection_test_node(self, state: ValidationState) -> ValidationState:
        """2.5단계: 데이터베이스 연결 테스트 노드"""
        st.write("🔗 **2.5단계: 데이터베이스 연결 테스트 중...**")

        selected_db = st.session_state.get("selected_database", None)

        if not selected_db:
            st.warning("⚠️ 데이터베이스가 선택되지 않았습니다.")
            state.update({"db_connection_status": "not_selected"})
            return state

        try:
            # 연결 테스트
            connection = connect_to_db(selected_db)
            cursor = connection.cursor()

            # 기본 정보 확인
            cursor.execute("SELECT VERSION()")
            version = cursor.fetchone()[0]

            cursor.execute("SELECT DATABASE()")
            current_db = cursor.fetchone()[0]

            cursor.close()
            connection.close()

            st.success(f"✅ 데이터베이스 연결 성공")
            st.info(f"📊 연결된 DB: {current_db} (MySQL {version})")

            state.update(
                {
                    "db_connection_status": "success",
                    "db_info": {
                        "name": current_db,
                        "version": version,
                        "secret_name": selected_db,
                    },
                }
            )

        except Exception as e:
            logger.error(f"데이터베이스 연결 테스트 실패: {e}")
            st.error(f"❌ 데이터베이스 연결 실패: {str(e)}")
            state.update(
                {"db_connection_status": "failed", "db_connection_error": str(e)}
            )

        return state

    def db_schema_check_node(self, state: ValidationState) -> ValidationState:
        """3단계: 데이터베이스 스키마 검증 노드"""
        st.write("🗄️ **3단계: 데이터베이스 스키마 검증 중...**")

        selected_db = st.session_state.get("selected_database", None)

        if not selected_db:
            st.warning("⚠️ 데이터베이스가 선택되지 않았습니다.")
            state.update({"schema_info": {"warning": "DB 선택 안됨"}})
            return state

        try:
            if mysql is None:
                st.warning(
                    "⚠️ MySQL 커넥터가 설치되지 않아 데이터베이스 검증을 건너뜁니다."
                )
                state.update({"schema_info": {"warning": "MySQL 커넥터 없음"}})
                return state

            # 데이터베이스 연결 (직접 새로 연결)
            connection = connect_to_db(selected_db)
            cursor = connection.cursor()

            ddl_type = state["ddl_type"]
            ddl_content = state["ddl_content"]
            schema_issues = []

            if ddl_type == "CREATE_TABLE":
                schema_issues.extend(self.check_table_creation(cursor, ddl_content))
            elif ddl_type == "ALTER_TABLE":
                schema_issues.extend(self.check_table_alteration(cursor, ddl_content))
            elif ddl_type in ["CREATE_INDEX", "CREATE_UNIQUE_INDEX"]:
                schema_issues.extend(self.check_index_creation(cursor, ddl_content))
            elif ddl_type == "DROP":
                schema_issues.extend(self.check_drop_operation(cursor, ddl_content))

            cursor.close()
            connection.close()

            state.update({"schema_info": {"issues": schema_issues}})

            if len(schema_issues) == 0:
                st.success("✅ 스키마 검증 통과")
            else:
                st.error(f"❌ 스키마 문제 발견: {len(schema_issues)}개")
                for issue in schema_issues:
                    st.write(f"  - {issue}")

        except Exception as e:
            logger.error(f"데이터베이스 스키마 검증 오류: {e}")
            st.error(f"❌ 데이터베이스 연결 오류: {str(e)}")
            state.update({"schema_info": {"error": str(e)}})

        return state

    def performance_check_node(self, state: ValidationState) -> ValidationState:
        """4단계: 성능 검증 노드"""
        st.write("⚡ **4단계: 성능 영향 분석 중...**")

        ddl_content = state["ddl_content"]
        ddl_type = state["ddl_type"]
        performance_issues = []

        # 성능 관련 검증
        if ddl_type == "CREATE_TABLE":
            performance_issues.extend(self.analyze_table_performance(ddl_content))
        elif ddl_type == "ALTER_TABLE":
            performance_issues.extend(self.analyze_alter_performance(ddl_content))
        elif ddl_type in ["CREATE_INDEX", "CREATE_UNIQUE_INDEX"]:
            performance_issues.extend(self.analyze_index_performance(ddl_content))

        state.update({"performance_issues": performance_issues})

        if len(performance_issues) == 0:
            st.success("✅ 성능 이슈 없음")
        else:
            st.warning(f"⚠️ 성능 주의사항: {len(performance_issues)}개")
            for issue in performance_issues:
                st.write(f"  - {issue}")

        return state

    def data_safety_check_node(self, state: ValidationState) -> ValidationState:
        """5단계: 데이터 안전성 검증 노드"""
        st.write("🛡️ **5단계: 데이터 안전성 검증 중...**")

        ddl_content = state["ddl_content"]
        ddl_type = state["ddl_type"]
        safety_issues = []

        # 데이터 안전성 검증
        if ddl_type == "DROP":
            safety_issues.extend(self.check_drop_safety(ddl_content))
        elif ddl_type == "ALTER_TABLE":
            safety_issues.extend(self.check_alter_safety(ddl_content))

        state.update({"data_safety_issues": safety_issues})

        if len(safety_issues) == 0:
            st.success("✅ 데이터 안전성 확인")
        else:
            st.error(f"❌ 데이터 안전성 위험: {len(safety_issues)}개")
            for issue in safety_issues:
                st.write(f"  - {issue}")

        return state

    def final_report_node(self, state: ValidationState) -> ValidationState:
        """6단계: 최종 보고서 생성 노드"""
        st.write("📊 **6단계: 최종 검증 보고서 생성 중...**")

        # 전체 결과 종합
        total_issues = (
            len(state.get("syntax_errors", []))
            + len(state.get("standard_issues", []))
            + len(state.get("schema_info", {}).get("issues", []))
            + len(state.get("performance_issues", []))
            + len(state.get("data_safety_issues", []))
        )

        # 권장사항 생성
        recommendations = self.generate_recommendations(state)

        final_result = {
            "overall_status": "PASS" if total_issues == 0 else "FAIL",
            "total_issues": total_issues,
            "syntax_valid": state.get("syntax_valid", False),
            "standard_compliant": state.get("standard_compliant", False),
            "summary": self.generate_summary(state),
        }

        state.update({"final_result": final_result, "recommendations": recommendations})

        # 결과 표시
        if total_issues == 0:
            st.success("🎉 **모든 검증 통과!**")
        else:
            st.error(f"❌ **총 {total_issues}개의 문제 발견**")

        return state

    # 헬퍼 메서드들
    def should_continue_after_syntax(self, state: ValidationState) -> str:
        """문법 검증 후 계속 진행할지 결정"""
        return "continue" if state.get("syntax_valid", False) else "stop"

    def extract_ddl_type(self, ddl_content: str) -> str:
        """DDL 타입 추출"""
        ddl_upper = ddl_content.upper().strip()
        if ddl_upper.startswith("CREATE TABLE"):
            return "CREATE_TABLE"
        elif ddl_upper.startswith("ALTER TABLE"):
            return "ALTER_TABLE"
        elif ddl_upper.startswith("CREATE INDEX"):
            return "CREATE_INDEX"
        elif ddl_upper.startswith("CREATE UNIQUE INDEX"):
            return "CREATE_UNIQUE_INDEX"
        elif ddl_upper.startswith("DROP"):
            return "DROP"
        else:
            return "UNKNOWN"

    def validate_syntax_with_claude(self, ddl_content: str) -> str:
        """Claude를 사용한 문법 검증"""
        prompt = f"""
        다음 DDL 문의 문법을 검증해주세요:
        
        {ddl_content}
        
        문법 오류가 있으면 구체적으로 지적해주세요.
        문법이 올바르면 "문법 검증 통과"라고 응답해주세요.
        """

        return self.call_claude(prompt)

    def validate_standards_with_claude(self, ddl_content: str, standards: List) -> str:
        """Claude를 사용한 표준 검증"""
        if not standards:
            return "표준 규칙을 찾을 수 없습니다."

        standards_text = "\n".join(
            [std.get("content", {}).get("text", str(std)) for std in standards]
        )

        # 선택된 데이터베이스의 스키마 정보 추가
        selected_db = st.session_state.get("selected_database", None)
        schema_context = ""
        if selected_db:
            try:
                schema_context = get_database_info(selected_db)
            except Exception as e:
                logger.error(f"스키마 정보 가져오기 실패: {e}")

        prompt = f"""
        다음 스키마 표준 규칙에 따라 DDL을 검증해주세요:
        
        표준 규칙:
        {standards_text}
        
        현재 데이터베이스 스키마 정보:
        {schema_context}
        
        검증할 DDL:
        {ddl_content}
        
        위반 사항이 있으면 구체적으로 나열해주세요.
        기존 스키마와의 호환성도 확인해주세요.
        """

        return self.call_claude(prompt)

    def call_claude(self, prompt: str) -> str:
        """Claude API 호출"""
        try:
            claude_input = json.dumps(
                {
                    "anthropic_version": "bedrock-2023-05-31",
                    "max_tokens": 2048,
                    "messages": [
                        {"role": "user", "content": [{"type": "text", "text": prompt}]}
                    ],
                    "temperature": 0.3,
                }
            )

            response = self.bedrock_client.invoke_model(
                modelId="anthropic.claude-3-sonnet-20240229-v1:0", body=claude_input
            )

            response_body = json.loads(response.get("body").read())
            return response_body.get("content", [{}])[0].get("text", "")

        except Exception as e:
            logger.error(f"Claude 호출 오류: {e}")
            return f"Claude 호출 중 오류 발생: {str(e)}"

    def query_knowledge_base(self, query: str) -> List:
        """Knowledge Base 검색"""
        try:
            response = self.bedrock_agent_client.retrieve(
                knowledgeBaseId=self.knowledge_base_id,
                retrievalQuery={"text": query},
                retrievalConfiguration={
                    "vectorSearchConfiguration": {"numberOfResults": 3}
                },
            )
            return response["retrievalResults"]
        except Exception as e:
            logger.error(f"Knowledge Base 검색 오류: {e}")
            return []

    def parse_standard_issues(self, claude_result: str) -> List[str]:
        """Claude 결과에서 표준 위반 사항 파싱"""
        issues = []
        lines = claude_result.split("\n")
        for line in lines:
            if "위반" in line or "문제" in line or "오류" in line:
                issues.append(line.strip())
        return issues

    def check_table_creation(self, cursor, ddl_content: str) -> List[str]:
        """테이블 생성 검증"""
        issues = []

        # 테이블명 추출
        table_match = re.search(
            r"CREATE TABLE\s+(?:IF NOT EXISTS\s+)?`?([\w_]+)`?",
            ddl_content,
            re.IGNORECASE,
        )
        if table_match:
            table_name = table_match.group(1)

            # 테이블 존재 여부 확인
            cursor.execute("SHOW TABLES LIKE %s", (table_name,))
            if cursor.fetchone():
                issues.append(f"테이블 '{table_name}'이 이미 존재합니다.")

        return issues

    def check_table_alteration(self, cursor, ddl_content: str) -> List[str]:
        """테이블 변경 검증"""
        issues = []

        # 테이블명 추출
        table_match = re.search(
            r"ALTER TABLE\s+`?([\w_]+)`?", ddl_content, re.IGNORECASE
        )
        if table_match:
            table_name = table_match.group(1)

            # 테이블 존재 여부 확인
            cursor.execute("SHOW TABLES LIKE %s", (table_name,))
            if not cursor.fetchone():
                issues.append(f"테이블 '{table_name}'이 존재하지 않습니다.")

        return issues

    def check_index_creation(self, cursor, ddl_content: str) -> List[str]:
        """인덱스 생성 검증"""
        issues = []

        # 인덱스명과 테이블명 추출
        index_match = re.search(
            r"CREATE\s+(?:UNIQUE\s+)?INDEX\s+`?([\w_]+)`?\s+ON\s+`?([\w_]+)`?",
            ddl_content,
            re.IGNORECASE,
        )
        if index_match:
            index_name = index_match.group(1)
            table_name = index_match.group(2)

            # 테이블 존재 여부 확인
            cursor.execute("SHOW TABLES LIKE %s", (table_name,))
            if not cursor.fetchone():
                issues.append(f"테이블 '{table_name}'이 존재하지 않습니다.")
            else:
                # 인덱스 존재 여부 확인
                try:
                    cursor.execute(
                        f"SHOW INDEX FROM `{table_name}` WHERE Key_name = %s",
                        (index_name,),
                    )
                    if cursor.fetchone():
                        issues.append(f"인덱스 '{index_name}'이 이미 존재합니다.")
                except Exception as e:
                    issues.append(f"인덱스 확인 중 오류: {str(e)}")

        return issues

    def check_drop_operation(self, cursor, ddl_content: str) -> List[str]:
        """DROP 연산 검증"""
        issues = []

        if "DROP TABLE" in ddl_content.upper():
            table_match = re.search(
                r"DROP TABLE\s+(?:IF EXISTS\s+)?`?([\w_]+)`?",
                ddl_content,
                re.IGNORECASE,
            )
            if table_match:
                table_name = table_match.group(1)

                # 테이블 존재 여부 확인
                cursor.execute("SHOW TABLES LIKE %s", (table_name,))
                if not cursor.fetchone():
                    issues.append(
                        f"삭제하려는 테이블 '{table_name}'이 존재하지 않습니다."
                    )
                else:
                    # 데이터 존재 여부 확인
                    try:
                        cursor.execute(f"SELECT COUNT(*) FROM `{table_name}`")
                        count = cursor.fetchone()[0]
                        if count > 0:
                            issues.append(
                                f"테이블 '{table_name}'에 {count}개의 데이터가 있습니다. 삭제 시 데이터 손실 위험!"
                            )
                    except Exception as e:
                        issues.append(f"테이블 데이터 확인 중 오류: {str(e)}")

        return issues

    def analyze_table_performance(self, ddl_content: str) -> List[str]:
        """테이블 성능 분석"""
        issues = []

        # TEXT/BLOB 컬럼 많은지 확인
        text_count = len(re.findall(r"\bTEXT\b|\bBLOB\b", ddl_content, re.IGNORECASE))
        if text_count > 3:
            issues.append(
                f"TEXT/BLOB 컬럼이 {text_count}개로 많습니다. 성능에 영향을 줄 수 있습니다."
            )

        # 인덱스 없는 외래키 확인
        if "FOREIGN KEY" in ddl_content.upper() and "INDEX" not in ddl_content.upper():
            issues.append(
                "외래키에 대한 인덱스가 없습니다. 성능 저하 가능성이 있습니다."
            )

        return issues

    def analyze_alter_performance(self, ddl_content: str) -> List[str]:
        """ALTER 성능 분석"""
        issues = []

        if "ADD COLUMN" in ddl_content.upper():
            issues.append(
                "컬럼 추가는 테이블 크기에 따라 시간이 오래 걸릴 수 있습니다."
            )

        if "MODIFY COLUMN" in ddl_content.upper():
            issues.append("컬럼 수정은 전체 테이블 재구성이 필요할 수 있습니다.")

        return issues

    def analyze_index_performance(self, ddl_content: str) -> List[str]:
        """인덱스 성능 분석"""
        issues = []

        # 복합 인덱스 컬럼 수 확인
        column_match = re.search(r"\((.*?)\)", ddl_content)
        if column_match:
            columns = column_match.group(1).split(",")
            if len(columns) > 5:
                issues.append(
                    f"복합 인덱스 컬럼이 {len(columns)}개로 많습니다. 인덱스 크기가 클 수 있습니다."
                )

        return issues

    def check_drop_safety(self, ddl_content: str) -> List[str]:
        """DROP 안전성 검증"""
        issues = []

        if "CASCADE" not in ddl_content.upper():
            issues.append("CASCADE 옵션이 없습니다. 참조 관계 확인이 필요합니다.")

        return issues

    def check_alter_safety(self, ddl_content: str) -> List[str]:
        """ALTER 안전성 검증"""
        issues = []

        if "DROP COLUMN" in ddl_content.upper():
            issues.append(
                "컬럼 삭제는 데이터 손실을 야기합니다. 백업 확인이 필요합니다."
            )

        return issues

    def generate_recommendations(self, state: ValidationState) -> List[str]:
        """권장사항 생성"""
        recommendations = []

        if state.get("syntax_errors"):
            recommendations.append("문법 오류를 수정한 후 다시 실행하세요.")

        if state.get("standard_issues"):
            recommendations.append("스키마 표준 규칙을 준수하도록 수정하세요.")

        if state.get("performance_issues"):
            recommendations.append("성능 최적화를 위한 인덱스 추가를 고려하세요.")

        if state.get("data_safety_issues"):
            recommendations.append("데이터 백업 후 작업을 진행하세요.")

        return recommendations

    def generate_summary(self, state: ValidationState) -> str:
        """요약 생성"""
        issues = []

        if state.get("syntax_errors"):
            issues.append(f"문법 오류 {len(state['syntax_errors'])}개")

        if state.get("standard_issues"):
            issues.append(f"표준 위반 {len(state['standard_issues'])}개")

        if state.get("performance_issues"):
            issues.append(f"성능 이슈 {len(state['performance_issues'])}개")

        if state.get("data_safety_issues"):
            issues.append(f"안전성 이슈 {len(state['data_safety_issues'])}개")

        if not issues:
            return "모든 검증을 통과했습니다."
        else:
            return f"발견된 문제: {', '.join(issues)}"


def main():
    st.title("🤖 DDL 검증 에이전트 (LangGraph)")
    st.write("LangGraph 기반 다단계 DDL 검증 시스템")

    # 워크플로우 다이어그램 표시
    if st.checkbox("🔄 검증 워크플로우 보기", value=False):
        st.write("### 🔄 DDL 검증 워크플로우")

        workflow_html = """
        <div style="text-align: center; padding: 20px;">
            <div style="display: inline-block; margin: 10px; padding: 15px; background: #e1f5fe; border-radius: 10px; border: 2px solid #01579b;">
                🔤 DDL 입력
            </div>
            <div style="margin: 10px;">⬇️</div>
            <div style="display: inline-block; margin: 10px; padding: 15px; background: #f3e5f5; border-radius: 10px; border: 2px solid #4a148c;">
                🔍 1. 문법 검증
            </div>
            <div style="margin: 10px;">⬇️</div>
            <div style="display: inline-block; margin: 10px; padding: 15px; background: #e8f5e8; border-radius: 10px; border: 2px solid #1b5e20;">
                📋 2. 표준 규칙 검증
            </div>
            <div style="margin: 10px;">⬇️</div>
            <div style="display: inline-block; margin: 10px; padding: 15px; background: #e8eaf6; border-radius: 10px; border: 2px solid #3f51b5;">
                🔗 2.5. DB 연결 테스트
            </div>
            <div style="margin: 10px;">⬇️</div>
            <div style="display: inline-block; margin: 10px; padding: 15px; background: #fce4ec; border-radius: 10px; border: 2px solid #880e4f;">
                🗄️ 3. DB 스키마 검증
            </div>
            <div style="margin: 10px;">⬇️</div>
            <div style="display: inline-block; margin: 10px; padding: 15px; background: #f1f8e9; border-radius: 10px; border: 2px solid #33691e;">
                ⚡ 4. 성능 영향 분석
            </div>
            <div style="margin: 10px;">⬇️</div>
            <div style="display: inline-block; margin: 10px; padding: 15px; background: #fff8e1; border-radius: 10px; border: 2px solid #f57f17;">
                🛡️ 5. 데이터 안전성 검증
            </div>
            <div style="margin: 10px;">⬇️</div>
            <div style="display: inline-block; margin: 10px; padding: 15px; background: #e3f2fd; border-radius: 10px; border: 2px solid #0d47a1;">
                📊 6. 최종 보고서
            </div>
        </div>
        """

        st.markdown(workflow_html, unsafe_allow_html=True)

    # 사이드바 설정
    st.sidebar.title("⚙️ 설정")

    # SSH 터널링 설정
    st.sidebar.subheader("🔐 SSH 터널링 설정")

    use_ssh_tunnel = st.sidebar.checkbox("SSH 터널 사용", value=True)

    if use_ssh_tunnel:
        ssh_host = st.sidebar.text_input("EC2 IP 주소", value="54.180.79.255")
        ssh_user = st.sidebar.text_input("SSH 사용자", value="ec2-user")
        ssh_key_path = st.sidebar.text_input(
            "SSH 키 파일 경로", value="/Users/heungh/test.pem"
        )

        # SSH 설정을 세션 상태에 저장
        st.session_state["ssh_config"] = {
            "use_ssh_tunnel": use_ssh_tunnel,
            "ssh_host": ssh_host,
            "ssh_user": ssh_user,
            "ssh_key_path": ssh_key_path,
        }

        st.sidebar.success(f"✅ SSH 터널: {ssh_host}")
    else:
        st.session_state["ssh_config"] = {"use_ssh_tunnel": False}
        st.sidebar.info("직접 DB 연결 모드")

    # MySQL 설치 상태 확인
    if mysql is None:
        st.sidebar.error("❌ MySQL 커넥터 미설치")
        st.sidebar.code("pip install mysql-connector-python")
    else:
        st.sidebar.success("✅ MySQL 커넥터 설치됨")

    # 데이터베이스 목록 표시
    st.sidebar.subheader("🗄️ 데이터베이스 선택")

    # 리전 선택
    regions = ["us-east-1", "us-west-2", "ap-northeast-2"]
    selected_region = st.sidebar.selectbox("AWS Region", regions)

    # 키워드로 데이터베이스 검색
    db_keyword = st.sidebar.text_input("DB 키워드 (예: gamedb)", value="gamedb")

    if st.sidebar.button("🔍 데이터베이스 검색"):
        try:
            # AWS 클라이언트 초기화
            boto3.setup_default_session(region_name=selected_region)

            # 키워드로 Secret 목록 가져오기
            db_list = get_secrets_by_keyword(db_keyword)
            st.session_state["available_databases"] = db_list

            if db_list:
                st.sidebar.success(f"✅ {len(db_list)}개 데이터베이스 발견")
            else:
                st.sidebar.warning("⚠️ 해당 키워드로 데이터베이스를 찾을 수 없습니다.")

        except Exception as e:
            st.sidebar.error(f"❌ 데이터베이스 검색 실패: {str(e)}")

    # 데이터베이스 선택
    available_dbs = st.session_state.get("available_databases", [])
    if available_dbs:
        selected_db = st.sidebar.selectbox(
            "데이터베이스 선택", options=available_dbs, key="database_selector"
        )
        st.session_state["selected_database"] = selected_db

        # 선택된 데이터베이스 연결 테스트
        if st.sidebar.button("🔗 연결 테스트"):
            try:
                connection = connect_to_db(selected_db)
                connection.close()
                st.sidebar.success(f"✅ {selected_db} 연결 성공!")

                # 스키마 정보 미리보기
                with st.sidebar.expander("📋 스키마 미리보기"):
                    schema_info = get_database_info(selected_db)
                    st.text(
                        schema_info[:500] + "..."
                        if len(schema_info) > 500
                        else schema_info
                    )

            except Exception as e:
                st.sidebar.error(f"❌ {selected_db} 연결 실패: {str(e)}")
    else:
        st.sidebar.info("💡 먼저 데이터베이스를 검색해주세요.")

    # DDL 입력
    st.header("📝 DDL 입력")

    # 파일 업로드 또는 직접 입력
    input_method = st.radio("입력 방법", ["파일 업로드", "직접 입력"])

    ddl_content = ""

    if input_method == "파일 업로드":
        uploaded_file = st.file_uploader("DDL 파일 선택", type=["sql", "txt"])
        if uploaded_file:
            ddl_content = str(uploaded_file.read(), "utf-8")
    else:
        ddl_content = st.text_area(
            "DDL 입력",
            height=200,
            placeholder="""
CREATE TABLE users (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    username VARCHAR(50) NOT NULL,
    email VARCHAR(255) NOT NULL
);
        """.strip(),
        )

    if ddl_content and st.button("🚀 검증 시작", type="primary"):
        # 에이전트 초기화
        agent = DDLValidationAgent()
        workflow = agent.create_workflow()

        # 초기 상태 설정
        initial_state = ValidationState(
            ddl_content=ddl_content,
            ddl_type="",
            syntax_valid=False,
            syntax_errors=[],
            standard_compliant=False,
            standard_issues=[],
            db_connection_info={
                "selected_database": st.session_state.get("selected_database", None)
            },
            schema_info={},
            performance_issues=[],
            data_safety_issues=[],
            final_result={},
            recommendations=[],
        )

        # 워크플로우 실행
        with st.container():
            st.write("## 🔄 검증 진행 상황")

            try:
                final_state = workflow.invoke(initial_state)

                # 최종 결과 표시
                st.write("## 📊 최종 검증 결과")

                result = final_state["final_result"]

                if result["overall_status"] == "PASS":
                    st.success("🎉 **전체 검증 통과!**")
                else:
                    st.error(f"❌ **검증 실패** (총 {result['total_issues']}개 문제)")

                # 상세 결과
                col1, col2 = st.columns(2)

                with col1:
                    st.write("### 📋 검증 항목별 결과")
                    st.write(
                        f"- 문법 검증: {'✅' if final_state['syntax_valid'] else '❌'}"
                    )
                    st.write(
                        f"- 표준 준수: {'✅' if final_state['standard_compliant'] else '❌'}"
                    )
                    st.write(
                        f"- DB 연결: {'✅' if final_state.get('db_connection_status') == 'success' else '❌'}"
                    )
                    st.write(
                        f"- 스키마 검증: {'✅' if not final_state.get('schema_info', {}).get('issues') else '❌'}"
                    )
                    st.write(
                        f"- 성능 검증: {'✅' if not final_state['performance_issues'] else '⚠️'}"
                    )
                    st.write(
                        f"- 안전성 검증: {'✅' if not final_state['data_safety_issues'] else '❌'}"
                    )

                    # DB 연결 정보 표시
                    if final_state.get("db_info"):
                        db_info = final_state["db_info"]
                        st.write(f"- 연결된 DB: {db_info.get('name', 'N/A')}")
                        st.write(f"- MySQL 버전: {db_info.get('version', 'N/A')}")

                with col2:
                    st.write("### 💡 권장사항")
                    for rec in final_state["recommendations"]:
                        st.write(f"- {rec}")

                # 상세 보고서 다운로드
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                report = generate_detailed_report(final_state, ddl_content)

                st.download_button(
                    label="📄 상세 보고서 다운로드",
                    data=report,
                    file_name=f"ddl_validation_report_{timestamp}.md",
                    mime="text/markdown",
                )

            except Exception as e:
                st.error(f"❌ 검증 중 오류 발생: {str(e)}")
                logger.error(f"워크플로우 실행 오류: {e}")


def generate_detailed_report(state: ValidationState, ddl_content: str) -> str:
    """상세 보고서 생성"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    report = f"""# DDL 검증 보고서

**검증 일시**: {timestamp}
**DDL 타입**: {state.get('ddl_type', 'UNKNOWN')}
**대상 데이터베이스**: {state.get('db_info', {}).get('name', 'N/A')}

## 원본 DDL
```sql
{ddl_content}
```

## 검증 결과 요약
- **전체 상태**: {state['final_result']['overall_status']}
- **총 문제 수**: {state['final_result']['total_issues']}
- **요약**: {state['final_result']['summary']}

## 상세 검증 결과

### 1. 문법 검증
- **결과**: {'통과' if state['syntax_valid'] else '실패'}
- **오류**: {', '.join(state['syntax_errors']) if state['syntax_errors'] else '없음'}

### 2. 표준 규칙 검증
- **결과**: {'준수' if state['standard_compliant'] else '위반'}
- **위반 사항**: {', '.join(state['standard_issues']) if state['standard_issues'] else '없음'}

### 2.5. 데이터베이스 연결
- **연결 상태**: {state.get('db_connection_status', 'N/A')}
- **DB 정보**: {state.get('db_info', {}).get('name', 'N/A')} ({state.get('db_info', {}).get('version', 'N/A')})

### 3. 스키마 검증
- **이슈**: {', '.join(state.get('schema_info', {}).get('issues', [])) if state.get('schema_info', {}).get('issues') else '없음'}

### 4. 성능 검증
- **이슈**: {', '.join(state['performance_issues']) if state['performance_issues'] else '없음'}

### 5. 안전성 검증
- **이슈**: {', '.join(state['data_safety_issues']) if state['data_safety_issues'] else '없음'}

## 권장사항
{chr(10).join([f'- {rec}' for rec in state['recommendations']]) if state['recommendations'] else '특별한 권장사항 없음'}

---
*Generated by DDL Validation Agent*
"""

    return report


if __name__ == "__main__":
    main()
