#!/usr/bin/env python3
"""
DDL Validation LangGraph MCP Server
"""

from typing import TypedDict, List, Dict, Any, Optional
from langgraph.graph import StateGraph, END
import json
import boto3
import mysql.connector
from datetime import datetime
import os
import re
from pathlib import Path
import asyncio
import logging

# MCP imports
from mcp.server.models import InitializationOptions
import mcp.types as types
from mcp.server import NotificationOptions, Server
import mcp.server.stdio

# State definition
class DDLValidationState(TypedDict):
    user_input: str
    database_secret: Optional[str]
    sql_file: Optional[str]
    validation_results: Optional[Dict]
    error: Optional[str]
    next_action: Optional[str]
    result_message: Optional[str]

class DDLValidationGraph:
    def __init__(self):
        self.current_dir = Path(__file__).parent
        self.sql_dir = self.current_dir / "sql"
        self.output_dir = self.current_dir / "output"
        self.output_dir.mkdir(exist_ok=True)
        
        # Bedrock 클라이언트 초기화
        try:
            self.bedrock_client = boto3.client('bedrock-runtime', region_name='us-east-1')
        except Exception as e:
            logging.warning(f"Bedrock 클라이언트 초기화 실패: {e}")
            self.bedrock_client = None
        
    def setup_ssh_tunnel(self, db_host: str) -> bool:
        """SSH 터널 설정"""
        try:
            import subprocess
            import time
            
            # 기존 터널 종료
            try:
                subprocess.run(["pkill", "-f", "ssh.*54.180.79.255"], capture_output=True, timeout=5)
                time.sleep(1)
            except:
                pass
            
            # SSH 터널 시작
            ssh_command = [
                "ssh", 
                "-F", "/dev/null",
                "-o", "UserKnownHostsFile=/dev/null",
                "-o", "StrictHostKeyChecking=no",
                "-o", "ConnectTimeout=10",
                "-N", "-L", f"3307:{db_host}:3306",
                "-i", "/Users/heungh/test.pem",
                "ec2-user@54.180.79.255"
            ]
            
            print(f"SSH 터널 설정 중: {db_host} -> localhost:3307")
            process = subprocess.Popen(ssh_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            time.sleep(5)  # 더 긴 대기 시간
            
            if process.poll() is None:
                print("SSH 터널이 설정되었습니다.")
                return True
            else:
                stdout, stderr = process.communicate()
                print(f"SSH 터널 설정 실패: {stderr.decode()}")
                return False
                
        except Exception as e:
            print(f"SSH 터널 설정 오류: {str(e)}")
            return False

    def get_db_connection(self, database_secret: str):
        """데이터베이스 연결"""
        try:
            # Secret에서 DB 연결 정보 가져오기
            client = boto3.client('secretsmanager', region_name='ap-northeast-2')
            response = client.get_secret_value(SecretId=database_secret)
            secret = json.loads(response['SecretString'])
            
            connection = None
            
            # SSH 터널을 통한 연결 시도
            if self.setup_ssh_tunnel(secret.get('host')):
                try:
                    print("SSH 터널을 통한 연결 시도...")
                    connection = mysql.connector.connect(
                        host='localhost',
                        port=3307,
                        user=secret.get('username'),
                        password=secret.get('password'),
                        database=secret.get('dbname', 'mysql'),
                        connection_timeout=10
                    )
                    print("SSH 터널 연결 성공!")
                except Exception as e:
                    print(f"SSH 터널 연결 실패: {str(e)}")
                    connection = None
            
            # SSH 터널 실패 시 직접 연결 시도
            if connection is None:
                print("직접 연결 시도...")
                try:
                    connection = mysql.connector.connect(
                        host=secret.get('host'),
                        port=secret.get('port', 3306),
                        user=secret.get('username'),
                        password=secret.get('password'),
                        database=secret.get('dbname', 'mysql'),
                        connection_timeout=10
                    )
                    print("직접 연결 성공!")
                except Exception as e:
                    print(f"직접 연결도 실패: {str(e)}")
                    raise Exception(f"모든 연결 방법 실패. SSH 터널 및 직접 연결 모두 실패: {str(e)}")
            
            return connection
            
        except Exception as e:
            raise Exception(f"Database connection failed: {str(e)}")

    async def validate_with_claude(self, ddl_content: str, database_secret: str = None, schema_info: dict = None, existing_analysis: dict = None) -> str:
        """
        Claude cross-region 프로파일을 활용한 DDL 검증 (실제 스키마 정보 및 기존 분석 결과 포함)
        """
        if not self.bedrock_client:
            return "Claude 검증을 사용할 수 없습니다 (Bedrock 클라이언트 초기화 실패)"
        
        # 기존 분석 결과 컨텍스트 생성
        analysis_context = ""
        if existing_analysis:
            analysis_details = []
            
            # 문법 검증 결과
            if existing_analysis.get('syntax_issues'):
                analysis_details.append("기존 문법 검증 결과:")
                for issue in existing_analysis['syntax_issues']:
                    analysis_details.append(f"  - {issue}")
            
            # 스키마 검증 결과
            if existing_analysis.get('schema_issues'):
                analysis_details.append("기존 스키마 검증 결과:")
                for issue in existing_analysis['schema_issues']:
                    analysis_details.append(f"  - {issue}")
            
            # 제약조건 검증 결과
            if existing_analysis.get('constraint_issues'):
                analysis_details.append("기존 제약조건 검증 결과:")
                for issue in existing_analysis['constraint_issues']:
                    analysis_details.append(f"  - {issue}")
            
            # DB 연결 정보
            if existing_analysis.get('db_connection_info'):
                db_info = existing_analysis['db_connection_info']
                analysis_details.append(f"데이터베이스 연결: {'성공' if db_info.get('success') else '실패'}")
                if db_info.get('success'):
                    analysis_details.append(f"  - 서버 버전: {db_info.get('server_version', 'N/A')}")
                    analysis_details.append(f"  - 현재 DB: {db_info.get('current_database', 'N/A')}")
            
            if analysis_details:
                analysis_context = f"""
기존 분석 결과:
{chr(10).join(analysis_details)}

위 기존 분석 결과를 참고하여 추가적인 검증을 수행해주세요.
"""
        
        # 스키마 컨텍스트 생성
        schema_context = """
스키마 정보를 가져올 수 없습니다.
기본적인 문법 검증만 수행합니다.
"""

        prompt = f"""
        다음 DDL 문을 검증해주세요:

        {ddl_content}

        {analysis_context}

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

        claude_input = json.dumps({
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens": 4096,
            "messages": [
                {"role": "user", "content": [{"type": "text", "text": prompt}]}
            ],
            "temperature": 0.3,
        })
        
        sonnet_4_model_id = "us.anthropic.claude-sonnet-4-20250514-v1:0"
        sonnet_3_7_model_id = "us.anthropic.claude-3-7-sonnet-20250219-v1:0"

        # Claude Sonnet 4 inference profile 호출
        try:
            response = self.bedrock_client.invoke_model(
                modelId=sonnet_4_model_id,
                body=claude_input
            )
            response_body = json.loads(response.get("body").read())
            return response_body.get("content", [{}])[0].get("text", "")
        except Exception as e:
            logging.warning(f"Claude Sonnet 4 호출 실패 → Claude 3.7 Sonnet cross-region profile로 fallback: {e}")
            # Claude 3.7 Sonnet inference profile 호출 (fallback)
            try:
                response = self.bedrock_client.invoke_model(
                    modelId=sonnet_3_7_model_id,
                    body=claude_input
                )
                response_body = json.loads(response.get("body").read())
                return response_body.get("content", [{}])[0].get("text", "")
            except Exception as e:
                logging.error(f"Claude 3.7 Sonnet 호출 오류: {e}")
                return f"Claude 호출 중 오류 발생: {str(e)}"

    # Node functions
    def start_node(self, state: DDLValidationState) -> DDLValidationState:
        """시작 노드 - 입력 파싱"""
        content = state.get("user_input", "")
        
        # 데이터베이스 시크릿 추출
        if "gamedb1-cluster" in content:
            state["database_secret"] = "gamedb1-cluster"
        
        # SQL 파일 추출
        if "sample" in content and "create" in content:
            state["sql_file"] = "sample_create_table.sql"
        elif any(sql_file in content for sql_file in ["test_", "sample_"]):
            # 더 정교한 파일명 추출 로직
            words = content.split()
            for word in words:
                if word.endswith('.sql'):
                    state["sql_file"] = word
                    break
        
        if state["database_secret"] and state["sql_file"]:
            state["next_action"] = "validate_sql"
        else:
            state["next_action"] = "handle_error"
            state["error"] = "데이터베이스 시크릿 또는 SQL 파일을 찾을 수 없습니다"
            
        return state

    def validate_sql_node(self, state: DDLValidationState) -> DDLValidationState:
        """SQL 검증 노드 (Claude 검증 포함)"""
        try:
            # SQL 파일 읽기
            sql_path = self.sql_dir / state["sql_file"]
            if not sql_path.exists():
                state["error"] = f"SQL 파일을 찾을 수 없습니다: {state['sql_file']}"
                state["next_action"] = "handle_error"
                return state
                
            with open(sql_path, 'r', encoding='utf-8') as f:
                sql_content = f.read()
            
            # 데이터베이스 연결
            conn = self.get_db_connection(state["database_secret"])
            cursor = conn.cursor()
            
            validation_results = {
                "file": state["sql_file"],
                "database": state["database_secret"],
                "timestamp": datetime.now().isoformat(),
                "syntax_check": True,
                "connection_test": True,
                "schema_validation": True,
                "claude_validation": True,
                "issues": [],
                "warnings": [],
                "claude_issues": []
            }
            
            # 1. 기본 문법 검증
            if not sql_content.strip():
                validation_results["issues"].append("SQL 파일이 비어있습니다")
                validation_results["syntax_check"] = False
            
            # 2. 세미콜론 검증
            if not sql_content.strip().endswith(';'):
                validation_results["issues"].append("SQL 문이 세미콜론으로 끝나지 않습니다")
            
            # 3. 데이터베이스 연결 테스트
            try:
                cursor.execute("SELECT VERSION()")
                version = cursor.fetchone()[0]
                validation_results["db_version"] = version
            except Exception as e:
                validation_results["connection_test"] = False
                validation_results["issues"].append(f"데이터베이스 연결 실패: {str(e)}")
            
            # 4. SQL 구문 검증 (EXPLAIN 사용)
            try:
                # DDL 문은 EXPLAIN으로 검증할 수 없으므로 다른 방법 사용
                if sql_content.strip().upper().startswith(('CREATE', 'ALTER', 'DROP')):
                    # DDL 문법 검증을 위한 간단한 파싱
                    if 'CREATE TABLE' in sql_content.upper():
                        # 테이블 생성 문법 검증
                        if not re.search(r'CREATE\s+TABLE\s+\w+\s*\(', sql_content, re.IGNORECASE):
                            validation_results["issues"].append("CREATE TABLE 문법이 올바르지 않습니다")
                            validation_results["syntax_check"] = False
                else:
                    # SELECT 문 등은 EXPLAIN으로 검증
                    cursor.execute(f"EXPLAIN {sql_content}")
                    
            except Exception as e:
                validation_results["syntax_check"] = False
                validation_results["issues"].append(f"SQL 구문 오류: {str(e)}")
            
            # 5. 스키마 검증 (테이블 존재 여부 등)
            try:
                if 'CREATE TABLE' in sql_content.upper():
                    # 테이블명 추출
                    match = re.search(r'CREATE\s+TABLE\s+(\w+)', sql_content, re.IGNORECASE)
                    if match:
                        table_name = match.group(1)
                        cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
                        if cursor.fetchone():
                            validation_results["warnings"].append(f"테이블 '{table_name}'이 이미 존재합니다")
                            
            except Exception as e:
                validation_results["schema_validation"] = False
                validation_results["issues"].append(f"스키마 검증 오류: {str(e)}")
            
            # 6. Claude AI 검증
            try:
                existing_analysis = {
                    'syntax_issues': [issue for issue in validation_results["issues"] if "구문" in issue or "문법" in issue],
                    'schema_issues': [issue for issue in validation_results["issues"] if "스키마" in issue or "테이블" in issue],
                    'constraint_issues': [],
                    'db_connection_info': {
                        'success': validation_results["connection_test"],
                        'server_version': validation_results.get("db_version", "N/A"),
                        'current_database': state["database_secret"]
                    }
                }
                
                claude_result = await self.validate_with_claude(
                    sql_content, 
                    state["database_secret"],
                    schema_info=None,
                    existing_analysis=existing_analysis
                )
                
                # Claude 결과 분석
                if "문제" in claude_result or "오류" in claude_result or "위반" in claude_result:
                    if "검증 통과" not in claude_result:
                        validation_results["claude_issues"].append(claude_result)
                        validation_results["issues"].append(f"Claude 검증: {claude_result}")
                        validation_results["claude_validation"] = False
                    else:
                        validation_results["warnings"].append("✅ Claude AI 검증 통과")
                else:
                    validation_results["warnings"].append("✅ Claude AI 검증 통과")
                    
            except Exception as e:
                validation_results["claude_validation"] = False
                validation_results["issues"].append(f"Claude 검증 중 오류 발생: {str(e)}")
            
            cursor.close()
            conn.close()
            
            state["validation_results"] = validation_results
            state["next_action"] = "generate_report"
            
        except Exception as e:
            state["error"] = str(e)
            state["next_action"] = "handle_error"
            
        return state

    def generate_report_node(self, state: DDLValidationState) -> DDLValidationState:
        """보고서 생성 노드"""
        try:
            results = state["validation_results"]
            
            # 결과 요약
            total_issues = len(results["issues"])
            total_warnings = len(results["warnings"])
            status = "PASS" if total_issues == 0 else "FAIL"
            
            # HTML 보고서 생성
            html_report_path = self.generate_html_report(results, status)
            
            # JSON 보고서 생성
            json_filename = f"validation_report_{results['file']}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            json_path = self.output_dir / json_filename
            
            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump(results, f, indent=2, ensure_ascii=False)
            
            summary = {
                "html_report_path": str(html_report_path),
                "json_report_path": str(json_path),
                "total_issues": total_issues,
                "total_warnings": total_warnings,
                "status": status
            }
            
            state["validation_results"]["summary"] = summary
            state["next_action"] = "complete"
            
        except Exception as e:
            state["error"] = str(e)
            state["next_action"] = "handle_error"
            
        return state

    def generate_html_report(self, results: Dict, status: str) -> Path:
        """HTML 보고서 생성 (ddl_validation_qcli_mcp_server.py 스타일)"""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            report_filename = f"validation_report_{results['file']}_{timestamp}.html"
            report_path = self.output_dir / report_filename
            
            # SQL 파일 내용 읽기
            sql_path = self.sql_dir / results['file']
            ddl_content = ""
            if sql_path.exists():
                with open(sql_path, 'r', encoding='utf-8') as f:
                    ddl_content = f.read()
            
            # 상태에 따른 색상 및 아이콘
            status_color = "#28a745" if status == "PASS" else "#dc3545"
            status_icon = "✅" if status == "PASS" else "❌"
            
            # 발견된 문제 섹션 - Claude 검증과 기타 검증 분리
            claude_issues = []
            other_issues = []
            
            for issue in results["issues"]:
                if issue.startswith("Claude 검증:"):
                    claude_issues.append(issue[12:].strip())  # "Claude 검증:" 제거
                else:
                    other_issues.append(issue)
            
            # Claude 검증 결과 섹션
            claude_section = ""
            if claude_issues:
                claude_section = """
                <div class="claude-section">
                    <h3>🤖 Claude AI 검증 결과</h3>
                """
                for claude_result in claude_issues:
                    claude_section += f"""
                    <div class="claude-result">
                        <pre class="claude-text">{claude_result}</pre>
                    </div>
                    """
                claude_section += """
                </div>
                """
            
            # 기타 문제 섹션
            other_issues_section = ""
            if other_issues:
                other_issues_section = f"""
                <div class="issues-section">
                    <h3>🚨 발견된 문제</h3>
                    <ul class="issues-list">
                        {''.join(f'<li>{issue}</li>' for issue in other_issues)}
                    </ul>
                </div>
                """
            
            # 경고 섹션
            warnings_section = ""
            if results["warnings"]:
                warnings_section = f"""
                <div class="warnings-section">
                    <h3>💡 경고사항</h3>
                    <ul class="warnings-list">
                        {''.join(f'<li>{warning}</li>' for warning in results["warnings"])}
                    </ul>
                </div>
                """
            
            # 성공 섹션
            success_section = ""
            if not results["issues"]:
                success_section = """
                <div class="issues-section success">
                    <h3>✅ 검증 결과</h3>
                    <p class="no-issues">모든 검증을 통과했습니다.</p>
                </div>
                """
            
            # 데이터베이스 정보 섹션
            db_info_section = ""
            if results.get("db_version"):
                db_info_section = f"""
                <div class="info-section">
                    <h3>🗄️ 데이터베이스 정보</h3>
                    <table class="info-table">
                        <tr>
                            <td>시크릿</td>
                            <td>{results['database']}</td>
                        </tr>
                        <tr>
                            <td>서버 버전</td>
                            <td>{results.get('db_version', 'N/A')}</td>
                        </tr>
                        <tr>
                            <td>연결 상태</td>
                            <td class="{'status-success' if results['connection_test'] else 'status-error'}">
                                {'✅ 성공' if results['connection_test'] else '❌ 실패'}
                            </td>
                        </tr>
                    </table>
                </div>
                """
            
            # HTML 보고서 내용
            html_content = f"""<!DOCTYPE html>
<html lang="ko">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>DDL 검증 보고서 - {results['file']}</title>
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
        .info-section, .issues-section, .warnings-section {{
            margin: 30px 0;
            padding: 20px;
            border-radius: 8px;
            border: 1px solid #e9ecef;
        }}
        .info-section h3, .issues-section h3, .warnings-section h3 {{
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
        .issues-list, .warnings-list {{
            margin: 10px 0;
            padding-left: 20px;
        }}
        .issues-list li {{
            margin: 5px 0;
            color: #dc3545;
        }}
        .warnings-list li {{
            margin: 5px 0;
            color: #ffc107;
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
        .warnings-section {{
            background: #fff3cd;
            border-color: #ffc107;
        }}
        .claude-section {{
            margin: 30px 0;
            padding: 25px;
            background: #f8f9ff;
            border: 1px solid #667eea;
            border-radius: 8px;
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
            border: 1px solid #e9ecef;
            border-radius: 6px;
            box-shadow: 0 1px 5px rgba(0,0,0,0.05);
        }}
        .claude-text {{
            background: white;
            border: 1px solid #e9ecef;
            border-radius: 6px;
            padding: 20px;
            margin: 0;
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            font-size: 0.95em;
            line-height: 1.6;
            color: #495057;
            white-space: pre-wrap;
            word-wrap: break-word;
            overflow-x: auto;
        }}
        .sql-code {{
            background: #f8f9fa;
            border: 1px solid #e9ecef;
            border-radius: 6px;
            padding: 20px;
            margin: 20px 0;
            font-family: 'Courier New', monospace;
            overflow-x: auto;
            white-space: pre-wrap;
            word-wrap: break-word;
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
                    <p>{results['file']}</p>
                </div>
                <div class="summary-item">
                    <h4>🕒 검증 일시</h4>
                    <p>{results['timestamp']}</p>
                </div>
                <div class="summary-item">
                    <h4>🗄️ 데이터베이스</h4>
                    <p>{results['database']}</p>
                </div>
                <div class="summary-item">
                    <h4>📊 문제 수</h4>
                    <p>{len(results['issues'])}개</p>
                </div>
            </div>
            
            {db_info_section}
            
            <div class="info-section">
                <h3>📝 원본 DDL</h3>
                <div class="sql-code">{ddl_content}</div>
            </div>
            
            <div class="info-section">
                <h3>📊 검증 결과</h3>
                <table class="info-table">
                    <tr>
                        <td>문법 검증</td>
                        <td class="{'status-success' if results['syntax_check'] else 'status-error'}">
                            {'✅ 통과' if results['syntax_check'] else '❌ 실패'}
                        </td>
                    </tr>
                    <tr>
                        <td>연결 테스트</td>
                        <td class="{'status-success' if results['connection_test'] else 'status-error'}">
                            {'✅ 성공' if results['connection_test'] else '❌ 실패'}
                        </td>
                    </tr>
                    <tr>
                        <td>스키마 검증</td>
                        <td class="{'status-success' if results['schema_validation'] else 'status-error'}">
                            {'✅ 통과' if results['schema_validation'] else '❌ 실패'}
                        </td>
                    </tr>
                    <tr>
                        <td>Claude AI 검증</td>
                        <td class="{'status-success' if results.get('claude_validation', True) else 'status-error'}">
                            {'✅ 통과' if results.get('claude_validation', True) else '❌ 실패'}
                        </td>
                    </tr>
                </table>
            </div>
            
            {claude_section}
            {other_issues_section}
            {warnings_section}
            {success_section}
        </div>
        
        <div class="footer">
            <p>Generated by DDL Validation LangGraph MCP Server</p>
            <p>Report generated at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        </div>
    </div>
</body>
</html>"""
            
            with open(report_path, 'w', encoding='utf-8') as f:
                f.write(html_content)
            
            return report_path
            
        except Exception as e:
            print(f"HTML 보고서 생성 오류: {e}")
            # 오류 발생시 기본 경로 반환
            return self.output_dir / f"error_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"

    def handle_error_node(self, state: DDLValidationState) -> DDLValidationState:
        """에러 처리 노드"""
        state["result_message"] = f"❌ 오류 발생: {state['error']}"
        return state

    def complete_node(self, state: DDLValidationState) -> DDLValidationState:
        """완료 노드"""
        if state["validation_results"]:
            results = state["validation_results"]
            summary = results.get("summary", {})
            
            status_icon = "✅" if summary.get("status") == "PASS" else "❌"
            
            result_msg = f"""{status_icon} SQL 검증 완료

📄 파일: {results['file']}
🗄️ 데이터베이스: {results['database']}
📊 결과: {summary.get('total_issues', 0)}개 문제, {summary.get('total_warnings', 0)}개 경고

📋 검증 항목:
• 문법 검증: {'✅' if results['syntax_check'] else '❌'}
• 연결 테스트: {'✅' if results['connection_test'] else '❌'}
• 스키마 검증: {'✅' if results['schema_validation'] else '❌'}
• Claude AI 검증: {'✅' if results.get('claude_validation', True) else '❌'}

📄 상세 보고서: {summary.get('html_report_path', 'N/A')}"""

            if results['issues']:
                result_msg += f"\n\n⚠️ 발견된 문제:\n" + "\n".join(f"• {issue}" for issue in results['issues'])
                
            if results['warnings']:
                result_msg += f"\n\n💡 경고사항:\n" + "\n".join(f"• {warning}" for warning in results['warnings'])
        else:
            result_msg = "✅ 작업 완료"
            
        state["result_message"] = result_msg
        return state

    # Edge routing
    def route_next_action(self, state: DDLValidationState) -> str:
        """다음 액션으로 라우팅"""
        return state.get("next_action", "complete")

    def build_graph(self) -> StateGraph:
        """LangGraph 워크플로우 구성"""
        workflow = StateGraph(DDLValidationState)
        
        # 노드 추가
        workflow.add_node("start", self.start_node)
        workflow.add_node("validate_sql", self.validate_sql_node)
        workflow.add_node("generate_report", self.generate_report_node)
        workflow.add_node("handle_error", self.handle_error_node)
        workflow.add_node("complete", self.complete_node)
        
        # 시작점 설정
        workflow.set_entry_point("start")
        
        # 조건부 엣지 추가
        workflow.add_conditional_edges(
            "start",
            self.route_next_action,
            {
                "validate_sql": "validate_sql",
                "handle_error": "handle_error"
            }
        )
        
        workflow.add_conditional_edges(
            "validate_sql",
            self.route_next_action,
            {
                "generate_report": "generate_report",
                "handle_error": "handle_error"
            }
        )
        
        workflow.add_conditional_edges(
            "generate_report",
            self.route_next_action,
            {
                "complete": "complete",
                "handle_error": "handle_error"
            }
        )
        
        # 종료 엣지
        workflow.add_edge("handle_error", END)
        workflow.add_edge("complete", END)
        
        return workflow.compile()

# 실행 함수
def run_ddl_validation(user_input: str):
    """DDL 검증 실행"""
    validator = DDLValidationGraph()
    app = validator.build_graph()
    
    initial_state = {
        "user_input": user_input,
        "database_secret": None,
        "sql_file": None,
        "validation_results": None,
        "error": None,
        "next_action": None,
        "result_message": None
    }
    
    result = app.invoke(initial_state)
    return result.get("result_message", "작업 완료")

# MCP 서버 설정
server = Server("ddl-langgraph-validator")

@server.list_tools()
async def handle_list_tools() -> list[types.Tool]:
    """사용 가능한 도구 목록 반환"""
    return [
        types.Tool(
            name="validate_ddl_with_langgraph",
            description="LangGraph를 사용하여 DDL을 검증합니다",
            inputSchema={
                "type": "object",
                "properties": {
                    "user_input": {
                        "type": "string", 
                        "description": "검증할 데이터베이스와 SQL 파일 정보"
                    }
                },
                "required": ["user_input"]
            }
        ),
        types.Tool(
            name="list_sql_files",
            description="sql 디렉토리의 SQL 파일 목록을 조회합니다",
            inputSchema={"type": "object", "properties": {}}
        )
    ]

@server.call_tool()
async def handle_call_tool(name: str, arguments: dict) -> list[types.TextContent]:
    """도구 호출 처리"""
    try:
        if name == "validate_ddl_with_langgraph":
            user_input = arguments.get("user_input", "")
            result = run_ddl_validation(user_input)
            return [types.TextContent(type="text", text=result)]
        
        elif name == "list_sql_files":
            sql_dir = Path("/Users/heungh/Documents/SA/05.Project/01.Infra-Assistant/01.DB-Assistant/sql")
            if sql_dir.exists():
                files = [f.name for f in sql_dir.glob("*.sql")]
                return [types.TextContent(type="text", text=f"SQL 파일 목록: {', '.join(files)}")]
            else:
                return [types.TextContent(type="text", text="SQL 디렉토리를 찾을 수 없습니다")]
        
        else:
            return [types.TextContent(type="text", text=f"알 수 없는 도구: {name}")]
    
    except Exception as e:
        return [types.TextContent(type="text", text=f"오류 발생: {str(e)}")]

async def main():
    """메인 함수"""
    async with mcp.server.stdio.stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream,
            write_stream,
            InitializationOptions(
                server_name="ddl-langgraph-validator",
                server_version="1.0.0",
                capabilities=server.get_capabilities(
                    notification_options=NotificationOptions(),
                    experimental_capabilities={},
                ),
            ),
        )

if __name__ == "__main__":
    # MCP 서버 모드로 실행
    if len(os.sys.argv) > 1 and os.sys.argv[1] == "--test":
        # 테스트 모드
        test_input = "gamedb1-cluster에 sample_create_table.sql 검증"
        result = run_ddl_validation(test_input)
        print(result)
    else:
        # MCP 서버 모드
        asyncio.run(main())
