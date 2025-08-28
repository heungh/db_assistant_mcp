#!/usr/bin/env python3
"""
데이터베이스 분석 MCP 서버
CloudWatch 메트릭 수집, 상관관계 분석, 아웃라이어 탐지, 회귀 분석 기능 제공
데이터베이스 연결, 성능 분석, AI 기반 진단 기능 통합
"""

import asyncio
import json
import os
import re
import subprocess
import time
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
from pathlib import Path

import boto3
from botocore.exceptions import ClientError
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')  # GUI 없는 환경에서 matplotlib 사용
import matplotlib.pyplot as plt
try:
    import seaborn as sns
except ImportError:
    sns = None
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import PolynomialFeatures
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error, r2_score
from sklearn.impute import SimpleImputer

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

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 현재 디렉토리 기준 경로 설정
CURRENT_DIR = Path(__file__).parent
OUTPUT_DIR = CURRENT_DIR / "output"
DATA_DIR = CURRENT_DIR / "data"

# 디렉토리 생성
OUTPUT_DIR.mkdir(exist_ok=True)
DATA_DIR.mkdir(exist_ok=True)

class AnalyzeDBServer:
    def __init__(self):
        # 기본 리전 설정
        self.default_region = "ap-northeast-2"
        
        # CloudWatch 클라이언트
        self.cloudwatch = None
        
        # Bedrock 클라이언트 (Claude AI 통합) - Cross Region Inference
        self.bedrock_client = boto3.client(
            "bedrock-runtime", region_name="us-east-1", verify=False
        )
        self.bedrock_agent_client = boto3.client(
            "bedrock-agent-runtime", region_name="us-east-1", verify=False
        )
        self.knowledge_base_id = "0WQUBRHVR8"
        
        # 데이터베이스 연결 관리
        self.shared_connection = None
        self.shared_cursor = None
        self.tunnel_used = False
        self.selected_database = None
        
        # 기본 메트릭 설정
        self.default_metrics = [
            'CPUUtilization', 'DatabaseConnections', 'DBLoad', 'DBLoadCPU', 
            'DBLoadNonCPU', 'FreeableMemory', 'ReadIOPS', 'WriteIOPS',
            'ReadLatency', 'WriteLatency', 'NetworkReceiveThroughput',
            'NetworkTransmitThroughput', 'BufferCacheHitRatio'
        ]
    
    def get_secret(self, secret_name):
        """AWS Secrets Manager에서 시크릿 값 가져오기"""
        try:
            session = boto3.session.Session()
            client = session.client(
                service_name="secretsmanager",
                region_name="ap-northeast-2",
                verify=False,
            )
            get_secret_value_response = client.get_secret_value(SecretId=secret_name)
            return json.loads(get_secret_value_response["SecretString"])
        except Exception as e:
            logger.error(f"시크릿 조회 실패: {e}")
            return None

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
            subprocess.run(["pkill", "-f", "ssh.*54.180.79.255"], capture_output=True)
            logger.info("SSH 터널이 정리되었습니다.")
        except Exception as e:
            logger.error(f"SSH 터널 정리 중 오류: {e}")

    def get_db_connection(self, database_secret: str, selected_database: str = None, use_ssh_tunnel: bool = True):
        """공통 DB 연결 함수"""
        if mysql is None:
            raise Exception("mysql-connector-python이 설치되지 않았습니다.")

        db_config = self.get_secret(database_secret)
        if not db_config:
            raise Exception(f"시크릿 {database_secret}을 찾을 수 없습니다.")

        connection_config = None
        tunnel_used = False

        database_name = selected_database or db_config.get("dbname", db_config.get("database"))
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

    def setup_shared_connection(self, database_secret: str, selected_database: str = None, use_ssh_tunnel: bool = True):
        """공용 DB 연결 설정"""
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

    async def query_knowledge_base(self, query: str, analysis_type: str = "performance") -> str:
        """Knowledge Base에서 관련 정보 조회"""
        try:
            # 분석 타입에 따른 쿼리 조정
            if analysis_type == "performance":
                kb_query = f"Aurora MySQL 성능 최적화 가이드 {query}"
            elif analysis_type == "troubleshooting":
                kb_query = f"Aurora MySQL 문제 해결 가이드 {query}"
            else:
                kb_query = f"데이터베이스 도메인 관리 규칙 {query}"
            
            response = self.bedrock_agent_client.retrieve(
                knowledgeBaseId=self.knowledge_base_id,
                retrievalQuery={"text": kb_query},
                retrievalConfiguration={
                    "vectorSearchConfiguration": {
                        "numberOfResults": 3
                    }
                }
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

    async def analyze_with_claude(self, metrics_data: str, scenario_context: str = "", analysis_type: str = "performance") -> str:
        """Claude AI를 활용한 성능 분석"""
        try:
            # Knowledge Base에서 관련 정보 조회
            knowledge_context = ""
            try:
                knowledge_info = await self.query_knowledge_base(scenario_context, analysis_type)
                if knowledge_info and knowledge_info != "관련 정보를 찾을 수 없습니다.":
                    knowledge_context = f"""
**참고 가이드라인:**
{knowledge_info}
"""
            except Exception as e:
                logger.warning(f"Knowledge Base 조회 실패: {e}")

            prompt = f"""당신은 Aurora MySQL 성능 분석 전문가입니다. 다음 메트릭 데이터를 분석하고 발견내용과 리뷰고려사항을 제공해주세요.

{knowledge_context}

**분석할 메트릭 데이터:**
{metrics_data}

**시나리오 컨텍스트:**
{scenario_context}

다음 형식으로 분석 결과를 제공해주세요:

## 발견내용
- 주요 성능 지표 분석
- 비정상적인 패턴이나 임계값 초과 항목
- 시간대별 트렌드 분석

## 리뷰고려사항
- 즉시 조치가 필요한 항목
- 중장기적 최적화 방안
- 모니터링 강화가 필요한 영역
- 구체적인 해결 방안 제시

분석은 실용적이고 구체적으로 작성해주세요."""

            claude_input = json.dumps({
                "anthropic_version": "bedrock-2023-05-31",
                "max_tokens": 2000,
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
                    modelId=sonnet_4_model_id, body=claude_input
                )
                response_body = json.loads(response.get("body").read())
                analysis_result = response_body.get("content", [{}])[0].get("text", "")
            except Exception as e:
                logger.warning(f"Claude Sonnet 4 호출 실패 → Claude 3.7 Sonnet cross-region profile로 fallback: {e}")
                # Claude 3.7 Sonnet inference profile 호출 (fallback)
                try:
                    response = self.bedrock_client.invoke_model(
                        modelId=sonnet_3_7_model_id, body=claude_input
                    )
                    response_body = json.loads(response.get("body").read())
                    analysis_result = response_body.get("content", [{}])[0].get("text", "")
                except Exception as e:
                    logger.error(f"Claude 3.7 Sonnet 호출 오류: {e}")
                    analysis_result = f"Claude 호출 중 오류 발생: {str(e)}"
            
            # 보고서 제목 업데이트 (Insight -> 발견내용, Recommendations -> 리뷰고려사항)
            analysis_result = self.update_report_titles(analysis_result)
            
            return analysis_result
            
        except Exception as e:
            logger.error(f"Claude 분석 실패: {e}")
            return f"AI 분석 중 오류가 발생했습니다: {str(e)}"

    async def get_database_performance_metrics(self, database_secret: str, metric_type: str = "all") -> str:
        """데이터베이스 성능 메트릭 조회"""
        try:
            connection, tunnel_used = self.get_db_connection(database_secret, self.selected_database, use_ssh_tunnel=True)
            cursor = connection.cursor()

            result = f"📊 **데이터베이스 성능 메트릭**\n\n"

            if metric_type in ["all", "query"]:
                # 쿼리 성능 통계
                cursor.execute("""
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
                """)

                query_stats = cursor.fetchall()
                if query_stats:
                    result += "🔍 **느린 쿼리 TOP 5:**\n"
                    for i, (pattern, count, avg_time, max_time, total_time) in enumerate(query_stats, 1):
                        pattern_short = (pattern[:60] + "...") if len(pattern) > 60 else pattern
                        result += f"{i}. {pattern_short}\n"
                        result += f"   - 실행횟수: {count:,}, 평균시간: {avg_time:.3f}초, 최대시간: {max_time:.3f}초\n\n"

            if metric_type in ["all", "connection"]:
                # 연결 통계
                cursor.execute("""
                    SELECT 
                        COUNT(*) as total_connections,
                        SUM(CASE WHEN COMMAND != 'Sleep' THEN 1 ELSE 0 END) as active_connections
                    FROM information_schema.processlist
                """)

                conn_stats = cursor.fetchone()
                if conn_stats:
                    result += f"🔗 **연결 통계:**\n"
                    result += f"- 총 연결: {conn_stats[0]}개\n"
                    result += f"- 활성 연결: {conn_stats[1]}개\n\n"

            if metric_type in ["all", "io"]:
                # InnoDB 상태 정보
                cursor.execute("SHOW ENGINE INNODB STATUS")
                innodb_status = cursor.fetchone()
                if innodb_status:
                    status_text = innodb_status[2]
                    # 간단한 파싱으로 주요 정보 추출
                    if "Buffer pool hit rate" in status_text:
                        import re
                        hit_rate_match = re.search(r'Buffer pool hit rate (\d+)', status_text)
                        if hit_rate_match:
                            hit_rate = hit_rate_match.group(1)
                            result += f"💾 **버퍼 풀 히트율:** {hit_rate}/1000\n\n"

            cursor.close()
            connection.close()

            if tunnel_used:
                self.cleanup_ssh_tunnel()

            return result

        except Exception as e:
            return f"❌ 성능 메트릭 조회 실패: {str(e)}"

    async def get_schema_summary(self, database_secret: str) -> str:
        """데이터베이스 스키마 요약 정보"""
        try:
            connection, tunnel_used = self.get_db_connection(database_secret, self.selected_database, use_ssh_tunnel=True)
            cursor = connection.cursor()

            result = f"🗄️ **데이터베이스 스키마 요약**\n\n"

            # 데이터베이스 정보
            cursor.execute("SELECT DATABASE()")
            current_db = cursor.fetchone()[0]
            result += f"**현재 데이터베이스:** {current_db}\n\n"

            # 테이블 목록과 기본 정보
            cursor.execute("""
                SELECT 
                    TABLE_NAME,
                    ENGINE,
                    TABLE_ROWS,
                    ROUND(((DATA_LENGTH + INDEX_LENGTH) / 1024 / 1024), 2) AS size_mb
                FROM information_schema.TABLES 
                WHERE TABLE_SCHEMA = DATABASE()
                ORDER BY size_mb DESC
            """)

            tables = cursor.fetchall()
            if tables:
                result += f"📋 **테이블 목록 ({len(tables)}개):**\n"
                for table_name, engine, rows, size_mb in tables:
                    result += f"- {table_name} ({engine}, {rows:,}행, {size_mb}MB)\n"
                result += "\n"

            cursor.close()
            connection.close()

            if tunnel_used:
                self.cleanup_ssh_tunnel()

            return result

        except Exception as e:
            return f"❌ 스키마 정보 조회 실패: {str(e)}"

    async def diagnose_connection_issues(self, database_secret: str) -> str:
        """시나리오 1: 연결 수 급증으로 인한 성능 저하 진단"""
        try:
            connection, tunnel_used = self.get_db_connection(database_secret, self.selected_database, use_ssh_tunnel=True)
            cursor = connection.cursor()

            result = "🔍 **연결 수 급증 성능 저하 진단**\n\n"

            # 1단계: 연결 상태 확인
            cursor.execute("""
                SELECT 
                    SUBSTRING_INDEX(host, ':', 1) as client_ip,
                    COUNT(*) as connection_count,
                    command,
                    state
                FROM information_schema.processlist 
                GROUP BY client_ip, command, state
                ORDER BY connection_count DESC
                LIMIT 10
            """)

            connections = cursor.fetchall()
            if connections:
                result += "📊 **클라이언트별 연결 현황:**\n"
                for client_ip, count, command, state in connections:
                    result += f"- {client_ip}: {count}개 연결 ({command}, {state})\n"
                result += "\n"

            # 2단계: 장시간 실행 쿼리 확인
            cursor.execute("""
                SELECT 
                    id, user, host, db, command, time, state, 
                    LEFT(info, 100) as query_preview
                FROM information_schema.processlist 
                WHERE time > 30 AND command != 'Sleep'
                ORDER BY time DESC
                LIMIT 5
            """)

            long_queries = cursor.fetchall()
            if long_queries:
                result += "⏰ **장시간 실행 쿼리:**\n"
                for query_id, user, host, db, command, time_sec, state, query in long_queries:
                    result += f"- ID {query_id}: {user}@{host} ({time_sec}초)\n"
                    result += f"  쿼리: {query}\n\n"

            cursor.close()
            connection.close()

            if tunnel_used:
                self.cleanup_ssh_tunnel()

            # Claude AI 분석 요청
            ai_analysis = await self.analyze_with_claude(
                result, 
                "연결 수 급증으로 인한 성능 저하 상황 분석", 
                "troubleshooting"
            )
            result += f"\n{ai_analysis}"

            return result

        except Exception as e:
            return f"❌ 연결 이슈 진단 실패: {str(e)}"

    async def diagnose_io_bottleneck(self, database_secret: str) -> str:
        """시나리오 2: 대용량 배치 작업으로 인한 I/O 병목 진단"""
        try:
            connection, tunnel_used = self.get_db_connection(database_secret, self.selected_database, use_ssh_tunnel=True)
            cursor = connection.cursor()

            result = "🔍 **I/O 병목 현상 진단**\n\n"

            # InnoDB 상태 확인
            cursor.execute("SHOW ENGINE INNODB STATUS")
            innodb_status = cursor.fetchone()
            
            if innodb_status:
                status_text = innodb_status[2]
                result += "📊 **InnoDB 상태 분석:**\n"
                
                # 로그 시퀀스 번호 추출
                import re
                log_match = re.search(r'Log sequence number (\d+)', status_text)
                if log_match:
                    result += f"- Log sequence number: {log_match.group(1)}\n"
                
                # 버퍼 풀 히트율 추출
                hit_rate_match = re.search(r'Buffer pool hit rate (\d+) / (\d+)', status_text)
                if hit_rate_match:
                    hit_rate = int(hit_rate_match.group(1)) / int(hit_rate_match.group(2)) * 100
                    result += f"- Buffer pool hit rate: {hit_rate:.2f}%\n"
                
                result += "\n"

            # 현재 실행 중인 대용량 작업 확인
            cursor.execute("""
                SELECT 
                    id, user, host, db, command, time, state,
                    LEFT(info, 200) as query_preview
                FROM information_schema.processlist 
                WHERE (info LIKE '%INSERT%' OR info LIKE '%UPDATE%' OR info LIKE '%DELETE%')
                AND time > 10
                ORDER BY time DESC
                LIMIT 5
            """)

            batch_queries = cursor.fetchall()
            if batch_queries:
                result += "🔄 **대용량 배치 작업:**\n"
                for query_id, user, host, db, command, time_sec, state, query in batch_queries:
                    result += f"- ID {query_id}: {user}@{host} ({time_sec}초)\n"
                    result += f"  작업: {query}\n\n"

            cursor.close()
            connection.close()

            if tunnel_used:
                self.cleanup_ssh_tunnel()

            # Claude AI 분석 요청
            ai_analysis = await self.analyze_with_claude(
                result, 
                "대용량 배치 작업으로 인한 I/O 병목 상황 분석", 
                "troubleshooting"
            )
            result += f"\n{ai_analysis}"

            return result

        except Exception as e:
            return f"❌ I/O 병목 진단 실패: {str(e)}"

    async def diagnose_memory_pressure(self, database_secret: str) -> str:
        """시나리오 3: 메모리 부족으로 인한 성능 저하 진단"""
        try:
            connection, tunnel_used = self.get_db_connection(database_secret, self.selected_database, use_ssh_tunnel=True)
            cursor = connection.cursor()

            result = "🔍 **메모리 부족 성능 저하 진단**\n\n"

            # 메모리 관련 상태 변수 확인
            cursor.execute("""
                SHOW STATUS WHERE Variable_name IN (
                    'Innodb_buffer_pool_pages_total',
                    'Innodb_buffer_pool_pages_free',
                    'Innodb_buffer_pool_pages_dirty',
                    'Innodb_buffer_pool_read_requests',
                    'Innodb_buffer_pool_reads'
                )
            """)

            memory_stats = cursor.fetchall()
            if memory_stats:
                result += "💾 **메모리 상태:**\n"
                stats_dict = {name: value for name, value in memory_stats}
                
                total_pages = int(stats_dict.get('Innodb_buffer_pool_pages_total', 0))
                free_pages = int(stats_dict.get('Innodb_buffer_pool_pages_free', 0))
                dirty_pages = int(stats_dict.get('Innodb_buffer_pool_pages_dirty', 0))
                
                if total_pages > 0:
                    used_pages = total_pages - free_pages
                    result += f"- 총 버퍼 풀 페이지: {total_pages:,}\n"
                    result += f"- 사용 중 페이지: {used_pages:,} ({used_pages/total_pages*100:.1f}%)\n"
                    result += f"- 더티 페이지: {dirty_pages:,} ({dirty_pages/total_pages*100:.1f}%)\n"
                
                read_requests = int(stats_dict.get('Innodb_buffer_pool_read_requests', 0))
                reads = int(stats_dict.get('Innodb_buffer_pool_reads', 0))
                
                if read_requests > 0:
                    hit_rate = (1 - reads/read_requests) * 100
                    result += f"- 버퍼 풀 히트율: {hit_rate:.2f}%\n"
                
                result += "\n"

            cursor.close()
            connection.close()

            if tunnel_used:
                self.cleanup_ssh_tunnel()

            # Claude AI 분석 요청
            ai_analysis = await self.analyze_with_claude(
                result, 
                "메모리 부족으로 인한 성능 저하 상황 분석", 
                "troubleshooting"
            )
            result += f"\n{ai_analysis}"

            return result

        except Exception as e:
            return f"❌ 메모리 압박 진단 실패: {str(e)}"

    async def dynamic_troubleshooting_workflow(self, database_secret: str, user_question: str) -> str:
        """동적 워크플로우: 사용자 질문에 따른 맞춤형 진단"""
        try:
            # 먼저 Knowledge Base에서 관련 정보 조회
            knowledge_info = await self.query_knowledge_base(user_question, "troubleshooting")
            
            # 질문 분석을 통한 시나리오 분류
            question_lower = user_question.lower()
            
            if any(keyword in question_lower for keyword in ['연결', 'connection', '타임아웃', 'timeout']):
                diagnosis_result = await self.diagnose_connection_issues(database_secret)
                scenario_type = "연결 문제"
            elif any(keyword in question_lower for keyword in ['느림', 'slow', 'io', '디스크', 'disk']):
                diagnosis_result = await self.diagnose_io_bottleneck(database_secret)
                scenario_type = "I/O 병목"
            elif any(keyword in question_lower for keyword in ['메모리', 'memory', '버퍼', 'buffer']):
                diagnosis_result = await self.diagnose_memory_pressure(database_secret)
                scenario_type = "메모리 부족"
            else:
                # 일반적인 성능 분석
                performance_metrics = await self.get_database_performance_metrics(database_secret)
                diagnosis_result = performance_metrics
                scenario_type = "일반 성능 분석"

            # Claude AI에게 종합 분석 요청
            comprehensive_analysis = await self.analyze_with_claude(
                f"사용자 질문: {user_question}\n\n진단 결과:\n{diagnosis_result}",
                f"{scenario_type} 상황에서의 종합적 분석 및 해결방안 제시",
                "troubleshooting"
            )

            result = f"""🤖 **동적 워크플로우 분석 결과**

**사용자 질문:** {user_question}
**분류된 시나리오:** {scenario_type}

**Knowledge Base 참고 정보:**
{knowledge_info}

**진단 결과:**
{diagnosis_result}

**종합 분석 및 권장사항:**
{comprehensive_analysis}
"""
            return result

        except Exception as e:
            return f"❌ 동적 워크플로우 실행 실패: {str(e)}"

    def save_analysis_history(self, analysis_result: str):
        """분석 결과를 히스토리 파일에 저장"""
        try:
            history_file = CURRENT_DIR / "analyze_db_history.md"
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            with open(history_file, "a", encoding="utf-8") as f:
                f.write(f"\n## {timestamp}\n\n")
                f.write(analysis_result)
                f.write("\n\n---\n")
            
            logger.info(f"분석 결과가 {history_file}에 저장되었습니다.")
        except Exception as e:
            logger.error(f"히스토리 저장 실패: {e}")

    def update_report_titles(self, content: str) -> str:
        """보고서의 Insight 타이틀을 발견내용, 리뷰고려사항으로 변경"""
        try:
            # Insight 관련 제목들을 한국어로 변경
            content = re.sub(r'## Insights?', '## 발견내용', content, flags=re.IGNORECASE)
            content = re.sub(r'## Key Insights?', '## 발견내용', content, flags=re.IGNORECASE)
            content = re.sub(r'## Analysis Insights?', '## 발견내용', content, flags=re.IGNORECASE)
            
            # Recommendations를 리뷰고려사항으로 변경
            content = re.sub(r'## Recommendations?', '## 리뷰고려사항', content, flags=re.IGNORECASE)
            content = re.sub(r'## Key Recommendations?', '## 리뷰고려사항', content, flags=re.IGNORECASE)
            content = re.sub(r'## Action Items?', '## 리뷰고려사항', content, flags=re.IGNORECASE)
            
            return content
        except Exception as e:
            logger.error(f"보고서 제목 업데이트 실패: {e}")
            return content

    async def list_database_secrets(self, keyword: str = None) -> str:
        """AWS Secrets Manager의 데이터베이스 시크릿 목록 조회 (다중 리전 지원)"""
        try:
            regions = [self.default_region, "us-east-1", "us-west-2"]
            all_secrets = []
            
            for region in regions:
                try:
                    session = boto3.session.Session()
                    client = session.client(
                        service_name="secretsmanager",
                        region_name=region,
                        verify=False,
                    )
                    
                    # 페이지네이션 처리
                    next_token = None
                    while True:
                        if next_token:
                            response = client.list_secrets(NextToken=next_token)
                        else:
                            response = client.list_secrets()
                        
                        for secret in response.get("SecretList", []):
                            secret["Region"] = region  # 리전 정보 추가
                            all_secrets.append(secret)
                        
                        if "NextToken" not in response:
                            break
                        next_token = response["NextToken"]
                        
                except Exception as e:
                    logger.warning(f"리전 {region}에서 시크릿 조회 실패: {e}")
                    continue
            
            result = "🔐 **데이터베이스 시크릿 목록:**\n\n"
            
            filtered_secrets = []
            for secret in all_secrets:
                secret_name = secret.get("Name", "")
                if keyword:
                    if keyword.lower() in secret_name.lower():
                        filtered_secrets.append(secret)
                else:
                    # 데이터베이스 관련 시크릿만 필터링
                    if any(db_keyword in secret_name.lower() for db_keyword in ['db', 'database', 'mysql', 'rds', 'aurora']):
                        filtered_secrets.append(secret)
            
            if filtered_secrets:
                for i, secret in enumerate(filtered_secrets, 1):
                    result += f"{i}. **{secret['Name']}** ({secret['Region']})\n"
                    if secret.get('Description'):
                        result += f"   - 설명: {secret['Description']}\n"
                    result += f"   - 생성일: {secret.get('CreatedDate', 'N/A')}\n\n"
            else:
                result += "조건에 맞는 시크릿을 찾을 수 없습니다.\n"
            
            return result
            
        except Exception as e:
            return f"시크릿 목록 조회 실패: {str(e)}"

    def setup_cloudwatch_client(self, region_name: str = None):
        """CloudWatch 클라이언트 설정"""
        try:
            if region_name is None:
                region_name = self.default_region
            self.cloudwatch = boto3.client('cloudwatch', region_name=region_name)
            return True
        except Exception as e:
            logger.error(f"CloudWatch 클라이언트 설정 실패: {e}")
            return False

    async def collect_metrics(self, db_instance_identifier: str, hours: int = 24, 
                            metrics: Optional[List[str]] = None, region: str = None) -> str:
        """CloudWatch에서 데이터베이스 메트릭 수집"""
        try:
            if region is None:
                region = self.default_region
                
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
                        Namespace='AWS/RDS',
                        MetricName=metric,
                        Dimensions=[
                            {
                                'Name': 'DBInstanceIdentifier',
                                'Value': db_instance_identifier
                            },
                        ],
                        StartTime=start_time,
                        EndTime=end_time,
                        Period=300,  # 5분 간격
                        Statistics=['Average']
                    )
                    
                    # 응답에서 데이터 추출
                    for point in response['Datapoints']:
                        data.append({
                            'Timestamp': point['Timestamp'].replace(tzinfo=None),
                            'Metric': metric,
                            'Value': point['Average']
                        })
                except Exception as e:
                    logger.error(f"메트릭 {metric} 수집 실패: {str(e)}")

            if not data:
                return "수집된 데이터가 없습니다."

            # 데이터프레임 생성
            df = pd.DataFrame(data)
            df = df.sort_values('Timestamp')

            # 피벗 테이블 생성
            pivot_df = df.pivot(index='Timestamp', columns='Metric', values='Value')

            # CSV 파일로 저장
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            csv_file = DATA_DIR / f'database_metrics_{db_instance_identifier}_{timestamp}.csv'
            pivot_df.to_csv(csv_file)

            return f"✅ 메트릭 수집 완료\n📊 수집된 메트릭: {len(metrics)}개\n📈 데이터 포인트: {len(data)}개\n💾 저장 위치: {csv_file}"

        except Exception as e:
            return f"메트릭 수집 중 오류 발생: {str(e)}"

    async def analyze_correlation(self, csv_file: str, target_metric: str = 'CPUUtilization', 
                                top_n: int = 10) -> str:
        """메트릭 간 상관관계 분석"""
        try:
            # CSV 파일 경로 처리
            if not csv_file.startswith('/'):
                csv_path = DATA_DIR / csv_file
            else:
                csv_path = Path(csv_file)

            if not csv_path.exists():
                return f"CSV 파일을 찾을 수 없습니다: {csv_path}"

            # 데이터 읽기
            df = pd.read_csv(csv_path, index_col='Timestamp', parse_dates=True)
            df = df.dropna()

            if target_metric not in df.columns:
                return f"타겟 메트릭 '{target_metric}'이 데이터에 없습니다.\n사용 가능한 메트릭: {list(df.columns)}"

            # 상관 분석
            correlation_matrix = df.corr()
            target_correlations = correlation_matrix[target_metric].abs()
            target_correlations = target_correlations.drop(target_metric, errors='ignore')
            top_correlations = target_correlations.nlargest(top_n)

            # 결과 문자열 생성
            result = f"📊 {target_metric}과 상관관계가 높은 상위 {top_n}개 메트릭:\n\n"
            for metric, correlation in top_correlations.items():
                result += f"• {metric}: {correlation:.4f}\n"

            # 시각화
            plt.figure(figsize=(12, 6))
            top_correlations.plot(kind='bar')
            plt.title(f'Top {top_n} Metrics Correlated with {target_metric}')
            plt.xlabel('Metrics')
            plt.ylabel('Correlation Coefficient')
            plt.xticks(rotation=45, ha='right')
            plt.tight_layout()
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            graph_file = OUTPUT_DIR / f'correlation_analysis_{target_metric}_{timestamp}.png'
            plt.savefig(graph_file, dpi=300, bbox_inches='tight')
            plt.close()

            result += f"\n📈 상관관계 그래프가 저장되었습니다: {graph_file}"
            return result

        except Exception as e:
            return f"상관관계 분석 중 오류 발생: {str(e)}"

    async def detect_outliers(self, csv_file: str, std_threshold: float = 2.0) -> str:
        """아웃라이어 탐지"""
        try:
            # CSV 파일 경로 처리
            if not csv_file.startswith('/'):
                csv_path = DATA_DIR / csv_file
            else:
                csv_path = Path(csv_file)

            if not csv_path.exists():
                return f"CSV 파일을 찾을 수 없습니다: {csv_path}"

            # 데이터 읽기
            df = pd.read_csv(csv_path, index_col='Timestamp', parse_dates=True)
            df = df.dropna()

            # 각 메트릭에 대한 통계 계산
            stats = df.agg(['mean', 'max', 'min', 'std'])

            result = "📊 각 메트릭의 통계:\n"
            result += stats.to_string() + "\n\n"

            result += f"🚨 아웃라이어 탐지 결과 (임계값: ±{std_threshold}σ):\n\n"

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
                    
                    outlier_summary.append({
                        'metric': column,
                        'count': len(outliers),
                        'percentage': (len(outliers) / len(series)) * 100
                    })
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

    async def perform_regression_analysis(self, csv_file: str, predictor_metric: str, 
                                        target_metric: str = 'CPUUtilization') -> str:
        """회귀 분석 수행"""
        try:
            # CSV 파일 경로 처리
            if not csv_file.startswith('/'):
                csv_path = DATA_DIR / csv_file
            else:
                csv_path = Path(csv_file)

            if not csv_path.exists():
                return f"CSV 파일을 찾을 수 없습니다: {csv_path}"

            # 데이터 읽기
            df = pd.read_csv(csv_path, index_col='Timestamp', parse_dates=True)

            # 필요한 메트릭 확인
            if predictor_metric not in df.columns or target_metric not in df.columns:
                return f"필요한 메트릭이 데이터에 없습니다.\n사용 가능한 메트릭: {list(df.columns)}"

            # 데이터 준비
            X = df[predictor_metric].values.reshape(-1, 1)
            y = df[target_metric].values

            # NaN 값 처리
            imputer = SimpleImputer(strategy='mean')
            X = imputer.fit_transform(X)
            y = imputer.fit_transform(y.reshape(-1, 1)).ravel()

            # 데이터 분할
            X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

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

            # 그래프 그리기
            plt.figure(figsize=(12, 8))
            
            # 산점도
            plt.subplot(2, 1, 1)
            plt.scatter(X_test, y_test, color='blue', alpha=0.6, label='실제 데이터')
            
            # 예측 곡선을 위한 정렬된 데이터
            X_plot = np.linspace(X_test.min(), X_test.max(), 100).reshape(-1, 1)
            X_plot_poly = poly_features.transform(X_plot)
            y_plot_pred = model.predict(X_plot_poly)
            
            plt.plot(X_plot, y_plot_pred, color='red', linewidth=2, label='예측 모델')
            plt.title(f'{predictor_metric} vs {target_metric} 회귀 분석')
            plt.xlabel(predictor_metric)
            plt.ylabel(target_metric)
            plt.legend()
            plt.grid(True, alpha=0.3)

            # 잔차 플롯
            plt.subplot(2, 1, 2)
            residuals = y_test - y_pred
            plt.scatter(y_pred, residuals, color='green', alpha=0.6)
            plt.axhline(y=0, color='red', linestyle='--')
            plt.title('잔차 플롯 (Residual Plot)')
            plt.xlabel('예측값')
            plt.ylabel('잔차')
            plt.grid(True, alpha=0.3)

            plt.tight_layout()
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            graph_file = OUTPUT_DIR / f'regression_analysis_{predictor_metric}_{target_metric}_{timestamp}.png'
            plt.savefig(graph_file, dpi=300, bbox_inches='tight')
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
        try:
            # CSV 파일 경로 처리
            if not csv_file.startswith('/'):
                csv_path = DATA_DIR / csv_file
            else:
                csv_path = Path(csv_file)

            if not csv_path.exists():
                return f"CSV 파일을 찾을 수 없습니다: {csv_path}"

            # 데이터 읽기
            df = pd.read_csv(csv_path, index_col='Timestamp', parse_dates=True)
            
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

    async def generate_correlation_report(self, csv_file: str, target_metrics: List[str] = None) -> str:
        """상관관계 상세 분석 및 HTML 리포트 생성"""
        try:
            # CSV 파일 경로 처리
            if not csv_file.startswith('/'):
                csv_path = DATA_DIR / csv_file
            else:
                csv_path = Path(csv_file)

            if not csv_path.exists():
                return f"CSV 파일을 찾을 수 없습니다: {csv_path}"

            # 데이터 읽기
            df = pd.read_csv(csv_path, index_col='Timestamp', parse_dates=True)
            df = df.dropna()

            # 기본 타겟 메트릭 설정
            if target_metrics is None:
                target_metrics = ['CPUUtilization', 'DatabaseConnections', 'WriteIOPS', 'ReadIOPS']
            
            # 존재하는 메트릭만 필터링
            available_metrics = [m for m in target_metrics if m in df.columns]
            if not available_metrics:
                return f"지정된 타겟 메트릭이 데이터에 없습니다. 사용 가능한 메트릭: {list(df.columns)}"

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            report_file = OUTPUT_DIR / f'correlation_detailed_report_{timestamp}.html'
            
            # 성능 문제 진단 수행
            performance_analysis = self._analyze_performance_issues(df)
            outlier_analysis = self._detect_outliers_for_report(df)
            
            # HTML 리포트 생성
            html_content = self._generate_correlation_html(df, available_metrics, csv_file, performance_analysis, outlier_analysis)
            
            with open(report_file, 'w', encoding='utf-8') as f:
                f.write(html_content)

            # 요약 결과 생성
            correlation_matrix = df.corr()
            result = f"📊 상관관계 상세 분석 완료\n\n"
            result += f"📁 분석 파일: {csv_file}\n"
            result += f"📅 데이터 기간: {df.index.min()} ~ {df.index.max()}\n"
            result += f"📈 데이터 포인트: {len(df)}개\n"
            result += f"📋 분석 메트릭: {len(available_metrics)}개\n\n"

            # 각 타겟 메트릭별 상위 상관관계
            for target in available_metrics:
                target_corr = correlation_matrix[target].abs().drop(target, errors='ignore')
                top_3 = target_corr.nlargest(3)
                result += f"🎯 {target} 상위 상관관계:\n"
                for metric, corr in top_3.items():
                    result += f"  • {metric}: {corr:.4f}\n"
                result += "\n"

            result += f"📄 상세 리포트: {report_file}\n"
            result += f"📊 리포트에는 히트맵, 산점도, 시계열 분석이 포함되어 있습니다."

            return result

        except Exception as e:
            return f"상관관계 리포트 생성 중 오류 발생: {str(e)}"

    def _analyze_performance_issues(self, df: pd.DataFrame) -> dict:
        """성능 문제 분석"""
        analysis = {
            'cpu_spikes': [],
            'connection_issues': [],
            'io_bottlenecks': [],
            'memory_pressure': [],
            'key_findings': []
        }
        
        try:
            # CPU 급증 탐지
            if 'CPUUtilization' in df.columns:
                cpu_mean = df['CPUUtilization'].mean()
                cpu_std = df['CPUUtilization'].std()
                cpu_threshold = cpu_mean + 2 * cpu_std
                cpu_spikes = df[df['CPUUtilization'] > cpu_threshold]
                
                for idx, row in cpu_spikes.iterrows():
                    analysis['cpu_spikes'].append({
                        'timestamp': idx.strftime('%Y-%m-%d %H:%M:%S'),
                        'value': row['CPUUtilization'],
                        'severity': 'High' if row['CPUUtilization'] > cpu_mean + 3 * cpu_std else 'Medium'
                    })
            
            # 연결 수 문제 탐지
            if 'DatabaseConnections' in df.columns:
                conn_mean = df['DatabaseConnections'].mean()
                conn_std = df['DatabaseConnections'].std()
                conn_threshold = conn_mean + 2 * conn_std
                conn_spikes = df[df['DatabaseConnections'] > conn_threshold]
                
                for idx, row in conn_spikes.iterrows():
                    analysis['connection_issues'].append({
                        'timestamp': idx.strftime('%Y-%m-%d %H:%M:%S'),
                        'value': row['DatabaseConnections'],
                        'severity': 'High' if row['DatabaseConnections'] > conn_mean + 3 * conn_std else 'Medium'
                    })
            
            # I/O 병목 탐지
            if 'ReadIOPS' in df.columns and 'WriteIOPS' in df.columns:
                read_mean = df['ReadIOPS'].mean()
                read_std = df['ReadIOPS'].std()
                write_mean = df['WriteIOPS'].mean()
                write_std = df['WriteIOPS'].std()
                
                read_spikes = df[df['ReadIOPS'] > read_mean + 2 * read_std]
                write_spikes = df[df['WriteIOPS'] > write_mean + 2 * write_std]
                
                for idx, row in read_spikes.iterrows():
                    analysis['io_bottlenecks'].append({
                        'timestamp': idx.strftime('%Y-%m-%d %H:%M:%S'),
                        'type': 'Read',
                        'value': row['ReadIOPS'],
                        'severity': 'High' if row['ReadIOPS'] > read_mean + 3 * read_std else 'Medium'
                    })
                
                for idx, row in write_spikes.iterrows():
                    analysis['io_bottlenecks'].append({
                        'timestamp': idx.strftime('%Y-%m-%d %H:%M:%S'),
                        'type': 'Write',
                        'value': row['WriteIOPS'],
                        'severity': 'High' if row['WriteIOPS'] > write_mean + 3 * write_std else 'Medium'
                    })
            
            # 메모리 압박 탐지
            if 'FreeableMemory' in df.columns:
                mem_mean = df['FreeableMemory'].mean()
                mem_std = df['FreeableMemory'].std()
                mem_threshold = mem_mean - 2 * mem_std
                mem_pressure = df[df['FreeableMemory'] < mem_threshold]
                
                for idx, row in mem_pressure.iterrows():
                    analysis['memory_pressure'].append({
                        'timestamp': idx.strftime('%Y-%m-%d %H:%M:%S'),
                        'value': row['FreeableMemory'],
                        'severity': 'High' if row['FreeableMemory'] < mem_mean - 3 * mem_std else 'Medium'
                    })
            
            # 주요 발견사항 생성
            if analysis['cpu_spikes']:
                analysis['key_findings'].append(f"CPU 사용률 급증 {len(analysis['cpu_spikes'])}회 탐지")
            if analysis['connection_issues']:
                analysis['key_findings'].append(f"연결 수 급증 {len(analysis['connection_issues'])}회 탐지")
            if analysis['io_bottlenecks']:
                analysis['key_findings'].append(f"I/O 병목 현상 {len(analysis['io_bottlenecks'])}회 탐지")
            if analysis['memory_pressure']:
                analysis['key_findings'].append(f"메모리 압박 상황 {len(analysis['memory_pressure'])}회 탐지")
                
        except Exception as e:
            analysis['error'] = str(e)
        
        return analysis
    
    def _detect_outliers_for_report(self, df: pd.DataFrame, std_threshold: float = 2.0) -> dict:
        """리포트용 아웃라이어 탐지"""
        outliers = {}
        
        try:
            for column in df.columns:
                if df[column].dtype in ['float64', 'int64']:
                    mean_val = df[column].mean()
                    std_val = df[column].std()
                    
                    if std_val > 0:
                        upper_bound = mean_val + std_threshold * std_val
                        lower_bound = mean_val - std_threshold * std_val
                        
                        outlier_data = df[(df[column] > upper_bound) | (df[column] < lower_bound)]
                        
                        if len(outlier_data) > 0:
                            outliers[column] = {
                                'count': len(outlier_data),
                                'percentage': (len(outlier_data) / len(df)) * 100,
                                'values': []
                            }
                            
                            # 상위 5개만 저장
                            for idx, row in outlier_data.head(5).iterrows():
                                outliers[column]['values'].append({
                                    'timestamp': idx.strftime('%Y-%m-%d %H:%M:%S'),
                                    'value': row[column],
                                    'deviation': abs(row[column] - mean_val) / std_val
                                })
        except Exception as e:
            outliers['error'] = str(e)
        
        return outliers

    def _generate_correlation_html(self, df: pd.DataFrame, target_metrics: List[str], csv_file: str, 
                                 performance_analysis: dict, outlier_analysis: dict) -> str:
        """상관관계 분석 HTML 리포트 생성"""
        correlation_matrix = df.corr()
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # 히트맵 생성
        plt.figure(figsize=(12, 10))
        if sns is not None:
            sns.heatmap(correlation_matrix, annot=True, cmap='coolwarm', center=0, 
                       square=True, fmt='.2f', cbar_kws={'shrink': 0.8})
        else:
            im = plt.imshow(correlation_matrix, cmap='coolwarm', aspect='auto')
            plt.colorbar(im, shrink=0.8)
            for i in range(len(correlation_matrix.columns)):
                for j in range(len(correlation_matrix.columns)):
                    plt.text(j, i, f'{correlation_matrix.iloc[i, j]:.2f}', 
                            ha='center', va='center', fontsize=8)
            plt.xticks(range(len(correlation_matrix.columns)), correlation_matrix.columns, rotation=45)
            plt.yticks(range(len(correlation_matrix.columns)), correlation_matrix.columns)
        
        plt.title('Correlation Matrix Heatmap')
        plt.tight_layout()
        heatmap_file = f'correlation_heatmap_{timestamp}.png'
        plt.savefig(OUTPUT_DIR / heatmap_file, dpi=300, bbox_inches='tight')
        plt.close()

        # 시계열 차트 생성 (모든 메트릭)
        plt.figure(figsize=(20, 12))
        metrics_to_plot = list(df.columns)[:12]  # 최대 12개 메트릭
        rows = 4
        cols = 3
        
        for i, metric in enumerate(metrics_to_plot):
            plt.subplot(rows, cols, i+1)
            plt.plot(df.index, df[metric], label=metric, linewidth=1, alpha=0.8)
            plt.title(f'{metric}', fontsize=10)
            plt.xlabel('Time', fontsize=8)
            plt.ylabel('Value', fontsize=8)
            plt.xticks(rotation=45, fontsize=8)
            plt.yticks(fontsize=8)
            plt.grid(True, alpha=0.3)
            
            # 아웃라이어 표시
            if metric in outlier_analysis:
                for outlier in outlier_analysis[metric]['values'][:3]:
                    outlier_time = pd.to_datetime(outlier['timestamp'])
                    if outlier_time in df.index:
                        plt.scatter(outlier_time, outlier['value'], color='red', s=50, alpha=0.7)
        
        plt.tight_layout()
        timeseries_file = f'correlation_timeseries_{timestamp}.png'
        plt.savefig(OUTPUT_DIR / timeseries_file, dpi=300, bbox_inches='tight')
        plt.close()

        # 성능 문제 시각화
        plt.figure(figsize=(15, 10))
        
        # CPU와 DBLoad 관계
        if 'CPUUtilization' in df.columns and 'DBLoad' in df.columns:
            plt.subplot(2, 2, 1)
            plt.scatter(df['CPUUtilization'], df['DBLoad'], alpha=0.6)
            plt.xlabel('CPU Utilization (%)')
            plt.ylabel('DB Load')
            plt.title('CPU vs DB Load')
            plt.grid(True, alpha=0.3)
        
        # 연결 수와 네트워크 처리량
        if 'DatabaseConnections' in df.columns and 'NetworkTransmitThroughput' in df.columns:
            plt.subplot(2, 2, 2)
            plt.scatter(df['DatabaseConnections'], df['NetworkTransmitThroughput'], alpha=0.6)
            plt.xlabel('Database Connections')
            plt.ylabel('Network Transmit Throughput')
            plt.title('Connections vs Network')
            plt.grid(True, alpha=0.3)
        
        # I/O 패턴
        if 'ReadIOPS' in df.columns and 'WriteIOPS' in df.columns:
            plt.subplot(2, 2, 3)
            plt.scatter(df['ReadIOPS'], df['WriteIOPS'], alpha=0.6)
            plt.xlabel('Read IOPS')
            plt.ylabel('Write IOPS')
            plt.title('Read vs Write IOPS')
            plt.grid(True, alpha=0.3)
        
        # 메모리와 CPU
        if 'FreeableMemory' in df.columns and 'CPUUtilization' in df.columns:
            plt.subplot(2, 2, 4)
            plt.scatter(df['FreeableMemory'], df['CPUUtilization'], alpha=0.6)
            plt.xlabel('Freeable Memory')
            plt.ylabel('CPU Utilization (%)')
            plt.title('Memory vs CPU')
            plt.grid(True, alpha=0.3)
        
        plt.tight_layout()
        scatter_file = f'performance_scatter_{timestamp}.png'
        plt.savefig(OUTPUT_DIR / scatter_file, dpi=300, bbox_inches='tight')
        plt.close()

        # HTML 템플릿
        html_template = f"""
<!DOCTYPE html>
<html lang="ko">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>데이터베이스 성능 분석 리포트 - {csv_file}</title>
    <style>
        body {{ font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; margin: 0; padding: 20px; background-color: #f5f5f5; }}
        .container {{ max-width: 1400px; margin: 0 auto; background: white; padding: 30px; border-radius: 10px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }}
        .header {{ text-align: center; margin-bottom: 30px; padding: 20px; background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; border-radius: 10px; }}
        .section {{ margin: 30px 0; padding: 20px; border: 1px solid #e0e0e0; border-radius: 8px; }}
        .alert {{ padding: 15px; margin: 10px 0; border-radius: 5px; }}
        .alert-danger {{ background-color: #f8d7da; border-color: #f5c6cb; color: #721c24; }}
        .alert-warning {{ background-color: #fff3cd; border-color: #ffeaa7; color: #856404; }}
        .alert-info {{ background-color: #d1ecf1; border-color: #bee5eb; color: #0c5460; }}
        .metric-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 20px; margin: 20px 0; }}
        .metric-card {{ padding: 15px; background: #f8f9fa; border-radius: 8px; border-left: 4px solid #007bff; }}
        .performance-card {{ padding: 15px; background: #fff5f5; border-radius: 8px; border-left: 4px solid #dc3545; }}
        .correlation-table {{ width: 100%; border-collapse: collapse; margin: 20px 0; }}
        .correlation-table th, .correlation-table td {{ padding: 10px; text-align: left; border-bottom: 1px solid #ddd; }}
        .correlation-table th {{ background-color: #f8f9fa; font-weight: bold; }}
        .high-corr {{ color: #dc3545; font-weight: bold; }}
        .medium-corr {{ color: #fd7e14; font-weight: bold; }}
        .low-corr {{ color: #28a745; }}
        .chart-container {{ text-align: center; margin: 20px 0; }}
        .chart-container img {{ max-width: 100%; height: auto; border-radius: 8px; box-shadow: 0 2px 8px rgba(0,0,0,0.1); }}
        .summary-stats {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 15px; }}
        .stat-box {{ text-align: center; padding: 15px; background: #e3f2fd; border-radius: 8px; }}
        .stat-number {{ font-size: 24px; font-weight: bold; color: #1976d2; }}
        .stat-label {{ font-size: 14px; color: #666; }}
        .issue-list {{ list-style: none; padding: 0; }}
        .issue-item {{ padding: 10px; margin: 5px 0; background: #fff; border-left: 4px solid #dc3545; border-radius: 4px; }}
        .issue-high {{ border-left-color: #dc3545; }}
        .issue-medium {{ border-left-color: #fd7e14; }}
        .timestamp {{ font-family: monospace; color: #666; font-size: 0.9em; }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>📊 데이터베이스 성능 분석 리포트</h1>
            <p>파일: {csv_file} | 생성일: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        </div>

        <div class="section">
            <h2>📈 데이터 개요</h2>
            <div class="summary-stats">
                <div class="stat-box">
                    <div class="stat-number">{len(df)}</div>
                    <div class="stat-label">데이터 포인트</div>
                </div>
                <div class="stat-box">
                    <div class="stat-number">{len(df.columns)}</div>
                    <div class="stat-label">메트릭 수</div>
                </div>
                <div class="stat-box">
                    <div class="stat-number">{len(target_metrics)}</div>
                    <div class="stat-label">분석 대상</div>
                </div>
                <div class="stat-box">
                    <div class="stat-number">{(df.index.max() - df.index.min()).days + 1}</div>
                    <div class="stat-label">분석 일수</div>
                </div>
            </div>
        </div>

        <div class="section">
            <h2>🚨 성능 문제 진단</h2>
        """

        # 성능 문제 진단 결과 추가
        if performance_analysis.get('key_findings'):
            html_template += '<div class="alert alert-danger"><h3>주요 발견사항</h3><ul>'
            for finding in performance_analysis['key_findings']:
                html_template += f'<li>{finding}</li>'
            html_template += '</ul></div>'

        html_template += '<div class="metric-grid">'

        # CPU 급증 이벤트
        if performance_analysis.get('cpu_spikes'):
            html_template += '''
                <div class="performance-card">
                    <h3>🔥 CPU 사용률 급증</h3>
                    <ul class="issue-list">
            '''
            for spike in performance_analysis['cpu_spikes'][:5]:
                severity_class = 'issue-high' if spike['severity'] == 'High' else 'issue-medium'
                html_template += f'''
                    <li class="issue-item {severity_class}">
                        <div class="timestamp">{spike['timestamp']}</div>
                        <div>CPU: {spike['value']:.2f}% ({spike['severity']})</div>
                    </li>
                '''
            html_template += '</ul></div>'

        # 연결 수 문제
        if performance_analysis.get('connection_issues'):
            html_template += '''
                <div class="performance-card">
                    <h3>🔗 연결 수 급증</h3>
                    <ul class="issue-list">
            '''
            for issue in performance_analysis['connection_issues'][:5]:
                severity_class = 'issue-high' if issue['severity'] == 'High' else 'issue-medium'
                html_template += f'''
                    <li class="issue-item {severity_class}">
                        <div class="timestamp">{issue['timestamp']}</div>
                        <div>연결 수: {issue['value']:.0f} ({issue['severity']})</div>
                    </li>
                '''
            html_template += '</ul></div>'

        # I/O 병목
        if performance_analysis.get('io_bottlenecks'):
            html_template += '''
                <div class="performance-card">
                    <h3>💾 I/O 병목 현상</h3>
                    <ul class="issue-list">
            '''
            for bottleneck in performance_analysis['io_bottlenecks'][:5]:
                severity_class = 'issue-high' if bottleneck['severity'] == 'High' else 'issue-medium'
                html_template += f'''
                    <li class="issue-item {severity_class}">
                        <div class="timestamp">{bottleneck['timestamp']}</div>
                        <div>{bottleneck['type']} IOPS: {bottleneck['value']:.2f} ({bottleneck['severity']})</div>
                    </li>
                '''
            html_template += '</ul></div>'

        # 메모리 압박
        if performance_analysis.get('memory_pressure'):
            html_template += '''
                <div class="performance-card">
                    <h3>🧠 메모리 압박</h3>
                    <ul class="issue-list">
            '''
            for pressure in performance_analysis['memory_pressure'][:5]:
                severity_class = 'issue-high' if pressure['severity'] == 'High' else 'issue-medium'
                html_template += f'''
                    <li class="issue-item {severity_class}">
                        <div class="timestamp">{pressure['timestamp']}</div>
                        <div>여유 메모리: {pressure['value']:,.0f} bytes ({pressure['severity']})</div>
                    </li>
                '''
            html_template += '</ul></div>'

        html_template += '</div></div>'

        # 아웃라이어 분석 섹션
        html_template += '''
        <div class="section">
            <h2>📊 아웃라이어 분석</h2>
            <div class="metric-grid">
        '''

        for metric, data in outlier_analysis.items():
            if metric != 'error' and data['count'] > 0:
                html_template += f'''
                    <div class="metric-card">
                        <h3>{metric}</h3>
                        <p>아웃라이어: {data['count']}개 ({data['percentage']:.1f}%)</p>
                        <ul class="issue-list">
                '''
                for outlier in data['values']:
                    html_template += f'''
                        <li class="issue-item">
                            <div class="timestamp">{outlier['timestamp']}</div>
                            <div>값: {outlier['value']:.4f} (편차: {outlier['deviation']:.2f}σ)</div>
                        </li>
                    '''
                html_template += '</ul></div>'

        html_template += '</div></div>'

        # 차트 섹션들
        html_template += f'''
        <div class="section">
            <h2>🔥 상관관계 히트맵</h2>
            <div class="chart-container">
                <img src="{heatmap_file}" alt="Correlation Heatmap">
            </div>
            <p>히트맵에서 빨간색은 양의 상관관계, 파란색은 음의 상관관계를 나타냅니다. 색이 진할수록 상관관계가 강합니다.</p>
        </div>

        <div class="section">
            <h2>📊 전체 메트릭 시계열 분석</h2>
            <div class="chart-container">
                <img src="{timeseries_file}" alt="Time Series Analysis">
            </div>
            <p>모든 메트릭의 시간에 따른 변화 패턴을 보여줍니다. 빨간 점은 아웃라이어를 나타냅니다.</p>
        </div>

        <div class="section">
            <h2>🎯 성능 메트릭 상관관계</h2>
            <div class="chart-container">
                <img src="{scatter_file}" alt="Performance Scatter Plots">
            </div>
            <p>주요 성능 메트릭 간의 상관관계를 산점도로 표시합니다.</p>
        </div>

        <div class="section">
            <h2>🎯 주요 상관관계 분석</h2>
            <div class="metric-grid">
        '''

        # 각 타겟 메트릭별 상관관계 카드 생성
        for target in target_metrics:
            if target in df.columns:
                target_corr = correlation_matrix[target].abs().drop(target, errors='ignore')
                top_5 = target_corr.nlargest(5)
                
                html_template += f'''
                    <div class="metric-card">
                        <h3>{target}</h3>
                        <table class="correlation-table">
                            <tr><th>메트릭</th><th>상관계수</th><th>강도</th></tr>
                '''
                
                for metric, corr in top_5.items():
                    if corr >= 0.8:
                        corr_class = "high-corr"
                        strength = "매우 강함"
                    elif corr >= 0.5:
                        corr_class = "medium-corr"
                        strength = "보통"
                    else:
                        corr_class = "low-corr"
                        strength = "약함"
                    
                    html_template += f'''
                            <tr>
                                <td>{metric}</td>
                                <td class="{corr_class}">{corr:.4f}</td>
                                <td>{strength}</td>
                            </tr>
                    '''
                
                html_template += '''
                        </table>
                    </div>
                '''

        html_template += '''
            </div>
        </div>

        <div class="section">
            <h2>💡 분석 결과 및 권장사항</h2>
            <div class="metric-grid">
        '''

        # 분석 결과 및 권장사항 생성
        high_correlations = []
        for target in target_metrics:
            if target in df.columns:
                target_corr = correlation_matrix[target].abs().drop(target, errors='ignore')
                high_corr_metrics = target_corr[target_corr >= 0.8]
                if len(high_corr_metrics) > 0:
                    high_correlations.append((target, high_corr_metrics))

        if high_correlations:
            html_template += '''
                <div class="metric-card">
                    <h3>🔍 주요 발견사항</h3>
                    <ul>
            '''
            for target, corr_metrics in high_correlations[:3]:
                html_template += f"<li><strong>{target}</strong>는 {len(corr_metrics)}개 메트릭과 강한 상관관계</li>"
            html_template += '''
                    </ul>
                </div>
                <div class="metric-card">
                    <h3>⚡ 성능 최적화 권장사항</h3>
                    <ul>
                        <li>높은 상관관계를 보이는 메트릭들을 함께 모니터링</li>
                        <li>CPU 사용률과 연관된 메트릭들의 임계값 설정</li>
                        <li>연결 수 증가 시 네트워크 대역폭 모니터링 강화</li>
                        <li>I/O 메트릭과 스토리지 성능 지표 연계 분석</li>
                        <li>아웃라이어 발생 시점의 슬로우 쿼리 로그 분석</li>
                        <li>Performance Insights를 통한 상세 쿼리 분석</li>
                    </ul>
                </div>
            '''

        html_template += '''
            </div>
        </div>
    </div>
</body>
</html>
        '''

        return html_template
        """상관관계 분석 HTML 리포트 생성"""
        correlation_matrix = df.corr()
        
        # 히트맵 생성
        plt.figure(figsize=(12, 10))
        if sns is not None:
            sns.heatmap(correlation_matrix, annot=True, cmap='coolwarm', center=0, 
                       square=True, fmt='.2f', cbar_kws={'shrink': 0.8})
        else:
            # seaborn이 없을 경우 matplotlib으로 대체
            im = plt.imshow(correlation_matrix, cmap='coolwarm', aspect='auto')
            plt.colorbar(im, shrink=0.8)
            # 상관계수 값을 텍스트로 표시
            for i in range(len(correlation_matrix.columns)):
                for j in range(len(correlation_matrix.columns)):
                    plt.text(j, i, f'{correlation_matrix.iloc[i, j]:.2f}', 
                            ha='center', va='center', fontsize=8)
            plt.xticks(range(len(correlation_matrix.columns)), correlation_matrix.columns, rotation=45)
            plt.yticks(range(len(correlation_matrix.columns)), correlation_matrix.columns)
        
        plt.title('Correlation Matrix Heatmap')
        plt.tight_layout()
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        heatmap_file = f'correlation_heatmap_{timestamp}.png'
        plt.savefig(OUTPUT_DIR / heatmap_file, dpi=300, bbox_inches='tight')
        plt.close()

        # 시계열 차트 생성
        plt.figure(figsize=(15, 8))
        for i, metric in enumerate(target_metrics[:4]):  # 최대 4개만 표시
            plt.subplot(2, 2, i+1)
            plt.plot(df.index, df[metric], label=metric, linewidth=1)
            plt.title(f'{metric} Time Series')
            plt.xlabel('Time')
            plt.ylabel('Value')
            plt.xticks(rotation=45)
            plt.grid(True, alpha=0.3)
        
        plt.tight_layout()
        timeseries_file = f'correlation_timeseries_{timestamp}.png'
        plt.savefig(OUTPUT_DIR / timeseries_file, dpi=300, bbox_inches='tight')
        plt.close()

        # HTML 템플릿
        html_template = f"""
<!DOCTYPE html>
<html lang="ko">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>상관관계 분석 리포트 - {csv_file}</title>
    <style>
        body {{ font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; margin: 0; padding: 20px; background-color: #f5f5f5; }}
        .container {{ max-width: 1200px; margin: 0 auto; background: white; padding: 30px; border-radius: 10px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }}
        .header {{ text-align: center; margin-bottom: 30px; padding: 20px; background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; border-radius: 10px; }}
        .section {{ margin: 30px 0; padding: 20px; border: 1px solid #e0e0e0; border-radius: 8px; }}
        .metric-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 20px; margin: 20px 0; }}
        .metric-card {{ padding: 15px; background: #f8f9fa; border-radius: 8px; border-left: 4px solid #007bff; }}
        .correlation-table {{ width: 100%; border-collapse: collapse; margin: 20px 0; }}
        .correlation-table th, .correlation-table td {{ padding: 10px; text-align: left; border-bottom: 1px solid #ddd; }}
        .correlation-table th {{ background-color: #f8f9fa; font-weight: bold; }}
        .high-corr {{ color: #dc3545; font-weight: bold; }}
        .medium-corr {{ color: #fd7e14; font-weight: bold; }}
        .low-corr {{ color: #28a745; }}
        .chart-container {{ text-align: center; margin: 20px 0; }}
        .chart-container img {{ max-width: 100%; height: auto; border-radius: 8px; box-shadow: 0 2px 8px rgba(0,0,0,0.1); }}
        .summary-stats {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 15px; }}
        .stat-box {{ text-align: center; padding: 15px; background: #e3f2fd; border-radius: 8px; }}
        .stat-number {{ font-size: 24px; font-weight: bold; color: #1976d2; }}
        .stat-label {{ font-size: 14px; color: #666; }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>📊 상관관계 분석 리포트</h1>
            <p>파일: {csv_file} | 생성일: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        </div>

        <div class="section">
            <h2>📈 데이터 개요</h2>
            <div class="summary-stats">
                <div class="stat-box">
                    <div class="stat-number">{len(df)}</div>
                    <div class="stat-label">데이터 포인트</div>
                </div>
                <div class="stat-box">
                    <div class="stat-number">{len(df.columns)}</div>
                    <div class="stat-label">메트릭 수</div>
                </div>
                <div class="stat-box">
                    <div class="stat-number">{len(target_metrics)}</div>
                    <div class="stat-label">분석 대상</div>
                </div>
                <div class="stat-box">
                    <div class="stat-number">{df.index.max() - df.index.min()}</div>
                    <div class="stat-label">분석 기간</div>
                </div>
            </div>
        </div>

        <div class="section">
            <h2>🔥 상관관계 히트맵</h2>
            <div class="chart-container">
                <img src="{heatmap_file}" alt="Correlation Heatmap">
            </div>
            <p>히트맵에서 빨간색은 양의 상관관계, 파란색은 음의 상관관계를 나타냅니다. 색이 진할수록 상관관계가 강합니다.</p>
        </div>

        <div class="section">
            <h2>📊 시계열 분석</h2>
            <div class="chart-container">
                <img src="{timeseries_file}" alt="Time Series Analysis">
            </div>
            <p>주요 메트릭들의 시간에 따른 변화 패턴을 보여줍니다.</p>
        </div>

        <div class="section">
            <h2>🎯 주요 상관관계 분석</h2>
            <div class="metric-grid">
        """

        # 각 타겟 메트릭별 상관관계 카드 생성
        for target in target_metrics:
            target_corr = correlation_matrix[target].abs().drop(target, errors='ignore')
            top_5 = target_corr.nlargest(5)
            
            html_template += f"""
                <div class="metric-card">
                    <h3>{target}</h3>
                    <table class="correlation-table">
                        <tr><th>메트릭</th><th>상관계수</th><th>강도</th></tr>
            """
            
            for metric, corr in top_5.items():
                if corr >= 0.8:
                    corr_class = "high-corr"
                    strength = "매우 강함"
                elif corr >= 0.5:
                    corr_class = "medium-corr"
                    strength = "보통"
                else:
                    corr_class = "low-corr"
                    strength = "약함"
                
                html_template += f"""
                        <tr>
                            <td>{metric}</td>
                            <td class="{corr_class}">{corr:.4f}</td>
                            <td>{strength}</td>
                        </tr>
                """
            
            html_template += """
                    </table>
                </div>
            """

        html_template += """
            </div>
        </div>

        <div class="section">
            <h2>💡 분석 결과 및 권장사항</h2>
            <div class="metric-grid">
        """

        # 분석 결과 및 권장사항 생성
        high_correlations = []
        for target in target_metrics:
            target_corr = correlation_matrix[target].abs().drop(target, errors='ignore')
            high_corr_metrics = target_corr[target_corr >= 0.8]
            if len(high_corr_metrics) > 0:
                high_correlations.append((target, high_corr_metrics))

        if high_correlations:
            html_template += """
                <div class="metric-card">
                    <h3>🔍 주요 발견사항</h3>
                    <ul>
            """
            for target, corr_metrics in high_correlations[:3]:
                html_template += f"<li><strong>{target}</strong>는 {len(corr_metrics)}개 메트릭과 강한 상관관계</li>"
            html_template += """
                    </ul>
                </div>
                <div class="metric-card">
                    <h3>⚡ 성능 최적화 권장사항</h3>
                    <ul>
                        <li>높은 상관관계를 보이는 메트릭들을 함께 모니터링</li>
                        <li>CPU 사용률과 연관된 메트릭들의 임계값 설정</li>
                        <li>연결 수 증가 시 네트워크 대역폭 모니터링 강화</li>
                        <li>I/O 메트릭과 스토리지 성능 지표 연계 분석</li>
                    </ul>
                </div>
            """

        html_template += """
            </div>
        </div>
    </div>
</body>
</html>
        """

        return html_template


# MCP 서버 설정
server = Server("analyze-db")

@server.list_tools()
async def handle_list_tools() -> list[types.Tool]:
    """사용 가능한 도구 목록 반환"""
    return [
        # 데이터베이스 연결 및 성능 분석 도구
        types.Tool(
            name="list_database_secrets",
            description="AWS Secrets Manager의 데이터베이스 시크릿 목록을 조회합니다",
            inputSchema={
                "type": "object",
                "properties": {
                    "keyword": {
                        "type": "string",
                        "description": "검색할 키워드 (선택사항)"
                    }
                }
            }
        ),
        types.Tool(
            name="test_database_connection",
            description="데이터베이스 연결을 테스트합니다",
            inputSchema={
                "type": "object",
                "properties": {
                    "database_secret": {
                        "type": "string",
                        "description": "데이터베이스 시크릿 이름"
                    }
                },
                "required": ["database_secret"]
            }
        ),
        types.Tool(
            name="get_database_performance_metrics",
            description="데이터베이스 성능 메트릭을 조회합니다",
            inputSchema={
                "type": "object",
                "properties": {
                    "database_secret": {
                        "type": "string",
                        "description": "데이터베이스 시크릿 이름"
                    },
                    "metric_type": {
                        "type": "string",
                        "description": "메트릭 타입 (all, query, connection, io)",
                        "default": "all"
                    }
                },
                "required": ["database_secret"]
            }
        ),
        types.Tool(
            name="get_schema_summary",
            description="데이터베이스 스키마 요약 정보를 조회합니다",
            inputSchema={
                "type": "object",
                "properties": {
                    "database_secret": {
                        "type": "string",
                        "description": "데이터베이스 시크릿 이름"
                    }
                },
                "required": ["database_secret"]
            }
        ),
        # 시나리오 기반 진단 도구
        types.Tool(
            name="diagnose_connection_issues",
            description="연결 수 급증으로 인한 성능 저하를 진단합니다",
            inputSchema={
                "type": "object",
                "properties": {
                    "database_secret": {
                        "type": "string",
                        "description": "데이터베이스 시크릿 이름"
                    }
                },
                "required": ["database_secret"]
            }
        ),
        types.Tool(
            name="diagnose_io_bottleneck",
            description="I/O 병목 현상을 진단합니다",
            inputSchema={
                "type": "object",
                "properties": {
                    "database_secret": {
                        "type": "string",
                        "description": "데이터베이스 시크릿 이름"
                    }
                },
                "required": ["database_secret"]
            }
        ),
        types.Tool(
            name="diagnose_memory_pressure",
            description="메모리 부족으로 인한 성능 저하를 진단합니다",
            inputSchema={
                "type": "object",
                "properties": {
                    "database_secret": {
                        "type": "string",
                        "description": "데이터베이스 시크릿 이름"
                    }
                },
                "required": ["database_secret"]
            }
        ),
        types.Tool(
            name="dynamic_troubleshooting_workflow",
            description="사용자 질문에 따른 동적 워크플로우로 데이터베이스 문제를 진단합니다",
            inputSchema={
                "type": "object",
                "properties": {
                    "database_secret": {
                        "type": "string",
                        "description": "데이터베이스 시크릿 이름"
                    },
                    "user_question": {
                        "type": "string",
                        "description": "사용자의 문제 상황 질문"
                    }
                },
                "required": ["database_secret", "user_question"]
            }
        ),
        # CloudWatch 메트릭 수집 및 분석 도구
        types.Tool(
            name="collect_db_metrics",
            description="CloudWatch에서 데이터베이스 메트릭을 수집합니다",
            inputSchema={
                "type": "object",
                "properties": {
                    "db_instance_identifier": {
                        "type": "string",
                        "description": "데이터베이스 인스턴스 식별자"
                    },
                    "hours": {
                        "type": "integer",
                        "description": "수집할 시간 범위 (시간 단위, 기본값: 24)",
                        "default": 24
                    },
                    "metrics": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "수집할 메트릭 목록 (선택사항)"
                    },
                    "region": {
                        "type": "string",
                        "description": "AWS 리전 (기본값: us-east-1)",
                        "default": "us-east-1"
                    }
                },
                "required": ["db_instance_identifier"]
            }
        ),
        types.Tool(
            name="analyze_metric_correlation",
            description="메트릭 간 상관관계를 분석합니다",
            inputSchema={
                "type": "object",
                "properties": {
                    "csv_file": {
                        "type": "string",
                        "description": "분석할 CSV 파일명"
                    },
                    "target_metric": {
                        "type": "string",
                        "description": "타겟 메트릭 (기본값: CPUUtilization)",
                        "default": "CPUUtilization"
                    },
                    "top_n": {
                        "type": "integer",
                        "description": "상위 N개 메트릭 (기본값: 10)",
                        "default": 10
                    }
                },
                "required": ["csv_file"]
            }
        ),
        types.Tool(
            name="generate_correlation_report",
            description="상관관계 상세 분석 및 HTML 리포트를 생성합니다",
            inputSchema={
                "type": "object",
                "properties": {
                    "csv_file": {
                        "type": "string",
                        "description": "분석할 CSV 파일명"
                    },
                    "target_metrics": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "분석할 타겟 메트릭 목록 (선택사항, 기본값: CPUUtilization, DatabaseConnections, WriteIOPS, ReadIOPS)"
                    }
                },
                "required": ["csv_file"]
            }
        ),
        types.Tool(
            name="detect_metric_outliers",
            description="메트릭 데이터에서 아웃라이어를 탐지합니다",
            inputSchema={
                "type": "object",
                "properties": {
                    "csv_file": {
                        "type": "string",
                        "description": "분석할 CSV 파일명"
                    },
                    "std_threshold": {
                        "type": "number",
                        "description": "표준편차 임계값 (기본값: 2.0)",
                        "default": 2.0
                    }
                },
                "required": ["csv_file"]
            }
        ),
        types.Tool(
            name="perform_regression_analysis",
            description="메트릭 간 회귀 분석을 수행합니다",
            inputSchema={
                "type": "object",
                "properties": {
                    "csv_file": {
                        "type": "string",
                        "description": "분석할 CSV 파일명"
                    },
                    "predictor_metric": {
                        "type": "string",
                        "description": "예측 변수 메트릭"
                    },
                    "target_metric": {
                        "type": "string",
                        "description": "타겟 메트릭 (기본값: CPUUtilization)",
                        "default": "CPUUtilization"
                    }
                },
                "required": ["csv_file", "predictor_metric"]
            }
        ),
        types.Tool(
            name="list_data_files",
            description="데이터 디렉토리의 CSV 파일 목록을 조회합니다",
            inputSchema={
                "type": "object",
                "properties": {}
            }
        ),
        types.Tool(
            name="get_metric_summary",
            description="CSV 파일의 메트릭 요약 정보를 조회합니다",
            inputSchema={
                "type": "object",
                "properties": {
                    "csv_file": {
                        "type": "string",
                        "description": "요약할 CSV 파일명"
                    }
                },
                "required": ["csv_file"]
            }
        )
    ]

@server.call_tool()
async def handle_call_tool(name: str, arguments: dict) -> list[types.TextContent]:
    """도구 호출 처리"""
    try:
        db_server = AnalyzeDBServer()
        
        # 데이터베이스 연결 및 성능 분석 도구
        if name == "list_database_secrets":
            result = await db_server.list_database_secrets(arguments.get("keyword"))
        elif name == "test_database_connection":
            connection, tunnel_used = db_server.get_db_connection(arguments["database_secret"])
            if connection.is_connected():
                db_info = connection.get_server_info()
                result = f"✅ 데이터베이스 연결 성공! 서버 버전: {db_info}"
                connection.close()
                if tunnel_used:
                    db_server.cleanup_ssh_tunnel()
            else:
                result = "❌ 데이터베이스 연결 실패"
        elif name == "get_database_performance_metrics":
            result = await db_server.get_database_performance_metrics(
                arguments["database_secret"],
                arguments.get("metric_type", "all")
            )
        elif name == "get_schema_summary":
            result = await db_server.get_schema_summary(arguments["database_secret"])
        
        # 시나리오 기반 진단 도구
        elif name == "diagnose_connection_issues":
            result = await db_server.diagnose_connection_issues(arguments["database_secret"])
            db_server.save_analysis_history(result)
        elif name == "diagnose_io_bottleneck":
            result = await db_server.diagnose_io_bottleneck(arguments["database_secret"])
            db_server.save_analysis_history(result)
        elif name == "diagnose_memory_pressure":
            result = await db_server.diagnose_memory_pressure(arguments["database_secret"])
            db_server.save_analysis_history(result)
        elif name == "dynamic_troubleshooting_workflow":
            result = await db_server.dynamic_troubleshooting_workflow(
                arguments["database_secret"],
                arguments["user_question"]
            )
            db_server.save_analysis_history(result)
        
        # CloudWatch 메트릭 수집 및 분석 도구
        elif name == "collect_db_metrics":
            result = await db_server.collect_metrics(
                arguments["db_instance_identifier"],
                arguments.get("hours", 24),
                arguments.get("metrics"),
                arguments.get("region", db_server.default_region)
            )
        elif name == "analyze_metric_correlation":
            result = await db_server.analyze_correlation(
                arguments["csv_file"],
                arguments.get("target_metric", "CPUUtilization"),
                arguments.get("top_n", 10)
            )
        elif name == "detect_metric_outliers":
            result = await db_server.detect_outliers(
                arguments["csv_file"],
                arguments.get("std_threshold", 2.0)
            )
        elif name == "perform_regression_analysis":
            result = await db_server.perform_regression_analysis(
                arguments["csv_file"],
                arguments["predictor_metric"],
                arguments.get("target_metric", "CPUUtilization")
            )
        elif name == "list_data_files":
            result = await db_server.list_data_files()
        elif name == "get_metric_summary":
            result = await db_server.get_metric_summary(arguments["csv_file"])
        elif name == "generate_correlation_report":
            result = await db_server.generate_correlation_report(
                arguments["csv_file"],
                arguments.get("target_metrics")
            )
        else:
            result = f"알 수 없는 도구: {name}"
        
        return [types.TextContent(type="text", text=result)]
    except Exception as e:
        error_msg = f"도구 실행 중 오류 발생: {str(e)}"
        logger.error(error_msg)
        return [types.TextContent(type="text", text=error_msg)]

async def main():
    # Stdin/stdout를 통한 MCP 서버 실행
    async with mcp.server.stdio.stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream,
            write_stream,
            InitializationOptions(
                server_name="analyze-db",
                server_version="1.0.0",
                capabilities=server.get_capabilities(
                    notification_options=NotificationOptions(),
                    experimental_capabilities={},
                ),
            ),
        )

if __name__ == "__main__":
    asyncio.run(main())