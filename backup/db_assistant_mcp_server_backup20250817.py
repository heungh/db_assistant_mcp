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
try:
    import pandas as pd
    import numpy as np
    import matplotlib
    matplotlib.use('Agg')  # GUI 없는 환경에서 matplotlib 사용
    import matplotlib.pyplot as plt
    from sklearn.model_selection import train_test_split
    from sklearn.preprocessing import PolynomialFeatures
    from sklearn.linear_model import LinearRegression
    from sklearn.metrics import mean_squared_error, r2_score
    from sklearn.impute import SimpleImputer
    ANALYSIS_AVAILABLE = True
except ImportError:
    ANALYSIS_AVAILABLE = False

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
        # 분석 관련 초기화
        self.cloudwatch = None
        self.default_metrics = [
            'CPUUtilization', 'DatabaseConnections', 'DBLoad', 'DBLoadCPU', 
            'DBLoadNonCPU', 'FreeableMemory', 'ReadIOPS', 'WriteIOPS',
            'ReadLatency', 'WriteLatency', 'NetworkReceiveThroughput',
            'NetworkTransmitThroughput', 'BufferCacheHitRatio'
        ]

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
                
                all_secrets.extend([secret["Name"] for secret in response["SecretList"]])
                
                if 'NextToken' not in response:
                    break
                next_token = response['NextToken']
            
            # 키워드 필터링
            if keyword:
                filtered_secrets = [
                    secret for secret in all_secrets
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
                "-F", "/dev/null",
                "-o", "UserKnownHostsFile=/dev/null",
                "-o", "StrictHostKeyChecking=no",
                "-i", "/Users/heungh/test.pem",
                "-f", "-N",
                "-L", f"3307:{db_host}:3306",
                "ec2-user@54.180.79.255"
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

    async def get_db_connection(self, database_secret: str, selected_database: str = None, use_ssh_tunnel: bool = True):
        """공통 DB 연결 함수"""
        if mysql is None:
            raise Exception("mysql-connector-python이 설치되지 않았습니다. pip install mysql-connector-python을 실행해주세요.")
        
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
        database_name = selected_database or db_config.get('dbname', db_config.get('database'))
        
        if use_ssh_tunnel:
            if self.setup_ssh_tunnel(db_config.get('host')):
                connection_config = {
                    'host': 'localhost',
                    'port': 3307,
                    'user': db_config.get('username'),
                    'password': db_config.get('password'),
                    'database': database_name,
                    'connection_timeout': 10
                }
                tunnel_used = True
        
        if not connection_config:
            connection_config = {
                'host': db_config.get('host'),
                'port': db_config.get('port', 3306),
                'user': db_config.get('username'),
                'password': db_config.get('password'),
                'database': database_name,
                'connection_timeout': 10
            }
        
        connection = mysql.connector.connect(**connection_config)
        return connection, tunnel_used

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
                return f"'{keyword}' 키워드로 찾은 시크릿이 없습니다." if keyword else "시크릿이 없습니다."
            
            secret_list = "\n".join([f"- {secret}" for secret in secrets])
            return f"데이터베이스 시크릿 목록:\n{secret_list}"
        except Exception as e:
            return f"시크릿 목록 조회 실패: {str(e)}"

    async def test_database_connection(self, database_secret: str, use_ssh_tunnel: bool = True) -> str:
        """데이터베이스 연결 테스트"""
        try:
            connection, tunnel_used = await self.get_db_connection(database_secret, self.selected_database, use_ssh_tunnel)
            
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
                    if db not in ['information_schema', 'performance_schema', 'mysql', 'sys']:
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

    async def list_databases(self, database_secret: str, use_ssh_tunnel: bool = True) -> str:
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
            get_secret_value_response = client.get_secret_value(SecretId=database_secret)
            secret = get_secret_value_response["SecretString"]
            db_config = json.loads(secret)
            
            connection_config = None
            tunnel_used = False
            
            if use_ssh_tunnel:
                if self.setup_ssh_tunnel(db_config.get('host')):
                    connection_config = {
                        'host': 'localhost',
                        'port': 3307,
                        'user': db_config.get('username'),
                        'password': db_config.get('password'),
                        'connection_timeout': 10
                    }
                    tunnel_used = True
            
            if not connection_config:
                connection_config = {
                    'host': db_config.get('host'),
                    'port': db_config.get('port', 3306),
                    'user': db_config.get('username'),
                    'password': db_config.get('password'),
                    'connection_timeout': 10
                }
            
            # 데이터베이스 없이 연결
            connection = mysql.connector.connect(**connection_config)
            cursor = connection.cursor()
            
            # 데이터베이스 목록 조회
            cursor.execute("SHOW DATABASES")
            databases = [db[0] for db in cursor.fetchall() if db[0] not in ['information_schema', 'performance_schema', 'mysql', 'sys']]
            
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

    async def select_database(self, database_secret: str, database_selection: str, use_ssh_tunnel: bool = True) -> str:
        """데이터베이스 선택 (USE 명령어 실행)"""
        try:
            # 먼저 데이터베이스 목록을 가져와서 유효성 검증
            db_list_result = await self.list_databases(database_secret, use_ssh_tunnel)
            
            # 데이터베이스 목록에서 실제 DB 이름들 추출
            lines = db_list_result.split('\n')
            databases = []
            for line in lines:
                if line.strip() and line[0].isdigit() and '. ' in line:
                    db_name = line.split('. ', 1)[1]
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
            connection, tunnel_used = await self.get_db_connection(database_secret, None, use_ssh_tunnel)
            
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
            connection, tunnel_used = await self.get_db_connection(database_secret, self.selected_database)
            cursor = connection.cursor()
            
            # 현재 데이터베이스 확인
            cursor.execute("SELECT DATABASE()")
            current_db = cursor.fetchone()[0]
            
            # 테이블 정보 수집
            cursor.execute("""
                SELECT table_name, table_type, engine, table_rows, 
                       data_length, index_length, table_comment
                FROM information_schema.tables 
                WHERE table_schema = DATABASE()
                ORDER BY table_name
            """)
            
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
                cursor.execute("""
                    SELECT COUNT(*) FROM information_schema.columns 
                    WHERE table_schema = DATABASE() AND table_name = %s
                """, (table_name,))
                column_count = cursor.fetchone()[0]
                
                # 인덱스 수 조회
                cursor.execute("""
                    SELECT COUNT(DISTINCT index_name) FROM information_schema.statistics 
                    WHERE table_schema = DATABASE() AND table_name = %s
                """, (table_name,))
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
            connection, tunnel_used = await self.get_db_connection(database_secret, self.selected_database)
            cursor = connection.cursor()
            
            # 테이블 존재 확인
            cursor.execute("""
                SELECT COUNT(*) FROM information_schema.tables 
                WHERE table_schema = DATABASE() AND table_name = %s
            """, (table_name,))
            
            if cursor.fetchone()[0] == 0:
                return f"❌ 테이블 '{table_name}'을 찾을 수 없습니다."
            
            # 컬럼 정보 조회
            cursor.execute("""
                SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT, 
                       COLUMN_COMMENT, COLUMN_KEY, EXTRA
                FROM information_schema.columns 
                WHERE table_schema = DATABASE() AND table_name = %s
                ORDER BY ORDINAL_POSITION
            """, (table_name,))
            
            columns = cursor.fetchall()
            
            result = f"📋 **테이블 '{table_name}' 스키마 정보**\n\n"
            result += f"📊 **컬럼 목록** ({len(columns)}개):\n"
            
            for col in columns:
                col_name, data_type, is_nullable, default_val, comment, key, extra = col
                
                result += f"\n🔹 **{col_name}**\n"
                result += f"   - 타입: {data_type}\n"
                result += f"   - NULL 허용: {'예' if is_nullable == 'YES' else '아니오'}\n"
                
                if default_val is not None:
                    result += f"   - 기본값: {default_val}\n"
                
                if key:
                    key_type = {"PRI": "기본키", "UNI": "고유키", "MUL": "인덱스"}.get(key, key)
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
            connection, tunnel_used = await self.get_db_connection(database_secret, self.selected_database)
            cursor = connection.cursor()
            
            # 테이블 존재 확인
            cursor.execute("""
                SELECT COUNT(*) FROM information_schema.tables 
                WHERE table_schema = DATABASE() AND table_name = %s
            """, (table_name,))
            
            if cursor.fetchone()[0] == 0:
                return f"❌ 테이블 '{table_name}'을 찾을 수 없습니다."
            
            # 인덱스 정보 조회
            cursor.execute("""
                SELECT INDEX_NAME, COLUMN_NAME, SEQ_IN_INDEX, NON_UNIQUE, 
                       INDEX_TYPE, CARDINALITY, NULLABLE, INDEX_COMMENT
                FROM information_schema.statistics 
                WHERE table_schema = DATABASE() AND table_name = %s
                ORDER BY INDEX_NAME, SEQ_IN_INDEX
            """, (table_name,))
            
            indexes = cursor.fetchall()
            
            if not indexes:
                result = f"📋 **테이블 '{table_name}' 인덱스 정보**\n\n❌ 인덱스가 없습니다."
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

    async def get_performance_metrics(self, database_secret: str, metric_type: str = "all") -> str:
        """데이터베이스 성능 메트릭 조회"""
        try:
            connection, tunnel_used = await self.get_db_connection(database_secret, self.selected_database)
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
            
            cursor.close()
            connection.close()
            
            if tunnel_used:
                self.cleanup_ssh_tunnel()
            
            return result
            
        except Exception as e:
            return f"❌ 성능 메트릭 조회 실패: {str(e)}"

    # === DDL 검증 관련 메서드 ===
    
    async def validate_sql_file(self, filename: str, database_secret: Optional[str] = None) -> str:
        """특정 SQL 파일 검증"""
        try:
            sql_file_path = SQL_DIR / filename
            if not sql_file_path.exists():
                return f"SQL 파일을 찾을 수 없습니다: {filename}"

            with open(sql_file_path, 'r', encoding='utf-8') as f:
                ddl_content = f.read()

            result = await self.validate_ddl(ddl_content, database_secret, filename)
            return result
        except Exception as e:
            return f"SQL 파일 검증 실패: {str(e)}"

    async def validate_ddl(self, ddl_content: str, database_secret: Optional[str], filename: str) -> str:
        """DDL 검증 실행"""
        try:
            # 디버그 로그 파일 생성
            debug_log_path = OUTPUT_DIR / f"debug_log_{filename}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
            
            def debug_log(message):
                with open(debug_log_path, 'a', encoding='utf-8') as f:
                    f.write(f"{datetime.now().strftime('%H:%M:%S')} - {message}\n")
                    f.flush()
            
            debug_log(f"validate_ddl 시작 - 파일: {filename}")
            debug_log(f"DDL 내용: {ddl_content.strip()}")
            
            issues = []
            db_connection_info = None
            schema_validation = None
            constraint_validation = None
            
            # 1. 기본 문법 검증
            if not ddl_content.strip().endswith(";"):
                issues.append("세미콜론이 누락되었습니다.")
                debug_log("세미콜론 검증 실패")
            else:
                debug_log("세미콜론 검증 통과")
            
            # 2. DDL 타입 확인
            ddl_type = self.extract_ddl_type(ddl_content)
            debug_log(f"DDL 타입: {ddl_type}")
            
            # 3. 데이터베이스 연결 테스트 (database_secret이 제공된 경우)
            if database_secret:
                debug_log(f"데이터베이스 연결 테스트 시작: {database_secret}")
                try:
                    db_connection_info = await self.test_database_connection_for_validation(database_secret)
                    debug_log(f"DB 연결 결과: {db_connection_info['success']}")
                    
                    if not db_connection_info["success"]:
                        issues.append(f"DB 연결 실패: {db_connection_info['error']}")
                        debug_log(f"DB 연결 실패: {db_connection_info['error']}")
                    else:
                        debug_log(f"DB 연결 성공, DDL 타입 체크: {ddl_type}")
                        # DDL 구문에 대해서만 스키마/제약조건 검증 수행
                        if ddl_type in ["CREATE_TABLE", "ALTER_TABLE", "CREATE_INDEX", "DROP"]:
                            debug_log("스키마 검증 대상 DDL 타입")
                            # 4. 스키마 검증
                            try:
                                debug_log("스키마 검증 시작")
                                schema_validation = await self.validate_schema_with_debug(ddl_content, database_secret, debug_log)
                                debug_log(f"스키마 검증 완료: success={schema_validation['success']}")
                                
                                if schema_validation["success"]:
                                    debug_log(f"검증 결과 개수: {len(schema_validation['validation_results'])}")
                                    for i, result in enumerate(schema_validation["validation_results"]):
                                        debug_log(f"결과 [{i}]: {result}")
                                        if result.get("issues") and len(result["issues"]) > 0:
                                            debug_log(f"이슈 발견: {result['issues']}")
                                            issues.extend([f"스키마 검증: {issue}" for issue in result["issues"]])
                                        else:
                                            debug_log("이슈 없음")
                                else:
                                    issues.append(f"스키마 검증 실패: {schema_validation['error']}")
                                    debug_log(f"스키마 검증 실패: {schema_validation['error']}")
                            except Exception as e:
                                logger.error(f"스키마 검증 오류: {e}")
                                issues.append(f"스키마 검증 중 오류 발생: {str(e)}")
                                debug_log(f"스키마 검증 예외: {e}")
                        else:
                            # SELECT, SHOW 등의 쿼리문은 기본 문법 검증만 수행
                            logger.info(f"쿼리문 ({ddl_type}) 감지: 스키마/제약조건 검증 건너뜀")
                            debug_log(f"쿼리문 감지, 스키마 검증 건너뜀: {ddl_type}")
                            
                except Exception as e:
                    logger.error(f"DB 연결 테스트 오류: {e}")
                    issues.append(f"DB 연결 테스트 중 오류 발생: {str(e)}")
                    debug_log(f"DB 연결 테스트 예외: {e}")
            else:
                debug_log("database_secret 없음, DB 검증 건너뜀")
            
            # 검증 완료
            debug_log(f"최종 이슈 개수: {len(issues)}")
            debug_log(f"이슈 목록: {issues}")
            
            # 결과 생성
            if not issues:
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
            await self.generate_html_report(report_path, filename, ddl_content, ddl_type, 
                                          status, summary, issues, db_connection_info, 
                                          schema_validation, constraint_validation, database_secret)
            
            return f"{summary}\n\n📄 상세 보고서가 저장되었습니다: {report_path}\n🔍 디버그 로그: {debug_log_path}"
            
        except Exception as e:
            return f"DDL 검증 중 오류 발생: {str(e)}"

    def extract_ddl_type(self, ddl_content: str) -> str:
        """DDL 타입 추출"""
        # 주석과 빈 줄을 제거하고 실제 DDL 구문만 추출
        lines = ddl_content.strip().split('\n')
        ddl_lines = []
        
        for line in lines:
            line = line.strip()
            # 주석 라인이나 빈 라인 건너뛰기
            if line and not line.startswith('--') and not line.startswith('#'):
                ddl_lines.append(line)
        
        if not ddl_lines:
            return "UNKNOWN"
        
        # 첫 번째 유효한 DDL 라인으로 타입 판단
        ddl_upper = ' '.join(ddl_lines).upper().strip()
        
        if ddl_upper.startswith("CREATE TABLE"):
            return "CREATE_TABLE"
        elif ddl_upper.startswith("ALTER TABLE"):
            return "ALTER_TABLE"
        elif ddl_upper.startswith("CREATE INDEX"):
            return "CREATE_INDEX"
        elif ddl_upper.startswith("DROP TABLE"):
            return "DROP TABLE"
        elif ddl_upper.startswith("DROP INDEX"):
            return "DROP INDEX"
        elif ddl_upper.startswith("USE "):
            return "USE"
        elif ddl_upper.startswith("SHOW "):
            return "SHOW"
        elif ddl_upper.startswith("SELECT"):
            return "SELECT"
        else:
            return "UNKNOWN"

    def detect_ddl_type(self, ddl_content: str) -> str:
        """DDL 타입 감지"""
        ddl_upper = ddl_content.upper().strip()
        
        if ddl_upper.startswith('CREATE TABLE'):
            return 'CREATE_TABLE'
        elif ddl_upper.startswith('ALTER TABLE'):
            return 'ALTER_TABLE'
        elif ddl_upper.startswith('DROP TABLE'):
            return 'DROP_TABLE'
        elif ddl_upper.startswith('CREATE INDEX'):
            return 'CREATE_INDEX'
        elif ddl_upper.startswith('DROP INDEX'):
            return 'DROP_INDEX'
        elif ddl_upper.startswith('INSERT'):
            return 'INSERT'
        elif ddl_upper.startswith('UPDATE'):
            return 'UPDATE'
        elif ddl_upper.startswith('DELETE'):
            return 'DELETE'
        elif ddl_upper.startswith('SELECT'):
            return 'SELECT'
        else:
            return 'UNKNOWN'

    async def validate_with_claude(self, ddl_content: str) -> str:
        """
        Claude cross-region 프로파일을 활용한 DDL 검증 (Sonnet 4 → 3.7 fallback)
        """
        prompt = f"""
        다음 DDL 문을 검증해주세요:

        {ddl_content}

        문법 오류, 표준 규칙 위반, 성능 문제가 있는지 확인해주세요.
        문제가 있으면 구체적으로 지적해주세요. 문제가 없으면 "검증 통과"라고 응답해주세요.
        """

        claude_input = json.dumps({
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens": 1024,
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
            logger.warning(f"Claude Sonnet 4 호출 실패 → Claude 3.7 Sonnet cross-region profile로 fallback: {e}")
            # Claude 3.7 Sonnet inference profile 호출 (fallback)
            try:
                response = self.bedrock_client.invoke_model(
                    modelId=sonnet_3_7_model_id,
                    body=claude_input
                )
                response_body = json.loads(response.get("body").read())
                return response_body.get("content", [{}])[0].get("text", "")
            except Exception as e:
                logger.error(f"Claude 3.7 Sonnet 호출 오류: {e}")
                return f"Claude 호출 중 오류 발생: {str(e)}"

    async def test_database_connection_for_validation(self, database_secret: str, use_ssh_tunnel: bool = True) -> Dict[str, Any]:
        """검증용 데이터베이스 연결 테스트"""
        try:
            connection, tunnel_used = await self.get_db_connection(database_secret, self.selected_database, use_ssh_tunnel)
            
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
                    "port": 3307 if tunnel_used else 3306
                }
                
                # SSH 터널 정리
                if tunnel_used:
                    self.cleanup_ssh_tunnel()
                
                return result
            else:
                if tunnel_used:
                    self.cleanup_ssh_tunnel()
                return {
                    "success": False,
                    "error": "데이터베이스 연결에 실패했습니다."
                }
                
        except MySQLError as e:
            if use_ssh_tunnel:
                self.cleanup_ssh_tunnel()
            return {
                "success": False,
                "error": f"MySQL 오류: {str(e)}"
            }
        except Exception as e:
            if use_ssh_tunnel:
                self.cleanup_ssh_tunnel()
            return {
                "success": False,
                "error": f"연결 테스트 오류: {str(e)}"
            }

    async def validate_schema_with_debug(self, ddl_content: str, database_secret: str, debug_log, use_ssh_tunnel: bool = True) -> Dict[str, Any]:
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
                    "error": "DDL에서 구문 정보를 추출할 수 없습니다."
                }
            
            connection, tunnel_used = await self.get_db_connection(database_secret, self.selected_database, use_ssh_tunnel)
            cursor = connection.cursor()
            debug_log(f"DB 연결 성공, 터널 사용: {tunnel_used}")
            
            validation_results = []
            
            # DDL 구문 유형별 검증
            for ddl_statement in ddl_info:
                ddl_type = ddl_statement['type']
                table_name = ddl_statement['table']
                debug_log(f"검증 중: {ddl_type} on {table_name}")
                
                if ddl_type == 'CREATE_TABLE':
                    debug_log("CREATE_TABLE 검증 호출")
                    result = await self.validate_create_table_with_debug(cursor, ddl_statement, debug_log)
                    debug_log(f"CREATE_TABLE 검증 결과: {result}")
                elif ddl_type == 'ALTER_TABLE':
                    debug_log("ALTER_TABLE 검증 호출")
                    result = await self.validate_alter_table_with_debug(cursor, ddl_statement, debug_log)
                    debug_log(f"ALTER_TABLE 검증 결과: {result}")
                elif ddl_type == 'CREATE_INDEX':
                    result = await self.validate_create_index(cursor, ddl_statement)
                elif ddl_type == 'DROP_TABLE':
                    result = await self.validate_drop_table(cursor, ddl_statement)
                elif ddl_type == 'CREATE_INDEX':
                    debug_log("CREATE_INDEX 검증 호출")
                    result = await self.validate_create_index_with_debug(cursor, ddl_statement, debug_log)
                    debug_log(f"CREATE_INDEX 검증 결과: {result}")
                elif ddl_type == 'DROP_INDEX':
                    debug_log("DROP_INDEX 검증 호출")
                    result = await self.validate_drop_index_with_debug(cursor, ddl_statement, debug_log)
                    debug_log(f"DROP_INDEX 검증 결과: {result}")
                else:
                    result = {
                        "table": table_name,
                        "ddl_type": ddl_type,
                        "valid": False,
                        "issues": [f"지원하지 않는 DDL 구문 유형: {ddl_type}"]
                    }
                
                validation_results.append(result)
                debug_log(f"검증 결과 추가됨: {result}")
            
            cursor.close()
            connection.close()
            
            # SSH 터널 정리
            if tunnel_used:
                self.cleanup_ssh_tunnel()
            
            debug_log(f"validate_schema 완료, 결과 개수: {len(validation_results)}")
            return {
                "success": True,
                "validation_results": validation_results
            }
            
        except Exception as e:
            debug_log(f"validate_schema 예외: {e}")
            if use_ssh_tunnel:
                self.cleanup_ssh_tunnel()
            return {
                "success": False,
                "error": f"스키마 검증 오류: {str(e)}"
            }

    def parse_ddl_detailed_with_debug(self, ddl_content: str, debug_log) -> List[Dict[str, Any]]:
        """DDL 구문을 상세하게 파싱하여 구문 유형별 정보 추출 (디버그 버전)"""
        ddl_statements = []
        
        debug_log(f"DDL 파싱 시작: {repr(ddl_content)}")
        
        # CREATE TABLE 파싱
        create_table_pattern = r'CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?`?(\w+)`?'
        create_table_matches = re.findall(create_table_pattern, ddl_content, re.IGNORECASE)
        
        debug_log(f"CREATE TABLE 파싱 - 결과: {create_table_matches}")
        
        for table_name in create_table_matches:
            ddl_statements.append({
                'type': 'CREATE_TABLE',
                'table': table_name.lower()
            })
            debug_log(f"CREATE TABLE 구문 추가됨: {table_name}")
        
        # ALTER TABLE 파싱
        alter_table_pattern = r'ALTER\s+TABLE\s+`?(\w+)`?'
        alter_table_matches = re.findall(alter_table_pattern, ddl_content, re.IGNORECASE)
        
        debug_log(f"ALTER TABLE 파싱 - 결과: {alter_table_matches}")
        
        for table_name in alter_table_matches:
            ddl_statements.append({
                'type': 'ALTER_TABLE',
                'table': table_name.lower()
            })
            debug_log(f"ALTER TABLE 구문 추가됨: {table_name}")
        
        # CREATE INDEX 파싱
        create_index_pattern = r'CREATE\s+(?:UNIQUE\s+)?INDEX\s+`?(\w+)`?\s+ON\s+`?(\w+)`?\s*\((.*?)\)'
        create_index_matches = re.findall(create_index_pattern, ddl_content, re.IGNORECASE)
        
        debug_log(f"CREATE INDEX 파싱 - 결과: {create_index_matches}")
        
        for index_name, table_name, columns in create_index_matches:
            ddl_statements.append({
                'type': 'CREATE_INDEX',
                'table': table_name.lower(),
                'index_name': index_name.lower(),
                'columns': columns.strip()
            })
            debug_log(f"CREATE INDEX 구문 추가됨: {index_name} on {table_name}({columns})")

        # DROP INDEX 파싱
        drop_index_pattern = r'DROP\s+INDEX\s+`?(\w+)`?\s+ON\s+`?(\w+)`?'
        drop_index_matches = re.findall(drop_index_pattern, ddl_content, re.IGNORECASE)
        
        debug_log(f"DROP INDEX 파싱 - 패턴: {drop_index_pattern}")
        debug_log(f"DROP INDEX 파싱 - 결과: {drop_index_matches}")
        
        for index_name, table_name in drop_index_matches:
            ddl_statements.append({
                'type': 'DROP_INDEX',
                'table': table_name.lower(),
                'index_name': index_name.lower()
            })
            debug_log(f"DROP INDEX 구문 추가됨: {index_name} on {table_name}")
        
        debug_log(f"전체 파싱 결과: {len(ddl_statements)}개 구문")
        for i, stmt in enumerate(ddl_statements):
            debug_log(f"  [{i}] {stmt['type']}: {stmt}")
        
        return ddl_statements

    async def validate_create_index_with_debug(self, cursor, ddl_statement: Dict[str, Any], debug_log) -> Dict[str, Any]:
        """CREATE INDEX 구문 검증 (디버그 버전)"""
        table_name = ddl_statement['table']
        index_name = ddl_statement['index_name']
        columns = ddl_statement['columns']
        
        debug_log(f"CREATE INDEX 검증 시작: table={table_name}, index={index_name}, columns={columns}")
        
        issues = []
        
        # 1. 테이블 존재 여부 확인
        try:
            cursor.execute("SHOW TABLES LIKE %s", (table_name,))
            table_exists = cursor.fetchone() is not None
            debug_log(f"테이블 존재 여부: {table_exists}")
            
            if not table_exists:
                issues.append(f"테이블 '{table_name}'이 존재하지 않습니다.")
            else:
                # 2. 인덱스 이미 존재하는지 확인
                cursor.execute("SHOW INDEX FROM `{}`".format(table_name))
                existing_indexes = cursor.fetchall()
                existing_index_names = [idx[2] for idx in existing_indexes]  # Key_name
                debug_log(f"기존 인덱스: {existing_index_names}")
                
                if index_name in existing_index_names:
                    issues.append(f"인덱스 '{index_name}'이 이미 존재합니다.")
                
                # 3. 컬럼 존재 여부 확인
                cursor.execute("DESCRIBE `{}`".format(table_name))
                table_columns = cursor.fetchall()
                existing_columns = [col[0] for col in table_columns]  # Field name
                debug_log(f"테이블 컬럼: {existing_columns}")
                
                # 인덱스 컬럼 파싱 (email, name 등)
                index_columns = [col.strip().strip('`') for col in columns.split(',')]
                debug_log(f"인덱스 컬럼: {index_columns}")
                
                for col in index_columns:
                    if col not in existing_columns:
                        issues.append(f"컬럼 '{col}'이 테이블 '{table_name}'에 존재하지 않습니다.")
                        
        except Exception as e:
            debug_log(f"CREATE INDEX 검증 중 오류: {str(e)}")
            issues.append(f"CREATE INDEX 검증 중 오류가 발생했습니다: {str(e)}")
        
        result = {
            'valid': len(issues) == 0,
            'issues': issues,
            'table_name': table_name,
            'index_name': index_name,
            'columns': columns
        }
        
        debug_log(f"CREATE INDEX 검증 완료: issues={len(issues)}, valid={len(issues) == 0}")
        debug_log(f"최종 결과: {result}")
        
        return result

    async def validate_drop_index_with_debug(self, cursor, ddl_statement: Dict[str, Any], debug_log) -> Dict[str, Any]:
        """DROP INDEX 구문 검증 (디버그 버전)"""
        table_name = ddl_statement['table']
        index_name = ddl_statement['index_name']
        
        debug_log(f"DROP INDEX 검증 시작: table={table_name}, index={index_name}")
        
        issues = []
        
        # 테이블 존재 여부 확인
        cursor.execute("""
            SELECT COUNT(*) FROM information_schema.tables 
            WHERE table_schema = DATABASE() AND table_name = %s
        """, (table_name,))
        
        table_exists = cursor.fetchone()[0] > 0
        debug_log(f"테이블 '{table_name}' 존재 여부: {table_exists}")
        
        if not table_exists:
            issues.append(f"테이블 '{table_name}'이 존재하지 않습니다.")
            debug_log(f"테이블 '{table_name}'이 존재하지 않음 - 이슈 추가")
        else:
            # 인덱스 존재 여부 확인
            cursor.execute("""
                SELECT COUNT(*) FROM information_schema.statistics 
                WHERE table_schema = DATABASE() AND table_name = %s AND index_name = %s
            """, (table_name, index_name))
            
            index_exists = cursor.fetchone()[0] > 0
            debug_log(f"인덱스 '{index_name}' 존재 여부: {index_exists}")
            
            if not index_exists:
                issues.append(f"인덱스 '{index_name}'이 테이블 '{table_name}'에 존재하지 않습니다.")
                debug_log(f"인덱스 '{index_name}'이 존재하지 않음 - 이슈 추가")
        
        result = {
            "table": table_name,
            "ddl_type": "DROP_INDEX",
            "valid": len(issues) == 0,
            "issues": issues,
            "details": {
                "index_name": index_name,
                "table_exists": table_exists
            }
        } #test
        debug_log(f"DROP INDEX 검증 완료: issues={len(issues)}, valid={len(issues) == 0}")
        debug_log(f"최종 결과: {result}")
        
        return result

    async def validate_create_table_with_debug(self, cursor, ddl_statement: Dict[str, Any], debug_log) -> Dict[str, Any]:
        """CREATE TABLE 구문 검증 (디버그 버전)"""
        table_name = ddl_statement['table']
        
        debug_log(f"CREATE TABLE 검증 시작: table={table_name}")
        
        issues = []
        
        # 테이블 존재 여부 확인
        cursor.execute("""
            SELECT COUNT(*) FROM information_schema.tables 
            WHERE table_schema = DATABASE() AND table_name = %s
        """, (table_name,))
        
        table_exists = cursor.fetchone()[0] > 0
        debug_log(f"테이블 존재 여부: {table_exists}")
        
        if table_exists:
            issues.append(f"테이블 '{table_name}'이 이미 존재합니다.")
        
        result = {
            "table": table_name,
            "ddl_type": "CREATE_TABLE",
            "valid": len(issues) == 0,
            "issues": issues,
            "details": {
                "table_exists": table_exists
            }
        }
        
        debug_log(f"CREATE TABLE 검증 완료: issues={len(issues)}, valid={len(issues) == 0}")
        debug_log(f"최종 결과: {result}")
        
        return result

    async def validate_alter_table_with_debug(self, cursor, ddl_statement: Dict[str, Any], debug_log) -> Dict[str, Any]:
        """ALTER TABLE 구문 검증 (디버그 버전)"""
        table_name = ddl_statement['table']
        
        debug_log(f"ALTER TABLE 검증 시작: table={table_name}")
        
        issues = []
        
        # 테이블 존재 여부 확인
        cursor.execute("""
            SELECT COUNT(*) FROM information_schema.tables 
            WHERE table_schema = DATABASE() AND table_name = %s
        """, (table_name,))
        
        table_exists = cursor.fetchone()[0] > 0
        debug_log(f"테이블 존재 여부: {table_exists}")
        
        if not table_exists:
            issues.append(f"테이블 '{table_name}'이 존재하지 않습니다.")
        
        result = {
            "table": table_name,
            "ddl_type": "ALTER_TABLE",
            "valid": len(issues) == 0,
            "issues": issues,
            "details": {
                "table_exists": table_exists
            }
        }
        
        debug_log(f"ALTER TABLE 검증 완료: issues={len(issues)}, valid={len(issues) == 0}")
        debug_log(f"최종 결과: {result}")
        
        return result
        """DDL 구문 유형에 따른 스키마 검증"""
        try:
            print(f"[DEBUG] validate_schema 시작")
            # DDL 구문 유형 및 상세 정보 파싱
            ddl_info = self.parse_ddl_detailed(ddl_content)
            print(f"[DEBUG] 파싱된 DDL 정보: {ddl_info}")
            
            if not ddl_info:
                print(f"[DEBUG] DDL 파싱 실패")
                return {
                    "success": False,
                    "error": "DDL에서 구문 정보를 추출할 수 없습니다."
                }
            
            connection, tunnel_used = await self.get_db_connection(database_secret, self.selected_database, use_ssh_tunnel)
            cursor = connection.cursor()
            print(f"[DEBUG] DB 연결 성공, 터널 사용: {tunnel_used}")
            
            validation_results = []
            
            # DDL 구문 유형별 검증
            for ddl_statement in ddl_info:
                ddl_type = ddl_statement['type']
                table_name = ddl_statement['table']
                print(f"[DEBUG] 검증 중: {ddl_type} on {table_name}")
                
                if ddl_type == 'CREATE_TABLE':
                    result = await self.validate_create_table(cursor, ddl_statement)
                elif ddl_type == 'ALTER_TABLE':
                    result = await self.validate_alter_table(cursor, ddl_statement)
                elif ddl_type == 'CREATE_INDEX':
                    result = await self.validate_create_index(cursor, ddl_statement)
                elif ddl_type == 'DROP_TABLE':
                    result = await self.validate_drop_table(cursor, ddl_statement)
                elif ddl_type == 'DROP_INDEX':
                    print(f"[DEBUG] DROP_INDEX 검증 호출")
                    result = await self.validate_drop_index(cursor, ddl_statement)
                    print(f"[DEBUG] DROP_INDEX 검증 결과: {result}")
                else:
                    result = {
                        "table": table_name,
                        "ddl_type": ddl_type,
                        "valid": False,
                        "issues": [f"지원하지 않는 DDL 구문 유형: {ddl_type}"]
                    }
                
                validation_results.append(result)
                print(f"[DEBUG] 검증 결과 추가됨: {result}")
            
            cursor.close()
            connection.close()
            
            # SSH 터널 정리
            if tunnel_used:
                self.cleanup_ssh_tunnel()
            
            print(f"[DEBUG] validate_schema 완료, 결과 개수: {len(validation_results)}")
            return {
                "success": True,
                "validation_results": validation_results
            }
            
        except Exception as e:
            print(f"[DEBUG] validate_schema 예외: {e}")
            if use_ssh_tunnel:
                self.cleanup_ssh_tunnel()
            return {
                "success": False,
                "error": f"스키마 검증 오류: {str(e)}"
            }

    async def validate_constraints(self, ddl_content: str, database_secret: str, use_ssh_tunnel: bool = True) -> Dict[str, Any]:
        """제약조건 검증 - FK, 인덱스, 제약조건 확인"""
        try:
            # DDL에서 제약조건 정보 추출
            constraints_info = self.parse_ddl_constraints(ddl_content)
            
            connection, tunnel_used = await self.get_db_connection(database_secret, self.selected_database, use_ssh_tunnel)
            cursor = connection.cursor()
            
            constraint_results = []
            
            # 외래키 제약조건 검증
            if constraints_info.get('foreign_keys'):
                for fk in constraints_info['foreign_keys']:
                    # 참조 테이블 존재 여부 확인
                    cursor.execute("""
                        SELECT COUNT(*) FROM information_schema.tables 
                        WHERE table_schema = DATABASE() AND table_name = %s
                    """, (fk['referenced_table'],))
                    
                    ref_table_exists = cursor.fetchone()[0] > 0
                    
                    if ref_table_exists:
                        # 참조 컬럼 존재 여부 확인
                        cursor.execute("""
                            SELECT COUNT(*) FROM information_schema.columns 
                            WHERE table_schema = DATABASE() 
                            AND table_name = %s AND column_name = %s
                        """, (fk['referenced_table'], fk['referenced_column']))
                        
                        ref_column_exists = cursor.fetchone()[0] > 0
                        
                        constraint_results.append({
                            "type": "FOREIGN_KEY",
                            "constraint": f"{fk['column']} -> {fk['referenced_table']}.{fk['referenced_column']}",
                            "valid": ref_column_exists,
                            "issue": None if ref_column_exists else f"참조 컬럼 '{fk['referenced_table']}.{fk['referenced_column']}'이 존재하지 않습니다."
                        })
                    else:
                        constraint_results.append({
                            "type": "FOREIGN_KEY",
                            "constraint": f"{fk['column']} -> {fk['referenced_table']}.{fk['referenced_column']}",
                            "valid": False,
                            "issue": f"참조 테이블 '{fk['referenced_table']}'이 존재하지 않습니다."
                        })
            
            cursor.close()
            connection.close()
            
            # SSH 터널 정리
            if tunnel_used:
                self.cleanup_ssh_tunnel()
            
            return {
                "success": True,
                "constraint_results": constraint_results
            }
            
        except Exception as e:
            if use_ssh_tunnel:
                self.cleanup_ssh_tunnel()
            return {
                "success": False,
                "error": f"제약조건 검증 오류: {str(e)}"
            }

    def parse_ddl_detailed(self, ddl_content: str) -> List[Dict[str, Any]]:
        """DDL 구문을 상세하게 파싱하여 구문 유형별 정보 추출"""
        ddl_statements = []
        
        # CREATE TABLE 파싱
        create_table_pattern = r'CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?`?(\w+)`?\s*\((.*?)\)(?:\s*ENGINE\s*=\s*\w+)?(?:\s*COMMENT\s*=\s*[\'"][^\'"]*[\'"])?'
        create_matches = re.findall(create_table_pattern, ddl_content, re.DOTALL | re.IGNORECASE)
        
        for table_name, columns_def in create_matches:
            columns_info = self.parse_create_table_columns(columns_def)
            ddl_statements.append({
                'type': 'CREATE_TABLE',
                'table': table_name.lower(),
                'columns': columns_info['columns'],
                'constraints': columns_info['constraints']
            })
        
        # ALTER TABLE 파싱
        alter_patterns = [
            # ADD COLUMN
            (r'ALTER\s+TABLE\s+`?(\w+)`?\s+ADD\s+(?:COLUMN\s+)?`?(\w+)`?\s+([^,;]+)', 'ADD_COLUMN'),
            # DROP COLUMN
            (r'ALTER\s+TABLE\s+`?(\w+)`?\s+DROP\s+(?:COLUMN\s+)?`?(\w+)`?', 'DROP_COLUMN'),
            # MODIFY COLUMN
            (r'ALTER\s+TABLE\s+`?(\w+)`?\s+MODIFY\s+(?:COLUMN\s+)?`?(\w+)`?\s+([^,;]+)', 'MODIFY_COLUMN'),
            # CHANGE COLUMN
            (r'ALTER\s+TABLE\s+`?(\w+)`?\s+CHANGE\s+(?:COLUMN\s+)?`?(\w+)`?\s+`?(\w+)`?\s+([^,;]+)', 'CHANGE_COLUMN')
        ]
        
        for pattern, alter_type in alter_patterns:
            matches = re.findall(pattern, ddl_content, re.IGNORECASE)
            for match in matches:
                if alter_type == 'CHANGE_COLUMN':
                    table_name, old_column, new_column, column_def = match
                    ddl_statements.append({
                        'type': 'ALTER_TABLE',
                        'table': table_name.lower(),
                        'alter_type': alter_type,
                        'old_column': old_column.lower(),
                        'new_column': new_column.lower(),
                        'column_definition': column_def.strip()
                    })
                else:
                    table_name, column_name = match[:2]
                    column_def = match[2] if len(match) > 2 else None
                    ddl_statements.append({
                        'type': 'ALTER_TABLE',
                        'table': table_name.lower(),
                        'alter_type': alter_type,
                        'column': column_name.lower(),
                        'column_definition': column_def.strip() if column_def else None
                    })
        
        # CREATE INDEX 파싱
        create_index_pattern = r'CREATE\s+(?:UNIQUE\s+)?INDEX\s+`?(\w+)`?\s+ON\s+`?(\w+)`?\s*\(([^)]+)\)'
        index_matches = re.findall(create_index_pattern, ddl_content, re.IGNORECASE)
        
        for index_name, table_name, columns in index_matches:
            ddl_statements.append({
                'type': 'CREATE_INDEX',
                'table': table_name.lower(),
                'index_name': index_name.lower(),
                'columns': [col.strip().strip('`').lower() for col in columns.split(',')]
            })
        
        # DROP TABLE 파싱
        drop_table_pattern = r'DROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?`?(\w+)`?'
        drop_table_matches = re.findall(drop_table_pattern, ddl_content, re.IGNORECASE)
        
        for table_name in drop_table_matches:
            ddl_statements.append({
                'type': 'DROP_TABLE',
                'table': table_name.lower()
            })
        
        # DROP INDEX 파싱
        drop_index_pattern = r'DROP\s+INDEX\s+`?(\w+)`?\s+ON\s+`?(\w+)`?'
        drop_index_matches = re.findall(drop_index_pattern, ddl_content, re.IGNORECASE)
        
        print(f"[DEBUG] DROP INDEX 파싱 - 패턴: {drop_index_pattern}")
        print(f"[DEBUG] DROP INDEX 파싱 - 입력: {repr(ddl_content)}")
        print(f"[DEBUG] DROP INDEX 파싱 - 결과: {drop_index_matches}")
        
        for index_name, table_name in drop_index_matches:
            ddl_statements.append({
                'type': 'DROP_INDEX',
                'table': table_name.lower(),
                'index_name': index_name.lower()
            })
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
        lines = [line.strip() for line in columns_def.split(',')]
        
        for line in lines:
            line = line.strip()
            if not line:
                continue
                
            # 제약조건 확인
            if re.match(r'(?:CONSTRAINT|PRIMARY\s+KEY|FOREIGN\s+KEY|UNIQUE|INDEX|KEY)', line, re.IGNORECASE):
                constraints.append(line)
            else:
                # 컬럼 정의 파싱
                column_match = re.match(r'`?(\w+)`?\s+([^,\s]+)(?:\s+(.*))?', line, re.IGNORECASE)
                if column_match:
                    column_name = column_match.group(1).lower()
                    data_type = column_match.group(2).upper()
                    attributes = column_match.group(3) or ''
                    
                    columns.append({
                        'name': column_name,
                        'data_type': data_type,
                        'attributes': attributes.strip()
                    })
        
        return {
            'columns': columns,
            'constraints': constraints
        }

    def parse_ddl_constraints(self, ddl_content: str) -> Dict[str, List[Dict]]:
        """DDL에서 제약조건 정보 추출"""
        constraints = {
            'foreign_keys': [],
            'indexes': [],
            'primary_keys': []
        }
        
        # 외래키 패턴 매칭
        fk_pattern = r'FOREIGN\s+KEY\s*\(`?(\w+)`?\)\s*REFERENCES\s+`?(\w+)`?\s*\(`?(\w+)`?\)'
        fk_matches = re.findall(fk_pattern, ddl_content, re.IGNORECASE)
        
        for column, ref_table, ref_column in fk_matches:
            constraints['foreign_keys'].append({
                'column': column,
                'referenced_table': ref_table,
                'referenced_column': ref_column
            })
        
        return constraints
        
    async def validate_create_table(self, cursor, ddl_statement: Dict[str, Any]) -> Dict[str, Any]:
        """CREATE TABLE 구문 검증"""
        table_name = ddl_statement['table']
        columns = ddl_statement['columns']
        
        # 테이블 존재 여부 확인
        cursor.execute("""
            SELECT COUNT(*) FROM information_schema.tables 
            WHERE table_schema = DATABASE() AND table_name = %s
        """, (table_name,))
        
        table_exists = cursor.fetchone()[0] > 0
        issues = []
        
        if table_exists:
            issues.append(f"테이블 '{table_name}'이 이미 존재합니다.")
        
        return {
            "table": table_name,
            "ddl_type": "CREATE_TABLE",
            "valid": not table_exists,
            "issues": issues,
            "details": {
                "table_exists": table_exists,
                "columns_count": len(columns)
            }
        }

    async def validate_alter_table(self, cursor, ddl_statement: Dict[str, Any]) -> Dict[str, Any]:
        """ALTER TABLE 구문 검증"""
        table_name = ddl_statement['table']
        alter_type = ddl_statement['alter_type']
        
        # 테이블 존재 여부 확인
        cursor.execute("""
            SELECT COUNT(*) FROM information_schema.tables 
            WHERE table_schema = DATABASE() AND table_name = %s
        """, (table_name,))
        
        table_exists = cursor.fetchone()[0] > 0
        issues = []
        
        if not table_exists:
            issues.append(f"테이블 '{table_name}'이 존재하지 않습니다.")
            return {
                "table": table_name,
                "ddl_type": "ALTER_TABLE",
                "alter_type": alter_type,
                "valid": False,
                "issues": issues
            }
        
        # 현재 테이블의 컬럼 정보 조회
        cursor.execute("""
            SELECT column_name, data_type, character_maximum_length, numeric_precision, numeric_scale, is_nullable
            FROM information_schema.columns 
            WHERE table_schema = DATABASE() AND table_name = %s
        """, (table_name,))
        
        existing_columns = {row[0].lower(): {
            'data_type': row[1].upper(),
            'max_length': row[2],
            'precision': row[3],
            'scale': row[4],
            'is_nullable': row[5]
        } for row in cursor.fetchall()}
        
        # ALTER 유형별 검증
        if alter_type == 'ADD_COLUMN':
            column_name = ddl_statement['column']
            if column_name in existing_columns:
                issues.append(f"컬럼 '{column_name}'이 이미 존재합니다.")
        
        elif alter_type == 'DROP_COLUMN':
            column_name = ddl_statement['column']
            if column_name not in existing_columns:
                issues.append(f"컬럼 '{column_name}'이 존재하지 않습니다.")
        
        elif alter_type == 'MODIFY_COLUMN':
            column_name = ddl_statement['column']
            new_definition = ddl_statement['column_definition']
            
            if column_name not in existing_columns:
                issues.append(f"컬럼 '{column_name}'이 존재하지 않습니다.")
            else:
                # 데이터 타입 변경 가능성 검증
                validation_result = self.validate_column_type_change(
                    existing_columns[column_name], new_definition
                )
                if not validation_result['valid']:
                    issues.extend(validation_result['issues'])
        
        elif alter_type == 'CHANGE_COLUMN':
            old_column = ddl_statement['old_column']
            new_column = ddl_statement['new_column']
            new_definition = ddl_statement['column_definition']
            
            if old_column not in existing_columns:
                issues.append(f"기존 컬럼 '{old_column}'이 존재하지 않습니다.")
            elif new_column != old_column and new_column in existing_columns:
                issues.append(f"새 컬럼명 '{new_column}'이 이미 존재합니다.")
            else:
                # 데이터 타입 변경 가능성 검증
                validation_result = self.validate_column_type_change(
                    existing_columns[old_column], new_definition
                )
                if not validation_result['valid']:
                    issues.extend(validation_result['issues'])
        
        return {
            "table": table_name,
            "ddl_type": "ALTER_TABLE",
            "alter_type": alter_type,
            "valid": len(issues) == 0,
            "issues": issues,
            "details": {
                "existing_columns": list(existing_columns.keys())
            }
        }

    async def validate_create_index(self, cursor, ddl_statement: Dict[str, Any]) -> Dict[str, Any]:
        """CREATE INDEX 구문 검증"""
        table_name = ddl_statement['table']
        index_name = ddl_statement['index_name']
        columns = ddl_statement['columns']
        
        issues = []
        
        # 테이블 존재 여부 확인
        cursor.execute("""
            SELECT COUNT(*) FROM information_schema.tables 
            WHERE table_schema = DATABASE() AND table_name = %s
        """, (table_name,))
        
        table_exists = cursor.fetchone()[0] > 0
        
        if not table_exists:
            issues.append(f"테이블 '{table_name}'이 존재하지 않습니다.")
        else:
            # 인덱스 존재 여부 확인
            cursor.execute("""
                SELECT COUNT(*) FROM information_schema.statistics 
                WHERE table_schema = DATABASE() AND table_name = %s AND index_name = %s
            """, (table_name, index_name))
            
            index_exists = cursor.fetchone()[0] > 0
            
            if index_exists:
                issues.append(f"인덱스 '{index_name}'이 이미 존재합니다.")
            
            # 컬럼 존재 여부 확인
            cursor.execute("""
                SELECT column_name FROM information_schema.columns 
                WHERE table_schema = DATABASE() AND table_name = %s
            """, (table_name,))
            
            existing_columns = {row[0].lower() for row in cursor.fetchall()}
            
            for column in columns:
                if column not in existing_columns:
                    issues.append(f"컬럼 '{column}'이 테이블 '{table_name}'에 존재하지 않습니다.")
        
        return {
            "table": table_name,
            "ddl_type": "CREATE_INDEX",
            "valid": len(issues) == 0,
            "issues": issues,
            "details": {
                "index_name": index_name,
                "columns": columns
            }
        }

    async def validate_drop_table(self, cursor, ddl_statement: Dict[str, Any]) -> Dict[str, Any]:
        """DROP TABLE 구문 검증"""
        table_name = ddl_statement['table']
        
        # 테이블 존재 여부 확인
        cursor.execute("""
            SELECT COUNT(*) FROM information_schema.tables 
            WHERE table_schema = DATABASE() AND table_name = %s
        """, (table_name,))
        
        table_exists = cursor.fetchone()[0] > 0
        issues = []
        
        if not table_exists:
            issues.append(f"테이블 '{table_name}'이 존재하지 않습니다.")
        
        return {
            "table": table_name,
            "ddl_type": "DROP_TABLE",
            "valid": table_exists,
            "issues": issues,
            "details": {
                "table_exists": table_exists
            }
        }

    async def validate_drop_index(self, cursor, ddl_statement: Dict[str, Any]) -> Dict[str, Any]:
        """DROP INDEX 구문 검증"""
        table_name = ddl_statement['table']
        index_name = ddl_statement['index_name']
        
        print(f"[DEBUG] DROP INDEX 검증 시작: table={table_name}, index={index_name}")
        
        issues = []
        
        # 테이블 존재 여부 확인
        cursor.execute("""
            SELECT COUNT(*) FROM information_schema.tables 
            WHERE table_schema = DATABASE() AND table_name = %s
        """, (table_name,))
        
        table_exists = cursor.fetchone()[0] > 0
        print(f"[DEBUG] 테이블 '{table_name}' 존재 여부: {table_exists}")
        
        if not table_exists:
            issues.append(f"테이블 '{table_name}'이 존재하지 않습니다.")
            print(f"[DEBUG] 테이블 '{table_name}'이 존재하지 않음 - 이슈 추가")
        else:
            # 인덱스 존재 여부 확인
            cursor.execute("""
                SELECT COUNT(*) FROM information_schema.statistics 
                WHERE table_schema = DATABASE() AND table_name = %s AND index_name = %s
            """, (table_name, index_name))
            
            index_exists = cursor.fetchone()[0] > 0
            print(f"[DEBUG] 인덱스 '{index_name}' 존재 여부: {index_exists}")
            
            if not index_exists:
                issues.append(f"인덱스 '{index_name}'이 테이블 '{table_name}'에 존재하지 않습니다.")
                print(f"[DEBUG] 인덱스 '{index_name}'이 존재하지 않음 - 이슈 추가")
        
        result = {
            "table": table_name,
            "ddl_type": "DROP_INDEX",
            "valid": len(issues) == 0,
            "issues": issues,
            "details": {
                "index_name": index_name,
                "table_exists": table_exists
            }
        }
        
        print(f"[DEBUG] DROP INDEX 검증 완료: issues={len(issues)}, valid={len(issues) == 0}")
        print(f"[DEBUG] 최종 결과: {result}")
        
        return result

    def validate_column_type_change(self, existing_column: Dict[str, Any], new_definition: str) -> Dict[str, Any]:
        """컬럼 데이터 타입 변경 가능성 검증"""
        issues = []
        
        # 새로운 데이터 타입 파싱
        new_type_info = self.parse_data_type(new_definition.split()[0])
        existing_type = existing_column['data_type']
        
        # 호환되지 않는 타입 변경 검사
        incompatible_changes = [
            # 문자열 -> 숫자
            (['VARCHAR', 'CHAR', 'TEXT'], ['INT', 'BIGINT', 'DECIMAL', 'FLOAT', 'DOUBLE']),
            # 숫자 -> 문자열 (일반적으로 안전하지만 데이터 손실 가능)
            (['INT', 'BIGINT', 'DECIMAL', 'FLOAT', 'DOUBLE'], ['VARCHAR', 'CHAR']),
            # 날짜/시간 타입 변경
            (['DATE', 'DATETIME', 'TIMESTAMP'], ['INT', 'VARCHAR', 'CHAR']),
        ]
        
        for from_types, to_types in incompatible_changes:
            if existing_type in from_types and new_type_info['type'] in to_types:
                issues.append(f"데이터 타입을 {existing_type}에서 {new_type_info['type']}로 변경하는 것은 데이터 손실을 야기할 수 있습니다.")
        
        # 길이 축소 검사
        if existing_type in ['VARCHAR', 'CHAR'] and new_type_info['type'] in ['VARCHAR', 'CHAR']:
            existing_length = existing_column['max_length']
            new_length = new_type_info['length']
            
            if existing_length and new_length and new_length < existing_length:
                issues.append(f"컬럼 길이를 {existing_length}에서 {new_length}로 축소하는 것은 데이터 손실을 야기할 수 있습니다.")
        
        # 정밀도 축소 검사 (DECIMAL)
        if existing_type == 'DECIMAL' and new_type_info['type'] == 'DECIMAL':
            existing_precision = existing_column['precision']
            existing_scale = existing_column['scale']
            new_precision = new_type_info['precision']
            new_scale = new_type_info['scale']
            
            if (existing_precision and new_precision and new_precision < existing_precision) or \
               (existing_scale and new_scale and new_scale < existing_scale):
                issues.append(f"DECIMAL 정밀도를 ({existing_precision},{existing_scale})에서 ({new_precision},{new_scale})로 축소하는 것은 데이터 손실을 야기할 수 있습니다.")
        
        return {
            'valid': len(issues) == 0,
            'issues': issues
        }

    def parse_data_type(self, data_type_str: str) -> Dict[str, Any]:
        """데이터 타입 문자열을 파싱하여 타입과 길이 정보 추출"""
        # VARCHAR(255), INT(11), DECIMAL(10,2) 등을 파싱
        type_match = re.match(r'(\w+)(?:\(([^)]+)\))?', data_type_str.upper())
        if not type_match:
            return {'type': data_type_str.upper(), 'length': None, 'precision': None, 'scale': None}
        
        base_type = type_match.group(1)
        params = type_match.group(2)
        
        result = {'type': base_type, 'length': None, 'precision': None, 'scale': None}
        
        if params:
            if ',' in params:
                # DECIMAL(10,2) 형태
                parts = [p.strip() for p in params.split(',')]
                result['precision'] = int(parts[0]) if parts[0].isdigit() else None
                result['scale'] = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else None
            else:
                # VARCHAR(255), INT(11) 형태
                result['length'] = int(params) if params.isdigit() else None
        
        return result

    async def generate_html_report(self, report_path: Path, filename: str, ddl_content: str, 
                                 ddl_type: str, status: str, summary: str, issues: List[str],
                                 db_connection_info: Optional[Dict], schema_validation: Optional[Dict],
                                 constraint_validation: Optional[Dict], database_secret: Optional[str]):
        """HTML 보고서 생성"""
        try:
            # 상태에 따른 색상 및 아이콘
            status_color = "#28a745" if status == "PASS" else "#dc3545"
            status_icon = "✅" if status == "PASS" else "❌"
            
            # DB 연결 정보 섹션 제거 (요청사항에 따라)
            db_info_section = ""
            
            # 발견된 문제 섹션
            issues_section = ""
            if issues:
                issues_section = """
                <div class="issues-section">
                    <h3>🚨 발견된 문제</h3>
                    <ul class="issues-list">
                """
                for issue in issues:
                    issues_section += f"<li>{issue}</li>"
                issues_section += """
                    </ul>
                </div>
                """
            else:
                issues_section = """
                <div class="issues-section success">
                    <h3>✅ 발견된 문제</h3>
                    <p class="no-issues">문제가 발견되지 않았습니다.</p>
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
            
            {issues_section}
        </div>
        
        <div class="footer">
            <p>Generated by DB Assistant MCP Server</p>
            <p>Report generated at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        </div>
    </div>
</body>
</html>"""
            
            with open(report_path, 'w', encoding='utf-8') as f:
                f.write(report_content)
                
        except Exception as e:
            logger.error(f"HTML 보고서 생성 오류: {e}")

    async def generate_consolidated_html_report(self, validation_results: List[Dict], database_secret: str) -> str:
        """여러 SQL 파일의 통합 HTML 보고서 생성"""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            report_filename = f"consolidated_validation_report_{timestamp}.html"
            report_path = OUTPUT_DIR / report_filename
            
            # 전체 통계 계산
            total_files = len(validation_results)
            passed_files = sum(1 for r in validation_results if r['status'] == 'PASS')
            failed_files = total_files - passed_files
            
            # 파일별 결과 섹션 생성
            file_sections = ""
            for i, result in enumerate(validation_results, 1):
                status_icon = "✅" if result['status'] == 'PASS' else "❌"
                status_class = "success" if result['status'] == 'PASS' else "error"
                
                issues_html = ""
                if result['issues']:
                    issues_html = "<ul class='issues-list'>"
                    for issue in result['issues']:
                        issues_html += f"<li>{issue}</li>"
                    issues_html += "</ul>"
                else:
                    issues_html = "<p class='no-issues'>문제가 발견되지 않았습니다.</p>"
                
                file_sections += f"""
                <div class="file-section {status_class}">
                    <h3>{status_icon} {i}. {result['filename']}</h3>
                    <div class="file-details">
                        <div class="file-info">
                            <span><strong>DDL 타입:</strong> {result['ddl_type']}</span>
                            <span><strong>상태:</strong> {result['status']}</span>
                            <span><strong>문제 수:</strong> {len(result['issues'])}개</span>
                        </div>
                        <div class="sql-code">
{result['ddl_content']}
                        </div>
                        {f'<div class="issues-section"><h4>🚨 발견된 문제</h4>{issues_html}</div>' if result['issues'] else ''}
                    </div>
                </div>
                """
            
            # HTML 보고서 내용
            report_content = f"""<!DOCTYPE html>
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
            max-height: none;
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
            <h1>📊 통합 DDL 검증 보고서</h1>
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
            with open(report_path, 'w', encoding='utf-8') as f:
                f.write(report_content)
            
            return str(report_path)
            
        except Exception as e:
            logger.error(f"통합 HTML 보고서 생성 오류: {e}")
            return f"보고서 생성 실패: {str(e)}"

    async def validate_all_sql_files(self, database_secret: Optional[str] = None) -> str:
        """모든 SQL 파일 검증 및 통합 보고서 생성 (최대 5개)"""
        try:
            sql_files = list(SQL_DIR.glob("*.sql"))
            if not sql_files:
                return "sql 디렉토리에 SQL 파일이 없습니다."
            
            # 최대 5개 파일만 처리
            files_to_process = sql_files[:5]
            if len(sql_files) > 5:
                logger.warning(f"SQL 파일이 {len(sql_files)}개 있지만 처음 5개만 처리합니다.")
            
            validation_results = []
            summary_results = []
            
            for sql_file in files_to_process:
                try:
                    # 개별 파일 검증 (보고서 생성 없이)
                    ddl_content = sql_file.read_text(encoding='utf-8')
                    ddl_type = self.detect_ddl_type(ddl_content)
                    
                    # 데이터베이스 연결 및 검증
                    db_connection_info = None
                    issues = []
                    
                    if database_secret:
                        db_connection_info = await self.test_db_connection(database_secret)
                        if db_connection_info and db_connection_info.get("success"):
                            # 스키마 검증
                            schema_validation = await self.validate_schema(ddl_content, database_secret)
                            if schema_validation and not schema_validation.get("valid", True):
                                issues.extend(schema_validation.get("issues", []))
                            
                            # 제약조건 검증
                            constraint_validation = await self.validate_constraints(ddl_content, database_secret)
                            if constraint_validation and not constraint_validation.get("valid", True):
                                issues.extend(constraint_validation.get("issues", []))
                    
                    status = "PASS" if not issues else "FAIL"
                    
                    # 결과 저장
                    validation_results.append({
                        'filename': sql_file.name,
                        'ddl_content': ddl_content,
                        'ddl_type': ddl_type,
                        'status': status,
                        'issues': issues
                    })
                    
                    summary_results.append(f"**{sql_file.name}**: {'✅ 통과' if status == 'PASS' else f'❌ 실패 ({len(issues)}개 문제)'}")
                    
                except Exception as e:
                    validation_results.append({
                        'filename': sql_file.name,
                        'ddl_content': f"파일 읽기 실패: {str(e)}",
                        'ddl_type': "UNKNOWN",
                        'status': "FAIL",
                        'issues': [f"검증 실패: {str(e)}"]
                    })
                    summary_results.append(f"**{sql_file.name}**: ❌ 검증 실패 - {str(e)}")
            
            # 통합 HTML 보고서 생성
            if validation_results:
                report_path = await self.generate_consolidated_html_report(validation_results, database_secret or "N/A")
                
                # 통계 계산
                total_files = len(validation_results)
                passed_files = sum(1 for r in validation_results if r['status'] == 'PASS')
                failed_files = total_files - passed_files
                
                summary = f"📊 총 {total_files}개 파일 검증 완료"
                if len(sql_files) > 5:
                    summary += f" (전체 {len(sql_files)}개 중 5개 처리)"
                
                summary += f"\n• 통과: {passed_files}개 ({round(passed_files/total_files*100)}%)"
                summary += f"\n• 실패: {failed_files}개 ({round(failed_files/total_files*100)}%)"
                summary += f"\n\n📄 통합 보고서가 저장되었습니다: {report_path}"
                
                return f"{summary}\n\n" + "\n".join(summary_results)
            else:
                return "검증할 파일이 없습니다."
            
        except Exception as e:
            return f"전체 SQL 파일 검증 실패: {str(e)}"

    async def copy_sql_file(self, source_path: str, target_name: Optional[str] = None) -> str:
        """SQL 파일을 sql 디렉토리로 복사"""
        try:
            source = Path(source_path)
            if not source.exists():
                return f"소스 파일을 찾을 수 없습니다: {source_path}"
            
            if not source.suffix.lower() == '.sql':
                return f"SQL 파일이 아닙니다: {source_path}"
            
            # 대상 파일명 결정
            if target_name:
                if not target_name.endswith('.sql'):
                    target_name += '.sql'
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
    
    def setup_cloudwatch_client(self, region_name: str = 'us-east-1'):
        """CloudWatch 클라이언트 설정"""
        try:
            self.cloudwatch = boto3.client('cloudwatch', region_name=region_name)
            return True
        except Exception as e:
            logger.error(f"CloudWatch 클라이언트 설정 실패: {e}")
            return False

    async def collect_db_metrics(self, db_instance_identifier: str, hours: int = 24, 
                               metrics: Optional[List[str]] = None, region: str = 'us-east-1') -> str:
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

    async def analyze_metric_correlation(self, csv_file: str, target_metric: str = 'CPUUtilization', 
                                       top_n: int = 10) -> str:
        """메트릭 간 상관관계 분석"""
        if not ANALYSIS_AVAILABLE:
            return "❌ 분석 라이브러리가 설치되지 않았습니다."
        
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

    async def detect_metric_outliers(self, csv_file: str, std_threshold: float = 2.0) -> str:
        """아웃라이어 탐지"""
        if not ANALYSIS_AVAILABLE:
            return "❌ 분석 라이브러리가 설치되지 않았습니다."
        
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
        if not ANALYSIS_AVAILABLE:
            return "❌ 분석 라이브러리가 설치되지 않았습니다."
        
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
        if not ANALYSIS_AVAILABLE:
            return "❌ 분석 라이브러리가 설치되지 않았습니다."
        
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
            inputSchema={
                "type": "object",
                "properties": {}
            }
        ),
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
            name="list_databases",
            description="데이터베이스 목록을 조회하고 선택할 수 있습니다",
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
            name="select_database",
            description="특정 데이터베이스를 선택합니다 (USE 명령어 실행). 먼저 list_databases로 목록을 확인한 후 번호나 이름으로 선택하세요.",
            inputSchema={
                "type": "object",
                "properties": {
                    "database_secret": {
                        "type": "string",
                        "description": "데이터베이스 시크릿 이름"
                    },
                    "database_selection": {
                        "type": "string",
                        "description": "선택할 데이터베이스 (번호 또는 이름)"
                    }
                },
                "required": ["database_secret", "database_selection"]
            }
        ),
        types.Tool(
            name="get_schema_summary",
            description="현재 데이터베이스 스키마의 요약 정보를 제공합니다",
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
            name="get_table_schema",
            description="특정 테이블의 상세 스키마 정보를 조회합니다",
            inputSchema={
                "type": "object",
                "properties": {
                    "database_secret": {
                        "type": "string",
                        "description": "데이터베이스 시크릿 이름"
                    },
                    "table_name": {
                        "type": "string",
                        "description": "조회할 테이블 이름"
                    }
                },
                "required": ["database_secret", "table_name"]
            }
        ),
        types.Tool(
            name="get_table_index",
            description="특정 테이블의 인덱스 정보를 조회합니다",
            inputSchema={
                "type": "object",
                "properties": {
                    "database_secret": {
                        "type": "string",
                        "description": "데이터베이스 시크릿 이름"
                    },
                    "table_name": {
                        "type": "string",
                        "description": "조회할 테이블 이름"
                    }
                },
                "required": ["database_secret", "table_name"]
            }
        ),
        types.Tool(
            name="get_performance_metrics",
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
                        "description": "메트릭 타입 (all, query, io, memory, connection)",
                        "enum": ["all", "query", "io", "memory", "connection"],
                        "default": "all"
                    }
                },
                "required": ["database_secret"]
            }
        ),
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
            name="validate_sql_file",
            description="특정 SQL 파일을 검증합니다",
            inputSchema={
                "type": "object",
                "properties": {
                    "filename": {
                        "type": "string",
                        "description": "검증할 SQL 파일명"
                    },
                    "database_secret": {
                        "type": "string", 
                        "description": "데이터베이스 시크릿 이름 (선택사항)"
                    }
                },
                "required": ["filename"]
            }
        ),
        types.Tool(
            name="validate_all_sql",
            description="sql 디렉토리의 SQL 파일들을 검증합니다 (최대 5개)",
            inputSchema={
                "type": "object",
                "properties": {
                    "database_secret": {
                        "type": "string",
                        "description": "데이터베이스 시크릿 이름 (선택사항)"
                    }
                }
            }
        ),
        types.Tool(
            name="copy_sql_to_directory",
            description="SQL 파일을 sql 디렉토리로 복사합니다",
            inputSchema={
                "type": "object",
                "properties": {
                    "source_path": {
                        "type": "string",
                        "description": "복사할 SQL 파일의 경로"
                    },
                    "target_name": {
                        "type": "string",
                        "description": "대상 파일명 (선택사항, 기본값은 원본 파일명)"
                    }
                },
                "required": ["source_path"]
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
            result = await db_assistant.list_databases(
                arguments["database_secret"]
            )
        elif name == "select_database":
            result = await db_assistant.select_database(
                arguments["database_secret"],
                arguments["database_selection"]
            )
        elif name == "get_schema_summary":
            result = await db_assistant.get_schema_summary(
                arguments["database_secret"]
            )
        elif name == "get_table_schema":
            result = await db_assistant.get_table_schema(
                arguments["database_secret"],
                arguments["table_name"]
            )
        elif name == "get_table_index":
            result = await db_assistant.get_table_index(
                arguments["database_secret"],
                arguments["table_name"]
            )
        elif name == "get_performance_metrics":
            result = await db_assistant.get_performance_metrics(
                arguments["database_secret"],
                arguments.get("metric_type", "all")
            )
        elif name == "collect_db_metrics":
            result = await db_assistant.collect_db_metrics(
                arguments["db_instance_identifier"],
                arguments.get("hours", 24),
                arguments.get("metrics"),
                arguments.get("region", "us-east-1")
            )
        elif name == "analyze_metric_correlation":
            result = await db_assistant.analyze_metric_correlation(
                arguments["csv_file"],
                arguments.get("target_metric", "CPUUtilization"),
                arguments.get("top_n", 10)
            )
        elif name == "detect_metric_outliers":
            result = await db_assistant.detect_metric_outliers(
                arguments["csv_file"],
                arguments.get("std_threshold", 2.0)
            )
        elif name == "perform_regression_analysis":
            result = await db_assistant.perform_regression_analysis(
                arguments["csv_file"],
                arguments["predictor_metric"],
                arguments.get("target_metric", "CPUUtilization")
            )
        elif name == "list_data_files":
            result = await db_assistant.list_data_files()
        elif name == "validate_sql_file":
            result = await db_assistant.validate_sql_file(
                arguments["filename"],
                arguments.get("database_secret")
            )
        elif name == "validate_all_sql":
            result = await db_assistant.validate_all_sql_files(
                arguments.get("database_secret")
            )
        elif name == "copy_sql_to_directory":
            result = await db_assistant.copy_sql_file(
                arguments["source_path"],
                arguments.get("target_name")
            )
        elif name == "get_metric_summary":
            result = await db_assistant.get_metric_summary(arguments["csv_file"])
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
