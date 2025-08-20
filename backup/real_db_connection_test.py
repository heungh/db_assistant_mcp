#!/usr/bin/env python3
import asyncio
import mysql.connector
import boto3
import json
import subprocess
import time
import logging

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class RealDatabaseTest:
    def __init__(self):
        self.selected_database = "test"
        self.ssh_tunnel_process = None
    
    def setup_ssh_tunnel(self, db_host: str, region: str = "ap-northeast-2") -> bool:
        """SSH 터널 설정 (기존 MCP 서버와 동일)"""
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
                "-L", f"3307:{db_host}:3306",
                "-N", "-f",
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

    def get_db_connection(self, database_secret: str, selected_database: str = None, use_ssh_tunnel: bool = True):
        """공통 DB 연결 함수 (기존 MCP 서버와 동일)"""
        try:
            # AWS Secrets Manager에서 DB 정보 가져오기
            secrets_client = boto3.client('secretsmanager', region_name='ap-northeast-2')
            secret_response = secrets_client.get_secret_value(SecretId=database_secret)
            db_config = json.loads(secret_response['SecretString'])
            
            tunnel_used = False
            
            if use_ssh_tunnel:
                if self.setup_ssh_tunnel(db_config.get('host')):
                    connection_config = {
                        'host': 'localhost',
                        'port': 3307,
                        'user': db_config['username'],
                        'password': db_config['password'],
                        'autocommit': True
                    }
                    tunnel_used = True
                else:
                    logger.error("SSH 터널 설정 실패, 직접 연결 시도")
                    connection_config = {
                        'host': db_config['host'],
                        'port': db_config.get('port', 3306),
                        'user': db_config['username'],
                        'password': db_config['password'],
                        'autocommit': True
                    }
            else:
                connection_config = {
                    'host': db_config['host'],
                    'port': db_config.get('port', 3306),
                    'user': db_config['username'],
                    'password': db_config['password'],
                    'autocommit': True
                }
            
            # 데이터베이스 지정
            if selected_database:
                connection_config['database'] = selected_database
            
            connection = mysql.connector.connect(**connection_config)
            logger.info(f"DB 연결 성공: {database_secret}")
            
            return connection, tunnel_used
            
        except Exception as e:
            logger.error(f"DB 연결 실패: {e}")
            return None, False

    def get_table_info(self, cursor):
        """커서를 받아서 테이블 정보 조회"""
        try:
            query = """
            SELECT 
                TABLE_NAME,
                TABLE_TYPE,
                ENGINE,
                TABLE_ROWS,
                DATA_LENGTH,
                CREATE_TIME
            FROM information_schema.TABLES 
            WHERE TABLE_SCHEMA = %s 
            LIMIT 1
            """
            
            cursor.execute(query, (self.selected_database,))
            result = cursor.fetchone()
            
            if result:
                print(f"📋 테이블 정보:")
                print(f"  - 테이블명: {result[0]}")
                print(f"  - 타입: {result[1]}")
                print(f"  - 엔진: {result[2]}")
                print(f"  - 행 수: {result[3]}")
                print(f"  - 데이터 크기: {result[4]} bytes")
                print(f"  - 생성일: {result[5]}")
                return result[0]  # 테이블명 반환
            else:
                print("❌ 테이블을 찾을 수 없습니다.")
                return None
                
        except Exception as e:
            print(f"❌ 테이블 조회 실패: {e}")
            return None
    
    def get_column_info(self, cursor, table_name):
        """커서를 받아서 컬럼 정보 조회"""
        try:
            query = """
            SELECT 
                COLUMN_NAME,
                DATA_TYPE,
                IS_NULLABLE,
                COLUMN_DEFAULT,
                COLUMN_KEY
            FROM information_schema.COLUMNS 
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
            LIMIT 5
            """
            
            cursor.execute(query, (self.selected_database, table_name))
            results = cursor.fetchall()
            
            if results:
                print(f"\n📋 {table_name} 컬럼 정보:")
                for col in results:
                    print(f"  - {col[0]}: {col[1]} "
                          f"{'NULL' if col[2] == 'YES' else 'NOT NULL'} "
                          f"{col[4] if col[4] else ''}")
            
            return results
            
        except Exception as e:
            print(f"❌ 컬럼 조회 실패: {e}")
            return None

    def get_database_info(self, cursor):
        """현재 데이터베이스 정보 조회"""
        try:
            cursor.execute("SELECT DATABASE() as current_db")
            current_db = cursor.fetchone()
            
            cursor.execute("SELECT VERSION() as version")
            version = cursor.fetchone()
            
            print(f"\n📍 데이터베이스 정보:")
            print(f"  - 현재 DB: {current_db[0]}")
            print(f"  - MySQL 버전: {version[0]}")
            
            return current_db[0], version[0]
            
        except Exception as e:
            print(f"❌ DB 정보 조회 실패: {e}")
            return None, None

def main():
    """메인 함수 - 실제 DB 연결 테스트"""
    db_test = RealDatabaseTest()
    
    try:
        print("🔌 gamedb1-cluster 연결 테스트 시작...")
        
        # 1. 한 번만 DB 연결
        connection, tunnel_used = db_test.get_db_connection("gamedb1-cluster")
        
        if not connection:
            print("❌ DB 연결 실패로 종료")
            return
        
        print(f"✅ DB 연결 성공 (터널 사용: {tunnel_used})")
        
        # 2. 커서 생성 (한 번만)
        cursor = connection.cursor()
        
        # 3. 여러 쿼리를 같은 커서로 실행
        db_name, version = db_test.get_database_info(cursor)
        table_name = db_test.get_table_info(cursor)
        
        if table_name:
            db_test.get_column_info(cursor, table_name)
        
        print(f"\n✅ 모든 작업 완료 - 연결은 한 번만! (터널: {tunnel_used})")
        
    except Exception as e:
        print(f"❌ 오류 발생: {e}")
        
    finally:
        # 4. 마지막에 연결 정리
        try:
            if 'cursor' in locals() and cursor:
                cursor.close()
                print("✅ 커서 닫기 완료")
            if 'connection' in locals() and connection:
                connection.close()
                print("✅ DB 연결 닫기 완료")
            if tunnel_used:
                db_test.cleanup_ssh_tunnel()
                print("✅ SSH 터널 정리 완료")
        except Exception as e:
            print(f"❌ 정리 중 오류: {e}")

if __name__ == "__main__":
    main()
