#!/usr/bin/env python3
import asyncio
import mysql.connector
import boto3
import json
import subprocess
import time

class DatabaseConnectionExample:
    def __init__(self):
        self.ssh_tunnel_process = None
        self.selected_database = "test"
    
    async def get_db_connection_once(self, database_secret: str):
        """한 번만 DB 연결을 수행하고 커서 반환"""
        try:
            # AWS Secrets Manager에서 DB 정보 가져오기
            secrets_client = boto3.client('secretsmanager', region_name='ap-northeast-2')
            secret_response = secrets_client.get_secret_value(SecretId=database_secret)
            secret_data = json.loads(secret_response['SecretString'])
            
            # SSH 터널 설정
            ssh_command = [
                'ssh', '-i', '/Users/heungh/test.pem',
                '-L', f"3307:{secret_data['host']}:3306",
                '-N', '-f', 'ec2-user@54.180.79.255'
            ]
            
            print("SSH 터널 설정 중...")
            self.ssh_tunnel_process = subprocess.Popen(ssh_command)
            time.sleep(3)  # 터널 설정 대기
            
            # MySQL 연결
            connection = mysql.connector.connect(
                host='127.0.0.1',
                port=3307,
                user=secret_data['username'],
                password=secret_data['password'],
                database=self.selected_database,
                autocommit=True
            )
            
            cursor = connection.cursor(dictionary=True)
            print(f"✅ DB 연결 성공: {database_secret} -> {self.selected_database}")
            
            return connection, cursor
            
        except Exception as e:
            print(f"❌ DB 연결 실패: {e}")
            return None, None
    
    def get_table_list(self, cursor):
        """커서를 받아서 테이블 목록 조회"""
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
                print(f"  - 테이블명: {result['TABLE_NAME']}")
                print(f"  - 타입: {result['TABLE_TYPE']}")
                print(f"  - 엔진: {result['ENGINE']}")
                print(f"  - 행 수: {result['TABLE_ROWS']}")
                print(f"  - 데이터 크기: {result['DATA_LENGTH']} bytes")
                print(f"  - 생성일: {result['CREATE_TIME']}")
            else:
                print("❌ 테이블을 찾을 수 없습니다.")
                
            return result
            
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
                    print(f"  - {col['COLUMN_NAME']}: {col['DATA_TYPE']} "
                          f"{'NULL' if col['IS_NULLABLE'] == 'YES' else 'NOT NULL'} "
                          f"{col['COLUMN_KEY']}")
            
            return results
            
        except Exception as e:
            print(f"❌ 컬럼 조회 실패: {e}")
            return None
    
    def cleanup_connection(self, connection, cursor):
        """연결 정리"""
        try:
            if cursor:
                cursor.close()
            if connection:
                connection.close()
            if self.ssh_tunnel_process:
                self.ssh_tunnel_process.terminate()
            print("✅ 연결 정리 완료")
        except Exception as e:
            print(f"❌ 연결 정리 실패: {e}")

async def main():
    """메인 함수 - 한 번 연결해서 여러 쿼리 실행"""
    db_example = DatabaseConnectionExample()
    
    # 1. 한 번만 DB 연결
    connection, cursor = await db_example.get_db_connection_once("gamedb1-cluster")
    
    if not cursor:
        print("❌ DB 연결 실패로 종료")
        return
    
    try:
        # 2. 첫 번째 함수: 테이블 정보 조회
        table_info = db_example.get_table_list(cursor)
        
        if table_info:
            # 3. 두 번째 함수: 컬럼 정보 조회 (같은 커서 재사용)
            db_example.get_column_info(cursor, table_info['TABLE_NAME'])
        
        # 4. 추가 쿼리도 같은 커서로 실행 가능
        cursor.execute("SELECT DATABASE() as current_db")
        current_db = cursor.fetchone()
        print(f"\n📍 현재 DB: {current_db['current_db']}")
        
    finally:
        # 5. 마지막에 연결 정리
        db_example.cleanup_connection(connection, cursor)

if __name__ == "__main__":
    asyncio.run(main())
