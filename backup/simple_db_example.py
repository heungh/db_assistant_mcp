#!/usr/bin/env python3
"""
DB 연결 재사용 예제 - 핵심 개념 설명
한 번 연결해서 여러 함수에서 커서 재사용하는 방법
"""

class DatabaseExample:
    def __init__(self):
        self.connection = None
        self.cursor = None
    
    def connect_once(self):
        """1. 메인에서 한 번만 DB 연결"""
        print("🔌 DB 연결 중...")
        # 실제로는 mysql.connector.connect() 사용
        self.connection = "mock_connection"
        self.cursor = "mock_cursor"
        print("✅ DB 연결 완료")
        return self.cursor
    
    def get_table_info(self, cursor):
        """2. 커서를 받아서 테이블 정보 조회"""
        print(f"📋 테이블 조회 (커서: {cursor})")
        # 실제로는: cursor.execute("SELECT * FROM information_schema.TABLES LIMIT 1")
        mock_table = {
            'TABLE_NAME': 'users',
            'TABLE_TYPE': 'BASE TABLE',
            'ENGINE': 'InnoDB'
        }
        print(f"  - 테이블: {mock_table['TABLE_NAME']}")
        return mock_table
    
    def get_column_info(self, cursor, table_name):
        """3. 같은 커서로 컬럼 정보 조회"""
        print(f"📋 컬럼 조회 (커서: {cursor}, 테이블: {table_name})")
        # 실제로는: cursor.execute("SELECT * FROM information_schema.COLUMNS WHERE TABLE_NAME = %s", (table_name,))
        mock_columns = [
            {'COLUMN_NAME': 'id', 'DATA_TYPE': 'int'},
            {'COLUMN_NAME': 'name', 'DATA_TYPE': 'varchar'}
        ]
        for col in mock_columns:
            print(f"  - {col['COLUMN_NAME']}: {col['DATA_TYPE']}")
        return mock_columns
    
    def get_index_info(self, cursor, table_name):
        """4. 같은 커서로 인덱스 정보 조회"""
        print(f"📋 인덱스 조회 (커서: {cursor}, 테이블: {table_name})")
        # 실제로는: cursor.execute("SHOW INDEX FROM %s", (table_name,))
        mock_indexes = [
            {'Key_name': 'PRIMARY', 'Column_name': 'id'},
            {'Key_name': 'idx_name', 'Column_name': 'name'}
        ]
        for idx in mock_indexes:
            print(f"  - {idx['Key_name']}: {idx['Column_name']}")
        return mock_indexes
    
    def cleanup(self):
        """5. 마지막에 연결 정리"""
        print("🧹 연결 정리")
        if self.cursor:
            # 실제로는: self.cursor.close()
            print("  - 커서 닫기")
        if self.connection:
            # 실제로는: self.connection.close()
            print("  - 연결 닫기")

def main():
    """메인 함수 - 연결 재사용 패턴"""
    db = DatabaseExample()
    
    try:
        # 1. 한 번만 연결
        cursor = db.connect_once()
        
        # 2. 여러 함수에서 같은 커서 재사용
        table_info = db.get_table_info(cursor)
        db.get_column_info(cursor, table_info['TABLE_NAME'])
        db.get_index_info(cursor, table_info['TABLE_NAME'])
        
        print("\n✅ 모든 작업 완료 - 연결은 한 번만!")
        
    finally:
        # 3. 마지막에 정리
        db.cleanup()

if __name__ == "__main__":
    main()
