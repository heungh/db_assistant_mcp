#!/usr/bin/env python3
import mysql.connector

# SSH 터널을 통한 연결 테스트
try:
    connection = mysql.connector.connect(
        host='localhost',
        port=3307,
        user='dbadmin',
        password='12345678',
        database='test',
        connection_timeout=10
    )
    
    if connection.is_connected():
        print("✅ SSH 터널 연결 성공!")
        
        cursor = connection.cursor()
        
        # SELECT DATABASE() 테스트
        print("1. SELECT DATABASE() 테스트:")
        cursor.execute("SELECT DATABASE()")
        result = cursor.fetchone()
        print(f"   결과: {result}")
        print(f"   타입: {type(result)}")
        if result:
            print(f"   첫 번째 값: {result[0]}")
        
        cursor.close()
        connection.close()
        print("✅ 연결 종료")
    else:
        print("❌ 연결 실패")
        
except Exception as e:
    import traceback
    print(f"❌ 오류 발생: {e}")
    print(f"상세 오류:\n{traceback.format_exc()}")
