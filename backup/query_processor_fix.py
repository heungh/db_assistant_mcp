def split_multi_queries(sql_content):
    """다중 쿼리를 개별 쿼리로 분리"""
    import re
    
    # 주석 제거 후 쿼리 분리
    clean_sql = re.sub(r'--.*?\n', '\n', sql_content)
    queries = [q.strip() for q in clean_sql.split(';') if q.strip()]
    
    return queries

def get_explain_as_string(cursor, query):
    """EXPLAIN 결과를 문자열로 반환 (파싱하지 않음)"""
    try:
        cursor.execute(f"EXPLAIN {query}")
        results = cursor.fetchall()
        
        # 결과를 문자열로 변환
        explain_str = "EXPLAIN 결과:\n"
        for row in results:
            explain_str += str(row) + "\n"
            
        return explain_str
        
    except Exception as e:
        return f"EXPLAIN 실행 오류: {str(e)}"
