#!/usr/bin/env python3
"""
기존 MCP 서버에 DML 컬럼 검증을 추가하는 패치
"""

# validate_ddl 함수의 Claude 검증 전에 추가할 코드

def add_dml_column_validation():
    """
    이 코드를 validate_ddl 함수의 1270라인 근처 (Claude 검증 시작 전)에 추가
    """
    
    # 추가할 import (파일 상단에)
    import sqlparse
    
    # validate_ddl 함수 내부에 추가할 코드:
    
    # 3.5. DML 쿼리의 컬럼 존재 여부 검증 (Claude 검증 전에 추가)
    dml_column_issues = []
    if sql_type in ['SELECT', 'UPDATE', 'DELETE'] and database_secret:
        try:
            debug_log("DML 컬럼 검증 시작")
            cursor = self.get_shared_cursor()
            if cursor:
                dml_validation_result = self.validate_dml_columns(ddl_content, cursor, debug_log)
                if dml_validation_result['queries_with_issues'] > 0:
                    for query_result in dml_validation_result['results']:
                        for issue in query_result['issues']:
                            dml_column_issues.append({
                                'type': 'COLUMN_NOT_EXISTS',
                                'severity': 'ERROR',
                                'message': issue['message'],
                                'table': issue['table'],
                                'column': issue['column'],
                                'query_type': query_result['query_type']
                            })
                            debug_log(f"DML 컬럼 오류 발견: {issue['message']}")
                
                debug_log(f"DML 컬럼 검증 완료: {len(dml_column_issues)}개 이슈 발견")
            else:
                debug_log("DML 컬럼 검증 건너뜀: 공용 커서 없음")
        except Exception as e:
            debug_log(f"DML 컬럼 검증 오류: {e}")
    
    # 그리고 최종 이슈 집계 부분에서 dml_column_issues를 추가:
    # all_issues.extend(dml_column_issues)


def validate_dml_columns_method():
    """
    DDLValidationMCPServer 클래스에 추가할 메서드
    """
    
    def validate_dml_columns(self, sql_content: str, cursor, debug_log) -> dict:
        """DML 쿼리의 컬럼 존재 여부 검증"""
        try:
            import sqlparse
            import re
            
            # 스키마 캐시
            schema_cache = {}
            
            def get_table_columns(table_name: str) -> set:
                """테이블의 컬럼 목록 조회"""
                if table_name in schema_cache:
                    return schema_cache[table_name]
                
                try:
                    cursor.execute("""
                        SELECT column_name 
                        FROM information_schema.columns 
                        WHERE table_schema = DATABASE() AND table_name = %s
                    """, (table_name,))
                    
                    columns = {row[0].lower() for row in cursor.fetchall()}
                    schema_cache[table_name] = columns
                    debug_log(f"테이블 '{table_name}' 컬럼 조회: {columns}")
                    return columns
                except Exception as e:
                    debug_log(f"테이블 '{table_name}' 컬럼 조회 실패: {e}")
                    return set()
            
            # SQL 문에서 사용된 컬럼들 추출
            validation_results = []
            
            # 주석 제거
            cleaned_sql = re.sub(r'--.*$', '', sql_content, flags=re.MULTILINE)
            
            # 각 쿼리별로 검증
            statements = sqlparse.split(cleaned_sql)
            
            for stmt in statements:
                if not stmt.strip():
                    continue
                
                stmt_lower = stmt.lower()
                issues = []
                
                # SELECT, UPDATE, DELETE 쿼리에서 컬럼 추출
                if any(keyword in stmt_lower for keyword in ['select', 'update', 'delete']):
                    # 테이블.컬럼 형태의 컬럼 참조 찾기
                    column_refs = re.findall(r'(\w+)\.(\w+)', stmt_lower)
                    
                    # FROM 절에서 테이블 추출
                    from_tables = re.findall(r'from\s+(\w+)(?:\s+(?:as\s+)?(\w+))?', stmt_lower)
                    join_tables = re.findall(r'join\s+(\w+)(?:\s+(?:as\s+)?(\w+))?', stmt_lower)
                    
                    # 테이블 별칭 매핑
                    table_aliases = {}
                    all_tables = set()
                    
                    for table, alias in from_tables + join_tables:
                        all_tables.add(table)
                        if alias:
                            table_aliases[alias] = table
                    
                    # 컬럼 존재 여부 검증
                    for table_ref, column_name in column_refs:
                        # 실제 테이블명 해결
                        actual_table = table_aliases.get(table_ref, table_ref)
                        
                        if actual_table in all_tables or actual_table in schema_cache or self._table_exists(actual_table, cursor):
                            existing_columns = get_table_columns(actual_table)
                            
                            if column_name not in existing_columns and column_name != '*':
                                issues.append({
                                    'type': 'MISSING_COLUMN',
                                    'table': actual_table,
                                    'column': column_name,
                                    'message': f"컬럼 '{column_name}'이 테이블 '{actual_table}'에 존재하지 않습니다"
                                })
                
                if issues:
                    validation_results.append({
                        'sql': stmt.strip()[:100] + '...' if len(stmt.strip()) > 100 else stmt.strip(),
                        'issues': issues
                    })
            
            return {
                'total_queries': len([s for s in statements if s.strip()]),
                'queries_with_issues': len(validation_results),
                'results': validation_results
            }
            
        except Exception as e:
            debug_log(f"DML 컬럼 검증 예외: {e}")
            return {'total_queries': 0, 'queries_with_issues': 0, 'results': []}
    
    def _table_exists(self, table_name: str, cursor) -> bool:
        """테이블 존재 여부 확인"""
        try:
            cursor.execute("""
                SELECT COUNT(*) FROM information_schema.tables 
                WHERE table_schema = DATABASE() AND table_name = %s
            """, (table_name,))
            return cursor.fetchone()[0] > 0
        except:
            return False


if __name__ == "__main__":
    print("이 파일은 패치 가이드입니다.")
    print("1. validate_dml_columns 메서드를 DDLValidationMCPServer 클래스에 추가")
    print("2. validate_ddl 함수의 Claude 검증 전에 DML 컬럼 검증 로직 추가")
    print("3. 최종 이슈 집계에 dml_column_issues 포함")
