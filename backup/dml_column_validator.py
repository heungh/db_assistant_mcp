#!/usr/bin/env python3
"""
DML 쿼리의 컬럼 존재 여부 검증 모듈
"""

import re
import sqlparse
from typing import Dict, List, Set, Tuple


class DMLColumnValidator:
    """DML 쿼리에서 사용되는 컬럼들의 존재 여부를 검증하는 클래스"""
    
    def __init__(self, cursor):
        self.cursor = cursor
        self.schema_cache = {}
    
    def get_table_columns(self, table_name: str) -> Set[str]:
        """테이블의 컬럼 목록을 가져와서 캐시"""
        if table_name in self.schema_cache:
            return self.schema_cache[table_name]
        
        try:
            self.cursor.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_schema = DATABASE() AND table_name = %s
            """, (table_name,))
            
            columns = {row[0].lower() for row in self.cursor.fetchall()}
            self.schema_cache[table_name] = columns
            return columns
        except Exception:
            return set()
    
    def parse_dml_queries(self, sql_content: str) -> List[Dict]:
        """DML 쿼리들을 파싱하여 사용된 테이블과 컬럼 추출"""
        queries = []
        
        # 주석 제거 후 쿼리 분리
        cleaned_sql = re.sub(r'--.*$', '', sql_content, flags=re.MULTILINE)
        statements = sqlparse.split(cleaned_sql)
        
        for stmt in statements:
            if not stmt.strip():
                continue
                
            parsed = sqlparse.parse(stmt)[0]
            query_type = self._get_query_type(parsed)
            
            if query_type in ['SELECT', 'UPDATE', 'DELETE']:
                tables, columns = self._extract_tables_and_columns(parsed, query_type)
                queries.append({
                    'type': query_type,
                    'sql': stmt.strip(),
                    'tables': tables,
                    'columns': columns
                })
        
        return queries
    
    def _get_query_type(self, parsed) -> str:
        """쿼리 타입 추출"""
        for token in parsed.tokens:
            if token.ttype is sqlparse.tokens.Keyword.DML:
                return token.value.upper()
        return 'UNKNOWN'
    
    def _extract_tables_and_columns(self, parsed, query_type: str) -> Tuple[Set[str], Dict[str, Set[str]]]:
        """테이블과 컬럼 추출"""
        tables = set()
        columns = {}  # table_name -> set of columns
        
        # 간단한 정규식 기반 추출 (복잡한 파싱 대신)
        sql_text = str(parsed).lower()
        
        # FROM 절에서 테이블 추출
        from_pattern = r'from\s+(\w+)(?:\s+as\s+(\w+))?'
        from_matches = re.findall(from_pattern, sql_text)
        
        for table_name, alias in from_matches:
            tables.add(table_name)
            if alias:
                tables.add(alias)
        
        # JOIN 절에서 테이블 추출
        join_pattern = r'join\s+(\w+)(?:\s+as\s+(\w+))?'
        join_matches = re.findall(join_pattern, sql_text)
        
        for table_name, alias in join_matches:
            tables.add(table_name)
            if alias:
                tables.add(alias)
        
        # 컬럼 추출 (테이블.컬럼 형태)
        column_pattern = r'(\w+)\.(\w+)'
        column_matches = re.findall(column_pattern, sql_text)
        
        for table_ref, column_name in column_matches:
            if table_ref not in columns:
                columns[table_ref] = set()
            columns[table_ref].add(column_name)
        
        return tables, columns
    
    def validate_dml_columns(self, sql_content: str) -> Dict:
        """DML 쿼리의 컬럼 존재 여부 검증"""
        queries = self.parse_dml_queries(sql_content)
        validation_results = []
        
        for query in queries:
            result = {
                'query_type': query['type'],
                'sql': query['sql'][:100] + '...' if len(query['sql']) > 100 else query['sql'],
                'issues': []
            }
            
            # 각 테이블의 컬럼 검증
            for table_ref, used_columns in query['columns'].items():
                # 테이블 별칭을 실제 테이블명으로 매핑 (간단한 경우만)
                actual_table = self._resolve_table_name(table_ref, query['tables'])
                
                if actual_table:
                    existing_columns = self.get_table_columns(actual_table)
                    
                    for column in used_columns:
                        if column not in existing_columns:
                            result['issues'].append({
                                'type': 'MISSING_COLUMN',
                                'table': actual_table,
                                'column': column,
                                'message': f"컬럼 '{column}'이 테이블 '{actual_table}'에 존재하지 않습니다"
                            })
            
            validation_results.append(result)
        
        return {
            'total_queries': len(queries),
            'queries_with_issues': len([r for r in validation_results if r['issues']]),
            'results': validation_results
        }
    
    def _resolve_table_name(self, table_ref: str, all_tables: Set[str]) -> str:
        """테이블 참조를 실제 테이블명으로 해결"""
        # 간단한 매핑 (실제로는 더 복잡한 로직 필요)
        if table_ref in all_tables:
            return table_ref
        
        # 별칭인 경우 실제 테이블명 찾기 (여기서는 간단히 처리)
        return table_ref
