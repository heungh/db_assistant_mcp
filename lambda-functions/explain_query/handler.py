"""
Lambda Function: explain_query
쿼리 실행 계획 분석 (EXPLAIN) 및 S3 저장
"""

import json
import logging
import os
from typing import Dict, Any
from datetime import datetime

import boto3
import pymysql

logger = logging.getLogger()
logger.setLevel(logging.INFO)

S3_BUCKET = os.getenv('QUERY_RESULTS_BUCKET', 'db-assistant-query-results')


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    쿼리 실행 계획 분석 (EXPLAIN)

    입력:
    {
        "secret_name": "rds/gamedb1/admin",
        "database": "gamedb",
        "query": "SELECT * FROM users WHERE email = 'john@example.com'",
        "format": "json",  # "traditional", "json", "tree"
        "region": "ap-northeast-2"
    }

    출력:
    {
        "status": "success",
        "query": "SELECT * FROM users WHERE email = 'john@example.com'",
        "explain_result": [
            {
                "id": 1,
                "select_type": "SIMPLE",
                "table": "users",
                "type": "ALL",
                "possible_keys": null,
                "key": null,
                "rows": 10000,
                "Extra": "Using where"
            }
        ],
        "analysis": {
            "table_scan": true,
            "index_used": false,
            "rows_examined": 10000,
            "recommendations": ["email 컬럼에 인덱스를 추가하세요"]
        }
    }
    """

    logger.info("=== EXPLAIN 분석 시작 ===")
    logger.info(f"Event: {json.dumps(event, default=str)}")

    connection = None

    try:
        database_secret = event.get('database_secret') or event.get('secret_name')
        database = event.get('database')
        query = event.get('query')

        if not all([database_secret, database, query]):
            return {
                'statusCode': 400,
                'body': {
                    'success': False,
                    'error': 'database_secret, database, query 필수'
                }
            }

        region = event.get('region', 'ap-northeast-2')

        logger.info(f"EXPLAIN 실행 시작: {database_secret}/{database}")
        logger.info(f"Query: {query[:100]}...")

        # Secrets Manager에서 자격증명 가져오기
        secretsmanager = boto3.client('secretsmanager', region_name=region)
        secret_response = secretsmanager.get_secret_value(SecretId=database_secret)
        credentials = json.loads(secret_response['SecretString'])

        # DB 연결
        connection = pymysql.connect(
            host=credentials['host'],
            port=int(credentials.get('port', 3306)),
            user=credentials['username'],
            password=credentials['password'],
            database=database,
            connect_timeout=10
        )

        cursor = connection.cursor(pymysql.cursors.DictCursor)
        logger.info("DB 연결 성공")

        # 쿼리 정리
        query_clean = query.strip().rstrip(';')
        if query_clean.upper().startswith('EXPLAIN'):
            query_clean = query_clean[7:].strip()

        # EXPLAIN 실행
        explain_query = f"EXPLAIN {query_clean}"
        cursor.execute(explain_query)
        explain_data = cursor.fetchall()

        logger.info(f"EXPLAIN 결과: {len(explain_data)}개 행")

        # 성능 이슈 분석
        performance_issues = []
        recommendations = []

        for row in explain_data:
            # bytes -> str 변환
            row_clean = {}
            for k, v in row.items():
                if isinstance(v, bytes):
                    row_clean[k] = v.decode('utf-8')
                else:
                    row_clean[k] = v

            # Full Table Scan 체크
            if row_clean.get('type') == 'ALL':
                performance_issues.append({
                    'severity': 'HIGH',
                    'issue': 'Full Table Scan',
                    'table': row_clean.get('table'),
                    'rows': row_clean.get('rows'),
                    'description': f"테이블 '{row_clean.get('table')}'에서 전체 테이블 스캔 발생"
                })
                recommendations.append(f"테이블 '{row_clean.get('table')}'에 인덱스 추가 권장")

            # Using filesort 체크
            if row_clean.get('Extra') and 'Using filesort' in row_clean.get('Extra'):
                performance_issues.append({
                    'severity': 'MEDIUM',
                    'issue': 'Using filesort',
                    'table': row_clean.get('table'),
                    'description': '정렬을 위해 추가 파일 정렬 필요'
                })
                recommendations.append('ORDER BY 절에 사용된 컬럼에 인덱스 추가 권장')

            # Using temporary 체크
            if row_clean.get('Extra') and 'Using temporary' in row_clean.get('Extra'):
                performance_issues.append({
                    'severity': 'MEDIUM',
                    'issue': 'Using temporary',
                    'table': row_clean.get('table'),
                    'description': '임시 테이블 사용'
                })
                recommendations.append('GROUP BY나 DISTINCT 사용 최적화 권장')

            # 많은 행 스캔 체크
            rows = row_clean.get('rows', 0)
            if rows and rows > 100000:
                performance_issues.append({
                    'severity': 'HIGH',
                    'issue': 'Large Row Scan',
                    'table': row_clean.get('table'),
                    'rows': rows,
                    'description': f"{rows:,}개 행 스캔 예상"
                })
                recommendations.append('WHERE 조건 추가 또는 인덱스 최적화 권장')

        result = {
            'success': True,
            'query': query_clean,
            'explain_data': explain_data,
            'performance_issues': performance_issues,
            'performance_issue_count': len(performance_issues),
            'recommendations': recommendations,
            'analyzed_at': datetime.utcnow().isoformat()
        }

        cursor.close()
        connection.close()

        logger.info(f"분석 완료: {len(performance_issues)}개 이슈 발견")

        # S3에 결과 저장
        try:
            timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
            query_hash = abs(hash(query_clean)) % 10000
            s3_key = f"explain-results/{database_secret}/{database}/{timestamp}_{query_hash}.json"

            s3_client = boto3.client('s3', region_name=region)
            s3_client.put_object(
                Bucket=S3_BUCKET,
                Key=s3_key,
                Body=json.dumps(result, indent=2, ensure_ascii=False, default=str),
                ContentType='application/json'
            )

            logger.info(f"S3 저장 완료: s3://{S3_BUCKET}/{s3_key}")
            result['s3_location'] = f"s3://{S3_BUCKET}/{s3_key}"

        except Exception as e:
            logger.error(f"S3 저장 실패: {str(e)}")
            result['s3_error'] = str(e)

        return {
            'statusCode': 200,
            'body': result
        }

    except Exception as e:
        logger.error(f"EXPLAIN 실패: {str(e)}", exc_info=True)

        if connection:
            try:
                connection.close()
            except:
                pass

        return {
            'statusCode': 500,
            'body': {
                'success': False,
                'error': str(e)
            }
        }
