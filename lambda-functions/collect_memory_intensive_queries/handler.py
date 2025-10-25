"""
Lambda Function: collect-memory-intensive-queries
Performance Schema에서 메모리 집약적 쿼리 수집 및 S3 저장
"""

import json
import logging
from typing import Dict, Any, List
from datetime import datetime
import boto3
import pymysql

logger = logging.getLogger()
logger.setLevel(logging.INFO)

S3_BUCKET = 'db-assistant-query-results-dev'


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Performance Schema에서 메모리 집약적 쿼리 수집

    입력:
    {
        "database_secret": "gamedb1-cluster",
        "db_instance_identifier": "gamedb1-1",  # optional
        "start_time": "2025-10-21 00:00:00",    # optional
        "end_time": "2025-10-21 23:59:59",      # optional
        "region": "ap-northeast-2"
    }

    출력:
    {
        "statusCode": 200,
        "body": {
            "success": true,
            "queries": [
                {
                    "sql": "SELECT * FROM users WHERE ...",
                    "source": "performance_schema",
                    "max_memory_used": 104857600
                }
            ],
            "query_count": 10
        }
    }
    """
    connection = None

    try:
        database_secret = event.get('database_secret')
        db_instance_identifier = event.get('db_instance_identifier')
        start_time = event.get('start_time')
        end_time = event.get('end_time')
        region = event.get('region', 'ap-northeast-2')

        if not database_secret:
            return {
                'statusCode': 400,
                'body': json.dumps({'error': 'database_secret 필수'})
            }

        logger.info(f"메모리 집약 쿼리 수집 시작: {database_secret}")

        # Secrets Manager에서 DB 접속 정보 가져오기
        secrets_client = boto3.client('secretsmanager', region_name=region)
        secret_response = secrets_client.get_secret_value(SecretId=database_secret)
        secret = json.loads(secret_response['SecretString'])

        # DB 연결 설정
        db_config = {
            'host': secret.get('host'),
            'port': int(secret.get('port', 3306)),
            'user': secret.get('username'),
            'password': secret.get('password'),
            'database': secret.get('dbname', 'mysql'),
            'connect_timeout': 10,
            'read_timeout': 30,
            'write_timeout': 30
        }

        # 특정 인스턴스 지정 시 (Read Replica 등)
        if db_instance_identifier:
            logger.info(f"인스턴스 지정: {db_instance_identifier}")

        # DB 연결
        connection = pymysql.connect(**db_config)
        cursor = connection.cursor()

        logger.info("DB 연결 성공")

        collected_queries = []

        # 시간 필터 조건 생성
        time_filter = ""
        if start_time and end_time:
            time_filter = f"AND FIRST_SEEN >= '{start_time}' AND LAST_SEEN <= '{end_time}'"
        elif start_time:
            time_filter = f"AND FIRST_SEEN >= '{start_time}'"
        elif end_time:
            time_filter = f"AND LAST_SEEN <= '{end_time}'"

        # Performance Schema에서 메모리 집약적 쿼리 수집
        try:
            query_sql = f"""
                SELECT
                    QUERY_SAMPLE_TEXT,
                    MAX_MEMORY_USED,
                    COUNT_STAR,
                    AVG_TIMER_WAIT/1000000000000 as avg_time_sec
                FROM performance_schema.events_statements_summary_by_digest
                WHERE DIGEST_TEXT IS NOT NULL
                    AND MAX_MEMORY_USED > 100*1024*1024
                    AND DIGEST_TEXT NOT LIKE '%performance_schema%'
                    AND DIGEST_TEXT NOT LIKE '%information_schema%'
                    AND DIGEST_TEXT NOT LIKE 'EXPLAIN%'
                    {time_filter}
                ORDER BY MAX_MEMORY_USED DESC
                LIMIT 20
            """

            cursor.execute(query_sql)
            results = cursor.fetchall()

            for row in results:
                query_text, max_memory, exec_count, avg_time = row

                if query_text and query_text.strip():
                    query_clean = query_text.strip()

                    # 필터링
                    if not query_clean.upper().startswith('EXPLAIN'):
                        if 'performance_schema' not in query_clean.lower() and 'information_schema' not in query_clean.lower():
                            collected_queries.append({
                                'sql': query_clean,
                                'source': 'performance_schema',
                                'max_memory_used': int(max_memory) if max_memory else 0,
                                'exec_count': int(exec_count) if exec_count else 0,
                                'avg_time': float(avg_time) if avg_time else 0.0
                            })

            logger.info(f"Performance Schema: {len(collected_queries)}개 수집")

        except Exception as e:
            logger.warning(f"Performance Schema 조회 실패: {str(e)}")

        # 결과 정리
        cursor.close()
        connection.close()

        result = {
            'success': True,
            'queries': collected_queries,
            'query_count': len(collected_queries),
            'collected_at': datetime.utcnow().isoformat()
        }

        logger.info(f"수집 완료: {len(collected_queries)}개 쿼리")

        # S3에 결과 저장
        s3_key = None
        if collected_queries:
            try:
                timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
                s3_key = f"memory-intensive-queries/{database_secret}/{timestamp}.json"

                s3_client = boto3.client('s3', region_name=region)
                s3_client.put_object(
                    Bucket=S3_BUCKET,
                    Key=s3_key,
                    Body=json.dumps(result, indent=2, ensure_ascii=False),
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
        logger.error(f"메모리 집약 쿼리 수집 실패: {str(e)}", exc_info=True)

        if connection:
            try:
                connection.close()
            except:
                pass

        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }
