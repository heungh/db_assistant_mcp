"""
Lambda Function: validate-schema
DDL 스키마 검증 및 S3 저장
"""

import json
import logging
import os
import re
from typing import Dict, Any, List
from datetime import datetime
import boto3
import pymysql

logger = logging.getLogger()
logger.setLevel(logging.INFO)

S3_BUCKET = os.getenv('QUERY_RESULTS_BUCKET', 'db-assistant-query-results')


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    DDL 스키마 검증

    Parameters:
    - event: {
        "database_secret": "gamedb1-cluster",
        "database": "gamedb",
        "ddl_content": "CREATE TABLE users (...)",
        "region": "ap-northeast-2"
      }

    Returns:
    - statusCode: 200 (성공) / 400 (입력 오류) / 500 (실패)
    - body: {
        "success": true/false,
        "valid": true/false,
        "ddl_type": "CREATE_TABLE",
        "issues": [...],
        "warnings": [...],
        "s3_location": "s3://..."
      }
    """
    connection = None

    try:
        database_secret = event.get('database_secret')
        database = event.get('database')
        ddl_content = event.get('ddl_content')
        region = event.get('region', 'ap-northeast-2')

        if not all([database_secret, database, ddl_content]):
            return {
                'statusCode': 400,
                'body': {
                    'success': False,
                    'error': 'database_secret, database, ddl_content 필수'
                }
            }

        logger.info(f"DDL 검증 시작: {database_secret}/{database}")

        # DDL 타입 분석
        ddl_type = detect_ddl_type(ddl_content)
        logger.info(f"DDL 타입: {ddl_type}")

        # Secrets Manager에서 DB 접속 정보 가져오기
        secrets_client = boto3.client('secretsmanager', region_name=region)
        secret_response = secrets_client.get_secret_value(SecretId=database_secret)
        secret = json.loads(secret_response['SecretString'])

        # DB 연결
        connection = pymysql.connect(
            host=secret.get('host'),
            port=int(secret.get('port', 3306)),
            user=secret.get('username'),
            password=secret.get('password'),
            database=database,
            connect_timeout=10
        )

        cursor = connection.cursor()
        logger.info("DB 연결 성공")

        # 검증 수행
        result = {
            'success': True,
            'valid': True,
            'ddl_type': ddl_type,
            'issues': [],
            'warnings': [],
            'validated_at': datetime.utcnow().isoformat()
        }

        if ddl_type == 'CREATE_TABLE':
            validate_create_table(cursor, ddl_content, result)
        elif ddl_type == 'ALTER_TABLE':
            validate_alter_table(cursor, ddl_content, result)
        elif ddl_type == 'DROP_TABLE':
            validate_drop_table(cursor, ddl_content, result)
        elif ddl_type == 'CREATE_INDEX':
            validate_create_index(cursor, ddl_content, result)
        else:
            result['warnings'].append(f"검증 미지원 DDL 타입: {ddl_type}")

        cursor.close()
        connection.close()

        logger.info(f"검증 완료: valid={result['valid']}, issues={len(result['issues'])}")

        # S3에 결과 저장
        try:
            timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
            s3_key = f"schema-validation/{database_secret}/{database}/{timestamp}.json"

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
        logger.error(f"DDL 검증 실패: {str(e)}", exc_info=True)

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


def detect_ddl_type(ddl_content: str) -> str:
    """DDL 타입 감지"""
    ddl_upper = ddl_content.strip().upper()

    if ddl_upper.startswith('CREATE TABLE'):
        return 'CREATE_TABLE'
    elif ddl_upper.startswith('ALTER TABLE'):
        return 'ALTER_TABLE'
    elif ddl_upper.startswith('DROP TABLE'):
        return 'DROP_TABLE'
    elif ddl_upper.startswith('CREATE INDEX') or ddl_upper.startswith('CREATE UNIQUE INDEX'):
        return 'CREATE_INDEX'
    elif ddl_upper.startswith('DROP INDEX'):
        return 'DROP_INDEX'
    else:
        return 'UNKNOWN'


def validate_create_table(cursor, ddl_content: str, result: Dict):
    """CREATE TABLE 검증"""
    # 테이블 이름 추출
    match = re.search(r'CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?`?(\w+)`?', ddl_content, re.IGNORECASE)
    if not match:
        result['valid'] = False
        result['issues'].append("테이블 이름을 파싱할 수 없음")
        return

    table_name = match.group(1)
    result['table_name'] = table_name

    # 테이블 존재 여부 확인
    cursor.execute("SHOW TABLES LIKE %s", (table_name,))
    if cursor.fetchone():
        result['warnings'].append(f"테이블 '{table_name}'이 이미 존재함 (IF NOT EXISTS 사용 권장)")

    # 외래 키 검증
    fk_pattern = r'FOREIGN\s+KEY\s+\([^)]+\)\s+REFERENCES\s+`?(\w+)`?'
    foreign_keys = re.findall(fk_pattern, ddl_content, re.IGNORECASE)

    for ref_table in foreign_keys:
        cursor.execute("SHOW TABLES LIKE %s", (ref_table,))
        if not cursor.fetchone():
            result['valid'] = False
            result['issues'].append(f"외래 키 참조 테이블 '{ref_table}'이 존재하지 않음")


def validate_alter_table(cursor, ddl_content: str, result: Dict):
    """ALTER TABLE 검증"""
    # 테이블 이름 추출
    match = re.search(r'ALTER\s+TABLE\s+`?(\w+)`?', ddl_content, re.IGNORECASE)
    if not match:
        result['valid'] = False
        result['issues'].append("테이블 이름을 파싱할 수 없음")
        return

    table_name = match.group(1)
    result['table_name'] = table_name

    # 테이블 존재 여부 확인
    cursor.execute("SHOW TABLES LIKE %s", (table_name,))
    if not cursor.fetchone():
        result['valid'] = False
        result['issues'].append(f"테이블 '{table_name}'이 존재하지 않음")


def validate_drop_table(cursor, ddl_content: str, result: Dict):
    """DROP TABLE 검증"""
    # 테이블 이름 추출
    match = re.search(r'DROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?`?(\w+)`?', ddl_content, re.IGNORECASE)
    if not match:
        result['valid'] = False
        result['issues'].append("테이블 이름을 파싱할 수 없음")
        return

    table_name = match.group(1)
    result['table_name'] = table_name

    # 테이블 존재 여부 확인
    cursor.execute("SHOW TABLES LIKE %s", (table_name,))
    if not cursor.fetchone():
        result['warnings'].append(f"테이블 '{table_name}'이 존재하지 않음 (IF EXISTS 사용 권장)")


def validate_create_index(cursor, ddl_content: str, result: Dict):
    """CREATE INDEX 검증"""
    # 테이블 이름 추출
    match = re.search(r'ON\s+`?(\w+)`?', ddl_content, re.IGNORECASE)
    if not match:
        result['valid'] = False
        result['issues'].append("테이블 이름을 파싱할 수 없음")
        return

    table_name = match.group(1)
    result['table_name'] = table_name

    # 테이블 존재 여부 확인
    cursor.execute("SHOW TABLES LIKE %s", (table_name,))
    if not cursor.fetchone():
        result['valid'] = False
        result['issues'].append(f"테이블 '{table_name}'이 존재하지 않음")
