"""
Lambda Function: db-assistant-get-secret
Purpose: Secrets Manager에서 특정 Secret 조회
"""

import json
import boto3
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    """
    Secrets Manager에서 특정 Secret 조회

    Parameters:
    - event: {
        "secret_name": "gamedb1-cluster"  # Secret 이름 (필수)
      }

    Returns:
    - statusCode: 200 (성공) / 400 (입력 오류) / 500 (실패)
    - body: {
        "success": true/false,
        "secret": {
          "host": "...",
          "username": "...",
          "password": "...",
          "port": 3306,
          "dbname": "..."
        }
      }
    """
    try:
        # 입력 검증
        secret_name = event.get('secret_name')
        if not secret_name:
            return {
                'statusCode': 400,
                'body': {
                    'success': False,
                    'error': 'secret_name is required'
                }
            }

        logger.info(f"Secret 조회 시작: {secret_name}")

        # Secrets Manager 클라이언트 생성
        region = event.get('region', 'ap-northeast-2')
        client = boto3.client(
            service_name='secretsmanager',
            region_name=region
        )

        # Secret 조회
        response = client.get_secret_value(SecretId=secret_name)
        secret_string = response['SecretString']
        secret_data = json.loads(secret_string)

        logger.info(f"Secret 조회 성공: {secret_name}")

        return {
            'statusCode': 200,
            'body': {
                'success': True,
                'secret_name': secret_name,
                'secret': secret_data
            }
        }

    except client.exceptions.ResourceNotFoundException:
        logger.error(f"Secret을 찾을 수 없음: {secret_name}")
        return {
            'statusCode': 404,
            'body': {
                'success': False,
                'error': f'Secret not found: {secret_name}'
            }
        }

    except Exception as e:
        logger.error(f"Secret 조회 실패: {str(e)}")
        return {
            'statusCode': 500,
            'body': {
                'success': False,
                'error': str(e)
            }
        }
