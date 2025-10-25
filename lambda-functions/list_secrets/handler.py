"""
Lambda Function: db-assistant-list-secrets
Purpose: Secrets Manager에서 키워드로 Secret 목록 조회 (페이지네이션 처리)
"""

import json
import boto3
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    """
    Secrets Manager에서 키워드로 Secret 목록 조회

    Parameters:
    - event: {
        "keyword": "gamedb",  # 검색 키워드 (optional)
        "region": "ap-northeast-2"  # AWS 리전 (optional, 기본값: ap-northeast-2)
      }

    Returns:
    - statusCode: 200 (성공) / 500 (실패)
    - body: {
        "success": true/false,
        "secrets": ["secret1", "secret2", ...],
        "count": 10
      }
    """
    try:
        # 입력 파라미터
        keyword = event.get('keyword', '')
        region = event.get('region', 'ap-northeast-2')

        logger.info(f"Secret 목록 조회 시작 (키워드: '{keyword}', 리전: {region})")

        # Secrets Manager 클라이언트 생성
        client = boto3.client(
            service_name='secretsmanager',
            region_name=region
        )

        all_secrets = []
        next_token = None

        # 페이지네이션 처리
        while True:
            if next_token:
                response = client.list_secrets(NextToken=next_token)
            else:
                response = client.list_secrets()

            # Secret 이름 추출
            all_secrets.extend([
                secret['Name']
                for secret in response['SecretList']
            ])

            # 다음 페이지 확인
            if 'NextToken' not in response:
                break
            next_token = response['NextToken']

        logger.info(f"총 {len(all_secrets)}개의 Secret 발견")

        # 키워드 필터링
        if keyword:
            filtered_secrets = [
                secret
                for secret in all_secrets
                if keyword.lower() in secret.lower()
            ]
            logger.info(f"키워드 '{keyword}' 필터링 후: {len(filtered_secrets)}개")
        else:
            filtered_secrets = all_secrets

        return {
            'statusCode': 200,
            'body': {
                'success': True,
                'secrets': filtered_secrets,
                'count': len(filtered_secrets),
                'keyword': keyword,
                'region': region
            }
        }

    except Exception as e:
        logger.error(f"Secret 목록 조회 실패: {str(e)}")
        return {
            'statusCode': 500,
            'body': {
                'success': False,
                'error': str(e)
            }
        }
