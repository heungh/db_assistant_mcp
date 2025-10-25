"""
AWS 서비스 관리 헬퍼 모듈

Lambda, Secrets Manager, CloudWatch 등 AWS 서비스와의 통신을 관리하는 헬퍼 클래스를 제공합니다.
"""

import json
import asyncio
import boto3
from utils.logging_utils import logger


class AWSHelper:
    """
    AWS 서비스 관리 헬퍼 클래스

    Lambda 함수 호출, Secrets Manager 조회, CloudWatch 클라이언트 설정 등을 담당합니다.
    """

    def __init__(self, parent):
        """
        Args:
            parent: 부모 객체 (DBAssistantMCPServer 인스턴스)
        """
        self.parent = parent

    async def call_lambda(self, function_name: str, payload: dict) -> dict:
        """
        Lambda 함수 호출 헬퍼 (하이브리드 아키텍처용)

        RDS/CloudWatch API 호출을 Lambda로 오프로드하여
        원본 서버는 복잡한 분석 로직에만 집중
        """
        try:
            full_name = f"db-assistant-{function_name}-dev"
            logger.info(f"Lambda 호출: {full_name}")

            response = self.parent.lambda_client.invoke(
                FunctionName=full_name,
                InvocationType='RequestResponse',
                Payload=json.dumps(payload)
            )

            result = json.loads(response['Payload'].read())

            if response['StatusCode'] == 200 and result.get('statusCode') == 200:
                body = result.get('body', '{}')
                if isinstance(body, str):
                    body = json.loads(body)
                logger.info(f"Lambda 호출 성공: {full_name}")
                return body
            else:
                error_msg = result.get('body', {}).get('error', 'Unknown error')
                logger.error(f"Lambda 오류: {error_msg}")
                raise Exception(f"Lambda 오류: {error_msg}")

        except Exception as e:
            logger.error(f"Lambda 호출 실패 ({function_name}): {str(e)}")
            raise

    def get_secret(self, secret_name):
        """Secrets Manager에서 DB 연결 정보 가져오기 (Lambda 사용)"""
        try:
            # Lambda 함수 호출
            result = asyncio.run(self.call_lambda('get-secret', {
                'secret_name': secret_name,
                'region': 'ap-northeast-2'
            }))

            if result.get('success'):
                return result['secret']
            else:
                error_msg = result.get('error', 'Unknown error')
                logger.error(f"Secret 조회 실패 (Lambda): {error_msg}")
                raise Exception(error_msg)

        except Exception as e:
            logger.error(f"Secret 조회 실패: {e}")
            raise e

    def get_secrets_by_keyword(self, keyword=""):
        """키워드로 Secret 목록 가져오기 (Lambda 사용)"""
        try:
            # Lambda 함수 호출
            result = asyncio.run(self.call_lambda('list-secrets', {
                'keyword': keyword,
                'region': 'ap-northeast-2'
            }))

            if result.get('success'):
                return result.get('secrets', [])
            else:
                error_msg = result.get('error', 'Unknown error')
                logger.error(f"Secret 목록 조회 실패 (Lambda): {error_msg}")
                return []

        except Exception as e:
            logger.error(f"Secret 목록 조회 실패: {e}")
            return []

    def setup_cloudwatch_client(self, region_name: str = "us-east-1"):
        """CloudWatch 클라이언트 설정"""
        try:
            self.parent.cloudwatch = boto3.client("cloudwatch", region_name=region_name)
            return True
        except Exception as e:
            logger.error(f"CloudWatch 클라이언트 설정 실패: {e}")
            return False
