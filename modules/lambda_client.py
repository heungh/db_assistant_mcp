"""
Lambda 함수 호출 전용 모듈

하이브리드 아키텍처에서 Lambda 함수들과의 통신을 전담하는 클라이언트
RDS/CloudWatch API 호출을 Lambda로 오프로드하여 원본 서버는 복잡한 분석 로직에만 집중
"""

import json
import logging
import boto3
from botocore.config import Config
from typing import Dict, Any
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)

# SQL 파일 저장 디렉토리
SQL_DIR = Path("output/sql")
SQL_DIR.mkdir(parents=True, exist_ok=True)


class LambdaClient:
    """
    Lambda 함수 호출을 담당하는 클래스

    하이브리드 아키텍처에서 Lambda 함수들과의 통신을 전담합니다.
    """

    def __init__(self, region: str = 'ap-northeast-2'):
        """
        Args:
            region: AWS 리전
        """
        self.region = region

        # VPC Lambda의 Cold Start를 고려한 타임아웃 설정
        # VPC ENI 생성 + DB 연결 + 쿼리 실행 + 로컬 네트워크 지연을 위해 충분한 시간 확보
        config = Config(
            read_timeout=180,      # 읽기 타임아웃 180초 (Lambda 90초 + 여유 90초)
            connect_timeout=30,    # 연결 타임아웃 30초 (로컬 네트워크 지연 대비)
            retries={'max_attempts': 1}  # 재시도 없음 (MCP 서버 응답 지연 방지)
        )

        self.lambda_client = boto3.client('lambda', region_name=region, config=config)
        logger.info(f"LambdaClient 초기화 완료 - 리전: {region}, read_timeout: 180s, connect_timeout: 30s")

    async def _call_lambda(self, function_name: str, payload: dict) -> dict:
        """
        Lambda 함수 호출 헬퍼 (하이브리드 아키텍처용)

        RDS/CloudWatch API 호출을 Lambda로 오프로드하여
        원본 서버는 복잡한 분석 로직에만 집중

        Args:
            function_name: Lambda 함수명 (접두사 없이, 예: 'validate-schema')
            payload: Lambda 함수에 전달할 페이로드

        Returns:
            dict: Lambda 함수 실행 결과

        Raises:
            Exception: Lambda 호출 실패 시
        """
        try:
            full_name = f"db-assistant-{function_name}-dev"
            logger.info(f"Lambda 호출: {full_name}")

            response = self.lambda_client.invoke(
                FunctionName=full_name,
                InvocationType='RequestResponse',
                Payload=json.dumps(payload)
            )

            result = json.loads(response['Payload'].read())

            # 상세 로깅: result 타입 확인
            logger.debug(f"Lambda result 타입: {type(result)}")
            logger.debug(f"Lambda result 내용: {str(result)[:500]}")

            if response['StatusCode'] == 200 and result.get('statusCode') == 200:
                body = result.get('body', '{}')
                logger.debug(f"Lambda body 타입 (파싱 전): {type(body)}")

                if isinstance(body, str):
                    body = json.loads(body)
                    logger.debug(f"Lambda body 타입 (파싱 후): {type(body)}")

                # body가 딕셔너리가 아닌 경우 처리
                if not isinstance(body, dict):
                    logger.error(f"Lambda body가 딕셔너리가 아님: {type(body)}, 내용: {str(body)[:200]}")
                    return {
                        'success': False,
                        'error': f'Lambda 응답 형식 오류: body가 {type(body).__name__} 타입입니다'
                    }

                logger.info(f"Lambda 호출 성공: {full_name}")
                return body
            else:
                error_msg = result.get('body', {}).get('error', 'Unknown error')
                logger.error(f"Lambda 오류: {error_msg}")
                return {
                    'success': False,
                    'error': f'Lambda 오류: {error_msg}'
                }

        except Exception as e:
            error_msg = str(e)
            logger.error(f"Lambda 호출 실패 ({function_name}): {error_msg}")

            # 타임아웃 에러인지 확인
            if 'timed out' in error_msg.lower() or 'timeout' in error_msg.lower():
                return {
                    'success': False,
                    'error': f'Lambda 타임아웃: VPC Cold Start로 인해 {function_name} 함수 실행이 지연되었습니다. 잠시 후 다시 시도해주세요.'
                }

            # 일반 에러
            return {
                'success': False,
                'error': f'Lambda 호출 실패: {error_msg}'
            }

    async def validate_schema(
        self,
        database_secret: str,
        database: str,
        ddl_content: str,
        region: str = "ap-northeast-2"
    ) -> dict:
        """DDL 스키마 검증 (Lambda 사용)

        Args:
            database_secret: Secrets Manager secret name
            database: 데이터베이스 이름
            ddl_content: DDL 구문
            region: AWS 리전

        Returns:
            dict: {
                'success': bool,
                'valid': bool,
                'ddl_type': str,
                'table_name': str,
                'issues': list,
                'warnings': list,
                's3_location': str
            }
        """
        try:
            # Lambda가 database=None 처리를 담당
            logger.info(f"Lambda로 DDL 스키마 검증: {database_secret}/{database}")

            # Lambda 호출
            lambda_result = await self._call_lambda('validate-schema', {
                'database_secret': database_secret,
                'database': database,
                'ddl_content': ddl_content,
                'region': region
            })

            if not lambda_result.get('success'):
                error_msg = lambda_result.get('error', 'Lambda 호출 실패')
                logger.error(f"DDL 스키마 검증 실패 (Lambda): {error_msg}")
                return {
                    'success': False,
                    'valid': False,
                    'error': error_msg
                }

            # Lambda 결과 반환
            logger.info(f"DDL 스키마 검증 완료 - Valid: {lambda_result.get('valid')}, "
                       f"Issues: {len(lambda_result.get('issues', []))}, "
                       f"Warnings: {len(lambda_result.get('warnings', []))}")

            return lambda_result

        except Exception as e:
            logger.error(f"DDL 스키마 검증 오류: {str(e)}")
            return {
                'success': False,
                'valid': False,
                'error': str(e)
            }

    async def explain_query(
        self,
        database_secret: str,
        database: str,
        query: str,
        region: str = "ap-northeast-2"
    ) -> dict:
        """쿼리 실행 계획 분석 (Lambda 사용)

        Args:
            database_secret: Secrets Manager secret name
            database: 데이터베이스 이름
            query: 분석할 쿼리
            region: AWS 리전

        Returns:
            dict: {
                'success': bool,
                'query': str,
                'explain_data': list,
                'performance_issues': list,
                'performance_issue_count': int,
                'recommendations': list,
                's3_location': str
            }
        """
        try:
            # Lambda가 database=None 처리를 담당
            logger.info(f"Lambda로 EXPLAIN 분석: {database_secret}/{database}")

            # Lambda 호출
            lambda_result = await self._call_lambda('explain-query', {
                'database_secret': database_secret,
                'database': database,
                'query': query,
                'region': region
            })

            if not lambda_result.get('success'):
                error_msg = lambda_result.get('error', 'Lambda 호출 실패')
                logger.error(f"EXPLAIN 분석 실패 (Lambda): {error_msg}")
                return {
                    'success': False,
                    'error': error_msg
                }

            # Lambda 결과 반환
            logger.info(f"EXPLAIN 분석 완료 - "
                       f"성능 이슈: {lambda_result.get('performance_issue_count', 0)}개, "
                       f"권장사항: {len(lambda_result.get('recommendations', []))}개")

            return lambda_result

        except Exception as e:
            logger.error(f"EXPLAIN 분석 오류: {str(e)}")
            return {
                'success': False,
                'error': str(e)
            }

    async def collect_cpu_intensive_queries(
        self,
        database_secret: str,
        db_instance_identifier: str = None,
        start_time: str = None,
        end_time: str = None,
    ) -> dict:
        """CPU 집약적 쿼리 수집 (Lambda 사용)

        Args:
            database_secret: Secrets Manager secret name
            db_instance_identifier: DB 인스턴스 식별자
            start_time: 시작 시간
            end_time: 종료 시간

        Returns:
            dict: {
                'success': bool,
                'queries': list,
                'error': str (실패 시)
            }
        """
        try:
            logger.info(f"Lambda로 CPU 집약 쿼리 수집: {database_secret}")

            # Lambda 호출
            lambda_result = await self._call_lambda('collect-cpu-intensive-queries', {
                'database_secret': database_secret,
                'db_instance_identifier': db_instance_identifier,
                'start_time': start_time,
                'end_time': end_time,
                'region': self.region
            })

            if not lambda_result.get('success'):
                error_msg = lambda_result.get('error', 'Lambda 호출 실패')
                logger.error(f"CPU 집약적 쿼리 수집 실패: {error_msg}")
                return {
                    'success': False,
                    'error': error_msg
                }

            logger.info(f"CPU 집약적 쿼리 {len(lambda_result.get('queries', []))}개 수집 완료")
            return lambda_result

        except Exception as e:
            logger.error(f"CPU 집약 쿼리 수집 실패: {str(e)}")
            return {
                'success': False,
                'error': str(e)
            }

    async def collect_temp_space_intensive_queries(
        self,
        database_secret: str,
        db_instance_identifier: str = None,
        start_time: str = None,
        end_time: str = None,
    ) -> dict:
        """임시 공간 집약적 쿼리 수집 (Lambda 사용)

        Args:
            database_secret: Secrets Manager secret name
            db_instance_identifier: DB 인스턴스 식별자
            start_time: 시작 시간
            end_time: 종료 시간

        Returns:
            dict: {
                'success': bool,
                'queries': list,
                'error': str (실패 시)
            }
        """
        try:
            logger.info(f"Lambda로 Temp 공간 집약 쿼리 수집: {database_secret}")

            # Lambda 호출
            lambda_result = await self._call_lambda('collect-temp-space-intensive-queries', {
                'database_secret': database_secret,
                'db_instance_identifier': db_instance_identifier,
                'start_time': start_time,
                'end_time': end_time,
                'region': self.region
            })

            if not lambda_result.get('success'):
                error_msg = lambda_result.get('error', 'Lambda 호출 실패')
                logger.error(f"임시 공간 집약적 쿼리 수집 실패: {error_msg}")
                return {
                    'success': False,
                    'error': error_msg
                }

            logger.info(f"임시 공간 집약적 쿼리 {len(lambda_result.get('queries', []))}개 수집 완료")
            return lambda_result

        except Exception as e:
            logger.error(f"Temp 공간 집약 쿼리 수집 실패: {str(e)}")
            return {
                'success': False,
                'error': str(e)
            }
