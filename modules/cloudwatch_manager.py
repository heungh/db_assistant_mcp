"""
CloudWatch 메트릭 수집 및 분석 모듈

CloudWatch 메트릭 수집, 분석, 이상 징후 탐지를 담당하는 클래스
대부분의 CloudWatch API 호출은 Lambda로 오프로드되며, 이 모듈은 데이터 변환 및 분석을 담당
"""

import json
import logging
import boto3
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
from pathlib import Path

# 분석 라이브러리
try:
    import pandas as pd
    import numpy as np
    ANALYSIS_AVAILABLE = True
except ImportError:
    ANALYSIS_AVAILABLE = False

logger = logging.getLogger(__name__)

# 디렉토리 경로
DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)


class CloudWatchManager:
    """
    CloudWatch 메트릭 수집 및 분석을 담당하는 클래스

    Lambda를 통해 CloudWatch API를 호출하고, 수집된 데이터를 분석합니다.
    """

    def __init__(self, region: str = 'ap-northeast-2', lambda_client=None):
        """
        Args:
            region: AWS 리전
            lambda_client: LambdaClient 인스턴스 (Lambda 호출용)
        """
        self.region = region
        self.cloudwatch = None
        self.lambda_client = lambda_client
        self.current_instance_class = "r5.large"  # 기본값

        # 기본 메트릭 목록
        self.default_metrics = [
            "CPUUtilization",
            "DatabaseConnections",
            "FreeableMemory",
            "ReadLatency",
            "WriteLatency",
            "ReadIOPS",
            "WriteIOPS",
        ]

        logger.info(f"CloudWatchManager 초기화 완료 - 리전: {region}")

    def setup_cloudwatch_client(self, region_name: str = None) -> bool:
        """CloudWatch 클라이언트 설정"""
        try:
            if region_name is None:
                region_name = self.region
            self.cloudwatch = boto3.client("cloudwatch", region_name=region_name)
            logger.info(f"CloudWatch 클라이언트 설정 완료 - 리전: {region_name}")
            return True
        except Exception as e:
            logger.error(f"CloudWatch 클라이언트 설정 실패: {e}")
            return False

    async def collect_db_metrics(
        self,
        db_instance_identifier: str,
        hours: int = 24,
        metrics: Optional[List[str]] = None,
        region: str = None,
    ) -> str:
        """CloudWatch에서 데이터베이스 메트릭 수집

        Args:
            db_instance_identifier: DB 인스턴스 식별자
            hours: 수집 기간 (시간)
            metrics: 수집할 메트릭 목록
            region: AWS 리전

        Returns:
            str: 수집 결과 메시지
        """
        if not ANALYSIS_AVAILABLE:
            return "❌ 분석 라이브러리가 설치되지 않았습니다. pip install pandas numpy scikit-learn을 실행해주세요."

        if region is None:
            region = self.region

        try:
            if not self.setup_cloudwatch_client(region):
                return "CloudWatch 클라이언트 설정에 실패했습니다."

            # Lambda 클라이언트가 없으면 오류
            if not self.lambda_client:
                return "❌ Lambda 클라이언트가 설정되지 않았습니다."

            # 클러스터 확인 및 인스턴스 identifier 변환 (Lambda 사용)
            try:
                logger.info(f"Lambda로 RDS 정보 조회: {db_instance_identifier}")
                rds_info = await self.lambda_client._call_lambda('get-rds-cluster-info', {
                    'identifier': db_instance_identifier,
                    'region': region
                })

                if rds_info['type'] == 'cluster':
                    # 클러스터인 경우 첫 번째 writer 인스턴스 사용
                    original_id = db_instance_identifier
                    for member in rds_info['members']:
                        if member['is_writer']:
                            db_instance_identifier = member['identifier']
                            self.current_instance_class = member.get('instance_class', 'r5.large')
                            break
                    logger.info(
                        f"클러스터 {original_id}에서 인스턴스 {db_instance_identifier}로 변환"
                    )
                else:
                    # 인스턴스인 경우
                    self.current_instance_class = rds_info.get('instance_class', 'r5.large')
                    logger.info(f"인스턴스 클래스: {self.current_instance_class}")

            except Exception as e:
                logger.warning(f"RDS 정보 조회 실패 (기본값 사용): {str(e)}")
                self.current_instance_class = "r5.large"  # 기본값

            if not metrics:
                metrics = self.default_metrics

            # CloudWatch 메트릭 수집 (Lambda 사용)
            logger.info(f"Lambda로 CloudWatch 메트릭 수집: {db_instance_identifier}")
            try:
                metrics_result = await self.lambda_client._call_lambda('get-cloudwatch-metrics-raw', {
                    'instance_identifier': db_instance_identifier,
                    'metrics': metrics,
                    'hours': hours,
                    'region': region,
                    'period': 300
                })

                # Lambda에서 받은 데이터를 pandas 형식으로 변환
                data = []
                for point in metrics_result.get('metrics_data', []):
                    # timestamp를 datetime으로 변환
                    timestamp = datetime.fromisoformat(point['timestamp'].replace('Z', '+00:00'))
                    data.append({
                        "Timestamp": timestamp.replace(tzinfo=None),
                        "Metric": point['metric'],
                        "Value": point['average']  # 평균값 사용
                    })

                logger.info(f"Lambda에서 {len(data)}개 데이터 포인트 수집")

            except Exception as e:
                logger.error(f"Lambda 메트릭 수집 실패: {str(e)}")
                data = []

            if not data:
                return "수집된 데이터가 없습니다."

            # 데이터프레임 생성
            df = pd.DataFrame(data)
            df = df.sort_values("Timestamp")

            # 피벗 테이블 생성
            pivot_df = df.pivot(index="Timestamp", columns="Metric", values="Value")

            # CSV 파일로 저장 (임시)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            csv_filename = f"database_metrics_{db_instance_identifier}_{timestamp}.csv"
            csv_file = DATA_DIR / csv_filename
            pivot_df.to_csv(csv_file)

            # S3에 업로드
            s3_bucket = "db-assistant-query-results-dev"
            s3_key = f"metrics/{db_instance_identifier}/{csv_filename}"

            try:
                s3_client = boto3.client('s3', region_name=region)

                # CSV 파일 업로드
                s3_client.upload_file(str(csv_file), s3_bucket, s3_key)
                logger.info(f"CSV 파일 S3 업로드 완료: s3://{s3_bucket}/{s3_key}")

                # Pre-signed URL 생성 (7일 유효)
                presigned_url = s3_client.generate_presigned_url(
                    'get_object',
                    Params={'Bucket': s3_bucket, 'Key': s3_key},
                    ExpiresIn=604800  # 7일 = 604800초
                )

                # 로컬 파일 유지 (분석용)
                logger.info(f"로컬 CSV 파일 유지: {csv_file} (분석용)")

                return f"✅ 메트릭 수집 완료\n📊 수집된 메트릭: {len(metrics)}개\n📈 데이터 포인트: {len(data)}개\n💾 S3 저장 위치: s3://{s3_bucket}/{s3_key}\n📁 로컬 저장: {csv_file}\n🔗 다운로드 URL (7일 유효): {presigned_url}"

            except Exception as s3_error:
                logger.error(f"S3 업로드 실패, 로컬 파일 유지: {s3_error}")
                return f"✅ 메트릭 수집 완료\n📊 수집된 메트릭: {len(metrics)}개\n📈 데이터 포인트: {len(data)}개\n💾 로컬 저장: {csv_file}\n⚠️  S3 업로드 실패: {str(s3_error)}"

        except Exception as e:
            logger.error(f"메트릭 수집 중 오류 발생: {str(e)}")
            return f"메트릭 수집 중 오류 발생: {str(e)}"

    async def analyze_metric_correlation(
        self, csv_file: str, target_metric: str = "CPUUtilization", top_n: int = 10
    ) -> str:
        """메트릭 간 상관관계 분석

        Args:
            csv_file: 분석할 CSV 파일 경로
            target_metric: 대상 메트릭
            top_n: 표시할 상위 N개

        Returns:
            str: 분석 결과
        """
        if not ANALYSIS_AVAILABLE:
            return "❌ 분석 라이브러리가 설치되지 않았습니다."

        try:
            # CSV 파일 경로 처리
            if not csv_file.startswith("/"):
                csv_path = DATA_DIR / csv_file
            else:
                csv_path = Path(csv_file)

            if not csv_path.exists():
                return f"CSV 파일을 찾을 수 없습니다: {csv_path}"

            # 데이터 읽기
            df = pd.read_csv(csv_path, index_col="Timestamp", parse_dates=True)
            df = df.dropna()

            if target_metric not in df.columns:
                return f"타겟 메트릭 '{target_metric}'이 데이터에 없습니다.\n사용 가능한 메트릭: {list(df.columns)}"

            # 상관 분석
            correlation_matrix = df.corr()
            target_correlations = correlation_matrix[target_metric].abs()
            target_correlations = target_correlations.drop(
                target_metric, errors="ignore"
            )
            top_correlations = target_correlations.nlargest(top_n)

            # 결과 문자열 생성
            result = f"📊 {target_metric}과 상관관계가 높은 상위 {top_n}개 메트릭:\n\n"
            for metric, correlation in top_correlations.items():
                result += f"• {metric}: {correlation:.4f}\n"

            return result

        except Exception as e:
            logger.error(f"상관관계 분석 중 오류 발생: {str(e)}")
            return f"상관관계 분석 중 오류 발생: {str(e)}"
