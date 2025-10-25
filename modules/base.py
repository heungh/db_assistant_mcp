"""
기본 설정 및 초기화 모듈

DB Assistant의 기본 설정과 초기화 기능을 제공합니다.
"""

import boto3
from typing import Dict, Any, Optional
import logging

from utils.constants import DEFAULT_REGION, BEDROCK_REGION, KNOWLEDGE_BASE_REGION

logger = logging.getLogger(__name__)


class BaseConfig:
    """기본 설정 클래스"""

    # 지원하는 AWS 리전 목록
    SUPPORTED_REGIONS = {
        "ap-northeast-1",
        "ap-northeast-2",
        "ap-northeast-3",
        "ap-south-1",
        "ap-southeast-1",
        "ap-southeast-2",
        "ap-east-1",
        "eu-west-1",
        "eu-west-2",
        "eu-west-3",
        "eu-central-1",
        "eu-north-1",
        "us-east-1",
        "us-east-2",
        "us-west-1",
        "us-west-2",
        "ca-central-1",
        "sa-east-1",
        "me-south-1",
        "af-south-1",
    }

    def __init__(self):
        """초기화"""
        self.default_region = self.get_default_region()
        self.bedrock_region = BEDROCK_REGION
        self.knowledge_base_region = KNOWLEDGE_BASE_REGION

    @staticmethod
    def get_default_region() -> str:
        """
        현재 AWS 프로파일의 기본 리전 가져오기

        Returns:
            AWS 리전 이름
        """
        try:
            session = boto3.Session()
            region = session.region_name
            if region:
                logger.info(f"AWS 기본 리전: {region}")
                return region
            else:
                logger.info(f"AWS 리전 설정 없음, 기본값 사용: {DEFAULT_REGION}")
                return DEFAULT_REGION
        except Exception as e:
            logger.warning(f"AWS 리전 조회 실패, 기본값 사용: {e}")
            return DEFAULT_REGION

    def set_default_region(self, region_name: str) -> Dict[str, Any]:
        """
        기본 AWS 리전 변경

        Args:
            region_name: 변경할 리전 이름

        Returns:
            결과 딕셔너리
        """
        if region_name not in self.SUPPORTED_REGIONS:
            return {
                "success": False,
                "message": f"지원하지 않는 리전입니다: {region_name}",
                "supported_regions": sorted(list(self.SUPPORTED_REGIONS))
            }

        old_region = self.default_region
        self.default_region = region_name

        logger.info(f"기본 리전 변경: {old_region} -> {region_name}")

        return {
            "success": True,
            "message": f"기본 리전이 {region_name}으로 변경되었습니다.",
            "old_region": old_region,
            "new_region": region_name
        }

    def get_region_info(self, region_name: Optional[str] = None) -> Dict[str, Any]:
        """
        리전 정보 조회

        Args:
            region_name: 조회할 리전 이름 (없으면 기본 리전)

        Returns:
            리전 정보 딕셔너리
        """
        target_region = region_name or self.default_region

        region_names = {
            "ap-northeast-1": "도쿄 (Tokyo)",
            "ap-northeast-2": "서울 (Seoul)",
            "ap-northeast-3": "오사카 (Osaka)",
            "ap-south-1": "뭄바이 (Mumbai)",
            "ap-southeast-1": "싱가포르 (Singapore)",
            "ap-southeast-2": "시드니 (Sydney)",
            "ap-east-1": "홍콩 (Hong Kong)",
            "eu-west-1": "아일랜드 (Ireland)",
            "eu-west-2": "런던 (London)",
            "eu-west-3": "파리 (Paris)",
            "eu-central-1": "프랑크푸르트 (Frankfurt)",
            "eu-north-1": "스톡홀름 (Stockholm)",
            "us-east-1": "버지니아 북부 (N. Virginia)",
            "us-east-2": "오하이오 (Ohio)",
            "us-west-1": "캘리포니아 북부 (N. California)",
            "us-west-2": "오레곤 (Oregon)",
            "ca-central-1": "캐나다 중부 (Central)",
            "sa-east-1": "상파울루 (São Paulo)",
            "me-south-1": "바레인 (Bahrain)",
            "af-south-1": "케이프타운 (Cape Town)",
        }

        return {
            "region_code": target_region,
            "region_name": region_names.get(target_region, "알 수 없음"),
            "is_supported": target_region in self.SUPPORTED_REGIONS,
            "is_default": target_region == self.default_region,
        }

    def get_all_regions(self) -> list:
        """
        지원하는 모든 리전 목록 조회

        Returns:
            리전 정보 리스트
        """
        regions = []
        for region_code in sorted(self.SUPPORTED_REGIONS):
            regions.append(self.get_region_info(region_code))
        return regions

    @staticmethod
    def initialize_aws_clients(region_name: Optional[str] = None) -> Dict[str, Any]:
        """
        AWS 클라이언트 초기화

        Args:
            region_name: AWS 리전 이름

        Returns:
            초기화된 클라이언트 딕셔너리
        """
        region = region_name or DEFAULT_REGION

        try:
            clients = {
                "bedrock": boto3.client("bedrock-runtime", region_name=BEDROCK_REGION, verify=False),
                "bedrock_agent": boto3.client("bedrock-agent-runtime", region_name=KNOWLEDGE_BASE_REGION, verify=False),
                "secretsmanager": boto3.client("secretsmanager", region_name=region, verify=False),
                "rds": boto3.client("rds", region_name=region, verify=False),
                "cloudwatch": boto3.client("cloudwatch", region_name=region, verify=False),
            }

            logger.info(f"AWS 클라이언트 초기화 완료 (리전: {region})")
            return clients

        except Exception as e:
            logger.error(f"AWS 클라이언트 초기화 실패: {e}")
            raise

    def get_config_summary(self) -> Dict[str, Any]:
        """
        설정 요약 정보 조회

        Returns:
            설정 요약 딕셔너리
        """
        return {
            "default_region": self.default_region,
            "bedrock_region": self.bedrock_region,
            "knowledge_base_region": self.knowledge_base_region,
            "supported_regions_count": len(self.SUPPORTED_REGIONS),
        }
