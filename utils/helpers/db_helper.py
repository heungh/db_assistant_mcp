"""
DB 연결 관리 헬퍼 모듈

MySQL/Aurora 데이터베이스 연결을 관리하는 헬퍼 클래스를 제공합니다.
"""

import json
import boto3
from utils.logging_utils import logger

try:
    import mysql.connector
    mysql = mysql.connector
except ImportError:
    mysql = None


class DBHelper:
    """
    DB 연결 관리 헬퍼 클래스

    MySQL/Aurora 데이터베이스 연결을 생성하고 관리합니다.
    공용 연결을 통한 연결 재사용을 지원합니다.
    """

    def __init__(self, parent):
        """
        Args:
            parent: 부모 객체 (DBAssistantMCPServer 인스턴스)
        """
        self.parent = parent

    def get_db_connection(
        self,
        database_secret: str,
        selected_database: str = None,
        db_instance_identifier: str = None,
    ):
        """공통 DB 연결 함수"""
        if mysql is None:
            raise Exception(
                "mysql-connector-python이 설치되지 않았습니다. pip install mysql-connector-python을 실행해주세요."
            )

        # Secret에서 DB 연결 정보 가져오기
        session = boto3.session.Session()
        client = session.client(
            service_name="secretsmanager",
            region_name="ap-northeast-2",
            verify=False,
        )
        get_secret_value_response = client.get_secret_value(SecretId=database_secret)
        secret = get_secret_value_response["SecretString"]
        db_config = json.loads(secret)

        # 선택된 데이터베이스가 있으면 사용, 없으면 기본값 사용
        database_name = selected_database or db_config.get(
            "dbname", db_config.get("database")
        )
        # database_name이 None이 아닌 경우에만 문자열로 변환
        if database_name is not None:
            database_name = str(database_name)

        # db_instance_identifier가 제공되면 해당 인스턴스 엔드포인트 사용
        host = db_config.get("host")
        if db_instance_identifier:
            # 클러스터 엔드포인트를 인스턴스 엔드포인트로 변경
            if ".cluster-" in host:
                # aurora-cluster.cluster-xxx.region.rds.amazonaws.com -> instance-id.xxx.region.rds.amazonaws.com
                host_parts = host.split(".")
                if len(host_parts) >= 4:
                    # cluster- 부분을 제거하고 인스턴스 ID로 교체
                    host_parts[1] = host_parts[1].replace("cluster-", "")
                    host = f"{db_instance_identifier}.{'.'.join(host_parts[1:])}"
            else:
                # 단일 인스턴스인 경우 인스턴스 ID로 교체
                host_parts = host.split(".")
                if len(host_parts) >= 4:
                    host = f"{db_instance_identifier}.{'.'.join(host_parts[1:])}"

        connection_config = {
            "host": host,
            "port": db_config.get("port", 3306),
            "user": db_config.get("username"),
            "password": db_config.get("password"),
            "database": database_name,
            "connection_timeout": 10,
        }

        connection = mysql.connector.connect(**connection_config)
        return connection

    def setup_shared_connection(
        self,
        database_secret: str,
        selected_database: str = None,
        db_instance_identifier: str = None,
    ):
        """공용 DB 연결 설정 (한 번만 호출)"""
        try:
            if self.parent.shared_connection and self.parent.shared_connection.is_connected():
                logger.info("이미 활성화된 공용 연결이 있습니다.")
                return True

            self.parent.shared_connection = self.get_db_connection(
                database_secret,
                selected_database,
                db_instance_identifier,
            )

            if self.parent.shared_connection and self.parent.shared_connection.is_connected():
                self.parent.shared_cursor = self.parent.shared_connection.cursor()
                # 연결된 호스트 정보 로깅
                host_info = (
                    f"인스턴스: {db_instance_identifier}"
                    if db_instance_identifier
                    else "클러스터 엔드포인트"
                )
                logger.info(f"공용 DB 연결 설정 완료 - {host_info}")
                return True
            else:
                logger.error("공용 DB 연결 실패")
                return False

        except Exception as e:
            logger.error(f"공용 DB 연결 설정 오류: {e}")
            return False

    def cleanup_shared_connection(self):
        """공용 DB 연결 정리"""
        try:
            if self.parent.shared_cursor:
                self.parent.shared_cursor.close()
                self.parent.shared_cursor = None
                logger.info("공용 커서 닫기 완료")

            if self.parent.shared_connection and self.parent.shared_connection.is_connected():
                self.parent.shared_connection.close()
                self.parent.shared_connection = None
                logger.info("공용 DB 연결 닫기 완료")

        except Exception as e:
            logger.error(f"공용 연결 정리 중 오류: {e}")

    def get_shared_cursor(self):
        """공용 커서 반환"""
        if self.parent.shared_cursor is None:
            logger.error(
                "공용 커서가 설정되지 않았습니다. setup_shared_connection()을 먼저 호출하세요."
            )
            return None
        return self.parent.shared_cursor
