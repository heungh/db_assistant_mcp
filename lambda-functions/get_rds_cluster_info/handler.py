"""
RDS 클러스터/인스턴스 정보 조회 Lambda 함수
원본 서버에서 호출하여 RDS API 부분만 처리
"""
import json
import logging
import boto3
from typing import Dict, Any, Optional

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    RDS 클러스터 또는 인스턴스 정보 조회

    입력:
        - identifier: 클러스터 ID 또는 인스턴스 ID
        - region: AWS 리전 (기본값: ap-northeast-2)

    출력:
        - type: "cluster" | "instance"
        - info: 클러스터 또는 인스턴스 정보
        - members: 클러스터인 경우 멤버 인스턴스 목록
    """
    logger.info(f"=== RDS 정보 조회 시작 ===")
    logger.info(f"Event: {json.dumps(event)}")

    try:
        identifier = event.get('identifier')
        region = event.get('region', 'ap-northeast-2')

        if not identifier:
            return {
                'statusCode': 400,
                'body': json.dumps({'error': 'identifier가 필요합니다'})
            }

        rds_client = boto3.client('rds', region_name=region)

        # 먼저 클러스터로 조회 시도
        try:
            logger.info(f"클러스터 조회 시도: {identifier}")
            cluster_response = rds_client.describe_db_clusters(
                DBClusterIdentifier=identifier
            )

            if cluster_response['DBClusters']:
                cluster = cluster_response['DBClusters'][0]

                # 멤버 인스턴스 정보 수집
                members = []
                for member in cluster.get('DBClusterMembers', []):
                    member_id = member['DBInstanceIdentifier']
                    is_writer = member.get('IsClusterWriter', False)

                    # 각 멤버의 상세 정보 조회
                    try:
                        instance_response = rds_client.describe_db_instances(
                            DBInstanceIdentifier=member_id
                        )
                        if instance_response['DBInstances']:
                            instance = instance_response['DBInstances'][0]
                            members.append({
                                'identifier': member_id,
                                'is_writer': is_writer,
                                'instance_class': instance.get('DBInstanceClass'),
                                'status': instance.get('DBInstanceStatus'),
                                'availability_zone': instance.get('AvailabilityZone'),
                                'endpoint': instance.get('Endpoint', {}).get('Address')
                            })
                    except Exception as e:
                        logger.warning(f"멤버 {member_id} 조회 실패: {e}")

                result = {
                    'type': 'cluster',
                    'identifier': cluster['DBClusterIdentifier'],
                    'status': cluster.get('Status'),
                    'engine': cluster.get('Engine'),
                    'engine_version': cluster.get('EngineVersion'),
                    'endpoint': cluster.get('Endpoint'),
                    'reader_endpoint': cluster.get('ReaderEndpoint'),
                    'port': cluster.get('Port'),
                    'master_username': cluster.get('MasterUsername'),
                    'multi_az': cluster.get('MultiAZ', False),
                    'members': members,
                    'member_count': len(members)
                }

                logger.info(f"클러스터 조회 성공: {identifier}, 멤버: {len(members)}개")

                return {
                    'statusCode': 200,
                    'body': json.dumps(result, default=str, ensure_ascii=False)
                }

        except rds_client.exceptions.DBClusterNotFoundFault:
            logger.info(f"{identifier}는 클러스터가 아님, 인스턴스로 조회")

        # 인스턴스로 조회
        try:
            logger.info(f"인스턴스 조회 시도: {identifier}")
            instance_response = rds_client.describe_db_instances(
                DBInstanceIdentifier=identifier
            )

            if instance_response['DBInstances']:
                instance = instance_response['DBInstances'][0]

                result = {
                    'type': 'instance',
                    'identifier': instance['DBInstanceIdentifier'],
                    'instance_class': instance.get('DBInstanceClass'),
                    'engine': instance.get('Engine'),
                    'engine_version': instance.get('EngineVersion'),
                    'status': instance.get('DBInstanceStatus'),
                    'availability_zone': instance.get('AvailabilityZone'),
                    'multi_az': instance.get('MultiAZ', False),
                    'endpoint': instance.get('Endpoint', {}).get('Address'),
                    'port': instance.get('Endpoint', {}).get('Port'),
                    'allocated_storage': instance.get('AllocatedStorage'),
                    'storage_type': instance.get('StorageType'),
                    'master_username': instance.get('MasterUsername'),
                    'db_cluster_identifier': instance.get('DBClusterIdentifier')
                }

                logger.info(f"인스턴스 조회 성공: {identifier}")

                return {
                    'statusCode': 200,
                    'body': json.dumps(result, default=str, ensure_ascii=False)
                }

        except rds_client.exceptions.DBInstanceNotFoundFault:
            logger.error(f"{identifier}는 클러스터도 인스턴스도 아님")
            return {
                'statusCode': 404,
                'body': json.dumps({'error': f'{identifier}를 찾을 수 없습니다'})
            }

        return {
            'statusCode': 404,
            'body': json.dumps({'error': f'{identifier}를 찾을 수 없습니다'})
        }

    except Exception as e:
        logger.error(f"RDS 정보 조회 실패: {str(e)}", exc_info=True)
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)}, ensure_ascii=False)
        }
