"""
Lambda Function: db-assistant-collect-cluster-events
Purpose: RDS API로 Aurora 클러스터 이벤트 수집 (클러스터 + 인스턴스 레벨)
"""

import json
import boto3
import logging
from datetime import datetime, timedelta

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def categorize_event_severity(message):
    """이벤트 메시지 기반 심각도 분류"""
    message_lower = message.lower()

    if any(
        keyword in message_lower
        for keyword in [
            "failed",
            "error",
            "critical",
            "fatal",
            "crash",
            "corruption",
        ]
    ):
        return "HIGH"
    elif any(
        keyword in message_lower
        for keyword in ["warning", "slow", "timeout", "retry", "restart", "reboot"]
    ):
        return "MEDIUM"
    else:
        return "LOW"

def lambda_handler(event, context):
    """
    RDS API로 Aurora 클러스터 이벤트 수집

    Parameters:
    - event: {
        "cluster_id": "gamedb1-cluster",  # 클러스터 ID (필수)
        "region": "ap-northeast-2",       # AWS 리전 (optional, 기본값: ap-northeast-2)
        "hours": 24                        # 수집 기간 (optional, 기본값: 24시간)
      }

    Returns:
    - statusCode: 200 (성공) / 400 (입력 오류) / 500 (실패)
    - body: {
        "success": true/false,
        "events": [
          {
            "date": "2025-10-22 12:00:00",
            "message": "...",
            "source_id": "gamedb1-cluster",
            "event_categories": [...],
            "severity": "HIGH/MEDIUM/LOW"
          },
          ...
        ],
        "event_count": 10
      }
    """
    try:
        # 입력 검증
        cluster_id = event.get('cluster_id')
        if not cluster_id:
            return {
                'statusCode': 400,
                'body': {
                    'success': False,
                    'error': 'cluster_id is required'
                }
            }

        region = event.get('region', 'ap-northeast-2')
        hours = event.get('hours', 24)

        logger.info(f"클러스터 이벤트 수집 시작: {cluster_id} (리전: {region}, 기간: {hours}시간)")

        # RDS 클라이언트 생성
        rds_client = boto3.client('rds', region_name=region)
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(hours=hours)

        events = []

        # 1. 클러스터 레벨 이벤트 수집
        try:
            response = rds_client.describe_events(
                SourceIdentifier=cluster_id,
                SourceType='db-cluster',
                StartTime=start_time,
                EndTime=end_time,
            )

            for ev in response.get('Events', []):
                events.append({
                    'date': ev['Date'].strftime('%Y-%m-%d %H:%M:%S'),
                    'message': ev['Message'],
                    'source_id': ev.get('SourceIdentifier', cluster_id),
                    'event_categories': ev.get('EventCategories', []),
                    'severity': categorize_event_severity(ev['Message']),
                    'type': 'cluster'
                })

            logger.info(f"클러스터 이벤트 {len(events)}개 수집")

        except Exception as e:
            logger.error(f"클러스터 이벤트 수집 실패: {e}")

        # 2. 인스턴스 레벨 이벤트 수집
        try:
            cluster_info = rds_client.describe_db_clusters(
                DBClusterIdentifier=cluster_id
            )['DBClusters'][0]

            instance_count = 0
            for member in cluster_info['DBClusterMembers']:
                instance_id = member['DBInstanceIdentifier']
                try:
                    instance_response = rds_client.describe_events(
                        SourceIdentifier=instance_id,
                        SourceType='db-instance',
                        StartTime=start_time,
                        EndTime=end_time,
                    )

                    for ev in instance_response.get('Events', []):
                        events.append({
                            'date': ev['Date'].strftime('%Y-%m-%d %H:%M:%S'),
                            'message': f"[{instance_id}] {ev['Message']}",
                            'source_id': instance_id,
                            'event_categories': ev.get('EventCategories', []),
                            'severity': categorize_event_severity(ev['Message']),
                            'type': 'instance'
                        })

                    instance_count += 1

                except Exception as e:
                    logger.warning(f"인스턴스 이벤트 수집 실패 {instance_id}: {e}")

            logger.info(f"인스턴스 이벤트 수집 완료: {instance_count}개 인스턴스 처리")

        except Exception as e:
            logger.error(f"인스턴스 이벤트 수집 실패: {e}")

        # 날짜순 정렬 (최신 순)
        events.sort(key=lambda x: x['date'], reverse=True)

        logger.info(f"총 {len(events)}개의 이벤트 수집 완료")

        return {
            'statusCode': 200,
            'body': {
                'success': True,
                'cluster_id': cluster_id,
                'region': region,
                'hours': hours,
                'time_range': {
                    'start': start_time.strftime('%Y-%m-%d %H:%M:%S'),
                    'end': end_time.strftime('%Y-%m-%d %H:%M:%S')
                },
                'events': events,
                'event_count': len(events),
                'severity_breakdown': {
                    'HIGH': sum(1 for e in events if e['severity'] == 'HIGH'),
                    'MEDIUM': sum(1 for e in events if e['severity'] == 'MEDIUM'),
                    'LOW': sum(1 for e in events if e['severity'] == 'LOW')
                }
            }
        }

    except Exception as e:
        logger.error(f"클러스터 이벤트 수집 오류: {str(e)}")
        return {
            'statusCode': 500,
            'body': {
                'success': False,
                'error': str(e)
            }
        }
