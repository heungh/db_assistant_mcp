"""
Lambda Function: db-assistant-collect-cluster-metrics
Purpose: CloudWatch에서 Aurora 클러스터 레벨 메트릭 수집
"""

import json
import boto3
import logging
from datetime import datetime, timedelta

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    """
    CloudWatch에서 Aurora 클러스터 레벨 메트릭 수집

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
        "metrics": {
          "CPUUtilization": [...],
          "DatabaseConnections": [...],
          ...
        },
        "metrics_count": 15
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

        logger.info(f"클러스터 메트릭 수집 시작: {cluster_id} (리전: {region}, 기간: {hours}시간)")

        # CloudWatch 클라이언트 생성
        cloudwatch = boto3.client('cloudwatch', region_name=region)
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(hours=hours)

        # 클러스터 레벨 메트릭 정의
        cluster_metrics = [
            "CPUUtilization",
            "DatabaseConnections",
            "FreeableMemory",
            "ReadIOPS",
            "WriteIOPS",
            "ReadLatency",
            "WriteLatency",
            "NetworkReceiveThroughput",
            "NetworkTransmitThroughput",
            "AuroraReplicaLag",
            "AuroraReplicaLagMaximum",
            "AuroraReplicaLagMinimum",
            "VolumeBytesUsed",
            "VolumeReadIOPs",
            "VolumeWriteIOPs",
        ]

        metrics_data = {}
        failed_metrics = []

        for metric_name in cluster_metrics:
            try:
                response = cloudwatch.get_metric_statistics(
                    Namespace='AWS/RDS',
                    MetricName=metric_name,
                    Dimensions=[
                        {'Name': 'DBClusterIdentifier', 'Value': cluster_id}
                    ],
                    StartTime=start_time,
                    EndTime=end_time,
                    Period=300,  # 5분 간격
                    Statistics=['Average', 'Maximum', 'Minimum'],
                )

                if response['Datapoints']:
                    # Datapoints를 JSON 직렬화 가능하도록 변환
                    datapoints = []
                    for dp in response['Datapoints']:
                        datapoints.append({
                            'Timestamp': dp['Timestamp'].isoformat(),
                            'Average': dp.get('Average'),
                            'Maximum': dp.get('Maximum'),
                            'Minimum': dp.get('Minimum'),
                            'Unit': dp.get('Unit')
                        })
                    metrics_data[metric_name] = datapoints
                    logger.info(f"메트릭 수집 성공: {metric_name} ({len(datapoints)}개 데이터포인트)")
                else:
                    logger.warning(f"메트릭 데이터 없음: {metric_name}")

            except Exception as e:
                logger.warning(f"클러스터 메트릭 수집 실패 {metric_name}: {e}")
                failed_metrics.append({'metric': metric_name, 'error': str(e)})

        logger.info(f"메트릭 수집 완료: 성공 {len(metrics_data)}개, 실패 {len(failed_metrics)}개")

        return {
            'statusCode': 200,
            'body': {
                'success': True,
                'cluster_id': cluster_id,
                'region': region,
                'hours': hours,
                'time_range': {
                    'start': start_time.isoformat(),
                    'end': end_time.isoformat()
                },
                'metrics': metrics_data,
                'metrics_count': len(metrics_data),
                'failed_metrics': failed_metrics
            }
        }

    except Exception as e:
        logger.error(f"클러스터 메트릭 수집 오류: {str(e)}")
        return {
            'statusCode': 500,
            'body': {
                'success': False,
                'error': str(e)
            }
        }
