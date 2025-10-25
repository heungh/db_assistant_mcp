"""
CloudWatch 메트릭 Raw 데이터 수집 Lambda 함수
원본 서버에서 호출하여 CloudWatch API 부분만 처리
"""
import json
import logging
import boto3
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    CloudWatch에서 RDS 메트릭 수집

    입력:
        - instance_identifier: 인스턴스 ID
        - metrics: 수집할 메트릭 리스트 (선택)
        - hours: 수집 기간 (시간, 기본값: 24)
        - region: AWS 리전 (기본값: ap-northeast-2)
        - period: 데이터 포인트 간격(초, 기본값: 300)

    출력:
        - metrics_data: [{timestamp, metric, value}, ...]
        - summary: {metric_count, datapoint_count}
    """
    logger.info(f"=== CloudWatch 메트릭 수집 시작 ===")
    logger.info(f"Event: {json.dumps(event, default=str)}")

    try:
        instance_id = event.get('instance_identifier')
        region = event.get('region', 'ap-northeast-2')
        hours = event.get('hours', 24)
        period = event.get('period', 300)  # 5분 간격

        # 기본 메트릭 목록
        default_metrics = [
            'CPUUtilization',
            'DatabaseConnections',
            'FreeableMemory',
            'ReadLatency',
            'WriteLatency',
            'ReadIOPS',
            'WriteIOPS',
            'ReadThroughput',
            'WriteThroughput',
            'NetworkReceiveThroughput',
            'NetworkTransmitThroughput',
            'FreeStorageSpace'
        ]

        metrics = event.get('metrics', default_metrics)

        if not instance_id:
            return {
                'statusCode': 400,
                'body': json.dumps({'error': 'instance_identifier가 필요합니다'})
            }

        cloudwatch = boto3.client('cloudwatch', region_name=region)

        # 시간 범위 설정
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(hours=hours)

        logger.info(f"메트릭 수집: {instance_id}, 기간: {start_time} ~ {end_time}")

        # 데이터를 저장할 리스트
        all_data = []
        failed_metrics = []

        # 각 메트릭에 대해 데이터 수집
        for metric in metrics:
            try:
                response = cloudwatch.get_metric_statistics(
                    Namespace='AWS/RDS',
                    MetricName=metric,
                    Dimensions=[
                        {
                            'Name': 'DBInstanceIdentifier',
                            'Value': instance_id
                        }
                    ],
                    StartTime=start_time,
                    EndTime=end_time,
                    Period=period,
                    Statistics=['Average', 'Maximum', 'Minimum']
                )

                # 응답에서 데이터 추출
                for point in response['Datapoints']:
                    all_data.append({
                        'timestamp': point['Timestamp'].isoformat(),
                        'metric': metric,
                        'average': point.get('Average'),
                        'maximum': point.get('Maximum'),
                        'minimum': point.get('Minimum'),
                        'unit': point.get('Unit', '')
                    })

                logger.info(f"메트릭 {metric}: {len(response['Datapoints'])}개 수집")

            except Exception as e:
                logger.error(f"메트릭 {metric} 수집 실패: {str(e)}")
                failed_metrics.append(metric)

        result = {
            'instance_identifier': instance_id,
            'metrics_data': all_data,
            'summary': {
                'requested_metrics': len(metrics),
                'successful_metrics': len(metrics) - len(failed_metrics),
                'failed_metrics': failed_metrics,
                'total_datapoints': len(all_data),
                'period_hours': hours,
                'start_time': start_time.isoformat(),
                'end_time': end_time.isoformat()
            }
        }

        logger.info(f"수집 완료: {len(all_data)}개 데이터 포인트")

        return {
            'statusCode': 200,
            'body': json.dumps(result, ensure_ascii=False)
        }

    except Exception as e:
        logger.error(f"메트릭 수집 실패: {str(e)}", exc_info=True)
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)}, ensure_ascii=False)
        }
