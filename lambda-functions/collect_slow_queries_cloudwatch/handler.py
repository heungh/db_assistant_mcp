"""
Lambda Function: collect-slow-queries-cloudwatch
CloudWatch Logs에서 Slow Query 수집 (데이터 수집만, 파일 저장은 EC2에서)
"""

import json
import logging
from datetime import datetime
from typing import Dict, Any, List
import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    CloudWatch Logs에서 Slow Query 수집

    입력:
    {
        "cluster_identifier": "gamedb1-cluster",
        "start_time": "2025-10-21T00:00:00",
        "end_time": "2025-10-21T23:59:59",
        "region": "ap-northeast-2"
    }

    출력:
    {
        "statusCode": 200,
        "body": {
            "success": true,
            "instances": {
                "instance-1": [
                    {
                        "query_time": "# Query_time: 5.123",
                        "user_host": "# User@Host: ...",
                        "time": "# Time: ...",
                        "sql": "SELECT * FROM ..."
                    }
                ]
            },
            "total_queries": 45,
            "instance_count": 3
        }
    }
    """
    try:
        cluster_id = event.get('cluster_identifier')
        start_time_str = event.get('start_time')
        end_time_str = event.get('end_time')
        region = event.get('region', 'ap-northeast-2')

        if not all([cluster_id, start_time_str, end_time_str]):
            return {
                'statusCode': 400,
                'body': json.dumps({'error': 'cluster_identifier, start_time, end_time 필수'})
            }

        # 시간 변환
        start_dt = datetime.fromisoformat(start_time_str.replace('Z', '+00:00'))
        end_dt = datetime.fromisoformat(end_time_str.replace('Z', '+00:00'))
        start_time_ms = int(start_dt.timestamp() * 1000)
        end_time_ms = int(end_dt.timestamp() * 1000)

        # CloudWatch Logs 클라이언트
        logs_client = boto3.client('logs', region_name=region)
        log_group_name = f"/aws/rds/cluster/{cluster_id}/slowquery"

        logger.info(f"Slow Query 수집 시작: {cluster_id}, {log_group_name}")

        # 로그 스트림 목록 조회
        try:
            streams_response = logs_client.describe_log_streams(
                logGroupName=log_group_name,
                orderBy='LastEventTime',
                descending=True
            )
        except logs_client.exceptions.ResourceNotFoundException:
            return {
                'statusCode': 404,
                'body': json.dumps({
                    'success': False,
                    'error': f'로그 그룹을 찾을 수 없음: {log_group_name}'
                })
            }

        log_streams = streams_response.get('logStreams', [])

        if not log_streams:
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'success': False,
                    'message': f'로그 스트림이 없음: {log_group_name}'
                })
            }

        # 인스턴스별 Slow Query 수집
        instances_data = {}
        total_queries = 0

        for stream in log_streams:
            stream_name = stream['logStreamName']
            instance_id = stream_name.split('/')[-1] if '/' in stream_name else stream_name

            logger.info(f"처리 중: {instance_id}")

            try:
                # 로그 이벤트 조회
                response = logs_client.filter_log_events(
                    logGroupName=log_group_name,
                    logStreamNames=[stream_name],
                    startTime=start_time_ms,
                    endTime=end_time_ms
                )

                events = response.get('events', [])

                if not events:
                    continue

                # Slow Query 파싱
                slow_queries = parse_slow_queries(events, instance_id)

                if slow_queries:
                    instances_data[instance_id] = slow_queries
                    total_queries += len(slow_queries)
                    logger.info(f"{instance_id}: {len(slow_queries)}개 수집")

            except Exception as e:
                logger.error(f"{instance_id} 처리 실패: {str(e)}")
                continue

        # 결과 반환
        result = {
            'success': True,
            'instances': instances_data,
            'total_queries': total_queries,
            'instance_count': len(instances_data),
            'log_group': log_group_name
        }

        logger.info(f"수집 완료: {total_queries}개 쿼리, {len(instances_data)}개 인스턴스")

        return {
            'statusCode': 200,
            'body': json.dumps(result)
        }

    except Exception as e:
        logger.error(f"Slow Query 수집 실패: {str(e)}", exc_info=True)
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }


def parse_slow_queries(events: List[Dict], instance_id: str) -> List[Dict]:
    """CloudWatch Logs 이벤트에서 Slow Query 파싱"""
    slow_queries = []

    for event in events:
        message = event['message'].replace('\\n', '\n')

        if '# Query_time: ' in message:
            lines = message.split('\n')
            query_info = {'instance_id': instance_id}
            sql_lines = []

            for line in lines:
                if line.startswith('# Query_time:'):
                    query_info['query_time'] = line
                elif line.startswith('# User@Host:'):
                    query_info['user_host'] = line
                elif line.startswith('# Time:'):
                    query_info['time'] = line
                elif not line.startswith('#') and line.strip() and not line.startswith('SET timestamp'):
                    if not line.startswith('use '):
                        sql_lines.append(line.strip())

            if sql_lines:
                query_info['sql'] = ' '.join(sql_lines)
                slow_queries.append(query_info)

    return slow_queries
