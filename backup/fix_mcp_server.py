#!/usr/bin/env python3
"""
MCP 서버의 get_secrets_by_keyword 함수 수정안
"""

def get_secrets_by_keyword(self, keyword=""):
    """키워드로 Secret 목록 가져오기 (페이지네이션 처리)"""
    try:
        secrets_manager = boto3.client(
            service_name="secretsmanager",
            region_name="ap-northeast-2",
            verify=False,
        )
        
        all_secrets = []
        next_token = None
        
        # 페이지네이션 처리
        while True:
            if next_token:
                response = secrets_manager.list_secrets(NextToken=next_token)
            else:
                response = secrets_manager.list_secrets()
            
            all_secrets.extend([secret["Name"] for secret in response["SecretList"]])
            
            if 'NextToken' not in response:
                break
            next_token = response['NextToken']
        
        # 키워드 필터링
        if keyword:
            filtered_secrets = [
                secret for secret in all_secrets
                if keyword.lower() in secret.lower()
            ]
            return filtered_secrets
        else:
            return all_secrets
            
    except Exception as e:
        logger.error(f"Secret 목록 조회 실패: {e}")
        return []

print("수정된 함수:")
print("1. 페이지네이션 처리 추가")
print("2. 모든 시크릿을 가져온 후 필터링")
print("3. gamedb1-cluster를 포함한 모든 gamedb 시크릿 반환 가능")
