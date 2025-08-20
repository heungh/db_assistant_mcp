#!/usr/bin/env python3
import boto3

def get_all_secrets_with_pagination():
    """페이지네이션을 처리하여 모든 시크릿 가져오기"""
    secrets_manager = boto3.client(
        service_name="secretsmanager",
        region_name="ap-northeast-2",
        verify=False,
    )
    
    all_secrets = []
    next_token = None
    
    while True:
        if next_token:
            response = secrets_manager.list_secrets(NextToken=next_token)
        else:
            response = secrets_manager.list_secrets()
        
        all_secrets.extend([secret["Name"] for secret in response["SecretList"]])
        
        if 'NextToken' not in response:
            break
        next_token = response['NextToken']
    
    return all_secrets

def test_fixed_function():
    """수정된 함수 테스트"""
    print("=== 페이지네이션 처리된 전체 시크릿 목록 ===")
    all_secrets = get_all_secrets_with_pagination()
    print(f"총 {len(all_secrets)}개 시크릿:")
    
    for i, secret in enumerate(all_secrets, 1):
        print(f"{i:2d}. {secret}")
    
    print("\n=== gamedb 필터링 결과 ===")
    gamedb_secrets = [s for s in all_secrets if "gamedb" in s.lower()]
    print(f"gamedb 포함 시크릿 {len(gamedb_secrets)}개:")
    for secret in sorted(gamedb_secrets):
        print(f"- {secret}")

if __name__ == "__main__":
    test_fixed_function()
