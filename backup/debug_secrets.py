#!/usr/bin/env python3
import boto3

def debug_secrets():
    """시크릿 목록 디버깅"""
    secrets_manager = boto3.client(
        service_name="secretsmanager",
        region_name="ap-northeast-2",
        verify=False,
    )
    
    print("=== 전체 시크릿 목록 (페이지네이션 없음) ===")
    response = secrets_manager.list_secrets()
    secrets = [secret["Name"] for secret in response["SecretList"]]
    print(f"총 {len(secrets)}개 시크릿:")
    for i, secret in enumerate(secrets, 1):
        print(f"{i:2d}. {secret}")
    
    print(f"\nNextToken 존재: {'NextToken' in response}")
    if 'NextToken' in response:
        print(f"NextToken: {response['NextToken'][:50]}...")
    
    print("\n=== gamedb 필터링 결과 ===")
    gamedb_secrets = [s for s in secrets if "gamedb" in s.lower()]
    print(f"gamedb 포함 시크릿 {len(gamedb_secrets)}개:")
    for secret in gamedb_secrets:
        print(f"- {secret}")

if __name__ == "__main__":
    debug_secrets()
