#!/bin/bash
# SSH 터널 설정 스크립트

# 기존 터널 종료
pkill -f "ssh.*11.111.11.111"

# SSH 터널 시작 (MySQL용)
ssh -i /Users/abcdefg/abcdefg.pem -f -N -L 3307:abcdefg-cluster.cluster-c3abcdefg123456.ap-northeast-2.rds.amazonaws.com:3306 ec2-user@11.111.11.111 &

# 잠시 대기
sleep 3

echo "SSH 터널이 시작되었습니다. 로컬 포트 3307로 연결하세요."
