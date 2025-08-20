#!/usr/bin/env python3
"""
데이터베이스 분석 MCP 서버
CloudWatch 메트릭 수집, 상관관계 분석, 아웃라이어 탐지, 회귀 분석 기능 제공
"""

import asyncio
import json
import os
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
from pathlib import Path

import boto3
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')  # GUI 없는 환경에서 matplotlib 사용
import matplotlib.pyplot as plt
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import PolynomialFeatures
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error, r2_score
from sklearn.impute import SimpleImputer

from mcp.server.models import InitializationOptions
import mcp.types as types
from mcp.server import NotificationOptions, Server
import mcp.server.stdio

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 현재 디렉토리 기준 경로 설정
CURRENT_DIR = Path(__file__).parent
OUTPUT_DIR = CURRENT_DIR / "output"
DATA_DIR = CURRENT_DIR / "data"

# 디렉토리 생성
OUTPUT_DIR.mkdir(exist_ok=True)
DATA_DIR.mkdir(exist_ok=True)

class AnalyzeDBServer:
    def __init__(self):
        self.cloudwatch = None
        self.default_metrics = [
            'CPUUtilization', 'DatabaseConnections', 'DBLoad', 'DBLoadCPU', 
            'DBLoadNonCPU', 'FreeableMemory', 'ReadIOPS', 'WriteIOPS',
            'ReadLatency', 'WriteLatency', 'NetworkReceiveThroughput',
            'NetworkTransmitThroughput', 'BufferCacheHitRatio'
        ]

    def setup_cloudwatch_client(self, region_name: str = 'us-east-1'):
        """CloudWatch 클라이언트 설정"""
        try:
            self.cloudwatch = boto3.client('cloudwatch', region_name=region_name)
            return True
        except Exception as e:
            logger.error(f"CloudWatch 클라이언트 설정 실패: {e}")
            return False

    async def collect_metrics(self, db_instance_identifier: str, hours: int = 24, 
                            metrics: Optional[List[str]] = None, region: str = 'us-east-1') -> str:
        """CloudWatch에서 데이터베이스 메트릭 수집"""
        try:
            if not self.setup_cloudwatch_client(region):
                return "CloudWatch 클라이언트 설정에 실패했습니다."

            if not metrics:
                metrics = self.default_metrics

            # 시간 범위 설정
            end_time = datetime.utcnow()
            start_time = end_time - timedelta(hours=hours)

            # 데이터를 저장할 리스트
            data = []

            # 각 메트릭에 대해 데이터 수집
            for metric in metrics:
                try:
                    response = self.cloudwatch.get_metric_statistics(
                        Namespace='AWS/RDS',
                        MetricName=metric,
                        Dimensions=[
                            {
                                'Name': 'DBInstanceIdentifier',
                                'Value': db_instance_identifier
                            },
                        ],
                        StartTime=start_time,
                        EndTime=end_time,
                        Period=300,  # 5분 간격
                        Statistics=['Average']
                    )
                    
                    # 응답에서 데이터 추출
                    for point in response['Datapoints']:
                        data.append({
                            'Timestamp': point['Timestamp'].replace(tzinfo=None),
                            'Metric': metric,
                            'Value': point['Average']
                        })
                except Exception as e:
                    logger.error(f"메트릭 {metric} 수집 실패: {str(e)}")

            if not data:
                return "수집된 데이터가 없습니다."

            # 데이터프레임 생성
            df = pd.DataFrame(data)
            df = df.sort_values('Timestamp')

            # 피벗 테이블 생성
            pivot_df = df.pivot(index='Timestamp', columns='Metric', values='Value')

            # CSV 파일로 저장
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            csv_file = DATA_DIR / f'database_metrics_{db_instance_identifier}_{timestamp}.csv'
            pivot_df.to_csv(csv_file)

            return f"✅ 메트릭 수집 완료\n📊 수집된 메트릭: {len(metrics)}개\n📈 데이터 포인트: {len(data)}개\n💾 저장 위치: {csv_file}"

        except Exception as e:
            return f"메트릭 수집 중 오류 발생: {str(e)}"

    async def analyze_correlation(self, csv_file: str, target_metric: str = 'CPUUtilization', 
                                top_n: int = 10) -> str:
        """메트릭 간 상관관계 분석"""
        try:
            # CSV 파일 경로 처리
            if not csv_file.startswith('/'):
                csv_path = DATA_DIR / csv_file
            else:
                csv_path = Path(csv_file)

            if not csv_path.exists():
                return f"CSV 파일을 찾을 수 없습니다: {csv_path}"

            # 데이터 읽기
            df = pd.read_csv(csv_path, index_col='Timestamp', parse_dates=True)
            df = df.dropna()

            if target_metric not in df.columns:
                return f"타겟 메트릭 '{target_metric}'이 데이터에 없습니다.\n사용 가능한 메트릭: {list(df.columns)}"

            # 상관 분석
            correlation_matrix = df.corr()
            target_correlations = correlation_matrix[target_metric].abs()
            target_correlations = target_correlations.drop(target_metric, errors='ignore')
            top_correlations = target_correlations.nlargest(top_n)

            # 결과 문자열 생성
            result = f"📊 {target_metric}과 상관관계가 높은 상위 {top_n}개 메트릭:\n\n"
            for metric, correlation in top_correlations.items():
                result += f"• {metric}: {correlation:.4f}\n"

            # 시각화
            plt.figure(figsize=(12, 6))
            top_correlations.plot(kind='bar')
            plt.title(f'Top {top_n} Metrics Correlated with {target_metric}')
            plt.xlabel('Metrics')
            plt.ylabel('Correlation Coefficient')
            plt.xticks(rotation=45, ha='right')
            plt.tight_layout()
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            graph_file = OUTPUT_DIR / f'correlation_analysis_{target_metric}_{timestamp}.png'
            plt.savefig(graph_file, dpi=300, bbox_inches='tight')
            plt.close()

            result += f"\n📈 상관관계 그래프가 저장되었습니다: {graph_file}"
            return result

        except Exception as e:
            return f"상관관계 분석 중 오류 발생: {str(e)}"

    async def detect_outliers(self, csv_file: str, std_threshold: float = 2.0) -> str:
        """아웃라이어 탐지"""
        try:
            # CSV 파일 경로 처리
            if not csv_file.startswith('/'):
                csv_path = DATA_DIR / csv_file
            else:
                csv_path = Path(csv_file)

            if not csv_path.exists():
                return f"CSV 파일을 찾을 수 없습니다: {csv_path}"

            # 데이터 읽기
            df = pd.read_csv(csv_path, index_col='Timestamp', parse_dates=True)
            df = df.dropna()

            # 각 메트릭에 대한 통계 계산
            stats = df.agg(['mean', 'max', 'min', 'std'])

            result = "📊 각 메트릭의 통계:\n"
            result += stats.to_string() + "\n\n"

            result += f"🚨 아웃라이어 탐지 결과 (임계값: ±{std_threshold}σ):\n\n"

            outlier_summary = []

            # 각 메트릭에 대해 아웃라이어 탐지
            for column in df.columns:
                series = df[column]
                mean = series.mean()
                std = series.std()
                lower_bound = mean - std_threshold * std
                upper_bound = mean + std_threshold * std
                
                outliers = series[(series < lower_bound) | (series > upper_bound)]
                
                if not outliers.empty:
                    result += f"⚠️ {column} 메트릭의 아웃라이어 ({len(outliers)}개):\n"
                    result += f"   정상 범위: {lower_bound:.2f} ~ {upper_bound:.2f}\n"
                    
                    # 최대 5개까지만 표시
                    for i, (timestamp, value) in enumerate(outliers.items()):
                        if i >= 5:
                            result += f"   ... 및 {len(outliers) - 5}개 더\n"
                            break
                        result += f"   • {timestamp}: {value:.2f}\n"
                    result += "\n"
                    
                    outlier_summary.append({
                        'metric': column,
                        'count': len(outliers),
                        'percentage': (len(outliers) / len(series)) * 100
                    })
                else:
                    result += f"✅ {column}: 아웃라이어 없음\n"

            # 요약 정보
            if outlier_summary:
                result += "\n📋 아웃라이어 요약:\n"
                for summary in outlier_summary:
                    result += f"• {summary['metric']}: {summary['count']}개 ({summary['percentage']:.1f}%)\n"

            return result

        except Exception as e:
            return f"아웃라이어 탐지 중 오류 발생: {str(e)}"

    async def perform_regression_analysis(self, csv_file: str, predictor_metric: str, 
                                        target_metric: str = 'CPUUtilization') -> str:
        """회귀 분석 수행"""
        try:
            # CSV 파일 경로 처리
            if not csv_file.startswith('/'):
                csv_path = DATA_DIR / csv_file
            else:
                csv_path = Path(csv_file)

            if not csv_path.exists():
                return f"CSV 파일을 찾을 수 없습니다: {csv_path}"

            # 데이터 읽기
            df = pd.read_csv(csv_path, index_col='Timestamp', parse_dates=True)

            # 필요한 메트릭 확인
            if predictor_metric not in df.columns or target_metric not in df.columns:
                return f"필요한 메트릭이 데이터에 없습니다.\n사용 가능한 메트릭: {list(df.columns)}"

            # 데이터 준비
            X = df[predictor_metric].values.reshape(-1, 1)
            y = df[target_metric].values

            # NaN 값 처리
            imputer = SimpleImputer(strategy='mean')
            X = imputer.fit_transform(X)
            y = imputer.fit_transform(y.reshape(-1, 1)).ravel()

            # 데이터 분할
            X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

            # 다항 회귀 모델 생성 (2차)
            poly_features = PolynomialFeatures(degree=2, include_bias=False)
            X_poly_train = poly_features.fit_transform(X_train)
            X_poly_test = poly_features.transform(X_test)

            # 모델 학습
            model = LinearRegression()
            model.fit(X_poly_train, y_train)

            # 예측
            y_pred = model.predict(X_poly_test)

            # 모델 평가
            mse = mean_squared_error(y_test, y_pred)
            r2 = r2_score(y_test, y_pred)

            # 계수 출력
            coefficients = model.coef_
            intercept = model.intercept_

            result = f"📈 회귀 분석 결과 ({predictor_metric} → {target_metric}):\n\n"
            result += f"📊 모델 성능:\n"
            result += f"• Mean Squared Error: {mse:.4f}\n"
            result += f"• R-squared Score: {r2:.4f}\n\n"
            
            result += f"🔢 다항 회귀 모델 (2차):\n"
            result += f"y = {coefficients[1]:.4f}x² + {coefficients[0]:.4f}x + {intercept:.4f}\n\n"

            # 그래프 그리기
            plt.figure(figsize=(12, 8))
            
            # 산점도
            plt.subplot(2, 1, 1)
            plt.scatter(X_test, y_test, color='blue', alpha=0.6, label='실제 데이터')
            
            # 예측 곡선을 위한 정렬된 데이터
            X_plot = np.linspace(X_test.min(), X_test.max(), 100).reshape(-1, 1)
            X_plot_poly = poly_features.transform(X_plot)
            y_plot_pred = model.predict(X_plot_poly)
            
            plt.plot(X_plot, y_plot_pred, color='red', linewidth=2, label='예측 모델')
            plt.title(f'{predictor_metric} vs {target_metric} 회귀 분석')
            plt.xlabel(predictor_metric)
            plt.ylabel(target_metric)
            plt.legend()
            plt.grid(True, alpha=0.3)

            # 잔차 플롯
            plt.subplot(2, 1, 2)
            residuals = y_test - y_pred
            plt.scatter(y_pred, residuals, color='green', alpha=0.6)
            plt.axhline(y=0, color='red', linestyle='--')
            plt.title('잔차 플롯 (Residual Plot)')
            plt.xlabel('예측값')
            plt.ylabel('잔차')
            plt.grid(True, alpha=0.3)

            plt.tight_layout()
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            graph_file = OUTPUT_DIR / f'regression_analysis_{predictor_metric}_{target_metric}_{timestamp}.png'
            plt.savefig(graph_file, dpi=300, bbox_inches='tight')
            plt.close()

            result += f"📈 회귀 분석 그래프가 저장되었습니다: {graph_file}"
            return result

        except Exception as e:
            return f"회귀 분석 중 오류 발생: {str(e)}"

    async def list_data_files(self) -> str:
        """데이터 파일 목록 조회"""
        try:
            csv_files = list(DATA_DIR.glob("*.csv"))
            if not csv_files:
                return "data 디렉토리에 CSV 파일이 없습니다."

            result = "📁 데이터 파일 목록:\n\n"
            for file in csv_files:
                file_size = file.stat().st_size
                modified_time = datetime.fromtimestamp(file.stat().st_mtime)
                result += f"• {file.name}\n"
                result += f"  크기: {file_size:,} bytes\n"
                result += f"  수정일: {modified_time.strftime('%Y-%m-%d %H:%M:%S')}\n\n"

            return result
        except Exception as e:
            return f"데이터 파일 목록 조회 실패: {str(e)}"

    async def get_metric_summary(self, csv_file: str) -> str:
        """메트릭 요약 정보 조회"""
        try:
            # CSV 파일 경로 처리
            if not csv_file.startswith('/'):
                csv_path = DATA_DIR / csv_file
            else:
                csv_path = Path(csv_file)

            if not csv_path.exists():
                return f"CSV 파일을 찾을 수 없습니다: {csv_path}"

            # 데이터 읽기
            df = pd.read_csv(csv_path, index_col='Timestamp', parse_dates=True)
            
            result = f"📊 메트릭 요약 정보 ({csv_file}):\n\n"
            result += f"📅 데이터 기간: {df.index.min()} ~ {df.index.max()}\n"
            result += f"📈 데이터 포인트: {len(df)}개\n"
            result += f"📋 메트릭 수: {len(df.columns)}개\n\n"
            
            result += "📊 메트릭 목록:\n"
            for i, column in enumerate(df.columns, 1):
                non_null_count = df[column].count()
                result += f"{i:2d}. {column} ({non_null_count}개 데이터)\n"

            # 기본 통계
            result += f"\n📈 기본 통계:\n"
            stats = df.describe()
            result += stats.to_string()

            return result

        except Exception as e:
            return f"메트릭 요약 정보 조회 중 오류 발생: {str(e)}"


# MCP 서버 설정
server = Server("analyze-db")

@server.list_tools()
async def handle_list_tools() -> list[types.Tool]:
    """사용 가능한 도구 목록 반환"""
    return [
        types.Tool(
            name="collect_db_metrics",
            description="CloudWatch에서 데이터베이스 메트릭을 수집합니다",
            inputSchema={
                "type": "object",
                "properties": {
                    "db_instance_identifier": {
                        "type": "string",
                        "description": "데이터베이스 인스턴스 식별자"
                    },
                    "hours": {
                        "type": "integer",
                        "description": "수집할 시간 범위 (시간 단위, 기본값: 24)",
                        "default": 24
                    },
                    "metrics": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "수집할 메트릭 목록 (선택사항)"
                    },
                    "region": {
                        "type": "string",
                        "description": "AWS 리전 (기본값: us-east-1)",
                        "default": "us-east-1"
                    }
                },
                "required": ["db_instance_identifier"]
            }
        ),
        types.Tool(
            name="analyze_metric_correlation",
            description="메트릭 간 상관관계를 분석합니다",
            inputSchema={
                "type": "object",
                "properties": {
                    "csv_file": {
                        "type": "string",
                        "description": "분석할 CSV 파일명"
                    },
                    "target_metric": {
                        "type": "string",
                        "description": "타겟 메트릭 (기본값: CPUUtilization)",
                        "default": "CPUUtilization"
                    },
                    "top_n": {
                        "type": "integer",
                        "description": "상위 N개 메트릭 (기본값: 10)",
                        "default": 10
                    }
                },
                "required": ["csv_file"]
            }
        ),
        types.Tool(
            name="detect_metric_outliers",
            description="메트릭 데이터에서 아웃라이어를 탐지합니다",
            inputSchema={
                "type": "object",
                "properties": {
                    "csv_file": {
                        "type": "string",
                        "description": "분석할 CSV 파일명"
                    },
                    "std_threshold": {
                        "type": "number",
                        "description": "표준편차 임계값 (기본값: 2.0)",
                        "default": 2.0
                    }
                },
                "required": ["csv_file"]
            }
        ),
        types.Tool(
            name="perform_regression_analysis",
            description="메트릭 간 회귀 분석을 수행합니다",
            inputSchema={
                "type": "object",
                "properties": {
                    "csv_file": {
                        "type": "string",
                        "description": "분석할 CSV 파일명"
                    },
                    "predictor_metric": {
                        "type": "string",
                        "description": "예측 변수 메트릭"
                    },
                    "target_metric": {
                        "type": "string",
                        "description": "타겟 메트릭 (기본값: CPUUtilization)",
                        "default": "CPUUtilization"
                    }
                },
                "required": ["csv_file", "predictor_metric"]
            }
        ),
        types.Tool(
            name="list_data_files",
            description="데이터 디렉토리의 CSV 파일 목록을 조회합니다",
            inputSchema={
                "type": "object",
                "properties": {}
            }
        ),
        types.Tool(
            name="get_metric_summary",
            description="CSV 파일의 메트릭 요약 정보를 조회합니다",
            inputSchema={
                "type": "object",
                "properties": {
                    "csv_file": {
                        "type": "string",
                        "description": "요약할 CSV 파일명"
                    }
                },
                "required": ["csv_file"]
            }
        )
    ]

@server.call_tool()
async def handle_call_tool(name: str, arguments: dict) -> list[types.TextContent]:
    """도구 호출 처리"""
    try:
        db_server = AnalyzeDBServer()
        
        if name == "collect_db_metrics":
            result = await db_server.collect_metrics(
                arguments["db_instance_identifier"],
                arguments.get("hours", 24),
                arguments.get("metrics"),
                arguments.get("region", "us-east-1")
            )
        elif name == "analyze_metric_correlation":
            result = await db_server.analyze_correlation(
                arguments["csv_file"],
                arguments.get("target_metric", "CPUUtilization"),
                arguments.get("top_n", 10)
            )
        elif name == "detect_metric_outliers":
            result = await db_server.detect_outliers(
                arguments["csv_file"],
                arguments.get("std_threshold", 2.0)
            )
        elif name == "perform_regression_analysis":
            result = await db_server.perform_regression_analysis(
                arguments["csv_file"],
                arguments["predictor_metric"],
                arguments.get("target_metric", "CPUUtilization")
            )
        elif name == "list_data_files":
            result = await db_server.list_data_files()
        elif name == "get_metric_summary":
            result = await db_server.get_metric_summary(arguments["csv_file"])
        else:
            result = f"알 수 없는 도구: {name}"
        
        return [types.TextContent(type="text", text=result)]
    except Exception as e:
        error_msg = f"도구 실행 중 오류 발생: {str(e)}"
        logger.error(error_msg)
        return [types.TextContent(type="text", text=error_msg)]

async def main():
    # Stdin/stdout를 통한 MCP 서버 실행
    async with mcp.server.stdio.stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream,
            write_stream,
            InitializationOptions(
                server_name="analyze-db",
                server_version="1.0.0",
                capabilities=server.get_capabilities(
                    notification_options=NotificationOptions(),
                    experimental_capabilities={},
                ),
            ),
        )

if __name__ == "__main__":
    asyncio.run(main())