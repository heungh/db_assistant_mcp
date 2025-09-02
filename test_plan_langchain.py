#!/usr/bin/env python3
"""
LangChain 기반 동적 플래닝 MCP 서버 - 실제 실행 버전
사용자 요청에 따라 동적으로 실행 계획을 생성하고 실제 MCP 도구를 호출하여 실행하는 서버
"""

import asyncio
import json
import logging
import boto3
from typing import Dict, List, Any, Optional
from pathlib import Path
import sys
import os
import importlib.util

# MCP 관련 import
from mcp.server.models import InitializationOptions
import mcp.types as types
from mcp.server import NotificationOptions, Server
import mcp.server.stdio

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 서버 인스턴스 생성
server = Server("dynamic-planning-mcp-server")

class DynamicPlanningMCPServer:
    def __init__(self):
        # Bedrock 클라이언트 초기화 (기존 서버와 동일)
        self.bedrock_client = boto3.client(
            "bedrock-runtime", region_name="us-west-2", verify=False
        )
        
        # 실제 MCP 서버 인스턴스 로드
        self.real_mcp_server = self._load_real_mcp_server()
        
        # 사용 가능한 도구들 정의
        self.available_tools = {
            "validate_sql_file": {
                "description": "SQL 파일 검증 및 분석",
                "params": ["filename", "database_secret"]
            },
            "get_performance_metrics": {
                "description": "데이터베이스 성능 메트릭 조회",
                "params": ["database_secret", "metric_type"]
            },
            "diagnose_memory_pressure": {
                "description": "메모리 부족 문제 진단",
                "params": ["database_secret"]
            },
            "diagnose_io_bottleneck": {
                "description": "I/O 병목 현상 진단",
                "params": ["database_secret"]
            },
            "collect_slow_queries": {
                "description": "느린 쿼리 수집 및 분석",
                "params": ["database_secret"]
            },
            "get_schema_summary": {
                "description": "데이터베이스 스키마 요약 정보",
                "params": ["database_secret"]
            },
            "test_database_connection": {
                "description": "데이터베이스 연결 테스트",
                "params": ["database_secret"]
            }
        }
        
        self.current_plan = []
        self.execution_results = []

    def _load_real_mcp_server(self):
        """실제 MCP 서버 인스턴스 로드"""
        try:
            # ddl_validation_qcli_mcp_server.py 모듈 동적 로드
            spec = importlib.util.spec_from_file_location(
                "ddl_validation_server", 
                "/Users/heungh/Documents/SA/05.Project/01.Infra-Assistant/01.DB-Assistant/ddl_validation_qcli_mcp_server.py"
            )
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            
            # DBAssistantMCPServer 인스턴스 생성
            return module.DBAssistantMCPServer()
        except Exception as e:
            logger.error(f"실제 MCP 서버 로드 실패: {e}")
            return None

    async def get_available_mcp_tools(self) -> Dict[str, Any]:
        """MCP 서버에서 사용 가능한 도구들을 동적으로 조회"""
        if not self.real_mcp_server:
            return {}
        
        try:
            # MCP 서버의 도구 목록 조회
            tools = {}
            
            # 실제 MCP 서버에서 지원하는 메서드들 동적 검색
            server_methods = [method for method in dir(self.real_mcp_server) 
                            if not method.startswith('_') and callable(getattr(self.real_mcp_server, method))]
            
            # 주요 도구들만 필터링
            key_tools = [
                'test_database_connection', 'get_performance_metrics', 'validate_sql_file',
                'diagnose_memory_pressure', 'diagnose_io_bottleneck', 'collect_slow_queries',
                'get_schema_summary', 'list_sql_files', 'list_database_secrets',
                'get_table_schema', 'collect_cpu_intensive_queries', 'collect_memory_intensive_queries'
            ]
            
            for tool_name in key_tools:
                if tool_name in server_methods:
                    method = getattr(self.real_mcp_server, tool_name)
                    # 메서드 시그니처 분석
                    import inspect
                    sig = inspect.signature(method)
                    params = list(sig.parameters.keys())
                    
                    tools[tool_name] = {
                        'description': method.__doc__ or f"{tool_name} 실행",
                        'params': params,
                        'available': True
                    }
            
            return tools
            
        except Exception as e:
            logger.error(f"MCP 도구 조회 실패: {e}")
            return {}

    async def execute_mcp_tool_dynamically(self, tool_name: str, params: Dict) -> Dict:
        """동적으로 MCP 도구 실행"""
        if not self.real_mcp_server:
            return {
                "status": "error",
                "result": "❌ MCP 서버를 사용할 수 없습니다",
                "details": "서버 로드 실패"
            }
        
        try:
            # 도구가 존재하는지 확인
            if not hasattr(self.real_mcp_server, tool_name):
                return {
                    "status": "error",
                    "result": f"❌ 도구 '{tool_name}'을 찾을 수 없습니다",
                    "details": f"사용 가능한 도구들을 확인하세요"
                }
            
            # 메서드 가져오기
            method = getattr(self.real_mcp_server, tool_name)
            
            # 파라미터 준비
            import inspect
            sig = inspect.signature(method)
            method_params = {}
            
            for param_name in sig.parameters.keys():
                if param_name in params:
                    method_params[param_name] = params[param_name]
                elif param_name == 'database_secret' and 'database_secret' not in params:
                    method_params[param_name] = "rds-mysql-dev"  # 기본값
            
            # 메서드 실행
            if asyncio.iscoroutinefunction(method):
                result = await method(**method_params)
            else:
                result = method(**method_params)
            
            # 결과 상태 판단
            status = "success"
            if isinstance(result, str):
                if any(word in result.lower() for word in ['오류', 'error', '실패', 'fail']):
                    status = "error"
                elif any(word in result.lower() for word in ['경고', 'warning', '주의']):
                    status = "warning"
            
            return {
                "status": status,
                "result": f"✅ {tool_name} 실행 완료",
                "details": result[:300] + "..." if len(str(result)) > 300 else str(result),
                "raw_result": result
            }
            
        except Exception as e:
            logger.error(f"동적 도구 실행 실패 - {tool_name}: {e}")
            return {
                "status": "error",
                "result": f"❌ {tool_name} 실행 중 오류 발생",
                "details": str(e)
            }

    async def call_claude(self, prompt: str) -> str:
        """Claude AI 호출 (기존 서버 방식 사용)"""
        
        # 입력 토큰 제한 확인 (약 150K 토큰으로 안전하게 설정)
        max_input_tokens = 150000
        estimated_tokens = len(prompt) * 1.5  # 한국어 기준 추정
        
        if estimated_tokens > max_input_tokens:
            logger.warning(f"입력이 너무 큽니다. 추정 토큰: {estimated_tokens}")
            # 프롬프트 자르기
            prompt = prompt[:int(max_input_tokens / 1.5)] + "\n\n[내용이 잘렸습니다. 요약된 계획을 생성합니다.]"
        
        claude_input = json.dumps({
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens": 8192,  # 최대 출력 토큰으로 증가
            "messages": [
                {"role": "user", "content": [{"type": "text", "text": prompt}]}
            ],
            "temperature": 0.3,
        })

        sonnet_4_model_id = "us.anthropic.claude-sonnet-4-20250514-v1:0"
        
        try:
            response = self.bedrock_client.invoke_model(
                modelId=sonnet_4_model_id, body=claude_input
            )
            response_body = json.loads(response.get("body").read())
            return response_body.get("content", [{}])[0].get("text", "")
        except Exception as e:
            logger.error(f"Claude 호출 실패: {e}")
            return f"AI 분석 실패: {str(e)}"

    async def create_dynamic_plan(self, user_request: str, database_secret: str = None) -> List[Dict]:
        """사용자 요청 분석하여 동적 실행 계획 생성"""
        
        # 실제 사용 가능한 도구들 동적 조회
        available_tools = await self.get_available_mcp_tools()
        
        if available_tools:
            tools_info = "\n".join([
                f"- {name}: {info['description']} (파라미터: {', '.join(info['params'])})"
                for name, info in available_tools.items()
            ])
        else:
            # 기본 도구 목록 사용
            tools_info = "\n".join([
                f"- {name}: {info['description']} (파라미터: {', '.join(info['params'])})"
                for name, info in self.available_tools.items()
            ])
        
        prompt = f"""
당신은 데이터베이스 작업을 위한 지능형 플래너입니다.
사용자의 요청을 분석하여 최적의 실행 계획을 JSON 형태로 생성하세요.

사용자 요청: "{user_request}"
데이터베이스 시크릿: {database_secret or "미지정"}

현재 사용 가능한 도구들:
{tools_info}

실행 계획을 다음 JSON 형식으로 반환하세요:
[
  {{
    "step": 1,
    "tool": "도구명",
    "params": {{"param1": "value1", "param2": "value2"}},
    "description": "이 단계에서 수행할 작업 설명",
    "expected_outcome": "예상 결과"
  }},
  ...
]

규칙:
1. 논리적 순서로 단계를 배열하세요
2. database_secret이 필요한 도구는 반드시 포함하세요
3. 사용자 요청에 가장 적합한 도구들만 선택하세요
4. 각 단계는 이전 단계의 결과를 활용할 수 있어야 합니다
5. JSON 형식만 반환하고 다른 텍스트는 포함하지 마세요
"""

        try:
            response = await self.call_claude(prompt)
            # JSON 부분만 추출
            if "```json" in response:
                json_part = response.split("```json")[1].split("```")[0].strip()
            elif "[" in response and "]" in response:
                start = response.find("[")
                end = response.rfind("]") + 1
                json_part = response[start:end]
            else:
                json_part = response.strip()
            
            plan = json.loads(json_part)
            self.current_plan = plan
            return plan
            
        except Exception as e:
            logger.error(f"플랜 생성 실패: {e}")
            # 기본 플랜 반환
            default_plan = [{
                "step": 1,
                "tool": "test_database_connection",
                "params": {"database_secret": database_secret or "rds-mysql-dev"},
                "description": "데이터베이스 연결 상태 확인",
                "expected_outcome": "연결 성공/실패 확인"
            }]
            self.current_plan = default_plan
            return default_plan

    async def execute_plan_step(self, step: Dict) -> Dict:
        """개별 플랜 단계 동적 실행"""
        tool_name = step.get("tool")
        params = step.get("params", {})
        
        # 동적으로 MCP 도구 실행
        return await self.execute_mcp_tool_dynamically(tool_name, params)

    async def execute_plan(self, plan: List[Dict]) -> Dict:
        """생성된 플랜 순차 실행 (실제 실행)"""
        results = []
        
        for step in plan:
            try:
                logger.info(f"실행 중: {step.get('tool')} - {step.get('description')}")
                step_result = await self.execute_plan_step(step)
                results.append({
                    "step": step.get("step"),
                    "tool": step.get("tool"),
                    "description": step.get("description"),
                    "status": step_result["status"],
                    "result": step_result["result"],
                    "details": step_result.get("details", "")
                })
                
            except Exception as e:
                logger.error(f"단계 실행 실패: {e}")
                results.append({
                    "step": step.get("step"),
                    "tool": step.get("tool"),
                    "status": "error",
                    "result": f"❌ 실행 실패: {str(e)}",
                    "details": ""
                })
        
        self.execution_results = results
        return {"plan_results": results}

# 전역 서버 인스턴스
dynamic_server = DynamicPlanningMCPServer()

@server.list_tools()
async def handle_list_tools() -> list[types.Tool]:
    """사용 가능한 도구 목록 반환"""
    return [
        types.Tool(
            name="list_available_mcp_tools",
            description="현재 MCP 서버에서 사용 가능한 모든 도구들을 동적으로 조회",
            inputSchema={
                "type": "object",
                "properties": {},
                "required": []
            }
        ),
        types.Tool(
            name="create_dynamic_plan",
            description="사용자 요청을 분석하여 동적 실행 계획 생성",
            inputSchema={
                "type": "object",
                "properties": {
                    "user_request": {
                        "type": "string",
                        "description": "사용자의 요청 또는 질문"
                    },
                    "database_secret": {
                        "type": "string", 
                        "description": "데이터베이스 시크릿 이름 (선택사항)"
                    }
                },
                "required": ["user_request"]
            }
        ),
        types.Tool(
            name="execute_dynamic_plan",
            description="생성된 동적 계획을 실행",
            inputSchema={
                "type": "object",
                "properties": {
                    "plan": {
                        "type": "array",
                        "description": "실행할 계획 (선택사항, 없으면 현재 계획 사용)"
                    }
                }
            }
        ),
        types.Tool(
            name="dynamic_workflow",
            description="사용자 요청에 따른 완전 자동화 워크플로우 (계획 생성 + 실행)",
            inputSchema={
                "type": "object",
                "properties": {
                    "user_request": {
                        "type": "string",
                        "description": "사용자의 요청 또는 질문"
                    },
                    "database_secret": {
                        "type": "string",
                        "description": "데이터베이스 시크릿 이름 (선택사항)"
                    }
                },
                "required": ["user_request"]
            }
        )
    ]

@server.call_tool()
async def handle_call_tool(name: str, arguments: dict) -> list[types.TextContent]:
    """도구 호출 처리"""
    
    if name == "list_available_mcp_tools":
        tools = await dynamic_server.get_available_mcp_tools()
        
        if not tools:
            return [types.TextContent(
                type="text",
                text="❌ MCP 도구를 조회할 수 없습니다. 서버 연결을 확인하세요."
            )]
        
        result = f"""
🔧 사용 가능한 MCP 도구들 ({len(tools)}개):

"""
        
        for tool_name, info in tools.items():
            result += f"""
📋 {tool_name}
   📝 설명: {info.get('description', '설명 없음')}
   🔧 파라미터: {', '.join(info.get('params', []))}
   ✅ 상태: {'사용 가능' if info.get('available') else '사용 불가'}
"""
        
        return [types.TextContent(type="text", text=result)]
    
    elif name == "create_dynamic_plan":
        user_request = arguments.get("user_request")
        database_secret = arguments.get("database_secret")
        
        plan = await dynamic_server.create_dynamic_plan(user_request, database_secret)
        
        result = f"""
🎯 동적 실행 계획 생성 완료

📋 사용자 요청: "{user_request}"
🗄️ 데이터베이스: {database_secret or "미지정"}

📊 생성된 계획 ({len(plan)}단계):
"""
        
        for step in plan:
            result += f"""
{step['step']}. {step.get('description', '설명 없음')}
   🔧 도구: {step['tool']}
   📝 파라미터: {step.get('params', {})}
   🎯 예상결과: {step.get('expected_outcome', '미정의')}
"""
        
        return [types.TextContent(type="text", text=result)]
    
    elif name == "execute_dynamic_plan":
        plan = arguments.get("plan", dynamic_server.current_plan)
        
        if not plan:
            return [types.TextContent(
                type="text", 
                text="❌ 실행할 계획이 없습니다. 먼저 create_dynamic_plan을 실행하세요."
            )]
        
        execution_result = await dynamic_server.execute_plan(plan)
        results = execution_result["plan_results"]
        
        success_count = sum(1 for r in results if r["status"] == "success")
        warning_count = sum(1 for r in results if r["status"] == "warning") 
        error_count = sum(1 for r in results if r["status"] == "error")
        
        result = f"""
🚀 동적 계획 실행 완료

📊 실행 요약:
• 총 단계: {len(results)}개
• ✅ 성공: {success_count}개
• ⚠️ 경고: {warning_count}개  
• ❌ 실패: {error_count}개

📋 상세 결과:
"""
        
        for step_result in results:
            status_icon = {"success": "✅", "warning": "⚠️", "error": "❌"}.get(
                step_result["status"], "❓"
            )
            result += f"""
{status_icon} 단계 {step_result['step']}: {step_result.get('description', '설명 없음')}
   🔧 도구: {step_result['tool']}
   📊 결과: {step_result['result']}
   📝 세부사항: {step_result.get('details', '없음')}
"""
        
        return [types.TextContent(type="text", text=result)]
    
    elif name == "dynamic_workflow":
        user_request = arguments.get("user_request")
        database_secret = arguments.get("database_secret")
        
        # 1단계: 계획 생성
        plan = await dynamic_server.create_dynamic_plan(user_request, database_secret)
        
        # 2단계: 계획 실행
        execution_result = await dynamic_server.execute_plan(plan)
        results = execution_result["plan_results"]
        
        success_count = sum(1 for r in results if r["status"] == "success")
        warning_count = sum(1 for r in results if r["status"] == "warning")
        error_count = sum(1 for r in results if r["status"] == "error")
        
        result = f"""
🤖 지능형 동적 워크플로우 완료

📋 사용자 요청: "{user_request}"
🗄️ 데이터베이스: {database_secret or "미지정"}

🎯 생성된 계획: {len(plan)}단계
📊 실행 결과: ✅{success_count} ⚠️{warning_count} ❌{error_count}

📈 단계별 실행 결과:
"""
        
        for i, (step, step_result) in enumerate(zip(plan, results), 1):
            status_icon = {"success": "✅", "warning": "⚠️", "error": "❌"}.get(
                step_result["status"], "❓"
            )
            result += f"""
{status_icon} {i}. {step.get('description', '설명 없음')}
   🔧 {step['tool']} → {step_result['result']}
   📝 {step_result.get('details', '')}
"""
        
        # 요약 및 권장사항
        if error_count > 0:
            result += f"\n⚠️ {error_count}개 단계에서 오류가 발생했습니다. 로그를 확인하세요."
        elif warning_count > 0:
            result += f"\n💡 {warning_count}개 단계에서 주의사항이 발견되었습니다."
        else:
            result += "\n🎉 모든 단계가 성공적으로 완료되었습니다!"
        
        return [types.TextContent(type="text", text=result)]
    
    else:
        return [types.TextContent(
            type="text", 
            text=f"❌ 알 수 없는 도구: {name}"
        )]

async def main():
    """메인 실행 함수"""
    # Stdio 서버 실행
    async with mcp.server.stdio.stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream,
            write_stream,
            InitializationOptions(
                server_name="dynamic-planning-mcp-server",
                server_version="1.0.0",
                capabilities=server.get_capabilities(
                    notification_options=NotificationOptions(),
                    experimental_capabilities={},
                ),
            ),
        )

if __name__ == "__main__":
    asyncio.run(main())
