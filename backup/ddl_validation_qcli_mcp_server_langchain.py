#!/usr/bin/env python3
"""
DDL Validation MCP Server with LangChain Node-based Architecture
"""

import asyncio
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass

from mcp.server.models import InitializationOptions
from mcp.server import NotificationOptions, Server
from mcp.types import Resource, Tool, TextContent, ImageContent, EmbeddedResource
from mcp import types
import mcp.server.stdio

# LangChain imports
from langgraph.graph import StateGraph, END
from typing_extensions import TypedDict

# 기존 DDL Validator 클래스 import (기존 코드에서)
import sys
sys.path.append(str(Path(__file__).parent))

# 상태 정의
class ExecutionState(TypedDict):
    operation: str
    parameters: Dict[str, Any]
    plan: Optional[Dict[str, Any]]
    confirmation: Optional[str]
    result: Optional[str]
    error: Optional[str]

@dataclass
class ToolNode:
    """툴 노드 정의"""
    name: str
    description: str
    function: Callable
    input_schema: Dict[str, Any]
    requires_plan: bool = True

class LangChainMCPServer:
    """LangChain 기반 MCP 서버"""
    
    def __init__(self):
        self.server = Server("ddl-validation-langchain")
        self.tools: Dict[str, ToolNode] = {}
        self.workflow = None
        self.current_state: Optional[ExecutionState] = None
        
        # 기존 DDL Validator 인스턴스
        from ddl_validation_qcli_mcp_server import DDLValidator
        self.ddl_validator = DDLValidator()
        
        self._setup_workflow()
        self._register_tools()
    
    def _setup_workflow(self):
        """LangChain 워크플로우 설정"""
        workflow = StateGraph(ExecutionState)
        
        # 노드 추가
        workflow.add_node("plan_generator", self._plan_generator_node)
        workflow.add_node("confirmation_checker", self._confirmation_checker_node)
        workflow.add_node("tool_executor", self._tool_executor_node)
        workflow.add_node("error_handler", self._error_handler_node)
        
        # 엣지 설정
        workflow.set_entry_point("plan_generator")
        
        workflow.add_conditional_edges(
            "plan_generator",
            self._should_confirm,
            {
                "confirm": "confirmation_checker",
                "execute": "tool_executor",
                "error": "error_handler"
            }
        )
        
        workflow.add_conditional_edges(
            "confirmation_checker", 
            self._check_confirmation,
            {
                "approved": "tool_executor",
                "rejected": END,
                "waiting": END
            }
        )
        
        workflow.add_edge("tool_executor", END)
        workflow.add_edge("error_handler", END)
        
        self.workflow = workflow.compile()
    
    def _register_tools(self):
        """툴 등록"""
        # 기본 툴들 등록
        self.register_tool(
            name="validate_sql_file",
            description="SQL 파일을 검증합니다",
            function=self.ddl_validator.validate_sql_file,
            input_schema={
                "type": "object",
                "properties": {
                    "filename": {"type": "string"},
                    "database_secret": {"type": "string"}
                },
                "required": ["filename"]
            }
        )
        
        self.register_tool(
            name="validate_all_sql",
            description="모든 SQL 파일을 검증합니다",
            function=self.ddl_validator.validate_all_sql_files,
            input_schema={
                "type": "object",
                "properties": {
                    "database_secret": {"type": "string"}
                }
            }
        )
        
        self.register_tool(
            name="test_database_connection",
            description="데이터베이스 연결을 테스트합니다",
            function=self.ddl_validator.test_connection_only,
            input_schema={
                "type": "object",
                "properties": {
                    "database_secret": {"type": "string"}
                },
                "required": ["database_secret"]
            }
        )
        
        self.register_tool(
            name="analyze_current_schema",
            description="현재 데이터베이스 스키마를 분석합니다",
            function=self.ddl_validator.analyze_current_schema,
            input_schema={
                "type": "object",
                "properties": {
                    "database_secret": {"type": "string"}
                },
                "required": ["database_secret"]
            }
        )
        
        self.register_tool(
            name="get_aurora_mysql_parameters",
            description="Aurora MySQL 파라미터를 조회합니다",
            function=self.ddl_validator.get_aurora_mysql_parameters,
            input_schema={
                "type": "object",
                "properties": {
                    "cluster_identifier": {"type": "string"},
                    "region": {"type": "string", "default": "ap-northeast-2"},
                    "category": {"type": "string", "default": "all"},
                    "filter_type": {"type": "string", "default": "important"}
                },
                "required": ["cluster_identifier"]
            }
        )
        
        # 계획 관련 툴들 (계획 생성 불필요)
        self.register_tool(
            name="list_sql_files",
            description="SQL 파일 목록을 조회합니다",
            function=self.ddl_validator.list_sql_files,
            input_schema={"type": "object"},
            requires_plan=False
        )
        
        self.register_tool(
            name="list_database_secrets",
            description="데이터베이스 시크릿 목록을 조회합니다",
            function=self.ddl_validator.list_database_secrets,
            input_schema={
                "type": "object",
                "properties": {
                    "keyword": {"type": "string"}
                }
            },
            requires_plan=False
        )
    
    def register_tool(self, name: str, description: str, function: Callable, 
                     input_schema: Dict[str, Any], requires_plan: bool = True):
        """동적 툴 등록"""
        self.tools[name] = ToolNode(
            name=name,
            description=description,
            function=function,
            input_schema=input_schema,
            requires_plan=requires_plan
        )
    
    async def _plan_generator_node(self, state: ExecutionState) -> ExecutionState:
        """실행 계획 생성 노드"""
        try:
            operation = state["operation"]
            parameters = state["parameters"]
            
            # 계획이 필요한 툴인지 확인
            tool_node = self.tools.get(operation)
            if not tool_node or not tool_node.requires_plan:
                # 계획 불필요한 툴은 바로 실행
                state["plan"] = None
                return state
            
            # 실행 계획 생성
            plan = await self._create_execution_plan(operation, **parameters)
            state["plan"] = plan
            
            return state
            
        except Exception as e:
            state["error"] = f"계획 생성 실패: {str(e)}"
            return state
    
    async def _confirmation_checker_node(self, state: ExecutionState) -> ExecutionState:
        """확인 체크 노드"""
        plan = state["plan"]
        if not plan:
            return state
            
        # 계획 표시 및 확인 요청
        plan_display = self._format_plan_display(plan)
        confirmation_message = f"""📋 **실행 계획을 생성했습니다:**

{plan_display}

❓ **이 계획대로 진행하시겠습니까?** (y/n)

💡 **참고:** 'y' 또는 'yes'로 응답하면 실행을 시작합니다.
"""
        
        state["result"] = confirmation_message
        return state
    
    async def _tool_executor_node(self, state: ExecutionState) -> ExecutionState:
        """툴 실행 노드"""
        try:
            operation = state["operation"]
            parameters = state["parameters"]
            
            tool_node = self.tools.get(operation)
            if not tool_node:
                state["error"] = f"알 수 없는 툴: {operation}"
                return state
            
            # 툴 실행
            if asyncio.iscoroutinefunction(tool_node.function):
                result = await tool_node.function(**parameters)
            else:
                result = tool_node.function(**parameters)
            
            # 실행 완료 메시지 추가
            if state.get("plan"):
                result = f"🚀 **실행 시작:** {operation}\n\n{result}\n\n✅ **실행 완료:** {operation}"
            
            state["result"] = result
            return state
            
        except Exception as e:
            state["error"] = f"툴 실행 실패: {str(e)}"
            return state
    
    async def _error_handler_node(self, state: ExecutionState) -> ExecutionState:
        """에러 처리 노드"""
        error = state.get("error", "알 수 없는 오류")
        state["result"] = f"❌ 오류 발생: {error}"
        return state
    
    def _should_confirm(self, state: ExecutionState) -> str:
        """확인이 필요한지 판단"""
        if state.get("error"):
            return "error"
        elif state.get("plan"):
            return "confirm"
        else:
            return "execute"
    
    def _check_confirmation(self, state: ExecutionState) -> str:
        """확인 상태 체크"""
        confirmation = state.get("confirmation", "").lower()
        if confirmation in ['y', 'yes', '예', 'ㅇ']:
            return "approved"
        elif confirmation in ['n', 'no', '아니오', 'ㄴ']:
            return "rejected"
        else:
            return "waiting"
    
    async def _create_execution_plan(self, operation: str, **kwargs) -> Dict[str, Any]:
        """실행 계획 생성"""
        plan_steps = []
        tool_name = operation
        
        # 툴별 실행 계획 정의
        if operation == "validate_sql_file":
            filename = kwargs.get('filename')
            database_secret = kwargs.get('database_secret')
            
            plan_steps = [
                {"step": 1, "action": "파일 존재 확인", "target": filename, "tool": "fs_read"},
                {"step": 2, "action": "DDL 내용 읽기", "target": filename, "tool": "fs_read"},
                {"step": 3, "action": "기본 문법 검증", "target": "DDL 구문", "tool": "internal_parser"},
            ]
            
            if database_secret:
                plan_steps.extend([
                    {"step": 4, "action": "데이터베이스 연결 테스트", "target": database_secret, "tool": "test_database_connection"},
                    {"step": 5, "action": "스키마 검증", "target": "현재 스키마와 비교", "tool": "analyze_current_schema"},
                    {"step": 6, "action": "제약조건 검증", "target": "FK, 인덱스 등", "tool": "check_ddl_conflicts"},
                ])
            
            plan_steps.extend([
                {"step": len(plan_steps) + 1, "action": "Claude AI 검증", "target": "고급 분석", "tool": "claude_analysis"},
                {"step": len(plan_steps) + 2, "action": "HTML 보고서 생성", "target": "output 디렉토리", "tool": "fs_write"}
            ])
            
        elif operation == "validate_all_sql":
            plan_steps = [
                {"step": 1, "action": "SQL 파일 목록 조회", "target": "sql 디렉토리", "tool": "list_sql_files"},
                {"step": 2, "action": "파일 개수 확인", "target": "최대 5개 제한", "tool": "internal_check"},
                {"step": 3, "action": "각 파일 순차 검증", "target": "개별 파일", "tool": "validate_sql_file"},
                {"step": 4, "action": "종합 결과 생성", "target": "전체 요약", "tool": "fs_write"}
            ]
            
        elif operation == "test_database_connection":
            database_secret = kwargs.get('database_secret')
            plan_steps = [
                {"step": 1, "action": "시크릿 정보 조회", "target": database_secret, "tool": "aws_secrets"},
                {"step": 2, "action": "데이터베이스 연결 시도", "target": "MySQL 연결", "tool": "mysql_connector"},
                {"step": 3, "action": "연결 상태 확인", "target": "연결 테스트", "tool": "internal_check"}
            ]
            
        elif operation == "analyze_current_schema":
            database_secret = kwargs.get('database_secret')
            plan_steps = [
                {"step": 1, "action": "데이터베이스 연결", "target": database_secret, "tool": "test_database_connection"},
                {"step": 2, "action": "테이블 목록 조회", "target": "information_schema", "tool": "mysql_query"},
                {"step": 3, "action": "컬럼 정보 수집", "target": "각 테이블", "tool": "mysql_query"},
                {"step": 4, "action": "인덱스 정보 수집", "target": "각 테이블", "tool": "mysql_query"},
                {"step": 5, "action": "외래키 정보 수집", "target": "각 테이블", "tool": "mysql_query"},
                {"step": 6, "action": "스키마 분석 결과 생성", "target": "종합 정보", "tool": "internal_analysis"}
            ]
            
        elif operation == "get_aurora_mysql_parameters":
            cluster_identifier = kwargs.get('cluster_identifier')
            plan_steps = [
                {"step": 1, "action": "클러스터 정보 조회", "target": cluster_identifier, "tool": "use_aws"},
                {"step": 2, "action": "파라미터 그룹 확인", "target": "적용된 그룹", "tool": "use_aws"},
                {"step": 3, "action": "파라미터 값 조회", "target": "주요 설정", "tool": "use_aws"},
                {"step": 4, "action": "커스텀 설정 필터링", "target": "사용자 정의 값", "tool": "internal_filter"}
            ]
            
        else:
            plan_steps = [
                {"step": 1, "action": f"{operation} 실행", "target": "기본 동작", "tool": operation}
            ]
        
        return {
            "operation": operation,
            "tool_name": tool_name,
            "parameters": kwargs,
            "steps": plan_steps,
            "created_at": datetime.now().isoformat(),
            "status": "created"
        }
    
    def _format_plan_display(self, plan: Dict[str, Any]) -> str:
        """계획 표시 형식 생성"""
        result = f"""🎯 **작업:** {plan['operation']}
📅 **생성 시간:** {plan['created_at']}
🔧 **매칭 툴:** {plan.get('tool_name', plan['operation'])}

📝 **실행 단계:**"""
        
        for step in plan['steps']:
            tool_info = f" [{step.get('tool', 'internal')}]" if step.get('tool') else ""
            result += f"\n   {step['step']}. {step['action']} → {step['target']}{tool_info}"
        
        if plan['parameters']:
            result += f"\n\n⚙️ **매개변수:**"
            for key, value in plan['parameters'].items():
                if value:
                    result += f"\n   • {key}: {value}"
        
        return result
    
    async def execute_tool(self, name: str, arguments: Dict[str, Any]) -> str:
        """툴 실행 (LangChain 워크플로우 사용)"""
        # 초기 상태 설정
        initial_state: ExecutionState = {
            "operation": name,
            "parameters": arguments,
            "plan": None,
            "confirmation": None,
            "result": None,
            "error": None
        }
        
        # 워크플로우 실행
        final_state = await self.workflow.ainvoke(initial_state)
        
        # 결과 반환
        if final_state.get("error"):
            return f"❌ {final_state['error']}"
        else:
            return final_state.get("result", "✅ 실행 완료")
    
    async def confirm_and_execute(self, confirmation: str) -> str:
        """확인 후 실행"""
        if not self.current_state:
            return "❌ 실행할 계획이 없습니다."
        
        # 확인 상태 업데이트
        self.current_state["confirmation"] = confirmation
        
        # 워크플로우 재실행 (확인 단계부터)
        final_state = await self.workflow.ainvoke(self.current_state)
        
        if confirmation.lower() not in ['y', 'yes', '예', 'ㅇ']:
            self.current_state = None
            return "❌ 실행이 취소되었습니다."
        
        # 실행 단계로 이동
        self.current_state["confirmation"] = "y"
        final_state = await self.workflow.ainvoke(self.current_state)
        
        self.current_state = None
        return final_state.get("result", "✅ 실행 완료")

# MCP 서버 설정
server = Server("ddl-validation-langchain")
langchain_server = LangChainMCPServer()

@server.list_resources()
async def handle_list_resources() -> list[types.Resource]:
    """리소스 목록 반환"""
    return []

@server.list_tools()
async def handle_list_tools() -> list[types.Tool]:
    """도구 목록 반환"""
    tools = []
    
    for tool_name, tool_node in langchain_server.tools.items():
        tools.append(types.Tool(
            name=tool_name,
            description=tool_node.description,
            inputSchema=tool_node.input_schema
        ))
    
    # 확인 실행 툴 추가
    tools.append(types.Tool(
        name="confirm_and_execute",
        description="계획 확인 후 실행합니다",
        inputSchema={
            "type": "object",
            "properties": {
                "confirmation": {"type": "string", "description": "실행 확인 (y/yes/n/no)"}
            },
            "required": ["confirmation"]
        }
    ))
    
    return tools

@server.call_tool()
async def handle_call_tool(name: str, arguments: dict) -> list[types.TextContent]:
    """도구 호출 처리"""
    try:
        if name == "confirm_and_execute":
            result = await langchain_server.confirm_and_execute(arguments["confirmation"])
        else:
            # LangChain 워크플로우로 실행
            result = await langchain_server.execute_tool(name, arguments)
            
            # 계획이 생성된 경우 현재 상태 저장
            if "실행 계획을 생성했습니다" in result:
                langchain_server.current_state = {
                    "operation": name,
                    "parameters": arguments,
                    "plan": await langchain_server._create_execution_plan(name, **arguments),
                    "confirmation": None,
                    "result": None,
                    "error": None
                }
        
        return [types.TextContent(type="text", text=result)]
        
    except Exception as e:
        error_msg = f"도구 실행 중 오류 발생: {str(e)}"
        return [types.TextContent(type="text", text=error_msg)]

async def main():
    """메인 함수"""
    async with mcp.server.stdio.stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream,
            write_stream,
            InitializationOptions(
                server_name="ddl-validation-langchain",
                server_version="1.0.0",
                capabilities=server.get_capabilities(
                    notification_options=NotificationOptions(),
                    experimental_capabilities={},
                ),
            ),
        )

if __name__ == "__main__":
    asyncio.run(main())
