"""
AI 통합 모듈

Claude AI 및 Knowledge Base와의 통합 기능을 제공합니다.
"""

from typing import Dict, List, Any, Optional
import logging
import boto3

from .shared_types import (
    KnowledgeBaseQuery,
    KnowledgeBaseResult,
    AIRecommendation,
    SQLType,
)
from utils.constants import KNOWLEDGE_BASE_ID

logger = logging.getLogger(__name__)


class AIIntegration:
    """AI 통합 클래스"""

    def __init__(
        self,
        bedrock_client: Any,
        bedrock_agent_client: Any,
        knowledge_base_id: str = KNOWLEDGE_BASE_ID
    ):
        """
        초기화

        Args:
            bedrock_client: Bedrock 런타임 클라이언트
            bedrock_agent_client: Bedrock Agent 런타임 클라이언트
            knowledge_base_id: Knowledge Base ID
        """
        self.bedrock_client = bedrock_client
        self.bedrock_agent_client = bedrock_agent_client
        self.knowledge_base_id = knowledge_base_id

    def query_knowledge_base(
        self,
        query: str,
        sql_type: Optional[str] = None
    ) -> List[KnowledgeBaseResult]:
        """
        Knowledge Base 조회

        Args:
            query: 검색 쿼리
            sql_type: SQL 타입 (선택사항)

        Returns:
            KnowledgeBaseResult 리스트
        """
        logger.info(f"Knowledge Base 조회: {query[:50]}...")

        try:
            # SQL 타입에 따른 쿼리 조정
            ddl_types = ["CREATE_TABLE", "ALTER_TABLE", "CREATE_INDEX", "DROP_TABLE", "DROP_INDEX"]
            dql_types = ["SELECT", "UPDATE", "DELETE", "INSERT"]

            if sql_type in ddl_types:
                kb_query = f"데이터베이스 도메인 관리 규칙 {query}"
            elif sql_type in dql_types:
                kb_query = f"Aurora MySQL 최적화 가이드 {query}"
            else:
                kb_query = f"데이터베이스 도메인 관리 규칙 {query}"

            response = self.bedrock_agent_client.retrieve(
                knowledgeBaseId=self.knowledge_base_id,
                retrievalQuery={"text": kb_query},
                retrievalConfiguration={
                    "vectorSearchConfiguration": {"numberOfResults": 3}
                },
            )

            # 검색 결과에서 텍스트 추출
            results = []
            for result in response.get("retrievalResults", []):
                content = result.get("content", {}).get("text", "")
                if content:
                    kb_result = KnowledgeBaseResult(
                        content=content,
                        relevance_score=result.get("score", 0.0),
                        source=result.get("location", {}).get("s3Location", {}).get("uri"),
                    )
                    results.append(kb_result)

            logger.info(f"Knowledge Base 조회 완료: {len(results)}개 결과")
            return results

        except Exception as e:
            logger.error(f"Knowledge Base 조회 실패: {e}")
            return []

    def validate_with_claude(
        self,
        sql_content: str,
        context: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Claude AI를 사용한 SQL 검증

        Args:
            sql_content: SQL 내용
            context: 컨텍스트 정보

        Returns:
            검증 결과 딕셔너리
        """
        logger.info("Claude AI 검증 시작")

        try:
            # Knowledge Base에서 관련 정보 조회
            kb_results = self.query_knowledge_base(
                sql_content[:200],
                context.get("sql_type")
            )

            knowledge_content = "\n\n".join([r.content for r in kb_results])

            # Claude에게 검증 요청
            prompt = self._create_validation_prompt(sql_content, knowledge_content, context)

            response = self.bedrock_client.invoke_model(
                modelId="anthropic.claude-3-5-sonnet-20241022-v2:0",
                body={
                    "anthropic_version": "bedrock-2023-05-31",
                    "max_tokens": 4096,
                    "messages": [{"role": "user", "content": prompt}],
                    "temperature": 0.0,
                }
            )

            # 응답 파싱 (구현 필요)
            result = self._parse_claude_response(response)

            logger.info("Claude AI 검증 완료")
            return result

        except Exception as e:
            logger.error(f"Claude AI 검증 실패: {e}")
            return {
                "success": False,
                "error": str(e),
                "recommendations": []
            }

    def text_to_sql(
        self,
        natural_language: str,
        context: Dict[str, Any]
    ) -> str:
        """
        자연어를 SQL로 변환

        Args:
            natural_language: 자연어 쿼리
            context: 데이터베이스 스키마 등의 컨텍스트

        Returns:
            생성된 SQL 문자열
        """
        logger.info(f"자연어를 SQL로 변환: {natural_language[:50]}...")

        try:
            prompt = self._create_text_to_sql_prompt(natural_language, context)

            # Claude 호출 (구현 필요)
            sql = self._invoke_claude_for_sql_generation(prompt)

            logger.info("SQL 생성 완료")
            return sql

        except Exception as e:
            logger.error(f"SQL 생성 실패: {e}")
            return ""

    def get_recommendations(
        self,
        sql_content: str,
        validation_result: Dict[str, Any]
    ) -> List[AIRecommendation]:
        """
        AI 기반 권장사항 조회

        Args:
            sql_content: SQL 내용
            validation_result: 검증 결과

        Returns:
            AIRecommendation 리스트
        """
        logger.info("AI 권장사항 조회 시작")

        recommendations = []

        # 성능 이슈가 있으면 권장사항 생성
        if validation_result.get("performance_issues"):
            for issue in validation_result["performance_issues"]:
                rec = AIRecommendation(
                    category="performance",
                    severity="high" if "critical" in issue.lower() else "medium",
                    title="성능 최적화 필요",
                    description=issue,
                )
                recommendations.append(rec)

        logger.info(f"AI 권장사항 {len(recommendations)}개 생성")
        return recommendations

    def _create_validation_prompt(
        self,
        sql_content: str,
        knowledge_content: str,
        context: Dict[str, Any]
    ) -> str:
        """검증용 프롬프트 생성"""
        return f"""
다음 SQL 문을 검증해주세요:

{sql_content}

참고 자료:
{knowledge_content}

컨텍스트:
- SQL 타입: {context.get('sql_type', 'Unknown')}
- 데이터베이스: {context.get('database', 'Unknown')}

검증 항목:
1. 구문 정확성
2. 성능 이슈
3. 보안 취약점
4. 베스트 프랙티스 준수 여부

결과를 JSON 형식으로 제공해주세요.
"""

    def _create_text_to_sql_prompt(
        self,
        natural_language: str,
        context: Dict[str, Any]
    ) -> str:
        """Text-to-SQL 프롬프트 생성"""
        return f"""
다음 자연어 질의를 SQL로 변환해주세요:

{natural_language}

스키마 정보:
{context.get('schema', 'No schema provided')}

SQL 문만 반환해주세요.
"""

    def _parse_claude_response(self, response: Any) -> Dict[str, Any]:
        """Claude 응답 파싱 (구현 필요)"""
        # TODO: 실제 응답 파싱 로직 구현
        return {
            "success": True,
            "recommendations": [],
            "issues": []
        }

    def _invoke_claude_for_sql_generation(self, prompt: str) -> str:
        """SQL 생성을 위한 Claude 호출 (구현 필요)"""
        # TODO: 실제 Claude 호출 로직 구현
        return "-- Generated SQL (implementation needed)"
