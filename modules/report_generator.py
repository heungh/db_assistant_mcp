"""
보고서 생성 모듈

HTML 보고서, 성능 보고서, 클러스터 보고서 등을 생성합니다.
"""

from pathlib import Path
from typing import Dict, List, Any, Optional
from datetime import datetime
import logging

from .shared_types import ReportConfig, ChartData
from .interfaces import ReportGeneratorInterface
from utils.formatters import (
    format_bytes,
    format_number,
    format_percentage,
    format_duration,
    format_timestamp,
)

logger = logging.getLogger(__name__)


class ReportGenerator(ReportGeneratorInterface):
    """보고서 생성 클래스"""

    def __init__(self):
        """초기화"""
        self.output_dir = Path("output")
        self.output_dir.mkdir(exist_ok=True)

    def generate_report(
        self,
        config: ReportConfig,
        data: Dict[str, Any]
    ) -> Path:
        """
        보고서 생성

        Args:
            config: 보고서 설정
            data: 보고서 데이터

        Returns:
            생성된 보고서 파일 경로
        """
        logger.info(f"보고서 생성 시작: {config.title}")

        if config.format == "html":
            html_content = self.generate_html(data)
            output_path = Path(config.output_path)
            output_path.parent.mkdir(parents=True, exist_ok=True)

            with open(output_path, "w", encoding="utf-8") as f:
                f.write(html_content)

            logger.info(f"보고서 생성 완료: {output_path}")
            return output_path
        else:
            raise ValueError(f"지원하지 않는 포맷: {config.format}")

    def generate_html(self, data: Dict[str, Any]) -> str:
        """
        HTML 보고서 생성

        Args:
            data: 보고서 데이터

        Returns:
            HTML 문자열
        """
        report_type = data.get("type", "general")

        if report_type == "performance":
            return self._generate_performance_html_report(data)
        elif report_type == "cluster":
            return self._generate_cluster_html_report(data)
        elif report_type == "error_log":
            return self._generate_error_log_html_report(data)
        else:
            return self._generate_generic_html_report(data)

    def create_charts(self, data: Dict[str, Any]) -> List[str]:
        """
        차트 생성

        Args:
            data: 차트 데이터

        Returns:
            차트 HTML 리스트
        """
        charts = []
        chart_data_list = data.get("charts", [])

        for chart_data in chart_data_list:
            chart_html = self._create_chart(chart_data)
            charts.append(chart_html)

        return charts

    def _generate_performance_html_report(self, data: Dict[str, Any]) -> str:
        """성능 HTML 보고서 생성"""
        title = data.get("title", "성능 분석 보고서")
        metrics = data.get("metrics", {})
        charts = data.get("charts", [])

        html = f"""
<!DOCTYPE html>
<html lang="ko">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{title}</title>
    <style>
        {self._get_report_css()}
    </style>
</head>
<body>
    <div class="container">
        <h1>{title}</h1>
        <p class="timestamp">생성 시간: {format_timestamp(datetime.now())}</p>

        <div class="section">
            <h2>메트릭 요약</h2>
            {self._generate_metrics_table(metrics)}
        </div>

        <div class="section">
            <h2>성능 차트</h2>
            {self._generate_charts_html(charts)}
        </div>

        <div class="section">
            <h2>권장 사항</h2>
            {self._generate_recommendations_html(data.get("recommendations", []))}
        </div>
    </div>
</body>
</html>
"""
        return html

    def _generate_cluster_html_report(self, data: Dict[str, Any]) -> str:
        """클러스터 HTML 보고서 생성"""
        title = data.get("title", "클러스터 성능 보고서")
        cluster_info = data.get("cluster_info", {})

        html = f"""
<!DOCTYPE html>
<html lang="ko">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{title}</title>
    <style>
        {self._get_report_css()}
    </style>
</head>
<body>
    <div class="container">
        <h1>{title}</h1>
        <p class="timestamp">생성 시간: {format_timestamp(datetime.now())}</p>

        <div class="section">
            <h2>클러스터 정보</h2>
            {self._generate_cluster_info_html(cluster_info)}
        </div>

        <div class="section">
            <h2>노드별 성능</h2>
            {self._generate_node_metrics_html(data.get("node_metrics", []))}
        </div>
    </div>
</body>
</html>
"""
        return html

    def _generate_error_log_html_report(self, data: Dict[str, Any]) -> str:
        """에러 로그 HTML 보고서 생성"""
        title = data.get("title", "에러 로그 분석 보고서")
        error_summary = data.get("error_summary", {})

        html = f"""
<!DOCTYPE html>
<html lang="ko">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{title}</title>
    <style>
        {self._get_report_css()}
    </style>
</head>
<body>
    <div class="container">
        <h1>{title}</h1>
        <p class="timestamp">생성 시간: {format_timestamp(datetime.now())}</p>

        <div class="section">
            <h2>에러 요약</h2>
            {self._generate_error_summary_html(error_summary)}
        </div>

        <div class="section">
            <h2>에러 패턴</h2>
            {self._generate_error_patterns_html(data.get("patterns", []))}
        </div>
    </div>
</body>
</html>
"""
        return html

    def _generate_generic_html_report(self, data: Dict[str, Any]) -> str:
        """일반 HTML 보고서 생성"""
        title = data.get("title", "보고서")

        html = f"""
<!DOCTYPE html>
<html lang="ko">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{title}</title>
    <style>
        {self._get_report_css()}
    </style>
</head>
<body>
    <div class="container">
        <h1>{title}</h1>
        <p class="timestamp">생성 시간: {format_timestamp(datetime.now())}</p>

        <div class="section">
            <h2>내용</h2>
            <pre>{data.get("content", "데이터 없음")}</pre>
        </div>
    </div>
</body>
</html>
"""
        return html

    def _generate_metrics_table(self, metrics: Dict[str, Any]) -> str:
        """메트릭 테이블 생성"""
        if not metrics:
            return "<p>메트릭 데이터가 없습니다.</p>"

        rows = []
        for key, value in metrics.items():
            formatted_value = self._format_metric_value(key, value)
            rows.append(f"<tr><td>{key}</td><td>{formatted_value}</td></tr>")

        return f"""
<table class="metrics-table">
    <thead>
        <tr>
            <th>메트릭</th>
            <th>값</th>
        </tr>
    </thead>
    <tbody>
        {''.join(rows)}
    </tbody>
</table>
"""

    def _generate_charts_html(self, charts: List[Dict[str, Any]]) -> str:
        """차트 HTML 생성"""
        if not charts:
            return "<p>차트 데이터가 없습니다.</p>"

        chart_htmls = []
        for chart in charts:
            chart_html = f"""
<div class="chart-container">
    <h3>{chart.get('title', '차트')}</h3>
    <div class="chart-placeholder">
        [차트: {chart.get('chart_type', 'line')}]
    </div>
</div>
"""
            chart_htmls.append(chart_html)

        return ''.join(chart_htmls)

    def _generate_recommendations_html(self, recommendations: List[str]) -> str:
        """권장사항 HTML 생성"""
        if not recommendations:
            return "<p>권장사항이 없습니다.</p>"

        items = [f"<li>{rec}</li>" for rec in recommendations]
        return f"<ul class='recommendations'>{''.join(items)}</ul>"

    def _generate_cluster_info_html(self, cluster_info: Dict[str, Any]) -> str:
        """클러스터 정보 HTML 생성"""
        return f"""
<table class="info-table">
    <tr><td>클러스터 ID</td><td>{cluster_info.get('cluster_id', 'N/A')}</td></tr>
    <tr><td>엔진</td><td>{cluster_info.get('engine', 'N/A')}</td></tr>
    <tr><td>상태</td><td>{cluster_info.get('status', 'N/A')}</td></tr>
</table>
"""

    def _generate_node_metrics_html(self, node_metrics: List[Dict[str, Any]]) -> str:
        """노드 메트릭 HTML 생성"""
        if not node_metrics:
            return "<p>노드 메트릭 데이터가 없습니다.</p>"

        rows = []
        for node in node_metrics:
            rows.append(f"""
<tr>
    <td>{node.get('node_id', 'N/A')}</td>
    <td>{node.get('role', 'N/A')}</td>
    <td>{format_percentage(node.get('cpu', 0))}</td>
    <td>{format_bytes(node.get('memory', 0))}</td>
</tr>
""")

        return f"""
<table class="metrics-table">
    <thead>
        <tr>
            <th>노드 ID</th>
            <th>역할</th>
            <th>CPU</th>
            <th>메모리</th>
        </tr>
    </thead>
    <tbody>
        {''.join(rows)}
    </tbody>
</table>
"""

    def _generate_error_summary_html(self, error_summary: Dict[str, Any]) -> str:
        """에러 요약 HTML 생성"""
        return f"""
<table class="info-table">
    <tr><td>총 에러 수</td><td>{error_summary.get('total_errors', 0)}</td></tr>
    <tr><td>고유 패턴 수</td><td>{error_summary.get('unique_patterns', 0)}</td></tr>
    <tr><td>분석 기간</td><td>{error_summary.get('period', 'N/A')}</td></tr>
</table>
"""

    def _generate_error_patterns_html(self, patterns: List[Dict[str, Any]]) -> str:
        """에러 패턴 HTML 생성"""
        if not patterns:
            return "<p>에러 패턴이 없습니다.</p>"

        rows = []
        for pattern in patterns:
            rows.append(f"""
<tr>
    <td>{pattern.get('pattern', 'N/A')}</td>
    <td>{pattern.get('occurrences', 0)}</td>
    <td>{pattern.get('severity', 'medium')}</td>
</tr>
""")

        return f"""
<table class="metrics-table">
    <thead>
        <tr>
            <th>패턴</th>
            <th>발생 횟수</th>
            <th>심각도</th>
        </tr>
    </thead>
    <tbody>
        {''.join(rows)}
    </tbody>
</table>
"""

    def _create_chart(self, chart_data: Dict[str, Any]) -> str:
        """개별 차트 생성"""
        return f"""
<div class="chart-container">
    <h3>{chart_data.get('title', '차트')}</h3>
    <div class="chart-placeholder">
        [차트 타입: {chart_data.get('chart_type', 'line')}]
    </div>
</div>
"""

    def _format_metric_value(self, metric_name: str, value: Any) -> str:
        """메트릭 값 포맷팅"""
        if isinstance(value, (int, float)):
            if "Percent" in metric_name or "Utilization" in metric_name:
                return format_percentage(value / 100 if value > 1 else value)
            elif "Memory" in metric_name or "Bytes" in metric_name:
                return format_bytes(int(value))
            elif "Latency" in metric_name or "Duration" in metric_name:
                return format_duration(value)
            else:
                return format_number(value)
        return str(value)

    def _get_report_css(self) -> str:
        """보고서 CSS 스타일"""
        return """
        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            margin: 0;
            padding: 20px;
            background-color: #f5f5f5;
        }
        .container {
            max-width: 1200px;
            margin: 0 auto;
            background-color: white;
            padding: 30px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }
        h1 {
            color: #333;
            border-bottom: 3px solid #4CAF50;
            padding-bottom: 10px;
        }
        h2 {
            color: #555;
            margin-top: 30px;
        }
        .timestamp {
            color: #777;
            font-size: 0.9em;
        }
        .section {
            margin: 20px 0;
        }
        table {
            width: 100%;
            border-collapse: collapse;
            margin: 15px 0;
        }
        th, td {
            padding: 12px;
            text-align: left;
            border-bottom: 1px solid #ddd;
        }
        th {
            background-color: #4CAF50;
            color: white;
        }
        tr:hover {
            background-color: #f5f5f5;
        }
        .chart-container {
            margin: 20px 0;
            padding: 15px;
            border: 1px solid #ddd;
            border-radius: 4px;
        }
        .chart-placeholder {
            height: 300px;
            background-color: #f9f9f9;
            display: flex;
            align-items: center;
            justify-content: center;
            color: #999;
        }
        .recommendations {
            list-style-type: none;
            padding: 0;
        }
        .recommendations li {
            padding: 10px;
            margin: 5px 0;
            background-color: #e8f5e9;
            border-left: 4px solid #4CAF50;
            border-radius: 4px;
        }
        """

    # ============================================================
    # Week 3 리팩토링: HTML 유틸리티 메서드 추가
    # 메인 파일에서 이동 (db_assistant_mcp_server.py)
    # ============================================================

    def format_metrics_as_html(self, metrics: dict) -> str:
        """메트릭 딕셔너리를 HTML로 포맷 (메인 파일에서 이동: 5802라인)"""
        # HTML 형태로 포맷
        html = f"""
        <div class="metric-grid">
            <div class="metric-card">
                <div class="metric-title">🖥️ CPU 사용률 (%)</div>
                <div class="metric-value">평균: {metrics.get('cpu_mean', 0):.1f}%</div>
                <div class="metric-unit">최대: {metrics.get('cpu_max', 0):.1f}% | 최소: {metrics.get('cpu_min', 0):.1f}%</div>
            </div>
            <div class="metric-card">
                <div class="metric-title">💾 메모리 사용률 (%)</div>
                <div class="metric-value">평균: {metrics.get('memory_usage_mean', 0):.1f}%</div>
                <div class="metric-unit">최대: {metrics.get('memory_usage_max', 0):.1f}% | 최소: {metrics.get('memory_usage_min', 0):.1f}%</div>
            </div>
            <div class="metric-card">
                <div class="metric-title">✍️ Write IOPS</div>
                <div class="metric-value">평균: {metrics.get('write_iops_mean', 0):.2f}</div>
                <div class="metric-unit">최대: {metrics.get('write_iops_max', 0):.2f} | 최소: {metrics.get('write_iops_min', 0):.2f}</div>
            </div>
            <div class="metric-card">
                <div class="metric-title">📖 Read IOPS</div>
                <div class="metric-value">평균: {metrics.get('read_iops_mean', 0):.3f}</div>
                <div class="metric-unit">최대: {metrics.get('read_iops_max', 0):.3f} | 최소: {metrics.get('read_iops_min', 0):.3f}</div>
            </div>
            <div class="metric-card">
                <div class="metric-title">🔗 연결 수</div>
                <div class="metric-value">평균: {metrics.get('connections_mean', 0):.1f}개</div>
                <div class="metric-unit">최대: {metrics.get('connections_max', 0):.0f}개 | 최소: {metrics.get('connections_min', 0):.0f}개</div>
            </div>
        </div>
        """

        return html

    def convert_urls_to_html_links(self, text: str) -> str:
        """텍스트 내의 URL을 HTML 링크로 변환하고 파일명을 링크로 만듦 (메인 파일에서 이동: 5837라인)"""
        import re

        # 패턴 1: "파일명.sql\n🔗 다운로드 (7일 유효): URL" 형식을 "파일명.sql (링크)" 형식으로 변환
        # 예: cpu_intensive_queries_gamedb1-1_20251023.sql\n🔗 다운로드 (7일 유효): https://...
        sql_file_pattern = r'([a-zA-Z0-9_\-]+\.sql)\n🔗 다운로드 \(7일 유효\): (https?://[^\s<>"]+)'

        def replace_sql_file(match):
            filename = match.group(1)
            url = match.group(2)
            return f'<a href="{url}" target="_blank" style="color: #007bff; text-decoration: underline; font-weight: bold;">{filename}</a>'

        # SQL 파일 + URL 패턴 먼저 변환
        html_text = re.sub(sql_file_pattern, replace_sql_file, text)

        # 패턴 2: href 속성 안에 있지 않은 일반 URL만 링크로 변환
        # Negative lookbehind를 사용하여 'href="' 뒤에 있는 URL은 제외
        url_pattern = r'(?<!href=")(https?://[^\s<>"]+)(?!")'

        def replace_url(match):
            url = match.group(1)
            return f'<a href="{url}" target="_blank" style="color: #007bff; text-decoration: underline;">{url}</a>'

        # 아직 링크로 변환되지 않은 URL들 변환 (href 안의 URL은 제외)
        html_text = re.sub(url_pattern, replace_url, html_text)

        # 줄바꿈을 <br>로 변환
        html_text = html_text.replace('\n', '<br>')

        return html_text

    def generate_threshold_html(self, thresholds: dict) -> str:
        """임계값 설정을 HTML 테이블로 생성 (메인 파일에서 이동: 7724라인)"""
        html = """
        <div id="thresholdModal" class="modal">
            <div class="modal-content">
                <span class="close">&times;</span>
                <h2>📊 메트릭 임계값 설정</h2>
                <table class="threshold-table">
                    <thead>
                        <tr>
                            <th>메트릭</th>
                            <th>탐지 방식</th>
                            <th>임계값</th>
                            <th>설명</th>
                        </tr>
                    </thead>
                    <tbody>
        """

        for metric, config in thresholds.items():
            method = config.get("method", "iqr")
            threshold_info = []

            if method == "absolute":
                if config.get("high_threshold"):
                    threshold_info.append(f"상한: {config['high_threshold']}")
                if config.get("low_threshold"):
                    threshold_info.append(f"하한: {config['low_threshold']}")
            elif method == "spike":
                threshold_info.append(f"급증 배수: {config.get('spike_factor', 3.0)}")
            elif method == "percentage":
                if config.get("low_threshold"):
                    threshold_info.append(f"최소: {config['low_threshold']}%")

            threshold_str = ", ".join(threshold_info) if threshold_info else "IQR 방식"
            description = config.get("description", f"{metric} 메트릭")

            html += f"""
                        <tr>
                            <td>{metric}</td>
                            <td>{method}</td>
                            <td>{threshold_str}</td>
                            <td>{description}</td>
                        </tr>
            """

        html += """
                    </tbody>
                </table>
                <p><strong>📁 설정 파일:</strong> input/metric_thresholds_*.txt</p>
                <p><strong>💡 수정 방법:</strong> input 폴더의 최신 임계값 파일을 편집하세요.</p>
            </div>
        </div>
        """

        return html

    def save_outlier_html_report(
        self, result: str, threshold_html: str, report_path: Path
    ):
        """아웃라이어 분석 HTML 보고서 저장 (메인 파일에서 이동: 7780라인)"""
        from utils.logging_utils import debug_log

        html_content = f"""
<!DOCTYPE html>
<html lang="ko">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>아웃라이어 분석 보고서</title>
    <style>
        body {{ font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; margin: 20px; background-color: #f5f5f5; }}
        .container {{ max-width: 1200px; margin: 0 auto; background: white; padding: 30px; border-radius: 10px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }}
        .header {{ text-align: center; margin-bottom: 30px; }}
        .btn {{ background-color: #007bff; color: white; padding: 10px 20px; border: none; border-radius: 5px; cursor: pointer; margin: 10px; }}
        .btn:hover {{ background-color: #0056b3; }}
        .result-content {{ white-space: pre-wrap; font-family: monospace; background: #f8f9fa; padding: 20px; border-radius: 5px; }}
        .modal {{ display: none; position: fixed; z-index: 1; left: 0; top: 0; width: 100%; height: 100%; background-color: rgba(0,0,0,0.4); }}
        .modal-content {{ background-color: #fefefe; margin: 5% auto; padding: 20px; border: none; border-radius: 10px; width: 80%; max-width: 800px; }}
        .close {{ color: #aaa; float: right; font-size: 28px; font-weight: bold; cursor: pointer; }}
        .close:hover {{ color: black; }}
        .threshold-table {{ width: 100%; border-collapse: collapse; margin-top: 20px; }}
        .threshold-table th, .threshold-table td {{ border: 1px solid #ddd; padding: 12px; text-align: left; }}
        .threshold-table th {{ background-color: #f2f2f2; font-weight: bold; }}
        .threshold-table tr:nth-child(even) {{ background-color: #f9f9f9; }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🔍 아웃라이어 분석 보고서</h1>
            <p>생성 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
            <button class="btn" onclick="document.getElementById('thresholdModal').style.display='block'">
                📊 임계값 설정 보기
            </button>
        </div>

        <div class="result-content">{result}</div>

        {threshold_html}
    </div>

    <script>
        // 모달 창 제어
        var modal = document.getElementById('thresholdModal');
        var span = document.getElementsByClassName('close')[0];

        span.onclick = function() {{
            modal.style.display = 'none';
        }}

        window.onclick = function(event) {{
            if (event.target == modal) {{
                modal.style.display = 'none';
            }}
        }}
    </script>
</body>
</html>
        """

        try:
            with open(report_path, "w", encoding="utf-8") as f:
                f.write(html_content)
        except Exception as e:
            debug_log(f"HTML 보고서 저장 실패: {e}")
