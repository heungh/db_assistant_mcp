# 벡터 저장소 완전 기능 다이어그램

## 전체 시스템 아키텍처

```mermaid
graph TB
    subgraph "사용자 인터페이스"
        U1[save_to_vector_store]
        U2[update_vector_content]
        U3[query_vector_store]
        U4[sync_knowledge_base]
    end
    
    subgraph "내부 검증 함수들"
        I1[_check_content_similarity]
        I2[_search_similar_content]
        I3[_analyze_content_conflicts]
        I4[_extract_keywords]
    end
    
    subgraph "저장소 계층"
        S1[로컬 파일 시스템<br/>vector/]
        S2[AWS S3<br/>bedrockagent-hhs/]
        S3[Knowledge Base<br/>Bedrock]
    end
    
    subgraph "AI 서비스"
        A1[Claude Sonnet 4<br/>충돌 분석]
        A2[Bedrock Runtime<br/>검색 서비스]
    end
    
    U1 --> I1
    U1 --> I2
    U1 --> I3
    U1 --> I4
    U2 --> S1
    U3 --> A2
    U4 --> S3
    
    I1 --> S1
    I2 --> S1
    I3 --> A1
    I4 --> S1
    
    U1 --> S1
    U1 --> S2
    U2 --> S2
    
    S2 --> S3
    
    style U1 fill:#e1f5fe
    style U2 fill:#fff3e0
    style U3 fill:#e8f5e8
    style U4 fill:#f3e5f5
    style A1 fill:#ffebee
    style A2 fill:#ffebee
```

## save_to_vector_store 상세 워크플로우

```mermaid
flowchart TD
    A[save_to_vector_store 호출] --> B{force_save?}
    
    B -->|false| C[중복/충돌 검사 시작]
    B -->|true| P[검사 건너뛰기]
    
    C --> D[_check_content_similarity]
    D --> E[기존 vector/ 파일들 스캔]
    E --> F[텍스트 정규화 및 키워드 추출]
    F --> G{유사도 ≥ 70%?}
    
    G -->|Yes| H[_analyze_content_conflicts]
    G -->|No| O[새 콘텐츠로 판단]
    
    H --> I[Claude AI 충돌 분석 요청]
    I --> J[기존 내용과 비교 분석]
    J --> K{실제 충돌 존재?}
    
    K -->|Yes| L[❌ 저장 거부<br/>충돌 상세 보고]
    K -->|No| O[새 콘텐츠로 판단]
    
    O --> P[_extract_keywords 실행]
    P --> Q[YAML 메타데이터 생성]
    Q --> R[로컬 Markdown 파일 저장]
    R --> S[S3 업로드 실행]
    S --> T[✅ 저장 완료 응답]
    
    style A fill:#e1f5fe
    style L fill:#ffebee
    style T fill:#e8f5e8
```

## update_vector_content 워크플로우

```mermaid
flowchart TD
    A[update_vector_content 호출] --> B[파일명으로 기존 파일 검색]
    B --> C{파일 존재?}
    
    C -->|No| D[❌ 파일 없음 오류]
    C -->|Yes| E{update_mode?}
    
    E -->|append| F[기존 내용 읽기]
    E -->|replace| G[기존 내용 무시]
    
    F --> H[새 내용을 기존 내용에 추가]
    G --> I[새 내용으로 완전 교체]
    
    H --> J[메타데이터 업데이트]
    I --> J
    
    J --> K[last_updated 갱신]
    K --> L[version 증가]
    L --> M[로컬 파일 저장]
    M --> N[S3 업로드]
    N --> O[✅ 업데이트 완료]
    
    style A fill:#fff3e0
    style D fill:#ffebee
    style O fill:#e8f5e8
```

## query_vector_store 워크플로우

```mermaid
flowchart TD
    A[query_vector_store 호출] --> B[검색 쿼리 전처리]
    B --> C[Bedrock Knowledge Base 호출]
    C --> D[bedrock_agent_runtime.retrieve]
    D --> E[벡터 유사도 검색 실행]
    E --> F[관련 문서 청크 반환]
    F --> G[결과 점수별 정렬]
    G --> H[max_results 개수만큼 필터링]
    H --> I[검색 결과 포맷팅]
    I --> J[✅ 검색 결과 반환]
    
    style A fill:#e8f5e8
    style J fill:#e8f5e8
```

## sync_knowledge_base 워크플로우

```mermaid
flowchart TD
    A[sync_knowledge_base 호출] --> B[Knowledge Base ID 확인]
    B --> C[데이터 소스 ID 조회]
    C --> D[start_ingestion_job 실행]
    D --> E[동기화 작업 시작]
    E --> F[작업 ID 생성]
    F --> G[작업 상태 모니터링]
    G --> H{작업 완료?}
    
    H -->|진행중| I[STARTING/IN_PROGRESS]
    H -->|완료| J[COMPLETE]
    H -->|실패| K[FAILED]
    
    I --> L[⏳ 진행 상태 반환]
    J --> M[✅ 동기화 완료]
    K --> N[❌ 동기화 실패]
    
    style A fill:#f3e5f5
    style M fill:#e8f5e8
    style N fill:#ffebee
```

## 내부 함수들 상세 다이어그램

### _check_content_similarity 함수

```mermaid
flowchart TD
    A[_check_content_similarity] --> B[vector/ 디렉토리 스캔]
    B --> C[기존 .md 파일들 읽기]
    C --> D[각 파일별 유사도 계산]
    
    subgraph "유사도 계산 로직"
        E[텍스트 정규화<br/>소문자 변환, 특수문자 제거]
        F[키워드 추출<br/>공백 기준 분할]
        G[교집합 계산<br/>공통 키워드 수]
        H[합집합 계산<br/>전체 키워드 수]
        I[유사도 = 교집합/합집합 × 100]
    end
    
    D --> E
    E --> F
    F --> G
    G --> H
    H --> I
    
    I --> J{최대 유사도 ≥ 70%?}
    J -->|Yes| K[유사 파일 정보 반환]
    J -->|No| L[유사도 낮음 반환]
    
    style A fill:#e3f2fd
    style K fill:#fff3e0
    style L fill:#e8f5e8
```

### _analyze_content_conflicts 함수

```mermaid
sequenceDiagram
    participant Func as _analyze_content_conflicts
    participant Claude as Claude Sonnet 4
    participant KB as Knowledge Base
    
    Func->>Func: 기존 내용과 새 내용 준비
    Func->>Claude: 충돌 분석 요청
    
    Note over Claude: AI 분석 수행
    Claude->>Claude: 내용 비교 및 모순 탐지
    Claude->>Claude: 충돌 심각도 평가
    Claude->>Claude: 해결 방안 제시
    
    Claude-->>Func: 분석 결과 반환
    
    alt 충돌 발견
        Func->>Func: 충돌 상세 정보 생성
        Func-->>Func: False 반환 (저장 거부)
    else 충돌 없음
        Func->>KB: 기존 내용 보완으로 판단
        Func-->>Func: True 반환 (저장 허용)
    end
```

### _extract_keywords 함수

```mermaid
flowchart TD
    A[_extract_keywords] --> B[콘텐츠 텍스트 분석]
    B --> C[불용어 제거]
    C --> D[단어 빈도 계산]
    D --> E[중요도 기반 정렬]
    E --> F[상위 키워드 선별]
    F --> G[기본 태그 추가]
    
    subgraph "기본 태그 로직"
        H[database 관련 키워드 탐지]
        I[performance 관련 키워드 탐지]
        J[optimization 관련 키워드 탐지]
        K[troubleshooting 관련 키워드 탐지]
    end
    
    G --> H
    G --> I
    G --> J
    G --> K
    
    H --> L[최종 태그 배열 생성]
    I --> L
    J --> L
    K --> L
    
    L --> M[키워드 리스트 반환]
    
    style A fill:#e3f2fd
    style M fill:#e8f5e8
```

## 파일 시스템 구조

```mermaid
graph TD
    subgraph "로컬 저장소 (vector/)"
        A[20250914_topic1.md]
        B[20250913_topic2.md]
        C[20250912_topic3.md]
    end
    
    subgraph "S3 버킷 구조"
        D[s3://bedrockagent-hhs/]
        E[database-standards/]
        F[performance-optimization/]
        G[troubleshooting/]
        H[examples/]
    end
    
    subgraph "Knowledge Base"
        I[벡터 인덱스]
        J[문서 청크]
        K[메타데이터]
    end
    
    A --> D
    B --> D
    C --> D
    
    D --> E
    D --> F
    D --> G
    D --> H
    
    E --> I
    F --> I
    G --> I
    H --> I
    
    I --> J
    I --> K
    
    style A fill:#e8f5e8
    style D fill:#e3f2fd
    style I fill:#f3e5f5
```

## 메타데이터 구조

```mermaid
graph LR
    subgraph "YAML 헤더 구조"
        A[title: 문서 제목]
        B[category: 카테고리]
        C[tags: 태그 배열]
        D[version: 버전 번호]
        E[last_updated: 수정 날짜]
        F[author: DB Assistant]
        G[source: conversation]
        H[similarity_check: 유사도 정보]
        I[conflict_analysis: 충돌 분석 결과]
    end
    
    subgraph "카테고리 값"
        J[database-standards]
        K[performance-optimization]
        L[troubleshooting]
        M[examples]
    end
    
    B --> J
    B --> K
    B --> L
    B --> M
    
    style A fill:#fff3e0
    style B fill:#e3f2fd
    style C fill:#e8f5e8
```

## 에러 처리 및 복구 메커니즘

```mermaid
flowchart TD
    A[함수 실행] --> B{에러 발생?}
    
    B -->|No| C[정상 처리]
    B -->|Yes| D{에러 유형 판별}
    
    D -->|파일 I/O 에러| E[로컬 저장소 권한 확인]
    D -->|S3 업로드 에러| F[AWS 자격증명 확인]
    D -->|Claude API 에러| G[Bedrock 서비스 상태 확인]
    D -->|Knowledge Base 에러| H[KB ID 및 권한 확인]
    
    E --> I[권한 수정 후 재시도]
    F --> J[자격증명 갱신 후 재시도]
    G --> K[대체 모델 사용]
    H --> L[수동 동기화 안내]
    
    I --> M{재시도 성공?}
    J --> M
    K --> M
    L --> N[부분 성공 처리]
    
    M -->|Yes| C
    M -->|No| O[❌ 최종 실패]
    
    C --> P[✅ 성공 완료]
    
    style A fill:#e1f5fe
    style O fill:#ffebee
    style P fill:#e8f5e8
```

## 성능 최적화 포인트

```mermaid
graph TD
    subgraph "최적화 전략"
        A[파일 캐싱<br/>중복 읽기 방지]
        B[배치 처리<br/>S3 업로드 최적화]
        C[비동기 처리<br/>Knowledge Base 동기화]
        D[메모리 관리<br/>대용량 파일 처리]
    end
    
    subgraph "성능 지표"
        E[응답 시간 < 5초]
        F[메모리 사용량 < 100MB]
        G[동시 처리 지원]
        H[에러 복구율 > 95%]
    end
    
    A --> E
    B --> F
    C --> G
    D --> H
    
    style A fill:#e8f5e8
    style B fill:#e3f2fd
    style C fill:#fff3e0
    style D fill:#f3e5f5
```
