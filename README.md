# YouTube MCP 서버 컬렉션

![Python](https://img.shields.io/badge/Python-3.13-blue)
![MCP](https://img.shields.io/badge/MCP-1.12.4-green)
![DuckDB](https://img.shields.io/badge/DuckDB-1.3.2-orange)
![License](https://img.shields.io/badge/License-MIT-yellow)

YouTube 동영상 URL 수집 및 상세 정보 추출을 위한 Model Context Protocol(MCP) 서버 모음입니다.

## 📚 목차
- [🚀 프로젝트 구성](#-프로젝트-구성)
- [📋 주요 기능](#-주요-기능)
- [🛠️ 설치 및 설정](#️-설치-및-설정)
- [⚠️ 문제 해결](#️-문제-해결)
- [📊 데이터 스키마](#-데이터-스키마)
- [🎯 활용 예시](#-활용-예시)
- [🏗️ 프로젝트 구조](#️-프로젝트-구조)
- [🔧 기술 스택](#-기술-스택)

## 🚀 프로젝트 구성

### 1. YouTube URL 수집 서버 (`youtube-mcp`)
- **PyTube** 기반 YouTube 동영상 URL 수집
- 채널/재생목록 자동 감지
- 빠른 URL 추출 (1,583 videos/second)

### 2. YouTube 상세 정보 서버 (`youtube-detail-mcp`)  
- **yt-dlp** 기반 상세 메타데이터 추출
- 컨퍼런스 정보 자동 인식 (PyCon KR, DjangoCon 등)
- 조회수, 재생시간, 설명 등 풍부한 정보

### 3. DuckDB 데이터베이스
- 고성능 로컬 데이터베이스
- `video_urls` 테이블: 기본 URL 정보
- `video_details` 테이블: 상세 메타데이터

## 📋 주요 기능

### YouTube URL 수집 도구
- `collect_channel_videos` - YouTube 채널에서 동영상 URL 수집
- `collect_playlist_videos` - YouTube 재생목록에서 동영상 URL 수집
- `auto_collect_videos` - URL 유형 자동 감지 및 수집
- `get_collected_videos` - 수집된 동영상 URL 조회

### YouTube 상세 정보 도구
- `extract_video_details` - 단일 동영상 상세 정보 추출
- `batch_extract_details` - 여러 동영상 일괄 처리
- `process_unprocessed_videos` - 미처리 동영상 자동 처리
- `get_video_details` - 저장된 상세 정보 조회
- `get_conference_statistics` - 컨퍼런스 통계 분석

## 🛠️ 설치 및 설정

### 1. 저장소 클론
```bash
git clone <repository-url>
cd pyconmcp
```

### 2. 의존성 설치
```bash
# UV가 없는 경우 먼저 설치
curl -LsSf https://astral.sh/uv/install.sh | sh

# 프로젝트 의존성 설치
uv sync
```

### 3. VS Code MCP 설정 (필수)
MCP 서버를 VS Code에서 사용하려면 설정 파일을 생성하고 경로를 수정해야 합니다:

```bash
# 예시 설정 파일을 복사
cp .vscode/mcp.json.example .vscode/mcp.json
```

**중요**: `.vscode/mcp.json` 파일에서 `/path/to/your/pyconmcp`를 실제 프로젝트 경로로 변경하세요.

#### macOS/Linux 사용자
```bash
# 현재 디렉토리 경로 확인
pwd
# 예: /Users/username/pyconmcp

# mcp.json에서 경로 변경 (예시)
# "/path/to/your/pyconmcp" → "/Users/username/pyconmcp"
```

#### Windows 사용자
```cmd
# 현재 디렉토리 경로 확인
cd
# 예: C:\Users\username\pyconmcp

# mcp.json에서 경로 변경 (예시)
# "/path/to/your/pyconmcp" → "C:\\Users\\username\\pyconmcp"
```

### 4. MCP 설정 파일 상세 설명

`.vscode/mcp.json` 파일에는 3개의 MCP 서버가 정의되어 있습니다:

#### 📝 수정해야 하는 부분
```json
{
  "mcpServers": {
    "youtube-mcp": {
      "command": "uv",
      "args": [
        "run",
        "python",
        "/path/to/your/pyconmcp/mcp_server/youtube_server.py"  // 👈 이 경로 수정
      ]
    },
    "youtube-detail-mcp": {
      "command": "uv",
      "args": [
        "run", 
        "python",
        "/path/to/your/pyconmcp/mcp_server/youtube_detail_server.py"  // 👈 이 경로 수정
      ]
    },
    "duckdb": {
      "command": "uvx",
      "args": [
        "mcp-server-duckdb",
        "/path/to/your/pyconmcp/youtube_videos.db"  // 👈 이 경로 수정
      ]
    }
  }
}
```

### 5. VS Code 재시작
설정 완료 후 VS Code를 재시작하여 MCP 서버를 로드하세요.

### 6. MCP 서버 동작 확인

VS Code에서 GitHub Copilot Chat을 열고 다음 명령어로 MCP 서버가 정상 작동하는지 확인하세요:

```
@workspace MCP 서버 상태 확인
```

또는 각 서버의 도구를 직접 테스트:

```
# YouTube URL 수집 서버 테스트
수집된 동영상 URL을 조회해줘

# DuckDB 서버 테스트  
video_urls 테이블의 데이터를 보여줘
```

### 7. 사용법 및 예제

#### 기본 사용법
GitHub Copilot Chat에서 자연어로 요청하면 자동으로 적절한 MCP 도구가 호출됩니다:

```
# URL 수집
"PyCon KR 2024 재생목록에서 동영상 URL을 수집해줘"
"https://www.youtube.com/@PyConKorea 채널의 영상을 수집해줘"

# 상세 정보 추출
"수집된 영상들의 상세 정보를 추출해줘"
"특정 영상의 조회수와 재생시간을 알려줘"

# 데이터 분석
"채널별 영상 수를 집계해줘"
"가장 인기 있는 영상 10개를 보여줘"
```

#### 고급 사용법
```
# 배치 처리
"미처리된 모든 영상의 상세 정보를 일괄 추출해줘"

# 통계 분석
"컨퍼런스별 영상 통계를 보여줘"
"총 재생시간과 평균 조회수를 계산해줘"

# 필터링 검색
"제목에 'FastAPI'가 포함된 영상을 찾아줘"
"2024년 PyCon 영상만 필터링해줘"
```

## ⚠️ 문제 해결

### 1. MCP 서버가 로드되지 않는 경우
- `.vscode/mcp.json` 파일의 경로가 정확한지 확인
- VS Code를 완전히 재시작
- 터미널에서 `uv sync` 재실행

### 2. 권한 오류가 발생하는 경우
```bash
# macOS/Linux에서 실행 권한 부여
chmod +x mcp_server/*.py

# Windows에서는 관리자 권한으로 실행
```

### 3. 의존성 설치 오류
```bash
# Python 버전 확인 (3.11+ 필요)
python --version

# UV 재설치
curl -LsSf https://astral.sh/uv/install.sh | sh

# 캐시 초기화 후 재설치
uv cache clean
uv sync
```

## 📊 데이터 스키마

### video_urls 테이블
- `id`: 고유 ID
- `url`: YouTube 동영상 URL
- `title`: 동영상 제목
- `channel_name`: 채널명
- `source_type`: 수집 소스 (channel/playlist)
- `source_url`: 원본 URL
- `collected_at`: 수집 시간

### video_details 테이블
- `id`: 고유 ID
- `video_url`: YouTube 동영상 URL
- `title`, `description`: 제목, 설명
- `view_count`, `like_count`: 조회수, 좋아요 수
- `duration`: 재생시간 (초)
- `conference_name`, `conference_year`: 컨퍼런스 정보
- `tags`: 태그 (JSON 형태)
- `thumbnail_url`: 썸네일 URL

## 🎯 활용 예시

### 빠른 시작 가이드
1. **저장소 클론 및 설정**
   ```bash
   git clone <repository-url>
   cd pyconmcp
   uv sync
   cp .vscode/mcp.json.example .vscode/mcp.json
   # mcp.json에서 경로 수정 후 VS Code 재시작
   ```

2. **첫 번째 데이터 수집**
   ```
   "PyCon KR 2024 재생목록을 수집해줘: https://www.youtube.com/playlist?list=PLZPhyNeJvHRmKUdJP8F8plZXrrhZFANlI"
   ```

3. **수집된 데이터 확인**
   ```
   "수집된 영상 목록을 보여줘"
   ```

### PyCon KR 2024 영상 분석 결과
- **총 영상 수**: 38개
- **총 재생시간**: 18.1시간
- **평균 조회수**: 262회
- **성공률**: 92.7%

### 인기 영상 Top 3
1. 파이콘 한국 2024 무엇이든 물어보세요! (987 views)
2. 인공지능과 파이썬으로 금융 데이터 분석 (938 views)  
3. FastAPI with Dependency Injector (768 views)

## 🏗️ 프로젝트 구조

```
pyconmcp/
├── mcp_server/
│   ├── youtube_server.py          # URL 수집 서버
│   └── youtube_detail_server.py   # 상세 정보 서버
├── .vscode/
│   ├── mcp.json.example          # MCP 서버 설정 예시
│   └── mcp.json                  # MCP 서버 설정 (사용자별)
├── youtube_videos.db             # DuckDB 데이터베이스 (로컬)
├── pyproject.toml                # 프로젝트 설정
└── README.md                     # 문서

```

## 🔧 기술 스택

- **Python 3.13** - 런타임 환경
- **UV** - 패키지 관리자
- **PyTube 15.0.0** - YouTube URL 추출
- **yt-dlp 2025.8.11** - 상세 메타데이터 추출
- **DuckDB 1.3.2** - 고성능 로컬 데이터베이스
- **Loguru 0.7.0** - 구조화된 로깅
- **MCP 1.12.4** - Model Context Protocol

## 📄 라이선스

이 프로젝트는 MIT 라이선스 하에 배포됩니다.

## 데이터베이스 스키마

DuckDB 데이터베이스는 다음과 같은 스키마로 동영상 URL을 저장합니다:

```sql
CREATE TABLE video_urls (
    id INTEGER PRIMARY KEY,
    url TEXT UNIQUE,
    title TEXT,
    channel_name TEXT,
    source_type TEXT,  -- 'channel' 또는 'playlist'
    source_url TEXT,
    collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
```

## 의존성

- **pytube**: YouTube 동영상 추출
- **duckdb**: 로컬 데이터베이스 저장
- **loguru**: 로깅
- **mcp**: Model Context Protocol 서버 프레임워크

