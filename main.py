#!/usr/bin/env python3
"""
YouTube MCP Server Collection

Model Context Protocol 서버들을 관리하고 데이터베이스 상태를 확인하는 유틸리티입니다.
"""

import duckdb
from pathlib import Path

def show_database_stats():
    """데이터베이스 통계 정보 출력"""
    db_path = Path("youtube_videos.db")
    
    if not db_path.exists():
        print("❌ 데이터베이스 파일이 존재하지 않습니다.")
        return
    
    conn = duckdb.connect(str(db_path))
    
    try:
        print("📊 YouTube MCP 데이터베이스 통계")
        print("=" * 40)
        
        # 기본 URL 통계
        url_count = conn.execute("SELECT COUNT(*) FROM video_urls").fetchone()[0]
        print(f"🔗 수집된 URL 수: {url_count}개")
        
        # 상세 정보 통계
        detail_count = conn.execute("SELECT COUNT(*) FROM video_details").fetchone()[0]
        print(f"📹 상세 정보 보유: {detail_count}개")
        
        if detail_count > 0:
            # 컨퍼런스별 통계
            conf_stats = conn.execute("""
                SELECT conference_name, conference_year, COUNT(*) as count
                FROM video_details 
                WHERE conference_name IS NOT NULL
                GROUP BY conference_name, conference_year
                ORDER BY conference_year DESC, count DESC
            """).fetchall()
            
            print(f"🎯 컨퍼런스별 분포:")
            for conf_name, conf_year, count in conf_stats:
                print(f"   - {conf_name} {conf_year}: {count}개")
            
            # 총 통계
            total_stats = conn.execute("""
                SELECT 
                    SUM(view_count) as total_views,
                    AVG(view_count) as avg_views,
                    SUM(duration) as total_duration
                FROM video_details
            """).fetchone()
            
            total_views, avg_views, total_duration = total_stats
            print(f"👁️ 총 조회수: {total_views:,}")
            print(f"📊 평균 조회수: {avg_views:.0f}")
            print(f"⏱️ 총 재생시간: {total_duration/3600:.1f}시간")
        
    except Exception as e:
        print(f"❌ 데이터베이스 조회 오류: {e}")
    finally:
        conn.close()

def main():
    """메인 함수"""
    print("🚀 YouTube MCP Server Collection")
    print("=" * 40)
    print()
    print("📋 사용 가능한 MCP 서버:")
    print("  - youtube-mcp: YouTube URL 수집")
    print("  - youtube-detail-mcp: 상세 정보 추출")
    print("  - duckdb: 데이터베이스 분석")
    print()
    
    show_database_stats()
    
    print()
    print("💡 VS Code에서 GitHub Copilot Chat을 사용하여")
    print("   MCP 도구들을 활용해보세요!")

if __name__ == "__main__":
    main()
