#!/usr/bin/env python3

import asyncio
import duckdb
from pathlib import Path
from mcp_server.youtube_server import (
    get_video_urls_from_playlist,
    save_video_urls,
    init_database
)
from loguru import logger

# Database path
DB_PATH = Path("youtube_videos.db")

def get_all_collected_videos():
    """데이터베이스에서 모든 수집된 영상 조회"""
    conn = duckdb.connect(str(DB_PATH))
    try:
        result = conn.execute("""
            SELECT url, title, channel_name, source_type, source_url, collected_at
            FROM video_urls
            ORDER BY collected_at DESC
        """).fetchall()
        
        videos = []
        for row in result:
            videos.append({
                'url': row[0],
                'title': row[1],
                'channel_name': row[2],
                'source_type': row[3],
                'source_url': row[4],
                'collected_at': row[5]
            })
        return videos
    finally:
        conn.close()

async def collect_all_pycon_playlists():
    """모든 PyCon KR 플레이리스트에서 영상 수집"""
    
    # PyCon KR 플레이리스트 목록
    playlists = {
        "PyCon.KR 2024": "https://www.youtube.com/playlist?list=PLZPhyNeJvHRldfheI6KcgektEiXAIPJaY",
        "PyCon.KR 2023": "https://www.youtube.com/playlist?list=PLZPhyNeJvHRllQiXsJAryqWmqWrwFxY8I",
        "PyCon.KR 2022": "https://www.youtube.com/playlist?list=PLZPhyNeJvHRnlqQwMj-WNlrsac7yTiVhk",
        "PyCon.KR 2020": "https://www.youtube.com/playlist?list=PLZPhyNeJvHRk9wIL9rZekFLIfT3aVcHT7",
        "PyCon.KR 2019": "https://www.youtube.com/playlist?list=PLZPhyNeJvHRlECdmkJ7M8konKB0NhBfve",
        "PyCon.KR 2018": "https://www.youtube.com/playlist?list=PLZPhyNeJvHRmnMr5yucZ9Eu-yVhjRRsOM",
        "PyCon.KR 2017": "https://www.youtube.com/playlist?list=PLZPhyNeJvHRmvCnWMBZJiFXu9kDUcn5FG",
        "PyCon.KR 2016(APAC)": "https://www.youtube.com/playlist?list=PLZPhyNeJvHRnSJ2sAnqCGFnVRKo98EgCp",
        "PyCon.KR 2015": "https://www.youtube.com/playlist?list=PLZPhyNeJvHRnoO_m1hH78j0JRj8LgUICN",
        "PyCon.KR 2014": "https://www.youtube.com/playlist?list=PLZPhyNeJvHRnchPDpnFV1uUmLhR_JG3A8"
    }
    
    logger.info("🚀 PyCon KR 모든 연도 플레이리스트 수집 시작")
    
    # 데이터베이스 초기화
    init_database()
    
    total_collected = 0
    success_count = 0
    error_count = 0
    
    for year, playlist_url in playlists.items():
        try:
            logger.info(f"📋 {year} 플레이리스트 처리 중...")
            logger.info(f"   URL: {playlist_url}")
            
            # 영상 수집
            collected_videos = get_video_urls_from_playlist(playlist_url)
            
            if collected_videos:
                # 데이터베이스에 저장
                save_video_urls(collected_videos)
                video_count = len(collected_videos)
                total_collected += video_count
                success_count += 1
                
                logger.info(f"✅ {year}: {video_count}개 영상 수집 완료")
                
                # 처음 3개 영상 제목 미리보기
                logger.info(f"   미리보기:")
                for i, video in enumerate(collected_videos[:3], 1):
                    logger.info(f"     {i}. {video['title'][:60]}...")
                if len(collected_videos) > 3:
                    logger.info(f"     ... 및 {len(collected_videos) - 3}개 더")
            else:
                logger.warning(f"⚠️ {year}: 수집된 영상이 없습니다")
                
        except Exception as e:
            error_count += 1
            logger.error(f"❌ {year} 처리 실패: {str(e)}")
            continue
    
    # 최종 요약
    logger.info(f"🎉 전체 수집 완료!")
    logger.info(f"   ✅ 성공한 플레이리스트: {success_count}개")
    logger.info(f"   ❌ 실패한 플레이리스트: {error_count}개")
    logger.info(f"   📊 총 수집된 영상: {total_collected}개")
    
    # 데이터베이스 전체 통계
    show_final_database_stats()

def show_final_database_stats():
    """최종 데이터베이스 통계 표시"""
    try:
        all_videos = get_all_collected_videos()
        
        logger.info(f"📈 데이터베이스 최종 통계:")
        logger.info(f"   📺 총 저장된 영상: {len(all_videos)}개")
        
        # 연도별 통계
        year_stats = {}
        source_stats = {}
        channel_stats = {}
        
        for video in all_videos:
            # 소스 URL에서 연도 추출 시도
            source_url = video.get('source_url', '')
            title = video.get('title', '')
            
            # 제목이나 URL에서 연도 찾기
            for year in ['2024', '2023', '2022', '2021', '2020', '2019', '2018', '2017', '2016', '2015', '2014']:
                if year in source_url or year in title:
                    year_key = f"PyCon KR {year}"
                    year_stats[year_key] = year_stats.get(year_key, 0) + 1
                    break
            
            # 소스 타입별 통계
            source_type = video.get('source_type', 'unknown')
            source_stats[source_type] = source_stats.get(source_type, 0) + 1
            
            # 채널별 통계
            channel = video.get('channel_name', 'Unknown')
            channel_stats[channel] = channel_stats.get(channel, 0) + 1
        
        if year_stats:
            logger.info(f"   🎯 연도별 분포:")
            for year, count in sorted(year_stats.items(), key=lambda x: x[0], reverse=True):
                logger.info(f"     - {year}: {count}개")
        
        if source_stats:
            logger.info(f"   📋 소스 타입별:")
            for source_type, count in source_stats.items():
                logger.info(f"     - {source_type}: {count}개")
        
        if channel_stats:
            logger.info(f"   📺 주요 채널:")
            sorted_channels = sorted(channel_stats.items(), key=lambda x: x[1], reverse=True)
            for channel, count in sorted_channels[:5]:  # 상위 5개 채널만 표시
                logger.info(f"     - {channel}: {count}개")
        
    except Exception as e:
        logger.error(f"통계 생성 오류: {e}")

if __name__ == "__main__":
    asyncio.run(collect_all_pycon_playlists())
    """모든 PyCon KR 플레이리스트에서 영상 수집"""
    
    # PyCon KR 플레이리스트 목록
    playlists = {
        "PyCon.KR 2024": "https://www.youtube.com/playlist?list=PLZPhyNeJvHRldfheI6KcgektEiXAIPJaY",
        "PyCon.KR 2023": "https://www.youtube.com/playlist?list=PLZPhyNeJvHRllQiXsJAryqWmqWrwFxY8I",
        "PyCon.KR 2022": "https://www.youtube.com/playlist?list=PLZPhyNeJvHRnlqQwMj-WNlrsac7yTiVhk",
        "PyCon.KR 2020": "https://www.youtube.com/playlist?list=PLZPhyNeJvHRk9wIL9rZekFLIfT3aVcHT7",
        "PyCon.KR 2019": "https://www.youtube.com/playlist?list=PLZPhyNeJvHRlECdmkJ7M8konKB0NhBfve",
        "PyCon.KR 2018": "https://www.youtube.com/playlist?list=PLZPhyNeJvHRmnMr5yucZ9Eu-yVhjRRsOM",
        "PyCon.KR 2017": "https://www.youtube.com/playlist?list=PLZPhyNeJvHRmvCnWMBZJiFXu9kDUcn5FG",
        "PyCon.KR 2016(APAC)": "https://www.youtube.com/watch?v=UWDRX4z4-k0&list=PLZPhyNeJvHRnSJ2sAnqCGFnVRKo98EgCp",
        "PyCon.KR 2015": "https://www.youtube.com/watch?v=0abmVNlkxRo&list=PLZPhyNeJvHRnoO_m1hH78j0JRj8LgUICN",
        "PyCon.KR 2014": "https://www.youtube.com/watch?v=krK5Ei6kFoc&list=PLZPhyNeJvHRnchPDpnFV1uUmLhR_JG3A8"
    }
    
    logger.info("🚀 PyCon KR 모든 연도 플레이리스트 수집 시작")
    
    # 데이터베이스 초기화
    init_database()
    
    total_collected = 0
    success_count = 0
    error_count = 0
    
    for year, playlist_url in playlists.items():
        try:
            logger.info(f"📋 {year} 플레이리스트 처리 중...")
            logger.info(f"   URL: {playlist_url}")
            
            # 영상 수집
            collected_videos = get_video_urls_from_playlist(playlist_url)
            video_count = len(collected_videos)
            
            total_collected += video_count
            success_count += 1
            
            logger.info(f"✅ {year}: {video_count}개 영상 수집 완료")
            
            # 처음 3개 영상 제목 미리보기
            if collected_videos:
                logger.info(f"   미리보기:")
                for i, video in enumerate(collected_videos[:3], 1):
                    logger.info(f"     {i}. {video['title'][:60]}...")
                if len(collected_videos) > 3:
                    logger.info(f"     ... 및 {len(collected_videos) - 3}개 더")
            
        except Exception as e:
            error_count += 1
            logger.error(f"❌ {year} 처리 실패: {str(e)}")
            continue
    
    # 최종 요약
    logger.info(f"🎉 전체 수집 완료!")
    logger.info(f"   ✅ 성공한 플레이리스트: {success_count}개")
    logger.info(f"   ❌ 실패한 플레이리스트: {error_count}개")
    logger.info(f"   📊 총 수집된 영상: {total_collected}개")
    
    # 데이터베이스 전체 통계
    show_final_database_stats()

def show_final_database_stats():
    """최종 데이터베이스 통계 표시"""
    try:
        all_videos = get_all_collected_videos()
        
        logger.info(f"📈 데이터베이스 최종 통계:")
        logger.info(f"   📺 총 저장된 영상: {len(all_videos)}개")
        
        # 연도별 통계
        year_stats = {}
        source_stats = {}
        channel_stats = {}
        
        for video in all_videos:
            # 소스 URL에서 연도 추출 시도
            source_url = video.get('source_url', '')
            if 'pycon' in source_url.lower():
                for year in ['2024', '2023', '2022', '2021', '2020', '2019', '2018', '2017', '2016', '2015', '2014']:
                    if year in source_url:
                        year_key = f"PyCon KR {year}"
                        year_stats[year_key] = year_stats.get(year_key, 0) + 1
                        break
            
            # 소스 타입별 통계
            source_type = video.get('source_type', 'unknown')
            source_stats[source_type] = source_stats.get(source_type, 0) + 1
            
            # 채널별 통계
            channel = video.get('channel_name', 'Unknown')
            channel_stats[channel] = channel_stats.get(channel, 0) + 1
        
        if year_stats:
            logger.info(f"   🎯 연도별 분포:")
            for year, count in sorted(year_stats.items(), key=lambda x: x[0], reverse=True):
                logger.info(f"     - {year}: {count}개")
        
        if source_stats:
            logger.info(f"   📋 소스 타입별:")
            for source_type, count in source_stats.items():
                logger.info(f"     - {source_type}: {count}개")
        
        if channel_stats:
            logger.info(f"   📺 주요 채널:")
            sorted_channels = sorted(channel_stats.items(), key=lambda x: x[1], reverse=True)
            for channel, count in sorted_channels[:5]:  # 상위 5개 채널만 표시
                logger.info(f"     - {channel}: {count}개")
        
    except Exception as e:
        logger.error(f"통계 생성 오류: {e}")

if __name__ == "__main__":
    asyncio.run(collect_all_pycon_playlists())
