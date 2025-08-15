#!/usr/bin/env python3

import asyncio
import json
from typing import Any, Dict, List
from urllib.parse import urlparse, parse_qs
import duckdb
from pathlib import Path

from loguru import logger
from mcp.server import Server
from mcp.server.models import InitializationOptions
from mcp.server.stdio import stdio_server
from mcp.types import (
    Resource,
    Tool,
    TextContent,
)
from pytube import Channel, Playlist
from pytube.exceptions import VideoUnavailable, PytubeError

# Initialize logger
logger.add("youtube_mcp.log", rotation="10 MB", level="INFO")

# Database setup
DB_PATH = Path("youtube_videos.db")

# 임시 NotificationOptions 클래스 정의
class NotificationOptions:
    prompts_changed = None
    resources_changed = None
    tools_changed = None

def init_database():
    """Initialize DuckDB database with video URLs table"""
    conn = duckdb.connect(str(DB_PATH))
    conn.execute("""
        CREATE SEQUENCE IF NOT EXISTS video_id_seq START 1;
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS video_urls (
            id INTEGER PRIMARY KEY DEFAULT nextval('video_id_seq'),
            url TEXT UNIQUE,
            title TEXT,
            channel_name TEXT,
            source_type TEXT,  -- 'channel' or 'playlist'
            source_url TEXT,
            collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.close()
    logger.info("Database initialized")

def save_video_urls(video_data: List[Dict[str, Any]]):
    """Save video URLs to DuckDB"""
    if not video_data:
        return
    
    conn = duckdb.connect(str(DB_PATH))
    try:
        for data in video_data:
            conn.execute("""
                INSERT OR IGNORE INTO video_urls 
                (url, title, channel_name, source_type, source_url)
                VALUES (?, ?, ?, ?, ?)
            """, (
                data['url'],
                data.get('title', ''),
                data.get('channel_name', ''),
                data.get('source_type', ''),
                data.get('source_url', '')
            ))
        logger.info(f"Saved {len(video_data)} video URLs to database")
    except Exception as e:
        logger.error(f"Error saving to database: {e}")
        raise
    finally:
        conn.close()

def get_video_urls_from_channel(channel_url: str) -> List[Dict[str, Any]]:
    """Extract video URLs from a YouTube channel"""
    try:
        logger.info(f"Fetching videos from channel: {channel_url}")
        channel = Channel(channel_url)
        
        video_data = []
        for video_url in channel.video_urls:
            video_data.append({
                'url': video_url,
                'title': '',  # Not collecting detailed info as requested
                'channel_name': channel.channel_name,
                'source_type': 'channel',
                'source_url': channel_url
            })
        
        logger.info(f"Found {len(video_data)} videos in channel")
        return video_data
        
    except Exception as e:
        logger.error(f"Error fetching channel videos: {e}")
        raise RuntimeError(f"Failed to fetch channel videos: {str(e)}")

def get_video_urls_from_playlist(playlist_url: str) -> List[Dict[str, Any]]:
    """Extract video URLs from a YouTube playlist"""
    try:
        logger.info(f"Fetching videos from playlist: {playlist_url}")
        playlist = Playlist(playlist_url)
        
        video_data = []
        for video_url in playlist.video_urls:
            video_data.append({
                'url': video_url,
                'title': '',  # Not collecting detailed info as requested
                'channel_name': '',
                'source_type': 'playlist',
                'source_url': playlist_url
            })
        
        logger.info(f"Found {len(video_data)} videos in playlist")
        return video_data
        
    except Exception as e:
        logger.error(f"Error fetching playlist videos: {e}")
        raise RuntimeError(f"Failed to fetch playlist videos: {str(e)}")

def identify_youtube_url_type(url: str) -> str:
    """Identify if URL is a channel or playlist"""
    parsed = urlparse(url)
    
    if 'playlist' in parsed.query or '/playlist' in parsed.path:
        return 'playlist'
    elif '/channel/' in parsed.path or '/c/' in parsed.path or '/@' in parsed.path:
        return 'channel'
    else:
        # Try to determine from URL structure
        if '/user/' in parsed.path:
            return 'channel'
        return 'unknown'

# Initialize database on startup
init_database()

# Create MCP server
server = Server("youtube-mcp-server")

@server.list_tools()
async def handle_list_tools() -> List[Tool]:
    """List available tools"""
    return [
        Tool(
            name="collect_channel_videos",
            description="Collect video URLs from a YouTube channel",
            inputSchema={
                "type": "object",
                "properties": {
                    "channel_url": {
                        "type": "string",
                        "description": "YouTube channel URL"
                    }
                },
                "required": ["channel_url"]
            }
        ),
        Tool(
            name="collect_playlist_videos",
            description="Collect video URLs from a YouTube playlist",
            inputSchema={
                "type": "object",
                "properties": {
                    "playlist_url": {
                        "type": "string",
                        "description": "YouTube playlist URL"
                    }
                },
                "required": ["playlist_url"]
            }
        ),
        Tool(
            name="auto_collect_videos",
            description="Automatically detect URL type and collect video URLs",
            inputSchema={
                "type": "object",
                "properties": {
                    "url": {
                        "type": "string",
                        "description": "YouTube channel or playlist URL"
                    }
                },
                "required": ["url"]
            }
        ),
        Tool(
            name="get_collected_videos",
            description="Get all collected video URLs from database",
            inputSchema={
                "type": "object",
                "properties": {
                    "limit": {
                        "type": "integer",
                        "description": "Maximum number of results to return",
                        "default": 100
                    }
                }
            }
        )
    ]

@server.call_tool()
async def handle_call_tool(name: str, arguments: Dict[str, Any]) -> List[TextContent]:
    """Handle tool calls"""
    
    if name == "collect_channel_videos":
        channel_url = arguments["channel_url"]
        try:
            video_data = get_video_urls_from_channel(channel_url)
            save_video_urls(video_data)
            
            return [TextContent(
                type="text",
                text=f"Successfully collected {len(video_data)} video URLs from channel and saved to database."
            )]
        except Exception as e:
            logger.error(f"Error in collect_channel_videos: {e}")
            raise RuntimeError(str(e))
    
    elif name == "collect_playlist_videos":
        playlist_url = arguments["playlist_url"]
        try:
            video_data = get_video_urls_from_playlist(playlist_url)
            save_video_urls(video_data)
            
            return [TextContent(
                type="text",
                text=f"Successfully collected {len(video_data)} video URLs from playlist and saved to database."
            )]
        except Exception as e:
            logger.error(f"Error in collect_playlist_videos: {e}")
            raise RuntimeError(str(e))
    
    elif name == "auto_collect_videos":
        url = arguments["url"]
        try:
            url_type = identify_youtube_url_type(url)
            
            if url_type == "channel":
                video_data = get_video_urls_from_channel(url)
            elif url_type == "playlist":
                video_data = get_video_urls_from_playlist(url)
            else:
                raise ValueError("Could not identify URL type. Please use a valid YouTube channel or playlist URL.")
            
            save_video_urls(video_data)
            
            return [TextContent(
                type="text",
                text=f"Successfully collected {len(video_data)} video URLs from {url_type} and saved to database."
            )]
        except Exception as e:
            logger.error(f"Error in auto_collect_videos: {e}")
            raise RuntimeError(str(e))
    
    elif name == "get_collected_videos":
        limit = arguments.get("limit", 100)
        try:
            conn = duckdb.connect(str(DB_PATH))
            result = conn.execute("""
                SELECT url, title, channel_name, source_type, source_url, collected_at
                FROM video_urls
                ORDER BY collected_at DESC
                LIMIT ?
            """, (limit,)).fetchall()
            conn.close()
            
            videos_text = "\n".join([
                f"URL: {row[0]}\nChannel: {row[2]}\nSource: {row[3]} ({row[4]})\nCollected: {row[5]}\n---"
                for row in result
            ])
            
            return [TextContent(
                type="text",
                text=f"Found {len(result)} collected video URLs:\n\n{videos_text}"
            )]
        except Exception as e:
            logger.error(f"Error in get_collected_videos: {e}")
            raise RuntimeError(str(e))
    
    else:
        raise ValueError(f"Unknown tool: {name}")

async def main():
    """Main entry point"""
    logger.info("Starting YouTube MCP Server")
    
    # Use stdio transport
    async with stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream,
            write_stream,
            InitializationOptions(
                server_name="youtube-mcp-server",
                server_version="1.0.0",
                capabilities=server.get_capabilities(
                    notification_options=NotificationOptions(),
                    experimental_capabilities={},
                ),
            ),
        )

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 2 and sys.argv[1] == "collect":
        # 명령줄에서 직접 수집 실행
        url = sys.argv[2]
        init_database()
        
        try:
            url_type = identify_youtube_url_type(url)
            if url_type == "playlist":
                print(f"플레이리스트에서 비디오 수집 중: {url}")
                video_data = get_video_urls_from_playlist(url)
                save_video_urls(video_data)
                print(f"수집 완료: {len(video_data)}개 비디오")
            elif url_type == "channel":
                print(f"채널에서 비디오 수집 중: {url}")
                video_data = get_video_urls_from_channel(url)
                save_video_urls(video_data)
                print(f"수집 완료: {len(video_data)}개 비디오")
            else:
                print(f"지원하지 않는 URL 타입: {url_type}")
        except Exception as e:
            print(f"수집 중 오류 발생: {e}")
    else:
        asyncio.run(main())