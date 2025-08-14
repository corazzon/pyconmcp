#!/usr/bin/env python3

import asyncio
import json
import re
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse
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
import yt_dlp

# Initialize logger
logger.add("youtube_detail_mcp.log", rotation="10 MB", level="INFO")

# Database setup
DB_PATH = Path("youtube_videos.db")

def init_video_details_table():
    """Initialize video details table in DuckDB"""
    conn = duckdb.connect(str(DB_PATH))
    try:
        # Drop table if exists and recreate with proper sequence
        conn.execute("DROP TABLE IF EXISTS video_details")
        conn.execute("DROP SEQUENCE IF EXISTS video_details_id_seq")
        
        # Create sequence for auto-increment
        conn.execute("CREATE SEQUENCE video_details_id_seq")
        
        # Create table with proper auto-increment
        conn.execute("""
            CREATE TABLE video_details (
                id INTEGER DEFAULT nextval('video_details_id_seq') PRIMARY KEY,
                video_url TEXT UNIQUE,
                video_id TEXT,
                title TEXT,
                description TEXT,
                channel_name TEXT,
                upload_date TEXT,
                duration INTEGER,
                view_count INTEGER,
                like_count INTEGER,
                comment_count INTEGER,
                conference_name TEXT,
                conference_year INTEGER,
                tags TEXT,
                thumbnail_url TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        logger.info("Video details table initialized")
    except Exception as e:
        logger.error(f"Error initializing video details table: {e}")
        raise
    finally:
        conn.close()

def extract_conference_info(title: str, description: str, channel_name: str) -> tuple[Optional[str], Optional[int]]:
    """Extract conference name and year from video metadata"""
    conference_name = None
    conference_year = None
    
    # Combine all text for analysis
    text_to_analyze = f"{title} {description} {channel_name}".lower()
    
    # Conference name patterns
    conference_patterns = [
        r'pycon\s*kr\s*(\d{4})?',
        r'pycon\s*korea\s*(\d{4})?',
        r'python\s*conference\s*(\d{4})?',
        r'파이콘\s*(\d{4})?',
        r'djangocon\s*(\d{4})?',
        r'europython\s*(\d{4})?',
        r'pycascades\s*(\d{4})?',
        r'scipy\s*(\d{4})?',
        r'jupyter\s*con\s*(\d{4})?',
    ]
    
    # Extract conference name and year
    for pattern in conference_patterns:
        match = re.search(pattern, text_to_analyze)
        if match:
            if 'pycon' in pattern:
                if 'kr' in pattern or 'korea' in pattern or '파이콘' in pattern:
                    conference_name = "PyCon KR"
                else:
                    conference_name = "PyCon"
            elif 'django' in pattern:
                conference_name = "DjangoCon"
            elif 'europython' in pattern:
                conference_name = "EuroPython"
            elif 'pycascades' in pattern:
                conference_name = "PyCascades"
            elif 'scipy' in pattern:
                conference_name = "SciPy"
            elif 'jupyter' in pattern:
                conference_name = "JupyterCon"
            else:
                conference_name = "Python Conference"
            
            # Extract year if captured
            if match.group(1):
                conference_year = int(match.group(1))
            break
    
    # If no conference found, try to extract year separately
    if conference_year is None:
        year_match = re.search(r'\b(20\d{2})\b', text_to_analyze)
        if year_match:
            conference_year = int(year_match.group(1))
    
    # Try to detect from channel name patterns
    if conference_name is None:
        if any(keyword in channel_name.lower() for keyword in ['pycon', '파이콘', 'python']):
            if any(keyword in channel_name.lower() for keyword in ['kr', 'korea', '한국']):
                conference_name = "PyCon KR"
            else:
                conference_name = "PyCon"
    
    return conference_name, conference_year

def get_video_details_with_ytdlp(video_url: str) -> Dict[str, Any]:
    """Get detailed video information using yt-dlp"""
    ydl_opts = {
        'quiet': True,
        'no_warnings': True,
        'extract_flat': False,
    }
    
    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            logger.info(f"Extracting details for: {video_url}")
            info = ydl.extract_info(video_url, download=False)
            
            # Extract conference information
            title = info.get('title', '')
            description = info.get('description', '')
            channel_name = info.get('uploader', '') or info.get('channel', '')
            
            conference_name, conference_year = extract_conference_info(title, description, channel_name)
            
            video_details = {
                'video_url': video_url,
                'video_id': info.get('id', ''),
                'title': title,
                'description': description or '',
                'channel_name': channel_name,
                'upload_date': info.get('upload_date', ''),
                'duration': info.get('duration', 0),
                'view_count': info.get('view_count', 0),
                'like_count': info.get('like_count', 0),
                'comment_count': info.get('comment_count', 0),
                'conference_name': conference_name,
                'conference_year': conference_year,
                'tags': json.dumps(info.get('tags', [])),
                'thumbnail_url': info.get('thumbnail', ''),
            }
            
            logger.info(f"Successfully extracted details for video: {title}")
            return video_details
            
    except Exception as e:
        logger.error(f"Error extracting video details for {video_url}: {e}")
        raise

def save_video_details(video_details: Dict[str, Any]):
    """Save video details to DuckDB"""
    conn = duckdb.connect(str(DB_PATH))
    try:
        # First try to delete existing record if any
        conn.execute("DELETE FROM video_details WHERE video_url = ?", (video_details['video_url'],))
        
        # Then insert the new record
        conn.execute("""
            INSERT INTO video_details 
            (video_url, video_id, title, description, channel_name, upload_date, 
             duration, view_count, like_count, comment_count, conference_name, 
             conference_year, tags, thumbnail_url)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            video_details['video_url'],
            video_details['video_id'],
            video_details['title'],
            video_details['description'],
            video_details['channel_name'],
            video_details['upload_date'],
            video_details['duration'],
            video_details['view_count'],
            video_details['like_count'],
            video_details['comment_count'],
            video_details['conference_name'],
            video_details['conference_year'],
            video_details['tags'],
            video_details['thumbnail_url'],
        ))
        logger.info(f"Saved video details for: {video_details['title']}")
    except Exception as e:
        logger.error(f"Error saving video details: {e}")
        raise
    finally:
        conn.close()

def get_unprocessed_video_urls() -> List[str]:
    """Get video URLs that haven't been processed for details yet"""
    conn = duckdb.connect(str(DB_PATH))
    try:
        # Get URLs from video_urls table that are not in video_details table
        result = conn.execute("""
            SELECT DISTINCT v.url
            FROM video_urls v
            LEFT JOIN video_details vd ON v.url = vd.video_url
            WHERE vd.video_url IS NULL
            ORDER BY v.collected_at DESC
        """).fetchall()
        
        urls = [row[0] for row in result]
        logger.info(f"Found {len(urls)} unprocessed video URLs")
        return urls
        
    except Exception as e:
        logger.error(f"Error getting unprocessed URLs: {e}")
        return []
    finally:
        conn.close()

# Initialize database on startup
init_video_details_table()

# Create MCP server
server = Server("youtube-detail-mcp-server")

@server.list_tools()
async def handle_list_tools() -> List[Tool]:
    """List available tools"""
    return [
        Tool(
            name="extract_video_details",
            description="Extract detailed information from a single YouTube video URL using yt-dlp",
            inputSchema={
                "type": "object",
                "properties": {
                    "video_url": {
                        "type": "string",
                        "description": "YouTube video URL to extract details from"
                    }
                },
                "required": ["video_url"]
            }
        ),
        Tool(
            name="batch_extract_details",
            description="Extract details from multiple video URLs",
            inputSchema={
                "type": "object",
                "properties": {
                    "video_urls": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "List of YouTube video URLs"
                    }
                },
                "required": ["video_urls"]
            }
        ),
        Tool(
            name="process_unprocessed_videos",
            description="Process all unprocessed videos from the video_urls table",
            inputSchema={
                "type": "object",
                "properties": {
                    "limit": {
                        "type": "integer",
                        "description": "Maximum number of videos to process",
                        "default": 10
                    }
                }
            }
        ),
        Tool(
            name="get_video_details",
            description="Get stored video details from database",
            inputSchema={
                "type": "object",
                "properties": {
                    "conference_name": {
                        "type": "string",
                        "description": "Filter by conference name"
                    },
                    "conference_year": {
                        "type": "integer",
                        "description": "Filter by conference year"
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum number of results",
                        "default": 20
                    }
                }
            }
        ),
        Tool(
            name="get_conference_statistics",
            description="Get statistics about conferences and videos",
            inputSchema={
                "type": "object",
                "properties": {}
            }
        )
    ]

@server.call_tool()
async def handle_call_tool(name: str, arguments: Dict[str, Any]) -> List[TextContent]:
    """Handle tool calls"""
    
    if name == "extract_video_details":
        video_url = arguments["video_url"]
        try:
            video_details = get_video_details_with_ytdlp(video_url)
            save_video_details(video_details)
            
            return [TextContent(
                type="text",
                text=f"Successfully extracted details for: {video_details['title']}\n"
                     f"Conference: {video_details['conference_name']} ({video_details['conference_year']})\n"
                     f"Channel: {video_details['channel_name']}\n"
                     f"Duration: {video_details['duration']} seconds\n"
                     f"Views: {video_details['view_count']:,}"
            )]
        except Exception as e:
            logger.error(f"Error in extract_video_details: {e}")
            raise RuntimeError(str(e))
    
    elif name == "batch_extract_details":
        video_urls = arguments["video_urls"]
        success_count = 0
        error_count = 0
        results = []
        
        for video_url in video_urls:
            try:
                video_details = get_video_details_with_ytdlp(video_url)
                save_video_details(video_details)
                success_count += 1
                results.append(f"✅ {video_details['title']}")
            except Exception as e:
                error_count += 1
                results.append(f"❌ {video_url}: {str(e)}")
                logger.error(f"Error processing {video_url}: {e}")
        
        return [TextContent(
            type="text",
            text=f"Batch processing completed:\n"
                 f"✅ Success: {success_count}\n"
                 f"❌ Errors: {error_count}\n\n"
                 f"Results:\n" + "\n".join(results)
        )]
    
    elif name == "process_unprocessed_videos":
        limit = arguments.get("limit", 10)
        try:
            unprocessed_urls = get_unprocessed_video_urls()
            urls_to_process = unprocessed_urls[:limit]
            
            if not urls_to_process:
                return [TextContent(
                    type="text",
                    text="No unprocessed videos found."
                )]
            
            success_count = 0
            error_count = 0
            results = []
            
            for video_url in urls_to_process:
                try:
                    video_details = get_video_details_with_ytdlp(video_url)
                    save_video_details(video_details)
                    success_count += 1
                    results.append(f"✅ {video_details['title'][:50]}...")
                except Exception as e:
                    error_count += 1
                    results.append(f"❌ {video_url}: {str(e)[:50]}...")
                    logger.error(f"Error processing {video_url}: {e}")
            
            return [TextContent(
                type="text",
                text=f"Processed {len(urls_to_process)} unprocessed videos:\n"
                     f"✅ Success: {success_count}\n"
                     f"❌ Errors: {error_count}\n\n"
                     f"Results:\n" + "\n".join(results[:10])
            )]
        except Exception as e:
            logger.error(f"Error in process_unprocessed_videos: {e}")
            raise RuntimeError(str(e))
    
    elif name == "get_video_details":
        conference_name = arguments.get("conference_name")
        conference_year = arguments.get("conference_year")
        limit = arguments.get("limit", 20)
        
        try:
            conn = duckdb.connect(str(DB_PATH))
            
            # Build query
            query = "SELECT title, conference_name, conference_year, channel_name, view_count, duration, video_url FROM video_details WHERE 1=1"
            params = []
            
            if conference_name:
                query += " AND conference_name LIKE ?"
                params.append(f"%{conference_name}%")
            
            if conference_year:
                query += " AND conference_year = ?"
                params.append(conference_year)
            
            query += " ORDER BY view_count DESC LIMIT ?"
            params.append(limit)
            
            result = conn.execute(query, params).fetchall()
            conn.close()
            
            if not result:
                return [TextContent(
                    type="text",
                    text="No video details found matching the criteria."
                )]
            
            videos_text = "\n".join([
                f"📹 {row[0]}\n"
                f"   🎯 Conference: {row[1]} ({row[2]})\n"
                f"   📺 Channel: {row[3]}\n"
                f"   👁️ Views: {row[4]:,} | ⏱️ Duration: {row[5]}s\n"
                f"   🔗 {row[6]}\n"
                for row in result
            ])
            
            return [TextContent(
                type="text",
                text=f"Found {len(result)} video details:\n\n{videos_text}"
            )]
        except Exception as e:
            logger.error(f"Error in get_video_details: {e}")
            raise RuntimeError(str(e))
    
    elif name == "get_conference_statistics":
        try:
            conn = duckdb.connect(str(DB_PATH))
            
            # Conference statistics
            conf_stats = conn.execute("""
                SELECT 
                    conference_name,
                    conference_year,
                    COUNT(*) as video_count,
                    AVG(view_count) as avg_views,
                    SUM(duration) as total_duration
                FROM video_details 
                WHERE conference_name IS NOT NULL
                GROUP BY conference_name, conference_year
                ORDER BY conference_year DESC, video_count DESC
            """).fetchall()
            
            # Overall statistics
            overall_stats = conn.execute("""
                SELECT 
                    COUNT(*) as total_videos,
                    COUNT(DISTINCT conference_name) as unique_conferences,
                    COUNT(DISTINCT conference_year) as unique_years,
                    AVG(view_count) as avg_views,
                    SUM(duration) as total_duration
                FROM video_details
            """).fetchone()
            
            conn.close()
            
            # Format statistics
            stats_text = f"📊 Overall Statistics:\n"
            stats_text += f"   📹 Total Videos: {overall_stats[0]}\n"
            stats_text += f"   🎯 Unique Conferences: {overall_stats[1]}\n"
            stats_text += f"   📅 Years Covered: {overall_stats[2]}\n"
            stats_text += f"   👁️ Average Views: {overall_stats[3]:,.0f}\n"
            stats_text += f"   ⏱️ Total Duration: {overall_stats[4]/3600:.1f} hours\n\n"
            
            stats_text += "📋 Conference Breakdown:\n"
            for conf_name, conf_year, count, avg_views, total_dur in conf_stats:
                stats_text += f"   🎯 {conf_name} {conf_year}: {count} videos, "
                stats_text += f"{avg_views:,.0f} avg views, {total_dur/3600:.1f}h\n"
            
            return [TextContent(
                type="text",
                text=stats_text
            )]
        except Exception as e:
            logger.error(f"Error in get_conference_statistics: {e}")
            raise RuntimeError(str(e))
    
    else:
        raise ValueError(f"Unknown tool: {name}")

async def main():
    """Main entry point"""
    logger.info("Starting YouTube Detail MCP Server")
    
    # Use stdio transport
    async with stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream,
            write_stream,
            InitializationOptions(
                server_name="youtube-detail-mcp-server",
                server_version="1.0.0",
                capabilities=server.get_capabilities(
                    notification_options=None,
                    experimental_capabilities={},
                ),
            ),
        )

if __name__ == "__main__":
    asyncio.run(main())
