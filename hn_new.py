#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "httpx",
#     "fastapi",
#     "uvicorn",
#     "jinja2",
# ]
# ///
"""
HackerNews "new" story browser with content extraction.

Fetches new stories on a schedule (hourly by default), extracts article content
via Cloudflare Browser Rendering API, serves a fast keyboard-navigable UI.
All configuration (filters, merit/demerit, read-later) managed via UI.

Usage:
    ./hn_new.py                  # Start server (localhost:8000)
    ./hn_new.py --public         # Bind to all interfaces (0.0.0.0)
    ./hn_new.py --port 8080      # Custom port
    ./hn_new.py --reset          # Reset checkpoint to now

Environment variables (see .env.example):
    CF_ACCOUNT_ID       - Cloudflare account ID (required)
    CF_API_TOKEN        - Cloudflare API token (required)
    HN_USER             - Basic auth username (optional)
    HN_PASSWORD         - Basic auth password (optional)
    CF_BROWSER_TIMEOUT_MS - Page load timeout in ms (default: 2000, max: 60000)
    HN_FETCH_INTERVAL   - Minutes between fetches (default: 60)
    HN_CONTENT_WORKERS  - Content extraction workers (default: 3)
"""

import argparse
import asyncio
import base64
import logging
import zlib
import os
import re
import secrets
import sqlite3
import tempfile
import threading
import time
import zipfile
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional
from urllib.parse import unquote, urlparse

import httpx
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response


# Set up logging with timestamps, colors, and aligned prefixes
class ColoredFormatter(logging.Formatter):
    """Formatter with colored levels and source prefixes."""

    LEVEL_COLORS = {
        "DEBUG": "\033[36m",  # Cyan
        "INFO": "\033[32m",  # Green
        "WARNING": "\033[33m",  # Yellow
        "ERROR": "\033[31m",  # Red
        "CRITICAL": "\033[35m",  # Magenta
    }
    PREFIX_COLORS = {
        "http": "\033[34m",  # Blue
        "fetch": "\033[34m",  # Blue (outgoing requests)
        "fetcher": "\033[35m",  # Magenta
        "worker": "\033[36m",  # Cyan
        "front-page": "\033[33m",  # Yellow
    }
    RESET = "\033[0m"
    PREFIX_WIDTH = 10  # Fixed width for alignment

    def format(self, record):
        # Color the level
        level_color = self.LEVEL_COLORS.get(record.levelname, "")
        colored_level = f"{level_color}{record.levelname:<7}{self.RESET}"

        # Determine prefix from logger name or message
        prefix = self._extract_prefix(record)
        prefix_color = self._get_prefix_color(prefix)
        # Brackets enclose the padded prefix: [prefix    ]
        colored_prefix = f"{prefix_color}[{prefix:<{self.PREFIX_WIDTH}}]{self.RESET}"

        # Clean message (remove prefix if it was in message)
        msg = self._clean_message(record.getMessage(), prefix)

        # Format timestamp
        timestamp = self.formatTime(record, self.datefmt)

        return f"{timestamp} {colored_level} {colored_prefix} {msg}"

    def _extract_prefix(self, record):
        """Extract prefix from logger name or message."""
        # Check logger name first
        if record.name.startswith("uvicorn"):
            return "http"
        if record.name.startswith("httpx"):
            return "fetch"

        # Check if message starts with [prefix]
        msg = record.getMessage()
        if msg.startswith("["):
            end = msg.find("]")
            if end > 0:
                return msg[1:end]

        return "main"

    def _get_prefix_color(self, prefix):
        """Get color for a prefix."""
        for key, color in self.PREFIX_COLORS.items():
            if key in prefix.lower():
                return color
        return ""

    def _clean_message(self, msg, prefix):
        """Remove prefix from message if present."""
        tag = f"[{prefix}]"
        if msg.startswith(tag):
            return msg[len(tag) :].lstrip()
        return msg


def setup_logging():
    """Configure logging for the application and uvicorn."""
    handler = logging.StreamHandler()
    handler.setFormatter(ColoredFormatter(datefmt="%H:%M:%S"))

    # Configure root logger
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.handlers = [handler]

    # Configure uvicorn and httpx loggers to use same format
    for name in ("uvicorn", "uvicorn.access", "uvicorn.error", "httpx"):
        logger = logging.getLogger(name)
        logger.handlers = [handler]
        logger.propagate = False


setup_logging()
log = logging.getLogger("hn_new")

# =============================================================================
# Configuration
# =============================================================================

DATA_DIR = Path(__file__).parent / ".hn_data"
DB_FILE = DATA_DIR / "hn.db"
FRONTEND_DIR = Path(__file__).parent / "frontend"
FRONTEND_ZIP = Path(__file__).parent / "ui.zip"  # Alternative: serve from zip

HN_API_BASE = "https://hacker-news.firebaseio.com/v0"
HN_NEW_STORIES = f"{HN_API_BASE}/newstories.json"
HN_TOP_STORIES = f"{HN_API_BASE}/topstories.json"
HN_ITEM = f"{HN_API_BASE}/item/{{id}}.json"

# Front page tracking
FRONT_PAGE_POLL_INTERVAL = 5 * 60  # Check every 5 minutes

TEASER_LENGTH = 300  # characters for teaser

# Fetch settings
DEFAULT_LOOKBACK_HOURS = 80  # ~3 days for initial fetch
ALGOLIA_PAGE_SIZE = 100
ALGOLIA_MAX_PAGES = 100  # 5000 stories max via Algolia
ID_WALK_DELAY = 0.5  # seconds between ID walk requests
ID_WALK_BATCH_SIZE = 500  # how many IDs to walk before checking if we're done

# Rate limiting for content fetching (per domain)
DOMAIN_REQUEST_DELAY = 2.0  # seconds between requests to same domain

# Story fetch scheduling
FETCH_INTERVAL_MINUTES = 60  # Fetch new stories every hour

# Cloudflare Browser Rendering settings
CF_BROWSER_TIMEOUT_MS = 2000  # Max time to wait for page load (default 2s, max 60s)

# Cloudflare Browser Rendering API (from environment)
# Load .env file if present
_env_file = Path(__file__).parent / ".env"
if _env_file.exists():
    for line in _env_file.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, _, value = line.partition("=")
            os.environ.setdefault(key.strip(), value.strip())

CF_ACCOUNT_ID = os.environ.get("CF_ACCOUNT_ID", "")
CF_API_TOKEN = os.environ.get("CF_API_TOKEN", "")
CF_BROWSER_API = f"https://api.cloudflare.com/client/v4/accounts/{CF_ACCOUNT_ID}/browser-rendering/markdown"

# Cleanup configuration
CLEANUP_DISMISSED_HOURS = int(os.environ.get("CLEANUP_DISMISSED_HOURS", "24"))
CLEANUP_STORY_DAYS = int(os.environ.get("CLEANUP_STORY_DAYS", "14"))
CLEANUP_CONTENT_CACHE_DAYS = int(os.environ.get("CLEANUP_CONTENT_CACHE_DAYS", "90"))

# =============================================================================
# Content Compression
# =============================================================================
# Content is stored compressed with "z:" prefix. Uncompressed content has no prefix.
# This allows backwards compatibility - old uncompressed content still works.

COMPRESS_PREFIX = "z:"


def compress_content(text: str) -> str:
    """Compress text content for storage. Returns z: prefixed base64 string."""
    if not text:
        return text
    compressed = zlib.compress(text.encode("utf-8"), level=6)
    return COMPRESS_PREFIX + base64.b64encode(compressed).decode("ascii")


def decompress_content(data: str) -> str:
    """Decompress content if compressed, otherwise return as-is."""
    if not data:
        return data
    if not data.startswith(COMPRESS_PREFIX):
        return data  # Not compressed, return as-is
    try:
        compressed = base64.b64decode(data[len(COMPRESS_PREFIX) :])
        return zlib.decompress(compressed).decode("utf-8")
    except Exception:
        # If decompression fails, return original (safety fallback)
        return data


# =============================================================================
# Database Schema & Operations
# =============================================================================

SCHEMA = """
-- Stories fetched from HN
CREATE TABLE IF NOT EXISTS stories (
    id INTEGER PRIMARY KEY,
    title TEXT NOT NULL,
    url TEXT,
    domain TEXT,
    by TEXT,
    time INTEGER,
    score INTEGER DEFAULT 0,
    descendants INTEGER DEFAULT 0,
    content TEXT,  -- extracted markdown content
    content_status TEXT DEFAULT 'pending',  -- pending, fetching, retry, done, failed, blocked, skipped
    content_attempts INTEGER DEFAULT 0,  -- number of fetch attempts (for retry limiting)
    content_source TEXT,  -- 'cloudflare'
    browser_ms REAL DEFAULT 0,  -- Cloudflare billing: browser milliseconds used
    hit_front_page INTEGER DEFAULT 0,  -- 1 if story appeared in topstories
    front_page_rank INTEGER,  -- highest rank achieved on front page (1-30)
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Track URLs we've already fetched content for (dedup across stories)
CREATE TABLE IF NOT EXISTS fetched_urls (
    url TEXT PRIMARY KEY,
    content TEXT,
    content_source TEXT,
    browser_ms REAL DEFAULT 0,
    fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Usage tracking for Cloudflare billing
CREATE TABLE IF NOT EXISTS usage_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    story_id INTEGER,
    url TEXT,
    browser_ms REAL,
    source TEXT,  -- 'cloudflare'
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Monthly usage summaries (aggregated from usage_log, kept for 3 years)
CREATE TABLE IF NOT EXISTS usage_summary (
    month TEXT PRIMARY KEY,  -- 'YYYY-MM' format
    request_count INTEGER DEFAULT 0,
    total_browser_ms REAL DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Blocked domains (never show stories from these)
CREATE TABLE IF NOT EXISTS blocked_domains (
    domain TEXT PRIMARY KEY,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Blocked words in titles (never show stories containing these)
CREATE TABLE IF NOT EXISTS blocked_words (
    word TEXT PRIMARY KEY,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Merit words (boost these stories)
CREATE TABLE IF NOT EXISTS merit_words (
    word TEXT PRIMARY KEY,
    weight INTEGER DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Demerit words (penalize these stories)
CREATE TABLE IF NOT EXISTS demerit_words (
    word TEXT PRIMARY KEY,
    weight INTEGER DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Merit domains
CREATE TABLE IF NOT EXISTS merit_domains (
    domain TEXT PRIMARY KEY,
    weight INTEGER DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Demerit domains
CREATE TABLE IF NOT EXISTS demerit_domains (
    domain TEXT PRIMARY KEY,
    weight INTEGER DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Read later list
CREATE TABLE IF NOT EXISTS read_later (
    story_id INTEGER PRIMARY KEY REFERENCES stories(id),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Dismissed stories (hide from current session, kept after story deletion to prevent re-fetch)
CREATE TABLE IF NOT EXISTS dismissed (
    story_id INTEGER PRIMARY KEY,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Reading history (stories you've opened)
CREATE TABLE IF NOT EXISTS history (
    story_id INTEGER PRIMARY KEY REFERENCES stories(id),
    opened_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_stories_domain ON stories(domain);
CREATE INDEX IF NOT EXISTS idx_stories_time ON stories(time DESC);
CREATE INDEX IF NOT EXISTS idx_stories_content_status ON stories(content_status);
CREATE INDEX IF NOT EXISTS idx_stories_content_queue ON stories(content_status, content_attempts, time);
"""


class Database:
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._local = threading.local()

    def _get_conn(self) -> sqlite3.Connection:
        if not hasattr(self._local, "conn"):
            self._local.conn = sqlite3.connect(
                str(self.db_path), check_same_thread=False
            )
            self._local.conn.row_factory = sqlite3.Row
            # Safety
            self._local.conn.execute("PRAGMA foreign_keys = ON")
            self._local.conn.execute("PRAGMA journal_mode = WAL")
            self._local.conn.execute("PRAGMA synchronous = NORMAL")  # Safe with WAL
            self._local.conn.execute("PRAGMA busy_timeout = 5000")  # Wait 5s for locks
            # Performance
            self._local.conn.execute("PRAGMA cache_size = -64000")  # 64MB cache
            self._local.conn.execute("PRAGMA temp_store = MEMORY")
            self._local.conn.execute("PRAGMA mmap_size = 268435456")  # 256MB mmap
        return self._local.conn

    def init(self):
        conn = self._get_conn()
        conn.executescript(SCHEMA)
        # Migrations for existing databases
        self._migrate_add_front_page_columns()
        self._migrate_add_teaser_column()
        conn.commit()

    def _migrate_add_teaser_column(self):
        """Add teaser column if it doesn't exist, populate from existing content."""
        try:
            self.execute("SELECT teaser FROM stories LIMIT 1")
        except sqlite3.OperationalError:
            log.info("Adding teaser column to stories table...")
            self.execute("ALTER TABLE stories ADD COLUMN teaser TEXT")
            # Populate teasers for existing content (in batches to avoid memory issues)
            log.info("Populating teasers for existing stories...")
            populated = 0
            while True:
                rows = self.fetchall(
                    """
                    SELECT id, content FROM stories
                    WHERE content IS NOT NULL AND content != '' AND teaser IS NULL
                    LIMIT 100
                    """
                )
                if not rows:
                    break
                for row in rows:
                    try:
                        content = decompress_content(row["content"])
                        teaser = content[:TEASER_LENGTH].strip()
                        if len(content) > TEASER_LENGTH:
                            teaser += "..."
                        self.execute(
                            "UPDATE stories SET teaser = ? WHERE id = ?",
                            (teaser, row["id"]),
                        )
                        populated += 1
                    except Exception as e:
                        log.warning(
                            f"Failed to generate teaser for story {row['id']}: {e}"
                        )
                        # Mark as processed with empty teaser to avoid infinite loop
                        self.execute(
                            "UPDATE stories SET teaser = '' WHERE id = ?",
                            (row["id"],),
                        )
                self.commit()
            log.info(f"Populated {populated} teasers")

    def _migrate_add_front_page_columns(self):
        """Add hit_front_page and front_page_rank columns if they don't exist."""
        try:
            self.execute("SELECT hit_front_page FROM stories LIMIT 1")
        except sqlite3.OperationalError:
            self.execute(
                "ALTER TABLE stories ADD COLUMN hit_front_page INTEGER DEFAULT 0"
            )
            self.execute("ALTER TABLE stories ADD COLUMN front_page_rank INTEGER")

    def execute(self, sql: str, params: tuple = ()) -> sqlite3.Cursor:
        return self._get_conn().execute(sql, params)

    def executemany(self, sql: str, params_list: list) -> sqlite3.Cursor:
        return self._get_conn().executemany(sql, params_list)

    def commit(self):
        self._get_conn().commit()

    def fetchone(self, sql: str, params: tuple = ()) -> Optional[sqlite3.Row]:
        return self.execute(sql, params).fetchone()

    def fetchall(self, sql: str, params: tuple = ()) -> list[sqlite3.Row]:
        return self.execute(sql, params).fetchall()

    # --- Story operations ---

    def upsert_story(self, story: dict):
        url = story.get("url")
        hn_text = story.get("text")  # Self-post text from HN API

        # For stories without URL but with text (Ask HN, etc.), use the text as content
        if not url and hn_text:
            # Compress HN text content before storing
            compressed_text = compress_content(hn_text)
            self.execute(
                """
                INSERT INTO stories (id, title, url, domain, by, time, score, descendants, content, content_status, content_source)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'done', 'hn_text')
                ON CONFLICT(id) DO UPDATE SET
                    score = excluded.score,
                    descendants = excluded.descendants,
                    content = COALESCE(stories.content, excluded.content),
                    content_status = CASE WHEN stories.content IS NULL THEN 'done' ELSE stories.content_status END,
                    content_source = CASE WHEN stories.content IS NULL THEN 'hn_text' ELSE stories.content_source END,
                    updated_at = CURRENT_TIMESTAMP
            """,
                (
                    story["id"],
                    story["title"],
                    url,
                    story.get("domain"),
                    story.get("by"),
                    story.get("time"),
                    story.get("score", 0),
                    story.get("descendants", 0),
                    compressed_text,
                ),
            )
        else:
            self.execute(
                """
                INSERT INTO stories (id, title, url, domain, by, time, score, descendants)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    score = excluded.score,
                    descendants = excluded.descendants,
                    updated_at = CURRENT_TIMESTAMP
            """,
                (
                    story["id"],
                    story["title"],
                    url,
                    story.get("domain"),
                    story.get("by"),
                    story.get("time"),
                    story.get("score", 0),
                    story.get("descendants", 0),
                ),
            )

    def get_stories(
        self,
        dismissed_only: bool = False,
        include_blocked: bool = False,
        include_read_later: bool = False,
        limit: int = 50,
        cursor_time: int = None,
        cursor_id: int = None,
        sort: str = "newest",
    ) -> dict:
        """Get stories with computed scores, filtering, and deduplication.

        Uses keyset pagination with over-fetch for efficiency. Fetches batches
        from SQL and applies Python filters until we have enough stories.

        Args:
            dismissed_only: If True, show ONLY dismissed stories. If False, exclude dismissed.
            include_blocked: If True, include stories from blocked domains.
            include_read_later: If True, include stories marked for read later.
            limit: Max number of stories to return.
            cursor_time: Timestamp of last story from previous page (for keyset pagination).
            cursor_id: ID of last story from previous page (tiebreaker for same timestamp).
            sort: 'newest' or 'oldest'.

        Returns:
            dict with 'stories' list, 'has_more' boolean, and 'next_cursor' string.
        """
        SQL_MULTIPLIER = 3  # Fetch 3x limit to account for filtering
        sql_limit = limit * SQL_MULTIPLIER

        # Pre-fetch filter data (only once per call)
        merit_words = {
            r["word"].lower(): r["weight"]
            for r in self.fetchall("SELECT word, weight FROM merit_words")
        }
        demerit_words = {
            r["word"].lower(): r["weight"]
            for r in self.fetchall("SELECT word, weight FROM demerit_words")
        }
        blocked_words = {
            r["word"].lower() for r in self.fetchall("SELECT word FROM blocked_words")
        }

        stories = []
        seen_urls = set()
        current_cursor_time = cursor_time
        current_cursor_id = cursor_id
        has_more_in_db = True

        # Keep fetching batches until we have enough stories or run out
        while len(stories) < limit and has_more_in_db:
            # Build query with keyset pagination
            query = """
                WITH scored AS (
                    SELECT
                        s.id, s.title, s.url, s.domain, s.by, s.time, s.score, s.descendants,
                        s.content_status, s.teaser, s.hit_front_page, s.front_page_rank,
                        COALESCE(md.weight, 0) as domain_merit,
                        COALESCE(dd.weight, 0) as domain_demerit,
                        CASE WHEN rl.story_id IS NOT NULL THEN 1 ELSE 0 END as is_read_later,
                        CASE WHEN d.story_id IS NOT NULL THEN 1 ELSE 0 END as is_dismissed,
                        CASE WHEN h.story_id IS NOT NULL THEN 1 ELSE 0 END as is_read,
                        CASE WHEN bd.domain IS NOT NULL THEN 1 ELSE 0 END as is_domain_blocked
                    FROM stories s
                    LEFT JOIN merit_domains md ON s.domain = md.domain
                    LEFT JOIN demerit_domains dd ON s.domain = dd.domain
                    LEFT JOIN read_later rl ON s.id = rl.story_id
                    LEFT JOIN dismissed d ON s.id = d.story_id
                    LEFT JOIN history h ON s.id = h.story_id
                    LEFT JOIN blocked_domains bd ON s.domain = bd.domain
                )
                SELECT * FROM scored
                WHERE 1=1
            """
            params = []

            # Exclusive filter: show ONLY dismissed OR ONLY non-dismissed
            if dismissed_only:
                query += " AND is_dismissed = 1"
            else:
                query += " AND is_dismissed = 0"
            if not include_blocked:
                query += " AND is_domain_blocked = 0"
            if not include_read_later:
                query += " AND is_read_later = 0"

            # Keyset pagination cursor
            if current_cursor_time is not None and current_cursor_id is not None:
                if sort == "newest":
                    # For newest first: get stories older than cursor
                    query += " AND (time < ? OR (time = ? AND id < ?))"
                else:
                    # For oldest first: get stories newer than cursor
                    query += " AND (time > ? OR (time = ? AND id > ?))"
                params.extend(
                    [current_cursor_time, current_cursor_time, current_cursor_id]
                )

            # Order and limit
            if sort == "newest":
                query += " ORDER BY time DESC, id DESC"
            else:
                query += " ORDER BY time ASC, id ASC"
            query += f" LIMIT {sql_limit}"

            rows = self.fetchall(query, tuple(params))
            has_more_in_db = len(rows) == sql_limit

            if not rows:
                break

            # Process rows through Python filters
            for row in rows:
                story = dict(row)

                # Skip blocked words in title
                title_lower = story["title"].lower()
                if not include_blocked and any(
                    bw in title_lower for bw in blocked_words
                ):
                    continue

                # Deduplication: keep first occurrence (by sort order)
                url = story.get("url") or f"hn:{story['id']}"
                if url in seen_urls:
                    continue
                seen_urls.add(url)

                # Compute word-based score
                word_merit = sum(
                    w for word, w in merit_words.items() if word in title_lower
                )
                word_demerit = sum(
                    w for word, w in demerit_words.items() if word in title_lower
                )

                story["word_merit"] = word_merit
                story["word_demerit"] = word_demerit
                story["merit_score"] = story["domain_merit"] + word_merit
                story["demerit_score"] = story["domain_demerit"] + word_demerit
                story["net_score"] = story["merit_score"] - story["demerit_score"]

                stories.append(story)

                if len(stories) >= limit:
                    break

            # Update cursor for next batch (if needed)
            if rows:
                last_row = rows[-1]
                current_cursor_time = last_row["time"]
                current_cursor_id = last_row["id"]

        # Build next_cursor from the last story we're returning
        next_cursor = None
        if stories and (has_more_in_db or len(stories) >= limit):
            last_story = stories[-1] if len(stories) <= limit else stories[limit - 1]
            next_cursor = f"{last_story['time']}:{last_story['id']}"

        result_stories = stories[:limit]
        # has_more is true if we have more stories than limit OR there's more in DB
        has_more = len(stories) > limit or has_more_in_db

        return {
            "stories": result_stories,
            "has_more": has_more,
            "next_cursor": next_cursor,
        }

    def update_content(
        self,
        story_id: int,
        content: str,
        status: str = "done",
        source: str = None,
        browser_ms: float = 0,
    ):
        # Generate teaser from content
        teaser = None
        if content:
            # Content may be compressed, decompress to generate teaser
            text = (
                decompress_content(content)
                if content.startswith(COMPRESS_PREFIX)
                else content
            )
            teaser = text[:TEASER_LENGTH].strip()
            if len(text) > TEASER_LENGTH:
                teaser += "..."

        self.execute(
            """
            UPDATE stories SET content = ?, content_status = ?, content_source = ?,
                browser_ms = ?, teaser = ?, updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
        """,
            (content, status, source, browser_ms, teaser, story_id),
        )
        self.commit()

    # --- Content Queue (atomic operations for single worker) ---

    def claim_next_content_job(self, max_attempts: int = 3) -> Optional[dict]:
        """
        Atomically claim the next story needing content fetch.
        Returns story dict or None if no work available.

        Uses UPDATE...RETURNING (SQLite 3.35+) for true atomicity - the subquery
        and update happen as one operation, preventing race conditions where
        multiple workers could claim the same job.
        """
        # Atomic claim: UPDATE with subquery finds and claims in one operation
        # This prevents TOCTOU race conditions with multiple workers
        row = self.fetchone(
            """
            UPDATE stories
            SET content_status = 'fetching',
                content_attempts = content_attempts + 1,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = (
                SELECT id FROM stories
                WHERE content_status IN ('pending', 'retry')
                  AND url IS NOT NULL
                  AND content_attempts < ?
                ORDER BY content_attempts ASC, time ASC
                LIMIT 1
            )
            RETURNING id, url, domain, content_attempts
        """,
            (max_attempts,),
        )
        if row:
            self.commit()
            return dict(row)
        return None

    def complete_content_job(
        self,
        story_id: int,
        content: str,
        status: str,
        source: str = None,
        browser_ms: float = 0,
    ):
        """Mark a content fetch job as complete (done, failed, blocked, skipped)."""
        # Compress content before storing
        compressed = compress_content(content) if content else content
        self.execute(
            """
            UPDATE stories
            SET content = ?, content_status = ?, content_source = ?,
                browser_ms = ?, updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
        """,
            (compressed, status, source, browser_ms, story_id),
        )
        self.commit()

    def retry_content_job(self, story_id: int):
        """Mark a content fetch job for retry."""
        self.execute(
            """
            UPDATE stories
            SET content_status = 'retry', updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
        """,
            (story_id,),
        )
        self.commit()

    def get_content_queue_diagnostic(self) -> dict:
        """Get detailed diagnostic info about the content queue."""
        row = self.fetchone("""
            SELECT
                SUM(CASE WHEN content_status = 'pending' AND content_attempts = 0 THEN 1 ELSE 0 END) as pending_fresh,
                SUM(CASE WHEN content_status = 'pending' AND content_attempts > 0 THEN 1 ELSE 0 END) as pending_with_attempts,
                SUM(CASE WHEN content_status = 'retry' THEN 1 ELSE 0 END) as retry,
                SUM(CASE WHEN content_status = 'fetching' THEN 1 ELSE 0 END) as fetching,
                SUM(CASE WHEN content_status = 'done' THEN 1 ELSE 0 END) as done,
                SUM(CASE WHEN content_status = 'failed' THEN 1 ELSE 0 END) as failed,
                SUM(CASE WHEN content_status = 'blocked' THEN 1 ELSE 0 END) as blocked,
                SUM(CASE WHEN content_status = 'skipped' THEN 1 ELSE 0 END) as skipped,
                SUM(CASE WHEN url IS NULL THEN 1 ELSE 0 END) as no_url,
                SUM(CASE WHEN content_attempts >= 3 AND content_status NOT IN ('done', 'failed', 'blocked', 'skipped') THEN 1 ELSE 0 END) as exhausted_not_failed
            FROM stories
        """)
        return dict(row) if row else {}

    def cleanup_stuck_content_jobs(self, max_attempts: int = 3) -> int:
        """
        Reset stuck 'fetching' jobs and mark exhausted retries as failed.
        Called on startup and periodically to recover from crashes.
        Returns number of jobs reset.
        """
        # First, log diagnostic info
        diag = self.get_content_queue_diagnostic()
        if (
            diag.get("fetching", 0) > 0
            or diag.get("exhausted_not_failed", 0) > 0
            or diag.get("no_url", 0) > 0
        ):
            log.info(f"Queue diagnostic: {diag}")

        total_fixed = 0

        # Mark stories without URLs as 'skipped' (Ask HN, Tell HN, etc.)
        cursor = self.execute(
            """
            UPDATE stories
            SET content_status = 'skipped', updated_at = CURRENT_TIMESTAMP
            WHERE url IS NULL
              AND content_status NOT IN ('done', 'failed', 'blocked', 'skipped')
        """
        )
        skipped_count = cursor.rowcount
        total_fixed += skipped_count

        # Reset stuck 'fetching' jobs back to 'retry'
        cursor = self.execute(
            """
            UPDATE stories
            SET content_status = 'retry', updated_at = CURRENT_TIMESTAMP
            WHERE content_status = 'fetching'
              AND content_attempts < ?
        """,
            (max_attempts,),
        )
        reset_count = cursor.rowcount
        total_fixed += reset_count

        # Mark exhausted retries as 'failed' (any status except terminal ones)
        cursor = self.execute(
            """
            UPDATE stories
            SET content_status = 'failed', updated_at = CURRENT_TIMESTAMP
            WHERE content_status NOT IN ('done', 'failed', 'blocked', 'skipped')
              AND content_attempts >= ?
        """,
            (max_attempts,),
        )
        failed_count = cursor.rowcount
        total_fixed += failed_count

        self.commit()

        if total_fixed > 0:
            log.info(
                f"Content queue cleanup: {skipped_count} skipped (no URL), {reset_count} reset to retry, {failed_count} marked failed"
            )

        return total_fixed

    def get_content_queue_stats(self) -> dict:
        """Get stats about the content fetch queue."""
        row = self.fetchone("""
            SELECT
                SUM(CASE WHEN content_status = 'pending' THEN 1 ELSE 0 END) as pending,
                SUM(CASE WHEN content_status = 'retry' THEN 1 ELSE 0 END) as retry,
                SUM(CASE WHEN content_status = 'fetching' THEN 1 ELSE 0 END) as fetching,
                SUM(CASE WHEN content_status = 'done' THEN 1 ELSE 0 END) as done,
                SUM(CASE WHEN content_status = 'failed' THEN 1 ELSE 0 END) as failed,
                SUM(CASE WHEN content_status = 'blocked' THEN 1 ELSE 0 END) as blocked,
                SUM(CASE WHEN content_status = 'skipped' THEN 1 ELSE 0 END) as skipped
            FROM stories WHERE url IS NOT NULL
        """)
        return dict(row) if row else {}

    # --- Story Sync Checkpoint (derived from stories table) ---

    def get_newest_story_time(self) -> Optional[int]:
        """Get the timestamp of the newest story we have (our sync checkpoint)."""
        row = self.fetchone("SELECT MAX(time) as max_time FROM stories")
        return row["max_time"] if row and row["max_time"] else None

    def get_oldest_story_time(self) -> Optional[int]:
        """Get the timestamp of the oldest story we have."""
        row = self.fetchone("SELECT MIN(time) as min_time FROM stories")
        return row["min_time"] if row and row["min_time"] else None

    # --- Blocked domains/words ---

    def add_blocked_domain(self, domain: str):
        self.execute(
            "INSERT OR IGNORE INTO blocked_domains (domain) VALUES (?)", (domain,)
        )
        self.commit()

    def remove_blocked_domain(self, domain: str):
        self.execute("DELETE FROM blocked_domains WHERE domain = ?", (domain,))
        self.commit()

    def get_blocked_domains(self) -> list[str]:
        return [
            r["domain"]
            for r in self.fetchall("SELECT domain FROM blocked_domains ORDER BY domain")
        ]

    def add_blocked_word(self, word: str):
        self.execute(
            "INSERT OR IGNORE INTO blocked_words (word) VALUES (?)", (word.lower(),)
        )
        self.commit()

    def remove_blocked_word(self, word: str):
        self.execute("DELETE FROM blocked_words WHERE word = ?", (word.lower(),))
        self.commit()

    def get_blocked_words(self) -> list[str]:
        return [
            r["word"]
            for r in self.fetchall("SELECT word FROM blocked_words ORDER BY word")
        ]

    # --- Merit/Demerit ---

    def add_merit_word(self, word: str, weight: int = 1):
        self.execute(
            "INSERT OR REPLACE INTO merit_words (word, weight) VALUES (?, ?)",
            (word.lower(), weight),
        )
        self.commit()

    def remove_merit_word(self, word: str):
        self.execute("DELETE FROM merit_words WHERE word = ?", (word.lower(),))
        self.commit()

    def get_merit_words(self) -> list[dict]:
        return [
            dict(r)
            for r in self.fetchall("SELECT word, weight FROM merit_words ORDER BY word")
        ]

    def add_demerit_word(self, word: str, weight: int = 1):
        self.execute(
            "INSERT OR REPLACE INTO demerit_words (word, weight) VALUES (?, ?)",
            (word.lower(), weight),
        )
        self.commit()

    def remove_demerit_word(self, word: str):
        self.execute("DELETE FROM demerit_words WHERE word = ?", (word.lower(),))
        self.commit()

    def get_demerit_words(self) -> list[dict]:
        return [
            dict(r)
            for r in self.fetchall(
                "SELECT word, weight FROM demerit_words ORDER BY word"
            )
        ]

    def add_merit_domain(self, domain: str, weight: int = 1):
        self.execute(
            "INSERT OR REPLACE INTO merit_domains (domain, weight) VALUES (?, ?)",
            (domain, weight),
        )
        self.commit()

    def remove_merit_domain(self, domain: str):
        self.execute("DELETE FROM merit_domains WHERE domain = ?", (domain,))
        self.commit()

    def get_merit_domains(self) -> list[dict]:
        return [
            dict(r)
            for r in self.fetchall(
                "SELECT domain, weight FROM merit_domains ORDER BY domain"
            )
        ]

    def add_demerit_domain(self, domain: str, weight: int = 1):
        self.execute(
            "INSERT OR REPLACE INTO demerit_domains (domain, weight) VALUES (?, ?)",
            (domain, weight),
        )
        self.commit()

    def remove_demerit_domain(self, domain: str):
        self.execute("DELETE FROM demerit_domains WHERE domain = ?", (domain,))
        self.commit()

    def get_demerit_domains(self) -> list[dict]:
        return [
            dict(r)
            for r in self.fetchall(
                "SELECT domain, weight FROM demerit_domains ORDER BY domain"
            )
        ]

    # --- Read later ---

    def add_read_later(self, story_id: int):
        self.execute(
            "INSERT OR IGNORE INTO read_later (story_id) VALUES (?)", (story_id,)
        )
        self.commit()

    def remove_read_later(self, story_id: int):
        self.execute("DELETE FROM read_later WHERE story_id = ?", (story_id,))
        self.commit()

    def get_read_later(
        self,
        dismissed_only: bool = False,
        limit: int = 50,
        cursor_time: int = None,
        cursor_id: int = None,
        sort: str = "newest",
    ) -> dict:
        """Get read later stories with keyset pagination.

        Args:
            dismissed_only: If True, show ONLY dismissed read later stories.
                           If False, exclude dismissed ones.
            limit: Max number of stories to return.
            cursor_time: Timestamp of last story from previous page.
            cursor_id: ID of last story from previous page (tiebreaker).
            sort: 'newest' or 'oldest'.

        Returns:
            dict with 'stories' list, 'has_more' boolean, and 'next_cursor' string.
        """
        # Read later is typically small, but use consistent pagination approach
        SQL_MULTIPLIER = 3
        sql_limit = limit * SQL_MULTIPLIER

        # Pre-fetch merit/demerit words
        merit_words = {w["word"].lower(): w["weight"] for w in self.get_merit_words()}
        demerit_words = {
            w["word"].lower(): w["weight"] for w in self.get_demerit_words()
        }

        stories = []
        current_cursor_time = cursor_time
        current_cursor_id = cursor_id
        has_more_in_db = True

        while len(stories) < limit and has_more_in_db:
            query = """
                SELECT s.id, s.title, s.url, s.domain, s.by, s.time, s.score, s.descendants,
                    s.content_status, s.teaser, s.hit_front_page, s.front_page_rank,
                    COALESCE(md.weight, 0) as domain_merit,
                    COALESCE(dd.weight, 0) as domain_demerit,
                    1 as is_read_later,
                    CASE WHEN d.story_id IS NOT NULL THEN 1 ELSE 0 END as is_dismissed,
                    CASE WHEN h.story_id IS NOT NULL THEN 1 ELSE 0 END as is_read
                FROM stories s
                JOIN read_later rl ON s.id = rl.story_id
                LEFT JOIN dismissed d ON s.id = d.story_id
                LEFT JOIN history h ON s.id = h.story_id
                LEFT JOIN merit_domains md ON s.domain = md.domain
                LEFT JOIN demerit_domains dd ON s.domain = dd.domain
            """
            params = []

            # Exclusive filter: show ONLY dismissed OR ONLY non-dismissed
            if dismissed_only:
                query += " WHERE d.story_id IS NOT NULL"
            else:
                query += " WHERE d.story_id IS NULL"

            # Keyset pagination cursor
            if current_cursor_time is not None and current_cursor_id is not None:
                if sort == "newest":
                    query += " AND (s.time < ? OR (s.time = ? AND s.id < ?))"
                else:
                    query += " AND (s.time > ? OR (s.time = ? AND s.id > ?))"
                params.extend(
                    [current_cursor_time, current_cursor_time, current_cursor_id]
                )

            if sort == "newest":
                query += " ORDER BY s.time DESC, s.id DESC"
            else:
                query += " ORDER BY s.time ASC, s.id ASC"
            query += f" LIMIT {sql_limit}"

            rows = self.fetchall(query, tuple(params))
            has_more_in_db = len(rows) == sql_limit

            if not rows:
                break

            for row in rows:
                story = dict(row)
                title_lower = story["title"].lower()

                # Compute word-based merit/demerit scores
                word_merit = sum(
                    w for word, w in merit_words.items() if word in title_lower
                )
                word_demerit = sum(
                    w for word, w in demerit_words.items() if word in title_lower
                )

                story["word_merit"] = word_merit
                story["word_demerit"] = word_demerit
                story["merit_score"] = story["domain_merit"] + word_merit
                story["demerit_score"] = story["domain_demerit"] + word_demerit
                story["net_score"] = story["merit_score"] - story["demerit_score"]

                stories.append(story)

                if len(stories) >= limit:
                    break

            # Update cursor for next batch
            if rows:
                last_row = rows[-1]
                current_cursor_time = last_row["time"]
                current_cursor_id = last_row["id"]

        # Build next_cursor from the last story we're returning
        next_cursor = None
        if stories and (has_more_in_db or len(stories) >= limit):
            last_story = stories[-1] if len(stories) <= limit else stories[limit - 1]
            next_cursor = f"{last_story['time']}:{last_story['id']}"

        result_stories = stories[:limit]
        has_more = len(stories) > limit or has_more_in_db

        return {
            "stories": result_stories,
            "has_more": has_more,
            "next_cursor": next_cursor,
        }

    # --- Dismissed ---

    def dismiss_story(self, story_id: int):
        self.execute(
            "INSERT OR IGNORE INTO dismissed (story_id) VALUES (?)", (story_id,)
        )
        # Story stays in read_later (if present) until cleanup runs after 24 hours.
        # This allows dismissed read later stories to remain visible with "show dismissed".
        self.commit()

    def undismiss_story(self, story_id: int):
        self.execute("DELETE FROM dismissed WHERE story_id = ?", (story_id,))
        self.commit()

    def clear_dismissed(self):
        self.execute("DELETE FROM dismissed")
        self.commit()

    def cleanup_stories(
        self,
        dismissed_hours: int = 24,
        max_age_days: int = 14,
        content_cache_days: int = 90,
    ) -> dict:
        """
        Clean up old stories and related data to keep database size bounded.

        1. Delete stories dismissed more than `dismissed_hours` ago
        2. Delete stories older than `max_age_days` (except those in read_later)
        3. Delete content cache older than `content_cache_days`
        4. Aggregate and delete usage logs older than 6 months
        5. Delete usage summaries older than 36 months

        Keeps dismissed markers to prevent re-fetching.
        Returns count of deleted stories by reason.
        """
        dismissed_cutoff = int(time.time()) - (dismissed_hours * 3600)
        age_cutoff = int(time.time()) - (max_age_days * 24 * 3600)
        cache_cutoff = int(time.time()) - (content_cache_days * 24 * 3600)

        # Count before deletion for reporting
        dismissed_count = self.fetchone(
            """
            SELECT COUNT(*) as c FROM stories s
            INNER JOIN dismissed d ON s.id = d.story_id
            WHERE d.created_at < datetime(?, 'unixepoch')
        """,
            (dismissed_cutoff,),
        )["c"]

        old_count = self.fetchone(
            """
            SELECT COUNT(*) as c FROM stories s
            WHERE s.time < ?
            AND s.id NOT IN (SELECT story_id FROM read_later)
            AND s.id NOT IN (SELECT story_id FROM dismissed)
        """,
            (age_cutoff,),
        )["c"]

        # Build list of story IDs to delete (dismissed past grace period)
        dismissed_ids = [
            r["id"]
            for r in self.fetchall(
                """
                SELECT s.id FROM stories s
                INNER JOIN dismissed d ON s.id = d.story_id
                WHERE d.created_at < datetime(?, 'unixepoch')
            """,
                (dismissed_cutoff,),
            )
        ]

        # Build list of old story IDs to delete
        old_ids = [
            r["id"]
            for r in self.fetchall(
                """
                SELECT s.id FROM stories s
                WHERE s.time < ?
                AND s.id NOT IN (SELECT story_id FROM read_later)
                AND s.id NOT IN (SELECT story_id FROM dismissed)
            """,
                (age_cutoff,),
            )
        ]

        all_ids_to_delete = dismissed_ids + old_ids

        if all_ids_to_delete:
            # Delete from child tables FIRST (FK constraint compliance)
            # Note: dismissed table may have FK on older databases (before schema change)
            placeholders = ",".join("?" * len(all_ids_to_delete))
            self.execute(
                f"DELETE FROM history WHERE story_id IN ({placeholders})",
                all_ids_to_delete,
            )
            self.execute(
                f"DELETE FROM read_later WHERE story_id IN ({placeholders})",
                all_ids_to_delete,
            )
            self.execute(
                f"DELETE FROM dismissed WHERE story_id IN ({placeholders})",
                all_ids_to_delete,
            )

            # Now safe to delete stories
            self.execute(
                f"DELETE FROM stories WHERE id IN ({placeholders})",
                all_ids_to_delete,
            )

        # Clean up old dismissed markers (>60 days) - these story IDs won't reappear
        # in HN's new feed, so no risk of re-fetching
        marker_cutoff = int(time.time()) - (60 * 24 * 3600)
        self.execute(
            "DELETE FROM dismissed WHERE created_at < datetime(?, 'unixepoch')",
            (marker_cutoff,),
        )

        # Clean up old content cache
        self.execute(
            "DELETE FROM fetched_urls WHERE fetched_at < datetime(?, 'unixepoch')",
            (cache_cutoff,),
        )

        # Aggregate usage logs into monthly summaries before deleting
        # Get the cutoff month (6 months ago, not counting current)
        now = datetime.now()
        # First day of current month
        current_month_start = now.replace(
            day=1, hour=0, minute=0, second=0, microsecond=0
        )
        # 6 months ago
        usage_cutoff = current_month_start - timedelta(days=6 * 30)
        usage_cutoff_str = usage_cutoff.strftime("%Y-%m-%d")

        # Aggregate old usage logs into monthly summaries
        self.execute(
            """
            INSERT INTO usage_summary (month, request_count, total_browser_ms)
            SELECT
                strftime('%Y-%m', created_at) as month,
                COUNT(*) as request_count,
                SUM(browser_ms) as total_browser_ms
            FROM usage_log
            WHERE created_at < ?
            GROUP BY strftime('%Y-%m', created_at)
            ON CONFLICT(month) DO UPDATE SET
                request_count = usage_summary.request_count + excluded.request_count,
                total_browser_ms = usage_summary.total_browser_ms + excluded.total_browser_ms
        """,
            (usage_cutoff_str,),
        )

        # Delete old usage logs (now that they're summarized)
        self.execute("DELETE FROM usage_log WHERE created_at < ?", (usage_cutoff_str,))

        # Delete old usage summaries (>36 months)
        summary_cutoff = current_month_start - timedelta(days=36 * 30)
        summary_cutoff_str = summary_cutoff.strftime("%Y-%m")
        self.execute("DELETE FROM usage_summary WHERE month < ?", (summary_cutoff_str,))

        self.commit()

        total = dismissed_count + old_count
        if total > 0:
            log.info(
                f"Cleanup: {dismissed_count} dismissed, {old_count} old stories deleted"
            )

        return {"dismissed": dismissed_count, "old": old_count}

    def backup_rotate(self) -> Optional[str]:
        """
        Create a backup and rotate according to retention policy.

        Retention slots:
        - Hourly: 1h, 2h, 6h, 12h (4 files)
        - Daily: 1d-7d (7 files)
        - Weekly: 1w-4w (4 files)
        Total: 15 backup files max

        Returns the path of the new backup, or None if backup failed.
        """
        backup_dir = self.db_path.parent / "backups"
        backup_dir.mkdir(exist_ok=True)

        # Slot definitions: (filename, min_age_hours, max_age_hours)
        # A backup in slot X should be between min and max age
        hourly_slots = [
            ("backup-1h.db", 0, 2),
            ("backup-2h.db", 2, 6),
            ("backup-6h.db", 6, 12),
            ("backup-12h.db", 12, 24),
        ]
        daily_slots = [(f"backup-{i}d.db", 24 * i, 24 * (i + 1)) for i in range(1, 8)]
        weekly_slots = [
            (f"backup-{i}w.db", 24 * 7 * i, 24 * 7 * (i + 1)) for i in range(1, 5)
        ]

        all_slots = hourly_slots + daily_slots + weekly_slots

        def get_file_age_hours(path: Path) -> Optional[float]:
            if not path.exists():
                return None
            age_seconds = time.time() - path.stat().st_mtime
            return age_seconds / 3600

        def copy_file(src: Path, dst: Path):
            import shutil

            shutil.copy2(src, dst)

        # Create new backup using SQLite backup API
        new_backup = backup_dir / "backup-new.db"
        try:
            # Use SQLite's online backup API for consistency
            src_conn = self._get_conn()
            dst_conn = sqlite3.connect(str(new_backup))
            src_conn.backup(dst_conn)
            dst_conn.close()
        except Exception as e:
            log.error(f"Backup failed: {e}")
            return None

        # Rotate backups: work backwards through slots
        # Move older backups to their next slot if they've aged out
        for i in range(len(all_slots) - 1, -1, -1):
            slot_name, min_age, max_age = all_slots[i]
            slot_path = backup_dir / slot_name
            age = get_file_age_hours(slot_path)

            if age is None:
                continue  # No backup in this slot

            # If this backup is older than max_age, move to next slot or delete
            if age > max_age:
                if i < len(all_slots) - 1:
                    # Move to next slot (only delete source if copy succeeds)
                    next_slot_name = all_slots[i + 1][0]
                    next_slot_path = backup_dir / next_slot_name
                    try:
                        copy_file(slot_path, next_slot_path)
                        slot_path.unlink()
                    except Exception as e:
                        log.warning(f"Failed to rotate backup {slot_name}: {e}")
                else:
                    # Last slot, just delete (too old)
                    try:
                        slot_path.unlink()
                    except Exception as e:
                        log.warning(f"Failed to delete old backup {slot_name}: {e}")

        # Move new backup to 1h slot
        slot_1h = backup_dir / "backup-1h.db"
        if slot_1h.exists():
            # Current 1h might need to move to 2h first
            age = get_file_age_hours(slot_1h)
            if age and age >= 1:  # At least 1 hour old, eligible for 2h slot
                slot_2h = backup_dir / "backup-2h.db"
                try:
                    copy_file(slot_1h, slot_2h)
                except Exception as e:
                    log.warning(f"Failed to rotate 1h backup to 2h: {e}")
            try:
                slot_1h.unlink()
            except Exception as e:
                log.warning(f"Failed to remove old 1h backup: {e}")

        new_backup.rename(slot_1h)
        log.debug(f"Backup created: {slot_1h}")

        return str(slot_1h)

    def migrate_compress_content(self, batch_size: int = 100) -> dict:
        """
        Migrate existing uncompressed content to compressed format.

        SAFETY FEATURES:
        - Creates a backup before starting
        - Processes in batches with verification
        - Each row is verified after compression (decompress and compare)
        - Rolls back batch on any error
        - Returns stats on completion

        Returns dict with migration stats.
        """
        # Create backup first
        log.info("Creating backup before compression migration...")
        backup_path = self.backup_rotate()
        if not backup_path:
            raise RuntimeError("Failed to create backup before migration")
        log.info(f"Backup created: {backup_path}")

        # Count uncompressed content (doesn't start with z:)
        total = self.fetchone(
            f"SELECT COUNT(*) as c FROM stories WHERE content IS NOT NULL AND content != '' AND content NOT LIKE '{COMPRESS_PREFIX}%'"
        )["c"]

        if total == 0:
            log.info("No uncompressed content to migrate")
            return {"migrated": 0, "total": 0, "already_compressed": 0}

        log.info(f"Found {total} stories with uncompressed content")

        migrated = 0
        errors = 0

        while True:
            # Get next batch of uncompressed content
            rows = self.fetchall(
                f"""
                SELECT id, content FROM stories
                WHERE content IS NOT NULL AND content != '' AND content NOT LIKE '{COMPRESS_PREFIX}%'
                LIMIT ?
            """,
                (batch_size,),
            )

            if not rows:
                break

            batch_updates = []
            for row in rows:
                story_id = row["id"]
                original = row["content"]

                # Compress
                compressed = compress_content(original)

                # Verify by decompressing
                decompressed = decompress_content(compressed)
                if decompressed != original:
                    log.error(
                        f"Verification failed for story {story_id}: content mismatch!"
                    )
                    errors += 1
                    continue

                batch_updates.append((compressed, story_id))

            # Apply batch update
            if batch_updates:
                self.executemany(
                    "UPDATE stories SET content = ? WHERE id = ?",
                    batch_updates,
                )
                self.commit()
                migrated += len(batch_updates)
                log.info(f"Migrated {migrated}/{total} stories...")

        # Count already compressed
        already_compressed = self.fetchone(
            f"SELECT COUNT(*) as c FROM stories WHERE content LIKE '{COMPRESS_PREFIX}%'"
        )["c"]

        log.info(
            f"Migration complete: {migrated} migrated, {errors} errors, {already_compressed} total compressed"
        )

        # VACUUM to reclaim space after compression
        log.info("Running VACUUM to reclaim disk space...")
        self.vacuum()
        log.info("VACUUM complete")

        return {
            "migrated": migrated,
            "errors": errors,
            "total_compressed": already_compressed,
            "backup": backup_path,
        }

    def vacuum(self):
        """
        Reclaim disk space by rebuilding the database.

        Note: This requires exclusive access and temporary disk space
        roughly equal to the current database size.
        """
        self.execute("VACUUM")
        self.commit()

    def maybe_vacuum(
        self, min_free_pages: int = 1000, min_free_percent: float = 5.0
    ) -> bool:
        """
        Run VACUUM if there's significant free space to reclaim.

        Args:
            min_free_pages: Minimum free pages before vacuuming (default 1000 = ~4MB)
            min_free_percent: Minimum free space as % of total (default 5%)

        Returns:
            True if VACUUM was run, False if skipped.
        """
        free_pages = self.fetchone("PRAGMA freelist_count")["freelist_count"]
        total_pages = self.fetchone("PRAGMA page_count")["page_count"]

        if total_pages == 0:
            return False

        free_percent = (free_pages / total_pages) * 100

        if free_pages >= min_free_pages or free_percent >= min_free_percent:
            page_size = self.fetchone("PRAGMA page_size")["page_size"]
            free_mb = (free_pages * page_size) / 1024 / 1024
            log.info(
                f"Running VACUUM to reclaim {free_mb:.1f}MB ({free_percent:.1f}% free)"
            )
            self.vacuum()
            return True

        return False

    # --- History ---

    def add_to_history(self, story_id: int):
        self.execute(
            "INSERT OR REPLACE INTO history (story_id, opened_at) VALUES (?, CURRENT_TIMESTAMP)",
            (story_id,),
        )
        self.commit()

    # --- URL Dedup ---

    def get_cached_content(self, url: str) -> Optional[dict]:
        """Check if we already fetched this URL."""
        row = self.fetchone(
            "SELECT content, content_source, browser_ms FROM fetched_urls WHERE url = ?",
            (url,),
        )
        if row:
            return {
                "content": row["content"],
                "source": row["content_source"],
                "browser_ms": row["browser_ms"],
            }
        return None

    def cache_content(self, url: str, content: str, source: str, browser_ms: float = 0):
        """Cache fetched content for URL dedup."""
        self.execute(
            """
            INSERT OR REPLACE INTO fetched_urls (url, content, content_source, browser_ms, fetched_at)
            VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
        """,
            (url, content, source, browser_ms),
        )
        self.commit()

    # --- Usage Tracking ---

    def log_usage(self, story_id: int, url: str, browser_ms: float, source: str):
        """Log usage for billing tracking."""
        self.execute(
            """
            INSERT INTO usage_log (story_id, url, browser_ms, source) VALUES (?, ?, ?, ?)
        """,
            (story_id, url, browser_ms, source),
        )
        self.commit()

    def get_usage_stats(self) -> dict:
        """Get usage statistics for billing."""
        today = self.fetchone("""
            SELECT COALESCE(SUM(browser_ms), 0) as ms, COUNT(*) as requests
            FROM usage_log WHERE date(created_at) = date('now')
        """)
        week = self.fetchone("""
            SELECT COALESCE(SUM(browser_ms), 0) as ms, COUNT(*) as requests
            FROM usage_log WHERE created_at >= datetime('now', '-7 days')
        """)
        month = self.fetchone("""
            SELECT COALESCE(SUM(browser_ms), 0) as ms, COUNT(*) as requests
            FROM usage_log WHERE created_at >= datetime('now', '-30 days')
        """)
        total = self.fetchone("""
            SELECT COALESCE(SUM(browser_ms), 0) as ms, COUNT(*) as requests FROM usage_log
        """)
        by_source = self.fetchall("""
            SELECT source, COALESCE(SUM(browser_ms), 0) as ms, COUNT(*) as requests
            FROM usage_log GROUP BY source
        """)
        return {
            "today": {"browser_ms": today["ms"], "requests": today["requests"]},
            "week": {"browser_ms": week["ms"], "requests": week["requests"]},
            "month": {"browser_ms": month["ms"], "requests": month["requests"]},
            "total": {"browser_ms": total["ms"], "requests": total["requests"]},
            "by_source": {
                r["source"]: {"browser_ms": r["ms"], "requests": r["requests"]}
                for r in by_source
            },
        }

    # --- Stats ---

    def get_stats(self) -> dict:
        return {
            "total_stories": self.fetchone("SELECT COUNT(*) as c FROM stories")["c"],
            # pending_content includes 'pending', 'retry', and 'fetching' - all work remaining
            "pending_content": self.fetchone(
                "SELECT COUNT(*) as c FROM stories WHERE content_status IN ('pending', 'retry', 'fetching')"
            )["c"],
            "fetching_content": self.fetchone(
                "SELECT COUNT(*) as c FROM stories WHERE content_status = 'fetching'"
            )["c"],
            "done_content": self.fetchone(
                "SELECT COUNT(*) as c FROM stories WHERE content_status = 'done'"
            )["c"],
            "failed_content": self.fetchone(
                "SELECT COUNT(*) as c FROM stories WHERE content_status = 'failed'"
            )["c"],
            "blocked_content": self.fetchone(
                "SELECT COUNT(*) as c FROM stories WHERE content_status = 'blocked'"
            )["c"],
            "blocked_domains": self.fetchone(
                "SELECT COUNT(*) as c FROM blocked_domains"
            )["c"],
            "blocked_words": self.fetchone("SELECT COUNT(*) as c FROM blocked_words")[
                "c"
            ],
            "read_later": self.fetchone("SELECT COUNT(*) as c FROM read_later")["c"],
            "dismissed": self.fetchone("SELECT COUNT(*) as c FROM dismissed")["c"],
            "front_page_stories": self.fetchone(
                "SELECT COUNT(*) as c FROM stories WHERE hit_front_page = 1"
            )["c"],
        }

    # --- Front Page Tracking ---

    def update_front_page_stories(self, top_story_ids: list[int]) -> int:
        """
        Mark stories that appear in topstories as hitting the front page.
        Updates front_page_rank to the best (lowest) rank achieved.
        Returns number of stories updated.
        """
        if not top_story_ids:
            return 0

        updated = 0
        for rank, story_id in enumerate(
            top_story_ids[:30], start=1
        ):  # Top 30 = front page
            cursor = self.execute(
                """
                UPDATE stories
                SET hit_front_page = 1,
                    front_page_rank = CASE
                        WHEN front_page_rank IS NULL THEN ?
                        WHEN ? < front_page_rank THEN ?
                        ELSE front_page_rank
                    END,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ? AND (hit_front_page = 0 OR front_page_rank IS NULL OR front_page_rank > ?)
            """,
                (rank, rank, rank, story_id, rank),
            )
            updated += cursor.rowcount
        self.commit()
        return updated


# =============================================================================
# HN API Client
# =============================================================================


def extract_domain(url: Optional[str]) -> Optional[str]:
    if not url:
        return None
    try:
        parsed = urlparse(url)
        domain = parsed.netloc.lower()
        if domain.startswith("www."):
            domain = domain[4:]
        return domain
    except Exception:
        return None


def parse_algolia_story(hit: dict) -> Optional[dict]:
    """Parse Algolia API hit into our story format."""
    if hit.get("_tags") and "story" not in hit.get("_tags", []):
        return None
    return {
        "id": int(hit["objectID"]),
        "title": hit.get("title") or "[no title]",
        "url": hit.get("url"),
        "text": hit.get("story_text"),  # Self-post text (Ask HN, etc.)
        "domain": extract_domain(hit.get("url")),
        "by": hit.get("author") or "[deleted]",
        "time": hit.get("created_at_i", 0),
        "score": hit.get("points") or 0,
        "descendants": hit.get("num_comments") or 0,
    }


async def fetch_via_algolia(
    client: httpx.AsyncClient, since_time: int, on_progress=None
) -> tuple[list[dict], bool]:
    """
    Fetch stories via Algolia API using keyset pagination.
    Algolia limits results to ~1000 per query, so we paginate by time windows.
    Fetches ALL stories back to since_time.
    Returns (stories, success).
    """
    all_stories = []
    # Use keyset pagination: fetch batches, then use oldest timestamp as upper bound
    upper_bound = None  # No upper bound initially (fetch newest first)

    while True:  # Keep fetching until we reach since_time
        # Fetch one batch (up to 1000 results due to Algolia limit)
        batch_stories = []
        page = 0

        while len(batch_stories) < 1000 and page < 10:  # Algolia caps at ~10 pages
            try:
                # Build numeric filter
                filters = [f"created_at_i>{since_time}"]
                if upper_bound:
                    filters.append(f"created_at_i<{upper_bound}")

                url = (
                    f"https://hn.algolia.com/api/v1/search_by_date"
                    f"?tags=story"
                    f"&numericFilters={','.join(filters)}"
                    f"&hitsPerPage={ALGOLIA_PAGE_SIZE}"
                    f"&page={page}"
                )
                resp = await client.get(url, timeout=30)
                resp.raise_for_status()
                data = resp.json()

                hits = data.get("hits", [])
                if not hits:
                    break

                for hit in hits:
                    story = parse_algolia_story(hit)
                    if story and story["time"] > since_time:
                        batch_stories.append(story)

                page += 1
                total_pages = data.get("nbPages", 0)
                if page >= total_pages:
                    break

                await asyncio.sleep(0.1)  # Be nice to Algolia

            except Exception as e:
                log.error(f"Algolia API error: {e}")
                if page == 0 and not all_stories:
                    return [], False  # Complete failure on first request
                break  # Partial success, continue with what we have

        if not batch_stories:
            break  # No more stories to fetch

        all_stories.extend(batch_stories)
        log.info(
            f"Algolia batch: {len(batch_stories)} stories (total: {len(all_stories)})"
        )

        # Check if we've reached the time boundary
        oldest_in_batch = min(s["time"] for s in batch_stories)
        if oldest_in_batch <= since_time:
            break  # We've fetched everything back to the checkpoint

        # Check if this batch was incomplete (less than limit = we got everything)
        if len(batch_stories) < 1000:
            break  # Algolia returned all available stories

        # Set upper bound for next batch (keyset pagination)
        upper_bound = oldest_in_batch
        await asyncio.sleep(0.2)  # Brief pause between batches

    return all_stories, True


async def fetch_via_firebase(client: httpx.AsyncClient) -> list[int]:
    """Fetch list of new story IDs from Firebase API."""
    try:
        resp = await client.get(HN_NEW_STORIES, timeout=30)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        log.error(f"Firebase API error: {e}")
        return []


async def fetch_story_by_id(client: httpx.AsyncClient, story_id: int) -> Optional[dict]:
    """Fetch a single story by ID from Firebase API."""
    try:
        resp = await client.get(HN_ITEM.format(id=story_id), timeout=15)
        resp.raise_for_status()
        data = resp.json()
        if data is None or data.get("type") != "story":
            return None
        return {
            "id": data["id"],
            "title": data.get("title", "[no title]"),
            "url": data.get("url"),
            "text": data.get("text"),  # Self-post text (Ask HN, etc.)
            "domain": extract_domain(data.get("url")),
            "by": data.get("by", "[deleted]"),
            "time": data.get("time", 0),
            "score": data.get("score", 0),
            "descendants": data.get("descendants", 0),
        }
    except Exception:
        return None


async def fetch_via_firebase_newstories(
    client: httpx.AsyncClient,
    since_time: int,
    since_id: Optional[int],
    on_progress=None,
) -> tuple[list[dict], int]:
    """
    Fetch stories via Firebase newstories endpoint.
    Limited to ~500 most recent stories (API limitation).
    Returns (stories, max_id_seen).
    """
    stories = []
    max_id_seen = 0

    story_ids = await fetch_via_firebase(client)
    if not story_ids:
        return [], 0

    log.info(f"Firebase: fetching from {len(story_ids)} story IDs...")

    for i, sid in enumerate(story_ids):
        max_id_seen = max(max_id_seen, sid)

        # Stop if we've reached checkpoint ID
        if since_id and sid <= since_id:
            log.info(f"Firebase: reached checkpoint ID {since_id}")
            break

        story = await fetch_story_by_id(client, sid)
        if story:
            if story["time"] > since_time:
                stories.append(story)
            elif story["time"] <= since_time:
                # Reached time boundary
                log.info("Firebase: reached time boundary")
                break

        await asyncio.sleep(0.05)

    log.info(f"Firebase: fetched {len(stories)} stories")
    return stories, max_id_seen


async def fetch_via_id_walk(
    client: httpx.AsyncClient,
    start_id: int,
    since_time: int,
    since_id: Optional[int],
    on_progress=None,
) -> list[dict]:
    """
    Walk backwards from start_id fetching stories until we reach since_time.
    Slow but thorough fallback.
    """
    stories = []
    current_id = start_id
    consecutive_failures = 0
    max_consecutive_failures = 100  # Stop if we hit too many non-stories in a row

    log.info(f"ID walk: starting from {start_id}...")

    while consecutive_failures < max_consecutive_failures:
        if since_id and current_id <= since_id:
            log.info(f"ID walk: reached checkpoint ID {since_id}")
            break

        story = await fetch_story_by_id(client, current_id)

        if story:
            consecutive_failures = 0
            if story["time"] > since_time:
                stories.append(story)
                if len(stories) % 50 == 0:
                    log.info(f"ID walk: {len(stories)} stories so far...")
            elif story["time"] <= since_time:
                # We've gone past our time window
                log.info(f"ID walk: reached time boundary at ID {current_id}")
                break
        else:
            consecutive_failures += 1

        current_id -= 1
        await asyncio.sleep(ID_WALK_DELAY)  # Rate limiting

    if consecutive_failures >= max_consecutive_failures:
        log.info(
            f"ID walk: stopped after {max_consecutive_failures} consecutive non-stories"
        )

    log.info(f"ID walk: fetched {len(stories)} stories")
    return stories


async def fetch_new_stories(
    db: Database,
    lookback_hours: int = DEFAULT_LOOKBACK_HOURS,
) -> int:
    """
    Fetch stories from checkpoint to now, oldest to newest.

    - Checkpoint = MAX(time) from stories table (or now - lookback_hours if empty)
    - Fetches in time windows, inserting as we go (crash-safe)
    - Uses Algolia primarily, falls back to Firebase/ID-walk if needed
    """
    now = int(time.time())

    # Get checkpoint from stories table
    checkpoint_time = db.get_newest_story_time()
    if checkpoint_time:
        since_time = checkpoint_time
        log.info(f"Resuming from checkpoint: {checkpoint_time}")
    else:
        since_time = now - (lookback_hours * 3600)
        log.info(f"No stories found, looking back {lookback_hours} hours")

    if since_time >= now:
        log.info("Already up to date")
        return 0

    total_fetched = 0

    async with httpx.AsyncClient() as client:
        # Primary: Algolia (fetches oldest to newest via time windows)
        log.info(f"Fetching stories from {since_time} to {now}...")
        algolia_stories, algolia_ok = await fetch_via_algolia(client, since_time)

        if algolia_stories:
            # Sort oldest first and insert
            algolia_stories.sort(key=lambda s: s["time"])
            for story in algolia_stories:
                db.upsert_story(story)
            db.commit()
            total_fetched += len(algolia_stories)
            newest_time = max(s["time"] for s in algolia_stories)
            log.info(
                f"Algolia: inserted {len(algolia_stories)} stories, newest at {newest_time}"
            )

        # If Algolia failed, try Firebase for recent stories
        if not algolia_ok:
            log.warning("Algolia failed, trying Firebase...")
            firebase_stories, _ = await fetch_via_firebase_newstories(
                client, since_time, since_id=None
            )
            if firebase_stories:
                firebase_stories.sort(key=lambda s: s["time"])
                for story in firebase_stories:
                    db.upsert_story(story)
                db.commit()
                total_fetched += len(firebase_stories)
                log.info(f"Firebase: inserted {len(firebase_stories)} stories")

    log.info(f"Total: {total_fetched} stories fetched")
    return total_fetched


# =============================================================================
# Content Extraction (Cloudflare Browser Rendering API)
# =============================================================================

# Global domain rate limiter (shared across all workers)
domain_last_request: dict[str, float] = {}
domain_lock = asyncio.Lock()

# Global Cloudflare rate limit state (shared across all workers)
cf_rate_limit_until: float = 0  # UTC timestamp when CF rate limit expires
cf_rate_limit_lock = asyncio.Lock()

# Blocking detection patterns
BLOCKING_PATTERNS = [
    r"captcha",
    r"please verify",
    r"access denied",
    r"forbidden",
    r"rate limit",
    r"too many requests",
    r"blocked",
    r"unusual traffic",
    r"security check",
    r"ddos protection",
    r"challenge-platform",
    r"hcaptcha",
    r"recaptcha",
    r"just a moment",
    r"checking your browser",
    r"enable javascript",
    r"redirecting",
]
BLOCKING_REGEX = re.compile("|".join(BLOCKING_PATTERNS), re.IGNORECASE)

# Minimum content length to consider valid (very short = likely blocked)
MIN_CONTENT_LENGTH = 200

# Retry settings
MAX_RETRIES = 3
RETRY_BASE_DELAY = 30  # seconds, will exponentially increase


async def wait_for_domain_rate_limit(domain: str):
    """Wait if we've recently hit this domain."""
    async with domain_lock:
        now = time.time()
        last_request = domain_last_request.get(domain, 0)
        wait_time = DOMAIN_REQUEST_DELAY - (now - last_request)
        if wait_time > 0:
            await asyncio.sleep(wait_time)
        domain_last_request[domain] = time.time()


@dataclass
class FetchResult:
    content: Optional[str]
    status: str  # "done", "failed", "blocked", "timeout"
    source: str  # "cloudflare"
    browser_ms: float = 0
    error: Optional[str] = None


def detect_blocking(content: str) -> bool:
    """Detect if content looks like a blocking/captcha page."""
    if not content:
        return False

    # Very short content is suspicious
    if len(content) < MIN_CONTENT_LENGTH:
        if BLOCKING_REGEX.search(content):
            return True
        return False

    # Check for blocking patterns in first part of content
    first_chunk = content[:2000].lower()
    if BLOCKING_REGEX.search(first_chunk):
        if len(content) > 3000:
            return False
        return True

    return False


def get_next_utc_midnight() -> float:
    """Get timestamp of next UTC midnight."""
    now = datetime.now(timezone.utc)
    # Tomorrow at 00:00:00 UTC
    tomorrow = now.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(
        days=1
    )
    return tomorrow.timestamp()


async def fetch_content_cloudflare(url: str) -> FetchResult:
    """Fetch content via Cloudflare Browser Rendering API (returns markdown)."""
    global cf_quota_exceeded_until

    # Check if we're in quota exceeded state
    if cf_quota_exceeded_until > 0 and time.time() < cf_quota_exceeded_until:
        return FetchResult(
            None,
            "quota_exceeded",
            "cloudflare",
            0,
            "Daily quota exceeded, waiting for reset",
        )

    try:
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                CF_BROWSER_API,
                headers={
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {CF_API_TOKEN}",
                },
                json={
                    "url": url,
                    "gotoOptions": {
                        "timeout": cf_timeout_ms,
                    },
                },
                timeout=60,
            )

            if resp.status_code == 429:
                # Check if it's daily quota exceeded vs rate limit
                try:
                    data = resp.json()
                    errors = data.get("errors", [])
                    error_msg = str(errors)
                    if "time limit exceeded for today" in error_msg.lower():
                        cf_quota_exceeded_until = get_next_utc_midnight()
                        log.warning(
                            "Cloudflare daily quota exceeded, pausing until UTC midnight"
                        )
                        return FetchResult(
                            None,
                            "quota_exceeded",
                            "cloudflare",
                            0,
                            "Daily quota exceeded",
                        )
                except Exception:
                    pass
                # Regular rate limit - check Retry-After header
                retry_after = resp.headers.get("Retry-After", "60")
                return FetchResult(
                    None,
                    "rate_limited",
                    "cloudflare",
                    0,
                    f"Rate limited, retry after {retry_after}s",
                )

            if resp.status_code != 200:
                return FetchResult(
                    None, "failed", "cloudflare", 0, f"HTTP {resp.status_code}"
                )

            data = resp.json()
            if not data.get("success"):
                errors = data.get("errors", [])
                return FetchResult(None, "failed", "cloudflare", 0, str(errors))

            content = data.get("result", "")
            browser_ms = float(resp.headers.get("x-browser-ms-used", 0))

            if not content or len(content.strip()) < 50:
                return FetchResult(
                    None, "failed", "cloudflare", browser_ms, "Empty content"
                )

            if detect_blocking(content):
                return FetchResult(
                    content, "blocked", "cloudflare", browser_ms, "Blocking detected"
                )

            return FetchResult(content, "done", "cloudflare", browser_ms)

    except httpx.TimeoutException:
        return FetchResult(None, "timeout", "cloudflare", 0, "Request timed out")
    except Exception as e:
        return FetchResult(None, "failed", "cloudflare", 0, str(e))


async def fetch_content(url: str) -> FetchResult:
    """Fetch content via Cloudflare Browser Rendering API."""
    return await fetch_content_cloudflare(url)


async def content_worker(worker_id: int, db: Database, stop_event: asyncio.Event):
    """
    Content worker that fetches article content for stories.
    Multiple workers can run concurrently - uses atomic job claiming and
    shared rate limit state to coordinate.
    """
    global cf_rate_limit_until
    log.info(f"[worker-{worker_id}] started")

    idle_iterations = 0  # Track consecutive idle loops for periodic cleanup

    while not stop_event.is_set():
        # Check global CF rate limit (shared across all workers)
        # Reading a float is atomic; lock only needed for writes
        now = time.time()
        if cf_rate_limit_until > now:
            wait_time = cf_rate_limit_until - now
            log.info(
                f"[worker-{worker_id}] CF rate limited, waiting {wait_time:.0f}s..."
            )
            await asyncio.sleep(min(wait_time, 30))
            continue

        # Check daily quota exceeded (Free plan only)
        if cf_quota_exceeded_until > now:
            await asyncio.sleep(60)  # Check every minute
            continue

        # Atomically claim next job (safe for multiple workers)
        job = db.claim_next_content_job(max_attempts=MAX_RETRIES)
        if not job:
            idle_iterations += 1
            # Periodic cleanup when idle (only worker 1 does this to avoid duplicates)
            if worker_id == 1 and idle_iterations >= 12:  # ~60 seconds of idle
                idle_iterations = 0
                db.cleanup_stuck_content_jobs(max_attempts=MAX_RETRIES)
            await asyncio.sleep(5)  # No work, wait before checking again
            continue

        idle_iterations = 0  # Reset on work found
        story_id = job["id"]
        url = job["url"]
        domain = job.get("domain") or extract_domain(url)
        attempts = job["content_attempts"]

        try:
            # Check cache first (avoid fetching same URL twice)
            cached = db.get_cached_content(url)
            if cached:
                db.complete_content_job(
                    story_id,
                    cached["content"],
                    "done",
                    cached["source"],
                    cached["browser_ms"],
                )
                log.info(f"[worker-{worker_id}] cached {story_id} ({domain})")
                continue

            # Wait for domain rate limit (shared across all workers)
            # This ensures we don't hammer the same domain from multiple workers
            await wait_for_domain_rate_limit(domain)

            # Fetch content
            result = await fetch_content(url)

            if result.status == "done":
                db.cache_content(url, result.content, result.source, result.browser_ms)
                db.complete_content_job(
                    story_id, result.content, "done", result.source, result.browser_ms
                )
                db.log_usage(story_id, url, result.browser_ms, result.source)
                log.info(
                    f"[worker-{worker_id}] done {story_id} ({domain}) [{result.browser_ms:.0f}ms]"
                )

            elif result.status == "blocked":
                if attempts >= MAX_RETRIES:
                    db.complete_content_job(
                        story_id,
                        result.content or "",
                        "blocked",
                        result.source,
                        result.browser_ms,
                    )
                    if result.browser_ms > 0:
                        db.log_usage(story_id, url, result.browser_ms, result.source)
                    log.warning(
                        f"[worker-{worker_id}] BLOCKED {story_id} ({domain}) after {attempts} attempts"
                    )
                else:
                    db.retry_content_job(story_id)
                    log.info(
                        f"[worker-{worker_id}] blocked {story_id} ({domain}), attempt {attempts}/{MAX_RETRIES}"
                    )

            elif result.status == "timeout":
                if attempts >= MAX_RETRIES:
                    db.complete_content_job(story_id, "", "failed", result.source)
                    log.warning(
                        f"[worker-{worker_id}] FAILED {story_id} ({domain}) - timeouts after {attempts} attempts"
                    )
                else:
                    db.retry_content_job(story_id)
                    log.info(
                        f"[worker-{worker_id}] timeout {story_id} ({domain}), attempt {attempts}/{MAX_RETRIES}"
                    )

            elif result.status == "quota_exceeded":
                # Daily quota exceeded (Free plan) - put back and wait
                db.retry_content_job(story_id)
                log.warning(
                    f"[worker-{worker_id}] Quota exceeded, waiting for UTC midnight..."
                )
                await asyncio.sleep(60)

            elif result.status == "rate_limited":
                # Rate limited by CF - put back and set global delay
                db.retry_content_job(story_id)
                delay = 60  # default
                if result.error and "retry after" in result.error.lower():
                    try:
                        delay = int(result.error.split()[-1].rstrip("s"))
                    except (ValueError, IndexError):
                        pass
                async with cf_rate_limit_lock:
                    cf_rate_limit_until = time.time() + delay
                log.info(
                    f"[worker-{worker_id}] CF rate limited, all workers waiting {delay}s..."
                )

            else:
                # Other failure
                if attempts >= MAX_RETRIES:
                    db.complete_content_job(story_id, "", "failed", result.source)
                    log.warning(
                        f"[worker-{worker_id}] FAILED {story_id} ({domain}) - {result.error}"
                    )
                else:
                    db.retry_content_job(story_id)
                    log.info(
                        f"[worker-{worker_id}] failed {story_id} ({domain}), attempt {attempts}/{MAX_RETRIES}: {result.error}"
                    )

        except Exception as e:
            # Unexpected error - ensure job doesn't stay stuck in 'fetching'
            log.error(f"[worker-{worker_id}] unexpected error for {story_id}: {e}")
            try:
                if attempts >= MAX_RETRIES:
                    db.complete_content_job(story_id, "", "failed")
                else:
                    db.retry_content_job(story_id)
            except Exception:
                pass  # Best effort - cleanup will catch it later

    log.info(f"[worker-{worker_id}] stopped")


async def story_fetcher(
    db: Database, stop_event: asyncio.Event, interval_minutes: int = 60
):
    """Background task that fetches new stories on a schedule."""
    log.info(f"Story fetcher started (interval: {interval_minutes}m)")

    # Fetch immediately on start
    log.info("[fetcher] Initial fetch...")
    try:
        count = await fetch_new_stories(db)
        log.info(f"[fetcher] Initial fetch complete: {count} stories")
    except Exception as e:
        log.error(f"[fetcher] Initial fetch error: {e}")

    # Schedule subsequent fetches on the hour
    while not stop_event.is_set():
        # Calculate time until next interval
        now = datetime.now()
        # Next run at the top of the next interval
        minutes_until_next = interval_minutes - (now.minute % interval_minutes)
        if minutes_until_next == interval_minutes and now.second == 0:
            minutes_until_next = 0
        seconds_until_next = minutes_until_next * 60 - now.second

        if seconds_until_next > 0:
            log.debug(
                f"[fetcher] Next fetch in {seconds_until_next // 60}m {seconds_until_next % 60}s"
            )
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=seconds_until_next)
                break  # Stop event was set
            except asyncio.TimeoutError:
                pass  # Time to fetch

        if stop_event.is_set():
            break

        log.info(
            f"[fetcher] Scheduled fetch at {datetime.now().strftime('%H:%M:%S')}..."
        )
        try:
            count = await fetch_new_stories(db)
            log.info(f"[fetcher] Fetched {count} new stories")
        except Exception as e:
            log.error(f"[fetcher] Fetch error: {e}")

    log.info("Story fetcher stopped")


async def front_page_tracker(db: Database, stop_event: asyncio.Event):
    """Background task that polls topstories to track which stories hit the front page."""
    log.info(f"Front page tracker started (interval: {FRONT_PAGE_POLL_INTERVAL}s)")

    while not stop_event.is_set():
        try:
            async with httpx.AsyncClient() as client:
                resp = await client.get(HN_TOP_STORIES, timeout=30)
                resp.raise_for_status()
                top_ids = resp.json()

                if top_ids:
                    updated = db.update_front_page_stories(top_ids)
                    if updated > 0:
                        log.info(f"[front-page] Updated {updated} stories")

        except Exception as e:
            log.error(f"[front-page] Error polling topstories: {e}")

        # Wait for next poll
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=FRONT_PAGE_POLL_INTERVAL)
            break  # Stop event was set
        except asyncio.TimeoutError:
            pass  # Time to poll again

    log.info("Front page tracker stopped")


async def story_cleanup(db: Database, stop_event: asyncio.Event):
    """Background task that periodically cleans up old stories and creates backups."""
    cleanup_interval = 3600  # Run every hour
    vacuum_interval = 24  # Run vacuum every 24 cleanups (daily)
    cleanup_count = 0

    log.info(
        f"Story cleanup started (dismissed: {CLEANUP_DISMISSED_HOURS}h, max age: {CLEANUP_STORY_DAYS}d, cache: {CLEANUP_CONTENT_CACHE_DAYS}d)"
    )

    # Run cleanup and backup immediately on startup
    db.cleanup_stories(
        CLEANUP_DISMISSED_HOURS, CLEANUP_STORY_DAYS, CLEANUP_CONTENT_CACHE_DAYS
    )
    db.backup_rotate()
    cleanup_count += 1

    while not stop_event.is_set():
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=cleanup_interval)
            break  # Stop event was set
        except asyncio.TimeoutError:
            pass  # Time to run cleanup

        db.cleanup_stories(
            CLEANUP_DISMISSED_HOURS, CLEANUP_STORY_DAYS, CLEANUP_CONTENT_CACHE_DAYS
        )
        db.backup_rotate()
        cleanup_count += 1

        # Run vacuum daily (every 24 cleanups) if there's significant free space
        if cleanup_count % vacuum_interval == 0:
            db.maybe_vacuum()

    log.info("Story cleanup stopped")


# =============================================================================
# FastAPI Application
# =============================================================================

# Global state
db: Database = None
content_workers: list[asyncio.Task] = []
story_fetcher_task: asyncio.Task = None
front_page_tracker_task: asyncio.Task = None
cleanup_task: asyncio.Task = None
stop_event: asyncio.Event = None
fetch_status: dict = {"status": "idle", "progress": 0, "total": 0, "fetched": 0}
cf_quota_exceeded_until: float = 0  # UTC timestamp when quota resets (0 = not exceeded)
cf_timeout_ms: int = CF_BROWSER_TIMEOUT_MS  # Configurable via --cf-timeout

# Content worker configuration
# Paid plan: up to 180 req/min from CF, but domain rate limiting (2s/domain) is the real limit
# Multiple workers help when fetching from many different domains
DEFAULT_CONTENT_WORKERS = 3


@asynccontextmanager
async def lifespan(app: FastAPI):
    global \
        db, \
        content_workers, \
        stop_event, \
        story_fetcher_task, \
        front_page_tracker_task, \
        cleanup_task

    # Initialize
    db = Database(DB_FILE)
    db.init()
    stop_event = asyncio.Event()

    # Clean up any stuck content jobs from previous run (e.g., server crash)
    db.cleanup_stuck_content_jobs(max_attempts=MAX_RETRIES)

    # Start story fetcher (background, non-blocking)
    # Fetches immediately then on schedule
    story_fetcher_task = asyncio.create_task(
        story_fetcher(db, stop_event, app.state.fetch_interval)
    )

    # Start front page tracker (polls topstories to detect front page hits)
    front_page_tracker_task = asyncio.create_task(front_page_tracker(db, stop_event))

    # Start story cleanup task (removes old/dismissed stories)
    cleanup_task = asyncio.create_task(story_cleanup(db, stop_event))

    # Start content workers
    # Multiple workers can run concurrently - they coordinate via:
    # - Atomic job claiming (UPDATE...RETURNING)
    # - Shared domain rate limiter (2s between requests to same domain)
    # - Shared CF rate limit state (all workers pause if CF returns 429)
    num_workers = app.state.num_workers
    for i in range(num_workers):
        task = asyncio.create_task(content_worker(i + 1, db, stop_event))
        content_workers.append(task)
    log.info(f"Started {num_workers} content worker(s)")

    log.info(f"Server ready - http://127.0.0.1:{app.state.port}")

    yield

    # Shutdown
    log.info("Shutting down...")
    stop_event.set()

    # Cancel story fetcher
    if story_fetcher_task:
        story_fetcher_task.cancel()
        try:
            await story_fetcher_task
        except asyncio.CancelledError:
            pass

    # Cancel front page tracker
    if front_page_tracker_task:
        front_page_tracker_task.cancel()
        try:
            await front_page_tracker_task
        except asyncio.CancelledError:
            pass

    # Cancel cleanup task
    if cleanup_task:
        cleanup_task.cancel()
        try:
            await cleanup_task
        except asyncio.CancelledError:
            pass

    # Cancel content workers
    for task in content_workers:
        task.cancel()
    await asyncio.gather(*content_workers, return_exceptions=True)
    log.info("Shutdown complete")


app = FastAPI(lifespan=lifespan)
app.state.fetch_interval = FETCH_INTERVAL_MINUTES
app.state.port = 8000
app.state.num_workers = DEFAULT_CONTENT_WORKERS
app.state.auth_user = None
app.state.auth_pass = None


# Basic auth middleware
class BasicAuthMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        # Skip auth if not configured
        if not app.state.auth_user or not app.state.auth_pass:
            return await call_next(request)

        # Check Authorization header
        auth_header = request.headers.get("Authorization")
        if auth_header and auth_header.startswith("Basic "):
            try:
                credentials = base64.b64decode(auth_header[6:]).decode("utf-8")
                username, password = credentials.split(":", 1)
                # Use constant-time comparison to prevent timing attacks
                if secrets.compare_digest(
                    username, app.state.auth_user
                ) and secrets.compare_digest(password, app.state.auth_pass):
                    return await call_next(request)
            except Exception:
                pass

        # Request authentication
        return Response(
            content="Authentication required",
            status_code=401,
            headers={"WWW-Authenticate": 'Basic realm="HN New"'},
        )


app.add_middleware(BasicAuthMiddleware)


# Request logging middleware - logs with real client IP
class RequestLoggingMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        # Get real client IP (check X-Forwarded-For for reverse proxy)
        forwarded = request.headers.get("X-Forwarded-For")
        if forwarded:
            # X-Forwarded-For can be comma-separated list, first is original client
            client_ip = forwarded.split(",")[0].strip()
        else:
            client_ip = request.client.host if request.client else "-"

        # Process request
        start_time = time.time()
        response = await call_next(request)
        duration_ms = (time.time() - start_time) * 1000

        # Log request (skip noisy SSE endpoint)
        if not request.url.path.endswith("/updates"):
            log.info(
                f'{client_ip} "{request.method} {request.url.path}" {response.status_code} {duration_ms:.0f}ms'
            )

        return response


app.add_middleware(RequestLoggingMiddleware)


# Serve frontend static files (from directory or zip)
# Custom handler to serve from zip file
class ZipStaticFiles:
    def __init__(self, zip_path: Path):
        self.zip_path = zip_path
        self._zip = None

    def _get_zip(self):
        if self._zip is None:
            self._zip = zipfile.ZipFile(self.zip_path, "r")
        return self._zip

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            return
        path = scope["path"].removeprefix("/static/")
        if not path:
            path = "index.html"

        try:
            zf = self._get_zip()
            content = zf.read(path)

            # Determine content type
            content_type = "application/octet-stream"
            if path.endswith(".html"):
                content_type = "text/html; charset=utf-8"
            elif path.endswith(".css"):
                content_type = "text/css; charset=utf-8"
            elif path.endswith(".js"):
                content_type = "application/javascript; charset=utf-8"
            elif path.endswith(".json"):
                content_type = "application/json"
            elif path.endswith(".png"):
                content_type = "image/png"
            elif path.endswith(".svg"):
                content_type = "image/svg+xml"

            await send(
                {
                    "type": "http.response.start",
                    "status": 200,
                    "headers": [[b"content-type", content_type.encode()]],
                }
            )
            await send({"type": "http.response.body", "body": content})
        except KeyError:
            await send(
                {
                    "type": "http.response.start",
                    "status": 404,
                    "headers": [[b"content-type", b"text/plain"]],
                }
            )
            await send({"type": "http.response.body", "body": b"Not found"})


# Prefer directory, fall back to zip
if FRONTEND_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(FRONTEND_DIR)), name="static")
    templates = Jinja2Templates(directory=str(FRONTEND_DIR))
elif FRONTEND_ZIP.exists():
    app.mount("/static", ZipStaticFiles(FRONTEND_ZIP))
    # For templates, extract index.html to temp location
    _temp_dir = tempfile.mkdtemp()
    with zipfile.ZipFile(FRONTEND_ZIP, "r") as zf:
        zf.extract("index.html", _temp_dir)
    templates = Jinja2Templates(directory=_temp_dir)
else:
    templates = None
    log.warning("No frontend found (neither frontend/ dir nor ui.zip)")


# --- HTML Routes ---


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    if templates is None:
        return HTMLResponse(
            "<h1>Frontend not found</h1><p>Create frontend/ directory or provide ui.zip</p>"
        )
    return templates.TemplateResponse("index.html", {"request": request})


# --- API Routes ---


def parse_cursor(cursor: str) -> tuple[int, int] | tuple[None, None]:
    """Parse cursor string 'time:id' into (time, id) tuple."""
    if not cursor:
        return None, None
    try:
        parts = cursor.split(":")
        if len(parts) == 2:
            return int(parts[0]), int(parts[1])
    except (ValueError, IndexError):
        pass
    return None, None


@app.get("/api/stories")
async def get_stories(
    dismissed_only: bool = False,
    include_blocked: bool = False,
    include_read_later: bool = False,
    read_later_only: bool = False,
    limit: int = 50,
    cursor: str = None,
    sort: str = "newest",
):
    try:
        cursor_time, cursor_id = parse_cursor(cursor)
        if read_later_only:
            return db.get_read_later(
                dismissed_only,
                limit=limit,
                cursor_time=cursor_time,
                cursor_id=cursor_id,
                sort=sort,
            )
        return db.get_stories(
            dismissed_only,
            include_blocked,
            include_read_later,
            limit=limit,
            cursor_time=cursor_time,
            cursor_id=cursor_id,
            sort=sort,
        )
    except Exception as e:
        log.exception(f"Error in get_stories: {e}")
        raise


@app.get("/api/story/{story_id}")
async def get_story(story_id: int):
    row = db.fetchone("SELECT * FROM stories WHERE id = ?", (story_id,))
    if not row:
        raise HTTPException(404, "Story not found")
    story = dict(row)
    if story.get("content"):
        story["content"] = decompress_content(story["content"])
    return story


@app.get("/api/story/{story_id}/content")
async def get_story_content(story_id: int):
    row = db.fetchone(
        "SELECT content, content_status FROM stories WHERE id = ?", (story_id,)
    )
    if not row:
        raise HTTPException(404, "Story not found")
    content = decompress_content(row["content"]) if row["content"] else row["content"]
    return {"content": content, "status": row["content_status"]}


@app.post("/api/story/{story_id}/opened")
async def mark_story_opened(story_id: int):
    db.add_to_history(story_id)
    return {"ok": True}


# --- Blocked domains ---


@app.get("/api/blocked/domains")
async def get_blocked_domains():
    return db.get_blocked_domains()


@app.post("/api/blocked/domains")
async def add_blocked_domain(domain: str = Query(...)):
    db.add_blocked_domain(domain)
    return {"ok": True}


@app.delete("/api/blocked/domains")
async def remove_blocked_domain(domain: str = Query(...)):
    db.remove_blocked_domain(domain)
    return {"ok": True}


# --- Blocked words ---


@app.get("/api/blocked/words")
async def get_blocked_words():
    return db.get_blocked_words()


@app.post("/api/blocked/words")
async def add_blocked_word(word: str = Query(...)):
    db.add_blocked_word(word)
    return {"ok": True}


@app.delete("/api/blocked/words")
async def remove_blocked_word(word: str = Query(...)):
    db.remove_blocked_word(word)
    return {"ok": True}


# --- Merit words ---


@app.get("/api/merit/words")
async def get_merit_words():
    return db.get_merit_words()


@app.post("/api/merit/words")
async def add_merit_word(word: str = Query(...), weight: int = Query(1)):
    db.add_merit_word(word, weight)
    return {"ok": True}


@app.delete("/api/merit/words")
async def remove_merit_word(word: str = Query(...)):
    db.remove_merit_word(word)
    return {"ok": True}


# --- Demerit words ---


@app.get("/api/demerit/words")
async def get_demerit_words():
    return db.get_demerit_words()


@app.post("/api/demerit/words")
async def add_demerit_word(word: str = Query(...), weight: int = Query(1)):
    db.add_demerit_word(word, weight)
    return {"ok": True}


@app.delete("/api/demerit/words")
async def remove_demerit_word(word: str = Query(...)):
    db.remove_demerit_word(word)
    return {"ok": True}


# --- Merit domains ---


@app.get("/api/merit/domains")
async def get_merit_domains():
    return db.get_merit_domains()


@app.post("/api/merit/domains")
async def add_merit_domain(domain: str = Query(...), weight: int = Query(1)):
    db.add_merit_domain(domain, weight)
    return {"ok": True}


@app.delete("/api/merit/domains")
async def remove_merit_domain(domain: str = Query(...)):
    db.remove_merit_domain(domain)
    return {"ok": True}


# --- Demerit domains ---


@app.get("/api/demerit/domains")
async def get_demerit_domains():
    return db.get_demerit_domains()


@app.post("/api/demerit/domains")
async def add_demerit_domain(domain: str = Query(...), weight: int = Query(1)):
    db.add_demerit_domain(domain, weight)
    return {"ok": True}


@app.delete("/api/demerit/domains")
async def remove_demerit_domain(domain: str = Query(...)):
    db.remove_demerit_domain(domain)
    return {"ok": True}


# --- Read later ---


@app.get("/api/readlater")
async def get_read_later(
    dismissed_only: bool = False,
    limit: int = 50,
    cursor: str = None,
    sort: str = "newest",
):
    cursor_time, cursor_id = parse_cursor(cursor)
    return db.get_read_later(
        dismissed_only,
        limit=limit,
        cursor_time=cursor_time,
        cursor_id=cursor_id,
        sort=sort,
    )


@app.post("/api/readlater/{story_id}")
async def add_read_later(story_id: int):
    db.add_read_later(story_id)
    return {"ok": True}


@app.delete("/api/readlater/{story_id}")
async def remove_read_later(story_id: int):
    db.remove_read_later(story_id)
    return {"ok": True}


# --- Dismissed ---


@app.post("/api/dismiss/{story_id}")
async def dismiss_story(story_id: int):
    db.dismiss_story(story_id)
    return {"ok": True}


@app.delete("/api/dismiss/{story_id}")
async def undismiss_story(story_id: int):
    db.undismiss_story(story_id)
    return {"ok": True}


@app.delete("/api/dismiss")
async def clear_dismissed():
    db.clear_dismissed()
    return {"ok": True}


# --- Stats and Status ---


@app.get("/api/stats")
async def get_stats():
    return db.get_stats()


@app.get("/api/usage")
async def get_usage():
    """Get Cloudflare browser rendering usage statistics."""
    return db.get_usage_stats()


@app.get("/api/status")
async def get_status():
    quota_info = None
    if cf_quota_exceeded_until > 0:
        remaining = cf_quota_exceeded_until - time.time()
        if remaining > 0:
            quota_info = {
                "exceeded": True,
                "resets_in_seconds": int(remaining),
            }
    return {
        "fetch": fetch_status,
        "workers": len(content_workers),
        "cf_quota": quota_info,
        "cf_timeout_ms": cf_timeout_ms,
    }


@app.post("/api/fetch")
async def trigger_fetch():
    """Trigger a fetch of new stories (fetches all back to checkpoint)."""
    global fetch_status

    if fetch_status["status"] == "fetching":
        return {"ok": False, "error": "Already fetching"}

    fetch_status = {"status": "fetching", "progress": 0, "total": 0, "fetched": 0}

    try:
        fetched = await fetch_new_stories(db)
        fetch_status = {"status": "done", "progress": 0, "total": 0, "fetched": fetched}
        return {"ok": True, "fetched": fetched}
    except Exception as e:
        fetch_status = {"status": "error", "error": str(e)}
        raise HTTPException(500, str(e))


@app.post("/api/batch")
async def batch_requests(request: Request):
    """Process multiple API requests in a single call (for UI batching)."""
    try:
        body = await request.json()
        requests_list = body.get("requests", [])

        for req in requests_list:
            method = req.get("method", "").upper()
            path = req.get("path", "")

            # Route to appropriate handler based on path pattern
            if path.startswith("/api/dismiss/"):
                story_id = int(path.split("/")[-1])
                if method == "POST":
                    db.dismiss_story(story_id)
                elif method == "DELETE":
                    db.undismiss_story(story_id)
            elif path.startswith("/api/readlater/"):
                story_id = int(path.split("/")[-1])
                if method == "POST":
                    db.add_read_later(story_id)
                elif method == "DELETE":
                    db.remove_read_later(story_id)
            elif path.startswith("/api/blocked/domains") and method == "POST":
                # Parse domain from query string
                if "domain=" in path:
                    domain = path.split("domain=")[-1].split("&")[0]
                    db.add_blocked_domain(unquote(domain))

        return {"ok": True, "processed": len(requests_list)}
    except Exception as e:
        raise HTTPException(400, str(e))


@app.get("/api/stories/updates")
async def get_story_updates():
    """Get stories with recently updated content (for efficient polling)."""
    # Return stories updated in the last minute with content
    rows = db.fetchall("""
        SELECT id, title, content, content_status
        FROM stories
        WHERE content_status IN ('done', 'blocked', 'failed')
          AND updated_at >= datetime('now', '-60 seconds')
        ORDER BY updated_at DESC
        LIMIT 50
    """)
    result = []
    for row in rows:
        story = dict(row)
        # Decompress content and generate teaser
        if story.get("content"):
            content = decompress_content(story["content"])
            story["content"] = content
            teaser = content[:TEASER_LENGTH].strip()
            if len(content) > TEASER_LENGTH:
                teaser += "..."
            story["teaser"] = teaser
        else:
            story["teaser"] = None
        result.append(story)
    return result


# =============================================================================
# Main
# =============================================================================


async def main_async(args):
    global db

    # Validate Cloudflare credentials
    if not CF_ACCOUNT_ID or not CF_API_TOKEN:
        log.warning("CF_ACCOUNT_ID and CF_API_TOKEN not set!")
        log.warning("Content extraction will not work. Set these in .env file.")

    # Initialize DB
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    db = Database(DB_FILE)
    db.init()

    # Handle reset - clears all stories to start fresh
    if args.reset:
        db.execute("DELETE FROM stories")
        db.execute("DELETE FROM fetched_urls")
        db.execute("DELETE FROM usage_log")
        db.execute("DELETE FROM dismissed")
        db.execute("DELETE FROM history")
        db.commit()
        log.info(
            "Reset complete - all stories cleared. Next fetch will look back from now."
        )
        return

    # Configure app state
    app.state.fetch_interval = args.fetch_interval
    app.state.port = args.port
    app.state.num_workers = args.num_workers
    app.state.auth_user = args.user
    app.state.auth_pass = args.password
    app.state.cf_timeout = min(args.cf_timeout, 60000)  # Cap at 60s (CF max)

    # Set global for use in fetch function
    global cf_timeout_ms
    cf_timeout_ms = app.state.cf_timeout

    # Start server
    host = "0.0.0.0" if args.public else "127.0.0.1"
    log.info(f"Starting server on http://{host}:{args.port}")
    if args.user:
        log.info(f"Basic auth enabled (user: {args.user})")

    import uvicorn

    config = uvicorn.Config(
        app,
        host=host,
        port=args.port,
        log_level="info",
        log_config=None,
        access_log=False,
    )
    server = uvicorn.Server(config)
    await server.serve()


def main():
    parser = argparse.ArgumentParser(description="HN New Story Browser")
    parser.add_argument("--port", type=int, default=8000, help="Server port")
    parser.add_argument(
        "--public", action="store_true", help="Bind to 0.0.0.0 (all interfaces)"
    )
    parser.add_argument(
        "--reset", action="store_true", help="Reset checkpoint to now (fetch nothing)"
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=None,
        help=f"Number of content workers (default: {DEFAULT_CONTENT_WORKERS})",
    )
    parser.add_argument(
        "--migrate-compress",
        action="store_true",
        help="Migrate existing content to compressed format (creates backup first)",
    )
    parser.add_argument(
        "--vacuum",
        action="store_true",
        help="Reclaim disk space by rebuilding the database (run after cleanup)",
    )
    args = parser.parse_args()

    # Handle migration mode (run and exit, don't start server)
    if args.migrate_compress:
        db = Database(DB_FILE)
        db.init()
        result = db.migrate_compress_content()
        print(f"Migration complete: {result}")
        return

    # Handle vacuum mode (run and exit, don't start server)
    if args.vacuum:
        db = Database(DB_FILE)
        db.init()
        db_path = Path(DB_FILE)
        size_before = db_path.stat().st_size if db_path.exists() else 0
        print(f"Database size before: {size_before / 1024 / 1024:.2f} MB")
        print("Running VACUUM (this may take a moment)...")
        db.vacuum()
        size_after = db_path.stat().st_size
        saved = size_before - size_after
        print(f"Database size after: {size_after / 1024 / 1024:.2f} MB")
        print(
            f"Space reclaimed: {saved / 1024 / 1024:.2f} MB ({saved / size_before * 100:.1f}%)"
            if size_before > 0
            else ""
        )
        return

    # All config from env (see .env.example)
    args.user = os.environ.get("HN_USER")
    args.password = os.environ.get("HN_PASSWORD")
    args.fetch_interval = int(
        os.environ.get("HN_FETCH_INTERVAL", FETCH_INTERVAL_MINUTES)
    )
    args.cf_timeout = min(
        int(os.environ.get("CF_BROWSER_TIMEOUT_MS", CF_BROWSER_TIMEOUT_MS)), 60000
    )
    # Worker count: CLI arg > env var > default
    if args.workers is not None:
        args.num_workers = args.workers
    else:
        args.num_workers = int(
            os.environ.get("HN_CONTENT_WORKERS", DEFAULT_CONTENT_WORKERS)
        )

    try:
        asyncio.run(main_async(args))
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass  # Graceful shutdown already handled in lifespan


if __name__ == "__main__":
    main()
