# HN New

A personal HackerNews "new" story browser with content extraction.

Fetches new stories on a schedule, extracts article content via Cloudflare Browser Rendering API, and serves a fast keyboard-navigable UI for scanning through stories.

## Features

- **Story fetching**: Pulls from HN Algolia API with Firebase/ID-walking fallbacks
- **Content extraction**: Cloudflare Browser Rendering API (returns markdown)
- **Front page tracking**: Monitors HN front page and highlights stories that make it
- **Self-post support**: Ask HN, Tell HN, and other text posts display inline
- **Scheduled fetching**: Hourly by default, configurable
- **Keyboard navigation**: vim-style (j/k) with auto-expanding content
- **Mobile-friendly**: Card-based feed with tap zones, FAB menu, bottom sheet
- **Filtering**: Block domains/words, merit/demerit scoring
- **Read later**: Save stories for later reading
- **Activity stats**: Track dismiss/save/expand actions (hour/day/week)
- **Offline detection**: Shows banner when offline, auto-refreshes when back
- **Usage tracking**: Monitor Cloudflare API usage for billing
- **Quota handling**: Pauses extraction when daily free tier limit is reached
- **Sidebar layout**: Compact header in left sidebar to maximize reading space

## Quick Start

```bash
# Clone
git clone https://github.com/yourusername/hnnew.git
cd hnnew

# Configure (required: Cloudflare credentials)
cp .env.example .env
nano .env  # Add your CF_ACCOUNT_ID and CF_API_TOKEN

# Run (requires uv - https://github.com/astral-sh/uv)
./hn_new.py
```

Open http://localhost:8000

The server will:
1. Start immediately (ready to serve UI)
2. Begin fetching stories in the background
3. Continue fetching every hour automatically

## Installation

### Prerequisites

- Python 3.11+
- [uv](https://github.com/astral-sh/uv) for running the script
- Cloudflare account with Browser Rendering API access

### From Release

Download the latest release zip containing `hn_new.py` and `ui.zip`:

```bash
unzip hn-new-vX.X.X.zip
cp .env.example .env
# Edit .env with your credentials
./hn_new.py
```

### From Source

```bash
git clone https://github.com/yourusername/hnnew.git
cd hnnew
cp .env.example .env
# Edit .env
./hn_new.py
```

## Configuration

All configuration via `.env` file (see `.env.example`):

| Variable | Description | Default |
|----------|-------------|---------|
| `CF_ACCOUNT_ID` | Cloudflare account ID | (required) |
| `CF_API_TOKEN` | Cloudflare API token | (required) |
| `HN_USER` | Basic auth username | (none) |
| `HN_PASSWORD` | Basic auth password | (none) |
| `CF_BROWSER_TIMEOUT_MS` | Page load timeout in ms | 2000 |
| `HN_FETCH_INTERVAL` | Minutes between fetches | 60 |
| `HN_CONTENT_WORKERS` | Content extraction workers | 3 |
| `CLEANUP_DISMISSED_HOURS` | Hours before dismissed stories are deleted | 24 |
| `CLEANUP_STORY_DAYS` | Days before old stories are deleted (read later exempt) | 14 |
| `CLEANUP_CONTENT_CACHE_DAYS` | Days before cached content is deleted | 90 |

### CLI Options

| Option | Description |
|--------|-------------|
| `--port PORT` | Server port (default: 8000) |
| `--public` | Bind to 0.0.0.0 (all interfaces) |
| `--reset` | Clear all stories and start fresh |
| `--workers N` | Number of content workers (default: 3) |
| `--migrate-compress` | Compress existing content (creates backup first, run once) |
| `--vacuum` | Reclaim disk space immediately (auto-runs daily if needed) |

### Keyboard Shortcuts

| Key | Action |
|-----|--------|
| `j` / `k` | Next / Previous story |
| `o` | Open story link |
| `Enter` | Open HN comments |
| `e` | Expand/collapse content |
| `r` | Toggle read later |
| `d` | Dismiss story |
| `b` | Block domain |
| `f` | Fetch new stories |
| `1-3` | Switch views (All/Read Later/Settings) |
| `?` | Toggle keyboard help |

## Deployment

### Directory Structure

```
/opt/hn-new/
├── hn_new.py      # Main script
├── ui.zip         # Frontend assets
├── .env           # Secrets (CF credentials, auth)
└── .hn_data/      # Data directory (created automatically)
    ├── hn.db      # SQLite database
    └── backups/   # Hourly/daily/weekly backups (15 files max)
```

### Step-by-Step Deployment

1. **Create directory and copy files:**
```bash
sudo mkdir -p /opt/hn-new
sudo chown www-data:www-data /opt/hn-new
scp hn_new.py ui.zip .env.example user@server:/opt/hn-new/
```

2. **Create .env with your credentials:**
```bash
sudo -u www-data cp /opt/hn-new/.env.example /opt/hn-new/.env
sudo -u www-data nano /opt/hn-new/.env
# Fill in: CF_ACCOUNT_ID, CF_API_TOKEN, HN_USER, HN_PASSWORD
sudo chmod 600 /opt/hn-new/.env
```

3. **Install uv on server:**
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

4. **Set up systemd service:**
```bash
sudo cp hn-new.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now hn-new
```

5. **Check status:**
```bash
sudo systemctl status hn-new
sudo journalctl -u hn-new -f
```

### HTTPS with Caddy

Add to `/etc/caddy/Caddyfile`:

```
hn.yourdomain.com {
    reverse_proxy localhost:8000
    encode gzip
}
```

Caddy automatically provisions Let's Encrypt certificates.

## Development

```bash
# Run locally
make run

# Lint and format
make lint
make format

# Build UI zip for deployment
make ui.zip

# Build release archive
make release

# Clean build artifacts
make clean
```

### GitHub Releases

Releases are automated via GitHub Actions. To create a release:

```bash
git tag v1.0.0
git push origin v1.0.0
```

This triggers the workflow which builds and publishes:
- `hn-new-v1.0.0.zip` - Complete archive with all files
- `hn_new.py` - Standalone Python script
- `ui.zip` - Frontend assets

## Architecture

- **Single Python file** (`hn_new.py`) - all backend code
- **Frontend** (`frontend/` or `ui.zip`) - static HTML/CSS/JS
- **SQLite database** (`.hn_data/hn.db`) - stories, settings, usage tracking

### Background Tasks

1. **Story Fetcher**: Runs on startup + hourly, fetches new stories from HN
2. **Content Workers**: Multiple workers extract article content via Cloudflare (rate-limited per domain)
3. **Front Page Tracker**: Polls HN front page every 5 minutes to track which stories make it
4. **Story Cleanup**: Runs hourly, manages data retention (see below)

All tasks run independently; server is always ready to serve requests.

### Data Retention

The cleanup task manages database size with the following retention policy:

| Data | Retention | Configurable |
|------|-----------|--------------|
| Dismissed stories | 24 hours grace period | `CLEANUP_DISMISSED_HOURS` |
| Old stories | 14 days (read later exempt) | `CLEANUP_STORY_DAYS` |
| Content cache | 90 days | `CLEANUP_CONTENT_CACHE_DAYS` |
| Dismissed markers | 60 days | No |
| Usage logs | 6 months | No |
| Usage summaries | 36 months | No |

Usage logs are aggregated into monthly summaries (request count + browser time) before deletion, preserving billing history for 3 years.

### Backups

Automatic backups are created hourly in `.hn_data/backups/` with a rotation scheme:

| Slot | Files | Coverage |
|------|-------|----------|
| Hourly | 1h, 2h, 6h, 12h | Last 12 hours (4 files) |
| Daily | 1d-7d | Last week (7 files) |
| Weekly | 1w-4w | Last month (4 files) |

Total: 15 backup files max. Backups use SQLite's online backup API for consistency.

### Free Tier Quota

The Cloudflare free tier has a 10-minute daily browser time limit. When exceeded:
- Workers pause and wait for UTC midnight reset
- UI shows quota status in the Settings view
- Story fetching continues (only content extraction pauses)

## Utilities

### fetch_content.py

Standalone CLI tool for extracting article content as markdown with local images. Useful for saving articles for offline reading.

```bash
# Basic usage
./fetch_content.py https://example.com/article

# Custom output directory
./fetch_content.py https://example.com/article --output ./my-article

# Skip image downloads
./fetch_content.py https://example.com/article --no-images
```

Output is saved to `fetched/<date>-<slug>/index.md` with images downloaded locally.
