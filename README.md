# Curling Stadium Exporter

Prometheus exporter that monitors Curling Stadium YouTube live streams. Checks stream liveness, quality (resolution/bitrate), and manifest validity.

Designed for [Vancouver Curling Club](https://www.youtube.com/@CurlingStadiumVancouver-jx5sj) but configurable for any YouTube channel with a predictable stream title format and Google Calendar-based draw schedule.

## Features

- **Stream liveness**: Per-sheet `curling_stream_up{sheet="1"}` metric
- **Quality metrics**: Resolution, bitrate per stream
- **Manifest validation**: Checks that stream formats are available
- **Calendar integration**: Knows when draws are scheduled via Google Calendar (iCal)
- **Expected vs actual**: Detects when fewer streams are live than expected

## Metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `curling_stream_up` | gauge | sheet, video_id | 1 if stream is live |
| `curling_stream_resolution_height` | gauge | sheet, video_id | Video height (pixels) |
| `curling_stream_resolution_width` | gauge | sheet, video_id | Video width (pixels) |
| `curling_stream_bitrate_kbps` | gauge | sheet, video_id | Total bitrate (kbps) |
| `curling_stream_manifest_ok` | gauge | sheet, video_id | 1 if manifest is valid |
| `curling_stream_live_count` | gauge | | Total live streams |
| `curling_stream_expected_count` | gauge | | Expected streams (from calendar) |
| `curling_stream_count_mismatch` | gauge | | 1 if live < expected |
| `curling_stream_monitor_up` | gauge | | 1 if monitor is healthy |
| `curling_stream_monitor_last_check` | gauge | | Unix timestamp of last check |
| `curling_stream_calendar_up` | gauge | | 1 if calendar fetch succeeded |
| `curling_stream_youtube_up` | gauge | | 1 if YouTube fetch succeeded |

## Quick Start

### Docker Compose

```bash
# Place your YouTube cookies file in the repo root
cp /path/to/youtube-cookies.txt .

docker compose up -d
```

### Docker Run

```bash
docker build -t curling-stadium-exporter .

docker run -d \
  --name curling-stadium-exporter \
  -p 8052:8052 \
  -v /path/to/youtube-cookies.txt:/data/youtube-cookies.txt:ro \
  curling-stadium-exporter
```

### Prometheus

Add to your `prometheus.yml`:

```yaml
scrape_configs:
  - job_name: 'curling_stream_monitor'
    scrape_interval: 30s
    scrape_timeout: 10s
    static_configs:
    - targets: ['<host>:8052']
```

### Grafana

Import `grafana-dashboard.json` and select your Prometheus datasource.

## Configuration

All configuration is via environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `PORT` | `8052` | HTTP server port |
| `YT_CHANNEL_URL` | VCC channel | YouTube channel URL |
| `YT_COOKIES_FILE` | `/data/youtube-cookies.txt` | Path to Netscape cookie file |
| `YT_POLL_INTERVAL` | `120` | Seconds between YouTube stream discovery |
| `ICAL_URL` | VCC calendar | Google Calendar iCal URL |
| `ICAL_REFRESH_INTERVAL` | `3600` | Seconds between calendar refreshes |
| `TIMEZONE` | `America/Vancouver` | Timezone for schedule logic |
| `LOG_LEVEL` | `INFO` | Logging level |

## YouTube Cookies

YouTube requires authentication for reliable stream access. Export cookies from your browser using a "Get cookies.txt" extension and mount the file into the container.

## Stream Title Format

The monitor parses stream titles matching the pattern:

```
SHEET <number> | MM-DD-YYYY
```

This is configurable by modifying `_SHEET_RE` in `stream_monitor.py`.

## HTTP Endpoints

| Path | Description |
|------|-------------|
| `/metrics` | Prometheus metrics |
| `/healthz` | Health check (200/503) |
| `/status` | JSON status of all streams |

## Architecture

Two background threads:
1. **Calendar** — fetches iCal feed, parses draw schedule
2. **YouTube** — discovers today's streams, fetches per-stream metadata (liveness, resolution, bitrate) in a single pass

Stream metadata is fetched via `yt-dlp` subprocess (not the Python API, which has issues with YouTube's `n` challenge on live streams). Polls every 2 minutes. Includes exponential backoff if YouTube rate-limits requests.

## Testing

```bash
pip install -r requirements.txt -r requirements-test.txt
pytest
```
