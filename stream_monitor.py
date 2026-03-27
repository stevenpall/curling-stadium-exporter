#!/usr/bin/env python3
"""Curling Stream Health Monitor — self-contained Prometheus exporter that
checks YouTube live streams for liveness and quality.

Independently fetches the stream list from YouTube and the draw schedule
from Google Calendar. No dependency on other exporters.
"""
import json
import logging
import os
import re
import subprocess
import threading
import time
from datetime import datetime, date, timedelta
from http.server import BaseHTTPRequestHandler, HTTPServer
from zoneinfo import ZoneInfo

import icalendar
import recurring_ical_events
import requests
import yt_dlp

# ── CONFIG ──────────────────────────────────────────────────────────
HOST = os.getenv("HOST", "0.0.0.0")
PORT = int(os.getenv("PORT", "8052"))

YT_CHANNEL_URL = os.getenv(
    "YT_CHANNEL_URL",
    "https://www.youtube.com/@CurlingStadiumVancouver-jx5sj",
)
YT_COOKIES_FILE = os.getenv("YT_COOKIES_FILE", "/data/youtube-cookies.txt")
YT_POLL_INTERVAL = float(os.getenv("YT_POLL_INTERVAL", "120"))  # 2 min
YT_IDLE_POLL_INTERVAL = float(os.getenv("YT_IDLE_POLL_INTERVAL", "300"))  # 5 min when no draw

ICAL_URL = os.getenv(
    "ICAL_URL",
    "https://calendar.google.com/calendar/ical/"
    "8k10cuiqb0j1m02l1cio5l61dg%40group.calendar.google.com/public/basic.ics",
)
ICAL_REFRESH_INTERVAL = float(os.getenv("ICAL_REFRESH_INTERVAL", "3600"))
ICAL_TIMEOUT = float(os.getenv("ICAL_TIMEOUT", "15"))
DEFAULT_DRAW_DURATION_MIN = int(os.getenv("DEFAULT_DRAW_DURATION_MIN", "120"))
TIMEZONE = os.getenv("TIMEZONE", "America/Vancouver")

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("stream_monitor")

# ── CALENDAR ────────────────────────────────────────────────────────
_cal_lock = threading.Lock()
_cal_fetch_ok: bool = False
_today_draws: list[dict] = []
_today_date: date | None = None

_SHEETS_RE = re.compile(r"^\((\d+)\??(?:\+)?\)")


def _parse_sheets(summary: str) -> int:
    m = _SHEETS_RE.match(summary.strip())
    return int(m.group(1)) if m else 0


def _fetch_calendar():
    global _cal_fetch_ok, _today_draws, _today_date

    logger.info("Fetching iCal feed")
    resp = requests.get(ICAL_URL, timeout=ICAL_TIMEOUT)
    resp.raise_for_status()

    cal = icalendar.Calendar.from_ical(resp.text)
    tz = ZoneInfo(TIMEZONE)
    today = datetime.now(tz).date()

    start_dt = datetime.combine(today, datetime.min.time(), tzinfo=tz)
    end_dt = start_dt + timedelta(days=1)
    events = recurring_ical_events.of(cal).between(start_dt, end_dt)

    draws = []
    for ev in events:
        summary = str(ev.get("SUMMARY", ""))
        sheets = _parse_sheets(summary)
        if sheets == 0:
            continue
        dtstart = ev.get("DTSTART").dt
        dtend = ev.get("DTEND").dt if ev.get("DTEND") else None
        if isinstance(dtstart, date) and not isinstance(dtstart, datetime):
            continue
        if dtstart.tzinfo is None:
            dtstart = dtstart.replace(tzinfo=tz)
        else:
            dtstart = dtstart.astimezone(tz)
        if dtend is None:
            dtend = dtstart + timedelta(minutes=DEFAULT_DRAW_DURATION_MIN)
        elif isinstance(dtend, date) and not isinstance(dtend, datetime):
            continue
        else:
            if dtend.tzinfo is None:
                dtend = dtend.replace(tzinfo=tz)
            else:
                dtend = dtend.astimezone(tz)
        draws.append({"start": dtstart, "end": dtend, "summary": summary, "sheets": sheets})

    draws.sort(key=lambda d: d["start"])
    logger.info("Calendar: %d draws for %s", len(draws), today)

    with _cal_lock:
        _cal_fetch_ok = True
        _today_draws = draws
        _today_date = today


def _get_expected_sheets() -> int:
    """Return the expected number of sheets for the currently active draw."""
    tz = ZoneInfo(TIMEZONE)
    now = datetime.now(tz)
    with _cal_lock:
        for d in _today_draws:
            if d["start"] <= now < d["end"]:
                return d["sheets"]
    return 0


def _calendar_loop():
    while True:
        try:
            _fetch_calendar()
        except Exception as e:
            logger.error("Calendar fetch failed: %s", e)
            with _cal_lock:
                global _cal_fetch_ok
                _cal_fetch_ok = False
        time.sleep(ICAL_REFRESH_INTERVAL)


# ── YOUTUBE STREAM DISCOVERY + HEALTH ───────────────────────────────
_yt_lock = threading.Lock()
_yt_streams: list[dict] = []
_yt_fetch_ok: bool = False

_health_lock = threading.Lock()
_health: dict[int, dict] = {}
_expected_sheets: int = 0
_live_count: int = 0
_last_check_ts: float = 0.0
_check_ok: bool = False

_SHEET_RE = re.compile(r"SHEET\s+(\d+)\s*\|\s*(\d{2})-(\d{2})-(\d{2,4})")


class RateLimitError(Exception):
    pass


def _get_stream_metadata(video_id: str) -> dict | None:
    """Get full stream metadata via yt-dlp subprocess. Returns parsed JSON or None.
    Raises RateLimitError if YouTube rate-limits the request."""
    cmd = [
        "yt-dlp", "--js-runtime", "node", "--remote-components", "ejs:github",
        "--dump-json", "--no-download",
    ]
    if os.path.exists(YT_COOKIES_FILE):
        cmd += ["--cookies", YT_COOKIES_FILE]
    cmd.append(f"https://www.youtube.com/watch?v={video_id}")

    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if proc.returncode != 0:
            if "rate-limited" in proc.stderr or "rate limit" in proc.stderr.lower():
                raise RateLimitError(f"YouTube rate-limited for {video_id}")
            logger.warning("yt-dlp failed for %s: %s", video_id, proc.stderr[:200])
            return None
        return json.loads(proc.stdout)
    except RateLimitError:
        raise
    except subprocess.TimeoutExpired:
        logger.warning("yt-dlp timed out for %s", video_id)
        return None
    except Exception as e:
        logger.warning("Metadata failed for %s: %s", video_id, e)
        return None


def _fetch_and_check_streams():
    """Combined YouTube discovery + health check in a single pass.

    1. List today's streams from the channel (flat listing, fast)
    2. For each stream, get full metadata via yt-dlp (one call per stream)
    3. Update both _yt_streams and _health from the same data
    """
    global _yt_streams, _yt_fetch_ok
    global _health, _expected_sheets, _live_count, _last_check_ts, _check_ok

    tz = ZoneInfo(TIMEZONE)
    today = datetime.now(tz).date()
    expected = _get_expected_sheets()

    # Step 1: flat listing (fast, single API call)
    ydl_opts = {
        "quiet": True,
        "no_warnings": True,
        "extract_flat": "in_playlist",
        "playlistend": 50,
        "js_runtime": "node",
    }
    if os.path.exists(YT_COOKIES_FILE):
        ydl_opts["cookiefile"] = YT_COOKIES_FILE

    url = f"{YT_CHANNEL_URL}/streams"
    logger.info("Fetching YouTube stream list")

    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        playlist = ydl.extract_info(url, download=False)

    if not playlist or "entries" not in playlist:
        with _yt_lock:
            _yt_fetch_ok = False
        return

    today_entries = []
    for entry in playlist["entries"]:
        if not entry:
            continue
        title = entry.get("title", "")
        m = _SHEET_RE.search(title)
        if not m:
            continue
        sheet = int(m.group(1))
        mm, dd = int(m.group(2)), int(m.group(3))
        yr = int(m.group(4))
        yr = yr if yr >= 100 else 2000 + yr
        try:
            stream_date = date(yr, mm, dd)
        except ValueError:
            continue
        if stream_date != today:
            continue
        today_entries.append({
            "id": entry.get("id", ""),
            "title": title,
            "sheet": sheet,
        })

    if not today_entries:
        logger.info("No streams found for today")
        with _yt_lock:
            _yt_streams = []
            _yt_fetch_ok = True
        with _health_lock:
            _health = {}
            _expected_sheets = expected
            _live_count = 0
            _last_check_ts = time.time()
            _check_ok = True
        return

    # Step 2: pick one stream per sheet (prefer most recent)
    by_sheet: dict[int, dict] = {}
    for entry in today_entries:
        sheet = entry["sheet"]
        if sheet not in by_sheet:
            by_sheet[sheet] = entry

    # Step 3: get full metadata per stream (one yt-dlp call each)
    streams = []
    results = {}

    for sheet, entry in sorted(by_sheet.items()):
        video_id = entry["id"]
        info = _get_stream_metadata(video_id)  # raises RateLimitError

        stream_data = {
            "id": video_id,
            "title": entry["title"],
            "sheet": sheet,
            "is_live": False,
        }

        health_data = {
            "stream_up": 0,
            "resolution_height": 0,
            "resolution_width": 0,
            "bitrate": 0,
            "manifest_ok": 0,
            "video_id": video_id,
            "last_check_ts": time.time(),
        }

        if info:
            is_live = info.get("is_live", False)
            stream_data["is_live"] = is_live
            health_data["stream_up"] = 1 if is_live else 0
            health_data["resolution_height"] = info.get("height", 0) or 0
            health_data["resolution_width"] = info.get("width", 0) or 0
            health_data["bitrate"] = round(info.get("tbr", 0) or 0, 1)
            health_data["manifest_ok"] = 1 if (info.get("formats") and is_live) else 0

            if is_live:
                logger.info("Sheet %d: UP (%dx%d, %.0fkbps)",
                           sheet, health_data["resolution_width"],
                           health_data["resolution_height"], health_data["bitrate"])
            else:
                logger.info("Sheet %d: not live", sheet)

        streams.append(stream_data)
        results[sheet] = health_data

        # Small delay between calls to avoid rate limiting
        time.sleep(1)

    # Add stream_up=0 entries for any expected sheets with no stream
    if expected > 0:
        for sheet_num in range(1, 9):
            if sheet_num not in results:
                results[sheet_num] = {
                    "stream_up": 0, "resolution_height": 0, "resolution_width": 0,
                    "bitrate": 0, "manifest_ok": 0,
                    "video_id": "", "last_check_ts": time.time(),
                }

    up = sum(1 for h in results.values() if h["stream_up"])
    live = [s for s in streams if s["is_live"]]
    logger.info("Found %d streams (%d live, expected %d)", len(streams), len(live), expected)

    with _yt_lock:
        _yt_streams = streams
        _yt_fetch_ok = True

    with _health_lock:
        _health = results
        _expected_sheets = expected
        _live_count = up
        _last_check_ts = time.time()
        _check_ok = True


def _youtube_loop():
    backoff = 0  # exponential backoff multiplier (0 = no backoff)
    MAX_BACKOFF = 5  # max 2^5 = 32x multiplier

    while True:
        try:
            _fetch_and_check_streams()
            backoff = 0  # reset on success
        except RateLimitError as e:
            backoff = min(backoff + 1, MAX_BACKOFF)
            wait = YT_POLL_INTERVAL * (2 ** backoff)
            logger.warning("Rate limited by YouTube — backing off %ds (level %d): %s",
                          int(wait), backoff, e)
            with _health_lock:
                global _check_ok
                _check_ok = False
            time.sleep(wait)
            continue
        except Exception as e:
            logger.error("YouTube fetch failed: %s", e)
            with _yt_lock:
                global _yt_fetch_ok
                _yt_fetch_ok = False
            with _health_lock:
                _check_ok = False

        # Poll faster during active draws, slower when idle
        expected = _get_expected_sheets()
        interval = YT_POLL_INTERVAL if expected > 0 else YT_IDLE_POLL_INTERVAL
        time.sleep(interval)


# ── PROMETHEUS ──────────────────────────────────────────────────────
def _format_metrics() -> str:
    lines: list[str] = []
    seen: set[str] = set()

    def header(name, help_text):
        if name not in seen:
            lines.append(f"# HELP {name} {help_text}")
            lines.append(f"# TYPE {name} gauge")
            seen.add(name)

    with _health_lock:
        health = dict(_health)
        expected = _expected_sheets
        live_count = _live_count
        last_ts = _last_check_ts
        ok = _check_ok

    header("curling_stream_up", "1 if the stream for this sheet is live.")
    header("curling_stream_resolution_height", "Video height in pixels.")
    header("curling_stream_resolution_width", "Video width in pixels.")
    header("curling_stream_bitrate_kbps", "Total bitrate in kbps.")
    header("curling_stream_manifest_ok", "1 if the stream manifest is valid.")

    for sheet, h in sorted(health.items()):
        labels = f'sheet="{sheet}",video_id="{h["video_id"]}"'
        lines.append(f'curling_stream_up{{{labels}}} {h["stream_up"]}')
        lines.append(f'curling_stream_resolution_height{{{labels}}} {h["resolution_height"]}')
        lines.append(f'curling_stream_resolution_width{{{labels}}} {h["resolution_width"]}')
        lines.append(f'curling_stream_bitrate_kbps{{{labels}}} {h["bitrate"]}')
        lines.append(f'curling_stream_manifest_ok{{{labels}}} {h["manifest_ok"]}')

    header("curling_stream_expected_count", "Expected live streams based on calendar.")
    lines.append(f"curling_stream_expected_count {expected}")

    header("curling_stream_live_count", "Actual live streams detected.")
    lines.append(f"curling_stream_live_count {live_count}")

    header("curling_stream_count_mismatch", "1 if live count differs from expected.")
    mismatch = 1 if (expected > 0 and live_count != expected) else 0
    lines.append(f"curling_stream_count_mismatch {mismatch}")

    header("curling_stream_monitor_up", "1 if last health check succeeded.")
    lines.append(f"curling_stream_monitor_up {1 if ok else 0}")

    header("curling_stream_monitor_last_check", "Unix timestamp of last health check.")
    lines.append(f"curling_stream_monitor_last_check {int(last_ts)}")

    with _cal_lock:
        cal_ok = _cal_fetch_ok
    header("curling_stream_calendar_up", "1 if calendar fetch succeeded.")
    lines.append(f"curling_stream_calendar_up {1 if cal_ok else 0}")

    with _yt_lock:
        yt_ok = _yt_fetch_ok
    header("curling_stream_youtube_up", "1 if YouTube fetch succeeded.")
    lines.append(f"curling_stream_youtube_up {1 if yt_ok else 0}")

    return "\n".join(lines) + "\n"


# ── HTTP SERVER ─────────────────────────────────────────────────────
class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        logger.info("%s - - %s", self.address_string(), fmt % args)

    def _text(self, code, body, ct="text/plain; charset=utf-8"):
        self.send_response(code)
        self.send_header("Content-Type", ct)
        self.end_headers()
        self.wfile.write(body.encode())

    def do_GET(self):
        if self.path == "/metrics":
            self._text(200, _format_metrics(), ct="text/plain; version=0.0.4")
        elif self.path == "/healthz":
            with _health_lock:
                ok = _check_ok
            self._text(200 if ok else 503,
                      json.dumps({"up": ok}), ct="application/json")
        elif self.path == "/status":
            with _health_lock:
                payload = {
                    "expected_sheets": _expected_sheets,
                    "live_count": _live_count,
                    "last_check": int(_last_check_ts),
                    "streams": {str(k): v for k, v in sorted(_health.items())},
                }
            self._text(200, json.dumps(payload, indent=2), ct="application/json")
        else:
            self._text(404, "Not Found")


# ── MAIN ────────────────────────────────────────────────────────────
if __name__ == "__main__":
    logger.info("Starting Curling Stream Monitor on http://%s:%d", HOST, PORT)

    threading.Thread(target=_calendar_loop, daemon=True, name="calendar").start()
    threading.Thread(target=_youtube_loop, daemon=True, name="youtube").start()

    logger.info("Threads started (yt=%ds/%ds, cal=%ds)",
                int(YT_POLL_INTERVAL), int(YT_IDLE_POLL_INTERVAL),
                int(ICAL_REFRESH_INTERVAL))

    HTTPServer((HOST, PORT), Handler).serve_forever()
