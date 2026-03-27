"""Tests for stream_monitor.py — unit tests that don't require network access."""
import json
import sys
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch, MagicMock
from zoneinfo import ZoneInfo

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))
import stream_monitor


# ── Calendar parsing ────────────────────────────────────────────────

class TestParseSheets:
    def test_standard_format(self):
        assert stream_monitor._parse_sheets("(8) Monday League") == 8

    def test_with_question_mark(self):
        assert stream_monitor._parse_sheets("(6?) Friday Open") == 6

    def test_with_plus(self):
        assert stream_monitor._parse_sheets("(8+) Tournament") == 8

    def test_no_match(self):
        assert stream_monitor._parse_sheets("No sheets here") == 0

    def test_empty_string(self):
        assert stream_monitor._parse_sheets("") == 0


class TestGetExpectedSheets:
    def test_returns_sheets_during_draw(self):
        tz = ZoneInfo("America/Vancouver")
        now = datetime.now(tz)
        with stream_monitor._cal_lock:
            stream_monitor._today_draws = [{
                "start": now - timedelta(minutes=30),
                "end": now + timedelta(minutes=90),
                "summary": "(8) Test Draw",
                "sheets": 8,
            }]
            stream_monitor._today_date = now.date()
            stream_monitor._cal_fetch_ok = True
        assert stream_monitor._get_expected_sheets() == 8

    def test_returns_zero_outside_draw(self):
        tz = ZoneInfo("America/Vancouver")
        now = datetime.now(tz)
        with stream_monitor._cal_lock:
            stream_monitor._today_draws = [{
                "start": now + timedelta(hours=5),
                "end": now + timedelta(hours=7),
                "summary": "(8) Future Draw",
                "sheets": 8,
            }]
            stream_monitor._today_date = now.date()
            stream_monitor._cal_fetch_ok = True
        assert stream_monitor._get_expected_sheets() == 0

    def test_returns_zero_no_draws(self):
        with stream_monitor._cal_lock:
            stream_monitor._today_draws = []
        assert stream_monitor._get_expected_sheets() == 0


# ── Stream title parsing ────────────────────────────────────────────

class TestSheetRegex:
    def test_standard_4digit_year(self):
        m = stream_monitor._SHEET_RE.search("SHEET 3 | 03-26-2026")
        assert m is not None
        assert int(m.group(1)) == 3
        assert int(m.group(2)) == 3
        assert int(m.group(3)) == 26
        assert int(m.group(4)) == 2026

    def test_2digit_year(self):
        m = stream_monitor._SHEET_RE.search("SHEET 7 | 03-26-26")
        assert m is not None
        assert int(m.group(4)) == 26

    def test_with_extra_text(self):
        m = stream_monitor._SHEET_RE.search("SHEET 1 | 03-26-2026 2026-03-26 18:32")
        assert m is not None
        assert int(m.group(1)) == 1

    def test_no_match(self):
        m = stream_monitor._SHEET_RE.search("Some Random Video Title")
        assert m is None


# ── Stream metadata ─────────────────────────────────────────────────

class TestGetStreamMetadata:
    @patch("stream_monitor.subprocess.run")
    def test_successful(self, mock_run):
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout=json.dumps({
                "is_live": True,
                "height": 1080,
                "width": 1920,
                "tbr": 5420.7,
                "formats": [{"format_id": "96"}],
            }),
            stderr="",
        )
        result = stream_monitor._get_stream_metadata("abc123")
        assert result is not None
        assert result["is_live"] is True
        assert result["height"] == 1080

    @patch("stream_monitor.subprocess.run")
    def test_failure(self, mock_run):
        mock_run.return_value = MagicMock(
            returncode=1,
            stdout="",
            stderr="ERROR: video not found",
        )
        result = stream_monitor._get_stream_metadata("bad123")
        assert result is None

    @patch("stream_monitor.subprocess.run")
    def test_timeout(self, mock_run):
        from subprocess import TimeoutExpired
        mock_run.side_effect = TimeoutExpired(cmd="yt-dlp", timeout=30)
        result = stream_monitor._get_stream_metadata("slow123")
        assert result is None


# ── Prometheus metrics format ───────────────────────────────────────

class TestFormatMetrics:
    def setup_method(self):
        with stream_monitor._health_lock:
            stream_monitor._health = {
                1: {"stream_up": 1, "resolution_height": 1080, "resolution_width": 1920,
                    "bitrate": 5420.7, "manifest_ok": 1, "video_id": "abc", "last_check_ts": 0},
                2: {"stream_up": 0, "resolution_height": 0, "resolution_width": 0,
                    "bitrate": 0, "manifest_ok": 0, "video_id": "def", "last_check_ts": 0},
            }
            stream_monitor._expected_sheets = 8
            stream_monitor._live_count = 1
            stream_monitor._last_check_ts = 1000000
            stream_monitor._check_ok = True
        with stream_monitor._cal_lock:
            stream_monitor._cal_fetch_ok = True
        with stream_monitor._yt_lock:
            stream_monitor._yt_fetch_ok = True

    def test_contains_per_sheet_metrics(self):
        output = stream_monitor._format_metrics()
        assert 'curling_stream_up{sheet="1"' in output
        assert 'curling_stream_up{sheet="2"' in output

    def test_contains_aggregate_metrics(self):
        output = stream_monitor._format_metrics()
        assert "curling_stream_live_count 1" in output
        assert "curling_stream_expected_count 8" in output

    def test_count_mismatch_detected(self):
        output = stream_monitor._format_metrics()
        assert "curling_stream_count_mismatch 1" in output

    def test_no_mismatch_when_no_expected(self):
        with stream_monitor._health_lock:
            stream_monitor._expected_sheets = 0
        output = stream_monitor._format_metrics()
        assert "curling_stream_count_mismatch 0" in output

    def test_help_and_type_headers(self):
        output = stream_monitor._format_metrics()
        assert "# HELP curling_stream_up" in output
        assert "# TYPE curling_stream_up gauge" in output

    def test_monitor_health_metrics(self):
        output = stream_monitor._format_metrics()
        assert "curling_stream_monitor_up 1" in output
        assert "curling_stream_calendar_up 1" in output
        assert "curling_stream_youtube_up 1" in output

    def test_no_frozen_metrics(self):
        output = stream_monitor._format_metrics()
        assert "frozen" not in output
