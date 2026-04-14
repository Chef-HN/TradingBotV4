from __future__ import annotations

from datetime import UTC, datetime

from scripts.run_worker import _compute_next_daily_close_utc


def test_next_daily_close_respects_singapore_midnight() -> None:
    now_utc = datetime(2026, 4, 10, 15, 30, tzinfo=UTC)  # 23:30 in Singapore
    next_utc, next_local = _compute_next_daily_close_utc(
        now_utc=now_utc,
        timezone_iana="Asia/Singapore",
        close_hour=0,
        close_minute=0,
    )
    assert next_local.hour == 0
    assert next_local.minute == 0
    assert next_utc == datetime(2026, 4, 10, 16, 0, tzinfo=UTC)


def test_next_daily_close_respects_paris_midnight() -> None:
    now_utc = datetime(2026, 4, 10, 10, 0, tzinfo=UTC)  # 12:00 in Paris (CEST)
    next_utc, next_local = _compute_next_daily_close_utc(
        now_utc=now_utc,
        timezone_iana="Europe/Paris",
        close_hour=0,
        close_minute=0,
    )
    assert next_local.hour == 0
    assert next_local.minute == 0
    assert next_utc == datetime(2026, 4, 10, 22, 0, tzinfo=UTC)
