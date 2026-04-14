from __future__ import annotations

import pytest
from fastapi import HTTPException

from api.routes.dashboard import _detect_local_timezone_iana, _normalize_schedule_mode


def test_normalize_schedule_mode_values() -> None:
    assert _normalize_schedule_mode(None) is None
    assert _normalize_schedule_mode("next_cycle") == "next_cycle"
    assert _normalize_schedule_mode(" immediate ") == "immediate"


def test_normalize_schedule_mode_rejects_invalid() -> None:
    with pytest.raises(HTTPException) as exc:
        _normalize_schedule_mode("tomorrow")
    assert exc.value.status_code == 400


def test_detect_local_timezone_returns_non_empty_string() -> None:
    tz = _detect_local_timezone_iana()
    assert isinstance(tz, str)
    assert tz
