from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from uuid import uuid4

from scripts.run_worker import RuntimeContext, SymbolContext, _process_runtime_commands


class _FakeStateStore:
    def __init__(self, commands: list[dict]) -> None:
        self._commands = commands
        self.skip_daily_close = False

    async def pop_commands(self) -> list[dict]:
        out = list(self._commands)
        self._commands = []
        return out

    async def set_skip_daily_close(self, value: bool) -> None:
        self.skip_daily_close = value


def _runtime() -> RuntimeContext:
    return RuntimeContext(
        mode="paper",
        started_at=datetime.now(UTC),
        config_loaded=True,
    )


def test_schedule_change_next_cycle_is_queued() -> None:
    runtime = _runtime()
    runtime.local_timezone_iana = "Europe/Amsterdam"
    runtime.daily_close_hour = 0
    runtime.daily_close_minute = 0

    store = _FakeStateStore(
        [
            {
                "type": "update_daily_close_schedule",
                "local_timezone_iana": "Asia/Singapore",
                "daily_close_hour": 0,
                "daily_close_minute": 0,
                "mode": "next_cycle",
            }
        ]
    )
    asyncio.run(_process_runtime_commands(runtime, store))

    assert runtime.local_timezone_iana == "Europe/Amsterdam"
    assert runtime.pending_schedule_after_close is not None
    assert runtime.pending_schedule_after_close["local_timezone_iana"] == "Asia/Singapore"


def test_schedule_change_immediate_resyncs_runtime() -> None:
    runtime = _runtime()
    runtime.local_timezone_iana = "Europe/Amsterdam"
    runtime.daily_close_hour = 0
    runtime.daily_close_minute = 0
    runtime.next_daily_close_at = datetime.now(UTC)

    store = _FakeStateStore(
        [
            {
                "type": "update_daily_close_schedule",
                "local_timezone_iana": "Asia/Singapore",
                "daily_close_hour": 0,
                "daily_close_minute": 0,
                "mode": "immediate",
            }
        ]
    )
    asyncio.run(_process_runtime_commands(runtime, store))

    assert runtime.local_timezone_iana == "Asia/Singapore"
    assert runtime.schedule_resync_requested is True
    assert runtime.next_daily_close_at is None
    assert runtime.pending_schedule_after_close is None


def test_reset_command_is_routed_to_target_symbol_only() -> None:
    runtime = _runtime()
    runtime.symbol_contexts = {
        "SOL-USD": SymbolContext(product_id="SOL-USD", session_id=uuid4()),
        "DOGE-USD": SymbolContext(product_id="DOGE-USD", session_id=uuid4()),
    }

    store = _FakeStateStore(
        [
            {
                "type": "reset",
                "product_id": "SOL-USD",
                "triggered_by": "maintenance",
                "reset_type": "hard",
            }
        ]
    )
    asyncio.run(_process_runtime_commands(runtime, store))

    assert runtime.symbol_contexts["SOL-USD"].reset_requested is True
    assert runtime.symbol_contexts["SOL-USD"].reset_triggered_by == "maintenance"
    assert runtime.symbol_contexts["SOL-USD"].reset_type == "hard"

    assert runtime.symbol_contexts["DOGE-USD"].reset_requested is False
