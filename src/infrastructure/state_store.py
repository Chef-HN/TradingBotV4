"""
Redis state/command interface for decoupled worker ↔ API communication.

Worker publishes RuntimeContext state; API reads it and pushes commands back.
"""
from __future__ import annotations

import json
from datetime import datetime
from decimal import Decimal
from typing import Any

import redis.asyncio as aioredis

STATE_TTL_SECONDS = 30
HEARTBEAT_TTL_SECONDS = 15  # worker must renew every publish cycle (< 15s normally)


def _state_key(exchange: str) -> str:
    return f"tb:v3:{exchange}:state"


def _heartbeat_key(exchange: str) -> str:
    return f"tb:v3:{exchange}:heartbeat"


def _commands_key(exchange: str) -> str:
    return f"tb:v3:{exchange}:commands"


def _skip_close_key(exchange: str) -> str:
    return f"tb:v3:{exchange}:settings:skip_daily_close"


def _started_at_key(exchange: str) -> str:
    return f"tb:v3:{exchange}:settings:started_at"


def _worker_started_at_key(exchange: str) -> str:
    """Tracks when the current worker *process* started. Always reset on startup, never restored."""
    return f"tb:v3:{exchange}:settings:worker_started_at"


# Legacy keys (pre-orchestrator) — used for migration only
_LEGACY_STATE_KEY = "tb:v3:state"
_LEGACY_COMMANDS_KEY = "tb:v3:commands"
_LEGACY_SKIP_CLOSE_KEY = "tb:v3:settings:skip_daily_close"


class _BotEncoder(json.JSONEncoder):
    """JSON encoder that handles Decimal (→ str) and datetime (→ ISO string)."""

    def default(self, obj: Any) -> Any:
        if isinstance(obj, Decimal):
            return str(obj)
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)


class StateStore:
    def __init__(self, redis_url: str, exchange: str = "") -> None:
        self._redis: aioredis.Redis = aioredis.from_url(
            redis_url, decode_responses=True
        )
        self._exchange = exchange.lower()

    @property
    def _key_state(self) -> str:
        return _state_key(self._exchange) if self._exchange else _LEGACY_STATE_KEY

    @property
    def _key_commands(self) -> str:
        return _commands_key(self._exchange) if self._exchange else _LEGACY_COMMANDS_KEY

    @property
    def _key_skip_close(self) -> str:
        return _skip_close_key(self._exchange) if self._exchange else _LEGACY_SKIP_CLOSE_KEY

    @property
    def _key_started_at(self) -> str:
        return _started_at_key(self._exchange)

    async def publish_state(self, state_dict: dict) -> None:
        """Serialize state_dict to JSON and SET with TTL."""
        payload = json.dumps(state_dict, cls=_BotEncoder)
        await self._redis.set(self._key_state, payload, ex=STATE_TTL_SECONDS)

    async def publish_heartbeat(self) -> None:
        """Renew the worker heartbeat key. Expires HEARTBEAT_TTL_SECONDS after last write."""
        key = _heartbeat_key(self._exchange) if self._exchange else "tb:v3:heartbeat"
        await self._redis.set(key, "1", ex=HEARTBEAT_TTL_SECONDS)

    async def worker_alive(self, exchange: str) -> bool:
        """True if the worker for this exchange has written a heartbeat recently."""
        return bool(await self._redis.exists(_heartbeat_key(exchange)))

    async def get_state(self) -> dict | None:
        """GET the current state JSON; returns None if key is missing/expired."""
        raw = await self._redis.get(self._key_state)
        if raw is None:
            return None
        return json.loads(raw)

    async def get_state_for_exchange(self, exchange: str) -> dict | None:
        """GET state for a specific exchange (used by API to aggregate)."""
        raw = await self._redis.get(_state_key(exchange))
        if raw is None:
            # Fall back to legacy key if no exchange-scoped state exists
            raw = await self._redis.get(_LEGACY_STATE_KEY)
            if raw is None:
                return None
        return json.loads(raw)

    async def push_command(self, cmd: dict) -> None:
        """LPUSH a command dict onto the command list (API → worker)."""
        payload = json.dumps(cmd, cls=_BotEncoder)
        await self._redis.lpush(self._key_commands, payload)

    async def push_command_to_exchange(self, exchange: str, cmd: dict) -> None:
        """Push a command to a specific exchange's queue."""
        payload = json.dumps(cmd, cls=_BotEncoder)
        await self._redis.lpush(_commands_key(exchange), payload)

    async def pop_commands(self) -> list[dict]:
        """Drain all pending commands via RPOP loop (worker reads oldest-first)."""
        commands: list[dict] = []
        while True:
            raw = await self._redis.rpop(self._key_commands)
            if raw is None:
                break
            try:
                commands.append(json.loads(raw))
            except json.JSONDecodeError:
                pass
        return commands

    async def get_skip_daily_close(self) -> bool:
        """Read the persisted skip_daily_close flag (survives worker restarts)."""
        return await self._redis.exists(self._key_skip_close) > 0

    async def set_skip_daily_close(self, value: bool) -> None:
        """Persist or clear the skip_daily_close flag."""
        if value:
            await self._redis.set(self._key_skip_close, "1")
        else:
            await self._redis.delete(self._key_skip_close)

    async def get_started_at(self) -> datetime | None:
        """Read persisted worker started_at (survives restarts)."""
        raw = await self._redis.get(self._key_started_at)
        if raw:
            return datetime.fromisoformat(raw)
        return None

    async def set_started_at(self, value: datetime) -> None:
        """Persist worker started_at so uptime survives restarts."""
        await self._redis.set(self._key_started_at, value.isoformat())

    async def set_worker_started_at(self, value: datetime) -> None:
        """Record when the current process started. Always written fresh on startup."""
        key = _worker_started_at_key(self._exchange)
        await self._redis.set(key, value.isoformat())

    async def get_worker_started_at(self) -> datetime | None:
        key = _worker_started_at_key(self._exchange)
        raw = await self._redis.get(key)
        if raw:
            return datetime.fromisoformat(raw)
        return None

    async def close(self) -> None:
        await self._redis.aclose()
