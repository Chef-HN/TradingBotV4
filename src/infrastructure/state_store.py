"""
Redis state/command interface for decoupled worker <-> API communication.

V4 namespace:
    tb:v4:{tenant_id}:{exchange}:{pair_or_all}:{kind}
"""
from __future__ import annotations

import json
from datetime import datetime
from decimal import Decimal
from typing import Any

import redis.asyncio as aioredis

from application.contracts.worker_protocol import (
    RuntimeCommand,
    build_command_envelope,
    build_state_payload,
    parse_runtime_command,
)
from infrastructure.tenancy import DEFAULT_TENANT_ID

STATE_TTL_SECONDS = 30
HEARTBEAT_TTL_SECONDS = 15


def _part(value: str | None, fallback: str) -> str:
    cleaned = (value or "").strip().lower()
    return cleaned or fallback


def _prefix(*, tenant_id: str, exchange: str, product_id: str) -> str:
    return f"tb:v4:{tenant_id}:{exchange}:{product_id}"


def _state_key(*, tenant_id: str, exchange: str, product_id: str) -> str:
    return f"{_prefix(tenant_id=tenant_id, exchange=exchange, product_id=product_id)}:state"


def _heartbeat_key(*, tenant_id: str, exchange: str, product_id: str) -> str:
    return f"{_prefix(tenant_id=tenant_id, exchange=exchange, product_id=product_id)}:heartbeat"


def _commands_key(*, tenant_id: str, exchange: str, product_id: str) -> str:
    return f"{_prefix(tenant_id=tenant_id, exchange=exchange, product_id=product_id)}:commands"


def _skip_close_key(*, tenant_id: str, exchange: str) -> str:
    return f"tb:v4:{tenant_id}:{exchange}:settings:skip_daily_close"


def _started_at_key(*, tenant_id: str, exchange: str) -> str:
    return f"tb:v4:{tenant_id}:{exchange}:settings:started_at"


def _worker_started_at_key(*, tenant_id: str, exchange: str) -> str:
    return f"tb:v4:{tenant_id}:{exchange}:settings:worker_started_at"


_LEGACY_STATE_KEY = "tb:v3:state"
_LEGACY_COMMANDS_KEY = "tb:v3:commands"
_LEGACY_SKIP_CLOSE_KEY = "tb:v3:settings:skip_daily_close"


class _BotEncoder(json.JSONEncoder):
    def default(self, obj: Any) -> Any:
        if isinstance(obj, Decimal):
            return str(obj)
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)


class StateStore:
    def __init__(
        self,
        redis_url: str,
        exchange: str = "",
        tenant_id: str = DEFAULT_TENANT_ID,
        product_id: str = "all",
    ) -> None:
        self._redis: aioredis.Redis = aioredis.from_url(redis_url, decode_responses=True)
        self._exchange = _part(exchange, "")
        self._tenant_id = _part(tenant_id, DEFAULT_TENANT_ID)
        self._product_id = _part(product_id, "all")

    @property
    def _key_state(self) -> str:
        if not self._exchange:
            return _LEGACY_STATE_KEY
        return _state_key(tenant_id=self._tenant_id, exchange=self._exchange, product_id=self._product_id)

    @property
    def _key_commands(self) -> str:
        if not self._exchange:
            return _LEGACY_COMMANDS_KEY
        return _commands_key(tenant_id=self._tenant_id, exchange=self._exchange, product_id="all")

    @property
    def _key_skip_close(self) -> str:
        if not self._exchange:
            return _LEGACY_SKIP_CLOSE_KEY
        return _skip_close_key(tenant_id=self._tenant_id, exchange=self._exchange)

    @property
    def _key_started_at(self) -> str:
        return _started_at_key(tenant_id=self._tenant_id, exchange=self._exchange)

    async def publish_state(self, state_dict: dict) -> None:
        payload = json.dumps(
            build_state_payload(
                tenant_id=self._tenant_id,
                exchange=self._exchange,
                product_id=self._product_id,
                state=state_dict,
            ),
            cls=_BotEncoder,
        )
        await self._redis.set(self._key_state, payload, ex=STATE_TTL_SECONDS)

    async def publish_heartbeat(self) -> None:
        if not self._exchange:
            await self._redis.set("tb:v3:heartbeat", "1", ex=HEARTBEAT_TTL_SECONDS)
            return
        await self._redis.set(
            _heartbeat_key(tenant_id=self._tenant_id, exchange=self._exchange, product_id="all"),
            "1",
            ex=HEARTBEAT_TTL_SECONDS,
        )

    async def worker_alive(
        self,
        exchange: str,
        tenant_id: str | None = None,
        product_id: str = "all",
    ) -> bool:
        ex = _part(exchange, "")
        if not ex:
            return bool(await self._redis.exists("tb:v3:heartbeat"))
        tid = _part(tenant_id, self._tenant_id)
        key = _heartbeat_key(tenant_id=tid, exchange=ex, product_id=_part(product_id, "all"))
        if await self._redis.exists(key):
            return True
        return bool(await self._redis.exists("tb:v3:heartbeat"))

    async def get_state(self) -> dict | None:
        raw = await self._redis.get(self._key_state)
        if raw is None and self._key_state != _LEGACY_STATE_KEY:
            raw = await self._redis.get(_LEGACY_STATE_KEY)
        if raw is None:
            return None
        return json.loads(raw)

    async def get_state_for_exchange(
        self,
        exchange: str,
        tenant_id: str | None = None,
        product_id: str = "all",
    ) -> dict | None:
        ex = _part(exchange, "")
        if not ex:
            return await self.get_state()
        tid = _part(tenant_id, self._tenant_id)
        key = _state_key(tenant_id=tid, exchange=ex, product_id=_part(product_id, "all"))
        raw = await self._redis.get(key)
        if raw is None:
            raw = await self._redis.get(_LEGACY_STATE_KEY)
            if raw is None:
                return None
        return json.loads(raw)

    async def push_command(self, cmd: dict) -> None:
        command = self._build_command_payload(
            cmd,
            exchange=self._exchange,
            tenant_id=self._tenant_id,
            product_id=cmd.get("product_id", "all") if isinstance(cmd, dict) else "all",
        )
        payload = json.dumps(command, cls=_BotEncoder)
        await self._redis.lpush(self._key_commands, payload)

    async def push_command_to_exchange(
        self,
        exchange: str,
        cmd: dict,
        tenant_id: str | None = None,
        product_id: str | None = None,
    ) -> None:
        ex = _part(exchange, "")
        if not ex:
            await self.push_command(cmd)
            return
        tid = _part(tenant_id, self._tenant_id)
        target_pid = _part(product_id, "all")
        command = self._build_command_payload(
            cmd,
            exchange=ex,
            tenant_id=tid,
            product_id=target_pid,
        )
        payload = json.dumps(command, cls=_BotEncoder)
        key = _commands_key(tenant_id=tid, exchange=ex, product_id="all")
        await self._redis.lpush(key, payload)

    async def pop_commands(self) -> list[dict]:
        commands: list[dict] = []
        while True:
            raw = await self._redis.rpop(self._key_commands)
            if raw is None:
                break
            try:
                commands.append(json.loads(raw))
            except json.JSONDecodeError:
                continue
        return commands

    async def pop_runtime_commands(
        self,
        *,
        default_tenant_id: str | None = None,
        default_exchange: str | None = None,
        default_product_id: str = "all",
    ) -> list[RuntimeCommand]:
        parsed: list[RuntimeCommand] = []
        for raw in await self.pop_commands():
            command = parse_runtime_command(
                raw,
                default_tenant_id=_part(default_tenant_id, self._tenant_id),
                default_exchange=_part(default_exchange, self._exchange),
                default_product_id=default_product_id,
            )
            if command is not None:
                parsed.append(command)
        return parsed

    async def get_skip_daily_close(self) -> bool:
        return await self._redis.exists(self._key_skip_close) > 0

    async def set_skip_daily_close(self, value: bool) -> None:
        if value:
            await self._redis.set(self._key_skip_close, "1")
        else:
            await self._redis.delete(self._key_skip_close)

    async def get_started_at(self) -> datetime | None:
        raw = await self._redis.get(self._key_started_at)
        if raw:
            return datetime.fromisoformat(raw)
        return None

    async def set_started_at(self, value: datetime) -> None:
        await self._redis.set(self._key_started_at, value.isoformat())

    async def set_worker_started_at(self, value: datetime) -> None:
        key = _worker_started_at_key(tenant_id=self._tenant_id, exchange=self._exchange)
        await self._redis.set(key, value.isoformat())

    async def get_worker_started_at(self) -> datetime | None:
        key = _worker_started_at_key(tenant_id=self._tenant_id, exchange=self._exchange)
        raw = await self._redis.get(key)
        if raw:
            return datetime.fromisoformat(raw)
        return None

    async def close(self) -> None:
        await self._redis.aclose()

    @staticmethod
    def _extract_payload(cmd: dict[str, Any]) -> dict[str, Any]:
        payload = cmd.get("payload")
        if isinstance(payload, dict):
            return dict(payload)
        return {
            key: value
            for key, value in cmd.items()
            if key
            not in {
                "protocol_version",
                "kind",
                "command_id",
                "command_type",
                "type",
                "tenant_id",
                "exchange",
                "product_id",
                "requested_at",
                "actor",
                "reason",
                "payload",
            }
        }

    def _build_command_payload(
        self,
        cmd: dict[str, Any],
        *,
        exchange: str,
        tenant_id: str,
        product_id: str,
    ) -> dict[str, Any]:
        if not isinstance(cmd, dict):
            raise ValueError("Command payload must be a dict")
        command_type = str(cmd.get("command_type") or cmd.get("type") or "").strip().lower()
        if not command_type:
            raise ValueError("Command is missing required field 'type'")
        payload = self._extract_payload(cmd)
        actor = cmd.get("actor")
        if actor is None:
            actor = cmd.get("triggered_by")
        actor_str = str(actor).strip() if isinstance(actor, str) and actor.strip() else None
        reason = str(cmd.get("reason")).strip() if isinstance(cmd.get("reason"), str) and str(cmd.get("reason")).strip() else None
        command_id = str(cmd.get("command_id")).strip() if isinstance(cmd.get("command_id"), str) else None
        requested_at = None
        if isinstance(cmd.get("requested_at"), str):
            try:
                requested_at = datetime.fromisoformat(cmd["requested_at"])
            except ValueError:
                requested_at = None
        envelope = build_command_envelope(
            command_type=command_type,
            tenant_id=tenant_id,
            exchange=exchange,
            product_id=product_id,
            payload=payload,
            actor=actor_str,
            reason=reason,
            command_id=command_id,
            requested_at=requested_at,
        )
        return envelope
