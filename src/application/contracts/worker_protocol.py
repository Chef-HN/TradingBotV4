from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from infrastructure.tenancy import DEFAULT_TENANT_ID

WORKER_PROTOCOL_VERSION = "tb.v4.control/1"
WORKER_PROTOCOL_KIND_COMMAND = "command"
WORKER_PROTOCOL_KIND_STATE = "state"

COMMAND_RESET = "reset"
COMMAND_SKIP_DAILY_CLOSE = "skip_daily_close"
COMMAND_UPDATE_DAILY_CLOSE_SCHEDULE = "update_daily_close_schedule"

SUPPORTED_COMMAND_TYPES = {
    COMMAND_RESET,
    COMMAND_SKIP_DAILY_CLOSE,
    COMMAND_UPDATE_DAILY_CLOSE_SCHEDULE,
}

_COMMAND_RESERVED_FIELDS = {
    "protocol_version",
    "kind",
    "command_id",
    "command_type",
    "tenant_id",
    "exchange",
    "product_id",
    "requested_at",
    "actor",
    "reason",
    "payload",
    # Legacy root alias.
    "type",
}


def _norm_part(value: str | None, fallback: str) -> str:
    cleaned = (value or "").strip().lower()
    return cleaned or fallback


def _norm_product_id(value: str | None, fallback: str = "all") -> str:
    raw = (value or "").strip()
    if not raw:
        return fallback
    return raw.lower() if raw.lower() == "all" else raw.upper()


def _parse_iso_datetime(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _extract_payload(raw: dict[str, Any]) -> dict[str, Any]:
    payload = raw.get("payload")
    if isinstance(payload, dict):
        merged = dict(payload)
        for key, value in raw.items():
            if key in _COMMAND_RESERVED_FIELDS:
                continue
            merged.setdefault(key, value)
        return merged
    return {
        key: value
        for key, value in raw.items()
        if key not in _COMMAND_RESERVED_FIELDS
    }


@dataclass(frozen=True)
class RuntimeCommand:
    command_id: str
    command_type: str
    tenant_id: str
    exchange: str
    product_id: str
    actor: str | None
    reason: str | None
    requested_at: datetime
    payload: dict[str, Any]
    raw: dict[str, Any]


def build_command_envelope(
    *,
    command_type: str,
    tenant_id: str,
    exchange: str,
    product_id: str = "all",
    payload: dict[str, Any] | None = None,
    actor: str | None = None,
    reason: str | None = None,
    command_id: str | None = None,
    requested_at: datetime | None = None,
) -> dict[str, Any]:
    ctype = command_type.strip().lower()
    if ctype not in SUPPORTED_COMMAND_TYPES:
        raise ValueError(f"Unsupported command_type '{command_type}'")

    now = requested_at or datetime.now(UTC)
    now_utc = now if now.tzinfo else now.replace(tzinfo=UTC)
    now_utc = now_utc.astimezone(UTC)
    cmd_id = (command_id or str(uuid4())).strip() or str(uuid4())
    tid = _norm_part(tenant_id, DEFAULT_TENANT_ID)
    ex = _norm_part(exchange, "")
    pid = _norm_product_id(product_id, "all")
    body = dict(payload or {})

    envelope = {
        "protocol_version": WORKER_PROTOCOL_VERSION,
        "kind": WORKER_PROTOCOL_KIND_COMMAND,
        "command_id": cmd_id,
        "command_type": ctype,
        "type": ctype,  # legacy alias for older worker parsers
        "tenant_id": tid,
        "exchange": ex,
        "product_id": pid,
        "requested_at": now_utc.isoformat(),
        "actor": actor,
        "reason": reason,
        "payload": body,
    }
    # Backward-compatible flattening for workers still reading root fields only.
    for key, value in body.items():
        envelope.setdefault(key, value)
    return envelope


def parse_runtime_command(
    raw: dict[str, Any],
    *,
    default_tenant_id: str = DEFAULT_TENANT_ID,
    default_exchange: str = "",
    default_product_id: str = "all",
) -> RuntimeCommand | None:
    if not isinstance(raw, dict):
        return None

    ctype_raw = raw.get("command_type") or raw.get("type")
    if not isinstance(ctype_raw, str):
        return None
    ctype = ctype_raw.strip().lower()
    if ctype not in SUPPORTED_COMMAND_TYPES:
        return None

    command_id = str(raw.get("command_id") or "").strip() or str(uuid4())
    tenant_id = _norm_part(str(raw.get("tenant_id") or ""), _norm_part(default_tenant_id, DEFAULT_TENANT_ID))
    exchange = _norm_part(str(raw.get("exchange") or ""), _norm_part(default_exchange, ""))
    product_id = _norm_product_id(str(raw.get("product_id") or ""), _norm_product_id(default_product_id, "all"))
    actor = raw.get("actor")
    if actor is None:
        actor = raw.get("triggered_by")
    actor = str(actor).strip() if isinstance(actor, str) and actor.strip() else None
    reason = str(raw.get("reason")).strip() if isinstance(raw.get("reason"), str) and str(raw.get("reason")).strip() else None

    requested_at = _parse_iso_datetime(raw.get("requested_at")) or datetime.now(UTC)
    payload = _extract_payload(raw)

    return RuntimeCommand(
        command_id=command_id,
        command_type=ctype,
        tenant_id=tenant_id,
        exchange=exchange,
        product_id=product_id,
        actor=actor,
        reason=reason,
        requested_at=requested_at,
        payload=payload,
        raw=dict(raw),
    )


def build_state_payload(
    *,
    tenant_id: str,
    exchange: str,
    product_id: str = "all",
    state: dict[str, Any],
    emitted_at: datetime | None = None,
) -> dict[str, Any]:
    now = emitted_at or datetime.now(UTC)
    now_utc = now if now.tzinfo else now.replace(tzinfo=UTC)
    now_utc = now_utc.astimezone(UTC)
    payload = dict(state)
    payload["_protocol"] = {
        "protocol_version": WORKER_PROTOCOL_VERSION,
        "kind": WORKER_PROTOCOL_KIND_STATE,
        "tenant_id": _norm_part(tenant_id, DEFAULT_TENANT_ID),
        "exchange": _norm_part(exchange, ""),
        "product_id": _norm_product_id(product_id, "all"),
        "emitted_at": now_utc.isoformat(),
    }
    return payload
