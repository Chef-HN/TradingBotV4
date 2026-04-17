from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any, Literal
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel

from api.auth.dependencies import ApiPrincipal, require_api_auth
from infrastructure.persistence.database import AsyncSessionFactory
from infrastructure.persistence.repositories.tenant_pair_strategy_repository import (
    TenantPairStrategyRepository,
)
from infrastructure.state_store import StateStore

router = APIRouter()

_store: StateStore | None = None

_SCHEDULE_FIELDS = {"local_timezone_iana", "daily_close_hour", "daily_close_minute"}


def set_state_store(store: StateStore) -> None:
    global _store
    _store = store


def _get_store() -> StateStore:
    if _store is None:
        raise HTTPException(status_code=503, detail="State store not initialized")
    return _store


def _validate_timezone_iana(value: str) -> str:
    try:
        ZoneInfo(value)
    except ZoneInfoNotFoundError as exc:
        raise HTTPException(status_code=400, detail=f"Invalid IANA timezone: {value}") from exc
    return value


def _validate_params(params: dict[str, Any]) -> None:
    if "local_timezone_iana" in params:
        params["local_timezone_iana"] = _validate_timezone_iana(str(params["local_timezone_iana"]))

    if "daily_close_hour" in params:
        hour = int(params["daily_close_hour"])
        if hour < 0 or hour > 23:
            raise HTTPException(status_code=400, detail="daily_close_hour must be in [0, 23]")
    if "daily_close_minute" in params:
        minute = int(params["daily_close_minute"])
        if minute < 0 or minute > 59:
            raise HTTPException(status_code=400, detail="daily_close_minute must be in [0, 59]")

    positive_numeric = (
        "spacing_bps",
        "rebalance_threshold_bps",
        "level_size_quote",
        "max_inventory_ratio",
        "maker_fee_rate",
        "stale_reprice_threshold_bps",
        "total_wallet_usd",
        "session_capital_usd",
        "rebalance_defer_max_drift_bps",
        "spread_freeze_bps",
        "regime_stress_spread_bps",
        "regime_trend_slope_threshold",
        "regime_mr_distance_threshold_bps",
        "regime_hysteresis_bps",
        "regime_rsi_bear_threshold",
        "regime_rsi_bull_threshold",
    )
    for key in positive_numeric:
        if key in params and Decimal(str(params[key])) <= 0:
            raise HTTPException(status_code=400, detail=f"{key} must be > 0")

    positive_int = (
        "grid_levels",
        "stale_order_age_seconds",
        "rebalance_defer_seconds",
        "ws_retry_window_seconds",
        "ws_initial_retry_delay_seconds",
        "ws_max_retry_delay_seconds",
        "ws_message_timeout_seconds",
        "ws_heartbeat_timeout_seconds",
    )
    for key in positive_int:
        if key in params and int(params[key]) <= 0:
            raise HTTPException(status_code=400, detail=f"{key} must be > 0")


class StrategyChangeRequest(BaseModel):
    params: dict[str, Any]
    updated_by: str = "system"
    reason: str | None = None
    schedule_change_mode: Literal["next_cycle", "immediate"] | None = None
    confirm_immediate: bool = False


class TimezoneChangeRequest(BaseModel):
    local_timezone_iana: str
    apply_mode: Literal["next_cycle", "immediate"] = "next_cycle"
    updated_by: str = "system"
    reason: str | None = None
    confirm_immediate: bool = False


@router.get("/strategies/{exchange_name}/{product_id}/resolved")
async def get_resolved_pair_strategy(
    exchange_name: str,
    product_id: str,
    principal: ApiPrincipal = Depends(require_api_auth),
) -> dict:
    async with AsyncSessionFactory() as db:
        repo = TenantPairStrategyRepository(db)
        row = await repo.get_active(principal.tenant_id, exchange_name.lower(), product_id.upper())
    if row is None:
        raise HTTPException(
            status_code=404,
            detail=f"No active pair strategy for tenant={principal.tenant_id} exchange={exchange_name} pair={product_id}",
        )
    return TenantPairStrategyRepository.row_to_dict(row)


@router.post("/strategies/{exchange_name}/{product_id}/changes/preview")
async def preview_pair_strategy_change(
    exchange_name: str,
    product_id: str,
    body: StrategyChangeRequest,
    principal: ApiPrincipal = Depends(require_api_auth),
) -> dict:
    if not body.params:
        raise HTTPException(status_code=400, detail="params cannot be empty")
    params = dict(body.params)
    _validate_params(params)
    async with AsyncSessionFactory() as db:
        repo = TenantPairStrategyRepository(db)
        try:
            return await repo.preview_changes(
                tenant_id=principal.tenant_id,
                exchange_name=exchange_name.lower(),
                product_id=product_id.upper(),
                params=params,
                proposed_by=body.updated_by,
                reason=body.reason,
            )
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/strategies/{exchange_name}/{product_id}/changes/apply")
async def apply_pair_strategy_change(
    exchange_name: str,
    product_id: str,
    body: StrategyChangeRequest,
    principal: ApiPrincipal = Depends(require_api_auth),
) -> dict:
    if not body.params:
        raise HTTPException(status_code=400, detail="params cannot be empty")
    params = dict(body.params)
    _validate_params(params)
    schedule_changed = any(k in params for k in _SCHEDULE_FIELDS)

    mode = body.schedule_change_mode or ("next_cycle" if schedule_changed else None)
    if mode == "immediate" and not body.confirm_immediate:
        raise HTTPException(
            status_code=409,
            detail="Immediate timezone schedule change requires confirm_immediate=true",
        )

    async with AsyncSessionFactory() as db:
        repo = TenantPairStrategyRepository(db)
        try:
            row = await repo.apply_changes(
                tenant_id=principal.tenant_id,
                exchange_name=exchange_name.lower(),
                product_id=product_id.upper(),
                params=params,
                updated_by=body.updated_by,
                reason=body.reason,
            )
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    if schedule_changed and mode is not None:
        cmd = {
            "type": "update_daily_close_schedule",
            "product_id": product_id.upper(),
            "local_timezone_iana": row.local_timezone_iana,
            "daily_close_hour": row.daily_close_hour,
            "daily_close_minute": row.daily_close_minute,
            "mode": mode,
        }
        await _get_store().push_command_to_exchange(
            exchange_name.lower(),
            cmd,
            tenant_id=principal.tenant_id,
            product_id=product_id.upper(),
        )

    payload = TenantPairStrategyRepository.row_to_dict(row)
    if schedule_changed and mode is not None:
        payload["schedule_change_mode_applied"] = mode
    return payload


@router.post("/sessions/{exchange_name}/{product_id}/timezone-change")
async def request_timezone_change(
    exchange_name: str,
    product_id: str,
    body: TimezoneChangeRequest,
    principal: ApiPrincipal = Depends(require_api_auth),
) -> dict:
    _validate_timezone_iana(body.local_timezone_iana)
    if body.apply_mode == "immediate" and not body.confirm_immediate:
        raise HTTPException(
            status_code=409,
            detail="Immediate timezone change requires confirm_immediate=true",
        )

    params = {"local_timezone_iana": body.local_timezone_iana}
    async with AsyncSessionFactory() as db:
        repo = TenantPairStrategyRepository(db)
        try:
            row = await repo.apply_changes(
                tenant_id=principal.tenant_id,
                exchange_name=exchange_name.lower(),
                product_id=product_id.upper(),
                params=params,
                updated_by=body.updated_by,
                reason=body.reason,
            )
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    cmd = {
        "type": "update_daily_close_schedule",
        "product_id": product_id.upper(),
        "local_timezone_iana": row.local_timezone_iana,
        "daily_close_hour": row.daily_close_hour,
        "daily_close_minute": row.daily_close_minute,
        "mode": body.apply_mode,
    }
    await _get_store().push_command_to_exchange(
        exchange_name.lower(),
        cmd,
        tenant_id=principal.tenant_id,
        product_id=product_id.upper(),
    )
    return {
        "tenant_id": principal.tenant_id,
        "exchange_name": exchange_name.lower(),
        "product_id": product_id.upper(),
        "apply_mode": body.apply_mode,
        "timezone": row.local_timezone_iana,
        "daily_close_hour": row.daily_close_hour,
        "daily_close_minute": row.daily_close_minute,
    }


@router.get("/analysis/window")
async def get_analysis_window(
    exchange_name: str = Query(...),
    product_id: str = Query(...),
    principal: ApiPrincipal = Depends(require_api_auth),
) -> dict:
    now = datetime.now(UTC)
    fallback_start = now - timedelta(hours=24)
    async with AsyncSessionFactory() as db:
        repo = TenantPairStrategyRepository(db)
        row = await repo.get_active(principal.tenant_id, exchange_name.lower(), product_id.upper())
        if row is None:
            raise HTTPException(
                status_code=404,
                detail=f"No active pair strategy for tenant={principal.tenant_id} exchange={exchange_name} pair={product_id}",
            )
        last_param_change = await repo.get_last_param_change(principal.tenant_id, exchange_name.lower(), product_id.upper())

    warning = None
    source = "tenant_pair_strategy_history"
    if last_param_change is None:
        source = "tenant_pair_strategies.updated_at"
        warning = "tenant_pair_strategy_history empty; fallback to tenant_pair_strategies.updated_at"
        last_param_change = row.updated_at
    start = max(fallback_start, last_param_change)
    return {
        "tenant_id": principal.tenant_id,
        "exchange_name": row.exchange_name,
        "product_id": row.product_id,
        "window_start": start.isoformat(),
        "window_end": now.isoformat(),
        "last_param_change": last_param_change.isoformat() if last_param_change else None,
        "source": source,
        "warning": warning,
    }
