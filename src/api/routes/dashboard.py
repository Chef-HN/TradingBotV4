from __future__ import annotations

import asyncio
import re
import subprocess
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Literal
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from api.schemas.dashboard import BotStatus, LevelSchema, SymbolSummary
from infrastructure.persistence.database import AsyncSessionFactory
from infrastructure.persistence.repositories.exchange_strategy_repository import ExchangeStrategyRepository
from infrastructure.persistence.repositories.grid_repository import GridRepository
from infrastructure.state_store import StateStore

_ENV_PATH = Path(__file__).resolve().parents[3] / ".env"
_SUPPORTED_EXCHANGES = {"coinbase", "bybit"}
_WINDOWS_TZ_TO_IANA = {
    "Romance Daylight Time": "Europe/Paris",
    "Romance Standard Time": "Europe/Paris",
    "W. Europe Daylight Time": "Europe/Berlin",
    "W. Europe Standard Time": "Europe/Berlin",
    "GMT Standard Time": "Europe/London",
    "GMT Daylight Time": "Europe/London",
    "Singapore Standard Time": "Asia/Singapore",
}

router = APIRouter()

# Injected at startup by run_api.py
_store: StateStore | None = None


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


async def _resolve_target_exchanges(store: StateStore, requested: str | None) -> list[str]:
    if requested:
        exchange = requested.strip().lower()
        if exchange not in _SUPPORTED_EXCHANGES:
            raise HTTPException(status_code=400, detail=f"Unsupported exchange: {exchange}")
        return [exchange]
    exchanges = sorted(_SUPPORTED_EXCHANGES)
    alive = await asyncio.gather(
        *(store.worker_alive(ex) for ex in exchanges),
        return_exceptions=True,
    )
    targets = [ex for ex, ok in zip(exchanges, alive) if ok is True]
    if targets:
        return targets
    # Fallback to configured exchange to avoid legacy queue usage.
    return [_get_env_exchange()]


def _get_env_exchange() -> str:
    import os
    name = os.environ.get("EXCHANGE_NAME", "").strip().lower()
    if name in _SUPPORTED_EXCHANGES:
        return name
    if _ENV_PATH.exists():
        text = _ENV_PATH.read_text(encoding="utf-8")
        m = re.search(r"^EXCHANGE_NAME=(.*)$", text, re.MULTILINE)
        parsed = m.group(1).strip().lower() if m else ""
        if parsed in _SUPPORTED_EXCHANGES:
            return parsed
    return "coinbase"


def _detect_local_timezone_iana() -> str:
    local_now = datetime.now().astimezone()
    tz_key = getattr(local_now.tzinfo, "key", None)
    if isinstance(tz_key, str) and tz_key:
        return tz_key
    tz_name = str(local_now.tzinfo)
    mapped = _WINDOWS_TZ_TO_IANA.get(tz_name)
    if mapped:
        return mapped
    return "UTC"


async def _get_state_for_exchange(exchange: str | None) -> dict:
    """Get worker state, optionally for a specific exchange."""
    store = _get_store()
    if exchange:
        data = await store.get_state_for_exchange(exchange.lower())
    else:
        # Try each exchange, return first found
        for ex in _SUPPORTED_EXCHANGES:
            data = await store.get_state_for_exchange(ex)
            if data is not None:
                return data
        data = await store.get_state()
    if data is None:
        raise HTTPException(status_code=503, detail="Worker state unavailable — bot may not be running")
    return data


@router.get("/status", response_model=BotStatus)
async def get_status(exchange: str | None = Query(default=None)) -> BotStatus:
    data = await _get_state_for_exchange(exchange)
    store = _get_store()
    if exchange:
        alive = await store.worker_alive(exchange.lower())
    else:
        # Check all supported exchanges
        results = await asyncio.gather(
            *(store.worker_alive(ex) for ex in _SUPPORTED_EXCHANGES),
            return_exceptions=True,
        )
        alive = any(r is True for r in results)
    return BotStatus.model_validate({**data, "worker_alive": alive})


class SkipCloseRequest(BaseModel):
    exchange: str = ""


@router.post("/skip-daily-close")
async def skip_daily_close(body: SkipCloseRequest = SkipCloseRequest()) -> dict:
    store = _get_store()
    cmd = {"type": "skip_daily_close"}
    targets = await _resolve_target_exchanges(store, body.exchange or None)
    for exchange in targets:
        await store.push_command_to_exchange(exchange, cmd)
    return {"status": "daily_close_skipped", "targets": targets}


@router.get("/fills/{product_id}")
async def get_fills(product_id: str, exchange: str | None = Query(default=None), limit: int = 50) -> list[dict]:
    data = await _get_state_for_exchange(exchange)

    # Find the current session_id for this product_id from Redis state
    session_id: str | None = None
    for sym in data.get("symbols", []):
        if sym.get("product_id") == product_id:
            session_id = sym.get("session_id")
            break

    if session_id is None:
        raise HTTPException(status_code=404, detail=f"Unknown symbol: {product_id}")

    async with AsyncSessionFactory() as db:
        repo = GridRepository(db)
        fills = await repo.get_recent_fills(product_id, session_id, limit)

    return fills


@router.get("/exchange")
async def get_exchange() -> dict:
    """Return the currently configured exchange."""
    name = _get_env_exchange()
    return {"exchange": name, "supported": sorted(_SUPPORTED_EXCHANGES)}


class ExchangeRequest(BaseModel):
    exchange: str


@router.post("/exchange")
async def set_exchange(body: ExchangeRequest) -> dict:
    """Update EXCHANGE_NAME in .env. Worker restart required to take effect."""
    name = body.exchange.lower().strip()
    if name not in _SUPPORTED_EXCHANGES:
        raise HTTPException(status_code=400, detail=f"Unsupported exchange: {name}. Choose from {_SUPPORTED_EXCHANGES}")
    text = _ENV_PATH.read_text(encoding="utf-8")
    if re.search(r"^EXCHANGE_NAME=", text, re.MULTILINE):
        text = re.sub(r"^EXCHANGE_NAME=.*$", f"EXCHANGE_NAME={name}", text, flags=re.MULTILINE)
    else:
        text = f"EXCHANGE_NAME={name}\n" + text
    _ENV_PATH.write_text(text, encoding="utf-8")
    return {"exchange": name, "status": "updated", "note": "Restart the worker to apply"}


class RestartRequest(BaseModel):
    exchange: str


@router.post("/restart-worker")
async def restart_worker(body: RestartRequest) -> dict:
    """Restart the worker via supervisorctl."""
    exchange = body.exchange.lower().strip()
    if exchange not in _SUPPORTED_EXCHANGES:
        raise HTTPException(status_code=400, detail=f"Unsupported exchange: {exchange}")

    result = await asyncio.to_thread(
        subprocess.run,
        ["supervisorctl", "restart", "worker"],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        raise HTTPException(status_code=500, detail=result.stderr.strip() or "Failed to restart worker")
    return {"status": "restarted", "exchange": exchange}


@router.get("/pnl")
async def get_pnl(exchange: str | None = Query(default=None)) -> dict:
    data = await _get_state_for_exchange(exchange)

    result = {}
    for sym in data.get("symbols", []):
        product_id = sym.get("product_id", "")
        session_id = sym.get("session_id")
        current_equity = float(sym.get("total_equity", "0"))

        # Last close delta: equity change of the most recently closed session
        # Uptime PnL: sum of equity deltas of all sessions since worker started_at
        pnl_last_close = None
        pnl_since_uptime_start = None
        product_id_sym = sym.get("product_id", "")
        started_at_str = data.get("started_at")
        async with AsyncSessionFactory() as db:
            repo = GridRepository(db)
            try:
                pnl_last_close = await repo.get_last_close_delta(product_id_sym)
            except Exception:
                pass
            try:
                if started_at_str:
                    since = datetime.fromisoformat(started_at_str)
                    pnl_since_uptime_start = await repo.get_uptime_pnl(
                        product_id_sym, since, current_equity
                    )
            except Exception:
                pass

        result[product_id] = {
            "realized_pnl": sym.get("realized_pnl", "0"),
            "unrealized_pnl": sym.get("unrealized_pnl", "0"),
            "total_equity": sym.get("total_equity", "0"),
            "reserve_usd": sym.get("reserve_usd", "0"),
            "total_fills": sym.get("total_fills", 0),
            "win_rate": "N/A",
            "pnl_last_close": pnl_last_close,
            "pnl_since_uptime_start": pnl_since_uptime_start,
        }
    return result


# ---------------------------------------------------------------------------
# Process management
# ---------------------------------------------------------------------------

@router.get("/processes")
async def get_processes() -> dict:
    """Return status of all supervised processes via supervisorctl."""
    result = await asyncio.to_thread(
        subprocess.run,
        ["supervisorctl", "status"],
        capture_output=True, text=True,
    )
    processes = {}
    for line in result.stdout.splitlines():
        parts = line.split()
        if len(parts) < 2:
            continue
        name = parts[0]
        status = parts[1]
        pid = None
        m = re.search(r"pid (\d+)", line)
        if m:
            pid = int(m.group(1))
        processes[name] = {
            "pid": pid,
            "alive": status == "RUNNING",
            "started_at": None,
            "symbols": [],
        }
    return processes


# ---------------------------------------------------------------------------
# Exchange strategy management
# ---------------------------------------------------------------------------

class StrategyRequest(BaseModel):
    name: str
    exchange_name: str
    spacing_bps: Decimal
    rebalance_threshold_bps: Decimal
    grid_levels: int
    level_size_quote: Decimal
    max_inventory_ratio: Decimal
    maker_fee_rate: Decimal
    stale_reprice_threshold_bps: Decimal
    stale_order_age_seconds: int
    local_timezone_iana: str = "UTC"
    daily_close_hour: int = 0
    daily_close_minute: int = 0
    spread_freeze_bps: Decimal = Decimal("50")
    regime_stress_spread_bps: Decimal = Decimal("35")
    regime_trend_slope_threshold: Decimal = Decimal("0.0005")
    regime_mr_distance_threshold_bps: Decimal = Decimal("18")
    regime_hysteresis_bps: Decimal = Decimal("4")
    regime_rsi_bear_threshold: Decimal = Decimal("42")
    regime_rsi_bull_threshold: Decimal = Decimal("58")
    ws_retry_window_seconds: int = 3600
    ws_initial_retry_delay_seconds: int = 5
    ws_max_retry_delay_seconds: int = 60
    ws_message_timeout_seconds: int = 90
    ws_heartbeat_timeout_seconds: int = 30
    schedule_change_mode: Literal["next_cycle", "immediate"] | None = None
    symbol_overrides: dict | None = None
    updated_by: str = "abraham"


class StrategyPatchRequest(BaseModel):
    """PATCH: only the fields you want to change + who is making the change.
    If product_id is set, changes go into symbol_overrides for that pair only."""
    updated_by: str = "abraham"
    product_id: str | None = None
    spacing_bps: Decimal | None = None
    rebalance_threshold_bps: Decimal | None = None
    grid_levels: int | None = None
    level_size_quote: Decimal | None = None
    max_inventory_ratio: Decimal | None = None
    maker_fee_rate: Decimal | None = None
    stale_reprice_threshold_bps: Decimal | None = None
    stale_order_age_seconds: int | None = None
    symbol_overrides: dict | None = None
    total_wallet_usd: Decimal | None = None
    session_capital_usd: Decimal | None = None
    maker_only: bool | None = None
    paper_mode: bool | None = None
    symbols: str | None = None
    rebalance_defer_seconds: int | None = None
    rebalance_defer_max_drift_bps: Decimal | None = None
    local_timezone_iana: str | None = None
    daily_close_hour: int | None = None
    daily_close_minute: int | None = None
    spread_freeze_bps: Decimal | None = None
    regime_stress_spread_bps: Decimal | None = None
    regime_trend_slope_threshold: Decimal | None = None
    regime_mr_distance_threshold_bps: Decimal | None = None
    regime_hysteresis_bps: Decimal | None = None
    regime_rsi_bear_threshold: Decimal | None = None
    regime_rsi_bull_threshold: Decimal | None = None
    ws_retry_window_seconds: int | None = None
    ws_initial_retry_delay_seconds: int | None = None
    ws_max_retry_delay_seconds: int | None = None
    ws_message_timeout_seconds: int | None = None
    ws_heartbeat_timeout_seconds: int | None = None
    schedule_change_mode: Literal["next_cycle", "immediate"] | None = None


def _validate_strategy_params(params: dict) -> None:
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


_SCHEDULE_FIELDS = {"local_timezone_iana", "daily_close_hour", "daily_close_minute"}
_PAIR_SCOPED_FIELDS = {
    "spacing_bps",
    "rebalance_threshold_bps",
    "grid_levels",
    "level_size_quote",
    "stale_reprice_threshold_bps",
    "stale_order_age_seconds",
    "rebalance_defer_seconds",
    "rebalance_defer_max_drift_bps",
    "max_inventory_ratio",
    "session_capital_usd",
    "maker_only",
}


def _build_schedule_change_command(row, mode: str) -> dict:
    return {
        "type": "update_daily_close_schedule",
        "local_timezone_iana": row.local_timezone_iana,
        "daily_close_hour": row.daily_close_hour,
        "daily_close_minute": row.daily_close_minute,
        "mode": mode,
    }


def _normalize_schedule_mode(mode: str | None) -> str | None:
    if mode is None:
        return None
    cleaned = mode.strip().lower()
    if cleaned not in {"next_cycle", "immediate"}:
        raise HTTPException(
            status_code=400,
            detail="schedule_change_mode must be 'next_cycle' or 'immediate'",
        )
    return cleaned


def _validate_pair_scoped_patch(params: dict, product_id: str | None) -> None:
    """Prevent global edits of pair-specific strategy parameters."""
    if product_id is not None:
        return
    global_fields = sorted(field for field in params if field in _PAIR_SCOPED_FIELDS)
    if global_fields:
        raise HTTPException(
            status_code=400,
            detail=(
                "Pair-specific parameters require product_id. "
                f"Global update blocked for fields: {global_fields}"
            ),
        )


@router.get("/strategies")
async def list_strategies() -> list[dict]:
    """List all exchange strategies stored in DB."""
    async with AsyncSessionFactory() as db:
        repo = ExchangeStrategyRepository(db)
        rows = await repo.list_all()
    return [ExchangeStrategyRepository.row_to_dict(r) for r in rows]


@router.get("/strategies/{exchange_name}")
async def get_strategy(exchange_name: str) -> dict:
    """Get the active strategy for a given exchange."""
    async with AsyncSessionFactory() as db:
        repo = ExchangeStrategyRepository(db)
        row = await repo.get_active(exchange_name.lower())
    if row is None:
        raise HTTPException(status_code=404, detail=f"No active strategy for exchange: {exchange_name}")
    return ExchangeStrategyRepository.row_to_dict(row)


@router.get("/strategies/{exchange_name}/resolved")
async def get_resolved_strategy(exchange_name: str) -> dict:
    """Get the active strategy with resolved per-symbol params."""
    async with AsyncSessionFactory() as db:
        repo = ExchangeStrategyRepository(db)
        row = await repo.get_active(exchange_name.lower())
    if row is None:
        raise HTTPException(status_code=404, detail=f"No active strategy for exchange: {exchange_name}")
    base = ExchangeStrategyRepository.row_to_dict(row)
    symbols = [s.strip() for s in (row.symbols or "").split(",") if s.strip()]
    per_symbol = {}
    for sym in symbols:
        per_symbol[sym] = repo.get_resolved_params(row, sym)
    return {"base": base, "symbols": per_symbol}


@router.get("/strategies/{exchange_name}/analysis-window")
async def get_strategy_analysis_window(exchange_name: str) -> dict:
    """Temporal window for parameter evaluation: max(now-24h, last_param_change)."""
    now = datetime.now(UTC)
    fallback_start = now - timedelta(hours=24)
    async with AsyncSessionFactory() as db:
        repo = ExchangeStrategyRepository(db)
        row = await repo.get_active(exchange_name.lower())
        if row is None:
            raise HTTPException(status_code=404, detail=f"No active strategy for exchange: {exchange_name}")
        last_param_change = await repo.get_last_param_change(row.id)

    warning = None
    source = "strategy_param_history"
    if last_param_change is None:
        source = "exchange_strategies.updated_at"
        warning = "strategy_param_history empty; fallback to exchange_strategies.updated_at"
        last_param_change = row.updated_at
    start = max(fallback_start, last_param_change)
    return {
        "exchange": row.exchange_name,
        "strategy_name": row.name,
        "window_start": start.isoformat(),
        "window_end": now.isoformat(),
        "last_param_change": last_param_change.isoformat() if last_param_change else None,
        "source": source,
        "warning": warning,
    }


@router.get("/strategies/{exchange_name}/timezone-drift")
async def get_strategy_timezone_drift(exchange_name: str) -> dict:
    """Compare strategy close timezone vs detected host local timezone."""
    async with AsyncSessionFactory() as db:
        repo = ExchangeStrategyRepository(db)
        row = await repo.get_active(exchange_name.lower())
    if row is None:
        raise HTTPException(status_code=404, detail=f"No active strategy for exchange: {exchange_name}")

    detected = _detect_local_timezone_iana()
    drift = detected != row.local_timezone_iana
    return {
        "exchange": row.exchange_name,
        "strategy_name": row.name,
        "configured_timezone": row.local_timezone_iana,
        "detected_local_timezone": detected,
        "drift_detected": drift,
        "message": (
            "Timezone changed. Keep current close cycle (next_cycle) or apply immediately (immediate)."
            if drift else
            "Configured timezone matches detected local timezone."
        ),
    }


@router.put("/strategies/{strategy_name}")
async def upsert_strategy(strategy_name: str, body: StrategyRequest) -> dict:
    """Create or full-update a strategy by name. Sets it as active for its exchange."""
    params = body.model_dump()
    _validate_strategy_params(params)
    schedule_mode = _normalize_schedule_mode(body.schedule_change_mode)
    schedule_changed = False
    previous_schedule: dict | None = None
    async with AsyncSessionFactory() as db:
        repo = ExchangeStrategyRepository(db)
        existing = await repo.get_by_name(strategy_name)
        if existing is not None:
            previous_schedule = {
                "local_timezone_iana": existing.local_timezone_iana,
                "daily_close_hour": existing.daily_close_hour,
                "daily_close_minute": existing.daily_close_minute,
            }
            next_schedule = {
                "local_timezone_iana": body.local_timezone_iana,
                "daily_close_hour": body.daily_close_hour,
                "daily_close_minute": body.daily_close_minute,
            }
            schedule_changed = previous_schedule != next_schedule
            if schedule_changed and schedule_mode is None:
                raise HTTPException(
                    status_code=409,
                    detail={
                        "message": (
                            "Daily close schedule change detected. "
                            "Confirm with schedule_change_mode='next_cycle' or 'immediate'."
                        ),
                        "previous_schedule": previous_schedule,
                        "requested_schedule": next_schedule,
                    },
                )

        row = await repo.upsert(
            name=strategy_name,
            exchange_name=body.exchange_name.lower(),
            updated_by=body.updated_by,
            spacing_bps=body.spacing_bps,
            rebalance_threshold_bps=body.rebalance_threshold_bps,
            grid_levels=body.grid_levels,
            level_size_quote=body.level_size_quote,
            max_inventory_ratio=body.max_inventory_ratio,
            maker_fee_rate=body.maker_fee_rate,
            stale_reprice_threshold_bps=body.stale_reprice_threshold_bps,
            stale_order_age_seconds=body.stale_order_age_seconds,
            local_timezone_iana=body.local_timezone_iana,
            daily_close_hour=body.daily_close_hour,
            daily_close_minute=body.daily_close_minute,
            spread_freeze_bps=body.spread_freeze_bps,
            regime_stress_spread_bps=body.regime_stress_spread_bps,
            regime_trend_slope_threshold=body.regime_trend_slope_threshold,
            regime_mr_distance_threshold_bps=body.regime_mr_distance_threshold_bps,
            regime_hysteresis_bps=body.regime_hysteresis_bps,
            regime_rsi_bear_threshold=body.regime_rsi_bear_threshold,
            regime_rsi_bull_threshold=body.regime_rsi_bull_threshold,
            ws_retry_window_seconds=body.ws_retry_window_seconds,
            ws_initial_retry_delay_seconds=body.ws_initial_retry_delay_seconds,
            ws_max_retry_delay_seconds=body.ws_max_retry_delay_seconds,
            ws_message_timeout_seconds=body.ws_message_timeout_seconds,
            ws_heartbeat_timeout_seconds=body.ws_heartbeat_timeout_seconds,
            symbol_overrides=body.symbol_overrides,
            make_active=True,
        )
    if schedule_changed and schedule_mode is not None:
        store = _get_store()
        cmd = _build_schedule_change_command(row, schedule_mode)
        await store.push_command_to_exchange(row.exchange_name.lower(), cmd)

    payload = ExchangeStrategyRepository.row_to_dict(row)
    if schedule_changed and schedule_mode is not None:
        payload["schedule_change_mode_applied"] = schedule_mode
    return payload


@router.patch("/strategies/{strategy_name}")
async def patch_strategy(strategy_name: str, body: StrategyPatchRequest) -> dict:
    """Partial update: only change the fields you send. Full SCD2 audit trail.
    If product_id is set, changes apply to that symbol's overrides only."""
    params = {
        k: v for k, v in body.model_dump(exclude={"updated_by", "product_id", "schedule_change_mode"}).items()
        if v is not None
    }
    if not params:
        raise HTTPException(status_code=400, detail="No fields to update")
    _validate_strategy_params(params)
    _validate_pair_scoped_patch(params, body.product_id)
    schedule_mode = _normalize_schedule_mode(body.schedule_change_mode)
    schedule_changed = False
    previous_schedule: dict | None = None
    requested_schedule: dict | None = None
    async with AsyncSessionFactory() as db:
        repo = ExchangeStrategyRepository(db)
        try:
            existing = await repo.get_by_name(strategy_name)
            if existing is None:
                raise HTTPException(status_code=404, detail=f"Strategy not found: {strategy_name}")

            if body.product_id is None and any(field in params for field in _SCHEDULE_FIELDS):
                previous_schedule = {
                    "local_timezone_iana": existing.local_timezone_iana,
                    "daily_close_hour": existing.daily_close_hour,
                    "daily_close_minute": existing.daily_close_minute,
                }
                requested_schedule = {
                    "local_timezone_iana": params.get("local_timezone_iana", existing.local_timezone_iana),
                    "daily_close_hour": int(params.get("daily_close_hour", existing.daily_close_hour)),
                    "daily_close_minute": int(params.get("daily_close_minute", existing.daily_close_minute)),
                }
                schedule_changed = previous_schedule != requested_schedule
                if schedule_changed and schedule_mode is None:
                    raise HTTPException(
                        status_code=409,
                        detail={
                            "message": (
                                "Daily close schedule change detected. "
                                "Confirm with schedule_change_mode='next_cycle' or 'immediate'."
                            ),
                            "previous_schedule": previous_schedule,
                            "requested_schedule": requested_schedule,
                        },
                    )

            row = await repo.update_strategy_params(
                strategy_name=strategy_name,
                updated_by=body.updated_by,
                params=params,
                product_id=body.product_id,
            )
        except ValueError as e:
            raise HTTPException(status_code=404, detail=str(e))

    if schedule_changed and schedule_mode is not None:
        store = _get_store()
        cmd = _build_schedule_change_command(row, schedule_mode)
        await store.push_command_to_exchange(row.exchange_name.lower(), cmd)

    payload = ExchangeStrategyRepository.row_to_dict(row)
    if schedule_changed and schedule_mode is not None:
        payload["schedule_change_mode_applied"] = schedule_mode
    return payload


@router.get("/strategies/{strategy_name}/history")
async def get_strategy_history(strategy_name: str, limit: int = 50) -> list[dict]:
    """Return SCD2 parameter change history for a strategy."""
    async with AsyncSessionFactory() as db:
        repo = ExchangeStrategyRepository(db)
        return await repo.get_param_history(strategy_name, limit)


@router.post("/strategies/{strategy_name}/activate")
async def activate_strategy(strategy_name: str) -> dict:
    """Make a strategy the active one for its exchange."""
    async with AsyncSessionFactory() as db:
        repo = ExchangeStrategyRepository(db)
        row = await repo.set_active(strategy_name)
    if row is None:
        raise HTTPException(status_code=404, detail=f"Strategy not found: {strategy_name}")
    return ExchangeStrategyRepository.row_to_dict(row)
