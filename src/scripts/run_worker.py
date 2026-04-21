"""
TradingBotV3 â€” Grid Worker Process
Runs the grid engine only; publishes state to Redis and reads commands from Redis.

Run:
    python -m scripts.run_worker
"""
from __future__ import annotations

import asyncio
import os
import signal
import sys
from collections import deque
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace
from typing import Any
from uuid import uuid4
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from application.services.event_bus import InMemoryEventBus
from application.services.market_data_engine import MarketDataEngine
from application.services.trading_kernel import TradingKernel
from config import get_settings
from domain.enums import OrderSide
from domain.events import MarketTickReceived
from domain.models import Fill, GridLevel, GridState, MarketSnapshot, OrderIntent, RiskState
from domain.models.regime import RegimeState
from infrastructure.bybit import BybitAdapter
from infrastructure.bybit.rest import BybitRESTClient
from infrastructure.bybit.ws import BybitWebSocketClient
from infrastructure.coinbase import CoinbaseAdapter
from infrastructure.coinbase.rest import CoinbaseRESTClient
from infrastructure.coinbase.ws import CoinbaseWebSocketClient
from infrastructure.encryption import get_encryption_manager
from infrastructure.paper import PaperTradingAdapter
from infrastructure.persistence.database import AsyncSessionFactory
from infrastructure.persistence.repositories.credentials_repository import CredentialsRepository
from infrastructure.persistence.repositories.grid_repository import GridRepository
from infrastructure.persistence.repositories.tenant_pair_strategy_repository import (
    TenantPairStrategyRepository,
)
from infrastructure.state_store import StateStore
from risk.engine import RiskEngine
from strategy.neutral_grid import NeutralGridEngine
from strategy.regime import RegimeEngine

settings = get_settings()


# ---------------------------------------------------------------------------
# Runtime context per symbol
# ---------------------------------------------------------------------------

@dataclass
class SymbolContext:
    product_id: str
    session_id: Any  # UUID
    grid_state: GridState | None = None
    regime_state: RegimeState | None = None
    risk_state: RiskState | None = None
    market_snapshot: MarketSnapshot | None = None
    paper_adapter: PaperTradingAdapter | None = None
    recent_fills: deque = field(default_factory=lambda: deque(maxlen=200))
    stress_paused_until: datetime | None = None
    reset_requested: bool = False
    reset_triggered_by: str = "Abraham"
    reset_type: str = "daily_close"  # "daily_close" = reconcile, "hard" = back to defaults
    reserve_usd: Decimal = field(default_factory=lambda: Decimal("0"))
    underfunded: bool = False
    underfunded_shortfall_usd: Decimal = field(default_factory=lambda: Decimal("0"))


@dataclass
class RuntimeContext:
    mode: str
    started_at: datetime          # session/day start â€” preserved across restarts
    worker_started_at: datetime = field(default_factory=lambda: datetime.now(UTC))  # current process start
    symbol_contexts: dict[str, SymbolContext] = field(default_factory=dict)
    skip_daily_close: bool = False
    next_daily_close_at: datetime | None = None
    local_timezone_iana: str = "UTC"
    daily_close_hour: int = 0
    daily_close_minute: int = 0
    ws_retry_window_seconds: int = 3600
    ws_initial_retry_delay_seconds: int = 5
    ws_max_retry_delay_seconds: int = 60
    ws_message_timeout_seconds: int = 90
    ws_heartbeat_timeout_seconds: int = 30
    config_loaded: bool = False
    schedule_resync_requested: bool = False
    pending_schedule_after_close: dict[str, Any] | None = None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_LOG_DIR = os.environ.get("LOG_DIR", os.path.join(os.path.dirname(__file__), "../.."))
_LOG_FILE = os.path.join(_LOG_DIR, "v3.run.log")


def _emit(msg: str) -> None:
    line = f"[{datetime.now(UTC).strftime('%Y-%m-%d %H:%M:%S')}] {msg}"
    sys.stdout.buffer.write((line + "\n").encode("utf-8", errors="replace"))
    sys.stdout.buffer.flush()
    # Append to persistent rotating log (survives restarts)
    try:
        with open(_LOG_FILE, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass


async def _persist(coro) -> None:
    """Fire-and-forget DB write. Errors are logged but never crash the worker."""
    try:
        await coro
    except Exception as exc:
        _emit(f"[DB] write error: {exc}")


def _compute_next_daily_close_utc(
    *,
    now_utc: datetime,
    timezone_iana: str,
    close_hour: int,
    close_minute: int,
) -> tuple[datetime, datetime]:
    """Return (next_close_utc, next_close_local) for the configured local clock time."""
    local_tz = ZoneInfo(timezone_iana)
    local_now = now_utc.astimezone(local_tz)
    next_local = local_now.replace(
        hour=close_hour,
        minute=close_minute,
        second=0,
        microsecond=0,
    )
    if next_local <= local_now:
        next_local = next_local + timedelta(days=1)
    return next_local.astimezone(UTC), next_local


def _apply_runtime_schedule(
    runtime: RuntimeContext,
    *,
    timezone_iana: str,
    close_hour: int,
    close_minute: int,
) -> None:
    runtime.local_timezone_iana = timezone_iana
    runtime.daily_close_hour = close_hour
    runtime.daily_close_minute = close_minute


def _current_schedule_payload(runtime: RuntimeContext) -> dict[str, Any]:
    return {
        "local_timezone_iana": runtime.local_timezone_iana,
        "daily_close_hour": runtime.daily_close_hour,
        "daily_close_minute": runtime.daily_close_minute,
    }


def _apply_session_capital(
    base_qty: Decimal,
    quote_qty: Decimal,
    mid: Decimal,
    session_capital_usd: Decimal,
) -> tuple[Decimal, Decimal, Decimal]:
    """
    Cap deployed capital to session_capital_usd.

    If session_capital_usd == 0, returns balances unchanged (no cap).
    Otherwise scales both base and quote proportionally so that
    total_equity_usd <= session_capital_usd.
    Returns (base_qty, quote_qty, base_cost).
    """
    if session_capital_usd <= 0 or mid <= 0:
        return base_qty, quote_qty, base_qty * mid

    total_usd = quote_qty + base_qty * mid
    if total_usd <= 0:
        return base_qty, quote_qty, Decimal("0")

    cap = min(session_capital_usd, total_usd)
    ratio = cap / total_usd
    new_base = (base_qty * ratio).quantize(Decimal("0.00000001"))
    new_quote = (quote_qty * ratio).quantize(Decimal("0.00000001"))
    reserve = total_usd - cap
    _emit(
        f"Session capital cap: deploying ${cap:.2f} of ${total_usd:.2f} "
        f"(reserve=${reserve:.2f})"
    )
    return new_base, new_quote, new_base * mid


_SYMBOL_DECIMAL_OVERRIDE_FIELDS = (
    "spacing_bps",
    "rebalance_threshold_bps",
    "level_size_quote",
    "stale_reprice_threshold_bps",
    "rebalance_defer_max_drift_bps",
    "max_inventory_ratio",
    "session_capital_usd",
)
_SYMBOL_INT_OVERRIDE_FIELDS = (
    "grid_levels",
    "stale_order_age_seconds",
    "rebalance_defer_seconds",
)
_SYMBOL_BOOL_OVERRIDE_FIELDS = ("maker_only",)
_SYMBOL_REQUIRED_OVERRIDE_FIELDS = (
    *_SYMBOL_DECIMAL_OVERRIDE_FIELDS,
    *_SYMBOL_INT_OVERRIDE_FIELDS,
    *_SYMBOL_BOOL_OVERRIDE_FIELDS,
)


def _resolve_symbol_settings(base: Any, overrides: dict | None, symbol: str) -> Any:
    """Return StrategySettings for a symbol from pair-scoped overrides only."""
    sym_dict = (overrides or {}).get(symbol)
    if not sym_dict:
        raise RuntimeError(
            f"Missing per-symbol strategy for '{symbol}'. "
            "Global fallback is disabled; configure symbol_overrides in DB."
        )
    missing = [field for field in _SYMBOL_REQUIRED_OVERRIDE_FIELDS if field not in sym_dict]
    if missing:
        raise RuntimeError(
            f"Incomplete per-symbol strategy for '{symbol}'. "
            f"Missing fields: {missing}"
        )
    update: dict = {}
    for key in _SYMBOL_DECIMAL_OVERRIDE_FIELDS:
        if key in sym_dict:
            update[key] = Decimal(str(sym_dict[key]))
    for key in _SYMBOL_INT_OVERRIDE_FIELDS:
        if key in sym_dict:
            update[key] = int(sym_dict[key])
    for key in _SYMBOL_BOOL_OVERRIDE_FIELDS:
        if key in sym_dict:
            update[key] = bool(sym_dict[key])
    if not update:
        raise RuntimeError(
            f"Per-symbol strategy for '{symbol}' produced no settings update."
        )
    return base.model_copy(update=update)


_PAIR_GLOBAL_UNIFORM_FIELDS = (
    "paper_mode",
    "total_wallet_usd",
    "maker_fee_rate",
    "local_timezone_iana",
    "daily_close_hour",
    "daily_close_minute",
    "spread_freeze_bps",
    "regime_stress_spread_bps",
    "regime_trend_slope_threshold",
    "regime_mr_distance_threshold_bps",
    "regime_hysteresis_bps",
    "regime_rsi_bear_threshold",
    "regime_rsi_bull_threshold",
    "ws_retry_window_seconds",
    "ws_initial_retry_delay_seconds",
    "ws_max_retry_delay_seconds",
    "ws_message_timeout_seconds",
    "ws_heartbeat_timeout_seconds",
)


def _build_pair_strategy_synthetic_row(
    *,
    exchange_name: str,
    tenant_id: str,
    pair_rows: list[Any],
) -> Any:
    """
    Convert tenant_pair_strategies rows to a synthetic strategy object compatible
    with existing worker code path.
    """
    if not pair_rows:
        return None

    first = pair_rows[0]
    for field in _PAIR_GLOBAL_UNIFORM_FIELDS:
        values = {str(getattr(r, field)) for r in pair_rows}
        if len(values) > 1:
            raise RuntimeError(
                "Worker fail-fast: tenant_pair_strategies for "
                f"tenant={tenant_id} exchange={exchange_name} must share '{field}'. "
                f"Found values: {sorted(values)}"
            )

    sorted_rows = sorted(pair_rows, key=lambda r: str(r.product_id))
    symbols = [str(r.product_id).upper() for r in sorted_rows]
    symbol_overrides: dict[str, dict[str, Any]] = {}
    for row in sorted_rows:
        symbol = str(row.product_id).upper()
        symbol_overrides[symbol] = {
            "spacing_bps": float(row.spacing_bps),
            "rebalance_threshold_bps": float(row.rebalance_threshold_bps),
            "grid_levels": int(row.grid_levels),
            "level_size_quote": float(row.level_size_quote),
            "stale_reprice_threshold_bps": float(row.stale_reprice_threshold_bps),
            "stale_order_age_seconds": int(row.stale_order_age_seconds),
            "rebalance_defer_seconds": int(row.rebalance_defer_seconds),
            "rebalance_defer_max_drift_bps": float(row.rebalance_defer_max_drift_bps),
            "max_inventory_ratio": float(row.max_inventory_ratio),
            "session_capital_usd": float(row.session_capital_usd),
            "maker_only": bool(row.maker_only),
        }

    return SimpleNamespace(
        id=None,
        tenant_id=tenant_id,
        name=f"tenant-{tenant_id}-{exchange_name}-pair-strategy",
        exchange_name=exchange_name,
        is_active=True,
        symbols=",".join(symbols),
        spacing_bps=first.spacing_bps,
        rebalance_threshold_bps=first.rebalance_threshold_bps,
        grid_levels=first.grid_levels,
        level_size_quote=first.level_size_quote,
        max_inventory_ratio=first.max_inventory_ratio,
        maker_fee_rate=first.maker_fee_rate,
        stale_reprice_threshold_bps=first.stale_reprice_threshold_bps,
        stale_order_age_seconds=first.stale_order_age_seconds,
        rebalance_defer_seconds=first.rebalance_defer_seconds,
        rebalance_defer_max_drift_bps=first.rebalance_defer_max_drift_bps,
        paper_mode=first.paper_mode,
        total_wallet_usd=first.total_wallet_usd,
        session_capital_usd=first.session_capital_usd,
        maker_only=first.maker_only,
        local_timezone_iana=first.local_timezone_iana,
        daily_close_hour=first.daily_close_hour,
        daily_close_minute=first.daily_close_minute,
        spread_freeze_bps=first.spread_freeze_bps,
        regime_stress_spread_bps=first.regime_stress_spread_bps,
        regime_trend_slope_threshold=first.regime_trend_slope_threshold,
        regime_mr_distance_threshold_bps=first.regime_mr_distance_threshold_bps,
        regime_hysteresis_bps=first.regime_hysteresis_bps,
        regime_rsi_bear_threshold=first.regime_rsi_bear_threshold,
        regime_rsi_bull_threshold=first.regime_rsi_bull_threshold,
        ws_retry_window_seconds=first.ws_retry_window_seconds,
        ws_initial_retry_delay_seconds=first.ws_initial_retry_delay_seconds,
        ws_max_retry_delay_seconds=first.ws_max_retry_delay_seconds,
        ws_message_timeout_seconds=first.ws_message_timeout_seconds,
        ws_heartbeat_timeout_seconds=first.ws_heartbeat_timeout_seconds,
        symbol_overrides=symbol_overrides,
        updated_by=getattr(first, "updated_by", "system"),
    )


def _build_order_payload(intent: OrderIntent) -> dict:
    return {
        "client_order_id": intent.intent_id,
        "product_id": intent.product_id,
        "side": intent.side.value,
        "order_configuration": {
            "limit_limit_gtc": {
                "base_size": str(intent.size_base),
                "limit_price": str(intent.price),
                "post_only": intent.post_only,
            }
        },
    }


async def _load_credentials_from_db(exchange_name: str) -> dict[str, str] | None:
    """
    Load encrypted exchange credentials from database.

    Returns dict with 'api_key', 'api_secret' keys, or None if not found.
    """
    try:
        async with AsyncSessionFactory() as db:
            repo = CredentialsRepository(db)
            creds = await repo.get_credentials(
                exchange_name.lower(),
                tenant_id=settings.app.default_tenant_id,
            )

        if creds is None:
            _emit(f"No credentials found in DB for exchange: {exchange_name}")
            return None

        return {
            "api_key": creds.api_key,
            "api_secret": creds.api_secret,
            "api_passphrase": creds.api_passphrase,
        }
    except Exception as e:
        _emit(f"Failed to load credentials from DB: {e}")
        return None


# ---------------------------------------------------------------------------
# Per-symbol grid worker
# ---------------------------------------------------------------------------

class GridWorker:
    def __init__(
        self,
        product_id: str,
        runtime: RuntimeContext,
        grid_engine: NeutralGridEngine,
        regime_engine: RegimeEngine,
        risk_engine: RiskEngine,
        coinbase_adapter: CoinbaseAdapter | None,
        paper_mode: bool,
        state_store: StateStore,
        maker_fee_rate: Decimal = Decimal("0.0004"),
        bybit_adapter: BybitAdapter | None = None,
    ) -> None:
        self.product_id = product_id
        self.runtime = runtime
        self.grid_engine = grid_engine
        self.regime_engine = regime_engine
        self.risk_engine = risk_engine
        self.coinbase_adapter = coinbase_adapter
        self.bybit_adapter = bybit_adapter
        # Unified reference: whichever exchange is active
        self._exchange_adapter: CoinbaseAdapter | BybitAdapter | None = bybit_adapter or coinbase_adapter
        self.paper_mode = paper_mode
        self.state_store = state_store
        self.maker_fee_rate = maker_fee_rate
        self.kernel = TradingKernel(
            grid_engine=self.grid_engine,
            regime_engine=self.regime_engine,
            risk_engine=self.risk_engine,
        )

    def _ctx(self) -> SymbolContext:
        return self.runtime.symbol_contexts[self.product_id]

    async def initialize(self, initial_mid: Decimal) -> None:
        ctx = self._ctx()
        st = self.grid_engine.settings
        mode = self.runtime.mode
        resumed: dict | None = None

        if self.paper_mode:
            ctx.paper_adapter = PaperTradingAdapter(fee_rate=self.maker_fee_rate)

            # Try to resume last session from DB
            resumed = await self._db_load_last_session(self.product_id)
            if resumed:
                base_qty = resumed["base_inventory"]
                quote_qty = resumed["quote_inventory"]
                base_cost = resumed["base_inventory_cost"]
                ctx.session_id = resumed["session_id"]
                ctx.reserve_usd = resumed.get("reserve_usd", Decimal("0"))
                _emit(
                    f"[{self.product_id}] Resuming session {ctx.session_id} â€” "
                    f"base={base_qty:.4f} quote={quote_qty:.4f} pnl={resumed['realized_pnl_quote']:.4f} "
                    f"reserve=${ctx.reserve_usd:.2f}"
                )
            else:
                # Fresh start: deploy session capital 50/50 base+quote (neutral grid)
                session_capital = st.session_capital_usd
                half = session_capital / Decimal("2")
                base_qty = (half / initial_mid).quantize(Decimal("0.00000001"))
                quote_qty = half
                base_cost = base_qty * initial_mid
                ctx.reserve_usd = st.total_wallet_usd - session_capital
                _emit(
                    f"[{self.product_id}] Wallet: "
                    f"session=${session_capital} (50/50 split) reserve=${ctx.reserve_usd}"
                )
        else:
            assert self._exchange_adapter is not None
            balances = await self._exchange_adapter.get_balances()
            base_currency = self.product_id.split("-")[0]
            quote_currency = self.product_id.split("-")[1]
            base_qty = balances.get(base_currency, Decimal("0"))
            quote_qty = balances.get(quote_currency, Decimal("0"))
            base_cost = base_qty * initial_mid
            base_qty, quote_qty, base_cost = _apply_session_capital(
                base_qty, quote_qty, initial_mid, st.session_capital_usd
            )

        ctx.grid_state = self.grid_engine.build_initial_grid(
            product_id=self.product_id,
            session_id=ctx.session_id,
            mid=initial_mid,
            base_inventory=base_qty,
            quote_inventory=quote_qty,
            base_inventory_cost=base_cost,
            prior_realized_pnl=resumed["realized_pnl_quote"] if resumed else Decimal("0"),
            prior_total_fills=resumed["total_fills"] if resumed else 0,
        )
        _emit(
            f"[{self.product_id}] Grid built: "
            f"{len(ctx.grid_state.bid_levels)} bids + {len(ctx.grid_state.ask_levels)} asks "
            f"around mid={initial_mid} base={base_qty} quote={quote_qty}"
        )

        # Persist session + all initial levels + cancel stale orphans + initial equity snapshot
        await _persist(self._db_sync_state(ctx.grid_state, cancel_stale=True))
        await _persist(self._db_save_equity_snapshot(ctx.grid_state, initial_mid, "init"))

    async def _db_load_last_session(self, product_id: str) -> dict | None:
        """
        Load the last active session and reconstruct inventory from saved fills.
        Returns None if no previous session exists (fresh start).
        """
        try:
            st = self.grid_engine.settings
            async with AsyncSessionFactory() as db:
                repo = GridRepository(db)
                state = await repo.load_last_session_state(product_id)
                if state is None:
                    return None

                # Reconstruct inventory by replaying all fills from the saved session
                half_usd = st.session_capital_usd / Decimal("2")

                # Use fills to reconstruct actual inventory
                # Start from initial split, replay each fill
                fill_rows = state["fill_rows"]
                if not fill_rows:
                    # No fills yet â€” close this empty session and start fresh
                    async with AsyncSessionFactory() as db:
                        repo = GridRepository(db)
                        await repo.close_session(state["session_id"])
                    return None

                # Replay fills to reconstruct terminal inventory
                base_acc = half_usd / Decimal(str(fill_rows[0].price)) if fill_rows else Decimal("0")
                quote_acc = half_usd
                cost_acc = half_usd

                for fr in sorted(fill_rows, key=lambda r: r.trade_time):
                    price = Decimal(str(fr.price))
                    size = Decimal(str(fr.size_base))
                    qv = Decimal(str(fr.quote_value))
                    fee = Decimal(str(fr.fee_quote))
                    if fr.side == "BUY":
                        base_acc += size
                        quote_acc -= qv + fee
                        cost_acc += qv + fee
                    else:
                        fraction = min(size / base_acc, Decimal("1")) if base_acc > 0 else Decimal("1")
                        cost_acc = cost_acc * (Decimal("1") - fraction)
                        base_acc = max(Decimal("0"), base_acc - size)
                        quote_acc += qv - fee

                last_price = Decimal(str(fill_rows[-1].price))
                btc_value = base_acc * last_price
                total_value = btc_value + quote_acc
                if total_value > 0:
                    long_ratio = btc_value / total_value
                    if long_ratio > Decimal("0.70") or quote_acc < st.level_size_quote:
                        _emit(
                            f"[{product_id}] Resumed inventory too imbalanced "
                            f"(long_ratio={long_ratio:.1%}, quote=${quote_acc:.2f}) â€” closing old session and starting fresh"
                        )
                        # Close the imbalanced session so it doesn't become an orphan
                        async with AsyncSessionFactory() as db:
                            repo = GridRepository(db)
                            await repo.close_session(state["session_id"])
                        return None

                return {
                    "session_id": state["session_id"],
                    "base_inventory": base_acc.quantize(Decimal("0.00000001")),
                    "quote_inventory": quote_acc.quantize(Decimal("0.00000001")),
                    "base_inventory_cost": max(Decimal("0"), cost_acc).quantize(Decimal("0.00000001")),
                    "realized_pnl_quote": state["realized_pnl_quote"],
                    "total_fills": state["total_fills"],
                    "reserve_usd": state.get("reserve_usd", Decimal("0")),
                }
        except Exception as exc:
            _emit(f"[{product_id}] Could not load last session ({exc}) â€” starting fresh")
            try:
                async with AsyncSessionFactory() as db:
                    repo = GridRepository(db)
                    state = await repo.load_last_session_state(product_id)
                    if state:
                        await repo.close_session(state["session_id"])
            except Exception:
                pass
            return None

    async def _db_save_session(self, grid_state: GridState, mode: str) -> None:
        ctx = self._ctx()
        st = self.grid_engine.settings
        # Build strategy snapshot once â€” upsert_session only writes it on INSERT
        strategy_snapshot = {
            "level_size_quote": float(st.level_size_quote),
            "rebalance_threshold_bps": float(st.rebalance_threshold_bps),
            "max_inventory_ratio": float(st.max_inventory_ratio),
            "maker_fee_rate": float(self.maker_fee_rate),
            "symbol_overrides": None,  # per-symbol overrides already merged into st
        }
        async with AsyncSessionFactory() as db:
            repo = GridRepository(db)
            await repo.upsert_session(
                grid_state, mode,
                reserve_usd=ctx.reserve_usd,
                underfunded=ctx.underfunded,
                underfunded_shortfall_usd=ctx.underfunded_shortfall_usd,
                strategy_snapshot=strategy_snapshot,
            )
            await db.commit()

    async def _db_upsert_level(self, level: GridLevel) -> None:
        async with AsyncSessionFactory() as db:
            repo = GridRepository(db)
            await repo.upsert_level(level)
            await db.commit()

    async def _db_save_fill(self, fill: Fill) -> None:
        ctx = self._ctx()
        async with AsyncSessionFactory() as db:
            repo = GridRepository(db)
            await repo.save_fill(fill, ctx.session_id)
            await db.commit()

    async def _db_save_tick(self, snapshot: MarketSnapshot) -> None:
        async with AsyncSessionFactory() as db:
            repo = GridRepository(db)
            await repo.save_tick(snapshot)
            await db.commit()

    async def _db_save_equity_snapshot(
        self, grid_state: GridState, mid_price: Decimal, trigger: str
    ) -> None:
        async with AsyncSessionFactory() as db:
            repo = GridRepository(db)
            await repo.save_equity_snapshot(grid_state, mid_price, trigger)
            await db.commit()

    async def _db_save_restart(self, session_id, triggered_by: str) -> None:
        async with AsyncSessionFactory() as db:
            repo = GridRepository(db)
            await repo.save_restart(session_id, self.product_id, triggered_by)
            await db.commit()

    async def _db_close_session(self, session_id) -> None:
        """Mark old session as closed in DB."""
        async with AsyncSessionFactory() as db:
            repo = GridRepository(db)
            await repo.close_session(session_id)
            await db.commit()

    async def _db_sync_state(self, grid_state: GridState, cancel_stale: bool = False) -> None:
        """Update session PnL + upsert all current levels in one transaction.

        cancel_stale=True: on startup, cancel any open/pending levels in the DB
        that are not part of the current in-memory grid (orphans from prior runs).
        """
        mode = self.runtime.mode
        ctx = self._ctx()
        async with AsyncSessionFactory() as db:
            repo = GridRepository(db)
            await repo.upsert_session(
                grid_state,
                mode,
                reserve_usd=ctx.reserve_usd,
                underfunded=ctx.underfunded,
                underfunded_shortfall_usd=ctx.underfunded_shortfall_usd,
            )
            for lvl in grid_state.bid_levels + grid_state.ask_levels:
                await repo.upsert_level(lvl)
            if cancel_stale:
                active_ids = [lvl.level_id for lvl in grid_state.bid_levels + grid_state.ask_levels]
                cancelled = await repo.cancel_stale_levels(grid_state.session_id, active_ids)
                if cancelled:
                    _emit(f"[{grid_state.product_id}] Cancelled {cancelled} stale grid levels from prior runs")
                # Cancel open/pending levels from ALL previous sessions for this product.
                # Paper adapter loses its in-memory orders on restart, so any 'open' levels
                # from a different session_id are orphaned and will never fill.
                orphaned = await repo.cancel_orphan_levels(grid_state.product_id, grid_state.session_id)
                if orphaned:
                    _emit(f"[{grid_state.product_id}] Cancelled {orphaned} orphan levels from previous sessions")
            await db.commit()

    async def _reset(self, market: MarketSnapshot) -> None:
        """Wipe current session and restart.

        Paper mode: resets to session_capital_usd split evenly.
        Live mode: cancels all open orders then reads real exchange balances.
        """
        ctx = self._ctx()
        triggered_by = ctx.reset_triggered_by
        reset_type = ctx.reset_type
        ctx.reset_requested = False
        ctx.reset_triggered_by = "Abraham"  # restore default
        ctx.reset_type = "daily_close"
        st = self.grid_engine.settings
        mid = market.mid

        ctx.recent_fills.clear()
        ctx.stress_paused_until = None
        ctx.underfunded = False
        ctx.underfunded_shortfall_usd = Decimal("0")
        old_session_id = ctx.session_id
        ctx.session_id = uuid4()
        # Close old session in DB
        if old_session_id:
            await _persist(self._db_close_session(old_session_id))

        if self.paper_mode and ctx.paper_adapter:
            ctx.paper_adapter.open_orders.clear()

        if reset_type == "hard":
            # Hard reset: back to configured defaults (paper only)
            new_session_capital = st.session_capital_usd
            ctx.reserve_usd = st.total_wallet_usd - new_session_capital
            _emit(
                f"[{self.product_id}] Hard reset: "
                f"session=${new_session_capital:.2f} reserve=${ctx.reserve_usd:.2f}"
            )
            half = new_session_capital / Decimal("2")
            base_qty = (half / mid).quantize(Decimal("0.00000001"))
            quote_qty = half
            base_cost = base_qty * mid
        else:
            # Daily close: sell base (if any), reconcile reserve, redeploy session_capital
            if not self.paper_mode:
                assert self._exchange_adapter is not None
                # Cancel all open orders first, then read real balances
                await self._cancel_all(ctx)
                base_currency = self.product_id.split("-")[0]
                quote_currency = self.product_id.split("-")[1]
                try:
                    balances = await self._exchange_adapter.get_balances()
                    live_base = balances.get(base_currency, Decimal("0"))
                    live_quote = balances.get(quote_currency, Decimal("0"))
                except Exception as exc:
                    _emit(f"[{self.product_id}] RESET: could not fetch balances ({exc}) â€” aborting reset")
                    return
                # Sync grid state to real balances so equity calc is accurate
                if ctx.grid_state:
                    ctx.grid_state.base_inventory = live_base
                    ctx.grid_state.quote_inventory = live_quote

            current_equity = Decimal("0")
            if ctx.grid_state:
                current_equity = ctx.grid_state.quote_inventory + ctx.grid_state.base_inventory * mid

            session_capital = st.session_capital_usd
            deploy_capital = session_capital

            if current_equity >= session_capital:
                # Case 1: surplus â€” sell all base inventory, move surplus to reserve
                if self.paper_mode:
                    # Arithmetic: pretend we sold all base at mid
                    realized_quote = current_equity  # quote + base*mid = full equity in USD
                    surplus = realized_quote - session_capital
                    ctx.reserve_usd += surplus
                    _emit(
                        f"[{self.product_id}] Daily close (paper): equity=${current_equity:.2f} "
                        f"surplus=${surplus:.2f} â†’ reserve=${ctx.reserve_usd:.2f}"
                    )
                else:
                    # Live: execute real market sell for current base holding
                    base_to_sell = ctx.grid_state.base_inventory if ctx.grid_state else Decimal("0")
                    if base_to_sell > Decimal("0"):
                        try:
                            quote_received = await self._exchange_adapter.market_sell(
                                self.product_id, base_to_sell
                            )
                        except Exception as exc:
                            _emit(f"[{self.product_id}] Daily close: market_sell failed ({exc}) â€” aborting reset")
                            return
                    else:
                        quote_received = ctx.grid_state.quote_inventory if ctx.grid_state else Decimal("0")
                    surplus = quote_received - session_capital
                    ctx.reserve_usd += max(Decimal("0"), surplus)
                    _emit(
                        f"[{self.product_id}] Daily close (live): sold {base_to_sell} base â†’ "
                        f"${quote_received:.2f} surplus=${surplus:.2f} reserve=${ctx.reserve_usd:.2f}"
                    )
            else:
                # Case 2: deficit â€” inject from reserve, no sell
                deficit = session_capital - current_equity
                available = ctx.reserve_usd
                injection = min(deficit, available)
                ctx.reserve_usd = max(Decimal("0"), ctx.reserve_usd - injection)
                shortfall = max(Decimal("0"), deficit - injection)
                if shortfall > 0:
                    ctx.underfunded = True
                    ctx.underfunded_shortfall_usd = shortfall
                    deploy_capital = max(Decimal("0"), session_capital - shortfall)
                    _emit(
                        f"[{self.product_id}] ALERT underfunded: shortfall=${shortfall:.2f} "
                        "pausing new entries for this session"
                    )
                _emit(
                    f"[{self.product_id}] Daily close: equity=${current_equity:.2f} "
                    f"deficit=${deficit:.2f} injected=${injection:.2f} reserve=${ctx.reserve_usd:.2f}"
                )

            # Redeploy session_capital 50/50 (paper: arithmetic split; live: re-read balances)
            if self.paper_mode:
                if deploy_capital <= 0:
                    base_qty = Decimal("0")
                    quote_qty = Decimal("0")
                    base_cost = Decimal("0")
                else:
                    half = deploy_capital / Decimal("2")
                    base_qty = (half / mid).quantize(Decimal("0.00000001"))
                    quote_qty = half
                    base_cost = base_qty * mid
            else:
                # Re-read balances after market sell to get actual holdings
                try:
                    balances = await self._exchange_adapter.get_balances()
                    base_qty = balances.get(base_currency, Decimal("0"))
                    quote_qty = balances.get(quote_currency, Decimal("0"))
                except Exception as exc:
                    _emit(f"[{self.product_id}] RESET: post-sell balance fetch failed ({exc}) â€” aborting")
                    return
                base_qty, quote_qty, base_cost = _apply_session_capital(
                    base_qty, quote_qty, mid, deploy_capital
                )

        ctx.grid_state = self.grid_engine.build_initial_grid(
            product_id=self.product_id,
            session_id=ctx.session_id,
            mid=mid,
            base_inventory=base_qty,
            quote_inventory=quote_qty,
            base_inventory_cost=base_cost,
            prior_realized_pnl=Decimal("0"),
            prior_total_fills=0,
        )
        _emit(
            f"[{self.product_id}] RESET by {triggered_by} â€” new session {ctx.session_id} "
            f"base={base_qty} quote={quote_qty} mid={mid}"
        )
        await _persist(self._db_sync_state(ctx.grid_state))
        await _persist(self._db_save_restart(ctx.session_id, triggered_by))
        await _persist(self._db_save_equity_snapshot(ctx.grid_state, mid, "reset"))

    async def _publish_state(self) -> None:
        """Build the state dict and publish it to Redis."""
        runtime = self.runtime
        now = datetime.now(UTC)

        total_realized = Decimal("0")
        total_unrealized = Decimal("0")
        total_fills = 0
        symbol_dicts: list[dict] = []

        for product_id, ctx in runtime.symbol_contexts.items():
            grid = ctx.grid_state
            regime = ctx.regime_state
            risk = ctx.risk_state
            market = ctx.market_snapshot

            if grid is None or market is None:
                continue

            base_inventory_usd = grid.base_inventory * market.mid
            total_realized += grid.realized_pnl_quote
            total_unrealized += grid.unrealized_pnl_quote
            total_fills += grid.total_fills

            bid_levels = [
                {
                    "level_index": lvl.level_index,
                    "side": "bid",
                    "price": str(lvl.price),
                    "size_base": str(lvl.size_base),
                    "size_quote": str(lvl.size_quote),
                    "status": lvl.status,
                    "fill_price": str(lvl.fill_price) if lvl.fill_price is not None else None,
                    "opened_at": lvl.opened_at.isoformat() if lvl.opened_at else None,
                    "age_seconds": (now - lvl.opened_at).total_seconds() if lvl.opened_at else None,
                }
                for lvl in sorted(grid.bid_levels, key=lambda x: x.level_index)
            ]
            ask_levels = [
                {
                    "level_index": lvl.level_index,
                    "side": "ask",
                    "price": str(lvl.price),
                    "size_base": str(lvl.size_base),
                    "size_quote": str(lvl.size_quote),
                    "status": lvl.status,
                    "fill_price": str(lvl.fill_price) if lvl.fill_price is not None else None,
                    "opened_at": lvl.opened_at.isoformat() if lvl.opened_at else None,
                    "age_seconds": (now - lvl.opened_at).total_seconds() if lvl.opened_at else None,
                }
                for lvl in sorted(grid.ask_levels, key=lambda x: x.level_index)
            ]

            symbol_dicts.append(
                {
                    "product_id": product_id,
                    "session_id": str(ctx.session_id),
                    "regime": regime.regime.value if regime else "UNKNOWN",
                    "regime_confidence": str(regime.confidence) if regime else "0",
                    "regime_reasons": regime.reason_codes if regime else [],
                    "risk_mode": risk.risk_mode.value if risk else "UNKNOWN",
                    "risk_reasons": risk.reason_codes if risk else [],
                    "mid_price": str(market.mid),
                    "price_time": market.event_time.isoformat(),
                    "spread_bps": str(market.spread_bps),
                    "rsi": str(market.rsi),
                    "mid_anchor": str(grid.mid_anchor),
                    "base_inventory": str(grid.base_inventory),
                    "quote_inventory": str(grid.quote_inventory),
                    "base_inventory_usd": str(base_inventory_usd),
                    "total_equity": str(grid.total_equity),
                    "unrealized_pnl": str(grid.unrealized_pnl_quote),
                    "realized_pnl": str(grid.realized_pnl_quote),
                    "total_fills": grid.total_fills,
                    "rebalance_count": grid.rebalance_count,
                    "open_bid_count": len(grid.open_bid_levels),
                    "open_ask_count": len(grid.open_ask_levels),
                    "reserve_usd": str(ctx.reserve_usd),
                    "underfunded": ctx.underfunded,
                    "underfunded_shortfall_usd": str(ctx.underfunded_shortfall_usd),
                    "bid_levels": bid_levels,
                    "ask_levels": ask_levels,
                    "updated_at": grid.updated_at.isoformat(),
                }
            )

        state_dict = {
            "mode": runtime.mode,
            "started_at": runtime.started_at.isoformat(),
            "uptime_seconds": (now - runtime.worker_started_at).total_seconds(),
            "session_uptime_seconds": (now - runtime.started_at).total_seconds(),
            "next_daily_close_at": runtime.next_daily_close_at.isoformat() if runtime.next_daily_close_at else None,
            "daily_close_schedule": _current_schedule_payload(runtime),
            "pending_daily_close_schedule": runtime.pending_schedule_after_close,
            "skip_daily_close": runtime.skip_daily_close,
            "total_realized_pnl": str(total_realized),
            "total_unrealized_pnl": str(total_unrealized),
            "total_fills": total_fills,
            "total_symbols": len(symbol_dicts),
            "symbols": symbol_dicts,
            "updated_at": now.isoformat(),
        }

        await self.state_store.publish_state(state_dict)
        await self.state_store.publish_heartbeat()

    async def on_tick(self, market: MarketSnapshot) -> None:
        ctx = self._ctx()
        ctx.market_snapshot = market
        now = datetime.now(UTC)

        # Save every tick to DB (fire-and-forget, non-blocking)
        asyncio.create_task(_persist(self._db_save_tick(market)))

        # Handle pending reset before anything else
        if ctx.reset_requested:
            await self._reset(market)
            # Publish updated state after reset
            asyncio.create_task(_persist(self._publish_state()))
            return

        if ctx.grid_state is None:
            return

        # Stress pause check
        if ctx.stress_paused_until and now < ctx.stress_paused_until:
            return

        # Paper: simulate fills on current open orders
        new_fills: list[Fill] = []
        if self.paper_mode and ctx.paper_adapter:
            new_fills = await ctx.paper_adapter.on_market_snapshot(market)

        # Process each fill
        for fill in new_fills:
            ctx.recent_fills.append(fill)
            ctx.grid_state, replenish_action = self.grid_engine.apply_fill(
                state=ctx.grid_state,
                fill=fill,
                market=market,
            )
            flip_side = "â†’ASK" if fill.side.value == "BUY" else "â†’BID"
            flip_info = (
                f" flip{flip_side}@{replenish_action.level.price:.6f}"
                if replenish_action else " (counter exists)"
            )
            _emit(
                f"[{self.product_id}] FILL {fill.side.value} px={fill.price:.6f} "
                f"sz={fill.size_base:.4f} lvl={fill.level_index}{flip_info} "
                f"r_pnl={ctx.grid_state.realized_pnl_quote:.4f}"
            )
            # Persist fill + updated session PnL + equity snapshot
            # Fill save is critical â€” retry once on failure to avoid data loss
            try:
                await self._db_save_fill(fill)
            except Exception as exc:
                _emit(f"[{self.product_id}] CRITICAL: fill DB save failed, retrying: {exc}")
                try:
                    await self._db_save_fill(fill)
                except Exception as exc2:
                    _emit(f"[{self.product_id}] CRITICAL: fill DB save retry failed: {exc2}")
            await _persist(self._db_save_session(ctx.grid_state, self.runtime.mode))
            await _persist(self._db_save_equity_snapshot(ctx.grid_state, market.mid, "fill"))
            if replenish_action and replenish_action.action_type == "place":
                await self._place_level(replenish_action.level, ctx, now)
        kernel_result = self.kernel.evaluate_tick(
            product_id=self.product_id,
            grid_state=ctx.grid_state,
            market=market,
            previous_regime=ctx.regime_state,
            previous_risk=ctx.risk_state,
            stress_pause_seconds=settings.risk.stress_pause_seconds,
            now=now,
        )
        ctx.regime_state = kernel_result.regime_state

        if kernel_result.stress_paused_until is not None:
            ctx.stress_paused_until = kernel_result.stress_paused_until
            _emit(
                f"[{self.product_id}] STRESS - pausing {settings.risk.stress_pause_seconds}s"
            )
            asyncio.create_task(_persist(self._publish_state()))
            return

        if kernel_result.risk_state is not None:
            ctx.risk_state = kernel_result.risk_state
        risk_decision = kernel_result.risk_decision
        if risk_decision is None:
            asyncio.create_task(_persist(self._publish_state()))
            return

        if risk_decision.should_cancel_all:
            await self._cancel_all(ctx)
            _emit(f"[{self.product_id}] RISK shutdown - cancelled all. reasons={risk_decision.reasons}")
            asyncio.create_task(_persist(self._publish_state()))
            return

        # Grid evaluation (rebalance + stale orders)
        decision = kernel_result.grid_decision
        if decision.updated_state and decision.rebalanced:
            # Full grid rebalance
            _emit(
                f"[{self.product_id}] REBALANCE #{decision.updated_state.rebalance_count} "
                f"anchor {ctx.grid_state.mid_anchor:.6f} -> {market.mid:.6f}"
            )
            for action in decision.actions:
                if action.action_type == "cancel" and action.level.client_order_id:
                    await self._cancel_order(action.level.client_order_id, ctx)
            ctx.grid_state = decision.updated_state
            # Persist new grid state + cancel DB rows for levels no longer active
            await _persist(self._db_sync_state(ctx.grid_state, cancel_stale=True))
            await _persist(self._db_save_equity_snapshot(ctx.grid_state, market.mid, "rebalance"))
        elif decision.updated_state and not decision.rebalanced:
            # Bid replenishment: update state only, no cancel/DB sync overhead
            ctx.grid_state = decision.updated_state
        elif decision.actions:
            for action in decision.actions:
                if action.action_type == "cancel" and action.level.client_order_id:
                    coid = action.level.client_order_id
                    await self._cancel_order(coid, ctx)
                    ctx.grid_state = self.grid_engine.apply_order_cancelled(
                        state=ctx.grid_state,
                        client_order_id=coid,
                    )

        # Place all pending levels (within risk limits)
        if ctx.underfunded:
            asyncio.create_task(_persist(self._publish_state()))
            return
        for lvl in self.grid_engine.get_pending_levels(ctx.grid_state):
            if lvl.side == OrderSide.BUY and not risk_decision.allow_new_bids:
                continue
            if lvl.side == OrderSide.SELL and not risk_decision.allow_new_asks:
                continue
            await self._place_level(lvl, ctx, now)

        # Publish state to Redis (fire-and-forget)
        asyncio.create_task(_persist(self._publish_state()))

    async def _place_level(self, level: GridLevel, ctx: SymbolContext, now: datetime) -> None:
        regime_str = ctx.regime_state.regime.value if ctx.regime_state else "UNKNOWN"
        intent = self.grid_engine.build_intent_for_level(level, regime=regime_str, now=now)

        if self.paper_mode and ctx.paper_adapter:
            current_mid = ctx.market_snapshot.mid if ctx.market_snapshot else None
            order = await ctx.paper_adapter.place_order(intent, current_mid=current_mid)
            client_order_id = order.client_order_id
            order_id = order.order_id
        else:
            assert self._exchange_adapter is not None
            try:
                payload = _build_order_payload(intent)
                response = await self._exchange_adapter.create_order(payload)
                if not response.get("success", False):
                    _emit(f"[{self.product_id}] Order rejected: {response.get('error_response', response)}")
                    return
                order_data = response.get("success_response", {})
                client_order_id = order_data.get("client_order_id", intent.intent_id)
                order_id = order_data.get("order_id")
                if not order_id:
                    _emit(f"[{self.product_id}] Order accepted but no order_id in response â€” skipping state update")
                    return
            except Exception as exc:
                _emit(f"[{self.product_id}] Place order error: {exc}")
                return

        ctx.grid_state = self.grid_engine.apply_order_placed(
            state=ctx.grid_state,
            level=level,
            client_order_id=client_order_id,
            order_id=order_id,
            now=now,
        )
        # Persist the level now that it has a client_order_id and status="open"
        placed_level = next(
            (l for l in ctx.grid_state.bid_levels + ctx.grid_state.ask_levels
             if l.client_order_id == client_order_id),
            None,
        )
        if placed_level:
            await _persist(self._db_upsert_level(placed_level))

    async def _cancel_order(self, client_order_id: str, ctx: SymbolContext) -> None:
        if self.paper_mode and ctx.paper_adapter:
            await ctx.paper_adapter.cancel_order(client_order_id)
        elif self._exchange_adapter:
            try:
                if isinstance(self._exchange_adapter, BybitAdapter):
                    await self._exchange_adapter.cancel_order_by_id(client_order_id, self.product_id)
                else:
                    await self._exchange_adapter.cancel_orders([client_order_id])
            except Exception as exc:
                _emit(f"[{self.product_id}] Cancel error: {exc}")

    async def _cancel_all(self, ctx: SymbolContext) -> None:
        if ctx.grid_state is None:
            return
        now = datetime.now(UTC)
        for lvl in ctx.grid_state.open_bid_levels + ctx.grid_state.open_ask_levels:
            if lvl.client_order_id:
                await self._cancel_order(lvl.client_order_id, ctx)
                ctx.grid_state = self.grid_engine.apply_order_cancelled(
                    state=ctx.grid_state,
                    client_order_id=lvl.client_order_id,
                    now=now,
                )


# ---------------------------------------------------------------------------
# Main orchestrator
# ---------------------------------------------------------------------------

async def _heartbeat_loop(state_store: StateStore) -> None:
    """Publish Redis heartbeat every 5 seconds, independent of ticker/WebSocket activity."""
    while True:
        try:
            await state_store.publish_heartbeat()
        except Exception as exc:
            _emit(f"Heartbeat publish failed: {exc}")
        await asyncio.sleep(5)


async def _daily_close_loop(runtime: RuntimeContext, state_store: StateStore) -> None:
    """
    Reset all symbol sessions at local midnight every day.

    Schedule is sticky for the current cycle:
    if timezone/close-time changes mid-cycle, they are applied only after the next close
    unless an explicit immediate resync command is received.
    """
    while not runtime.config_loaded:
        await asyncio.sleep(1)

    while True:
        await _process_runtime_commands(runtime, state_store)

        if runtime.next_daily_close_at is None or runtime.schedule_resync_requested:
            runtime.schedule_resync_requested = False
            now_utc = datetime.now(UTC)
            next_close_utc, next_close_local = _compute_next_daily_close_utc(
                now_utc=now_utc,
                timezone_iana=runtime.local_timezone_iana,
                close_hour=runtime.daily_close_hour,
                close_minute=runtime.daily_close_minute,
            )
            runtime.next_daily_close_at = next_close_utc
            _emit(
                "Daily close scheduled for "
                f"{next_close_local.isoformat()} ({runtime.local_timezone_iana}) "
                f"/ {next_close_utc.isoformat()} UTC"
            )

        remaining = (runtime.next_daily_close_at - datetime.now(UTC)).total_seconds()
        if remaining > 0:
            await asyncio.sleep(min(30, max(1, remaining)))
            continue

        if runtime.skip_daily_close:
            _emit("Daily close SKIPPED by user request")
            runtime.skip_daily_close = False
            await state_store.set_skip_daily_close(False)
            runtime.next_daily_close_at = None
            continue

        _emit("Daily close â€” resetting all symbol sessions")
        for ctx in runtime.symbol_contexts.values():
            ctx.reset_triggered_by = "daily_close_auto"
            ctx.reset_type = "daily_close"
            ctx.reset_requested = True

        if runtime.pending_schedule_after_close:
            pending = runtime.pending_schedule_after_close
            _apply_runtime_schedule(
                runtime,
                timezone_iana=str(pending["local_timezone_iana"]),
                close_hour=int(pending["daily_close_hour"]),
                close_minute=int(pending["daily_close_minute"]),
            )
            runtime.pending_schedule_after_close = None
            _emit(
                "Applied pending daily close schedule after cycle close: "
                f"{runtime.local_timezone_iana} "
                f"{runtime.daily_close_hour:02d}:{runtime.daily_close_minute:02d}"
            )

        runtime.next_daily_close_at = None


async def _process_runtime_commands(runtime: RuntimeContext, state_store: StateStore) -> None:
    """Drain command queue once per worker process loop (not per symbol)."""
    try:
        commands = await state_store.pop_commands()
    except Exception as exc:
        _emit(f"Command drain error: {exc}")
        return

    for cmd in commands:
        cmd_type = cmd.get("type")
        if cmd_type == "reset":
            triggered_by = cmd.get("triggered_by", "Abraham")
            reset_type = cmd.get("reset_type", "daily_close")
            _emit(f"Redis command: reset (triggered_by={triggered_by}, type={reset_type})")
            for ctx in runtime.symbol_contexts.values():
                ctx.reset_triggered_by = triggered_by
                ctx.reset_type = reset_type
                ctx.reset_requested = True
        elif cmd_type == "skip_daily_close":
            _emit("Redis command: skip_daily_close")
            runtime.skip_daily_close = True
            asyncio.create_task(_persist(state_store.set_skip_daily_close(True)))
        elif cmd_type == "update_daily_close_schedule":
            tz_name = str(cmd.get("local_timezone_iana", runtime.local_timezone_iana))
            close_hour = int(cmd.get("daily_close_hour", runtime.daily_close_hour))
            close_minute = int(cmd.get("daily_close_minute", runtime.daily_close_minute))
            mode = str(cmd.get("mode", "next_cycle")).strip().lower()
            try:
                ZoneInfo(tz_name)
            except ZoneInfoNotFoundError:
                _emit(f"Ignoring schedule update with invalid timezone: {tz_name}")
                continue
            if close_hour < 0 or close_hour > 23 or close_minute < 0 or close_minute > 59:
                _emit(
                    "Ignoring schedule update with invalid local close time: "
                    f"{close_hour:02d}:{close_minute:02d}"
                )
                continue

            if mode == "immediate":
                _apply_runtime_schedule(
                    runtime,
                    timezone_iana=tz_name,
                    close_hour=close_hour,
                    close_minute=close_minute,
                )
                runtime.pending_schedule_after_close = None
                runtime.schedule_resync_requested = True
                runtime.next_daily_close_at = None
                _emit(
                    "Redis command: update_daily_close_schedule applied immediately -> "
                    f"{tz_name} {close_hour:02d}:{close_minute:02d}"
                )
            else:
                runtime.pending_schedule_after_close = {
                    "local_timezone_iana": tz_name,
                    "daily_close_hour": close_hour,
                    "daily_close_minute": close_minute,
                }
                _emit(
                    "Redis command: update_daily_close_schedule queued for next cycle -> "
                    f"{tz_name} {close_hour:02d}:{close_minute:02d}"
                )
        else:
            _emit(f"Unknown Redis command type: {cmd_type}")


async def _run_worker(runtime: RuntimeContext, state_store: StateStore) -> None:
    exchange_name = settings.exchange.name.lower()  # "coinbase" | "bybit"
    _emit(f"Exchange: {exchange_name}")

    # V4 runtime source of truth: tenant_pair_strategies (pair-scoped only).
    # Global legacy strategy fallback is intentionally disabled.
    st = settings.strategy
    tenant_id = settings.app.default_tenant_id
    db_strat = None
    async with AsyncSessionFactory() as db:
        pair_repo = TenantPairStrategyRepository(db)
        pair_rows = await pair_repo.list_active_for_exchange(tenant_id, exchange_name)
        if pair_rows:
            db_strat = _build_pair_strategy_synthetic_row(
                exchange_name=exchange_name,
                tenant_id=tenant_id,
                pair_rows=pair_rows,
            )
            _emit(
                "Strategy loaded from tenant_pair_strategies: "
                f"tenant={tenant_id} exchange={exchange_name} pairs={len(pair_rows)}"
            )
    if db_strat is None:
        raise RuntimeError(
            f"No active pair strategy rows for exchange='{exchange_name}' tenant='{tenant_id}'. "
            "Worker fail-fast: V4 runtime only accepts tenant_pair_strategies."
        )

    required_runtime_fields = (
        "local_timezone_iana",
        "daily_close_hour",
        "daily_close_minute",
        "spread_freeze_bps",
        "regime_stress_spread_bps",
        "regime_trend_slope_threshold",
        "regime_mr_distance_threshold_bps",
        "regime_hysteresis_bps",
        "regime_rsi_bear_threshold",
        "regime_rsi_bull_threshold",
        "ws_retry_window_seconds",
        "ws_initial_retry_delay_seconds",
        "ws_max_retry_delay_seconds",
        "ws_message_timeout_seconds",
        "ws_heartbeat_timeout_seconds",
    )
    missing_fields = [f for f in required_runtime_fields if getattr(db_strat, f, None) is None]
    if missing_fields:
        raise RuntimeError(
            "Worker fail-fast: missing required DB strategy params "
            f"for '{db_strat.name}': {missing_fields}"
        )

    try:
        ZoneInfo(db_strat.local_timezone_iana)
    except ZoneInfoNotFoundError as exc:
        raise RuntimeError(
            f"Invalid local_timezone_iana in DB strategy '{db_strat.name}': "
            f"{db_strat.local_timezone_iana}"
        ) from exc
    if db_strat is not None:
        _emit(f"Strategy loaded from DB: '{db_strat.name}' (exchange={db_strat.exchange_name})")
        st = st.model_copy(update={
            "symbols": db_strat.symbols,
            "spacing_bps": Decimal(str(db_strat.spacing_bps)),
            "rebalance_threshold_bps": Decimal(str(db_strat.rebalance_threshold_bps)),
            "grid_levels": db_strat.grid_levels,
            "level_size_quote": Decimal(str(db_strat.level_size_quote)),
            "max_inventory_ratio": Decimal(str(db_strat.max_inventory_ratio)),
            "stale_reprice_threshold_bps": Decimal(str(db_strat.stale_reprice_threshold_bps)),
            "stale_order_age_seconds": db_strat.stale_order_age_seconds,
            "rebalance_defer_seconds": db_strat.rebalance_defer_seconds,
            "rebalance_defer_max_drift_bps": Decimal(str(db_strat.rebalance_defer_max_drift_bps)),
            "paper_mode": db_strat.paper_mode,
            "total_wallet_usd": Decimal(str(db_strat.total_wallet_usd)),
            "session_capital_usd": Decimal(str(db_strat.session_capital_usd)),
            "maker_only": db_strat.maker_only,
            "local_timezone_iana": db_strat.local_timezone_iana,
            "daily_close_hour": db_strat.daily_close_hour,
            "daily_close_minute": db_strat.daily_close_minute,
            "spread_freeze_bps": Decimal(str(db_strat.spread_freeze_bps)),
            "regime_stress_spread_bps": Decimal(str(db_strat.regime_stress_spread_bps)),
            "regime_trend_slope_threshold": Decimal(str(db_strat.regime_trend_slope_threshold)),
            "regime_mr_distance_threshold_bps": Decimal(str(db_strat.regime_mr_distance_threshold_bps)),
            "regime_hysteresis_bps": Decimal(str(db_strat.regime_hysteresis_bps)),
            "regime_rsi_bear_threshold": Decimal(str(db_strat.regime_rsi_bear_threshold)),
            "regime_rsi_bull_threshold": Decimal(str(db_strat.regime_rsi_bull_threshold)),
            "ws_retry_window_seconds": db_strat.ws_retry_window_seconds,
            "ws_initial_retry_delay_seconds": db_strat.ws_initial_retry_delay_seconds,
            "ws_max_retry_delay_seconds": db_strat.ws_max_retry_delay_seconds,
            "ws_message_timeout_seconds": db_strat.ws_message_timeout_seconds,
            "ws_heartbeat_timeout_seconds": db_strat.ws_heartbeat_timeout_seconds,
        })
    runtime.local_timezone_iana = st.local_timezone_iana
    runtime.daily_close_hour = st.daily_close_hour
    runtime.daily_close_minute = st.daily_close_minute
    runtime.ws_retry_window_seconds = st.ws_retry_window_seconds
    runtime.ws_initial_retry_delay_seconds = st.ws_initial_retry_delay_seconds
    runtime.ws_max_retry_delay_seconds = st.ws_max_retry_delay_seconds
    runtime.ws_message_timeout_seconds = st.ws_message_timeout_seconds
    runtime.ws_heartbeat_timeout_seconds = st.ws_heartbeat_timeout_seconds
    runtime.mode = "paper" if st.paper_mode else "live"

    if runtime.daily_close_hour < 0 or runtime.daily_close_hour > 23:
        raise RuntimeError(f"Invalid daily_close_hour in strategy '{db_strat.name}'")
    if runtime.daily_close_minute < 0 or runtime.daily_close_minute > 59:
        raise RuntimeError(f"Invalid daily_close_minute in strategy '{db_strat.name}'")
    if runtime.ws_retry_window_seconds <= 0:
        raise RuntimeError(f"Invalid ws_retry_window_seconds in strategy '{db_strat.name}'")
    if runtime.ws_initial_retry_delay_seconds <= 0:
        raise RuntimeError(f"Invalid ws_initial_retry_delay_seconds in strategy '{db_strat.name}'")
    if runtime.ws_max_retry_delay_seconds <= 0:
        raise RuntimeError(f"Invalid ws_max_retry_delay_seconds in strategy '{db_strat.name}'")
    if runtime.ws_message_timeout_seconds <= 0:
        raise RuntimeError(f"Invalid ws_message_timeout_seconds in strategy '{db_strat.name}'")
    if runtime.ws_heartbeat_timeout_seconds <= 0:
        raise RuntimeError(f"Invalid ws_heartbeat_timeout_seconds in strategy '{db_strat.name}'")
    runtime.config_loaded = True

    symbol_overrides: dict[str, dict[str, Any]] = db_strat.symbol_overrides or {}
    symbols = st.symbol_list()
    paper_mode = st.paper_mode
    if not symbols:
        raise RuntimeError(
            f"Strategy '{db_strat.name}' has no symbols configured; worker cannot start."
        )

    missing_symbols = [symbol for symbol in symbols if symbol not in symbol_overrides]
    if missing_symbols:
        raise RuntimeError(
            "Worker fail-fast: every pair must have its own strategy in symbol_overrides. "
            f"Missing pairs: {missing_symbols}"
        )

    incomplete_symbols: dict[str, list[str]] = {}
    for symbol in symbols:
        sym_cfg = symbol_overrides.get(symbol) or {}
        missing_fields = [f for f in _SYMBOL_REQUIRED_OVERRIDE_FIELDS if f not in sym_cfg]
        if missing_fields:
            incomplete_symbols[symbol] = missing_fields
    if incomplete_symbols:
        raise RuntimeError(
            "Worker fail-fast: incomplete pair strategy definitions in symbol_overrides. "
            f"Details: {incomplete_symbols}"
        )

    override_keys = list(symbol_overrides.keys())
    _emit(
        f"Pair-scoped strategy loaded for symbols={symbols} "
        f"(override_keys={override_keys})"
    )

    event_bus = InMemoryEventBus()
    market_data_engine = MarketDataEngine(event_bus)
    # One NeutralGridEngine per symbol, each with its own resolved settings
    grid_engines: dict[str, NeutralGridEngine] = {
        symbol: NeutralGridEngine(_resolve_symbol_settings(st, symbol_overrides, symbol))
        for symbol in symbols
    }
    regime_engine = RegimeEngine(
        stress_spread_bps=st.regime_stress_spread_bps,
        trend_slope_threshold=st.regime_trend_slope_threshold,
        mr_distance_threshold_bps=st.regime_mr_distance_threshold_bps,
        hysteresis_bps=st.regime_hysteresis_bps,
        rsi_bear_threshold=st.regime_rsi_bear_threshold,
        rsi_bull_threshold=st.regime_rsi_bull_threshold,
    )
    risk_engine = RiskEngine(settings.risk, spread_freeze_bps=st.spread_freeze_bps)

    coinbase_adapter: CoinbaseAdapter | None = None
    bybit_adapter: BybitAdapter | None = None
    rest_client: CoinbaseRESTClient | None = None
    bybit_rest_client: BybitRESTClient | None = None

    # Load credentials from database (encrypted)
    db_creds = await _load_credentials_from_db(exchange_name)

    if exchange_name == "bybit":
        # Try DB credentials first, fallback to .env if not found
        api_key = db_creds.get("api_key") if db_creds else None
        api_secret = db_creds.get("api_secret") if db_creds else None

        if not api_key and settings.bybit.api_key:
            # Fallback: use .env credentials
            _emit("WARNING: Using Bybit credentials from .env (fallback). Store in DB for better security.")
            api_key = settings.bybit.api_key
            api_secret = settings.bybit.api_secret

        if api_key:
            # Temporarily override settings with DB credentials
            settings.bybit.api_key = api_key
            settings.bybit.api_secret = api_secret
            bybit_rest_client = BybitRESTClient(settings.bybit)
            bybit_adapter = BybitAdapter(bybit_rest_client)
            _emit(f"Bybit credentials loaded from database")
    else:
        # Coinbase
        api_key = db_creds.get("api_key") if db_creds else None
        api_secret = db_creds.get("api_secret") if db_creds else None

        if not api_key and settings.coinbase.api_key:
            # Fallback: use .env credentials
            _emit("WARNING: Using Coinbase credentials from .env (fallback). Store in DB for better security.")
            api_key = settings.coinbase.api_key
            api_secret = settings.coinbase.api_secret

        if api_key:
            # Temporarily override settings with DB credentials
            settings.coinbase.api_key = api_key
            settings.coinbase.api_secret = api_secret
            rest_client = CoinbaseRESTClient(settings.coinbase)
            coinbase_adapter = CoinbaseAdapter(rest_client)
            _emit(f"Coinbase credentials loaded from database")

    active_adapter = bybit_adapter or coinbase_adapter

    # Fetch maker fee rate â€” DB strategy wins, then exchange API, then .env fallback
    maker_fee_rate = Decimal(str(settings.strategy.fallback_fee_rate))  # fallback
    if db_strat is not None:
        maker_fee_rate = Decimal(str(db_strat.maker_fee_rate))
        _emit(f"Maker fee rate: {maker_fee_rate} ({float(maker_fee_rate)*100:.4f}%) [from DB strategy '{db_strat.name}']")
    elif exchange_name == "bybit":
        # Bybit fee rate requires Account permission which may not be granted.
        # Use the configured value (BYBIT_MAKER_FEE_RATE, default 0.001 = 0.10%).
        maker_fee_rate = settings.bybit.maker_fee_rate
        _emit(f"Bybit maker fee rate: {maker_fee_rate} ({float(maker_fee_rate)*100:.4f}%) [from config]")
    elif active_adapter:
        try:
            fee_data = await active_adapter.get_fee_tier()
            tier = fee_data.get("fee_tier", {})
            raw_maker = tier.get("maker_fee_rate", None)
            raw_taker = tier.get("taker_fee_rate", None)
            _emit(
                f"{exchange_name.capitalize()} fee tier: maker={raw_maker} taker={raw_taker} "
                f"tier_name={tier.get('pricing_tier', 'unknown')}"
            )
            if raw_maker is not None:
                maker_fee_rate = Decimal(str(raw_maker))
                _emit(f"Using maker fee rate: {maker_fee_rate} ({float(maker_fee_rate)*100:.4f}%)")
            else:
                _emit(f"maker_fee_rate not in response â€” using default {maker_fee_rate}")
        except Exception as exc:
            _emit(f"Could not fetch {exchange_name} fee tier (using default {maker_fee_rate}): {exc}")
    else:
        _emit(f"No {exchange_name} credentials â€” paper fee rate: {maker_fee_rate}")

    # Build symbol contexts
    for symbol in symbols:
        runtime.symbol_contexts[symbol] = SymbolContext(
            product_id=symbol,
            session_id=uuid4(),
        )

    # Workers
    workers: dict[str, GridWorker] = {
        symbol: GridWorker(
            product_id=symbol,
            runtime=runtime,
            grid_engine=grid_engines[symbol],
            regime_engine=regime_engine,
            risk_engine=risk_engine,
            coinbase_adapter=coinbase_adapter,
            bybit_adapter=bybit_adapter,
            paper_mode=paper_mode,
            state_store=state_store,
            maker_fee_rate=maker_fee_rate,
        )
        for symbol in symbols
    }

    # Connect WS with retry â€” up to 1 hour of attempts before giving up
    if exchange_name == "bybit":
        ws_client: CoinbaseWebSocketClient | BybitWebSocketClient = BybitWebSocketClient(settings.bybit)
    else:
        ws_client = CoinbaseWebSocketClient(settings.coinbase)

    _ws_retry_deadline = asyncio.get_event_loop().time() + runtime.ws_retry_window_seconds
    _ws_retry_delay = max(1, runtime.ws_initial_retry_delay_seconds)
    while True:
        try:
            await ws_client.connect()
            await ws_client.subscribe_market_data(symbols, ["ticker", "heartbeats"])
            break
        except Exception as exc:
            remaining = _ws_retry_deadline - asyncio.get_event_loop().time()
            if remaining <= 0:
                raise RuntimeError(
                    f"Could not connect to {exchange_name} WebSocket "
                    f"after {runtime.ws_retry_window_seconds}s: {exc}"
                ) from exc
            _emit(f"WS connect failed ({exc}) â€” retrying in {_ws_retry_delay}s (up to {remaining/60:.0f}min left)")
            await asyncio.sleep(_ws_retry_delay)
            _ws_retry_delay = min(
                _ws_retry_delay * 2,
                max(runtime.ws_initial_retry_delay_seconds, runtime.ws_max_retry_delay_seconds),
            )

    # Give WS a moment to deliver first tick, then fetch mid from REST
    for symbol in symbols:
        mid = Decimal("0.1")
        if active_adapter:
            try:
                bid, ask = await active_adapter.get_best_bid_ask(symbol)
                mid = (bid + ask) / Decimal("2")
            except Exception as exc:
                _emit(f"[{symbol}] Could not fetch initial mid from REST: {exc}")
        await workers[symbol].initialize(mid)

        # Pre-warm RSI with last 24 completed 5-min candles from REST API
        try:
            import time as _time
            now_ts = int(_time.time())
            if exchange_name == "bybit" and bybit_rest_client:
                bybit_sym = symbol.replace("-", "").replace("USD", "USDT")
                data = await bybit_rest_client.get(
                    "/v5/market/kline",
                    params={
                        "category": "spot",
                        "symbol": bybit_sym,
                        "interval": "5",
                        "limit": "26",
                    },
                )
                # Bybit kline: [timestamp, open, high, low, close, volume, turnover] newest first
                raw_candles = data.get("result", {}).get("list", [])
                closes = [Decimal(str(c[4])) for c in reversed(raw_candles[1:])]  # skip newest
                market_data_engine.seed_candle_closes(symbol, closes)
                _emit(f"[{symbol}] RSI pre-warmed: {len(closes)} x 5-min candles")
            elif rest_client:
                start_ts = now_ts - 25 * 300
                data = await rest_client.get(
                    f"/api/v3/brokerage/products/{symbol}/candles",
                    auth=True,
                    params={
                        "start": str(start_ts),
                        "end": str(now_ts),
                        "granularity": "FIVE_MINUTE",
                    },
                )
                candles = sorted(data.get("candles", []), key=lambda c: int(c["start"]))
                closes = [Decimal(str(c["close"])) for c in candles[:-1]]
                market_data_engine.seed_candle_closes(symbol, closes)
                _emit(f"[{symbol}] RSI pre-warmed: {len(closes)} x 5-min candles")
        except Exception as exc:
            _emit(f"[{symbol}] RSI pre-warm failed ({exc}) â€” will warm up live")

        _emit(f"[{symbol}] Ready â€” session_id={runtime.symbol_contexts[symbol].session_id}")

    # Main message loop â€” with per-message timeout to detect dead WebSocket connections.
    # Normal Bybit WS sends ping frames every ~20s even with no market activity.
    # If we receive nothing for 90s the connection is stale; exit so supervisor restarts.
    _WS_MSG_TIMEOUT = runtime.ws_message_timeout_seconds
    _emit(f"Listening to market data for {symbols}")
    _msg_iter = ws_client.iter_messages()
    while True:
        try:
            raw_message = await asyncio.wait_for(_msg_iter.__anext__(), timeout=_WS_MSG_TIMEOUT)
        except asyncio.TimeoutError:
            _emit(
                f"WebSocket silent for {_WS_MSG_TIMEOUT}s â€” connection stale, "
                "exiting so supervisor can restart"
            )
            break
        except StopAsyncIteration:
            _emit("WebSocket stream ended â€” exiting for supervisor restart")
            break
        await _process_runtime_commands(runtime, state_store)
        await market_data_engine.process_ws_message(raw_message)
        if symbols and all(
            market_data_engine.has_seen_heartbeat(sym)
            for sym in symbols
        ) and all(
            market_data_engine.is_heartbeat_stale(sym, runtime.ws_heartbeat_timeout_seconds)
            for sym in symbols
        ):
            _emit(
                f"Heartbeat stale for all symbols (> {runtime.ws_heartbeat_timeout_seconds}s) - "
                "exiting so supervisor can restart"
            )
            break
        channel = raw_message.get("channel", "")
        if channel != "ticker":
            continue
        for event_data in raw_message.get("events", []):
            for ticker in event_data.get("tickers", []):
                product_id = ticker.get("product_id", "")
                if product_id not in workers:
                    continue
                snapshot = market_data_engine.get_snapshot(product_id)
                if snapshot is not None:
                    await workers[product_id].on_tick(snapshot)

    for c in [rest_client, bybit_rest_client]:
        if c:
            await c.close()


async def main() -> None:
    exchange_name = settings.exchange.name.lower()

    state_store = StateStore(
        settings.redis.url,
        exchange=exchange_name,
        tenant_id=settings.app.default_tenant_id,
    )

    # Restore session started_at from Redis (for PnL context continuity across restarts)
    restored_started_at = await state_store.get_started_at()
    if restored_started_at:
        started_at = restored_started_at
        _emit(f"started_at restored from Redis: {started_at.isoformat()}")
    else:
        started_at = datetime.now(UTC)
        await state_store.set_started_at(started_at)

    # Worker process uptime â€” always written fresh, never restored
    worker_started_at = datetime.now(UTC)
    await state_store.set_worker_started_at(worker_started_at)

    runtime = RuntimeContext(
        mode="paper" if settings.strategy.paper_mode else "live",
        started_at=started_at,
        worker_started_at=worker_started_at,
    )

    # Log this process startup to DB
    import os as _os
    from infrastructure.persistence.orm.worker_process_log import WorkerProcessLogRow
    _process_log_id: int | None = None
    try:
        async with AsyncSessionFactory() as db:
            log_row = WorkerProcessLogRow(
                exchange=exchange_name,
                started_at=worker_started_at,
                pid=_os.getpid(),
            )
            db.add(log_row)
            await db.commit()
            await db.refresh(log_row)
            _process_log_id = log_row.id
            _emit(f"Worker process logged (id={_process_log_id} pid={_os.getpid()})")
    except Exception as exc:
        _emit(f"Worker process log insert failed (non-fatal): {exc}")

    # Restore skip_daily_close from Redis (survives worker restarts)
    runtime.skip_daily_close = await state_store.get_skip_daily_close()
    if runtime.skip_daily_close:
        _emit("skip_daily_close restored from Redis â€” daily close will be skipped")

    _emit(
        f"TradingBotV3 worker starting â€” mode={runtime.mode} exchange={exchange_name}"
    )

    # Stop event â€” set by SIGTERM (supervisor stop) or SIGINT
    stop_event = asyncio.Event()
    loop = asyncio.get_event_loop()
    try:
        # add_signal_handler is not available on Windows
        loop.add_signal_handler(signal.SIGTERM, stop_event.set)
        loop.add_signal_handler(signal.SIGINT, stop_event.set)
    except NotImplementedError:
        # Windows: use signal.signal instead (runs in main thread only)
        signal.signal(signal.SIGTERM, lambda *_: stop_event.set())
        signal.signal(signal.SIGINT, lambda *_: stop_event.set())

    tasks: list[asyncio.Task] = [
        asyncio.create_task(_run_worker(runtime, state_store), name="worker"),
        asyncio.create_task(_daily_close_loop(runtime, state_store), name="daily_close"),
        asyncio.create_task(_heartbeat_loop(state_store), name="heartbeat"),
    ]

    try:
        # Wait for either SIGTERM/SIGINT or worker exit
        stop_task = asyncio.create_task(stop_event.wait(), name="stop_wait")
        done, pending = await asyncio.wait(
            [stop_task, *tasks],
            return_when=asyncio.FIRST_COMPLETED,
        )
        for d in done:
            if d in tasks:
                exc = d.exception()
                if exc is not None:
                    _emit(f"Worker task failed: {exc}")
        # Cancel remaining tasks
        for t in pending:
            t.cancel()
        for task in tasks:
            if not task.done():
                task.cancel()
        _emit("Worker shutting down...")
    except asyncio.CancelledError:
        _emit("Worker cancelled...")
        for task in tasks:
            task.cancel()
    finally:
        _emit(f"Worker stopped (exchange={exchange_name})")
        # Mark shutdown in worker process log
        if _process_log_id is not None:
            try:
                async with AsyncSessionFactory() as db:
                    from infrastructure.persistence.orm.worker_process_log import WorkerProcessLogRow
                    from sqlalchemy import update as _sa_update
                    await db.execute(
                        _sa_update(WorkerProcessLogRow)
                        .where(WorkerProcessLogRow.id == _process_log_id)
                        .values(stopped_at=datetime.now(UTC), stop_reason="graceful")
                    )
                    await db.commit()
            except Exception:
                pass
        await state_store.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass

