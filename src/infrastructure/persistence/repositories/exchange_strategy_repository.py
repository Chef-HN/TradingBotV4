from __future__ import annotations

import copy
import logging
from datetime import UTC, datetime
from decimal import Decimal

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from infrastructure.persistence.orm.exchange_strategies import ExchangeStrategyRow
from infrastructure.persistence.orm.strategy_param_history import StrategyParamHistoryRow

logger = logging.getLogger(__name__)

# Fields tracked for SCD2 diffing (all tunable params on the base row)
_TRACKED_FIELDS = [
    "spacing_bps", "rebalance_threshold_bps", "grid_levels", "level_size_quote",
    "max_inventory_ratio", "maker_fee_rate", "stale_reprice_threshold_bps",
    "stale_order_age_seconds", "rebalance_defer_seconds", "rebalance_defer_max_drift_bps",
    "total_wallet_usd", "session_capital_usd", "maker_only", "paper_mode",
    "symbols",
    "local_timezone_iana", "daily_close_hour", "daily_close_minute",
    "spread_freeze_bps",
    "regime_stress_spread_bps", "regime_trend_slope_threshold",
    "regime_mr_distance_threshold_bps", "regime_hysteresis_bps",
    "regime_rsi_bear_threshold", "regime_rsi_bull_threshold",
    "ws_retry_window_seconds", "ws_initial_retry_delay_seconds",
    "ws_max_retry_delay_seconds", "ws_message_timeout_seconds",
    "ws_heartbeat_timeout_seconds",
    "symbol_overrides",
]

# Fields that can be overridden per symbol (must match _resolve_symbol_settings whitelist)
_SYMBOL_OVERRIDABLE = {
    "spacing_bps", "rebalance_threshold_bps", "grid_levels", "level_size_quote",
    "stale_reprice_threshold_bps", "stale_order_age_seconds",
    "rebalance_defer_seconds", "rebalance_defer_max_drift_bps",
    "max_inventory_ratio", "session_capital_usd", "maker_only",
}


def _snapshot(row: ExchangeStrategyRow) -> dict:
    """Capture all tracked fields from a live row."""
    return {f: getattr(row, f) for f in _TRACKED_FIELDS}


def _build_change_summary(old: dict, new: dict, product_id: str | None = None) -> str:
    """Human-readable diff: 'spacing_bps: 10→25, grid_levels: 10→6'."""
    changes = []
    for f in _TRACKED_FIELDS:
        ov, nv = old.get(f), new.get(f)
        if isinstance(ov, float) and isinstance(nv, (Decimal, float)):
            if abs(float(ov) - float(nv)) < 1e-10:
                continue
        elif ov == nv:
            continue
        changes.append(f"{f}: {ov}→{nv}")
    prefix = f"[{product_id}] " if product_id else ""
    return prefix + ("; ".join(changes) if changes else "no changes")


class ExchangeStrategyRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get_active(self, exchange_name: str) -> ExchangeStrategyRow | None:
        result = await self._session.execute(
            select(ExchangeStrategyRow)
            .where(
                ExchangeStrategyRow.exchange_name == exchange_name,
                ExchangeStrategyRow.is_active.is_(True),
            )
            .order_by(ExchangeStrategyRow.updated_at.desc())
            .limit(1)
        )
        return result.scalar_one_or_none()

    async def get_by_name(self, name: str) -> ExchangeStrategyRow | None:
        result = await self._session.execute(
            select(ExchangeStrategyRow).where(ExchangeStrategyRow.name == name)
        )
        return result.scalar_one_or_none()

    async def list_all(self) -> list[ExchangeStrategyRow]:
        result = await self._session.execute(
            select(ExchangeStrategyRow).order_by(
                ExchangeStrategyRow.exchange_name, ExchangeStrategyRow.updated_at.desc()
            )
        )
        return list(result.scalars().all())

    async def list_by_exchange(self, exchange_name: str) -> list[ExchangeStrategyRow]:
        result = await self._session.execute(
            select(ExchangeStrategyRow)
            .where(ExchangeStrategyRow.exchange_name == exchange_name)
            .order_by(ExchangeStrategyRow.updated_at.desc())
        )
        return list(result.scalars().all())

    # ------------------------------------------------------------------
    # CENTRAL UPDATE FUNCTION — all param changes go through here
    # ------------------------------------------------------------------

    async def update_strategy_params(
        self,
        *,
        strategy_name: str,
        updated_by: str,
        params: dict,
        product_id: str | None = None,
    ) -> ExchangeStrategyRow:
        """
        Update strategy parameters with full SCD Type 2 audit trail.

        Args:
            strategy_name: Name of the strategy (e.g. 'bybit-default').
            updated_by: Who is making the change ('claude' or 'abraham').
            params: Dict of field_name→new_value for fields to change.
            product_id: If provided, changes go into symbol_overrides[product_id]
                        instead of the base strategy fields. Only fields in
                        _SYMBOL_OVERRIDABLE are accepted for per-symbol updates.
                        If None, changes apply to the base strategy (all symbols).

        Returns:
            The updated ExchangeStrategyRow.

        Raises:
            ValueError: If the strategy does not exist or invalid fields for per-symbol.
        """
        now = datetime.now(UTC)

        existing = await self.get_by_name(strategy_name)
        if existing is None:
            raise ValueError(f"Strategy not found: {strategy_name}")

        # 1. Snapshot OLD values before any mutation
        old_snapshot = _snapshot(existing)

        # 2. Close the current history row (set valid_to)
        current_history = await self._session.execute(
            select(StrategyParamHistoryRow)
            .where(
                StrategyParamHistoryRow.strategy_id == existing.id,
                StrategyParamHistoryRow.valid_to.is_(None),
            )
        )
        current_row = current_history.scalar_one_or_none()
        if current_row is not None:
            current_row.valid_to = now

        # 3. Apply changes
        if product_id:
            # Per-symbol: update symbol_overrides JSON
            bad_fields = set(params.keys()) - _SYMBOL_OVERRIDABLE
            if bad_fields:
                raise ValueError(
                    f"Fields {bad_fields} cannot be overridden per symbol. "
                    f"Allowed: {_SYMBOL_OVERRIDABLE}"
                )
            overrides = copy.deepcopy(existing.symbol_overrides or {})
            sym_dict = overrides.get(product_id, {})
            for field, value in params.items():
                if isinstance(value, Decimal):
                    sym_dict[field] = float(value)
                else:
                    sym_dict[field] = value
            overrides[product_id] = sym_dict
            existing.symbol_overrides = overrides
        else:
            # Base strategy: update fields directly
            for field, value in params.items():
                if field not in _TRACKED_FIELDS:
                    continue
                if isinstance(value, Decimal):
                    setattr(existing, field, float(value))
                else:
                    setattr(existing, field, value)

        existing.updated_by = updated_by
        existing.updated_at = now

        # 4. Build change summary from old→new
        new_snapshot = _snapshot(existing)
        summary = _build_change_summary(old_snapshot, new_snapshot, product_id)

        # 5. Insert new history row (valid_to=NULL = current version)
        history_row = StrategyParamHistoryRow(
            strategy_id=existing.id,
            strategy_name=existing.name,
            exchange_name=existing.exchange_name,
            spacing_bps=float(existing.spacing_bps),
            rebalance_threshold_bps=float(existing.rebalance_threshold_bps),
            grid_levels=existing.grid_levels,
            level_size_quote=float(existing.level_size_quote),
            max_inventory_ratio=float(existing.max_inventory_ratio),
            maker_fee_rate=float(existing.maker_fee_rate),
            stale_reprice_threshold_bps=float(existing.stale_reprice_threshold_bps),
            stale_order_age_seconds=existing.stale_order_age_seconds,
            rebalance_defer_seconds=existing.rebalance_defer_seconds,
            rebalance_defer_max_drift_bps=float(existing.rebalance_defer_max_drift_bps),
            total_wallet_usd=float(existing.total_wallet_usd),
            session_capital_usd=float(existing.session_capital_usd),
            maker_only=existing.maker_only,
            paper_mode=existing.paper_mode,
            symbols=existing.symbols,
            local_timezone_iana=existing.local_timezone_iana,
            daily_close_hour=existing.daily_close_hour,
            daily_close_minute=existing.daily_close_minute,
            spread_freeze_bps=float(existing.spread_freeze_bps),
            regime_stress_spread_bps=float(existing.regime_stress_spread_bps),
            regime_trend_slope_threshold=float(existing.regime_trend_slope_threshold),
            regime_mr_distance_threshold_bps=float(existing.regime_mr_distance_threshold_bps),
            regime_hysteresis_bps=float(existing.regime_hysteresis_bps),
            regime_rsi_bear_threshold=float(existing.regime_rsi_bear_threshold),
            regime_rsi_bull_threshold=float(existing.regime_rsi_bull_threshold),
            ws_retry_window_seconds=existing.ws_retry_window_seconds,
            ws_initial_retry_delay_seconds=existing.ws_initial_retry_delay_seconds,
            ws_max_retry_delay_seconds=existing.ws_max_retry_delay_seconds,
            ws_message_timeout_seconds=existing.ws_message_timeout_seconds,
            ws_heartbeat_timeout_seconds=existing.ws_heartbeat_timeout_seconds,
            symbol_overrides=existing.symbol_overrides,
            valid_from=now,
            valid_to=None,
            updated_by=updated_by,
            change_summary=summary,
        )
        self._session.add(history_row)

        # 6. Commit
        await self._session.commit()
        await self._session.refresh(existing)

        # 7. Log
        target = f" [{product_id}]" if product_id else ""
        logger.info(
            "Strategy '%s'%s updated by %s at %s | %s",
            strategy_name, target, updated_by, now.isoformat(), summary,
        )

        return existing

    # ------------------------------------------------------------------
    # UPSERT (create or full-replace) — also goes through SCD2 logging
    # ------------------------------------------------------------------

    async def upsert(
        self,
        *,
        name: str,
        exchange_name: str,
        updated_by: str,
        spacing_bps: Decimal,
        rebalance_threshold_bps: Decimal,
        grid_levels: int,
        level_size_quote: Decimal,
        max_inventory_ratio: Decimal,
        maker_fee_rate: Decimal,
        stale_reprice_threshold_bps: Decimal,
        stale_order_age_seconds: int,
        local_timezone_iana: str = "UTC",
        daily_close_hour: int = 0,
        daily_close_minute: int = 0,
        spread_freeze_bps: Decimal = Decimal("50"),
        regime_stress_spread_bps: Decimal = Decimal("35"),
        regime_trend_slope_threshold: Decimal = Decimal("0.0005"),
        regime_mr_distance_threshold_bps: Decimal = Decimal("18"),
        regime_hysteresis_bps: Decimal = Decimal("4"),
        regime_rsi_bear_threshold: Decimal = Decimal("42"),
        regime_rsi_bull_threshold: Decimal = Decimal("58"),
        ws_retry_window_seconds: int = 3600,
        ws_initial_retry_delay_seconds: int = 5,
        ws_max_retry_delay_seconds: int = 60,
        ws_message_timeout_seconds: int = 90,
        ws_heartbeat_timeout_seconds: int = 30,
        symbol_overrides: dict | None = None,
        make_active: bool = True,
    ) -> ExchangeStrategyRow:
        """Insert or full-update a strategy by name. Logs SCD2 history."""
        now = datetime.now(UTC)
        existing = await self.get_by_name(name)

        all_params = {
            "spacing_bps": spacing_bps,
            "rebalance_threshold_bps": rebalance_threshold_bps,
            "grid_levels": grid_levels,
            "level_size_quote": level_size_quote,
            "max_inventory_ratio": max_inventory_ratio,
            "maker_fee_rate": maker_fee_rate,
            "stale_reprice_threshold_bps": stale_reprice_threshold_bps,
            "stale_order_age_seconds": stale_order_age_seconds,
            "local_timezone_iana": local_timezone_iana,
            "daily_close_hour": daily_close_hour,
            "daily_close_minute": daily_close_minute,
            "spread_freeze_bps": spread_freeze_bps,
            "regime_stress_spread_bps": regime_stress_spread_bps,
            "regime_trend_slope_threshold": regime_trend_slope_threshold,
            "regime_mr_distance_threshold_bps": regime_mr_distance_threshold_bps,
            "regime_hysteresis_bps": regime_hysteresis_bps,
            "regime_rsi_bear_threshold": regime_rsi_bear_threshold,
            "regime_rsi_bull_threshold": regime_rsi_bull_threshold,
            "ws_retry_window_seconds": ws_retry_window_seconds,
            "ws_initial_retry_delay_seconds": ws_initial_retry_delay_seconds,
            "ws_max_retry_delay_seconds": ws_max_retry_delay_seconds,
            "ws_message_timeout_seconds": ws_message_timeout_seconds,
            "ws_heartbeat_timeout_seconds": ws_heartbeat_timeout_seconds,
            "symbol_overrides": symbol_overrides,
        }

        if existing:
            existing.exchange_name = exchange_name
            existing.is_active = make_active
            row = await self.update_strategy_params(
                strategy_name=name,
                updated_by=updated_by,
                params=all_params,
            )
        else:
            row = ExchangeStrategyRow(
                name=name,
                exchange_name=exchange_name,
                spacing_bps=float(spacing_bps),
                rebalance_threshold_bps=float(rebalance_threshold_bps),
                grid_levels=grid_levels,
                level_size_quote=float(level_size_quote),
                max_inventory_ratio=float(max_inventory_ratio),
                maker_fee_rate=float(maker_fee_rate),
                stale_reprice_threshold_bps=float(stale_reprice_threshold_bps),
                stale_order_age_seconds=stale_order_age_seconds,
                local_timezone_iana=local_timezone_iana,
                daily_close_hour=daily_close_hour,
                daily_close_minute=daily_close_minute,
                spread_freeze_bps=float(spread_freeze_bps),
                regime_stress_spread_bps=float(regime_stress_spread_bps),
                regime_trend_slope_threshold=float(regime_trend_slope_threshold),
                regime_mr_distance_threshold_bps=float(regime_mr_distance_threshold_bps),
                regime_hysteresis_bps=float(regime_hysteresis_bps),
                regime_rsi_bear_threshold=float(regime_rsi_bear_threshold),
                regime_rsi_bull_threshold=float(regime_rsi_bull_threshold),
                ws_retry_window_seconds=ws_retry_window_seconds,
                ws_initial_retry_delay_seconds=ws_initial_retry_delay_seconds,
                ws_max_retry_delay_seconds=ws_max_retry_delay_seconds,
                ws_message_timeout_seconds=ws_message_timeout_seconds,
                ws_heartbeat_timeout_seconds=ws_heartbeat_timeout_seconds,
                symbol_overrides=symbol_overrides,
                is_active=make_active,
                updated_by=updated_by,
                created_at=now,
                updated_at=now,
            )
            self._session.add(row)
            await self._session.flush()

            history_row = StrategyParamHistoryRow(
                strategy_id=row.id,
                strategy_name=row.name,
                exchange_name=row.exchange_name,
                spacing_bps=float(spacing_bps),
                rebalance_threshold_bps=float(rebalance_threshold_bps),
                grid_levels=row.grid_levels,
                level_size_quote=float(level_size_quote),
                max_inventory_ratio=float(max_inventory_ratio),
                maker_fee_rate=float(maker_fee_rate),
                stale_reprice_threshold_bps=float(stale_reprice_threshold_bps),
                stale_order_age_seconds=row.stale_order_age_seconds,
                rebalance_defer_seconds=row.rebalance_defer_seconds,
                rebalance_defer_max_drift_bps=float(row.rebalance_defer_max_drift_bps),
                total_wallet_usd=float(row.total_wallet_usd),
                session_capital_usd=float(row.session_capital_usd),
                maker_only=row.maker_only,
                paper_mode=row.paper_mode,
                symbols=row.symbols,
                local_timezone_iana=row.local_timezone_iana,
                daily_close_hour=row.daily_close_hour,
                daily_close_minute=row.daily_close_minute,
                spread_freeze_bps=float(row.spread_freeze_bps),
                regime_stress_spread_bps=float(row.regime_stress_spread_bps),
                regime_trend_slope_threshold=float(row.regime_trend_slope_threshold),
                regime_mr_distance_threshold_bps=float(row.regime_mr_distance_threshold_bps),
                regime_hysteresis_bps=float(row.regime_hysteresis_bps),
                regime_rsi_bear_threshold=float(row.regime_rsi_bear_threshold),
                regime_rsi_bull_threshold=float(row.regime_rsi_bull_threshold),
                ws_retry_window_seconds=row.ws_retry_window_seconds,
                ws_initial_retry_delay_seconds=row.ws_initial_retry_delay_seconds,
                ws_max_retry_delay_seconds=row.ws_max_retry_delay_seconds,
                ws_message_timeout_seconds=row.ws_message_timeout_seconds,
                ws_heartbeat_timeout_seconds=row.ws_heartbeat_timeout_seconds,
                symbol_overrides=row.symbol_overrides,
                valid_from=now,
                valid_to=None,
                updated_by=updated_by,
                change_summary="initial creation",
            )
            self._session.add(history_row)
            await self._session.commit()
            await self._session.refresh(row)

            logger.info("Strategy '%s' created by %s at %s", name, updated_by, now.isoformat())

        if make_active:
            await self._session.execute(
                update(ExchangeStrategyRow)
                .where(
                    ExchangeStrategyRow.exchange_name == exchange_name,
                    ExchangeStrategyRow.name != name,
                )
                .values(is_active=False)
            )
            await self._session.commit()

        return row

    async def set_active(self, name: str) -> ExchangeStrategyRow | None:
        row = await self.get_by_name(name)
        if row is None:
            return None
        await self._session.execute(
            update(ExchangeStrategyRow)
            .where(ExchangeStrategyRow.exchange_name == row.exchange_name)
            .values(is_active=False)
        )
        row.is_active = True
        row.updated_at = datetime.now(UTC)
        await self._session.commit()
        await self._session.refresh(row)
        return row

    async def get_param_history(self, strategy_name: str, limit: int = 50) -> list[dict]:
        """Return SCD2 parameter change history for a strategy, newest first."""
        result = await self._session.execute(
            select(StrategyParamHistoryRow)
            .where(StrategyParamHistoryRow.strategy_name == strategy_name)
            .order_by(StrategyParamHistoryRow.valid_from.desc())
            .limit(limit)
        )
        rows = result.scalars().all()
        return [
            {
                "history_id": r.history_id,
                "valid_from": r.valid_from.isoformat() if r.valid_from else None,
                "valid_to": r.valid_to.isoformat() if r.valid_to else None,
                "updated_by": r.updated_by,
                "change_summary": r.change_summary,
                "spacing_bps": str(r.spacing_bps),
                "rebalance_threshold_bps": str(r.rebalance_threshold_bps),
                "grid_levels": r.grid_levels,
                "level_size_quote": str(r.level_size_quote),
                "max_inventory_ratio": str(r.max_inventory_ratio),
                "maker_fee_rate": str(r.maker_fee_rate),
                "stale_reprice_threshold_bps": str(r.stale_reprice_threshold_bps),
                "stale_order_age_seconds": r.stale_order_age_seconds,
                "rebalance_defer_seconds": r.rebalance_defer_seconds,
                "rebalance_defer_max_drift_bps": str(r.rebalance_defer_max_drift_bps),
                "total_wallet_usd": str(r.total_wallet_usd),
                "session_capital_usd": str(r.session_capital_usd),
                "local_timezone_iana": r.local_timezone_iana,
                "daily_close_hour": r.daily_close_hour,
                "daily_close_minute": r.daily_close_minute,
                "spread_freeze_bps": str(r.spread_freeze_bps),
                "regime_stress_spread_bps": str(r.regime_stress_spread_bps),
                "regime_trend_slope_threshold": str(r.regime_trend_slope_threshold),
                "regime_mr_distance_threshold_bps": str(r.regime_mr_distance_threshold_bps),
                "regime_hysteresis_bps": str(r.regime_hysteresis_bps),
                "regime_rsi_bear_threshold": str(r.regime_rsi_bear_threshold),
                "regime_rsi_bull_threshold": str(r.regime_rsi_bull_threshold),
                "ws_retry_window_seconds": r.ws_retry_window_seconds,
                "ws_initial_retry_delay_seconds": r.ws_initial_retry_delay_seconds,
                "ws_max_retry_delay_seconds": r.ws_max_retry_delay_seconds,
                "ws_message_timeout_seconds": r.ws_message_timeout_seconds,
                "ws_heartbeat_timeout_seconds": r.ws_heartbeat_timeout_seconds,
                "symbol_overrides": r.symbol_overrides,
            }
            for r in rows
        ]

    async def get_last_param_change(self, strategy_id: int) -> datetime | None:
        result = await self._session.execute(
            select(StrategyParamHistoryRow.valid_from)
            .where(StrategyParamHistoryRow.strategy_id == strategy_id)
            .order_by(StrategyParamHistoryRow.valid_from.desc())
            .limit(1)
        )
        return result.scalar_one_or_none()

    def get_resolved_params(self, row: ExchangeStrategyRow, product_id: str) -> dict:
        """Return effective params for a symbol with pair-scoped fields sourced from overrides."""
        base = self.row_to_dict(row)
        overrides = (row.symbol_overrides or {}).get(product_id, {})
        resolved = {**base}
        for key in _SYMBOL_OVERRIDABLE:
            if key in overrides:
                value = overrides[key]
                resolved[key] = str(value) if isinstance(value, (int, float)) else value
            else:
                resolved.pop(key, None)
        resolved["_source"] = {
            key: ("override" if key in overrides else "missing")
            for key in _SYMBOL_OVERRIDABLE
        }
        return resolved

    @staticmethod
    def row_to_dict(row: ExchangeStrategyRow) -> dict:
        return {
            "id": row.id,
            "name": row.name,
            "exchange_name": row.exchange_name,
            "is_active": row.is_active,
            "spacing_bps": str(row.spacing_bps),
            "rebalance_threshold_bps": str(row.rebalance_threshold_bps),
            "grid_levels": row.grid_levels,
            "level_size_quote": str(row.level_size_quote),
            "max_inventory_ratio": str(row.max_inventory_ratio),
            "maker_fee_rate": str(row.maker_fee_rate),
            "stale_reprice_threshold_bps": str(row.stale_reprice_threshold_bps),
            "stale_order_age_seconds": row.stale_order_age_seconds,
            "rebalance_defer_seconds": row.rebalance_defer_seconds,
            "rebalance_defer_max_drift_bps": str(row.rebalance_defer_max_drift_bps),
            "total_wallet_usd": str(row.total_wallet_usd),
            "session_capital_usd": str(row.session_capital_usd),
            "maker_only": row.maker_only,
            "paper_mode": row.paper_mode,
            "symbols": row.symbols,
            "local_timezone_iana": row.local_timezone_iana,
            "daily_close_hour": row.daily_close_hour,
            "daily_close_minute": row.daily_close_minute,
            "spread_freeze_bps": str(row.spread_freeze_bps),
            "regime_stress_spread_bps": str(row.regime_stress_spread_bps),
            "regime_trend_slope_threshold": str(row.regime_trend_slope_threshold),
            "regime_mr_distance_threshold_bps": str(row.regime_mr_distance_threshold_bps),
            "regime_hysteresis_bps": str(row.regime_hysteresis_bps),
            "regime_rsi_bear_threshold": str(row.regime_rsi_bear_threshold),
            "regime_rsi_bull_threshold": str(row.regime_rsi_bull_threshold),
            "ws_retry_window_seconds": row.ws_retry_window_seconds,
            "ws_initial_retry_delay_seconds": row.ws_initial_retry_delay_seconds,
            "ws_max_retry_delay_seconds": row.ws_max_retry_delay_seconds,
            "ws_message_timeout_seconds": row.ws_message_timeout_seconds,
            "ws_heartbeat_timeout_seconds": row.ws_heartbeat_timeout_seconds,
            "symbol_overrides": row.symbol_overrides or {},
            "updated_by": row.updated_by,
            "updated_at": row.updated_at.isoformat() if row.updated_at else None,
        }
