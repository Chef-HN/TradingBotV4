from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from infrastructure.persistence.orm.parameter_change_audit import ParameterChangeAuditRow
from infrastructure.persistence.orm.tenant_pair_strategies import TenantPairStrategyRow
from infrastructure.persistence.orm.tenant_pair_strategy_history import TenantPairStrategyHistoryRow

_TRACKED_FIELDS = [
    "spacing_bps",
    "rebalance_threshold_bps",
    "grid_levels",
    "level_size_quote",
    "max_inventory_ratio",
    "maker_fee_rate",
    "stale_reprice_threshold_bps",
    "stale_order_age_seconds",
    "rebalance_defer_seconds",
    "rebalance_defer_max_drift_bps",
    "total_wallet_usd",
    "session_capital_usd",
    "maker_only",
    "paper_mode",
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
]


def _to_store_value(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    return value


class TenantPairStrategyRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def list_active_for_exchange(self, tenant_id: str, exchange_name: str) -> list[TenantPairStrategyRow]:
        result = await self._session.execute(
            select(TenantPairStrategyRow).where(
                TenantPairStrategyRow.tenant_id == tenant_id,
                TenantPairStrategyRow.exchange_name == exchange_name,
                TenantPairStrategyRow.is_active.is_(True),
            )
        )
        return list(result.scalars().all())

    async def get_active(self, tenant_id: str, exchange_name: str, product_id: str) -> TenantPairStrategyRow | None:
        result = await self._session.execute(
            select(TenantPairStrategyRow).where(
                TenantPairStrategyRow.tenant_id == tenant_id,
                TenantPairStrategyRow.exchange_name == exchange_name,
                TenantPairStrategyRow.product_id == product_id,
                TenantPairStrategyRow.is_active.is_(True),
            )
        )
        return result.scalar_one_or_none()

    async def get_last_param_change(self, tenant_id: str, exchange_name: str, product_id: str) -> datetime | None:
        row = await self.get_active(tenant_id, exchange_name, product_id)
        if row is None:
            return None
        result = await self._session.execute(
            select(TenantPairStrategyHistoryRow.valid_from)
            .where(
                TenantPairStrategyHistoryRow.strategy_id == row.id,
            )
            .order_by(TenantPairStrategyHistoryRow.valid_from.desc())
            .limit(1)
        )
        return result.scalar_one_or_none()

    async def upsert_initial(
        self,
        *,
        tenant_id: str,
        exchange_name: str,
        product_id: str,
        payload: dict[str, Any],
        updated_by: str,
    ) -> TenantPairStrategyRow:
        now = datetime.now(UTC)
        row = await self.get_active(tenant_id, exchange_name, product_id)
        if row is None:
            row = TenantPairStrategyRow(
                tenant_id=tenant_id,
                exchange_name=exchange_name,
                product_id=product_id,
                is_active=True,
                updated_by=updated_by,
                created_at=now,
                updated_at=now,
                **{k: _to_store_value(payload[k]) for k in _TRACKED_FIELDS if k in payload},
            )
            self._session.add(row)
            await self._session.flush()
        else:
            for key in _TRACKED_FIELDS:
                if key in payload:
                    setattr(row, key, _to_store_value(payload[key]))
            row.updated_by = updated_by
            row.updated_at = now

        await self._insert_history(row, updated_by=updated_by, valid_from=now, summary="initial seed")
        await self._session.commit()
        await self._session.refresh(row)
        return row

    async def preview_changes(
        self,
        *,
        tenant_id: str,
        exchange_name: str,
        product_id: str,
        params: dict[str, Any],
        proposed_by: str,
        reason: str | None,
    ) -> dict[str, Any]:
        row = await self.get_active(tenant_id, exchange_name, product_id)
        if row is None:
            raise ValueError(f"No active pair strategy for {tenant_id}:{exchange_name}:{product_id}")

        current = self.row_to_dict(row)
        merged = dict(current)
        for key, value in params.items():
            if key in _TRACKED_FIELDS:
                merged[key] = value

        diff = {
            key: {"old": current.get(key), "new": merged.get(key)}
            for key in _TRACKED_FIELDS
            if current.get(key) != merged.get(key)
        }

        audit = ParameterChangeAuditRow(
            tenant_id=tenant_id,
            exchange_name=exchange_name,
            product_id=product_id,
            change_type="preview",
            proposed_by=proposed_by,
            reason=reason,
            change_payload=params,
            change_diff=diff or None,
            approved=False,
            created_at=datetime.now(UTC),
        )
        self._session.add(audit)
        await self._session.commit()

        return {
            "tenant_id": tenant_id,
            "exchange_name": exchange_name,
            "product_id": product_id,
            "current": current,
            "proposed": merged,
            "diff": diff,
        }

    async def apply_changes(
        self,
        *,
        tenant_id: str,
        exchange_name: str,
        product_id: str,
        params: dict[str, Any],
        updated_by: str,
        reason: str | None,
    ) -> TenantPairStrategyRow:
        row = await self.get_active(tenant_id, exchange_name, product_id)
        if row is None:
            raise ValueError(f"No active pair strategy for {tenant_id}:{exchange_name}:{product_id}")

        now = datetime.now(UTC)
        before = self.row_to_dict(row)

        current_history = await self._session.execute(
            select(TenantPairStrategyHistoryRow).where(
                TenantPairStrategyHistoryRow.strategy_id == row.id,
                TenantPairStrategyHistoryRow.valid_to.is_(None),
            )
        )
        history_row = current_history.scalar_one_or_none()
        if history_row is not None:
            history_row.valid_to = now

        for key, value in params.items():
            if key in _TRACKED_FIELDS:
                setattr(row, key, _to_store_value(value))
        row.updated_by = updated_by
        row.updated_at = now

        after = self.row_to_dict(row)
        diff = {
            key: {"old": before.get(key), "new": after.get(key)}
            for key in _TRACKED_FIELDS
            if before.get(key) != after.get(key)
        }
        summary = "; ".join([f"{k}:{v['old']}->{v['new']}" for k, v in diff.items()]) or "no changes"

        await self._insert_history(row, updated_by=updated_by, valid_from=now, summary=summary)

        audit = ParameterChangeAuditRow(
            tenant_id=tenant_id,
            exchange_name=exchange_name,
            product_id=product_id,
            change_type="apply",
            proposed_by=updated_by,
            reason=reason,
            change_payload=params,
            change_diff=diff or None,
            approved=True,
            created_at=now,
        )
        self._session.add(audit)
        await self._session.commit()
        await self._session.refresh(row)
        return row

    async def _insert_history(
        self,
        row: TenantPairStrategyRow,
        *,
        updated_by: str,
        valid_from: datetime,
        summary: str,
    ) -> None:
        history = TenantPairStrategyHistoryRow(
            strategy_id=row.id,
            tenant_id=row.tenant_id,
            exchange_name=row.exchange_name,
            product_id=row.product_id,
            valid_from=valid_from,
            valid_to=None,
            updated_by=updated_by,
            change_summary=summary,
            **{key: getattr(row, key) for key in _TRACKED_FIELDS},
        )
        self._session.add(history)

    @staticmethod
    def row_to_dict(row: TenantPairStrategyRow) -> dict[str, Any]:
        return {
            "id": row.id,
            "tenant_id": row.tenant_id,
            "exchange_name": row.exchange_name,
            "product_id": row.product_id,
            "is_active": row.is_active,
            "updated_by": row.updated_by,
            "updated_at": row.updated_at.isoformat() if row.updated_at else None,
            **{key: getattr(row, key) for key in _TRACKED_FIELDS},
        }
