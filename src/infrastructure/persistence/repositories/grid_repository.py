from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from domain.enums import OrderSide
from domain.models import Fill, GridLevel, GridState, MarketSnapshot

from infrastructure.persistence.orm.bot_restarts import BotRestartRow
from infrastructure.persistence.orm.equity_snapshots import EquitySnapshotRow
from infrastructure.persistence.orm.fills import FillRow
from infrastructure.persistence.orm.grid_levels import GridLevelRow
from infrastructure.persistence.orm.sessions import SessionRow
from infrastructure.persistence.orm.ticks import TickRow


class GridRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def upsert_session(
        self,
        grid_state: GridState,
        mode: str,
        reserve_usd: Decimal = Decimal("0"),
        underfunded: bool = False,
        underfunded_shortfall_usd: Decimal = Decimal("0"),
        strategy_snapshot: dict | None = None,
    ) -> None:
        """Insert or update a session row.

        `strategy_snapshot` is only written on INSERT (first call for a new session_id).
        It captures the exact strategy parameters active when this session started,
        so that fills can always be traced back to the params that generated them
        even after the strategy is reconfigured.

        Expected keys: level_size_quote, rebalance_threshold_bps,
                       max_inventory_ratio, maker_fee_rate, symbol_overrides.
        """
        existing = await self._session.get(SessionRow, str(grid_state.session_id))
        now = datetime.now(UTC)
        if existing is None:
            snap = strategy_snapshot or {}
            row = SessionRow(
                session_id=str(grid_state.session_id),
                product_id=grid_state.product_id,
                mode=mode,
                status="active",
                mid_anchor=float(grid_state.mid_anchor),
                spacing_bps=float(grid_state.spacing_bps),
                grid_levels=len(grid_state.bid_levels),
                realized_pnl_quote=float(grid_state.realized_pnl_quote),
                total_fills=grid_state.total_fills,
                reserve_usd=float(reserve_usd),
                underfunded=underfunded,
                underfunded_shortfall_usd=float(underfunded_shortfall_usd),
                level_size_quote=snap.get("level_size_quote"),
                rebalance_threshold_bps=snap.get("rebalance_threshold_bps"),
                max_inventory_ratio=snap.get("max_inventory_ratio"),
                maker_fee_rate=snap.get("maker_fee_rate"),
                symbol_overrides=snap.get("symbol_overrides"),
                started_at=now,
                updated_at=now,
            )
            self._session.add(row)
        else:
            existing.mid_anchor = float(grid_state.mid_anchor)
            existing.realized_pnl_quote = float(grid_state.realized_pnl_quote)
            existing.total_fills = grid_state.total_fills
            existing.reserve_usd = float(reserve_usd)
            existing.underfunded = underfunded
            existing.underfunded_shortfall_usd = float(underfunded_shortfall_usd)
            existing.updated_at = now
        await self._session.flush()

    async def close_session(self, session_id: UUID) -> None:
        """Mark a session as closed with current timestamp."""
        existing = await self._session.get(SessionRow, str(session_id))
        if existing is not None:
            existing.status = "closed"
            existing.ended_at = datetime.now(UTC)
            await self._session.flush()

    async def upsert_level(self, level: GridLevel) -> None:
        existing = await self._session.get(GridLevelRow, str(level.level_id))
        if existing is None:
            row = GridLevelRow(
                level_id=str(level.level_id),
                product_id=level.product_id,
                session_id=str(level.session_id),
                side=level.side.value,
                level_index=level.level_index,
                price=float(level.price),
                size_base=float(level.size_base),
                size_quote=float(level.size_quote),
                client_order_id=level.client_order_id,
                order_id=level.order_id,
                status=level.status,
                fill_price=float(level.fill_price) if level.fill_price else None,
                fill_fee_quote=float(level.fill_fee_quote) if level.fill_fee_quote else None,
                created_at=level.created_at,
                updated_at=level.updated_at,
                opened_at=level.opened_at,
                filled_at=level.filled_at,
            )
            self._session.add(row)
        else:
            existing.client_order_id = level.client_order_id
            existing.order_id = level.order_id
            existing.status = level.status
            existing.fill_price = float(level.fill_price) if level.fill_price else None
            existing.fill_fee_quote = float(level.fill_fee_quote) if level.fill_fee_quote else None
            existing.updated_at = level.updated_at
            existing.opened_at = level.opened_at
            existing.filled_at = level.filled_at
        await self._session.flush()

    async def save_fill(self, fill: Fill, session_id: UUID) -> None:
        row = FillRow(
            fill_id=fill.fill_id,
            order_id=fill.order_id,
            client_order_id=fill.client_order_id,
            product_id=fill.product_id,
            session_id=str(session_id),
            side=fill.side.value,
            price=float(fill.price),
            size_base=float(fill.size_base),
            quote_value=float(fill.quote_value),
            fee_quote=float(fill.fee_quote),
            level_index=fill.level_index,
            grid_side=fill.grid_side,
            liquidity_indicator=fill.liquidity_indicator,
            trade_time=fill.trade_time,
        )
        self._session.add(row)
        await self._session.flush()

    async def load_last_session_state(self, product_id: str) -> dict | None:
        """
        Returns the most recent active session's inventory snapshot, or None.
        Used to resume paper trading after a restart.

        Closes all other stale 'active' sessions for this product to prevent
        orphan accumulation from crashes or ungraceful shutdowns.
        """
        stmt = (
            select(SessionRow)
            .where(SessionRow.product_id == product_id, SessionRow.status == "active")
            .order_by(SessionRow.started_at.desc())
            .limit(1)
        )
        result = await self._session.execute(stmt)
        row = result.scalar_one_or_none()
        if row is None:
            return None

        # Close all OTHER active sessions for this product (orphans from crashes)
        now = datetime.now(UTC)
        orphans_stmt = (
            select(SessionRow)
            .where(
                SessionRow.product_id == product_id,
                SessionRow.status == "active",
                SessionRow.session_id != str(row.session_id),
            )
        )
        orphans_result = await self._session.execute(orphans_stmt)
        for orphan in orphans_result.scalars().all():
            orphan.status = "closed"
            orphan.ended_at = now
        await self._session.flush()

        # Compute inventory from fills: starting inventory ± all fills
        fills_stmt = select(FillRow).where(FillRow.session_id == str(row.session_id))
        fills_result = await self._session.execute(fills_stmt)
        fill_rows = fills_result.scalars().all()
        return {
            "session_id": UUID(str(row.session_id)),
            "realized_pnl_quote": Decimal(str(row.realized_pnl_quote)),
            "total_fills": row.total_fills,
            "fill_rows": fill_rows,
            "reserve_usd": Decimal(str(row.reserve_usd)),
        }

    async def cancel_stale_levels(self, session_id: UUID, active_level_ids: list[UUID]) -> int:
        """Mark all open/pending levels for this session as cancelled, except the active ones."""
        from sqlalchemy import and_, not_
        now = datetime.now(UTC)
        active_strs = [str(lid) for lid in active_level_ids]
        conditions = [
            GridLevelRow.session_id == str(session_id),
            GridLevelRow.status.in_(["pending", "open"]),
        ]
        if active_strs:
            conditions.append(not_(GridLevelRow.level_id.in_(active_strs)))
        stmt = (
            update(GridLevelRow)
            .where(and_(*conditions))
            .values(status="cancelled", updated_at=now)
        )
        result = await self._session.execute(stmt)
        await self._session.flush()
        return result.rowcount

    async def cancel_orphan_levels(self, product_id: str, current_session_id: UUID) -> int:
        """Cancel all open/pending levels for a product that belong to other sessions.

        Called on worker startup to clean up levels from previous runs whose paper
        adapter state was lost on restart. Without this, old 'open' levels linger
        indefinitely because cancel_stale_levels only sees the current session.
        """
        from sqlalchemy import and_
        now = datetime.now(UTC)
        stmt = (
            update(GridLevelRow)
            .where(and_(
                GridLevelRow.product_id == product_id,
                GridLevelRow.session_id != str(current_session_id),
                GridLevelRow.status.in_(["pending", "open"]),
            ))
            .values(status="cancelled", updated_at=now)
        )
        result = await self._session.execute(stmt)
        await self._session.flush()
        return result.rowcount

    async def load_open_levels(self, product_id: str, session_id: UUID) -> list[GridLevel]:
        stmt = select(GridLevelRow).where(
            GridLevelRow.product_id == product_id,
            GridLevelRow.session_id == str(session_id),
            GridLevelRow.status.in_(["pending", "open"]),
        )
        result = await self._session.execute(stmt)
        rows = result.scalars().all()
        return [self._row_to_level(r) for r in rows]

    async def load_fills_today(self, product_id: str, session_id: UUID) -> list[Fill]:
        stmt = select(FillRow).where(
            FillRow.product_id == product_id,
            FillRow.session_id == str(session_id),
        )
        result = await self._session.execute(stmt)
        rows = result.scalars().all()
        return [self._row_to_fill(r) for r in rows]

    def _row_to_level(self, row: GridLevelRow) -> GridLevel:
        from domain.enums import OrderSide
        return GridLevel(
            level_id=UUID(row.level_id),
            product_id=row.product_id,
            session_id=UUID(row.session_id),
            side=OrderSide(row.side),
            level_index=row.level_index,
            price=Decimal(str(row.price)),
            size_base=Decimal(str(row.size_base)),
            size_quote=Decimal(str(row.size_quote)),
            client_order_id=row.client_order_id,
            order_id=row.order_id,
            status=row.status,
            fill_price=Decimal(str(row.fill_price)) if row.fill_price is not None else None,
            fill_fee_quote=Decimal(str(row.fill_fee_quote)) if row.fill_fee_quote is not None else None,
            created_at=row.created_at,
            updated_at=row.updated_at,
            opened_at=row.opened_at,
            filled_at=row.filled_at,
        )

    def _row_to_fill(self, row: FillRow) -> Fill:
        return Fill(
            fill_id=row.fill_id,
            order_id=row.order_id,
            client_order_id=row.client_order_id,
            product_id=row.product_id,
            side=OrderSide(row.side),
            price=Decimal(str(row.price)),
            size_base=Decimal(str(row.size_base)),
            quote_value=Decimal(str(row.quote_value)),
            fee_quote=Decimal(str(row.fee_quote)),
            level_index=row.level_index,
            grid_side=row.grid_side,
            liquidity_indicator=row.liquidity_indicator,
            trade_time=row.trade_time,
        )

    async def save_tick(self, snapshot: MarketSnapshot) -> None:
        row = TickRow(
            product_id=snapshot.product_id,
            bid=float(snapshot.bid),
            ask=float(snapshot.ask),
            mid=float(snapshot.mid),
            last_trade_price=float(snapshot.last_trade_price),
            event_time=snapshot.event_time,
        )
        self._session.add(row)
        await self._session.flush()

    async def save_equity_snapshot(
        self,
        grid_state: GridState,
        mid_price: Decimal,
        trigger: str,
    ) -> None:
        now = datetime.now(UTC)
        row = EquitySnapshotRow(
            session_id=str(grid_state.session_id),
            product_id=grid_state.product_id,
            total_equity=float(grid_state.total_equity),
            quote_inventory=float(grid_state.quote_inventory),
            base_inventory=float(grid_state.base_inventory),
            realized_pnl=float(grid_state.realized_pnl_quote),
            unrealized_pnl=float(grid_state.unrealized_pnl_quote),
            mid_anchor=float(grid_state.mid_anchor),
            mid_price=float(mid_price),
            trigger=trigger,
            recorded_at=now,
        )
        self._session.add(row)
        await self._session.flush()

    async def get_init_equity(self, session_id: str) -> float | None:
        """Return total_equity from the first 'init' snapshot of the session."""
        stmt = (
            select(EquitySnapshotRow.total_equity)
            .where(
                EquitySnapshotRow.session_id == session_id,
                EquitySnapshotRow.trigger == "init",
            )
            .order_by(EquitySnapshotRow.recorded_at.asc())
            .limit(1)
        )
        result = await self._session.execute(stmt)
        row = result.scalar_one_or_none()
        return float(row) if row is not None else None

    async def get_session_equity_delta(self, session_id: str) -> tuple[float | None, float | None]:
        """Return (init_equity, final_equity) for a session using equity_snapshots."""
        init_stmt = (
            select(EquitySnapshotRow.total_equity)
            .where(EquitySnapshotRow.session_id == session_id,
                   EquitySnapshotRow.trigger == "init")
            .order_by(EquitySnapshotRow.recorded_at.asc())
            .limit(1)
        )
        last_stmt = (
            select(EquitySnapshotRow.total_equity)
            .where(EquitySnapshotRow.session_id == session_id)
            .order_by(EquitySnapshotRow.recorded_at.desc())
            .limit(1)
        )
        init_row  = (await self._session.execute(init_stmt)).scalar_one_or_none()
        final_row = (await self._session.execute(last_stmt)).scalar_one_or_none()
        return (
            float(init_row)  if init_row  is not None else None,
            float(final_row) if final_row is not None else None,
        )

    async def get_last_close_delta(self, product_id: str) -> float | None:
        """Equity delta (final - init) of the most recently closed session."""
        stmt = (
            select(SessionRow.session_id)
            .where(SessionRow.product_id == product_id,
                   SessionRow.status == "closed")
            .order_by(SessionRow.ended_at.desc())
            .limit(1)
        )
        row = (await self._session.execute(stmt)).first()
        if row is None:
            return None
        init_eq, final_eq = await self.get_session_equity_delta(str(row.session_id))
        if init_eq is None or final_eq is None:
            return None
        return final_eq - init_eq

    async def get_uptime_pnl(self, product_id: str, since: datetime, current_equity: float) -> float:
        """Total equity-based PnL since `since`:
        sum of (final - init) for closed sessions ended >= since,
        plus (current_equity - init_equity) for the active session."""
        # Closed sessions ended since uptime start
        stmt = (
            select(SessionRow.session_id)
            .where(SessionRow.product_id == product_id,
                   SessionRow.status == "closed",
                   SessionRow.ended_at >= since)
            .order_by(SessionRow.ended_at.asc())
        )
        rows = (await self._session.execute(stmt)).fetchall()
        total = 0.0
        for row in rows:
            init_eq, final_eq = await self.get_session_equity_delta(str(row.session_id))
            if init_eq is not None and final_eq is not None:
                total += final_eq - init_eq

        # Active session's current delta
        active_stmt = (
            select(SessionRow.session_id)
            .where(SessionRow.product_id == product_id,
                   SessionRow.status == "active")
            .limit(1)
        )
        active_row = (await self._session.execute(active_stmt)).first()
        if active_row:
            init_eq, _ = await self.get_session_equity_delta(str(active_row.session_id))
            if init_eq is not None:
                total += current_equity - init_eq

        return total

    async def save_restart(
        self,
        session_id: UUID,
        product_id: str,
        triggered_by: str,
    ) -> None:
        row = BotRestartRow(
            session_id=str(session_id),
            product_id=product_id,
            triggered_by=triggered_by,
            restarted_at=datetime.now(UTC),
        )
        self._session.add(row)
        await self._session.flush()

    async def get_recent_fills(
        self, product_id: str, session_id: str, limit: int = 50
    ) -> list[dict]:
        """
        Query fills for the given product_id and session_id,
        ordered by trade_time DESC, up to limit rows.
        Returns list of dicts matching the fills response format.
        """
        stmt = (
            select(FillRow)
            .where(
                FillRow.product_id == product_id,
                FillRow.session_id == session_id,
            )
            .order_by(FillRow.trade_time.desc())
            .limit(limit)
        )
        result = await self._session.execute(stmt)
        rows = result.scalars().all()
        return [
            {
                "fill_id": row.fill_id,
                "side": row.side,
                "price": str(Decimal(str(row.price))),
                "size_base": str(Decimal(str(row.size_base))),
                "quote_value": str(Decimal(str(row.quote_value))),
                "fee_quote": str(Decimal(str(row.fee_quote))),
                "level_index": row.level_index,
                "grid_side": row.grid_side,
                "trade_time": row.trade_time.isoformat(),
            }
            for row in rows
        ]
