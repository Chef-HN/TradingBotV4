from __future__ import annotations

from sqlalchemy import Boolean, DateTime, Integer, JSON, Numeric, String, Uuid
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base


class SessionRow(Base):
    __tablename__ = "sessions"

    session_id: Mapped[str] = mapped_column(Uuid(as_uuid=False), primary_key=True)
    tenant_id: Mapped[str] = mapped_column(Uuid(as_uuid=False), nullable=False, index=True)
    product_id: Mapped[str] = mapped_column(String(20), nullable=False, index=True)
    mode: Mapped[str] = mapped_column(String(10), nullable=False)  # paper | live
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="active")
    mid_anchor: Mapped[float] = mapped_column(Numeric(20, 10), nullable=False)
    spacing_bps: Mapped[float] = mapped_column(Numeric(10, 4), nullable=False)
    grid_levels: Mapped[int] = mapped_column(Integer, nullable=False)
    realized_pnl_quote: Mapped[float] = mapped_column(Numeric(20, 10), nullable=False, default=0)
    total_fills: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    reserve_usd: Mapped[float] = mapped_column(Numeric(20, 10), nullable=False, server_default="0")
    underfunded: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    underfunded_shortfall_usd: Mapped[float] = mapped_column(
        Numeric(20, 10), nullable=False, server_default="0"
    )
    # Strategy snapshot — captures exact params active when this session started
    level_size_quote: Mapped[float | None] = mapped_column(Numeric(20, 10), nullable=True)
    rebalance_threshold_bps: Mapped[float | None] = mapped_column(Numeric(10, 4), nullable=True)
    max_inventory_ratio: Mapped[float | None] = mapped_column(Numeric(6, 4), nullable=True)
    maker_fee_rate: Mapped[float | None] = mapped_column(Numeric(10, 6), nullable=True)
    symbol_overrides: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    started_at: Mapped[object] = mapped_column(DateTime(timezone=True), nullable=False)
    ended_at: Mapped[object | None] = mapped_column(DateTime(timezone=True), nullable=True)
    updated_at: Mapped[object] = mapped_column(DateTime(timezone=True), nullable=False)
