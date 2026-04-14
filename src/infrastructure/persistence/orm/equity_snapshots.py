from __future__ import annotations

from sqlalchemy import BigInteger, DateTime, Index, Numeric, String, Uuid
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base


class EquitySnapshotRow(Base):
    __tablename__ = "equity_snapshots"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    session_id: Mapped[str] = mapped_column(Uuid(as_uuid=False), nullable=False)
    product_id: Mapped[str] = mapped_column(String(20), nullable=False)
    total_equity: Mapped[float] = mapped_column(Numeric(20, 10), nullable=False)
    quote_inventory: Mapped[float] = mapped_column(Numeric(20, 10), nullable=False)
    base_inventory: Mapped[float] = mapped_column(Numeric(20, 10), nullable=False)
    realized_pnl: Mapped[float] = mapped_column(Numeric(20, 10), nullable=False)
    unrealized_pnl: Mapped[float] = mapped_column(Numeric(20, 10), nullable=False)
    mid_anchor: Mapped[float] = mapped_column(Numeric(20, 10), nullable=False)
    mid_price: Mapped[float] = mapped_column(Numeric(20, 10), nullable=False)
    trigger: Mapped[str] = mapped_column(String(20), nullable=False)  # init | fill | rebalance
    recorded_at: Mapped[object] = mapped_column(DateTime(timezone=True), nullable=False)

    __table_args__ = (
        Index("idx_equity_session_time", "session_id", "recorded_at"),
        Index("idx_equity_product_time", "product_id", "recorded_at"),
    )
