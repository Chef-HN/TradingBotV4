from __future__ import annotations

from sqlalchemy import BigInteger, DateTime, Index, Numeric, String
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base


class TickRow(Base):
    __tablename__ = "ticks"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    product_id: Mapped[str] = mapped_column(String(20), nullable=False)
    bid: Mapped[float] = mapped_column(Numeric(20, 10), nullable=False)
    ask: Mapped[float] = mapped_column(Numeric(20, 10), nullable=False)
    mid: Mapped[float] = mapped_column(Numeric(20, 10), nullable=False)
    last_trade_price: Mapped[float] = mapped_column(Numeric(20, 10), nullable=False)
    event_time: Mapped[object] = mapped_column(DateTime(timezone=True), nullable=False)

    __table_args__ = (
        Index("idx_ticks_product_time", "product_id", "event_time"),
    )
