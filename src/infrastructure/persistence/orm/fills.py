from __future__ import annotations

from sqlalchemy import DateTime, Integer, Numeric, String, Uuid
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base


class FillRow(Base):
    __tablename__ = "fills"

    fill_id: Mapped[str] = mapped_column(String(80), primary_key=True)
    order_id: Mapped[str | None] = mapped_column(String(100), nullable=True)
    client_order_id: Mapped[str] = mapped_column(String(100), nullable=False, index=True)
    product_id: Mapped[str] = mapped_column(String(20), nullable=False, index=True)
    session_id: Mapped[str | None] = mapped_column(Uuid(as_uuid=False), nullable=True, index=True)
    side: Mapped[str] = mapped_column(String(4), nullable=False)
    price: Mapped[float] = mapped_column(Numeric(20, 10), nullable=False)
    size_base: Mapped[float] = mapped_column(Numeric(20, 10), nullable=False)
    quote_value: Mapped[float] = mapped_column(Numeric(20, 10), nullable=False)
    fee_quote: Mapped[float] = mapped_column(Numeric(20, 10), nullable=False)
    level_index: Mapped[int | None] = mapped_column(Integer, nullable=True)
    grid_side: Mapped[str | None] = mapped_column(String(4), nullable=True)
    liquidity_indicator: Mapped[str] = mapped_column(String(10), nullable=False)
    trade_time: Mapped[object] = mapped_column(DateTime(timezone=True), nullable=False)
