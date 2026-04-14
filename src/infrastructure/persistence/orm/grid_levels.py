from __future__ import annotations

from sqlalchemy import Boolean, DateTime, Integer, Numeric, String, Uuid
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base


class GridLevelRow(Base):
    __tablename__ = "grid_levels"

    level_id: Mapped[str] = mapped_column(Uuid(as_uuid=False), primary_key=True)
    product_id: Mapped[str] = mapped_column(String(20), nullable=False, index=True)
    session_id: Mapped[str] = mapped_column(Uuid(as_uuid=False), nullable=False, index=True)
    side: Mapped[str] = mapped_column(String(4), nullable=False)
    level_index: Mapped[int] = mapped_column(Integer, nullable=False)
    price: Mapped[float] = mapped_column(Numeric(20, 10), nullable=False)
    size_base: Mapped[float] = mapped_column(Numeric(20, 10), nullable=False)
    size_quote: Mapped[float] = mapped_column(Numeric(20, 10), nullable=False)
    client_order_id: Mapped[str | None] = mapped_column(String(100), nullable=True)
    order_id: Mapped[str | None] = mapped_column(String(100), nullable=True)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="pending")
    fill_price: Mapped[float | None] = mapped_column(Numeric(20, 10), nullable=True)
    fill_fee_quote: Mapped[float | None] = mapped_column(Numeric(20, 10), nullable=True)
    created_at: Mapped[object] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[object] = mapped_column(DateTime(timezone=True), nullable=False)
    opened_at: Mapped[object | None] = mapped_column(DateTime(timezone=True), nullable=True)
    filled_at: Mapped[object | None] = mapped_column(DateTime(timezone=True), nullable=True)
