from __future__ import annotations

from sqlalchemy import Boolean, DateTime, Integer, Numeric, String, Uuid
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base


class TenantRow(Base):
    __tablename__ = "tenants"

    id: Mapped[str] = mapped_column(Uuid(as_uuid=False), primary_key=True)
    name: Mapped[str] = mapped_column(String(120), nullable=False)
    slug: Mapped[str] = mapped_column(String(80), nullable=False, unique=True)
    tier: Mapped[str] = mapped_column(String(20), nullable=False, default="entry")
    max_capital: Mapped[float] = mapped_column(Numeric(20, 10), nullable=False, default=100)
    max_pairs: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    max_exchanges: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    created_at: Mapped[object] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[object] = mapped_column(DateTime(timezone=True), nullable=False)
