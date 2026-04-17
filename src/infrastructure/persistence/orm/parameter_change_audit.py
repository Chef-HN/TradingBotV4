from __future__ import annotations

from sqlalchemy import BigInteger, Boolean, DateTime, JSON, String, Text, Uuid
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base


class ParameterChangeAuditRow(Base):
    __tablename__ = "parameter_change_audit"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    tenant_id: Mapped[str] = mapped_column(Uuid(as_uuid=False), nullable=False, index=True)
    exchange_name: Mapped[str] = mapped_column(String(20), nullable=False)
    product_id: Mapped[str] = mapped_column(String(20), nullable=False)
    change_type: Mapped[str] = mapped_column(String(20), nullable=False)
    proposed_by: Mapped[str] = mapped_column(String(120), nullable=False)
    reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    change_payload: Mapped[dict] = mapped_column(JSON, nullable=False)
    change_diff: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    approved: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    created_at: Mapped[object] = mapped_column(DateTime(timezone=True), nullable=False)
