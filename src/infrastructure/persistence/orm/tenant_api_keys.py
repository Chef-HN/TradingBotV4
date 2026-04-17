from __future__ import annotations

from sqlalchemy import Boolean, DateTime, String, Uuid
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base


class TenantApiKeyRow(Base):
    __tablename__ = "tenant_api_keys"

    id: Mapped[str] = mapped_column(Uuid(as_uuid=False), primary_key=True)
    tenant_id: Mapped[str] = mapped_column(Uuid(as_uuid=False), nullable=False, index=True)
    key_hash: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)
    key_prefix: Mapped[str] = mapped_column(String(20), nullable=False)
    key_last4: Mapped[str] = mapped_column(String(8), nullable=False)
    label: Mapped[str | None] = mapped_column(String(120), nullable=True)
    created_by: Mapped[str | None] = mapped_column(String(120), nullable=True)
    created_at: Mapped[object] = mapped_column(DateTime(timezone=True), nullable=False)
    expires_at: Mapped[object | None] = mapped_column(DateTime(timezone=True), nullable=True)
    revoked_at: Mapped[object | None] = mapped_column(DateTime(timezone=True), nullable=True)
    active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
