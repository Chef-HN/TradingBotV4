"""ORM model for encrypted exchange credentials."""

from __future__ import annotations

from sqlalchemy import DateTime, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base


class ExchangeCredentialsRow(Base):
    """Store encrypted API credentials for exchanges."""

    __tablename__ = "exchange_credentials"

    id: Mapped[str] = mapped_column(String(36), primary_key=True)  # UUID as string
    exchange_name: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    api_key_encrypted: Mapped[str] = mapped_column(Text, nullable=False)
    api_secret_encrypted: Mapped[str] = mapped_column(Text, nullable=False)
    api_passphrase_encrypted: Mapped[str | None] = mapped_column(Text, nullable=True)  # For some exchanges
    encryption_key_id: Mapped[str] = mapped_column(String(100), nullable=False)  # For key rotation
    created_at: Mapped[object] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[object] = mapped_column(DateTime(timezone=True), nullable=False)
    created_by: Mapped[str] = mapped_column(String(100), nullable=True)  # Username or "system"
    active: Mapped[bool] = mapped_column(default=True, nullable=False)  # Allow disabling without deletion
