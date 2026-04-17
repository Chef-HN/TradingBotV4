"""Domain model for exchange credentials."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass
class ExchangeCredentials:
    """Represents encrypted exchange credentials."""

    id: str
    tenant_id: str
    exchange_name: str
    api_key: str  # Decrypted in memory only
    api_secret: str  # Decrypted in memory only
    api_passphrase: str | None
    encryption_key_id: str
    created_at: datetime
    updated_at: datetime
    created_by: str | None
    active: bool
