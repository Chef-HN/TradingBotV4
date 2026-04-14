from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from pydantic import Field

from domain.enums import RiskMode

from .base import DomainModel


class RiskState(DomainModel):
    product_id: str
    risk_mode: RiskMode
    freeze_new_bids: bool = False
    freeze_new_asks: bool = False
    flatten_required: bool = False
    shutdown_lock_triggered: bool = False
    shutdown_lock_at: datetime | None = None
    current_unrealized_loss: Decimal = Decimal("0")
    current_realized_pnl: Decimal = Decimal("0")
    reason_codes: list[str] = Field(default_factory=list)
    updated_at: datetime
