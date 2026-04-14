from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from pydantic import Field

from domain.enums import RegimeName

from .base import DomainModel


class RegimeState(DomainModel):
    product_id: str
    regime: RegimeName
    confidence: Decimal
    ema_slope: Decimal
    vwap_distance_bps: Decimal
    spread_zscore: Decimal
    order_book_imbalance: Decimal
    flow_bias: Decimal
    hysteresis_anchor: Decimal
    updated_at: datetime
    reason_codes: list[str] = Field(default_factory=list)
