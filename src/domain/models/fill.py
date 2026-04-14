from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from domain.enums import OrderSide

from .base import DomainModel


class Fill(DomainModel):
    fill_id: str
    order_id: str | None
    client_order_id: str
    product_id: str
    side: OrderSide
    price: Decimal
    size_base: Decimal
    quote_value: Decimal
    fee_quote: Decimal
    liquidity_indicator: str
    trade_time: datetime
    level_index: int | None = None
    grid_side: str | None = None
