from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from domain.enums import OrderSide, OrderStatus, OrderType, TimeInForce

from .base import DomainModel


class OrderIntent(DomainModel):
    intent_id: str
    correlation_id: str
    product_id: str
    side: OrderSide
    intent_type: str
    tif: TimeInForce
    price: Decimal | None = None
    size_base: Decimal | None = None
    size_quote: Decimal | None = None
    post_only: bool = True
    level_index: int | None = None      # which grid level this intent belongs to
    grid_side: str | None = None        # "bid" or "ask"
    strategy_reason: str
    regime_at_decision: str
    created_at: datetime


class VenueOrder(DomainModel):
    order_id: str | None = None
    client_order_id: str
    product_id: str
    side: OrderSide
    order_type: OrderType
    tif: TimeInForce
    status: OrderStatus
    requested_price: Decimal | None = None
    requested_size_base: Decimal | None = None
    requested_size_quote: Decimal | None = None
    level_index: int | None = None
    grid_side: str | None = None
    filled_size_base: Decimal = Decimal("0")
    filled_value_quote: Decimal = Decimal("0")
    fees_quote: Decimal = Decimal("0")
    post_only: bool = True
    reject_reason: str | None = None
    venue_payload: dict | None = None
    created_at: datetime
    updated_at: datetime
