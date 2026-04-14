from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from domain.enums import OrderSide, OrderStatus, OrderType, TimeInForce
from domain.models import VenueOrder

from infrastructure.bybit.rest.models import BybitOrderDTO


_STATUS_MAP = {
    "New": OrderStatus.OPEN,
    "PartiallyFilled": OrderStatus.OPEN,
    "Filled": OrderStatus.FILLED,
    "Cancelled": OrderStatus.CANCELLED,
    "Rejected": OrderStatus.REJECTED,
    "Untriggered": OrderStatus.OPEN,
}


def map_order(dto: BybitOrderDTO) -> VenueOrder:
    status = _STATUS_MAP.get(dto.orderStatus or "New", OrderStatus.OPEN)
    return VenueOrder(
        order_id=dto.orderId,
        client_order_id=dto.orderLinkId or "",
        product_id=dto.symbol,
        side=OrderSide.BUY if dto.side.lower() == "buy" else OrderSide.SELL,
        order_type=OrderType.LIMIT,
        tif=TimeInForce.GTC,
        status=status,
        requested_price=Decimal(dto.price) if dto.price else None,
        requested_size_base=Decimal(dto.qty) if dto.qty else None,
        requested_size_quote=None,
        filled_size_base=Decimal(dto.cumExecQty or "0"),
        filled_value_quote=Decimal(dto.cumExecValue or "0"),
        fees_quote=Decimal(dto.cumExecFee or "0"),
        post_only=True,
        venue_payload=dto.model_dump(),
        created_at=datetime.now(UTC),
        updated_at=datetime.now(UTC),
    )
