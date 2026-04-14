from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from domain.enums import OrderSide, OrderStatus, OrderType, TimeInForce
from domain.models import VenueOrder

from infrastructure.coinbase.rest.models import CoinbaseOrderDTO


def map_order(dto: CoinbaseOrderDTO) -> VenueOrder:
    status_text = (dto.status or "OPEN").upper()
    tif_text = (dto.time_in_force or "GTC").upper()
    order_type_text = (dto.order_type or "LIMIT").upper()
    if status_text not in OrderStatus.__members__:
        status_text = "OPEN"
    return VenueOrder(
        order_id=dto.order_id,
        client_order_id=dto.client_order_id or "",
        product_id=dto.product_id,
        side=OrderSide(dto.side.upper()),
        order_type=OrderType.LIMIT if order_type_text.startswith("LIMIT") else OrderType.MARKET,
        tif=TimeInForce[tif_text] if tif_text in TimeInForce.__members__ else TimeInForce.GTC,
        status=OrderStatus[status_text],
        requested_price=Decimal(dto.limit_price) if dto.limit_price else None,
        requested_size_base=None,
        requested_size_quote=None,
        filled_size_base=Decimal(dto.filled_size or "0"),
        filled_value_quote=Decimal(dto.filled_value or "0"),
        fees_quote=Decimal(dto.total_fees or "0"),
        post_only=True,
        venue_payload=dto.model_dump(),
        created_at=datetime.now(UTC),
        updated_at=datetime.now(UTC),
    )
