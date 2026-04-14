from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from domain.enums import OrderSide
from domain.models import Fill

from infrastructure.bybit.rest.models import BybitFillDTO


def map_fill(dto: BybitFillDTO) -> Fill:
    if dto.execTime:
        try:
            trade_time = datetime.fromtimestamp(int(dto.execTime) / 1000, tz=UTC)
        except (ValueError, OSError):
            trade_time = datetime.now(UTC)
    else:
        trade_time = datetime.now(UTC)

    price = Decimal(dto.execPrice)
    size = Decimal(dto.execQty)
    quote_value = Decimal(dto.execValue) if dto.execValue else price * size

    return Fill(
        fill_id=dto.execId or "",
        order_id=dto.orderId,
        client_order_id=dto.orderLinkId or "",
        product_id=dto.symbol,
        side=OrderSide.BUY if dto.side.lower() == "buy" else OrderSide.SELL,
        price=price,
        size_base=size,
        quote_value=quote_value,
        fee_quote=Decimal(dto.execFee or "0"),
        liquidity_indicator="M" if dto.isMaker else "T",
        trade_time=trade_time,
    )
