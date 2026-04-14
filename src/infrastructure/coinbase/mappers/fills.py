from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from domain.enums import OrderSide
from domain.models import Fill

from infrastructure.coinbase.rest.models import CoinbaseFillDTO


def map_fill(dto: CoinbaseFillDTO) -> Fill:
    trade_time = (
        datetime.fromisoformat(dto.trade_time.replace("Z", "+00:00"))
        if dto.trade_time
        else datetime.now(UTC)
    )
    return Fill(
        fill_id=dto.trade_id or dto.entry_id or "",
        order_id=dto.order_id,
        client_order_id=dto.client_order_id or "",
        product_id=dto.product_id,
        side=OrderSide(dto.side.upper()),
        price=Decimal(dto.price),
        size_base=Decimal(dto.size),
        quote_value=Decimal(dto.price) * Decimal(dto.size),
        fee_quote=Decimal(dto.commission or "0"),
        liquidity_indicator=dto.liquidity_indicator or "UNKNOWN_LIQUIDITY",
        trade_time=trade_time,
    )
