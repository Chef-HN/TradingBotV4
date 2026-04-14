from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from domain.models import ProductMetadata

from infrastructure.coinbase.rest.models import CoinbaseProductDTO


def map_product(dto: CoinbaseProductDTO) -> ProductMetadata:
    return ProductMetadata(
        product_id=dto.product_id,
        base_currency=dto.base_currency_id,
        quote_currency=dto.quote_currency_id,
        status=dto.status,
        price_increment=Decimal(dto.price_increment),
        base_increment=Decimal(dto.base_increment),
        quote_increment=Decimal(dto.quote_increment or dto.price_increment),
        min_market_funds=Decimal(dto.quote_min_size or "0"),
        min_limit_size=Decimal(dto.base_min_size or "0"),
        post_only_supported=bool(dto.post_only),
        cancel_only=bool(dto.cancel_only),
        auction_mode=bool(dto.auction_mode),
        last_refresh_at=datetime.now(UTC),
    )
