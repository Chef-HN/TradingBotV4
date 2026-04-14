from __future__ import annotations

from decimal import Decimal

from pydantic import BaseModel, ConfigDict


class CoinbaseAccountDTO(BaseModel):
    model_config = ConfigDict(extra="ignore")

    uuid: str
    currency: str
    available_balance: dict
    hold: dict

    @property
    def free_amount(self) -> Decimal:
        return Decimal(str(self.available_balance.get("value", "0")))

    @property
    def held_amount(self) -> Decimal:
        return Decimal(str(self.hold.get("value", "0")))


class CoinbaseProductDTO(BaseModel):
    model_config = ConfigDict(extra="ignore")

    product_id: str
    base_currency_id: str
    quote_currency_id: str
    status: str
    price_increment: str
    base_increment: str
    quote_increment: str | None = None
    base_min_size: str | None = None
    quote_min_size: str | None = None
    cancel_only: bool | None = False
    auction_mode: bool | None = False
    post_only: bool | None = True


class CoinbaseOrderDTO(BaseModel):
    model_config = ConfigDict(extra="ignore")

    order_id: str | None = None
    client_order_id: str | None = None
    product_id: str
    side: str
    status: str | None = None
    order_type: str | None = None
    time_in_force: str | None = None
    limit_price: str | None = None
    filled_size: str | None = None
    filled_value: str | None = None
    total_fees: str | None = None


class CoinbaseFillDTO(BaseModel):
    model_config = ConfigDict(extra="ignore")

    entry_id: str | None = None
    trade_id: str | None = None
    order_id: str | None = None
    client_order_id: str | None = None
    product_id: str
    side: str
    price: str
    size: str
    commission: str | None = "0"
    liquidity_indicator: str | None = "UNKNOWN_LIQUIDITY"
    trade_time: str
