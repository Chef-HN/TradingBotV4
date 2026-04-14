from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from .base import DomainModel


class OrderBookSnapshot(DomainModel):
    product_id: str
    best_bid_price: Decimal
    best_bid_size: Decimal
    best_ask_price: Decimal
    best_ask_size: Decimal
    imbalance: Decimal
    spread_abs: Decimal
    spread_bps: Decimal
    sequence: int
    event_time: datetime


class MarketSnapshot(DomainModel):
    product_id: str
    bid: Decimal
    ask: Decimal
    mid: Decimal
    microprice: Decimal
    short_vwap: Decimal
    short_ema: Decimal
    long_ema: Decimal = Decimal("0")
    rsi: Decimal = Decimal("50")
    realized_volatility: Decimal
    spread_abs: Decimal
    spread_bps: Decimal
    spread_zscore: Decimal
    flow_bias: Decimal
    top_book_imbalance: Decimal
    last_trade_price: Decimal
    last_trade_size: Decimal
    event_time: datetime
    source_latency_ms: int = 0
