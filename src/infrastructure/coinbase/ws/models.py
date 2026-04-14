from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from pydantic import BaseModel, ConfigDict

from domain.models import OrderBookSnapshot


class CoinbaseWSMessage(BaseModel):
    model_config = ConfigDict(extra="ignore")

    channel: str | None = None
    timestamp: str | None = None
    sequence_num: int | None = None
    events: list[dict] = []


def parse_timestamp(raw_value: str | None) -> datetime:
    if not raw_value:
        return datetime.now(UTC)
    return datetime.fromisoformat(raw_value.replace("Z", "+00:00"))


def build_orderbook_snapshot(
    product_id: str,
    bid_price: Decimal,
    bid_size: Decimal,
    ask_price: Decimal,
    ask_size: Decimal,
    sequence: int,
    event_time: datetime,
) -> OrderBookSnapshot:
    spread_abs = ask_price - bid_price
    mid = (ask_price + bid_price) / Decimal("2")
    spread_bps = Decimal("0") if mid == 0 else (spread_abs / mid) * Decimal("10000")
    imbalance = Decimal("0") if bid_size + ask_size == 0 else bid_size / (bid_size + ask_size)
    return OrderBookSnapshot(
        product_id=product_id,
        best_bid_price=bid_price,
        best_bid_size=bid_size,
        best_ask_price=ask_price,
        best_ask_size=ask_size,
        imbalance=imbalance,
        spread_abs=spread_abs,
        spread_bps=spread_bps,
        sequence=sequence,
        event_time=event_time,
    )
