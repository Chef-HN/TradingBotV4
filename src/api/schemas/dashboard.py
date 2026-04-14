from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel


class LevelSchema(BaseModel):
    level_index: int
    side: str
    price: Decimal
    size_base: Decimal
    size_quote: Decimal
    status: str
    fill_price: Decimal | None = None
    opened_at: datetime | None = None
    age_seconds: float | None = None


class SymbolSummary(BaseModel):
    product_id: str
    session_id: str = ""
    regime: str
    regime_confidence: Decimal
    regime_reasons: list[str]
    risk_mode: str
    risk_reasons: list[str]
    mid_price: Decimal
    price_time: datetime
    spread_bps: Decimal
    rsi: Decimal
    mid_anchor: Decimal
    base_inventory: Decimal
    quote_inventory: Decimal
    base_inventory_usd: Decimal
    total_equity: Decimal
    reserve_usd: Decimal = Decimal("0")
    unrealized_pnl: Decimal
    realized_pnl: Decimal
    total_fills: int
    rebalance_count: int
    open_bid_count: int
    open_ask_count: int
    underfunded: bool = False
    underfunded_shortfall_usd: Decimal = Decimal("0")
    bid_levels: list[LevelSchema]
    ask_levels: list[LevelSchema]
    updated_at: datetime


class BotStatus(BaseModel):
    mode: str   # paper | live
    uptime_seconds: float
    total_symbols: int
    total_realized_pnl: Decimal
    total_unrealized_pnl: Decimal
    total_fills: int
    symbols: list[SymbolSummary]
    started_at: datetime
    updated_at: datetime
    next_daily_close_at: datetime | None = None
    skip_daily_close: bool = False
    worker_alive: bool = False
