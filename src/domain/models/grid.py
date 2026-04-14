from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from uuid import UUID, uuid4

from pydantic import Field

from domain.enums import OrderSide

from .base import DomainModel


class GridLevel(DomainModel):
    """A single price level in the neutral grid."""

    level_id: UUID = Field(default_factory=uuid4)
    product_id: str
    session_id: UUID
    side: OrderSide
    level_index: int           # 0 = closest to mid, N-1 = furthest
    price: Decimal
    size_base: Decimal
    size_quote: Decimal
    client_order_id: str | None = None
    order_id: str | None = None
    status: str = "pending"    # pending | open | filled | cancelled
    fill_price: Decimal | None = None
    fill_fee_quote: Decimal | None = None
    is_flip: bool = False          # True if this level was created by flip logic (not grid build)
    created_at: datetime
    updated_at: datetime
    opened_at: datetime | None = None
    filled_at: datetime | None = None


class GridState(DomainModel):
    """Snapshot of the full neutral grid for one symbol at one point in time."""

    product_id: str
    session_id: UUID
    mid_anchor: Decimal           # price the grid was centered on
    spacing_bps: Decimal
    bid_levels: list[GridLevel] = Field(default_factory=list)
    ask_levels: list[GridLevel] = Field(default_factory=list)
    base_inventory: Decimal       # base currency held (e.g. HBAR)
    quote_inventory: Decimal      # quote currency available (USD)
    base_inventory_cost: Decimal  # total USD cost of current base inventory
    realized_pnl_quote: Decimal = Decimal("0")
    total_fills: int = 0
    rebalance_count: int = 0
    last_fill_at: datetime | None = None
    updated_at: datetime

    @property
    def open_bid_levels(self) -> list[GridLevel]:
        return [lvl for lvl in self.bid_levels if lvl.status == "open"]

    @property
    def open_ask_levels(self) -> list[GridLevel]:
        return [lvl for lvl in self.ask_levels if lvl.status == "open"]

    @property
    def total_open_levels(self) -> int:
        return len(self.open_bid_levels) + len(self.open_ask_levels)

    @property
    def inventory_value_quote(self) -> Decimal:
        """Rough mark-to-market of base inventory at mid anchor."""
        return self.base_inventory * self.mid_anchor

    @property
    def total_equity(self) -> Decimal:
        return self.quote_inventory + self.inventory_value_quote

    @property
    def unrealized_pnl_quote(self) -> Decimal:
        return self.inventory_value_quote - self.base_inventory_cost
