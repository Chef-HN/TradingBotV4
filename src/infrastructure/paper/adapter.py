from __future__ import annotations

from collections import OrderedDict
from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

from domain.enums import OrderSide, OrderStatus, OrderType
from domain.models import Fill, MarketSnapshot, OrderIntent, VenueOrder


class PaperTradingAdapter:
    """Simulates order execution against live market data ticks."""

    def __init__(self, fee_rate: Decimal = Decimal("0.0010")) -> None:
        self.fee_rate = fee_rate
        self.open_orders: OrderedDict[str, VenueOrder] = OrderedDict()
        self.fills: list[Fill] = []
        # Tracks the mid price when each order was placed (or last seen before fill).
        # Used to enforce price-crossing: a buy only fills when mid crosses DOWN through
        # the order price (not if mid was already below when the order was placed).
        self._last_mid: dict[str, Decimal] = {}

    async def place_order(
        self, intent: OrderIntent, current_mid: Decimal | None = None
    ) -> VenueOrder:
        client_order_id = f"paper-{intent.intent_id.replace('-', '')[:20]}"
        order = VenueOrder(
            order_id=client_order_id,
            client_order_id=client_order_id,
            product_id=intent.product_id,
            side=intent.side,
            order_type=OrderType.LIMIT if intent.price is not None else OrderType.MARKET,
            tif=intent.tif,
            status=OrderStatus.OPEN,
            requested_price=intent.price,
            requested_size_base=intent.size_base,
            requested_size_quote=intent.size_quote,
            level_index=intent.level_index,
            grid_side=intent.grid_side,
            post_only=intent.post_only,
            created_at=datetime.now(UTC),
            updated_at=datetime.now(UTC),
        )
        self.open_orders[client_order_id] = order
        if current_mid is not None:
            self._last_mid[client_order_id] = current_mid
        return order

    async def cancel_order(self, client_order_id: str) -> None:
        if client_order_id in self.open_orders:
            order = self.open_orders[client_order_id].model_copy(
                update={"status": OrderStatus.CANCELLED, "updated_at": datetime.now(UTC)}
            )
            self.open_orders[client_order_id] = order
        self._last_mid.pop(client_order_id, None)

    async def on_market_snapshot(self, snapshot: MarketSnapshot) -> list[Fill]:
        """Check all open orders against current market prices; return fills.

        Fill condition uses price-crossing logic: a limit buy fills only when the
        mid crosses DOWN through the order price (prev_mid > price >= mid).
        A limit sell fills only when mid crosses UP through the order price.

        This prevents the fill storm where a replenished order at the same price
        fires again on the very next tick while mid is still at the level.
        """
        generated: list[Fill] = []
        mid = snapshot.mid
        for client_order_id, order in list(self.open_orders.items()):
            if order.status != OrderStatus.OPEN:
                continue
            prev_mid = self._last_mid.get(client_order_id)
            if (
                order.side == OrderSide.BUY
                and order.requested_price is not None
                and mid <= order.requested_price
                and (prev_mid is None or prev_mid > order.requested_price)
            ):
                generated.append(self._fill_order(order, order.requested_price))
                self._last_mid.pop(client_order_id, None)
            elif (
                order.side == OrderSide.SELL
                and order.requested_price is not None
                and mid >= order.requested_price
                and (prev_mid is None or prev_mid < order.requested_price)
            ):
                generated.append(self._fill_order(order, order.requested_price))
                self._last_mid.pop(client_order_id, None)
            else:
                self._last_mid[client_order_id] = mid
        return generated

    def _fill_order(self, order: VenueOrder, fill_price: Decimal) -> Fill:
        size_base = order.requested_size_base
        if size_base is None and order.requested_size_quote is not None and fill_price != 0:
            size_base = (order.requested_size_quote / fill_price).quantize(Decimal("0.00000001"))
        size_base = size_base or Decimal("0")
        quote_value = (fill_price * size_base).quantize(Decimal("0.00000001"))
        fee_quote = (quote_value * self.fee_rate).quantize(Decimal("0.00000001"))
        fill = Fill(
            fill_id=f"fill-{uuid4().hex[:16]}",
            order_id=order.order_id,
            client_order_id=order.client_order_id,
            product_id=order.product_id,
            side=order.side,
            price=fill_price,
            size_base=size_base,
            quote_value=quote_value,
            fee_quote=fee_quote,
            liquidity_indicator="M",
            trade_time=datetime.now(UTC),
            level_index=order.level_index,
            grid_side=order.grid_side,
        )
        self.fills.append(fill)
        self.open_orders[order.client_order_id] = order.model_copy(
            update={
                "status": OrderStatus.FILLED,
                "filled_size_base": size_base,
                "filled_value_quote": quote_value,
                "fees_quote": fee_quote,
                "updated_at": fill.trade_time,
            }
        )
        return fill
