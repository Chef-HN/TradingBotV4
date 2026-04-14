from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID, uuid4

logger = logging.getLogger(__name__)

from config import StrategySettings
from domain.enums import OrderSide, TimeInForce
from domain.models import Fill, GridLevel, GridState, MarketSnapshot, OrderIntent

from .inventory_skew import apply_skew_to_size, compute_inventory_skew


@dataclass
class GridAction:
    """A single action the engine wants to take."""

    action_type: str  # "place" | "cancel" | "cancel_and_replace"
    level: GridLevel
    new_intent: OrderIntent | None = None


@dataclass
class GridDecision:
    """Full output of one NeutralGridEngine evaluation cycle."""

    actions: list[GridAction] = field(default_factory=list)
    updated_state: GridState | None = None
    rebalanced: bool = False
    reason: str = ""


class NeutralGridEngine:
    """
    Neutral (bidirectional) grid market-making engine.

    Responsibilities:
    - Build the initial grid centered on current mid price
    - On fill: replenish the filled level (place a new order at the same level)
    - On price drift: detect when mid has moved beyond rebalance_threshold_bps
      from the current anchor and rebuild the entire grid
    - On stale orders: detect and reprice orders that are too far from target
    - Apply inventory skew to level sizes
    """

    def __init__(self, settings: StrategySettings) -> None:
        self.settings = settings

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def build_initial_grid(
        self,
        *,
        product_id: str,
        session_id: UUID,
        mid: Decimal,
        base_inventory: Decimal,
        quote_inventory: Decimal,
        base_inventory_cost: Decimal,
        tick_size: Decimal = Decimal("0.0001"),
        prior_realized_pnl: Decimal = Decimal("0"),
        prior_total_fills: int = 0,
    ) -> GridState:
        """Create a brand-new grid centered on `mid`."""
        bid_levels = self._build_levels(
            product_id=product_id,
            session_id=session_id,
            side=OrderSide.BUY,
            anchor=mid,
            tick_size=tick_size,
            base_inventory=base_inventory,
            quote_inventory=quote_inventory,
            base_inventory_cost=base_inventory_cost,
        )
        ask_levels = self._build_levels(
            product_id=product_id,
            session_id=session_id,
            side=OrderSide.SELL,
            anchor=mid,
            tick_size=tick_size,
            base_inventory=base_inventory,
            quote_inventory=quote_inventory,
            base_inventory_cost=base_inventory_cost,
        )
        now = datetime.now(UTC)
        return GridState(
            product_id=product_id,
            session_id=session_id,
            mid_anchor=mid,
            spacing_bps=self.settings.spacing_bps,
            bid_levels=bid_levels,
            ask_levels=ask_levels,
            base_inventory=base_inventory,
            quote_inventory=quote_inventory,
            base_inventory_cost=base_inventory_cost,
            realized_pnl_quote=prior_realized_pnl,
            total_fills=prior_total_fills,
            rebalance_count=0,
            updated_at=now,
        )

    def evaluate(
        self,
        *,
        state: GridState,
        market: MarketSnapshot,
        regime: str,
        now: datetime | None = None,
    ) -> GridDecision:
        """
        Main evaluation loop: produces actions needed to maintain the grid.
        Called on every market tick.
        """
        now = now or datetime.now(UTC)
        decision = GridDecision()

        # Check if grid needs full rebalance
        if self._needs_rebalance(state, market.mid, now=now):
            return self._rebalance(state, market, now=now)

        # Check for stale open orders that need repricing
        stale_actions = self._find_stale_orders(state, market, now=now)
        decision.actions.extend(stale_actions)

        # Fix Q5: build an effective state that excludes levels already queued for
        # cancellation in stale_actions, so replenishment guards see the true
        # post-cancellation book and are not blocked by phantom active orders.
        stale_cancelled_ids = {
            a.level.client_order_id
            for a in stale_actions
            if a.action_type == "cancel" and a.level.client_order_id
        }
        effective_state = (
            state.model_copy(update={
                "ask_levels": [
                    lvl for lvl in state.ask_levels
                    if lvl.client_order_id not in stale_cancelled_ids
                ],
                "bid_levels": [
                    lvl for lvl in state.bid_levels
                    if lvl.client_order_id not in stale_cancelled_ids
                ],
            })
            if stale_cancelled_ids else state
        )

        # Bid-replenishment: restore one bid when the bid side is fully depleted
        replenishment = self._try_replenish_bid(effective_state, market, now=now)
        if replenishment is not None:
            action, new_state = replenishment
            decision.actions.append(action)
            decision.updated_state = new_state

        # Ask-replenishment: restore one ask when the ask side is fully depleted
        # Uses updated state from bid-replenishment if it fired, already reflecting
        # the stale cancellations via effective_state.
        eval_state = decision.updated_state if decision.updated_state is not None else effective_state
        ask_replenishment = self._try_replenish_ask(eval_state, market, now=now)
        if ask_replenishment is not None:
            action, new_state = ask_replenishment
            decision.actions.append(action)
            decision.updated_state = new_state

        return decision

    def _try_replenish_bid(
        self,
        state: GridState,
        market: MarketSnapshot,
        now: datetime,
    ) -> tuple[GridAction, GridState] | None:
        """Place one bid at current_mid - 1×spacing when all bids are gone.

        Fires only when:
        - No active or pending bids exist
        - Enough quote balance to fund at least one level
        - Price drift is still within the rebalance threshold
        - BTC inventory ratio is below the long guard threshold (0.65)
        - The new bid price would not cross any open ask
        """
        # Gap 3: check both open AND pending bids
        active_bids = [lvl for lvl in state.bid_levels if lvl.status in ("open", "pending")]
        if active_bids:
            return None

        if state.quote_inventory < self.settings.level_size_quote:
            return None

        # Only replenish inside the rebalance window
        drift_bps = abs(market.mid - state.mid_anchor) / state.mid_anchor * Decimal("10000")
        if drift_bps >= self.settings.rebalance_threshold_bps:
            return None

        # Inventory guard — block replenishment above 65% long (aligned with _rebalance guard)
        inventory_value = state.base_inventory * market.mid
        total_equity = state.quote_inventory + inventory_value
        if total_equity > 0:
            long_ratio = inventory_value / total_equity
            if long_ratio > Decimal("0.65"):
                return None

        # Compute new bid price: current_mid - 1×spacing
        tick_size = self._infer_tick_size(market.bid)
        spread = market.mid * self.settings.spacing_bps / Decimal("10000")
        new_bid_price = (
            (market.mid - spread) / tick_size
        ).quantize(Decimal("1")) * tick_size

        # Gap 2: crossed-book check
        open_asks = [lvl for lvl in state.ask_levels if lvl.status in ("open", "pending")]
        if open_asks and new_bid_price >= min(lvl.price for lvl in open_asks):
            return None

        # Size with inventory skew
        eq = total_equity if total_equity > 0 else self.settings.level_size_quote
        skew = compute_inventory_skew(
            state.base_inventory, market.mid, eq, self.settings.max_inventory_ratio
        )
        size_quote = apply_skew_to_size(self.settings.level_size_quote, skew, "bid")
        size_base = (
            (size_quote / new_bid_price).quantize(Decimal("0.00000001"))
            if new_bid_price > 0 else Decimal("0")
        )

        new_bid = GridLevel(
            level_id=uuid4(),
            product_id=state.product_id,
            session_id=state.session_id,
            side=OrderSide.BUY,
            level_index=0,
            price=new_bid_price,
            size_base=size_base,
            size_quote=size_quote,
            is_flip=True,
            status="pending",
            created_at=now,
            updated_at=now,
        )

        new_state = state.model_copy(
            update={"bid_levels": list(state.bid_levels) + [new_bid]}
        )
        return GridAction(action_type="place", level=new_bid), new_state

    def _try_replenish_ask(
        self,
        state: GridState,
        market: MarketSnapshot,
        now: datetime,
    ) -> tuple[GridAction, GridState] | None:
        """Place one ask at current_mid + 1×spacing when all asks are gone.

        Fires only when:
        - No active or pending asks exist
        - Enough base inventory to fund at least one level
        - Price drift is still within the rebalance threshold
        - Base inventory ratio exceeds 50% (bot is actually long, has something to sell)
        - The new ask price would not cross any open bid
        """
        active_asks = [lvl for lvl in state.ask_levels if lvl.status in ("open", "pending")]
        if active_asks:
            return None

        # Need base inventory to cover the ask size
        if market.mid <= 0:
            return None
        min_base = self.settings.level_size_quote / market.mid
        if state.base_inventory < min_base:
            return None

        # Only replenish inside the rebalance window
        drift_bps = abs(market.mid - state.mid_anchor) / state.mid_anchor * Decimal("10000")
        if drift_bps >= self.settings.rebalance_threshold_bps:
            return None

        # Only replenish when meaningfully long (avoids churn near balanced 50/50 state)
        inventory_value = state.base_inventory * market.mid
        total_equity = state.quote_inventory + inventory_value
        if total_equity > 0:
            long_ratio = inventory_value / total_equity
            if long_ratio <= Decimal("0.55"):
                return None

        # Compute new ask price: current_mid + 1×spacing
        tick_size = self._infer_tick_size(market.ask)
        spread = market.mid * self.settings.spacing_bps / Decimal("10000")
        new_ask_price = (
            (market.mid + spread) / tick_size
        ).quantize(Decimal("1")) * tick_size

        # Crossed-book check
        open_bids = [lvl for lvl in state.bid_levels if lvl.status in ("open", "pending")]
        if open_bids and new_ask_price <= max(lvl.price for lvl in open_bids):
            return None

        # Size with inventory skew
        eq = total_equity if total_equity > 0 else self.settings.level_size_quote
        skew = compute_inventory_skew(
            state.base_inventory, market.mid, eq, self.settings.max_inventory_ratio
        )
        size_quote = apply_skew_to_size(self.settings.level_size_quote, skew, "ask")
        size_base = (
            (size_quote / new_ask_price).quantize(Decimal("0.00000001"))
            if new_ask_price > 0 else Decimal("0")
        )

        new_ask = GridLevel(
            level_id=uuid4(),
            product_id=state.product_id,
            session_id=state.session_id,
            side=OrderSide.SELL,
            level_index=0,
            price=new_ask_price,
            size_base=size_base,
            size_quote=size_quote,
            is_flip=True,
            status="pending",
            created_at=now,
            updated_at=now,
        )

        new_state = state.model_copy(
            update={"ask_levels": list(state.ask_levels) + [new_ask]}
        )
        return GridAction(action_type="place", level=new_ask), new_state

    def apply_fill(
        self,
        *,
        state: GridState,
        fill: Fill,
        market: MarketSnapshot,
        now: datetime | None = None,
    ) -> tuple[GridState, GridAction | None]:
        """
        Process a fill: mark the level as filled, update inventory,
        compute realized PnL (if sell), and return a replenish action.

        The replenish level is embedded directly into the returned state as
        "pending" — this ensures GridState always tracks every open order and
        prevents inventory divergence from ghost fills.
        """
        now = now or datetime.now(UTC)

        # Find the level this fill corresponds to
        filled_level = self._find_level_by_order(state, fill.client_order_id)
        if filled_level is None:
            return state, None

        # Update inventories
        if fill.side == OrderSide.BUY:
            new_base = state.base_inventory + fill.size_base
            new_quote = state.quote_inventory - fill.quote_value - fill.fee_quote
            new_cost = state.base_inventory_cost + fill.quote_value + fill.fee_quote
            realized_delta = Decimal("0")
        else:
            # Guard: cannot sell what we don't have
            if state.base_inventory <= Decimal("0"):
                return state, None
            # Sell: realize PnL = (sell_price - avg_cost) * size_base - fees
            # Guard: fraction_sold must not exceed 1 to avoid cost basis going negative
            if state.base_inventory > 0:
                avg_cost = state.base_inventory_cost / state.base_inventory
                fraction_sold = min(fill.size_base / state.base_inventory, Decimal("1"))
                new_cost = state.base_inventory_cost * (Decimal("1") - fraction_sold)
            else:
                avg_cost = Decimal("0")
                new_cost = Decimal("0")
            realized_delta = (fill.price - avg_cost) * fill.size_base - fill.fee_quote
            new_base = max(Decimal("0"), state.base_inventory - fill.size_base)
            new_quote = state.quote_inventory + fill.quote_value - fill.fee_quote

        # Flip logic: a filled BID becomes an ASK at the mirror position above anchor;
        # a filled ASK becomes a BID at the mirror position below anchor.
        # If the counter-side already has a live (open/pending) order at that level_index,
        # no new order is needed — remove the filled slot and keep the existing counter-order.
        flip_level = self._build_flip_level(
            filled_level=filled_level,
            base_inventory=new_base,
            quote_inventory=new_quote,
            market=market,
            mid_anchor=state.mid_anchor,
            now=now,
        )

        # Inventory hard cap: suppress flip when position is already extreme.
        # Derived from max_inventory_ratio to stay consistent if that setting changes.
        hard_cap_long = self.settings.max_inventory_ratio + Decimal("0.10")
        hard_cap_short = Decimal("1") - hard_cap_long
        inventory_capped = False
        inv_value = new_base * market.mid
        total_eq = new_quote + inv_value
        if total_eq > 0:
            long_ratio = inv_value / total_eq
            flip_side = OrderSide.SELL if fill.side == OrderSide.BUY else OrderSide.BUY
            if flip_side == OrderSide.BUY and long_ratio > hard_cap_long:
                inventory_capped = True
                logger.info(
                    "Inventory cap: suppressing flip BID (long_ratio=%.2f > %.2f)",
                    long_ratio, hard_cap_long,
                )
            elif flip_side == OrderSide.SELL and long_ratio < hard_cap_short:
                inventory_capped = True
                logger.info(
                    "Inventory cap: suppressing flip ASK (long_ratio=%.2f < %.2f)",
                    long_ratio, hard_cap_short,
                )

        if fill.side == OrderSide.BUY:
            # Remove filled bid; check if an ask at this level_index already exists
            updated_bid_levels = [
                lvl for lvl in state.bid_levels
                if lvl.client_order_id != fill.client_order_id
            ]
            counter_exists = inventory_capped or any(
                lvl.level_index == filled_level.level_index and lvl.status in ("open", "pending")
                for lvl in state.ask_levels
            )
            updated_ask_levels = (
                state.ask_levels if counter_exists
                else list(state.ask_levels) + [flip_level]
            )
        else:
            # Remove filled ask; check if a bid at this level_index already exists
            updated_ask_levels = [
                lvl for lvl in state.ask_levels
                if lvl.client_order_id != fill.client_order_id
            ]
            counter_exists = inventory_capped or any(
                lvl.level_index == filled_level.level_index and lvl.status in ("open", "pending")
                for lvl in state.bid_levels
            )
            updated_bid_levels = (
                state.bid_levels if counter_exists
                else list(state.bid_levels) + [flip_level]
            )

        replenish_action = None if counter_exists else GridAction(action_type="place", level=flip_level)

        if new_cost < 0:
            logger.warning(
                "base_inventory_cost went negative (%.8f) after fill %s — clamping to 0. "
                "This may indicate a cost-basis tracking drift.",
                new_cost, fill.client_order_id,
            )

        new_state = state.model_copy(
            update={
                "bid_levels": updated_bid_levels,
                "ask_levels": updated_ask_levels,
                "base_inventory": new_base,
                "quote_inventory": new_quote,
                "base_inventory_cost": max(Decimal("0"), new_cost),
                "realized_pnl_quote": state.realized_pnl_quote + realized_delta,
                "total_fills": state.total_fills + 1,
                "last_fill_at": now,
                "updated_at": now,
            }
        )
        return new_state, replenish_action

    def apply_order_placed(
        self,
        *,
        state: GridState,
        level: GridLevel,
        client_order_id: str,
        order_id: str | None,
        now: datetime | None = None,
    ) -> GridState:
        """Mark a level as 'open' after the order was successfully placed."""
        now = now or datetime.now(UTC)
        updated = level.model_copy(
            update={
                "client_order_id": client_order_id,
                "order_id": order_id,
                "status": "open",
                "opened_at": now,
                "updated_at": now,
            }
        )
        bid_levels = [
            updated if lvl.level_id == level.level_id else lvl
            for lvl in state.bid_levels
        ]
        ask_levels = [
            updated if lvl.level_id == level.level_id else lvl
            for lvl in state.ask_levels
        ]
        return state.model_copy(
            update={"bid_levels": bid_levels, "ask_levels": ask_levels, "updated_at": now}
        )

    def apply_order_cancelled(
        self,
        *,
        state: GridState,
        client_order_id: str,
        now: datetime | None = None,
    ) -> GridState:
        """Mark a level as 'pending' after its order was cancelled."""
        now = now or datetime.now(UTC)
        bid_levels = [
            lvl.model_copy(
                update={
                    "client_order_id": None,
                    "order_id": None,
                    "status": "pending",
                    "updated_at": now,
                }
            )
            if lvl.client_order_id == client_order_id
            else lvl
            for lvl in state.bid_levels
        ]
        ask_levels = [
            lvl.model_copy(
                update={
                    "client_order_id": None,
                    "order_id": None,
                    "status": "pending",
                    "updated_at": now,
                }
            )
            if lvl.client_order_id == client_order_id
            else lvl
            for lvl in state.ask_levels
        ]
        return state.model_copy(
            update={"bid_levels": bid_levels, "ask_levels": ask_levels, "updated_at": now}
        )

    def get_pending_levels(self, state: GridState) -> list[GridLevel]:
        """Return all levels that need an order placed."""
        return [lvl for lvl in state.bid_levels + state.ask_levels if lvl.status == "pending"]

    def build_intent_for_level(
        self,
        level: GridLevel,
        regime: str,
        now: datetime | None = None,
    ) -> OrderIntent:
        now = now or datetime.now(UTC)
        return OrderIntent(
            intent_id=str(uuid4()),
            correlation_id=f"ng-{level.product_id}-{level.side.value.lower()}{level.level_index}-{int(now.timestamp()*1000)}",
            product_id=level.product_id,
            side=level.side,
            intent_type="neutral_grid_level",
            tif=TimeInForce.GTC,
            price=level.price,
            size_base=level.size_base,
            size_quote=None,
            post_only=self.settings.maker_only,
            level_index=level.level_index,
            grid_side="bid" if level.side == OrderSide.BUY else "ask",
            strategy_reason=f"neutral_grid_level_{level.side.value.lower()}_{level.level_index}",
            regime_at_decision=regime,
            created_at=now,
        )

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _build_levels(
        self,
        *,
        product_id: str,
        session_id: UUID,
        side: OrderSide,
        anchor: Decimal,
        tick_size: Decimal,
        base_inventory: Decimal,
        quote_inventory: Decimal,
        base_inventory_cost: Decimal,
        existing_realized_pnl: Decimal = Decimal("0"),
    ) -> list[GridLevel]:
        total_equity = quote_inventory + base_inventory * anchor
        levels: list[GridLevel] = []
        now = datetime.now(UTC)

        for i in range(self.settings.grid_levels):
            # Level 0 is closest to mid; higher indices are further away
            spacing_multiplier = Decimal(str(i + 1))
            spread = anchor * self.settings.spacing_bps / Decimal("10000")
            if side == OrderSide.BUY:
                price = anchor - spread * spacing_multiplier
            else:
                price = anchor + spread * spacing_multiplier

            # Snap to tick size
            price = (price / tick_size).quantize(Decimal("1")) * tick_size

            # Inventory-skewed size
            skew = compute_inventory_skew(base_inventory, anchor, total_equity, self.settings.max_inventory_ratio)
            grid_side = "bid" if side == OrderSide.BUY else "ask"
            size_quote = apply_skew_to_size(self.settings.level_size_quote, skew, grid_side)
            size_base = Decimal("0")
            if price > 0:
                size_base = (size_quote / price).quantize(Decimal("0.00000001"))

            levels.append(
                GridLevel(
                    level_id=uuid4(),
                    product_id=product_id,
                    session_id=session_id,
                    side=side,
                    level_index=i,
                    price=price,
                    size_base=size_base,
                    size_quote=size_quote,
                    status="pending",
                    created_at=now,
                    updated_at=now,
                )
            )
        return levels

    def _needs_rebalance(
        self, state: GridState, current_mid: Decimal, now: datetime | None = None
    ) -> bool:
        if state.mid_anchor == 0:
            return True
        drift_bps = abs(current_mid - state.mid_anchor) / state.mid_anchor * Decimal("10000")

        # Normal threshold not reached — no rebalance
        if drift_bps < self.settings.rebalance_threshold_bps:
            return False

        # Fix C: emergency override evaluated first — extreme drift always rebalances,
        # bypassing defer and inventory checks (previously unreachable due to ordering).
        if drift_bps >= self.settings.rebalance_defer_max_drift_bps:
            return True

        # Threshold reached — check if we should defer due to recent fill
        if state.last_fill_at is not None and self.settings.rebalance_defer_seconds > 0:
            now = now or datetime.now(UTC)
            seconds_since_fill = (now - state.last_fill_at).total_seconds()
            if seconds_since_fill < self.settings.rebalance_defer_seconds:
                return False

        # Fix A: inventory guard removed from here.
        # When long_ratio > 65% and price is falling, the rebalance now fires but
        # _rebalance() skips placing new bids while still repricing asks toward
        # current mid — unblocking the sell side without chasing the downtrend.
        return True

    def _rebalance(
        self, state: GridState, market: MarketSnapshot, now: datetime
    ) -> GridDecision:
        """Incremental rebalance: keep viable flip orders, only replace stale levels.

        Instead of cancelling everything and rebuilding from scratch (which destroys
        pending flip orders close to the current price), this:
        1. Builds the ideal new grid around current mid
        2. Keeps existing orders that are within stale_reprice_threshold_bps of their
           new target price (they're still good)
        3. Only cancels and replaces orders that have drifted too far

        Fix A: when long_ratio > 65% and price is falling, skips placing new bids
        but always rebuilds asks toward current mid — unblocking the sell side.

        Fix B: abandons flip asks stranded more than 3× rebalance_threshold above
        current mid before the merge, so they don't survive across rebalances.
        """
        actions: list[GridAction] = []
        tick_size = self._infer_tick_size(market.bid)
        threshold_bps = self.settings.stale_reprice_threshold_bps

        # Fix A: inventory guard — skip new bids when too long in a falling market,
        # but always reprice asks. This replaces the hard block in _needs_rebalance.
        skip_new_bids = False
        if market.mid < state.mid_anchor:
            inventory_value = state.base_inventory * market.mid
            total_equity = state.quote_inventory + inventory_value
            if total_equity > 0:
                long_ratio = inventory_value / total_equity
                if long_ratio > Decimal("0.65"):
                    skip_new_bids = True

        # Fix B (rebalance path): cancel flip orders stranded far from current mid
        # before the merge so they don't accumulate across successive rebalances.
        flip_abandon_bps = self.settings.rebalance_threshold_bps * Decimal("3")
        filtered_ask_levels: list[GridLevel] = []
        for lvl in state.ask_levels:
            if (
                lvl.is_flip
                and lvl.side == OrderSide.SELL
                and lvl.status == "open"
                and lvl.client_order_id
                and market.mid > 0
            ):
                distance_bps = (lvl.price - market.mid) / market.mid * Decimal("10000")
                if distance_bps > flip_abandon_bps:
                    actions.append(GridAction(action_type="cancel", level=lvl))
                    continue
            filtered_ask_levels.append(lvl)

        # Symmetric: cancel flip bids stranded far below current mid
        filtered_bid_levels: list[GridLevel] = []
        for lvl in state.bid_levels:
            if (
                lvl.is_flip
                and lvl.side == OrderSide.BUY
                and lvl.status == "open"
                and lvl.client_order_id
                and market.mid > 0
            ):
                distance_bps = (market.mid - lvl.price) / market.mid * Decimal("10000")
                if distance_bps > flip_abandon_bps:
                    actions.append(GridAction(action_type="cancel", level=lvl))
                    continue
            filtered_bid_levels.append(lvl)

        # Build new ask levels (always — this pulls asks back toward current mid)
        new_ask_levels = self._build_levels(
            product_id=state.product_id,
            session_id=state.session_id,
            side=OrderSide.SELL,
            anchor=market.mid,
            tick_size=tick_size,
            base_inventory=state.base_inventory,
            quote_inventory=state.quote_inventory,
            base_inventory_cost=state.base_inventory_cost,
        )

        # Build new bid levels only when inventory is not skewed too long.
        # When skip_new_bids=True, cancel all open non-flip bids to stop accumulating
        # inventory, then keep only flip bids (pending buy-backs from prior sells).
        max_levels_per_side = self.settings.grid_levels * 3
        if skip_new_bids:
            final_bid_levels = []
            for lvl in filtered_bid_levels:
                if lvl.is_flip and lvl.status in ("open", "pending"):
                    final_bid_levels.append(lvl)
                elif lvl.status == "open" and lvl.client_order_id:
                    actions.append(GridAction(action_type="cancel", level=lvl))
                # pending non-flip bids are simply dropped (not yet placed)
        else:
            new_bid_levels = self._build_levels(
                product_id=state.product_id,
                session_id=state.session_id,
                side=OrderSide.BUY,
                anchor=market.mid,
                tick_size=tick_size,
                base_inventory=state.base_inventory,
                quote_inventory=state.quote_inventory,
                base_inventory_cost=state.base_inventory_cost,
            )
            final_bid_levels = self._merge_levels(filtered_bid_levels, new_bid_levels, threshold_bps, actions, max_levels_per_side, market.mid)

        # Merge ask levels with new targets
        final_ask_levels = self._merge_levels(filtered_ask_levels, new_ask_levels, threshold_bps, actions, max_levels_per_side, market.mid)

        new_state = state.model_copy(
            update={
                "mid_anchor": market.mid,
                "spacing_bps": self.settings.spacing_bps,
                "bid_levels": final_bid_levels,
                "ask_levels": final_ask_levels,
                "rebalance_count": state.rebalance_count + 1,
                "updated_at": now,
            }
        )
        return GridDecision(actions=actions, updated_state=new_state, rebalanced=True, reason="mid_drift")

    def _merge_levels(
        self,
        existing_levels: list[GridLevel],
        new_levels: list[GridLevel],
        threshold_bps: Decimal,
        actions: list[GridAction],
        max_levels: int = 15,
        current_mid: Decimal = Decimal("0"),
    ) -> list[GridLevel]:
        """Merge existing orders with new target levels, keeping orders close enough to target.

        For each new target level_index, find any existing open/pending order at that index.
        If the existing order's price is within threshold_bps of the new target, keep it.
        Otherwise, cancel the old one and use the new level.
        Any existing orders not matched to a new level_index (e.g. flip orders at non-standard
        positions) are kept as-is — they represent valuable pending round trips.
        Caps total levels per side at max_levels to prevent unbounded accumulation.
        """
        # Index existing levels by level_index for O(1) lookup.
        # When duplicates exist at the same index, prefer: open > pending, flip > non-flip.
        existing_by_idx: dict[int, GridLevel] = {}
        extra_levels: list[GridLevel] = []
        for lvl in existing_levels:
            if lvl.level_index in existing_by_idx:
                incumbent = existing_by_idx[lvl.level_index]
                # Prefer open over pending (avoid unnecessary cancel+place churn)
                if lvl.status == "open" and incumbent.status == "pending":
                    extra_levels.append(incumbent)
                    existing_by_idx[lvl.level_index] = lvl
                # Among same status, prefer flip (earned round-trip) over regular
                elif lvl.is_flip and not incumbent.is_flip and lvl.status == incumbent.status:
                    extra_levels.append(incumbent)
                    existing_by_idx[lvl.level_index] = lvl
                else:
                    extra_levels.append(lvl)
            else:
                existing_by_idx[lvl.level_index] = lvl

        matched_indices: set[int] = set()
        merged: list[GridLevel] = []

        for new_lvl in new_levels:
            old_lvl = existing_by_idx.get(new_lvl.level_index)
            if old_lvl is not None and old_lvl.status in ("open", "pending"):
                matched_indices.add(new_lvl.level_index)
                # Check if existing order is close enough to keep
                if new_lvl.price > 0:
                    deviation_bps = abs(old_lvl.price - new_lvl.price) / new_lvl.price * Decimal("10000")
                else:
                    deviation_bps = Decimal("9999")

                if deviation_bps < threshold_bps:
                    # Keep existing order — it's close enough
                    merged.append(old_lvl)
                else:
                    # Cancel stale order, use new level
                    if old_lvl.status == "open" and old_lvl.client_order_id:
                        actions.append(GridAction(action_type="cancel", level=old_lvl))
                    merged.append(new_lvl)
                    actions.append(GridAction(action_type="place", level=new_lvl))
            else:
                # No existing order at this index — place new one
                merged.append(new_lvl)
                actions.append(GridAction(action_type="place", level=new_lvl))

        # Keep unmatched existing orders (flip orders at indices not in the new grid)
        for idx, lvl in existing_by_idx.items():
            if idx not in matched_indices and lvl.status in ("open", "pending"):
                merged.append(lvl)

        # Cancel duplicate extras instead of keeping them (prevents unbounded accumulation)
        for lvl in extra_levels:
            if lvl.status == "open" and lvl.client_order_id:
                actions.append(GridAction(action_type="cancel", level=lvl))
            # pending extras are simply dropped (never placed on exchange)

        # Cap total levels to prevent unbounded accumulation
        if len(merged) > max_levels and current_mid > 0:
            # Sort by distance from current mid, drop furthest pending levels
            merged.sort(key=lambda lvl: abs(lvl.price - current_mid))
            kept: list[GridLevel] = []
            for lvl in merged:
                if len(kept) < max_levels or lvl.status == "open":
                    kept.append(lvl)
                # else: drop this pending level (furthest from mid)
            merged = kept

        return merged

    def _find_stale_orders(
        self, state: GridState, market: MarketSnapshot, now: datetime
    ) -> list[GridAction]:
        """
        Find open orders whose price is too far from the current target price
        (i.e., the grid shifted but the order was not replaced during rebalance).
        This can happen for individual levels after partial fills or minor drifts.
        """
        actions: list[GridAction] = []
        flip_abandon_bps = self.settings.rebalance_threshold_bps * Decimal("3")
        for lvl in state.bid_levels + state.ask_levels:
            if lvl.status != "open":
                continue
            # Fix B (non-rebalance path): flip orders stranded far from current mid
            # are cancelled and not replaced — letting the next rebalance place fresh
            # orders near the current market price.
            if lvl.is_flip:
                if lvl.client_order_id and market.mid > 0:
                    if lvl.side == OrderSide.SELL:
                        distance_bps = (lvl.price - market.mid) / market.mid * Decimal("10000")
                    else:  # BUY — symmetric: abandon flip bids stranded far below mid
                        distance_bps = (market.mid - lvl.price) / market.mid * Decimal("10000")
                    if distance_bps > flip_abandon_bps:
                        actions.append(GridAction(action_type="cancel", level=lvl))
                continue
            order_age = (now - (lvl.opened_at or lvl.updated_at)).total_seconds()
            if order_age < self.settings.stale_order_age_seconds:
                continue
            # Compute what price this level should be at now
            target = self._target_price_for_level(lvl, state.mid_anchor)
            if target <= 0:
                continue
            deviation_bps = abs(lvl.price - target) / target * Decimal("10000")
            if deviation_bps >= self.settings.stale_reprice_threshold_bps:
                actions.append(GridAction(action_type="cancel", level=lvl))
        return actions

    def _build_flip_level(
        self,
        *,
        filled_level: GridLevel,
        base_inventory: Decimal,
        quote_inventory: Decimal,
        market: MarketSnapshot,
        mid_anchor: Decimal,
        now: datetime,
    ) -> GridLevel:
        """Build the counter-order for a filled level (flip logic).

        A filled BID → ASK placed one spacing above the fill price:
            filled_level.price + 1 × spacing
        A filled ASK → BID placed one spacing below the fill price:
            filled_level.price - 1 × spacing

        This keeps the flip close to the current price, making round trips
        achievable even when price has drifted far from the anchor.
        """
        tick_size = self._infer_tick_size(market.bid)
        total_equity = quote_inventory + base_inventory * market.mid
        spread = filled_level.price * self.settings.spacing_bps / Decimal("10000")

        if filled_level.side == OrderSide.BUY:
            flip_side = OrderSide.SELL
            grid_side = "ask"
            price = filled_level.price + spread
        else:
            flip_side = OrderSide.BUY
            grid_side = "bid"
            price = filled_level.price - spread

        price = (price / tick_size).quantize(Decimal("1")) * tick_size

        skew = compute_inventory_skew(
            base_inventory, market.mid, total_equity, self.settings.max_inventory_ratio
        )
        size_quote = apply_skew_to_size(self.settings.level_size_quote, skew, grid_side)
        size_base = (size_quote / price).quantize(Decimal("0.00000001")) if price > 0 else Decimal("0")

        return GridLevel(
            level_id=uuid4(),
            product_id=filled_level.product_id,
            session_id=filled_level.session_id,
            side=flip_side,
            level_index=filled_level.level_index,
            price=price,
            size_base=size_base,
            size_quote=size_quote,
            is_flip=True,
            status="pending",
            created_at=now,
            updated_at=now,
        )

    def _target_price_for_level(self, level: GridLevel, mid_anchor: Decimal) -> Decimal:
        spacing_mult = Decimal(str(level.level_index + 1))
        spread = mid_anchor * self.settings.spacing_bps / Decimal("10000")
        if level.side == OrderSide.BUY:
            return mid_anchor - spread * spacing_mult
        return mid_anchor + spread * spacing_mult

    def _find_level_by_order(self, state: GridState, client_order_id: str) -> GridLevel | None:
        for lvl in state.bid_levels + state.ask_levels:
            if lvl.client_order_id == client_order_id:
                return lvl
        return None

    @staticmethod
    def _infer_tick_size(price: Decimal) -> Decimal:
        """Infer a reasonable tick size from price magnitude."""
        if price >= Decimal("100"):
            return Decimal("0.01")
        if price >= Decimal("1"):
            return Decimal("0.001")
        if price >= Decimal("0.1"):
            return Decimal("0.0001")
        if price >= Decimal("0.01"):
            return Decimal("0.00001")
        return Decimal("0.000001")
