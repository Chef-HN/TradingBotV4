from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal

from config import RiskSettings
from domain.enums import RegimeName, RiskMode
from domain.models import GridState, MarketSnapshot, RiskState


@dataclass(slots=True)
class RiskDecision:
    allow_new_bids: bool
    allow_new_asks: bool
    should_cancel_all: bool = False
    risk_mode: RiskMode = RiskMode.NORMAL
    reasons: list[str] = field(default_factory=list)


class RiskEngine:
    """
    Risk engine for the neutral grid.

    Unlike V2 (directional long only), the neutral grid holds both sides,
    so risk checks differ:
    - Stress regime: pause ALL new orders (both sides)
    - TREND_DOWN: allow asks to execute (we want to sell), block new bids
    - TREND_UP: allow bids, block new asks
    - MR: allow both sides
    - Inventory ratio exceeded: block new bids
    - Daily loss limit: cancel all, shutdown
    """

    def __init__(self, settings: RiskSettings, spread_freeze_bps: Decimal = Decimal("50")) -> None:
        self.settings = settings
        self.spread_freeze_bps = spread_freeze_bps

    def evaluate(
        self,
        *,
        product_id: str,
        grid_state: GridState,
        market: MarketSnapshot,
        regime: RegimeName,
        previous_state: RiskState | None,
    ) -> tuple[RiskState, RiskDecision]:
        reasons: list[str] = []
        allow_new_bids = True
        allow_new_asks = True
        should_cancel_all = False
        risk_mode = RiskMode.NORMAL

        # ---- Daily loss limit ----
        if grid_state.realized_pnl_quote <= (self.settings.max_daily_realized_loss * Decimal("-1")):
            allow_new_bids = False
            allow_new_asks = False
            should_cancel_all = True
            risk_mode = RiskMode.SHUTDOWN
            reasons.append("max_daily_realized_loss")

        # ---- Unrealized loss per symbol ----
        elif grid_state.unrealized_pnl_quote <= (self.settings.max_unrealized_loss_per_symbol * Decimal("-1")):
            allow_new_bids = False
            risk_mode = RiskMode.DEFENSIVE_UNWIND
            reasons.append("max_unrealized_loss_per_symbol")

        # ---- Spread too wide (stress) ----
        elif market.spread_bps > self.spread_freeze_bps:
            allow_new_bids = False
            allow_new_asks = False
            risk_mode = RiskMode.FROZEN
            reasons.append("spread_too_wide")

        # ---- Regime-based rules ----
        elif regime == RegimeName.STRESS:
            allow_new_bids = False
            allow_new_asks = False
            risk_mode = RiskMode.FROZEN
            reasons.append("regime_stress")

        elif regime in (RegimeName.TREND_DOWN, RegimeName.TREND_UP):
            # For a neutral grid maker, both sides must stay open for round trips.
            # Inventory skew already reduces the risky side's size naturally.
            # We flag REDUCED mode so the worker can log it, but do NOT block
            # either side — blocking bids in TREND_DOWN kills the grid.
            if risk_mode == RiskMode.NORMAL:
                risk_mode = RiskMode.REDUCED
            reasons.append(f"regime_{regime.value.lower()}_reduced")

        # ---- Inventory cap: block new buys if deployed notional exceeds limit ----
        if grid_state.base_inventory > 0 and market.mid > 0:
            deployed_notional = grid_state.base_inventory * market.mid
            if deployed_notional >= self.settings.max_total_notional:
                allow_new_bids = False
                reasons.append("inventory_ratio_cap")

        # ---- Max open levels per side ----
        if len(grid_state.open_bid_levels) >= self.settings.max_open_levels_per_side:
            allow_new_bids = False
            reasons.append("max_open_bid_levels")
        if len(grid_state.open_ask_levels) >= self.settings.max_open_levels_per_side:
            allow_new_asks = False
            reasons.append("max_open_ask_levels")

        shutdown_lock_at = None
        if previous_state and previous_state.shutdown_lock_triggered:
            shutdown_lock_at = previous_state.shutdown_lock_at
        if risk_mode == RiskMode.SHUTDOWN and shutdown_lock_at is None:
            shutdown_lock_at = datetime.now(UTC)

        risk_state = RiskState(
            product_id=product_id,
            risk_mode=risk_mode,
            freeze_new_bids=not allow_new_bids,
            freeze_new_asks=not allow_new_asks,
            flatten_required=should_cancel_all,
            shutdown_lock_triggered=risk_mode == RiskMode.SHUTDOWN,
            shutdown_lock_at=shutdown_lock_at,
            current_unrealized_loss=abs(min(grid_state.unrealized_pnl_quote, Decimal("0"))),
            current_realized_pnl=grid_state.realized_pnl_quote,
            reason_codes=reasons,
            updated_at=datetime.now(UTC),
        )
        decision = RiskDecision(
            allow_new_bids=allow_new_bids,
            allow_new_asks=allow_new_asks,
            should_cancel_all=should_cancel_all,
            risk_mode=risk_mode,
            reasons=reasons,
        )
        return risk_state, decision
