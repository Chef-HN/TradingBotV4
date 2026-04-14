from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

from config import RiskSettings
from domain.enums import RegimeName, RiskMode
from domain.models import GridState, MarketSnapshot
from risk.engine import RiskEngine


def _market(spread_bps: Decimal) -> MarketSnapshot:
    mid = Decimal("100")
    spread_abs = mid * spread_bps / Decimal("10000")
    bid = mid - (spread_abs / Decimal("2"))
    ask = mid + (spread_abs / Decimal("2"))
    now = datetime.now(UTC)
    return MarketSnapshot(
        product_id="SOL-USD",
        bid=bid,
        ask=ask,
        mid=mid,
        microprice=mid,
        short_vwap=mid,
        short_ema=mid,
        long_ema=mid,
        rsi=Decimal("50"),
        realized_volatility=Decimal("0"),
        spread_abs=spread_abs,
        spread_bps=spread_bps,
        spread_zscore=Decimal("0"),
        flow_bias=Decimal("0"),
        top_book_imbalance=Decimal("0.5"),
        last_trade_price=mid,
        last_trade_size=Decimal("1"),
        event_time=now,
        source_latency_ms=0,
    )


def _grid_state() -> GridState:
    now = datetime.now(UTC)
    return GridState(
        product_id="SOL-USD",
        session_id=uuid4(),
        mid_anchor=Decimal("100"),
        spacing_bps=Decimal("35"),
        bid_levels=[],
        ask_levels=[],
        base_inventory=Decimal("0"),
        quote_inventory=Decimal("100"),
        base_inventory_cost=Decimal("0"),
        realized_pnl_quote=Decimal("0"),
        total_fills=0,
        rebalance_count=0,
        updated_at=now,
    )


def test_spread_freeze_threshold_is_db_configurable() -> None:
    grid = _grid_state()
    market = _market(Decimal("60"))
    settings = RiskSettings()

    permissive = RiskEngine(settings, spread_freeze_bps=Decimal("70"))
    _, permissive_decision = permissive.evaluate(
        product_id="SOL-USD",
        grid_state=grid,
        market=market,
        regime=RegimeName.MR_GOOD,
        previous_state=None,
    )
    assert permissive_decision.risk_mode != RiskMode.FROZEN
    assert permissive_decision.allow_new_bids
    assert permissive_decision.allow_new_asks

    strict = RiskEngine(settings, spread_freeze_bps=Decimal("50"))
    _, strict_decision = strict.evaluate(
        product_id="SOL-USD",
        grid_state=grid,
        market=market,
        regime=RegimeName.MR_GOOD,
        previous_state=None,
    )
    assert strict_decision.risk_mode == RiskMode.FROZEN
    assert not strict_decision.allow_new_bids
    assert not strict_decision.allow_new_asks
