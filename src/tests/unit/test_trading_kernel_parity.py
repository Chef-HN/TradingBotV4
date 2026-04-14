from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

from application.services.trading_kernel import TradingKernel
from config import RiskSettings, StrategySettings
from domain.models import GridState, MarketSnapshot
from risk.engine import RiskEngine
from strategy.neutral_grid import NeutralGridEngine
from strategy.regime import RegimeEngine


def _market_snapshot() -> MarketSnapshot:
    now = datetime.now(UTC)
    return MarketSnapshot(
        product_id="DOGE-USD",
        bid=Decimal("0.1999"),
        ask=Decimal("0.2001"),
        mid=Decimal("0.2"),
        microprice=Decimal("0.2"),
        short_vwap=Decimal("0.2"),
        short_ema=Decimal("0.2"),
        long_ema=Decimal("0.2"),
        rsi=Decimal("50"),
        realized_volatility=Decimal("0"),
        spread_abs=Decimal("0.0002"),
        spread_bps=Decimal("10"),
        spread_zscore=Decimal("0"),
        flow_bias=Decimal("0"),
        top_book_imbalance=Decimal("0.5"),
        last_trade_price=Decimal("0.2"),
        last_trade_size=Decimal("100"),
        event_time=now,
        source_latency_ms=0,
    )


def _grid_state() -> GridState:
    now = datetime.now(UTC)
    return GridState(
        product_id="DOGE-USD",
        session_id=uuid4(),
        mid_anchor=Decimal("0.2"),
        spacing_bps=Decimal("35"),
        bid_levels=[],
        ask_levels=[],
        base_inventory=Decimal("250"),
        quote_inventory=Decimal("50"),
        base_inventory_cost=Decimal("50"),
        realized_pnl_quote=Decimal("0"),
        total_fills=0,
        rebalance_count=0,
        updated_at=now,
    )


def test_trading_kernel_emits_identical_decisions_for_same_inputs() -> None:
    strategy = StrategySettings(
        symbols="DOGE-USD",
        grid_levels=3,
        spacing_bps=Decimal("35"),
        level_size_quote=Decimal("6"),
        rebalance_threshold_bps=Decimal("85"),
        stale_reprice_threshold_bps=Decimal("28"),
        rebalance_defer_seconds=30,
    )
    market = _market_snapshot()
    grid = _grid_state()

    def build_kernel() -> TradingKernel:
        return TradingKernel(
            grid_engine=NeutralGridEngine(strategy),
            regime_engine=RegimeEngine(
                stress_spread_bps=Decimal("35"),
                trend_slope_threshold=Decimal("0.0005"),
                mr_distance_threshold_bps=Decimal("18"),
                hysteresis_bps=Decimal("4"),
                rsi_bear_threshold=Decimal("42"),
                rsi_bull_threshold=Decimal("58"),
            ),
            risk_engine=RiskEngine(RiskSettings(), spread_freeze_bps=Decimal("50")),
        )

    paper_kernel = build_kernel()
    replay_kernel = build_kernel()

    paper_result = paper_kernel.evaluate_tick(
        product_id="DOGE-USD",
        grid_state=grid,
        market=market,
        previous_regime=None,
        previous_risk=None,
        stress_pause_seconds=60,
        now=datetime.now(UTC),
    )
    replay_result = replay_kernel.evaluate_tick(
        product_id="DOGE-USD",
        grid_state=grid,
        market=market,
        previous_regime=None,
        previous_risk=None,
        stress_pause_seconds=60,
        now=datetime.now(UTC),
    )

    assert paper_result.regime_state.regime == replay_result.regime_state.regime
    assert paper_result.risk_decision is not None
    assert replay_result.risk_decision is not None
    assert paper_result.risk_decision.allow_new_bids == replay_result.risk_decision.allow_new_bids
    assert paper_result.risk_decision.allow_new_asks == replay_result.risk_decision.allow_new_asks
    assert len(paper_result.grid_decision.actions) == len(replay_result.grid_decision.actions)
