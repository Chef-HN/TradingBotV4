"""
Tests for NeutralGridEngine — rebalance deferral, fill logic, grid construction.

Pure unit tests: no DB, no Redis, no network.
"""
from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import uuid4

import pytest

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from config.settings import StrategySettings
from domain.enums import OrderSide
from domain.models import Fill, GridLevel, GridState, MarketSnapshot
from strategy.neutral_grid.engine import NeutralGridEngine


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_settings(**overrides) -> StrategySettings:
    defaults = dict(
        symbols="BTC-USD",
        grid_levels=5,
        spacing_bps=Decimal("40"),
        level_size_quote=Decimal("10"),
        max_inventory_ratio=Decimal("0.6"),
        rebalance_threshold_bps=Decimal("120"),
        stale_reprice_threshold_bps=Decimal("5"),
        stale_order_age_seconds=120,
        paper_mode=True,
        paper_total_wallet_usd=Decimal("200"),
        paper_base_inventory_usd=Decimal("100"),
        paper_fee_rate=Decimal("0.001"),
        maker_only=True,
        rebalance_defer_seconds=90,
        rebalance_defer_max_drift_bps=Decimal("200"),
    )
    defaults.update(overrides)
    return StrategySettings(**defaults)


def _make_market(mid: Decimal) -> MarketSnapshot:
    spread = mid * Decimal("0.0001")  # 1bps spread
    return MarketSnapshot(
        product_id="BTC-USD",
        bid=mid - spread,
        ask=mid + spread,
        mid=mid,
        microprice=mid,
        short_vwap=mid,
        short_ema=mid,
        realized_volatility=Decimal("0.01"),
        spread_abs=spread * 2,
        spread_bps=Decimal("1"),
        spread_zscore=Decimal("0"),
        flow_bias=Decimal("0"),
        top_book_imbalance=Decimal("0.5"),
        last_trade_price=mid,
        last_trade_size=Decimal("0.001"),
        event_time=datetime.now(UTC),
    )


def _build_state(engine: NeutralGridEngine, mid: Decimal) -> GridState:
    session_id = uuid4()
    base_qty = (Decimal("50") / mid).quantize(Decimal("0.00000001"))
    return engine.build_initial_grid(
        product_id="BTC-USD",
        session_id=session_id,
        mid=mid,
        base_inventory=base_qty,
        quote_inventory=Decimal("50"),
        base_inventory_cost=base_qty * mid,
    )


def _make_fill(
    side: OrderSide,
    price: Decimal,
    size_base: Decimal,
    client_order_id: str,
    fee_rate: Decimal = Decimal("0.001"),
) -> Fill:
    quote_value = (price * size_base).quantize(Decimal("0.00000001"))
    fee = (quote_value * fee_rate).quantize(Decimal("0.00000001"))
    return Fill(
        fill_id=f"fill-{uuid4().hex[:16]}",
        order_id=client_order_id,
        client_order_id=client_order_id,
        product_id="BTC-USD",
        side=side,
        price=price,
        size_base=size_base,
        quote_value=quote_value,
        fee_quote=fee,
        liquidity_indicator="M",
        trade_time=datetime.now(UTC),
    )


# ---------------------------------------------------------------------------
# Grid construction tests
# ---------------------------------------------------------------------------

class TestGridConstruction:
    def test_initial_grid_has_correct_level_count(self):
        engine = NeutralGridEngine(_make_settings(grid_levels=5))
        state = _build_state(engine, Decimal("71000"))
        assert len(state.bid_levels) == 5
        assert len(state.ask_levels) == 5

    def test_initial_grid_levels_are_pending(self):
        engine = NeutralGridEngine(_make_settings())
        state = _build_state(engine, Decimal("71000"))
        for lvl in state.bid_levels + state.ask_levels:
            assert lvl.status == "pending"

    def test_bid_prices_decrease_with_level_index(self):
        engine = NeutralGridEngine(_make_settings())
        state = _build_state(engine, Decimal("71000"))
        for i in range(1, len(state.bid_levels)):
            assert state.bid_levels[i].price < state.bid_levels[i - 1].price

    def test_ask_prices_increase_with_level_index(self):
        engine = NeutralGridEngine(_make_settings())
        state = _build_state(engine, Decimal("71000"))
        for i in range(1, len(state.ask_levels)):
            assert state.ask_levels[i].price > state.ask_levels[i - 1].price

    def test_spacing_matches_settings(self):
        mid = Decimal("71000")
        engine = NeutralGridEngine(_make_settings(spacing_bps=Decimal("40")))
        state = _build_state(engine, mid)
        expected_spread = mid * Decimal("40") / Decimal("10000")
        # Level 0 bid should be at mid - 1×spacing (snapped to tick)
        assert abs(state.bid_levels[0].price - (mid - expected_spread)) < Decimal("0.02")

    def test_grid_levels_3_vs_5(self):
        engine3 = NeutralGridEngine(_make_settings(grid_levels=3))
        engine5 = NeutralGridEngine(_make_settings(grid_levels=5))
        state3 = _build_state(engine3, Decimal("71000"))
        state5 = _build_state(engine5, Decimal("71000"))
        assert len(state3.bid_levels) == 3
        assert len(state5.bid_levels) == 5
        # 5-level grid should cover wider range
        assert state5.bid_levels[-1].price < state3.bid_levels[-1].price

    def test_last_fill_at_is_none_on_fresh_grid(self):
        engine = NeutralGridEngine(_make_settings())
        state = _build_state(engine, Decimal("71000"))
        assert state.last_fill_at is None


# ---------------------------------------------------------------------------
# Rebalance tests
# ---------------------------------------------------------------------------

class TestRebalance:
    def test_no_rebalance_within_threshold(self):
        engine = NeutralGridEngine(_make_settings(rebalance_threshold_bps=Decimal("120")))
        state = _build_state(engine, Decimal("71000"))
        # Move mid by 100bps (< 120 threshold)
        market = _make_market(Decimal("71710"))
        decision = engine.evaluate(state=state, market=market, regime="normal")
        assert not decision.rebalanced

    def test_rebalance_at_threshold(self):
        engine = NeutralGridEngine(_make_settings(rebalance_threshold_bps=Decimal("120")))
        state = _build_state(engine, Decimal("71000"))
        # Move mid by exactly 120bps = $852
        market = _make_market(Decimal("71852"))
        decision = engine.evaluate(state=state, market=market, regime="normal")
        assert decision.rebalanced

    def test_rebalance_updates_anchor(self):
        engine = NeutralGridEngine(_make_settings(rebalance_threshold_bps=Decimal("120")))
        state = _build_state(engine, Decimal("71000"))
        new_mid = Decimal("71900")
        market = _make_market(new_mid)
        decision = engine.evaluate(state=state, market=market, regime="normal")
        assert decision.rebalanced
        assert decision.updated_state is not None
        assert decision.updated_state.mid_anchor == new_mid


# ---------------------------------------------------------------------------
# Rebalance deferral tests — THE MAIN FEATURE
# ---------------------------------------------------------------------------

class TestRebalanceDeferral:
    def test_deferral_suppresses_rebalance_after_recent_fill(self):
        """After a fill, rebalance should be deferred even if threshold is reached."""
        engine = NeutralGridEngine(_make_settings(
            rebalance_threshold_bps=Decimal("120"),
            rebalance_defer_seconds=90,
            rebalance_defer_max_drift_bps=Decimal("200"),
        ))
        state = _build_state(engine, Decimal("71000"))

        # Simulate a fill 30 seconds ago
        now = datetime.now(UTC)
        state = state.model_copy(update={"last_fill_at": now - timedelta(seconds=30)})

        # Price drifts 130bps (past threshold of 120, but within defer max of 200)
        market = _make_market(Decimal("71923"))  # ~130bps drift
        decision = engine.evaluate(state=state, market=market, regime="normal", now=now)
        assert not decision.rebalanced, "Should be deferred due to recent fill"

    def test_deferral_expires_after_timeout(self):
        """After defer_seconds pass, rebalance should fire normally."""
        engine = NeutralGridEngine(_make_settings(
            rebalance_threshold_bps=Decimal("120"),
            rebalance_defer_seconds=90,
            rebalance_defer_max_drift_bps=Decimal("200"),
        ))
        state = _build_state(engine, Decimal("71000"))

        # Simulate a fill 100 seconds ago (> 90s defer window)
        now = datetime.now(UTC)
        state = state.model_copy(update={"last_fill_at": now - timedelta(seconds=100)})

        # Price drifts 130bps (past threshold)
        market = _make_market(Decimal("71923"))
        decision = engine.evaluate(state=state, market=market, regime="normal", now=now)
        assert decision.rebalanced, "Deferral expired — should rebalance"

    def test_emergency_override_during_deferral(self):
        """Extreme drift (>200bps) should override deferral and force rebalance."""
        engine = NeutralGridEngine(_make_settings(
            rebalance_threshold_bps=Decimal("120"),
            rebalance_defer_seconds=90,
            rebalance_defer_max_drift_bps=Decimal("200"),
        ))
        state = _build_state(engine, Decimal("71000"))

        # Simulate a fill 10 seconds ago (well within defer window)
        now = datetime.now(UTC)
        state = state.model_copy(update={"last_fill_at": now - timedelta(seconds=10)})

        # Price crashes 250bps (> 200bps emergency threshold)
        market = _make_market(Decimal("72775"))  # ~250bps drift
        decision = engine.evaluate(state=state, market=market, regime="normal", now=now)
        assert decision.rebalanced, "Emergency override should force rebalance"

    def test_no_deferral_when_no_previous_fill(self):
        """No fill history → no deferral, rebalance fires at normal threshold."""
        engine = NeutralGridEngine(_make_settings(
            rebalance_threshold_bps=Decimal("120"),
            rebalance_defer_seconds=90,
        ))
        state = _build_state(engine, Decimal("71000"))
        assert state.last_fill_at is None

        market = _make_market(Decimal("71860"))  # ~121bps
        decision = engine.evaluate(state=state, market=market, regime="normal")
        assert decision.rebalanced

    def test_deferral_disabled_when_defer_seconds_zero(self):
        """rebalance_defer_seconds=0 disables deferral entirely."""
        engine = NeutralGridEngine(_make_settings(
            rebalance_threshold_bps=Decimal("120"),
            rebalance_defer_seconds=0,
        ))
        state = _build_state(engine, Decimal("71000"))

        now = datetime.now(UTC)
        state = state.model_copy(update={"last_fill_at": now - timedelta(seconds=5)})

        market = _make_market(Decimal("71860"))
        decision = engine.evaluate(state=state, market=market, regime="normal", now=now)
        assert decision.rebalanced, "Deferral is disabled — should rebalance"

    def test_deferral_boundary_at_exactly_defer_seconds(self):
        """At exactly defer_seconds, the deferral should have expired."""
        engine = NeutralGridEngine(_make_settings(
            rebalance_threshold_bps=Decimal("120"),
            rebalance_defer_seconds=90,
            rebalance_defer_max_drift_bps=Decimal("200"),
        ))
        state = _build_state(engine, Decimal("71000"))

        now = datetime.now(UTC)
        # Exactly at the boundary
        state = state.model_copy(update={"last_fill_at": now - timedelta(seconds=90)})

        market = _make_market(Decimal("71860"))
        decision = engine.evaluate(state=state, market=market, regime="normal", now=now)
        assert decision.rebalanced, "At exactly defer_seconds, deferral should be expired"

    def test_deferral_boundary_at_exactly_max_drift(self):
        """At exactly defer_max_drift_bps, emergency override should trigger."""
        engine = NeutralGridEngine(_make_settings(
            rebalance_threshold_bps=Decimal("120"),
            rebalance_defer_seconds=90,
            rebalance_defer_max_drift_bps=Decimal("200"),
        ))
        state = _build_state(engine, Decimal("71000"))

        now = datetime.now(UTC)
        state = state.model_copy(update={"last_fill_at": now - timedelta(seconds=10)})

        # Exactly 200bps drift
        market = _make_market(Decimal("72420"))  # 71000 * 1.02 = 72420
        decision = engine.evaluate(state=state, market=market, regime="normal", now=now)
        assert decision.rebalanced, "At exactly max_drift_bps, emergency override should fire"


# ---------------------------------------------------------------------------
# Fill processing tests
# ---------------------------------------------------------------------------

class TestFillProcessing:
    def test_buy_fill_updates_inventory(self):
        engine = NeutralGridEngine(_make_settings())
        state = _build_state(engine, Decimal("71000"))
        market = _make_market(Decimal("71000"))

        # Mark level 0 bid as open
        bid0 = state.bid_levels[0]
        state = engine.apply_order_placed(
            state=state, level=bid0,
            client_order_id="test-buy-0", order_id="ex-0",
        )

        fill = _make_fill(
            side=OrderSide.BUY,
            price=bid0.price,
            size_base=bid0.size_base,
            client_order_id="test-buy-0",
        )
        new_state, action = engine.apply_fill(state=state, fill=fill, market=market)

        assert new_state.base_inventory > state.base_inventory
        assert new_state.quote_inventory < state.quote_inventory
        assert new_state.total_fills == state.total_fills + 1

    def test_buy_fill_sets_last_fill_at(self):
        engine = NeutralGridEngine(_make_settings())
        state = _build_state(engine, Decimal("71000"))
        market = _make_market(Decimal("71000"))
        assert state.last_fill_at is None

        bid0 = state.bid_levels[0]
        state = engine.apply_order_placed(
            state=state, level=bid0,
            client_order_id="test-buy-0", order_id="ex-0",
        )

        fill = _make_fill(
            side=OrderSide.BUY,
            price=bid0.price,
            size_base=bid0.size_base,
            client_order_id="test-buy-0",
        )
        now = datetime.now(UTC)
        new_state, _ = engine.apply_fill(state=state, fill=fill, market=market, now=now)

        assert new_state.last_fill_at == now

    def test_sell_fill_realizes_pnl(self):
        engine = NeutralGridEngine(_make_settings())
        state = _build_state(engine, Decimal("71000"))
        market = _make_market(Decimal("71000"))

        ask0 = state.ask_levels[0]
        state = engine.apply_order_placed(
            state=state, level=ask0,
            client_order_id="test-sell-0", order_id="ex-1",
        )

        fill = _make_fill(
            side=OrderSide.SELL,
            price=ask0.price,
            size_base=ask0.size_base,
            client_order_id="test-sell-0",
        )
        new_state, _ = engine.apply_fill(state=state, fill=fill, market=market)

        # Should have realized some PnL (sell above avg cost)
        assert new_state.realized_pnl_quote != Decimal("0")

    def test_fill_creates_flip_order(self):
        """When no counter-side order exists at the same level_index, a flip is created."""
        engine = NeutralGridEngine(_make_settings())
        state = _build_state(engine, Decimal("71000"))
        market = _make_market(Decimal("71000"))

        bid0 = state.bid_levels[0]
        state = engine.apply_order_placed(
            state=state, level=bid0,
            client_order_id="test-buy-0", order_id="ex-0",
        )

        # Remove the existing ask at level_index 0 so the flip logic creates one
        state = state.model_copy(update={
            "ask_levels": [lvl for lvl in state.ask_levels if lvl.level_index != 0]
        })

        fill = _make_fill(
            side=OrderSide.BUY,
            price=bid0.price,
            size_base=bid0.size_base,
            client_order_id="test-buy-0",
        )
        new_state, action = engine.apply_fill(state=state, fill=fill, market=market)

        assert action is not None
        assert action.action_type == "place"
        assert action.level.side == OrderSide.SELL  # flip: buy → sell


# ---------------------------------------------------------------------------
# Orchestrator tests
# ---------------------------------------------------------------------------

class TestOrchestrator:
    def test_acquire_and_release_lock(self):
        from scripts.orchestrator import ProcessOrchestrator
        orch = ProcessOrchestrator()
        exchange = "test_exchange"
        try:
            assert orch.acquire_lock(exchange)
            assert orch.is_running(exchange)
            orch.release_lock(exchange)
            assert not orch.is_running(exchange)
        finally:
            orch._remove_lock(exchange)

    def test_cannot_acquire_same_lock_twice(self):
        from scripts.orchestrator import ProcessOrchestrator
        orch = ProcessOrchestrator()
        exchange = "test_exchange2"
        try:
            assert orch.acquire_lock(exchange)
            # Second acquire should fail (same PID, process is alive)
            assert not orch.acquire_lock(exchange)
        finally:
            orch.release_lock(exchange)

    def test_different_exchanges_independent(self):
        from scripts.orchestrator import ProcessOrchestrator
        orch = ProcessOrchestrator()
        try:
            assert orch.acquire_lock("test_ex_a")
            assert orch.acquire_lock("test_ex_b")
            assert orch.is_running("test_ex_a")
            assert orch.is_running("test_ex_b")
            orch.release_lock("test_ex_a")
            assert not orch.is_running("test_ex_a")
            assert orch.is_running("test_ex_b")  # B unaffected
        finally:
            orch._remove_lock("test_ex_a")
            orch._remove_lock("test_ex_b")

    def test_stop_signal_per_exchange(self):
        from scripts.orchestrator import ProcessOrchestrator
        orch = ProcessOrchestrator()
        exchange = "test_signal"
        try:
            orch.acquire_lock(exchange)
            assert orch.signal_stop(exchange)
            assert orch.check_stop_signal(exchange)
            orch.clear_stop_signal(exchange)
            assert not orch.check_stop_signal(exchange)
        finally:
            orch.release_lock(exchange)

    def test_cleanup_orphans(self):
        from scripts.orchestrator import ProcessOrchestrator
        import json
        orch = ProcessOrchestrator()
        # Write a fake lock with a dead PID
        fake_exchange = "test_orphan"
        orch._write_lock(fake_exchange, {
            "pid": 99999999,  # very unlikely to be alive
            "exchange": fake_exchange,
            "started_at": "2020-01-01T00:00:00",
            "symbols": [],
        })
        assert orch._read_lock(fake_exchange) is not None
        cleaned = orch.cleanup_orphans()
        assert fake_exchange in cleaned
        assert orch._read_lock(fake_exchange) is None

    def test_status_returns_process_info(self):
        from scripts.orchestrator import ProcessOrchestrator
        orch = ProcessOrchestrator()
        exchange = "test_status"
        try:
            orch.acquire_lock(exchange)
            orch.update_lock_symbols(exchange, ["BTC-USD"])
            info = orch.status()
            assert exchange in info
            assert info[exchange].alive
            assert info[exchange].symbols == ["BTC-USD"]
        finally:
            orch.release_lock(exchange)


# ---------------------------------------------------------------------------
# StateStore key scoping tests
# ---------------------------------------------------------------------------

class TestStateStoreKeys:
    def test_exchange_scoped_keys(self):
        from infrastructure.state_store import StateStore
        s = StateStore("redis://localhost:6379/1", exchange="bybit")
        assert "bybit" in s._key_state
        assert "bybit" in s._key_commands
        assert "bybit" in s._key_skip_close

    def test_different_exchanges_different_keys(self):
        from infrastructure.state_store import StateStore
        s1 = StateStore("redis://localhost:6379/1", exchange="bybit")
        s2 = StateStore("redis://localhost:6379/1", exchange="coinbase")
        assert s1._key_state != s2._key_state
        assert s1._key_commands != s2._key_commands

    def test_legacy_keys_without_exchange(self):
        from infrastructure.state_store import StateStore
        s = StateStore("redis://localhost:6379/1")
        assert s._key_state == "tb:v3:state"
        assert s._key_commands == "tb:v3:commands"
