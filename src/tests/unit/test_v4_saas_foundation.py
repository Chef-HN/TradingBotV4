from __future__ import annotations

from types import SimpleNamespace

import pytest

from infrastructure.state_store import StateStore
from scripts.run_worker import _build_pair_strategy_synthetic_row


def _row(
    product_id: str,
    *,
    paper_mode: bool = True,
    local_timezone_iana: str = "Europe/Paris",
) -> SimpleNamespace:
    return SimpleNamespace(
        product_id=product_id,
        spacing_bps=35,
        rebalance_threshold_bps=110,
        grid_levels=3,
        level_size_quote=4.5,
        max_inventory_ratio=0.6,
        maker_fee_rate=0.001,
        stale_reprice_threshold_bps=40,
        stale_order_age_seconds=120,
        rebalance_defer_seconds=75,
        rebalance_defer_max_drift_bps=200,
        total_wallet_usd=200,
        session_capital_usd=100,
        maker_only=True,
        paper_mode=paper_mode,
        local_timezone_iana=local_timezone_iana,
        daily_close_hour=0,
        daily_close_minute=0,
        spread_freeze_bps=50,
        regime_stress_spread_bps=35,
        regime_trend_slope_threshold=0.0005,
        regime_mr_distance_threshold_bps=18,
        regime_hysteresis_bps=4,
        regime_rsi_bear_threshold=42,
        regime_rsi_bull_threshold=58,
        ws_retry_window_seconds=3600,
        ws_initial_retry_delay_seconds=5,
        ws_max_retry_delay_seconds=60,
        ws_message_timeout_seconds=90,
        ws_heartbeat_timeout_seconds=30,
        updated_by="tester",
    )


def test_build_pair_strategy_synthetic_row_success() -> None:
    rows = [_row("SOL-USD"), _row("DOGE-USD")]
    synth = _build_pair_strategy_synthetic_row(
        exchange_name="bybit",
        tenant_id="00000000-0000-0000-0000-000000000001",
        pair_rows=rows,
    )
    assert synth.symbols == "DOGE-USD,SOL-USD"
    assert set(synth.symbol_overrides.keys()) == {"SOL-USD", "DOGE-USD"}
    assert synth.symbol_overrides["SOL-USD"]["spacing_bps"] == 35
    assert synth.local_timezone_iana == "Europe/Paris"


def test_build_pair_strategy_synthetic_row_fail_fast_on_mismatch() -> None:
    rows = [_row("SOL-USD", paper_mode=True), _row("DOGE-USD", paper_mode=False)]
    with pytest.raises(RuntimeError, match="must share 'paper_mode'"):
        _build_pair_strategy_synthetic_row(
            exchange_name="bybit",
            tenant_id="00000000-0000-0000-0000-000000000001",
            pair_rows=rows,
        )


def test_state_store_uses_v4_namespace() -> None:
    store = StateStore(
        "redis://localhost:6379/1",
        exchange="bybit",
        tenant_id="Tenant-ABC",
        product_id="SOL-USD",
    )
    assert store._key_state == "tb:v4:tenant-abc:bybit:sol-usd:state"
    assert store._key_commands == "tb:v4:tenant-abc:bybit:all:commands"
