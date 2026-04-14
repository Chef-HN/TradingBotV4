from decimal import Decimal

import pytest

from scripts.run_worker import _resolve_symbol_settings


class _DummySettings:
    def __init__(self) -> None:
        self.spacing_bps = Decimal("10")
        self.rebalance_threshold_bps = Decimal("50")
        self.grid_levels = 4
        self.level_size_quote = Decimal("5")
        self.stale_reprice_threshold_bps = Decimal("20")
        self.stale_order_age_seconds = 120
        self.rebalance_defer_seconds = 60
        self.rebalance_defer_max_drift_bps = Decimal("200")
        self.max_inventory_ratio = Decimal("0.60")
        self.session_capital_usd = Decimal("100")
        self.maker_only = True

    def model_copy(self, update: dict):
        clone = _DummySettings.__new__(_DummySettings)
        clone.__dict__ = self.__dict__.copy()
        clone.__dict__.update(update)
        return clone


def _complete_symbol_override() -> dict:
    return {
        "spacing_bps": 35,
        "rebalance_threshold_bps": 110,
        "grid_levels": 3,
        "level_size_quote": 4.5,
        "stale_reprice_threshold_bps": 40,
        "stale_order_age_seconds": 120,
        "rebalance_defer_seconds": 75,
        "rebalance_defer_max_drift_bps": 200,
        "max_inventory_ratio": 0.6,
        "session_capital_usd": 100,
        "maker_only": True,
    }


def test_resolve_symbol_settings_requires_override_for_symbol() -> None:
    base = _DummySettings()
    with pytest.raises(RuntimeError, match="Missing per-symbol strategy"):
        _resolve_symbol_settings(base, {"DOGE-USD": _complete_symbol_override()}, "SOL-USD")


def test_resolve_symbol_settings_requires_complete_pair_fields() -> None:
    base = _DummySettings()
    bad = _complete_symbol_override()
    bad.pop("stale_order_age_seconds")
    with pytest.raises(RuntimeError, match="Incomplete per-symbol strategy"):
        _resolve_symbol_settings(base, {"SOL-USD": bad}, "SOL-USD")


def test_resolve_symbol_settings_uses_pair_override_only() -> None:
    base = _DummySettings()
    resolved = _resolve_symbol_settings(
        base,
        {"SOL-USD": _complete_symbol_override()},
        "SOL-USD",
    )
    assert resolved.spacing_bps == Decimal("35")
    assert resolved.rebalance_threshold_bps == Decimal("110")
    assert resolved.grid_levels == 3
    assert resolved.level_size_quote == Decimal("4.5")
    assert resolved.maker_only is True
