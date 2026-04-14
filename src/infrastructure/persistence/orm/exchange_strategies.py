from __future__ import annotations

from sqlalchemy import Boolean, DateTime, Integer, JSON, Numeric, String
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base


class ExchangeStrategyRow(Base):
    __tablename__ = "exchange_strategies"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    # Human-readable name, e.g. "bybit-tight-40bps" or "coinbase-default"
    name: Mapped[str] = mapped_column(String(100), nullable=False, unique=True)
    exchange_name: Mapped[str] = mapped_column(String(20), nullable=False, index=True)
    # Only one strategy per exchange can be active at a time
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)

    # Grid parameters
    spacing_bps: Mapped[float] = mapped_column(Numeric(10, 4), nullable=False)
    rebalance_threshold_bps: Mapped[float] = mapped_column(Numeric(10, 4), nullable=False)
    grid_levels: Mapped[int] = mapped_column(Integer, nullable=False)
    level_size_quote: Mapped[float] = mapped_column(Numeric(20, 10), nullable=False)
    max_inventory_ratio: Mapped[float] = mapped_column(Numeric(10, 4), nullable=False)
    maker_fee_rate: Mapped[float] = mapped_column(Numeric(10, 6), nullable=False)
    stale_reprice_threshold_bps: Mapped[float] = mapped_column(Numeric(10, 4), nullable=False)
    stale_order_age_seconds: Mapped[int] = mapped_column(Integer, nullable=False)

    # Rebalance deferral
    rebalance_defer_seconds: Mapped[int] = mapped_column(Integer, nullable=False, default=90)
    rebalance_defer_max_drift_bps: Mapped[float] = mapped_column(Numeric(10, 4), nullable=False, default=200)

    # Operational
    symbols: Mapped[str] = mapped_column(String(500), nullable=False, default="BTC-USD")
    paper_mode: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)

    # Capital & order params (formerly in .env)
    total_wallet_usd: Mapped[float] = mapped_column(Numeric(20, 10), nullable=False, default=200)
    session_capital_usd: Mapped[float] = mapped_column(Numeric(20, 10), nullable=False, default=100)
    maker_only: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)

    # Daily close schedule (local to configured timezone)
    local_timezone_iana: Mapped[str] = mapped_column(String(64), nullable=False, default="UTC")
    daily_close_hour: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    daily_close_minute: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    # Runtime/risk/regime params
    spread_freeze_bps: Mapped[float] = mapped_column(Numeric(10, 4), nullable=False, default=50)
    regime_stress_spread_bps: Mapped[float] = mapped_column(Numeric(10, 4), nullable=False, default=35)
    regime_trend_slope_threshold: Mapped[float] = mapped_column(Numeric(18, 8), nullable=False, default=0.0005)
    regime_mr_distance_threshold_bps: Mapped[float] = mapped_column(Numeric(10, 4), nullable=False, default=18)
    regime_hysteresis_bps: Mapped[float] = mapped_column(Numeric(10, 4), nullable=False, default=4)
    regime_rsi_bear_threshold: Mapped[float] = mapped_column(Numeric(10, 4), nullable=False, default=42)
    regime_rsi_bull_threshold: Mapped[float] = mapped_column(Numeric(10, 4), nullable=False, default=58)
    ws_retry_window_seconds: Mapped[int] = mapped_column(Integer, nullable=False, default=3600)
    ws_initial_retry_delay_seconds: Mapped[int] = mapped_column(Integer, nullable=False, default=5)
    ws_max_retry_delay_seconds: Mapped[int] = mapped_column(Integer, nullable=False, default=60)
    ws_message_timeout_seconds: Mapped[int] = mapped_column(Integer, nullable=False, default=90)
    ws_heartbeat_timeout_seconds: Mapped[int] = mapped_column(Integer, nullable=False, default=30)

    # Per-symbol parameter overrides (optional)
    # e.g. {"SOL-USD": {"spacing_bps": 20}, "DOGE-USD": {"spacing_bps": 15}}
    symbol_overrides: Mapped[dict | None] = mapped_column(JSON, nullable=True, default=None)

    updated_by: Mapped[str] = mapped_column(String(50), nullable=False, default="system")

    created_at: Mapped[object] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[object] = mapped_column(DateTime(timezone=True), nullable=False)
