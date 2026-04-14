from __future__ import annotations

from sqlalchemy import Boolean, DateTime, Integer, JSON, Numeric, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base


class StrategyParamHistoryRow(Base):
    __tablename__ = "strategy_param_history"

    history_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    strategy_id: Mapped[int] = mapped_column(Integer, nullable=False)
    strategy_name: Mapped[str] = mapped_column(String(100), nullable=False)
    exchange_name: Mapped[str] = mapped_column(String(20), nullable=False)

    # Snapshot of ALL params at this version
    spacing_bps: Mapped[float] = mapped_column(Numeric(10, 4), nullable=False)
    rebalance_threshold_bps: Mapped[float] = mapped_column(Numeric(10, 4), nullable=False)
    grid_levels: Mapped[int] = mapped_column(Integer, nullable=False)
    level_size_quote: Mapped[float] = mapped_column(Numeric(20, 10), nullable=False)
    max_inventory_ratio: Mapped[float] = mapped_column(Numeric(10, 4), nullable=False)
    maker_fee_rate: Mapped[float] = mapped_column(Numeric(10, 6), nullable=False)
    stale_reprice_threshold_bps: Mapped[float] = mapped_column(Numeric(10, 4), nullable=False)
    stale_order_age_seconds: Mapped[int] = mapped_column(Integer, nullable=False)
    rebalance_defer_seconds: Mapped[int] = mapped_column(Integer, nullable=False)
    rebalance_defer_max_drift_bps: Mapped[float] = mapped_column(Numeric(10, 4), nullable=False)
    total_wallet_usd: Mapped[float] = mapped_column(Numeric(20, 10), nullable=False)
    session_capital_usd: Mapped[float] = mapped_column(Numeric(20, 10), nullable=False)
    maker_only: Mapped[bool] = mapped_column(Boolean, nullable=False)
    paper_mode: Mapped[bool] = mapped_column(Boolean, nullable=False)
    symbols: Mapped[str] = mapped_column(String(500), nullable=False)
    local_timezone_iana: Mapped[str] = mapped_column(String(64), nullable=False, default="UTC")
    daily_close_hour: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    daily_close_minute: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
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
    symbol_overrides: Mapped[dict | None] = mapped_column(JSON, nullable=True)

    # SCD2 fields
    valid_from: Mapped[object] = mapped_column(DateTime(timezone=True), nullable=False)
    valid_to: Mapped[object | None] = mapped_column(DateTime(timezone=True), nullable=True)
    updated_by: Mapped[str] = mapped_column(String(50), nullable=False)
    change_summary: Mapped[str | None] = mapped_column(Text, nullable=True)
