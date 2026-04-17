from __future__ import annotations

from sqlalchemy import BigInteger, Boolean, DateTime, Integer, Numeric, String, Uuid
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base


class TenantPairStrategyRow(Base):
    __tablename__ = "tenant_pair_strategies"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    tenant_id: Mapped[str] = mapped_column(Uuid(as_uuid=False), nullable=False, index=True)
    exchange_name: Mapped[str] = mapped_column(String(20), nullable=False, index=True)
    product_id: Mapped[str] = mapped_column(String(20), nullable=False, index=True)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)

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
    local_timezone_iana: Mapped[str] = mapped_column(String(64), nullable=False)
    daily_close_hour: Mapped[int] = mapped_column(Integer, nullable=False)
    daily_close_minute: Mapped[int] = mapped_column(Integer, nullable=False)
    spread_freeze_bps: Mapped[float] = mapped_column(Numeric(10, 4), nullable=False)
    regime_stress_spread_bps: Mapped[float] = mapped_column(Numeric(10, 4), nullable=False)
    regime_trend_slope_threshold: Mapped[float] = mapped_column(Numeric(18, 8), nullable=False)
    regime_mr_distance_threshold_bps: Mapped[float] = mapped_column(Numeric(10, 4), nullable=False)
    regime_hysteresis_bps: Mapped[float] = mapped_column(Numeric(10, 4), nullable=False)
    regime_rsi_bear_threshold: Mapped[float] = mapped_column(Numeric(10, 4), nullable=False)
    regime_rsi_bull_threshold: Mapped[float] = mapped_column(Numeric(10, 4), nullable=False)
    ws_retry_window_seconds: Mapped[int] = mapped_column(Integer, nullable=False)
    ws_initial_retry_delay_seconds: Mapped[int] = mapped_column(Integer, nullable=False)
    ws_max_retry_delay_seconds: Mapped[int] = mapped_column(Integer, nullable=False)
    ws_message_timeout_seconds: Mapped[int] = mapped_column(Integer, nullable=False)
    ws_heartbeat_timeout_seconds: Mapped[int] = mapped_column(Integer, nullable=False)

    updated_by: Mapped[str] = mapped_column(String(50), nullable=False, default="system")
    created_at: Mapped[object] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[object] = mapped_column(DateTime(timezone=True), nullable=False)
