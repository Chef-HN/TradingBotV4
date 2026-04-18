from __future__ import annotations

from decimal import Decimal
from functools import lru_cache

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from auth_kit.config import AuthSettings as _AuthSettings


class AppSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_prefix="APP_", extra="ignore")
    name: str = "TradingBotV3"
    env: str = "local"
    debug: bool = False
    host: str = "0.0.0.0"
    port: int = 8090
    api_key: str = ""
    default_tenant_id: str = "00000000-0000-0000-0000-000000000001"
    run_api: bool = True
    run_worker: bool = True


class DatabaseSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_prefix="DB_", extra="ignore")
    host: str = "localhost"
    port: int = 5433
    name: str = "tradingbotv3"
    user: str = "tradingbot"
    password: str = "tradingbot"
    echo: bool = False

    @property
    def dsn(self) -> str:
        return f"postgresql+asyncpg://{self.user}:{self.password}@{self.host}:{self.port}/{self.name}"


class RedisSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_prefix="REDIS_", extra="ignore")
    host: str = "localhost"
    port: int = 6379
    db: int = 1
    password: str = ""

    @property
    def url(self) -> str:
        if self.password:
            return f"redis://:{self.password}@{self.host}:{self.port}/{self.db}"
        return f"redis://{self.host}:{self.port}/{self.db}"


class CoinbaseSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_prefix="COINBASE_", extra="ignore")
    api_key: str = ""
    api_secret: str = ""
    api_secret_file: str = ""
    rest_base_url: str = "https://api.coinbase.com"
    ws_base_url: str = "wss://advanced-trade-ws.coinbase.com"
    sandbox: bool = False
    rest_timeout_seconds: int = 10
    ws_heartbeat_timeout_seconds: int = 30
    ws_reconnect_delay_seconds: int = 5


class BybitSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_prefix="BYBIT_", extra="ignore")
    api_key: str = ""
    api_secret: str = ""
    rest_base_url: str = "https://api.bybit.com"
    ws_base_url: str = "wss://stream.bybit.com/v5/public/spot"
    rest_timeout_seconds: int = 10
    ws_heartbeat_timeout_seconds: int = 30
    ws_reconnect_delay_seconds: int = 5
    # Bybit Unified Account SPOT maker fee (VIP0 = 0.10%)
    # Set this in .env as BYBIT_MAKER_FEE_RATE to override.
    maker_fee_rate: Decimal = Field(default=Decimal("0.001"))


class ExchangeSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_prefix="EXCHANGE_", extra="ignore")
    name: str = Field(default="coinbase")   # "coinbase" | "bybit"


class StrategySettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_prefix="STRATEGY_", extra="ignore")

    # Comma-separated list of symbols to trade
    symbols: str = "HBAR-USD"
    grid_levels: int = 5
    # Spacing between levels in basis points (e.g. 15 = 0.15%)
    spacing_bps: Decimal = Field(default=Decimal("15"))
    # USD size per grid level
    level_size_quote: Decimal = Field(default=Decimal("10"))
    # Max fraction of portfolio allocated to base inventory per symbol
    max_inventory_ratio: Decimal = Field(default=Decimal("0.6"))
    # Mid must drift this many bps from anchor to trigger full grid rebalance
    # 45 bps = 3 spacing levels of distance before rebalancing, allowing deeper
    # levels time to fill before the grid is rebuilt.
    rebalance_threshold_bps: Decimal = Field(default=Decimal("45"))
    # How many bps a stale open order's price is from current target before repricing
    stale_reprice_threshold_bps: Decimal = Field(default=Decimal("5"))
    # Age in seconds before an unfilled order is considered stale
    stale_order_age_seconds: int = 120
    paper_mode: bool = True
    # Total wallet capital (paper: simulated balance; live: ignored, read from exchange)
    total_wallet_usd: Decimal = Field(default=Decimal("200"))
    # USD capital deployed per session. Reserve = total_wallet - session_capital.
    # In paper mode, taken from total_wallet_usd.
    # In live mode, capped from exchange balance (0 = use full balance).
    session_capital_usd: Decimal = Field(default=Decimal("100"))
    maker_only: bool = True
    # Seconds to defer rebalance after a fill, giving flip orders time to execute.
    # During this window, rebalance is suppressed unless drift exceeds
    # rebalance_defer_max_drift_bps.
    rebalance_defer_seconds: int = 90
    # If drift exceeds this during deferral, rebalance anyway (emergency override).
    rebalance_defer_max_drift_bps: Decimal = Field(default=Decimal("200"))
    # Fallback fee rate when exchange credentials are unavailable.
    fallback_fee_rate: Decimal = Field(default=Decimal("0.0004"))
    # Local daily close schedule.
    local_timezone_iana: str = "UTC"
    daily_close_hour: int = 0
    daily_close_minute: int = 0
    # Runtime / risk / regime thresholds.
    spread_freeze_bps: Decimal = Field(default=Decimal("50"))
    regime_stress_spread_bps: Decimal = Field(default=Decimal("35"))
    regime_trend_slope_threshold: Decimal = Field(default=Decimal("0.0005"))
    regime_mr_distance_threshold_bps: Decimal = Field(default=Decimal("18"))
    regime_hysteresis_bps: Decimal = Field(default=Decimal("4"))
    regime_rsi_bear_threshold: Decimal = Field(default=Decimal("42"))
    regime_rsi_bull_threshold: Decimal = Field(default=Decimal("58"))
    ws_retry_window_seconds: int = 3600
    ws_initial_retry_delay_seconds: int = 5
    ws_max_retry_delay_seconds: int = 60
    ws_message_timeout_seconds: int = 90
    ws_heartbeat_timeout_seconds: int = 30

    @field_validator("symbols", mode="before")
    @classmethod
    def strip_symbols(cls, v: str) -> str:
        return v.strip()

    @field_validator("daily_close_hour")
    @classmethod
    def validate_daily_close_hour(cls, v: int) -> int:
        if v < 0 or v > 23:
            raise ValueError("daily_close_hour must be in [0, 23]")
        return v

    @field_validator("daily_close_minute")
    @classmethod
    def validate_daily_close_minute(cls, v: int) -> int:
        if v < 0 or v > 59:
            raise ValueError("daily_close_minute must be in [0, 59]")
        return v

    def symbol_list(self) -> list[str]:
        return [s.strip() for s in self.symbols.split(",") if s.strip()]


class RiskSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_prefix="RISK_", extra="ignore")
    # Max simultaneous open bid levels per symbol
    max_open_levels_per_side: int = 5
    # Max total deployed notional across all symbols (USD)
    max_total_notional: Decimal = Field(default=Decimal("200"))
    # Max daily realized loss per session (USD, positive number = loss limit)
    max_daily_realized_loss: Decimal = Field(default=Decimal("20"))
    # Max unrealized loss per symbol before defensive unwind
    max_unrealized_loss_per_symbol: Decimal = Field(default=Decimal("15"))
    # Seconds to pause new orders after STRESS regime detected
    stress_pause_seconds: int = 60


class AuthConfig(_AuthSettings):
    """Auth-kit settings — reads JWT_SECRET, SMTP_* etc. from .env (no prefix)."""
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")
    app_name: str = "TradingBotV3"


class LoggingSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_prefix="LOG_", extra="ignore")
    level: str = "INFO"
    enable_json_logs: bool = Field(default=True, validation_alias="LOG_JSON")


class Settings(BaseSettings):
    app: AppSettings = AppSettings()
    db: DatabaseSettings = DatabaseSettings()
    redis: RedisSettings = RedisSettings()
    coinbase: CoinbaseSettings = CoinbaseSettings()
    bybit: BybitSettings = BybitSettings()
    exchange: ExchangeSettings = ExchangeSettings()
    strategy: StrategySettings = StrategySettings()
    risk: RiskSettings = RiskSettings()
    logging: LoggingSettings = LoggingSettings()
    auth: AuthConfig = AuthConfig()


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
