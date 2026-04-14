-- Runtime, regime, and scheduling parameters fully managed in DB.
-- Also extends history and session tables for full auditability.

ALTER TABLE exchange_strategies
    ADD COLUMN IF NOT EXISTS local_timezone_iana VARCHAR(64) NOT NULL DEFAULT 'UTC',
    ADD COLUMN IF NOT EXISTS daily_close_hour INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS daily_close_minute INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS spread_freeze_bps NUMERIC(10, 4) NOT NULL DEFAULT 50,
    ADD COLUMN IF NOT EXISTS regime_stress_spread_bps NUMERIC(10, 4) NOT NULL DEFAULT 35,
    ADD COLUMN IF NOT EXISTS regime_trend_slope_threshold NUMERIC(18, 8) NOT NULL DEFAULT 0.0005,
    ADD COLUMN IF NOT EXISTS regime_mr_distance_threshold_bps NUMERIC(10, 4) NOT NULL DEFAULT 18,
    ADD COLUMN IF NOT EXISTS regime_hysteresis_bps NUMERIC(10, 4) NOT NULL DEFAULT 4,
    ADD COLUMN IF NOT EXISTS regime_rsi_bear_threshold NUMERIC(10, 4) NOT NULL DEFAULT 42,
    ADD COLUMN IF NOT EXISTS regime_rsi_bull_threshold NUMERIC(10, 4) NOT NULL DEFAULT 58,
    ADD COLUMN IF NOT EXISTS ws_retry_window_seconds INTEGER NOT NULL DEFAULT 3600,
    ADD COLUMN IF NOT EXISTS ws_initial_retry_delay_seconds INTEGER NOT NULL DEFAULT 5,
    ADD COLUMN IF NOT EXISTS ws_max_retry_delay_seconds INTEGER NOT NULL DEFAULT 60,
    ADD COLUMN IF NOT EXISTS ws_message_timeout_seconds INTEGER NOT NULL DEFAULT 90,
    ADD COLUMN IF NOT EXISTS ws_heartbeat_timeout_seconds INTEGER NOT NULL DEFAULT 30;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'chk_exchange_strategies_daily_close_hour'
    ) THEN
        ALTER TABLE exchange_strategies
            ADD CONSTRAINT chk_exchange_strategies_daily_close_hour
            CHECK (daily_close_hour BETWEEN 0 AND 23);
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'chk_exchange_strategies_daily_close_minute'
    ) THEN
        ALTER TABLE exchange_strategies
            ADD CONSTRAINT chk_exchange_strategies_daily_close_minute
            CHECK (daily_close_minute BETWEEN 0 AND 59);
    END IF;
END $$;

ALTER TABLE strategy_param_history
    ADD COLUMN IF NOT EXISTS local_timezone_iana VARCHAR(64) NOT NULL DEFAULT 'UTC',
    ADD COLUMN IF NOT EXISTS daily_close_hour INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS daily_close_minute INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS spread_freeze_bps NUMERIC(10, 4) NOT NULL DEFAULT 50,
    ADD COLUMN IF NOT EXISTS regime_stress_spread_bps NUMERIC(10, 4) NOT NULL DEFAULT 35,
    ADD COLUMN IF NOT EXISTS regime_trend_slope_threshold NUMERIC(18, 8) NOT NULL DEFAULT 0.0005,
    ADD COLUMN IF NOT EXISTS regime_mr_distance_threshold_bps NUMERIC(10, 4) NOT NULL DEFAULT 18,
    ADD COLUMN IF NOT EXISTS regime_hysteresis_bps NUMERIC(10, 4) NOT NULL DEFAULT 4,
    ADD COLUMN IF NOT EXISTS regime_rsi_bear_threshold NUMERIC(10, 4) NOT NULL DEFAULT 42,
    ADD COLUMN IF NOT EXISTS regime_rsi_bull_threshold NUMERIC(10, 4) NOT NULL DEFAULT 58,
    ADD COLUMN IF NOT EXISTS ws_retry_window_seconds INTEGER NOT NULL DEFAULT 3600,
    ADD COLUMN IF NOT EXISTS ws_initial_retry_delay_seconds INTEGER NOT NULL DEFAULT 5,
    ADD COLUMN IF NOT EXISTS ws_max_retry_delay_seconds INTEGER NOT NULL DEFAULT 60,
    ADD COLUMN IF NOT EXISTS ws_message_timeout_seconds INTEGER NOT NULL DEFAULT 90,
    ADD COLUMN IF NOT EXISTS ws_heartbeat_timeout_seconds INTEGER NOT NULL DEFAULT 30;

ALTER TABLE sessions
    ADD COLUMN IF NOT EXISTS underfunded BOOLEAN NOT NULL DEFAULT false,
    ADD COLUMN IF NOT EXISTS underfunded_shortfall_usd NUMERIC(20, 10) NOT NULL DEFAULT 0;

COMMENT ON COLUMN exchange_strategies.local_timezone_iana IS
    'IANA timezone used for local daily close scheduling (e.g. Asia/Singapore)';
COMMENT ON COLUMN exchange_strategies.daily_close_hour IS
    'Local hour for daily close in local_timezone_iana';
COMMENT ON COLUMN exchange_strategies.daily_close_minute IS
    'Local minute for daily close in local_timezone_iana';
COMMENT ON COLUMN exchange_strategies.spread_freeze_bps IS
    'Risk freeze threshold when market spread exceeds this value';
