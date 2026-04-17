-- V4 SaaS foundation: multi-tenant schema, API keys, pair strategies, and tenant-scoped analytics.
-- Additive migration. No table drops. Existing V3 structures remain available.

CREATE TABLE IF NOT EXISTS tenants (
    id UUID PRIMARY KEY,
    name VARCHAR(120) NOT NULL,
    slug VARCHAR(80) NOT NULL UNIQUE,
    tier VARCHAR(20) NOT NULL DEFAULT 'entry',
    max_capital NUMERIC(20, 10) NOT NULL DEFAULT 100,
    max_pairs INTEGER NOT NULL DEFAULT 1,
    max_exchanges INTEGER NOT NULL DEFAULT 1,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

INSERT INTO tenants (id, name, slug, tier, max_capital, max_pairs, max_exchanges, is_active, created_at, updated_at)
VALUES (
    '00000000-0000-0000-0000-000000000001',
    'Default Tenant',
    'default',
    'entry',
    100,
    1,
    1,
    TRUE,
    NOW(),
    NOW()
)
ON CONFLICT (id) DO NOTHING;

CREATE TABLE IF NOT EXISTS tenant_api_keys (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL REFERENCES tenants(id),
    key_hash CHAR(64) NOT NULL UNIQUE,
    key_prefix VARCHAR(20) NOT NULL,
    key_last4 VARCHAR(8) NOT NULL,
    label VARCHAR(120),
    created_by VARCHAR(120),
    created_at TIMESTAMPTZ NOT NULL,
    expires_at TIMESTAMPTZ,
    revoked_at TIMESTAMPTZ,
    active BOOLEAN NOT NULL DEFAULT TRUE
);
CREATE INDEX IF NOT EXISTS idx_tenant_api_keys_tenant_active ON tenant_api_keys(tenant_id, active, created_at DESC);

CREATE TABLE IF NOT EXISTS tenant_pair_strategies (
    id BIGSERIAL PRIMARY KEY,
    tenant_id UUID NOT NULL REFERENCES tenants(id),
    exchange_name VARCHAR(20) NOT NULL,
    product_id VARCHAR(20) NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,

    spacing_bps NUMERIC(10, 4) NOT NULL,
    rebalance_threshold_bps NUMERIC(10, 4) NOT NULL,
    grid_levels INTEGER NOT NULL,
    level_size_quote NUMERIC(20, 10) NOT NULL,
    max_inventory_ratio NUMERIC(10, 4) NOT NULL,
    maker_fee_rate NUMERIC(10, 6) NOT NULL,
    stale_reprice_threshold_bps NUMERIC(10, 4) NOT NULL,
    stale_order_age_seconds INTEGER NOT NULL,
    rebalance_defer_seconds INTEGER NOT NULL DEFAULT 90,
    rebalance_defer_max_drift_bps NUMERIC(10, 4) NOT NULL DEFAULT 200,
    total_wallet_usd NUMERIC(20, 10) NOT NULL DEFAULT 200,
    session_capital_usd NUMERIC(20, 10) NOT NULL DEFAULT 100,
    maker_only BOOLEAN NOT NULL DEFAULT TRUE,
    paper_mode BOOLEAN NOT NULL DEFAULT TRUE,
    local_timezone_iana VARCHAR(64) NOT NULL DEFAULT 'UTC',
    daily_close_hour INTEGER NOT NULL DEFAULT 0,
    daily_close_minute INTEGER NOT NULL DEFAULT 0,
    spread_freeze_bps NUMERIC(10, 4) NOT NULL DEFAULT 50,
    regime_stress_spread_bps NUMERIC(10, 4) NOT NULL DEFAULT 35,
    regime_trend_slope_threshold NUMERIC(18, 8) NOT NULL DEFAULT 0.0005,
    regime_mr_distance_threshold_bps NUMERIC(10, 4) NOT NULL DEFAULT 18,
    regime_hysteresis_bps NUMERIC(10, 4) NOT NULL DEFAULT 4,
    regime_rsi_bear_threshold NUMERIC(10, 4) NOT NULL DEFAULT 42,
    regime_rsi_bull_threshold NUMERIC(10, 4) NOT NULL DEFAULT 58,
    ws_retry_window_seconds INTEGER NOT NULL DEFAULT 3600,
    ws_initial_retry_delay_seconds INTEGER NOT NULL DEFAULT 5,
    ws_max_retry_delay_seconds INTEGER NOT NULL DEFAULT 60,
    ws_message_timeout_seconds INTEGER NOT NULL DEFAULT 90,
    ws_heartbeat_timeout_seconds INTEGER NOT NULL DEFAULT 30,

    updated_by VARCHAR(50) NOT NULL DEFAULT 'system',
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_tenant_pair_strategies_active ON tenant_pair_strategies(tenant_id, exchange_name, product_id) WHERE is_active IS TRUE;
CREATE INDEX IF NOT EXISTS idx_tenant_pair_strategies_lookup ON tenant_pair_strategies(tenant_id, exchange_name, product_id);

CREATE TABLE IF NOT EXISTS tenant_pair_strategy_history (
    history_id BIGSERIAL PRIMARY KEY,
    strategy_id BIGINT NOT NULL REFERENCES tenant_pair_strategies(id),
    tenant_id UUID NOT NULL REFERENCES tenants(id),
    exchange_name VARCHAR(20) NOT NULL,
    product_id VARCHAR(20) NOT NULL,

    spacing_bps NUMERIC(10, 4) NOT NULL,
    rebalance_threshold_bps NUMERIC(10, 4) NOT NULL,
    grid_levels INTEGER NOT NULL,
    level_size_quote NUMERIC(20, 10) NOT NULL,
    max_inventory_ratio NUMERIC(10, 4) NOT NULL,
    maker_fee_rate NUMERIC(10, 6) NOT NULL,
    stale_reprice_threshold_bps NUMERIC(10, 4) NOT NULL,
    stale_order_age_seconds INTEGER NOT NULL,
    rebalance_defer_seconds INTEGER NOT NULL,
    rebalance_defer_max_drift_bps NUMERIC(10, 4) NOT NULL,
    total_wallet_usd NUMERIC(20, 10) NOT NULL,
    session_capital_usd NUMERIC(20, 10) NOT NULL,
    maker_only BOOLEAN NOT NULL,
    paper_mode BOOLEAN NOT NULL,
    local_timezone_iana VARCHAR(64) NOT NULL,
    daily_close_hour INTEGER NOT NULL,
    daily_close_minute INTEGER NOT NULL,
    spread_freeze_bps NUMERIC(10, 4) NOT NULL,
    regime_stress_spread_bps NUMERIC(10, 4) NOT NULL,
    regime_trend_slope_threshold NUMERIC(18, 8) NOT NULL,
    regime_mr_distance_threshold_bps NUMERIC(10, 4) NOT NULL,
    regime_hysteresis_bps NUMERIC(10, 4) NOT NULL,
    regime_rsi_bear_threshold NUMERIC(10, 4) NOT NULL,
    regime_rsi_bull_threshold NUMERIC(10, 4) NOT NULL,
    ws_retry_window_seconds INTEGER NOT NULL,
    ws_initial_retry_delay_seconds INTEGER NOT NULL,
    ws_max_retry_delay_seconds INTEGER NOT NULL,
    ws_message_timeout_seconds INTEGER NOT NULL,
    ws_heartbeat_timeout_seconds INTEGER NOT NULL,

    valid_from TIMESTAMPTZ NOT NULL,
    valid_to TIMESTAMPTZ,
    updated_by VARCHAR(50) NOT NULL,
    change_summary TEXT
);
CREATE INDEX IF NOT EXISTS idx_tpsh_lookup ON tenant_pair_strategy_history(tenant_id, exchange_name, product_id, valid_from DESC);
CREATE INDEX IF NOT EXISTS idx_tpsh_current ON tenant_pair_strategy_history(strategy_id) WHERE valid_to IS NULL;

CREATE TABLE IF NOT EXISTS parameter_change_audit (
    id BIGSERIAL PRIMARY KEY,
    tenant_id UUID NOT NULL REFERENCES tenants(id),
    exchange_name VARCHAR(20) NOT NULL,
    product_id VARCHAR(20) NOT NULL,
    change_type VARCHAR(20) NOT NULL,
    proposed_by VARCHAR(120) NOT NULL,
    reason TEXT,
    change_payload JSONB NOT NULL,
    change_diff JSONB,
    approved BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_param_change_audit_lookup ON parameter_change_audit(tenant_id, exchange_name, product_id, created_at DESC);

ALTER TABLE sessions ADD COLUMN IF NOT EXISTS tenant_id UUID;
UPDATE sessions SET tenant_id = '00000000-0000-0000-0000-000000000001' WHERE tenant_id IS NULL;
ALTER TABLE sessions ALTER COLUMN tenant_id SET DEFAULT '00000000-0000-0000-0000-000000000001';
ALTER TABLE sessions ALTER COLUMN tenant_id SET NOT NULL;
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_sessions_tenant_id') THEN
        ALTER TABLE sessions ADD CONSTRAINT fk_sessions_tenant_id FOREIGN KEY (tenant_id) REFERENCES tenants(id);
    END IF;
END $$;
CREATE INDEX IF NOT EXISTS idx_sessions_tenant_product ON sessions(tenant_id, product_id);
CREATE INDEX IF NOT EXISTS idx_sessions_tenant_session ON sessions(tenant_id, session_id);
CREATE INDEX IF NOT EXISTS idx_sessions_tenant_started ON sessions(tenant_id, started_at DESC);

ALTER TABLE equity_snapshots ADD COLUMN IF NOT EXISTS tenant_id UUID;
UPDATE equity_snapshots SET tenant_id = '00000000-0000-0000-0000-000000000001' WHERE tenant_id IS NULL;
ALTER TABLE equity_snapshots ALTER COLUMN tenant_id SET DEFAULT '00000000-0000-0000-0000-000000000001';
ALTER TABLE equity_snapshots ALTER COLUMN tenant_id SET NOT NULL;
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_equity_snapshots_tenant_id') THEN
        ALTER TABLE equity_snapshots ADD CONSTRAINT fk_equity_snapshots_tenant_id FOREIGN KEY (tenant_id) REFERENCES tenants(id);
    END IF;
END $$;
CREATE INDEX IF NOT EXISTS idx_equity_tenant_product_time ON equity_snapshots(tenant_id, product_id, recorded_at DESC);
CREATE INDEX IF NOT EXISTS idx_equity_tenant_session_time ON equity_snapshots(tenant_id, session_id, recorded_at DESC);

ALTER TABLE fills ADD COLUMN IF NOT EXISTS tenant_id UUID;
UPDATE fills SET tenant_id = '00000000-0000-0000-0000-000000000001' WHERE tenant_id IS NULL;
ALTER TABLE fills ALTER COLUMN tenant_id SET DEFAULT '00000000-0000-0000-0000-000000000001';
ALTER TABLE fills ALTER COLUMN tenant_id SET NOT NULL;
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_fills_tenant_id') THEN
        ALTER TABLE fills ADD CONSTRAINT fk_fills_tenant_id FOREIGN KEY (tenant_id) REFERENCES tenants(id);
    END IF;
END $$;
CREATE INDEX IF NOT EXISTS idx_fills_tenant_product_time ON fills(tenant_id, product_id, trade_time DESC);
CREATE INDEX IF NOT EXISTS idx_fills_tenant_session_time ON fills(tenant_id, session_id, trade_time DESC);

ALTER TABLE grid_levels ADD COLUMN IF NOT EXISTS tenant_id UUID;
UPDATE grid_levels SET tenant_id = '00000000-0000-0000-0000-000000000001' WHERE tenant_id IS NULL;
ALTER TABLE grid_levels ALTER COLUMN tenant_id SET DEFAULT '00000000-0000-0000-0000-000000000001';
ALTER TABLE grid_levels ALTER COLUMN tenant_id SET NOT NULL;
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_grid_levels_tenant_id') THEN
        ALTER TABLE grid_levels ADD CONSTRAINT fk_grid_levels_tenant_id FOREIGN KEY (tenant_id) REFERENCES tenants(id);
    END IF;
END $$;
CREATE INDEX IF NOT EXISTS idx_grid_levels_tenant_product ON grid_levels(tenant_id, product_id);
CREATE INDEX IF NOT EXISTS idx_grid_levels_tenant_session ON grid_levels(tenant_id, session_id);

ALTER TABLE ticks ADD COLUMN IF NOT EXISTS tenant_id UUID;
UPDATE ticks SET tenant_id = '00000000-0000-0000-0000-000000000001' WHERE tenant_id IS NULL;
ALTER TABLE ticks ALTER COLUMN tenant_id SET DEFAULT '00000000-0000-0000-0000-000000000001';
ALTER TABLE ticks ALTER COLUMN tenant_id SET NOT NULL;
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_ticks_tenant_id') THEN
        ALTER TABLE ticks ADD CONSTRAINT fk_ticks_tenant_id FOREIGN KEY (tenant_id) REFERENCES tenants(id);
    END IF;
END $$;
CREATE INDEX IF NOT EXISTS idx_ticks_tenant_product_time ON ticks(tenant_id, product_id, event_time DESC);

ALTER TABLE bot_restarts ADD COLUMN IF NOT EXISTS tenant_id UUID;
UPDATE bot_restarts SET tenant_id = '00000000-0000-0000-0000-000000000001' WHERE tenant_id IS NULL;
ALTER TABLE bot_restarts ALTER COLUMN tenant_id SET DEFAULT '00000000-0000-0000-0000-000000000001';
ALTER TABLE bot_restarts ALTER COLUMN tenant_id SET NOT NULL;
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_bot_restarts_tenant_id') THEN
        ALTER TABLE bot_restarts ADD CONSTRAINT fk_bot_restarts_tenant_id FOREIGN KEY (tenant_id) REFERENCES tenants(id);
    END IF;
END $$;
CREATE INDEX IF NOT EXISTS idx_bot_restarts_tenant_product_time ON bot_restarts(tenant_id, product_id, restarted_at DESC);

ALTER TABLE worker_process_log ADD COLUMN IF NOT EXISTS tenant_id UUID;
UPDATE worker_process_log SET tenant_id = '00000000-0000-0000-0000-000000000001' WHERE tenant_id IS NULL;
ALTER TABLE worker_process_log ALTER COLUMN tenant_id SET DEFAULT '00000000-0000-0000-0000-000000000001';
ALTER TABLE worker_process_log ALTER COLUMN tenant_id SET NOT NULL;
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_worker_process_log_tenant_id') THEN
        ALTER TABLE worker_process_log ADD CONSTRAINT fk_worker_process_log_tenant_id FOREIGN KEY (tenant_id) REFERENCES tenants(id);
    END IF;
END $$;
CREATE INDEX IF NOT EXISTS idx_worker_process_log_tenant_exchange ON worker_process_log(tenant_id, exchange, started_at DESC);

ALTER TABLE exchange_credentials ADD COLUMN IF NOT EXISTS tenant_id UUID;
UPDATE exchange_credentials SET tenant_id = '00000000-0000-0000-0000-000000000001' WHERE tenant_id IS NULL;
ALTER TABLE exchange_credentials ALTER COLUMN tenant_id SET DEFAULT '00000000-0000-0000-0000-000000000001';
ALTER TABLE exchange_credentials ALTER COLUMN tenant_id SET NOT NULL;
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_exchange_credentials_tenant_id') THEN
        ALTER TABLE exchange_credentials ADD CONSTRAINT fk_exchange_credentials_tenant_id FOREIGN KEY (tenant_id) REFERENCES tenants(id);
    END IF;
END $$;
DROP INDEX IF EXISTS idx_exchange_credentials_unique_active;
CREATE UNIQUE INDEX IF NOT EXISTS idx_exchange_credentials_unique_active_tenant
ON exchange_credentials(tenant_id, exchange_name)
WHERE active IS TRUE;
CREATE INDEX IF NOT EXISTS idx_exchange_credentials_tenant_exchange ON exchange_credentials(tenant_id, exchange_name);

ALTER TABLE users ADD COLUMN IF NOT EXISTS tenant_id UUID;
UPDATE users SET tenant_id = '00000000-0000-0000-0000-000000000001' WHERE tenant_id IS NULL;
ALTER TABLE users ALTER COLUMN tenant_id SET DEFAULT '00000000-0000-0000-0000-000000000001';
ALTER TABLE users ALTER COLUMN tenant_id SET NOT NULL;
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_users_tenant_id') THEN
        ALTER TABLE users ADD CONSTRAINT fk_users_tenant_id FOREIGN KEY (tenant_id) REFERENCES tenants(id);
    END IF;
END $$;
CREATE INDEX IF NOT EXISTS idx_users_tenant_email ON users(tenant_id, email);

ALTER TABLE otp_codes ADD COLUMN IF NOT EXISTS tenant_id UUID;
UPDATE otp_codes SET tenant_id = '00000000-0000-0000-0000-000000000001' WHERE tenant_id IS NULL;
ALTER TABLE otp_codes ALTER COLUMN tenant_id SET DEFAULT '00000000-0000-0000-0000-000000000001';
ALTER TABLE otp_codes ALTER COLUMN tenant_id SET NOT NULL;
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_otp_codes_tenant_id') THEN
        ALTER TABLE otp_codes ADD CONSTRAINT fk_otp_codes_tenant_id FOREIGN KEY (tenant_id) REFERENCES tenants(id);
    END IF;
END $$;
CREATE INDEX IF NOT EXISTS idx_otp_codes_tenant_email ON otp_codes(tenant_id, email);

ALTER TABLE exchange_strategies ADD COLUMN IF NOT EXISTS tenant_id UUID;
UPDATE exchange_strategies SET tenant_id = '00000000-0000-0000-0000-000000000001' WHERE tenant_id IS NULL;
ALTER TABLE exchange_strategies ALTER COLUMN tenant_id SET DEFAULT '00000000-0000-0000-0000-000000000001';
ALTER TABLE exchange_strategies ALTER COLUMN tenant_id SET NOT NULL;
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_exchange_strategies_tenant_id') THEN
        ALTER TABLE exchange_strategies ADD CONSTRAINT fk_exchange_strategies_tenant_id FOREIGN KEY (tenant_id) REFERENCES tenants(id);
    END IF;
END $$;
CREATE INDEX IF NOT EXISTS idx_exchange_strategies_tenant_exchange ON exchange_strategies(tenant_id, exchange_name, updated_at DESC);

ALTER TABLE strategy_param_history ADD COLUMN IF NOT EXISTS tenant_id UUID;
UPDATE strategy_param_history sph
SET tenant_id = es.tenant_id
FROM exchange_strategies es
WHERE sph.strategy_id = es.id AND sph.tenant_id IS NULL;
UPDATE strategy_param_history SET tenant_id = '00000000-0000-0000-0000-000000000001' WHERE tenant_id IS NULL;
ALTER TABLE strategy_param_history ALTER COLUMN tenant_id SET DEFAULT '00000000-0000-0000-0000-000000000001';
ALTER TABLE strategy_param_history ALTER COLUMN tenant_id SET NOT NULL;
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_strategy_param_history_tenant_id') THEN
        ALTER TABLE strategy_param_history ADD CONSTRAINT fk_strategy_param_history_tenant_id FOREIGN KEY (tenant_id) REFERENCES tenants(id);
    END IF;
END $$;
CREATE INDEX IF NOT EXISTS idx_sph_tenant_strategy ON strategy_param_history(tenant_id, strategy_id, valid_from DESC);

INSERT INTO tenant_pair_strategies (
    tenant_id,
    exchange_name,
    product_id,
    is_active,
    spacing_bps,
    rebalance_threshold_bps,
    grid_levels,
    level_size_quote,
    max_inventory_ratio,
    maker_fee_rate,
    stale_reprice_threshold_bps,
    stale_order_age_seconds,
    rebalance_defer_seconds,
    rebalance_defer_max_drift_bps,
    total_wallet_usd,
    session_capital_usd,
    maker_only,
    paper_mode,
    local_timezone_iana,
    daily_close_hour,
    daily_close_minute,
    spread_freeze_bps,
    regime_stress_spread_bps,
    regime_trend_slope_threshold,
    regime_mr_distance_threshold_bps,
    regime_hysteresis_bps,
    regime_rsi_bear_threshold,
    regime_rsi_bull_threshold,
    ws_retry_window_seconds,
    ws_initial_retry_delay_seconds,
    ws_max_retry_delay_seconds,
    ws_message_timeout_seconds,
    ws_heartbeat_timeout_seconds,
    updated_by,
    created_at,
    updated_at
)
SELECT
    es.tenant_id,
    es.exchange_name,
    TRIM(sym.symbol) AS product_id,
    es.is_active,
    COALESCE((es.symbol_overrides -> TRIM(sym.symbol) ->> 'spacing_bps')::NUMERIC, es.spacing_bps),
    COALESCE((es.symbol_overrides -> TRIM(sym.symbol) ->> 'rebalance_threshold_bps')::NUMERIC, es.rebalance_threshold_bps),
    COALESCE((es.symbol_overrides -> TRIM(sym.symbol) ->> 'grid_levels')::INTEGER, es.grid_levels),
    COALESCE((es.symbol_overrides -> TRIM(sym.symbol) ->> 'level_size_quote')::NUMERIC, es.level_size_quote),
    COALESCE((es.symbol_overrides -> TRIM(sym.symbol) ->> 'max_inventory_ratio')::NUMERIC, es.max_inventory_ratio),
    es.maker_fee_rate,
    COALESCE((es.symbol_overrides -> TRIM(sym.symbol) ->> 'stale_reprice_threshold_bps')::NUMERIC, es.stale_reprice_threshold_bps),
    COALESCE((es.symbol_overrides -> TRIM(sym.symbol) ->> 'stale_order_age_seconds')::INTEGER, es.stale_order_age_seconds),
    COALESCE((es.symbol_overrides -> TRIM(sym.symbol) ->> 'rebalance_defer_seconds')::INTEGER, es.rebalance_defer_seconds),
    COALESCE((es.symbol_overrides -> TRIM(sym.symbol) ->> 'rebalance_defer_max_drift_bps')::NUMERIC, es.rebalance_defer_max_drift_bps),
    es.total_wallet_usd,
    COALESCE((es.symbol_overrides -> TRIM(sym.symbol) ->> 'session_capital_usd')::NUMERIC, es.session_capital_usd),
    COALESCE((es.symbol_overrides -> TRIM(sym.symbol) ->> 'maker_only')::BOOLEAN, es.maker_only),
    es.paper_mode,
    es.local_timezone_iana,
    es.daily_close_hour,
    es.daily_close_minute,
    es.spread_freeze_bps,
    es.regime_stress_spread_bps,
    es.regime_trend_slope_threshold,
    es.regime_mr_distance_threshold_bps,
    es.regime_hysteresis_bps,
    es.regime_rsi_bear_threshold,
    es.regime_rsi_bull_threshold,
    es.ws_retry_window_seconds,
    es.ws_initial_retry_delay_seconds,
    es.ws_max_retry_delay_seconds,
    es.ws_message_timeout_seconds,
    es.ws_heartbeat_timeout_seconds,
    es.updated_by,
    es.created_at,
    es.updated_at
FROM exchange_strategies es
CROSS JOIN LATERAL regexp_split_to_table(COALESCE(es.symbols, ''), ',') AS sym(symbol)
WHERE TRIM(sym.symbol) <> ''
ON CONFLICT DO NOTHING;

INSERT INTO tenant_pair_strategy_history (
    strategy_id,
    tenant_id,
    exchange_name,
    product_id,
    spacing_bps,
    rebalance_threshold_bps,
    grid_levels,
    level_size_quote,
    max_inventory_ratio,
    maker_fee_rate,
    stale_reprice_threshold_bps,
    stale_order_age_seconds,
    rebalance_defer_seconds,
    rebalance_defer_max_drift_bps,
    total_wallet_usd,
    session_capital_usd,
    maker_only,
    paper_mode,
    local_timezone_iana,
    daily_close_hour,
    daily_close_minute,
    spread_freeze_bps,
    regime_stress_spread_bps,
    regime_trend_slope_threshold,
    regime_mr_distance_threshold_bps,
    regime_hysteresis_bps,
    regime_rsi_bear_threshold,
    regime_rsi_bull_threshold,
    ws_retry_window_seconds,
    ws_initial_retry_delay_seconds,
    ws_max_retry_delay_seconds,
    ws_message_timeout_seconds,
    ws_heartbeat_timeout_seconds,
    valid_from,
    valid_to,
    updated_by,
    change_summary
)
SELECT
    tps.id,
    tps.tenant_id,
    tps.exchange_name,
    tps.product_id,
    tps.spacing_bps,
    tps.rebalance_threshold_bps,
    tps.grid_levels,
    tps.level_size_quote,
    tps.max_inventory_ratio,
    tps.maker_fee_rate,
    tps.stale_reprice_threshold_bps,
    tps.stale_order_age_seconds,
    tps.rebalance_defer_seconds,
    tps.rebalance_defer_max_drift_bps,
    tps.total_wallet_usd,
    tps.session_capital_usd,
    tps.maker_only,
    tps.paper_mode,
    tps.local_timezone_iana,
    tps.daily_close_hour,
    tps.daily_close_minute,
    tps.spread_freeze_bps,
    tps.regime_stress_spread_bps,
    tps.regime_trend_slope_threshold,
    tps.regime_mr_distance_threshold_bps,
    tps.regime_hysteresis_bps,
    tps.regime_rsi_bear_threshold,
    tps.regime_rsi_bull_threshold,
    tps.ws_retry_window_seconds,
    tps.ws_initial_retry_delay_seconds,
    tps.ws_max_retry_delay_seconds,
    tps.ws_message_timeout_seconds,
    tps.ws_heartbeat_timeout_seconds,
    tps.updated_at,
    NULL,
    tps.updated_by,
    'seeded from legacy exchange_strategies'
FROM tenant_pair_strategies tps
WHERE NOT EXISTS (
    SELECT 1 FROM tenant_pair_strategy_history h WHERE h.strategy_id = tps.id
);

CREATE OR REPLACE VIEW v_daily_pnl_tenant AS
SELECT
    f.tenant_id,
    DATE_TRUNC('day', f.trade_time AT TIME ZONE 'UTC') AS trade_date,
    f.product_id,
    COUNT(*) AS fills,
    SUM(CASE WHEN f.side = 'SELL' THEN f.quote_value ELSE -f.quote_value END) AS net_flow_usd,
    SUM(f.fee_quote) AS total_fees,
    MIN(f.price) AS low_price,
    MAX(f.price) AS high_price,
    AVG(f.price) AS avg_price
FROM fills f
GROUP BY f.tenant_id, DATE_TRUNC('day', f.trade_time AT TIME ZONE 'UTC'), f.product_id;

CREATE OR REPLACE VIEW v_session_pnl_tenant AS
SELECT
    s.tenant_id,
    s.session_id,
    s.product_id,
    s.mode,
    s.status,
    s.mid_anchor,
    s.spacing_bps,
    s.grid_levels,
    s.realized_pnl_quote,
    s.total_fills,
    COUNT(f.fill_id) AS fill_count,
    SUM(f.quote_value) AS total_volume_usd,
    SUM(f.fee_quote) AS total_fees_paid,
    SUM(CASE WHEN f.side = 'BUY' THEN f.quote_value ELSE 0 END) AS total_buy_value,
    SUM(CASE WHEN f.side = 'SELL' THEN f.quote_value ELSE 0 END) AS total_sell_value,
    s.started_at,
    s.ended_at,
    EXTRACT(EPOCH FROM (COALESCE(s.ended_at, NOW()) - s.started_at)) AS duration_seconds
FROM sessions s
LEFT JOIN fills f
    ON f.session_id = s.session_id
   AND f.tenant_id = s.tenant_id
GROUP BY s.tenant_id, s.session_id;
