-- SCD Type 2 history for exchange_strategies + updated_by audit column

-- 1. Add updated_by to the live table
ALTER TABLE exchange_strategies ADD COLUMN IF NOT EXISTS updated_by VARCHAR(50) NOT NULL DEFAULT 'system';

-- 2. History table (SCD Type 2): each row = one version of params
CREATE TABLE IF NOT EXISTS strategy_param_history (
    history_id   SERIAL PRIMARY KEY,
    strategy_id  INTEGER NOT NULL REFERENCES exchange_strategies(id),
    strategy_name VARCHAR(100) NOT NULL,
    exchange_name VARCHAR(20) NOT NULL,

    -- Snapshot of ALL params at this version
    spacing_bps              NUMERIC(10,4) NOT NULL,
    rebalance_threshold_bps  NUMERIC(10,4) NOT NULL,
    grid_levels              INTEGER NOT NULL,
    level_size_quote         NUMERIC(20,10) NOT NULL,
    max_inventory_ratio      NUMERIC(10,4) NOT NULL,
    maker_fee_rate           NUMERIC(10,6) NOT NULL,
    stale_reprice_threshold_bps NUMERIC(10,4) NOT NULL,
    stale_order_age_seconds  INTEGER NOT NULL,
    rebalance_defer_seconds  INTEGER NOT NULL,
    rebalance_defer_max_drift_bps NUMERIC(10,4) NOT NULL,
    total_wallet_usd         NUMERIC(20,10) NOT NULL,
    session_capital_usd      NUMERIC(20,10) NOT NULL,
    maker_only               BOOLEAN NOT NULL,
    paper_mode               BOOLEAN NOT NULL,
    symbols                  VARCHAR(500) NOT NULL,
    symbol_overrides         JSONB,

    -- SCD2 fields
    valid_from   TIMESTAMPTZ NOT NULL,
    valid_to     TIMESTAMPTZ,            -- NULL = current version
    updated_by   VARCHAR(50) NOT NULL,
    change_summary TEXT                   -- human-readable diff of what changed
);

CREATE INDEX IF NOT EXISTS idx_sph_strategy_id ON strategy_param_history(strategy_id);
CREATE INDEX IF NOT EXISTS idx_sph_valid_from ON strategy_param_history(valid_from);
CREATE INDEX IF NOT EXISTS idx_sph_current ON strategy_param_history(strategy_id) WHERE valid_to IS NULL;
