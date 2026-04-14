-- Exchange-specific strategy configurations.
-- One active strategy per exchange at a time (is_active = true).
-- Name is a human-readable slug: e.g. "bybit-tight-40bps", "coinbase-default".

CREATE TABLE IF NOT EXISTS exchange_strategies (
    id                          SERIAL PRIMARY KEY,
    name                        VARCHAR(100) NOT NULL UNIQUE,
    exchange_name               VARCHAR(20)  NOT NULL,
    is_active                   BOOLEAN      NOT NULL DEFAULT true,

    spacing_bps                 NUMERIC(10, 4)  NOT NULL,
    rebalance_threshold_bps     NUMERIC(10, 4)  NOT NULL,
    grid_levels                 INTEGER         NOT NULL,
    level_size_quote            NUMERIC(20, 10) NOT NULL,
    max_inventory_ratio         NUMERIC(10, 4)  NOT NULL,
    maker_fee_rate              NUMERIC(10, 6)  NOT NULL,
    stale_reprice_threshold_bps NUMERIC(10, 4)  NOT NULL,
    stale_order_age_seconds     INTEGER         NOT NULL,

    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_exchange_strategies_exchange
    ON exchange_strategies (exchange_name);

-- Seed default strategies (adjust values to match current .env)
INSERT INTO exchange_strategies (
    name, exchange_name, is_active,
    spacing_bps, rebalance_threshold_bps, grid_levels,
    level_size_quote, max_inventory_ratio, maker_fee_rate,
    stale_reprice_threshold_bps, stale_order_age_seconds
) VALUES
    ('coinbase-default', 'coinbase', true,
     130, 200, 3,
     10, 0.6, 0.006,
     5, 120),
    ('bybit-default', 'bybit', true,
     40, 80, 3,
     10, 0.6, 0.001,
     5, 120)
ON CONFLICT (name) DO NOTHING;
