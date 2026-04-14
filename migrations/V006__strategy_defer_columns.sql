-- Add rebalance deferral columns to exchange_strategies.
-- These control how long after a fill the engine waits before rebalancing,
-- and the emergency drift threshold that overrides the deferral.

ALTER TABLE exchange_strategies
    ADD COLUMN IF NOT EXISTS rebalance_defer_seconds        INTEGER      NOT NULL DEFAULT 90,
    ADD COLUMN IF NOT EXISTS rebalance_defer_max_drift_bps  NUMERIC(10,4) NOT NULL DEFAULT 200;

-- Also add symbols column so each DB strategy row defines which symbols it trades.
-- This replaces the STRATEGY_SYMBOLS env var.
ALTER TABLE exchange_strategies
    ADD COLUMN IF NOT EXISTS symbols TEXT NOT NULL DEFAULT 'BTC-USD';

-- Also add paper_mode so it's per-strategy in DB.
ALTER TABLE exchange_strategies
    ADD COLUMN IF NOT EXISTS paper_mode BOOLEAN NOT NULL DEFAULT true;
