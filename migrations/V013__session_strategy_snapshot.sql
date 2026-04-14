-- Add strategy parameter snapshot to sessions so fills can always be traced
-- back to the exact parameters that were active when they occurred.
-- Existing sessions get NULLs (unknown) which is correct — data was not captured.

ALTER TABLE sessions
    ADD COLUMN IF NOT EXISTS level_size_quote        NUMERIC(20, 10),
    ADD COLUMN IF NOT EXISTS rebalance_threshold_bps NUMERIC(10, 4),
    ADD COLUMN IF NOT EXISTS max_inventory_ratio     NUMERIC(6, 4),
    ADD COLUMN IF NOT EXISTS maker_fee_rate          NUMERIC(10, 6),
    ADD COLUMN IF NOT EXISTS symbol_overrides        JSONB;

COMMENT ON COLUMN sessions.level_size_quote        IS 'USD size per grid level at session start';
COMMENT ON COLUMN sessions.rebalance_threshold_bps IS 'Mid drift threshold to trigger full rebalance';
COMMENT ON COLUMN sessions.max_inventory_ratio     IS 'Max fraction of portfolio in base inventory';
COMMENT ON COLUMN sessions.maker_fee_rate          IS 'Maker fee rate applied (e.g. 0.001 = 0.1%)';
COMMENT ON COLUMN sessions.symbol_overrides        IS 'Per-symbol overrides applied at session start (JSON snapshot)';
