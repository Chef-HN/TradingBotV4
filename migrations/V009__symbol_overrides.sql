-- Per-symbol parameter overrides for exchange_strategies.
-- Stored as JSONB: {"SOL-USD": {"spacing_bps": 20}, "DOGE-USD": {"spacing_bps": 15, "rebalance_threshold_bps": 50}}
-- NULL means no overrides — all symbols use the base row values.
ALTER TABLE exchange_strategies
    ADD COLUMN IF NOT EXISTS symbol_overrides JSONB NULL DEFAULT NULL;
