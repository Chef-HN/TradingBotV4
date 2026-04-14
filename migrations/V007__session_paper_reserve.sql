-- Persist the reserve ("caja") so it survives worker restarts.
-- Works for both paper and live modes.
-- Default 0 is correct: existing sessions never tracked this value.
ALTER TABLE sessions
    ADD COLUMN IF NOT EXISTS reserve_usd NUMERIC(20, 10) NOT NULL DEFAULT 0;
