-- V004: Track every bot session reset with who triggered it
CREATE TABLE IF NOT EXISTS bot_restarts (
    id          BIGSERIAL PRIMARY KEY,
    session_id  UUID          NOT NULL,   -- new session created by the restart
    product_id  VARCHAR(20)   NOT NULL,
    triggered_by VARCHAR(50)  NOT NULL,   -- 'Abraham' | 'Claude' | 'daily_close'
    restarted_at TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_bot_restarts_product_time
    ON bot_restarts (product_id, restarted_at DESC);
