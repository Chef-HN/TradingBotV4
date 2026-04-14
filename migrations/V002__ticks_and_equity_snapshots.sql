-- V002: Market tick history and equity snapshots
-- ticks: every WebSocket ticker update per symbol
-- equity_snapshots: equity state recorded each time it changes (fill, rebalance, init)

CREATE TABLE IF NOT EXISTS ticks (
    id               BIGSERIAL     PRIMARY KEY,
    product_id       VARCHAR(20)   NOT NULL,
    bid              NUMERIC(20,10) NOT NULL,
    ask              NUMERIC(20,10) NOT NULL,
    mid              NUMERIC(20,10) NOT NULL,
    last_trade_price NUMERIC(20,10) NOT NULL,
    event_time       TIMESTAMPTZ   NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_ticks_product_time ON ticks(product_id, event_time DESC);


CREATE TABLE IF NOT EXISTS equity_snapshots (
    id              BIGSERIAL      PRIMARY KEY,
    session_id      UUID           NOT NULL REFERENCES sessions(session_id) ON DELETE CASCADE,
    product_id      VARCHAR(20)    NOT NULL,
    total_equity    NUMERIC(20,10) NOT NULL,
    quote_inventory NUMERIC(20,10) NOT NULL,
    base_inventory  NUMERIC(20,10) NOT NULL,
    realized_pnl    NUMERIC(20,10) NOT NULL,
    unrealized_pnl  NUMERIC(20,10) NOT NULL,
    mid_anchor      NUMERIC(20,10) NOT NULL,
    mid_price       NUMERIC(20,10) NOT NULL,
    trigger         VARCHAR(20)    NOT NULL,   -- init | fill | rebalance
    recorded_at     TIMESTAMPTZ    NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_equity_session_time ON equity_snapshots(session_id, recorded_at DESC);
CREATE INDEX IF NOT EXISTS idx_equity_product_time ON equity_snapshots(product_id, recorded_at DESC);
