-- TradingBotV3 initial schema
-- Neutral grid market-making bot

CREATE TABLE IF NOT EXISTS sessions (
    session_id      UUID PRIMARY KEY,
    product_id      VARCHAR(20)  NOT NULL,
    mode            VARCHAR(10)  NOT NULL DEFAULT 'paper',   -- paper | live
    status          VARCHAR(20)  NOT NULL DEFAULT 'active',  -- active | closed
    mid_anchor      NUMERIC(20,10) NOT NULL,
    spacing_bps     NUMERIC(10,4)  NOT NULL,
    grid_levels     INTEGER      NOT NULL,
    realized_pnl_quote NUMERIC(20,10) NOT NULL DEFAULT 0,
    total_fills     INTEGER      NOT NULL DEFAULT 0,
    started_at      TIMESTAMPTZ  NOT NULL,
    ended_at        TIMESTAMPTZ,
    updated_at      TIMESTAMPTZ  NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_sessions_product_id ON sessions(product_id);
CREATE INDEX IF NOT EXISTS idx_sessions_status     ON sessions(status);


CREATE TABLE IF NOT EXISTS grid_levels (
    level_id        UUID PRIMARY KEY,
    product_id      VARCHAR(20)  NOT NULL,
    session_id      UUID         NOT NULL REFERENCES sessions(session_id) ON DELETE CASCADE,
    side            VARCHAR(4)   NOT NULL,   -- BUY | SELL
    level_index     INTEGER      NOT NULL,
    price           NUMERIC(20,10) NOT NULL,
    size_base       NUMERIC(20,10) NOT NULL,
    size_quote      NUMERIC(20,10) NOT NULL,
    client_order_id VARCHAR(100),
    order_id        VARCHAR(100),
    status          VARCHAR(20)  NOT NULL DEFAULT 'pending',  -- pending | open | filled | cancelled
    fill_price      NUMERIC(20,10),
    fill_fee_quote  NUMERIC(20,10),
    created_at      TIMESTAMPTZ  NOT NULL,
    updated_at      TIMESTAMPTZ  NOT NULL,
    filled_at       TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_grid_levels_session    ON grid_levels(session_id);
CREATE INDEX IF NOT EXISTS idx_grid_levels_product    ON grid_levels(product_id);
CREATE INDEX IF NOT EXISTS idx_grid_levels_status     ON grid_levels(status);
CREATE INDEX IF NOT EXISTS idx_grid_levels_order_id   ON grid_levels(client_order_id);


CREATE TABLE IF NOT EXISTS fills (
    fill_id              VARCHAR(80)   PRIMARY KEY,
    order_id             VARCHAR(100),
    client_order_id      VARCHAR(100)  NOT NULL,
    product_id           VARCHAR(20)   NOT NULL,
    session_id           UUID          REFERENCES sessions(session_id) ON DELETE SET NULL,
    side                 VARCHAR(4)    NOT NULL,
    price                NUMERIC(20,10) NOT NULL,
    size_base            NUMERIC(20,10) NOT NULL,
    quote_value          NUMERIC(20,10) NOT NULL,
    fee_quote            NUMERIC(20,10) NOT NULL,
    level_index          INTEGER,
    grid_side            VARCHAR(4),   -- bid | ask
    liquidity_indicator  VARCHAR(10)   NOT NULL DEFAULT 'M',
    trade_time           TIMESTAMPTZ   NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_fills_product_id   ON fills(product_id);
CREATE INDEX IF NOT EXISTS idx_fills_session_id   ON fills(session_id);
CREATE INDEX IF NOT EXISTS idx_fills_trade_time   ON fills(trade_time);
CREATE INDEX IF NOT EXISTS idx_fills_order_id     ON fills(client_order_id);


-- Performance analytics view
CREATE OR REPLACE VIEW v_session_pnl AS
SELECT
    s.session_id,
    s.product_id,
    s.mode,
    s.status,
    s.mid_anchor,
    s.spacing_bps,
    s.grid_levels,
    s.realized_pnl_quote,
    s.total_fills,
    COUNT(f.fill_id)                              AS fill_count,
    SUM(f.quote_value)                            AS total_volume_usd,
    SUM(f.fee_quote)                              AS total_fees_paid,
    SUM(CASE WHEN f.side = 'BUY'  THEN f.quote_value ELSE 0 END) AS total_buy_value,
    SUM(CASE WHEN f.side = 'SELL' THEN f.quote_value ELSE 0 END) AS total_sell_value,
    s.started_at,
    s.ended_at,
    EXTRACT(EPOCH FROM (COALESCE(s.ended_at, NOW()) - s.started_at)) AS duration_seconds
FROM sessions s
LEFT JOIN fills f ON f.session_id = s.session_id
GROUP BY s.session_id;


-- Daily PnL view
CREATE OR REPLACE VIEW v_daily_pnl AS
SELECT
    DATE_TRUNC('day', f.trade_time AT TIME ZONE 'UTC') AS trade_date,
    f.product_id,
    COUNT(*)                                            AS fills,
    SUM(CASE WHEN f.side = 'SELL' THEN f.quote_value ELSE -f.quote_value END)
                                                        AS net_flow_usd,
    SUM(f.fee_quote)                                    AS total_fees,
    MIN(f.price)                                        AS low_price,
    MAX(f.price)                                        AS high_price,
    AVG(f.price)                                        AS avg_price
FROM fills f
GROUP BY 1, 2
ORDER BY 1 DESC, 2;
