-- Track each worker process startup with its exact start time.
-- Allows reconstructing worker uptime history and correlating fills/events
-- with the worker process that generated them.

CREATE TABLE IF NOT EXISTS worker_process_log (
    id           BIGSERIAL PRIMARY KEY,
    exchange     VARCHAR(20) NOT NULL,
    started_at   TIMESTAMP WITH TIME ZONE NOT NULL,
    pid          INTEGER,          -- OS process ID at startup (informational)
    stopped_at   TIMESTAMP WITH TIME ZONE,
    stop_reason  VARCHAR(50)       -- 'graceful' | 'crash' | 'signal' | NULL=still running
);

CREATE INDEX IF NOT EXISTS idx_worker_process_log_exchange ON worker_process_log(exchange, started_at DESC);

COMMENT ON TABLE worker_process_log IS
    'One row per worker process lifecycle. stopped_at/stop_reason filled on graceful shutdown.';
