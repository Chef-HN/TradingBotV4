use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use tokio_postgres::Client;

use crate::kernel::types::{MarketTick, StrategyConfig};

use super::MarketDataProvider;

const DEFAULT_MARKET_DB_DSN: &str = "postgresql://tradingbot:tradingbot@localhost:5433/tradingbotv3";
const DEFAULT_MARKET_DB_TABLE: &str = "ticks";
const START_MODE_LATEST: &str = "latest";
const START_MODE_OLDEST: &str = "oldest";

pub struct PostgresTailMarketDataProvider {
    tenant_id: String,
    exchange: String,
    product_id: String,
    poll_interval: Duration,
    db_dsn: String,
    table_name: String,
    start_mode: String,
    client: Option<Client>,
    cursor_initialized: bool,
    last_seen_tick_id: Option<i64>,
}

impl PostgresTailMarketDataProvider {
    pub fn from_strategy(config: &StrategyConfig, poll_interval_ms: u64) -> Result<Self> {
        let raw_dsn = std::env::var("TB_MARKET_DATA_DB_DSN")
            .or_else(|_| std::env::var("TB_V3_DB_DSN"))
            .unwrap_or_else(|_| DEFAULT_MARKET_DB_DSN.to_string());
        let table_name = sanitize_table_name(
            &std::env::var("TB_MARKET_DATA_DB_TABLE")
                .unwrap_or_else(|_| DEFAULT_MARKET_DB_TABLE.to_string()),
        )?;

        let start_mode = std::env::var("TB_MARKET_DATA_DB_START_MODE")
            .unwrap_or_else(|_| START_MODE_LATEST.to_string())
            .trim()
            .to_lowercase();

        if start_mode != START_MODE_LATEST && start_mode != START_MODE_OLDEST {
            return Err(anyhow!(
                "unsupported TB_MARKET_DATA_DB_START_MODE='{start_mode}'. expected '{START_MODE_LATEST}' or '{START_MODE_OLDEST}'"
            ));
        }

        Ok(Self {
            tenant_id: config.tenant_id.clone(),
            exchange: config.exchange.clone(),
            product_id: config.product_id.clone(),
            poll_interval: Duration::from_millis(poll_interval_ms.max(100)),
            db_dsn: normalize_postgres_dsn(raw_dsn),
            table_name,
            start_mode,
            client: None,
            cursor_initialized: false,
            last_seen_tick_id: None,
        })
    }

    async fn ensure_connected(&mut self) -> Result<()> {
        if self.client.is_some() {
            return Ok(());
        }

        let (client, connection) = tokio_postgres::connect(&self.db_dsn, tokio_postgres::NoTls)
            .await
            .with_context(|| {
                format!(
                    "failed to connect market data postgres with dsn '{}'",
                    self.db_dsn
                )
            })?;

        tokio::spawn(async move {
            if let Err(err) = connection.await {
                eprintln!("market-data postgres connection task ended: {err}");
            }
        });

        self.client = Some(client);
        self.cursor_initialized = false;
        Ok(())
    }

    async fn initialize_cursor_if_needed(&mut self) -> Result<()> {
        if self.cursor_initialized {
            return Ok(());
        }

        if self.start_mode == START_MODE_LATEST {
            let query = format!(
                "SELECT MAX(id)::bigint AS max_id FROM {} WHERE product_id = $1",
                self.table_name
            );
            let client = self
                .client
                .as_ref()
                .ok_or_else(|| anyhow!("market data postgres client is not connected"))?;
            let row = client
                .query_one(&query, &[&self.product_id])
                .await
                .with_context(|| {
                    format!(
                        "failed to initialize cursor for product='{}' from table='{}'",
                        self.product_id, self.table_name
                    )
                })?;
            let max_id: Option<i64> = row.get("max_id");
            self.last_seen_tick_id = max_id;
        }

        self.cursor_initialized = true;
        Ok(())
    }

    async fn fetch_next_tick(&self) -> Result<Option<(i64, MarketTick)>> {
        let query = format!(
            r#"
            SELECT
                id::bigint AS id,
                bid::float8 AS bid,
                ask::float8 AS ask,
                mid::float8 AS mid,
                event_time
            FROM {}
            WHERE product_id = $1
              AND ($2::bigint IS NULL OR id > $2)
            ORDER BY id ASC
            LIMIT 1
            "#,
            self.table_name
        );

        let client = self
            .client
            .as_ref()
            .ok_or_else(|| anyhow!("market data postgres client is not connected"))?;

        let row_opt = client
            .query_opt(&query, &[&self.product_id, &self.last_seen_tick_id])
            .await
            .with_context(|| {
                format!(
                    "query failed for postgres_tail provider table='{}' product='{}'",
                    self.table_name, self.product_id
                )
            })?;

        let Some(row) = row_opt else {
            return Ok(None);
        };

        let id: i64 = row.get("id");
        let bid: f64 = row.get("bid");
        let ask: f64 = row.get("ask");
        let mut mid: f64 = row.get("mid");
        if mid <= 0.0 && ask >= bid && bid > 0.0 {
            mid = (bid + ask) / 2.0;
        }
        if bid <= 0.0 || ask <= 0.0 || mid <= 0.0 {
            return Err(anyhow!(
                "invalid tick row from table='{}' id={} product='{}' bid={} ask={} mid={}",
                self.table_name,
                id,
                self.product_id,
                bid,
                ask,
                mid
            ));
        }

        let event_time: DateTime<Utc> = row.get("event_time");
        let event_ts_ms = event_time.timestamp_millis();
        let tick = MarketTick {
            tenant_id: self.tenant_id.clone(),
            exchange: self.exchange.clone(),
            product_id: self.product_id.clone(),
            bid,
            ask,
            mid,
            event_ts_ms: if event_ts_ms > 0 {
                event_ts_ms
            } else {
                now_ms()
            },
        };

        Ok(Some((id, tick)))
    }
}

#[async_trait]
impl MarketDataProvider for PostgresTailMarketDataProvider {
    async fn next_tick(&mut self) -> Result<MarketTick> {
        loop {
            self.ensure_connected().await?;
            self.initialize_cursor_if_needed().await?;

            let next = match self.fetch_next_tick().await {
                Ok(v) => v,
                Err(err) => {
                    self.client = None;
                    tokio::time::sleep(self.poll_interval).await;
                    return Err(err);
                }
            };

            if let Some((tick_id, tick)) = next {
                self.last_seen_tick_id = Some(tick_id);
                return Ok(tick);
            }

            tokio::time::sleep(self.poll_interval).await;
        }
    }
}

fn sanitize_table_name(raw: &str) -> Result<String> {
    let candidate = raw.trim();
    if candidate.is_empty() {
        return Err(anyhow!("TB_MARKET_DATA_DB_TABLE must not be empty"));
    }

    for part in candidate.split('.') {
        if part.is_empty() {
            return Err(anyhow!("invalid table identifier '{}'", candidate));
        }
        let mut chars = part.chars();
        let Some(first) = chars.next() else {
            return Err(anyhow!("invalid table identifier '{}'", candidate));
        };
        if !(first == '_' || first.is_ascii_alphabetic()) {
            return Err(anyhow!("invalid table identifier '{}'", candidate));
        }
        if !chars.all(|c| c == '_' || c.is_ascii_alphanumeric()) {
            return Err(anyhow!("invalid table identifier '{}'", candidate));
        }
    }

    Ok(candidate.to_string())
}

fn normalize_postgres_dsn(raw: String) -> String {
    raw.replace("postgresql+asyncpg://", "postgresql://")
}

fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};

    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanitize_table_name_accepts_safe_identifiers() {
        assert_eq!(sanitize_table_name("ticks").expect("ok"), "ticks");
        assert_eq!(
            sanitize_table_name("public.ticks").expect("ok"),
            "public.ticks"
        );
        assert_eq!(
            sanitize_table_name("_custom_123.table_2").expect("ok"),
            "_custom_123.table_2"
        );
    }

    #[test]
    fn sanitize_table_name_rejects_unsafe_identifiers() {
        assert!(sanitize_table_name("").is_err());
        assert!(sanitize_table_name("ticks;drop table ticks").is_err());
        assert!(sanitize_table_name("1ticks").is_err());
        assert!(sanitize_table_name("public.\"ticks\"").is_err());
        assert!(sanitize_table_name("ticks where 1=1").is_err());
    }
}
