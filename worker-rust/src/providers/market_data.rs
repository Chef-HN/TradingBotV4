use std::{collections::VecDeque, fs, time::Duration};

use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;

use crate::kernel::types::{MarketTick, StrategyConfig};

use super::MarketDataProvider;

pub struct ReplayMarketDataProvider {
    queue: VecDeque<MarketTick>,
}

impl ReplayMarketDataProvider {
    pub fn new(ticks: Vec<MarketTick>) -> Self {
        Self {
            queue: ticks.into_iter().collect(),
        }
    }

    pub fn from_path(path: &str) -> Result<Self> {
        let raw = fs::read_to_string(path)
            .with_context(|| format!("failed to read replay file '{path}'"))?;
        let ticks = parse_ticks_document(&raw)
            .with_context(|| format!("failed to parse replay ticks from '{path}'"))?;
        if ticks.is_empty() {
            return Err(anyhow!("replay file '{path}' has no ticks"));
        }
        Ok(Self::new(ticks))
    }
}

#[async_trait]
impl MarketDataProvider for ReplayMarketDataProvider {
    async fn next_tick(&mut self) -> Result<MarketTick> {
        self.queue
            .pop_front()
            .ok_or_else(|| anyhow!("replay stream exhausted"))
    }
}

pub struct SyntheticMarketDataProvider {
    tenant_id: String,
    exchange: String,
    product_id: String,
    base_mid: f64,
    spread_bps: f64,
    drift_step: i64,
    tick_interval: Duration,
}

impl SyntheticMarketDataProvider {
    pub fn from_strategy(config: &StrategyConfig, tick_interval_ms: u64) -> Self {
        Self {
            tenant_id: config.tenant_id.clone(),
            exchange: config.exchange.clone(),
            product_id: config.product_id.clone(),
            base_mid: 100.0,
            spread_bps: 8.0,
            drift_step: 0,
            tick_interval: Duration::from_millis(tick_interval_ms.max(100)),
        }
    }
}

#[async_trait]
impl MarketDataProvider for SyntheticMarketDataProvider {
    async fn next_tick(&mut self) -> Result<MarketTick> {
        tokio::time::sleep(self.tick_interval).await;
        self.drift_step += 1;

        let wave = ((self.drift_step % 20) - 10) as f64;
        let mid = self.base_mid + (wave * 0.18);
        let spread = mid * (self.spread_bps / 10_000.0);

        Ok(MarketTick {
            tenant_id: self.tenant_id.clone(),
            exchange: self.exchange.clone(),
            product_id: self.product_id.clone(),
            bid: mid - spread / 2.0,
            ask: mid + spread / 2.0,
            mid,
            event_ts_ms: now_ms(),
        })
    }
}

fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};

    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or_default()
}

fn parse_ticks_document(raw: &str) -> Result<Vec<MarketTick>> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(Vec::new());
    }

    if trimmed.starts_with('[') {
        return serde_json::from_str::<Vec<MarketTick>>(trimmed)
            .context("expected JSON array of MarketTick");
    }

    let mut ticks = Vec::new();
    for (index, line) in raw.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let tick = serde_json::from_str::<MarketTick>(line)
            .with_context(|| format!("invalid json line {}", index + 1))?;
        ticks.push(tick);
    }
    Ok(ticks)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_ticks_accepts_json_array() {
        let ticks = parse_ticks_document(
            r#"[{"tenant_id":"t1","exchange":"bybit","product_id":"SOL-USD","bid":100.0,"ask":100.2,"mid":100.1,"event_ts_ms":1}]"#,
        )
        .expect("parsed");
        assert_eq!(ticks.len(), 1);
        assert_eq!(ticks[0].product_id, "SOL-USD");
    }

    #[test]
    fn parse_ticks_accepts_json_lines() {
        let ticks = parse_ticks_document(
            r#"{"tenant_id":"t1","exchange":"bybit","product_id":"DOGE-USD","bid":0.15,"ask":0.151,"mid":0.1505,"event_ts_ms":1}
{"tenant_id":"t1","exchange":"bybit","product_id":"DOGE-USD","bid":0.151,"ask":0.152,"mid":0.1515,"event_ts_ms":2}"#,
        )
        .expect("parsed");
        assert_eq!(ticks.len(), 2);
        assert_eq!(ticks[1].event_ts_ms, 2);
    }
}
