use std::{collections::VecDeque, time::Duration};

use anyhow::{anyhow, Result};
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
