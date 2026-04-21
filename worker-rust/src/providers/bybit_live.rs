use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::Result;
use async_trait::async_trait;

use crate::kernel::types::{FillEvent, MarketTick, OrderRequest};

use super::ExecutionProvider;

#[derive(Default)]
pub struct BybitLiveExecutionProvider {
    seq: AtomicU64,
}

#[async_trait]
impl ExecutionProvider for BybitLiveExecutionProvider {
    async fn submit(&mut self, request: &OrderRequest) -> Result<String> {
        // Placeholder while integrating the real Bybit API adapter.
        let id = self.seq.fetch_add(1, Ordering::Relaxed) + 1;
        Ok(format!(
            "live-{}-{}-{}",
            request.product_id.to_lowercase(),
            request.side,
            id
        ))
    }

    async fn cancel(&mut self, _tenant_id: &str, _exchange: &str, _product_id: &str, _order_id: &str) -> Result<()> {
        Ok(())
    }

    async fn on_market_tick(&mut self, _tick: &MarketTick) -> Result<()> {
        Ok(())
    }

    async fn flush_fills(&mut self, _tenant_id: &str, _exchange: &str, _product_id: &str) -> Result<Vec<FillEvent>> {
        Ok(Vec::new())
    }

    async fn liquidate_inventory(&mut self, _tenant_id: &str, _exchange: &str, _product_id: &str) -> Result<()> {
        Ok(())
    }
}
