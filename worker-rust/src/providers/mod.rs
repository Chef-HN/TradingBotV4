mod bybit_live;
mod bybit_simulator;
mod market_data;

use anyhow::Result;
use async_trait::async_trait;

use crate::kernel::types::{FillEvent, MarketTick, OrderRequest};

pub use bybit_live::BybitLiveExecutionProvider;
pub use bybit_simulator::BybitSimulatorExecutionProvider;
pub use market_data::{ReplayMarketDataProvider, SyntheticMarketDataProvider};

#[async_trait]
pub trait MarketDataProvider: Send {
    async fn next_tick(&mut self) -> Result<MarketTick>;
}

#[async_trait]
pub trait ExecutionProvider: Send {
    async fn submit(&mut self, request: &OrderRequest) -> Result<String>;
    async fn cancel(&mut self, tenant_id: &str, exchange: &str, product_id: &str, order_id: &str) -> Result<()>;
    async fn on_market_tick(&mut self, tick: &MarketTick) -> Result<()>;
    async fn flush_fills(&mut self, tenant_id: &str, exchange: &str, product_id: &str) -> Result<Vec<FillEvent>>;
    async fn liquidate_inventory(&mut self, tenant_id: &str, exchange: &str, product_id: &str) -> Result<()>;
}
