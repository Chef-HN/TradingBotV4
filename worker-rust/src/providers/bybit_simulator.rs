use std::{
    collections::{HashMap, VecDeque},
    sync::atomic::{AtomicU64, Ordering},
};

use anyhow::Result;
use async_trait::async_trait;

use crate::kernel::types::{FillEvent, MarketTick, OrderRequest};

use super::ExecutionProvider;

#[derive(Debug, Clone)]
struct StoredOrder {
    order_id: String,
    request: OrderRequest,
}

#[derive(Default)]
pub struct BybitSimulatorExecutionProvider {
    seq: AtomicU64,
    open_orders: HashMap<String, StoredOrder>,
    pending_fills: VecDeque<FillEvent>,
    taker_fee_bps: f64,
}

impl BybitSimulatorExecutionProvider {
    fn next_order_id(&self, product_id: &str, side: &str) -> String {
        let id = self.seq.fetch_add(1, Ordering::Relaxed) + 1;
        format!("sim-{}-{}-{}", product_id.to_lowercase(), side, id)
    }
}

#[async_trait]
impl ExecutionProvider for BybitSimulatorExecutionProvider {
    async fn submit(&mut self, request: &OrderRequest) -> Result<String> {
        let order_id = self.next_order_id(&request.product_id, &request.side);
        let stored = StoredOrder {
            order_id: order_id.clone(),
            request: request.clone(),
        };
        self.open_orders.insert(order_id.clone(), stored);
        Ok(order_id)
    }

    async fn cancel(&mut self, _tenant_id: &str, _exchange: &str, _product_id: &str, order_id: &str) -> Result<()> {
        self.open_orders.remove(order_id);
        Ok(())
    }

    async fn on_market_tick(&mut self, tick: &MarketTick) -> Result<()> {
        let mut filled_ids = Vec::new();
        for stored in self.open_orders.values() {
            if stored.request.tenant_id != tick.tenant_id
                || stored.request.exchange != tick.exchange
                || stored.request.product_id != tick.product_id
            {
                continue;
            }
            if should_cross(&stored.request.side, stored.request.price, tick) {
                let fee_quote = stored.request.size_base * stored.request.price * (self.taker_fee_bps / 10_000.0);
                self.pending_fills.push_back(FillEvent {
                    tenant_id: stored.request.tenant_id.clone(),
                    exchange: stored.request.exchange.clone(),
                    product_id: stored.request.product_id.clone(),
                    order_id: stored.order_id.clone(),
                    side: stored.request.side.clone(),
                    price: stored.request.price,
                    size_base: stored.request.size_base,
                    fee_quote,
                    event_ts_ms: tick.event_ts_ms,
                });
                filled_ids.push(stored.order_id.clone());
            }
        }
        for id in filled_ids {
            self.open_orders.remove(&id);
        }
        Ok(())
    }

    async fn flush_fills(&mut self, tenant_id: &str, exchange: &str, product_id: &str) -> Result<Vec<FillEvent>> {
        let mut rest = VecDeque::new();
        let mut selected = Vec::new();
        while let Some(fill) = self.pending_fills.pop_front() {
            if fill.tenant_id == tenant_id && fill.exchange == exchange && fill.product_id == product_id {
                selected.push(fill);
            } else {
                rest.push_back(fill);
            }
        }
        self.pending_fills = rest;
        Ok(selected)
    }

    async fn liquidate_inventory(&mut self, tenant_id: &str, exchange: &str, product_id: &str) -> Result<()> {
        self.open_orders.retain(|_, o| {
            !(o.request.tenant_id == tenant_id && o.request.exchange == exchange && o.request.product_id == product_id)
        });
        Ok(())
    }
}

fn should_cross(side: &str, order_price: f64, tick: &MarketTick) -> bool {
    match side {
        "buy" => tick.ask <= order_price,
        "sell" => tick.bid >= order_price,
        _ => false,
    }
}
