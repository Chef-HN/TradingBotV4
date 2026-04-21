use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StrategyConfig {
    pub tenant_id: String,
    pub exchange: String,
    pub product_id: String,
    pub spacing_bps: f64,
    pub rebalance_threshold_bps: f64,
    pub grid_levels: i32,
    pub level_size_quote: f64,
    pub local_timezone_iana: String,
    pub daily_close_hour: u8,
    pub daily_close_minute: u8,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketTick {
    pub tenant_id: String,
    pub exchange: String,
    pub product_id: String,
    pub bid: f64,
    pub ask: f64,
    pub mid: f64,
    pub event_ts_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderRequest {
    pub tenant_id: String,
    pub exchange: String,
    pub product_id: String,
    pub side: String,
    pub price: f64,
    pub size_base: f64,
    pub post_only: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FillEvent {
    pub tenant_id: String,
    pub exchange: String,
    pub product_id: String,
    pub order_id: String,
    pub side: String,
    pub price: f64,
    pub size_base: f64,
    pub fee_quote: f64,
    pub event_ts_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum KernelDecision {
    PlaceOrder(OrderRequest),
    CancelOrder {
        tenant_id: String,
        exchange: String,
        product_id: String,
        order_id: String,
    },
    LiquidateInventory {
        tenant_id: String,
        exchange: String,
        product_id: String,
        reason: String,
    },
    Noop,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum CloseTimezoneChangeMode {
    NextCycle,
    Immediate,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DailyCloseInput {
    pub tenant_id: String,
    pub exchange: String,
    pub product_id: String,
    pub equity_usd: f64,
    pub session_capital_usd: f64,
    pub reserve_usd: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DailyCloseOutcome {
    pub action: String,
    pub transfer_usd: f64,
    pub resulting_reserve_usd: f64,
    pub resulting_session_capital_usd: f64,
    pub underfunded: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerStateEvent {
    pub tenant_id: String,
    pub exchange: String,
    pub product_id: String,
    pub state_type: String,
    pub payload: serde_json::Value,
    pub emitted_at_ts_ms: i64,
}
