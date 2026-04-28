use std::collections::HashSet;

use serde_json::json;
use thiserror::Error;

use super::types::{
    DailyCloseInput, DailyCloseOutcome, FillEvent, KernelDecision, MarketTick, OrderRequest,
    StrategyConfig, WorkerStateEvent,
};

#[derive(Debug, Error)]
pub enum KernelError {
    #[error("invalid strategy config: {0}")]
    InvalidConfig(String),
    #[error("tick does not match kernel routing: expected {expected_exchange}/{expected_product}, got {exchange}/{product}")]
    TickRoutingMismatch {
        expected_exchange: String,
        expected_product: String,
        exchange: String,
        product: String,
    },
}

#[derive(Debug, Clone, Default)]
pub struct KernelOutput {
    pub decisions: Vec<KernelDecision>,
    pub events: Vec<WorkerStateEvent>,
}

pub struct TradingKernel {
    config: StrategyConfig,
    last_reference_mid: Option<f64>,
    active_order_ids: HashSet<String>,
}

impl TradingKernel {
    pub fn new(config: StrategyConfig) -> Result<Self, KernelError> {
        validate_config(&config)?;
        Ok(Self {
            config,
            last_reference_mid: None,
            active_order_ids: HashSet::new(),
        })
    }

    pub fn on_tick(&mut self, tick: &MarketTick) -> Result<KernelOutput, KernelError> {
        if tick.exchange != self.config.exchange || tick.product_id != self.config.product_id {
            return Err(KernelError::TickRoutingMismatch {
                expected_exchange: self.config.exchange.clone(),
                expected_product: self.config.product_id.clone(),
                exchange: tick.exchange.clone(),
                product: tick.product_id.clone(),
            });
        }

        if self.last_reference_mid.is_none() {
            self.last_reference_mid = Some(tick.mid);
            let decisions = self.grid_decisions(tick.mid);
            let events = vec![build_event(
                &self.config,
                "kernel_bootstrap_grid",
                json!({
                    "reference_mid": tick.mid,
                    "grid_levels": self.config.grid_levels,
                    "spacing_bps": self.config.spacing_bps,
                }),
                tick.event_ts_ms,
            )];
            return Ok(KernelOutput { decisions, events });
        }

        let reference_mid = self
            .last_reference_mid
            .expect("last_reference_mid is initialized above");
        let drift_bps = ((tick.mid - reference_mid) / reference_mid).abs() * 10_000.0;
        if drift_bps >= self.config.rebalance_threshold_bps {
            self.last_reference_mid = Some(tick.mid);
            let mut decisions = self.cancel_active_order_decisions();
            decisions.extend(self.grid_decisions(tick.mid));
            let events = vec![build_event(
                &self.config,
                "kernel_rebalance_grid",
                json!({
                    "reference_mid_before": reference_mid,
                    "reference_mid_after": tick.mid,
                    "drift_bps": drift_bps,
                    "threshold_bps": self.config.rebalance_threshold_bps,
                }),
                tick.event_ts_ms,
            )];
            return Ok(KernelOutput { decisions, events });
        }

        Ok(KernelOutput::default())
    }

    pub fn on_fill(&mut self, fill: &FillEvent) -> KernelOutput {
        self.active_order_ids.remove(&fill.order_id);
        let event = build_event(
            &self.config,
            "kernel_fill_processed",
            json!({
                "order_id": fill.order_id,
                "side": fill.side,
                "price": fill.price,
                "size_base": fill.size_base,
                "fee_quote": fill.fee_quote,
            }),
            fill.event_ts_ms,
        );
        KernelOutput {
            decisions: Vec::new(),
            events: vec![event],
        }
    }

    pub fn on_daily_close(
        &mut self,
        input: &DailyCloseInput,
        emitted_at_ts_ms: i64,
    ) -> (DailyCloseOutcome, KernelOutput) {
        let target = input.session_capital_usd.max(0.0);
        let mut reserve = input.reserve_usd.max(0.0);

        if input.equity_usd > target {
            let transfer = (input.equity_usd - target).max(0.0);
            reserve += transfer;

            let mut decisions = self.cancel_active_order_decisions();
            decisions.push(KernelDecision::LiquidateInventory {
                tenant_id: self.config.tenant_id.clone(),
                exchange: self.config.exchange.clone(),
                product_id: self.config.product_id.clone(),
                reason: "daily_close_profit_sweep".to_string(),
            });

            let outcome = DailyCloseOutcome {
                action: "profit_sweep".to_string(),
                transfer_usd: transfer,
                resulting_reserve_usd: reserve,
                resulting_session_capital_usd: target,
                underfunded: false,
            };
            let event = build_event(
                &self.config,
                "daily_close_profit_sweep",
                json!({
                    "equity_usd": input.equity_usd,
                    "target_session_capital_usd": target,
                    "transfer_to_reserve_usd": transfer,
                    "resulting_reserve_usd": reserve,
                }),
                emitted_at_ts_ms,
            );
            return (
                outcome,
                KernelOutput {
                    decisions,
                    events: vec![event],
                },
            );
        }

        if input.equity_usd < target {
            let deficit = (target - input.equity_usd).max(0.0);
            let transfer = deficit.min(reserve);
            reserve -= transfer;
            let underfunded = transfer < deficit;

            let action = if underfunded {
                "reserve_injection_underfunded"
            } else {
                "reserve_injection"
            };
            let outcome = DailyCloseOutcome {
                action: action.to_string(),
                transfer_usd: transfer,
                resulting_reserve_usd: reserve,
                resulting_session_capital_usd: input.equity_usd + transfer,
                underfunded,
            };
            let event = build_event(
                &self.config,
                action,
                json!({
                    "equity_usd": input.equity_usd,
                    "target_session_capital_usd": target,
                    "deficit_usd": deficit,
                    "transfer_from_reserve_usd": transfer,
                    "resulting_reserve_usd": reserve,
                    "underfunded": underfunded,
                }),
                emitted_at_ts_ms,
            );
            return (
                outcome,
                KernelOutput {
                    decisions: Vec::new(),
                    events: vec![event],
                },
            );
        }

        let outcome = DailyCloseOutcome {
            action: "flat".to_string(),
            transfer_usd: 0.0,
            resulting_reserve_usd: reserve,
            resulting_session_capital_usd: target,
            underfunded: false,
        };
        let event = build_event(
            &self.config,
            "daily_close_flat",
            json!({
                "equity_usd": input.equity_usd,
                "target_session_capital_usd": target,
            }),
            emitted_at_ts_ms,
        );
        (
            outcome,
            KernelOutput {
                decisions: Vec::new(),
                events: vec![event],
            },
        )
    }

    pub fn register_open_order(&mut self, order_id: String) {
        self.active_order_ids.insert(order_id);
    }

    pub fn forget_open_order(&mut self, order_id: &str) {
        self.active_order_ids.remove(order_id);
    }

    pub fn active_order_count(&self) -> usize {
        self.active_order_ids.len()
    }

    pub fn reference_mid(&self) -> Option<f64> {
        self.last_reference_mid
    }

    fn cancel_active_order_decisions(&self) -> Vec<KernelDecision> {
        self.active_order_ids
            .iter()
            .cloned()
            .map(|order_id| KernelDecision::CancelOrder {
                tenant_id: self.config.tenant_id.clone(),
                exchange: self.config.exchange.clone(),
                product_id: self.config.product_id.clone(),
                order_id,
            })
            .collect()
    }

    fn grid_decisions(&self, reference_mid: f64) -> Vec<KernelDecision> {
        let mut decisions = Vec::new();
        let levels = self.config.grid_levels.max(1) as usize;
        for level in 1..=levels {
            let offset = self.config.spacing_bps / 10_000.0 * level as f64;
            let bid_price = reference_mid * (1.0 - offset);
            let ask_price = reference_mid * (1.0 + offset);
            decisions.push(KernelDecision::PlaceOrder(OrderRequest {
                tenant_id: self.config.tenant_id.clone(),
                exchange: self.config.exchange.clone(),
                product_id: self.config.product_id.clone(),
                side: "buy".to_string(),
                price: bid_price,
                size_base: quote_to_base(self.config.level_size_quote, bid_price),
                post_only: true,
            }));
            decisions.push(KernelDecision::PlaceOrder(OrderRequest {
                tenant_id: self.config.tenant_id.clone(),
                exchange: self.config.exchange.clone(),
                product_id: self.config.product_id.clone(),
                side: "sell".to_string(),
                price: ask_price,
                size_base: quote_to_base(self.config.level_size_quote, ask_price),
                post_only: true,
            }));
        }
        decisions
    }
}

fn validate_config(config: &StrategyConfig) -> Result<(), KernelError> {
    if config.grid_levels <= 0 {
        return Err(KernelError::InvalidConfig(
            "grid_levels must be > 0".to_string(),
        ));
    }
    if config.spacing_bps <= 0.0 {
        return Err(KernelError::InvalidConfig(
            "spacing_bps must be > 0".to_string(),
        ));
    }
    if config.rebalance_threshold_bps <= 0.0 {
        return Err(KernelError::InvalidConfig(
            "rebalance_threshold_bps must be > 0".to_string(),
        ));
    }
    if config.level_size_quote <= 0.0 {
        return Err(KernelError::InvalidConfig(
            "level_size_quote must be > 0".to_string(),
        ));
    }
    if config.daily_close_hour > 23 {
        return Err(KernelError::InvalidConfig(
            "daily_close_hour must be between 0 and 23".to_string(),
        ));
    }
    if config.daily_close_minute > 59 {
        return Err(KernelError::InvalidConfig(
            "daily_close_minute must be between 0 and 59".to_string(),
        ));
    }
    Ok(())
}

fn build_event(
    config: &StrategyConfig,
    state_type: &str,
    payload: serde_json::Value,
    emitted_at_ts_ms: i64,
) -> WorkerStateEvent {
    WorkerStateEvent {
        tenant_id: config.tenant_id.clone(),
        exchange: config.exchange.clone(),
        product_id: config.product_id.clone(),
        state_type: state_type.to_string(),
        payload,
        emitted_at_ts_ms,
    }
}

fn quote_to_base(level_size_quote: f64, price: f64) -> f64 {
    if price <= 0.0 {
        return 0.0;
    }
    level_size_quote / price
}
