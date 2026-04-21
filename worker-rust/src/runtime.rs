use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, TimeZone, Utc};
use chrono_tz::Tz;
use serde_json::json;

use crate::{
    kernel::{
        types::{CloseTimezoneChangeMode, DailyCloseInput, KernelDecision, StrategyConfig, WorkerStateEvent},
        KernelOutput, TradingKernel,
    },
    providers::{
        BybitLiveExecutionProvider, BybitSimulatorExecutionProvider, ExecutionProvider, MarketDataProvider,
        SyntheticMarketDataProvider,
    },
    publisher::{StatePublisher, StdoutStatePublisher},
};

pub async fn run() -> Result<()> {
    let config = load_strategy_from_env()?;
    let mode = std::env::var("TB_EXECUTION_MODE").unwrap_or_else(|_| "simulator".to_string());
    let tick_interval_ms = read_env_u64("TB_TICK_INTERVAL_MS", 1_000);

    let mut kernel = TradingKernel::new(config.clone())?;
    let mut market_data: Box<dyn MarketDataProvider> =
        Box::new(SyntheticMarketDataProvider::from_strategy(&config, tick_interval_ms));
    let mut execution: Box<dyn ExecutionProvider> = match mode.as_str() {
        "live" => Box::new(BybitLiveExecutionProvider::default()),
        _ => Box::new(BybitSimulatorExecutionProvider::default()),
    };
    let mut publisher: Box<dyn StatePublisher> = Box::new(StdoutStatePublisher);

    let mut scheduler = DailyCloseScheduler::new(
        &config.local_timezone_iana,
        config.daily_close_hour,
        config.daily_close_minute,
        Utc::now(),
    )?;
    let mut reserve_usd = read_env_f64("TB_RESERVE_USD", 0.0);
    let session_capital_usd = read_env_f64("TB_SESSION_CAPITAL_USD", 100.0);

    println!(
        "{}",
        json!({
            "state_type": "worker_boot",
            "tenant_id": config.tenant_id,
            "exchange": config.exchange,
            "product_id": config.product_id,
            "execution_mode": mode,
            "session_timezone_iana": scheduler.session_timezone_iana(),
            "next_close_utc": scheduler.next_close_utc().to_rfc3339(),
        })
    );

    let shutdown = tokio::signal::ctrl_c();
    tokio::pin!(shutdown);

    loop {
        tokio::select! {
            _ = &mut shutdown => {
                println!("{}", json!({"state_type":"worker_shutdown"}));
                break;
            }
            tick = market_data.next_tick() => {
                let tick = tick?;
                execution.on_market_tick(&tick).await?;

                let output = kernel.on_tick(&tick)?;
                process_kernel_output(output, &mut *execution, &mut *publisher, &mut kernel).await?;

                let fills = execution
                    .flush_fills(&tick.tenant_id, &tick.exchange, &tick.product_id)
                    .await?;
                for fill in fills {
                    let out = kernel.on_fill(&fill);
                    process_kernel_output(out, &mut *execution, &mut *publisher, &mut kernel).await?;
                }

                let now_utc = Utc::now();
                if scheduler.should_trigger(now_utc) {
                    let (equity_usd, daily_close_event_ts) = synthesize_equity_snapshot(&tick);
                    let close_input = DailyCloseInput {
                        tenant_id: tick.tenant_id.clone(),
                        exchange: tick.exchange.clone(),
                        product_id: tick.product_id.clone(),
                        equity_usd,
                        session_capital_usd,
                        reserve_usd,
                    };

                    let (outcome, close_out) = kernel.on_daily_close(&close_input, daily_close_event_ts);
                    reserve_usd = outcome.resulting_reserve_usd;
                    process_kernel_output(close_out, &mut *execution, &mut *publisher, &mut kernel).await?;

                    let close_event = WorkerStateEvent {
                        tenant_id: tick.tenant_id.clone(),
                        exchange: tick.exchange.clone(),
                        product_id: tick.product_id.clone(),
                        state_type: "daily_close_outcome".to_string(),
                        payload: serde_json::to_value(&outcome)?,
                        emitted_at_ts_ms: daily_close_event_ts,
                    };
                    publisher.publish(&close_event).await?;

                    scheduler.on_close_executed(now_utc)?;
                }
            }
        }
    }

    Ok(())
}

async fn process_kernel_output(
    output: KernelOutput,
    execution: &mut dyn ExecutionProvider,
    publisher: &mut dyn StatePublisher,
    kernel: &mut TradingKernel,
) -> Result<()> {
    for decision in output.decisions {
        match decision {
            KernelDecision::PlaceOrder(order) => {
                let order_id = execution.submit(&order).await?;
                kernel.register_open_order(order_id.clone());
                publisher
                    .publish(&WorkerStateEvent {
                        tenant_id: order.tenant_id,
                        exchange: order.exchange,
                        product_id: order.product_id,
                        state_type: "order_submitted".to_string(),
                        payload: json!({
                            "order_id": order_id,
                            "side": order.side,
                            "price": order.price,
                            "size_base": order.size_base,
                            "post_only": order.post_only,
                        }),
                        emitted_at_ts_ms: now_ms(),
                    })
                    .await?;
            }
            KernelDecision::CancelOrder {
                tenant_id,
                exchange,
                product_id,
                order_id,
            } => {
                execution
                    .cancel(&tenant_id, &exchange, &product_id, &order_id)
                    .await?;
                kernel.forget_open_order(&order_id);
                publisher
                    .publish(&WorkerStateEvent {
                        tenant_id,
                        exchange,
                        product_id,
                        state_type: "order_canceled".to_string(),
                        payload: json!({ "order_id": order_id }),
                        emitted_at_ts_ms: now_ms(),
                    })
                    .await?;
            }
            KernelDecision::LiquidateInventory {
                tenant_id,
                exchange,
                product_id,
                reason,
            } => {
                execution
                    .liquidate_inventory(&tenant_id, &exchange, &product_id)
                    .await?;
                publisher
                    .publish(&WorkerStateEvent {
                        tenant_id,
                        exchange,
                        product_id,
                        state_type: "inventory_liquidation_requested".to_string(),
                        payload: json!({ "reason": reason }),
                        emitted_at_ts_ms: now_ms(),
                    })
                    .await?;
            }
            KernelDecision::Noop => {}
        }
    }

    for event in output.events {
        publisher.publish(&event).await?;
    }
    Ok(())
}

fn load_strategy_from_env() -> Result<StrategyConfig> {
    let tenant_id = std::env::var("TB_TENANT_ID")
        .unwrap_or_else(|_| "00000000-0000-0000-0000-000000000001".to_string());
    let exchange = std::env::var("TB_EXCHANGE").unwrap_or_else(|_| "bybit".to_string());
    let product_id = std::env::var("TB_PRODUCT_ID").unwrap_or_else(|_| "SOL-USD".to_string());

    let spacing_bps = read_required_env_f64("TB_SPACING_BPS")
        .context("TB_SPACING_BPS is required (fail-fast if config is incomplete)")?;
    let rebalance_threshold_bps = read_required_env_f64("TB_REBALANCE_THRESHOLD_BPS")
        .context("TB_REBALANCE_THRESHOLD_BPS is required (fail-fast if config is incomplete)")?;
    let grid_levels = read_required_env_i32("TB_GRID_LEVELS")
        .context("TB_GRID_LEVELS is required (fail-fast if config is incomplete)")?;
    let level_size_quote = read_required_env_f64("TB_LEVEL_SIZE_QUOTE")
        .context("TB_LEVEL_SIZE_QUOTE is required (fail-fast if config is incomplete)")?;
    let local_timezone_iana = std::env::var("TB_LOCAL_TIMEZONE_IANA")
        .context("TB_LOCAL_TIMEZONE_IANA is required (fail-fast if config is incomplete)")?;
    let daily_close_hour = read_required_env_u8("TB_DAILY_CLOSE_HOUR")
        .context("TB_DAILY_CLOSE_HOUR is required (fail-fast if config is incomplete)")?;
    let daily_close_minute = read_required_env_u8("TB_DAILY_CLOSE_MINUTE")
        .context("TB_DAILY_CLOSE_MINUTE is required (fail-fast if config is incomplete)")?;

    Ok(StrategyConfig {
        tenant_id,
        exchange,
        product_id,
        spacing_bps,
        rebalance_threshold_bps,
        grid_levels,
        level_size_quote,
        local_timezone_iana,
        daily_close_hour,
        daily_close_minute,
    })
}

fn read_required_env_f64(key: &str) -> Result<f64> {
    let raw = std::env::var(key).map_err(|_| anyhow!("{key} not set"))?;
    raw.parse::<f64>()
        .map_err(|e| anyhow!("{key} must be numeric: {e}"))
}

fn read_required_env_i32(key: &str) -> Result<i32> {
    let raw = std::env::var(key).map_err(|_| anyhow!("{key} not set"))?;
    raw.parse::<i32>()
        .map_err(|e| anyhow!("{key} must be integer: {e}"))
}

fn read_required_env_u8(key: &str) -> Result<u8> {
    let raw = std::env::var(key).map_err(|_| anyhow!("{key} not set"))?;
    raw.parse::<u8>()
        .map_err(|e| anyhow!("{key} must be integer in [0,255]: {e}"))
}

fn read_env_f64(key: &str, default: f64) -> f64 {
    std::env::var(key)
        .ok()
        .and_then(|x| x.parse::<f64>().ok())
        .unwrap_or(default)
}

fn read_env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|x| x.parse::<u64>().ok())
        .unwrap_or(default)
}

fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};

    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or_default()
}

fn synthesize_equity_snapshot(tick: &crate::kernel::types::MarketTick) -> (f64, i64) {
    // Placeholder hook: in production this will read equity from tenant/par runtime state.
    let synthetic_equity = tick.mid;
    (synthetic_equity, tick.event_ts_ms)
}

#[derive(Debug)]
struct DailyCloseScheduler {
    active_timezone_name: String,
    active_timezone: Tz,
    pending_timezone_name: Option<String>,
    close_hour: u8,
    close_minute: u8,
    next_close_utc: DateTime<Utc>,
}

impl DailyCloseScheduler {
    fn new(timezone_iana: &str, close_hour: u8, close_minute: u8, now_utc: DateTime<Utc>) -> Result<Self> {
        let active_timezone: Tz = timezone_iana
            .parse()
            .map_err(|_| anyhow!("invalid timezone IANA: {timezone_iana}"))?;
        validate_close_time(close_hour, close_minute)?;
        let next_close_utc = compute_next_close_utc(active_timezone, close_hour, close_minute, now_utc)?;
        Ok(Self {
            active_timezone_name: timezone_iana.to_string(),
            active_timezone,
            pending_timezone_name: None,
            close_hour,
            close_minute,
            next_close_utc,
        })
    }

    fn should_trigger(&self, now_utc: DateTime<Utc>) -> bool {
        now_utc >= self.next_close_utc
    }

    fn on_close_executed(&mut self, now_utc: DateTime<Utc>) -> Result<()> {
        if let Some(next_tz) = self.pending_timezone_name.take() {
            let parsed: Tz = next_tz
                .parse()
                .map_err(|_| anyhow!("invalid pending timezone IANA: {next_tz}"))?;
            self.active_timezone = parsed;
            self.active_timezone_name = next_tz;
        }
        self.next_close_utc =
            compute_next_close_utc(self.active_timezone, self.close_hour, self.close_minute, now_utc)?;
        Ok(())
    }

    #[allow(dead_code)]
    fn apply_timezone_change(
        &mut self,
        timezone_iana: &str,
        mode: CloseTimezoneChangeMode,
        now_utc: DateTime<Utc>,
    ) -> Result<()> {
        let parsed: Tz = timezone_iana
            .parse()
            .map_err(|_| anyhow!("invalid timezone IANA: {timezone_iana}"))?;
        match mode {
            CloseTimezoneChangeMode::NextCycle => {
                self.pending_timezone_name = Some(timezone_iana.to_string());
            }
            CloseTimezoneChangeMode::Immediate => {
                self.pending_timezone_name = None;
                self.active_timezone = parsed;
                self.active_timezone_name = timezone_iana.to_string();
                self.next_close_utc =
                    compute_next_close_utc(self.active_timezone, self.close_hour, self.close_minute, now_utc)?;
            }
        }
        Ok(())
    }

    fn next_close_utc(&self) -> DateTime<Utc> {
        self.next_close_utc
    }

    fn session_timezone_iana(&self) -> &str {
        &self.active_timezone_name
    }
}

fn validate_close_time(hour: u8, minute: u8) -> Result<()> {
    if hour > 23 {
        return Err(anyhow!("daily_close_hour must be between 0 and 23"));
    }
    if minute > 59 {
        return Err(anyhow!("daily_close_minute must be between 0 and 59"));
    }
    Ok(())
}

fn compute_next_close_utc(tz: Tz, close_hour: u8, close_minute: u8, now_utc: DateTime<Utc>) -> Result<DateTime<Utc>> {
    let local_now = now_utc.with_timezone(&tz);
    let today = local_now.date_naive();

    let today_close = resolve_local_datetime(tz, today, close_hour, close_minute)?;
    let next_local_close = if local_now >= today_close {
        let tomorrow = today.succ_opt().ok_or_else(|| anyhow!("date overflow"))?;
        resolve_local_datetime(tz, tomorrow, close_hour, close_minute)?
    } else {
        today_close
    };
    Ok(next_local_close.with_timezone(&Utc))
}

fn resolve_local_datetime(
    tz: Tz,
    date: chrono::NaiveDate,
    close_hour: u8,
    close_minute: u8,
) -> Result<chrono::DateTime<Tz>> {
    let naive = date
        .and_hms_opt(close_hour as u32, close_minute as u32, 0)
        .ok_or_else(|| anyhow!("invalid close hour/minute"))?;

    let local = tz
        .from_local_datetime(&naive)
        .single()
        .or_else(|| tz.from_local_datetime(&naive).earliest())
        .or_else(|| tz.from_local_datetime(&naive).latest())
        .ok_or_else(|| anyhow!("cannot resolve local datetime for timezone"))?;

    Ok(local)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;

    #[test]
    fn next_cycle_keeps_original_timezone_until_close() {
        let now_utc = Utc
            .with_ymd_and_hms(2026, 4, 20, 8, 0, 0)
            .single()
            .expect("valid dt");
        let mut scheduler = DailyCloseScheduler::new("Europe/Amsterdam", 0, 0, now_utc).expect("scheduler");
        let baseline_next_close = scheduler.next_close_utc();

        scheduler
            .apply_timezone_change("Asia/Singapore", CloseTimezoneChangeMode::NextCycle, now_utc)
            .expect("apply change");
        assert_eq!(scheduler.session_timezone_iana(), "Europe/Amsterdam");
        assert_eq!(scheduler.next_close_utc(), baseline_next_close);

        let after_close = baseline_next_close + Duration::seconds(1);
        scheduler.on_close_executed(after_close).expect("roll close");
        assert_eq!(scheduler.session_timezone_iana(), "Asia/Singapore");
    }

    #[test]
    fn immediate_timezone_change_recomputes_next_close() {
        let now_utc = Utc
            .with_ymd_and_hms(2026, 4, 20, 8, 0, 0)
            .single()
            .expect("valid dt");
        let mut scheduler = DailyCloseScheduler::new("Europe/Amsterdam", 0, 0, now_utc).expect("scheduler");
        let prev = scheduler.next_close_utc();

        scheduler
            .apply_timezone_change("Asia/Singapore", CloseTimezoneChangeMode::Immediate, now_utc)
            .expect("apply immediate");

        assert_eq!(scheduler.session_timezone_iana(), "Asia/Singapore");
        assert_ne!(scheduler.next_close_utc(), prev);
    }
}
