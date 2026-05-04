use anyhow::{anyhow, Result};
use chrono::{DateTime, TimeZone, Utc};
use chrono_tz::Tz;
use serde_json::{json, Value};
use std::time::Instant;
use uuid::Uuid;

use crate::{
    config_source::{load_strategy_from_db, RuntimeSources},
    control_plane::{RedisControlPlane, RuntimeCommand},
    kernel::{
        types::{
            CloseTimezoneChangeMode, DailyCloseInput, KernelDecision, MarketTick, StrategyConfig,
            WorkerStateEvent,
        },
        KernelOutput, TradingKernel,
    },
    providers::{
        BybitLiveExecutionProvider, BybitRestMarketDataProvider, BybitSimulatorExecutionProvider,
        ExecutionProvider, MarketDataProvider, ReplayMarketDataProvider,
        SyntheticMarketDataProvider,
    },
    publisher::{StatePublisher, StdoutStatePublisher},
};

const EXEC_MODE_LIVE: &str = "live";
const EXEC_MODE_SIMULATOR: &str = "simulator";
const MARKET_DATA_SYNTHETIC: &str = "synthetic";
const MARKET_DATA_REPLAY: &str = "replay";
const MARKET_DATA_BYBIT_REST: &str = "bybit_rest";
const SCHEDULE_MODE_IMMEDIATE: &str = "immediate";
const SCHEDULE_MODE_NEXT_CYCLE: &str = "next_cycle";
const ENV_ALLOW_RESET_COMMAND: &str = "TB_ALLOW_RESET_COMMAND";
const ENV_MARKET_DATA_GAP_WARN_MS: &str = "TB_MARKET_DATA_GAP_WARN_MS";
const ENV_COMMAND_LAG_WARN_MS: &str = "TB_COMMAND_LAG_WARN_MS";
const ENV_HEARTBEAT_LAG_WARN_MS: &str = "TB_HEARTBEAT_LAG_WARN_MS";

#[derive(Debug, Default)]
struct RuntimeCommandEffects {
    reset_kernel: bool,
    max_command_lag_ms: Option<i64>,
    lagged_commands: i64,
}

#[derive(Debug, Default, Clone)]
struct KernelOutputStats {
    orders_submitted: i64,
    orders_canceled: i64,
    liquidations_requested: i64,
    events_published: i64,
}

#[derive(Debug, Default, Clone)]
struct RuntimeObservability {
    ticks_processed: i64,
    commands_processed: i64,
    last_command_batch_size: i64,
    max_command_batch_size: i64,
    orders_submitted: i64,
    orders_canceled: i64,
    liquidations_requested: i64,
    fills_processed: i64,
    kernel_events_published: i64,
    reconciliation_mismatches: i64,
    market_data_gap_events: i64,
    command_lag_events: i64,
    heartbeat_lag_events: i64,
    last_tick_gap_ms: Option<i64>,
    max_tick_gap_ms: i64,
    last_command_lag_ms: Option<i64>,
    max_command_lag_ms: i64,
    last_heartbeat_gap_ms: Option<i64>,
    max_heartbeat_gap_ms: i64,
    last_market_data_latency_ms: i64,
    max_market_data_latency_ms: i64,
    last_command_poll_latency_ms: i64,
    max_command_poll_latency_ms: i64,
    last_command_apply_latency_ms: i64,
    max_command_apply_latency_ms: i64,
    last_execution_latency_ms: i64,
    max_execution_latency_ms: i64,
    last_kernel_latency_ms: i64,
    max_kernel_latency_ms: i64,
    last_fill_flush_latency_ms: i64,
    max_fill_flush_latency_ms: i64,
    last_fill_process_latency_ms: i64,
    max_fill_process_latency_ms: i64,
    last_reconciliation_latency_ms: i64,
    max_reconciliation_latency_ms: i64,
    last_daily_close_latency_ms: i64,
    max_daily_close_latency_ms: i64,
    last_state_publish_latency_ms: i64,
    max_state_publish_latency_ms: i64,
    last_heartbeat_publish_latency_ms: i64,
    max_heartbeat_publish_latency_ms: i64,
    last_cycle_correlation_id: Option<String>,
}

impl RuntimeObservability {
    fn apply_kernel_output_stats(&mut self, stats: &KernelOutputStats) {
        self.orders_submitted += stats.orders_submitted;
        self.orders_canceled += stats.orders_canceled;
        self.liquidations_requested += stats.liquidations_requested;
        self.kernel_events_published += stats.events_published;
    }

    fn record_stage_latency(&mut self, stage: &str, latency_ms: i64) {
        match stage {
            "market_data" => update_latency_metric(
                &mut self.last_market_data_latency_ms,
                &mut self.max_market_data_latency_ms,
                latency_ms,
            ),
            "command_poll" => update_latency_metric(
                &mut self.last_command_poll_latency_ms,
                &mut self.max_command_poll_latency_ms,
                latency_ms,
            ),
            "command_apply" => update_latency_metric(
                &mut self.last_command_apply_latency_ms,
                &mut self.max_command_apply_latency_ms,
                latency_ms,
            ),
            "execution_on_tick" => update_latency_metric(
                &mut self.last_execution_latency_ms,
                &mut self.max_execution_latency_ms,
                latency_ms,
            ),
            "kernel_tick" => update_latency_metric(
                &mut self.last_kernel_latency_ms,
                &mut self.max_kernel_latency_ms,
                latency_ms,
            ),
            "fill_flush" => update_latency_metric(
                &mut self.last_fill_flush_latency_ms,
                &mut self.max_fill_flush_latency_ms,
                latency_ms,
            ),
            "fill_process" => update_latency_metric(
                &mut self.last_fill_process_latency_ms,
                &mut self.max_fill_process_latency_ms,
                latency_ms,
            ),
            "reconciliation" => update_latency_metric(
                &mut self.last_reconciliation_latency_ms,
                &mut self.max_reconciliation_latency_ms,
                latency_ms,
            ),
            "daily_close" => update_latency_metric(
                &mut self.last_daily_close_latency_ms,
                &mut self.max_daily_close_latency_ms,
                latency_ms,
            ),
            "state_publish" => update_latency_metric(
                &mut self.last_state_publish_latency_ms,
                &mut self.max_state_publish_latency_ms,
                latency_ms,
            ),
            "heartbeat_publish" => update_latency_metric(
                &mut self.last_heartbeat_publish_latency_ms,
                &mut self.max_heartbeat_publish_latency_ms,
                latency_ms,
            ),
            _ => {}
        }
    }
}

impl RuntimeCommandEffects {
    fn observe_command_lag(&mut self, command_lag_ms: Option<i64>) {
        let Some(lag_ms) = command_lag_ms else {
            return;
        };
        self.lagged_commands += 1;
        self.max_command_lag_ms = Some(match self.max_command_lag_ms {
            Some(current) => current.max(lag_ms),
            None => lag_ms,
        });
    }
}

pub async fn run() -> Result<()> {
    let sources = RuntimeSources::from_env()?;
    let strategy_snapshot = load_strategy_from_db(
        &sources.db_dsn,
        &sources.tenant_id,
        &sources.exchange,
        &sources.product_id,
    )
    .await?;

    let config = strategy_snapshot.strategy;
    let mode = resolve_execution_mode(
        sources.execution_mode_override.as_deref(),
        strategy_snapshot.paper_mode,
    );
    let tick_interval_ms = sources.tick_interval_ms;
    let allow_reset_command = read_env_bool(ENV_ALLOW_RESET_COMMAND, false);
    let (market_data_provider_name, mut market_data) =
        build_market_data_provider(&config, &mode, tick_interval_ms)?;
    let replay_mode = market_data_provider_name.eq_ignore_ascii_case(MARKET_DATA_REPLAY)
        || market_data_provider_name.eq_ignore_ascii_case("replay_json")
        || market_data_provider_name.eq_ignore_ascii_case("replay_jsonl");

    let mut kernel = TradingKernel::new(config.clone())?;
    let mut execution: Box<dyn ExecutionProvider> = match mode.as_str() {
        EXEC_MODE_LIVE => Box::new(BybitLiveExecutionProvider::new_from_env()?),
        _ => Box::new(BybitSimulatorExecutionProvider::default()),
    };
    let mut publisher: Box<dyn StatePublisher> = Box::new(StdoutStatePublisher);

    let mut scheduler = DailyCloseScheduler::new(
        &config.local_timezone_iana,
        config.daily_close_hour,
        config.daily_close_minute,
        Utc::now(),
    )?;

    let mut reserve_usd = sources.reserve_usd;
    let session_capital_usd = sources
        .session_capital_override_usd
        .unwrap_or(strategy_snapshot.session_capital_usd);
    let mut skip_next_daily_close = false;

    let started_at = Utc::now();
    let session_id = format!("rust-{}", Uuid::new_v4());
    let mut total_fills: i64 = 0;
    let mut last_tick: Option<MarketTick> = None;
    let mut cycle_seq: u64 = 0;
    let mut last_tick_event_ts_ms: Option<i64> = None;
    let mut observability = RuntimeObservability::default();
    let default_gap_warn_ms = ((tick_interval_ms as i64) * 3).max(5_000);
    let market_data_gap_warn_ms =
        read_env_i64(ENV_MARKET_DATA_GAP_WARN_MS, default_gap_warn_ms).max(250);
    let command_lag_warn_ms =
        read_env_i64(ENV_COMMAND_LAG_WARN_MS, (tick_interval_ms as i64).max(1_500)).max(100);
    let heartbeat_lag_warn_ms = read_env_i64(ENV_HEARTBEAT_LAG_WARN_MS, 12_000).max(250);

    let mut control_plane = RedisControlPlane::connect(
        &sources.redis_url,
        &config.tenant_id,
        &config.exchange,
        &config.product_id,
        "all",
    )
    .await?;

    println!(
        "{}",
        json!({
            "state_type": "worker_boot",
            "tenant_id": config.tenant_id,
            "exchange": config.exchange,
            "product_id": config.product_id,
            "execution_mode": mode,
            "market_data_provider": market_data_provider_name.as_str(),
            "session_timezone_iana": scheduler.session_timezone_iana(),
            "next_close_utc": scheduler.next_close_utc().to_rfc3339(),
        })
    );

    let state_publish_started = Instant::now();
    publish_runtime_state(
        &mut control_plane,
        &config,
        &mode,
        started_at,
        &session_id,
        &scheduler,
        skip_next_daily_close,
        reserve_usd,
        session_capital_usd,
        total_fills,
        &kernel,
        last_tick.as_ref(),
        &observability,
        market_data_gap_warn_ms,
        command_lag_warn_ms,
        heartbeat_lag_warn_ms,
    )
    .await?;
    observability.record_stage_latency("state_publish", elapsed_ms(state_publish_started));

    let heartbeat_publish_started = Instant::now();
    control_plane.publish_heartbeat().await?;
    observability.record_stage_latency("heartbeat_publish", elapsed_ms(heartbeat_publish_started));
    let mut last_heartbeat_publish_ts_ms = now_ms();

    let shutdown = tokio::signal::ctrl_c();
    tokio::pin!(shutdown);

    loop {
        tokio::select! {
            _ = &mut shutdown => {
                println!("{}", json!({"state_type":"worker_shutdown"}));
                break;
            }
            market_data_out = async {
                let started = Instant::now();
                let tick = market_data.next_tick().await;
                (tick, elapsed_ms(started))
            } => {
                cycle_seq = cycle_seq.saturating_add(1);
                let correlation_id = format!("cycle:{}:{cycle_seq}", session_id);
                observability.last_cycle_correlation_id = Some(correlation_id.clone());
                let (tick, market_data_latency_ms) = market_data_out;
                observability.record_stage_latency("market_data", market_data_latency_ms);
                let now_utc = Utc::now();
                let command_poll_started = Instant::now();
                let commands = control_plane.pop_commands().await?;
                observability
                    .record_stage_latency("command_poll", elapsed_ms(command_poll_started));
                let command_batch_size = commands.len() as i64;
                observability.commands_processed += command_batch_size;
                observability.last_command_batch_size = command_batch_size;
                observability.max_command_batch_size =
                    observability.max_command_batch_size.max(command_batch_size);
                let command_apply_started = Instant::now();
                let command_effects = apply_runtime_commands(
                    commands,
                    &config,
                    &mut scheduler,
                    &mut skip_next_daily_close,
                    allow_reset_command,
                    &mut *publisher,
                    now_utc,
                    &correlation_id,
                ).await?;
                observability
                    .record_stage_latency("command_apply", elapsed_ms(command_apply_started));
                if let Some(max_lag_ms) = command_effects.max_command_lag_ms {
                    observability.last_command_lag_ms = Some(max_lag_ms);
                    observability.max_command_lag_ms =
                        observability.max_command_lag_ms.max(max_lag_ms);
                    if max_lag_ms > command_lag_warn_ms {
                        observability.command_lag_events += 1;
                        let lag_event = WorkerStateEvent {
                            tenant_id: config.tenant_id.clone(),
                            exchange: config.exchange.clone(),
                            product_id: config.product_id.clone(),
                            state_type: "command_lag_detected".to_string(),
                            payload: with_correlation_payload(
                                json!({
                                    "max_command_lag_ms": max_lag_ms,
                                    "lagged_commands": command_effects.lagged_commands,
                                    "threshold_ms": command_lag_warn_ms,
                                }),
                                &correlation_id,
                            ),
                            emitted_at_ts_ms: now_ms(),
                        };
                        publisher.publish(&lag_event).await?;
                    }
                }

                if command_effects.reset_kernel {
                    execution
                        .liquidate_inventory(&config.tenant_id, &config.exchange, &config.product_id)
                        .await?;
                    kernel = TradingKernel::new(config.clone())?;
                    let reset_applied = WorkerStateEvent {
                        tenant_id: config.tenant_id.clone(),
                        exchange: config.exchange.clone(),
                        product_id: config.product_id.clone(),
                        state_type: "kernel_reset_applied".to_string(),
                        payload: with_correlation_payload(
                            json!({"source":"redis_command"}),
                            &correlation_id,
                        ),
                        emitted_at_ts_ms: now_ms(),
                    };
                    publisher.publish(&reset_applied).await?;
                }

                let tick = match tick {
                    Ok(t) => t,
                    Err(err) => {
                        if replay_mode && err.to_string().contains("replay stream exhausted") {
                            let replay_done = WorkerStateEvent {
                                tenant_id: config.tenant_id.clone(),
                                exchange: config.exchange.clone(),
                                product_id: config.product_id.clone(),
                                state_type: "replay_completed".to_string(),
                                payload: with_correlation_payload(
                                    json!({
                                        "market_data_provider": market_data_provider_name.as_str(),
                                        "session_id": session_id,
                                        "total_fills": total_fills,
                                    }),
                                    &correlation_id,
                                ),
                                emitted_at_ts_ms: now_ms(),
                            };
                            publisher.publish(&replay_done).await?;
                            break;
                        }
                        return Err(err);
                    }
                };

                observability.ticks_processed += 1;
                if let Some(gap_ms) = compute_tick_gap_ms(last_tick_event_ts_ms, tick.event_ts_ms) {
                    observability.last_tick_gap_ms = Some(gap_ms);
                    observability.max_tick_gap_ms = observability.max_tick_gap_ms.max(gap_ms);
                    if is_market_data_gap(gap_ms, market_data_gap_warn_ms) {
                        observability.market_data_gap_events += 1;
                        let gap_event = WorkerStateEvent {
                            tenant_id: tick.tenant_id.clone(),
                            exchange: tick.exchange.clone(),
                            product_id: tick.product_id.clone(),
                            state_type: "market_data_gap_detected".to_string(),
                            payload: with_correlation_payload(
                                json!({
                                    "market_data_provider": market_data_provider_name.as_str(),
                                    "gap_ms": gap_ms,
                                    "threshold_ms": market_data_gap_warn_ms,
                                    "previous_tick_ts_ms": last_tick_event_ts_ms,
                                    "current_tick_ts_ms": tick.event_ts_ms,
                                }),
                                &correlation_id,
                            ),
                            emitted_at_ts_ms: now_ms(),
                        };
                        publisher.publish(&gap_event).await?;
                    }
                }
                last_tick_event_ts_ms = Some(tick.event_ts_ms);
                let execution_started = Instant::now();
                execution.on_market_tick(&tick).await?;
                observability.record_stage_latency("execution_on_tick", elapsed_ms(execution_started));

                let kernel_started = Instant::now();
                let output = kernel.on_tick(&tick)?;
                observability.record_stage_latency("kernel_tick", elapsed_ms(kernel_started));
                let output_stats = process_kernel_output(
                    output,
                    &mut *execution,
                    &mut *publisher,
                    &mut kernel,
                    &correlation_id,
                )
                .await?;
                observability.apply_kernel_output_stats(&output_stats);

                let fill_flush_started = Instant::now();
                let fills = execution
                    .flush_fills(&tick.tenant_id, &tick.exchange, &tick.product_id)
                    .await?;
                observability.record_stage_latency("fill_flush", elapsed_ms(fill_flush_started));
                let fill_process_started = Instant::now();
                for fill in fills {
                    let out = kernel.on_fill(&fill);
                    let fill_stats = process_kernel_output(
                        out,
                        &mut *execution,
                        &mut *publisher,
                        &mut kernel,
                        &correlation_id,
                    )
                    .await?;
                    observability.apply_kernel_output_stats(&fill_stats);
                    total_fills += 1;
                    observability.fills_processed += 1;
                }
                observability.record_stage_latency("fill_process", elapsed_ms(fill_process_started));

                let reconciliation_started = Instant::now();
                let reconciliation = execution
                    .reconciliation_snapshot(&tick.tenant_id, &tick.exchange, &tick.product_id)
                    .await?;
                observability.record_stage_latency("reconciliation", elapsed_ms(reconciliation_started));
                let kernel_open_order_count = kernel.active_order_count();
                if reconciliation.open_order_count != kernel_open_order_count {
                    observability.reconciliation_mismatches += 1;
                    let mismatch_event = WorkerStateEvent {
                        tenant_id: tick.tenant_id.clone(),
                        exchange: tick.exchange.clone(),
                        product_id: tick.product_id.clone(),
                        state_type: "execution_reconciliation_mismatch".to_string(),
                        payload: with_correlation_payload(
                            json!({
                                "provider": reconciliation.provider_name,
                                "provider_open_order_count": reconciliation.open_order_count,
                                "kernel_open_order_count": kernel_open_order_count,
                                "total_fills": total_fills,
                            }),
                            &correlation_id,
                        ),
                        emitted_at_ts_ms: now_ms(),
                    };
                    publisher.publish(&mismatch_event).await?;
                }

                let now_utc = Utc::now();
                if scheduler.should_trigger(now_utc) {
                    let daily_close_started = Instant::now();
                    if skip_next_daily_close {
                        skip_next_daily_close = false;
                        let skip_event = WorkerStateEvent {
                            tenant_id: tick.tenant_id.clone(),
                            exchange: tick.exchange.clone(),
                            product_id: tick.product_id.clone(),
                            state_type: "daily_close_skipped".to_string(),
                            payload: with_correlation_payload(
                                json!({"reason":"command_skip_daily_close"}),
                                &correlation_id,
                            ),
                            emitted_at_ts_ms: now_ms(),
                        };
                        publisher.publish(&skip_event).await?;
                        scheduler.on_close_executed(now_utc)?;
                    } else {
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
                        let close_stats = process_kernel_output(
                            close_out,
                            &mut *execution,
                            &mut *publisher,
                            &mut kernel,
                            &correlation_id,
                        )
                        .await?;
                        observability.apply_kernel_output_stats(&close_stats);

                        let close_event = WorkerStateEvent {
                            tenant_id: tick.tenant_id.clone(),
                            exchange: tick.exchange.clone(),
                            product_id: tick.product_id.clone(),
                            state_type: "daily_close_outcome".to_string(),
                            payload: with_correlation_payload(
                                serde_json::to_value(&outcome)?,
                                &correlation_id,
                            ),
                            emitted_at_ts_ms: daily_close_event_ts,
                        };
                        publisher.publish(&close_event).await?;

                        scheduler.on_close_executed(now_utc)?;
                    }
                    observability.record_stage_latency("daily_close", elapsed_ms(daily_close_started));
                }

                last_tick = Some(tick);
                let state_publish_started = Instant::now();
                publish_runtime_state(
                    &mut control_plane,
                    &config,
                    &mode,
                    started_at,
                    &session_id,
                    &scheduler,
                    skip_next_daily_close,
                    reserve_usd,
                    session_capital_usd,
                    total_fills,
                    &kernel,
                    last_tick.as_ref(),
                    &observability,
                    market_data_gap_warn_ms,
                    command_lag_warn_ms,
                    heartbeat_lag_warn_ms,
                )
                .await?;
                observability.record_stage_latency("state_publish", elapsed_ms(state_publish_started));
                let heartbeat_publish_started = Instant::now();
                control_plane.publish_heartbeat().await?;
                observability.record_stage_latency(
                    "heartbeat_publish",
                    elapsed_ms(heartbeat_publish_started),
                );
                let heartbeat_ts_ms = now_ms();
                let heartbeat_gap_ms = heartbeat_ts_ms - last_heartbeat_publish_ts_ms;
                if heartbeat_gap_ms >= 0 {
                    observability.last_heartbeat_gap_ms = Some(heartbeat_gap_ms);
                    observability.max_heartbeat_gap_ms =
                        observability.max_heartbeat_gap_ms.max(heartbeat_gap_ms);
                    if heartbeat_gap_ms > heartbeat_lag_warn_ms {
                        observability.heartbeat_lag_events += 1;
                        let heartbeat_lag_event = WorkerStateEvent {
                            tenant_id: config.tenant_id.clone(),
                            exchange: config.exchange.clone(),
                            product_id: config.product_id.clone(),
                            state_type: "heartbeat_lag_detected".to_string(),
                            payload: with_correlation_payload(
                                json!({
                                    "heartbeat_gap_ms": heartbeat_gap_ms,
                                    "threshold_ms": heartbeat_lag_warn_ms,
                                }),
                                &correlation_id,
                            ),
                            emitted_at_ts_ms: heartbeat_ts_ms,
                        };
                        publisher.publish(&heartbeat_lag_event).await?;
                    }
                }
                last_heartbeat_publish_ts_ms = heartbeat_ts_ms;
            }
        }
    }

    Ok(())
}

fn resolve_execution_mode(override_mode: Option<&str>, paper_mode: bool) -> String {
    if let Some(mode) = override_mode {
        let normalized = mode.trim().to_lowercase();
        if normalized == EXEC_MODE_LIVE {
            return EXEC_MODE_LIVE.to_string();
        }
        if normalized == EXEC_MODE_SIMULATOR || normalized == "paper" {
            return EXEC_MODE_SIMULATOR.to_string();
        }
    }

    if paper_mode {
        EXEC_MODE_SIMULATOR.to_string()
    } else {
        EXEC_MODE_LIVE.to_string()
    }
}

fn build_market_data_provider(
    config: &StrategyConfig,
    execution_mode: &str,
    tick_interval_ms: u64,
) -> Result<(String, Box<dyn MarketDataProvider>)> {
    let selected = std::env::var("TB_MARKET_DATA_PROVIDER")
        .ok()
        .map(|v| v.trim().to_lowercase())
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| {
            if execution_mode == EXEC_MODE_LIVE {
                MARKET_DATA_BYBIT_REST.to_string()
            } else {
                MARKET_DATA_SYNTHETIC.to_string()
            }
        });

    match selected.as_str() {
        MARKET_DATA_BYBIT_REST => Ok((
            selected,
            Box::new(BybitRestMarketDataProvider::from_strategy(
                config,
                tick_interval_ms,
            )?),
        )),
        MARKET_DATA_REPLAY | "replay_json" | "replay_jsonl" => {
            let replay_path = std::env::var("TB_REPLAY_TICKS_PATH")
                .map_err(|_| anyhow!("TB_REPLAY_TICKS_PATH is required for replay market data mode"))?;
            Ok((
                selected,
                Box::new(ReplayMarketDataProvider::from_path(replay_path.trim())?),
            ))
        }
        MARKET_DATA_SYNTHETIC => Ok((
            selected,
            Box::new(SyntheticMarketDataProvider::from_strategy(
                config,
                tick_interval_ms,
            )),
        )),
        other => Err(anyhow!(
            "unsupported TB_MARKET_DATA_PROVIDER='{other}'. expected one of: {MARKET_DATA_BYBIT_REST}, {MARKET_DATA_REPLAY}, {MARKET_DATA_SYNTHETIC}"
        )),
    }
}

fn read_env_bool(key: &str, default: bool) -> bool {
    match std::env::var(key) {
        Ok(raw) => {
            let normalized = raw.trim().to_lowercase();
            matches!(normalized.as_str(), "1" | "true" | "yes" | "on")
        }
        Err(_) => default,
    }
}

fn read_env_i64(key: &str, default: i64) -> i64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.trim().parse::<i64>().ok())
        .unwrap_or(default)
}

async fn apply_runtime_commands(
    commands: Vec<RuntimeCommand>,
    config: &StrategyConfig,
    scheduler: &mut DailyCloseScheduler,
    skip_next_daily_close: &mut bool,
    allow_reset_command: bool,
    publisher: &mut dyn StatePublisher,
    now_utc: DateTime<Utc>,
    correlation_id: &str,
) -> Result<RuntimeCommandEffects> {
    let mut effects = RuntimeCommandEffects::default();

    for command in commands {
        match command {
            RuntimeCommand::Reset {
                command_id,
                product_id,
                actor,
                reset_type,
                command_lag_ms,
            } => {
                if !command_targets_product(&product_id, &config.product_id) {
                    continue;
                }
                effects.observe_command_lag(command_lag_ms);

                if !allow_reset_command {
                    let event = WorkerStateEvent {
                        tenant_id: config.tenant_id.clone(),
                        exchange: config.exchange.clone(),
                        product_id: config.product_id.clone(),
                        state_type: "command_reset_ignored".to_string(),
                        payload: with_correlation_payload(
                            json!({
                                "command_id": command_id,
                                "reason": format!("{ENV_ALLOW_RESET_COMMAND}=false"),
                            }),
                            correlation_id,
                        ),
                        emitted_at_ts_ms: now_ms(),
                    };
                    publisher.publish(&event).await?;
                    continue;
                }

                effects.reset_kernel = true;
                let event = WorkerStateEvent {
                    tenant_id: config.tenant_id.clone(),
                    exchange: config.exchange.clone(),
                    product_id: config.product_id.clone(),
                    state_type: "command_reset_received".to_string(),
                    payload: with_correlation_payload(
                        json!({
                            "command_id": command_id,
                            "reset_type": reset_type,
                            "actor": actor,
                        }),
                        correlation_id,
                    ),
                    emitted_at_ts_ms: now_ms(),
                };
                publisher.publish(&event).await?;
            }
            RuntimeCommand::SkipDailyClose {
                command_id,
                product_id,
                command_lag_ms,
            } => {
                if !command_targets_product(&product_id, &config.product_id) {
                    continue;
                }
                effects.observe_command_lag(command_lag_ms);

                *skip_next_daily_close = true;
                let event = WorkerStateEvent {
                    tenant_id: config.tenant_id.clone(),
                    exchange: config.exchange.clone(),
                    product_id: config.product_id.clone(),
                    state_type: "command_skip_daily_close_received".to_string(),
                    payload: with_correlation_payload(
                        json!({
                            "command_id": command_id,
                        }),
                        correlation_id,
                    ),
                    emitted_at_ts_ms: now_ms(),
                };
                publisher.publish(&event).await?;
            }
            RuntimeCommand::UpdateDailyCloseSchedule {
                command_id,
                product_id,
                local_timezone_iana,
                daily_close_hour,
                daily_close_minute,
                mode,
                command_lag_ms,
            } => {
                if !command_targets_product(&product_id, &config.product_id) {
                    continue;
                }
                effects.observe_command_lag(command_lag_ms);

                let schedule_mode = match parse_schedule_mode(&mode) {
                    Ok(v) => v,
                    Err(_) => {
                        let event = WorkerStateEvent {
                            tenant_id: config.tenant_id.clone(),
                            exchange: config.exchange.clone(),
                            product_id: config.product_id.clone(),
                            state_type: "command_schedule_rejected".to_string(),
                            payload: with_correlation_payload(
                                json!({
                                    "command_id": command_id,
                                    "reason": "invalid_mode",
                                    "mode": mode,
                                }),
                                correlation_id,
                            ),
                            emitted_at_ts_ms: now_ms(),
                        };
                        publisher.publish(&event).await?;
                        continue;
                    }
                };

                if scheduler
                    .apply_schedule_change(
                        &local_timezone_iana,
                        daily_close_hour,
                        daily_close_minute,
                        schedule_mode,
                        now_utc,
                    )
                    .is_err()
                {
                    let event = WorkerStateEvent {
                        tenant_id: config.tenant_id.clone(),
                        exchange: config.exchange.clone(),
                        product_id: config.product_id.clone(),
                        state_type: "command_schedule_rejected".to_string(),
                        payload: with_correlation_payload(
                            json!({
                                "command_id": command_id,
                                "reason": "invalid_timezone_or_time",
                                "local_timezone_iana": local_timezone_iana,
                                "daily_close_hour": daily_close_hour,
                                "daily_close_minute": daily_close_minute,
                            }),
                            correlation_id,
                        ),
                        emitted_at_ts_ms: now_ms(),
                    };
                    publisher.publish(&event).await?;
                    continue;
                }

                let event = WorkerStateEvent {
                    tenant_id: config.tenant_id.clone(),
                    exchange: config.exchange.clone(),
                    product_id: config.product_id.clone(),
                    state_type: "daily_close_schedule_updated".to_string(),
                    payload: with_correlation_payload(
                        json!({
                            "command_id": command_id,
                            "mode": mode,
                            "daily_close_schedule": scheduler.schedule_payload(),
                            "pending_daily_close_schedule": scheduler.pending_schedule_payload(),
                        }),
                        correlation_id,
                    ),
                    emitted_at_ts_ms: now_ms(),
                };
                publisher.publish(&event).await?;
            }
        }
    }

    Ok(effects)
}

fn command_targets_product(command_product_id: &str, runtime_product_id: &str) -> bool {
    command_product_id.eq_ignore_ascii_case("all")
        || command_product_id.eq_ignore_ascii_case(runtime_product_id)
}

fn parse_schedule_mode(raw: &str) -> Result<CloseTimezoneChangeMode> {
    let normalized = raw.trim().to_lowercase();
    match normalized.as_str() {
        SCHEDULE_MODE_IMMEDIATE => Ok(CloseTimezoneChangeMode::Immediate),
        "" | SCHEDULE_MODE_NEXT_CYCLE => Ok(CloseTimezoneChangeMode::NextCycle),
        _ => Err(anyhow!("invalid schedule mode '{normalized}'")),
    }
}

async fn publish_runtime_state(
    control_plane: &mut RedisControlPlane,
    config: &StrategyConfig,
    mode: &str,
    started_at: DateTime<Utc>,
    session_id: &str,
    scheduler: &DailyCloseScheduler,
    skip_next_daily_close: bool,
    reserve_usd: f64,
    session_capital_usd: f64,
    total_fills: i64,
    kernel: &TradingKernel,
    last_tick: Option<&MarketTick>,
    observability: &RuntimeObservability,
    market_data_gap_warn_ms: i64,
    command_lag_warn_ms: i64,
    heartbeat_lag_warn_ms: i64,
) -> Result<()> {
    let now_utc = Utc::now();
    let uptime = (now_utc - started_at).num_seconds().max(0);

    let latency_payload = json!({
        "market_data_last": observability.last_market_data_latency_ms,
        "market_data_max": observability.max_market_data_latency_ms,
        "command_poll_last": observability.last_command_poll_latency_ms,
        "command_poll_max": observability.max_command_poll_latency_ms,
        "command_apply_last": observability.last_command_apply_latency_ms,
        "command_apply_max": observability.max_command_apply_latency_ms,
        "execution_on_tick_last": observability.last_execution_latency_ms,
        "execution_on_tick_max": observability.max_execution_latency_ms,
        "kernel_tick_last": observability.last_kernel_latency_ms,
        "kernel_tick_max": observability.max_kernel_latency_ms,
        "fill_flush_last": observability.last_fill_flush_latency_ms,
        "fill_flush_max": observability.max_fill_flush_latency_ms,
        "fill_process_last": observability.last_fill_process_latency_ms,
        "fill_process_max": observability.max_fill_process_latency_ms,
        "reconciliation_last": observability.last_reconciliation_latency_ms,
        "reconciliation_max": observability.max_reconciliation_latency_ms,
        "daily_close_last": observability.last_daily_close_latency_ms,
        "daily_close_max": observability.max_daily_close_latency_ms,
        "state_publish_last": observability.last_state_publish_latency_ms,
        "state_publish_max": observability.max_state_publish_latency_ms,
        "heartbeat_publish_last": observability.last_heartbeat_publish_latency_ms,
        "heartbeat_publish_max": observability.max_heartbeat_publish_latency_ms,
    });

    let observability_payload = json!({
        "ticks_processed": observability.ticks_processed,
        "commands_processed": observability.commands_processed,
        "last_command_batch_size": observability.last_command_batch_size,
        "max_command_batch_size": observability.max_command_batch_size,
        "orders_submitted": observability.orders_submitted,
        "orders_canceled": observability.orders_canceled,
        "liquidations_requested": observability.liquidations_requested,
        "fills_processed": observability.fills_processed,
        "kernel_events_published": observability.kernel_events_published,
        "reconciliation_mismatches": observability.reconciliation_mismatches,
        "market_data_gap_events": observability.market_data_gap_events,
        "command_lag_events": observability.command_lag_events,
        "heartbeat_lag_events": observability.heartbeat_lag_events,
        "last_tick_gap_ms": observability.last_tick_gap_ms,
        "max_tick_gap_ms": observability.max_tick_gap_ms,
        "last_command_lag_ms": observability.last_command_lag_ms,
        "max_command_lag_ms": observability.max_command_lag_ms,
        "last_heartbeat_gap_ms": observability.last_heartbeat_gap_ms,
        "max_heartbeat_gap_ms": observability.max_heartbeat_gap_ms,
        "latency_ms": latency_payload,
        "market_data_gap_warn_ms": market_data_gap_warn_ms,
        "command_lag_warn_ms": command_lag_warn_ms,
        "heartbeat_lag_warn_ms": heartbeat_lag_warn_ms,
        "last_cycle_correlation_id": observability.last_cycle_correlation_id,
    });

    let mut state = json!({
        "mode": mode,
        "started_at": started_at.to_rfc3339(),
        "uptime_seconds": uptime,
        "session_uptime_seconds": uptime,
        "session_id": session_id,
        "tenant_id": config.tenant_id,
        "exchange": config.exchange,
        "product_id": config.product_id,
        "next_daily_close_at": scheduler.next_close_utc().to_rfc3339(),
        "daily_close_schedule": scheduler.schedule_payload(),
        "pending_daily_close_schedule": scheduler.pending_schedule_payload(),
        "skip_daily_close": skip_next_daily_close,
        "reserve_usd": reserve_usd,
        "session_capital_usd": session_capital_usd,
        "total_fills": total_fills,
        "open_order_count": kernel.active_order_count(),
        "reference_mid": kernel.reference_mid(),
        "observability": observability_payload,
        "updated_at": now_utc.to_rfc3339(),
    });

    if let Some(tick) = last_tick {
        state["last_tick_mid"] = json!(tick.mid);
        state["last_tick_ts_ms"] = json!(tick.event_ts_ms);
        state["last_tick_bid"] = json!(tick.bid);
        state["last_tick_ask"] = json!(tick.ask);
    }

    control_plane.publish_state(state).await
}

async fn process_kernel_output(
    output: KernelOutput,
    execution: &mut dyn ExecutionProvider,
    publisher: &mut dyn StatePublisher,
    kernel: &mut TradingKernel,
    correlation_id: &str,
) -> Result<KernelOutputStats> {
    let mut stats = KernelOutputStats::default();
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
                        payload: with_correlation_payload(
                            json!({
                                "order_id": order_id,
                                "side": order.side,
                                "price": order.price,
                                "size_base": order.size_base,
                                "post_only": order.post_only,
                            }),
                            correlation_id,
                        ),
                        emitted_at_ts_ms: now_ms(),
                    })
                    .await?;
                stats.orders_submitted += 1;
                stats.events_published += 1;
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
                        payload: with_correlation_payload(
                            json!({ "order_id": order_id }),
                            correlation_id,
                        ),
                        emitted_at_ts_ms: now_ms(),
                    })
                    .await?;
                stats.orders_canceled += 1;
                stats.events_published += 1;
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
                        payload: with_correlation_payload(
                            json!({ "reason": reason }),
                            correlation_id,
                        ),
                        emitted_at_ts_ms: now_ms(),
                    })
                    .await?;
                stats.liquidations_requested += 1;
                stats.events_published += 1;
            }
            KernelDecision::Noop => {}
        }
    }

    for mut event in output.events {
        event.payload = with_correlation_payload(event.payload, correlation_id);
        publisher.publish(&event).await?;
        stats.events_published += 1;
    }
    Ok(stats)
}

fn elapsed_ms(started: Instant) -> i64 {
    started.elapsed().as_millis() as i64
}

fn update_latency_metric(last_ms: &mut i64, max_ms: &mut i64, current_ms: i64) {
    *last_ms = current_ms.max(0);
    *max_ms = (*max_ms).max(*last_ms);
}

fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};

    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or_default()
}

fn compute_tick_gap_ms(previous_tick_ts_ms: Option<i64>, current_tick_ts_ms: i64) -> Option<i64> {
    previous_tick_ts_ms.map(|previous| current_tick_ts_ms - previous)
}

fn is_market_data_gap(gap_ms: i64, threshold_ms: i64) -> bool {
    gap_ms >= 0 && gap_ms > threshold_ms
}

fn with_correlation_payload(payload: Value, correlation_id: &str) -> Value {
    match payload {
        Value::Object(mut map) => {
            map.entry("correlation_id".to_string())
                .or_insert_with(|| Value::String(correlation_id.to_string()));
            Value::Object(map)
        }
        other => other,
    }
}

fn synthesize_equity_snapshot(tick: &MarketTick) -> (f64, i64) {
    // Placeholder hook: in production this will read equity from tenant/par runtime state.
    let synthetic_equity = tick.mid;
    (synthetic_equity, tick.event_ts_ms)
}

#[derive(Debug, Clone)]
struct PendingSchedule {
    timezone_iana: String,
    close_hour: u8,
    close_minute: u8,
}

#[derive(Debug)]
struct DailyCloseScheduler {
    active_timezone_name: String,
    active_timezone: Tz,
    close_hour: u8,
    close_minute: u8,
    pending_schedule: Option<PendingSchedule>,
    next_close_utc: DateTime<Utc>,
}

impl DailyCloseScheduler {
    fn new(
        timezone_iana: &str,
        close_hour: u8,
        close_minute: u8,
        now_utc: DateTime<Utc>,
    ) -> Result<Self> {
        let active_timezone = parse_timezone(timezone_iana)?;
        validate_close_time(close_hour, close_minute)?;
        let next_close_utc =
            compute_next_close_utc(active_timezone, close_hour, close_minute, now_utc)?;

        Ok(Self {
            active_timezone_name: timezone_iana.to_string(),
            active_timezone,
            close_hour,
            close_minute,
            pending_schedule: None,
            next_close_utc,
        })
    }

    fn should_trigger(&self, now_utc: DateTime<Utc>) -> bool {
        now_utc >= self.next_close_utc
    }

    fn on_close_executed(&mut self, now_utc: DateTime<Utc>) -> Result<()> {
        if let Some(pending) = self.pending_schedule.take() {
            self.active_timezone = parse_timezone(&pending.timezone_iana)?;
            self.active_timezone_name = pending.timezone_iana;
            self.close_hour = pending.close_hour;
            self.close_minute = pending.close_minute;
        }

        self.next_close_utc = compute_next_close_utc(
            self.active_timezone,
            self.close_hour,
            self.close_minute,
            now_utc,
        )?;
        Ok(())
    }

    fn apply_schedule_change(
        &mut self,
        timezone_iana: &str,
        close_hour: u8,
        close_minute: u8,
        mode: CloseTimezoneChangeMode,
        now_utc: DateTime<Utc>,
    ) -> Result<()> {
        validate_close_time(close_hour, close_minute)?;
        let parsed_tz = parse_timezone(timezone_iana)?;

        match mode {
            CloseTimezoneChangeMode::NextCycle => {
                self.pending_schedule = Some(PendingSchedule {
                    timezone_iana: timezone_iana.to_string(),
                    close_hour,
                    close_minute,
                });
            }
            CloseTimezoneChangeMode::Immediate => {
                self.pending_schedule = None;
                self.active_timezone = parsed_tz;
                self.active_timezone_name = timezone_iana.to_string();
                self.close_hour = close_hour;
                self.close_minute = close_minute;
                self.next_close_utc = compute_next_close_utc(
                    self.active_timezone,
                    self.close_hour,
                    self.close_minute,
                    now_utc,
                )?;
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

    fn schedule_payload(&self) -> Value {
        json!({
            "local_timezone_iana": self.active_timezone_name,
            "daily_close_hour": self.close_hour,
            "daily_close_minute": self.close_minute,
        })
    }

    fn pending_schedule_payload(&self) -> Option<Value> {
        self.pending_schedule.as_ref().map(|pending| {
            json!({
                "local_timezone_iana": pending.timezone_iana,
                "daily_close_hour": pending.close_hour,
                "daily_close_minute": pending.close_minute,
            })
        })
    }
}

fn parse_timezone(timezone_iana: &str) -> Result<Tz> {
    timezone_iana
        .parse()
        .map_err(|_| anyhow!("invalid timezone IANA: {timezone_iana}"))
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

fn compute_next_close_utc(
    tz: Tz,
    close_hour: u8,
    close_minute: u8,
    now_utc: DateTime<Utc>,
) -> Result<DateTime<Utc>> {
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
    fn next_cycle_keeps_active_schedule_until_close() {
        let now_utc = Utc
            .with_ymd_and_hms(2026, 4, 20, 8, 0, 0)
            .single()
            .expect("valid dt");
        let mut scheduler =
            DailyCloseScheduler::new("Europe/Amsterdam", 0, 0, now_utc).expect("scheduler");
        let baseline_next_close = scheduler.next_close_utc();

        scheduler
            .apply_schedule_change(
                "Asia/Singapore",
                1,
                30,
                CloseTimezoneChangeMode::NextCycle,
                now_utc,
            )
            .expect("apply change");

        assert_eq!(scheduler.session_timezone_iana(), "Europe/Amsterdam");
        assert_eq!(scheduler.next_close_utc(), baseline_next_close);
        assert_eq!(scheduler.schedule_payload()["daily_close_hour"], 0);
        assert_eq!(scheduler.schedule_payload()["daily_close_minute"], 0);

        let pending = scheduler
            .pending_schedule_payload()
            .expect("pending schedule expected");
        assert_eq!(pending["local_timezone_iana"], "Asia/Singapore");
        assert_eq!(pending["daily_close_hour"], 1);
        assert_eq!(pending["daily_close_minute"], 30);

        let after_close = baseline_next_close + Duration::seconds(1);
        scheduler
            .on_close_executed(after_close)
            .expect("roll close");

        assert_eq!(scheduler.session_timezone_iana(), "Asia/Singapore");
        assert_eq!(scheduler.schedule_payload()["daily_close_hour"], 1);
        assert_eq!(scheduler.schedule_payload()["daily_close_minute"], 30);
    }

    #[test]
    fn immediate_schedule_change_recomputes_next_close() {
        let now_utc = Utc
            .with_ymd_and_hms(2026, 4, 20, 8, 0, 0)
            .single()
            .expect("valid dt");
        let mut scheduler =
            DailyCloseScheduler::new("Europe/Amsterdam", 0, 0, now_utc).expect("scheduler");
        let prev = scheduler.next_close_utc();

        scheduler
            .apply_schedule_change(
                "Asia/Singapore",
                3,
                15,
                CloseTimezoneChangeMode::Immediate,
                now_utc,
            )
            .expect("apply immediate");

        assert_eq!(scheduler.session_timezone_iana(), "Asia/Singapore");
        assert_eq!(scheduler.schedule_payload()["daily_close_hour"], 3);
        assert_eq!(scheduler.schedule_payload()["daily_close_minute"], 15);
        assert_ne!(scheduler.next_close_utc(), prev);
        assert!(scheduler.pending_schedule_payload().is_none());
    }

    #[test]
    fn tick_gap_detection_works_for_monotonic_stream() {
        assert_eq!(compute_tick_gap_ms(None, 1_000), None);
        assert_eq!(compute_tick_gap_ms(Some(1_000), 1_250), Some(250));
        assert_eq!(compute_tick_gap_ms(Some(1_250), 1_100), Some(-150));
    }

    #[test]
    fn market_data_gap_threshold_is_strict_and_non_negative() {
        assert!(!is_market_data_gap(-1, 500));
        assert!(!is_market_data_gap(500, 500));
        assert!(is_market_data_gap(501, 500));
    }

    #[test]
    fn correlation_id_is_attached_to_object_payload() {
        let enriched =
            with_correlation_payload(serde_json::json!({"state":"ok"}), "cycle:abc:1");
        assert_eq!(enriched["correlation_id"], "cycle:abc:1");

        let untouched = with_correlation_payload(serde_json::json!("plain"), "cycle:abc:1");
        assert_eq!(untouched, serde_json::json!("plain"));
    }
}
