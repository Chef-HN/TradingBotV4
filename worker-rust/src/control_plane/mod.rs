use std::collections::HashMap;

use anyhow::{anyhow, Context, Result};
use chrono::Utc;
use redis::{aio::MultiplexedConnection, Client};
use serde::Deserialize;
use serde_json::{json, Value};

const STATE_TTL_SECONDS: usize = 30;
const HEARTBEAT_TTL_SECONDS: usize = 15;

const COMMAND_RESET: &str = "reset";
const COMMAND_SKIP_DAILY_CLOSE: &str = "skip_daily_close";
const COMMAND_UPDATE_DAILY_CLOSE_SCHEDULE: &str = "update_daily_close_schedule";

#[derive(Debug, Clone)]
pub enum RuntimeCommand {
    Reset {
        command_id: String,
        product_id: String,
        actor: Option<String>,
        reset_type: String,
        command_lag_ms: Option<i64>,
    },
    SkipDailyClose {
        command_id: String,
        product_id: String,
        command_lag_ms: Option<i64>,
    },
    UpdateDailyCloseSchedule {
        command_id: String,
        product_id: String,
        local_timezone_iana: String,
        daily_close_hour: u8,
        daily_close_minute: u8,
        mode: String,
        command_lag_ms: Option<i64>,
    },
}

pub struct RedisControlPlane {
    tenant_id: String,
    exchange: String,
    product_id: String,
    state_product_id: String,
    state_key: String,
    commands_key: String,
    heartbeat_key: String,
    conn: MultiplexedConnection,
}

#[derive(Debug)]
struct CommandScope<'a> {
    tenant_id: &'a str,
    exchange: &'a str,
    product_id: &'a str,
}

impl RedisControlPlane {
    pub async fn connect(
        redis_url: &str,
        tenant_id: &str,
        exchange: &str,
        product_id: &str,
        state_product_id: &str,
    ) -> Result<Self> {
        let client =
            Client::open(redis_url).with_context(|| format!("invalid redis url '{redis_url}'"))?;
        let conn = client
            .get_multiplexed_async_connection()
            .await
            .with_context(|| format!("failed to connect redis '{redis_url}'"))?;

        let tid = normalize_lower(tenant_id, "00000000-0000-0000-0000-000000000001");
        let ex = normalize_lower(exchange, "");
        let pid = normalize_product(product_id, "all");
        let state_pid = normalize_product(state_product_id, "all");

        Ok(Self {
            tenant_id: tid.clone(),
            exchange: ex.clone(),
            product_id: pid,
            state_product_id: state_pid.clone(),
            state_key: format!("tb:v4:{tid}:{ex}:{state_pid}:state"),
            commands_key: format!("tb:v4:{tid}:{ex}:all:commands"),
            heartbeat_key: format!("tb:v4:{tid}:{ex}:{state_pid}:heartbeat"),
            conn,
        })
    }

    pub async fn pop_commands(&mut self) -> Result<Vec<RuntimeCommand>> {
        let mut output = Vec::new();
        loop {
            let raw: Option<String> = redis::cmd("RPOP")
                .arg(&self.commands_key)
                .query_async(&mut self.conn)
                .await
                .context("failed to RPOP command from redis")?;
            let Some(raw) = raw else {
                break;
            };
            if let Some(cmd) = self.parse_command(&raw)? {
                output.push(cmd);
            }
        }
        Ok(output)
    }

    pub async fn publish_state(&mut self, mut state: Value) -> Result<()> {
        if !state.is_object() {
            return Err(anyhow!("state payload must be a JSON object"));
        }

        let now = Utc::now().to_rfc3339();
        state["_protocol"] = json!({
            "protocol_version": "tb.v4.control/1",
            "kind": "state",
            "tenant_id": self.tenant_id,
            "exchange": self.exchange,
            "product_id": self.state_product_id,
            "emitted_at": now,
        });

        let payload = serde_json::to_string(&state).context("failed to serialize state payload")?;
        redis::cmd("SET")
            .arg(&self.state_key)
            .arg(payload)
            .arg("EX")
            .arg(STATE_TTL_SECONDS)
            .query_async::<()>(&mut self.conn)
            .await
            .context("failed to publish state to redis")?;
        Ok(())
    }

    pub async fn publish_heartbeat(&mut self) -> Result<()> {
        redis::cmd("SET")
            .arg(&self.heartbeat_key)
            .arg("1")
            .arg("EX")
            .arg(HEARTBEAT_TTL_SECONDS)
            .query_async::<()>(&mut self.conn)
            .await
            .context("failed to publish heartbeat to redis")?;
        Ok(())
    }

    fn parse_command(&self, raw: &str) -> Result<Option<RuntimeCommand>> {
        parse_command_for_scope(
            raw,
            &CommandScope {
                tenant_id: &self.tenant_id,
                exchange: &self.exchange,
                product_id: &self.product_id,
            },
        )
    }
}

fn parse_command_for_scope(raw: &str, scope: &CommandScope<'_>) -> Result<Option<RuntimeCommand>> {
    let envelope: CommandEnvelope = match serde_json::from_str(raw) {
        Ok(v) => v,
        Err(_) => return Ok(None),
    };

    let cmd_type = envelope
        .command_type
        .or(envelope.legacy_type)
        .map(|v| v.trim().to_lowercase())
        .unwrap_or_default();
    if cmd_type.is_empty() {
        return Ok(None);
    }

    let tenant = envelope
        .tenant_id
        .map(|x| x.to_lowercase())
        .unwrap_or_else(|| scope.tenant_id.to_lowercase());
    let exchange = envelope
        .exchange
        .map(|x| x.to_lowercase())
        .unwrap_or_else(|| scope.exchange.to_lowercase());
    if tenant != scope.tenant_id || exchange != scope.exchange {
        return Ok(None);
    }

    let product_id = normalize_product(envelope.product_id.as_deref().unwrap_or("all"), "all");
    if product_id != "all" && product_id != scope.product_id {
        return Ok(None);
    }

    let command_id = envelope
        .command_id
        .unwrap_or_else(|| format!("redis-{}", now_ms()));

    let payload = merged_payload(envelope.payload, envelope.extra);
    let command_lag_ms = extract_command_lag_ms(&payload, now_ms());
    match cmd_type.as_str() {
        COMMAND_RESET => {
            let reset_type = payload
                .get("reset_type")
                .and_then(|v| v.as_str())
                .unwrap_or("daily_close")
                .trim()
                .to_lowercase();
            let actor = envelope.actor.or_else(|| {
                payload
                    .get("triggered_by")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
            });
            Ok(Some(RuntimeCommand::Reset {
                command_id,
                product_id,
                actor,
                reset_type,
                command_lag_ms,
            }))
        }
        COMMAND_SKIP_DAILY_CLOSE => Ok(Some(RuntimeCommand::SkipDailyClose {
            command_id,
            product_id,
            command_lag_ms,
        })),
        COMMAND_UPDATE_DAILY_CLOSE_SCHEDULE => {
            let tz = payload
                .get("local_timezone_iana")
                .and_then(|v| v.as_str())
                .ok_or_else(|| anyhow!("command missing local_timezone_iana"))?
                .to_string();
            let hour = payload
                .get("daily_close_hour")
                .and_then(as_i64)
                .ok_or_else(|| anyhow!("command missing daily_close_hour"))?;
            let minute = payload
                .get("daily_close_minute")
                .and_then(as_i64)
                .ok_or_else(|| anyhow!("command missing daily_close_minute"))?;
            if !(0..=23).contains(&hour) || !(0..=59).contains(&minute) {
                return Ok(None);
            }
            let mode = payload
                .get("mode")
                .and_then(|v| v.as_str())
                .unwrap_or("next_cycle")
                .trim()
                .to_lowercase();
            Ok(Some(RuntimeCommand::UpdateDailyCloseSchedule {
                command_id,
                product_id,
                local_timezone_iana: tz,
                daily_close_hour: hour as u8,
                daily_close_minute: minute as u8,
                mode,
                command_lag_ms,
            }))
        }
        _ => Ok(None),
    }
}

#[derive(Debug, Deserialize)]
struct CommandEnvelope {
    #[serde(default)]
    command_id: Option<String>,
    #[serde(default)]
    command_type: Option<String>,
    #[serde(default, rename = "type")]
    legacy_type: Option<String>,
    #[serde(default)]
    tenant_id: Option<String>,
    #[serde(default)]
    exchange: Option<String>,
    #[serde(default)]
    product_id: Option<String>,
    #[serde(default)]
    actor: Option<String>,
    #[serde(default)]
    payload: Option<Value>,
    #[serde(flatten)]
    extra: HashMap<String, Value>,
}

fn merged_payload(payload: Option<Value>, extra: HashMap<String, Value>) -> Value {
    let mut map = payload
        .and_then(|v| v.as_object().cloned())
        .unwrap_or_default();
    for (key, value) in extra {
        map.entry(key).or_insert(value);
    }
    Value::Object(map)
}

fn as_i64(v: &Value) -> Option<i64> {
    match v {
        Value::Number(n) => n.as_i64(),
        Value::String(s) => s.parse::<i64>().ok(),
        _ => None,
    }
}

fn extract_command_lag_ms(payload: &Value, now_ts_ms: i64) -> Option<i64> {
    let candidates = [
        "created_at_ts_ms",
        "issued_at_ts_ms",
        "emitted_at_ts_ms",
        "timestamp_ts_ms",
        "timestamp_ms",
        "ts_ms",
    ];

    for key in candidates {
        let Some(raw) = payload.get(key).and_then(as_i64) else {
            continue;
        };
        let lag = now_ts_ms - raw;
        if lag >= 0 {
            return Some(lag);
        }
    }
    None
}

fn normalize_lower(value: &str, fallback: &str) -> String {
    let trimmed = value.trim().to_lowercase();
    if trimmed.is_empty() {
        fallback.to_string()
    } else {
        trimmed
    }
}

fn normalize_product(value: &str, fallback: &str) -> String {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return fallback.to_string();
    }
    if trimmed.eq_ignore_ascii_case("all") {
        return "all".to_string();
    }
    trimmed.to_uppercase()
}

fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};

    let dt = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    dt.as_millis() as i64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merge_payload_preserves_payload_values() {
        let payload = json!({"mode":"next_cycle","daily_close_hour":0});
        let mut extra = HashMap::new();
        extra.insert("mode".to_string(), json!("immediate"));
        extra.insert("daily_close_minute".to_string(), json!(15));
        let merged = merged_payload(Some(payload), extra);
        assert_eq!(merged["mode"], "next_cycle");
        assert_eq!(merged["daily_close_hour"], 0);
        assert_eq!(merged["daily_close_minute"], 15);
    }

    #[test]
    fn as_i64_parses_string_and_number() {
        assert_eq!(as_i64(&json!(7)), Some(7));
        assert_eq!(as_i64(&json!("8")), Some(8));
        assert_eq!(as_i64(&json!("x")), None);
    }

    #[test]
    fn parse_schedule_command_nested_payload() {
        let raw = json!({
            "command_type":"update_daily_close_schedule",
            "tenant_id":"t1",
            "exchange":"bybit",
            "product_id":"SOL-USD",
            "payload":{
                "local_timezone_iana":"Asia/Singapore",
                "daily_close_hour":0,
                "daily_close_minute":0,
                "mode":"next_cycle"
            }
        })
        .to_string();

        let parsed = parse_command_for_scope(
            &raw,
            &CommandScope {
                tenant_id: "t1",
                exchange: "bybit",
                product_id: "SOL-USD",
            },
        )
        .expect("parse ok");

        match parsed {
            Some(RuntimeCommand::UpdateDailyCloseSchedule {
                local_timezone_iana,
                daily_close_hour,
                daily_close_minute,
                mode,
                ..
            }) => {
                assert_eq!(local_timezone_iana, "Asia/Singapore");
                assert_eq!(daily_close_hour, 0);
                assert_eq!(daily_close_minute, 0);
                assert_eq!(mode, "next_cycle");
            }
            _ => panic!("expected update schedule command"),
        }
    }

    #[test]
    fn parse_schedule_command_flattened_payload() {
        let raw = json!({
            "type":"update_daily_close_schedule",
            "tenant_id":"t1",
            "exchange":"bybit",
            "product_id":"all",
            "local_timezone_iana":"Europe/Paris",
            "daily_close_hour":"1",
            "daily_close_minute":30,
            "mode":"immediate"
        })
        .to_string();

        let parsed = parse_command_for_scope(
            &raw,
            &CommandScope {
                tenant_id: "t1",
                exchange: "bybit",
                product_id: "DOGE-USD",
            },
        )
        .expect("parse ok");

        match parsed {
            Some(RuntimeCommand::UpdateDailyCloseSchedule {
                product_id,
                local_timezone_iana,
                daily_close_hour,
                daily_close_minute,
                mode,
                ..
            }) => {
                assert_eq!(product_id, "all");
                assert_eq!(local_timezone_iana, "Europe/Paris");
                assert_eq!(daily_close_hour, 1);
                assert_eq!(daily_close_minute, 30);
                assert_eq!(mode, "immediate");
            }
            _ => panic!("expected update schedule command"),
        }
    }

    #[test]
    fn parse_command_filters_mismatched_scope() {
        let raw = json!({
            "type":"skip_daily_close",
            "tenant_id":"t2",
            "exchange":"bybit",
            "product_id":"SOL-USD"
        })
        .to_string();

        let parsed = parse_command_for_scope(
            &raw,
            &CommandScope {
                tenant_id: "t1",
                exchange: "bybit",
                product_id: "SOL-USD",
            },
        )
        .expect("parse ok");
        assert!(parsed.is_none());
    }

    #[test]
    fn parse_schedule_command_rejects_invalid_time() {
        let raw = json!({
            "type":"update_daily_close_schedule",
            "tenant_id":"t1",
            "exchange":"bybit",
            "product_id":"SOL-USD",
            "local_timezone_iana":"Europe/Paris",
            "daily_close_hour":99,
            "daily_close_minute":0
        })
        .to_string();

        let parsed = parse_command_for_scope(
            &raw,
            &CommandScope {
                tenant_id: "t1",
                exchange: "bybit",
                product_id: "SOL-USD",
            },
        )
        .expect("parse ok");
        assert!(parsed.is_none());
    }

    #[test]
    fn parses_command_lag_from_payload_timestamp() {
        let now = now_ms();
        let raw = json!({
            "type":"skip_daily_close",
            "tenant_id":"t1",
            "exchange":"bybit",
            "product_id":"SOL-USD",
            "created_at_ts_ms": now - 1234
        })
        .to_string();

        let parsed = parse_command_for_scope(
            &raw,
            &CommandScope {
                tenant_id: "t1",
                exchange: "bybit",
                product_id: "SOL-USD",
            },
        )
        .expect("parse ok");

        match parsed {
            Some(RuntimeCommand::SkipDailyClose { command_lag_ms, .. }) => {
                let lag = command_lag_ms.expect("lag");
                assert!(lag >= 1200);
            }
            _ => panic!("expected skip daily close"),
        }
    }
}
