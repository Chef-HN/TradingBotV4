use crate::kernel::types::StrategyConfig;
use anyhow::{anyhow, Context, Result};

#[derive(Debug, Clone)]
pub struct RuntimeSources {
    pub tenant_id: String,
    pub exchange: String,
    pub product_id: String,
    pub redis_url: String,
    pub db_dsn: String,
    pub execution_mode_override: Option<String>,
    pub tick_interval_ms: u64,
    pub reserve_usd: f64,
    pub session_capital_override_usd: Option<f64>,
}

#[derive(Debug, Clone)]
pub struct DbStrategySnapshot {
    pub strategy: StrategyConfig,
    pub session_capital_usd: f64,
    pub paper_mode: bool,
}

impl RuntimeSources {
    pub fn from_env() -> Result<Self> {
        let tenant_id = std::env::var("TB_TENANT_ID")
            .unwrap_or_else(|_| "00000000-0000-0000-0000-000000000001".to_string())
            .trim()
            .to_lowercase();
        let exchange = std::env::var("TB_EXCHANGE")
            .unwrap_or_else(|_| "bybit".to_string())
            .trim()
            .to_lowercase();
        let product_id = normalize_product(
            &std::env::var("TB_PRODUCT_ID").unwrap_or_else(|_| "SOL-USD".to_string()),
        );
        let redis_url = std::env::var("TB_REDIS_URL")
            .or_else(|_| std::env::var("REDIS_URL"))
            .unwrap_or_else(|_| "redis://localhost:6379/1".to_string());
        let db_dsn = build_db_dsn()?;

        let execution_mode_override = std::env::var("TB_EXECUTION_MODE")
            .ok()
            .map(|v| v.trim().to_lowercase())
            .filter(|v| !v.is_empty());

        let tick_interval_ms = read_env_u64("TB_TICK_INTERVAL_MS", 1_000);
        let reserve_usd = read_env_f64("TB_RESERVE_USD", 0.0);
        let session_capital_override_usd = std::env::var("TB_SESSION_CAPITAL_USD")
            .ok()
            .and_then(|v| v.parse::<f64>().ok());

        Ok(Self {
            tenant_id,
            exchange,
            product_id,
            redis_url,
            db_dsn,
            execution_mode_override,
            tick_interval_ms,
            reserve_usd,
            session_capital_override_usd,
        })
    }
}

pub async fn load_strategy_from_db(
    db_dsn: &str,
    tenant_id: &str,
    exchange: &str,
    product_id: &str,
) -> Result<DbStrategySnapshot> {
    let (client, connection) = tokio_postgres::connect(db_dsn, tokio_postgres::NoTls)
        .await
        .with_context(|| format!("failed to connect postgres with dsn '{db_dsn}'"))?;
    tokio::spawn(async move {
        if let Err(err) = connection.await {
            eprintln!("postgres connection task ended: {err}");
        }
    });

    let row = client
        .query_opt(
            r#"
            SELECT
                spacing_bps::float8 AS spacing_bps,
                rebalance_threshold_bps::float8 AS rebalance_threshold_bps,
                grid_levels::int4 AS grid_levels,
                level_size_quote::float8 AS level_size_quote,
                local_timezone_iana,
                daily_close_hour::int4 AS daily_close_hour,
                daily_close_minute::int4 AS daily_close_minute,
                session_capital_usd::float8 AS session_capital_usd,
                paper_mode
            FROM tenant_pair_strategies
            WHERE tenant_id::text = $1
              AND exchange_name = $2
              AND product_id = $3
              AND is_active IS TRUE
            ORDER BY updated_at DESC
            LIMIT 1
            "#,
            &[&tenant_id, &exchange, &product_id],
        )
        .await
        .with_context(|| {
            format!(
                "query failed loading tenant_pair_strategies for tenant={tenant_id} exchange={exchange} product={product_id}"
            )
        })?;

    let row = row.ok_or_else(|| {
        anyhow!(
            "no active pair strategy row found for tenant={tenant_id} exchange={exchange} product={product_id}"
        )
    })?;

    let strategy = StrategyConfig {
        tenant_id: tenant_id.to_string(),
        exchange: exchange.to_string(),
        product_id: product_id.to_string(),
        spacing_bps: row.get::<_, f64>("spacing_bps"),
        rebalance_threshold_bps: row.get::<_, f64>("rebalance_threshold_bps"),
        grid_levels: row.get::<_, i32>("grid_levels"),
        level_size_quote: row.get::<_, f64>("level_size_quote"),
        local_timezone_iana: row.get::<_, String>("local_timezone_iana"),
        daily_close_hour: checked_u8(row.get::<_, i32>("daily_close_hour"), "daily_close_hour")?,
        daily_close_minute: checked_u8(
            row.get::<_, i32>("daily_close_minute"),
            "daily_close_minute",
        )?,
    };
    let session_capital_usd = row.get::<_, f64>("session_capital_usd");
    let paper_mode = row.get::<_, bool>("paper_mode");

    Ok(DbStrategySnapshot {
        strategy,
        session_capital_usd,
        paper_mode,
    })
}

fn build_db_dsn() -> Result<String> {
    if let Ok(raw) = std::env::var("TB_DB_DSN").or_else(|_| std::env::var("DB_DSN")) {
        return Ok(normalize_postgres_dsn(raw));
    }

    let host = std::env::var("DB_HOST").unwrap_or_else(|_| "localhost".to_string());
    let port = std::env::var("DB_PORT").unwrap_or_else(|_| "5433".to_string());
    let name = std::env::var("DB_NAME").unwrap_or_else(|_| "tradingbotv3".to_string());
    let user = std::env::var("DB_USER").unwrap_or_else(|_| "tradingbot".to_string());
    let password = std::env::var("DB_PASSWORD").unwrap_or_else(|_| "tradingbot".to_string());

    if host.trim().is_empty() || name.trim().is_empty() || user.trim().is_empty() {
        return Err(anyhow!("DB_HOST, DB_NAME and DB_USER must not be empty"));
    }

    Ok(format!(
        "postgresql://{}:{}@{}:{}/{}",
        user.trim(),
        password.trim(),
        host.trim(),
        port.trim(),
        name.trim()
    ))
}

fn normalize_postgres_dsn(raw: String) -> String {
    raw.replace("postgresql+asyncpg://", "postgresql://")
}

fn normalize_product(raw: &str) -> String {
    let trimmed = raw.trim();
    if trimmed.eq_ignore_ascii_case("all") {
        "all".to_string()
    } else {
        trimmed.to_uppercase()
    }
}

fn checked_u8(value: i32, field: &str) -> Result<u8> {
    if !(0..=255).contains(&value) {
        return Err(anyhow!("{field} must be in [0,255], got {value}"));
    }
    Ok(value as u8)
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
