use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use serde::Deserialize;

use crate::kernel::types::{MarketTick, StrategyConfig};

use super::MarketDataProvider;

const DEFAULT_BYBIT_BASE_URL: &str = "https://api.bybit.com";
const DEFAULT_BYBIT_CATEGORY: &str = "spot";
const DEFAULT_HTTP_TIMEOUT_MS: u64 = 5_000;

pub struct BybitRestMarketDataProvider {
    tenant_id: String,
    exchange: String,
    product_id: String,
    category: String,
    symbol: String,
    base_url: String,
    poll_interval: Duration,
    client: reqwest::Client,
}

impl BybitRestMarketDataProvider {
    pub fn from_strategy(config: &StrategyConfig, poll_interval_ms: u64) -> Result<Self> {
        let category = std::env::var("TB_BYBIT_CATEGORY")
            .unwrap_or_else(|_| DEFAULT_BYBIT_CATEGORY.to_string())
            .trim()
            .to_lowercase();
        let base_url = std::env::var("TB_BYBIT_BASE_URL")
            .unwrap_or_else(|_| DEFAULT_BYBIT_BASE_URL.to_string())
            .trim()
            .trim_end_matches('/')
            .to_string();
        let symbol = std::env::var("TB_BYBIT_SYMBOL")
            .ok()
            .map(|v| v.trim().to_uppercase())
            .filter(|v| !v.is_empty())
            .unwrap_or_else(|| product_id_to_bybit_symbol(&config.product_id));

        let http_timeout_ms = read_env_u64("TB_MARKET_HTTP_TIMEOUT_MS", DEFAULT_HTTP_TIMEOUT_MS);
        let client = reqwest::Client::builder()
            .timeout(Duration::from_millis(http_timeout_ms.max(500)))
            .build()
            .context("failed to construct reqwest client")?;

        Ok(Self {
            tenant_id: config.tenant_id.clone(),
            exchange: config.exchange.clone(),
            product_id: config.product_id.clone(),
            category,
            symbol,
            base_url,
            poll_interval: Duration::from_millis(poll_interval_ms.max(100)),
            client,
        })
    }

    async fn fetch_quote(&self) -> Result<(f64, f64, i64)> {
        let url = format!("{}/v5/market/tickers", self.base_url);
        let response = self
            .client
            .get(url)
            .query(&[
                ("category", self.category.as_str()),
                ("symbol", self.symbol.as_str()),
            ])
            .send()
            .await
            .with_context(|| {
                format!(
                    "bybit request failed category={} symbol={}",
                    self.category, self.symbol
                )
            })?
            .error_for_status()
            .context("bybit returned non-success status")?;

        let body: BybitTickerResponse = response
            .json()
            .await
            .context("failed to decode bybit ticker response json")?;
        parse_bybit_ticker_response(&body, &self.symbol)
    }
}

#[async_trait]
impl MarketDataProvider for BybitRestMarketDataProvider {
    async fn next_tick(&mut self) -> Result<MarketTick> {
        tokio::time::sleep(self.poll_interval).await;
        let (bid, ask, ts_ms) = self.fetch_quote().await?;
        let mid = if ask >= bid {
            (bid + ask) / 2.0
        } else {
            return Err(anyhow!(
                "invalid quote from bybit: ask({ask}) < bid({bid}) for symbol {}",
                self.symbol
            ));
        };

        Ok(MarketTick {
            tenant_id: self.tenant_id.clone(),
            exchange: self.exchange.clone(),
            product_id: self.product_id.clone(),
            bid,
            ask,
            mid,
            event_ts_ms: ts_ms,
        })
    }
}

#[derive(Debug, Deserialize)]
struct BybitTickerResponse {
    #[serde(rename = "retCode")]
    ret_code: i32,
    #[serde(rename = "retMsg")]
    ret_msg: String,
    result: Option<BybitTickerResult>,
    time: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct BybitTickerResult {
    list: Vec<BybitTickerItem>,
}

#[derive(Debug, Deserialize)]
struct BybitTickerItem {
    symbol: Option<String>,
    #[serde(rename = "bid1Price")]
    bid1_price: Option<String>,
    #[serde(rename = "ask1Price")]
    ask1_price: Option<String>,
    #[serde(rename = "lastPrice")]
    last_price: Option<String>,
}

fn parse_bybit_ticker_response(
    body: &BybitTickerResponse,
    expected_symbol: &str,
) -> Result<(f64, f64, i64)> {
    if body.ret_code != 0 {
        return Err(anyhow!(
            "bybit retCode={} retMsg={}",
            body.ret_code,
            body.ret_msg
        ));
    }

    let result = body
        .result
        .as_ref()
        .ok_or_else(|| anyhow!("bybit response missing result"))?;
    let first = result
        .list
        .iter()
        .find(|x| {
            x.symbol
                .as_ref()
                .map(|s| s.eq_ignore_ascii_case(expected_symbol))
                .unwrap_or(true)
        })
        .or_else(|| result.list.first())
        .ok_or_else(|| anyhow!("bybit response has empty ticker list"))?;

    let fallback = first
        .last_price
        .as_deref()
        .ok_or_else(|| anyhow!("bybit response missing lastPrice"))?;
    let bid = parse_f64(
        first.bid1_price.as_deref().unwrap_or(fallback),
        "bid1Price/lastPrice",
    )?;
    let ask = parse_f64(
        first.ask1_price.as_deref().unwrap_or(fallback),
        "ask1Price/lastPrice",
    )?;
    let ts_ms = body.time.unwrap_or_else(now_ms);

    Ok((bid, ask, ts_ms))
}

fn parse_f64(raw: &str, field: &str) -> Result<f64> {
    let value = raw
        .trim()
        .parse::<f64>()
        .with_context(|| format!("cannot parse {field}='{raw}' as number"))?;
    if value <= 0.0 {
        return Err(anyhow!("{field} must be > 0, got {value}"));
    }
    Ok(value)
}

fn read_env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|x| x.parse::<u64>().ok())
        .unwrap_or(default)
}

fn product_id_to_bybit_symbol(product_id: &str) -> String {
    let cleaned: String = product_id
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric())
        .collect::<String>()
        .to_uppercase();

    if cleaned.ends_with("USDT")
        || cleaned.ends_with("USDC")
        || cleaned.ends_with("BTC")
        || cleaned.ends_with("ETH")
    {
        return cleaned;
    }

    if cleaned.ends_with("USD") {
        let base = cleaned.trim_end_matches("USD");
        if !base.is_empty() {
            return format!("{base}USDT");
        }
    }

    cleaned
}

fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};

    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn product_id_maps_to_bybit_symbol() {
        assert_eq!(product_id_to_bybit_symbol("SOL-USD"), "SOLUSDT");
        assert_eq!(product_id_to_bybit_symbol("DOGE/USD"), "DOGEUSDT");
        assert_eq!(product_id_to_bybit_symbol("BTCUSDT"), "BTCUSDT");
    }

    #[test]
    fn parse_ticker_response_uses_bid_ask() {
        let body: BybitTickerResponse = serde_json::from_value(serde_json::json!({
            "retCode": 0,
            "retMsg": "OK",
            "result": {
                "list": [
                    {
                        "symbol": "SOLUSDT",
                        "bid1Price": "141.25",
                        "ask1Price": "141.27",
                        "lastPrice": "141.26"
                    }
                ]
            },
            "time": 1714022400000i64
        }))
        .expect("valid fixture");

        let (bid, ask, ts) = parse_bybit_ticker_response(&body, "SOLUSDT").expect("parsed");
        assert_eq!(bid, 141.25);
        assert_eq!(ask, 141.27);
        assert_eq!(ts, 1714022400000);
    }

    #[test]
    fn parse_ticker_response_falls_back_to_last_price() {
        let body: BybitTickerResponse = serde_json::from_value(serde_json::json!({
            "retCode": 0,
            "retMsg": "OK",
            "result": {
                "list": [
                    {
                        "symbol": "DOGEUSDT",
                        "lastPrice": "0.152"
                    }
                ]
            },
            "time": 1714022400001i64
        }))
        .expect("valid fixture");

        let (bid, ask, ts) = parse_bybit_ticker_response(&body, "DOGEUSDT").expect("parsed");
        assert_eq!(bid, 0.152);
        assert_eq!(ask, 0.152);
        assert_eq!(ts, 1714022400001);
    }
}
