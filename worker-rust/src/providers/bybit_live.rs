use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use hmac::{Hmac, Mac};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue, CONTENT_TYPE};
use serde::Deserialize;
use serde_json::{json, Value};
use sha2::Sha256;

use crate::kernel::types::{FillEvent, MarketTick, OrderRequest};

use super::ExecutionProvider;

type HmacSha256 = Hmac<Sha256>;

const DEFAULT_BYBIT_BASE_URL: &str = "https://api.bybit.com";
const DEFAULT_BYBIT_CATEGORY: &str = "spot";
const DEFAULT_RECV_WINDOW_MS: u64 = 5_000;
const DEFAULT_HTTP_TIMEOUT_MS: u64 = 8_000;
const MIN_LIQUIDATION_BASE_QTY: f64 = 1e-12;
const MAX_SEEN_EXEC_IDS: usize = 200_000;

const HEADER_API_KEY: &str = "X-BAPI-API-KEY";
const HEADER_TIMESTAMP: &str = "X-BAPI-TIMESTAMP";
const HEADER_SIGNATURE: &str = "X-BAPI-SIGN";
const HEADER_RECV_WINDOW: &str = "X-BAPI-RECV-WINDOW";
const HEADER_SIGN_TYPE: &str = "X-BAPI-SIGN-TYPE";

const ENDPOINT_ORDER_CREATE: &str = "/v5/order/create";
const ENDPOINT_ORDER_CANCEL: &str = "/v5/order/cancel";
const ENDPOINT_ORDER_CANCEL_ALL: &str = "/v5/order/cancel-all";
const ENDPOINT_EXECUTION_LIST: &str = "/v5/execution/list";
const ENDPOINT_WALLET_BALANCE: &str = "/v5/account/wallet-balance";
const ENDPOINT_INSTRUMENTS_INFO: &str = "/v5/market/instruments-info";

#[derive(Debug, Clone)]
struct BybitInstrumentRules {
    tick_size: f64,
    qty_step: f64,
    min_order_qty: Option<f64>,
    max_order_qty: Option<f64>,
    min_order_notional: Option<f64>,
}

pub struct BybitLiveExecutionProvider {
    api_key: String,
    api_secret: String,
    base_url: String,
    category: String,
    recv_window_ms: u64,
    client: reqwest::Client,
    seq: AtomicU64,
    seen_exec_ids: HashSet<String>,
    last_exec_time_ms: i64,
    rules_by_symbol: HashMap<String, BybitInstrumentRules>,
}

impl BybitLiveExecutionProvider {
    pub fn new_from_env() -> Result<Self> {
        let api_key = read_non_empty_env(&["TB_BYBIT_API_KEY", "BYBIT_API_KEY"])?;
        let api_secret = read_non_empty_env(&["TB_BYBIT_API_SECRET", "BYBIT_API_SECRET"])?;

        let base_url = read_optional_env(&["TB_BYBIT_REST_BASE_URL", "BYBIT_REST_BASE_URL"])
            .unwrap_or(DEFAULT_BYBIT_BASE_URL.to_string())
            .trim()
            .trim_end_matches('/')
            .to_string();
        let category =
            read_optional_env(&["TB_BYBIT_CATEGORY"]).unwrap_or(DEFAULT_BYBIT_CATEGORY.to_string());
        let recv_window_ms = read_env_u64("TB_BYBIT_RECV_WINDOW_MS", DEFAULT_RECV_WINDOW_MS);
        let timeout_ms =
            read_env_u64("TB_BYBIT_HTTP_TIMEOUT_MS", DEFAULT_HTTP_TIMEOUT_MS).max(1_000);

        let client = reqwest::Client::builder()
            .timeout(Duration::from_millis(timeout_ms))
            .build()
            .context("failed to construct Bybit HTTP client")?;

        Ok(Self {
            api_key,
            api_secret,
            base_url,
            category: category.trim().to_lowercase(),
            recv_window_ms,
            client,
            seq: AtomicU64::new(0),
            seen_exec_ids: HashSet::new(),
            last_exec_time_ms: now_ms() - 10 * 60 * 1000,
            rules_by_symbol: HashMap::new(),
        })
    }

    async fn signed_get_result(
        &self,
        path: &str,
        params: &[(String, String)],
    ) -> Result<(Value, Option<i64>)> {
        let query = build_query_string(params)?;
        let timestamp_ms = now_ms();
        let headers = self.build_signed_headers(timestamp_ms, &query)?;

        let mut url = format!("{}{}", self.base_url, path);
        if !query.is_empty() {
            url.push('?');
            url.push_str(&query);
        }

        let response = self
            .client
            .get(url)
            .headers(headers)
            .send()
            .await
            .with_context(|| format!("Bybit GET failed for path '{path}'"))?
            .error_for_status()
            .with_context(|| format!("Bybit GET returned non-success for path '{path}'"))?;

        let envelope: BybitEnvelope = response
            .json()
            .await
            .with_context(|| format!("Bybit GET decode failed for path '{path}'"))?;

        let result = unwrap_bybit_result(envelope, path)?;
        Ok((result, None))
    }

    async fn signed_post_result(&self, path: &str, body: &Value) -> Result<(Value, Option<i64>)> {
        let body_str =
            serde_json::to_string(body).context("failed to serialize Bybit POST body")?;
        let timestamp_ms = now_ms();
        let headers = self.build_signed_headers(timestamp_ms, &body_str)?;

        let response = self
            .client
            .post(format!("{}{}", self.base_url, path))
            .headers(headers)
            .body(body_str)
            .send()
            .await
            .with_context(|| format!("Bybit POST failed for path '{path}'"))?
            .error_for_status()
            .with_context(|| format!("Bybit POST returned non-success for path '{path}'"))?;

        let envelope: BybitEnvelope = response
            .json()
            .await
            .with_context(|| format!("Bybit POST decode failed for path '{path}'"))?;

        let result = unwrap_bybit_result(envelope, path)?;
        Ok((result, None))
    }

    fn build_signed_headers(&self, timestamp_ms: i64, payload: &str) -> Result<HeaderMap> {
        let timestamp = timestamp_ms.to_string();
        let recv_window = self.recv_window_ms.to_string();
        let prehash = format!("{timestamp}{}{recv_window}{payload}", self.api_key);
        let signature = hmac_sha256_hex(&self.api_secret, &prehash)?;

        let mut headers = HeaderMap::new();
        insert_header(&mut headers, HEADER_API_KEY, &self.api_key)?;
        insert_header(&mut headers, HEADER_TIMESTAMP, &timestamp)?;
        insert_header(&mut headers, HEADER_SIGNATURE, &signature)?;
        insert_header(&mut headers, HEADER_RECV_WINDOW, &recv_window)?;
        insert_header(&mut headers, HEADER_SIGN_TYPE, "2")?;
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        Ok(headers)
    }

    fn next_order_link_id(&self, product_id: &str, side: &str) -> String {
        let id = self.seq.fetch_add(1, Ordering::Relaxed) + 1;
        let side_tag = if side.eq_ignore_ascii_case("buy") {
            "b"
        } else {
            "s"
        };
        let mut value = format!(
            "tbv4-{}-{}-{}",
            product_id_to_bybit_symbol(product_id),
            side_tag,
            id
        );
        if value.len() > 36 {
            value.truncate(36);
        }
        value
    }

    fn parse_fill_event(
        &mut self,
        tenant_id: &str,
        exchange: &str,
        product_id: &str,
        item: &Value,
    ) -> Result<Option<FillEvent>> {
        let exec_id = item
            .get("execId")
            .and_then(|v| v.as_str())
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| format!("synthetic-{}", now_ms()));

        if !self.seen_exec_ids.insert(exec_id.clone()) {
            return Ok(None);
        }

        if self.seen_exec_ids.len() > MAX_SEEN_EXEC_IDS {
            self.seen_exec_ids.clear();
            self.seen_exec_ids.insert(exec_id.clone());
        }

        let side = normalize_fill_side(
            item.get("side")
                .and_then(|v| v.as_str())
                .ok_or_else(|| anyhow!("execution item missing side"))?,
        )?;
        let price = parse_number_str(item.get("execPrice"), "execPrice")?;
        let size_base = parse_number_str(item.get("execQty"), "execQty")?;
        let fee_quote = parse_number_str_opt(item.get("execFee")).unwrap_or(0.0);
        let event_ts_ms = parse_i64_opt(item.get("execTime")).unwrap_or_else(now_ms);
        self.last_exec_time_ms = self.last_exec_time_ms.max(event_ts_ms);

        let order_id = item
            .get("orderId")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();

        Ok(Some(FillEvent {
            tenant_id: tenant_id.to_string(),
            exchange: exchange.to_string(),
            product_id: product_id.to_string(),
            order_id,
            side,
            price,
            size_base,
            fee_quote,
            event_ts_ms,
        }))
    }

    async fn fetch_base_balance(&self, base_coin: &str) -> Result<f64> {
        let params = vec![
            ("accountType".to_string(), "UNIFIED".to_string()),
            ("coin".to_string(), base_coin.to_string()),
        ];
        let (result, _) = self
            .signed_get_result(ENDPOINT_WALLET_BALANCE, &params)
            .await?;

        let accounts = result
            .get("list")
            .and_then(|v| v.as_array())
            .ok_or_else(|| anyhow!("wallet balance result missing list[]"))?;

        for account in accounts {
            let Some(coins) = account.get("coin").and_then(|v| v.as_array()) else {
                continue;
            };
            for coin in coins {
                let name = coin.get("coin").and_then(|v| v.as_str()).unwrap_or("");
                if !name.eq_ignore_ascii_case(base_coin) {
                    continue;
                }

                if let Some(value) = parse_number_str_opt(coin.get("walletBalance")) {
                    return Ok(value.max(0.0));
                }
            }
        }

        Ok(0.0)
    }

    async fn get_instrument_rules(&mut self, symbol: &str) -> Result<BybitInstrumentRules> {
        if let Some(rules) = self.rules_by_symbol.get(symbol) {
            return Ok(rules.clone());
        }

        let params = vec![
            ("category".to_string(), self.category.clone()),
            ("symbol".to_string(), symbol.to_string()),
        ];
        let (result, _) = self
            .signed_get_result(ENDPOINT_INSTRUMENTS_INFO, &params)
            .await?;
        let list = result
            .get("list")
            .and_then(|v| v.as_array())
            .ok_or_else(|| anyhow!("instruments-info response missing result.list[]"))?;

        let item = list
            .iter()
            .find(|x| {
                x.get("symbol")
                    .and_then(|v| v.as_str())
                    .map(|s| s.eq_ignore_ascii_case(symbol))
                    .unwrap_or(false)
            })
            .or_else(|| list.first())
            .ok_or_else(|| anyhow!("instruments-info returned empty list for symbol {symbol}"))?;

        let rules = parse_instrument_rules(item)?;
        self.rules_by_symbol
            .insert(symbol.to_string(), rules.clone());
        Ok(rules)
    }

    fn normalize_limit_order(
        &self,
        side: &str,
        request_qty: f64,
        request_price: f64,
        rules: &BybitInstrumentRules,
    ) -> Result<(f64, f64)> {
        if request_qty <= 0.0 || request_price <= 0.0 {
            return Err(anyhow!(
                "order qty/price must be > 0, got qty={} price={}",
                request_qty,
                request_price
            ));
        }

        let qty = floor_to_step(request_qty, rules.qty_step);
        if qty <= 0.0 {
            return Err(anyhow!(
                "order qty {} rounded to zero with qty_step {}",
                request_qty,
                rules.qty_step
            ));
        }

        let price = if side.eq_ignore_ascii_case("Buy") {
            floor_to_step(request_price, rules.tick_size)
        } else {
            ceil_to_step(request_price, rules.tick_size)
        };
        if price <= 0.0 {
            return Err(anyhow!(
                "order price {} rounded to zero with tick_size {}",
                request_price,
                rules.tick_size
            ));
        }

        if let Some(min_qty) = rules.min_order_qty {
            if qty + 1e-12 < min_qty {
                return Err(anyhow!(
                    "order qty {} is below min_order_qty {} after quantization",
                    qty,
                    min_qty
                ));
            }
        }

        if let Some(max_qty) = rules.max_order_qty {
            if qty - 1e-12 > max_qty {
                return Err(anyhow!(
                    "order qty {} exceeds max_order_qty {} after quantization",
                    qty,
                    max_qty
                ));
            }
        }

        if let Some(min_notional) = rules.min_order_notional {
            let notional = qty * price;
            if notional + 1e-12 < min_notional {
                return Err(anyhow!(
                    "order notional {} is below min_order_notional {} after quantization",
                    notional,
                    min_notional
                ));
            }
        }

        Ok((qty, price))
    }

    fn normalize_market_sell_qty(
        &self,
        request_qty: f64,
        rules: &BybitInstrumentRules,
    ) -> Result<f64> {
        if request_qty <= 0.0 {
            return Ok(0.0);
        }
        let qty = floor_to_step(request_qty, rules.qty_step);
        if qty <= 0.0 {
            return Ok(0.0);
        }

        if let Some(min_qty) = rules.min_order_qty {
            if qty + 1e-12 < min_qty {
                return Ok(0.0);
            }
        }

        if let Some(max_qty) = rules.max_order_qty {
            if qty - 1e-12 > max_qty {
                return Ok(floor_to_step(max_qty, rules.qty_step));
            }
        }

        Ok(qty)
    }
}

#[async_trait]
impl ExecutionProvider for BybitLiveExecutionProvider {
    async fn submit(&mut self, request: &OrderRequest) -> Result<String> {
        let symbol = product_id_to_bybit_symbol(&request.product_id);
        let side = normalize_order_side(&request.side)?;
        let rules = self.get_instrument_rules(&symbol).await?;
        let (qty, price) =
            self.normalize_limit_order(side, request.size_base, request.price, &rules)?;
        let order_link_id = self.next_order_link_id(&request.product_id, &request.side);

        let body = json!({
            "category": self.category,
            "symbol": symbol,
            "side": side,
            "orderType": "Limit",
            "qty": format_decimal(qty),
            "price": format_decimal(price),
            "timeInForce": if request.post_only { "PostOnly" } else { "GTC" },
            "orderLinkId": order_link_id,
        });

        let (result, _) = self
            .signed_post_result(ENDPOINT_ORDER_CREATE, &body)
            .await?;
        let order_id = result
            .get("orderId")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow!("Bybit order/create missing result.orderId"))?
            .to_string();

        Ok(order_id)
    }

    async fn cancel(
        &mut self,
        _tenant_id: &str,
        _exchange: &str,
        product_id: &str,
        order_id: &str,
    ) -> Result<()> {
        let symbol = product_id_to_bybit_symbol(product_id);
        let body = json!({
            "category": self.category,
            "symbol": symbol,
            "orderId": order_id,
        });

        let _ = self
            .signed_post_result(ENDPOINT_ORDER_CANCEL, &body)
            .await?;
        Ok(())
    }

    async fn on_market_tick(&mut self, _tick: &MarketTick) -> Result<()> {
        Ok(())
    }

    async fn flush_fills(
        &mut self,
        tenant_id: &str,
        exchange: &str,
        product_id: &str,
    ) -> Result<Vec<FillEvent>> {
        let symbol = product_id_to_bybit_symbol(product_id);
        let params = vec![
            ("category".to_string(), self.category.clone()),
            ("symbol".to_string(), symbol),
            (
                "startTime".to_string(),
                self.last_exec_time_ms.max(0).to_string(),
            ),
            ("limit".to_string(), "100".to_string()),
        ];

        let (result, _) = self
            .signed_get_result(ENDPOINT_EXECUTION_LIST, &params)
            .await?;
        let items = result
            .get("list")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();

        let mut fills = Vec::new();
        for item in items {
            if let Some(fill) = self.parse_fill_event(tenant_id, exchange, product_id, &item)? {
                fills.push(fill);
            }
        }

        fills.sort_by_key(|f| f.event_ts_ms);
        Ok(fills)
    }

    async fn liquidate_inventory(
        &mut self,
        _tenant_id: &str,
        _exchange: &str,
        product_id: &str,
    ) -> Result<()> {
        let symbol = product_id_to_bybit_symbol(product_id);
        let base_coin = base_coin_from_product_id(product_id)?;
        let rules = self.get_instrument_rules(&symbol).await?;

        let cancel_all_body = json!({
            "category": self.category,
            "symbol": symbol,
        });
        let _ = self
            .signed_post_result(ENDPOINT_ORDER_CANCEL_ALL, &cancel_all_body)
            .await?;

        let base_balance = self.fetch_base_balance(&base_coin).await?;
        if base_balance <= MIN_LIQUIDATION_BASE_QTY {
            return Ok(());
        }
        let sell_qty = self.normalize_market_sell_qty(base_balance, &rules)?;
        if sell_qty <= MIN_LIQUIDATION_BASE_QTY {
            return Ok(());
        }

        let sell_body = json!({
            "category": self.category,
            "symbol": symbol,
            "side": "Sell",
            "orderType": "Market",
            "qty": format_decimal(sell_qty),
            "marketUnit": "baseCoin",
            "orderLinkId": self.next_order_link_id(product_id, "sell"),
        });

        let _ = self
            .signed_post_result(ENDPOINT_ORDER_CREATE, &sell_body)
            .await?;
        Ok(())
    }
}

#[derive(Debug, Deserialize)]
struct BybitEnvelope {
    #[serde(rename = "retCode")]
    ret_code: i32,
    #[serde(rename = "retMsg")]
    ret_msg: String,
    result: Option<Value>,
    #[serde(rename = "time")]
    _time: Option<i64>,
}

fn unwrap_bybit_result(envelope: BybitEnvelope, path: &str) -> Result<Value> {
    if envelope.ret_code != 0 {
        return Err(anyhow!(
            "Bybit API error on {}: retCode={} retMsg={}",
            path,
            envelope.ret_code,
            envelope.ret_msg
        ));
    }
    envelope
        .result
        .ok_or_else(|| anyhow!("Bybit API success response missing result for {}", path))
}

fn build_query_string(params: &[(String, String)]) -> Result<String> {
    let mut ordered: BTreeMap<&str, &str> = BTreeMap::new();
    for (k, v) in params {
        ordered.insert(k.as_str(), v.as_str());
    }
    serde_urlencoded::to_string(ordered).context("failed to encode query string")
}

fn insert_header(headers: &mut HeaderMap, key: &str, value: &str) -> Result<()> {
    let name = HeaderName::from_bytes(key.as_bytes())
        .with_context(|| format!("invalid header name '{key}'"))?;
    let val = HeaderValue::from_str(value)
        .with_context(|| format!("invalid header value for '{key}'"))?;
    headers.insert(name, val);
    Ok(())
}

fn hmac_sha256_hex(secret: &str, prehash: &str) -> Result<String> {
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes())
        .context("failed to initialize HMAC-SHA256")?;
    mac.update(prehash.as_bytes());
    let out = mac.finalize().into_bytes();
    Ok(hex::encode(out))
}

fn read_non_empty_env(keys: &[&str]) -> Result<String> {
    for key in keys {
        if let Ok(value) = std::env::var(key) {
            let trimmed = value.trim();
            if !trimmed.is_empty() {
                return Ok(trimmed.to_string());
            }
        }
    }
    Err(anyhow!("missing required env vars: {}", keys.join(" or ")))
}

fn read_optional_env(keys: &[&str]) -> Option<String> {
    for key in keys {
        if let Ok(value) = std::env::var(key) {
            let trimmed = value.trim();
            if !trimmed.is_empty() {
                return Some(trimmed.to_string());
            }
        }
    }
    None
}

fn read_env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|x| x.parse::<u64>().ok())
        .unwrap_or(default)
}

fn product_id_to_bybit_symbol(product_id: &str) -> String {
    let normalized = product_id.trim();
    if normalized.is_empty() {
        return "".to_string();
    }

    let compact: String = normalized
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric())
        .collect::<String>()
        .to_uppercase();

    if compact.ends_with("USDT") || compact.ends_with("USDC") {
        return compact;
    }

    if compact.ends_with("USD") {
        let base = compact.trim_end_matches("USD");
        if !base.is_empty() {
            return format!("{}USDT", base);
        }
    }

    compact
}

fn base_coin_from_product_id(product_id: &str) -> Result<String> {
    let left = product_id
        .split(['-', '/'])
        .next()
        .map(|s| {
            s.chars()
                .filter(|ch| ch.is_ascii_alphanumeric())
                .collect::<String>()
                .to_uppercase()
        })
        .unwrap_or_default();

    if left.is_empty() {
        return Err(anyhow!(
            "cannot infer base coin from product_id '{product_id}'"
        ));
    }
    Ok(left)
}

fn normalize_order_side(raw: &str) -> Result<&'static str> {
    let side = raw.trim().to_lowercase();
    match side.as_str() {
        "buy" => Ok("Buy"),
        "sell" => Ok("Sell"),
        _ => Err(anyhow!("unsupported order side '{raw}'")),
    }
}

fn normalize_fill_side(raw: &str) -> Result<String> {
    let side = raw.trim().to_lowercase();
    match side.as_str() {
        "buy" => Ok("buy".to_string()),
        "sell" => Ok("sell".to_string()),
        _ => Err(anyhow!("unsupported fill side '{raw}'")),
    }
}

fn parse_number_str(value: Option<&Value>, field: &str) -> Result<f64> {
    parse_number_str_opt(value).ok_or_else(|| anyhow!("missing or invalid numeric field '{field}'"))
}

fn parse_number_str_opt(value: Option<&Value>) -> Option<f64> {
    match value {
        Some(Value::String(s)) => s.trim().parse::<f64>().ok(),
        Some(Value::Number(n)) => n.as_f64(),
        _ => None,
    }
}

fn parse_i64_opt(value: Option<&Value>) -> Option<i64> {
    match value {
        Some(Value::String(s)) => s.trim().parse::<i64>().ok(),
        Some(Value::Number(n)) => n.as_i64(),
        _ => None,
    }
}

fn parse_instrument_rules(item: &Value) -> Result<BybitInstrumentRules> {
    let tick_size = parse_nested_number_required(
        item,
        &[("priceFilter", "tickSize"), ("priceFilter", "priceTick")],
        "priceFilter.tickSize",
    )?;

    let qty_step = parse_nested_number_optional(item, &[("lotSizeFilter", "qtyStep")])
        .or_else(|| parse_nested_number_optional(item, &[("lotSizeFilter", "basePrecision")]))
        .filter(|v| *v > 0.0)
        .ok_or_else(|| anyhow!("instrument rules missing lotSizeFilter.qtyStep/basePrecision"))?;

    let min_order_qty = parse_nested_number_optional(item, &[("lotSizeFilter", "minOrderQty")]);
    let max_order_qty = parse_nested_number_optional(item, &[("lotSizeFilter", "maxOrderQty")]);
    let min_order_notional =
        parse_nested_number_optional(item, &[("lotSizeFilter", "minOrderAmt")]);

    Ok(BybitInstrumentRules {
        tick_size,
        qty_step,
        min_order_qty,
        max_order_qty,
        min_order_notional,
    })
}

fn parse_nested_number_required(
    root: &Value,
    candidates: &[(&str, &str)],
    label: &str,
) -> Result<f64> {
    let value = parse_nested_number_optional(root, candidates)
        .ok_or_else(|| anyhow!("missing required instrument field '{label}'"))?;
    if value <= 0.0 {
        return Err(anyhow!(
            "instrument field '{label}' must be > 0, got {value}"
        ));
    }
    Ok(value)
}

fn parse_nested_number_optional(root: &Value, candidates: &[(&str, &str)]) -> Option<f64> {
    for (outer, inner) in candidates {
        let Some(v) = root.get(*outer).and_then(|x| x.get(*inner)) else {
            continue;
        };
        if let Some(parsed) = parse_number_str_opt(Some(v)) {
            return Some(parsed);
        }
    }
    None
}

fn floor_to_step(value: f64, step: f64) -> f64 {
    if step <= 0.0 {
        return value;
    }
    let units = (value / step).floor();
    (units * step).max(0.0)
}

fn ceil_to_step(value: f64, step: f64) -> f64 {
    if step <= 0.0 {
        return value;
    }
    let units = (value / step).ceil();
    (units * step).max(0.0)
}

fn format_decimal(value: f64) -> String {
    let mut s = format!("{value:.12}");
    while s.contains('.') && s.ends_with('0') {
        s.pop();
    }
    if s.ends_with('.') {
        s.pop();
    }
    if s.is_empty() {
        "0".to_string()
    } else {
        s
    }
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
    use std::collections::{HashMap, HashSet};

    use super::*;

    #[test]
    fn parses_instrument_rules_from_spot_shape() {
        let item = json!({
            "symbol":"SOLUSDT",
            "priceFilter": {"tickSize":"0.01"},
            "lotSizeFilter": {
                "basePrecision":"0.001",
                "minOrderQty":"0.01",
                "maxOrderQty":"500",
                "minOrderAmt":"1"
            }
        });

        let rules = parse_instrument_rules(&item).expect("rules");
        assert!((rules.tick_size - 0.01).abs() < 1e-12);
        assert!((rules.qty_step - 0.001).abs() < 1e-12);
        assert!((rules.min_order_qty.expect("min qty") - 0.01).abs() < 1e-12);
        assert!((rules.max_order_qty.expect("max qty") - 500.0).abs() < 1e-12);
        assert!((rules.min_order_notional.expect("min amt") - 1.0).abs() < 1e-12);
    }

    #[test]
    fn rounds_limit_order_by_side() {
        let provider = BybitLiveExecutionProvider {
            api_key: String::new(),
            api_secret: String::new(),
            base_url: String::new(),
            category: "spot".to_string(),
            recv_window_ms: 5_000,
            client: reqwest::Client::new(),
            seq: AtomicU64::new(0),
            seen_exec_ids: HashSet::new(),
            last_exec_time_ms: 0,
            rules_by_symbol: HashMap::new(),
        };
        let rules = BybitInstrumentRules {
            tick_size: 0.01,
            qty_step: 0.001,
            min_order_qty: Some(0.01),
            max_order_qty: Some(1000.0),
            min_order_notional: Some(1.0),
        };

        let (buy_qty, buy_price) = provider
            .normalize_limit_order("Buy", 1.23456, 100.127, &rules)
            .expect("buy");
        let (sell_qty, sell_price) = provider
            .normalize_limit_order("Sell", 1.23456, 100.127, &rules)
            .expect("sell");

        assert!((buy_qty - 1.234).abs() < 1e-9);
        assert!((sell_qty - 1.234).abs() < 1e-9);
        assert!((buy_price - 100.12).abs() < 1e-9);
        assert!((sell_price - 100.13).abs() < 1e-9);
    }

    #[test]
    fn rejects_limit_order_below_min_notional() {
        let provider = BybitLiveExecutionProvider {
            api_key: String::new(),
            api_secret: String::new(),
            base_url: String::new(),
            category: "spot".to_string(),
            recv_window_ms: 5_000,
            client: reqwest::Client::new(),
            seq: AtomicU64::new(0),
            seen_exec_ids: HashSet::new(),
            last_exec_time_ms: 0,
            rules_by_symbol: HashMap::new(),
        };
        let rules = BybitInstrumentRules {
            tick_size: 0.01,
            qty_step: 0.001,
            min_order_qty: Some(0.001),
            max_order_qty: None,
            min_order_notional: Some(10.0),
        };
        let err = provider
            .normalize_limit_order("Buy", 0.05, 100.0, &rules)
            .expect_err("must reject");
        assert!(err.to_string().contains("min_order_notional"));
    }

    #[test]
    fn hmac_signature_is_stable_for_known_vector() {
        let timestamp = "1658384314791";
        let api_key = "mykey";
        let recv_window = "5000";
        let query = "category=spot&symbol=SOLUSDT";
        let prehash = format!("{timestamp}{api_key}{recv_window}{query}");

        let signature = hmac_sha256_hex("mysecret", &prehash).expect("sign ok");

        assert_eq!(
            signature,
            "56b90d83b58aaeedadcab664ba8292a36cf1ad3274c5cbb41e3901a26181cf23"
        );
    }

    #[test]
    fn maps_product_id_to_symbol() {
        assert_eq!(product_id_to_bybit_symbol("SOL-USD"), "SOLUSDT");
        assert_eq!(product_id_to_bybit_symbol("DOGE/USD"), "DOGEUSDT");
        assert_eq!(product_id_to_bybit_symbol("BTCUSDT"), "BTCUSDT");
    }

    #[test]
    fn formats_decimal_for_api() {
        assert_eq!(format_decimal(1.2300000), "1.23");
        assert_eq!(format_decimal(10.0), "10");
        assert_eq!(format_decimal(0.0000012), "0.0000012");
    }

    #[test]
    fn normalizes_fill_side() {
        assert_eq!(normalize_fill_side("Buy").expect("buy"), "buy");
        assert_eq!(normalize_fill_side("sell").expect("sell"), "sell");
        assert!(normalize_fill_side("hold").is_err());
    }
}
