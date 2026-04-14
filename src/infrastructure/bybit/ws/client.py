from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import time
from collections.abc import AsyncIterator
from datetime import UTC, datetime

import aiohttp

from config import BybitSettings


def _bybit_symbol_to_internal(symbol: str) -> str:
    """Convert Bybit symbol (BTCUSDT) to internal format (BTC-USD)."""
    if symbol.endswith("USDT"):
        return symbol[:-4] + "-USD"
    if symbol.endswith("USDC"):
        return symbol[:-4] + "-USD"
    return symbol


class BybitWebSocketClient:
    """
    Bybit V5 public+private WebSocket client.

    iter_messages() yields dicts normalized to the Coinbase ticker format
    so the MarketDataEngine can consume them unchanged:

        {
            "channel": "ticker",
            "timestamp": "2026-...",
            "events": [{"tickers": [{"product_id": "BTC-USD", "best_bid": ..., ...}]}]
        }
    """

    _PUBLIC_URL = "wss://stream.bybit.com/v5/public/spot"
    _PRIVATE_URL = "wss://stream.bybit.com/v5/private"
    _PING_INTERVAL = 20  # seconds

    def __init__(self, settings: BybitSettings) -> None:
        self.settings = settings
        self._subscribed_symbols: list[str] = []   # internal format (BTC-USD)
        self._session: aiohttp.ClientSession | None = None
        self._ws_pub: aiohttp.ClientWebSocketResponse | None = None
        self._ws_priv: aiohttp.ClientWebSocketResponse | None = None
        self._ping_task: asyncio.Task | None = None
        # Last known bid/ask and trade price per symbol
        self._last_bid: dict[str, str] = {}
        self._last_ask: dict[str, str] = {}
        self._last_trade: dict[str, str] = {}
        self._last_trade_size: dict[str, str] = {}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def connect(self) -> None:
        self._session = aiohttp.ClientSession()
        self._ws_pub = await self._session.ws_connect(self._PUBLIC_URL)
        if self.settings.api_key and self.settings.api_secret:
            self._ws_priv = await self._session.ws_connect(self._PRIVATE_URL)
            await self._auth_private()

    async def close(self) -> None:
        if self._ping_task:
            self._ping_task.cancel()
            self._ping_task = None
        for ws in [self._ws_pub, self._ws_priv]:
            if ws is not None:
                try:
                    await ws.close()
                except Exception:
                    pass
        if self._session:
            await self._session.close()
        self._ws_pub = None
        self._ws_priv = None
        self._session = None

    async def subscribe_market_data(self, product_ids: list[str], channels: list[str]) -> None:
        """product_ids in internal format (BTC-USD). channels ignored — subscribes ticker + orderbook.1."""
        self._subscribed_symbols = list(product_ids)
        bybit_symbols = [self._to_bybit_symbol(p) for p in product_ids]
        args = (
            [f"tickers.{s}" for s in bybit_symbols]
            + [f"orderbook.1.{s}" for s in bybit_symbols]
        )
        await self._ws_pub.send_json({"op": "subscribe", "args": args})
        self._ping_task = asyncio.create_task(self._ping_loop())

    async def iter_messages(self) -> AsyncIterator[dict]:
        """Yield normalized ticker dicts (Coinbase format) from Bybit WS."""
        if self._ws_pub is None:
            raise RuntimeError("WebSocket not connected")
        while True:
            try:
                msg = await self._ws_pub.receive(timeout=self.settings.ws_heartbeat_timeout_seconds + 10)
                if msg.type == aiohttp.WSMsgType.TEXT:
                    raw = json.loads(msg.data)
                    normalized = self._normalize(raw)
                    if normalized:
                        yield normalized
                elif msg.type in {
                    aiohttp.WSMsgType.CLOSE,
                    aiohttp.WSMsgType.CLOSED,
                    aiohttp.WSMsgType.CLOSING,
                    aiohttp.WSMsgType.ERROR,
                }:
                    await self._reconnect()
                else:
                    continue
            except asyncio.TimeoutError:
                await self._reconnect()
            except aiohttp.ClientError:
                await self._reconnect()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _to_bybit_symbol(internal: str) -> str:
        """BTC-USD → BTCUSDT"""
        base, quote = internal.split("-", 1)
        return base + ("USDT" if quote == "USD" else quote)

    def _normalize(self, raw: dict) -> dict | None:
        """Convert Bybit ticker/orderbook message → Coinbase ticker format."""
        topic = raw.get("topic", "")
        data = raw.get("data", {})
        ts_ms = raw.get("ts", int(time.time() * 1000))
        ts_iso = datetime.fromtimestamp(ts_ms / 1000, tz=UTC).isoformat()

        if topic.startswith("orderbook.1."):
            # orderbook.1 gives best bid/ask — use it as the primary price source
            bybit_sym = topic.replace("orderbook.1.", "")
            internal_sym = _bybit_symbol_to_internal(bybit_sym)
            bids = data.get("b", [])
            asks = data.get("a", [])
            if bids:
                self._last_bid[internal_sym] = bids[0][0]
            if asks:
                self._last_ask[internal_sym] = asks[0][0]
            bid = self._last_bid.get(internal_sym)
            ask = self._last_ask.get(internal_sym)
            if not bid or not ask:
                return None
            # Emit a ticker event so MarketDataEngine always gets fresh bid/ask
            last = self._last_trade.get(internal_sym, bid)
            last_size = self._last_trade_size.get(internal_sym, "0")
            return {
                "channel": "ticker",
                "timestamp": ts_iso,
                "events": [{"tickers": [{
                    "product_id": internal_sym,
                    "best_bid": bid,
                    "best_ask": ask,
                    "price": last,
                    "last_size": last_size,
                    "time": ts_iso,
                }]}],
            }

        if topic.startswith("tickers."):
            # tickers channel provides lastPrice/lastSize (no bid/ask on Bybit spot).
            # We store these so orderbook.1 events can include trade price in their output.
            bybit_sym = data.get("symbol", topic.replace("tickers.", ""))
            internal_sym = _bybit_symbol_to_internal(bybit_sym)
            last = data.get("lastPrice", "0")
            last_size = data.get("lastSize", "0")
            if last and last != "0":
                self._last_trade[internal_sym] = last
                self._last_trade_size[internal_sym] = last_size

        # Only orderbook.1 emits ticker events; everything else is ignored
        return None

    async def _auth_private(self) -> None:
        expires = int((time.time() + 10) * 1000)
        sign = hmac.new(
            self.settings.api_secret.encode(),
            f"GET/realtime{expires}".encode(),
            hashlib.sha256,
        ).hexdigest()
        await self._ws_priv.send_json({
            "op": "auth",
            "args": [self.settings.api_key, expires, sign],
        })

    async def _ping_loop(self) -> None:
        while True:
            await asyncio.sleep(self._PING_INTERVAL)
            try:
                if self._ws_pub and not self._ws_pub.closed:
                    await self._ws_pub.send_json({"op": "ping"})
            except Exception:
                break

    async def _reconnect(self) -> None:
        syms = list(self._subscribed_symbols)
        await self.close()
        await asyncio.sleep(self.settings.ws_reconnect_delay_seconds)
        await self.connect()
        if syms:
            await self.subscribe_market_data(syms, ["ticker"])
