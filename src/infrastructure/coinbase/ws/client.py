from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator

import aiohttp

from config import CoinbaseSettings

from ..rest.auth import CoinbaseJWTAuth, resolve_api_secret


class CoinbaseWebSocketClient:
    def __init__(self, settings: CoinbaseSettings) -> None:
        self.settings = settings
        resolved_secret = resolve_api_secret(settings.api_secret, settings.api_secret_file)
        self._auth = CoinbaseJWTAuth(settings.api_key, resolved_secret) if settings.api_key and resolved_secret else None
        self._session: aiohttp.ClientSession | None = None
        self._ws: aiohttp.ClientWebSocketResponse | None = None
        self._market_subscriptions: list[tuple[list[str], list[str]]] = []
        self._user_subscriptions: list[list[str]] = []

    async def connect(self) -> None:
        self._session = aiohttp.ClientSession()
        self._ws = await self._session.ws_connect(
            self.settings.ws_base_url,
            heartbeat=self.settings.ws_heartbeat_timeout_seconds,
        )

    async def close(self) -> None:
        if self._ws is not None:
            await self._ws.close()
        if self._session is not None:
            await self._session.close()

    async def subscribe_market_data(self, product_ids: list[str], channels: list[str]) -> None:
        if self._ws is None:
            raise RuntimeError("WebSocket not connected")
        subscription = (list(product_ids), list(channels))
        if subscription not in self._market_subscriptions:
            self._market_subscriptions.append(subscription)
        for channel in channels:
            await self._ws.send_json({"type": "subscribe", "product_ids": product_ids, "channel": channel})

    async def subscribe_user(self, product_ids: list[str]) -> None:
        if self._ws is None:
            raise RuntimeError("WebSocket not connected")
        if not self._auth:
            raise RuntimeError("Authenticated WebSocket requires API credentials")
        if list(product_ids) not in self._user_subscriptions:
            self._user_subscriptions.append(list(product_ids))
        await self._ws.send_json(
            {
                "type": "subscribe",
                "product_ids": product_ids,
                "channel": "user",
                "jwt": self._auth.build_ws_jwt(),
            }
        )

    async def iter_messages(self) -> AsyncIterator[dict]:
        if self._ws is None:
            raise RuntimeError("WebSocket not connected")
        receive_timeout_seconds = max(self.settings.ws_heartbeat_timeout_seconds + 10, 30)
        while True:
            try:
                message = await self._ws.receive(timeout=receive_timeout_seconds)
                if message.type == aiohttp.WSMsgType.TEXT:
                    yield json.loads(message.data)
                elif message.type in {
                    aiohttp.WSMsgType.CLOSE,
                    aiohttp.WSMsgType.CLOSED,
                    aiohttp.WSMsgType.CLOSING,
                    aiohttp.WSMsgType.ERROR,
                }:
                    await self._reconnect_and_resubscribe()
                else:
                    continue
            except asyncio.TimeoutError:
                # Transport can stay open while upstream stops sending data.
                # Force reconnect so worker heartbeat does not stall indefinitely.
                await self._reconnect_and_resubscribe()
            except aiohttp.ClientError:
                await self._reconnect_and_resubscribe()

    async def _reconnect_and_resubscribe(self) -> None:
        await self.close()
        await asyncio.sleep(max(self.settings.ws_reconnect_delay_seconds, 1))
        await self.connect()
        for product_ids, channels in self._market_subscriptions:
            for channel in channels:
                await self._ws.send_json({"type": "subscribe", "product_ids": product_ids, "channel": channel})
        if self._auth:
            for product_ids in self._user_subscriptions:
                await self._ws.send_json(
                    {
                        "type": "subscribe",
                        "product_ids": product_ids,
                        "channel": "user",
                        "jwt": self._auth.build_ws_jwt(),
                    }
                )
