from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import httpx

from domain.models import Fill, VenueOrder

from .mappers.fills import map_fill
from .mappers.orders import map_order
from .rest.client import CoinbaseRESTClient
from .rest.models import CoinbaseFillDTO, CoinbaseOrderDTO


class CoinbaseAdapter:
    def __init__(self, rest_client: CoinbaseRESTClient) -> None:
        self.rest_client = rest_client

    async def close(self) -> None:
        await self.rest_client.close()

    async def get_best_bid_ask(self, product_id: str) -> tuple[Decimal, Decimal]:
        try:
            payload = await self.rest_client.get(
                "/api/v3/brokerage/best_bid_ask", auth=True, params={"product_ids": product_id}
            )
        except httpx.HTTPStatusError:
            payload = await self.rest_client.get(
                f"/api/v3/brokerage/market/products/{product_id}/ticker"
            )
            bid = Decimal(str(payload.get("best_bid", payload.get("price", "0"))))
            ask = Decimal(str(payload.get("best_ask", payload.get("price", "0"))))
            return bid, ask
        books = payload.get("pricebooks", [])
        if not books:
            raise ValueError(f"No best bid/ask for {product_id}")
        book = books[0]
        bid = Decimal(str(book["bids"][0]["price"]))
        ask = Decimal(str(book["asks"][0]["price"]))
        return bid, ask

    async def create_order(self, payload: dict[str, Any]) -> dict[str, Any]:
        return await self.rest_client.post(
            "/api/v3/brokerage/orders", auth=True, json_body=payload
        )

    async def cancel_orders(self, order_ids: list[str]) -> dict[str, Any]:
        return await self.rest_client.post(
            "/api/v3/brokerage/orders/batch_cancel",
            auth=True,
            json_body={"order_ids": order_ids},
        )

    async def list_open_orders(self, product_id: str | None = None) -> list[VenueOrder]:
        params: dict[str, Any] = {"order_status": "OPEN"}
        if product_id:
            params["product_id"] = product_id
        payload = await self.rest_client.get(
            "/api/v3/brokerage/orders/historical/batch", auth=True, params=params
        )
        return [map_order(CoinbaseOrderDTO.model_validate(item)) for item in payload.get("orders", [])]

    async def list_fills(self, product_id: str | None = None) -> list[Fill]:
        params = {"product_id": product_id} if product_id else None
        payload = await self.rest_client.get(
            "/api/v3/brokerage/orders/historical/fills", auth=True, params=params
        )
        return [map_fill(CoinbaseFillDTO.model_validate(item)) for item in payload.get("fills", [])]

    async def get_fee_tier(self) -> dict[str, Any]:
        return await self.rest_client.get(
            "/api/v3/brokerage/transaction_summary", auth=True
        )

    async def market_sell(self, product_id: str, base_size: Decimal) -> Decimal:
        """Place a market sell order. Returns filled quote amount (USD received)."""
        from uuid import uuid4
        payload = {
            "client_order_id": str(uuid4()),
            "product_id": product_id,
            "side": "SELL",
            "order_configuration": {
                "market_market_ioc": {
                    "base_size": str(base_size),
                }
            },
        }
        resp = await self.rest_client.post("/api/v3/brokerage/orders", auth=True, json_body=payload)
        if not resp.get("success"):
            raise RuntimeError(f"market_sell failed: {resp}")
        # Poll for fill to get exact quote received
        import asyncio
        order_id = resp["success_response"]["order_id"]
        for _ in range(10):
            await asyncio.sleep(1)
            fills_resp = await self.rest_client.get(
                "/api/v3/brokerage/orders/historical/fills",
                auth=True,
                params={"order_id": order_id},
            )
            fills = fills_resp.get("fills", [])
            if fills:
                return sum(Decimal(f["price"]) * Decimal(f["size"]) for f in fills)
        raise RuntimeError(f"market_sell: no fills after 10s for order {order_id}")

    async def get_balances(self) -> dict[str, Decimal]:
        """Returns {currency: free_amount} for all accounts."""
        payload = await self.rest_client.get("/api/v3/brokerage/accounts", auth=True)
        result: dict[str, Decimal] = {}
        for item in payload.get("accounts", []):
            currency = item.get("currency", "")
            available = item.get("available_balance", {}).get("value", "0")
            result[currency] = Decimal(str(available))
        return result
