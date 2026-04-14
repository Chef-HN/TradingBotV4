from __future__ import annotations

from decimal import Decimal
from typing import Any

from domain.models import Fill, VenueOrder

from .mappers.fills import map_fill
from .mappers.orders import map_order
from .rest.client import BybitRESTClient
from .rest.models import BybitFillDTO, BybitOrderDTO


def _to_bybit_symbol(internal: str) -> str:
    """BTC-USD → BTCUSDT"""
    base, quote = internal.split("-", 1)
    return base + ("USDT" if quote == "USD" else quote)


class BybitAdapter:
    """
    Exchange adapter for Bybit V5 spot.

    Accepts the same Coinbase-format payload in create_order() and returns
    a normalized response so the worker does not need exchange-specific branches.
    """

    def __init__(self, rest_client: BybitRESTClient) -> None:
        self.rest_client = rest_client

    async def close(self) -> None:
        await self.rest_client.close()

    async def get_best_bid_ask(self, product_id: str) -> tuple[Decimal, Decimal]:
        symbol = _to_bybit_symbol(product_id)
        data = await self.rest_client.get(
            "/v5/market/tickers",
            params={"category": "spot", "symbol": symbol},
        )
        item = data["result"]["list"][0]
        return Decimal(item["bid1Price"]), Decimal(item["ask1Price"])

    async def create_order(self, payload: dict[str, Any]) -> dict[str, Any]:
        """
        Accepts Coinbase-format payload:
            {
                "client_order_id": str,
                "product_id": str,           # BTC-USD
                "side": "BUY" | "SELL",
                "order_configuration": {
                    "limit_limit_gtc": {
                        "base_size": str,
                        "limit_price": str,
                        "post_only": bool,
                    }
                }
            }

        Returns normalized response:
            {"success": True, "success_response": {"order_id": str, "client_order_id": str}}
        """
        cfg = payload.get("order_configuration", {}).get("limit_limit_gtc", {})
        symbol = _to_bybit_symbol(payload["product_id"])
        side = "Buy" if payload["side"] == "BUY" else "Sell"
        tif = "PostOnly" if cfg.get("post_only", True) else "GTC"

        try:
            resp = await self.rest_client.post(
                "/v5/order/create",
                auth=True,
                json_body={
                    "category": "spot",
                    "symbol": symbol,
                    "side": side,
                    "orderType": "Limit",
                    "qty": cfg["base_size"],
                    "price": cfg["limit_price"],
                    "timeInForce": tif,
                    "orderLinkId": payload["client_order_id"],
                },
            )
            return {
                "success": True,
                "success_response": {
                    "order_id": resp["result"]["orderId"],
                    "client_order_id": resp["result"]["orderLinkId"],
                },
            }
        except Exception as exc:
            return {"success": False, "error_response": str(exc)}

    async def cancel_orders(self, order_ids: list[str], product_id: str = "") -> dict[str, Any]:
        """Cancel orders by client_order_id. If product_id is provided, cancels individually.
        Without product_id falls back to cancel-all for spot (last resort)."""
        results = []
        for client_id in order_ids:
            try:
                if product_id:
                    resp = await self.cancel_order_by_id(client_id, product_id)
                else:
                    # Bybit requires symbol for individual cancel — fall back to cancel-all
                    resp = await self.rest_client.post(
                        "/v5/order/cancel-all",
                        auth=True,
                        json_body={"category": "spot"},
                    )
                results.append(resp)
                if not product_id:
                    break  # cancel-all already got everything
            except Exception as exc:
                results.append({"error": str(exc)})
        return {"results": results}

    async def cancel_order_by_id(self, client_order_id: str, product_id: str) -> dict[str, Any]:
        """Cancel a specific order by client_order_id with known symbol."""
        symbol = _to_bybit_symbol(product_id)
        try:
            return await self.rest_client.post(
                "/v5/order/cancel",
                auth=True,
                json_body={
                    "category": "spot",
                    "symbol": symbol,
                    "orderLinkId": client_order_id,
                },
            )
        except Exception as exc:
            return {"error": str(exc)}

    async def list_open_orders(self, product_id: str | None = None) -> list[VenueOrder]:
        params: dict[str, Any] = {"category": "spot"}
        if product_id:
            params["symbol"] = _to_bybit_symbol(product_id)
        data = await self.rest_client.get("/v5/order/realtime", auth=True, params=params)
        return [
            map_order(BybitOrderDTO.model_validate(item))
            for item in data.get("result", {}).get("list", [])
        ]

    async def list_fills(self, product_id: str | None = None) -> list[Fill]:
        params: dict[str, Any] = {"category": "spot"}
        if product_id:
            params["symbol"] = _to_bybit_symbol(product_id)
        data = await self.rest_client.get("/v5/execution/list", auth=True, params=params)
        return [
            map_fill(BybitFillDTO.model_validate(item))
            for item in data.get("result", {}).get("list", [])
        ]

    async def get_fee_tier(self) -> dict[str, Any]:
        """Returns normalized fee tier dict matching Coinbase format."""
        data = await self.rest_client.get(
            "/v5/account/fee-rate",
            auth=True,
            params={"category": "spot"},
        )
        items = data.get("result", {}).get("list", [])
        if items:
            item = items[0]
            maker = item.get("makerFeeRate", "0.001")
            taker = item.get("takerFeeRate", "0.001")
        else:
            maker, taker = "0.001", "0.001"
        return {
            "fee_tier": {
                "maker_fee_rate": maker,
                "taker_fee_rate": taker,
                "pricing_tier": "Bybit base",
            }
        }

    async def market_sell(self, product_id: str, base_size: Decimal) -> Decimal:
        """Place a market sell order. Returns filled quote amount (USD received)."""
        import asyncio
        from uuid import uuid4
        symbol = _to_bybit_symbol(product_id)
        link_id = str(uuid4()).replace("-", "")[:36]
        resp = await self.rest_client.post(
            "/v5/order/create",
            auth=True,
            json_body={
                "category": "spot",
                "symbol": symbol,
                "side": "Sell",
                "orderType": "Market",
                "qty": str(base_size),
                "orderLinkId": link_id,
            },
        )
        order_id = resp.get("result", {}).get("orderId")
        if not order_id:
            raise RuntimeError(f"market_sell: no orderId in response: {resp}")
        # Poll for fill
        for _ in range(10):
            await asyncio.sleep(1)
            fills_resp = await self.rest_client.get(
                "/v5/execution/list",
                auth=True,
                params={"category": "spot", "orderId": order_id},
            )
            fills = fills_resp.get("result", {}).get("list", [])
            if fills:
                return sum(Decimal(f["execPrice"]) * Decimal(f["execQty"]) for f in fills)
        raise RuntimeError(f"market_sell: no fills after 10s for order {order_id}")

    async def get_balances(self) -> dict[str, Decimal]:
        """Returns {currency: free_amount}."""
        data = await self.rest_client.get(
            "/v5/account/wallet-balance",
            auth=True,
            params={"accountType": "UNIFIED"},
        )
        result: dict[str, Decimal] = {}
        for account in data.get("result", {}).get("list", []):
            for coin in account.get("coin", []):
                currency = coin.get("coin", "")
                free = coin.get("availableToWithdraw") or coin.get("walletBalance", "0")
                if currency == "USDT":
                    result["USD"] = Decimal(str(free))
                else:
                    result[currency] = Decimal(str(free))
        return result
