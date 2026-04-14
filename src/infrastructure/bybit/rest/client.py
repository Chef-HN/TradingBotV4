from __future__ import annotations

import json
from typing import Any
from urllib.parse import urlencode

import httpx

from config import BybitSettings

from .auth import BybitHMACAuth

_RECV_WINDOW = "5000"


class BybitRESTClient:
    def __init__(self, settings: BybitSettings) -> None:
        self.settings = settings
        self._auth = (
            BybitHMACAuth(settings.api_key, settings.api_secret)
            if settings.api_key and settings.api_secret
            else None
        )
        self._client = httpx.AsyncClient(
            base_url=settings.rest_base_url,
            timeout=settings.rest_timeout_seconds,
        )

    async def close(self) -> None:
        await self._client.aclose()

    async def get(
        self,
        path: str,
        *,
        auth: bool = False,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        query_str = urlencode(params or {})
        headers = (
            self._auth.build_headers(_RECV_WINDOW, query_str)
            if auth and self._auth
            else {"Content-Type": "application/json"}
        )
        response = await self._client.get(path, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()
        if data.get("retCode", 0) != 0:
            raise ValueError(f"Bybit API error {data['retCode']}: {data.get('retMsg')}")
        return data

    async def post(
        self,
        path: str,
        *,
        auth: bool = False,
        json_body: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        body_str = json.dumps(json_body or {})
        headers = (
            self._auth.build_headers(_RECV_WINDOW, body_str)
            if auth and self._auth
            else {"Content-Type": "application/json"}
        )
        response = await self._client.post(path, headers=headers, content=body_str)
        response.raise_for_status()
        data = response.json()
        if data.get("retCode", 0) != 0:
            raise ValueError(f"Bybit API error {data['retCode']}: {data.get('retMsg')}")
        return data
