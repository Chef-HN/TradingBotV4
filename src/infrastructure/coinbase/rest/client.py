from __future__ import annotations

from typing import Any

import httpx

from config import CoinbaseSettings

from .auth import CoinbaseJWTAuth, resolve_api_secret


class CoinbaseRESTClient:
    def __init__(self, settings: CoinbaseSettings) -> None:
        self.settings = settings
        resolved_secret = resolve_api_secret(settings.api_secret, settings.api_secret_file)
        self._auth = CoinbaseJWTAuth(settings.api_key, resolved_secret) if settings.api_key and resolved_secret else None
        self._client = httpx.AsyncClient(
            base_url=settings.rest_base_url,
            timeout=settings.rest_timeout_seconds,
            headers={"Content-Type": "application/json"},
        )

    async def close(self) -> None:
        await self._client.aclose()

    async def get(self, path: str, *, auth: bool = False, params: dict[str, Any] | None = None) -> dict[str, Any]:
        response = await self._client.get(path, headers=self._build_headers("GET", path, auth=auth), params=params)
        response.raise_for_status()
        return response.json()

    async def post(self, path: str, *, auth: bool = False, json_body: dict[str, Any] | None = None) -> dict[str, Any]:
        response = await self._client.post(path, headers=self._build_headers("POST", path, auth=auth), json=json_body)
        response.raise_for_status()
        return response.json()

    async def delete(self, path: str, *, auth: bool = False, json_body: dict[str, Any] | None = None) -> dict[str, Any]:
        response = await self._client.request("DELETE", path, headers=self._build_headers("DELETE", path, auth=auth), json=json_body)
        response.raise_for_status()
        return response.json()

    def _build_headers(self, method: str, path: str, *, auth: bool) -> dict[str, str]:
        headers = {"Content-Type": "application/json"}
        if auth and self._auth:
            headers["Authorization"] = f"Bearer {self._auth.build_rest_jwt(method, path)}"
        return headers
