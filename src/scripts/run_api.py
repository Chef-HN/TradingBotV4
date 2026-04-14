"""
TradingBotV3 — API Process
Runs FastAPI only; reads state from Redis, writes commands to Redis.

Run:
    python -m scripts.run_api
"""
from __future__ import annotations

import asyncio

import uvicorn

from api.app import app
from api.routes import dashboard as dashboard_module
from config import get_settings
from infrastructure.state_store import StateStore

settings = get_settings()


async def main() -> None:
    store = StateStore(settings.redis.url, exchange=settings.exchange.name.lower())
    dashboard_module.set_state_store(store)
    config = uvicorn.Config(
        app,
        host=settings.app.host,
        port=settings.app.port,
        log_level="warning",
    )
    server = uvicorn.Server(config)
    try:
        await server.serve()
    finally:
        await store.close()


if __name__ == "__main__":
    asyncio.run(main())
