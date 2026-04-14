from __future__ import annotations

import os

from fastapi import Depends, FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from auth_kit.auth.dependencies import AuthRequired
from api.auth.dependencies import auth_exception_handler, require_auth
from api.i18n.middleware import LocaleMiddleware
from .routes.auth_routes import router as auth_router
from .routes.credentials import router as credentials_router
from .routes.dashboard import router as dashboard_router
from .routes.health import router as health_router

_STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")

app = FastAPI(
    title="TradingBotV3",
    description="Neutral Grid Market-Making Bot — Dashboard API",
    version="0.1.0",
)

app.add_middleware(LocaleMiddleware)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST", "PUT", "DELETE"],
    allow_headers=["*"],
)

app.add_exception_handler(AuthRequired, auth_exception_handler)

app.include_router(health_router, tags=["health"])
app.include_router(auth_router, tags=["auth"])
app.include_router(
    credentials_router,
    prefix="/api",
    tags=["credentials"],
    dependencies=[Depends(require_auth)],
)
app.include_router(
    dashboard_router,
    prefix="/api",
    tags=["dashboard"],
    dependencies=[Depends(require_auth)],
)

app.mount("/static", StaticFiles(directory=_STATIC_DIR), name="static")


@app.get("/", include_in_schema=False)
async def dashboard_ui(_user=Depends(require_auth)) -> FileResponse:
    return FileResponse(os.path.join(_STATIC_DIR, "index.html"))
