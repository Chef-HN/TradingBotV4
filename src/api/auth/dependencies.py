from __future__ import annotations

from dataclasses import dataclass

from fastapi import HTTPException, Request, Response
from fastapi.responses import RedirectResponse

from auth_kit.auth.dependencies import AuthRequired
from auth_kit.auth.tokens import decode_access_token
from auth_kit.repositories.user_repo import get_user_by_id
from config import get_settings
from infrastructure.persistence.database import AsyncSessionFactory
from infrastructure.persistence.orm.users import UserRow
from infrastructure.persistence.repositories.tenant_repository import TenantRepository
from infrastructure.tenancy import DEFAULT_TENANT_ID


@dataclass(frozen=True)
class ApiPrincipal:
    tenant_id: str
    auth_mode: str
    user_id: str | None = None
    api_key_id: str | None = None


async def auth_exception_handler(request: Request, exc: AuthRequired) -> Response:
    return RedirectResponse(url="/auth/login", status_code=302)


def _set_request_principal(request: Request, principal: ApiPrincipal) -> None:
    request.state.tenant_id = principal.tenant_id
    request.state.api_principal = principal


async def get_current_user(request: Request) -> UserRow | None:
    token = request.cookies.get("access_token")
    if not token:
        return None
    settings = get_settings()
    user_id = decode_access_token(token, settings.auth.jwt_secret)
    if not user_id:
        return None
    async with AsyncSessionFactory() as db:
        return await get_user_by_id(db, UserRow, user_id)


async def require_auth(request: Request) -> UserRow:
    token = request.cookies.get("access_token")
    if not token:
        raise AuthRequired()
    settings = get_settings()
    user_id = decode_access_token(token, settings.auth.jwt_secret)
    if not user_id:
        raise AuthRequired()
    async with AsyncSessionFactory() as db:
        user = await get_user_by_id(db, UserRow, user_id)
    if user is None:
        raise AuthRequired()
    return user


async def _resolve_api_key_principal(request: Request) -> ApiPrincipal | None:
    auth_header = (request.headers.get("Authorization") or "").strip()
    if not auth_header:
        return None
    if not auth_header.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="Invalid Authorization scheme")
    raw_api_key = auth_header.split(" ", 1)[1].strip()
    if not raw_api_key:
        raise HTTPException(status_code=401, detail="Missing bearer token")

    async with AsyncSessionFactory() as db:
        repo = TenantRepository(db)
        resolved = await repo.resolve_api_key(raw_api_key)
    if resolved is None:
        raise HTTPException(status_code=401, detail="Invalid or expired API key")

    tenant, key_row = resolved
    principal = ApiPrincipal(
        tenant_id=tenant.id,
        auth_mode="api_key",
        user_id=None,
        api_key_id=key_row.id,
    )
    _set_request_principal(request, principal)
    return principal


async def require_api_auth(request: Request) -> ApiPrincipal:
    api_principal = await _resolve_api_key_principal(request)
    if api_principal is not None:
        return api_principal

    user = await get_current_user(request)
    if user is None:
        raise AuthRequired()
    tenant_id = getattr(user, "tenant_id", DEFAULT_TENANT_ID) or DEFAULT_TENANT_ID
    principal = ApiPrincipal(
        tenant_id=tenant_id,
        auth_mode="cookie",
        user_id=user.id,
        api_key_id=None,
    )
    _set_request_principal(request, principal)
    return principal


def get_request_tenant_id(request: Request) -> str:
    tenant_id = getattr(request.state, "tenant_id", None)
    return tenant_id or DEFAULT_TENANT_ID
