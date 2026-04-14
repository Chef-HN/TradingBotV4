from __future__ import annotations

from fastapi import Request, Response
from fastapi.responses import RedirectResponse

from auth_kit.auth.dependencies import AuthRequired
from auth_kit.auth.tokens import decode_access_token
from auth_kit.repositories.user_repo import get_user_by_id
from config import get_settings
from infrastructure.persistence.database import AsyncSessionFactory
from infrastructure.persistence.orm.users import UserRow


async def auth_exception_handler(request: Request, exc: AuthRequired) -> Response:
    return RedirectResponse(url="/auth/login", status_code=302)


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
