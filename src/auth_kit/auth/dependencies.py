from typing import Callable, Type

from fastapi import Request, Response
from fastapi.responses import RedirectResponse
from sqlalchemy.ext.asyncio import AsyncSession

from auth_kit.auth.tokens import decode_access_token
from auth_kit.repositories.user_repo import get_user_by_id


class AuthRequired(Exception):
    pass


def make_get_current_user(jwt_secret: str, user_model_class: Type) -> Callable:
    async def get_current_user(request: Request, db: AsyncSession):
        token = request.cookies.get("access_token")
        if not token:
            return None
        user_id = decode_access_token(token, jwt_secret)
        if not user_id:
            return None
        return await get_user_by_id(db, user_model_class, user_id)
    return get_current_user


def make_require_auth(jwt_secret: str, user_model_class: Type) -> Callable:
    async def require_auth(request: Request, db: AsyncSession):
        token = request.cookies.get("access_token")
        if not token:
            raise AuthRequired()
        user_id = decode_access_token(token, jwt_secret)
        if not user_id:
            raise AuthRequired()
        user = await get_user_by_id(db, user_model_class, user_id)
        if user is None:
            raise AuthRequired()
        return user
    return require_auth
