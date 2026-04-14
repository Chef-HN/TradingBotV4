from __future__ import annotations

from typing import Callable

from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import Response

from auth_kit.i18n import AUTH_TRANSLATIONS

_SUPPORTED = frozenset(AUTH_TRANSLATIONS.keys())
_DEFAULT = "en"


def _make_t(locale: str) -> Callable[[str], str]:
    strings = AUTH_TRANSLATIONS.get(locale, AUTH_TRANSLATIONS[_DEFAULT])
    fallback = AUTH_TRANSLATIONS[_DEFAULT]

    def t(key: str) -> str:
        return strings.get(key) or fallback.get(key) or key

    return t


class LocaleMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        lang = request.query_params.get("lang")
        if lang and lang in _SUPPORTED:
            locale = lang
        else:
            cookie = request.cookies.get("locale")
            locale = cookie if cookie in _SUPPORTED else _DEFAULT

        request.state.locale = locale
        request.state.t = _make_t(locale)
        return await call_next(request)
