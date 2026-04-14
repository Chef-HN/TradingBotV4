from __future__ import annotations

from pathlib import Path

from fastapi import Depends, Request
from fastapi.responses import RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.ext.asyncio import AsyncSession

from auth_kit.repositories.user_repo import delete_user
from auth_kit.router import create_auth_router
from api.auth.dependencies import require_auth
from config import get_settings
from infrastructure.persistence.database import AsyncSessionFactory
from infrastructure.persistence.orm.otp_codes import OtpRow
from infrastructure.persistence.orm.users import UserRow

_TEMPLATES_DIR = Path(__file__).resolve().parents[1] / "templates"


async def _get_db() -> AsyncSession:  # type: ignore[override]
    async with AsyncSessionFactory() as session:
        yield session


templates = Jinja2Templates(directory=str(_TEMPLATES_DIR))

router = create_auth_router(
    user_model=UserRow,
    otp_model=OtpRow,
    get_db=_get_db,
    settings=get_settings().auth,
    templates=templates,
    after_login_redirect="/",
)


@router.get("/account")
async def account_page(request: Request, user=Depends(require_auth)):
    return templates.TemplateResponse(
        request,
        "pages/account.html",
        {"t": request.state.t, "locale": request.state.locale, "user": user},
    )


@router.post("/account/delete")
async def delete_account(
    request: Request,
    db: AsyncSession = Depends(_get_db),
    user=Depends(require_auth),
):
    await delete_user(db, user)
    await db.commit()
    response = RedirectResponse(url="/auth/login", status_code=302)
    response.delete_cookie("access_token")
    return response
