"""
Factory that creates a fully-configured FastAPI auth router.

Apps call create_auth_router() with their model classes, DB dependency,
settings, and Jinja2Templates instance — and get back a ready-to-mount
APIRouter that handles the full auth flow (register, OTP, login, logout,
forgot/reset password).

App-specific routes (e.g. account deletion) are added directly to the
returned router or to a separate app router.
"""

from urllib.parse import urlencode, quote_plus

from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import RedirectResponse
from fastapi.templating import Jinja2Templates

from auth_kit.auth.passwords import verify_password
from auth_kit.auth.tokens import create_access_token
from auth_kit.repositories.user_repo import create_user, get_user_by_email, update_password
from auth_kit.services.otp_service import create_otp, verify_otp, send_otp_email


def create_auth_router(
    *,
    user_model,
    otp_model,
    get_db,
    settings,
    templates: Jinja2Templates,
    prefix: str = "/auth",
    # Template paths (relative to the app's templates dir)
    login_template: str = "pages/login.html",
    register_template: str = "pages/register.html",
    verify_otp_template: str = "pages/verify_otp.html",
    forgot_password_template: str = "pages/forgot_password.html",
    reset_password_template: str = "pages/reset_password.html",
    # Redirect targets
    after_login_redirect: str = "/dashboard",
    after_reset_redirect: str = "/auth/login?reset=1",
) -> APIRouter:
    """Return a configured APIRouter with all auth endpoints."""

    router = APIRouter(prefix=prefix)

    def _ctx(request: Request, **kwargs) -> dict:
        return {
            "t": getattr(request.state, "t", lambda k: k),
            "locale": getattr(request.state, "locale", "es"),
            "user": None,
            **kwargs,
        }

    def _smtp() -> dict:
        return dict(
            smtp_host=settings.smtp_host,
            smtp_port=settings.smtp_port,
            smtp_tls=settings.smtp_tls,
            smtp_user=settings.smtp_user,
            smtp_password=settings.smtp_password,
            smtp_from=settings.smtp_from,
            app_name=settings.app_name,
        )

    # -------------------------------------------------------------------------
    # Login
    # -------------------------------------------------------------------------

    @router.get("/login")
    async def login_page(request: Request):
        return templates.TemplateResponse(request, login_template, _ctx(request))

    @router.post("/login")
    async def login(
        request: Request,
        email: str = Form(...),
        password: str = Form(...),
        db=Depends(get_db),
    ):
        t = getattr(request.state, "t", lambda k: k)
        user = await get_user_by_email(db, user_model, email)
        if not user or not verify_password(password, user.password_hash):
            return templates.TemplateResponse(
                request, login_template,
                _ctx(request, error=t("auth.error_invalid_credentials")),
                status_code=401,
            )
        token = create_access_token(user.id, settings.jwt_secret, settings.jwt_expiry_hours)
        response = RedirectResponse(after_login_redirect, status_code=303)
        response.set_cookie("access_token", token, httponly=True, samesite="lax", max_age=86400)
        response.set_cookie("locale", user.preferred_locale, max_age=86400 * 365, samesite="lax")
        return response

    # -------------------------------------------------------------------------
    # Register (step 1: validate + send OTP)
    # -------------------------------------------------------------------------

    @router.get("/register")
    async def register_page(request: Request):
        return templates.TemplateResponse(request, register_template, _ctx(request))

    @router.post("/register")
    async def register(
        request: Request,
        display_name: str = Form(...),
        email: str = Form(...),
        password: str = Form(...),
        confirm_password: str = Form(default=""),
        preferred_locale: str = Form(default="es"),
        db=Depends(get_db),
    ):
        t = getattr(request.state, "t", lambda k: k)
        email = email.strip().lower()
        if confirm_password and password != confirm_password:
            return templates.TemplateResponse(
                request, register_template,
                _ctx(request, error=t("auth.error_password_mismatch")),
                status_code=400,
            )
        if await get_user_by_email(db, user_model, email):
            return templates.TemplateResponse(
                request, register_template,
                _ctx(request, error=t("auth.error_email_exists")),
                status_code=400,
            )
        code = await create_otp(db, otp_model, email)
        send_otp_email(email, code, locale=preferred_locale, **_smtp())
        params = urlencode({"email": email, "display_name": display_name, "preferred_locale": preferred_locale})
        return RedirectResponse(
            f"{prefix}/verify-otp?{params}&password={quote_plus(password)}",
            status_code=303,
        )

    # -------------------------------------------------------------------------
    # Verify OTP (step 2: enter code, create user)
    # -------------------------------------------------------------------------

    @router.get("/verify-otp")
    async def verify_otp_page(
        request: Request,
        email: str = "",
        display_name: str = "",
        preferred_locale: str = "es",
        password: str = "",
        error: str = "",
        message: str = "",
    ):
        t = getattr(request.state, "t", lambda k: k)
        return templates.TemplateResponse(
            request, verify_otp_template,
            _ctx(
                request,
                email=email,
                display_name=display_name,
                preferred_locale=preferred_locale,
                password=password,
                error=error or None,
                message=t("auth.otp_sent") if message == "otp_sent" else None,
            ),
        )

    @router.post("/verify-otp")
    async def verify_otp_submit(
        request: Request,
        email: str = Form(...),
        display_name: str = Form(...),
        preferred_locale: str = Form(default="es"),
        password: str = Form(...),
        code: str = Form(...),
        db=Depends(get_db),
    ):
        t = getattr(request.state, "t", lambda k: k)
        if not await verify_otp(db, otp_model, email, code):
            return templates.TemplateResponse(
                request, verify_otp_template,
                _ctx(
                    request,
                    email=email,
                    display_name=display_name,
                    preferred_locale=preferred_locale,
                    password=password,
                    error=t("auth.otp_invalid"),
                ),
                status_code=400,
            )
        user = await create_user(db, user_model, email, password, display_name, preferred_locale)
        token = create_access_token(user.id, settings.jwt_secret, settings.jwt_expiry_hours)
        response = RedirectResponse(after_login_redirect, status_code=303)
        response.set_cookie("access_token", token, httponly=True, samesite="lax", max_age=86400)
        response.set_cookie("locale", preferred_locale, max_age=86400 * 365, samesite="lax")
        return response

    @router.post("/resend-otp")
    async def resend_otp(
        request: Request,
        email: str = Form(...),
        display_name: str = Form(default=""),
        preferred_locale: str = Form(default="es"),
        password: str = Form(default=""),
        db=Depends(get_db),
    ):
        code = await create_otp(db, otp_model, email)
        send_otp_email(email, code, locale=preferred_locale, **_smtp())
        params = urlencode({
            "email": email,
            "display_name": display_name,
            "preferred_locale": preferred_locale,
            "message": "otp_sent",
        })
        return RedirectResponse(
            f"{prefix}/verify-otp?{params}&password={quote_plus(password)}",
            status_code=303,
        )

    # -------------------------------------------------------------------------
    # Logout
    # -------------------------------------------------------------------------

    @router.post("/logout")
    async def logout():
        response = RedirectResponse("/", status_code=303)
        response.delete_cookie("access_token")
        return response

    # -------------------------------------------------------------------------
    # Forgot password
    # -------------------------------------------------------------------------

    @router.get("/forgot-password")
    async def forgot_password_page(request: Request):
        return templates.TemplateResponse(request, forgot_password_template, _ctx(request))

    @router.post("/forgot-password")
    async def forgot_password_submit(request: Request, db=Depends(get_db)):
        t = getattr(request.state, "t", lambda k: k)
        form = await request.form()
        email = str(form.get("email", "")).strip().lower()
        locale = getattr(request.state, "locale", "es")

        user = await get_user_by_email(db, user_model, email)
        if not user:
            return templates.TemplateResponse(
                request, forgot_password_template,
                _ctx(request, error=t("auth.error_email_not_found"), email=email),
                status_code=400,
            )
        code = await create_otp(db, otp_model, email)
        send_otp_email(email, code, locale=locale, **_smtp())
        return templates.TemplateResponse(
            request, reset_password_template,
            _ctx(request, email=email),
        )

    # -------------------------------------------------------------------------
    # Reset password
    # -------------------------------------------------------------------------

    @router.post("/reset-password")
    async def reset_password_submit(request: Request, db=Depends(get_db)):
        t = getattr(request.state, "t", lambda k: k)
        form = await request.form()
        email = str(form.get("email", "")).strip().lower()
        code = str(form.get("code", "")).strip()
        new_password = str(form.get("new_password", ""))
        confirm_password = str(form.get("confirm_password", ""))

        if new_password != confirm_password:
            return templates.TemplateResponse(
                request, reset_password_template,
                _ctx(request, email=email, error=t("auth.passwords_mismatch")),
            )
        if not await verify_otp(db, otp_model, email, code):
            return templates.TemplateResponse(
                request, reset_password_template,
                _ctx(request, email=email, error=t("auth.otp_invalid")),
            )
        user = await get_user_by_email(db, user_model, email)
        if user:
            await update_password(db, user, new_password)
        return RedirectResponse(url=after_reset_redirect, status_code=302)

    return router
