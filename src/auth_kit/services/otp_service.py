import logging
import random
import smtplib
import string
from datetime import datetime, timedelta, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Type, TypeVar

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)
T = TypeVar("T")


def _generate_code() -> str:
    return "".join(random.choices(string.digits, k=6))


async def create_otp(db: AsyncSession, otp_model_class: Type[T], email: str) -> str:
    await db.execute(delete(otp_model_class).where(otp_model_class.email == email))
    code = _generate_code()
    expires_at = datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(minutes=10)
    otp = otp_model_class(email=email, code=code, expires_at=expires_at)
    db.add(otp)
    await db.commit()
    return code


async def verify_otp(db: AsyncSession, otp_model_class: Type[T], email: str, code: str) -> bool:
    now = datetime.utcnow()
    result = await db.execute(
        select(otp_model_class)
        .where(otp_model_class.email == email)
        .where(otp_model_class.code == code)
        .where(otp_model_class.used == False)  # noqa: E712
        .where(otp_model_class.expires_at > now)
        .order_by(otp_model_class.created_at.desc())
        .limit(1)
    )
    otp = result.scalar_one_or_none()
    if otp is None:
        return False
    otp.used = True
    await db.commit()
    return True


def send_otp_email(
    email: str,
    code: str,
    *,
    locale: str = "es",
    smtp_host: str = "",
    smtp_port: int = 587,
    smtp_tls: bool = True,
    smtp_user: str = "",
    smtp_password: str = "",
    smtp_from: str = "noreply@localhost",
    app_name: str = "App",
) -> None:
    if locale == "es":
        subject = f"Tu código de verificación — {app_name}"
        body = (
            f"Tu código de verificación es:\n\n    {code}\n\n"
            "Este código expira en 10 minutos.\n\n"
            "Si no solicitaste este código, ignora este mensaje."
        )
    else:
        subject = f"Your verification code — {app_name}"
        body = (
            f"Your verification code is:\n\n    {code}\n\n"
            "This code expires in 10 minutes.\n\n"
            "If you did not request this code, please ignore this message."
        )

    print(f"\n=== OTP EMAIL ===\nTo: {email}\nSubject: {subject}\nCode: {code}\n=================\n", flush=True)

    if not smtp_host:
        return

    try:
        msg = MIMEMultipart()
        msg["From"] = smtp_from
        msg["To"] = email
        msg["Subject"] = subject
        msg.attach(MIMEText(body, "plain", "utf-8"))
        with smtplib.SMTP(smtp_host, smtp_port) as server:
            if smtp_tls:
                server.starttls()
            if smtp_user and smtp_password:
                server.login(smtp_user, smtp_password)
            server.sendmail(smtp_from, email, msg.as_string())
    except Exception as exc:
        logger.error("Failed to send OTP email to %s: %s", email, exc)
