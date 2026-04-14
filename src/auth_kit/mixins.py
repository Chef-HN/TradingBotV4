"""
Column mixins for auth models.
Apps inherit from these to define concrete ORM models using their own Base.

Example:
    from auth_kit.mixins import UserMixin, OtpMixin
    from myapp.models.base import Base

    class UserRow(UserMixin, Base):
        __tablename__ = "users"

    class OtpRow(OtpMixin, Base):
        __tablename__ = "otp_codes"
"""
import uuid
from datetime import datetime

from sqlalchemy import String, DateTime, Boolean
from sqlalchemy.orm import mapped_column, Mapped


class UserMixin:
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    email: Mapped[str] = mapped_column(String(255), unique=True, nullable=False, index=True)
    password_hash: Mapped[str] = mapped_column(String(255), nullable=False)
    display_name: Mapped[str] = mapped_column(String(100), nullable=False, default="")
    preferred_locale: Mapped[str] = mapped_column(String(5), nullable=False, default="es")
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


class OtpMixin:
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    email: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    code: Mapped[str] = mapped_column(String(6), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    expires_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    used: Mapped[bool] = mapped_column(Boolean, default=False)
