from __future__ import annotations

from sqlalchemy import Uuid
from sqlalchemy.orm import Mapped, mapped_column

from auth_kit.mixins import OtpMixin
from infrastructure.tenancy import DEFAULT_TENANT_ID

from .base import Base


class OtpRow(OtpMixin, Base):
    __tablename__ = "otp_codes"

    tenant_id: Mapped[str] = mapped_column(
        Uuid(as_uuid=False),
        nullable=False,
        index=True,
        default=DEFAULT_TENANT_ID,
    )
