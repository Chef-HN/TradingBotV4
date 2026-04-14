from __future__ import annotations

from auth_kit.mixins import OtpMixin

from .base import Base


class OtpRow(OtpMixin, Base):
    __tablename__ = "otp_codes"
