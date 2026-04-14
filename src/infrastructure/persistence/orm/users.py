from __future__ import annotations

from auth_kit.mixins import UserMixin

from .base import Base


class UserRow(UserMixin, Base):
    __tablename__ = "users"
