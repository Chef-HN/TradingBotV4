from __future__ import annotations

from sqlalchemy import BigInteger, DateTime, Index, String, Uuid
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base


class BotRestartRow(Base):
    __tablename__ = "bot_restarts"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    session_id: Mapped[str] = mapped_column(Uuid(as_uuid=False), nullable=False)
    product_id: Mapped[str] = mapped_column(String(20), nullable=False)
    triggered_by: Mapped[str] = mapped_column(String(50), nullable=False)
    restarted_at: Mapped[object] = mapped_column(DateTime(timezone=True), nullable=False)

    __table_args__ = (
        Index("idx_bot_restarts_product_time", "product_id", "restarted_at"),
    )
