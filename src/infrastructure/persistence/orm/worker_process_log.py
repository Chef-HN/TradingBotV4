from __future__ import annotations

from sqlalchemy import BigInteger, DateTime, Integer, String, Uuid
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base


class WorkerProcessLogRow(Base):
    __tablename__ = "worker_process_log"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    tenant_id: Mapped[str] = mapped_column(Uuid(as_uuid=False), nullable=False, index=True)
    exchange: Mapped[str] = mapped_column(String(20), nullable=False, index=True)
    started_at: Mapped[object] = mapped_column(DateTime(timezone=True), nullable=False)
    pid: Mapped[int | None] = mapped_column(Integer, nullable=True)
    stopped_at: Mapped[object | None] = mapped_column(DateTime(timezone=True), nullable=True)
    stop_reason: Mapped[str | None] = mapped_column(String(50), nullable=True)
