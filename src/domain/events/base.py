from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel

from domain.enums import EventType


class DomainEvent(BaseModel):
    correlation_id: str
    event_type: EventType
    product_id: str
    emitted_at: datetime
    producer: str
