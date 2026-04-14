from __future__ import annotations

from domain.models.market import MarketSnapshot

from .base import DomainEvent


class MarketTickReceived(DomainEvent):
    snapshot: MarketSnapshot


class OrderBookUpdated(DomainEvent):
    snapshot: MarketSnapshot
