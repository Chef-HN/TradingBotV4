from __future__ import annotations

from pydantic import BaseModel, ConfigDict


class BybitOrderDTO(BaseModel):
    model_config = ConfigDict(extra="ignore")

    orderId: str | None = None
    orderLinkId: str | None = None
    symbol: str
    side: str
    orderStatus: str | None = None
    orderType: str | None = None
    timeInForce: str | None = None
    price: str | None = None
    qty: str | None = None
    cumExecQty: str | None = None
    cumExecValue: str | None = None
    cumExecFee: str | None = None


class BybitFillDTO(BaseModel):
    model_config = ConfigDict(extra="ignore")

    execId: str | None = None
    orderId: str | None = None
    orderLinkId: str | None = None
    symbol: str
    side: str
    execPrice: str
    execQty: str
    execValue: str | None = None
    execFee: str | None = "0"
    execTime: str | None = None
    isMaker: bool | None = None
