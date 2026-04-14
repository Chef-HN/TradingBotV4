from __future__ import annotations

import hashlib
import hmac
import time


class BybitHMACAuth:
    """Signs Bybit V5 REST requests with HMAC-SHA256."""

    def __init__(self, api_key: str, api_secret: str) -> None:
        self.api_key = api_key
        self._api_secret = api_secret

    def build_headers(self, recv_window: str, params_or_body: str) -> dict[str, str]:
        timestamp = str(int(time.time() * 1000))
        param_str = timestamp + self.api_key + recv_window + params_or_body
        sign = hmac.new(
            self._api_secret.encode("utf-8"),
            param_str.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        return {
            "X-BAPI-API-KEY": self.api_key,
            "X-BAPI-TIMESTAMP": timestamp,
            "X-BAPI-SIGN": sign,
            "X-BAPI-RECV-WINDOW": recv_window,
            "Content-Type": "application/json",
        }
