from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import jwt


def resolve_api_secret(api_secret: str, api_secret_file: str | None = None) -> str:
    if api_secret_file:
        secret_path = Path(api_secret_file).expanduser()
        if secret_path.exists():
            return secret_path.read_text(encoding="utf-8").strip()
    normalized = api_secret.strip()
    if "\\n" in normalized:
        normalized = normalized.replace("\\n", "\n")
    return normalized


class CoinbaseJWTAuth:
    def __init__(self, api_key: str, api_secret: str) -> None:
        self.api_key = api_key
        self.api_secret = api_secret

    def build_rest_jwt(self, method: str, path: str) -> str:
        now = datetime.now(UTC)
        payload = {
            "sub": self.api_key,
            "iss": "cdp",
            "nbf": int(now.timestamp()),
            "exp": int((now + timedelta(minutes=2)).timestamp()),
            "uri": f"{method.upper()} api.coinbase.com{path}",
        }
        headers = {"kid": self.api_key, "nonce": f"{int(now.timestamp() * 1000)}"}
        return jwt.encode(payload, self.api_secret, algorithm="ES256", headers=headers)

    def build_ws_jwt(self) -> str:
        now = datetime.now(UTC)
        payload = {
            "sub": self.api_key,
            "iss": "cdp",
            "nbf": int(now.timestamp()),
            "exp": int((now + timedelta(minutes=2)).timestamp()),
        }
        headers = {"kid": self.api_key, "nonce": f"{int(now.timestamp() * 1000)}"}
        return jwt.encode(payload, self.api_secret, algorithm="ES256", headers=headers)
