from __future__ import annotations

DEFAULT_TENANT_ID = "00000000-0000-0000-0000-000000000001"
DEFAULT_TENANT_SLUG = "default"
DEFAULT_TENANT_NAME = "Default Tenant"


def normalize_tenant_id(value: str | None) -> str:
    candidate = (value or "").strip()
    return candidate or DEFAULT_TENANT_ID
