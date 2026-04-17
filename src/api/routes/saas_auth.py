from __future__ import annotations

from fastapi import APIRouter, Depends
from pydantic import BaseModel, Field

from api.auth.dependencies import ApiPrincipal, require_api_auth
from infrastructure.persistence.database import AsyncSessionFactory
from infrastructure.persistence.repositories.tenant_repository import TenantRepository

router = APIRouter()


class SignupRequest(BaseModel):
    tenant_name: str = Field(min_length=1, max_length=120)
    tier: str = "entry"
    max_capital: float = 100.0
    max_pairs: int = 1
    max_exchanges: int = 1
    created_by: str = "system"
    api_key_label: str | None = None
    api_key_ttl_days: int | None = None


@router.post("/auth/signup")
async def signup(body: SignupRequest) -> dict:
    async with AsyncSessionFactory() as db:
        repo = TenantRepository(db)
        tenant, api_key = await repo.create_tenant_with_api_key(
            name=body.tenant_name,
            tier=body.tier,
            max_capital=body.max_capital,
            max_pairs=body.max_pairs,
            max_exchanges=body.max_exchanges,
            created_by=body.created_by,
            api_key_label=body.api_key_label,
            api_key_ttl_days=body.api_key_ttl_days,
        )
    return {
        "tenant": {
            "tenant_id": tenant.id,
            "name": tenant.name,
            "slug": tenant.slug,
            "tier": tenant.tier,
            "max_capital": str(tenant.max_capital),
            "max_pairs": tenant.max_pairs,
            "max_exchanges": tenant.max_exchanges,
        },
        "api_key": api_key,
        "message": "Store this api_key now. It will not be shown again.",
    }


class RotateApiKeyRequest(BaseModel):
    label: str | None = None
    ttl_days: int | None = None
    rotated_by: str = "system"


@router.post("/auth/api-key/rotate")
async def rotate_api_key(
    body: RotateApiKeyRequest,
    principal: ApiPrincipal = Depends(require_api_auth),
) -> dict:
    async with AsyncSessionFactory() as db:
        repo = TenantRepository(db)
        new_api_key = await repo.rotate_api_key(
            tenant_id=principal.tenant_id,
            rotated_by=body.rotated_by,
            label=body.label,
            ttl_days=body.ttl_days,
        )
    return {
        "tenant_id": principal.tenant_id,
        "api_key": new_api_key,
        "message": "Store this api_key now. Previous active keys were revoked.",
    }
