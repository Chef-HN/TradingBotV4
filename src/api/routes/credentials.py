"""API routes for managing encrypted exchange credentials."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from api.auth.dependencies import ApiPrincipal, require_api_auth
from infrastructure.persistence.database import AsyncSessionFactory
from infrastructure.persistence.repositories.credentials_repository import CredentialsRepository

router = APIRouter()


class SaveCredentialsRequest(BaseModel):
    """Request body for saving exchange credentials."""

    exchange_name: str  # "bybit" or "coinbase"
    api_key: str
    api_secret: str
    api_passphrase: str | None = None
    created_by: str = "user"


class CredentialsResponse(BaseModel):
    """Response body for credentials (never shows plaintext secrets)."""

    exchange_name: str
    api_key_last_4: str
    api_secret_last_4: str
    created_at: str
    updated_at: str
    active: bool


@router.post("/credentials")
async def save_credentials(
    body: SaveCredentialsRequest,
    principal: ApiPrincipal = Depends(require_api_auth),
) -> dict:
    """
    Save or update encrypted exchange credentials.

    The API key and secret are encrypted before storage and never stored in plaintext.
    The API key will be loaded automatically by the worker from the database.

    WARNING: Once saved, credentials cannot be retrieved for security reasons.
    """
    exchange_name = body.exchange_name.lower()

    if exchange_name not in ["bybit", "coinbase"]:
        raise HTTPException(status_code=400, detail=f"Unsupported exchange: {exchange_name}")

    if not body.api_key or not body.api_secret:
        raise HTTPException(status_code=400, detail="api_key and api_secret are required")

    try:
        async with AsyncSessionFactory() as db:
            repo = CredentialsRepository(db)
            creds = await repo.save_credentials(
                tenant_id=principal.tenant_id,
                exchange_name=exchange_name,
                api_key=body.api_key,
                api_secret=body.api_secret,
                api_passphrase=body.api_passphrase,
                created_by=body.created_by,
            )
            await db.commit()

        # Return last 4 chars only (for verification, not for use)
        return {
            "status": "saved",
            "exchange_name": creds.exchange_name,
            "api_key_last_4": creds.api_key[-4:] if len(creds.api_key) >= 4 else "****",
            "api_secret_last_4": creds.api_secret[-4:] if len(creds.api_secret) >= 4 else "****",
            "note": "Credentials encrypted and stored. Worker will load automatically on next restart.",
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to save credentials: {str(e)}")


@router.get("/credentials/{exchange_name}")
async def get_credentials_status(
    exchange_name: str,
    principal: ApiPrincipal = Depends(require_api_auth),
) -> dict:
    """
    Check if credentials exist for an exchange (returns status only, no plaintext secrets).

    Returns:
        - status: "configured" if credentials exist, "not_configured" otherwise
        - exchange_name: The exchange name
        - created_at: When credentials were last saved
    """
    exchange_name = exchange_name.lower()

    try:
        async with AsyncSessionFactory() as db:
            repo = CredentialsRepository(db)
            creds = await repo.get_credentials(exchange_name, tenant_id=principal.tenant_id)

        if creds is None:
            return {
                "status": "not_configured",
                "exchange_name": exchange_name,
                "message": f"No credentials found for {exchange_name}. Use POST /api/credentials to save.",
            }

        return {
            "status": "configured",
            "exchange_name": creds.exchange_name,
            "created_at": creds.created_at.isoformat(),
            "updated_at": creds.updated_at.isoformat(),
            "created_by": creds.created_by,
            "active": creds.active,
            "message": "Credentials are securely stored. They will be loaded by the worker.",
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to check credentials: {str(e)}")


@router.delete("/credentials/{exchange_name}")
async def deactivate_credentials(
    exchange_name: str,
    principal: ApiPrincipal = Depends(require_api_auth),
) -> dict:
    """
    Deactivate credentials for an exchange (soft delete).

    This does not permanently delete the credentials but marks them as inactive.
    The worker will fail to start without credentials for the configured exchange.
    """
    exchange_name = exchange_name.lower()

    try:
        async with AsyncSessionFactory() as db:
            repo = CredentialsRepository(db)
            await repo.deactivate_credentials(exchange_name, tenant_id=principal.tenant_id)
            await db.commit()

        return {
            "status": "deactivated",
            "exchange_name": exchange_name,
            "message": f"Credentials for {exchange_name} have been deactivated. Worker will fail to start without new credentials.",
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to deactivate credentials: {str(e)}")
