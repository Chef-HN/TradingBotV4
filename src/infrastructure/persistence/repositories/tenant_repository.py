from __future__ import annotations

import hashlib
import re
import secrets
from datetime import UTC, datetime, timedelta
from uuid import uuid4

from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from infrastructure.persistence.orm.tenant_api_keys import TenantApiKeyRow
from infrastructure.persistence.orm.tenants import TenantRow
from infrastructure.tenancy import DEFAULT_TENANT_ID, DEFAULT_TENANT_NAME, DEFAULT_TENANT_SLUG


def _slugify(name: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", name.strip().lower()).strip("-")
    return slug or f"tenant-{secrets.token_hex(2)}"


class TenantRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    @staticmethod
    def hash_api_key(value: str) -> str:
        return hashlib.sha256(value.encode("utf-8")).hexdigest()

    @staticmethod
    def generate_api_key() -> str:
        return f"tbv4_{secrets.token_urlsafe(32)}"

    async def ensure_default_tenant(self) -> TenantRow:
        row = await self.get_tenant(DEFAULT_TENANT_ID)
        if row is not None:
            return row
        now = datetime.now(UTC)
        row = TenantRow(
            id=DEFAULT_TENANT_ID,
            name=DEFAULT_TENANT_NAME,
            slug=DEFAULT_TENANT_SLUG,
            tier="entry",
            max_capital=100,
            max_pairs=1,
            max_exchanges=1,
            is_active=True,
            created_at=now,
            updated_at=now,
        )
        self._session.add(row)
        await self._session.commit()
        await self._session.refresh(row)
        return row

    async def get_tenant(self, tenant_id: str) -> TenantRow | None:
        result = await self._session.execute(
            select(TenantRow).where(TenantRow.id == tenant_id)
        )
        return result.scalar_one_or_none()

    async def create_tenant_with_api_key(
        self,
        *,
        name: str,
        tier: str,
        max_capital: float,
        max_pairs: int,
        max_exchanges: int,
        created_by: str,
        api_key_label: str | None = None,
        api_key_ttl_days: int | None = None,
    ) -> tuple[TenantRow, str]:
        now = datetime.now(UTC)
        tenant_id = str(uuid4())
        slug = _slugify(name)

        existing_slug = await self._session.execute(
            select(TenantRow.id).where(TenantRow.slug == slug)
        )
        if existing_slug.scalar_one_or_none() is not None:
            slug = f"{slug}-{secrets.token_hex(2)}"

        tenant = TenantRow(
            id=tenant_id,
            name=name.strip() or "Unnamed Tenant",
            slug=slug,
            tier=tier,
            max_capital=max_capital,
            max_pairs=max_pairs,
            max_exchanges=max_exchanges,
            is_active=True,
            created_at=now,
            updated_at=now,
        )
        self._session.add(tenant)

        plain_key = self.generate_api_key()
        expires_at = now + timedelta(days=api_key_ttl_days) if api_key_ttl_days else None
        api_key_row = TenantApiKeyRow(
            id=str(uuid4()),
            tenant_id=tenant_id,
            key_hash=self.hash_api_key(plain_key),
            key_prefix=plain_key[:12],
            key_last4=plain_key[-4:],
            label=api_key_label,
            created_by=created_by,
            created_at=now,
            expires_at=expires_at,
            revoked_at=None,
            active=True,
        )
        self._session.add(api_key_row)
        await self._session.commit()
        await self._session.refresh(tenant)
        return tenant, plain_key

    async def rotate_api_key(
        self,
        *,
        tenant_id: str,
        rotated_by: str,
        label: str | None = None,
        ttl_days: int | None = None,
    ) -> str:
        now = datetime.now(UTC)
        active_keys = await self._session.execute(
            select(TenantApiKeyRow).where(
                TenantApiKeyRow.tenant_id == tenant_id,
                TenantApiKeyRow.active.is_(True),
                TenantApiKeyRow.revoked_at.is_(None),
            )
        )
        for row in active_keys.scalars().all():
            row.active = False
            row.revoked_at = now

        plain_key = self.generate_api_key()
        expires_at = now + timedelta(days=ttl_days) if ttl_days else None
        new_key = TenantApiKeyRow(
            id=str(uuid4()),
            tenant_id=tenant_id,
            key_hash=self.hash_api_key(plain_key),
            key_prefix=plain_key[:12],
            key_last4=plain_key[-4:],
            label=label,
            created_by=rotated_by,
            created_at=now,
            expires_at=expires_at,
            revoked_at=None,
            active=True,
        )
        self._session.add(new_key)
        await self._session.commit()
        return plain_key

    async def resolve_api_key(self, raw_api_key: str) -> tuple[TenantRow, TenantApiKeyRow] | None:
        now = datetime.now(UTC)
        key_hash = self.hash_api_key(raw_api_key)
        result = await self._session.execute(
            select(TenantRow, TenantApiKeyRow)
            .join(TenantApiKeyRow, TenantApiKeyRow.tenant_id == TenantRow.id)
            .where(
                TenantApiKeyRow.key_hash == key_hash,
                TenantApiKeyRow.active.is_(True),
                TenantApiKeyRow.revoked_at.is_(None),
                or_(
                    TenantApiKeyRow.expires_at.is_(None),
                    TenantApiKeyRow.expires_at > now,
                ),
                TenantRow.is_active.is_(True),
            )
            .limit(1)
        )
        row = result.first()
        if row is None:
            return None
        return row[0], row[1]
