"""Repository for managing encrypted exchange credentials."""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from domain.models import ExchangeCredentials
from infrastructure.encryption import decrypt, encrypt
from infrastructure.persistence.orm.exchange_credentials import ExchangeCredentialsRow


class CredentialsRepository:
    """Manage encrypted exchange credentials in the database."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def save_credentials(
        self,
        exchange_name: str,
        api_key: str,
        api_secret: str,
        api_passphrase: str | None = None,
        created_by: str = "system",
    ) -> ExchangeCredentials:
        """
        Save or update exchange credentials (encrypted).

        Args:
            exchange_name: Name of exchange (bybit, coinbase)
            api_key: Unencrypted API key
            api_secret: Unencrypted API secret
            api_passphrase: Optional passphrase (some exchanges)
            created_by: User or system identifier

        Returns:
            ExchangeCredentials object
        """
        now = datetime.now(UTC)
        credential_id = str(uuid4())
        encryption_key_id = "2024-04-09-key-v1"  # TODO: externalize key version

        # Check if credentials already exist for this exchange
        existing = await self._session.execute(
            select(ExchangeCredentialsRow).where(
                ExchangeCredentialsRow.exchange_name == exchange_name,
                ExchangeCredentialsRow.active == True,
            )
        )
        existing_row = existing.scalar_one_or_none()

        # Encrypt sensitive data
        encrypted_key = encrypt(api_key)
        encrypted_secret = encrypt(api_secret)
        encrypted_passphrase = encrypt(api_passphrase) if api_passphrase else None

        if existing_row:
            # Update existing credentials
            existing_row.api_key_encrypted = encrypted_key
            existing_row.api_secret_encrypted = encrypted_secret
            existing_row.api_passphrase_encrypted = encrypted_passphrase
            existing_row.encryption_key_id = encryption_key_id
            existing_row.updated_at = now
            existing_row.created_by = created_by
            await self._session.flush()
            return self._row_to_model(existing_row)
        else:
            # Create new credentials
            row = ExchangeCredentialsRow(
                id=credential_id,
                exchange_name=exchange_name,
                api_key_encrypted=encrypted_key,
                api_secret_encrypted=encrypted_secret,
                api_passphrase_encrypted=encrypted_passphrase,
                encryption_key_id=encryption_key_id,
                created_at=now,
                updated_at=now,
                created_by=created_by,
                active=True,
            )
            self._session.add(row)
            await self._session.flush()
            return self._row_to_model(row)

    async def get_credentials(self, exchange_name: str) -> ExchangeCredentials | None:
        """
        Get decrypted credentials for an exchange.

        Args:
            exchange_name: Name of exchange

        Returns:
            ExchangeCredentials with decrypted values, or None if not found
        """
        stmt = select(ExchangeCredentialsRow).where(
            ExchangeCredentialsRow.exchange_name == exchange_name,
            ExchangeCredentialsRow.active == True,
        )
        result = await self._session.execute(stmt)
        row = result.scalar_one_or_none()

        if row is None:
            return None

        return self._row_to_model(row)

    async def get_all_credentials(self) -> list[ExchangeCredentials]:
        """Get all active credentials (all exchanges)."""
        stmt = select(ExchangeCredentialsRow).where(ExchangeCredentialsRow.active == True)
        result = await self._session.execute(stmt)
        rows = result.scalars().all()
        return [self._row_to_model(r) for r in rows]

    async def deactivate_credentials(self, exchange_name: str) -> None:
        """Soft-delete: mark credentials as inactive."""
        stmt = select(ExchangeCredentialsRow).where(
            ExchangeCredentialsRow.exchange_name == exchange_name
        )
        result = await self._session.execute(stmt)
        row = result.scalar_one_or_none()

        if row:
            row.active = False
            row.updated_at = datetime.now(UTC)
            await self._session.flush()

    def _row_to_model(self, row: ExchangeCredentialsRow) -> ExchangeCredentials:
        """Convert DB row to domain model (with decryption)."""
        return ExchangeCredentials(
            id=row.id,
            exchange_name=row.exchange_name,
            api_key=decrypt(row.api_key_encrypted),
            api_secret=decrypt(row.api_secret_encrypted),
            api_passphrase=decrypt(row.api_passphrase_encrypted) if row.api_passphrase_encrypted else None,
            encryption_key_id=row.encryption_key_id,
            created_at=row.created_at,
            updated_at=row.updated_at,
            created_by=row.created_by,
            active=row.active,
        )
