"""
Test encryption/decryption flow end-to-end.
Verifies that credentials are encrypted on save and decrypted on load.

Run:
    python -m scripts.test_encryption_flow
"""

from __future__ import annotations

import asyncio
import os
from datetime import UTC, datetime

from infrastructure.encryption import decrypt, encrypt, get_encryption_manager
from infrastructure.persistence.database import AsyncSessionFactory
from infrastructure.persistence.repositories.credentials_repository import CredentialsRepository


async def main() -> None:
    """Test encryption flow."""

    print("=" * 80)
    print("ENCRYPTION FLOW TEST")
    print("=" * 80)

    # Step 1: Verify encryption key is loaded
    print("\n[1] Checking master encryption key...")
    try:
        em = get_encryption_manager()
        print(f"    ✓ Master key loaded: {len(em.master_key)} bytes")
    except ValueError as e:
        print(f"    ❌ ERROR: {e}")
        return

    # Step 2: Test basic encryption/decryption
    print("\n[2] Testing basic encryption/decryption...")
    test_data = "myApiKey123456789"
    try:
        encrypted = encrypt(test_data)
        print(f"    ✓ Encrypted: {encrypted[:50]}...")
        decrypted = decrypt(encrypted)
        assert decrypted == test_data
        print(f"    ✓ Decrypted: {decrypted}")
    except Exception as e:
        print(f"    ❌ ERROR: {e}")
        return

    # Step 3: Save credentials to database
    print("\n[3] Saving Bybit credentials to database...")
    try:
        async with AsyncSessionFactory() as db:
            repo = CredentialsRepository(db)
            creds = await repo.save_credentials(
                exchange_name="bybit",
                api_key="eOmOtjnyNSYr0eIhZY",
                api_secret="me2BuYO2ZnVV0YhY2F2k2IyzC91XlyVSsZgp",
                api_passphrase=None,
                created_by="test_script",
            )
            await db.commit()

        print(f"    ✓ Credentials saved:")
        print(f"      - ID: {creds.id}")
        print(f"      - Exchange: {creds.exchange_name}")
        print(f"      - API key (last 4): ...{creds.api_key[-4:]}")
        print(f"      - Created by: {creds.created_by}")
    except Exception as e:
        print(f"    ❌ ERROR: {e}")
        return

    # Step 4: Retrieve and verify decryption
    print("\n[4] Retrieving credentials from database...")
    try:
        async with AsyncSessionFactory() as db:
            repo = CredentialsRepository(db)
            retrieved = await repo.get_credentials("bybit")

        if retrieved is None:
            print("    ❌ ERROR: Credentials not found in database")
            return

        print(f"    ✓ Credentials retrieved:")
        print(f"      - Exchange: {retrieved.exchange_name}")
        print(f"      - API key decrypted: {retrieved.api_key}")
        print(f"      - API secret decrypted: {retrieved.api_secret}")
        print(f"      - Created at: {retrieved.created_at}")
    except Exception as e:
        print(f"    ❌ ERROR: {e}")
        return

    # Step 5: Verify data integrity
    print("\n[5] Verifying data integrity...")
    try:
        assert retrieved.api_key == "eOmOtjnyNSYr0eIhZY"
        assert retrieved.api_secret == "me2BuYO2ZnVV0YhY2F2k2IyzC91XlyVSsZgp"
        assert retrieved.exchange_name == "bybit"
        print("    ✓ All data verified correctly")
    except AssertionError as e:
        print(f"    ❌ ERROR: Data mismatch: {e}")
        return

    # Step 6: Check that plaintext is NOT in database
    print("\n[6] Verifying plaintext NOT stored in database...")
    try:
        async with AsyncSessionFactory() as db:
            from sqlalchemy import text

            result = await db.execute(
                text("SELECT api_key_encrypted, api_secret_encrypted FROM exchange_credentials WHERE exchange_name = 'bybit'")
            )
            row = result.fetchone()

        if row is None:
            print("    ❌ ERROR: Could not read from database")
            return

        encrypted_key, encrypted_secret = row
        print(f"    Encrypted key (DB): {encrypted_key[:50]}...")
        print(f"    Encrypted secret (DB): {encrypted_secret[:50]}...")

        # Verify it's NOT the plaintext
        assert "eOmOtjnyNSYr0eIhZY" not in encrypted_key
        assert "me2BuYO2ZnVV0YhY2F2k2IyzC91XlyVSsZgp" not in encrypted_secret
        print("    ✓ Plaintext NOT found in database ✓")
    except Exception as e:
        print(f"    ❌ ERROR: {e}")
        return

    # Step 7: Test Coinbase credentials too
    print("\n[7] Testing Coinbase credentials...")
    try:
        async with AsyncSessionFactory() as db:
            repo = CredentialsRepository(db)
            coinbase_creds = await repo.save_credentials(
                exchange_name="coinbase",
                api_key="organizations/013da8ef-201c-4c5c-89c1-8e392efed60d/apiKeys/232929a5-a907-4fda-8aba-94863a252f88",
                api_secret="-----BEGIN EC PRIVATE KEY-----\nMHcCAQEEIH...",
                api_passphrase="test_passphrase",
                created_by="test_script",
            )
            await db.commit()

        print(f"    ✓ Coinbase credentials saved")
        print(f"      - Exchange: {coinbase_creds.exchange_name}")
        print(f"      - API key (first 50): {coinbase_creds.api_key[:50]}...")
        print(f"      - Passphrase: {coinbase_creds.api_passphrase}")
    except Exception as e:
        print(f"    ❌ ERROR: {e}")
        return

    # Step 8: List all credentials
    print("\n[8] Listing all active credentials...")
    try:
        async with AsyncSessionFactory() as db:
            repo = CredentialsRepository(db)
            all_creds = await repo.get_all_credentials()

        print(f"    ✓ Found {len(all_creds)} active credentials:")
        for cred in all_creds:
            print(f"      - {cred.exchange_name}: API key ...{cred.api_key[-4:]}")
    except Exception as e:
        print(f"    ❌ ERROR: {e}")
        return

    print("\n" + "=" * 80)
    print("✓ ALL TESTS PASSED")
    print("=" * 80)
    print("\nConclusion:")
    print("  - Encryption layer is working correctly")
    print("  - Credentials are encrypted at rest in database")
    print("  - Credentials are decrypted only in memory")
    print("  - Multiple exchanges are supported")
    print("\nNext steps:")
    print("  1. Test API endpoint with authentication")
    print("  2. Deploy to production with secrets manager")
    print("  3. Implement key rotation script")


if __name__ == "__main__":
    asyncio.run(main())
