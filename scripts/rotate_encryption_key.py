"""
Rotate encryption keys for exchange credentials.

This script re-encrypts all stored credentials with a new master key.
Used when the old key is compromised or during periodic key rotation.

Usage:
    python -m scripts.rotate_encryption_key --old-key OLD_KEY --new-key NEW_KEY

Example:
    python -m scripts.rotate_encryption_key \
      --old-key "gAAAAABlxyz..." \
      --new-key "gAAAAACdefg..."
"""

from __future__ import annotations

import argparse
import asyncio
from datetime import UTC, datetime

from infrastructure.encryption import EncryptionManager
from infrastructure.persistence.database import AsyncSessionFactory
from infrastructure.persistence.repositories.credentials_repository import CredentialsRepository


async def main(old_key: str, new_key: str) -> None:
    """Rotate encryption keys for all credentials."""

    print("=" * 80)
    print("ENCRYPTION KEY ROTATION")
    print("=" * 80)

    # Validate keys
    print("\n[1] Validating encryption keys...")
    try:
        old_manager = EncryptionManager(old_key)
        new_manager = EncryptionManager(new_key)
        print("    ✓ Old key: Valid")
        print("    ✓ New key: Valid")
    except ValueError as e:
        print(f"    ❌ ERROR: {e}")
        return

    # Fetch all credentials encrypted with old key
    print("\n[2] Fetching all credentials from database...")
    try:
        async with AsyncSessionFactory() as db:
            repo = CredentialsRepository(db)
            all_creds = await repo.get_all_credentials()
    except Exception as e:
        print(f"    ❌ ERROR: {e}")
        return

    if not all_creds:
        print("    ℹ️  No credentials found. Nothing to rotate.")
        return

    print(f"    ✓ Found {len(all_creds)} credentials to rotate")

    # Re-encrypt each credential
    print("\n[3] Re-encrypting credentials with new key...")
    rotated_count = 0

    for cred in all_creds:
        try:
            print(f"\n    Processing {cred.exchange_name}...")

            # Save with new key
            # Note: CredentialsRepository uses the global encryption manager,
            # so we need to temporarily replace it
            async with AsyncSessionFactory() as db:
                # Read encrypted data
                from sqlalchemy import text

                result = await db.execute(
                    text(
                        "SELECT api_key_encrypted, api_secret_encrypted, api_passphrase_encrypted "
                        "FROM exchange_credentials WHERE exchange_name = :exchange"
                    ),
                    {"exchange": cred.exchange_name},
                )
                row = result.fetchone()

                if row:
                    old_api_key_enc, old_api_secret_enc, old_api_pass_enc = row

                    # Decrypt with old key
                    api_key = old_manager.decrypt(old_api_key_enc)
                    api_secret = old_manager.decrypt(old_api_secret_enc)
                    api_passphrase = (
                        old_manager.decrypt(old_api_pass_enc) if old_api_pass_enc else None
                    )

                    # Re-encrypt with new key
                    new_api_key_enc = new_manager.encrypt(api_key)
                    new_api_secret_enc = new_manager.encrypt(api_secret)
                    new_api_pass_enc = (
                        new_manager.encrypt(api_passphrase) if api_passphrase else None
                    )

                    # Update database
                    await db.execute(
                        text(
                            "UPDATE exchange_credentials "
                            "SET api_key_encrypted = :key_enc, "
                            "    api_secret_encrypted = :secret_enc, "
                            "    api_passphrase_encrypted = :pass_enc, "
                            "    encryption_key_id = :key_id, "
                            "    updated_at = :updated_at "
                            "WHERE exchange_name = :exchange"
                        ),
                        {
                            "key_enc": new_api_key_enc,
                            "secret_enc": new_api_secret_enc,
                            "pass_enc": new_api_pass_enc,
                            "key_id": "2024-04-09-key-v2",  # Increment version
                            "updated_at": datetime.now(UTC),
                            "exchange": cred.exchange_name,
                        },
                    )
                    await db.commit()

                    print(f"      ✓ {cred.exchange_name}: re-encrypted successfully")
                    rotated_count += 1

        except Exception as e:
            print(f"      ❌ ERROR: {e}")
            continue

    # Verify rotation
    print("\n[4] Verifying rotation...")
    try:
        async with AsyncSessionFactory() as db:
            repo = CredentialsRepository(db)
            # Use new manager for verification
            # (In production, update the global MASTER_ENCRYPTION_KEY before this step)
            print("    ℹ️  Verification requires updating MASTER_ENCRYPTION_KEY in environment")
            print("    ℹ️  Please restart the worker with the new key")
    except Exception as e:
        print(f"    ❌ ERROR: {e}")

    print("\n" + "=" * 80)
    print(f"✓ ROTATION COMPLETE: {rotated_count}/{len(all_creds)} credentials rotated")
    print("=" * 80)

    print("\n⚠️  IMPORTANT NEXT STEPS:")
    print("  1. Update MASTER_ENCRYPTION_KEY environment variable:")
    print("     export MASTER_ENCRYPTION_KEY='<new_key>'")
    print("  2. Restart the worker:")
    print("     supervisorctl restart worker")
    print("  3. Verify worker starts without decryption errors")
    print("  4. Document the old key for audit purposes (store securely)")
    print("  5. Schedule periodic key rotation (quarterly recommended)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Rotate encryption keys for exchange credentials")
    parser.add_argument(
        "--old-key",
        required=True,
        help="Current master encryption key (will be replaced)",
    )
    parser.add_argument(
        "--new-key",
        required=True,
        help="New master encryption key",
    )

    args = parser.parse_args()

    asyncio.run(main(args.old_key, args.new_key))
