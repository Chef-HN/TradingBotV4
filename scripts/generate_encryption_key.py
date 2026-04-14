"""
Script to generate and display the master encryption key.

Usage:
  python -m scripts.generate_encryption_key

This will output a key that should be:
  1. Stored securely (AWS KMS, HashiCorp Vault, environment variable)
  2. Set in MASTER_ENCRYPTION_KEY environment variable
  3. NEVER committed to git

For local development:
  export MASTER_ENCRYPTION_KEY="<generated_key>"

For Docker/production:
  Set MASTER_ENCRYPTION_KEY in docker-compose.yml or k8s secrets
"""

from __future__ import annotations

from infrastructure.encryption import EncryptionManager


def main() -> None:
    """Generate a new encryption key."""
    key = EncryptionManager.generate_key()

    print("=" * 80)
    print("MASTER ENCRYPTION KEY GENERATED")
    print("=" * 80)
    print(f"\nKEY: {key}\n")
    print("INSTRUCTIONS:")
    print("1. Store this key securely (AWS KMS, Vault, etc.)")
    print("2. Set as environment variable: export MASTER_ENCRYPTION_KEY='<key>'")
    print("3. For Docker, add to docker-compose.yml:")
    print(f"   environment:\n     MASTER_ENCRYPTION_KEY: {key}")
    print("\n⚠️  NEVER commit this key to git")
    print("⚠️  This key is required to decrypt all stored credentials")
    print("=" * 80)


if __name__ == "__main__":
    main()
