"""AES-256 encryption/decryption for sensitive data."""

from __future__ import annotations

import os
from base64 import b64decode, b64encode

from cryptography.fernet import Fernet


class EncryptionManager:
    """Handles AES-256 encryption/decryption using Fernet (symmetric)."""

    def __init__(self, master_key: str | None = None):
        """
        Initialize encryption manager.

        Args:
            master_key: Master encryption key (32 bytes base64-encoded).
                       If None, tries to load from MASTER_ENCRYPTION_KEY env var.
        """
        if master_key is None:
            master_key = os.environ.get("MASTER_ENCRYPTION_KEY")
            if not master_key:
                raise ValueError(
                    "MASTER_ENCRYPTION_KEY not provided and not in environment. "
                    "Generate with: python -c 'from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())'"
                )

        self.master_key = master_key.encode() if isinstance(master_key, str) else master_key
        self.cipher_suite = Fernet(self.master_key)

    @staticmethod
    def generate_key() -> str:
        """Generate a new master encryption key."""
        return Fernet.generate_key().decode()

    def encrypt(self, plaintext: str) -> str:
        """
        Encrypt plaintext string.

        Args:
            plaintext: String to encrypt

        Returns:
            Base64-encoded ciphertext
        """
        if isinstance(plaintext, str):
            plaintext = plaintext.encode()
        ciphertext = self.cipher_suite.encrypt(plaintext)
        return b64encode(ciphertext).decode()

    def decrypt(self, ciphertext: str) -> str:
        """
        Decrypt ciphertext string.

        Args:
            ciphertext: Base64-encoded ciphertext

        Returns:
            Decrypted plaintext string
        """
        if isinstance(ciphertext, str):
            ciphertext = ciphertext.encode()
        try:
            decoded = b64decode(ciphertext)
            plaintext = self.cipher_suite.decrypt(decoded)
            return plaintext.decode()
        except Exception as e:
            raise ValueError(f"Decryption failed: {e}") from e

    def encrypt_bytes(self, data: bytes) -> bytes:
        """Encrypt bytes."""
        ciphertext = self.cipher_suite.encrypt(data)
        return b64encode(ciphertext)

    def decrypt_bytes(self, ciphertext: bytes) -> bytes:
        """Decrypt bytes."""
        try:
            decoded = b64decode(ciphertext)
            return self.cipher_suite.decrypt(decoded)
        except Exception as e:
            raise ValueError(f"Decryption failed: {e}") from e


# Global instance (lazy-loaded)
_encryption_manager: EncryptionManager | None = None


def get_encryption_manager() -> EncryptionManager:
    """Get or create global encryption manager."""
    global _encryption_manager
    if _encryption_manager is None:
        _encryption_manager = EncryptionManager()
    return _encryption_manager


def encrypt(plaintext: str) -> str:
    """Encrypt a string using global encryption manager."""
    return get_encryption_manager().encrypt(plaintext)


def decrypt(ciphertext: str) -> str:
    """Decrypt a string using global encryption manager."""
    return get_encryption_manager().decrypt(ciphertext)
