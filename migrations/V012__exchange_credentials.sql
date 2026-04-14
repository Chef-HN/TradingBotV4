-- Create table for encrypted exchange API credentials

CREATE TABLE IF NOT EXISTS exchange_credentials (
    id VARCHAR(36) PRIMARY KEY,
    exchange_name VARCHAR(50) NOT NULL,
    api_key_encrypted TEXT NOT NULL,
    api_secret_encrypted TEXT NOT NULL,
    api_passphrase_encrypted TEXT,
    encryption_key_id VARCHAR(100) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL,
    created_by VARCHAR(100),
    active BOOLEAN NOT NULL DEFAULT true
);

-- Unique index: only one active credential per exchange
CREATE UNIQUE INDEX IF NOT EXISTS idx_exchange_credentials_unique_active
  ON exchange_credentials(exchange_name)
  WHERE active = true;

-- Index for quick lookup by exchange
CREATE INDEX IF NOT EXISTS idx_exchange_credentials_exchange_name ON exchange_credentials(exchange_name);
CREATE INDEX IF NOT EXISTS idx_exchange_credentials_active ON exchange_credentials(active);
