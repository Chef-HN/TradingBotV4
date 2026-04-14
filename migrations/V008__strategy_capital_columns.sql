-- Move capital/order params from .env to exchange_strategies table
ALTER TABLE exchange_strategies ADD COLUMN IF NOT EXISTS total_wallet_usd NUMERIC(20,10) NOT NULL DEFAULT 200;
ALTER TABLE exchange_strategies ADD COLUMN IF NOT EXISTS session_capital_usd NUMERIC(20,10) NOT NULL DEFAULT 100;
ALTER TABLE exchange_strategies ADD COLUMN IF NOT EXISTS maker_only BOOLEAN NOT NULL DEFAULT true;
