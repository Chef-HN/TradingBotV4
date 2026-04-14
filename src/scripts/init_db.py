"""
Initialize / migrate the TradingBotV3 database schema.
Applies all V*.sql files in migrations/ in version order.
Safe to run multiple times — all statements use IF NOT EXISTS / ADD COLUMN IF NOT EXISTS.

Run:
    python -m scripts.init_db
"""
from __future__ import annotations

import asyncio
import os
import re
from pathlib import Path

import asyncpg


async def main() -> None:
    migrations_dir = Path(__file__).resolve().parents[2] / "migrations"

    from config import get_settings
    s = get_settings()
    db = s.db

    conn = await asyncpg.connect(
        host=db.host,
        port=db.port,
        database=db.name,
        user=db.user,
        password=db.password,
    )
    try:
        # Apply all V*.sql files in version order (V001, V002, … V999)
        migration_files = sorted(
            migrations_dir.glob("V*.sql"),
            key=lambda p: int(re.match(r"V(\d+)", p.name).group(1)),
        )
        for mf in migration_files:
            sql = mf.read_text(encoding="utf-8")
            await conn.execute(sql)
            print(f"  Applied: {mf.name}")
        print(f"Migrations complete ({len(migration_files)} files).")
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
