from __future__ import annotations

import argparse
import asyncio

import asyncpg


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Ensure an active tenant_pair_strategies row exists for Phase 4 shadow replay."
        )
    )
    parser.add_argument(
        "--db-dsn",
        required=True,
        help="PostgreSQL DSN for V4 staging schema.",
    )
    parser.add_argument(
        "--tenant-id",
        default="00000000-0000-0000-0000-000000000001",
        help="Tenant UUID string.",
    )
    parser.add_argument(
        "--exchange",
        default="bybit",
        help="Exchange name.",
    )
    parser.add_argument(
        "--product-id",
        default="SOL-USD",
        help="Product id for shadow replay seed.",
    )
    parser.add_argument(
        "--updated-by",
        default="phase4-shadow-gate",
        help="Audit actor used in tenant_pair_strategies and history.",
    )
    return parser.parse_args()


async def ensure_shadow_strategy(
    *,
    db_dsn: str,
    tenant_id: str,
    exchange: str,
    product_id: str,
    updated_by: str,
) -> int:
    conn = await asyncpg.connect(db_dsn)
    try:
        async with conn.transaction():
            existing = await conn.fetchrow(
                """
                SELECT id
                FROM tenant_pair_strategies
                WHERE tenant_id::text = $1
                  AND exchange_name = $2
                  AND product_id = $3
                  AND is_active IS TRUE
                ORDER BY updated_at DESC
                LIMIT 1
                """,
                tenant_id,
                exchange,
                product_id,
            )
            if existing is not None:
                strategy_id = int(existing["id"])
                print(
                    "Seed skipped: active tenant_pair_strategy already exists "
                    f"(id={strategy_id}, {tenant_id}/{exchange}/{product_id})."
                )
                return strategy_id

            source = await conn.fetchrow(
                """
                SELECT id
                FROM exchange_strategies
                WHERE tenant_id::text = $1
                  AND exchange_name = $2
                  AND is_active IS TRUE
                ORDER BY updated_at DESC
                LIMIT 1
                """,
                tenant_id,
                exchange,
            )
            if source is None:
                raise RuntimeError(
                    "No active exchange_strategies row found for "
                    f"{tenant_id}/{exchange}; cannot seed tenant_pair_strategies."
                )

            inserted = await conn.fetchrow(
                """
                INSERT INTO tenant_pair_strategies (
                    tenant_id,
                    exchange_name,
                    product_id,
                    is_active,
                    spacing_bps,
                    rebalance_threshold_bps,
                    grid_levels,
                    level_size_quote,
                    max_inventory_ratio,
                    maker_fee_rate,
                    stale_reprice_threshold_bps,
                    stale_order_age_seconds,
                    rebalance_defer_seconds,
                    rebalance_defer_max_drift_bps,
                    total_wallet_usd,
                    session_capital_usd,
                    maker_only,
                    paper_mode,
                    local_timezone_iana,
                    daily_close_hour,
                    daily_close_minute,
                    spread_freeze_bps,
                    regime_stress_spread_bps,
                    regime_trend_slope_threshold,
                    regime_mr_distance_threshold_bps,
                    regime_hysteresis_bps,
                    regime_rsi_bear_threshold,
                    regime_rsi_bull_threshold,
                    ws_retry_window_seconds,
                    ws_initial_retry_delay_seconds,
                    ws_max_retry_delay_seconds,
                    ws_message_timeout_seconds,
                    ws_heartbeat_timeout_seconds,
                    updated_by
                )
                SELECT
                    es.tenant_id,
                    es.exchange_name,
                    $3 AS product_id,
                    es.is_active,
                    COALESCE((es.symbol_overrides -> $3 ->> 'spacing_bps')::NUMERIC, es.spacing_bps),
                    COALESCE((es.symbol_overrides -> $3 ->> 'rebalance_threshold_bps')::NUMERIC, es.rebalance_threshold_bps),
                    COALESCE((es.symbol_overrides -> $3 ->> 'grid_levels')::INTEGER, es.grid_levels),
                    COALESCE((es.symbol_overrides -> $3 ->> 'level_size_quote')::NUMERIC, es.level_size_quote),
                    COALESCE((es.symbol_overrides -> $3 ->> 'max_inventory_ratio')::NUMERIC, es.max_inventory_ratio),
                    es.maker_fee_rate,
                    COALESCE((es.symbol_overrides -> $3 ->> 'stale_reprice_threshold_bps')::NUMERIC, es.stale_reprice_threshold_bps),
                    COALESCE((es.symbol_overrides -> $3 ->> 'stale_order_age_seconds')::INTEGER, es.stale_order_age_seconds),
                    COALESCE((es.symbol_overrides -> $3 ->> 'rebalance_defer_seconds')::INTEGER, es.rebalance_defer_seconds),
                    COALESCE((es.symbol_overrides -> $3 ->> 'rebalance_defer_max_drift_bps')::NUMERIC, es.rebalance_defer_max_drift_bps),
                    es.total_wallet_usd,
                    COALESCE((es.symbol_overrides -> $3 ->> 'session_capital_usd')::NUMERIC, es.session_capital_usd),
                    COALESCE((es.symbol_overrides -> $3 ->> 'maker_only')::BOOLEAN, es.maker_only),
                    es.paper_mode,
                    es.local_timezone_iana,
                    es.daily_close_hour,
                    es.daily_close_minute,
                    es.spread_freeze_bps,
                    es.regime_stress_spread_bps,
                    es.regime_trend_slope_threshold,
                    es.regime_mr_distance_threshold_bps,
                    es.regime_hysteresis_bps,
                    es.regime_rsi_bear_threshold,
                    es.regime_rsi_bull_threshold,
                    es.ws_retry_window_seconds,
                    es.ws_initial_retry_delay_seconds,
                    es.ws_max_retry_delay_seconds,
                    es.ws_message_timeout_seconds,
                    es.ws_heartbeat_timeout_seconds,
                    $4 AS updated_by
                FROM exchange_strategies es
                WHERE es.tenant_id::text = $1
                  AND es.exchange_name = $2
                  AND es.is_active IS TRUE
                ORDER BY es.updated_at DESC
                LIMIT 1
                ON CONFLICT DO NOTHING
                RETURNING id
                """,
                tenant_id,
                exchange,
                product_id,
                updated_by,
            )

            if inserted is not None:
                strategy_id = int(inserted["id"])
                print(
                    "Seed inserted: tenant_pair_strategy "
                    f"id={strategy_id} for {tenant_id}/{exchange}/{product_id}."
                )
            else:
                # Race-safe fallback: another writer may have created the row.
                row = await conn.fetchrow(
                    """
                    SELECT id
                    FROM tenant_pair_strategies
                    WHERE tenant_id::text = $1
                      AND exchange_name = $2
                      AND product_id = $3
                      AND is_active IS TRUE
                    ORDER BY updated_at DESC
                    LIMIT 1
                    """,
                    tenant_id,
                    exchange,
                    product_id,
                )
                if row is None:
                    raise RuntimeError(
                        "Seed failed: row was not inserted and no active row exists after insert attempt."
                    )
                strategy_id = int(row["id"])

            history_exists = await conn.fetchval(
                """
                SELECT 1
                FROM tenant_pair_strategy_history
                WHERE strategy_id = $1
                  AND valid_to IS NULL
                LIMIT 1
                """,
                strategy_id,
            )
            if history_exists:
                print(
                    "History skipped: active history row already exists "
                    f"for strategy_id={strategy_id}."
                )
                return strategy_id

            await conn.execute(
                """
                INSERT INTO tenant_pair_strategy_history (
                    strategy_id,
                    tenant_id,
                    exchange_name,
                    product_id,
                    spacing_bps,
                    rebalance_threshold_bps,
                    grid_levels,
                    level_size_quote,
                    max_inventory_ratio,
                    maker_fee_rate,
                    stale_reprice_threshold_bps,
                    stale_order_age_seconds,
                    rebalance_defer_seconds,
                    rebalance_defer_max_drift_bps,
                    total_wallet_usd,
                    session_capital_usd,
                    maker_only,
                    paper_mode,
                    local_timezone_iana,
                    daily_close_hour,
                    daily_close_minute,
                    spread_freeze_bps,
                    regime_stress_spread_bps,
                    regime_trend_slope_threshold,
                    regime_mr_distance_threshold_bps,
                    regime_hysteresis_bps,
                    regime_rsi_bear_threshold,
                    regime_rsi_bull_threshold,
                    ws_retry_window_seconds,
                    ws_initial_retry_delay_seconds,
                    ws_max_retry_delay_seconds,
                    ws_message_timeout_seconds,
                    ws_heartbeat_timeout_seconds,
                    valid_from,
                    valid_to,
                    updated_by,
                    change_summary
                )
                SELECT
                    tps.id,
                    tps.tenant_id,
                    tps.exchange_name,
                    tps.product_id,
                    tps.spacing_bps,
                    tps.rebalance_threshold_bps,
                    tps.grid_levels,
                    tps.level_size_quote,
                    tps.max_inventory_ratio,
                    tps.maker_fee_rate,
                    tps.stale_reprice_threshold_bps,
                    tps.stale_order_age_seconds,
                    tps.rebalance_defer_seconds,
                    tps.rebalance_defer_max_drift_bps,
                    tps.total_wallet_usd,
                    tps.session_capital_usd,
                    tps.maker_only,
                    tps.paper_mode,
                    tps.local_timezone_iana,
                    tps.daily_close_hour,
                    tps.daily_close_minute,
                    tps.spread_freeze_bps,
                    tps.regime_stress_spread_bps,
                    tps.regime_trend_slope_threshold,
                    tps.regime_mr_distance_threshold_bps,
                    tps.regime_hysteresis_bps,
                    tps.regime_rsi_bear_threshold,
                    tps.regime_rsi_bull_threshold,
                    tps.ws_retry_window_seconds,
                    tps.ws_initial_retry_delay_seconds,
                    tps.ws_max_retry_delay_seconds,
                    tps.ws_message_timeout_seconds,
                    tps.ws_heartbeat_timeout_seconds,
                    NOW(),
                    NULL,
                    $2,
                    'phase4 shadow seed'
                FROM tenant_pair_strategies tps
                WHERE tps.id = $1
                """,
                strategy_id,
                updated_by,
            )
            print(f"History inserted for strategy_id={strategy_id}.")
            return strategy_id
    finally:
        await conn.close()


async def main() -> int:
    args = parse_args()
    strategy_id = await ensure_shadow_strategy(
        db_dsn=args.db_dsn,
        tenant_id=args.tenant_id,
        exchange=args.exchange,
        product_id=args.product_id,
        updated_by=args.updated_by,
    )
    print(f"Seed complete. strategy_id={strategy_id}")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
