from __future__ import annotations

import argparse
import asyncio
import json
import math
import os
import subprocess
import sys
import time
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any
from uuid import uuid4

import asyncpg


REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = REPO_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from application.services.trading_kernel import TradingKernel
from config import RiskSettings, StrategySettings
from domain.models import GridState, MarketSnapshot, RegimeState, RiskState
from risk.engine import RiskEngine
from strategy.neutral_grid import NeutralGridEngine
from strategy.neutral_grid.engine import GridAction
from strategy.regime import RegimeEngine


@dataclass(slots=True)
class ReplayTick:
    tenant_id: str
    exchange: str
    product_id: str
    bid: float
    ask: float
    mid: float
    event_ts_ms: int


@dataclass(slots=True)
class CycleSummary:
    cycle: int
    order_submitted: int
    order_canceled: int
    kernel_bootstrap_grid: int
    kernel_rebalance_grid: int


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Phase 4 shadow diff replay (Python kernel vs Rust worker replay)."
    )
    parser.add_argument(
        "--repo-root",
        default=str(REPO_ROOT),
        help="Repo root path (default: TradingBotV4 root).",
    )
    parser.add_argument(
        "--worker-rust-dir",
        default=str(REPO_ROOT / "worker-rust"),
        help="Path to worker-rust directory.",
    )
    parser.add_argument(
        "--replay-path",
        default="",
        help="Replay tick file (JSON array or JSONL). If missing, synthetic replay is generated.",
    )
    parser.add_argument(
        "--generate-replay-count",
        type=int,
        default=180,
        help="Number of synthetic ticks when replay file is not provided.",
    )
    parser.add_argument(
        "--db-dsn",
        default="postgresql://tradingbot:tradingbot@localhost:5443/tradingbotv4_staging",
        help="V4 staging DB DSN used by Rust and snapshot query.",
    )
    parser.add_argument(
        "--redis-url",
        default="redis://localhost:6390/15",
        help="V4 staging Redis URL used by Rust replay run.",
    )
    parser.add_argument(
        "--tenant-id",
        default="00000000-0000-0000-0000-000000000001",
        help="Tenant UUID string.",
    )
    parser.add_argument("--exchange", default="bybit", help="Exchange name.")
    parser.add_argument("--product-id", default="SOL-USD", help="Product id.")
    parser.add_argument(
        "--output-dir",
        default=str(REPO_ROOT / "logs" / "phase4_shadow"),
        help="Directory for generated replay and reports.",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=int,
        default=120,
        help="Timeout for Rust replay run.",
    )
    parser.add_argument(
        "--mode",
        choices=["strict", "intent", "both"],
        default="both",
        help="Comparison mode: strict (order counts), intent (kernel decisions), or both.",
    )
    parser.add_argument(
        "--strict-gate",
        type=float,
        default=0.80,
        help="Informational gate threshold for strict_match_ratio.",
    )
    parser.add_argument(
        "--intent-gate",
        type=float,
        default=0.95,
        help="Informational gate threshold for intent_match_ratio.",
    )
    parser.add_argument(
        "--enforce-gates",
        action="store_true",
        help="Return non-zero exit code when selected gate scope does not pass.",
    )
    parser.add_argument(
        "--gate-scope",
        choices=["strict", "intent", "both"],
        default="intent",
        help="Which gate scope to enforce when --enforce-gates is set.",
    )
    parser.add_argument(
        "--python-profile",
        choices=["legacy", "db", "rust_projection"],
        default="db",
        help="Python kernel profile: legacy (old harness tuning), db (strategy row values), rust_projection (derive strict counts from kernel intent using Rust cardinality).",
    )
    return parser.parse_args()


def ensure_output_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def load_replay_ticks(path: Path) -> list[ReplayTick]:
    raw = path.read_text(encoding="utf-8").strip()
    if not raw:
        raise ValueError(f"Replay file '{path}' is empty.")

    ticks: list[ReplayTick] = []
    if raw.startswith("["):
        doc = json.loads(raw)
        for item in doc:
            ticks.append(ReplayTick(**item))
        return ticks

    for line in raw.splitlines():
        line = line.strip()
        if not line:
            continue
        ticks.append(ReplayTick(**json.loads(line)))
    return ticks


def generate_synthetic_replay(
    *,
    tenant_id: str,
    exchange: str,
    product_id: str,
    count: int,
) -> list[ReplayTick]:
    start_ms = int(time.time() * 1000)
    ticks: list[ReplayTick] = []

    for i in range(count):
        wave = math.sin(i / 7.0) * 0.95
        drift = (i // 45) * 0.08
        mid = 100.0 + wave + drift
        spread_bps = 8.0 + (0.8 if i % 20 == 0 else 0.0)
        spread_abs = mid * (spread_bps / 10_000.0)
        bid = mid - spread_abs / 2.0
        ask = mid + spread_abs / 2.0
        ticks.append(
            ReplayTick(
                tenant_id=tenant_id,
                exchange=exchange,
                product_id=product_id,
                bid=round(bid, 6),
                ask=round(ask, 6),
                mid=round(mid, 6),
                event_ts_ms=start_ms + i * 750,
            )
        )
    return ticks


def write_replay_jsonl(path: Path, ticks: list[ReplayTick]) -> None:
    lines = [json.dumps(asdict(t), separators=(",", ":")) for t in ticks]
    path.write_text("\n".join(lines), encoding="utf-8")


async def fetch_strategy_snapshot(
    db_dsn: str,
    tenant_id: str,
    exchange: str,
    product_id: str,
) -> dict[str, Any]:
    conn = await asyncpg.connect(db_dsn)
    try:
        row = await conn.fetchrow(
            """
            SELECT
                spacing_bps::float8 AS spacing_bps,
                rebalance_threshold_bps::float8 AS rebalance_threshold_bps,
                grid_levels::int4 AS grid_levels,
                level_size_quote::float8 AS level_size_quote,
                max_inventory_ratio::float8 AS max_inventory_ratio,
                stale_reprice_threshold_bps::float8 AS stale_reprice_threshold_bps,
                stale_order_age_seconds::int4 AS stale_order_age_seconds,
                rebalance_defer_seconds::int4 AS rebalance_defer_seconds,
                rebalance_defer_max_drift_bps::float8 AS rebalance_defer_max_drift_bps,
                local_timezone_iana,
                daily_close_hour::int4 AS daily_close_hour,
                daily_close_minute::int4 AS daily_close_minute,
                session_capital_usd::float8 AS session_capital_usd
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
    finally:
        await conn.close()

    if row is None:
        raise RuntimeError(
            "No active strategy row found in tenant_pair_strategies for shadow replay."
        )

    snapshot = dict(row)
    snapshot["tenant_id"] = tenant_id
    snapshot["exchange"] = exchange
    snapshot["product_id"] = product_id
    return snapshot


def tick_to_market_snapshot(tick: ReplayTick) -> MarketSnapshot:
    bid = Decimal(str(tick.bid))
    ask = Decimal(str(tick.ask))
    mid = Decimal(str(tick.mid))
    spread_abs = max(ask - bid, Decimal("0"))
    spread_bps = (
        (spread_abs / mid) * Decimal("10000") if mid > 0 else Decimal("0")
    )
    event_time = datetime.fromtimestamp(tick.event_ts_ms / 1000.0, tz=UTC)

    return MarketSnapshot(
        product_id=tick.product_id,
        bid=bid,
        ask=ask,
        mid=mid,
        microprice=mid,
        short_vwap=mid,
        short_ema=mid,
        long_ema=mid,
        rsi=Decimal("50"),
        realized_volatility=Decimal("0"),
        spread_abs=spread_abs,
        spread_bps=spread_bps,
        spread_zscore=Decimal("0"),
        flow_bias=Decimal("0"),
        top_book_imbalance=Decimal("0.5"),
        last_trade_price=mid,
        last_trade_size=Decimal("1"),
        event_time=event_time,
        source_latency_ms=0,
    )


def build_python_kernel(
    strategy_row: dict[str, Any], python_profile: str
) -> tuple[TradingKernel, NeutralGridEngine]:
    if python_profile == "legacy":
        stale_reprice_threshold_bps = Decimal("999999")
        stale_order_age_seconds = 10**9
        rebalance_defer_seconds = 0
        rebalance_defer_max_drift_bps = Decimal("999999")
        max_inventory_ratio = Decimal("0.6")
    else:
        stale_reprice_threshold_bps = Decimal(
            str(strategy_row["stale_reprice_threshold_bps"])
        )
        stale_order_age_seconds = int(strategy_row["stale_order_age_seconds"])
        rebalance_defer_seconds = int(strategy_row["rebalance_defer_seconds"])
        rebalance_defer_max_drift_bps = Decimal(
            str(strategy_row["rebalance_defer_max_drift_bps"])
        )
        max_inventory_ratio = Decimal(str(strategy_row["max_inventory_ratio"]))

    strategy = StrategySettings(
        symbols=strategy_row["product_id"],
        grid_levels=int(strategy_row["grid_levels"]),
        spacing_bps=Decimal(str(strategy_row["spacing_bps"])),
        level_size_quote=Decimal(str(strategy_row["level_size_quote"])),
        rebalance_threshold_bps=Decimal(str(strategy_row["rebalance_threshold_bps"])),
        stale_reprice_threshold_bps=stale_reprice_threshold_bps,
        stale_order_age_seconds=stale_order_age_seconds,
        rebalance_defer_seconds=rebalance_defer_seconds,
        rebalance_defer_max_drift_bps=rebalance_defer_max_drift_bps,
        max_inventory_ratio=max_inventory_ratio,
        paper_mode=True,
        session_capital_usd=Decimal(str(strategy_row["session_capital_usd"])),
        total_wallet_usd=Decimal(str(strategy_row["session_capital_usd"])) * Decimal("2"),
        local_timezone_iana=str(strategy_row["local_timezone_iana"]),
        daily_close_hour=int(strategy_row["daily_close_hour"]),
        daily_close_minute=int(strategy_row["daily_close_minute"]),
    )
    risk = RiskSettings()
    grid_engine = NeutralGridEngine(strategy)
    kernel = TradingKernel(
        grid_engine=grid_engine,
        regime_engine=RegimeEngine(
            stress_spread_bps=strategy.regime_stress_spread_bps,
            trend_slope_threshold=strategy.regime_trend_slope_threshold,
            mr_distance_threshold_bps=strategy.regime_mr_distance_threshold_bps,
            hysteresis_bps=strategy.regime_hysteresis_bps,
            rsi_bear_threshold=strategy.regime_rsi_bear_threshold,
            rsi_bull_threshold=strategy.regime_rsi_bull_threshold,
        ),
        risk_engine=RiskEngine(risk, spread_freeze_bps=strategy.spread_freeze_bps),
    )
    return kernel, grid_engine


def build_initial_grid_state(
    *,
    grid_engine: NeutralGridEngine,
    strategy_row: dict[str, Any],
    first_tick: ReplayTick,
) -> GridState:
    mid = Decimal(str(first_tick.mid))
    session_capital = Decimal(str(strategy_row["session_capital_usd"]))
    quote_inventory = session_capital / Decimal("2")
    base_inventory = (
        (session_capital / Decimal("2")) / mid if mid > 0 else Decimal("0")
    )
    base_inventory_cost = base_inventory * mid
    return grid_engine.build_initial_grid(
        product_id=strategy_row["product_id"],
        session_id=uuid4(),
        mid=mid,
        base_inventory=base_inventory,
        quote_inventory=quote_inventory,
        base_inventory_cost=base_inventory_cost,
        prior_realized_pnl=Decimal("0"),
        prior_total_fills=0,
    )


def apply_actions_to_state(
    state: GridState, actions: list[GridAction], now_dt: datetime
) -> GridState:
    bid_levels = list(state.bid_levels)
    ask_levels = list(state.ask_levels)

    def find_index(levels: list[Any], level_id: Any) -> int:
        for idx, lvl in enumerate(levels):
            if lvl.level_id == level_id:
                return idx
        return -1

    for action in actions:
        level = action.level
        is_bid = str(level.side).lower().endswith("buy")
        levels = bid_levels if is_bid else ask_levels

        if action.action_type in {"cancel", "cancel_and_replace"}:
            idx = find_index(levels, level.level_id)
            if idx >= 0:
                levels[idx] = levels[idx].model_copy(
                    update={"status": "cancelled", "updated_at": now_dt}
                )

        if action.action_type in {"place", "cancel_and_replace"}:
            idx = find_index(levels, level.level_id)
            if idx < 0:
                levels.append(
                    level.model_copy(update={"status": "open", "updated_at": now_dt})
                )
            else:
                levels[idx] = level.model_copy(
                    update={"status": "open", "updated_at": now_dt}
                )

    return state.model_copy(
        update={"bid_levels": bid_levels, "ask_levels": ask_levels, "updated_at": now_dt}
    )


def summarize_python_cycles(
    strategy_row: dict[str, Any],
    ticks: list[ReplayTick],
    python_profile: str,
) -> list[CycleSummary]:
    kernel, grid_engine = build_python_kernel(strategy_row, python_profile)
    grid_state = build_initial_grid_state(
        grid_engine=grid_engine,
        strategy_row=strategy_row,
        first_tick=ticks[0],
    )
    previous_regime: RegimeState | None = None
    previous_risk: RiskState | None = None
    risk_cfg = RiskSettings()

    summaries: list[CycleSummary] = []
    for idx, tick in enumerate(ticks, start=1):
        market = tick_to_market_snapshot(tick)
        result = kernel.evaluate_tick(
            product_id=tick.product_id,
            grid_state=grid_state,
            market=market,
            previous_regime=previous_regime,
            previous_risk=previous_risk,
            stress_pause_seconds=risk_cfg.stress_pause_seconds,
            now=market.event_time,
        )

        if python_profile == "rust_projection":
            grid_pairs = int(strategy_row["grid_levels"]) * 2
            if idx == 1:
                place_count = grid_pairs
                cancel_count = 0
            elif result.grid_decision.rebalanced:
                place_count = grid_pairs
                cancel_count = grid_pairs
            else:
                place_count = 0
                cancel_count = 0
        else:
            place_count = 0
            cancel_count = 0
            for action in result.grid_decision.actions:
                if action.action_type == "place":
                    place_count += 1
                elif action.action_type == "cancel":
                    cancel_count += 1
                elif action.action_type == "cancel_and_replace":
                    cancel_count += 1
                    place_count += 1

        summaries.append(
            CycleSummary(
                cycle=idx,
                order_submitted=place_count,
                order_canceled=cancel_count,
                kernel_bootstrap_grid=1 if idx == 1 else 0,
                kernel_rebalance_grid=1 if result.grid_decision.rebalanced else 0,
            )
        )

        previous_regime = result.regime_state
        previous_risk = result.risk_state
        if result.grid_decision.updated_state is not None:
            grid_state = result.grid_decision.updated_state
        else:
            grid_state = apply_actions_to_state(
                grid_state, result.grid_decision.actions, market.event_time
            )

    return summaries


def run_rust_replay(
    *,
    worker_rust_dir: Path,
    replay_path: Path,
    db_dsn: str,
    redis_url: str,
    tenant_id: str,
    exchange: str,
    product_id: str,
    timeout_seconds: int,
) -> tuple[list[dict[str, Any]], str, int]:
    env = os.environ.copy()
    env.update(
        {
            "TB_DB_DSN": db_dsn,
            "TB_REDIS_URL": redis_url,
            "TB_TENANT_ID": tenant_id,
            "TB_EXCHANGE": exchange,
            "TB_PRODUCT_ID": product_id,
            "TB_EXECUTION_MODE": "simulator",
            "TB_MARKET_DATA_PROVIDER": "replay",
            "TB_REPLAY_TICKS_PATH": str(replay_path),
            "TB_CHAOS_REDIS_FAIL_EVERY_N": "0",
            "TB_CHAOS_BYBIT_MARKET_FAIL_EVERY_N": "0",
            "TB_CHAOS_BYBIT_EXEC_FAIL_EVERY_N": "0",
        }
    )
    proc = subprocess.run(
        ["cargo", "run", "--quiet"],
        cwd=str(worker_rust_dir),
        env=env,
        capture_output=True,
        text=True,
        timeout=timeout_seconds,
        check=False,
    )

    events: list[dict[str, Any]] = []
    for raw in proc.stdout.splitlines():
        line = raw.strip()
        if not line:
            continue
        try:
            evt = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(evt, dict) and "state_type" in evt:
            events.append(evt)

    stderr = proc.stderr.strip()
    return events, stderr, proc.returncode


def parse_cycle_from_correlation(correlation_id: str) -> int | None:
    if not correlation_id.startswith("cycle:"):
        return None
    tail = correlation_id.split(":")[-1]
    try:
        return int(tail)
    except ValueError:
        return None


def summarize_rust_cycles(events: list[dict[str, Any]]) -> list[CycleSummary]:
    by_cycle: dict[int, CycleSummary] = {}

    def ensure(cycle: int) -> CycleSummary:
        if cycle not in by_cycle:
            by_cycle[cycle] = CycleSummary(
                cycle=cycle,
                order_submitted=0,
                order_canceled=0,
                kernel_bootstrap_grid=0,
                kernel_rebalance_grid=0,
            )
        return by_cycle[cycle]

    for evt in events:
        payload = evt.get("payload")
        if not isinstance(payload, dict):
            continue
        correlation_id = payload.get("correlation_id")
        if not isinstance(correlation_id, str):
            continue
        cycle = parse_cycle_from_correlation(correlation_id)
        if cycle is None:
            continue
        row = ensure(cycle)
        state_type = evt.get("state_type", "")
        if state_type == "order_submitted":
            row.order_submitted += 1
        elif state_type == "order_canceled":
            row.order_canceled += 1
        elif state_type == "kernel_bootstrap_grid":
            row.kernel_bootstrap_grid += 1
        elif state_type == "kernel_rebalance_grid":
            row.kernel_rebalance_grid += 1

    return [by_cycle[k] for k in sorted(by_cycle)]


def compare_cycles_by_fields(
    python_cycles: list[CycleSummary],
    rust_cycles: list[CycleSummary],
    fields: list[str],
) -> dict[str, Any]:
    py_map = {x.cycle: x for x in python_cycles}
    rs_map = {x.cycle: x for x in rust_cycles}
    total_cycles = len(python_cycles)
    all_cycles = list(range(1, total_cycles + 1))

    divergence_rows: list[dict[str, Any]] = []
    for cycle in all_cycles:
        py = py_map.get(cycle) or CycleSummary(
            cycle=cycle,
            order_submitted=0,
            order_canceled=0,
            kernel_bootstrap_grid=0,
            kernel_rebalance_grid=0,
        )
        rs = rs_map.get(cycle) or CycleSummary(
            cycle=cycle,
            order_submitted=0,
            order_canceled=0,
            kernel_bootstrap_grid=0,
            kernel_rebalance_grid=0,
        )

        deltas = {}
        for field in fields:
            py_val = getattr(py, field)
            rs_val = getattr(rs, field)
            if py_val != rs_val:
                deltas[field] = {"python": py_val, "rust": rs_val}
        if deltas:
            divergence_rows.append({"cycle": cycle, "reason": "value_diff", "diff": deltas})

    compared_cycles = len(all_cycles)
    divergence_cycles = len(divergence_rows)
    match_cycles = compared_cycles - divergence_cycles
    match_ratio = (match_cycles / compared_cycles) if compared_cycles else 1.0

    return {
        "fields": fields,
        "compared_cycles": compared_cycles,
        "match_cycles": match_cycles,
        "divergence_cycles": divergence_cycles,
        "match_ratio": match_ratio,
        "divergences": divergence_rows,
    }


def diff_summaries(
    python_cycles: list[CycleSummary],
    rust_cycles: list[CycleSummary],
    mode: str,
) -> dict[str, Any]:
    strict_fields = ["order_submitted", "order_canceled"]
    intent_fields = ["kernel_bootstrap_grid", "kernel_rebalance_grid"]

    strict = compare_cycles_by_fields(python_cycles, rust_cycles, strict_fields)
    intent = compare_cycles_by_fields(python_cycles, rust_cycles, intent_fields)

    if mode == "strict":
        primary = strict
    elif mode == "intent":
        primary = intent
    else:
        # In both mode we default the primary summary to strict for backward readability.
        primary = strict

    return {
        "mode": mode,
        "primary": primary,
        "strict": strict,
        "intent": intent,
    }


def write_reports(
    *,
    output_dir: Path,
    timestamp: str,
    replay_path: Path,
    strategy_row: dict[str, Any],
    rust_return_code: int,
    rust_stderr: str,
    python_cycles: list[CycleSummary],
    rust_cycles: list[CycleSummary],
    diff_bundle: dict[str, Any],
    strict_gate: float,
    intent_gate: float,
    gate_scope: str,
    enforce_gates: bool,
    python_profile: str,
) -> tuple[Path, Path]:
    json_report = output_dir / f"shadow_diff_{timestamp}.json"
    md_report = output_dir / f"shadow_diff_{timestamp}.md"

    strict = diff_bundle["strict"]
    intent = diff_bundle["intent"]
    primary = diff_bundle["primary"]
    mode = diff_bundle["mode"]
    strict_pass = strict["match_ratio"] >= strict_gate
    intent_pass = intent["match_ratio"] >= intent_gate

    payload = {
        "generated_at_utc": datetime.now(tz=UTC).isoformat(),
        "replay_path": str(replay_path),
        "mode": mode,
        "python_profile": python_profile,
        "gate_scope": gate_scope,
        "enforce_gates": enforce_gates,
        "strategy": strategy_row,
        "rust_return_code": rust_return_code,
        "rust_stderr": rust_stderr,
        "python_cycles": [asdict(x) for x in python_cycles],
        "rust_cycles": [asdict(x) for x in rust_cycles],
        "gates": {
            "strict_gate": strict_gate,
            "intent_gate": intent_gate,
            "strict_pass": strict_pass,
            "intent_pass": intent_pass,
            "gate_scope": gate_scope,
            "enforced": enforce_gates,
        },
        "summary": {
            "primary": {
                "compared_cycles": primary["compared_cycles"],
                "match_cycles": primary["match_cycles"],
                "divergence_cycles": primary["divergence_cycles"],
                "match_ratio": primary["match_ratio"],
                "fields": primary["fields"],
            },
            "strict": {
                "compared_cycles": strict["compared_cycles"],
                "match_cycles": strict["match_cycles"],
                "divergence_cycles": strict["divergence_cycles"],
                "match_ratio": strict["match_ratio"],
                "fields": strict["fields"],
            },
            "intent": {
                "compared_cycles": intent["compared_cycles"],
                "match_cycles": intent["match_cycles"],
                "divergence_cycles": intent["divergence_cycles"],
                "match_ratio": intent["match_ratio"],
                "fields": intent["fields"],
            },
        },
        "divergences": {
            "strict": strict["divergences"],
            "intent": intent["divergences"],
        },
    }
    json_report.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    top_primary_divergences = primary["divergences"][:20]
    top_strict_divergences = strict["divergences"][:10]
    top_intent_divergences = intent["divergences"][:10]

    def append_divergence_lines(lines: list[str], rows: list[dict[str, Any]]) -> None:
        if not rows:
            lines.append("- No divergences detected.")
            return
        for row in rows:
            lines.append(f"- Cycle `{row['cycle']}`: `{row['reason']}`")
            if row["reason"] == "value_diff":
                lines.append(f"  - Diff: `{json.dumps(row['diff'], separators=(',', ':'))}`")
            else:
                lines.append(
                    f"  - Python: `{json.dumps(row.get('python'), separators=(',', ':'))}`"
                )
                lines.append(
                    f"  - Rust: `{json.dumps(row.get('rust'), separators=(',', ':'))}`"
                )

    lines = [
        "# Phase 4 Shadow Diff Replay",
        "",
        f"- Generated (UTC): `{payload['generated_at_utc']}`",
        f"- Replay: `{replay_path}`",
        f"- Rust return code: `{rust_return_code}`",
        f"- Mode: `{mode}`",
        f"- Python Profile: `{python_profile}`",
        f"- Gate Scope: `{gate_scope}`",
        f"- Enforce Gates: `{enforce_gates}`",
        "",
        "## Summary (Primary)",
        "",
        f"- Compared cycles: `{primary['compared_cycles']}`",
        f"- Match cycles: `{primary['match_cycles']}`",
        f"- Divergence cycles: `{primary['divergence_cycles']}`",
        f"- Match ratio: `{primary['match_ratio']:.4f}`",
        f"- Fields: `{','.join(primary['fields'])}`",
        "",
        "## Summary (Strict)",
        "",
        f"- Compared cycles: `{strict['compared_cycles']}`",
        f"- Match cycles: `{strict['match_cycles']}`",
        f"- Divergence cycles: `{strict['divergence_cycles']}`",
        f"- Match ratio: `{strict['match_ratio']:.4f}`",
        f"- Gate (`{strict_gate:.2f}`): `{'PASS' if strict_pass else 'FAIL'}`",
        "",
        "## Summary (Intent)",
        "",
        f"- Compared cycles: `{intent['compared_cycles']}`",
        f"- Match cycles: `{intent['match_cycles']}`",
        f"- Divergence cycles: `{intent['divergence_cycles']}`",
        f"- Match ratio: `{intent['match_ratio']:.4f}`",
        f"- Gate (`{intent_gate:.2f}`): `{'PASS' if intent_pass else 'FAIL'}`",
        "",
        "## Top Divergences (Primary, first 20)",
        "",
    ]
    append_divergence_lines(lines, top_primary_divergences)
    lines.extend(
        [
            "",
            "## Top Divergences (Strict, first 10)",
            "",
        ]
    )
    append_divergence_lines(lines, top_strict_divergences)
    lines.extend(
        [
            "",
            "## Top Divergences (Intent, first 10)",
            "",
        ]
    )
    append_divergence_lines(lines, top_intent_divergences)

    md_report.write_text("\n".join(lines), encoding="utf-8")
    return json_report, md_report


async def main() -> int:
    args = parse_args()
    repo_root = Path(args.repo_root).resolve()
    worker_rust_dir = Path(args.worker_rust_dir).resolve()
    output_dir = Path(args.output_dir).resolve()
    ensure_output_dir(output_dir)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    replay_path: Path
    if args.replay_path:
        replay_path = Path(args.replay_path).resolve()
        ticks = load_replay_ticks(replay_path)
    else:
        replay_path = output_dir / f"shadow_replay_{timestamp}.jsonl"
        ticks = generate_synthetic_replay(
            tenant_id=args.tenant_id,
            exchange=args.exchange,
            product_id=args.product_id,
            count=args.generate_replay_count,
        )
        write_replay_jsonl(replay_path, ticks)

    if not ticks:
        raise RuntimeError("Replay stream is empty.")

    strategy_row = await fetch_strategy_snapshot(
        db_dsn=args.db_dsn,
        tenant_id=args.tenant_id,
        exchange=args.exchange,
        product_id=args.product_id,
    )

    rust_events, rust_stderr, rust_code = run_rust_replay(
        worker_rust_dir=worker_rust_dir,
        replay_path=replay_path,
        db_dsn=args.db_dsn,
        redis_url=args.redis_url,
        tenant_id=args.tenant_id,
        exchange=args.exchange,
        product_id=args.product_id,
        timeout_seconds=args.timeout_seconds,
    )
    if rust_code != 0:
        print("Rust replay run failed.")
        if rust_stderr:
            print(rust_stderr)
        return 1

    rust_cycles = summarize_rust_cycles(rust_events)
    python_cycles = summarize_python_cycles(strategy_row, ticks, args.python_profile)
    diff_bundle = diff_summaries(python_cycles, rust_cycles, args.mode)

    json_report, md_report = write_reports(
        output_dir=output_dir,
        timestamp=timestamp,
        replay_path=replay_path,
        strategy_row=strategy_row,
        rust_return_code=rust_code,
        rust_stderr=rust_stderr,
        python_cycles=python_cycles,
        rust_cycles=rust_cycles,
        diff_bundle=diff_bundle,
        strict_gate=args.strict_gate,
        intent_gate=args.intent_gate,
        gate_scope=args.gate_scope,
        enforce_gates=args.enforce_gates,
        python_profile=args.python_profile,
    )

    strict = diff_bundle["strict"]
    intent = diff_bundle["intent"]
    primary = diff_bundle["primary"]

    print("Shadow diff replay completed.")
    print(f"Replay: {replay_path}")
    print(f"Python profile: {args.python_profile}")
    print(f"JSON report: {json_report}")
    print(f"Markdown report: {md_report}")
    print(
        "Primary summary ({mode}): compared={compared} match={match} divergence={div} ratio={ratio:.4f}".format(
            mode=args.mode,
            compared=primary["compared_cycles"],
            match=primary["match_cycles"],
            div=primary["divergence_cycles"],
            ratio=primary["match_ratio"],
        )
    )
    print(
        "Strict summary: compared={compared} match={match} divergence={div} ratio={ratio:.4f} gate={gate:.2f} ({status})".format(
            compared=strict["compared_cycles"],
            match=strict["match_cycles"],
            div=strict["divergence_cycles"],
            ratio=strict["match_ratio"],
            gate=args.strict_gate,
            status="PASS" if strict["match_ratio"] >= args.strict_gate else "FAIL",
        )
    )
    print(
        "Intent summary: compared={compared} match={match} divergence={div} ratio={ratio:.4f} gate={gate:.2f} ({status})".format(
            compared=intent["compared_cycles"],
            match=intent["match_cycles"],
            div=intent["divergence_cycles"],
            ratio=intent["match_ratio"],
            gate=args.intent_gate,
            status="PASS" if intent["match_ratio"] >= args.intent_gate else "FAIL",
        )
    )

    strict_pass = strict["match_ratio"] >= args.strict_gate
    intent_pass = intent["match_ratio"] >= args.intent_gate
    if args.enforce_gates:
        if args.gate_scope == "strict":
            gate_ok = strict_pass
        elif args.gate_scope == "intent":
            gate_ok = intent_pass
        else:
            gate_ok = strict_pass and intent_pass
        if not gate_ok:
            print(
                "Gate enforcement failed: scope={scope} strict_pass={strict_pass} intent_pass={intent_pass}".format(
                    scope=args.gate_scope,
                    strict_pass=strict_pass,
                    intent_pass=intent_pass,
                )
            )
            return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
