from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol

from domain.enums import RegimeName
from domain.models import GridLevel, GridState, MarketSnapshot, RegimeState, RiskState
from risk.engine import RiskDecision, RiskEngine
from strategy.neutral_grid.engine import GridDecision, NeutralGridEngine
from strategy.regime.engine import RegimeEngine


class MarketDataProvider(Protocol):
    async def get_snapshot(self, product_id: str) -> MarketSnapshot | None: ...


class ExecutionProvider(Protocol):
    async def place_level(self, level: GridLevel) -> tuple[str, str | None] | None: ...

    async def cancel_order(self, product_id: str, client_order_id: str) -> None: ...


@dataclass(slots=True)
class KernelTickResult:
    regime_state: RegimeState
    risk_state: RiskState | None
    risk_decision: RiskDecision | None
    grid_decision: GridDecision
    stress_paused_until: datetime | None = None


class TradingKernel:
    """
    Unified strategy kernel used by paper/live/test flows.
    Inputs are market snapshots + current state; outputs are deterministic decisions.
    """

    def __init__(
        self,
        *,
        grid_engine: NeutralGridEngine,
        regime_engine: RegimeEngine,
        risk_engine: RiskEngine,
    ) -> None:
        self.grid_engine = grid_engine
        self.regime_engine = regime_engine
        self.risk_engine = risk_engine

    def evaluate_tick(
        self,
        *,
        product_id: str,
        grid_state: GridState,
        market: MarketSnapshot,
        previous_regime: RegimeState | None,
        previous_risk: RiskState | None,
        stress_pause_seconds: int,
        now: datetime | None = None,
    ) -> KernelTickResult:
        now = now or datetime.now(UTC)
        regime_state = self.regime_engine.evaluate(market, previous_regime)

        # Preserve legacy behavior: in STRESS we pause immediately before risk/grid checks.
        if regime_state.regime == RegimeName.STRESS:
            return KernelTickResult(
                regime_state=regime_state,
                risk_state=previous_risk,
                risk_decision=None,
                grid_decision=GridDecision(),
                stress_paused_until=datetime.fromtimestamp(
                    now.timestamp() + stress_pause_seconds, tz=UTC
                ),
            )

        risk_state, risk_decision = self.risk_engine.evaluate(
            product_id=product_id,
            grid_state=grid_state,
            market=market,
            regime=regime_state.regime,
            previous_state=previous_risk,
        )
        grid_decision = self.grid_engine.evaluate(
            state=grid_state,
            market=market,
            regime=regime_state.regime.value,
        )
        return KernelTickResult(
            regime_state=regime_state,
            risk_state=risk_state,
            risk_decision=risk_decision,
            grid_decision=grid_decision,
            stress_paused_until=None,
        )
