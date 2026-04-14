from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from domain.enums import RegimeName
from domain.models import MarketSnapshot, RegimeState


class RegimeEngine:
    def __init__(
        self,
        *,
        stress_spread_bps: Decimal = Decimal("35"),
        trend_slope_threshold: Decimal = Decimal("0.0005"),
        mr_distance_threshold_bps: Decimal = Decimal("18"),
        hysteresis_bps: Decimal = Decimal("4"),
        rsi_bear_threshold: Decimal = Decimal("42"),
        rsi_bull_threshold: Decimal = Decimal("58"),
    ) -> None:
        self.stress_spread_bps = stress_spread_bps
        self.trend_slope_threshold = trend_slope_threshold
        self.mr_distance_threshold_bps = mr_distance_threshold_bps
        self.hysteresis_bps = hysteresis_bps
        self.rsi_bear_threshold = rsi_bear_threshold
        self.rsi_bull_threshold = rsi_bull_threshold

    def evaluate(
        self,
        market: MarketSnapshot,
        previous_state: RegimeState | None = None,
    ) -> RegimeState:
        reason_codes: list[str] = []
        ema_slope = Decimal("0")
        if market.short_ema != 0:
            ema_slope = (market.mid - market.short_ema) / market.short_ema

        vwap_distance_bps = Decimal("0")
        if market.short_vwap != 0:
            vwap_distance_bps = ((market.mid - market.short_vwap) / market.short_vwap) * Decimal("10000")

        long_ema_bear = market.long_ema > 0 and market.mid < market.long_ema
        rsi_bear = market.rsi < self.rsi_bear_threshold
        rsi_bull = market.rsi > self.rsi_bull_threshold

        regime = RegimeName.MR_GOOD
        confidence = Decimal("0.55")

        if market.spread_bps >= self.stress_spread_bps or market.spread_zscore >= Decimal("3"):
            regime = RegimeName.STRESS
            confidence = Decimal("0.95")
            reason_codes.append("spread_stress")
        elif ema_slope >= self.trend_slope_threshold and rsi_bull:
            regime = RegimeName.TREND_UP
            confidence = Decimal("0.80")
            reason_codes.append("ema_trend_up")
        elif ema_slope <= (self.trend_slope_threshold * Decimal("-1")):
            regime = RegimeName.TREND_DOWN
            confidence = Decimal("0.85") if (rsi_bear or long_ema_bear) else Decimal("0.75")
            reason_codes.append("ema_trend_down")
            if rsi_bear:
                reason_codes.append("rsi_bear")
            if long_ema_bear:
                reason_codes.append("long_ema_bear")
        elif long_ema_bear and rsi_bear:
            regime = RegimeName.TREND_DOWN
            confidence = Decimal("0.80")
            reason_codes.append("rsi_bear")
            reason_codes.append("long_ema_bear")
        elif abs(vwap_distance_bps) >= self.mr_distance_threshold_bps:
            regime = RegimeName.MR_WEAK
            confidence = Decimal("0.65")
            reason_codes.append("far_from_vwap")
        else:
            regime = RegimeName.MR_GOOD
            confidence = Decimal("0.70")
            reason_codes.append("range_stable")

        if previous_state is not None and previous_state.regime != regime:
            if abs(vwap_distance_bps) < (self.mr_distance_threshold_bps + self.hysteresis_bps):
                if regime in {RegimeName.MR_GOOD, RegimeName.MR_WEAK} and previous_state.regime in {RegimeName.MR_GOOD, RegimeName.MR_WEAK}:
                    regime = previous_state.regime
                    reason_codes.append("mr_hysteresis_hold")

        return RegimeState(
            product_id=market.product_id,
            regime=regime,
            confidence=confidence,
            ema_slope=ema_slope,
            vwap_distance_bps=vwap_distance_bps,
            spread_zscore=market.spread_zscore,
            order_book_imbalance=market.top_book_imbalance,
            flow_bias=market.flow_bias,
            hysteresis_anchor=vwap_distance_bps,
            updated_at=datetime.now(UTC),
            reason_codes=reason_codes,
        )
