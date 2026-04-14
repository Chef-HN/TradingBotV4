from __future__ import annotations

from decimal import Decimal


def compute_inventory_skew(
    base_inventory: Decimal,
    mid_price: Decimal,
    total_equity: Decimal,
    max_inventory_ratio: Decimal,
) -> Decimal:
    """
    Returns a skew factor in [-1.0, 1.0].

    Positive skew → we hold too much base → favour asks (sell more, buy less).
    Negative skew → we hold too little base → favour bids (buy more, sell less).
    Zero skew → perfectly balanced.

    The skew is relative to how much of total equity is currently in base.
    """
    if total_equity <= 0 or mid_price <= 0:
        return Decimal("0")

    base_value = base_inventory * mid_price
    current_ratio = base_value / total_equity
    target_ratio = max_inventory_ratio / Decimal("2")  # ideal: half of max
    skew = (current_ratio - target_ratio) / target_ratio
    return max(Decimal("-1"), min(Decimal("1"), skew))


def apply_skew_to_size(
    base_size_quote: Decimal,
    skew: Decimal,
    side: str,
    min_fraction: Decimal = Decimal("0.25"),
) -> Decimal:
    """
    Adjusts order size based on inventory skew.

    For bids: positive skew → reduce size (inventory heavy → buy less)
    For asks: positive skew → increase size (inventory heavy → sell more)

    Sizes are bounded to [min_fraction * base_size, 2 * base_size].
    """
    if side == "bid":
        factor = Decimal("1") - (skew * Decimal("0.5"))
    else:
        factor = Decimal("1") + (skew * Decimal("0.5"))
    factor = max(min_fraction, min(Decimal("2"), factor))
    return (base_size_quote * factor).quantize(Decimal("0.01"))
