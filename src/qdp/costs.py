"""Execution cost model: per-instrument maker/taker fees and bps slippage.

Fees come from the instrument master (never hard-coded per strategy). Slippage applies only
to fills that cross the spread (market orders, stop-market exits, liquidations); resting
limit orders fill at their limit price with no slippage.
"""
from __future__ import annotations

from dataclasses import dataclass

from .instruments import Instrument

MAKER, TAKER = "maker", "taker"


@dataclass(frozen=True)
class CostModel:
    maker_fee: float
    taker_fee: float
    slippage_bps: float = 2.0

    @classmethod
    def for_instrument(cls, inst: Instrument, slippage_bps: float = 2.0) -> "CostModel":
        return cls(maker_fee=inst.maker_fee, taker_fee=inst.taker_fee, slippage_bps=slippage_bps)

    def fee_rate(self, liquidity: str) -> float:
        if liquidity == MAKER:
            return self.maker_fee
        if liquidity == TAKER:
            return self.taker_fee
        raise ValueError(liquidity)

    def fee(self, notional: float, liquidity: str) -> float:
        return abs(notional) * self.fee_rate(liquidity)

    def slipped_price(self, ref_price: float, side: int, is_buy: bool) -> float:
        """Adverse slippage for a taker fill. ``side`` is unused but kept for clarity of call
        sites; direction is determined by whether the fill buys or sells."""
        s = self.slippage_bps * 1e-4
        return ref_price * (1 + s) if is_buy else ref_price * (1 - s)

    def stressed(self, mult: float) -> "CostModel":
        return CostModel(self.maker_fee, self.taker_fee, self.slippage_bps * mult)
