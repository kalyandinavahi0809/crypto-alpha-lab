"""Isolated-margin math (Binance USD-M one-way mode, single position).

Liquidation price with isolated wallet balance WB, quantity Q (> 0), entry price EP,
maintenance rate MMR and maintenance amount cum:
    long : LP = (Q*EP - WB - cum) / (Q * (1 - MMR))
    short: LP = (Q*EP + WB + cum) / (Q * (1 + MMR))
"""
from __future__ import annotations

import numpy as np
from numba import njit


@njit(cache=True)
def bracket(notional, floors, rates):
    """(mmr, cum) for a notional given bracket floors/rates (cum derived for continuity)."""
    cum = 0.0
    rate = rates[0]
    cum_at = 0.0
    for i in range(len(floors)):
        if i > 0:
            cum += floors[i] * (rates[i] - rates[i - 1])
        if notional >= floors[i]:
            rate = rates[i]
            cum_at = cum
    return rate, cum_at


@njit(cache=True)
def liquidation_price(side, qty, entry, wallet, mmr, cum):
    if side == 1:
        return (qty * entry - wallet - cum) / (qty * (1.0 - mmr))
    return (qty * entry + wallet + cum) / (qty * (1.0 + mmr))


@njit(cache=True)
def margin_for_liq_at(side, qty, entry, liq, mmr, cum):
    """Isolated margin that puts the liquidation price exactly at ``liq``."""
    if side == 1:
        return qty * entry - cum - liq * qty * (1.0 - mmr)
    return liq * qty * (1.0 + mmr) - qty * entry - cum


def brackets_arrays(mmr_brackets) -> tuple[np.ndarray, np.ndarray]:
    floors = np.array([float(f) for f, _ in mmr_brackets])
    rates = np.array([float(r) for _, r in mmr_brackets])
    return floors, rates
