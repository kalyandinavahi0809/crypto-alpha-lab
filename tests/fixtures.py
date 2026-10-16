"""Deterministic test fixtures (unit tests only - never used for research results)."""
import numpy as np
import pandas as pd


def pivot_path(pivots, per_leg=3, wick=0.1):
    legs = per_leg if isinstance(per_leg, (list, tuple)) else [per_leg] * (len(pivots) - 1)
    closes = [pivots[0]]
    for a, b, m in zip(pivots[:-1], pivots[1:], legs):
        closes += list(np.linspace(a, b, m + 1)[1:])
    c = np.array(closes, float)
    o = np.r_[c[0], c[:-1]]
    h = np.maximum(o, c) + wick
    l = np.minimum(o, c) - wick
    return pd.DataFrame({"open": o, "high": h, "low": l, "close": c})


def random_walk_bars(n=1500, seed=7, start=100.0):
    rng = np.random.default_rng(seed)
    r = rng.standard_t(4, n) * 0.006
    c = start * np.exp(np.cumsum(r))
    o = np.r_[start, c[:-1]] * np.exp(rng.normal(0, 0.001, n))
    rng_hl = np.abs(rng.normal(0, 0.004, (2, n)))
    h = np.maximum(o, c) * (1 + rng_hl[0])
    l = np.minimum(o, c) * (1 - rng_hl[1])
    return pd.DataFrame({"open": o, "high": h, "low": l, "close": c})
