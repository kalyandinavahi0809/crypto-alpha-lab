"""Order intents for the Trend Pullback + Liquidity Sweep strategy and its baselines.

All arrays are aligned to the instrument's own bar grid (index t = bar t):
  lim_*_px / lim_*_stop  limit order resting DURING bar t (NaN = no order)
  mkt_*  / mkt_*_stop    market order executed at the OPEN of bar t (decided at close t-1)
  mkt_*_stopfrac         alternative stop given as a fraction of the fill price (baselines)
  trail_*_lv / _cb       latest confirmed opposite swing (state after close t) and the bar at
                         which it was confirmed, used by the swing-trail exit
"""
from __future__ import annotations

from dataclasses import dataclass, field

import numpy as np
import pandas as pd

from ..features.structure import StructureParams, structure_features


@dataclass
class OrderIntents:
    n: int
    lim_long_px: np.ndarray = None
    lim_long_stop: np.ndarray = None
    lim_short_px: np.ndarray = None
    lim_short_stop: np.ndarray = None
    mkt_long: np.ndarray = None
    mkt_long_stop: np.ndarray = None
    mkt_short: np.ndarray = None
    mkt_short_stop: np.ndarray = None
    mkt_stopfrac: np.ndarray = None
    trail_long_lv: np.ndarray = None
    trail_long_cb: np.ndarray = None
    trail_short_lv: np.ndarray = None
    trail_short_cb: np.ndarray = None
    meta: dict = field(default_factory=dict)

    def __post_init__(self):
        nanf = lambda: np.full(self.n, np.nan)
        for name in ("lim_long_px", "lim_long_stop", "lim_short_px", "lim_short_stop",
                     "mkt_long_stop", "mkt_short_stop", "mkt_stopfrac", "trail_long_lv",
                     "trail_short_lv"):
            if getattr(self, name) is None:
                setattr(self, name, nanf())
        for name in ("mkt_long", "mkt_short"):
            if getattr(self, name) is None:
                setattr(self, name, np.zeros(self.n, bool))
        for name in ("trail_long_cb", "trail_short_cb"):
            if getattr(self, name) is None:
                setattr(self, name, np.full(self.n, -1, np.int64))


def compute_structure(bars: pd.DataFrame, atr: np.ndarray, tick: np.ndarray,
                      params: StructureParams) -> dict[int, pd.DataFrame]:
    args = (bars["open"], bars["high"], bars["low"], bars["close"], atr, tick)
    return {1: structure_features(*args, params, side=1),
            -1: structure_features(*args, params, side=-1)}


def _shift_forward(x: np.ndarray, fill) -> np.ndarray:
    out = np.empty_like(x)
    out[0] = fill
    out[1:] = x[:-1]
    return out


def strategy_intents(struct: dict[int, pd.DataFrame], entry: str, sweep: bool) -> OrderIntents:
    """entry: 'limit' (maker limit at the zone edge on first mitigation) or 'confirm'
    (taker at the next open after the touch bar closes back beyond the swept level)."""
    n = len(struct[1])
    oi = OrderIntents(n, meta={"entry": entry, "sweep": sweep})
    for side, s in struct.items():
        tag = "long" if side == 1 else "short"
        if entry == "limit":
            armed = (s["arm_sw"] if sweep else s["arm_ns"]).to_numpy()
            px = np.where(armed, s["arm_px"].to_numpy(), np.nan)
            stop = np.where(armed, s["arm_stop"].to_numpy(), np.nan)
            setattr(oi, f"lim_{tag}_px", px)
            setattr(oi, f"lim_{tag}_stop", stop)
        elif entry == "confirm":
            sig = (s["cf_sw"] if sweep else s["cf_ns"]).to_numpy()
            stop = np.where(sig, s["cf_stop"].to_numpy(), np.nan)
            setattr(oi, f"mkt_{tag}", _shift_forward(sig, False))
            setattr(oi, f"mkt_{tag}_stop", _shift_forward(stop, np.nan))
        else:
            raise ValueError(entry)
        setattr(oi, f"trail_{tag}_lv", s["trail_lv"].to_numpy())
        setattr(oi, f"trail_{tag}_cb", s["trail_cbar"].to_numpy().astype(np.int64))
    return oi


def trend_regime(struct: dict[int, pd.DataFrame]) -> np.ndarray:
    """+1 / -1 / 0 trend state after close t (long machine's HH+HL, short machine's LH+LL)."""
    up = struct[1]["trend"].to_numpy() == 1
    dn = struct[-1]["trend"].to_numpy() == -1
    return np.where(up, 1, np.where(dn, -1, 0)).astype(np.int8)


def trend_pullback_intents(bars: pd.DataFrame, atr: np.ndarray, trend: np.ndarray,
                           pullback_atr: float, stop_atr: float, lookback: int = 20,
                           struct: dict[int, pd.DataFrame] | None = None) -> OrderIntents:
    """Baseline 2: trend filter alone. In an uptrend, once the close is ``pullback_atr`` ATRs
    below the highest high of the last ``lookback`` bars, buy the next open with a stop
    ``stop_atr`` ATRs away (same stop geometry as the strategy's median). One signal per
    pullback: re-armed only after a new ``lookback`` high. Mirror for shorts."""
    h, l, c = (bars[k].to_numpy(float) for k in ("high", "low", "close"))
    n = len(c)
    hh = pd.Series(h).rolling(lookback, min_periods=lookback).max().to_numpy()
    ll = pd.Series(l).rolling(lookback, min_periods=lookback).min().to_numpy()
    sig_l = np.zeros(n, bool)
    sig_s = np.zeros(n, bool)
    armed_l = armed_s = True
    for t in range(n):
        if np.isnan(atr[t]) or np.isnan(hh[t]):
            continue
        if h[t] >= hh[t]:
            armed_l = True
        if l[t] <= ll[t]:
            armed_s = True
        if armed_l and trend[t] == 1 and c[t] <= hh[t] - pullback_atr * atr[t]:
            sig_l[t] = True
            armed_l = False
        if armed_s and trend[t] == -1 and c[t] >= ll[t] + pullback_atr * atr[t]:
            sig_s[t] = True
            armed_s = False
    oi = OrderIntents(n, meta={"baseline": "trend_pullback", "pullback_atr": pullback_atr})
    oi.mkt_long = _shift_forward(sig_l, False)
    oi.mkt_short = _shift_forward(sig_s, False)
    oi.mkt_long_stop = _shift_forward(np.where(sig_l, c - stop_atr * atr, np.nan), np.nan)
    oi.mkt_short_stop = _shift_forward(np.where(sig_s, c + stop_atr * atr, np.nan), np.nan)
    if struct is not None:
        for side, tag in ((1, "long"), (-1, "short")):
            setattr(oi, f"trail_{tag}_lv", struct[side]["trail_lv"].to_numpy())
            setattr(oi, f"trail_{tag}_cb", struct[side]["trail_cbar"].to_numpy().astype(np.int64))
    return oi


def random_intents(trend: np.ndarray, side: int, stop_fracs: np.ndarray, n_entries: int,
                   rng: np.random.Generator, eligible: np.ndarray | None = None) -> OrderIntents:
    """Baseline 1: random market entries at the open of bars whose PRIOR close was in the same
    trend regime / side, with stop distances drawn from the strategy's own stop distances."""
    n = len(trend)
    regime = np.zeros(n, bool)
    regime[1:] = trend[:-1] == side
    if eligible is not None:
        regime &= eligible
    idx = np.nonzero(regime)[0]
    oi = OrderIntents(n, meta={"baseline": "random", "side": side})
    if len(idx) == 0 or n_entries == 0:
        return oi
    pick = rng.choice(idx, size=min(n_entries, len(idx)), replace=False)
    fr = rng.choice(stop_fracs, size=len(pick), replace=True)
    tag = "long" if side == 1 else "short"
    getattr(oi, f"mkt_{tag}")[pick] = True
    oi.mkt_stopfrac[pick] = fr
    return oi
