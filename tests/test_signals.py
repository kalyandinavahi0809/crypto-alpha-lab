import numpy as np

from fixtures import pivot_path, random_walk_bars
from qdp.features.atr import wilder_atr
from qdp.features.structure import StructureParams
from qdp.signals.trend_pullback import (compute_structure, random_intents, strategy_intents,
                                        trend_pullback_intents, trend_regime)


def _struct(bars, k=3):
    atr = wilder_atr(bars.high, bars.low, bars.close)
    return atr, compute_structure(bars, atr, np.full(len(bars), 1e-4), StructureParams(k=k))


def test_confirm_signal_executes_next_open():
    bars = random_walk_bars(3000, seed=11)
    _, st = _struct(bars)
    oi = strategy_intents(st, "confirm", sweep=False)
    cf = st[1]["cf_ns"].to_numpy()
    assert cf.any()
    assert (oi.mkt_long[1:] == cf[:-1]).all() and not oi.mkt_long[0]
    t = int(np.nonzero(oi.mkt_long)[0][0])
    assert oi.mkt_long_stop[t] == st[1]["cf_stop"].to_numpy()[t - 1]


def test_limit_orders_only_when_armed_and_sweep_is_subset():
    bars = random_walk_bars(3000, seed=12)
    _, st = _struct(bars)
    sw = strategy_intents(st, "limit", sweep=True)
    ns = strategy_intents(st, "limit", sweep=False)
    has_sw = ~np.isnan(sw.lim_long_px)
    has_ns = ~np.isnan(ns.lim_long_px)
    assert has_ns.any() and (has_sw <= has_ns).all()
    assert (sw.lim_long_stop[has_sw] < sw.lim_long_px[has_sw]).all()
    assert (sw.lim_short_stop[~np.isnan(sw.lim_short_px)] >
            sw.lim_short_px[~np.isnan(sw.lim_short_px)]).all()


def test_random_entries_respect_regime_and_side():
    bars = random_walk_bars(3000, seed=13)
    _, st = _struct(bars)
    tr = trend_regime(st)
    oi = random_intents(tr, 1, np.array([0.01, 0.02]), 50, np.random.default_rng(0))
    idx = np.nonzero(oi.mkt_long)[0]
    assert len(idx) == 50 and (tr[idx - 1] == 1).all() and not oi.mkt_short.any()
    assert set(np.round(oi.mkt_stopfrac[idx], 4)) <= {0.01, 0.02}


def test_trend_pullback_baseline_signals_in_trend_only():
    bars = random_walk_bars(3000, seed=14)
    atr, st = _struct(bars)
    tr = trend_regime(st)
    oi = trend_pullback_intents(bars, atr, tr, pullback_atr=1.0, stop_atr=1.5)
    idx = np.nonzero(oi.mkt_long)[0]
    assert len(idx) and (tr[idx - 1] == 1).all()
    np.testing.assert_allclose(oi.mkt_long_stop[idx],
                               bars.close.to_numpy()[idx - 1] - 1.5 * atr[idx - 1])
