import numpy as np
import pandas as pd
import pytest

from fixtures import pivot_path, random_walk_bars
from qdp.causal import CausalityError, assert_causal
from qdp.features.atr import wilder_atr
from qdp.features.structure import StructureParams, structure_features
from qdp.features.swings import confirmed_swings


def test_atr_wilder():
    h = np.array([2.0, 3, 4, 5]); l = np.array([1.0, 2, 3, 4]); c = np.array([1.5, 2.5, 3.5, 4.5])
    a = wilder_atr(h, l, c, n=2)
    assert np.isnan(a[0])
    assert a[1] == pytest.approx((1 + 1.5) / 2)
    assert a[2] == pytest.approx(a[1] + (1.5 - a[1]) / 2)


def test_swing_known_only_after_k_bars():
    high = np.array([1, 2, 3, 9, 3, 2, 1, 1, 1], float)
    low = high - 0.5
    sw = confirmed_swings(high, low, k=2)
    assert np.nonzero(~np.isnan(sw["sh_level"].to_numpy()))[0].tolist() == [5]   # bar 3 + k
    assert sw.loc[5, "sh_bar"] == 3 and sw.loc[5, "sh_level"] == 9
    leaky = confirmed_swings(high, low, k=2, leaky=True)
    assert np.nonzero(~np.isnan(leaky["sh_level"].to_numpy()))[0].tolist() == [3]


def test_swing_tie_rule_first_of_equal_highs():
    high = np.array([1, 2, 5, 5, 2, 1, 1], float)
    sw = confirmed_swings(high, high - 1, k=2)
    assert sw["sh_bar"].dropna().tolist() == [2]


CRAFT = [100, 110, 104, 114, 107, 118, 122, 115, 118, 106, 125]


def _craft(per_leg=3, **kw):
    bars = pivot_path(CRAFT, per_leg=per_leg)
    atr = np.full(len(bars), 1.0)
    p = StructureParams(k=2, **kw)
    return bars, structure_features(bars.open, bars.high, bars.low, bars.close, atr, 0.01, p)


def test_zone_bos_and_sweep_lifecycle():
    # fast final drop: the bounce high (bar 24) is not yet confirmed when price hits the zone
    bars, f = _craft(per_leg=[3] * 8 + [2, 3])
    bos_bars = np.nonzero(f["bos"].to_numpy())[0].tolist()
    assert 14 in bos_bars                       # close above the swing high at 114
    z = f.loc[14]
    # last down-close candle before the impulse (bar 12: open 109.33, close 107)
    assert z.zone_top == pytest.approx(109 + 1 / 3) and z.zone_bot == pytest.approx(106.9)
    assert f.loc[15, "trend"] == 1
    # no-sweep limit arms right after the BOS; sweep variant waits for a qualifying swing low
    assert f.loc[15, "arm_ns"] and not f.loc[15, "arm_sw"]
    first_sw = int(np.argmax(f["arm_sw"].to_numpy()))
    assert first_sw == 24                       # swing low 114.9 (bar 21) confirmed at close 23
    assert f.loc[first_sw, "arm_px"] == pytest.approx(z.zone_top)
    assert f.loc[first_sw, "arm_stop"] == pytest.approx(106.9 - 0.1)
    assert not f.loc[24, "swept"] and f.loc[25, "swept"]   # bar 25 trades below 114.9
    touch = int(np.nonzero(bars.low.to_numpy()[15:] <= z.zone_top)[0][0]) + 15
    assert touch == 26 and f.loc[touch, "arm_sw"]          # order resting during the touch bar
    assert not f.loc[touch + 1:, "arm_ns"].any()           # mitigated -> no more orders
    # the touch bar closes below the zone bottom -> invalidated, no confirmation entry
    assert bars.close[touch] < z.zone_bot and not f.loc[touch, "cf_sw"]


def test_confirmed_lower_high_in_pullback_removes_trend():
    # slow drop: the bounce makes a lower high (118.1 < 122) that is confirmed before the touch,
    # so HH+HL no longer holds and the order is pulled - the trend rule is applied literally
    bars, f = _craft()
    assert f.loc[26, "trend"] == 0
    assert not f.loc[27, "arm_sw"] and not f.loc[27, "arm_ns"]


def test_bos_close_rule_vs_wick():
    # a wick through the swing high without a close beyond it is not a BOS under the close rule
    piv = [100, 110, 104, 114, 107, 113.8, 108, 120]
    bars = pivot_path(piv)
    i = int(np.argmax(np.isclose(bars.close, 113.8)))
    bars.loc[i, "high"] = 114.5                 # wick above 114 (the swing high)
    atr = np.ones(len(bars))
    fc = structure_features(bars.open, bars.high, bars.low, bars.close, atr, .01, StructureParams(k=2))
    fw = structure_features(bars.open, bars.high, bars.low, bars.close, atr, .01,
                            StructureParams(k=2, bos_close=False))
    assert not fc.loc[i, "bos"] and fw.loc[i, "bos"]


def test_short_side_is_exact_mirror():
    bars = random_walk_bars(800)
    atr = wilder_atr(bars.high, bars.low, bars.close)
    p = StructureParams(k=3)
    short = structure_features(bars.open, bars.high, bars.low, bars.close, atr, 1e-4, p, side=-1)
    m = pd.DataFrame({"open": -bars.open, "high": -bars.low, "low": -bars.high, "close": -bars.close})
    long_m = structure_features(m.open, m.high, m.low, m.close, atr, 1e-4, p, side=1)
    np.testing.assert_allclose(short["arm_px"], -long_m["arm_px"])
    np.testing.assert_allclose(short["zone_top"], -long_m["zone_bot"])
    assert (short["trend"] == -long_m["trend"]).all()
    assert (short["cf_sw"] == long_m["cf_sw"]).all()


def _structure_fn(params, side):
    def fn(df):
        atr = wilder_atr(df.high, df.low, df.close)
        out = structure_features(df.open, df.high, df.low, df.close, atr, 1e-4, params, side)
        return out.drop(columns=["zone_id", "arm_zid", "cf_zid"])
    return fn


@pytest.mark.parametrize("k", [2, 3, 5])
@pytest.mark.parametrize("bos_close", [True, False])
@pytest.mark.parametrize("side", [1, -1])
def test_zone_and_sweep_features_are_causal(k, bos_close, side):
    bars = random_walk_bars(1200, seed=k)
    rep = assert_causal(_structure_fn(StructureParams(k=k, bos_close=bos_close), side), bars,
                        n_cuts=15)
    assert rep["cutpoints"] >= 10


def test_meta_leaky_swings_fail_causality():
    """Meta-test: a swing definition that does not wait for confirmation must be caught."""
    bars = random_walk_bars(1200, seed=3)
    with pytest.raises(CausalityError):
        assert_causal(lambda d: confirmed_swings(d.high, d.low, 3, leaky=True), bars)
    with pytest.raises(CausalityError):
        assert_causal(_structure_fn(StructureParams(k=3, leaky=True), 1), bars)
    # the confirmed definition passes the very same check
    assert_causal(lambda d: confirmed_swings(d.high, d.low, 3), bars)


def test_assert_causal_catches_simple_lookahead():
    bars = random_walk_bars(300)
    with pytest.raises(CausalityError):
        assert_causal(lambda d: pd.DataFrame({"x": d.close.shift(-1)}), bars)
    assert_causal(lambda d: pd.DataFrame({"x": d.close.rolling(5).mean()}), bars)
