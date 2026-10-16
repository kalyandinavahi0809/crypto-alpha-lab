import numpy as np

from fixtures import random_walk_bars
from qdp.features.build import build_structure_features, causality_audit, structure_frame
from qdp.features.structure import StructureParams
from qdp.instruments import InstrumentMaster
from qdp.layers import Layer


def _perp_bars(n=1500):
    b = random_walk_bars(n, seed=21)
    b.insert(0, "open_time", 1_600_000_000_000 // 3_600_000 * 3_600_000 +
             np.arange(n, dtype=np.int64) * 3_600_000)
    return b


def test_build_writes_features_partition(store):
    bars = _perp_bars()
    store.write_partition(Layer.CLEANED, "perp_bars",
                          {"venue": "binance_usdm", "symbol": "BTCUSDT", "interval": "1h"}, bars)
    m = InstrumentMaster.from_yaml()
    f = build_structure_features(store, m, "binance_usdm", "BTCUSDT", "1h", StructureParams(k=3))
    back = store.read_partition(Layer.FEATURES, "structure",
                                {"venue": "binance_usdm", "symbol": "BTCUSDT", "interval": "1h",
                                 "k": 3, "bos": "close"})
    assert len(back) == len(bars) and "L_arm_sw" in back and "S_cf_sw" in back
    assert (f["open_time"].to_numpy() == bars["open_time"].to_numpy()).all()


def test_causality_audit_report_flags_leaky_only():
    bars = _perp_bars(2500)
    tick = np.full(len(bars), 1e-4)
    ok = causality_audit(bars, tick, StructureParams(k=3), n_bars=2000, n_cuts=5)
    bad = causality_audit(bars, tick, StructureParams(k=3, leaky=True), n_bars=2000, n_cuts=5)
    assert ok["passed"] and not bad["passed"] and "future" in bad["error"]


def test_structure_frame_long_and_short_columns():
    bars = _perp_bars(600)
    f = structure_frame(bars, np.full(600, 1e-4), StructureParams(k=2))
    assert {"L_trend", "S_trend", "L_arm_px", "S_arm_px", "atr"} <= set(f.columns)
    both = (f["L_trend"] == 1) & (f["S_trend"] == -1)
    assert not both.any()        # HH+HL and LH+LL cannot hold at the same time
