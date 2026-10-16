import numpy as np
import pandas as pd

from qdp.adapters.base import FetchResult
from qdp.data_audit import coverage, funding_consistency, mark_history_probe, resample_consistency
from qdp.layers import Layer

H = 3_600_000
T0 = 1_704_067_200_000
V, S = "binance_usdm", "BTCUSDT"


def _bars(n, step, seed=0):
    rng = np.random.default_rng(seed)
    c = 100 + np.cumsum(rng.normal(0, 1, n))
    o = np.r_[100, c[:-1]]
    return pd.DataFrame({"open_time": T0 + np.arange(n, dtype=np.int64) * step, "open": o,
                         "high": np.maximum(o, c) + 1, "low": np.minimum(o, c) - 1, "close": c,
                         "volume": 1.0})


def _store_bars(store):
    one = _bars(48, H)
    g = one.assign(b=one.open_time // (4 * H)).groupby("b")
    four = pd.DataFrame({"open_time": g.open_time.first(), "open": g.open.first(), "high": g.high.max(),
                         "low": g.low.min(), "close": g.close.last(), "volume": g.volume.sum()}).reset_index(drop=True)
    four.loc[3, "high"] += 5.0                     # one inconsistent 4h bar
    for ds in ("klines", "mark_klines"):
        store.write_partition(Layer.CLEANED, ds, {"venue": V, "symbol": S, "interval": "1h"}, one)
        store.write_partition(Layer.CLEANED, ds, {"venue": V, "symbol": S, "interval": "4h"}, four)
        for iv, verified in (("1h", True), ("4h", False)):
            store.write_partition(Layer.RAW, ds, {"venue": V, "symbol": S, "interval": iv, "month": "2024-01"},
                                  one, meta={"checksum": "x", "verified": verified, "rows": 1})
    f = pd.DataFrame({"funding_time": T0 + np.array([0, 8, 16, 20, 24]) * H, "funding_rate": 1e-4,
                      "funding_interval_hours": [8, 8, 8, 4, np.nan],
                      "observed_spacing_h": [np.nan, 8, 8, 4, 4]})
    store.write_partition(Layer.CLEANED, "funding", {"venue": V, "symbol": S, "interval": "na"}, f)
    store.write_partition(Layer.RAW, "funding", {"venue": V, "symbol": S, "interval": "na", "month": "2024-01"},
                          f, meta={"checksum": "x", "verified": False, "rows": 5})


def test_coverage_counts_rows_and_sources(store):
    _store_bars(store)
    cov = coverage(store, V, [S], ["1h", "4h"]).set_index(["dataset", "interval"])
    assert cov.loc[("klines", "1h"), "rows"] == 48 and cov.loc[("klines", "4h"), "rows"] == 12
    assert cov.loc[("klines", "1h"), "months_archive_verified"] == 1
    assert cov.loc[("klines", "4h"), "months_rest"] == 1
    assert cov.loc[("funding", "events"), "rows"] == 5


def test_resample_consistency_flags_the_inconsistent_bar(store):
    _store_bars(store)
    out = {r["dataset"]: r for r in resample_consistency(store, V, S)}
    assert out["klines"]["compared"] == 12 and out["klines"]["mismatches"] == 1
    assert out["klines"]["first_mismatch"] == pd.Timestamp(T0 + 12 * H, unit="ms", tz="UTC")


def test_funding_consistency_and_mark_probe(store):
    _store_bars(store)
    fc = funding_consistency(store, V, S)
    assert fc["events"] == 5 and fc["without_reported_interval"] == 1
    assert fc["reported_vs_observed_disagree"] == 0          # 8h->4h change is consistent

    class Ad:
        def fetch_month(self, ds, sym, month, iv):
            fr = pd.DataFrame({"open_time": [T0]}) if month == "2019-12" else pd.DataFrame({"open_time": []})
            return FetchResult(fr, "c", "rest", False)
    pr = mark_history_probe(Ad(), S, ["2019-11", "2019-12"])
    assert [p["rows_returned"] for p in pr] == [0, 1]
