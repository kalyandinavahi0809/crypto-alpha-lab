import numpy as np
import pandas as pd
import pytest

from qdp.grid import (align_exact, expected_grid, find_gaps, map_events_to_bars,
                      resample_ohlc)

H = 3_600_000


def test_find_gaps_reports_missing_runs():
    ts = np.array([0, H, 2 * H, 5 * H, 6 * H], dtype=np.int64)
    g = find_gaps(ts, "1h")
    assert g.to_dict("records") == [{"gap_start": 3 * H, "gap_end": 4 * H, "missing_bars": 2}]
    with pytest.raises(ValueError):
        find_gaps(np.array([0, H + 5]), "1h")


def test_align_exact_never_fills():
    left = pd.DataFrame({"open_time": [0, H, 2 * H], "close": [1.0, 2.0, 3.0]})
    right = pd.DataFrame({"open_time": [0, 2 * H], "mark": [1.1, 3.1]})
    j, missing = align_exact(left, right, suffix="")
    assert np.isnan(j.loc[1, "mark"])
    assert missing["open_time"].tolist() == [H]


def test_resample_drops_incomplete_buckets():
    ts = expected_grid(0, 7 * H, "1h")
    df = pd.DataFrame({"open_time": ts, "open": np.arange(8.0), "high": np.arange(8.0) + 1,
                       "low": np.arange(8.0) - 1, "close": np.arange(8.0) + .5, "volume": 1.0})
    df = df.drop(index=6)   # bucket 2 (4h..8h) is incomplete
    out = resample_ohlc(df, "1h", "4h")
    assert out["open_time"].tolist() == [0]
    r = out.iloc[0]
    assert (r.open, r.high, r.low, r.close, r.volume) == (0.0, 4.0, -1.0, 3.5, 4.0)


def test_map_events_to_bars():
    bars = np.array([0, 4 * H, 8 * H])
    ev = np.array([0, 1, 4 * H, 5 * H, 9 * H])
    assert map_events_to_bars(bars, ev).tolist() == [0, 1, 1, 2, 3]
