import numpy as np
import pandas as pd
import pytest

from qdp.instruments import InstrumentMaster, make_instrument_id


@pytest.fixture
def master():
    return InstrumentMaster.from_yaml()


def test_perps_have_own_ids_distinct_from_spot(master):
    perp = master.find("binance_usdm", "BTCUSDT", "perp")
    spot = master.find("binance_spot", "BTCUSDT", "spot")
    assert perp.instrument_id == "binance_usdm.BTCUSDT.perp"
    assert spot.instrument_id != perp.instrument_id
    assert perp.is_perp and not spot.is_perp
    for sym in ("BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "DOGEUSDT"):
        p = master.find("binance_usdm", sym, "perp")
        assert p.tick_size > 0 and p.multiplier == 1.0
        assert 0 < p.maker_fee < p.taker_fee
        assert p.mmr_brackets


def test_effective_dated_versions_and_change_log(master):
    iid = make_instrument_id("binance_usdm", "BTCUSDT", "perp")
    master.add_version(iid, "2021-03-01", reason="tick", tick_size=1.0)
    master.add_version(iid, "2022-01-01", reason="funding", funding_interval_hours=4.0)
    assert master.get(iid, "2021-02-28").tick_size == pytest.approx(0.10)
    assert master.get(iid, "2021-03-01").tick_size == pytest.approx(1.0)
    # later versions inherit earlier changes; field changes only from their own start
    v = master.get(iid, "2023-01-01")
    assert v.tick_size == pytest.approx(1.0) and v.funding_interval_hours == 4.0
    assert master.get(iid, "2021-06-01").funding_interval_hours is None
    assert len(master.change_log) == 2
    ts = pd.to_datetime(["2020-01-01", "2021-03-01", "2024-01-01"], utc=True)
    np.testing.assert_allclose(master.field_series(iid, "tick_size", ts), [0.10, 1.0, 1.0])
    # round trip through the CLEANED-layer frame representation
    m2 = InstrumentMaster.from_frame(master.to_frame())
    assert m2.get(iid, "2023-06-01").funding_interval_hours == 4.0


def test_maintenance_brackets_continuous(master):
    p = master.find("binance_usdm", "BTCUSDT", "perp")
    floors = [f for f, _ in p.mmr_brackets][1:]
    for f in floors:
        lo_r, lo_c = p.maintenance(f - 1e-6)
        hi_r, hi_c = p.maintenance(f)
        # maintenance margin = notional*rate - cum is continuous at each bracket floor
        assert (f * lo_r - lo_c) == pytest.approx(f * hi_r - hi_c, rel=1e-9)
    assert p.maintenance(1000.0) == (0.004, 0.0)
