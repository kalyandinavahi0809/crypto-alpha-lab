import hashlib

import numpy as np
import pandas as pd
import pytest

from qdp.adapters.base import FetchResult, VenueAdapter
from qdp.clean import (apply_funding_intervals, apply_observed_ticks, clean_klines_frame,
                       infer_monthly_tick, normalize_funding_frame)
from qdp.ingest import ingest_series
from qdp.instruments import InstrumentMaster
from qdp.layers import Layer

H = 3_600_000


class FakeAdapter(VenueAdapter):
    venue, spot_venue = "fake", "fake_spot"

    def __init__(self):
        self.version = {"2024-01": 1, "2024-02": 1}
        self.fetches = 0

    def available_months(self, dataset, symbol, interval=None):
        return sorted(self.version)

    def _frame(self, month):
        base = int(pd.Timestamp(f"{month}-01", tz="UTC").timestamp() * 1000)
        v = self.version[month]
        return pd.DataFrame({"open_time": [base, base + H], "open": [1.0 * v] * 2,
                             "high": [2.0] * 2, "low": [0.5] * 2, "close": [1.5] * 2,
                             "volume": [1.0] * 2, "quote_volume": [1.0] * 2, "trades": [1, 1],
                             "px_decimals": [1, 1]})

    def remote_checksum(self, dataset, symbol, month, interval=None):
        return hashlib.sha256(str(self.version[month]).encode()).hexdigest()

    def fetch_month(self, dataset, symbol, month, interval=None):
        self.fetches += 1
        return FetchResult(self._frame(month), self.remote_checksum(dataset, symbol, month),
                           f"fake://{month}", True)


def test_ingest_is_idempotent_and_replaces_changed_partitions(store):
    ad = FakeAdapter()
    r1 = ingest_series(store, ad, "klines", "X", "1h", workers=1)
    assert r1["action"].tolist() == ["written", "written"]
    r2 = ingest_series(store, ad, "klines", "X", "1h", workers=1)
    assert r2["action"].tolist() == ["skipped", "skipped"] and ad.fetches == 2
    ad.version["2024-02"] = 2      # venue re-publishes one month
    r3 = ingest_series(store, ad, "klines", "X", "1h", workers=1)
    assert r3["action"].tolist() == ["skipped", "replaced"]
    raw = store.read_dataset(Layer.RAW, "klines", {"symbol": "X"})
    assert len(raw) == 4            # replaced, never appended
    assert sorted(raw["open"].unique().tolist()) == [1.0, 2.0]


def test_clean_klines_dedups_flags_and_logs_gaps():
    raw = pd.DataFrame({"open_time": [0, H, H, 4 * H], "open": [1.0, 1, 1, 1],
                        "high": [2.0, 2, 2, 0.9], "low": [0.5, .5, .5, .5],
                        "close": [1.5, 1.5, 1.6, 0.8], "month": "m"})
    clean, gaps, nbad = clean_klines_frame(raw, "1h")
    assert clean["open_time"].tolist() == [0, H, 4 * H]
    assert clean.loc[1, "close"] == 1.6            # last write wins
    assert nbad == 1 and clean["ohlc_flag"].tolist() == [False, False, True]
    assert gaps["missing_bars"].tolist() == [2]


def test_funding_normalization_and_interval_read_not_assumed():
    t0 = 1704067200000
    raw = pd.DataFrame({"funding_time": [t0 + 1, t0 + 8 * H + 2, t0 + 16 * H, t0 + 20 * H,
                                         t0 + 20 * H + 1],
                        "funding_rate": [1e-4, 2e-4, 3e-4, 4e-4, 5e-4],
                        "funding_interval_hours": [np.nan, np.nan, 8, 4, 4]})
    f = normalize_funding_frame(raw)
    assert (f["funding_time"] - t0).tolist() == [0, 8 * H, 16 * H, 20 * H]
    assert f["funding_rate"].iloc[-1] == 5e-4
    m = InstrumentMaster.from_yaml()
    iid = "binance_usdm.BTCUSDT.perp"
    apply_funding_intervals(m, iid, f)
    at = lambda ms: pd.Timestamp(t0 + ms, unit="ms", tz="UTC")
    assert m.get(iid, at(0)).funding_interval_hours == 8.0       # observed spacing (none reported)
    assert m.get(iid, at(20 * H)).funding_interval_hours == 4.0  # venue-reported change


def test_tick_size_change_detected_from_price_precision():
    raw = pd.DataFrame({"month": ["2020-01", "2020-01", "2020-02", "2020-03"],
                        "px_decimals": [2, 1, 2, 1]})
    ticks = infer_monthly_tick(raw)
    assert ticks["tick_observed"].tolist() == pytest.approx([0.01, 0.01, 0.1])
    m = InstrumentMaster.from_yaml()
    iid = "binance_usdm.BTCUSDT.perp"
    apply_observed_ticks(m, iid, ticks)
    assert m.get(iid, "2020-02-15").tick_size == pytest.approx(0.01)
    assert m.get(iid, "2020-03-02").tick_size == pytest.approx(0.1)
    assert len(m.change_log) == 2


def test_master_verified_against_venue_reference():
    from qdp.clean import verify_against_venue
    m = InstrumentMaster.from_yaml()
    info = [{"symbol": "BTCUSDT", "onboard_date": 1567965300000, "tick_size": 0.1, "lot_size": 0.001},
            {"symbol": "ETHUSDT", "onboard_date": 1574840700000, "tick_size": 0.05, "lot_size": 0.001}]
    v = verify_against_venue(m, "binance_usdm", info, ["BTCUSDT", "ETHUSDT", "SOLUSDT"])
    assert v.set_index("symbol").loc["BTCUSDT", ["listing_match", "tick_match"]].all()
    assert not v.set_index("symbol").loc["ETHUSDT", "tick_match"]
    assert v.set_index("symbol").loc["SOLUSDT", "status"] == "not in venue exchangeInfo"


def test_venue_margin_tiers_replace_config_snapshot():
    from qdp.clean import apply_venue_brackets
    m = InstrumentMaster.from_yaml()
    br = {"BTCUSDT": {"update_time": 1755585384254,
                      "brackets": [(0.0, 0.004, 0.0), (300000.0, 0.005, 300.0), (800000.0, 0.0065, 1500.0)]},
          "SOLUSDT": {"update_time": 1750661353462,
                      "brackets": [(0.0, 0.007, 0.0), (50000.0, 0.01, 150.0)]}}
    out = apply_venue_brackets(m, "binance_usdm", br, ["BTCUSDT", "SOLUSDT", "XRPUSDT"]).set_index("symbol")
    assert out.loc["BTCUSDT", "cum_matches_formula"] and out.loc["SOLUSDT", "cum_matches_formula"]
    sol = m.get("binance_usdm.SOLUSDT.perp", "2021-01-01")       # applies to the whole history
    assert sol.mmr_brackets == ((0.0, 0.007), (50000.0, 0.01))
    assert sol.maintenance(60_000.0) == (0.01, 150.0)
    assert out.loc["XRPUSDT", "source"].startswith("config snapshot")
