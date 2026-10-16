import numpy as np
import pandas as pd
import pytest

from qdp.research.benchmarks import equal_weight_basket, perp_daily_returns, window_stats
from qdp.research.stats import deflated_sharpe, expected_max_sharpe, percentile_of
from qdp.research.walkforward import anchored_folds, walk_forward


def test_anchored_folds_12m_train_3m_test():
    f = anchored_folds(pd.Timestamp("2020-01-15", tz="UTC"), pd.Timestamp("2021-07-01", tz="UTC"))
    assert [x["test_start"].strftime("%Y-%m") for x in f] == ["2021-01", "2021-04"]
    assert all(x["train_start"] == pd.Timestamp("2020-01-01", tz="UTC") for x in f)
    assert f[1]["train_end"] == f[1]["test_start"] == pd.Timestamp("2021-04-01", tz="UTC")


def test_walk_forward_selects_on_train_only():
    rng = np.random.default_rng(0)
    rows = []
    for cid, tr_mu, te_mu in (("a", 0.5, -0.2), ("b", 0.0, 0.3)):
        for d in pd.date_range("2020-01-01", "2021-06-30", freq="2D", tz="UTC"):
            mu = tr_mu if d < pd.Timestamp("2021-01-01", tz="UTC") else te_mu
            rows.append({"config_id": cid, "entry_ts": d, "r_multiple": mu + rng.normal(0, .01)})
    trades = pd.DataFrame(rows)
    folds = anchored_folds(pd.Timestamp("2020-01-01", tz="UTC"), pd.Timestamp("2021-04-01", tz="UTC"))
    chain, cells = walk_forward(trades, ["a", "b"], folds, min_train_trades=10)
    assert chain.iloc[0]["selected"] == "a"                  # best in train...
    assert chain.iloc[0]["test_mean_r"] == pytest.approx(-0.2, abs=0.01)   # ...reported OOS as is
    assert set(cells["config_id"]) == {"a", "b"}


def test_deflated_sharpe_penalizes_many_trials():
    rng = np.random.default_rng(1)
    r = rng.normal(0.001, 0.01, 1000)
    one = deflated_sharpe(r, np.array([0.1]), n_trials=1)
    many = deflated_sharpe(r, rng.normal(0, 0.03, 500), n_trials=500)
    assert many["sr0"] > 0 and many["dsr"] < one["dsr"]
    assert expected_max_sharpe(1, 0.01) == 0.0
    assert expected_max_sharpe(1000, 0.01) > expected_max_sharpe(10, 0.01)


def test_percentile_of():
    assert percentile_of(5.0, np.arange(10.0)) == 55.0
    assert percentile_of(100.0, np.arange(10.0)) == 100.0


def test_perp_buy_and_hold_pays_funding():
    H = 3_600_000
    t0 = 1_704_067_200_000
    ot = t0 + np.arange(72, dtype=np.int64) * H
    bars = pd.DataFrame({"open_time": ot, "close": 100.0})
    bars.loc[24:, "close"] = 110.0
    funding = pd.DataFrame({"funding_time": t0 + np.array([8, 16, 32]) * H,
                            "funding_rate": [0.001, 0.001, 0.002]})
    r = perp_daily_returns(bars, funding, H)
    assert len(r) == 2
    # day 2 (bars 24..47 close at 110): +10% price; funding during day 2: 0.002
    assert r.iloc[0] == pytest.approx(0.10 - 0.002)
    st = window_stats(pd.concat([r, r, r]).reset_index(drop=True).set_axis(
        pd.date_range("2024-01-02", periods=6, tz="UTC")), pd.Timestamp("2024-01-01", tz="UTC"),
        pd.Timestamp("2025-01-01", tz="UTC"))
    assert st["days"] == 6
    b = equal_weight_basket({"x": r, "y": r * 0})
    assert b.iloc[0] == pytest.approx(r.iloc[0] / 2)
