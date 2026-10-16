import numpy as np
import pandas as pd
import pytest

from qdp.costs import CostModel
from qdp.instruments import InstrumentMaster
from qdp.sim.engine import (EXIT_LIQ, EXIT_STOP, EXIT_TARGET, SimParams, prepare_market_data,
                            simulate)
from qdp.sim.margin import bracket, brackets_arrays, liquidation_price
from qdp.sim.metrics import trade_stats
from qdp.signals.trend_pullback import OrderIntents

H = 3_600_000
T0 = 1_704_067_200_000          # 2024-01-01 00:00 UTC
INST = InstrumentMaster.from_yaml().find("binance_usdm", "BTCUSDT", "perp")
COST = CostModel(maker_fee=0.0002, taker_fee=0.0005, slippage_bps=2.0)


def flat_bars(n=60, px=100.0):
    ot = T0 + np.arange(n, dtype=np.int64) * H
    df = pd.DataFrame({"open_time": ot, "open": px, "high": px, "low": px, "close": px})
    for c in ("open", "high", "low", "close"):
        df[f"mark_{c}"] = df[c]
    return df


def md_for(bars, funding=None, tick=0.01):
    if funding is None:
        funding = pd.DataFrame({"funding_time": np.array([], np.int64), "funding_rate": []})
    n = len(bars)
    return prepare_market_data(bars, funding, np.full(n, tick), np.full(n, 1.0), INST)


def mkt(n, side, at, stopfrac):
    oi = OrderIntents(n)
    getattr(oi, "mkt_long" if side == 1 else "mkt_short")[at] = True
    oi.mkt_stopfrac[at] = stopfrac
    return oi


# --------------------------------------------------------------------------- funding
@pytest.mark.parametrize("side", [1, -1])
def test_funding_applied_only_while_held_with_correct_sign(side):
    bars = flat_bars(60)
    # exit: target hit at bar 40 (R = 1, stop 5% away)
    if side == 1:
        bars.loc[40, ["high", "mark_high"]] = 120.0
    else:
        bars.loc[40, ["low", "mark_low"]] = 80.0
    rates = {
        T0 + 10 * H: 0.0010,             # at the entry bar's open: NOT held (entered at that open)
        T0 + 16 * H: 0.0001,             # held
        T0 + 24 * H + H // 2: -0.0003,   # mid-bar event -> applied at the next bar open, held
        T0 + 32 * H: 0.0002,             # held
        T0 + 40 * H: 0.0004,             # at the exit bar's open: still held (exit intrabar)
        T0 + 48 * H: 0.0050,             # after exit: NOT held
        T0 + 5 * H: 0.0070,              # before entry: NOT held
    }
    funding = pd.DataFrame({"funding_time": np.array(list(rates), np.int64),
                            "funding_rate": list(rates.values())})
    md = md_for(bars, funding)
    res = simulate(md, mkt(60, side, 10, 0.05), SimParams(target_r=1.0), COST)
    tr = res.trades.iloc[0]
    assert tr.entry_bar == 10 and tr.exit_bar == 40 and tr.exit_reason == EXIT_TARGET
    held = 0.0001 - 0.0003 + 0.0002 + 0.0004
    expected = -tr.qty * 100.0 * held * side
    assert tr.funding == pytest.approx(expected, rel=1e-12)
    assert tr.n_funding == 4
    # sign: positive net rate -> longs pay, shorts receive
    assert np.sign(tr.funding) == -side
    # funding is part of net P&L and its own attribution line
    assert tr.net_pnl == pytest.approx(tr.gross_pnl + tr.slippage + tr.fees + tr.funding)


def test_no_funding_when_flat():
    bars = flat_bars(30)
    funding = pd.DataFrame({"funding_time": T0 + np.arange(1, 30, 8, dtype=np.int64) * H,
                            "funding_rate": 0.01})
    res = simulate(md_for(bars, funding), OrderIntents(30), SimParams(), COST)
    assert res.trades.empty and res.equity[-1] == pytest.approx(100_000.0)


# --------------------------------------------------------------------------- margin / liq
def test_liquidation_formula_and_brackets():
    floors, rates = brackets_arrays(INST.mmr_brackets)
    for notional in (1_000.0, 60_000.0, 900_000.0):
        assert bracket(notional, floors, rates) == pytest.approx(INST.maintenance(notional))
    # long 1 BTC at 100 with 20 margin, mmr 0.4% -> LP solves wallet + pnl = mm
    lp = liquidation_price(1, 1.0, 100.0, 20.0, 0.004, 0.0)
    assert 20 + (lp - 100) == pytest.approx(0.004 * lp)
    lps = liquidation_price(-1, 1.0, 100.0, 20.0, 0.004, 0.0)
    assert 20 - (lps - 100) == pytest.approx(0.004 * lps)


def test_liquidation_checked_on_mark_before_stop():
    bars = flat_bars(30)
    # bar 12: last trades just through the 5% stop, but MARK spikes beyond liquidation (~-19.6%)
    bars.loc[12, "low"] = 94.0
    bars.loc[12, "mark_low"] = 75.0
    res = simulate(md_for(bars), mkt(30, 1, 10, 0.05), SimParams(target_r=2.0), COST)
    tr = res.trades.iloc[0]
    assert tr.exit_reason == EXIT_LIQ and res.counters["liquidations"] == 1
    # the whole isolated margin is lost (plus the entry fee) - the stop did not save it
    assert tr.net_pnl == pytest.approx(-tr.margin0 - tr.notional * COST.taker_fee, rel=1e-9)
    assert res.equity[-1] == pytest.approx(100_000 + tr.net_pnl)


def test_liq_inside_stop_is_rejected_or_delevered():
    bars = flat_bars(30)
    rej = simulate(md_for(bars), mkt(30, 1, 10, 0.30), SimParams(liq_policy="reject"), COST)
    assert rej.trades.empty and rej.counters["liq_rejected"] == 1
    dl = simulate(md_for(bars), mkt(30, 1, 10, 0.30), SimParams(liq_policy="delever"), COST)
    tr = dl.trades.iloc[0]
    assert dl.counters["delevered"] == 1 and tr.delevered == 1
    assert tr.liq_px < tr.stop0 and tr.leverage < 5.0


def test_leverage_cap_downsizes_tiny_stops():
    bars = flat_bars(30)
    res = simulate(md_for(bars), mkt(30, 1, 10, 0.0002), SimParams(), COST)
    tr = res.trades.iloc[0]
    assert res.counters["lev_capped"] == 1 and tr.lev_capped == 1
    assert tr.notional == pytest.approx(5.0 * 100_000 * (1 - 0.0005) , rel=1e-3)
    assert tr.risk_amt < 0.005 * 100_000


# --------------------------------------------------------------------------- fills
def test_sizing_risks_half_percent_and_fee_types():
    bars = flat_bars(30)
    bars.loc[20, "high"] = 130.0
    res = simulate(md_for(bars), mkt(30, 1, 10, 0.05), SimParams(target_r=2.0), COST)
    tr = res.trades.iloc[0]
    assert tr.risk_amt == pytest.approx(500.0)
    fill = 100.0 * 1.0002                                   # taker entry with 2bp slippage
    assert tr.entry_px == pytest.approx(fill) and tr.exit_reason == EXIT_TARGET
    entry_fee = tr.qty * fill * COST.taker_fee
    exit_fee = tr.qty * tr.exit_px * COST.maker_fee         # resting take-profit = maker
    assert -tr.fees == pytest.approx(entry_fee + exit_fee)
    assert tr.slippage == pytest.approx(-tr.qty * (fill - 100.0))
    assert tr.r_multiple == pytest.approx(tr.net_pnl / 500.0)


def test_stop_first_when_stop_and_target_in_same_bar_and_optimistic_variant():
    bars = flat_bars(30)
    bars.loc[15, ["high", "low"]] = [120.0, 90.0]
    bars.loc[15, ["mark_high", "mark_low"]] = [120.0, 90.0]
    pes = simulate(md_for(bars), mkt(30, 1, 10, 0.05), SimParams(target_r=2.0), COST)
    opt = simulate(md_for(bars), mkt(30, 1, 10, 0.05), SimParams(target_r=2.0, optimistic=True), COST)
    assert pes.trades.iloc[0].exit_reason == EXIT_STOP
    assert opt.trades.iloc[0].exit_reason == EXIT_TARGET


def test_gap_through_stop_fills_at_open_with_slippage():
    bars = flat_bars(30)
    bars.loc[15, ["open", "high", "low", "close"]] = [90.0, 91.0, 89.0, 90.0]
    res = simulate(md_for(bars), mkt(30, 1, 10, 0.05), SimParams(), COST)
    tr = res.trades.iloc[0]
    assert tr.exit_reason == EXIT_STOP and tr.exit_ref == 90.0
    assert tr.exit_px == pytest.approx(90.0 * (1 - 0.0002))


def test_limit_fill_requires_trade_through_by_one_tick():
    bars = flat_bars(40)
    oi = OrderIntents(40)
    oi.lim_long_px[10:13] = 99.0
    oi.lim_long_stop[10:13] = 95.0
    bars.loc[10, "low"] = 99.0          # touches the limit exactly: no fill
    bars.loc[11, "low"] = 98.995        # trades through by half a tick: no fill
    bars.loc[12, "low"] = 98.99         # through by one tick: fill
    res = simulate(md_for(bars, tick=0.01), oi, SimParams(), COST)
    tr = res.trades.iloc[0]
    assert tr.entry_bar == 12 and tr.entry_px == 99.0 and tr.entry_type == 0
    assert tr.slippage == 0.0 or tr.exit_reason != EXIT_TARGET
    assert -tr.fees >= tr.qty * 99.0 * COST.maker_fee


def test_limit_entry_bar_target_needs_optimistic_and_stop_is_checked():
    bars = flat_bars(40)
    oi = OrderIntents(40)
    oi.lim_long_px[10] = 99.0
    oi.lim_long_stop[10] = 98.0
    bars.loc[10, ["high", "low"]] = [104.0, 98.9]       # fill at 99, target 101 inside the bar
    pes = simulate(md_for(bars), oi, SimParams(target_r=2.0), COST)
    opt = simulate(md_for(bars), oi, SimParams(target_r=2.0, optimistic=True), COST)
    assert pes.trades.iloc[0].exit_bar > 10            # target not taken in the fill bar
    assert opt.trades.iloc[0].exit_bar == 10 and opt.trades.iloc[0].exit_reason == EXIT_TARGET
    bars.loc[10, "low"] = 97.0                          # fill then stop in the same bar
    st = simulate(md_for(bars), oi, SimParams(target_r=2.0), COST)
    assert st.trades.iloc[0].exit_bar == 10 and st.trades.iloc[0].exit_reason == EXIT_STOP


def test_mark_trigger_ignores_last_price_wick():
    bars = flat_bars(30)
    bars.loc[15, "low"] = 94.0          # last wicks through the stop, mark does not
    last = simulate(md_for(bars), mkt(30, 1, 10, 0.05), SimParams(), COST)
    mark = simulate(md_for(bars), mkt(30, 1, 10, 0.05), SimParams(stop_trigger="mark"), COST)
    assert last.trades.iloc[0].exit_bar == 15
    assert mark.trades.iloc[0].exit_bar == 29          # held to end of data


def test_one_position_no_pyramiding_and_short_mirror():
    bars = flat_bars(40)
    bars.loc[30, "low"] = 80.0
    oi = OrderIntents(40)
    oi.mkt_short[[10, 12, 14]] = True
    oi.mkt_stopfrac[[10, 12, 14]] = 0.05
    res = simulate(md_for(bars), oi, SimParams(target_r=2.0), COST)
    assert len(res.trades) == 1 and res.trades.iloc[0].side == -1
    assert res.trades.iloc[0].exit_reason == EXIT_TARGET and res.trades.iloc[0].net_pnl > 0


def test_side_filters_and_entry_window():
    bars = flat_bars(40)
    oi = mkt(40, 1, 10, 0.05)
    assert simulate(md_for(bars), oi, SimParams(allow_long=False), COST).trades.empty
    assert simulate(md_for(bars), oi, SimParams(), COST, entry_lo=11).trades.empty


def test_trade_stats_breakeven_and_attribution():
    tr = pd.DataFrame({"r_multiple": [2.0, -1.0, -1.0, 2.0], "r_ex_funding": [2.1, -.9, -1, 2],
                       "net_pnl": [1000.0, -500, -500, 1000], "gross_pnl": [1100.0, -450, -450, 1050],
                       "fees": [-90.0, -40, -40, -40], "slippage": [0.0, -10, -10, 0],
                       "funding": [-10.0, 0, 0, -10], "entry_bar": [0, 5, 9, 20],
                       "exit_bar": [2, 6, 12, 24], "exit_reason": [2, 1, 1, 2]})
    s = trade_stats(tr, months=2, interval_ms=H, target_r=2.0)
    assert s["win_rate"] == 0.5 and s["breakeven_win_rate"] == pytest.approx(1 / 3)
    assert s["nominal_breakeven_win_rate"] == pytest.approx(1 / 3)
    assert s["expectancy_r"] == 0.5 and s["profit_factor"] == 2.0
    assert s["trades_per_month"] == 2 and s["avg_hold_h"] == pytest.approx(2.5)
    assert s["net_pnl"] == pytest.approx(s["gross_pnl"] + s["fees"] + s["slippage"] + s["funding"])


def test_market_entry_as_maker_for_matched_baselines():
    bars = flat_bars(30)
    res = simulate(md_for(bars), mkt(30, 1, 10, 0.05), SimParams(market_entry_as_maker=True), COST)
    tr = res.trades.iloc[0]
    assert tr.entry_px == 100.0 and tr.qty * 100.0 * COST.maker_fee <= -tr.fees


def test_no_entry_without_mark_price():
    bars = flat_bars(30)
    bars.loc[10, ["mark_open", "mark_high", "mark_low", "mark_close"]] = np.nan
    res = simulate(md_for(bars), mkt(30, 1, 10, 0.05), SimParams(), COST)
    assert res.trades.empty and res.counters["no_mark_skipped"] == 1
    res = simulate(md_for(bars), mkt(30, 1, 10, 0.05), SimParams(require_mark=False), COST)
    assert len(res.trades) == 1
