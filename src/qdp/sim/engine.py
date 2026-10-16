"""Event-driven perpetual-futures simulator.

Bar-by-bar, one position per instrument, no pyramiding. Per bar, in this order:
  1. funding   for every funding event T with entry_bar_open < T <= this bar's open, the
               position pays/receives  -qty * mark_price(T) * rate(T) * sign(position)
               using the realized rate at T (no forward-looking rate); the isolated margin is
               adjusted by it (as on the venue).
  2. trailing  (swing-trail exit only) move the stop to the latest swing confirmed after
               entry (information up to the previous close).
  3. liquidation  checked against MARK price BEFORE stop/target: if the bar's mark extreme
               reaches the liquidation price the isolated margin is lost. The stop never
               "saves" a position that the mark price liquidates in the same bar.
  4. stop / target  stop-market triggers on LAST (or MARK); a gap through the stop fills at
               the open. Targets are resting limit orders (maker) and need price to trade
               through them by >= ``through_ticks`` ticks. If both are inside one bar the STOP
               fills first (``optimistic=True`` reverses this for the sensitivity run).
  5. entry     if flat (and no exit happened this bar): market orders fill at the open
               (taker + slippage); resting limit orders fill only if price trades through
               the limit by >= ``through_ticks`` ticks (maker, no slippage). A limit fill's
               own bar is then checked for liquidation and stop; its target can only fill
               from the next bar (the bar's high may precede the fill) unless optimistic.

Sizing: risk ``risk_pct`` of current equity between the entry fill and the stop. Isolated
margin = notional / max_leverage (capped at available equity, which downsizes the trade).
If the liquidation price would sit at/inside the stop, the trade is de-levered (margin
raised so liquidation lies beyond the stop, if equity allows) or rejected; both counted.
"""
from __future__ import annotations

from dataclasses import dataclass, field

import numpy as np
import pandas as pd
from numba import njit

from ..costs import CostModel
from ..grid import map_events_to_bars
from ..instruments import Instrument
from ..signals.trend_pullback import OrderIntents
from .margin import bracket, brackets_arrays, liquidation_price, margin_for_liq_at

EXIT_STOP, EXIT_TARGET, EXIT_TRAIL, EXIT_LIQ, EXIT_EOD = 1, 2, 3, 4, 5
EXIT_NAMES = {EXIT_STOP: "stop", EXIT_TARGET: "target", EXIT_TRAIL: "trail",
              EXIT_LIQ: "liquidation", EXIT_EOD: "end_of_data"}
TRADE_FIELDS = ("side", "entry_bar", "exit_bar", "entry_ref", "entry_px", "exit_ref", "exit_px",
                "stop0", "target", "qty", "notional", "margin0", "leverage", "liq_px",
                "gross_pnl", "slippage", "fees", "funding", "net_pnl", "risk_amt",
                "exit_reason", "entry_type", "n_funding", "equity_before", "lev_capped",
                "delevered")
COUNTERS = ("liq_rejected", "delevered", "lev_capped", "invalid_stop", "mark_fallback",
            "liquidations", "no_mark_skipped")


@njit(cache=True)
def _simulate(o, h, l, c, mo, mh, ml, mok, tick, atr, f_rate, f_px,
              lim_l_px, lim_l_stop, lim_s_px, lim_s_stop,
              mkt_l, mkt_l_stop, mkt_s, mkt_s_stop, mkt_frac,
              tr_l_lv, tr_l_cb, tr_s_lv, tr_s_cb,
              allow_long, allow_short, entry_lo, entry_hi,
              target_r, trail_atr, risk_pct, max_lev, maker, taker, slip,
              stop_on_mark, optimistic, liq_policy, through_ticks,
              mm_floors, mm_rates, eq0, mkt_as_maker, require_mark):
    n = len(o)
    cap = n // 2 + 16
    T = np.full((cap, 26), np.nan)
    cnt = np.zeros(7, np.int64)
    eq = np.empty(n)
    nt = 0
    cash = eq0
    pos = 0
    qty = 0.0
    ep_ref = 0.0
    ep = 0.0
    stop = 0.0
    tp = 0.0
    wallet = 0.0
    liq = 0.0
    entry_bar = -1
    fund_acc = 0.0
    nfund = 0
    entry_fee = 0.0
    entry_slip = 0.0
    eq_before = 0.0
    risk_amt = 0.0
    margin0 = 0.0
    capped = 0.0
    delev = 0.0
    etype = 0
    trail_mode = target_r <= 0.0
    mmr = 0.0
    cum = 0.0

    stop0 = 0.0
    liq0 = 0.0
    for t in range(n):
        exited = False
        new_limit = False
        can_enter = t >= entry_lo and t < entry_hi and t < n - 1
        # phases: 0 market entry at the open | 1 manage | 2 limit entry | 3 manage limit-fill bar
        for ph in range(4):
            if ph == 0 or ph == 2:
                if pos != 0 or exited or not can_enter:
                    continue
                side = 0
                et = 0
                ref = 0.0
                st = np.nan
                if ph == 0:
                    if allow_long and mkt_l[t]:
                        side = 1
                        et = 1
                        ref = o[t]
                        st = mkt_l_stop[t] if np.isnan(mkt_frac[t]) else o[t] * (1.0 - mkt_frac[t])
                    elif allow_short and mkt_s[t]:
                        side = -1
                        et = 1
                        ref = o[t]
                        st = mkt_s_stop[t] if np.isnan(mkt_frac[t]) else o[t] * (1.0 + mkt_frac[t])
                else:
                    if allow_long and not np.isnan(lim_l_px[t]) and \
                            l[t] <= lim_l_px[t] - through_ticks * tick[t]:
                        side = 1
                        ref = lim_l_px[t]
                        st = lim_l_stop[t]
                    elif allow_short and not np.isnan(lim_s_px[t]) and \
                            h[t] >= lim_s_px[t] + through_ticks * tick[t]:
                        side = -1
                        ref = lim_s_px[t]
                        st = lim_s_stop[t]
                if side == 0:
                    continue
                if require_mark and not mok[t]:
                    # no mark price on this bar: liquidation could not be checked against MARK
                    cnt[6] += 1
                    continue
                if et == 1 and not mkt_as_maker:
                    fill = ref * (1.0 + slip) if side == 1 else ref * (1.0 - slip)
                    fee_rate = taker
                else:
                    fill = ref
                    fee_rate = maker
                dist = (fill - st) * side
                if np.isnan(st) or dist <= 0.0:
                    cnt[3] += 1
                    continue
                q = risk_pct * cash / dist
                notional = q * fill
                m = notional / max_lev
                capped = 0.0
                if m > cash:
                    m = cash
                    q = cash * max_lev / fill
                    notional = q * fill
                    capped = 1.0
                    cnt[2] += 1
                r_mmr, r_cum = bracket(notional, mm_floors, mm_rates)
                lp = liquidation_price(side, q, fill, m, r_mmr, r_cum)
                # liquidation must lie strictly beyond the stop's (doubly) slipped fill
                guard = st * (1.0 - 2.0 * slip) if side == 1 else st * (1.0 + 2.0 * slip)
                bad = (side == 1 and lp >= guard) or (side == -1 and lp <= guard)
                delev = 0.0
                if bad:
                    ok = False
                    if liq_policy == 1:
                        m_req = margin_for_liq_at(side, q, fill, guard, r_mmr, r_cum) * 1.001
                        if m_req <= cash:
                            m = m_req
                            lp = liquidation_price(side, q, fill, m, r_mmr, r_cum)
                            delev = 1.0
                            cnt[1] += 1
                            ok = True
                    if not ok:
                        cnt[0] += 1
                        continue
                pos = side
                qty = q
                ep_ref = ref
                ep = fill
                stop = st
                stop0 = st
                tp = fill + side * target_r * dist if not trail_mode else np.nan
                wallet = m
                margin0 = m
                mmr = r_mmr
                cum = r_cum
                liq = lp
                liq0 = lp
                entry_bar = t
                fund_acc = 0.0
                nfund = 0
                eq_before = cash
                risk_amt = q * dist
                entry_fee = notional * fee_rate
                entry_slip = -q * abs(fill - ref)
                cash -= entry_fee
                etype = et
                new_limit = ph == 2
                continue

            # ---------------------------------------------------------- manage (ph 1 / 3)
            if pos == 0 or (ph == 3 and not new_limit):
                continue
            if t > entry_bar:
                if f_rate[t] != 0.0:
                    fp = -qty * f_px[t] * f_rate[t] * pos
                    fund_acc += fp
                    cash += fp
                    wallet += fp
                    nfund += 1
                    liq = liquidation_price(pos, qty, ep, wallet, mmr, cum)
                if trail_mode:
                    if pos == 1 and tr_l_cb[t - 1] >= entry_bar and not np.isnan(tr_l_lv[t - 1]):
                        cand = tr_l_lv[t - 1] - trail_atr * atr[t - 1]
                        if cand > stop:
                            stop = cand
                    elif pos == -1 and tr_s_cb[t - 1] >= entry_bar and not np.isnan(tr_s_lv[t - 1]):
                        cand = tr_s_lv[t - 1] + trail_atr * atr[t - 1]
                        if cand < stop:
                            stop = cand
            limit_entry_bar = ph == 3
            if mok[t]:
                mlo = ml[t]
                mhi = mh[t]
            else:
                mlo = l[t]
                mhi = h[t]
                cnt[4] += 1
            reason = 0
            xref = 0.0
            xfill = 0.0
            xfee_rate = 0.0
            # ---- liquidation vs MARK, before stop / target
            if (pos == 1 and mlo <= liq) or (pos == -1 and mhi >= liq):
                reason = 4
                xref = liq
                xfill = liq
            else:
                if stop_on_mark and mok[t]:
                    s_hit = (pos == 1 and ml[t] <= stop) or (pos == -1 and mh[t] >= stop)
                    s_gap = (pos == 1 and mo[t] <= stop) or (pos == -1 and mo[t] >= stop)
                else:
                    s_hit = (pos == 1 and l[t] <= stop) or (pos == -1 and h[t] >= stop)
                    s_gap = (pos == 1 and o[t] <= stop) or (pos == -1 and o[t] >= stop)
                if limit_entry_bar or (t == entry_bar):
                    s_gap = False
                t_hit = False
                t_gap = False
                if not trail_mode:
                    thr = through_ticks * tick[t]
                    t_hit = (pos == 1 and h[t] >= tp + thr) or (pos == -1 and l[t] <= tp - thr)
                    t_gap = (pos == 1 and o[t] >= tp + thr) or (pos == -1 and o[t] <= tp - thr)
                    if limit_entry_bar or (t == entry_bar):
                        t_gap = False
                    if limit_entry_bar and not optimistic:
                        t_hit = False   # the bar's high may have printed before the fill
                take_stop = False
                take_tgt = False
                if s_hit and t_hit:
                    if s_gap:
                        take_stop = True
                    elif t_gap:
                        take_tgt = True
                    elif optimistic:
                        take_tgt = True
                    else:
                        take_stop = True
                elif s_hit:
                    take_stop = True
                elif t_hit:
                    take_tgt = True
                if take_stop:
                    reason = 3 if trail_mode else 1
                    xref = o[t] if s_gap else stop
                    xfill = xref * (1.0 - slip) if pos == 1 else xref * (1.0 + slip)
                    xfee_rate = taker
                elif take_tgt:
                    reason = 2
                    xref = tp
                    xfill = tp
                    xfee_rate = maker
            if reason == 0 and t == n - 1:
                reason = 5
                xref = c[t]
                xfill = xref * (1.0 - slip) if pos == 1 else xref * (1.0 + slip)
                xfee_rate = taker
            if reason == 0:
                continue
            gross = qty * (xref - ep_ref) * pos
            xslip = -qty * abs(xfill - xref)
            if reason == 4:
                # the isolated wallet is lost; anything beyond the price move is the
                # liquidation clearance fee
                pnl_px = qty * (xfill - ep) * pos
                xfee = -wallet - pnl_px
                xfee = -xfee if xfee < 0.0 else 0.0
                cash += -wallet
                cnt[5] += 1
            else:
                xfee = abs(qty * xfill) * xfee_rate
                cash += qty * (xfill - ep) * pos - xfee
            net = gross + entry_slip + xslip - entry_fee - xfee + fund_acc
            if nt < cap:
                T[nt, 0] = pos
                T[nt, 1] = entry_bar
                T[nt, 2] = t
                T[nt, 3] = ep_ref
                T[nt, 4] = ep
                T[nt, 5] = xref
                T[nt, 6] = xfill
                T[nt, 7] = stop0
                T[nt, 8] = tp
                T[nt, 9] = qty
                T[nt, 10] = qty * ep
                T[nt, 11] = margin0
                T[nt, 12] = qty * ep / margin0
                T[nt, 13] = liq0
                T[nt, 14] = gross
                T[nt, 15] = entry_slip + xslip
                T[nt, 16] = -(entry_fee + xfee)
                T[nt, 17] = fund_acc
                T[nt, 18] = net
                T[nt, 19] = risk_amt
                T[nt, 20] = reason
                T[nt, 21] = etype
                T[nt, 22] = nfund
                T[nt, 23] = eq_before
                T[nt, 24] = capped
                T[nt, 25] = delev
                nt += 1
            pos = 0
            exited = True
        # ------------------------------------------------ mark-to-market equity at close
        if pos != 0:
            eq[t] = cash + qty * (c[t] - ep) * pos
        else:
            eq[t] = cash
    return T[:nt], cnt, eq


@dataclass
class SimParams:
    target_r: float = 2.0            # <= 0 -> swing-trail exit (no fixed target)
    trail_atr: float = 0.1
    risk_pct: float = 0.005
    max_leverage: float = 5.0
    stop_trigger: str = "last"       # "last" | "mark"
    optimistic: bool = False
    liq_policy: str = "delever"      # "delever" | "reject"
    through_ticks: int = 1
    initial_equity: float = 100_000.0
    allow_long: bool = True
    allow_short: bool = True
    # baselines only: give market entries maker pricing (fill at the open, maker fee, no
    # slippage) so a random-entry baseline pays the same entry costs as a limit-entry strategy
    market_entry_as_maker: bool = False
    # refuse entries on bars without a mark price (e.g. before the venue's mark history
    # starts), so no position depends on a last-price liquidation check at entry
    require_mark: bool = True


@dataclass
class SimResult:
    trades: pd.DataFrame
    equity: np.ndarray
    counters: dict
    params: SimParams = field(default=None)


@dataclass
class MarketData:
    """Arrays for one instrument on its own grid (prepared once, reused across runs)."""
    open_time: np.ndarray
    o: np.ndarray
    h: np.ndarray
    l: np.ndarray
    c: np.ndarray
    mo: np.ndarray
    mh: np.ndarray
    ml: np.ndarray
    mok: np.ndarray
    tick: np.ndarray
    atr: np.ndarray
    f_rate: np.ndarray
    f_px: np.ndarray
    mm_floors: np.ndarray
    mm_rates: np.ndarray
    funding_events_mapped: int = 0

    @property
    def n(self) -> int:
        return len(self.o)


def prepare_market_data(bars: pd.DataFrame, funding: pd.DataFrame, tick: np.ndarray,
                        atr: np.ndarray, inst: Instrument) -> MarketData:
    f64 = lambda x: np.ascontiguousarray(np.asarray(x, dtype=np.float64))
    ot = bars["open_time"].to_numpy(np.int64)
    o, h, l, c = (f64(bars[k]) for k in ("open", "high", "low", "close"))
    mok = ~bars[["mark_open", "mark_high", "mark_low", "mark_close"]].isna().any(axis=1).to_numpy()
    mo = np.where(mok, bars["mark_open"].to_numpy(float), o)
    mh = np.where(mok, bars["mark_high"].to_numpy(float), h)
    ml = np.where(mok, bars["mark_low"].to_numpy(float), l)
    f_rate = np.zeros(len(o))
    ft = funding["funding_time"].to_numpy(np.int64)
    fr = funding["funding_rate"].to_numpy(float)
    # events strictly after the first bar's open and not after the last bar's open
    keep = (ft > ot[0]) & (ft <= ot[-1])
    idx = map_events_to_bars(ot, ft[keep])
    np.add.at(f_rate, idx, fr[keep])
    floors, rates = brackets_arrays(inst.mmr_brackets)
    return MarketData(ot, o, h, l, c, f64(mo), f64(mh), f64(ml), np.ascontiguousarray(mok),
                      f64(tick), f64(atr), f_rate, f64(mo), floors, rates, int(keep.sum()))


def simulate(md: MarketData, oi: OrderIntents, params: SimParams, cost: CostModel,
             entry_lo: int = 0, entry_hi: int | None = None) -> SimResult:
    f64 = lambda x: np.ascontiguousarray(np.asarray(x, dtype=np.float64))
    b = lambda x: np.ascontiguousarray(np.asarray(x, dtype=np.bool_))
    i64 = lambda x: np.ascontiguousarray(np.asarray(x, dtype=np.int64))
    T, cnt, eq = _simulate(
        md.o, md.h, md.l, md.c, md.mo, md.mh, md.ml, md.mok, md.tick, md.atr, md.f_rate, md.f_px,
        f64(oi.lim_long_px), f64(oi.lim_long_stop), f64(oi.lim_short_px), f64(oi.lim_short_stop),
        b(oi.mkt_long), f64(oi.mkt_long_stop), b(oi.mkt_short), f64(oi.mkt_short_stop),
        f64(oi.mkt_stopfrac), f64(oi.trail_long_lv), i64(oi.trail_long_cb),
        f64(oi.trail_short_lv), i64(oi.trail_short_cb),
        bool(params.allow_long), bool(params.allow_short), int(entry_lo),
        int(md.n if entry_hi is None else entry_hi),
        float(params.target_r), float(params.trail_atr), float(params.risk_pct),
        float(params.max_leverage), float(cost.maker_fee), float(cost.taker_fee),
        float(cost.slippage_bps) * 1e-4, params.stop_trigger == "mark", bool(params.optimistic),
        1 if params.liq_policy == "delever" else 0, float(params.through_ticks),
        md.mm_floors, md.mm_rates, float(params.initial_equity),
        bool(params.market_entry_as_maker), bool(params.require_mark))
    trades = pd.DataFrame(T, columns=list(TRADE_FIELDS))
    for col in ("side", "entry_bar", "exit_bar", "exit_reason", "entry_type", "n_funding"):
        trades[col] = trades[col].astype(np.int64)
    if len(trades):
        trades["entry_time"] = md.open_time[trades["entry_bar"].to_numpy()]
        trades["exit_time"] = md.open_time[trades["exit_bar"].to_numpy()]
        trades["r_multiple"] = trades["net_pnl"] / trades["risk_amt"]
        trades["r_ex_funding"] = (trades["net_pnl"] - trades["funding"]) / trades["risk_amt"]
        trades["exit"] = trades["exit_reason"].map(EXIT_NAMES)
    return SimResult(trades, eq, dict(zip(COUNTERS, cnt.tolist())), params)
