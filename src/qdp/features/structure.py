"""Market-structure state machine: swings -> trend -> BOS -> zones -> liquidity sweeps.

Implemented once for the LONG side. The short side is the same machine run on the
price-mirrored series (o,h,l,c) -> (-o,-l,-h,-c): a demand zone in mirrored space is a supply
zone in real space, HH+HL becomes LH+LL, a sweep of a swing low becomes a sweep of a swing
high. ``structure_features(..., side=-1)`` maps the outputs back to real prices.

Definitions (long side; all decisions use information available at the time):
  swing      fractal with k bars each side; known only at the close of bar s+k.
  trend      +1 iff the last two confirmed swing highs AND lows are rising (HH+HL).
  BOS        a bar CLOSES above the most recent confirmed, not-yet-broken swing high
             (``bos_close=False`` lets a wick count - ablation only).
  impulse    origin = lowest low after the broken swing high, up to the BOS bar.
  zone       last down-close candle at/before the origin (or a base of up to ``base_max``
             consecutive down-close candles). Zone = [low, max(open, close)]; the bottom is
             extended to the origin low so the zone always contains the impulse origin.
             Unmitigated until the first bar after the BOS that trades into it; invalidated
             by a close below its bottom. One live zone per side (a newer BOS replaces it).
  sweep      price trades below a confirmed swing low that formed after the impulse origin,
             before or during the zone touch.

Per-bar outputs (index t):
  state after the close of bar t:  trend, zone_top, zone_bot, zone_id, bos, swept, trail_lv,
                                   trail_cbar
  orders resting DURING bar t (decided from state after close t-1):
      arm_px / arm_stop / arm_zid, arm_ns (no-sweep variant), arm_sw (sweep variant: either a
      qualifying swing low was already swept, or an unswept one sits above the limit price so
      a fill necessarily trades through it first)
  signals at the CLOSE of bar t, executed at the open of t+1 (confirmation entry):
      cf_ns / cf_sw / cf_stop / cf_zid. The first bar touching the zone must close back above
      the swept level (sweep variant) or above the zone top (no-sweep variant).
"""
from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd
from numba import njit

MAX_SW = 64     # ring buffer of confirmed swing lows kept for sweep eligibility
MAX_EL = 32     # eligible sweep levels per zone


@njit(cache=True)
def _run(o, h, l, c, atr, tick, k, bos_close, base_max, zone_lookback, stop_atr, leaky):
    n = len(o)
    trend = np.zeros(n, np.int8)
    zone_top = np.full(n, np.nan)
    zone_bot = np.full(n, np.nan)
    zone_idv = np.full(n, -1, np.int64)
    bos = np.zeros(n, np.bool_)
    swept_out = np.zeros(n, np.bool_)
    trail_lv = np.full(n, np.nan)
    trail_cbar = np.full(n, -1, np.int64)
    arm_px = np.full(n, np.nan)
    arm_stop = np.full(n, np.nan)
    arm_zid = np.full(n, -1, np.int64)
    arm_ns = np.zeros(n, np.bool_)
    arm_sw = np.zeros(n, np.bool_)
    cf_ns = np.zeros(n, np.bool_)
    cf_sw = np.zeros(n, np.bool_)
    cf_stop = np.full(n, np.nan)
    cf_zid = np.full(n, -1, np.int64)

    # last two confirmed swing highs / lows: index 1 = latest
    sh_lv = np.full(2, np.nan)
    sh_bar = np.full(2, -1, np.int64)
    sl_lv = np.full(2, np.nan)
    sl_bar = np.full(2, -1, np.int64)
    n_sh = 0
    n_sl = 0
    sh_broken = True
    cur_trend = 0
    # ring buffer of all confirmed swing lows (level, swing bar, confirm bar)
    rb_lv = np.full(MAX_SW, np.nan)
    rb_bar = np.full(MAX_SW, -1, np.int64)
    rb_cb = np.full(MAX_SW, -1, np.int64)
    rb_n = 0
    last_sl_lv = np.nan
    last_sl_cb = -1
    # zone state
    z_active = False
    z_mitig = False
    z_top = np.nan
    z_bot = np.nan
    z_origin = -1
    z_bos = -1
    z_id = -1
    el_lv = np.full(MAX_EL, np.nan)
    el_cb = np.full(MAX_EL, -1, np.int64)
    el_sw = np.zeros(MAX_EL, np.bool_)
    el_n = 0
    zone_counter = 0

    for t in range(n):
        # ---- A. limit order resting during bar t (state after close t-1) ----------------
        if t > 0 and z_active and (not z_mitig) and cur_trend == 1 and not np.isnan(atr[t - 1]):
            arm_px[t] = z_top
            arm_stop[t] = z_bot - stop_atr * atr[t - 1]
            arm_zid[t] = z_id
            arm_ns[t] = True
            ok = False
            for e in range(el_n):
                if el_sw[e] or el_lv[e] > z_top:
                    ok = True
                    break
            arm_sw[t] = ok

        # ---- B. intrabar: sweeps of eligible levels, first touch of the zone -------------
        touched_now = False
        if z_active:
            for e in range(el_n):
                if (not el_sw[e]) and el_cb[e] < t and l[t] < el_lv[e]:
                    el_sw[e] = True
            if (not z_mitig) and t > z_bos and l[t] <= z_top:
                z_mitig = True
                touched_now = True

        # ---- C. swing confirmation at the close of bar t ---------------------------------
        if leaky:
            s = t
            can = s >= k and s + k < n
        else:
            s = t - k
            can = s >= k
        if can:
            # swing high
            okh = True
            for j in range(s - k, s):
                if h[j] >= h[s]:
                    okh = False
                    break
            if okh:
                for j in range(s + 1, s + k + 1):
                    if h[j] > h[s]:
                        okh = False
                        break
            if okh:
                sh_lv[0] = sh_lv[1]
                sh_bar[0] = sh_bar[1]
                sh_lv[1] = h[s]
                sh_bar[1] = s
                n_sh += 1
                sh_broken = False
            # swing low
            okl = True
            for j in range(s - k, s):
                if l[j] <= l[s]:
                    okl = False
                    break
            if okl:
                for j in range(s + 1, s + k + 1):
                    if l[j] < l[s]:
                        okl = False
                        break
            if okl:
                sl_lv[0] = sl_lv[1]
                sl_bar[0] = sl_bar[1]
                sl_lv[1] = l[s]
                sl_bar[1] = s
                n_sl += 1
                slot = rb_n % MAX_SW
                rb_lv[slot] = l[s]
                rb_bar[slot] = s
                rb_cb[slot] = t
                rb_n += 1
                last_sl_lv = l[s]
                last_sl_cb = t
                if z_active and s > z_origin and el_n < MAX_EL:
                    el_lv[el_n] = l[s]
                    el_cb[el_n] = t
                    el_sw[el_n] = False
                    el_n += 1
        if n_sh >= 2 and n_sl >= 2:
            if sh_lv[1] > sh_lv[0] and sl_lv[1] > sl_lv[0]:
                cur_trend = 1
            elif sh_lv[1] < sh_lv[0] and sl_lv[1] < sl_lv[0]:
                cur_trend = -1
            else:
                cur_trend = 0
        trend[t] = cur_trend

        # ---- D. confirmation-entry signal at the close of the first-touch bar ------------
        if touched_now and cur_trend == 1 and not np.isnan(atr[t]):
            if c[t] > z_top:
                cf_ns[t] = True
            for e in range(el_n):
                if el_sw[e] and c[t] > el_lv[e]:
                    cf_sw[t] = True
                    break
            if cf_ns[t] or cf_sw[t]:
                cf_stop[t] = z_bot - stop_atr * atr[t]
                cf_zid[t] = z_id

        # ---- E. invalidation by a close through the far edge ------------------------------
        if z_active and c[t] < z_bot:
            z_active = False

        # ---- F. break of structure -> new zone --------------------------------------------
        if n_sh >= 1 and not sh_broken:
            px = c[t] if bos_close else h[t]
            if px > sh_lv[1] and t > sh_bar[1]:
                sh_broken = True
                bos[t] = True
                origin = sh_bar[1] + 1
                for j in range(sh_bar[1] + 1, t + 1):
                    if l[j] < l[origin]:
                        origin = j
                z = -1
                lo_lim = max(0, origin - zone_lookback)
                for j in range(origin, lo_lim - 1, -1):
                    if c[j] < o[j]:
                        z = j
                        break
                if z >= 0:
                    b0 = z
                    while b0 - 1 >= 0 and z - (b0 - 1) < base_max and c[b0 - 1] < o[b0 - 1]:
                        b0 -= 1
                    top = -np.inf
                    for j in range(b0, z + 1):
                        top = max(top, max(o[j], c[j]))
                    bot = np.inf
                    for j in range(b0, origin + 1):
                        bot = min(bot, l[j])
                    z_active = True
                    z_mitig = False
                    z_top = top
                    z_bot = bot
                    z_origin = origin
                    z_bos = t
                    z_id = zone_counter
                    zone_counter += 1
                    el_n = 0
                    m = min(rb_n, MAX_SW)
                    for q in range(m):
                        slot = (rb_n - m + q) % MAX_SW
                        if rb_bar[slot] > origin and el_n < MAX_EL:
                            lv = rb_lv[slot]
                            sw = False
                            for j in range(rb_cb[slot] + 1, t + 1):
                                if l[j] < lv:
                                    sw = True
                                    break
                            el_lv[el_n] = lv
                            el_cb[el_n] = rb_cb[slot]
                            el_sw[el_n] = sw
                            el_n += 1

        # ---- outputs: state after close t -------------------------------------------------
        if z_active and not z_mitig:
            zone_top[t] = z_top
            zone_bot[t] = z_bot
            zone_idv[t] = z_id
        if z_active:
            for e in range(el_n):
                if el_sw[e]:
                    swept_out[t] = True
                    break
        trail_lv[t] = last_sl_lv
        trail_cbar[t] = last_sl_cb

    return (trend, zone_top, zone_bot, zone_idv, bos, swept_out, trail_lv, trail_cbar,
            arm_px, arm_stop, arm_zid, arm_ns, arm_sw, cf_ns, cf_sw, cf_stop, cf_zid)


_COLS = ("trend", "zone_top", "zone_bot", "zone_id", "bos", "swept", "trail_lv", "trail_cbar",
         "arm_px", "arm_stop", "arm_zid", "arm_ns", "arm_sw", "cf_ns", "cf_sw", "cf_stop",
         "cf_zid")
_PRICE_COLS = ("zone_top", "zone_bot", "trail_lv", "arm_px", "arm_stop", "cf_stop")


@dataclass(frozen=True)
class StructureParams:
    k: int = 3
    bos_close: bool = True
    base_max: int = 1
    zone_lookback: int = 10
    stop_atr: float = 0.1
    leaky: bool = False


def structure_features(o, h, l, c, atr, tick, params: StructureParams, side: int = 1) -> pd.DataFrame:
    """Run the state machine for ``side`` (+1 long / -1 short) and return real-price outputs."""
    f = lambda x: np.ascontiguousarray(np.asarray(x, dtype=np.float64))
    o, h, l, c, atr = f(o), f(h), f(l), f(c), f(atr)
    tick = f(np.broadcast_to(tick, o.shape))
    if side == -1:
        o, h, l, c = -o, -l, -h, -c
    out = _run(o, h, l, c, atr, tick, int(params.k), bool(params.bos_close), int(params.base_max),
               int(params.zone_lookback), float(params.stop_atr), bool(params.leaky))
    df = pd.DataFrame(dict(zip(_COLS, out)))
    if side == -1:
        for col in _PRICE_COLS:
            df[col] = -df[col]
        df["zone_top"], df["zone_bot"] = df["zone_bot"].to_numpy(), df["zone_top"].to_numpy()
        df["trend"] = -df["trend"]
    return df
