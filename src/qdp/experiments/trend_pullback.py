"""Trend Pullback + Liquidity Sweep study on USD-M perpetuals.

    python -m qdp.experiments.trend_pullback [--config config/trend_pullback.yaml]

Pipeline (CLEANED -> FEATURES -> SIGNALS -> BACKTESTS):
  1. grid      every config x symbol x timeframe x side mode, simulated separately on the
               IS window and the OOS window (entries restricted to the window)
  2. select    per timeframe, choose (swing k, exit, entry type) on IS ONLY among the
               hypothesis cells (sweep on, close-BOS, last-price stop), pooled over symbols
  3. report    the selected config once on OOS; ablations; optimistic intrabar; slippage
               sensitivity and 2x stress; funding on/off
  4. validate  anchored walk-forward (12m/3m) over the whole grid; deflated Sharpe over
               the number of configs tried
  5. baselines random entries (1000 seeds), trend-only pullback, buy-and-hold perp/spot/basket
  6. audits    causality on real data (+ leaky meta-test), funding timestamps, intrabar fills;
               leak guard (OOS expectancy > 0.5R or Sharpe > 3)
Everything is written to the BACKTESTS layer; small tables/figures are copied to the report dir.
"""
from __future__ import annotations

import argparse
import json
import logging
import time
from dataclasses import asdict, dataclass, replace
from pathlib import Path

import numpy as np
import pandas as pd

from ..clean import load_master
from ..config import REPO_ROOT, data_root, load_yaml
from ..costs import CostModel
from ..features.build import causality_audit, structure_frame, tick_series
from ..features.structure import StructureParams
from ..grid import interval_ms
from ..layers import Layer, Store
from ..research.benchmarks import (equal_weight_basket, perp_daily_returns, spot_daily_returns,
                                   window_stats)
from ..research.stats import deflated_sharpe, percentile_of
from ..research.walkforward import anchored_folds, walk_forward
from ..signals.trend_pullback import (OrderIntents, random_intents, strategy_intents,
                                      trend_pullback_intents, trend_regime)
from ..sim.engine import EXIT_LIQ, EXIT_STOP, EXIT_TARGET, MarketData, SimParams, \
    prepare_market_data, simulate
from ..sim.metrics import equity_daily, equity_stats, trade_stats

log = logging.getLogger("trend_pullback")
UTC = "UTC"
SIDE_FLAGS = {"long": (True, False), "short": (False, True), "both": (True, True)}


@dataclass(frozen=True)
class Cell:
    k: int
    exit: str          # "1.5" | "2.0" | "3.0" | "trail"
    entry: str         # "limit" | "confirm"
    sweep: bool
    bos_close: bool
    trigger: str       # "last" | "mark"

    @property
    def cid(self) -> str:
        return (f"k{self.k}|x{self.exit}|{self.entry}|{'sweep' if self.sweep else 'nosweep'}|"
                f"{'bosclose' if self.bos_close else 'boswick'}|{self.trigger}")

    @property
    def target_r(self) -> float:
        return 0.0 if self.exit == "trail" else float(self.exit)


def grid_cells(cfg: dict) -> list[Cell]:
    f, s = cfg["features"], cfg["strategy"]
    exits = [("trail" if e == "trail" else f"{float(e):.1f}") for e in s["exits"]]
    return [Cell(k, x, e, sw, bc, tr) for k in f["swing_k"] for bc in f["bos_close"]
            for e in s["entries"] for sw in s["sweep"] for x in exits for tr in s["stop_trigger"]]


def is_hypothesis(c: Cell, cfg: dict) -> bool:
    h = cfg["strategy"]["hypothesis"]
    return c.sweep == h["sweep"] and c.bos_close == h["bos_close"] and c.trigger == h["stop_trigger"]


# ============================================================================ data access
class Universe:
    def __init__(self, cfg: dict, store: Store):
        self.cfg, self.store = cfg, store
        self.venue = cfg["data"]["venue"]
        self.master = load_master(store)
        self.sim_cfg = cfg["simulation"]
        self._md: dict = {}

    def inst(self, sym):
        return self.master.find(self.venue, sym, "perp")

    def cost(self, sym, slip_bps=None) -> CostModel:
        return CostModel.for_instrument(self.inst(sym), slip_bps if slip_bps is not None
                                        else self.sim_cfg["slippage_bps"])

    def market(self, sym: str, tf: str):
        key = (sym, tf)
        if key not in self._md:
            part = {"venue": self.venue, "symbol": sym, "interval": tf}
            bars = self.store.read_partition(Layer.CLEANED, "perp_bars", part)
            funding = self.store.read_partition(Layer.CLEANED, "funding",
                                                {"venue": self.venue, "symbol": sym, "interval": "na"})
            tick = tick_series(self.master, self.venue, sym, bars["open_time"])
            feats = structure_frame(bars, tick, StructureParams(), self.cfg["features"]["atr_n"])
            md = prepare_market_data(bars, funding, tick, feats["atr"].to_numpy(), self.inst(sym))
            self._md[key] = (bars, funding, tick, md)
        return self._md[key]

    def window_bars(self, md: MarketData, start, end) -> tuple[int, int]:
        ms = lambda x: int(pd.Timestamp(x).value // 1_000_000)
        lo = int(np.searchsorted(md.open_time, ms(start))) if start is not None else 0
        hi = int(np.searchsorted(md.open_time, ms(end))) if end is not None else md.n
        return lo, hi


def sim_params(cfg: dict, cell: Cell, side_mode: str, **over) -> SimParams:
    s = cfg["simulation"]
    al, ash = SIDE_FLAGS[side_mode]
    p = SimParams(target_r=cell.target_r, trail_atr=s["trail_atr_buffer"],
                  risk_pct=s["risk_per_trade"], max_leverage=s["max_leverage"],
                  stop_trigger=cell.trigger, optimistic=s["intrabar"] == "optimistic",
                  liq_policy=s["liq_policy"], through_ticks=s["through_ticks"],
                  initial_equity=s["initial_equity"], allow_long=al, allow_short=ash,
                  require_mark=s.get("require_mark_for_entry", True))
    return replace(p, **over)


def summarize(res, md: MarketData, lo: int, hi: int, iv_ms: int, target_r: float) -> dict:
    span_days = max((md.open_time[max(hi - 1, lo)] - md.open_time[lo]) / 86_400_000, 1.0) if hi > lo else 0
    months = span_days / 30.4375
    ts = trade_stats(res.trades, months, iv_ms, target_r if target_r > 0 else None)
    eq = equity_daily(res.equity[lo:hi], md.open_time[lo:hi], iv_ms) if hi > lo else pd.Series(dtype=float)
    es = equity_stats(eq)
    hold = res.trades
    extra = {"avg_leverage": hold["leverage"].mean() if len(hold) else np.nan,
             "target_exits": int((hold["exit_reason"] == EXIT_TARGET).sum()) if len(hold) else 0,
             "stop_exits": int(hold["exit_reason"].isin([EXIT_STOP, 3]).sum()) if len(hold) else 0}
    return {**ts, **es, **res.counters, **extra, "months": months}


# ============================================================================ the study
class Study:
    def __init__(self, cfg: dict, store: Store):
        self.cfg, self.store = cfg, store
        self.u = Universe(cfg, store)
        self.symbols = cfg["data"]["symbols"]
        self.tfs = cfg["data"]["intervals"]
        v = cfg["validation"]
        self.is_end = pd.Timestamp(v["is_end"], tz=UTC)
        self.oos_start = pd.Timestamp(v["oos_start"], tz=UTC)
        self.windows = {"IS": (None, self.is_end), "OOS": (self.oos_start, None)}
        self.cells = grid_cells(cfg)
        self.ds = cfg["output"]["backtests_dataset"]
        self._struct: dict = {}

    # ---------------------------------------------------------------- features / signals
    def structure(self, sym, tf, k, bos_close):
        key = (sym, tf, k, bos_close)
        if key not in self._struct:
            bars, _, tick, _ = self.u.market(sym, tf)
            p = StructureParams(k=k, bos_close=bos_close,
                                base_max=self.cfg["features"]["zone_base_max_bars"],
                                zone_lookback=self.cfg["features"]["zone_lookback_bars"],
                                stop_atr=self.cfg["features"]["stop_atr_buffer"])
            feats = structure_frame(bars, tick, p, self.cfg["features"]["atr_n"])
            self.store.write_partition(Layer.FEATURES, "structure",
                                       {"venue": self.u.venue, "symbol": sym, "interval": tf, "k": k,
                                        "bos": "close" if bos_close else "wick"}, feats)
            struct = {1: feats.filter(like="L_").rename(columns=lambda c: c[2:]),
                      -1: feats.filter(like="S_").rename(columns=lambda c: c[2:])}
            self._struct[key] = (struct, feats["atr"].to_numpy())
        return self._struct[key]

    def intents(self, sym, tf, k, bos_close, entry, sweep) -> OrderIntents:
        struct, _ = self.structure(sym, tf, k, bos_close)
        oi = strategy_intents(struct, entry, sweep)
        bars = self.u.market(sym, tf)[0]
        sig = pd.DataFrame({"open_time": bars["open_time"].to_numpy(),
                            "lim_long_px": oi.lim_long_px, "lim_long_stop": oi.lim_long_stop,
                            "lim_short_px": oi.lim_short_px, "lim_short_stop": oi.lim_short_stop,
                            "mkt_long": oi.mkt_long, "mkt_long_stop": oi.mkt_long_stop,
                            "mkt_short": oi.mkt_short, "mkt_short_stop": oi.mkt_short_stop})
        active = sig[["lim_long_px", "lim_short_px"]].notna().any(axis=1) | sig["mkt_long"] | sig["mkt_short"]
        self.store.write_partition(Layer.SIGNALS, "trend_pullback",
                                   {"venue": self.u.venue, "symbol": sym, "interval": tf, "k": k,
                                    "bos": "close" if bos_close else "wick", "entry": entry,
                                    "sweep": int(sweep)}, sig[active.to_numpy()])
        return oi

    # ---------------------------------------------------------------- 1. grid
    def run_grid(self) -> tuple[pd.DataFrame, pd.DataFrame]:
        rows, trades = [], []
        t0 = time.time()
        for tf in self.tfs:
            ivms = interval_ms(tf)
            for sym in self.symbols:
                _, _, _, md = self.u.market(sym, tf)
                wins = {w: self.u.window_bars(md, *self.windows[w]) for w in self.windows}
                for k in self.cfg["features"]["swing_k"]:
                    for bc in self.cfg["features"]["bos_close"]:
                        for entry in self.cfg["strategy"]["entries"]:
                            for sw in self.cfg["strategy"]["sweep"]:
                                oi = self.intents(sym, tf, k, bc, entry, sw)
                                for cell in [c for c in self.cells if (c.k, c.bos_close, c.entry, c.sweep) == (k, bc, entry, sw)]:
                                    for sm in self.cfg["strategy"]["side_modes"]:
                                        for w, (lo, hi) in wins.items():
                                            res = simulate(md, oi, sim_params(self.cfg, cell, sm),
                                                           self.u.cost(sym), lo, hi)
                                            row = {"tf": tf, "symbol": sym, "cid": cell.cid,
                                                   "side_mode": sm, "window": w, **asdict(cell),
                                                   **summarize(res, md, lo, hi, ivms, cell.target_r)}
                                            rows.append(row)
                                            if len(res.trades):
                                                t = res.trades[["side", "entry_time", "exit_time", "entry_bar",
                                                                "exit_bar", "r_multiple", "r_ex_funding",
                                                                "net_pnl", "funding", "fees", "slippage",
                                                                "gross_pnl", "exit_reason", "entry_px",
                                                                "stop0", "risk_amt"]].copy()
                                                t["tf"], t["symbol"], t["cid"] = tf, sym, cell.cid
                                                t["side_mode"], t["window"] = sm, w
                                                trades.append(t)
                log.info("grid %s %s done (%.0fs)", tf, sym, time.time() - t0)
        grid = pd.DataFrame(rows)
        tr = pd.concat(trades, ignore_index=True)
        tr["entry_ts"] = pd.to_datetime(tr["entry_time"], unit="ms", utc=True)
        for c in ("tf", "symbol", "cid", "side_mode", "window"):
            tr[c] = tr[c].astype("category")
        self.store.write_partition(Layer.BACKTESTS, self.ds, {"table": "grid_metrics"}, grid)
        self.store.write_partition(Layer.BACKTESTS, self.ds, {"table": "grid_trades"}, tr)
        return grid, tr

    def run_grid_exec_variants(self) -> pd.DataFrame:
        """Optimistic-intrabar and 2x-slippage runs of EVERY grid cell (combined sides), pooled
        over symbols, so the execution sensitivity is reported for the whole grid."""
        s = self.cfg["simulation"]
        variants = {"optimistic": ({"optimistic": True}, s["slippage_bps"]),
                    "stress_2x": ({}, s["slippage_bps"] * s["stress_slippage_mult"])}
        variants = {k: v for k, v in variants.items() if k in s.get("grid_exec_variants", variants)}
        acc: dict = {}
        for tf in self.tfs:
            for sym in self.symbols:
                _, _, _, md = self.u.market(sym, tf)
                wins = {w: self.u.window_bars(md, *self.windows[w]) for w in self.windows}
                for cell in self.cells:
                    oi = self.intents(sym, tf, cell.k, cell.bos_close, cell.entry, cell.sweep)
                    for vname, (over, slip) in variants.items():
                        for w, (lo, hi) in wins.items():
                            res = simulate(md, oi, sim_params(self.cfg, cell, "both", **over),
                                           self.u.cost(sym, slip), lo, hi)
                            r = res.trades["r_multiple"].to_numpy() if len(res.trades) else np.empty(0)
                            a = acc.setdefault((tf, cell.cid, vname, w), [0.0, 0])
                            a[0] += r.sum()
                            a[1] += len(r)
            log.info("grid exec variants %s done", tf)
        rows = [{"tf": tf, "cid": cid, "variant": v, "window": w, "trades": n,
                 "expectancy_r": (sr / n) if n else np.nan} for (tf, cid, v, w), (sr, n) in acc.items()]
        out = pd.DataFrame(rows)
        self.store.write_partition(Layer.BACKTESTS, self.ds, {"table": "grid_exec_variants"}, out)
        return out

    # ---------------------------------------------------------------- 2. selection (IS only)
    def select(self, trades: pd.DataFrame) -> dict:
        v = self.cfg["validation"]
        cand = [c.cid for c in self.cells if is_hypothesis(c, self.cfg)]
        out = {}
        for tf in self.tfs:
            t = trades[(trades.tf == tf) & (trades.window == "IS") & (trades.side_mode == "both")
                       & trades.cid.isin(cand)]
            g = t.groupby("cid", observed=True)["r_multiple"].agg(["mean", "size"])
            g = g.reindex(cand)
            g["eligible"] = g["size"].fillna(0) >= v["selection_min_trades"]
            ranked = g[g["eligible"]].sort_values(["mean", "size"], ascending=[False, False])
            best = ranked.index[0] if len(ranked) else g.sort_values("size", ascending=False).index[0]
            out[tf] = {"cid": best, "is_expectancy_r": float(g.loc[best, "mean"]),
                       "is_trades": int(g.loc[best, "size"]),
                       "candidates": g.reset_index().rename(columns={"index": "cid"}).to_dict("records")}
        self.store.write_text(Layer.BACKTESTS, f"{self.ds}/selection.json",
                              json.dumps(out, indent=1, default=float))
        return out

    def cell(self, cid: str) -> Cell:
        return next(c for c in self.cells if c.cid == cid)

    # ---------------------------------------------------------------- 3. selected config
    def pooled_rows(self, grid: pd.DataFrame, trades: pd.DataFrame, cids: list[str],
                    label_map: dict | None = None) -> pd.DataFrame:
        """Pooled (ALL symbols) trade stats + equal-weight portfolio equity stats per
        tf x cid x side_mode x window. Portfolio equity comes from per-symbol daily equity."""
        rows = []
        for (tf, cid, sm, w), t in trades[trades.cid.isin(cids)].groupby(
                ["tf", "cid", "side_mode", "window"], observed=True):
            g = grid[(grid.tf == tf) & (grid.cid == cid) & (grid.side_mode == sm) & (grid.window == w)]
            months = g["months"].max()
            ts = trade_stats(t.assign(exit_reason=t["exit_reason"]), months * len(g) / max(len(g), 1),
                             interval_ms(tf), self.cell(cid).target_r or None)
            ts["trades_per_month"] = len(t) / months if months else np.nan
            rows.append({"tf": tf, "symbol": "ALL", "cid": cid, "side_mode": sm, "window": w,
                         **ts, **{c: g[c].sum() for c in ("liq_rejected", "delevered", "lev_capped",
                                                          "liquidations")}})
        return pd.DataFrame(rows)

    def portfolio_equity(self, sel: dict, side_mode="both", **over) -> dict:
        """Equal-weight daily portfolio of per-symbol strategy equity (selected config)."""
        out = {}
        for tf in self.tfs:
            cell = self.cell(sel[tf]["cid"])
            for w in self.windows:
                rets = {}
                for sym in self.symbols:
                    _, _, _, md = self.u.market(sym, tf)
                    lo, hi = self.u.window_bars(md, *self.windows[w])
                    oi = self.intents(sym, tf, cell.k, cell.bos_close, cell.entry, cell.sweep)
                    res = simulate(md, oi, sim_params(self.cfg, cell, side_mode, **over),
                                   self.u.cost(sym), lo, hi)
                    eq = equity_daily(res.equity[lo:hi], md.open_time[lo:hi], interval_ms(tf))
                    rets[sym] = eq.pct_change()
                out[(tf, w)] = equal_weight_basket(rets).dropna()
        return out

    def variants(self, sel: dict) -> pd.DataFrame:
        """Selected config re-run under execution variants (not in the grid)."""
        s = self.cfg["simulation"]
        base = s["slippage_bps"]
        var = {"optimistic_intrabar": ({"optimistic": True}, base),
               "stress_2x_slippage": ({}, base * s["stress_slippage_mult"])}
        for b in s["slippage_sensitivity_bps"]:
            var[f"slippage_{b:g}bps"] = ({}, b)
        var["liq_policy_reject"] = ({"liq_policy": "reject"}, base)
        rows = []
        for tf in self.tfs:
            cell = self.cell(sel[tf]["cid"])
            for name, (over, slip) in var.items():
                for sm in self.cfg["strategy"]["side_modes"]:
                    for w in self.windows:
                        allt = []
                        agg = {"liq_rejected": 0, "delevered": 0, "lev_capped": 0, "liquidations": 0}
                        months = 0.0
                        for sym in self.symbols:
                            _, _, _, md = self.u.market(sym, tf)
                            lo, hi = self.u.window_bars(md, *self.windows[w])
                            oi = self.intents(sym, tf, cell.k, cell.bos_close, cell.entry, cell.sweep)
                            res = simulate(md, oi, sim_params(self.cfg, cell, sm, **over),
                                           self.u.cost(sym, slip), lo, hi)
                            allt.append(res.trades)
                            for kk in agg:
                                agg[kk] += res.counters[kk]
                            months = max(months, summarize(res, md, lo, hi, interval_ms(tf), cell.target_r)["months"])
                        t = pd.concat(allt, ignore_index=True)
                        ts = trade_stats(t, months, interval_ms(tf), cell.target_r or None)
                        rows.append({"tf": tf, "cid": cell.cid, "variant": name, "side_mode": sm,
                                     "window": w, **ts, **agg})
        return pd.DataFrame(rows)

    # ---------------------------------------------------------------- 4. validation
    def walk_forward(self, trades: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
        wf = self.cfg["validation"]["walk_forward"]
        cand = [c.cid for c in self.cells if is_hypothesis(c, self.cfg)]
        chains, cells = [], []
        for tf in self.tfs:
            t = trades[(trades.tf == tf) & (trades.side_mode == "both")]
            t = t.assign(config_id=t["cid"].astype(str))
            start = t["entry_ts"].min()
            end = t["entry_ts"].max() + pd.Timedelta(days=1)
            folds = anchored_folds(start, end, wf["train_months"], wf["test_months"])
            ch, ce = walk_forward(t, cand, folds, wf["min_train_trades"])
            ch["tf"], ce["tf"] = tf, tf
            ce["hypothesis_cell"] = ce["config_id"].isin(cand)
            chains.append(ch)
            cells.append(ce)
        return pd.concat(chains, ignore_index=True), pd.concat(cells, ignore_index=True)

    def deflated(self, sel: dict, grid_port: dict) -> pd.DataFrame:
        rows = []
        n_total = len(self.cells) * len(self.tfs) * len(self.cfg["strategy"]["side_modes"])
        for tf in self.tfs:
            srs = []
            for cid, r in grid_port[tf].items():
                r = r.dropna()
                srs.append(r.mean() / r.std(ddof=1) if len(r) > 2 and r.std(ddof=1) > 0 else np.nan)
            srs = np.array(srs)
            for w in ("IS", "OOS"):
                r = grid_port[(tf, w, "selected")]
                d = deflated_sharpe(r.to_numpy(), srs, n_trials=len(self.cells))
                rows.append({"tf": tf, "window": w, "cid": sel[tf]["cid"],
                             "configs_in_selection_grid": len(self.cells),
                             "configs_tested_total": n_total,
                             "trial_sr_std_daily": np.nanstd(srs, ddof=1), **d})
        return pd.DataFrame(rows)

    def grid_portfolios(self, sel: dict) -> dict:
        """IS daily portfolio returns for every grid cell (combined sides) -> trial Sharpes."""
        out = {}
        for tf in self.tfs:
            out[tf] = {}
            for cell in self.cells:
                rets = {}
                for sym in self.symbols:
                    _, _, _, md = self.u.market(sym, tf)
                    lo, hi = self.u.window_bars(md, *self.windows["IS"])
                    oi = self.intents(sym, tf, cell.k, cell.bos_close, cell.entry, cell.sweep)
                    res = simulate(md, oi, sim_params(self.cfg, cell, "both"), self.u.cost(sym), lo, hi)
                    rets[sym] = equity_daily(res.equity[lo:hi], md.open_time[lo:hi], interval_ms(tf)).pct_change()
                out[tf][cell.cid] = equal_weight_basket(rets).dropna()
        return out

    # ---------------------------------------------------------------- 5. baselines
    def random_baseline(self, sel: dict, trades: pd.DataFrame, over: dict | None = None,
                        nseeds: int | None = None) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Random-entry baseline. With ``over`` (e.g. optimistic intrabar), the strategy and the
        random runs are both simulated under that execution rule."""
        over = over or {}
        nseeds = nseeds or self.cfg["baselines"]["random_seeds"]
        summ, dist_rows = [], []
        for tf in self.tfs:
            cell = self.cell(sel[tf]["cid"])
            per_seed = {}     # (side) -> arrays of summed R and counts across symbols
            per_side_r = {}   # (side) -> strategy R multiples (all symbols)
            for side, sm in ((1, "long"), (-1, "short")):
                sum_r = np.zeros(nseeds)
                cnt = np.zeros(nseeds)
                strat_r = []
                for sym in self.symbols:
                    if over:
                        _, _, _, md_ = self.u.market(sym, tf)
                        lo_, hi_ = self.u.window_bars(md_, *self.windows["OOS"])
                        oi_ = self.intents(sym, tf, cell.k, cell.bos_close, cell.entry, cell.sweep)
                        st = simulate(md_, oi_, sim_params(self.cfg, cell, sm, **over), self.u.cost(sym),
                                      lo_, hi_).trades
                    else:
                        st = trades[(trades.tf == tf) & (trades.cid == cell.cid) & (trades.side_mode == sm)
                                    & (trades.window == "OOS") & (trades.symbol == sym)]
                    if st.empty:
                        continue
                    strat_r.append(st["r_multiple"].to_numpy())
                    fr = ((st["entry_px"] - st["stop0"]).abs() / st["entry_px"]).to_numpy()
                    _, _, _, md = self.u.market(sym, tf)
                    lo, hi = self.u.window_bars(md, *self.windows["OOS"])
                    struct, _ = self.structure(sym, tf, cell.k, cell.bos_close)
                    trend = trend_regime(struct)
                    elig = np.zeros(md.n, bool)
                    elig[lo:hi] = True
                    p = sim_params(self.cfg, cell, sm, market_entry_as_maker=(cell.entry == "limit"), **over)
                    cost = self.u.cost(sym)
                    trail = strategy_intents(struct, "confirm", True)
                    n_target = len(st)
                    m = n_target
                    seed_r = np.full(nseeds, np.nan)
                    seed_n = np.zeros(nseeds)
                    for seed in range(nseeds):
                        rng = np.random.default_rng([seed, side + 2, self.symbols.index(sym), self.tfs.index(tf)])
                        oi = random_intents(trend, side, fr, m, rng, elig)
                        for a in ("trail_long_lv", "trail_long_cb", "trail_short_lv", "trail_short_cb"):
                            setattr(oi, a, getattr(trail, a))
                        res = simulate(md, oi, p, cost, lo, hi)
                        if seed == 0 and len(res.trades) and len(res.trades) < n_target:
                            # calibrate candidate count so realized trades ~ strategy trades
                            m = int(np.ceil(n_target * n_target / len(res.trades)))
                        r = res.trades["r_multiple"].to_numpy()
                        sum_r[seed] += r.sum()
                        cnt[seed] += len(r)
                        seed_r[seed] = r.mean() if len(r) else np.nan
                        seed_n[seed] = len(r)
                    sr = np.concatenate([strat_r[-1]])
                    summ.append({"tf": tf, "cid": cell.cid, "side": sm, "symbol": sym,
                                 "strategy_trades": len(sr), "strategy_expectancy_r": sr.mean(),
                                 "random_median_r": np.nanmedian(seed_r),
                                 "random_p95_r": np.nanpercentile(seed_r, 95),
                                 "random_mean_trades": seed_n.mean(),
                                 "percentile": percentile_of(sr.mean(), seed_r)})
                if not strat_r:
                    continue
                allr = np.concatenate(strat_r)
                per_side_r[sm] = allr
                pooled = sum_r / np.maximum(cnt, 1)
                per_seed[sm] = (sum_r, cnt)
                summ.append({"tf": tf, "cid": cell.cid, "side": sm, "symbol": "ALL",
                             "strategy_trades": len(allr), "strategy_expectancy_r": allr.mean(),
                             "random_median_r": np.median(pooled), "random_p95_r": np.percentile(pooled, 95),
                             "random_mean_trades": cnt.mean(), "percentile": percentile_of(allr.mean(), pooled)})
                dist_rows += [{"tf": tf, "side": sm, "seed": i, "mean_r": pooled[i], "trades": cnt[i]}
                              for i in range(nseeds)]
            if len(per_seed) == 2:
                s_all = per_seed["long"][0] + per_seed["short"][0]
                c_all = per_seed["long"][1] + per_seed["short"][1]
                pooled = s_all / np.maximum(c_all, 1)
                st = pd.concat([pd.DataFrame({"r_multiple": a}) for a in per_side_r.values()],
                               ignore_index=True)
                summ.append({"tf": tf, "cid": cell.cid, "side": "long+short", "symbol": "ALL",
                             "strategy_trades": len(st), "strategy_expectancy_r": st["r_multiple"].mean(),
                             "random_median_r": np.median(pooled), "random_p95_r": np.percentile(pooled, 95),
                             "random_mean_trades": c_all.mean(),
                             "percentile": percentile_of(st["r_multiple"].mean(), pooled)})
                dist_rows += [{"tf": tf, "side": "long+short", "seed": i, "mean_r": pooled[i],
                               "trades": c_all[i]} for i in range(nseeds)]
        return pd.DataFrame(summ), pd.DataFrame(dist_rows)

    def trend_only_baseline(self, sel: dict, trades: pd.DataFrame) -> pd.DataFrame:
        b = self.cfg["baselines"]
        rows = []
        for tf in self.tfs:
            cell = self.cell(sel[tf]["cid"])
            # same stop geometry: median stop distance of the selected strategy's IS trades in ATRs
            st = trades[(trades.tf == tf) & (trades.cid == cell.cid) & (trades.window == "IS")
                        & (trades.side_mode == "both")]
            atr_units = []
            for sym in self.symbols:
                s = st[st.symbol == sym]
                if s.empty:
                    continue
                _, atr = self.structure(sym, tf, cell.k, cell.bos_close)
                a = atr[np.maximum(s["entry_bar"].to_numpy() - 1, 0)]
                atr_units.append(((s["entry_px"] - s["stop0"]).abs().to_numpy() / a))
            stop_atr = float(np.nanmedian(np.concatenate(atr_units))) if atr_units else 1.5
            for x in b["trend_pullback_atr"]:
                for sm in self.cfg["strategy"]["side_modes"]:
                    for w in self.windows:
                        allt, months = [], 0.0
                        for sym in self.symbols:
                            bars, _, _, md = self.u.market(sym, tf)
                            struct, atr = self.structure(sym, tf, cell.k, cell.bos_close)
                            oi = trend_pullback_intents(bars, atr, trend_regime(struct), x, stop_atr,
                                                        b["trend_pullback_lookback"], struct)
                            lo, hi = self.u.window_bars(md, *self.windows[w])
                            p = sim_params(self.cfg, cell, sm, market_entry_as_maker=(cell.entry == "limit"))
                            res = simulate(md, oi, p, self.u.cost(sym), lo, hi)
                            allt.append(res.trades.assign(symbol=sym))
                            months = max(months, (md.open_time[max(hi - 1, lo)] - md.open_time[lo]) / 86_400_000 / 30.4375)
                        t = pd.concat(allt, ignore_index=True)
                        rows.append({"tf": tf, "baseline": f"trend_only_pullback_{x:g}atr",
                                     "stop_atr": stop_atr, "side_mode": sm, "window": w,
                                     **trade_stats(t, months, interval_ms(tf), cell.target_r or None)})
        return pd.DataFrame(rows)

    def buy_and_hold(self) -> pd.DataFrame:
        rows = []
        spot_venue = "binance_spot"
        perp_r, spot_r = {}, {}
        for sym in self.symbols:
            bars, funding, _, _ = self.u.market(sym, "1h")
            perp_r[sym] = perp_daily_returns(bars, funding, interval_ms("1h"))
            spot = self.store.read_partition(Layer.CLEANED, "spot_klines",
                                             {"venue": spot_venue, "symbol": sym,
                                              "interval": self.cfg["data"]["spot_interval"]})
            spot_r[sym] = spot_daily_returns(spot)
        perp_r["EW_BASKET"] = equal_weight_basket({s: perp_r[s] for s in self.symbols})
        spot_r["EW_BASKET"] = equal_weight_basket({s: spot_r[s] for s in self.symbols})
        for w, (a, b) in self.windows.items():
            for sym in self.symbols + ["EW_BASKET"]:
                start = a if a is not None else perp_r[sym].index[0]
                end = b if b is not None else perp_r[sym].index[-1] + pd.Timedelta(days=1)
                p = window_stats(perp_r[sym], start, end, entry_cost=0.0005)
                s = window_stats(spot_r[sym], start, end, entry_cost=0.001)
                rows.append({"window": w, "symbol": sym, "benchmark": "buy_hold_perp_long_incl_funding", **p})
                rows.append({"window": w, "symbol": sym, "benchmark": "buy_hold_spot", **s})
        return pd.DataFrame(rows)

    # ---------------------------------------------------------------- 6. audits
    def audits(self, cells: list[tuple[str, str, str]]) -> dict:
        """Audit (tf, cid, scope) cells: causality of their features on real data (with the leaky
        meta-test), funding timestamps of every trade, and intrabar fill feasibility."""
        out = {"causality": [], "funding": [], "intrabar": []}
        ca = self.cfg["features"]["causality_audit"]
        done_causal = set()
        for tf, cid, scope in cells:
            cell = self.cell(cid)
            for sym in self.symbols:
                bars, funding, tick, md = self.u.market(sym, tf)
                key = (tf, sym, cell.k, cell.bos_close)
                if key not in done_causal:
                    done_causal.add(key)
                    for leaky in (False, True):
                        p = StructureParams(k=cell.k, bos_close=cell.bos_close, leaky=leaky,
                                            base_max=self.cfg["features"]["zone_base_max_bars"])
                        r = causality_audit(bars, tick, p, ca["bars"], ca["cuts"])
                        out["causality"].append({"tf": tf, "symbol": sym, **r})
                oi = self.intents(sym, tf, cell.k, cell.bos_close, cell.entry, cell.sweep)
                ft = funding["funding_time"].to_numpy(np.int64)
                fr = funding["funding_rate"].to_numpy(float)
                bad_f = bad_i = n = 0
                for w in self.windows:
                    lo, hi = self.u.window_bars(md, *self.windows[w])
                    res = simulate(md, oi, sim_params(self.cfg, cell, "both"), self.u.cost(sym), lo, hi)
                    for tr in res.trades.itertuples():
                        n += 1
                        # funding: recompute from the raw event list (entry < T <= exit)
                        a, b = md.open_time[tr.entry_bar], md.open_time[tr.exit_bar]
                        m = (ft > a) & (ft <= b)
                        bars_idx = np.searchsorted(md.open_time, ft[m])
                        exp = float(np.sum(-tr.qty * md.f_px[bars_idx] * fr[m] * tr.side))
                        nz = np.unique(bars_idx[md.f_rate[bars_idx] != 0.0])
                        if not np.isclose(exp, tr.funding, rtol=1e-9, atol=1e-9) or tr.n_funding != len(nz):
                            bad_f += 1
                        # intrabar: fills must be reachable inside their bar
                        eb, xb = tr.entry_bar, tr.exit_bar
                        tk = md.tick[eb]
                        if tr.entry_type == 0:
                            ok = (md.l[eb] <= tr.entry_ref - tk + 1e-12) if tr.side == 1 else \
                                 (md.h[eb] >= tr.entry_ref + tk - 1e-12)
                            if not ok or (tr.exit_reason == EXIT_TARGET and xb == eb):
                                bad_i += 1
                        else:
                            if tr.entry_ref != md.o[eb]:
                                bad_i += 1
                        if tr.exit_reason == EXIT_TARGET:
                            thr = md.tick[xb]
                            ok = (md.h[xb] >= tr.target + thr - 1e-12) if tr.side == 1 else \
                                 (md.l[xb] <= tr.target - thr + 1e-12)
                            if not ok:
                                bad_i += 1
                        if tr.exit_reason in (EXIT_STOP, 3) and cell.trigger == "last":
                            ok = (md.l[xb] <= tr.exit_ref + 1e-12) if tr.side == 1 else \
                                 (md.h[xb] >= tr.exit_ref - 1e-12)
                            if not ok:
                                bad_i += 1
                out["funding"].append({"tf": tf, "cid": cid, "scope": scope, "symbol": sym,
                                       "trades": n, "mismatches": bad_f})
                out["intrabar"].append({"tf": tf, "cid": cid, "scope": scope, "symbol": sym,
                                        "trades": n, "violations": bad_i})
        return {k: pd.DataFrame(v) for k, v in out.items()}

    def leak_guard(self, grid: pd.DataFrame, trades: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Rows of the whole grid tripping the guard (OOS, >= min trades) and, for cells whose
        POOLED OOS expectancy trips it, how concentrated the result is in a few trades."""
        lg = self.cfg["validation"]["leak_guard"]
        o = grid[(grid.window == "OOS") & (grid.trades >= lg["min_trades"])]
        flagged = o[(o.expectancy_r > lg["max_expectancy_r"]) | (o.sharpe > lg["max_sharpe"])]
        t = trades[(trades.window == "OOS") & (trades.side_mode == "both")]
        pooled = t.groupby(["tf", "cid"], observed=True)["r_multiple"].agg(["mean", "size"]).reset_index()
        hot = pooled[(pooled["mean"] > lg["max_expectancy_r"]) & (pooled["size"] >= lg["min_trades"])]
        conc = []
        for r in hot.itertuples():
            x = np.sort(t[(t.tf == r.tf) & (t.cid == r.cid)]["r_multiple"].to_numpy())[::-1]
            is_t = trades[(trades.window == "IS") & (trades.side_mode == "both") & (trades.tf == r.tf)
                          & (trades.cid == r.cid)]["r_multiple"]
            conc.append({"tf": r.tf, "cid": r.cid, "oos_trades": len(x), "oos_expectancy_r": x.mean(),
                         "oos_t_stat": x.mean() / (x.std(ddof=1) / np.sqrt(len(x))),
                         "top3_r": ", ".join(f"{v:.1f}" for v in x[:3]),
                         "oos_expectancy_ex_top3": x[3:].mean(), "is_expectancy_r": is_t.mean(),
                         "is_trades": len(is_t)})
        return flagged, pd.DataFrame(conc)


def audit_by_cell(st: "Study", aud: dict) -> pd.DataFrame:
    """One row per audited (tf, config): trades checked, funding mismatches, intrabar
    violations, and the causality result for the config's swing k / BOS rule."""
    f = aud["funding"].groupby(["tf", "cid", "scope"], as_index=False)[["trades", "mismatches"]].sum()
    i = aud["intrabar"].groupby(["tf", "cid", "scope"], as_index=False)["violations"].sum()
    out = f.merge(i, on=["tf", "cid", "scope"])
    ca = aud["causality"]
    rows = []
    for r in out.itertuples():
        c = st.cell(r.cid)
        sub = ca[(ca.tf == r.tf) & (ca.k == c.k) & (ca.bos_close == c.bos_close)]
        rows.append({"causal_passed": f"{int(sub.query('leaky == False')['passed'].sum())}/"
                                      f"{int((sub.leaky == False).sum())}",
                     "leaky_meta_failed": f"{int((~sub.query('leaky == True')['passed'].astype(bool)).sum())}/"
                                          f"{int((sub.leaky == True).sum())}"})
    out = pd.concat([out, pd.DataFrame(rows)], axis=1)
    out["all_clean"] = (out["mismatches"] == 0) & (out["violations"] == 0) & \
        out["causal_passed"].map(lambda x: x.split("/")[0] == x.split("/")[1]) & \
        out["leaky_meta_failed"].map(lambda x: x.split("/")[0] == x.split("/")[1])
    order = {"selected": 0, "leak_guard": 1, "grid": 2}
    return out.sort_values(["scope", "tf", "cid"], key=lambda col: col.map(order) if col.name == "scope" else col) \
        .reset_index(drop=True)


def data_audits(cfg: dict, store: Store) -> dict:
    from ..adapters import get_adapter
    from ..data_audit import coverage, funding_consistency, mark_history_probe, resample_consistency
    d = cfg["data"]
    venue, syms = d["venue"], d["symbols"]
    out = {"data_coverage": coverage(store, venue, syms, d["intervals"]),
           "data_resample_check": pd.DataFrame([r for s in syms for r in resample_consistency(store, venue, s)]),
           "data_funding_check": pd.DataFrame([funding_consistency(store, venue, s) for s in syms])}
    cov = out["data_coverage"]
    probes = []
    try:
        ad = get_adapter(venue)
        for s in syms:
            first_k = cov[(cov.symbol == s) & (cov.dataset == "klines") & (cov.interval == "1h")]["first"].iloc[0]
            first_m = cov[(cov.symbol == s) & (cov.dataset == "mark_klines") & (cov.interval == "1h")]["first"].iloc[0]
            if first_m > first_k + pd.Timedelta(days=1):
                months = pd.period_range(first_k.strftime("%Y-%m"), first_m.strftime("%Y-%m"), freq="M")
                probes += mark_history_probe(ad, s, months.strftime("%Y-%m").tolist())
    except Exception as exc:   # live probe is evidence only; never blocks the study
        probes.append({"symbol": "n/a", "month": "n/a", "rows_returned": np.nan, "source": f"probe failed: {exc}"})
    out["data_mark_probe"] = pd.DataFrame(probes)
    return out


# ============================================================================ driver
def run(cfg_path: str = "config/trend_pullback.yaml") -> dict:
    cfg = load_yaml(cfg_path)
    store = Store(data_root())
    st = Study(cfg, store)
    res: dict = {}
    t0 = time.time()
    grid, trades = st.run_grid()
    log.info("grid: %d rows, %d trades (%.0fs)", len(grid), len(trades), time.time() - t0)
    sel = st.select(trades)
    log.info("selected: %s", {tf: v["cid"] for tf, v in sel.items()})
    sel_cids = [sel[tf]["cid"] for tf in st.tfs]

    # ablations: one dimension toggled at a time around each tf's selected cell
    abl = []
    for tf in st.tfs:
        c = st.cell(sel[tf]["cid"])
        for name, alt in (("selected", c), ("sweep_off", replace(c, sweep=False)),
                          ("bos_wick", replace(c, bos_close=False)),
                          ("entry_" + ("confirm" if c.entry == "limit" else "limit"),
                           replace(c, entry="confirm" if c.entry == "limit" else "limit")),
                          ("stop_trigger_mark" if c.trigger == "last" else "stop_trigger_last",
                           replace(c, trigger="mark" if c.trigger == "last" else "last"))):
            abl.append({"tf": tf, "ablation": name, "cid": alt.cid})
    abl = pd.DataFrame(abl)
    pooled = st.pooled_rows(grid, trades, abl["cid"].unique().tolist())
    pooled = pooled.merge(abl, on=["tf", "cid"], how="inner")
    res["pooled"] = pooled
    res["variants"] = st.variants(sel)
    res["grid_exec_variants"] = st.run_grid_exec_variants()
    log.info("variants done (%.0fs)", time.time() - t0)

    gp = st.grid_portfolios(sel)
    port = st.portfolio_equity(sel)
    for (tf, w), r in port.items():
        gp[(tf, w, "selected")] = r
    res["dsr"] = st.deflated(sel, gp)
    res["portfolio_stats"] = pd.DataFrame([{"tf": tf, "window": w, **equity_stats((1 + gp[(tf, w, "selected")]).cumprod())}
                                           for tf in st.tfs for w in st.windows])
    log.info("dsr done (%.0fs)", time.time() - t0)
    res["wf_chain"], res["wf_cells"] = st.walk_forward(trades)
    log.info("walk-forward done (%.0fs)", time.time() - t0)
    res["random"], res["random_dist"] = st.random_baseline(sel, trades)
    log.info("random baseline done (%.0fs)", time.time() - t0)
    res["trend_only"] = st.trend_only_baseline(sel, trades)
    res["buy_hold"] = st.buy_and_hold()
    flagged, conc = st.leak_guard(grid, trades)
    res["leak_guard_rows"], res["leak_guard_concentration"] = flagged, conc
    # audit EVERY grid config (both timeframes); scope marks why a config matters
    lg_cells = {(r.tf, r.cid) for r in flagged[["tf", "cid"]].drop_duplicates().itertuples()}
    audit_cells = []
    for tf in st.tfs:
        for c in st.cells:
            scope = ("selected" if c.cid == sel[tf]["cid"] else
                     "leak_guard" if (tf, c.cid) in lg_cells else "grid")
            audit_cells.append((tf, c.cid, scope))
    aud = st.audits(audit_cells)
    res.update({f"audit_{k}": v for k, v in aud.items()})
    res["audit_by_cell"] = audit_by_cell(st, aud)
    res["random_optimistic"], _ = st.random_baseline(sel, trades, over={"optimistic": True})
    log.info("optimistic random baseline done (%.0fs)", time.time() - t0)
    res.update(data_audits(cfg, store))
    log.info("audits done (%.0fs)", time.time() - t0)

    for name, df in res.items():
        store.write_partition(Layer.BACKTESTS, st.ds, {"table": name}, df)
    res["grid"], res["trades"], res["selection"] = grid, trades, sel
    res["equity_portfolios"] = {k: v for k, v in gp.items() if isinstance(k, tuple)}
    res["_study"] = st
    return res


def main(argv=None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="config/trend_pullback.yaml")
    args = ap.parse_args(argv)
    from .report import write_report_artifacts
    res = run(args.config)
    write_report_artifacts(res, load_yaml(args.config))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
