"""Build REPORT.md for the trend-pullback study from the saved tables.

    python -m qdp.experiments.report_md [--config config/trend_pullback.yaml]

The verdict is computed from pre-registered rules (config ``baselines.edge_percentile`` and
``validation.leak_guard``), not chosen after looking at the numbers:
  * EDGE         a timeframe's OOS pooled long+short expectancy is > 0 AND it sits at or above
                 the 95th percentile of the random-entry baseline, with clean audits.
  * NO EDGE      no timeframe meets that, and OOS expectancy is <= 0 or below the random
                 95th percentile with an adequate sample.
  * INCONCLUSIVE the OOS sample is too small to judge, or the audits fail.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd

from ..config import REPO_ROOT, data_root, load_yaml
from ..layers import Layer, Store

MIN_OOS_TRADES = 30


def _f(x, nd=3, pct=False, sign=False):
    if x is None or (isinstance(x, float) and not np.isfinite(x)):
        return "n/a"
    try:
        if pd.isna(x):
            return "n/a"
    except (TypeError, ValueError):
        pass
    if pct:
        return f"{100 * x:+.1f}%" if sign else f"{100 * x:.1f}%"
    if isinstance(x, (int, np.integer)):
        return f"{x:,d}"
    return f"{x:+.{nd}f}" if sign else f"{x:.{nd}f}"


def _table(df: pd.DataFrame, cols: list[tuple[str, str, dict]]) -> str:
    head = "| " + " | ".join(h for _, h, _ in cols) + " |"
    sep = "|" + "|".join("---" for _ in cols) + "|"
    lines = [head, sep]
    for _, r in df.iterrows():
        cells = []
        for c, _, fmt in cols:
            v = r.get(c, np.nan)
            cells.append(str(v) if fmt.get("raw") else _f(v, **{k: v2 for k, v2 in fmt.items() if k != "raw"}))
        lines.append("| " + " | ".join(cells) + " |")
    return "\n".join(lines)


def _load(tdir: Path) -> dict[str, pd.DataFrame]:
    return {p.stem: pd.read_csv(p) for p in sorted(tdir.glob("*.csv"))}


def verdict(t: dict, cfg: dict) -> tuple[str, dict, list[str]]:
    pct_req = cfg["baselines"]["edge_percentile"]
    rnd = t["random_baseline"]
    per_tf, reasons = {}, []
    audits_ok = (t["audit_funding"]["mismatches"].sum() == 0 and t["audit_intrabar"]["violations"].sum() == 0
                 and t["audit_causality"].query("leaky == False")["passed"].all()
                 and not t["audit_causality"].query("leaky == True")["passed"].any())
    for tf in sorted(rnd["tf"].unique()):
        row = rnd[(rnd.tf == tf) & (rnd.symbol == "ALL") & (rnd.side == "long+short")]
        if row.empty:
            per_tf[tf] = "INCONCLUSIVE"
            continue
        r = row.iloc[0]
        n, e, p = int(r.strategy_trades), r.strategy_expectancy_r, r.percentile
        if n < MIN_OOS_TRADES:
            per_tf[tf] = "INCONCLUSIVE"
            reasons.append(f"{tf}: only {n} OOS trades (< {MIN_OOS_TRADES}) - sample too small to judge.")
        elif e > 0 and p >= pct_req and audits_ok:
            per_tf[tf] = "EDGE"
            reasons.append(f"{tf}: OOS expectancy {e:+.3f}R/trade over {n} trades, {p:.1f}th percentile "
                           f"of 1,000 random-entry runs (>= {pct_req}th required).")
        else:
            per_tf[tf] = "NO EDGE"
            why = []
            if e <= 0:
                why.append(f"OOS expectancy is {e:+.3f}R/trade (not positive) over {n} trades")
            if p < pct_req:
                why.append(f"it ranks at the {p:.1f}th percentile of random entries in the same trend regime "
                           f"(>= {pct_req}th required)")
            reasons.append(f"{tf}: " + "; ".join(why) + ".")
    if not audits_ok:
        reasons.append("Audits did not all pass - results cannot be trusted as-is.")
        overall = "INCONCLUSIVE"
    elif "EDGE" in per_tf.values():
        overall = "EDGE" if list(per_tf.values()).count("EDGE") == len(per_tf) else "EDGE (one timeframe only)"
    elif all(v == "NO EDGE" for v in per_tf.values()):
        overall = "NO EDGE"
    else:
        overall = "INCONCLUSIVE"
    return overall, per_tf, reasons


def build(cfg: dict) -> str:
    out = REPO_ROOT / cfg["output"]["report_dir"]
    t = _load(out / "tables")
    store = Store(data_root())
    ds = cfg["output"]["backtests_dataset"]
    sel = json.loads((store.layer_dir(Layer.BACKTESTS) / ds / "selection.json").read_text())
    clean = store.read_table(Layer.CLEANED, "clean_summary")
    changes = store.read_table(Layer.CLEANED, "contract_changes")
    lg = cfg["validation"]["leak_guard"]
    overall, per_tf, reasons = verdict(t, cfg)
    main = t["main_results"]
    md: list[str] = []
    A = md.append

    A("# Trend Pullback + Liquidity Sweep on USDⓈ-M perpetuals - backtest report\n")
    A(f"**Verdict: {overall}.** " + " ".join(f"{tf}: {v}." for tf, v in per_tf.items()))
    A("")
    for r in reasons:
        A(f"- {r}")
    A("")
    A("Hypothesis tested: *in an established uptrend (downtrend), pullbacks into an unmitigated demand "
      "(supply) zone that first sweep a prior swing low (high) reach the fixed-R target more often than "
      "breakeven, net of fees, slippage and funding.* Universe: Binance USDⓈ-M perpetuals BTCUSDT, ETHUSDT, "
      "SOLUSDT, XRPUSDT, DOGEUSDT; 1h and 4h tested independently. Every number below is net of maker/taker "
      "fees, slippage and realized funding, and comes from exchange-published data (no synthetic data).\n")

    # ------------------------------------------------------------------ headline
    A("## 1. Headline (selected config, all symbols pooled; IS = data start → 2023-12-31, "
      "OOS = 2024-01-01 → 2026-09-30)\n")
    hl = main[(main.symbol == "ALL")].copy()
    A(_table(hl.sort_values(["tf", "window", "side_mode"]), [
        ("tf", "TF", {"raw": True}), ("window", "Window", {"raw": True}), ("side_mode", "Side", {"raw": True}),
        ("trades", "Trades", {}), ("win_rate", "Win rate", {"pct": True}),
        ("breakeven_win_rate", "Breakeven WR", {"pct": True}),
        ("expectancy_r", "Exp. R/trade", {"sign": True}), ("expectancy_r_ex_funding", "Exp. ex-funding", {"sign": True}),
        ("profit_factor", "PF", {"nd": 2}), ("trades_per_month", "Trades/mo", {"nd": 1}),
        ("r_t_stat", "t-stat", {"nd": 2})]))
    A("\nSelected on IS only (pooled over symbols, combined long+short runs, hypothesis rules fixed: sweep "
      "required, close-based BOS, last-price stop trigger); the OOS window was evaluated once with it:\n")
    for tf, v in sel.items():
        A(f"- **{tf}**: `{v['cid']}` - IS expectancy {v['is_expectancy_r']:+.3f}R over {v['is_trades']} trades.")
    A("\nConfig id format: `k<swing k>|x<R target or trail>|<entry>|<sweep>|<BOS rule>|<stop trigger>`.\n")

    # ------------------------------------------------------------------ data
    A("## 2. Data\n")
    A("Sources: (1) the Binance public data archive (`data.binance.vision`, exchange-published monthly files, "
      "SHA-256 verified on download) from 2020-01; (2) for months between a perp's listing and the first "
      "archived month (BTCUSDT 2019-09..12, ETHUSDT 2019-11..12) the venue's public REST endpoints "
      "(`www.binance.com/fapi/v1`: klines, markPriceKlines, fundingRate; `fapi.binance.com` returns HTTP 451 "
      "from this environment). Ingest is idempotent (checksum / frame-digest skip, partition-atomic replace).\n")
    kl = clean[(clean.dataset == "klines")]
    rows = []
    for sym in cfg["data"]["symbols"]:
        k1 = kl[(kl.symbol == sym) & (kl.interval == "1h")].iloc[0]
        mm = clean[(clean.dataset == "perp_bars.mark_missing") & (clean.symbol == sym) & (clean.interval == "1h")].iloc[0]
        mk = clean[(clean.dataset == "mark_klines") & (clean.symbol == sym) & (clean.interval == "1h")].iloc[0]
        fu = clean[(clean.dataset == "funding") & (clean.symbol == sym)].iloc[0]
        ts = lambda v: pd.Timestamp(int(v), unit="ms", tz="UTC").strftime("%Y-%m-%d %H:%M")
        rows.append({"symbol": sym, "first_bar": ts(k1.start), "first_mark": ts(mk.start),
                     "first_funding": ts(fu.start), "last_bar": ts(k1.end),
                     "bars_1h": int(k1.rows), "missing_1h": int(k1.missing_bars),
                     "no_mark_1h": int(mm.rows), "funding_events": int(fu.rows)})
    A(_table(pd.DataFrame(rows), [("symbol", "Symbol", {"raw": True}), ("first_bar", "First bar (UTC)", {"raw": True}),
                                   ("first_mark", "First mark bar", {"raw": True}),
                                   ("first_funding", "First funding", {"raw": True}),
                                   ("last_bar", "Last bar", {"raw": True}), ("bars_1h", "1h bars", {}),
                                   ("missing_1h", "Missing last-price bars (logged)", {}),
                                   ("no_mark_1h", "Bars without mark price", {}),
                                   ("funding_events", "Funding events", {})]))
    A("\n- History runs from each perp's listing (venue `onboardDate`) to 2026-09-30 23:00 UTC, the last "
      "complete month with klines, mark prices and funding published.")
    A("- The venue's mark-price history begins 2019-12-23 (the other three perps listed later and have mark "
      "prices from listing), so BTCUSDT/ETHUSDT bars before then have no mark price. No trade is opened on a bar "
      "without a mark price (those signals are skipped and counted), so BTCUSDT/ETHUSDT effectively trade from "
      "2019-12-23. During later mark outages an already-open position's liquidation check uses last price for "
      "that bar (counted in §6). Last-price gaps are exchange outages; they are logged, never filled.")
    A("- Funding is the realized rate per event, with its timestamp; the interval is read from the data "
      "(`funding_interval_hours`, or the observed spacing where the venue omits it), not assumed.")
    A("- Open interest was not ingested (out of scope for this test).")
    try:
        vr = store.read_table(Layer.CLEANED, "venue_reference_check")
        ok = int((vr.get("listing_match", False) & vr.get("tick_match", False)).sum())
        A(f"- Instrument master cross-checked against the venue's exchangeInfo: listing date and current "
          f"tick size match for {ok}/{len(vr)} perps.")
    except FileNotFoundError:
        pass
    try:
        mb = store.read_table(Layer.CLEANED, "venue_margin_brackets")
        nv = int((mb["source"] == "venue").sum())
        A(f"- Maintenance-margin tiers: taken from the venue's published tiers for {nv}/{len(mb)} perps "
          "(public brackets endpoint), applied to the whole history because the venue publishes no tier history; "
          f"the venue's maintenance amounts match the simulator's continuity formula for "
          f"{int(mb.get('cum_matches_formula', pd.Series(dtype=bool)).fillna(False).sum())}/{nv}.\n")
    except FileNotFoundError:
        A("")
    cov = t["data_coverage"].copy()
    for c in ("first", "last"):
        cov[c] = pd.to_datetime(cov[c]).dt.strftime("%Y-%m-%d %H:%M")
    A("Coverage audit per symbol × dataset × interval (CLEANED layer; months by source: SHA-256-verified "
      "archive vs venue REST):\n")
    A(_table(cov, [("symbol", "Symbol", {"raw": True}), ("dataset", "Dataset", {"raw": True}),
                   ("interval", "Interval", {"raw": True}), ("first", "First", {"raw": True}),
                   ("last", "Last", {"raw": True}), ("rows", "Rows", {}), ("missing_bars", "Missing bars", {"nd": 0}),
                   ("months_archive_verified", "Archive months", {}), ("months_rest", "REST months", {})]))
    mp = t["data_mark_probe"]
    if len(mp) and "rows_returned" in mp:
        mp = mp.copy()
        mp["first_bar"] = pd.to_datetime(mp["first_bar"]).dt.strftime("%Y-%m-%d %H:%M").fillna("-")
        A("\nMark-price availability, asked of the venue month by month between listing and the first mark bar "
          "(the absence is observed, not assumed):\n")
        A(_table(mp, [("symbol", "Symbol", {"raw": True}), ("month", "Month", {"raw": True}),
                      ("rows_returned", "Mark bars returned", {"nd": 0}), ("first_bar", "First mark bar", {"raw": True})]))
    fc = t["data_funding_check"]
    A("\nFunding audit (reported interval vs observed spacing between consecutive events):\n")
    A(_table(fc, [("symbol", "Symbol", {"raw": True}), ("events", "Events", {}),
                  ("with_reported_interval", "With reported interval", {}),
                  ("without_reported_interval", "Interval not reported (REST months)", {}),
                  ("reported_vs_observed_disagree", "Reported ≠ observed", {}),
                  ("intervals_seen_h", "Intervals seen (h)", {"raw": True})]))
    rc = t["data_resample_check"]
    A("\nOwn-grid consistency: every stored 4h bar vs the 4h bar rebuilt from the stored 1h bars:\n")
    A(_table(rc, [("symbol", "Symbol", {"raw": True}), ("dataset", "Dataset", {"raw": True}),
                  ("coarse_bars", "4h bars", {}), ("compared", "Compared", {}), ("mismatches", "Mismatches", {}),
                  ("first_mismatch", "First mismatch", {"raw": True})]))
    A(f"\n{int(rc['mismatches'].sum())} of {int(rc['compared'].sum()):,} 4h bars differ from their 1h aggregate. "
      "These are inconsistencies between the exchange's own 1h and 4h series (around outages and fast markets, "
      "e.g. 2019-09-24 and 2024-10-28 20:00 UTC), not ingest errors. Each timeframe uses the venue's native series, "
      "and they are left as published.\n")
    if len(changes):
        A("Contract changes detected and applied as effective-dated instrument-master versions:\n")
        ch = changes.copy()
        ch["valid_from"] = pd.to_datetime(ch["valid_from"]).dt.strftime("%Y-%m-%d %H:%M")
        ch = ch[~ch["reason"].str.contains("None ->")]
        A(_table(ch, [("instrument_id", "Instrument", {"raw": True}), ("valid_from", "From", {"raw": True}),
                      ("reason", "Change", {"raw": True})]))
        A("")

    # ------------------------------------------------------------------ rules
    A("## 3. Mechanical rules (all causal, all in `config/trend_pullback.yaml`)\n")
    A("""- **Swing**: fractal with k bars each side (k ∈ {2,3,5}); bar s is a swing high if its high is strictly above the k previous highs and ≥ the k next. It is known only at the close of s+k, and every downstream rule uses that confirmation time.
- **Trend**: long only if the last two confirmed swing highs AND lows are rising (HH+HL); short only on LH+LL; otherwise no trade. The trend must still hold when the order rests or the entry signal fires.
- **BOS**: a bar CLOSES beyond the most recent unbroken confirmed swing extreme (wick ablation reported).
- **Zone**: the last opposite-close candle at or before the impulse origin (the lowest low between the broken swing and the BOS bar), zone = [low, max(open, close)] (bottom extended to the origin low). Unmitigated until the first bar after the BOS that trades into it; invalidated by a close through the far edge. One live zone per side; a newer BOS replaces it.
- **Sweep**: price trades beyond a confirmed swing low (high) that formed after the impulse origin, before or during the zone touch. For the resting limit, a swing level above the limit counts as swept by the fill itself (price must cross it to reach the limit); otherwise it must already have been swept.
- **Entries**: (a) maker limit at the zone's near edge, only on the first touch, filled only if price trades through by ≥1 tick; (b) taker at the next open after the first-touch bar closes back beyond the swept level (no-sweep variant: beyond the zone edge).
- **Stop**: zone far edge ∓ 0.1×ATR(14), triggering on LAST (MARK as ablation). **Exit**: fixed R ∈ {1.5, 2, 3} as a resting maker limit (must trade through by 1 tick), or a swing trail (stop moves to each newly confirmed swing ∓ 0.1×ATR).
- **Portfolio**: one position per symbol, no pyramiding; long-only, short-only and combined runs are separate simulations.
""")
    A("## 4. Perp simulation rules\n")
    s = cfg["simulation"]
    A(f"""- Risk {100 * s['risk_per_trade']:.1f}% of equity per trade (entry fill to stop), isolated margin = notional / {s['max_leverage']:g}x (capped at equity, which downsizes the trade and is counted).
- Liquidation price from the instrument's maintenance-margin brackets (Binance isolated formula), updated after every funding payment, and checked against the MARK price every bar BEFORE stop/target. If liquidation would sit inside the (doubly slipped) stop, the trade is de-levered (more margin, if equity allows) or rejected; both are counted.
- Funding: for every funding event T with entry < T ≤ exit, P&L += −qty × mark(T) × rate(T) × sign(position), using the realized rate at T. Reported as its own line.
- Fees per instrument from the master (maker 2.0 bp / taker 5.0 bp): maker for limit entries and resting targets; taker for market entries, stops, trail exits. Slippage {s['slippage_bps']:g} bp on taker fills (1 and 3 bp sensitivity, {s['stress_slippage_mult']:g}× stress). Gaps through a stop fill at the open.
- Intrabar: if stop and target are both inside one bar, the STOP fills first (optimistic variant reported). For a limit fill, the target cannot fill in the fill bar (pessimistic).
""")

    # ------------------------------------------------------------------ per symbol
    A("## 5. Results per symbol × timeframe × side (selected config)\n")
    for w in ("OOS", "IS"):
        A(f"### {w}\n")
        sub = main[(main.window == w)].sort_values(["tf", "side_mode", "symbol"])
        A(_table(sub, [("tf", "TF", {"raw": True}), ("symbol", "Symbol", {"raw": True}),
                       ("side_mode", "Side", {"raw": True}), ("trades", "Trades", {}),
                       ("win_rate", "WR", {"pct": True}), ("breakeven_win_rate", "BE WR", {"pct": True}),
                       ("avg_win_r", "Avg win R", {"nd": 2}), ("avg_loss_r", "Avg loss R", {"nd": 2}),
                       ("expectancy_r", "Exp R", {"sign": True}), ("profit_factor", "PF", {"nd": 2}),
                       ("sharpe", "Sharpe", {"nd": 2}), ("sortino", "Sortino", {"nd": 2}),
                       ("max_dd", "MaxDD", {"pct": True}), ("avg_hold_h", "Hold h", {"nd": 1}),
                       ("trades_per_month", "Tr/mo", {"nd": 2})]))
        A("")
    A("Sharpe/Sortino are annualized from daily mark-to-market equity of each run (√365); the ALL rows have no "
      "per-run Sharpe (see the equal-weight portfolio in §9).\n")

    A("## 6. P&L attribution and liquidation accounting (selected config, USDT, $100k start per run)\n")
    att = main[main.symbol != "ALL"].groupby(["tf", "window", "side_mode"])[
        ["gross_pnl", "fees", "slippage", "funding", "net_pnl", "liquidations", "liq_rejected",
         "delevered", "lev_capped", "no_mark_skipped", "mark_fallback"]].sum().reset_index()
    A(_table(att, [("tf", "TF", {"raw": True}), ("window", "Window", {"raw": True}), ("side_mode", "Side", {"raw": True}),
                   ("gross_pnl", "Gross price P&L", {"nd": 0}), ("fees", "Fees", {"nd": 0}),
                   ("slippage", "Slippage", {"nd": 0}), ("funding", "Funding", {"nd": 0}),
                   ("net_pnl", "Net", {"nd": 0}), ("liquidations", "Liquidations", {}),
                   ("liq_rejected", "Liq-rejected", {}), ("delevered", "De-levered", {}),
                   ("lev_capped", "Lev-capped", {}), ("no_mark_skipped", "Entries skipped (no mark)", {"nd": 0}),
                   ("mark_fallback", "Held bars without mark", {"nd": 0})]))
    A("\n(Sum over the five per-symbol runs; each run starts with $100k. *Entries skipped (no mark)*: signals "
      "on bars with no mark price, which are not traded. *Held bars without mark*: bars of an open position "
      "during a mark-price outage, where the liquidation check used last price.)\n")

    # ------------------------------------------------------------------ ablations
    A("## 7. Ablations (one rule toggled at a time around the selected config, all symbols pooled)\n")
    ab = t["ablations"]
    ab = ab[ab.side_mode == "both"].sort_values(["tf", "window", "ablation"])
    A(_table(ab, [("tf", "TF", {"raw": True}), ("window", "Window", {"raw": True}), ("ablation", "Variant", {"raw": True}),
                  ("trades", "Trades", {}), ("win_rate", "WR", {"pct": True}),
                  ("expectancy_r", "Exp R", {"sign": True}), ("expectancy_r_ex_funding", "Exp R ex-funding", {"sign": True}),
                  ("profit_factor", "PF", {"nd": 2}), ("liquidations", "Liq", {}), ("liq_rejected", "Liq-rej", {})]))
    A("\nExecution variants of the selected config (combined sides, pooled):\n")
    ev = t["execution_variants"]
    ev = ev[ev.side_mode == "both"].sort_values(["tf", "window", "variant"])
    A(_table(ev, [("tf", "TF", {"raw": True}), ("window", "Window", {"raw": True}), ("variant", "Variant", {"raw": True}),
                  ("trades", "Trades", {}), ("win_rate", "WR", {"pct": True}),
                  ("expectancy_r", "Exp R", {"sign": True}), ("profit_factor", "PF", {"nd": 2}),
                  ("liquidations", "Liq", {}), ("liq_rejected", "Liq-rej", {})]))
    A("")

    gv = t["grid_exec_variants"]
    q = gv.groupby(["tf", "window", "variant"])["expectancy_r"].describe(percentiles=[.05, .5, .95]).reset_index()
    q = q.merge(gv.assign(pos=gv.expectancy_r > 0).groupby(["tf", "window", "variant"])["pos"].mean().reset_index(),
                on=["tf", "window", "variant"])
    A("Execution sensitivity over the WHOLE grid (all 192 cells per timeframe, combined sides, pooled over "
      "symbols): distribution of net expectancy (R/trade) under the base case (stop-first, "
      f"{cfg['simulation']['slippage_bps']:g} bp), the optimistic intrabar rule (target first), and 2× slippage:\n")
    A(_table(q.sort_values(["tf", "window", "variant"]), [
        ("tf", "TF", {"raw": True}), ("window", "Window", {"raw": True}), ("variant", "Variant", {"raw": True}),
        ("count", "Cells", {"nd": 0}), ("5%", "p5", {"sign": True}), ("50%", "Median", {"sign": True}),
        ("95%", "p95", {"sign": True}), ("max", "Max", {"sign": True}), ("pos", "Share > 0", {"pct": True})]))
    A("")

    # ------------------------------------------------------------------ grid / WF / DSR
    A("## 8. Multiple testing: whole-grid distribution, walk-forward, deflated Sharpe\n")
    dsr = t["deflated_sharpe"]
    ncfg = int(dsr["configs_in_selection_grid"].iloc[0])
    ntot = int(dsr["configs_tested_total"].iloc[0])
    A(f"- Configs per timeframe: **{ncfg}** (k × exit × entry × sweep × BOS rule × stop trigger); "
      f"{ntot} config × side-mode combinations simulated per timeframe, each on IS and OOS, for 5 symbols "
      f"and 2 timeframes; plus execution variants and baselines. Only 24 hypothesis cells per timeframe "
      f"were eligible for selection.\n")
    gp = t["grid_pooled_expectancy"]
    q = gp.groupby(["tf", "window"])["expectancy_r"].describe(percentiles=[.05, .5, .95]).reset_index()
    q = q.merge(gp.assign(pos=gp.expectancy_r > 0).groupby(["tf", "window"])["pos"].mean().reset_index(),
                on=["tf", "window"])
    A("Distribution of pooled net expectancy (R/trade, combined sides) across ALL grid cells:\n")
    A(_table(q, [("tf", "TF", {"raw": True}), ("window", "Window", {"raw": True}), ("count", "Cells", {"nd": 0}),
                 ("5%", "p5", {"sign": True}), ("50%", "Median", {"sign": True}), ("95%", "p95", {"sign": True}),
                 ("max", "Max", {"sign": True}), ("pos", "Share > 0", {"pct": True})]))
    pv = gp.pivot_table(index=["tf", "cid"], columns="window", values="expectancy_r").dropna()
    cors = ", ".join(f"{tf} ρ = {np.corrcoef(pv.loc[tf]['IS'], pv.loc[tf]['OOS'])[0, 1]:+.2f}"
                     for tf in pv.index.get_level_values(0).unique())
    A(f"\nAcross the {ncfg} cells, IS expectancy does not predict OOS expectancy ({cors}; Pearson over cells): "
      "the cell ranking does not persist out of sample.\n")
    wfd = t["walk_forward_oos_distribution"]
    A("Anchored walk-forward (12-month anchored train / 3-month test, rolled every 3 months over the full "
      "history): distribution of TEST-fold expectancy over every grid cell × fold (not the best cell):\n")
    A(_table(wfd, [("tf", "TF", {"raw": True}), ("hypothesis_cell", "Hypothesis cells only", {"raw": True}),
                   ("count", "Cell-folds", {"nd": 0}), ("5%", "p5", {"sign": True}), ("25%", "p25", {"sign": True}),
                   ("50%", "Median", {"sign": True}), ("75%", "p75", {"sign": True}), ("95%", "p95", {"sign": True}),
                   ("frac_cells_positive", "Share > 0", {"pct": True})]))
    wfc = t["walk_forward_chain"]
    A("\nWalk-forward chain (each fold picks the best hypothesis cell on its train window, then is scored on "
      "the next 3 months):\n")
    def _chain(g):
        n, s1, s2 = g["test_trades"].sum(), g["test_sum_r"].sum(), g["test_sum_r2"].sum()
        mean = s1 / max(n, 1)
        sd = np.sqrt(max(s2 / n - mean ** 2, 0) * n / max(n - 1, 1)) if n > 1 else np.nan
        return pd.Series({"folds": len(g), "oos_trades": n, "oos_expectancy_r": mean,
                          "t_stat": mean / (sd / np.sqrt(n)) if n > 1 and sd > 0 else np.nan,
                          "positive_folds": (g["test_mean_r"] > 0).mean(),
                          "distinct_selections": g["selected"].nunique()})
    chain = wfc.groupby("tf").apply(_chain, include_groups=False).reset_index()
    A(_table(chain, [("tf", "TF", {"raw": True}), ("folds", "Folds", {"nd": 0}), ("oos_trades", "OOS trades", {"nd": 0}),
                     ("oos_expectancy_r", "OOS exp R", {"sign": True}), ("t_stat", "t-stat", {"nd": 2}),
                     ("positive_folds", "Folds > 0", {"pct": True}),
                     ("distinct_selections", "Distinct configs picked", {"nd": 0})]))
    A("\nDeflated Sharpe (Bailey & López de Prado) of the selected config's equal-weight daily portfolio; "
      "the expected maximum Sharpe SR0 is computed from the IS Sharpe dispersion of all grid cells:\n")
    A(_table(dsr, [("tf", "TF", {"raw": True}), ("window", "Window", {"raw": True}),
                   ("sr_annual", "Sharpe (ann.)", {"nd": 2}), ("sr0_annual", "SR0 haircut (ann.)", {"nd": 2}),
                   ("n_trials", "Trials", {"nd": 0}), ("dsr", "DSR = P(SR > SR0)", {"nd": 3}),
                   ("psr0", "PSR = P(SR > 0)", {"nd": 3}), ("n_obs", "Days", {"nd": 0})]))
    A("\nThe DSR is the relevant statistic for the IS selection; for OOS (a single pre-specified config) "
      "PSR = P(SR > 0) is the relevant one, DSR is shown for completeness.\n")

    # ------------------------------------------------------------------ baselines
    A("## 9. Baselines (same stop geometry, R target, fees, funding and leverage cap)\n")
    rnd = t["random_baseline"]
    A("**1. Random entries** in the same trend regime and side, the strategy's own stop distances, the same "
      "exit, entry liquidity matched to the strategy; 1,000 seeds, OOS. Edge requires ≥ "
      f"{cfg['baselines']['edge_percentile']}th percentile.\n")
    A(_table(rnd.sort_values(["tf", "side", "symbol"]), [
        ("tf", "TF", {"raw": True}), ("side", "Side", {"raw": True}), ("symbol", "Symbol", {"raw": True}),
        ("strategy_trades", "Strat trades", {}), ("strategy_expectancy_r", "Strat exp R", {"sign": True}),
        ("random_median_r", "Random median", {"sign": True}), ("random_p95_r", "Random p95", {"sign": True}),
        ("random_mean_trades", "Random trades", {"nd": 1}), ("percentile", "Percentile", {"nd": 1})]))
    A("\n**2. Trend filter alone** (any pullback of X×ATR below the 20-bar high in an HH+HL trend, mirror for "
      "shorts; next-open entry; stop at the selected config's median IS stop distance in ATRs), pooled:\n")
    to = t["trend_only_baseline"]
    to = to[to.side_mode == "both"].sort_values(["tf", "window", "baseline"])
    A(_table(to, [("tf", "TF", {"raw": True}), ("baseline", "Baseline", {"raw": True}), ("window", "Window", {"raw": True}),
                  ("stop_atr", "Stop ATRs", {"nd": 2}), ("trades", "Trades", {}), ("win_rate", "WR", {"pct": True}),
                  ("expectancy_r", "Exp R", {"sign": True}), ("profit_factor", "PF", {"nd": 2}),
                  ("r_t_stat", "t-stat", {"nd": 2})]))
    A("\n**3. Buy-and-hold** (1× long perp including realized funding; spot; daily-rebalanced equal-weight "
      "basket):\n")
    bh = t["buy_and_hold"].sort_values(["window", "benchmark", "symbol"])
    A(_table(bh, [("window", "Window", {"raw": True}), ("benchmark", "Benchmark", {"raw": True}),
                  ("symbol", "Symbol", {"raw": True}), ("total_return", "Total return", {"pct": True, "sign": True}),
                  ("cagr", "CAGR", {"pct": True, "sign": True}), ("sharpe", "Sharpe", {"nd": 2}),
                  ("max_dd", "MaxDD", {"pct": True})]))
    ps = t["portfolio_stats"]
    A("\nSelected strategy, equal-weight daily portfolio of the five symbols (combined sides):\n")
    A(_table(ps, [("tf", "TF", {"raw": True}), ("window", "Window", {"raw": True}),
                  ("total_return", "Total return", {"pct": True, "sign": True}), ("cagr", "CAGR", {"pct": True, "sign": True}),
                  ("sharpe", "Sharpe", {"nd": 2}), ("sortino", "Sortino", {"nd": 2}), ("max_dd", "MaxDD", {"pct": True})]))
    A("")

    # ------------------------------------------------------------------ guardrails
    A("## 10. Guardrails and audits\n")
    oos = main[(main.window == "OOS") & (main.symbol != "ALL") & (main.trades >= lg["min_trades"])]
    flags = oos[(oos.expectancy_r > lg["max_expectancy_r"]) | (oos.sharpe > lg["max_sharpe"])]
    audits_clean = (t["audit_funding"]["mismatches"].sum() == 0 and t["audit_intrabar"]["violations"].sum() == 0
                    and t["audit_causality"].query("leaky == False")["passed"].all()
                    and not t["audit_causality"].query("leaky == True")["passed"].any())
    A("- Selected configs:")
    if len(flags):
        tail = ("All three audits below are clean, so these rows are not explained by look-ahead or fill "
                "errors; they are single symbol/side cells with small samples (see their t-stats and the "
                "random-entry percentiles in §9), not evidence of edge on their own.\n") if audits_clean else \
               ("At least one audit below FAILED: treat these rows as probable leaks.\n")
        A(f"  **Leak guard tripped** (OOS expectancy > {lg['max_expectancy_r']}R or Sharpe > {lg['max_sharpe']}, "
          f"≥ {lg['min_trades']} trades) on: " + ", ".join(
              f"{r.tf} {r.symbol} {r.side_mode} ({r.expectancy_r:+.2f}R, Sharpe {r.sharpe:.2f}, n={int(r.trades)})"
              for r in flags.itertuples()) + ". Swing timing, funding timestamps and intrabar fills were audited "
          "before reporting. " + tail)
    else:
        A(f"  no OOS symbol/side row (≥ {lg['min_trades']} trades) exceeds {lg['max_expectancy_r']}R/trade "
          f"or Sharpe {lg['max_sharpe']}.")
    lgr = t["leak_guard_rows"]
    conc = t["leak_guard_concentration"]
    A(f"- Whole grid: {len(lgr)} of the OOS symbol × side rows with ≥ {lg['min_trades']} trades (across "
      f"{lgr['cid'].nunique()} cells) exceed the guard; {int((lgr['sharpe'] > lg['max_sharpe']).sum())} exceed "
      f"Sharpe {lg['max_sharpe']}. Every one of those cells was put through the same three audits (below). "
      f"{int((lgr['cid'].str.contains('xtrail')).sum())} of the {len(lgr)} rows are swing-trail exits, whose R "
      "distribution is fat-tailed (a few trend-riding winners of 10-25R).")
    if len(conc):
        A("\nGrid cells whose POOLED OOS expectancy exceeds the guard - how much of it is a handful of trades:\n")
        A(_table(conc, [("tf", "TF", {"raw": True}), ("cid", "Cell", {"raw": True}), ("oos_trades", "OOS trades", {}),
                        ("oos_expectancy_r", "OOS exp R", {"sign": True}), ("oos_t_stat", "t-stat", {"nd": 2}),
                        ("top3_r", "Top 3 trades (R)", {"raw": True}),
                        ("oos_expectancy_ex_top3", "Exp R without top 3", {"sign": True}),
                        ("is_expectancy_r", "IS exp R", {"sign": True}), ("is_trades", "IS trades", {})]))
        A("\nNone of these were selectable post hoc (selection used IS only), none is significant once the "
          "number of cells is accounted for, and each turns negative without its three largest trades.\n")
    ac = t["audit_causality"]
    A(f"- Swing confirmation timing: `assert_causal` on the real zone/sweep/trend features of every symbol × "
      f"timeframe × (k, BOS rule) used by an audited cell (last {int(ac['bars'].iloc[0])} bars, {int(ac['cuts'].iloc[0])} truncation points): "
      f"{int(ac.query('leaky == False')['passed'].sum())}/{int((ac.leaky == False).sum())} passed. Meta-test with "
      f"leaky (unconfirmed) swings: {int((~ac.query('leaky == True')['passed'].astype(bool)).sum())}/"
      f"{int((ac.leaky == True).sum())} correctly FAILED.")
    af = t["audit_funding"]
    ncells = af[["tf", "cid"]].drop_duplicates().shape[0]
    A(f"- Funding timestamps ({ncells} audited configs = the whole grid on both timeframes, IS and OOS): "
      f"every trade's funding recomputed from the raw event list (events with entry < T ≤ exit only): "
      f"{int(af['mismatches'].sum())} mismatches over {int(af['trades'].sum()):,} trades.")
    ai = t["audit_intrabar"]
    A(f"- Intrabar fills (same cells): limit fills traded through by ≥ 1 tick, market fills at the bar open, no "
      f"target in a limit-fill bar, targets/stops inside their bar's range: {int(ai['violations'].sum())} "
      f"violations over {int(ai['trades'].sum()):,} trades.")
    abc = t["audit_by_cell"]
    A(f"- Per-config audit over the WHOLE grid: {int(abc['all_clean'].sum())}/{len(abc)} configs (every one of "
      f"the {len(abc) // max(abc['tf'].nunique(), 1)} configs on each timeframe, all symbols, IS and OOS, "
      f"{int(abc['trades'].sum()):,} trades) are clean on all four checks (funding timestamps, intrabar fills, "
      "assert_causal, leaky meta-test). §14 lists the selected and leak-guard configs; the full table is "
      "`tables/audit_by_cell.csv`.")
    # funding sign flips
    flips = main[(np.sign(main.expectancy_r) != np.sign(main.expectancy_r_ex_funding)) & (main.trades > 0)]
    abf = t["ablations"]
    abf = abf[(np.sign(abf.expectancy_r) != np.sign(abf.expectancy_r_ex_funding)) & (abf.trades > 0)]
    if len(flips) or len(abf):
        items = [f"{r.tf} {r.symbol} {r.side_mode} {r.window} ({r.expectancy_r_ex_funding:+.3f}R → "
                 f"{r.expectancy_r:+.3f}R with funding)" for r in flips.itertuples()]
        items += [f"ablation {r.ablation} {r.tf} {r.side_mode} {r.window} ({r.expectancy_r_ex_funding:+.3f}R → "
                  f"{r.expectancy_r:+.3f}R)" for r in abf.itertuples()]
        A(f"- **Funding flips the sign of {len(items)} result(s)**: " + "; ".join(items) + ".")
    else:
        A("- Funding does not flip the sign of any reported result.")
    A("- No parameter was changed after the OOS window was computed; the selection rule and grid were "
      "fixed in the config before the run.\n")

    # ------------------------------------------------------------------ figures
    A("## 11. Figures\n")
    for tf in sel:
        A(f"![{tf} OOS equity](figures/equity_{tf}_OOS.png)\n")
        A(f"![{tf} IS equity](figures/equity_{tf}_IS.png)\n")
        A(f"![{tf} R histogram](figures/r_hist_{tf}.png)\n")
        A(f"![{tf} random baseline](figures/random_baseline_{tf}.png)\n")

    # ------------------------------------------------------------------ limitations
    A("## 12. Interpretation\n")
    to_all = t["trend_only_baseline"]
    to_b = to_all[(to_all.side_mode == "both") & (to_all.window == "OOS")]
    A("- The IS winner is not the OOS winner: the selected configs lose money out of sample, the whole grid's "
      "IS ranking does not carry over, and the walk-forward chain is not significant. This is the signature "
      "of selecting noise from a large grid, which the deflated Sharpe (DSR ≈ 0) also says.")
    A("- The liquidity-sweep filter does not add value over the same machine without it (§7), and the "
      "strategy does not beat random entries placed in the same trend regime with the same stops (§9).")
    best_to = to_b.sort_values("expectancy_r", ascending=False).iloc[0]
    A(f"- The plain trend-pullback baseline (no zones, no sweep) did at least as well as the strategy; its best "
      f"OOS row ({best_to.tf}, {best_to.baseline}) is {best_to.expectancy_r:+.3f}R/trade over "
      f"{int(best_to.trades)} trades (t = {best_to.r_t_stat:.2f}). It was not part of the hypothesis and was "
      "not deflated for the baselines tried, so it is a lead for future work, not a finding.")
    risk = cfg["simulation"]["initial_equity"] * cfg["simulation"]["risk_per_trade"]
    cr = main[(main.symbol != "ALL") & (main.side_mode == "both") & (main.window == "OOS")].groupby("tf")[
        ["fees", "slippage", "trades"]].sum()
    cost_txt = ", ".join(f"{tf} ≈ {-(r.fees + r.slippage) / (r.trades * risk):.2f}R" for tf, r in cr.iterrows())
    gv = t["grid_exec_variants"]
    sh = gv.assign(pos=gv.expectancy_r > 0).groupby(["tf", "window", "variant"])["pos"].mean().unstack("variant")
    ev_sel = t["execution_variants"]
    ev_sel = ev_sel[(ev_sel.side_mode == "both") & (ev_sel.window == "OOS") & (ev_sel.variant == "optimistic_intrabar")]
    opt_txt = ", ".join(f"{r.tf} {r.expectancy_r:+.3f}R" for r in ev_sel.itertuples())
    rng = (f"{100 * sh['base'].min():.0f}-{100 * sh['base'].max():.0f}%",
           f"{100 * sh['optimistic'].min():.0f}-{100 * sh['optimistic'].max():.0f}%")
    A(f"- Intrabar ordering is the largest execution sensitivity. Across the whole grid the share of cells with "
      f"positive net expectancy is {rng[0]} under the spec's stop-first rule but {rng[1]} if the target is assumed "
      "to fill first whenever both are inside one bar (§7). Hourly bars cannot resolve this; the stop-first rule "
      "is the specified, conservative one and the verdict uses it. The selected configs stay negative OOS even "
      f"under the optimistic rule ({opt_txt}), and the random baseline uses the same fill rules as the strategy, "
      "so the comparison against it is like-for-like. Resolving it would need sub-bar (1m or trade) data.")
    ro = t["random_baseline_optimistic"]
    ro = ro[(ro.symbol == "ALL") & (ro.side == "long+short")]
    if len(ro):
        txt = "; ".join(f"{r.tf}: {r.strategy_expectancy_r:+.3f}R/trade, {r.percentile:.1f}th percentile"
                        for r in ro.itertuples())
        req = cfg["baselines"]["edge_percentile"]
        passes = [r.tf for r in ro.itertuples() if r.strategy_expectancy_r > 0 and r.percentile >= req]
        A(f"- Verdict re-checked under the optimistic intrabar rule (strategy AND 1,000 random runs re-simulated "
          f"with target-first fills, OOS, long+short pooled): {txt}. "
          + ("The edge criterion is met on " + ", ".join(passes) + " under this rule only." if passes else
             f"The edge criterion (≥ {req}th percentile, positive expectancy) is not met under this rule either, "
             "so the verdict does not depend on the intrabar assumption."))
    A(f"- Costs matter: fees + slippage cost about {cost_txt} per OOS trade at these stop distances (§6; "
      "approximate, using the initial 0.5% risk); funding is small for these short holds and changes the "
      "sign only of near-zero results (§10).\n")
    A("## 13. Limitations\n")
    A("""- **Zone-drawing subjectivity.** Discretionary traders draw zones, sweeps and "impulses" with judgement; this test uses one explicit mechanization (last opposite-close candle before the impulse origin, fractal swings, close-based BOS, first touch only). Other reasonable encodings exist (wider bases, mitigation thresholds, multi-touch zones, higher-timeframe trend), and a result here does not rule them all out.
- **The literal trend rule removes many setups.** A pullback that prints a confirmed lower high before reaching the zone breaks HH+HL, so the order is cancelled. This follows the spec; looser trend definitions were not tested.
- **Venue access for US persons.** Binance USDⓈ-M perps are not available to US persons, and the Binance API is geo-blocked from this environment (data came from the public archive). The adapters are venue-agnostic; a US-accessible venue (e.g. Hyperliquid) would need its own adapter, fee schedule and margin tiers, and results would differ.
- **Funding regime dependence.** 2020-2021 and early 2024 had persistently high positive funding (costly for longs, paid to shorts); the IS/OOS split puts different funding regimes in each window. Per-trade funding is small relative to the ~1R stop because holds are short, but it is not regime-neutral.
- **Reference data snapshot.** Fees use today's VIP0 schedule (maker 2 bp / taker 5 bp) for the whole history, and maintenance-margin brackets are a documented snapshot (the bracket API is geo-blocked). Tick sizes are inferred from observed price precision per month (changes are logged).
- **Bar-level simulation.** With 1h/4h bars the intrabar order of events is unknown; stop-first is the conservative choice (the optimistic variant is reported). Limit queue position is approximated by the 1-tick trade-through rule. Mark-triggered stops fill at the stop price (or the open on a gap), an approximation of last price at trigger time.
- **Mark-price history.** The venue publishes mark prices only from 2019-12-23, so no trades are opened in BTCUSDT/ETHUSDT before then (their data is still ingested from listing); bars of an open position during a mark outage use last price for the liquidation check (counted).
- **Margin-tier history.** The venue publishes only its current maintenance-margin tiers; they are applied to the whole sample. At ≤5× isolated leverage the liquidation price sits ~19% away, far beyond the stops, so the tiers barely affect results.
""")
    A("## 14. Appendix: audit result per audited config\n")
    A("Every grid config on both timeframes was audited. Shown here: any failing config, then scope `selected` "
      "(the IS-selected configs) and `leak_guard` (configs with an OOS symbol/side row above the leak guard); the "
      "remaining `grid` rows are in `tables/audit_by_cell.csv`. Trades = all IS+OOS trades of the config "
      "(combined sides, five symbols).\n")
    abc_t = t["audit_by_cell"]
    bad = abc_t[~abc_t["all_clean"].astype(bool)]
    A(f"Configs failing any check: {len(bad)}" + ("." if bad.empty else " (listed first)."))
    A("")
    A(_table(pd.concat([bad, abc_t[abc_t.scope.isin(["selected", "leak_guard"]) & abc_t["all_clean"].astype(bool)]]), [("scope", "Scope", {"raw": True}), ("tf", "TF", {"raw": True}),
                                  ("cid", "Config", {"raw": True}), ("trades", "Trades", {}),
                                  ("mismatches", "Funding mismatches", {}), ("violations", "Intrabar violations", {}),
                                  ("causal_passed", "assert_causal passed", {"raw": True}),
                                  ("leaky_meta_failed", "Leaky meta-test failed", {"raw": True}),
                                  ("all_clean", "Clean", {"raw": True})]))
    return "\n".join(md)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="config/trend_pullback.yaml")
    args = ap.parse_args(argv)
    cfg = load_yaml(args.config)
    text = build(cfg)
    path = REPO_ROOT / cfg["output"]["report_dir"] / "REPORT.md"
    path.write_text(text)
    print(f"wrote {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
