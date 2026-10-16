"""Report artifacts for the trend-pullback study: tables (CSV), figures (PNG), REPORT.md."""
from __future__ import annotations

from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

from ..config import REPO_ROOT, data_root  # noqa: E402
from ..layers import Layer, Store  # noqa: E402

# reference categorical palette (light mode), fixed order - color follows the entity
SERIES = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#6250d6", "#e34948"]
SURFACE, INK, INK2, GRID = "#fcfcfb", "#0b0b0b", "#52514e", "#e4e3df"
SYMBOL_COLOR = {s: SERIES[i] for i, s in enumerate(["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "DOGEUSDT"])}


def _style(ax, title):
    ax.set_facecolor(SURFACE)
    ax.set_title(title, color=INK, fontsize=10, loc="left")
    ax.tick_params(colors=INK2, labelsize=8)
    for s in ("top", "right"):
        ax.spines[s].set_visible(False)
    for s in ("left", "bottom"):
        ax.spines[s].set_color(GRID)
    ax.grid(True, color=GRID, linewidth=0.6)
    ax.set_axisbelow(True)


def plot_equity(per_symbol: dict, portfolio: pd.Series | None, title: str, path: Path):
    fig, ax = plt.subplots(figsize=(9, 4.2), facecolor=SURFACE)
    for sym, eq in per_symbol.items():
        if len(eq):
            ax.plot(eq.index, eq.to_numpy() / eq.iloc[0], color=SYMBOL_COLOR.get(sym, SERIES[5]),
                    linewidth=1.4, label=sym)
    if portfolio is not None and len(portfolio):
        ax.plot(portfolio.index, (1 + portfolio).cumprod(), color=INK, linewidth=2.0,
                label="equal-weight portfolio")
    ax.axhline(1.0, color=INK2, linewidth=0.8, linestyle=":")
    _style(ax, title)
    ax.set_ylabel("equity (start = 1.0)", color=INK2, fontsize=8)
    ax.legend(frameon=False, fontsize=8, ncol=3, labelcolor=INK)
    fig.tight_layout()
    fig.savefig(path, dpi=130, facecolor=SURFACE)
    plt.close(fig)


def plot_r_hist(r_is: np.ndarray, r_oos: np.ndarray, title: str, path: Path):
    fig, axes = plt.subplots(1, 2, figsize=(9, 3.4), sharex=True, facecolor=SURFACE)
    lo = np.floor(min(np.nanmin(r_is, initial=-1), np.nanmin(r_oos, initial=-1)))
    hi = np.ceil(max(np.nanmax(r_is, initial=1), np.nanmax(r_oos, initial=1)))
    bins = np.arange(lo, hi + 0.25, 0.25)
    for ax, r, name, col in ((axes[0], r_is, "IS", SERIES[0]), (axes[1], r_oos, "OOS", SERIES[1])):
        if len(r):
            ax.hist(r, bins=bins, color=col, edgecolor=SURFACE, linewidth=1.0)
            ax.axvline(np.mean(r), color=INK, linewidth=1.5)
            ax.text(np.mean(r), ax.get_ylim()[1] * 0.92, f" mean {np.mean(r):+.3f}R (n={len(r)})",
                    color=INK, fontsize=8)
        ax.axvline(0, color=INK2, linewidth=0.8, linestyle=":")
        _style(ax, f"{title} - {name}")
        ax.set_xlabel("net R per trade", color=INK2, fontsize=8)
    fig.tight_layout()
    fig.savefig(path, dpi=130, facecolor=SURFACE)
    plt.close(fig)


def plot_random(dist: pd.DataFrame, strat: float, title: str, path: Path):
    fig, ax = plt.subplots(figsize=(6.5, 3.2), facecolor=SURFACE)
    v = dist["mean_r"].to_numpy()
    ax.hist(v, bins=40, color=SERIES[0], edgecolor=SURFACE, linewidth=0.8)
    ax.axvline(np.percentile(v, 95), color=INK2, linewidth=1.0, linestyle="--")
    ax.text(np.percentile(v, 95), ax.get_ylim()[1] * 0.9, " 95th pct", color=INK2, fontsize=8)
    ax.axvline(strat, color=INK, linewidth=2.0)
    ax.text(strat, ax.get_ylim()[1] * 0.75, f" strategy {strat:+.3f}R", color=INK, fontsize=8)
    _style(ax, title)
    ax.set_xlabel("OOS expectancy (net R/trade) of 1,000 random-entry runs", color=INK2, fontsize=8)
    fig.tight_layout()
    fig.savefig(path, dpi=130, facecolor=SURFACE)
    plt.close(fig)


def _equity_by_symbol(study_res: dict, tf: str, window: str, store: Store, cfg: dict):
    """Re-simulate the selected config per symbol (combined sides) for the equity figure."""
    from .trend_pullback import Study, sim_params
    from ..sim.engine import simulate
    from ..sim.metrics import equity_daily
    from ..grid import interval_ms
    st = study_res["_study"]
    cell = st.cell(study_res["selection"][tf]["cid"])
    out = {}
    for sym in st.symbols:
        _, _, _, md = st.u.market(sym, tf)
        lo, hi = st.u.window_bars(md, *st.windows[window])
        oi = st.intents(sym, tf, cell.k, cell.bos_close, cell.entry, cell.sweep)
        res = simulate(md, oi, sim_params(cfg, cell, "both"), st.u.cost(sym), lo, hi)
        out[sym] = equity_daily(res.equity[lo:hi], md.open_time[lo:hi], interval_ms(tf))
    return out


def write_report_artifacts(res: dict, cfg: dict) -> Path:
    out = REPO_ROOT / cfg["output"]["report_dir"]
    (out / "tables").mkdir(parents=True, exist_ok=True)
    (out / "figures").mkdir(parents=True, exist_ok=True)
    store = Store(data_root())
    ds = cfg["output"]["backtests_dataset"]
    sel = res["selection"]
    grid, trades = res["grid"], res["trades"]

    # ---- main results table: selected config per symbol x tf x side x window
    sel_cids = {tf: v["cid"] for tf, v in sel.items()}
    main = pd.concat([grid[(grid.tf == tf) & (grid.cid == cid)] for tf, cid in sel_cids.items()])
    pooled = res["pooled"]
    pooled_sel = pooled[pooled.ablation == "selected"]
    cols = ["tf", "symbol", "side_mode", "window", "cid", "trades", "win_rate", "breakeven_win_rate",
            "nominal_breakeven_win_rate", "avg_win_r", "avg_loss_r", "expectancy_r",
            "expectancy_r_ex_funding", "profit_factor", "sharpe", "sortino", "max_dd", "avg_hold_h",
            "trades_per_month", "gross_pnl", "fees", "slippage", "funding", "net_pnl",
            "liquidations", "liq_rejected", "delevered", "lev_capped", "no_mark_skipped", "mark_fallback",
            "r_t_stat"]
    main_t = pd.concat([main, pooled_sel], ignore_index=True)
    main_t = main_t[[c for c in cols if c in main_t.columns]].sort_values(
        ["tf", "window", "side_mode", "symbol"])
    tables = {"main_results": main_t, "ablations": pooled, "execution_variants": res["variants"],
              "deflated_sharpe": res["dsr"], "portfolio_stats": res["portfolio_stats"],
              "walk_forward_chain": res["wf_chain"], "random_baseline": res["random"],
              "trend_only_baseline": res["trend_only"], "buy_and_hold": res["buy_hold"],
              "audit_causality": res["audit_causality"], "audit_funding": res["audit_funding"],
              "audit_intrabar": res["audit_intrabar"],
              "leak_guard_rows": res["leak_guard_rows"][["tf", "symbol", "cid", "side_mode", "trades",
                                                         "expectancy_r", "sharpe", "r_t_stat"]],
              "leak_guard_concentration": res["leak_guard_concentration"],
              "audit_by_cell": res["audit_by_cell"], "random_baseline_optimistic": res["random_optimistic"],
              "data_coverage": res["data_coverage"], "data_resample_check": res["data_resample_check"],
              "data_funding_check": res["data_funding_check"], "data_mark_probe": res["data_mark_probe"]}
    wfc = res["wf_cells"]
    tables["walk_forward_oos_distribution"] = (
        wfc.groupby(["tf", "hypothesis_cell"])["test_mean_r"]
        .describe(percentiles=[.05, .25, .5, .75, .95]).reset_index()
        .merge(wfc.assign(pos=wfc["test_mean_r"] > 0).groupby(["tf", "hypothesis_cell"])["pos"]
               .mean().rename("frac_cells_positive").reset_index(), on=["tf", "hypothesis_cell"]))
    # whole grid, pooled over symbols (combined sides): the distribution, not the best cell
    gt = trades[trades.side_mode == "both"].groupby(["tf", "window", "cid"], observed=True)["r_multiple"] \
        .agg(["mean", "size"]).reset_index().rename(columns={"mean": "expectancy_r", "size": "trades"})
    tables["grid_pooled_expectancy"] = gt
    gv = res["grid_exec_variants"]
    allv = pd.concat([gt.assign(variant="base")[["tf", "cid", "variant", "window", "trades", "expectancy_r"]], gv],
                     ignore_index=True)
    tables["grid_exec_variants"] = allv
    for name, df in tables.items():
        df.to_csv(out / "tables" / f"{name}.csv", index=False, float_format="%.6g")

    # ---- figures (also stored in the BACKTESTS layer)
    figs = []
    eqp = res["equity_portfolios"]
    for tf in sel_cids:
        for w in ("IS", "OOS"):
            per = _equity_by_symbol(res, tf, w, store, cfg)
            p = out / "figures" / f"equity_{tf}_{w}.png"
            plot_equity(per, eqp.get((tf, w, "selected")),
                        f"{tf} {w}: selected config {sel_cids[tf]} (long+short, net of costs+funding)", p)
            figs.append(p)
        t = trades[(trades.tf == tf) & (trades.cid == sel_cids[tf]) & (trades.side_mode == "both")]
        p = out / "figures" / f"r_hist_{tf}.png"
        plot_r_hist(t[t.window == "IS"]["r_multiple"].to_numpy(), t[t.window == "OOS"]["r_multiple"].to_numpy(),
                    f"{tf} R-multiples", p)
        figs.append(p)
        rd = res["random_dist"]
        rr = res["random"]
        row = rr[(rr.tf == tf) & (rr.symbol == "ALL") & (rr.side == "long+short")]
        if len(row):
            p = out / "figures" / f"random_baseline_{tf}.png"
            plot_random(rd[(rd.tf == tf) & (rd.side == "long+short")], float(row["strategy_expectancy_r"].iloc[0]),
                        f"{tf}: strategy vs random entries (OOS, long+short pooled)", p)
            figs.append(p)
    for p in figs:
        store.write_text(Layer.BACKTESTS, f"{ds}/figures/.keep", "")
        target = store.layer_dir(Layer.BACKTESTS) / ds / "figures" / p.name
        target.write_bytes(p.read_bytes())
    return out
