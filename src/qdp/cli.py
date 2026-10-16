"""Command line entry points: ``python -m qdp.cli <command>``."""
from __future__ import annotations

import argparse
import logging
import sys

from .adapters import get_adapter
from .clean import run_clean
from .config import data_root, load_yaml, platform_config
from .ingest import ingest_series
from .instruments import InstrumentMaster
from .layers import Store, ensure_layers


def _store() -> Store:
    store = Store(data_root())
    ensure_layers(store)
    return store


def cmd_ingest(args) -> int:
    cfg = load_yaml(args.config)["data"]
    store = _store()
    adapter = get_adapter(cfg.get("venue") or platform_config()["default_venue"])
    workers = platform_config().get("http", {}).get("workers", 8)
    jobs = []
    for sym in cfg["symbols"]:
        for iv in cfg["intervals"]:
            jobs += [("klines", sym, iv), ("mark_klines", sym, iv)]
        jobs += [("funding", sym, None), ("spot_klines", sym, cfg["spot_interval"])]
    for ds, sym, iv in jobs:
        rep = ingest_series(store, adapter, ds, sym, iv, cfg.get("start_month"),
                            cfg.get("end_month"), force=args.force, workers=workers)
        counts = rep["action"].value_counts().to_dict() if len(rep) else {}
        first = rep["month"].min() if len(rep) else None
        print(f"{ds:12s} {sym:9s} {iv or '-':3s} months={len(rep):3d} first={first} {counts}")
    return 0


def cmd_clean(args) -> int:
    cfg = load_yaml(args.config)["data"]
    store = _store()
    adapter = get_adapter(cfg["venue"])
    master = InstrumentMaster.from_yaml()
    try:
        venue_info = adapter.exchange_info() if hasattr(adapter, "exchange_info") else None
    except Exception as exc:  # reference check is best-effort; the archive data stands alone
        logging.warning("venue exchangeInfo unavailable: %s", exc)
        venue_info = None
    try:
        venue_brackets = adapter.margin_brackets(cfg["symbols"]) if hasattr(adapter, "margin_brackets") else None
    except Exception as exc:
        logging.warning("venue margin brackets unavailable, using config snapshot: %s", exc)
        venue_brackets = None
    summary = run_clean(store, master, adapter.venue, adapter.spot_venue, cfg["symbols"],
                        cfg["intervals"], cfg["spot_interval"], venue_info, venue_brackets)
    print(summary.to_string())
    for c in master.change_log:
        print("CONTRACT CHANGE:", c)
    return 0


def main(argv=None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    p = argparse.ArgumentParser(prog="qdp")
    sub = p.add_subparsers(dest="cmd", required=True)
    for name, fn in (("ingest", cmd_ingest), ("clean", cmd_clean)):
        sp = sub.add_parser(name)
        sp.add_argument("--config", default="config/trend_pullback.yaml")
        sp.add_argument("--force", action="store_true")
        sp.set_defaults(fn=fn)
    args = p.parse_args(argv)
    return args.fn(args)


if __name__ == "__main__":
    sys.exit(main())
