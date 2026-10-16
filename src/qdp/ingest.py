"""Idempotent ingest of venue data into the RAW layer.

One RAW partition per (dataset, venue, symbol, interval, month). Each partition carries a
sidecar with the source checksum. Re-running an ingest:
  * skips a partition whose stored checksum equals the venue's current checksum,
  * atomically replaces a partition whose source changed (venue re-publication),
  * never appends, so RAW cannot accumulate duplicates.
"""
from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timezone

import pandas as pd

from .adapters.base import VenueAdapter
from .layers import Layer, Store

log = logging.getLogger(__name__)


def raw_partition(venue: str, symbol: str, interval: str | None, month: str) -> dict[str, str]:
    return {"venue": venue, "symbol": symbol, "interval": interval or "na", "month": month}


def dataset_venue(adapter: VenueAdapter, dataset: str) -> str:
    return adapter.spot_venue if dataset == "spot_klines" else adapter.venue


@dataclass
class IngestResult:
    dataset: str
    symbol: str
    interval: str | None
    month: str
    action: str            # "skipped" | "written" | "replaced" | "empty"
    rows: int
    checksum: str


def ingest_partition(store: Store, adapter: VenueAdapter, dataset: str, symbol: str,
                     month: str, interval: str | None = None, force: bool = False) -> IngestResult:
    venue = dataset_venue(adapter, dataset)
    part = raw_partition(venue, symbol, interval, month)
    meta = store.read_meta(Layer.RAW, dataset, part)
    remote = getattr(adapter, "remote_checksum", None)
    if meta and not force and remote is not None:
        rc = remote(dataset, symbol, month, interval)
        if rc is not None and rc == meta.get("checksum"):
            return IngestResult(dataset, symbol, interval, month, "skipped", meta["rows"], rc)
    res = adapter.fetch_month(dataset, symbol, month, interval)
    if res.frame.empty:
        # e.g. mark-price history starts after listing: nothing published for this month
        return IngestResult(dataset, symbol, interval, month, "empty", 0, res.checksum)
    if meta and not force and meta.get("checksum") == res.checksum:
        return IngestResult(dataset, symbol, interval, month, "skipped", meta["rows"], res.checksum)
    store.write_partition(Layer.RAW, dataset, part, res.frame, meta={
        "checksum": res.checksum, "verified": res.verified, "source": res.source,
        "rows": int(len(res.frame)),
        "ingested_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    })
    action = "replaced" if meta else "written"
    return IngestResult(dataset, symbol, interval, month, action, len(res.frame), res.checksum)


def ingest_series(store: Store, adapter: VenueAdapter, dataset: str, symbol: str,
                  interval: str | None = None, start_month: str | None = None,
                  end_month: str | None = None, force: bool = False,
                  workers: int = 8) -> pd.DataFrame:
    months = adapter.available_months(dataset, symbol, interval)
    months = [m for m in months if (not start_month or m >= start_month)
              and (not end_month or m <= end_month)]

    def one(m):
        return ingest_partition(store, adapter, dataset, symbol, m, interval, force)

    if workers > 1:
        with ThreadPoolExecutor(workers) as ex:
            results = list(ex.map(one, months))
    else:
        results = [one(m) for m in months]
    df = pd.DataFrame([r.__dict__ for r in results])
    if len(df):
        log.info("%s %s %s: %s", dataset, symbol, interval, df["action"].value_counts().to_dict())
    return df
