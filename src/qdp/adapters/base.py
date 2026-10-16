"""Venue-agnostic market-data adapter interface.

An adapter turns a venue's historical data into the platform's canonical RAW schemas,
one monthly partition at a time. Swapping venues (e.g. a US-accessible venue such as
Hyperliquid) means writing another adapter; ingest/clean/research code is unchanged.

Canonical RAW schemas (all timestamps int64 ms UTC):
  klines / mark_klines / spot_klines:
      open_time, open, high, low, close, volume, quote_volume, trades, px_decimals
  funding:
      funding_time, funding_rate, funding_interval_hours (float, NaN if venue omits it)
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass

import pandas as pd

DATASETS = ("klines", "mark_klines", "funding", "spot_klines")
KLINE_COLUMNS = ["open_time", "open", "high", "low", "close", "volume", "quote_volume",
                 "trades", "px_decimals"]
FUNDING_COLUMNS = ["funding_time", "funding_rate", "funding_interval_hours"]


@dataclass(frozen=True)
class FetchResult:
    frame: pd.DataFrame
    checksum: str          # sha256 of the source payload as delivered by the venue
    source: str            # URL / identifier of the source object
    verified: bool         # True if the checksum matched a venue-published checksum


class VenueAdapter(ABC):
    venue: str
    spot_venue: str

    @abstractmethod
    def available_months(self, dataset: str, symbol: str, interval: str | None = None) -> list[str]:
        """Sorted list of YYYY-MM partitions the venue publishes for this series."""

    @abstractmethod
    def fetch_month(self, dataset: str, symbol: str, month: str,
                    interval: str | None = None) -> FetchResult:
        """Fetch one monthly partition in the canonical schema."""


class ChecksumMismatch(RuntimeError):
    pass
