"""Venue adapter registry. Venue is a config parameter (default ``binance_usdm``)."""
from __future__ import annotations

from .base import DATASETS, ChecksumMismatch, FetchResult, VenueAdapter
from .binance_usdm import BinanceUSDMArchiveAdapter

_REGISTRY: dict[str, type[VenueAdapter]] = {
    BinanceUSDMArchiveAdapter.venue: BinanceUSDMArchiveAdapter,
}


def register_adapter(cls: type[VenueAdapter]) -> type[VenueAdapter]:
    _REGISTRY[cls.venue] = cls
    return cls


def get_adapter(venue: str, **kwargs) -> VenueAdapter:
    try:
        return _REGISTRY[venue](**kwargs)
    except KeyError as exc:
        raise KeyError(f"no adapter registered for venue {venue!r}; "
                       f"available: {sorted(_REGISTRY)}") from exc


__all__ = ["DATASETS", "ChecksumMismatch", "FetchResult", "VenueAdapter",
           "BinanceUSDMArchiveAdapter", "get_adapter", "register_adapter"]
