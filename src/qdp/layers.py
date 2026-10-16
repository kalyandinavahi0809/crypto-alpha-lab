"""Layered parquet store: RAW -> CLEANED -> FEATURES -> SIGNALS -> BACKTESTS.

Every dataset is a set of hive-style partitions. Writes are partition-atomic upserts:
the partition is written to a temporary file in the same directory and moved into place
with ``os.replace`` (atomic on POSIX), so a reader never sees a half-written partition and
re-running an ingest replaces the partition instead of appending duplicates.
"""
from __future__ import annotations

import json
import os
import tempfile
from enum import Enum
from pathlib import Path
from typing import Any, Iterable, Mapping

import pandas as pd


class Layer(str, Enum):
    RAW = "raw"
    CLEANED = "cleaned"
    FEATURES = "features"
    SIGNALS = "signals"
    BACKTESTS = "backtests"


_ORDER = [Layer.RAW, Layer.CLEANED, Layer.FEATURES, Layer.SIGNALS, Layer.BACKTESTS]


def _atomic_write_bytes(path: Path, writer) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    os.close(fd)
    try:
        writer(tmp)
        os.replace(tmp, path)
    except BaseException:
        if os.path.exists(tmp):
            os.remove(tmp)
        raise


class Store:
    """Filesystem-backed layered store."""

    def __init__(self, root: str | Path):
        self.root = Path(root)

    # ------------------------------------------------------------------ paths
    def layer_dir(self, layer: Layer) -> Path:
        return self.root / Layer(layer).value

    def partition_path(self, layer: Layer, dataset: str, partition: Mapping[str, Any],
                       ext: str = "parquet") -> Path:
        if not partition:
            return self.layer_dir(layer) / f"{dataset}.{ext}"
        items = list(partition.items())
        dirs = [f"{k}={v}" for k, v in items[:-1]]
        k, v = items[-1]
        return self.layer_dir(layer).joinpath(dataset, *dirs, f"{k}={v}.{ext}")

    def meta_path(self, layer: Layer, dataset: str, partition: Mapping[str, Any]) -> Path:
        return self.partition_path(layer, dataset, partition, ext="meta.json")

    # ------------------------------------------------------------------ writes
    def write_partition(self, layer: Layer, dataset: str, partition: Mapping[str, Any],
                        df: pd.DataFrame, meta: Mapping[str, Any] | None = None) -> Path:
        """Atomically upsert (replace) one partition. Optional sidecar metadata is written
        *after* the data so that a sidecar always describes a complete partition."""
        path = self.partition_path(layer, dataset, partition)
        _atomic_write_bytes(path, lambda tmp: df.to_parquet(tmp, index=False))
        if meta is not None:
            mpath = self.meta_path(layer, dataset, partition)
            payload = json.dumps(dict(meta), indent=1, sort_keys=True, default=str)
            _atomic_write_bytes(mpath, lambda tmp: Path(tmp).write_text(payload))
        return path

    def write_table(self, layer: Layer, name: str, df: pd.DataFrame) -> Path:
        return self.write_partition(layer, name, {}, df)

    def write_text(self, layer: Layer, relpath: str, text: str) -> Path:
        path = self.layer_dir(layer) / relpath
        _atomic_write_bytes(path, lambda tmp: Path(tmp).write_text(text))
        return path

    # ------------------------------------------------------------------ reads
    def read_meta(self, layer: Layer, dataset: str, partition: Mapping[str, Any]) -> dict | None:
        mpath = self.meta_path(layer, dataset, partition)
        if not mpath.exists() or not self.partition_path(layer, dataset, partition).exists():
            return None
        return json.loads(mpath.read_text())

    def has_partition(self, layer: Layer, dataset: str, partition: Mapping[str, Any]) -> bool:
        return self.partition_path(layer, dataset, partition).exists()

    def read_partition(self, layer: Layer, dataset: str, partition: Mapping[str, Any]) -> pd.DataFrame:
        return pd.read_parquet(self.partition_path(layer, dataset, partition))

    def read_table(self, layer: Layer, name: str) -> pd.DataFrame:
        return self.read_partition(layer, name, {})

    def list_partitions(self, layer: Layer, dataset: str,
                        filters: Mapping[str, Any] | None = None) -> list[dict[str, str]]:
        base = self.layer_dir(layer) / dataset
        if not base.exists():
            return []
        out = []
        for p in sorted(base.rglob("*.parquet")):
            if p.name.startswith("."):
                continue
            rel = p.relative_to(base).with_suffix("")
            parts = dict(seg.split("=", 1) for seg in rel.parts)
            if filters and any(str(parts.get(k)) != str(v) for k, v in filters.items()):
                continue
            out.append(parts)
        return out

    def read_dataset(self, layer: Layer, dataset: str,
                     filters: Mapping[str, Any] | None = None) -> pd.DataFrame:
        parts = self.list_partitions(layer, dataset, filters)
        frames = []
        for part in parts:
            df = self.read_partition(layer, dataset, part)
            for k, v in part.items():
                if k not in df.columns:
                    df[k] = v
            frames.append(df)
        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True)


def downstream(layer: Layer) -> list[Layer]:
    """Layers that depend on ``layer`` (used to document lineage)."""
    i = _ORDER.index(Layer(layer))
    return _ORDER[i + 1:]


def ensure_layers(store: Store, layers: Iterable[Layer] = _ORDER) -> None:
    for layer in layers:
        store.layer_dir(layer).mkdir(parents=True, exist_ok=True)
