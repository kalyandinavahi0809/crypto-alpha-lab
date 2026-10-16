import pandas as pd

from qdp.layers import Layer


def test_partition_upsert_is_atomic_and_idempotent(store):
    part = {"venue": "v", "symbol": "S", "month": "2024-01"}
    df1 = pd.DataFrame({"a": [1, 2]})
    df2 = pd.DataFrame({"a": [3]})
    p = store.write_partition(Layer.RAW, "ds", part, df1, meta={"checksum": "x"})
    store.write_partition(Layer.RAW, "ds", part, df1, meta={"checksum": "x"})
    assert store.read_partition(Layer.RAW, "ds", part)["a"].tolist() == [1, 2]
    store.write_partition(Layer.RAW, "ds", part, df2, meta={"checksum": "y"})
    assert store.read_partition(Layer.RAW, "ds", part)["a"].tolist() == [3]   # replaced, not appended
    assert store.read_meta(Layer.RAW, "ds", part)["checksum"] == "y"
    assert not [f for f in p.parent.iterdir() if f.name.endswith(".tmp")]   # no temp debris


def test_list_and_read_dataset_with_filters(store):
    for sym in ("A", "B"):
        for m in ("2024-01", "2024-02"):
            store.write_partition(Layer.RAW, "ds", {"symbol": sym, "month": m},
                                  pd.DataFrame({"x": [1]}))
    assert len(store.list_partitions(Layer.RAW, "ds")) == 4
    df = store.read_dataset(Layer.RAW, "ds", {"symbol": "B"})
    assert len(df) == 2 and set(df["symbol"]) == {"B"}


def test_failed_write_leaves_previous_partition(store, monkeypatch):
    part = {"m": "1"}
    store.write_partition(Layer.RAW, "ds", part, pd.DataFrame({"a": [1]}))

    class Boom(pd.DataFrame):
        def to_parquet(self, *a, **k):
            raise RuntimeError("disk full")

    try:
        store.write_partition(Layer.RAW, "ds", part, Boom({"a": [9]}))
    except RuntimeError:
        pass
    assert store.read_partition(Layer.RAW, "ds", part)["a"].tolist() == [1]
