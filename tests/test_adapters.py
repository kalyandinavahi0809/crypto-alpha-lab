import hashlib
import io
import zipfile

import numpy as np
import pytest

from qdp.adapters import ChecksumMismatch, get_adapter
from qdp.layers import Layer
from qdp.adapters.binance_usdm import (BinanceUSDMArchiveAdapter, parse_funding_csv,
                                       parse_kline_csv)

KL_HEADER = ("open_time,open,high,low,close,volume,close_time,quote_volume,count,"
             "taker_buy_volume,taker_buy_quote_volume,ignore\n")
KL_ROWS = ("1704067200000,42314.00,42603.20,42289.60,42503.50,8459.477,1704070799999,1.0,88,1,1,0\n"
           "1704070800000,42503.50,42832.00,42462.00,42647.90,9043.411,1704074399999,1.0,90,1,1,0\n")


def _zip(name, text):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr(name, text)
    return buf.getvalue()


def test_parse_kline_with_and_without_header():
    a = parse_kline_csv((KL_HEADER + KL_ROWS).encode())
    b = parse_kline_csv(KL_ROWS.encode())
    assert a.equals(b)
    assert a["open_time"].tolist() == [1704067200000, 1704070800000]
    assert a["px_decimals"].tolist() == [1, 1]     # "42603.20" -> trailing zero stripped
    assert a["close"].iloc[1] == pytest.approx(42647.9)


def test_parse_spot_microsecond_timestamps():
    row = "1740787200000000,84349.95,86558.00,83824.78,86064.53,1,1740873599999999,1,1,1,1,0\n"
    df = parse_kline_csv(row.encode())
    assert df["open_time"].iloc[0] == 1740787200000
    assert df["px_decimals"].iloc[0] == 2


def test_parse_funding_formats():
    new = "calc_time,funding_interval_hours,last_funding_rate\n1704067200000,8,0.00037409\n"
    df = parse_funding_csv(new.encode())
    assert df.iloc[0].tolist() == [1704067200000, 0.00037409, 8.0]
    legacy = "1577836800000,-0.00012359\n"     # no interval column: unknown, not assumed
    df2 = parse_funding_csv(legacy.encode())
    assert np.isnan(df2["funding_interval_hours"].iloc[0])


def test_fetch_month_verifies_checksum():
    payload = _zip("BTCUSDT-1h-2024-01.csv", KL_HEADER + KL_ROWS)
    good = hashlib.sha256(payload).hexdigest()
    store = {}

    def http(url):
        if url.endswith(".CHECKSUM"):
            return f"{store['ck']}  file.zip".encode()
        return payload

    ad = BinanceUSDMArchiveAdapter(http_get=http, rest_base=None)    # archive-only
    store["ck"] = good
    res = ad.fetch_month("klines", "BTCUSDT", "2024-01", "1h")
    assert res.verified and res.checksum == good and len(res.frame) == 2
    assert "futures/um/monthly/klines/BTCUSDT/1h/BTCUSDT-1h-2024-01.zip" in res.source
    store["ck"] = "0" * 64
    with pytest.raises(ChecksumMismatch):
        ad.fetch_month("klines", "BTCUSDT", "2024-01", "1h")


def test_listing_parses_months_and_paginates():
    pages = [
        "<IsTruncated>true</IsTruncated><Key>p/BTCUSDT-1h-2020-01.zip</Key>"
        "<Key>p/BTCUSDT-1h-2020-01.zip.CHECKSUM</Key>",
        "<IsTruncated>false</IsTruncated><Key>p/BTCUSDT-1h-2020-02.zip</Key>",
    ]
    calls = []

    def http(url):
        calls.append(url)
        return pages[len(calls) - 1].encode()

    ad = BinanceUSDMArchiveAdapter(http_get=http)
    assert ad.available_months("klines", "BTCUSDT", "1h") == ["2020-01", "2020-02"]
    assert "marker=" in calls[1]


def test_registry_is_venue_agnostic():
    assert get_adapter("binance_usdm").venue == "binance_usdm"
    with pytest.raises(KeyError):
        get_adapter("hyperliquid")


def _rest_fake(archive_months=("2020-01",)):
    import json as _json

    def http(url):
        if url.endswith("/exchangeInfo"):
            return _json.dumps({"symbols": [{"symbol": "BTCUSDT", "contractType": "PERPETUAL",
                                             "onboardDate": 1567965300000, "filters": [
                                                 {"filterType": "PRICE_FILTER", "tickSize": "0.10"},
                                                 {"filterType": "LOT_SIZE", "stepSize": "0.001"}]}]}).encode()
        if "s3-ap-northeast-1" in url:
            return "".join(f"<Key>p/BTCUSDT-1h-{m}.zip</Key>" for m in archive_months).encode()
        if "/klines?" in url:
            start = int(url.split("startTime=")[1].split("&")[0])
            if start >= 1569888000000:          # only September has bars in this fake
                return b"[]"
            return _json.dumps([[start + i * 3600000, "10000.5", "10001", "9999", "10000.25", "1",
                                 start + i * 3600000 + 3599999, "1", 1, "0", "0", "0"]
                                for i in range(3)]).encode()
        if "/markPriceKlines?" in url:
            return b"[]"                         # mark history starts later than listing
        if "/fundingRate?" in url:
            return _json.dumps([{"symbol": "BTCUSDT", "fundingTime": 1568102400000,
                                 "fundingRate": "0.00010000"}]).encode()
        raise AssertionError(url)
    return http


def test_rest_backfill_months_from_listing_to_archive_start():
    ad = BinanceUSDMArchiveAdapter(http_get=_rest_fake())
    assert ad.available_months("klines", "BTCUSDT", "1h") == ["2019-09", "2019-10", "2019-11",
                                                              "2019-12", "2020-01"]
    # spot is not backfilled from the futures REST API
    assert ad.available_months("spot_klines", "BTCUSDT", "1d") == ["2020-01"]
    res = ad.fetch_month("klines", "BTCUSDT", "2019-09", "1h")
    assert not res.verified and "www.binance.com/fapi/v1/klines" in res.source
    assert len(res.frame) == 3 and res.frame["px_decimals"].tolist() == [2, 2, 2]
    assert res.checksum == ad.fetch_month("klines", "BTCUSDT", "2019-09", "1h").checksum
    f = ad.fetch_month("funding", "BTCUSDT", "2019-09").frame
    assert f.iloc[0]["funding_rate"] == 1e-4 and np.isnan(f.iloc[0]["funding_interval_hours"])
    assert ad.remote_checksum("klines", "BTCUSDT", "2019-09", "1h") is None
    assert ad.exchange_info()[0]["tick_size"] == 0.1


def test_ingest_skips_empty_rest_months(store):
    from qdp.ingest import ingest_series
    ad = BinanceUSDMArchiveAdapter(http_get=_rest_fake(archive_months=("2019-11",)))
    rep = ingest_series(store, ad, "mark_klines", "BTCUSDT", "1h", end_month="2019-10", workers=1)
    assert rep["action"].tolist() == ["empty", "empty"]
    assert store.list_partitions(Layer.RAW, "mark_klines") == []
    rep = ingest_series(store, ad, "klines", "BTCUSDT", "1h", end_month="2019-10", workers=1)
    assert rep["action"].tolist() == ["written", "empty"]
    rep = ingest_series(store, ad, "klines", "BTCUSDT", "1h", end_month="2019-10", workers=1)
    assert rep["action"].tolist() == ["skipped", "empty"]       # idempotent via frame digest


def test_margin_brackets_parsed_from_venue_payload():
    import json as _json
    payload = {"data": {"brackets": [
        {"symbol": "BTCUSDT", "updateTime": 1, "riskBrackets": [
            {"bracketSeq": 2, "bracketNotionalFloor": 300000, "bracketMaintenanceMarginRate": 0.005,
             "cumFastMaintenanceAmount": 300},
            {"bracketSeq": 1, "bracketNotionalFloor": 0, "bracketMaintenanceMarginRate": 0.004,
             "cumFastMaintenanceAmount": 0}]},
        {"symbol": "OTHERUSDT", "updateTime": 1, "riskBrackets": []}]}}
    ad = BinanceUSDMArchiveAdapter(http_get=lambda url: _json.dumps(payload).encode())
    out = ad.margin_brackets(["BTCUSDT"])
    assert list(out) == ["BTCUSDT"]
    assert out["BTCUSDT"]["brackets"] == [(0.0, 0.004, 0.0), (300000.0, 0.005, 300.0)]
