"""Binance USD-M perpetuals adapter: public data archive + REST backfill.

Primary source is the public archive (data.binance.vision): exchange-published monthly zip
files with SHA-256 checksums, verified on every download. The archive starts in 2020-01, but
BTCUSDT/ETHUSDT were listed in 2019, so months between a perp's listing (``onboardDate`` from
exchangeInfo) and the first archived month are backfilled from the venue's public REST API
(same exchange data, paginated JSON; no published checksum, so ``verified=False`` and the
sha256 of the canonical frame is stored for idempotent re-ingest). ``fapi.binance.com`` is
geo-restricted from some locations (HTTP 451); the same public endpoints are served under
``www.binance.com/fapi/v1``, which is the default REST base here.

Datasets:
  klines       -> futures/um klines            (last-price OHLCV; signals and fills)
  mark_klines  -> futures/um markPriceKlines   (mark price; liquidation / mark-trigger checks)
  funding      -> futures/um fundingRate       (realized funding rate + interval per event)
  spot_klines  -> spot klines                  (spot benchmark only)
"""
from __future__ import annotations

import hashlib
import io
import json
import re
import time
import zipfile
from typing import Callable
from urllib.parse import quote

import numpy as np
import pandas as pd
import requests

from .base import (FUNDING_COLUMNS, KLINE_COLUMNS, ChecksumMismatch, FetchResult,
                   VenueAdapter)

ARCHIVE = "https://data.binance.vision"
REST_BASE = "https://www.binance.com/fapi/v1"
# public (no API key) maintenance-margin tiers served by the venue's web API; the
# fapi/v1/leverageBracket endpoint requires a signed request
BRACKETS_URL = "https://www.binance.com/bapi/futures/v1/friendly/future/common/brackets"
_REST_ENDPOINT = {"klines": "klines", "mark_klines": "markPriceKlines", "funding": "fundingRate"}
_IV_MS = {"1h": 3_600_000, "4h": 14_400_000, "1d": 86_400_000}
LISTING = "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision"

_PREFIX = {
    "klines": "data/futures/um/monthly/klines/{symbol}/{interval}/",
    "mark_klines": "data/futures/um/monthly/markPriceKlines/{symbol}/{interval}/",
    "funding": "data/futures/um/monthly/fundingRate/{symbol}/",
    "spot_klines": "data/spot/monthly/klines/{symbol}/{interval}/",
}
_FILE = {
    "klines": "{symbol}-{interval}-{month}.zip",
    "mark_klines": "{symbol}-{interval}-{month}.zip",
    "funding": "{symbol}-fundingRate-{month}.zip",
    "spot_klines": "{symbol}-{interval}-{month}.zip",
}
_MONTH_RE = re.compile(r"(\d{4}-\d{2})\.zip$")


def _http_get(url: str, timeout: float = 60, retries: int = 5, backoff: float = 2.0) -> bytes:
    last = None
    for attempt in range(retries):
        try:
            r = requests.get(url, timeout=timeout)
            if r.status_code == 404:
                raise FileNotFoundError(url)
            r.raise_for_status()
            return r.content
        except FileNotFoundError:
            raise
        except Exception as exc:  # network / 5xx: retry with exponential backoff
            last = exc
            time.sleep(backoff * (2 ** attempt))
    raise RuntimeError(f"GET failed after {retries} attempts: {url}") from last


def _frac_digits(s: pd.Series) -> np.ndarray:
    frac = s.str.partition(".")[2].str.rstrip("0")
    return frac.str.len().to_numpy(dtype=np.int64)


def _to_ms(x: pd.Series) -> pd.Series:
    """Binance spot files switched to microsecond timestamps in 2025; normalize to ms."""
    v = x.astype("int64")
    return v.where(v < 10**14, v // 1000)


def parse_kline_csv(raw: bytes) -> pd.DataFrame:
    text = raw.decode()
    first = text.split("\n", 1)[0]
    has_header = not first.split(",")[0].strip().isdigit()
    df = pd.read_csv(io.StringIO(text), header=0 if has_header else None, dtype=str)
    df = df.iloc[:, :12]
    df.columns = ["open_time", "open", "high", "low", "close", "volume", "close_time",
                  "quote_volume", "trades", "taker_buy_volume", "taker_buy_quote_volume",
                  "ignore"]
    dec = np.max(np.vstack([_frac_digits(df[c]) for c in ("open", "high", "low", "close")]),
                 axis=0)
    out = pd.DataFrame({
        "open_time": _to_ms(df["open_time"]),
        "open": df["open"].astype(float), "high": df["high"].astype(float),
        "low": df["low"].astype(float), "close": df["close"].astype(float),
        "volume": df["volume"].astype(float), "quote_volume": df["quote_volume"].astype(float),
        "trades": df["trades"].astype("int64"), "px_decimals": dec.astype("int64"),
    })
    return out[KLINE_COLUMNS]


def parse_funding_csv(raw: bytes) -> pd.DataFrame:
    text = raw.decode()
    first = text.split("\n", 1)[0]
    has_header = not first.split(",")[0].strip().isdigit()
    df = pd.read_csv(io.StringIO(text), header=0 if has_header else None)
    if has_header:
        cols = {c.strip().lower(): c for c in df.columns}
        t = df[cols.get("calc_time", df.columns[0])]
        rate = df[cols.get("last_funding_rate", df.columns[-1])]
        ih = df[cols["funding_interval_hours"]] if "funding_interval_hours" in cols else np.nan
    else:
        t, rate = df.iloc[:, 0], df.iloc[:, -1]
        ih = df.iloc[:, 1] if df.shape[1] >= 3 else np.nan
    out = pd.DataFrame({"funding_time": _to_ms(t), "funding_rate": rate.astype(float)})
    out["funding_interval_hours"] = np.nan if np.isscalar(ih) else np.asarray(ih, dtype=float)
    return out[FUNDING_COLUMNS]


class BinanceUSDMArchiveAdapter(VenueAdapter):
    venue = "binance_usdm"
    spot_venue = "binance_spot"

    def __init__(self, http_get: Callable[[str], bytes] | None = None,
                 verify_checksum: bool = True, rest_base: str | None = REST_BASE):
        self._get = http_get or _http_get
        self.verify_checksum = verify_checksum
        self.rest_base = rest_base
        self._onboard: dict[str, str] | None = None

    # ------------------------------------------------------------------ REST backfill
    def onboard_month(self, symbol: str) -> str | None:
        """Listing month of a perp from the venue's exchangeInfo (None if unavailable)."""
        if not self.rest_base:
            return None
        if self._onboard is None:
            try:
                info = json.loads(self._get(f"{self.rest_base}/exchangeInfo"))
                self._onboard = {
                    x["symbol"]: pd.Timestamp(int(x["onboardDate"]), unit="ms").strftime("%Y-%m")
                    for x in info.get("symbols", []) if x.get("contractType") == "PERPETUAL"}
            except Exception:
                self._onboard = {}
        return self._onboard.get(symbol)

    def exchange_info(self) -> list[dict]:
        """Perp reference data published by the venue (listing date, tick, lot step)."""
        info = json.loads(self._get(f"{self.rest_base}/exchangeInfo"))
        rows = []
        for x in info.get("symbols", []):
            if x.get("contractType") != "PERPETUAL":
                continue
            f = {flt["filterType"]: flt for flt in x.get("filters", [])}
            rows.append({"symbol": x["symbol"], "onboard_date": int(x["onboardDate"]),
                         "tick_size": float(f.get("PRICE_FILTER", {}).get("tickSize", "nan")),
                         "lot_size": float(f.get("LOT_SIZE", {}).get("stepSize", "nan"))})
        return rows

    def margin_brackets(self, symbols: list[str] | None = None) -> dict[str, dict]:
        """Current maintenance-margin tiers per perp: {symbol: {"update_time", "brackets":
        [(floor, mmr, cum)]}} as published by the venue."""
        data = json.loads(self._get(BRACKETS_URL))["data"]["brackets"]
        out = {}
        for x in data:
            if symbols and x["symbol"] not in symbols:
                continue
            out[x["symbol"]] = {
                "update_time": int(x["updateTime"]),
                "brackets": [(float(b["bracketNotionalFloor"]), float(b["bracketMaintenanceMarginRate"]),
                              float(b["cumFastMaintenanceAmount"])) for b in
                             sorted(x["riskBrackets"], key=lambda b: b["bracketSeq"])]}
        return out

    def _rest_month(self, dataset: str, symbol: str, month: str, interval: str | None) -> FetchResult:
        start = int(pd.Timestamp(f"{month}-01", tz="UTC").value // 1_000_000)
        end = int((pd.Timestamp(f"{month}-01", tz="UTC") + pd.DateOffset(months=1)).value // 1_000_000)
        ep = _REST_ENDPOINT[dataset]
        rows, cursor = [], start
        while cursor < end:
            url = f"{self.rest_base}/{ep}?symbol={symbol}&startTime={cursor}&endTime={end - 1}&limit=1000"
            if dataset != "funding":
                url += f"&interval={interval}"
            page = json.loads(self._get(url))
            if not page:
                break
            rows += page
            last = page[-1]["fundingTime"] if dataset == "funding" else page[-1][0]
            nxt = int(last) + (1 if dataset == "funding" else _IV_MS[interval])
            if nxt <= cursor or len(page) < 1000:
                break
            cursor = nxt
        if dataset == "funding":
            text = "calc_time,funding_interval_hours,last_funding_rate\n" + "".join(
                f"{r['fundingTime']},,{r['fundingRate']}\n" for r in rows)
            frame = parse_funding_csv(text.encode()) if rows else pd.DataFrame(columns=FUNDING_COLUMNS)
        else:
            text = "".join(",".join(str(v) for v in r[:12]) + "\n" for r in rows)
            frame = parse_kline_csv(text.encode()) if rows else pd.DataFrame(columns=KLINE_COLUMNS)
        digest = hashlib.sha256(frame.to_csv(index=False).encode()).hexdigest()
        return FetchResult(frame=frame, checksum=digest, source=f"{self.rest_base}/{ep}", verified=False)

    # ------------------------------------------------------------------ listing
    def _list_keys(self, prefix: str) -> list[str]:
        keys, marker = [], ""
        while True:
            url = f"{LISTING}?delimiter=/&prefix={quote(prefix)}"
            if marker:
                url += f"&marker={quote(marker)}"
            xml = self._get(url).decode()
            page = re.findall(r"<Key>([^<]+)</Key>", xml)
            keys += page
            if "<IsTruncated>true</IsTruncated>" not in xml or not page:
                return keys
            marker = page[-1]

    def archive_months(self, dataset: str, symbol: str, interval: str | None = None) -> list[str]:
        prefix = _PREFIX[dataset].format(symbol=symbol, interval=interval)
        months = []
        for k in self._list_keys(prefix):
            m = _MONTH_RE.search(k)
            if m:
                months.append(m.group(1))
        return sorted(set(months))

    def available_months(self, dataset: str, symbol: str, interval: str | None = None) -> list[str]:
        """Archive months plus, for perps, REST-backfilled months from listing to archive start."""
        months = self.archive_months(dataset, symbol, interval)
        listing = self.onboard_month(symbol) if dataset in _REST_ENDPOINT else None
        if listing and months and listing < months[0]:
            extra = pd.period_range(listing, months[0], freq="M")[:-1].strftime("%Y-%m").tolist()
            months = extra + months
        return months

    # ------------------------------------------------------------------ fetch
    def url_for(self, dataset: str, symbol: str, month: str, interval: str | None = None) -> str:
        path = _PREFIX[dataset] + _FILE[dataset]
        return f"{ARCHIVE}/" + path.format(symbol=symbol, interval=interval, month=month)

    def _in_archive(self, dataset, symbol, month, interval) -> bool:
        key = (dataset, symbol, interval)
        cache = self.__dict__.setdefault("_archive_cache", {})
        if key not in cache:
            cache[key] = set(self.archive_months(dataset, symbol, interval))
        return month in cache[key]

    def remote_checksum(self, dataset: str, symbol: str, month: str,
                        interval: str | None = None) -> str | None:
        """Venue-published SHA-256 (cheap; lets re-ingest skip unchanged partitions)."""
        if dataset in _REST_ENDPOINT and self.rest_base and not self._in_archive(dataset, symbol, month, interval):
            return None     # REST months: fetch and compare the frame digest instead
        try:
            url = self.url_for(dataset, symbol, month, interval) + ".CHECKSUM"
            return self._get(url).decode().split()[0].strip().lower()
        except FileNotFoundError:
            return None

    def fetch_month(self, dataset: str, symbol: str, month: str,
                    interval: str | None = None) -> FetchResult:
        if dataset in _REST_ENDPOINT and self.rest_base and not self._in_archive(dataset, symbol, month, interval):
            return self._rest_month(dataset, symbol, month, interval)
        url = self.url_for(dataset, symbol, month, interval)
        payload = self._get(url)
        digest = hashlib.sha256(payload).hexdigest()
        verified = False
        if self.verify_checksum:
            published = self._get(url + ".CHECKSUM").decode().split()[0].strip().lower()
            if published != digest:
                raise ChecksumMismatch(f"{url}: sha256 {digest} != published {published}")
            verified = True
        with zipfile.ZipFile(io.BytesIO(payload)) as zf:
            raw = zf.read(zf.namelist()[0])
        frame = parse_funding_csv(raw) if dataset == "funding" else parse_kline_csv(raw)
        return FetchResult(frame=frame, checksum=digest, source=url, verified=verified)
