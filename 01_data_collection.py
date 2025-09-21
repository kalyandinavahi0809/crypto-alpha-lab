# If running locally, ensure dependencies are installed:
# pip install requests pandas pyarrow fastparquet tqdm
import os
import json
import time
from datetime import datetime, timezone
from typing import List, Dict

import requests
import pandas as pd
from tqdm import tqdm

# Relative storage folder (macOS/Linux friendly)
BASE_DIR = os.path.abspath(os.path.join(os.getcwd()))
STORAGE_DIR = os.path.join(BASE_DIR, 'storage', 'ohlcv')
os.makedirs(STORAGE_DIR, exist_ok=True)
print(f'Storage directory: {os.path.relpath(STORAGE_DIR)}')


BINANCE_API = 'https://api.binance.com'
SESSION = requests.Session()
SESSION.headers.update({'User-Agent': 'crypto-alpha-lab/1.0'})

def get_exchange_info() -> Dict:
    url = f'{BINANCE_API}/api/v3/exchangeInfo'
    r = SESSION.get(url, timeout=20)
    r.raise_for_status()
    return r.json()

def top_spot_symbols(quote_priority: List[str] = None, limit: int = 25) -> List[str]:
    """Return top liquid spot symbols by quote asset priority and filters.
    We approximate "top" by focusing on common quote assets and active trading status.
    """
    if quote_priority is None:
        quote_priority = ['USDT', 'USDC', 'FDUSD', 'BTC', 'ETH']
    info = get_exchange_info()
    symbols = [s for s in info.get('symbols', []) if s.get('status') == 'TRADING' and s.get('isSpotTradingAllowed')]
    # Rank symbols by quote asset priority and base asset alphabetically as a tie-breaker
    def score(sym):
        q = sym.get('quoteAsset')
        return (quote_priority.index(q) if q in quote_priority else 999, sym.get('baseAsset', ''))
    ranked = sorted(symbols, key=score)
    picked = []
    seen_bases = set()
    for s in ranked:
        sym = s['symbol']
        # Skip leveraged/index/fiat-like instruments by simple heuristics
        if any(x in sym for x in ['UP', 'DOWN', 'BEAR', 'BULL']):
            continue
        if s.get('quoteAsset') not in quote_priority:
            continue
        # Prefer one quote per base to diversify the universe
        base = s.get('baseAsset')
        if base in seen_bases:
            continue
        seen_bases.add(base)
        picked.append(sym)
        if len(picked) >= limit:
            break
    return picked

def klines(symbol: str, interval: str = '1d', limit: int = 1000, start_time: int = None, end_time: int = None) -> pd.DataFrame:
    url = f'{BINANCE_API}/api/v3/klines'
    params = {'symbol': symbol, 'interval': interval, 'limit': limit}
    if start_time is not None: params['startTime'] = start_time
    if end_time is not None: params['endTime'] = end_time
    r = SESSION.get(url, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    cols = ['open_time','open','high','low','close','volume','close_time','quote_asset_volume','trades','taker_base_vol','taker_quote_vol','ignore']
    df = pd.DataFrame(data, columns=cols)
    if df.empty:
        return df
    df['open_time'] = pd.to_datetime(df['open_time'], unit='ms', utc=True)
    df['close_time'] = pd.to_datetime(df['close_time'], unit='ms', utc=True)
    num_cols = ['open','high','low','close','volume']
    df[num_cols] = df[num_cols].astype(float)
    return df[['open_time','open','high','low','close','volume','close_time']]

def save_field_parquet(df: pd.DataFrame, symbol: str, field: str):
    assert field in ['open','high','low','close','volume']
    # Each field to its own parquet per symbol, under storage/ohlcv/{field}/{symbol}.parquet
    field_dir = os.path.join(STORAGE_DIR, field)
    os.makedirs(field_dir, exist_ok=True)
    path = os.path.join(field_dir, f'{symbol}.parquet')
    out = df[['open_time', field]].copy()
    out = out.rename(columns={'open_time': 'timestamp', field: field})
    out.to_parquet(path, index=False)
    print(f'Saved {field} -> {os.path.relpath(path)} | rows={len(out)}')

def save_all_fields(df: pd.DataFrame, symbol: str):
    for f in ['open','high','low','close','volume']:
        save_field_parquet(df, symbol, f)


symbols = top_spot_symbols(limit=30)
print('Selected symbols:', symbols)

all_counts = {}
for sym in tqdm(symbols, desc='Downloading OHLCV (1d)'):
    try:
        df = klines(sym, interval='1d', limit=1000)
        if df.empty:
            print(f'No data for {sym}')
            continue
        save_all_fields(df, sym)
        all_counts[sym] = len(df)
        time.sleep(0.1)  # be gentle
    except requests.HTTPError as e:
        print(f'HTTP error for {sym}:', e)
    except Exception as e:
        print(f'Error for {sym}:', e)

print('Completed symbols:', list(all_counts.keys()))
print('Sample counts:', json.dumps({k: all_counts[k] for k in list(all_counts)[:5]}, indent=2))


def load_field(symbol: str, field: str) -> pd.DataFrame:
    path = os.path.join(STORAGE_DIR, field, f'{symbol}.parquet')
    return pd.read_parquet(path)

# Try up to first 3 symbols for quick validation
check_syms = list(all_counts.keys())[:3] if 'all_counts' in globals() else []
for sym in check_syms:
    for f in ['open','high','low','close','volume']:
        dfv = load_field(sym, f)
        print(sym, f, dfv.shape, dfv.head(2).to_dict(orient='records'))

