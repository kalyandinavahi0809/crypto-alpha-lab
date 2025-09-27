"""
Clean data collection script.
"""

import os
import time
import json
from typing import Dict

import pandas as pd
from tqdm import tqdm

# Config
USE_BINANCE_PACKAGE = True  # try to use python-binance if installed
BINANCE_TLD = 'US'  # use 'US' for Binance.US, set '' for global Binance
API_KEY = ''
API_SECRET = ''
START_DATE = '2020-01-01'
FREQ = '1d'

# Storage
BASE_DIR = os.path.abspath(os.path.join(os.getcwd()))
STORAGE_DIR = os.path.join(BASE_DIR, 'storage', 'ohlcv')
os.makedirs(STORAGE_DIR, exist_ok=True)
print(f'Storage directory: {os.path.relpath(STORAGE_DIR)}')


def try_import_binance():
    if not USE_BINANCE_PACKAGE:
        return None
    try:
        from binance.client import Client as BinanceClient
        return BinanceClient
    except Exception:
        return None


BinanceClient = try_import_binance()


def fetch_symbols_via_client(client, quote='USDT'):
    info = client.get_exchange_info()
    symbols = [s['symbol'] for s in info.get('symbols', []) if s.get('status') == 'TRADING' and s.get('quoteAsset') == quote]
    return symbols


def fetch_klines_via_client(client, symbol: str, freq: str, start_str: str):
    try:
        klines = client.get_historical_klines(symbol, freq, start_str)
        if not klines:
            return None
        df = pd.DataFrame(klines, columns=[
            'open_time','open','high','low','close','volume',
            'close_time','quote_asset_volume','num_trades',
            'taker_base_volume','taker_quote_volume','ignore'
        ])
        df['Date'] = pd.to_datetime(df['open_time'], unit='ms')
        df = df[['Date','open','high','low','close','volume']]
        for col in ['open','high','low','close','volume']:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        df.set_index('Date', inplace=True)
        return df
    except Exception as e:
        print(f'Error fetching klines for {symbol}: {e}')
        return None


def load_local_parquet_fallback():
    """Try to build data from storage/ohlcv/<field>/*.parquet files if present."""
    print('Attempting local parquet fallback...')
    open_dir = os.path.join(STORAGE_DIR, 'open')
    all_data: Dict[str, pd.DataFrame] = {}
    if not os.path.isdir(open_dir):
        return all_data
    for fname in os.listdir(open_dir):
        if not fname.endswith('.parquet'):
            continue
        sym = fname.replace('.parquet', '')
        try:
            df_open = pd.read_parquet(os.path.join(open_dir, fname))
            if 'timestamp' in df_open.columns:
                df_open = df_open.rename(columns={'timestamp': 'Date'})
            elif 'open_time' in df_open.columns:
                df_open = df_open.rename(columns={'open_time': 'Date'})
            df_open['Date'] = pd.to_datetime(df_open['Date'])
            df = df_open.set_index('Date')
            for col in ['open','high','low','close','volume']:
                if col not in df.columns:
                    df[col] = pd.NA
            all_data[sym] = df[['open','high','low','close','volume']]
            print(f'Loaded local parquet for {sym} rows={len(df)}')
        except Exception as e:
            print('Failed to read local parquet for', sym, e)
    return all_data


def create_small_mock():
    print('Creating small mock dataset')
    dates = pd.date_range(start=START_DATE, periods=10, freq='D')
    all_data: Dict[str, pd.DataFrame] = {}
    for sym in ['BTCUSDT', 'ETHUSDT', 'BNBUSDT']:
        df = pd.DataFrame({'open': 1.0, 'high': 1.0, 'low': 1.0, 'close': 1.0, 'volume': 0.0}, index=dates)
        all_data[sym] = df
    return all_data


def save_field_csv(df: pd.DataFrame, field: str, out_dir: str):
    out_file = os.path.join(out_dir, f'binance_{field}_daily.csv')
    df.to_csv(out_file)
    print(f'Saved {out_file} shape={df.shape}')


def main():
    all_data: Dict[str, pd.DataFrame] = {}
    errors: Dict[str, str] = {}
    all_counts: Dict[str, int] = {}

    if BinanceClient is not None:
        try:
            client = BinanceClient(api_key=API_KEY, api_secret=API_SECRET, tld=BINANCE_TLD)
            print('Initialized python-binance client')
            symbols = fetch_symbols_via_client(client, quote='USDT')
            print(f'Found {len(symbols)} USDT symbols')
            for sym in tqdm(symbols, desc='Fetching symbols'):
                df = fetch_klines_via_client(client, sym, FREQ, START_DATE)
                if df is not None and not df.empty:
                    all_data[sym] = df
                    all_counts[sym] = len(df)
                    print(f'{sym}: rows={len(df)} from {df.index.min().date()} to {df.index.max().date()}')
                else:
                    errors[sym] = 'no-data'
                time.sleep(0.2)
        except Exception as e:
            print('python-binance client failed:', e)

    if not all_data:
        local = load_local_parquet_fallback()
        if local:
            all_data = local
            all_counts = {k: len(v) for k, v in all_data.items()}

    if not all_data:
        all_data = create_small_mock()
        all_counts = {k: len(v) for k, v in all_data.items()}

    # Combine into panel-like OHLCV DataFrames and save CSVs under storage
    OHLC: Dict[str, pd.DataFrame] = {}
    for field in ['open', 'high', 'low', 'close', 'volume']:
        OHLC[field] = pd.DataFrame({sym: df[field] for sym, df in all_data.items()})
        save_field_csv(OHLC[field], field, out_dir=STORAGE_DIR)

    # Summary
    print('\nSUMMARY')
    print('Processed symbols:', len(all_counts))
    print('Errors:', len(errors))
    if all_counts:
        sample = dict(list(all_counts.items())[:5])
        print('Sample counts:', json.dumps(sample, indent=2))


if __name__ == '__main__':
    main()

                    if not all_data:
                        local = load_local_parquet_fallback()
                        if local:
                            all_data = local
                            all_counts = {k: len(v) for k, v in all_data.items()}

                    if not all_data:
                        all_data = create_small_mock()
                        all_counts = {k: len(v) for k, v in all_data.items()}

                    # Combine into panel-like OHLCV DataFrames and save CSVs under storage
                    OHLC: Dict[str, pd.DataFrame] = {}
                    for field in ['open', 'high', 'low', 'close', 'volume']:
                        OHLC[field] = pd.DataFrame({sym: df[field] for sym, df in all_data.items()})
                        save_field_csv(OHLC[field], field, out_dir=STORAGE_DIR)

                    # Summary
                    print('\nSUMMARY')
                    print('Processed symbols:', len(all_counts))
                    print('Errors:', len(errors))
                    if all_counts:
                        sample = dict(list(all_counts.items())[:5])
                        print('Sample counts:', json.dumps(sample, indent=2))


                if __name__ == '__main__':
                    main()

