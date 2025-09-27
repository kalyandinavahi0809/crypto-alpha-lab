# ====================================================================
# 01 — Data Collection (Binance OHLCV)  
# ====================================================================
# This script collects the full USDT universe OHLCV data from BinanceUS 
# using python-binance and saves results as Parquet files.
# You can use these files for any future model building for each OHLCV field.
# ====================================================================

# === STEP-1: Install and Import Dependencies ===
# !pip install python-binance --quiet

import pandas as pd
import time
from binance.client import Client as BinanceClient

# ====================================================================
# === CONFIG ===
# ====================================================================
BINANCE_TLD = 'US'     # use 'US' for Binance.US
API_KEY = ''           # optional
API_SECRET = ''
START_DATE = '2020-01-01'
FREQ = '1d'   # daily candles

# ====================================================================
# === INIT CLIENT ===
# ====================================================================
client = BinanceClient(api_key=API_KEY, api_secret=API_SECRET, tld=BINANCE_TLD)

# ====================================================================
# === STEP 1: Get full USDT universe ===
# ====================================================================
info = client.get_exchange_info()
all_symbols = [
    s['symbol'] for s in info['symbols']
    if s['status'] == 'TRADING' and s['quoteAsset'] == 'USDT'
]
print(f"✅ Found {len(all_symbols)} USDT trading pairs on BinanceUS")

# ====================================================================
# === FUNCTION: Fetch OHLCV ===
# ====================================================================
def get_binance_ohlcv(symbol, freq, start_str):
    """Fetch OHLCV candles from BinanceUS. Returns DataFrame with Date index."""
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
        df = df[['Date','open','high','low','close','volume']].astype(float, errors='ignore')
        df.set_index('Date', inplace=True)
        return df

    except Exception as e:
        print(f"⚠️ Error fetching {symbol}: {e}")
        return None

# ====================================================================
# === STEP 2: Loop over ALL symbols ===
# ====================================================================
all_data = {}
for symbol in all_symbols:
    df_symbol = get_binance_ohlcv(symbol, FREQ, START_DATE)
    if df_symbol is not None and not df_symbol.empty:
        all_data[symbol] = df_symbol
        print(f"✅ {symbol}: {len(df_symbol)} rows from {df_symbol.index.min().date()} to {df_symbol.index.max().date()}")
    else:
        print(f"⚠️ Skipped {symbol}")
    time.sleep(0.5)  # avoid hitting API limits

# ====================================================================
# === STEP 3: Combine into panel-like OHLCV DataFrames ===
# ====================================================================
OHLC = {}
for field in ['open','high','low','close','volume']:
    OHLC[field] = pd.DataFrame({
        sym: df[field] for sym, df in all_data.items()
    })

# ====================================================================
# === STEP 4: Save Parquet files ===
# ====================================================================
for field, df in OHLC.items():
    out_file = f"binance_{field}_daily.parquet"
    df.to_parquet(out_file)
    print(f"💾 Saved {out_file} with shape {df.shape}")

print("\nExample CLOSE prices:")
print(OHLC['close'].tail())