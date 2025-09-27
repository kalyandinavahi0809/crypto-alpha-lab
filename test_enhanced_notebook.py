# If running locally, ensure dependencies are installed:
# pip install requests pandas pyarrow fastparquet tqdm
import os
import json
import time
from datetime import datetime, timezone, timedelta
from typing import List, Dict
import random

import requests
import pandas as pd
from tqdm import tqdm

# Configuration
USE_MOCK_DATA = True  # Set to True for offline testing with mock data

# Relative storage folder (macOS/Linux friendly)
BASE_DIR = os.path.abspath(os.path.join(os.getcwd()))
STORAGE_DIR = os.path.join(BASE_DIR, 'storage', 'ohlcv')
os.makedirs(STORAGE_DIR, exist_ok=True)
print(f'Storage directory: {os.path.relpath(STORAGE_DIR)}')

if USE_MOCK_DATA:
    print('🧪 MOCK DATA MODE ENABLED - Using simulated data for testing')
else:
    print('🌐 LIVE DATA MODE - Will attempt to fetch real data from Binance API')

# Mock data for testing without internet access
MOCK_EXCHANGE_INFO = {
    "timezone": "UTC",
    "serverTime": 1645123456789,
    "symbols": [
        {
            "symbol": "BTCUSDT",
            "status": "TRADING",
            "baseAsset": "BTC",
            "quoteAsset": "USDT",
            "isSpotTradingAllowed": True
        },
        {
            "symbol": "ETHUSDT",
            "status": "TRADING",
            "baseAsset": "ETH",
            "quoteAsset": "USDT",
            "isSpotTradingAllowed": True
        },
        {
            "symbol": "ADAUSDT",
            "status": "TRADING",
            "baseAsset": "ADA",
            "quoteAsset": "USDT",
            "isSpotTradingAllowed": True
        },
        {
            "symbol": "DOTUSDT",
            "status": "TRADING",
            "baseAsset": "DOT",
            "quoteAsset": "USDT",
            "isSpotTradingAllowed": True
        },
        {
            "symbol": "SOLUSDT",
            "status": "TRADING",
            "baseAsset": "SOL",
            "quoteAsset": "USDT",
            "isSpotTradingAllowed": True
        },
        # Add more symbols for testing
        {
            "symbol": "BNBUSDT",
            "status": "TRADING",
            "baseAsset": "BNB",
            "quoteAsset": "USDT",
            "isSpotTradingAllowed": True
        },
        {
            "symbol": "XRPUSDT",
            "status": "TRADING",
            "baseAsset": "XRP",
            "quoteAsset": "USDT",
            "isSpotTradingAllowed": True
        }
    ]
}

class MockSession:
    """Mock requests.Session for testing without internet access"""
    def __init__(self):
        self.headers = {}
    
    def get(self, url, params=None, timeout=None):
        return MockResponse(url, params)

class MockResponse:
    """Mock response object"""
    def __init__(self, url, params=None):
        self.url = url
        self.params = params or {}
        self.status_code = 200
    
    def raise_for_status(self):
        pass
    
    def json(self):
        if "exchangeInfo" in self.url:
            return MOCK_EXCHANGE_INFO
        elif "klines" in self.url:
            return self._generate_mock_klines()
        return {}
    
    def _generate_mock_klines(self):
        """Generate realistic mock OHLCV data"""
        symbol = self.params.get('symbol', 'BTCUSDT')
        limit = int(self.params.get('limit', 10))
        
        # Set base prices for different assets
        price_map = {
            'BTC': 50000, 'ETH': 3000, 'BNB': 400, 'ADA': 1.0, 
            'DOT': 7.0, 'SOL': 100, 'XRP': 0.6
        }
        
        base_asset = next((k for k in price_map.keys() if k in symbol), 'BTC')
        base_price = price_map[base_asset]
        
        data = []
        end_time = datetime.now(timezone.utc)
        
        for i in range(limit):
            timestamp = int((end_time - timedelta(days=limit-i-1)).timestamp() * 1000)
            
            # Generate realistic OHLCV with some randomness
            price_variance = random.uniform(0.95, 1.05)
            open_price = base_price * price_variance
            
            high_price = open_price * random.uniform(1.0, 1.02)
            low_price = open_price * random.uniform(0.98, 1.0)
            close_price = open_price * random.uniform(0.99, 1.01)
            volume = random.uniform(100, 1000)
            
            data.append([
                timestamp,                    # Open time
                f"{open_price:.2f}",         # Open
                f"{high_price:.2f}",         # High
                f"{low_price:.2f}",          # Low
                f"{close_price:.2f}",        # Close
                f"{volume:.2f}",             # Volume
                timestamp + 86400000,        # Close time
                "0",                         # Quote asset volume
                100,                         # Number of trades
                "0",                         # Taker buy base asset volume
                "0",                         # Taker buy quote asset volume
                "0"                          # Ignore
            ])
        
        return data

BINANCE_API = 'https://api.binance.com'

# Initialize session based on mode
if USE_MOCK_DATA:
    SESSION = MockSession()
else:
    SESSION = requests.Session()
    SESSION.headers.update({'User-Agent': 'crypto-alpha-lab/1.0'})

def get_exchange_info() -> Dict:
    """Get exchange information from Binance API or mock data"""
    try:
        url = f'{BINANCE_API}/api/v3/exchangeInfo'
        r = SESSION.get(url, timeout=20)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        if not USE_MOCK_DATA:
            print(f"⚠️  Failed to fetch live data: {e}")
            print("💡 Consider setting USE_MOCK_DATA = True for offline testing")
        raise

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
    """Fetch OHLCV data for a symbol"""
    try:
        url = f'{BINANCE_API}/api/v3/klines'
        params = {'symbol': symbol, 'interval': interval, 'limit': limit}
        if start_time is not None: 
            params['startTime'] = start_time
        if end_time is not None: 
            params['endTime'] = end_time
        
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
        
    except Exception as e:
        if not USE_MOCK_DATA:
            print(f"⚠️  Failed to fetch data for {symbol}: {e}")
        raise

def save_field_parquet(df: pd.DataFrame, symbol: str, field: str):
    """Save a single field to parquet file"""
    assert field in ['open','high','low','close','volume'], f"Invalid field: {field}"
    
    # Each field to its own parquet per symbol, under storage/ohlcv/{field}/{symbol}.parquet
    field_dir = os.path.join(STORAGE_DIR, field)
    os.makedirs(field_dir, exist_ok=True)
    path = os.path.join(field_dir, f'{symbol}.parquet')
    
    out = df[['open_time', field]].copy()
    out = out.rename(columns={'open_time': 'timestamp', field: field})
    out.to_parquet(path, index=False)
    
    print(f'Saved {field} -> {os.path.relpath(path)} | rows={len(out)}')

def save_all_fields(df: pd.DataFrame, symbol: str):
    """Save all OHLCV fields for a symbol"""
    for f in ['open','high','low','close','volume']:
        save_field_parquet(df, symbol, f)

def load_field(symbol: str, field: str) -> pd.DataFrame:
    """Load a specific field for a symbol from parquet"""
    path = os.path.join(STORAGE_DIR, field, f'{symbol}.parquet')
    if not os.path.exists(path):
        raise FileNotFoundError(f"Data file not found: {path}")
    return pd.read_parquet(path)

# Get top symbols
symbols = top_spot_symbols(limit=10)  # Reduced for testing
print('Selected symbols:', symbols)
print(f'Found {len(symbols)} symbols to process')

# Download OHLCV data
all_counts = {}
errors = {}

for sym in tqdm(symbols, desc='Downloading OHLCV (1d)'):
    try:
        df = klines(sym, interval='1d', limit=100)  # Reduced limit for testing
        if df.empty:
            print(f'⚠️  No data for {sym}')
            continue
            
        save_all_fields(df, sym)
        all_counts[sym] = len(df)
        
        # Be gentle with API calls
        if not USE_MOCK_DATA:
            time.sleep(0.1)
            
    except requests.HTTPError as e:
        error_msg = f'HTTP error: {e}'
        print(f'❌ {sym}: {error_msg}')
        errors[sym] = error_msg
    except Exception as e:
        error_msg = f'Error: {e}'
        print(f'❌ {sym}: {error_msg}')
        errors[sym] = error_msg

# Summary
print(f'\n📊 SUMMARY:')
print(f'✅ Successfully processed: {len(all_counts)} symbols')
print(f'❌ Errors encountered: {len(errors)} symbols')

if all_counts:
    print(f'\n📈 Completed symbols: {list(all_counts.keys())}')
    sample_counts = {k: all_counts[k] for k in list(all_counts)[:5]}
    print(f'📋 Sample row counts:')
    print(json.dumps(sample_counts, indent=2))
else:
    print('⚠️  No data was successfully downloaded')

if errors:
    print(f'\n❌ Errors summary:')
    for sym, error in errors.items():
        print(f'  {sym}: {error}')

# Validate saved data by reloading and inspecting
print('🔍 VALIDATION: Reloading and inspecting saved data\n')

# Get symbols that were successfully processed
check_syms = list(all_counts.keys())[:3] if 'all_counts' in globals() and all_counts else []

if not check_syms:
    print('⚠️  No symbols to validate. Please run the data collection first.')
else:
    print(f'🧪 Validating data for: {check_syms}\n')
    
    validation_success = 0
    validation_errors = []
    
    for sym in check_syms:
        print(f'📊 {sym}:')
        
        for field in ['open','high','low','close','volume']:
            try:
                dfv = load_field(sym, field)
                sample_data = dfv.head(2).to_dict(orient='records')
                print(f'  ✅ {field}: {dfv.shape} rows')
                
                # Quick sanity checks
                if dfv.empty:
                    raise ValueError(f'{field} data is empty')
                
                if field in ['open', 'high', 'low', 'close'] and dfv[field].min() <= 0:
                    raise ValueError(f'{field} contains non-positive values')
                
                if field == 'volume' and dfv[field].min() < 0:
                    raise ValueError(f'{field} contains negative values')
                
                validation_success += 1
                
                # Show sample data for first field only to avoid clutter
                if field == 'open':
                    print(f'     Sample: {sample_data}')
                    
            except Exception as e:
                error_msg = f'{sym}.{field}: {e}'
                print(f'  ❌ {field}: {e}')
                validation_errors.append(error_msg)
        
        print()  # Empty line between symbols
    
    # Final validation summary
    total_expected = len(check_syms) * 5  # 5 fields per symbol
    print(f'\n📋 VALIDATION SUMMARY:')
    print(f'✅ Successful validations: {validation_success}/{total_expected}')
    print(f'❌ Validation errors: {len(validation_errors)}')
    
    if validation_errors:
        print('\n❌ Error details:')
        for error in validation_errors:
            print(f'  - {error}')
    
    if validation_success == total_expected:
        print('\n🎉 ALL VALIDATIONS PASSED! Data collection and storage working correctly.')
    else:
        print(f'\n⚠️  {total_expected - validation_success} validations failed.')

# Additional check: verify storage structure
print('\n🗂️  STORAGE STRUCTURE CHECK:')
try:
    if os.path.exists(STORAGE_DIR):
        field_dirs = [d for d in os.listdir(STORAGE_DIR) if os.path.isdir(os.path.join(STORAGE_DIR, d))]
        print(f'📁 Field directories: {sorted(field_dirs)}')
        
        for field_dir in sorted(field_dirs):
            field_path = os.path.join(STORAGE_DIR, field_dir)
            parquet_files = [f for f in os.listdir(field_path) if f.endswith('.parquet')]
            print(f'  📄 {field_dir}: {len(parquet_files)} files')
    else:
        print('❌ Storage directory does not exist')
except Exception as e:
    print(f'❌ Error checking storage structure: {e}')
