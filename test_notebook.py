#!/usr/bin/env python3
"""
Test and debug script for 01_data_collection.ipynb

This script provides mock data functionality to test the notebook
without requiring external internet access to Binance API.
"""

import os
import json
import time
from datetime import datetime, timezone, timedelta
from typing import List, Dict
import pandas as pd
from tqdm import tqdm

# Mock data for testing
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
    ]
}

class MockSession:
    """Mock requests.Session for testing without internet access"""
    
    def __init__(self):
        self.headers = {}
    
    def get(self, url, params=None, timeout=None):
        """Mock GET request"""
        return MockResponse(url, params)

class MockResponse:
    """Mock response object"""
    
    def __init__(self, url, params=None):
        self.url = url
        self.params = params or {}
        self.status_code = 200
    
    def raise_for_status(self):
        """Mock raise_for_status"""
        pass
    
    def json(self):
        """Return mock JSON data based on URL"""
        if "exchangeInfo" in self.url:
            return MOCK_EXCHANGE_INFO
        elif "klines" in self.url:
            return self._generate_mock_klines()
        else:
            return {}
    
    def _generate_mock_klines(self):
        """Generate mock OHLCV data"""
        symbol = self.params.get('symbol', 'BTCUSDT')
        limit = int(self.params.get('limit', 10))
        
        # Generate realistic-looking data
        base_price = 50000 if 'BTC' in symbol else 3000 if 'ETH' in symbol else 1.0
        data = []
        
        end_time = datetime.now(timezone.utc)
        
        for i in range(limit):
            timestamp = int((end_time - timedelta(days=limit-i-1)).timestamp() * 1000)
            
            # Generate realistic OHLCV with some randomness
            import random
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

def test_notebook_functions():
    """Test all functions from the notebook with mock data"""
    print("=" * 60)
    print("TESTING 01_DATA_COLLECTION NOTEBOOK FUNCTIONS")
    print("=" * 60)
    
    # Mock the session
    global SESSION
    SESSION = MockSession()
    
    # Test storage directory setup
    print("\n1. Testing storage directory setup...")
    BASE_DIR = os.path.abspath(os.path.join(os.getcwd()))
    STORAGE_DIR = os.path.join(BASE_DIR, 'storage', 'ohlcv')
    os.makedirs(STORAGE_DIR, exist_ok=True)
    print(f'✓ Storage directory created: {os.path.relpath(STORAGE_DIR)}')
    
    # Test get_exchange_info
    print("\n2. Testing get_exchange_info...")
    try:
        info = get_exchange_info()
        print(f"✓ Exchange info retrieved with {len(info.get('symbols', []))} symbols")
    except Exception as e:
        print(f"✗ Error in get_exchange_info: {e}")
        return False
    
    # Test top_spot_symbols
    print("\n3. Testing top_spot_symbols...")
    try:
        symbols = top_spot_symbols(limit=5)
        print(f"✓ Top symbols retrieved: {symbols}")
        if not symbols:
            print("✗ No symbols returned")
            return False
    except Exception as e:
        print(f"✗ Error in top_spot_symbols: {e}")
        return False
    
    # Test klines function
    print("\n4. Testing klines function...")
    try:
        test_symbol = symbols[0] if symbols else 'BTCUSDT'
        df = klines(test_symbol, interval='1d', limit=10)
        print(f"✓ OHLCV data retrieved for {test_symbol}: {df.shape}")
        print(f"  Columns: {list(df.columns)}")
        print(f"  First row: {df.iloc[0].to_dict()}")
        
        if df.empty:
            print("✗ DataFrame is empty")
            return False
            
    except Exception as e:
        print(f"✗ Error in klines: {e}")
        return False
    
    # Test save functions
    print("\n5. Testing save functions...")
    try:
        save_all_fields(df, test_symbol)
        print(f"✓ All fields saved for {test_symbol}")
        
        # Verify files were created
        for field in ['open', 'high', 'low', 'close', 'volume']:
            field_path = os.path.join(STORAGE_DIR, field, f'{test_symbol}.parquet')
            if os.path.exists(field_path):
                print(f"  ✓ {field} file created: {os.path.relpath(field_path)}")
            else:
                print(f"  ✗ {field} file missing")
                return False
                
    except Exception as e:
        print(f"✗ Error in save functions: {e}")
        return False
    
    # Test load function
    print("\n6. Testing load function...")
    try:
        for field in ['open', 'high', 'low', 'close', 'volume']:
            dfv = load_field(test_symbol, field)
            print(f"  ✓ {field} loaded: {dfv.shape}")
            
            if dfv.empty:
                print(f"  ✗ {field} DataFrame is empty")
                return False
                
    except Exception as e:
        print(f"✗ Error in load function: {e}")
        return False
    
    # Test complete workflow
    print("\n7. Testing complete workflow...")
    try:
        all_counts = {}
        test_symbols = symbols[:3]  # Test with first 3 symbols
        
        for sym in tqdm(test_symbols, desc='Testing complete workflow'):
            try:
                df = klines(sym, interval='1d', limit=50)
                if df.empty:
                    print(f'No data for {sym}')
                    continue
                save_all_fields(df, sym)
                all_counts[sym] = len(df)
                time.sleep(0.01)  # Much shorter sleep for testing
            except Exception as e:
                print(f'Error for {sym}: {e}')
        
        print(f"✓ Workflow completed for symbols: {list(all_counts.keys())}")
        print(f"  Sample counts: {json.dumps(all_counts, indent=2)}")
        
    except Exception as e:
        print(f"✗ Error in complete workflow: {e}")
        return False
    
    # Test validation
    print("\n8. Testing validation...")
    try:
        check_syms = list(all_counts.keys())[:2]
        for sym in check_syms:
            for f in ['open','high','low','close','volume']:
                dfv = load_field(sym, f)
                sample_data = dfv.head(2).to_dict(orient='records')
                print(f"  ✓ {sym} {f}: {dfv.shape} - {sample_data}")
        
    except Exception as e:
        print(f"✗ Error in validation: {e}")
        return False
    
    print("\n" + "=" * 60)
    print("✓ ALL TESTS PASSED SUCCESSFULLY!")
    print("=" * 60)
    return True

# Include all the original notebook functions with SESSION as mock
BINANCE_API = 'https://api.binance.com'
SESSION = MockSession()

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
    BASE_DIR = os.path.abspath(os.path.join(os.getcwd()))
    STORAGE_DIR = os.path.join(BASE_DIR, 'storage', 'ohlcv')
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

def load_field(symbol: str, field: str) -> pd.DataFrame:
    BASE_DIR = os.path.abspath(os.path.join(os.getcwd()))
    STORAGE_DIR = os.path.join(BASE_DIR, 'storage', 'ohlcv')
    path = os.path.join(STORAGE_DIR, field, f'{symbol}.parquet')
    return pd.read_parquet(path)

if __name__ == "__main__":
    success = test_notebook_functions()
    if success:
        print("\nThe notebook is working correctly with mock data!")
        print("To run with real Binance API data, ensure internet connectivity.")
    else:
        print("\nSome tests failed. Please check the errors above.")
        exit(1)