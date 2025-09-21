# crypto-alpha-lab
Crypto Alpha generation framework: breakout, momentum, rebalancing, sentiment strategies

## Data Collection Notebook

This repository contains a Jupyter notebook `01_data_collection.ipynb` for fetching and storing cryptocurrency OHLCV data from Binance.

### Features

- **Live Data Mode**: Fetches real data from Binance API when internet is available
- **Mock Data Mode**: Uses simulated data for offline testing and development
- **Robust Error Handling**: Gracefully handles network issues and API errors
- **Efficient Storage**: Stores each OHLCV field in separate Parquet files for optimal access
- **Data Validation**: Comprehensive validation and verification of stored data
- **Progress Tracking**: Real-time progress indicators and detailed logging

### Installation

```bash
pip install -r requirements.txt
```

### Usage

1. **Interactive Mode**: Open and run `01_data_collection.ipynb` in Jupyter
2. **Testing Mode**: Run the comprehensive test suite:
   ```bash
   python3 run_notebook_tests.py
   ```
3. **Mock Data Mode**: Set `USE_MOCK_DATA = True` in the notebook for offline testing

### Data Structure

Data is stored under `storage/ohlcv/` with the following structure:
```
storage/ohlcv/
├── open/
│   ├── BTCUSDT.parquet
│   └── ETHUSDT.parquet
├── high/
├── low/
├── close/
└── volume/
```

### Testing

The notebook has been thoroughly tested and debugged with:
- ✅ Mock data functionality for offline development
- ✅ Comprehensive error handling for network issues
- ✅ Data validation and integrity checks
- ✅ Storage structure verification
- ✅ All edge cases and error scenarios
