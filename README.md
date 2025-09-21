# crypto-alpha-lab

Crypto Alpha generation framework: breakout, momentum, rebalancing, sentiment strategies

## Overview

This repository contains notebooks and tools for collecting and analyzing cryptocurrency data to generate alpha signals.

## 📊 01_data_collection.ipynb

The main data collection notebook that fetches OHLCV (Open, High, Low, Close, Volume) data from Binance and stores it in an efficient parquet format.

### Features

- Fetches data for top liquid cryptocurrency pairs from Binance
- Stores each OHLCV field in separate parquet files for efficient access
- Handles network connectivity issues gracefully
- Cross-platform compatible (macOS/Linux/Windows)

### Requirements

```bash
pip install requests pandas pyarrow fastparquet tqdm jupyter
```

### Usage

1. **Run the notebook directly:**
   ```bash
   jupyter notebook 01_data_collection.ipynb
   ```

2. **Or execute programmatically:**
   ```bash
   jupyter nbconvert --to notebook --execute 01_data_collection.ipynb
   ```

### Data Storage Structure

The notebook creates the following directory structure:

```
storage/ohlcv/
├── open/
│   ├── BTCUSDT.parquet
│   ├── ETHUSDT.parquet
│   └── ...
├── high/
│   ├── BTCUSDT.parquet
│   └── ...
├── low/
├── close/
└── volume/
```

Each file contains:
- `timestamp`: UTC timestamp
- `{field}`: The OHLC or volume value

### Network Connectivity

The notebook includes robust error handling for network issues:

- **With Internet**: Fetches live data from Binance API
- **Without Internet**: Shows warnings but doesn't crash
- **Rate Limiting**: Includes built-in delays to respect API limits

### Testing

Run the test suite to verify functionality:

```bash
python3 run_notebook_test.py
```

Or run the mock test for offline validation:

```bash
python3 test_notebook_mock.py
```

## 🛠️ Development

### Project Structure

- `01_data_collection.ipynb` - Main data collection notebook
- `test_notebook_mock.py` - Offline testing with mock data
- `run_notebook_test.py` - Comprehensive notebook testing
- `storage/` - Data storage directory (created automatically)

### Contributing

1. Ensure all tests pass before committing
2. Add tests for new functionality
3. Update documentation as needed

## 📈 Future Enhancements

- [ ] Support for additional exchanges (Coinbase, Kraken, etc.)
- [ ] Real-time data streaming capabilities  
- [ ] Advanced data quality checks and validation
- [ ] Integration with alpha generation strategies
- [ ] Backtesting framework integration
