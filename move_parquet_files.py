#!/usr/bin/env python3
"""
Move Binance OHLCV Parquet files to data_collection folder.
Uses only Python standard libraries.
"""

import os
import shutil


def move_binance_parquet_files():
    """
    Move all Binance OHLCV Parquet files from the current directory 
    to a 'data_collection' folder. Creates the folder if it doesn't exist.
    """
    # Define the target directory
    target_dir = 'data_collection'
    
    # List of Binance OHLCV parquet files to move
    parquet_files = [
        'binance_close_daily.parquet',
        'binance_open_daily.parquet',
        'binance_high_daily.parquet',
        'binance_low_daily.parquet',
        'binance_volume_daily.parquet'
    ]
    
    # Create the target directory if it doesn't exist
    if not os.path.exists(target_dir):
        os.makedirs(target_dir)
        print(f"Created directory: {target_dir}")
    else:
        print(f"Directory already exists: {target_dir}")
    
    # Move each parquet file
    moved_files = []
    missing_files = []
    
    for filename in parquet_files:
        source_path = filename
        target_path = os.path.join(target_dir, filename)
        
        if os.path.exists(source_path):
            try:
                shutil.move(source_path, target_path)
                moved_files.append(filename)
                print(f"Moved: {filename} -> {target_path}")
            except Exception as e:
                print(f"Error moving {filename}: {e}")
        else:
            missing_files.append(filename)
            print(f"File not found: {filename}")
    
    # Summary
    print(f"\nSummary:")
    print(f"Successfully moved {len(moved_files)} files:")
    for filename in moved_files:
        print(f"  - {filename}")
    
    if missing_files:
        print(f"Files not found ({len(missing_files)}):")
        for filename in missing_files:
            print(f"  - {filename}")
    
    return len(moved_files), len(missing_files)


if __name__ == "__main__":
    moved_count, missing_count = move_binance_parquet_files()
    
    if missing_count == 0:
        print(f"\nAll {moved_count} Binance OHLCV parquet files successfully moved to data_collection folder!")
    else:
        print(f"\nMoved {moved_count} files, {missing_count} files were missing.")