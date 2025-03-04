#!/usr/bin/env python3
import os
import argparse
import pandas as pd
import numpy as np
import datetime as dt

def load_binance_data(binance_file):
    df = pd.read_csv(binance_file, parse_dates=['timestamp'])
    df.sort_values('timestamp', inplace=True)
    df.set_index('timestamp', inplace=True)
    if not df.index.is_unique:
        df = df[~df.index.duplicated(keep='first')]
    return df

def get_binance_price(binance_df, target_date):
    idx = binance_df.index.get_indexer([target_date], method='nearest')
    if idx[0] == -1:
        raise ValueError(f"Binance price not found for {target_date}")
    return binance_df.iloc[idx[0]]['close']

def process_coingecko_file(filepath, binance_folder):
    # Determine coin symbol from filename (e.g., "ada.csv" -> "ADA")
    coin_symbol = os.path.splitext(os.path.basename(filepath))[0].upper()
    print(f"==== process {coin_symbol} ======")

    # Load Coingecko data (assumes columns: date, market_cap, price)
    df = pd.read_csv(filepath)
    if 'date' not in df.columns:
        first_col = df.columns[0]
        if first_col.startswith("Unnamed"):
            df = df.rename(columns={first_col: 'date'})
    # Convert the 'date' column to datetime
    df['date'] = pd.to_datetime(df['date'])
    df.sort_values('date', inplace=True)
    df = df[['date', 'market_cap', 'price']]
    df.set_index('date', inplace=True)

    # Create a complete monthly date range based on month-end dates.
    start = df.index.min()
    end = df.index.max()
    full_dates = pd.date_range(start=start, end=end, freq='M')
    full_df = pd.DataFrame(index=full_dates)

    # Merge with existing data so missing month-end dates have NaN values.
    merged = full_df.join(df, how='left')

    # Compute token supply (market_cap / price) for available rows.
    merged['supply'] = merged['market_cap'] / merged['price']
    # Interpolate supply for missing rows.
    merged['supply'] = merged['supply'].interpolate(method='linear')

    # Load corresponding Binance data
    binance_file = os.path.join(binance_folder, f"{coin_symbol}USDT-1h-data.csv")
    if not os.path.exists(binance_file):
        print(f"Binance file for {coin_symbol} not found: {binance_file}. Skipping.")
        return
    binance_df = load_binance_data(binance_file)

    # For rows missing Coingecko data, use Binance price and interpolated supply.
    missing = merged['market_cap'].isna()
    for date in merged[missing].index:
        bin_price = get_binance_price(binance_df, date)
        supply = merged.loc[date, 'supply']
        merged.loc[date, 'price'] = bin_price
        merged.loc[date, 'market_cap'] = supply * bin_price

    # Drop the helper column and sort by date.
    merged.drop(columns=['supply'], inplace=True)
    merged.sort_index(inplace=True)

    # Overwrite the original Coingecko CSV file.
    merged.to_csv(filepath, date_format="%Y-%m-%d %H:%M:%S")
    print(f"Processed and updated: {filepath}")

def main():
    parser = argparse.ArgumentParser(
        description="Fill missing month-end rows in Coingecko CSVs using Binance data."
    )
    parser.add_argument("--coingecko-folder", required=True, help="Folder with Coingecko CSV files")
    parser.add_argument("--binance-folder", required=True, help="Folder with Binance CSV files")
    args = parser.parse_args()

    # Process each CSV in the Coingecko folder (recursively).
    for root, _, files in os.walk(args.coingecko_folder):
        for file in files:
            if file.endswith('.csv'):
                filepath = os.path.join(root, file)
                process_coingecko_file(filepath, args.binance_folder)

if __name__ == "__main__":
    main()
