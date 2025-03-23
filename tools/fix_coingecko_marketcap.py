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
    if binance_df.empty:
        return None
    min_date = pd.to_datetime(binance_df.index.min())
    max_date = pd.to_datetime(binance_df.index.max())
    if pd.isnull(min_date) or pd.isnull(max_date) or target_date < min_date or target_date > max_date:
        return None
    idx = binance_df.index.get_indexer([target_date], method='nearest')
    if idx[0] == -1:
        return None
    return binance_df.iloc[idx[0]]['close']

def process_coingecko_file(filepath, binance_folder):
    # Determine coin symbol from filename (e.g., "ada.csv" -> "ADA")
    coin_symbol = os.path.splitext(os.path.basename(filepath))[0].upper()

    # Load Coingecko CSV data and fix missing 'date' column if necessary.
    df = pd.read_csv(filepath)
    if 'date' not in df.columns:
        first_col = df.columns[0]
        if first_col.startswith("Unnamed"):
            df = df.rename(columns={first_col: 'date'})
    df['date'] = pd.to_datetime(df['date'])
    df.sort_values('date', inplace=True)
    df = df[['date', 'market_cap', 'price']]
    df.set_index('date', inplace=True)
    # Drop duplicates, keeping only the first occurrence
    df = df[~df.index.duplicated(keep='first')]

    # Create a complete daily date range using all days between the min and max dates.
    start = df.index.min()
    end = df.index.max()
    full_dates = pd.date_range(start=start, end=end, freq='D')

    # Merge with existing data so missing daily dates appear as NaN.
    full_index = df.index.union(full_dates)
    merged = df.reindex(full_index)

    # Compute token supply where available.
    merged['supply'] = merged['market_cap'] / merged['price']
    merged['supply'] = merged['supply'].interpolate(method='linear')

    # Load corresponding Binance data.
    binance_file = os.path.join(binance_folder, f"{coin_symbol}USDT-1h-data.csv")
    if not os.path.exists(binance_file):
        print(f"Binance file for {coin_symbol} not found: {binance_file}. Skipping.")
        return
    binance_df = load_binance_data(binance_file)

    # For rows missing market_cap, fill using Binance price and interpolated supply.
    missing_dates = merged[merged['market_cap'].isna()].index
    rows_to_drop = []
    for date in missing_dates:
        bin_price = get_binance_price(binance_df, date)
        if bin_price is None:
            # If no Binance price, skip this date (mark it for removal).
            rows_to_drop.append(date)
        else:
            supply = merged.loc[date, 'supply']
            merged.loc[date, 'price'] = bin_price
            merged.loc[date, 'market_cap'] = supply * bin_price

    if rows_to_drop:
        merged.drop(index=rows_to_drop, inplace=True)

    merged.drop(columns=['supply'], inplace=True)
    merged.sort_index(inplace=True)

    # Overwrite the original Coingecko CSV file.
    merged.to_csv(filepath, date_format="%Y-%m-%d %H:%M:%S")
    print(f"Processed and updated: {filepath}")

def main():
    parser = argparse.ArgumentParser(
        description="Fill missing daily rows in Coingecko CSVs using Binance data."
    )
    parser.add_argument("--coingecko-folder", required=True, help="Folder with Coingecko CSV files")
    parser.add_argument("--binance-folder", required=True, help="Folder with Binance CSV files")
    args = parser.parse_args()

    for root, _, files in os.walk(args.coingecko_folder):
        for file in files:
            if file.endswith('.csv'):
                filepath = os.path.join(root, file)
                process_coingecko_file(filepath, args.binance_folder)

if __name__ == "__main__":
    main()
