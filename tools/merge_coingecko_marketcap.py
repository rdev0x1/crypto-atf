#!/usr/bin/env python3
import os
import argparse
import pandas as pd
import numpy as np
from datetime import datetime
from crypto_stock import CryptoStock


def load_binance_data(binance_file):
    """
    Load the Binance historical data.
    Assumes a CSV with a 'timestamp' column and a 'close' price.
    Returns a DataFrame with datetime index.
    """
    df = pd.read_csv(binance_file, parse_dates=['timestamp'])
    df.sort_values('timestamp', inplace=True)
    df.set_index('timestamp', inplace=True)
    return df

def get_binance_price(binance_df, target_date):
    """
    For a given target_date (datetime), find the nearest Binance close price.
    """
    try:
        binance_df = binance_df.loc[~binance_df.index.duplicated(keep='first')]
        idx = binance_df.index.get_indexer([target_date], method='nearest')
    except:
        breakpoint()
    if idx[0] == -1:
        raise ValueError(f"Binance price not found for date {target_date}")
    return binance_df.iloc[idx[0]]['close']

def process_coin(file_paths, binance_df):
    """
    For a given coin (list of coin-gecko CSV file paths), this function:
      - Reads each CSV and keeps the columns: date, market_cap, price.
      - Combines and sorts the data.
      - Normalizes dates to 00:00:00 and deduplicates entries by day.
      - Creates a continuous daily date range.
      - Fills any missing day:
          * First, it checks the Binance close prices at the gap boundaries (start and end dates).
          * If both Binance prices are within ±20% of the coin-gecko prices, then for the missing day:
              - It interpolates the token supply (market_cap/price) between the known records,
                obtains the Binance price for that missing day, and computes market_cap.
          * Otherwise (or if Binance data is missing for the gap day), it simply linearly
            interpolates the coin-gecko market_cap and price.
    Returns a DataFrame with columns: date, market_cap, price.
    """
    dfs = []
    for file in file_paths:
        df = pd.read_csv(file)
        # Rename first column to 'date' if needed
        if 'date' not in df.columns:
            first_col = df.columns[0]
            if first_col.startswith("Unnamed"):
                df = df.rename(columns={first_col: 'date'})
        # Convert 'date' column to datetime and normalize to midnight
        df['date'] = pd.to_datetime(df['date']).dt.normalize()
        # Keep only the necessary columns
        df = df[['date', 'market_cap', 'price']]
        dfs.append(df)

    if not dfs:
        return None
    # Combine and sort all data
    df = pd.concat(dfs, ignore_index=True)
    df.sort_values('date', inplace=True)

    # Deduplicate by date (keep the last record for each day)
    df = df.groupby('date').last().reset_index()

    # Create a complete daily date range from the earliest to the latest date
    start = df['date'].min()
    end = df['date'].max()
    full_dates = pd.date_range(start=start, end=end, freq='D')
    full_df = pd.DataFrame({'date': full_dates})

    # Merge the complete daily range with the coin-gecko data
    merged = pd.merge(full_df, df, on='date', how='left', sort=True)

    # Compute token supply for known records (supply = market_cap / price)
    merged['supply'] = merged.apply(
        lambda row: row['market_cap'] / row['price']
        if pd.notnull(row['market_cap']) and pd.notnull(row['price']) and row['price'] != 0 
        else np.nan,
        axis=1
    )

    filled_rows = []
    # Iterate over the complete daily range
    for i, row in merged.iterrows():
        if pd.notnull(row['market_cap']):
            filled_rows.append(row)
        else:
            # Look for the previous known record
            j = i - 1
            while j >= 0 and pd.isnull(merged.loc[j, 'market_cap']):
                j -= 1
            # Look for the next known record
            k = i + 1
            while k < len(merged) and pd.isnull(merged.loc[k, 'market_cap']):
                k += 1
            if j < 0 or k >= len(merged):
                # Cannot interpolate at the boundaries
                continue

            record_prev = merged.loc[j]
            record_next = merged.loc[k]
            total_days = (record_next['date'] - record_prev['date']).days
            delta_days = (row['date'] - record_prev['date']).days
            factor = delta_days / total_days if total_days > 0 else 0

            # Try to check Binance data consistency at the boundaries
            use_binance = False
            try:
                binance_prev = get_binance_price(binance_df, record_prev['date'])
                binance_next = get_binance_price(binance_df, record_next['date'])
                # Verify that Binance prices are within ±20% of coin-gecko prices
                check_prev = abs(binance_prev - record_prev['price']) / record_prev['price'] <= 0.2
                check_next = abs(binance_next - record_next['price']) / record_next['price'] <= 0.2
                if check_prev and check_next:
                    use_binance = True
            except Exception as e:
                use_binance = False

            if use_binance:
                # Use Binance data for interpolation via token supply
                supply_prev = record_prev['supply']
                supply_next = record_next['supply']
                interpolated_supply = supply_prev + factor * (supply_next - supply_prev)
                try:
                    bin_price = get_binance_price(binance_df, row['date'])
                except Exception as e:
                    # If Binance data for the gap day is missing, fall back to simple linear interpolation
                    use_binance = False

            if use_binance:
                market_cap = interpolated_supply * bin_price
                filled = row.copy()
                filled['market_cap'] = market_cap
                filled['price'] = bin_price
                filled['supply'] = interpolated_supply
                filled_rows.append(filled)
            else:
                # Fallback: simple linear interpolation between coin-gecko data
                market_cap = record_prev['market_cap'] + factor * (record_next['market_cap'] - record_prev['market_cap'])
                price = record_prev['price'] + factor * (record_next['price'] - record_prev['price'])
                filled = row.copy()
                filled['market_cap'] = market_cap
                filled['price'] = price
                filled['supply'] = market_cap / price if price != 0 else np.nan
                filled_rows.append(filled)

    filled_df = pd.DataFrame(filled_rows)
    filled_df.sort_values('date', inplace=True)
    result = filled_df[['date', 'market_cap', 'price']]
    return result

def main():
    parser = argparse.ArgumentParser(
        description="Merge multiple coin-gecko CSVs and fill monthly gaps using Binance data."
    )
    parser.add_argument("--input-folders", nargs="+", required=True,
                        help="List of folders containing coin-gecko CSV files (e.g., data2/mcp old_data/mcp)")
    parser.add_argument("--binance-folder", required=True,
                        help="Path to Binance folder containing files like XRPUSDT-1h-data.csv")
    parser.add_argument("--output-folder", required=True,
                        help="Output folder (must be empty or non-existent)")
    args = parser.parse_args()

    if os.path.exists(args.output_folder) and os.listdir(args.output_folder):
        raise ValueError("Output folder must be empty or non-existent.")
    if not os.path.exists(args.output_folder):
        os.makedirs(args.output_folder)

    # Gather coin-gecko CSV files from all input folders (searching recursively)
    coin_files = {}
    for folder in args.input_folders:
        for root, dirs, files in os.walk(folder):
            for file in files:
                if file.endswith(".csv"):
                    file_path = os.path.join(root, file)
                    # Group files by coin name (e.g., "xrp.csv")
                    coin_files.setdefault(file, []).append(file_path)

    # Process each coin file group and merge using the appropriate Binance data.
    for coin, paths in coin_files.items():
        coin_symbol = os.path.splitext(coin)[0].upper()  # Convert 'xrp.csv' to 'XRP'

        if not CryptoStock.is_tradable(coin_symbol):
            print(f"skip untradable {coin_symbol}")
            continue

        binance_filename = f"{coin_symbol}USDT-1h-data.csv"
        binance_filepath = os.path.join(args.binance_folder, binance_filename)
        if not os.path.exists(binance_filepath):
            print(f"Warning: Binance file {binance_filename} not found in {args.binance_folder}. Skipping {coin_symbol}.")
            continue
        print(f"Processing {coin_symbol} from {len(paths)} file(s)...")
        binance_df = load_binance_data(binance_filepath)
        merged_df = process_coin(paths, binance_df)
        if merged_df is None or merged_df.empty:
            print(f"Warning: No data processed for {coin_symbol}.")
            continue
        output_path = os.path.join(args.output_folder, coin)
        merged_df.to_csv(output_path, index=False)
        print(f"Saved merged data to {output_path}")

if __name__ == "__main__":
    main()
