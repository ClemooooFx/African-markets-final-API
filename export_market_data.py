"""
Python script to export afrimarket data to JSON files
Run this script to generate data files that the frontend can use
Usage: python export_market_data.py
"""

import afrimarket as afm
import json
import os
import time
from pathlib import Path

# Configuration (can be overridden with env vars)
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "3"))          # exchanges per batch
PAUSE_SECONDS = int(os.getenv("PAUSE_SECONDS", "5"))   # pause between batches (seconds)

# Create data directory
OUT_DIR = Path("market_data")
OUT_DIR.mkdir(parents=True, exist_ok=True)

# List of all exchanges (ordered)
exchanges = {
    'bse': 'Botswana Stock Exchange',
    'brvm': 'Bourse Régionale des Valeurs Mobilières',
    'gse': 'Ghana Stock Exchange',
    'jse': 'Johannesburg Stock Exchange',
    'luse': 'Lusaka Securities Exchange',
    'mse': 'Malawi Stock Exchange',
    'nse': 'Nairobi Securities Exchange',
    'ngx': 'Nigerian Stock Exchange',
    'use': 'Uganda Securities Exchange',
    'zse': 'Zimbabwe Stock Exchange'
}

def safe_float(val, default=0.0):
    """Try to convert to float, return default on failure or empty-ish values."""
    try:
        if val is None:
            return default
        s = str(val).strip()
        if s == "" or s == '\u200b':
            return default
        return float(s)
    except Exception:
        return default

def write_json(path: Path, data):
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

def export_exchange_data(market_code, market_name):
    """Export all data for a specific exchange"""
    print(f"\nExporting data for {market_name} ({market_code})...")
    try:
        exchange = afm.Exchange(market=market_code)

        # 1. Get index price (last 10 days)
        try:
            index_df = exchange.get_index_price()
            if index_df is None or index_df.empty:
                print("Table Not Found")
                print(f"  ✗ Index price failed: No data returned")
            else:
                index_df_last_10 = index_df.tail(10).copy()
                # Convert Date column to string if present
                if 'Date' in index_df_last_10.columns:
                    index_df_last_10['Date'] = index_df_last_10['Date'].astype(str)
                index_data = index_df_last_10.to_dict('records')
                write_json(OUT_DIR / f'{market_code}_index.json', index_data)
                print(f"  ✓ Index price exported ({len(index_data)} days)")
        except Exception as e:
            print("Table Not Found")
            print(f"  ✗ Index price failed: {e}")

        # 2. Get top gainers
        try:
            gainers_df = exchange.get_top_gainers()
            if gainers_df is None or getattr(gainers_df, "empty", False):
                print("Table Not Found")
                print("  ✗ Top gainers failed: No data returned")
            else:
                gainers = []
                for _, row in gainers_df.iterrows():
                    # some rows may be malformed; guard conversions
                    try:
                        ticker = str(row.iloc[0]) if len(row) > 0 else ""
                        price_val = safe_float(row.iloc[1])
                        change = str(row.iloc[2]) if len(row) > 2 else ""
                        gainers.append({'ticker': ticker, 'price': price_val, 'change': change})
                    except Exception:
                        continue
                write_json(OUT_DIR / f'{market_code}_gainers.json', gainers)
                print(f"  ✓ Top gainers exported ({len(gainers)} stocks)")
        except Exception as e:
            print("Table Not Found")
            print(f"  ✗ Top gainers failed: {e}")

        # 3. Get bottom losers
        try:
            losers_df = exchange.get_bottom_losers()
            if losers_df is None or getattr(losers_df, "empty", False):
                print("Table Not Found")
                print("  ✗ Bottom losers failed: No data returned")
            else:
                losers = []
                for _, row in losers_df.iterrows():
                    try:
                        ticker = str(row.iloc[0]) if len(row) > 0 else ""
                        price_val = safe_float(row.iloc[1])
                        change = str(row.iloc[2]) if len(row) > 2 else ""
                        losers.append({'ticker': ticker, 'price': price_val, 'change': change})
                    except Exception:
                        continue
                write_json(OUT_DIR / f'{market_code}_losers.json', losers)
                print(f"  ✓ Bottom losers exported ({len(losers)} stocks)")
        except Exception as e:
            print("Table Not Found")
            print(f"  ✗ Bottom losers failed: {e}")

        # 4. Get listed companies
        try:
            companies_df = exchange.get_listed_companies()
            if companies_df is None or getattr(companies_df, "empty", False):
                print("Table Not Found")
                print("  ✗ Listed companies failed: No data returned")
            else:
                # normalize and fill na safely
                try:
                    companies_df = companies_df.fillna('')
                except Exception:
                    # if fillna fails, continue using it as is
                    pass

                companies = []
                for _, row in companies_df.iterrows():
                    try:
                        ticker = str(row.get('Ticker', row.get(0, ''))) if hasattr(row, 'get') else str(row['Ticker'])
                        name = str(row.get('Name', row.get(1, ''))) if hasattr(row, 'get') else str(row['Name'])
                        volume = str(row.get('Volume', 'N/A')) if hasattr(row, 'get') else str(row['Volume']) if 'Volume' in row else 'N/A'
                        price = safe_float(row.get('Price', 0)) if hasattr(row, 'get') else safe_float(row['Price']) if 'Price' in row else 0
                        change = safe_float(row.get('Change', 0)) if hasattr(row, 'get') else safe_float(row['Change']) if 'Change' in row else 0
                        companies.append({
                            'ticker': ticker,
                            'name': name,
                            'volume': volume if volume != '' else 'N/A',
                            'price': price,
                            'change': change
                        })
                    except Exception:
                        continue

                write_json(OUT_DIR / f'{market_code}_companies.json', companies)
                print(f"  ✓ Listed companies exported ({len(companies)} companies)")
        except Exception as e:
            print("Table Not Found")
            print(f"  ✗ Listed companies failed: {e}")

        print(f"✓ Completed {market_name}")
        return True

    except Exception as e:
        print(f"✗ Failed to export {market_name}: {e}")
        return False

def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def main():
    """Export data for all exchanges in batches."""
    print("=" * 60)
    print("African Markets Data Exporter")
    print("=" * 60)

    exchange_items = list(exchanges.items())
    total = len(exchange_items)
    success_count = 0

    total_batches = (total + BATCH_SIZE - 1) // BATCH_SIZE
    for batch_idx, batch in enumerate(chunks(exchange_items, BATCH_SIZE), start=1):
        print(f"\n--- Batch {batch_idx}/{total_batches}: processing {len(batch)} exchange(s) ---")
        for code, name in batch:
            if export_exchange_data(code, name):
                success_count += 1

        # If not the last batch, pause briefly to reduce risk of blocking/rate-limits
        if batch_idx < total_batches:
            print(f"\nPausing for {PAUSE_SECONDS} second(s) before next batch to reduce rate-limiting...")
            time.sleep(PAUSE_SECONDS)

    print("\n" + "=" * 60)
    print(f"Export complete: {success_count}/{total} exchanges successful")
    print("=" * 60)
    print(f"\nData exported to: {OUT_DIR.resolve()}")
    print("You can now use the frontend with local JSON files.")

if __name__ == "__main__":
    main()
