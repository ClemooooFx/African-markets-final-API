"""
Python script to export afrimarket data to JSON files in batches of 3 exchanges at a time
"""

import afrimarket as afm
import json
import os
import time
import pandas as pd

# Create data directory
os.makedirs('market_data', exist_ok=True)

import socket
socket.setdefaulttimeout(10)  # 30 seconds timeout

# Configure proxy if environment variable is set (for GitHub Actions)
PROXIES = None
if os.environ.get('USE_PROXY') == 'true':
    proxy_url = os.environ.get('HTTPS_PROXY') or os.environ.get('HTTP_PROXY')
    if proxy_url:
        PROXIES = {
            'http': proxy_url,
            'https': proxy_url
        }
        print(f"Using proxy: {proxy_url}")
        
        # Apply to requests session if afrimarket uses requests
        import urllib3
        # Note: This may not work if afrimarket uses its own session

# List of all exchanges
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

def export_exchange_data(market_code, market_name):
    """Export all data for a specific exchange"""
    print(f"\nExporting data for {market_name} ({market_code})...")

    try:
        # Try adding timeout when creating exchange
        exchange = afm.Exchange(market=market_code)

        # 1. Index price
        try:
            index_df = exchange.get_index_price().tail(10)
            index_df['Date'] = index_df['Date'].astype(str)
            with open(f'market_data/{market_code}_index.json', 'w') as f:
                json.dump(index_df.to_dict('records'), f, indent=2)
            print(f"  ✓ Index price exported ({len(index_df)} days)")
        except Exception as e:
            print(f"  ✗ Index price failed: {e}")

        # 2. Top gainers
        try:
            gainers_df = exchange.get_top_gainers()
            gainers = []
            for _, row in gainers_df.iterrows():
                try:
                    price_val = pd.to_numeric(row.iloc[1], errors='coerce') or 0
                    gainers.append({
                        'ticker': str(row.iloc[0]),
                        'price': float(price_val),
                        'change': str(row.iloc[2])
                    })
                except Exception:
                    continue
            with open(f'market_data/{market_code}_gainers.json', 'w') as f:
                json.dump(gainers, f, indent=2)
            print(f"  ✓ Top gainers exported ({len(gainers)} stocks)")
        except Exception as e:
            print(f"  ✗ Top gainers failed: {e}")

        # 3. Bottom losers
        try:
            losers_df = exchange.get_bottom_losers()
            losers = []
            for _, row in losers_df.iterrows():
                try:
                    price_val = pd.to_numeric(row.iloc[1], errors='coerce') or 0
                    losers.append({
                        'ticker': str(row.iloc[0]),
                        'price': float(price_val),
                        'change': str(row.iloc[2])
                    })
                except Exception:
                    continue
            with open(f'market_data/{market_code}_losers.json', 'w') as f:
                json.dump(losers, f, indent=2)
            print(f"  ✓ Bottom losers exported ({len(losers)} stocks)")
        except Exception as e:
            print(f"  ✗ Bottom losers failed: {e}")

        # 4. Listed companies
        try:
            companies_df = exchange.get_listed_companies().fillna('')
            companies = []
            for _, row in companies_df.iterrows():
                ticker = str(row.get('Ticker') or row.iloc[0])
                name = str(row.get('Name') or row.iloc[1])
                volume = str(row.get('Volume', 'N/A'))
                price = pd.to_numeric(row.get('Price', 0), errors='coerce') or 0
                change = pd.to_numeric(row.get('Change', 0), errors='coerce') or 0
                companies.append({
                    'ticker': ticker,
                    'name': name,
                    'volume': volume,
                    'price': float(price),
                    'change': float(change)
                })
            with open(f'market_data/{market_code}_companies.json', 'w') as f:
                json.dump(companies, f, indent=2)
            print(f"  ✓ Listed companies exported ({len(companies)} companies)")
        except Exception as e:
            print(f"  ✗ Listed companies failed: {e}")

        print(f"✓ Completed {market_name}")
        return True

    except Exception as e:
        print(f"✗ Failed to export {market_name}: {e}")
        return False

import time
from functools import wraps

def retry_with_backoff(max_retries=3, backoff_factor=2):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt == max_retries - 1:
                        raise
                    wait_time = backoff_factor ** attempt
                    print(f"  Retry {attempt + 1}/{max_retries} after {wait_time}s...")
                    time.sleep(wait_time)
            return None
        return wrapper
    return decorator

# Then wrap your API calls:
@retry_with_backoff(max_retries=3)
def get_index_with_retry(exchange):
    return exchange.get_index_price().tail(10)

def main():
    """Export data in batches of 3 exchanges"""
    print("=" * 60)
    print("African Markets Data Exporter - Batched Mode")
    print("=" * 60)

    exchange_items = list(exchanges.items())
    batch_size = 3
    total = len(exchange_items)
    success_count = 0

    for start in range(0, total, batch_size):
        batch = exchange_items[start:start + batch_size]
        print(f"\n--- Processing batch {start//batch_size + 1} ---")
        for code, name in batch:
            if export_exchange_data(code, name):
                success_count += 1

        # Brief pause between batches (avoids rate limits/timeouts)
        print(f"\nBatch {start//batch_size + 1} complete. Pausing 10 seconds...\n")
        time.sleep(10)

    print("=" * 60)
    print(f"Export complete: {success_count}/{total} exchanges successful")
    print("=" * 60)
    print(f"Data exported to: ./market_data/")

if __name__ == "__main__":
    main()
