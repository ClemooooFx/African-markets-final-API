"""
Python script to export afrimarket data to JSON files
Run this script to generate data files that the frontend can use
Usage: python export_market_data.py
"""

import afrimarket as afm
import json
import os

# Create data directory
os.makedirs('market_data', exist_ok=True)

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
        # Create exchange object
        exchange = afm.Exchange(market=market_code)
        
        # 1. Get index price (last 10 days)
        try:
            index_df = exchange.get_index_price()
            index_df_last_10 = index_df.tail(10).copy()
            
            # Convert Date column to string format
            if 'Date' in index_df_last_10.columns:
                index_df_last_10['Date'] = index_df_last_10['Date'].astype(str)
            
            index_data = index_df_last_10.to_dict('records')
            
            with open(f'market_data/{market_code}_index.json', 'w') as f:
                json.dump(index_data, f, indent=2)
            print(f"  ✓ Index price exported ({len(index_data)} days)")
        except Exception as e:
            print(f"  ✗ Index price failed: {e}")
        
        # 2. Get top gainers
        try:
            gainers_df = exchange.get_top_gainers()
            gainers = []
            
            for index, row in gainers_df.iterrows():
                try:
                    # Handle potential conversion issues
                    price_val = row.iloc[1]
                    if price_val == '' or price_val is None or str(price_val).strip() == '\u200b':
                        price_val = 0
                    else:
                        price_val = float(price_val)
                    
                    gainers.append({
                        'ticker': str(row.iloc[0]),
                        'price': price_val,
                        'change': str(row.iloc[2])
                    })
                except (ValueError, TypeError) as e:
                    # Skip rows with invalid data
                    continue
            
            with open(f'market_data/{market_code}_gainers.json', 'w') as f:
                json.dump(gainers, f, indent=2)
            print(f"  ✓ Top gainers exported ({len(gainers)} stocks)")
        except Exception as e:
            print(f"  ✗ Top gainers failed: {e}")
        
        # 3. Get bottom losers
        try:
            losers_df = exchange.get_bottom_losers()
            losers = []
            
            for index, row in losers_df.iterrows():
                try:
                    # Handle potential conversion issues
                    price_val = row.iloc[1]
                    if price_val == '' or price_val is None or str(price_val).strip() == '\u200b':
                        price_val = 0
                    else:
                        price_val = float(price_val)
                    
                    losers.append({
                        'ticker': str(row.iloc[0]),
                        'price': price_val,
                        'change': str(row.iloc[2])
                    })
                except (ValueError, TypeError) as e:
                    # Skip rows with invalid data
                    continue
            
            with open(f'market_data/{market_code}_losers.json', 'w') as f:
                json.dump(losers, f, indent=2)
            print(f"  ✓ Bottom losers exported ({len(losers)} stocks)")
        except Exception as e:
            print(f"  ✗ Bottom losers failed: {e}")
        
        # 4. Get listed companies
        try:
            companies_df = exchange.get_listed_companies()
            companies_df = companies_df.fillna('')
            
            companies = []
            for index, row in companies_df.iterrows():
                companies.append({
                    'ticker': str(row['Ticker']),
                    'name': str(row['Name']),
                    'volume': str(row['Volume']) if row['Volume'] != '' else 'N/A',
                    'price': float(row['Price']) if row['Price'] != '' else 0,
                    'change': float(row['Change']) if row['Change'] != '' else 0
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

def main():
    """Export data for all exchanges"""
    print("=" * 60)
    print("African Markets Data Exporter")
    print("=" * 60)
    
    success_count = 0
    
    for code, name in exchanges.items():
        if export_exchange_data(code, name):
            success_count += 1
    
    print("\n" + "=" * 60)
    print(f"Export complete: {success_count}/{len(exchanges)} exchanges successful")
    print("=" * 60)
    print(f"\nData exported to: ./market_data/")
    print("You can now use the frontend with local JSON files.")

if __name__ == "__main__":
    main()
