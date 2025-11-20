from flask import Flask, request, jsonify
import sqlite3
import requests
from datetime import datetime, timedelta
import os
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import yfinance as yf

app = Flask(__name__)

# Configuration
API_KEY = "Kg4yZ5T4QGXReGhZPik3gYD9kVjFPcOQfxqtVJ7gbHOk-Ks4"
COINDESK_API_KEY = "6c711a4a03b4884ffc3df8016bed4b72ae768910a9130442037eecef766ca50b"
DB_PATH = os.path.join(os.getcwd(), "crypto_prices.db")
CREDENTIALS_PATH = os.path.join(os.getcwd(), "credentials.json")
SHEET_NAME = "Historical Prices - Crypto, SP500, CD5"

# Ticker to Messari slug mapping
TICKER_TO_SLUG = {
    'BTC': 'bitcoin',
    'ETH': 'ethereum',
    'SOL': 'solana',
    'AVAX': 'avalanche',
    'AAVE': 'aave',
    'SUI': 'sui',
    'PENDLE': 'pendle',
    'IMX': 'immutable-x',
    'HYPE': 'hyperliquid',
    'CBBTC': 'coinbase-wrapped-btc',
    'USDC': 'circle-usdc-stablecoin',
    'LINK': 'chainlink',
    'UNI': 'uniswap',
    'RPL': 'rocket-pool',
    'SUSDE': 'ethena-staked-usde',
    'SENA': 'ethena-staked-ena',
    'SYRUP': 'maple-finance',
    'WSTETH': 'wrapped-steth',
    'USDT': 'tether',
    'ENA': 'ethena',
    'PEPE': 'pepe',
    'XLM': 'stellar',
    'SPX': 'spx6900',
    'FF': 'falcon-finance-ff',
    'FARTCOIN': 'fartcoin',
    'RESOLV': 'resolv',
}

# Yahoo Finance tickers (for traditional markets)
YAHOO_TICKERS = {
    'SP500': '^GSPC'  # S&P 500 Index
}

# CoinDesk API tickers
COINDESK_TICKERS = {
    'CD5': 'CD5-USD'  # CoinDesk 5 Index
}

def init_database():
    """Initialize SQLite database with tables"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS prices (
            date TEXT NOT NULL,
            asset TEXT NOT NULL,
            close_price REAL NOT NULL,
            PRIMARY KEY (date, asset)
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS assets_tracked (
            ticker TEXT PRIMARY KEY,
            slug TEXT NOT NULL,
            first_seen_date TEXT NOT NULL,
            last_updated TEXT NOT NULL
        )
    ''')
    
    conn.commit()
    conn.close()
    print("✅ Database initialized")

def get_google_sheet():
    """Connect to Google Sheets"""
    try:
        import gspread
        from google_auth_oauthlib.flow import InstalledAppFlow
        from google.auth.transport.requests import Request
        import pickle
        
        scopes = [
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive'
        ]
        
        creds = None
        token_path = os.path.expanduser("~/Desktop/crypto-tracker/token.pickle")
        
        # Load saved credentials
        if os.path.exists(token_path):
            with open(token_path, 'rb') as token:
                creds = pickle.load(token)
        
        # If no valid credentials, authenticate
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    CREDENTIALS_PATH, scopes)
                creds = flow.run_local_server(port=0)
            
            # Save credentials for next time
            with open(token_path, 'wb') as token:
                pickle.dump(creds, token)
        
        client = gspread.authorize(creds)
        sheet = client.open(SHEET_NAME).sheet1
        return sheet
        
    except Exception as e:
        print(f"❌ Error connecting to Google Sheets: {e}")
        import traceback
        traceback.print_exc()
        return None

def sync_to_google_sheets():
    """Sync database to Google Sheets in wide format"""
    print("\n=== Starting Google Sheets Sync ===")
    
    try:
        # Read data from SQLite
        conn = sqlite3.connect(DB_PATH)
        df = pd.read_sql_query("SELECT date, asset, close_price FROM prices ORDER BY date, asset", conn)
        conn.close()
        
        if df.empty:
            print("No data to sync")
            return False
        
        print(f"📊 Found {len(df)} price records")
        
        # Pivot to wide format: rows = dates, columns = assets
        pivot_df = df.pivot(index='date', columns='asset', values='close_price')
        pivot_df = pivot_df.reset_index()
        pivot_df = pivot_df.fillna('')  # Replace NaN with empty string
        
        # Sort by date descending (most recent first)
        pivot_df = pivot_df.sort_values('date', ascending=False)
        
        print(f"📈 Pivoted to wide format: {len(pivot_df)} rows × {len(pivot_df.columns)} columns")
        
        # Connect to Google Sheets
        sheet = get_google_sheet()
        if not sheet:
            return False
        
        # Clear existing data
        sheet.clear()
        
        # Convert to list of lists for upload
        data_to_upload = [pivot_df.columns.tolist()] + pivot_df.values.tolist()
        
        # Upload to sheet
        sheet.update(values=data_to_upload, range_name='A1')
        
        print(f"✅ Synced {len(pivot_df)} rows to Google Sheets")
        print(f"📋 Sheet URL: https://docs.google.com/spreadsheets/d/{sheet.spreadsheet.id}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error syncing to Google Sheets: {e}")
        import traceback
        traceback.print_exc()
        return False

def get_messari_slug(ticker):
    """Get Messari slug for a ticker"""
    if ticker in TICKER_TO_SLUG:
        return TICKER_TO_SLUG[ticker]
    
    url = f"https://data.messari.io/api/v2/assets/{ticker}/profile"
    headers = {"x-messari-api-key": API_KEY}
    
    try:
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code == 200:
            slug = response.json()['data']['slug']
            print(f"✅ Found slug for {ticker}: {slug}")
            return slug
    except Exception as e:
        print(f"❌ Could not find slug for {ticker}: {e}")
    
    return None

def fetch_historical_prices(slug, start_date, end_date):
    """Fetch historical prices from Messari"""
    url = f"https://data.messari.io/api/v1/assets/{slug}/metrics/price/time-series"
    headers = {"x-messari-api-key": API_KEY}
    params = {
        "start": start_date,
        "end": end_date,
        "interval": "1d"
    }
    
    try:
        response = requests.get(url, headers=headers, params=params, timeout=30)
        if response.status_code == 200:
            data = response.json()
            if data['data']['values']:
                return data['data']['values']
    except Exception as e:
        print(f"❌ Error fetching prices for {slug}: {e}")
    
    return None

def fetch_yahoo_prices(ticker, start_date, end_date):
    """Fetch historical prices from Yahoo Finance and forward-fill weekends"""
    try:
        yahoo_ticker = YAHOO_TICKERS.get(ticker)
        if not yahoo_ticker:
            return None
        
        print(f"  Fetching {ticker} from Yahoo Finance ({start_date} to {end_date})...")
        stock = yf.Ticker(yahoo_ticker)
        
        # Add one day to end_date to ensure we get the target date
        end_dt = datetime.strptime(end_date, '%Y-%m-%d') + timedelta(days=1)
        end_date_plus = end_dt.strftime('%Y-%m-%d')
        
        df = stock.history(start=start_date, end=end_date_plus)
        
        if df.empty:
            print(f"  No Yahoo data returned for {ticker}")
            return None
        
        print(f"  Got {len(df)} records from Yahoo Finance")
        
        # Convert to dictionary for easy lookup by date
        price_dict = {}
        for date, row in df.iterrows():
            date_naive = date.replace(tzinfo=None)
            date_str = date_naive.strftime('%Y-%m-%d')
            price_dict[date_str] = {
                'open': row['Open'],
                'high': row['High'],
                'low': row['Low'],
                'close': row['Close'],
                'volume': row['Volume']
            }
        
        # Now fill all dates including weekends with forward-fill
        start_dt = datetime.strptime(start_date, '%Y-%m-%d')
        end_dt = datetime.strptime(end_date, '%Y-%m-%d')
        
        prices = []
        current_date = start_dt
        last_known_price = None
        
        while current_date <= end_dt:
            date_str = current_date.strftime('%Y-%m-%d')
            
            if date_str in price_dict:
                # We have actual data for this date
                last_known_price = price_dict[date_str]
            
            # Use last known price (forward-fill for weekends/holidays)
            if last_known_price:
                timestamp_ms = int(current_date.timestamp() * 1000)
                prices.append([
                    timestamp_ms,
                    last_known_price['open'],
                    last_known_price['high'],
                    last_known_price['low'],
                    last_known_price['close'],
                    last_known_price['volume']
                ])
            
            current_date += timedelta(days=1)
        
        print(f"  Filled to {len(prices)} records (including weekends)")
        return prices
        
    except Exception as e:
        print(f"❌ Error fetching Yahoo prices for {ticker}: {e}")
        import traceback
        traceback.print_exc()
        return None

def fetch_coindesk_prices(ticker, start_date, end_date):
    """Fetch historical prices from CoinDesk API for indices like CD5"""
    try:
        coindesk_instrument = COINDESK_TICKERS.get(ticker)
        if not coindesk_instrument:
            return None
        
        print(f"  Fetching {ticker} from CoinDesk API ({start_date} to {end_date})...")
        
        # Convert dates to timestamps (CoinDesk uses Unix timestamps in seconds)
        start_dt = datetime.strptime(start_date, '%Y-%m-%d')
        end_dt = datetime.strptime(end_date, '%Y-%m-%d') + timedelta(days=1)  # Include end date
        
        start_ts = int(start_dt.timestamp())
        end_ts = int(end_dt.timestamp())
        
        # CoinDesk API endpoint for historical data
        # Using days endpoint for better performance
        url = f"https://data-api.coindesk.com/index/cc/v1/historical/days"
        headers = {
            "Authorization": f"Bearer {COINDESK_API_KEY}"
        }
        params = {
            "market": "cd_mc",
            "instrument": coindesk_instrument,  # singular, not plural
            "from_ts": start_ts,
            "to_ts": end_ts,
            "groups": "OHLC",  # Required parameter
            "limit": 2000  # Max results per request
        }
        
        all_data = []
        
        # Paginate through results if needed
        while True:
            response = requests.get(url, headers=headers, params=params, timeout=30)
            
            if response.status_code != 200:
                print(f"  ❌ CoinDesk API error: {response.status_code}")
                print(f"  Response: {response.text}")
                return None
            
            data = response.json()
            
            if not data.get('Data'):
                break
            
            all_data.extend(data['Data'])
            
            # Check if we need to paginate
            if len(data['Data']) < 2000:
                break
            
            # Update to_ts for next batch
            earliest_ts = data['Data'][-1]['TIMESTAMP']
            params['to_ts'] = earliest_ts - 86400  # Subtract one day
        
        if not all_data:
            print(f"  No data returned from CoinDesk API")
            return None
        
        print(f"  Got {len(all_data)} daily records from CoinDesk API")
        
        # Convert to our format and fill weekends
        daily_prices = {}
        for entry in all_data:
            timestamp = entry['TIMESTAMP']
            date_obj = datetime.fromtimestamp(timestamp)
            date_str = date_obj.strftime('%Y-%m-%d')
            
            daily_prices[date_str] = {
                'timestamp': timestamp,
                'open': entry.get('OPEN', entry['CLOSE']),
                'high': entry.get('HIGH', entry['CLOSE']),
                'low': entry.get('LOW', entry['CLOSE']),
                'close': entry['CLOSE'],
                'volume': 0  # CoinDesk index doesn't have volume
            }
        
        # Now fill all dates including weekends with forward-fill
        start_dt = datetime.strptime(start_date, '%Y-%m-%d')
        end_dt = datetime.strptime(end_date, '%Y-%m-%d')
        
        prices = []
        current_date = start_dt
        last_known_price = None
        
        while current_date <= end_dt:
            date_str = current_date.strftime('%Y-%m-%d')
            
            if date_str in daily_prices:
                # We have actual data for this date
                last_known_price = daily_prices[date_str]
            
            # Use last known price (forward-fill for weekends/holidays)
            if last_known_price:
                timestamp_ms = int(current_date.timestamp() * 1000)
                prices.append([
                    timestamp_ms,
                    last_known_price['open'],
                    last_known_price['high'],
                    last_known_price['low'],
                    last_known_price['close'],
                    last_known_price['volume']
                ])
            
            current_date += timedelta(days=1)
        
        print(f"  Filled to {len(prices)} records (including weekends)")
        return prices
        
    except Exception as e:
        print(f"❌ Error fetching CoinDesk prices for {ticker}: {e}")
        import traceback
        traceback.print_exc()
        return None

def fill_date_gaps(target_date):
    """Fill any gaps in dates for all tracked assets using forward-fill for weekends/holidays"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Get the latest date in database
    cursor.execute("SELECT MAX(date) FROM prices")
    result = cursor.fetchone()
    last_date_str = result[0]
    
    if not last_date_str:
        print("ℹ️  Database is empty, no gaps to fill")
        conn.close()
        return
    
    last_date = datetime.strptime(last_date_str, '%Y-%m-%d')
    target_date_obj = datetime.strptime(target_date, '%Y-%m-%d')
    
    # Calculate gap
    days_gap = (target_date_obj - last_date).days
    
    if days_gap <= 1:
        print("ℹ️  No date gaps detected")
        conn.close()
        return
    
    print(f"\n⚠️  GAP DETECTED: {days_gap - 1} days missing between {last_date_str} and {target_date}")
    print("🔄 Filling gaps with forward-fill method (weekends/holidays use last known price)...")
    
    # Get all tracked assets
    cursor.execute("SELECT ticker FROM assets_tracked")
    tracked_tickers = [row[0] for row in cursor.fetchall()]
    
    # For each missing day, forward-fill from the most recent price
    current_date = last_date + timedelta(days=1)
    filled_count = 0
    
    while current_date < target_date_obj:
        date_str = current_date.strftime('%Y-%m-%d')
        
        for ticker in tracked_tickers:
            # Get the most recent price for this ticker before the current date
            cursor.execute('''
                SELECT close_price FROM prices 
                WHERE asset = ? AND date < ?
                ORDER BY date DESC
                LIMIT 1
            ''', (ticker, date_str))
            
            result = cursor.fetchone()
            
            if result:
                last_known_price = result[0]
                
                # Insert this price for the missing date
                cursor.execute('''
                    INSERT OR REPLACE INTO prices (date, asset, close_price)
                    VALUES (?, ?, ?)
                ''', (date_str, ticker, last_known_price))
                
                filled_count += 1
        
        current_date += timedelta(days=1)
    
    conn.commit()
    conn.close()
    print(f"✅ Forward-filled {filled_count} price records across {days_gap - 1} days!")

def is_valid_date_for_processing(date_str):
    """Check if date should be processed (must be yesterday or earlier)"""
    target_date = datetime.strptime(date_str, '%Y-%m-%d').date()
    today = datetime.now().date()
    
    if target_date >= today:
        return False
    return True

def process_positions(positions_data):
    """Main function to process daily positions"""
    print(f"\n📊 Processing {len(positions_data)} positions...")
    
    init_database()
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    target_date = positions_data[0]['Date']
    print(f"📅 Target date: {target_date}")
    
    # Check if date is valid (not today)
    if not is_valid_date_for_processing(target_date):
        print(f"⚠️  Skipping {target_date} - market hasn't closed yet. Only processing completed trading days.")
        conn.close()
        return False
    
    # Fill any date gaps FIRST
    fill_date_gaps(target_date)
    
    # Extract unique tickers
    current_tickers = set()
    for position in positions_data:
        ticker = position['Asset Name'].strip().upper()
        if ticker:
            current_tickers.add(ticker)
    
    print(f"🔍 Found {len(current_tickers)} unique assets: {current_tickers}")
    
    # Check for new assets
    cursor.execute("SELECT ticker FROM assets_tracked")
    known_tickers = set(row[0] for row in cursor.fetchall())
    
    new_tickers = current_tickers - known_tickers
    all_tickers = current_tickers | known_tickers
    
    if new_tickers:
        print(f"\n🆕 NEW assets detected: {new_tickers}")
        print("⏳ Backfilling historical data from Jan 1, 2015...")
        
        for ticker in new_tickers:
            # Check if CoinDesk ticker
            if ticker in COINDESK_TICKERS:
                print(f"📊 Processing {ticker} from CoinDesk API...")
                historical_prices = fetch_coindesk_prices(ticker, "2015-01-01", target_date)
                slug = COINDESK_TICKERS[ticker]
            # Check if Yahoo Finance ticker
            elif ticker in YAHOO_TICKERS:
                print(f"📈 Processing {ticker} from Yahoo Finance...")
                historical_prices = fetch_yahoo_prices(ticker, "2015-01-01", target_date)
                slug = YAHOO_TICKERS[ticker]
            else:
                slug = get_messari_slug(ticker)
                if not slug:
                    print(f"⚠️  Skipping {ticker} - could not find slug")
                    continue
                
                historical_prices = fetch_historical_prices(slug, "2015-01-01", target_date)
            
            if historical_prices:
                print(f"📥 Fetched {len(historical_prices)} days of data for {ticker}")
                
                for price_data in historical_prices:
                    timestamp, open_p, high, low, close, volume = price_data
                    date_str = datetime.fromtimestamp(timestamp/1000).strftime('%Y-%m-%d')
                    
                    cursor.execute('''
                        INSERT OR REPLACE INTO prices (date, asset, close_price)
                        VALUES (?, ?, ?)
                    ''', (date_str, ticker, close))
                
                cursor.execute('''
                    INSERT INTO assets_tracked (ticker, slug, first_seen_date, last_updated)
                    VALUES (?, ?, ?, ?)
                ''', (ticker, slug, target_date, datetime.now().isoformat()))
                
                conn.commit()
                print(f"✅ Saved {ticker} historical data")
    
    # Update today's price for all tracked assets
    print(f"\n🔄 Updating {target_date} prices for all {len(all_tickers)} tracked assets...")
    
    for ticker in all_tickers:
        # Check if CoinDesk ticker
        if ticker in COINDESK_TICKERS:
            prices = fetch_coindesk_prices(ticker, target_date, target_date)
        # Check if Yahoo Finance ticker
        elif ticker in YAHOO_TICKERS:
            prices = fetch_yahoo_prices(ticker, target_date, target_date)
        else:
            cursor.execute("SELECT slug FROM assets_tracked WHERE ticker = ?", (ticker,))
            result = cursor.fetchone()
            
            if result:
                slug = result[0]
            else:
                slug = get_messari_slug(ticker)
                if not slug:
                    continue
            
            prices = fetch_historical_prices(slug, target_date, target_date)
        
        if prices:
            timestamp, open_p, high, low, close, volume = prices[0]
            
            cursor.execute('''
                INSERT OR REPLACE INTO prices (date, asset, close_price)
                VALUES (?, ?, ?)
            ''', (target_date, ticker, close))
            
            cursor.execute('''
                UPDATE assets_tracked 
                SET last_updated = ?
                WHERE ticker = ?
            ''', (datetime.now().isoformat(), ticker))
    
    conn.commit()
    conn.close()
    
    print(f"\n✅ Database updated successfully!")
    
    # Sync to Google Sheets
    sync_to_google_sheets()
    
    return True

@app.route('/webhook', methods=['POST'])
def webhook():
    """Endpoint for Zapier webhook"""
    try:
        data = request.get_json()
        
        if not data or 'positions' not in data:
            return jsonify({'error': 'No positions data provided'}), 400
        
        # Handle if positions comes as a JSON string
        positions_raw = data.get('positions', '')
        if isinstance(positions_raw, str):
            import json as json_module
            positions_dict = json_module.loads(positions_raw)
            positions_data = positions_dict['positions']
        else:
            positions_data = data['positions']
        
        # Process the positions
        success = process_positions(positions_data)
        
        if success:
            return jsonify({'status': 'success', 'message': 'Database updated and synced to Google Sheets!'}), 200
        else:
            return jsonify({'status': 'error', 'message': 'Processing failed'}), 500
            
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/manual-sync', methods=['POST'])
def manual_sync():
    """Manually trigger Google Sheets sync"""
    try:
        success = sync_to_google_sheets()
        
        if success:
            return jsonify({'status': 'success', 'message': 'Synced to Google Sheets!'}), 200
        else:
            return jsonify({'status': 'error', 'message': 'Sync failed'}), 500
            
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint"""
    return jsonify({'status': 'healthy'}), 200

if __name__ == '__main__':
    print("🚀 Starting Crypto Price Tracker Server...")
    print("📍 Listening on http://localhost:5001")
    print("🔗 Webhook endpoint: http://localhost:5001/webhook")
    print("📊 Manual sync: http://localhost:5001/manual-sync")
    init_database()
    app.run(host='0.0.0.0', port=5001, debug=True)