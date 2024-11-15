from flask import Flask, render_template, jsonify
import pandas as pd
import aiohttp
import asyncio
from datetime import datetime, timedelta
import os
import sqlite3
import logging
from tqdm.asyncio import tqdm_asyncio
import threading

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s:%(message)s')

app = Flask(__name__)

# Load Crypto tickers from CSV
crypto_df = pd.read_csv('data/polygon_cryptos.csv')
crypto_tickers = crypto_df['ticker'].tolist()

# Load ETF data from CSV files
etf_data_path = 'data/etf_data'
etf_tickers = ["XLB", "XLC", "XLE", "XLF", "XLI", "XLK", "XLP", "XLRE", "XLU", "XLV", "XLY"]
etf_data = {}

for ticker in etf_tickers:
    file_path = os.path.join(etf_data_path, f'{ticker}.csv')
    if os.path.exists(file_path):
        etf_data[ticker] = pd.read_csv(file_path)['Ticker'].tolist()
    else:
        logging.warning(f'ETF data file for {ticker} not found. Skipping...')

# Load IDX data from CSV files
idx_data_path = 'data/idx_data'
idx_tickers = {
    'dji': 'idx_dji.csv',
    'ndx': 'idx_ndx.csv',
    'rut': 'idx_rut.csv',
    'sp500': 'idx_sp500.csv'
}
idx_data = {}

for ticker, filename in idx_tickers.items():
    file_path = os.path.join(idx_data_path, filename)
    if os.path.exists(file_path):
        idx_data[ticker] = pd.read_csv(file_path)['Ticker'].tolist()
    else:
        logging.warning(f'IDX data file for {ticker} not found. Skipping...')

# Define the time intervals for fetching data
time_intervals = [
    ("minute", 1),
    ("hour", 1),
    ("day", 1),
    ("week", 1),
    ("month", 1)
]

# Define your Polygon.io API key
API_KEY = 'jz7gBlGqhxbOyYDpEU1X8ddhOwIsU881'

# Asynchronous functions for data fetching (unchanged from your original code)
async def fetch_price(session, ticker, semaphore, retries=3):
    async with semaphore:
        url = f'https://api.polygon.io/v2/last/trade/{ticker}?apiKey={API_KEY}'
        for attempt in range(retries):
            try:
                async with session.get(url, timeout=2) as response:
                    data = await response.json()
                    if 'results' in data:
                        return ticker, data['results']['p']
                    return ticker, None
            except asyncio.TimeoutError:
                logging.error(f'Timeout error for ticker {ticker} on attempt {attempt + 1}')
            except Exception as e:
                logging.error(f'Error fetching price for ticker {ticker} on attempt {attempt + 1}: {e}')
        return ticker, None

async def get_open_price_for_time_range(session, ticker, timespan, multiplier, start_date, end_date, semaphore, retries=3):
    async with semaphore:
        url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/{multiplier}/{timespan}/{start_date}/{end_date}?adjusted=true&sort=asc&apiKey={API_KEY}"
        for attempt in range(retries):
            try:
                async with session.get(url, timeout=2) as response:
                    data = await response.json()
                    if 'results' in data and len(data['results']) > 0:
                        if timespan != 'minute':
                            return ticker, data['results'][0]['o']
                        else:
                            return ticker, data['results'][multiplier-1]['o']
                    return ticker, None
            except asyncio.TimeoutError:
                logging.error(f'Timeout error for open price range for ticker {ticker} on attempt {attempt + 1}')
            except Exception as e:
                logging.error(f'Error fetching open price range for ticker {ticker} on attempt {attempt + 1}: {e}')
        return ticker, None

async def calculate_breadth(tickers, timespan, multiplier):
    advancing = 0
    declining = 0
    unchanged = 0
    prev_prices = {}
    new_prices = {}

    connector = aiohttp.TCPConnector(limit=200)
    semaphore = asyncio.Semaphore(50)

    async with aiohttp.ClientSession(connector=connector) as session:
        start_date = datetime.now() - timedelta(days=30*multiplier if timespan == "month" else multiplier)
        start_date = start_date.strftime('%Y-%m-%d')
        end_date = datetime.now().strftime('%Y-%m-%d')

        tasks = [get_open_price_for_time_range(session, ticker, timespan, multiplier, start_date, end_date, semaphore) for ticker in tickers]
        res = await tqdm_asyncio.gather(*tasks)

        for symbol, price in res:
            prev_prices[symbol] = price

        tasks = [fetch_price(session, ticker, semaphore) for ticker in tickers]
        res = await tqdm_asyncio.gather(*tasks)

        for symbol, price in res:
            new_prices[symbol] = price

        for ticker in tickers:
            if prev_prices[ticker] is not None and new_prices[ticker] is not None:
                if prev_prices[ticker] < new_prices[ticker]:
                    advancing += 1
                elif prev_prices[ticker] > new_prices[ticker]:
                    declining += 1
                else:
                    unchanged += 1

    return {
        "Declining": declining,
        "Unchanged": unchanged,
        "Advancing": advancing
    }

def init_db():
    if not os.path.exists('breadth_data.db'):
        conn = sqlite3.connect('breadth_data.db')
        c = conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS breadth_data
                     (index_name TEXT, multiplier INTEGER, timespan TEXT, declining INTEGER, unchanged INTEGER, advancing INTEGER, timestamp TEXT)''')
        conn.commit()
        conn.close()

def save_to_db(index_name, multiplier, timespan, breadth):
    conn = sqlite3.connect('breadth_data.db')
    c = conn.cursor()
    c.execute('''INSERT INTO breadth_data (index_name, multiplier, timespan, declining, unchanged, advancing, timestamp)
                 VALUES (?, ?, ?, ?, ?, ?, ?)''',
              (index_name, multiplier, timespan, breadth['Declining'], breadth['Unchanged'], breadth['Advancing'], datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
    conn.commit()
    conn.close()

async def get_data():
    for name, tickers in [("IDX", idx_data.keys()), ("Cryptos", crypto_tickers), ("ETF", etf_tickers)]:
        if name == "ETF":
            for timespan, multiplier in time_intervals:
                for ticker in tickers:
                    if ticker in etf_data:
                        breadth = await calculate_breadth(etf_data[ticker], timespan, multiplier)
                        save_to_db(ticker, multiplier, timespan, breadth)
        elif name == "IDX":
            for timespan, multiplier in time_intervals:
                for ticker in tickers:
                    if ticker in idx_data:
                        breadth = await calculate_breadth(idx_data[ticker], timespan, multiplier)
                        save_to_db(ticker, multiplier, timespan, breadth)
        else:
            for timespan, multiplier in time_intervals:
                breadth = await calculate_breadth(tickers, timespan, multiplier)
                save_to_db(name, multiplier, timespan, breadth)

async def operate():
    while True:
        logging.info('Getting and loading data...')
        await get_data()
        logging.info('Data procured')
        await asyncio.sleep(60)

# Start data collection in a background thread
def start_data_collection():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(get_data())

# Flask routes
@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/breadth_data')
def breadth_data():
    conn = sqlite3.connect('breadth_data.db')
    c = conn.cursor()
    c.execute('SELECT * FROM breadth_data ORDER BY timestamp DESC LIMIT 50')
    rows = c.fetchall()
    conn.close()

    data = [{
        "index_name": row[0],
        "multiplier": row[1],
        "timespan": row[2],
        "declining": row[3],
        "unchanged": row[4],
        "advancing": row[5],
        "timestamp": row[6]
    } for row in rows]

    return jsonify(data)

if __name__ == '__main__':
    init_db()

    # Start the data collection in a background thread
    data_thread = threading.Thread(target=start_data_collection, daemon=True)
    data_thread.start()

    # Start the Flask server
    app.run(debug=True)