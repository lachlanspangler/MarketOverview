import os
import requests
import time
# List of tickers
tickers = [
    "XLB", "XLC", "XLE", "XLF", "XLI", "XLK",
    "XLP", "XLRE", "XLU", "XLV", "XLY"
]

# Directory to save the downloaded files

# Base URL for exporting data from Finviz
base_url = 'https://elite.finviz.com/export.ashx?v=111&f=etf_heldby_{}&ft=5&auth=9670ef49-a23a-4382-8555-3bf6d7f794b9'

# Function to download data for a given ticker
def download_data(ticker):
    time.sleep(2)
    url = base_url.format(ticker)
    response = requests.get(url)
    
    if response.status_code == 200:
        file_path = os.path.join('./', f'{ticker}.csv')
        with open(file_path, 'wb') as file:
            file.write(response.content)
        print(f'Data for {ticker} saved to {file_path}')
    else:
        print(f'Failed to download data for {ticker}')

# Download data for each ticker
for ticker in tickers:
    download_data(ticker)
