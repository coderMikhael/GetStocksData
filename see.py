import pandas as pd
import NseIndia
from nselib import capital_market
from datetime import date
from dateutil.relativedelta import relativedelta
import warnings
import requests
from github import Github

# GitHub token for authentication
GITHUB_TOKEN = "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"

# Authenticate with GitHub
g = Github(GITHUB_TOKEN)
repo = g.get_repo("coderMikhael/GetStocksData")  # Replace with your repo

# Google Drive Upload code
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

SERVICE_ACCOUNT_FILE = 'gdrive.json'
SCOPES = ['https://www.googleapis.com/auth/drive.file']
credentials = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE, scopes=SCOPES)
service = build('drive', 'v3', credentials=credentials)

def upload_to_google_drive(file_path):
    file_metadata = {
        'name': 'stock_data.csv'
    }
    media = MediaFileUpload(file_path, mimetype='text/csv')
    request = service.files().create(
        body=file_metadata,
        media_body=media,
        fields='id'
    )
    response = request.execute()
    file_id = response.get('id')
    print(f'File uploaded successfully. File ID: {file_id}')

# Commit file to GitHub
def commit_to_github(file_path):
    with open(file_path, 'r') as file:
        content = file.read()
    
    # Create a new file or update an existing file in the repo
    repo.create_file(
        path='stock_data.csv',
        message='Add stock data CSV',
        content=content,
        branch='main'  # or 'master', depending on your default branch
    )
    print('File committed to GitHub.')

warnings.filterwarnings('ignore')
GIST_URL = "https://gist.githubusercontent.com/coderMikhael/e170ce9f636b0926206ea66245fa3ebc/raw/841160a56d36de5f3a69d84837e4f8c7437330f9/symbol_list.txt"

def fetch_symbol_list():
    response = requests.get(GIST_URL)
    response.raise_for_status()
    symbol_list = response.text.strip().split('\n')
    return symbol_list

def fetchStockData(symbol):
    nse = NseIndia.NSE()
    start_day = date.today()
    end_day = start_day - relativedelta(months=3)
    start_day = start_day.strftime("%d-%m-%Y")
    end_day = end_day.strftime("%d-%m-%Y")

    data = capital_market.price_volume_and_deliverable_position_data(symbol, end_day, start_day)
    df = pd.DataFrame(data)
    vol_list = df['TotalTradedQuantity'].tail(10)
    vol_list = vol_list.str.replace(',', '').astype(float)

    newd1 = nse.equity_info(symbol)
    newd2 = nse.equity_extra_info(symbol)

    ltp = newd1['priceInfo']['lastPrice']
    high = newd1['priceInfo']['intraDayHighLow']['max']
    low = newd1['priceInfo']['intraDayHighLow']['min']
    prevclose = newd1['priceInfo']['previousClose']
    change = round(ltp - prevclose, 2)
    avgvol = vol_list.mean()
    delp = df.iloc[-1]['%DlyQttoTradedQty']
    if delp == '-':
        delp = 0
    sympe = newd2['metadata']['pdSymbolPe']
    _3monHigh = df['ClosePrice'].max()
    _3monLow = df['ClosePrice'].min()

    stockData = {
        "Symbol": symbol,
        "LTP": ltp,
        "High": high,
        "Low": low,
        "PreviousClose": prevclose,
        "Change": float(change),
        "Last 10D avg Volume": avgvol,
        "Delivery %": delp,
        "Symbol P/E": sympe,
        "3 months high": _3monHigh,
        "3 months low": _3monLow
    }
    print(f"Done with {symbol}.")
    return stockData

def save_stock_data(symbol_list):
    stock_data_list = []

    for symbol in symbol_list:
        data = fetchStockData(symbol)
        stock_data_list.append(data)
    df = pd.DataFrame(stock_data_list)
    df_cleaned = df.drop_duplicates(subset='Symbol', keep='first')
    df_sorted = df_cleaned.sort_values(by='Symbol P/E')
    df_sorted.to_csv('stock_data.csv', index=False)
    print("All data saved, sorted by P/E, and duplicates removed in stock_data.csv")

def main():
    symbol_list = fetch_symbol_list()
    save_stock_data(symbol_list)
    commit_to_github('stock_data.csv')  # Commit the file to GitHub

if __name__ == "__main__":
    main()
    print("Execution Complete.")
