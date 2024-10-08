import pandas as pd
import NseIndia
from nselib import capital_market
from datetime import date
from dateutil.relativedelta import relativedelta
import warnings
import requests 
from github import Github
import os




warnings.filterwarnings('ignore')
GIST_URL = "https://gist.githubusercontent.com/coderMikhael/e170ce9f636b0926206ea66245fa3ebc/raw/8f1c0af1e4c90653e4f755525fda854ea320f7e3/symbol_list.txt"


def upload_csv_to_github():
    # Load environment variable
    token = os.getenv("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")
    if not token:
        raise ValueError("GitHub Personal Access Token not found in environment variables.")

    g = Github(token)
    repo_name = "coderMikhael/GetStocksData"
    file_name = "stock_data.csv"  # Adjust if necessary (e.g., "data/stock_data.csv")
    repo = g.get_repo(repo_name)

    # Read the content of the new CSV file
    with open(file_name, "r") as file:
        content = file.read()

    try:
        # Try to get the file from the repo
        existing_file = repo.get_contents(file_name)
        # Update the existing file
        repo.update_file(existing_file.path, "Update stock data CSV", content, existing_file.sha)
        print(f"Successfully updated {file_name} in {repo_name}.")
    except Exception as e:
        # If the file does not exist, create a new one
        repo.create_file(file_name, "Add new stock data CSV", content)
        print(f"Successfully created {file_name} in {repo_name}.")




def fetch_symbol_list():
    response = requests.get(GIST_URL)
    response.raise_for_status()
    symbol_list = response.text.strip().split('\n')
    symbol_list = ['20MICRONS', 'ZYDUSWELL', 'KALYAN', 'HITECH']
    return symbol_list

def fetchStockData(symbol):
    print(symbol)
    nse = NseIndia.NSE()
    start_day = date.today()
    end_day = start_day - relativedelta(months=3)
    start_day = start_day.strftime("%d-%m-%Y")
    end_day = end_day.strftime("%d-%m-%Y")

   
    data = capital_market.price_volume_and_deliverable_position_data(
        symbol, end_day, start_day)
    df = pd.DataFrame(data)
    vol_list = df['TotalTradedQuantity'].tail(10)
    vol_list = vol_list.astype(str).str.replace(',', '').astype(float)


 
    newd1 = nse.equity_info(symbol)
    newd2 = nse.equity_extra_info(symbol)

    if 'priceInfo' not in newd1:
        print(f"PriceInfo missing in response for {symbol}")
        return {
            "Symbol": symbol,
            "LTP": 9999999,
            "High": 9999999,
            "Low": 9999999,
            "PreviousClose": 9999999,
            "Change": 9999999,
            "Last 10D avg Volume": 9999999,
            "Delivery %": 9999999,
            "Symbol P/E": 9999999,
            "3 months high": 9999999,
            "3 months low": 9999999
        }

    if 'metadata' not in newd2:
        print(f"Metadata missing in response for {symbol}")
        return {
            "Symbol": symbol,
            "LTP": 9999999,
            "High": 9999999,
            "Low": 9999999,
            "PreviousClose": 9999999,
            "Change": 9999999,
            "Last 10D avg Volume": 9999999,
            "Delivery %": 9999999,
            "Symbol P/E": 9999999,
            "3 months high": 9999999,
            "3 months low": 9999999
        }

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

    def convert_to_float(value):
        if value in ['', '-', 'NA']:  # Check for empty, hyphen, or 'NA' values
            return 0.0
        try:
            return float(value)
        except ValueError:
            return 9999999.0

    sympe = convert_to_float(sympe)

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
    print(df_sorted)
    df_sorted.to_csv('stock_data.csv', index=False)
    print("All data saved, sorted by P/E, and duplicates removed in stock_data.csv")


def main():
    symbol_list = fetch_symbol_list()
    save_stock_data(symbol_list)


if __name__ == "__main__":
    main()
    print("Execution Complete.")
    upload_csv_to_github()
    print("File uploaded to Github.")
