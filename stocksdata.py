import pandas as pd
import NseIndia
from nselib import capital_market
from datetime import date, datetime, timedelta
from dateutil.relativedelta import relativedelta
import warnings
import os

warnings.filterwarnings('ignore')


def fetchStockData(symbol):
    nse = NseIndia.NSE()
    start_day = date.today()
    end_day = start_day - relativedelta(months=3)
    start_day = start_day.strftime("%d-%m-%Y")
    end_day = end_day.strftime("%d-%m-%Y")

    data = capital_market.price_volume_and_deliverable_position_data(
        symbol, end_day, start_day)
    df = pd.DataFrame(data)
    vol_list = df['TotalTradedQuantity'].tail(10)
    vol_list = vol_list.str.replace(',', '').astype(float)

    newd1 = nse.equity_info(symbol)
    newd2 = nse.equity_extra_info(symbol)

    ltp = newd1['priceInfo']['lastPrice']
    high = newd1['priceInfo']['intraDayHighLow']['max']
    low = newd1['priceInfo']['intraDayHighLow']['min']
    prevclose = newd1['priceInfo']['previousClose']
    change = round(ltp - prevclose, 2)  # Rounded to 2 decimal places
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
        "Change": float(change),  # Ensure Change is float
        "Last 10D avg Volume": avgvol,
        "Delivery %": delp,
        "Symbol P/E": sympe,
        "3 months high": _3monHigh,
        "3 months low": _3monLow
    }
    print(f"Done with {symbol}.")
    return stockData


def save_stock_data(symbol_list):
    for symbol in symbol_list:
        data = fetchStockData(symbol)
        df = pd.DataFrame([data])
        df.to_csv('stock_data.csv',
                  mode='a',
                  header=not os.path.exists('stock_data.csv'),
                  index=False)

        print(f"Data for {symbol} saved to stock_data.csv")


def save_timestamp():
    with open('timestamp.txt', 'w') as f:
        f.write(datetime.now().isoformat())


def load_timestamp():
    if os.path.exists('timestamp.txt'):
        with open('timestamp.txt', 'r') as f:
            return datetime.fromisoformat(f.read().strip())
    return None


def sort_by_pe():
    df = pd.read_csv('stock_data.csv')
    df_sorted = df.sort_values(by='Symbol P/E')
    df_sorted.to_csv('stock_data.csv', index=False)
    print("Sorted by P/E.")


def main():
    with open('symbol_list.txt', 'r') as f:
        symbol_list = [line.strip() for line in f.readlines()]

    last_timestamp = load_timestamp()
    if last_timestamp and datetime.now() - last_timestamp < timedelta(
            hours=12):
        print("Less than 12 hours since last run.")
        if os.path.exists('stock_data.csv'):
            try:
                df = pd.read_csv('stock_data.csv')
                if not df.empty:
                    last_symbol = df['Symbol'].iloc[-1]
                    if symbol_list[-1] in df['Symbol'].values:
                        print("Values have been updated in last 12 hours.")
                        symbol_list = []
                    elif last_symbol in symbol_list:
                        symbol_list = symbol_list[symbol_list.index(last_symbol
                                                                    ) + 1:]
            except pd.errors.EmptyDataError:

                pd.DataFrame(columns=[
                    "Symbol", "LTP", "High", "Low", "PreviousClose", "Change",
                    "Last 10D avg Volume", "Delivery %", "Symbol P/E",
                    "3 months high", "3 months low"
                ]).to_csv('stock_data.csv', index=False)

    else:
        print("More than 12 hours since last run, clearing stock_data.csv.")
        if os.path.exists('stock_data.csv'):
            os.remove('stock_data.csv')
        save_timestamp()

    save_stock_data(symbol_list)


def remove_duplicates(csv_file_path):
    df = pd.read_csv(csv_file_path)
    df_cleaned = df.drop_duplicates(subset='Symbol', keep='first')
    df_cleaned.to_csv(csv_file_path, index=False)
    print(f"Duplicates removed and cleaned data saved to {csv_file_path}")


if __name__ == "__main__":
    main()
    with open('symbol_list.txt', 'r') as f:
        sym_list = [line.strip() for line in f.readlines()]
    if os.path.exists('stock_data.csv'):
        try:
            df = pd.read_csv('stock_data.csv')
            if set(sym_list) == set(df['Symbol']):
                sort_by_pe()
        except pd.errors.EmptyDataError:
            pass

    df = pd.read_csv('stock_data.csv')
    duplicates = df[df.duplicated(subset=['Symbol'], keep=False)]
    if not duplicates.empty:
        remove_duplicates('stock_data.csv')
    else:
        print("No duplicates found.")

    print("Execution Complete.")
