import requests
import pandas as pd
pd.set_option('display.max_rows', 1000)
pd.set_option('display.max_columns', 1000)
pd.set_option('display.width', 5000)


class NSE:
    pre_market_categories = ['NIFTY 50', 'Nifty Bank', 'Emerge', 'Securities in F&O', 'Others', 'All']
    equity_market_categories = ['NIFTY 50', 'NIFTY NEXT 50', 'NIFTY MIDCAP 50', 'NIFTY MIDCAP 100', 'NIFTY MIDCAP 150', 'NIFTY SMALLCAP 50',
                        'NIFTY SMALLCAP 100', 'NIFTY SMALLCAP 250', 'NIFTY MIDSMALLCAP 400', 'NIFTY 100', 'NIFTY 200', 'NIFTY AUTO',
                        'NIFTY BANK', 'NIFTY ENERGY', 'NIFTY FINANCIAL SERVICES', 'NIFTY FINANCIAL SERVICES 25/50', 'NIFTY FMCG',
                        'NIFTY IT', 'NIFTY MEDIA', 'NIFTY METAL', 'NIFTY PHARMA', 'NIFTY PSU BANK', 'NIFTY REALTY',
                        'NIFTY PRIVATE BANK', 'Securities in F&O', 'Permitted to Trade', 'NIFTY DIVIDEND OPPORTUNITIES 50',
                        'NIFTY50 VALUE 20', 'NIFTY100 QUALITY 30', 'NIFTY50 EQUAL WEIGHT', 'NIFTY100 EQUAL WEIGHT',
                        'NIFTY100 LOW VOLATILITY 30', 'NIFTY ALPHA 50', 'NIFTY200 QUALITY 30', 'NIFTY ALPHA LOW-VOLATILITY 30',
                        'NIFTY200 MOMENTUM 30', 'NIFTY COMMODITIES', 'NIFTY INDIA CONSUMPTION', 'NIFTY CPSE', 'NIFTY INFRASTRUCTURE',
                        'NIFTY MNC', 'NIFTY GROWTH SECTORS 15', 'NIFTY PSE', 'NIFTY SERVICES SECTOR', 'NIFTY100 LIQUID 15',
                        'NIFTY MIDCAP LIQUID 15']
    holidays_categories = ["Clearing", "Trading"]

    def __init__(self):
        self.headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36'}
        self.session = requests.Session()
        self.session.get("http://nseindia.com", headers=self.headers)

    def pre_market_data(self, category):
        pre_market_category = {"NIFTY 50": "NIFTY", "Nifty Bank": "BANKNIFTY", "Emerge": "SME", "Securities in F&O": "FO",
                          "Others": "OTHERS", "All": "ALL"}
        data = self.session.get(f"https://www.nseindia.com/api/market-data-pre-open?key={pre_market_category[category]}",
                                headers=self.headers).json()["data"]
        new_data = []
        for i in data:
            new_data.append(i["metadata"])
        df = pd.DataFrame(new_data)
        df = df.set_index("symbol", drop=True)
        return df

    def equity_market_data(self, category, symbol_list=False):
        category = category.upper().replace(' ', '%20').replace('&', '%26')
        data = self.session.get(f"https://www.nseindia.com/api/equity-stockIndices?index={category}", headers=self.headers).json()["data"]
        df = pd.DataFrame(data)
        df = df.drop(["meta"], axis=1)
        df = df.set_index("symbol", drop=True)
        if symbol_list:
            return list(df.index)
        else:
            return df

    def about_holidays(self, category):
        data = self.session.get(f'https://www.nseindia.com/api/holiday-master?type={category.lower()}', headers=self.headers).json()
        df = pd.DataFrame(list(data.values())[0])
        return df

    def equity_info(self, symbol, trade_info=False):
        symbol = symbol.replace(' ', '%20').replace('&', '%26')
        url = 'https://www.nseindia.com/api/quote-equity?symbol=' + symbol + ("&section=trade_info" if trade_info else "")
        data = self.session.get(url, headers=self.headers).json()
        return data
    def equity_extra_info(self, symbol):
        symbol = symbol.replace(' ','%20').replace('&','%26')
        url = 'https://www.nseindia.com/api/quote-equity?symbol=' + symbol + ("&series=EQ")
        data0 = self.session.get(url, headers=self.headers).json()
        return data0
    def historical_data(self, symbol):
        symbol = symbol.replace(' ','%20').replace('&','%26')
        url = 'https://www.nseindia.com/api/historical/cm/equity?symbol=' + symbol
        data1 = self.session.get(url, headers=self.headers).json()
        return data1
    def corptype(self,symbol,strt,end):

        url = "https://www.nseindia.com/api/corporate-disclosure-getquote?symbol=" + symbol +"&corpType=announcement&market=equities&from_date=" + strt + "&to_date="+ end
        data1 = self.session.get(url, headers=self.headers).json()
        return data1


    def future_data(self, symbol, indices=False):
        symbol = symbol.replace(' ', '%20').replace('&', '%26')
        url = 'https://www.nseindia.com/api/quote-derivative?symbol=' + symbol
        data = self.session.get(url, headers=self.headers).json()
        print(data)
        lst = []
        for i in data["stocks"]:
            if i["metadata"]["instrumentType"] == ("Index Futures" if indices else "Stock Futures"):
                lst.append(i["metadata"])
        df = pd.DataFrame(lst)
        df = df.set_index("identifier", drop=True)
        return df

    def option_data(self, symbol, indices=False):
        symbol = symbol.replace(' ', '%20').replace('&', '%26')
        if not indices:
            url = 'https://www.nseindia.com/api/option-chain-equities?symbol=' + symbol
        else:
            url = 'https://www.nseindia.com/api/option-chain-indices?symbol=' + symbol
        data = self.session.get(url,headers=self.headers).json()["records"]
        my_df = []
        for i in data["data"]:
            for k, v in i.items():
                if k == "CE" or k == "PE":
                    info = v
                    info["instrumentType"] = k
                    info["timestamp"] = data["timestamp"]
                    my_df.append(info)
        df = pd.DataFrame(my_df)
        df = df.set_index("identifier", drop=True)
        return df

    def bulkdeal(self):

        url = pd.read_csv("https://archives.nseindia.com/content/equities/bulk.csv")

        # url = pd.read_csv("https://www.nseindia.com/api/historical/bulk-deals?from=" + dte + "&to="+ dte + "&csv = true")
        # print(url)
        # https: // www.nseindia.com / api / historical / bulk - deals?from=06 - 06 - 2024 & to = 06 - 06 - 2024

        # url = "https://www.nseindia.com/api/corporate-disclosure-getquote?symbol=" + symbol +"&corpType=announcement&market=equities&from_date=" + strt + "&to_date="+ end
        # data1 = self.session.get(url, headers=self.headers).json()
        return url

# TECHM
# WIPRO
# BAJFINANCE
# GRASIM
# ULTRACEMCO
# TCS
# JSWSTEEL
# HINDUNILVR
# INFY
# HDFCBANK
# TATAMOTORS
# LTIM
# BHARTIARTL
# TITAN
# HDFCLIFE
# ITC
# SBILIFE
# ICICIBANK
# SHRIRAMFIN
# MARUTI
# HCLTECH
# NESTLEIND
# HEROMOTOCO
# COALINDIA
# ASIANPAINT
# TATASTEEL
# KOTAKBANK
# BAJAJ-AUTO
# ADANIENT
# BPCL
# M&M
# DIVISLAB
# TATACONSUM
# BRITANNIA
# CIPLA
# ADANIPORTS
# RELIANCE
# HINDALCO
# POWERGRID
# ONGC
# AXISBANK
# SUNPHARMA
# BAJAJFINSV
# INDUSINDBK
# LT
# DRREDDY
# SBIN
# APOLLOHOSP
# EICHERMOT
# NTPC




