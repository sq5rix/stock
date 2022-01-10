import traceback
import pandas as pd
import arrow
import json
from secrets import AV_API_KEY
import requests
from time import sleep

DATA_FILE = 'datafile.pickle'
SP500 = 'sp500.csv'

def get_last_date(df):
    return max(df.index)

def get_stock(ticker, all_data=False):
    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={ticker}&interval=15min&slice=year1month12&apikey={AV_API_KEY}'
    if all_data:
        url += '&outputsize=full'
    index = 'Time Series (15min)'
    r = requests.get(url)
    try:
        df = pd.DataFrame(r.json()[index]).T
        df['ticker']=ticker
        sleep(5)
        print(f"ticker: {ticker}, len: {len(df)}")
    except:
        print(traceback.format_exc(limit=3))
        print("err ticker: ", ticker)
        return pd.DataFrame()
    return df

def get_crypto(ticker, all_data=False):
    url = f'https://www.alphavantage.co/query?function=CRYPTO_INTRADAY&symbol={ticker}&market=USD&slice=year2month12&interval=15min&apikey={AV_API_KEY}'
    if all_data:
        url += '&outputsize=full'
    index = 'Time Series Crypto (15min)'
    try:
        r = requests.get(url)
        df = pd.DataFrame(r.json()[index]).T
        df['ticker']=ticker
        print(f"ticker: {ticker}, len: {len(df)}")
    except:
        print(traceback.format_exc(limit=3))
        print("err ticker: ", ticker)
        return pd.DataFrame()
    sleep(5)
    return df

def save_df(df):
    df.to_pickle(DATA_FILE)

def read_df():
    return pd.read_pickle(DATA_FILE)

def get_multi(stocks, cryptos):
    df = get_stock(stocks[0])
    for a in stocks[1:]:
        df = df.append(get_stock(a, True))
    for b in cryptos:
        df = df.append(get_crypto(b, True))
    return df

def add_multi(stocks, cryptos, df):
    for a in stocks:
        x = get_stock(a)
        df = df.append(x[x.index>get_last_date(df)])
    for b in cryptos:
        x = get_crypto(b)
        df = df.append(x[x.index>get_last_date(df)])
    return df

def get_all_stocks():
    sp500 = pd.read_csv(SP500)
    return sp500['Symbol']

def get_all_crypto():
    return ['BTC', 'ETH']

def start_calcs():
    df = get_multi(get_all_stocks(), get_all_crypto())
    save_df(df)
    return df

if __name__ == '__main__':
    #print('get_all_stocks: ', list(get_all_stocks()))
    df = start_calcs()
    save_df(df)
    #df = read_df()
    #print(df)
    print('get_last_date(df): ', get_last_date(df))

