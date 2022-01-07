import pandas as pd
import arrow
import json
from secrets import AV_API_KEY
import requests

DATA_FILE = 'datafile.pickle'

def get_last_date(df):
    return max(df.index)

def get_stock(ticker, all_data=False):
    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={ticker}&interval=15min&slice=year1month12&apikey={AV_API_KEY}'
    if all_data:
        url += '&outputsize=full'
    index = 'Time Series (15min)'
    r = requests.get(url)
    df = pd.DataFrame(r.json()[index]).T
    df['ticker']=ticker
    return df

def get_crypto(ticker, all_data=False):
    url = f'https://www.alphavantage.co/query?function=CRYPTO_INTRADAY&symbol={ticker}&market=USD&slice=year1month12&interval=15min&apikey={AV_API_KEY}'
    if all_data:
        url += '&outputsize=full'
    index = 'Time Series Crypto (15min)'
    r = requests.get(url)
    df = pd.DataFrame(r.json()[index]).T
    df['ticker']=ticker
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
        x = x[x.index>get_last_date(df)]
        df = df.append(x)
    for b in cryptos:
        x = get_crypto(b)
        x = x[x.index>get_last_date(df)]
        df = df.append(x)
    return df

def get_all_stocks():
    return ['IBM', 'AAPL']

def get_all_crypto():
    return ['BTC', 'ETH']

def start_calcs():
    df = get_multi(get_all_stocks(), get_all_crypto())
    save_df(df)
    return df

if __name__ == '__main__':
    #df = start_calcs()
    df = read_df()
    print(df)
    print('get_last_date(df): ', get_last_date(df))

