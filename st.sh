#!/bin/bash

# created: GB June 2019

# Time-stamp: <2019-10-29 09:17:24 giulio>

#add your apikey
APIKEY=

#get S&P500 tickers
tickers=$( zcat constituents_sp500.csv.gz| gawk -F',' 'NR>1{print $1}' )

#download data and save in file
AVWEB="https://www.alphavantage.co/query?function=TIME_SERIES_DAILY_ADJUSTED&outputsize=full&apikey=${APIKEY}&datatype=csv"
for ticker in $tickers; do
    wget --output-document=${ticker}.csv "${AVWEB}&symbol=${ticker}"
    sleep 15s
done

