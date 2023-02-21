import requests
import json
import pandas as pd
import datetime
from datetime import datetime
ticker='Megacsv.csv'
api_key='<key>'
interval = '1day'
#api_url= f'https://api.twelvedata.com/stocks?type=stock&exchange=NYSE&apikey={api_key}&include_delisted=true'
#api_url2= f'https://api.twelvedata.com/stocks?type=stock&exchange=NYSE&apikey={api_key}&include_delisted=false'
#data=requests.get(api_url).json()
#data2=requests.get(api_url2).json()
#f = open("/home/akshay/Downloads/alpaca/"+ticker, "w")
#f.write(data.text)
#f.close()
import pprint
import os
import sys
import time
print("enter start date in format yyyy-mm-dd")
sdate=input()
print("enter end date in format yyyy-mm-dd")
edate=input()
print("suffix to the files")
suffix=input()
df = pd.read_csv("/home/akshay/Downloads/alpaca/"+ticker)
column = df.iloc[:, 0]
print(column)
for x in column:
 try:
    print(x)
    ticker=x
    api_url=f'https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/minute/{sdate}/{edate}?adjusted=true&sort=asc&limit=50000&apiKey=<key>'
    #api_url=f'https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/2022-07-01/2022-11-01?adjusted=true&sort=asc&limit=50000&apiKey=<key>'
    data3=requests.get(api_url)
    #data = json.loads(data3)
    #print(data3)
    #need to iterate through data3 and make it csv format wit right column names
    jdata = json.loads(data3.text)
    df = pd.DataFrame(jdata["results"])
    
    #df[["day", "month", "year"]] = df["t"].str.split("-", expand = True)
    #df['t'] = pd.to_datetime(df['t'], unit='ms')
    df['t']=pd.to_datetime(df['t'], unit='ms')\
                 .dt.tz_localize('UTC' )\
                 .dt.tz_convert('America/New_York')
    df['year'] = pd.DatetimeIndex(df['t']).year
    df['month'] = pd.DatetimeIndex(df['t']).month
    df['day'] = pd.DatetimeIndex(df['t']).day
    df['hour'] = pd.DatetimeIndex(df['t']).hour
    df['minutes'] = pd.DatetimeIndex(df['t']).minute
    df['date'] = pd.to_datetime(df['t']).dt.date
    df['Time'] = pd.to_datetime(df['t']).dt.time
    df.columns.values[0] = "volume"
    df.columns.values[1] = "volume_weighted"
    df.columns.values[2] = "open"
    df.columns.values[3] = "close"
    df.columns.values[4] = "high"
    df.columns.values[5] = "low"
    df.columns.values[6] = "date"
    df.columns.values[7] = "no_of_trades"
    del(df['date'])
    df=df.iloc[:,[7,8,9,10,11,2,4,5,3,0,1,6]]
    df.to_csv("/home/akshay/Downloads/polygon/"+ticker+"_"+suffix+".csv",index=False, sep=',')
    
    #print(df)
    #f = open("/home/akshay/Downloads/alpaca/"+ticker, "w")
    #f.write(df)
    #f.close()
    
    #print(pd.json_normalize(data3, sep=';'))
    #df = pd.read_json("/home/akshay/Downloads/alpaca/"+ticker)
    #data4=df.to_csv("/home/akshay/Downloads/alpaca/"+ticker+".csv", sep=';')
    #df2 = pd.DataFrame(data4)
    #print(df2)
    print(ticker+"..........DONE")
 except:
    print("error at "+ticker)
    continue

