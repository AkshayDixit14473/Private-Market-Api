import requests
import json
import pandas as pd
import datetime
from datetime import datetime
import pprint
import os
import sys
import time
import numpy as np
import csv
ticker='Megacsv.csv'
api_key='NFpgJgDtUOuxQb4D3liadFojYzwZpoyJ'
interval = '1day'
#api_url= f'https://api.twelvedata.com/stocks?type=stock&exchange=NYSE&apikey={api_key}&include_delisted=true'
#api_url2= f'https://api.twelvedata.com/stocks?type=stock&exchange=NYSE&apikey={api_key}&include_delisted=false'
#api_url=f'https://api.polygon.io/v3/reference/tickers?type=CS&market=stocks&active=true&sort=ticker&order=asc&limit=1000&apiKey=Kh6pQ50hBPUmltmd9KhQgsKaqjVmPvMk'
#api_url=f'https://api.polygon.io/v3/reference/tickers?cursor=YWN0aXZlPXRydWUmZGF0ZT0yMDIyLTExLTA0JmxpbWl0PTEwMDAmbWFya2V0PXN0b2NrcyZvcmRlcj1hc2MmcGFnZV9tYXJrZXI9TEJSVCU3Q2I0NjY1OWY1ZDZkMDVlYjgwMmJjNmUwZjE1YzNhOGVhMjk4YzY3MGM0ZjRjOTU3MGVkNDM3OGJjNzY1ZjM1NTQmc29ydD10aWNrZXImdHlwZT1DUw&apiKey=Kh6pQ50hBPUmltmd9KhQgsKaqjVmPvMk'
#data=requests.get(api_url).json()
#data2=requests.get(api_url2).json()
#f = open("/home/akshay/Downloads/alpaca/"+ticker, "w")
#f.write(data.text)
#f.close()


df = pd.read_csv("/home/akshay/Downloads/alpaca/"+ticker)
column = df.iloc[:, 0]
print(column)
with open("/home/akshay/Downloads/polygon/Industry_tickers.csv", 'w') as f:
 nestcolumns11=['Name', 'Industry']
 wr=csv.DictWriter(f, fieldnames=nestcolumns11)
 wr.writeheader()
 for x in column:
  try:
   print(x)
   ticker=x
   #api_url=f'https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/minute/2018-01-01/2018-07-01?adjusted=true&sort=asc&limit=50000&apiKey=Kh6pQ50hBPUmltmd9KhQgsKaqjVmPvMk'
   #api_url=f'https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/2018-01-01/2022-11-01?adjusted=true&sort=asc&limit=50000&apiKey=Kh6pQ50hBPUmltmd9KhQgsKaqjVmPvMk'
   api_url=f'https://api.polygon.io/v3/reference/tickers/{ticker}?apiKey=NFpgJgDtUOuxQb4D3liadFojYzwZpoyJ'
   data3=requests.get(api_url)
   #data = json.loads(data3)
   #print(data3)
   #need to iterate through data3 and make it csv format wit right column names
   jdata = json.loads(data3.text)
   df = pd.DataFrame(jdata["results"])
   #print(jdata["results"].keys())
    
   
    #f.write('ticker--------------\n')
    #f.write(jdata["results"]["ticker"]+'\n')
    #f.write('name--------------\n')
    #f.write(jdata["results"]["name"]+'\n')
    #f.write('market--------------\n')
    #f.write(jdata["results"]["market"]+'\n')
    #f.write('locale--------------\n')
    #f.write(jdata["results"]["locale"]+'\n')
    #f.write('primary_exchange--------------\n')
    #f.write(jdata["results"]["primary_exchange"]+'\n')
    #f.write('type--------------\n')
    #f.write(jdata["results"]["type"]+'\n')
    #f.write('currency_name--------------\n')
    #f.write(jdata["results"]["active"])
    #f.write(jdata["results"]["currency_name"]+'\n')
    #f.write('cik--------------\n')
    #f.write(jdata["results"]["cik"]+'\n')
    #f.write('composite_figi--------------\n')
    #f.write(jdata["results"]["composite_figi"]+'\n')
    #f.write('share_class_figi--------------\n')
    #f.write(jdata["results"]["share_class_figi"]+'\n')
    #f.write('phone_number--------------\n')
    #f.write(jdata["results"]["market_cap"])
   # f.write(jdata["results"]["phone_number"]+'\n')
   # f.write('description--------------\n')
    #f.write(jdata["results"]["address"])
    #f.write(jdata['results']['description']+'\n')
   # f.write('sic_code--------------\n')
   # f.write(jdata["results"]["sic_code"]+'\n')
   # f.write('sic_description--------------\n')
   # f.write(jdata["results"]["sic_description"]+'\n')
   # f.write('ticker_root--------------\n')
   # f.write(jdata["results"]["ticker_root"]+'\n')
   # f.write('homepage_url--------------\n')
   # f.write(jdata["results"]["homepage_url"]+'\n')
   # f.write('list_date--------------\n')
    #f.write(jdata["results"]["total_employees"])
   # f.write(jdata["results"]["list_date"]+'\n')
   # f.write('\n')
    #f.write(jdata["results"]["branding"])
    #f.write(jdata["results"]["share_class_shares_outstanding"])
    #f.write(jdata["results"]["weighted_shares_outstanding"])
    #f.write(jdata["results"]["round_lot"])
     
    #print(type(jdata["results"][key]))
    #nestcolumns11=['Name', 'Industry']
    #wr=csv.DictWriter(f, fieldnames=nestcolumns11)
    #wr.writeheader()
   wr.writerow({'Name' : jdata["results"]["ticker"],'Industry': jdata["results"]["sic_description"]})
    #f.write('jdata["results"]["name"]':'jdata["results"]["sic_description"]')
    #wr.writerow(jdata["results"]["name"])
    #wr.writerow(jdata["results"]["sic_description"])
    #df[["day", "month", "year"]] = df["t"].str.split("-", expand = True)
    #df['t'] = pd.to_datetime(df['t'], unit='ms')
    #df['t']=pd.to_datetime(df['t'], unit='ms')\
    #             .dt.tz_localize('UTC' )\
    #             .dt.tz_convert('America/New_York')
    #df['year'] = pd.DatetimeIndex(df['t']).year
    #df['month'] = pd.DatetimeIndex(df['t']).month
    #df['day'] = pd.DatetimeIndex(df['t']).day
    #df['hour'] = pd.DatetimeIndex(df['t']).hour
    #df['minutes'] = pd.DatetimeIndex(df['t']).minute
    #df['date'] = pd.to_datetime(df['t']).dt.date
    #df['Time'] = pd.to_datetime(df['t']).dt.time
    #df.columns.values[0] = "volume"
    #df.columns.values[1] = "volume_weighted"
    #df.columns.values[2] = "open"
    #df.columns.values[3] = "close"
    #df.columns.values[4] = "high"
    #df.columns.values[5] = "low"
    #df.columns.values[6] = "date"
    #df.columns.values[7] = "no_of_trades"
    #del(df['t'])
    #df=df.iloc[:,[7,8,9,10,11,2,4,5,3,0,1,6]]
    #df=df.iloc[:,[7,8,9,1,3,4,2,0,1,6]]
    #df.to_csv("/home/akshay/Downloads/polygon/"+ticker+".csv",index=False, sep=',')
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

f.close()
