import requests
import json
import pandas as pd
import datetime
from datetime import datetime
from datetime import date, timedelta
import pprint
import os
import sys
import time

ticker='delisted1'
api_key='<key>'
interval = '1day'
dates= '2022-01-01'
#api_url= f'https://api.twelvedata.com/stocks?type=stock&exchange=NYSE&apikey={api_key}&include_delisted=true'
#api_url2= f'https://api.twelvedata.com/stocks?type=stock&exchange=NYSE&apikey={api_key}&include_delisted=false'
#api_url=f'https://api.polygon.io/v3/reference/tickers?type=CS&market=stocks&active=true&sort=ticker&order=asc&limit=1000&apiKey=Kh6pQ50hBPUmltmd9KhQgsKaqjVmPvMk'
#api_url=f'https://api.polygon.io/v3/reference/tickers?cursor=YWN0aXZlPXRydWUmZGF0ZT0yMDIyLTExLTA0JmxpbWl0PTEwMDAmbWFya2V0PXN0b2NrcyZvcmRlcj1hc2MmcGFnZV9tYXJrZXI9TEJSVCU3Q2I0NjY1OWY1ZDZkMDVlYjgwMmJjNmUwZjE1YzNhOGVhMjk4YzY3MGM0ZjRjOTU3MGVkNDM3OGJjNzY1ZjM1NTQmc29ydD10aWNrZXImdHlwZT1DUw&apiKey=Kh6pQ50hBPUmltmd9KhQgsKaqjVmPvMk'

def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)

print("Change date from the program before progressing")
start_date = date(2022, 1, 1)
end_date = date(2022, 1, 3)
for single_date in daterange(start_date, end_date):
 print(single_date.strftime("%Y-%m-%d"))
 dates=single_date.strftime("%Y-%m-%d")
 print("           ")
 api_url=f'https://api.polygon.io/v3/reference/tickers?type=CS&date={dates}&active=false&sort=ticker&order=asc&limit=1000&apiKey={api_key}'
 data=requests.get(api_url)
 f = open("/home/akshay/Downloads/alpaca/"+dates+"_1.json", "w")
 f.write(data.text)
 f.close()
 print("FOR DATE  "+dates)
 print("     ")
 i=2
 next_api_url=api_url
 while i < 10:
  try:
    data2=requests.get(next_api_url).json()
    next=data2["next_url"]
    print(next)
    next_api_url=f'{next}&apiKey={api_key}'
    data2=requests.get(next_api_url)
    num=str(i)
    f = open("/home/akshay/Downloads/alpaca/"+dates+"/"+dates+"_"+num+".json", "w")
    f.write(data2.text)
    f.close()
    print(i)
    i=i+1
  except:
    i=i+1
 print("    ")
 print(dates)
 print("Done")
#for x in data["results"][0:1000]:
# try:
#    ticker=x["ticker"]
#    api_url=f'https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/minute/2022-05-01/2022-11-01?adjusted=true&sort=asc&limit=50000&apiKey=<key>'
#    data3=requests.get(api_url)
    #data = json.loads(data3)
    #print(data3)
    #need to iterate through data3 and make it csv format wit right column names
#    jdata = json.loads(data3.text)
#    df = pd.DataFrame(jdata["results"])
    
    #df[["day", "month", "year"]] = df["t"].str.split("-", expand = True)
    #df['t'] = pd.to_datetime(df['t'], unit='ms')
#    df['t']=pd.to_datetime(df['t'], unit='ms')\
#                 .dt.tz_localize('UTC' )\
#                 .dt.tz_convert('America/New_York')
    #print(df['t'])
#    df['year'] = pd.DatetimeIndex(df['t']).year
#    df['month'] = pd.DatetimeIndex(df['t']).month
#    df['day'] = pd.DatetimeIndex(df['t']).day
#    df['hour'] = pd.DatetimeIndex(df['t']).hour
#    df['minutes'] = pd.DatetimeIndex(df['t']).minute
    #df['date'] = pd.to_datetime(df['t']).dt.date
    #df['Time'] = pd.to_datetime(df['t']).dt.time
#    df.columns.values[0] = "volume"
#    df.columns.values[1] = "volume_weighted"
#    df.columns.values[2] = "open"
#    df.columns.values[3] = "close"
#    df.columns.values[4] = "high"
#    df.columns.values[5] = "low"
#    df.columns.values[6] = "date"
#    df.columns.values[7] = "no_of_trades"
#    del(df['date'])
#    df=df.iloc[:,[7,8,9,10,11,2,4,5,3,0,1,6]]
    #print(df)
#    df.to_csv("/home/akshay/Downloads/alpaca/"+ticker+"_MAY_NOV_2022.csv",index=False, sep=',')
    
    #print(df)
    #f = open("/home/akshay/Downloads/alpaca/"+ticker, "w")
    #f.write(df)
    #f.close()
    
    #print(pd.json_normalize(data3, sep=';'))
    #df = pd.read_json("/home/akshay/Downloads/alpaca/"+ticker)
    #data4=df.to_csv("/home/akshay/Downloads/alpaca/"+ticker+".csv", sep=';')
    #df2 = pd.DataFrame(data4)
    #print(df2)
#    print(ticker+"..........DONE")
# except:
#    print("error at "+ticker)
#    continue

