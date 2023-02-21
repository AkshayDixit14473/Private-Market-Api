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
import glob

ticker='delisted1'
api_key='NFpgJgDtUOuxQb4D3liadFojYzwZpoyJ'
interval = '1day'
list=[]
 
######################################### GETTING LATEST DATE ##############################################################

#api_url= f'https://api.twelvedata.com/stocks?type=stock&exchange=NYSE&apikey={api_key}&include_delisted=true'
#api_url2= f'https://api.twelvedata.com/stocks?type=stock&exchange=NYSE&apikey={api_key}&include_delisted=false'
#api_url=f'https://api.polygon.io/v3/reference/tickers?type=CS&market=stocks&active=true&sort=ticker&order=asc&limit=1000&apiKey=Kh6pQ50hBPUmltmd9KhQgsKaqjVmPvMk'
api_url=f'https://api.polygon.io/v3/reference/tickers?type=CS&market=stocks&active=true&order=asc&limit=1000&sort=ticker&apiKey=NFpgJgDtUOuxQb4D3liadFojYzwZpoyJ'
#api_url=f'https://api.polygon.io/v3/reference/tickers?cursor=YWN0aXZlPXRydWUmZGF0ZT0yMDIyLTExLTA0JmxpbWl0PTEwMDAmbWFya2V0PXN0b2NrcyZvcmRlcj1hc2MmcGFnZV9tYXJrZXI9TEJSVCU3Q2I0NjY1OWY1ZDZkMDVlYjgwMmJjNmUwZjE1YzNhOGVhMjk4YzY3MGM0ZjRjOTU3MGVkNDM3OGJjNzY1ZjM1NTQmc29ydD10aWNrZXImdHlwZT1DUw&apiKey=Kh6pQ50hBPUmltmd9KhQgsKaqjVmPvMk'

data=requests.get(api_url)
 
jdata = json.loads(data.text)
df1 = pd.DataFrame(jdata["results"])
date=df1["last_updated_utc"][1]
date=date[0:10]
date='2020-11-06'
print(date)
########################################## CHECKING FOR TRADE #################################################################
try:
 print("Checking trades on the date")
 api_url=f'https://api.polygon.io/v2/aggs/ticker/AAPL/range/1/minute/{date}/{date}?adjusted=true&sort=asc&limit=50000&apiKey=NFpgJgDtUOuxQb4D3liadFojYzwZpoyJ'
 data=requests.get(api_url)
 
 jdata = json.loads(data.text)
 df1 = pd.DataFrame(jdata["results"])
 print("No problem")
except:
 print("No trade on this date")
 exit()

##########################################  DOWNLOADING TICKERS ##############################################################
parent_dir="/home/akshay/Downloads/polygon/Latest_minute_level_data"
directory=date
path = os.path.join(parent_dir, directory)
os.mkdir(path)
print("Directory '%s' created" %directory)

parent_dir="/home/akshay/Downloads/polygon/Latest_minute_level_data/"+date
directory="Stocks"
path = os.path.join(parent_dir, directory)
os.mkdir(path)
print("Directory '%s' created" %directory)

f = open("/home/akshay/Downloads/polygon/Latest_minute_level_data/"+date+"/ticker_1.json", "w")
f.write(data.text)
f.close()

jdata = json.loads(data.text)
df1 = pd.DataFrame(jdata["results"])
df1.to_csv("/home/akshay/Downloads/polygon/Latest_minute_level_data/"+date+"/ticker_1.csv",index=False, sep=',')
print(df1)

i=2
next_api_url=api_url
while i < 20:
  try:
    data=requests.get(next_api_url).json()
    next=data["next_url"]
    print(next)
    print(i)
    next_api_url=f'{next}&apiKey={api_key}'
    data=requests.get(next_api_url)
    print(data)
    num=str(i)
    f = open("/home/akshay/Downloads/polygon/Latest_minute_level_data/"+date+"/ticker_"+num+".json", "w")
    f.write(data.text)
    f.close()
    jdata = json.loads(data.text)
    df1 = pd.DataFrame(jdata["results"])
    df1.to_csv("/home/akshay/Downloads/polygon/Latest_minute_level_data/"+date+"/ticker_"+num+".csv",index=False, sep=',')
    print(df1)
    i=i+1
  except:
    print("now in except")
    print(i)
    i=i+1

print("    ")
print(date+ " JSON Done")
print("   ")
 
print("Now Concatinating")
files = os.path.join("/home/akshay/Downloads/polygon/Latest_minute_level_data/"+date+"/ticker_*csv")
 
# list of merged files returned
files = glob.glob(files)
 
print("Resultant CSV after joining all CSV files at a particular location...");
 
# joining files with concat and read_csv
df = pd.concat(map(pd.read_csv, files), ignore_index=True)
#df = pd.concat(map(pd.read_csv, ["/home/akshay/Downloads/polygon/"+dates+"_1.csv","/home/akshay/Downloads/polygon/"+dates+"_2.csv","/home/akshay/Downloads/polygon/"+dates+"_3.csv","/home/akshay/Downloads/polygon/"+dates+"_4.csv","/home/akshay/Downloads/polygon/"+dates+"_5.csv","/home/akshay/Downloads/polygon/"+dates+"_6.csv","/home/akshay/Downloads/polygon/"+dates+"_7.csv","/home/akshay/Downloads/polygon/"+dates+"_8.csv","/home/akshay/Downloads/polygon/"+dates+"_9.csv","/home/akshay/Downloads/polygon/"+dates+"_10.csv","/home/akshay/Downloads/polygon/"+dates+"_11.csv"]), ignore_index=True)
print(df)
  
# convert dataframe to csv file
df.to_csv("/home/akshay/Downloads/polygon/Latest_minute_level_data/"+date+"/Megaticker.csv",index=False, sep=',')
print(date+"CSV Done")

##########################################  DOWNLOADING STOCKS  #############################################################

df = pd.read_csv("/home/akshay/Downloads/polygon/Latest_minute_level_data/"+date+"/Megaticker.csv")
column = df.iloc[:, 0]
print(column)
for x in column:
 try:
    print(x)
    ticker=x
    api_url=f'https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/minute/{date}/{date}?adjusted=true&sort=asc&limit=50000&apiKey=NFpgJgDtUOuxQb4D3liadFojYzwZpoyJ'
    #api_url=f'https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/2022-07-01/2022-11-01?adjusted=true&sort=asc&limit=50000&apiKey=Kh6pQ50hBPUmltmd9KhQgsKaqjVmPvMk'
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
    df.to_csv("/home/akshay/Downloads/polygon/Latest_minute_level_data/"+date+"/Stocks/"+ticker+".csv",index=False, sep=',')
    
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
    list.append(ticker)
    continue

print(list)
with open("/home/akshay/Downloads/errors", 'w') as f:
  f.write(list)
f.close()










#for x in data["results"][0:1000]:
# try:
#    ticker=x["ticker"]
#    api_url=f'https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/minute/2022-05-01/2022-11-01?adjusted=true&sort=asc&limit=50000&apiKey=Kh6pQ50hBPUmltmd9KhQgsKaqjVmPvMk'
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
#    df.to_csv("/home/akshay/Downloads/polygon/"+ticker+"_MAY_NOV_2022.csv",index=False, sep=',')
    
    #print(df)
    #f = open("/home/akshay/Downloads/polygon/"+ticker, "w")
    #f.write(df)
    #f.close()
    
    #print(pd.json_normalize(data3, sep=';'))
    #df = pd.read_json("/home/akshay/Downloads/polygon/"+ticker)
    #data4=df.to_csv("/home/akshay/Downloads/polygon/"+ticker+".csv", sep=';')
    #df2 = pd.DataFrame(data4)
    #print(df2)
#    print(ticker+"..........DONE")
# except:
#    print("error at "+ticker)
#    continue

