import requests
import json
import pandas as pd
import datetime
from datetime import datetime
import numpy as np
import csv
filename='Megacsv.csv'
api_key='<key>
interval = '1day'
#api_url= f'https://api.twelvedata.com/stocks?type=stock&exchange=NYSE&apikey={api_key}&include_delisted=true'
#api_url2= f'https://api.twelvedata.com/stocks?type=stock&exchange=NYSE&apikey={api_key}&include_delisted=false'
#data=requests.get(api_url).json()
#data2=requests.get(api_url2).json()
#f = open("/home/akshay/Downloads/alpaca/"+ticker, "w")
#f.write(data.text)
#f.close()
from dateutil import rrule
from datetime import datetime, timedelta
start_date = datetime(2020, 11, 1)
end_date = datetime(2022, 11, 1)
for dt in rrule.rrule(rrule.MONTHLY, dtstart=start_date, until=end_date):
 date=str(dt)
 date=date[0:10]
 print(date)
 df = pd.read_csv("/home/akshay/Downloads/alpaca/"+filename)
 column = df.iloc[:, 0]
 print(column)
 for x in column:
  try:
     print(x)
     ticker=x
     api_url=f'https://api.polygon.io/vX/reference/financials?ticker={x}&timeframe=quarterly&filing_date.gte={date}&period_of_report_date.gt={date}&apiKey=<key>
     data3=requests.get(api_url)
     #data = json.loads(data3)
     #print(data3)
     #need to iterate through data3 and make it csv format wit right column names
     jdata = json.loads(data3.text)
     df = pd.DataFrame(jdata["results"][0]["financials"])
     
     with open("/home/akshay/Downloads/alpaca/new/"+ticker+"_"+date+"_quarterly.csv", 'w') as f:
      #wr = csv.DictWriter(f, fieldnames=csv_columns1)
      #wr.writeheader()
      #wr.writerow(jdata["results"][0]["financials"]["balance_sheet"])
      #wr.writerow(jdata["results"][0]["financials"]["balance_sheet"]["fixed_assets"])
      #print(jdata["results"][0]["financials"]["balance_sheet"].keys())
      
      f.write('\nBalance_Sheet\n\n')
      #f.write('\nliabilities_and_equity\n')
      nestcolumns11=['unit', 'value', 'label', 'order']
      wr = csv.DictWriter(f, fieldnames=nestcolumns11)
      wr.writeheader()
      for key in jdata["results"][0]["financials"]["balance_sheet"]:
       #print(key)
       wr.writerow(jdata["results"][0]["financials"]["balance_sheet"][key])
      
      f.write('\ncash_flow_statement\n\n')
      wr = csv.DictWriter(f, fieldnames=nestcolumns11)
      wr.writeheader()
      for key in jdata["results"][0]["financials"]["cash_flow_statement"]:
       #print(key)
       wr.writerow(jdata["results"][0]["financials"]["cash_flow_statement"][key])
      
      f.write('\ncomprehensive_income\n\n')
      wr = csv.DictWriter(f, fieldnames=nestcolumns11)
      wr.writeheader()
      for key in jdata["results"][0]["financials"]["comprehensive_income"]:
       #print(key)
       wr.writerow(jdata["results"][0]["financials"]["comprehensive_income"][key])
      
      f.write('\nincome_statement\n\n')
      wr = csv.DictWriter(f, fieldnames=nestcolumns11)
      wr.writeheader()
      for key in jdata["results"][0]["financials"]["income_statement"]:
       #print(key)
       wr.writerow(jdata["results"][0]["financials"]["income_statement"][key])
       
     #print(df)
     #df[["day", "month", "year"]] = df["t"].str.split("-", expand = True)
     #df['t'] = pd.to_datetime(df['t'], unit='ms')
     #df['t']=pd.to_datetime(df['t'], unit='ms')\
     #             .dt.tz_localize('UTC' )\
     #             .dt.tz_convert('America/New_York')
     #print(df['t'])
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
     #del(df['date'])
     #df=df.iloc[:,[7,8,9,10,11,2,4,5,3,0,1,6]]
     #print(df)
     #print(df)
     #df.to_csv("/home/akshay/Downloads/alpaca/new/"+ticker+"_"+date+"_quarterly.csv",index=False, sep=',')
     #np.savetxt("/home/akshay/Downloads/alpaca/new/"+ticker+"_"+date+"_quarter.csv",jdata["results"],delimiter ="}",fmt ='% s')
     f.close()
     print("quaterly done")
     
     api_url=f'https://api.polygon.io/vX/reference/financials?ticker={x}&timeframe=annual&filing_date.gte={date}&period_of_report_date.gt={date}&apiKey=<key>
     data3=requests.get(api_url)
     jdata = json.loads(data3.text)
     #df = pd.DataFrame(jdata["results"])
     #print(type(jdata))
     #print(type(jdata["results"]))
     #print(type(jdata["results"][0]["financials"]["balance_sheet"]))
     df = pd.DataFrame(jdata["results"][0]["financials"])
     
     with open("/home/akshay/Downloads/alpaca/new/"+ticker+"_"+date+"_annual.csv", 'w') as f:
      #wr = csv.DictWriter(f, fieldnames=csv_columns1)
      #wr.writeheader()
      #wr.writerow(jdata["results"][0]["financials"]["balance_sheet"])
      #wr.writerow(jdata["results"][0]["financials"]["balance_sheet"]["fixed_assets"])
      #print(jdata["results"][0]["financials"]["balance_sheet"].keys())
      
      f.write('\nBalance_Sheet\n\n')
      #f.write('\nliabilities_and_equity\n')
      nestcolumns11=['unit', 'value', 'label', 'order']
      wr = csv.DictWriter(f, fieldnames=nestcolumns11)
      wr.writeheader()
      for key in jdata["results"][0]["financials"]["balance_sheet"]:
       #print(key)
       wr.writerow(jdata["results"][0]["financials"]["balance_sheet"][key])
      
      f.write('\ncash_flow_statement\n\n')
      wr = csv.DictWriter(f, fieldnames=nestcolumns11)
      wr.writeheader()
      for key in jdata["results"][0]["financials"]["cash_flow_statement"]:
       #print(key)
       wr.writerow(jdata["results"][0]["financials"]["cash_flow_statement"][key])
      
      f.write('\ncomprehensive_income\n\n')
      wr = csv.DictWriter(f, fieldnames=nestcolumns11)
      wr.writeheader()
      for key in jdata["results"][0]["financials"]["comprehensive_income"]:
       #print(key)
       wr.writerow(jdata["results"][0]["financials"]["comprehensive_income"][key])
      
      f.write('\nincome_statement\n\n')
      wr = csv.DictWriter(f, fieldnames=nestcolumns11)
      wr.writeheader()
      for key in jdata["results"][0]["financials"]["income_statement"]:
       #print(key)
       wr.writerow(jdata["results"][0]["financials"]["income_statement"][key])

      
      #wr = csv.DictWriter(f, fieldnames=nestcolumns11)
      #wr.writeheader()
      #wr.writerow(jdata["results"][0]["financials"]["balance_sheet"]["liabilities_and_equity"])
      #wr = csv.DictWriter(f, fieldnames=csv_columns2)
      #wr.writeheader()
      #wr.writerow(jdata["results"][0]["financials"]["cash_flow_statement"])
      #wr = csv.DictWriter(f, fieldnames=csv_columns3)
      #wr.writeheader()
      #wr.writerow(jdata["results"][0]["financials"]["comprehensive_income"])
      #wr = csv.DictWriter(f, fieldnames=csv_columns4)
      #wr.writeheader()
      #wr.writerow(jdata["results"][0]["financials"]["income_statement"])
     
     #df.to_csv("/home/akshay/Downloads/alpaca/new/"+ticker+"_"+date+"_annual.csv",index=False, sep=',')
     f.close()
     print("annually done")
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

