# Import Module
import os
import pandas as pd
import shutil
# Folder Path
path = "/home/akshay/Downloads/alpaca/Nasdaq_Stocks_Larger_Volume"
  
# Change the directory
os.chdir(path)
          
for file in os.listdir(path):
 if file.endswith(".csv"):
  file_path = f"{path}/{file}"
  file_path1 = f"/home/akshay/Downloads/alpaca/{file}"
  data = pd.read_csv(file_path, sep=';')
  #print(data)                                       Contains csv file with seperate rows due to seperator ;
  
  df = pd.DataFrame(data)
  #print(df)                                         Contains dataframe file
  #print(df["datetime"].unique())
  
  column = df.iloc[:, 0]                              #Contains Dataframe column 1 
  #print(column)
  #print(file_path)
  
  #with open(file_path, 'r') as file :                 #Finding and replacing , with
   #filedata = file.read()

  #filedata = filedata.replace(',', ';')
  #print(filedata)

  #with open(file_path, 'w') as file:
   #file.write(filedata)
   
  #bool=df.index.is_monotonic_increasing               #To check if dates are in sorted format
  #if bool==False:
   #print(file_path)

  #duplicate = df[df.duplicated]                       To check for duplicated rows
  #if not duplicate.empty:
   #print(file_path)
   #print(duplicate)
   
   #dc=df.drop_duplicates()
   #print(dc)
   #dc.to_csv(file_path,index=False)

  #row=column.loc[0]
  #print(row)
  
  #To check for duplicate dates code no 2
  duplicate = df[df.duplicated('datetime')]
  if not duplicate.empty:
   print(file_path)
   #print(duplicate)                                                            #Keep first duplicate code
   dc=df.sort_values('volume', ascending=False).drop_duplicates('datetime').sort_index()                       #Dropping duplicates with lowest volume
   dc=df.drop_duplicates(subset='datetime',keep='first')
   #print(dc)
   #dc.to_csv(file_path1,index=False, sep=';')
   print("      ")
  #else:
   #shutil.copy(file_path, file_path1)


