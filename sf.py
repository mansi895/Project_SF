import snowflake.connector
import requests 
import json
import pandas as pd

request_api=requests.get('https://api.senticrypt.com/v1/history/index.json')
data=request_api.text
data=json.loads(data)


data_captured=[]
df=pd.DataFrame(columns=["p1"])
for data1 in data_bitcoin:
    if data1 not in data_captured:
        data_captured.append(data1)
        api_data=requests.get(f'https://api.senticrypt.com/v1/history/{data1}')
        df1=pd.DataFrame(columns=['p1'])
        df1["data"]=json.loads(api_data.text)
        df=pd.concat([df,df1])
    else:
        print('the bitcoin data already exist')
        continue
'''lets make a connection to snowflake'''
#global connect1
#connect1=snowflake.connector.connect(
       # user='Mansi',
        #password='Mansi@1995',
        #account='dv00231.ca-central-1.aws',
        #database='LAGOZON_TRAINING',
        #schema='SENT_BIT',
        #warehouse='COMPUTE_WH'
            #)
       
from snowflake.connector.pandas_tools import write_pandas
success,nchunks,nrows ,_ =write_pandas(connect1,df,'S1')

my_cur=connect1.cursor()
my_cur.execute('select * from S1')
print(my_cur.fetchall())
