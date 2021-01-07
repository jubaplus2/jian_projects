
# coding: utf-8

# In[ ]:

import pandas as pd
import numpy as np
import os
import datetime
import logging
import hashlib
import gc
import glob
logging.basicConfig(filename='crmnewscore.log', level=logging.INFO)
logging.info('Started')

samplerows = None
activemos = '2018-12-29'
lapsed = '2017-12-29'
lastdate = '2018-12-29'


folder_write = '/home/jian/Projects/Big_Lots/Live_Ramp/Quarterly_Update/crm_newscore_20190107/'
try:
    os.stat(folder_write)
except:
    os.mkdir(folder_write)
    
# Adding control members


# In[ ]:

chunksize_num = 10**7
filename='/home/jian/Projects/Big_Lots/Live_Ramp/Quarterly_Update/crm_newscore_0922/combinedtransactions_0922.csv'
dftrans_before_20180922=pd.DataFrame()
count_i=0

for chunk in pd.read_csv(filename, chunksize=chunksize_num,dtype=str):
    chunk['total_transaction_amt']=chunk['total_transaction_amt'].astype(float)
    chunk['total_transaction_units']=chunk['total_transaction_units'].astype(float)
    chunk = chunk[['customer_id_hashed','transaction_date','transaction_time',
                   'transaction_id','location_id','total_transaction_units',
                   'total_transaction_amt']].drop_duplicates()
    dftrans_before_20180922=dftrans_before_20180922.append(chunk)
    count_i+=1
    print(count_i,datetime.datetime.now())
    

'''
dftrans_before_20180922 = pd.read_csv('/home/jian/Projects/Big_Lots/Live_Ramp/Quarterly_Update/crm_newscore_0922/combinedtransactions_0922.csv',dtype=str)
#dftrans = dftrans[dftrans['transaction_date']>=lapsed]
dftrans_before_20180922['total_transaction_amt']=dftrans_before_20180922['total_transaction_amt'].astype(float)
dftrans_before_20180922['total_transaction_units']=dftrans_before_20180922['total_transaction_units'].astype(float)

dftrans_before_20180922 = dftrans_before_20180922[['customer_id_hashed','transaction_date','transaction_time',
                   'transaction_id','location_id','total_transaction_units',
                   'total_transaction_amt']].drop_duplicates()
'''

del chunk

dftrans_before_20180922=dftrans_before_20180922.drop_duplicates()

print("Deduped",datetime.datetime.now())


# In[ ]:

def recursive_file_gen(my_root_dir):
    for root, dirs, files in os.walk(my_root_dir):
        for file in files:
            yield os.path.join(root, file)

weeks_after_20180922=[datetime.date(2018,9,29)+datetime.timedelta(days=x*7) for x in range(999)]
possible_recent_folders=["/home/jian/BigLots/MediaStorm_"+str(x)+"/" for x in weeks_after_20180922]
recent_file_list=[]
for dirc in possible_recent_folders:
    list_recent=[x for x in list(recursive_file_gen(dirc)) if ("DailySales" in x) & (".txt" in x) ]
    recent_file_list=recent_file_list+list_recent
recent_file_df=pd.DataFrame({"path":recent_file_list,"date":[datetime.datetime.strptime(x.split("DailySales")[1][:8],"%Y%m%d").date()-datetime.timedelta(days=3) for x in recent_file_list]},index=[x for x in range(len(recent_file_list))])


list_1_after_201806=[x for x in list(recursive_file_gen("/home/jian/BigLots/2018_by_weeks/")) if ("DailySales" in x) & (".txt" in x) ]
folder_date=[datetime.datetime.strptime(x.split("/")[len(x.split("/"))-2].split("_")[1],"%Y-%m-%d").date() for x in list_1_after_201806]
df_1_after_201806=pd.DataFrame({"date":folder_date,"path":list_1_after_201806},index=[x for x in range(len(list_1_after_201806))])
df_1_after_201806['date'].apply(lambda x: x.weekday()).unique()
df_1_after_201806=df_1_after_201806.sort_values("date").reset_index()
del df_1_after_201806['index']
new_dailysales_files=df_1_after_201806.append(recent_file_df)

new_dailysales_files=new_dailysales_files[new_dailysales_files['date']>datetime.date(2018,9,22)]
new_dailysales_files=new_dailysales_files[new_dailysales_files['date']<=datetime.date(2018,12,29)]

print(len(new_dailysales_files))
new_dailysales_files=new_dailysales_files['path'].unique().tolist()


# In[ ]:

combined_rewards_transaction_after_20180922_agg=pd.DataFrame()
count_i=1
print("Total Files: "+str(len(new_dailysales_files)))
for file_daily in new_dailysales_files:
    df=pd.read_table(file_daily,nrows = None,sep= '|',dtype =str)
    df['subclass_transaction_amt']=df['subclass_transaction_amt'].astype(float)
    df['subclass_transaction_units']=df['subclass_transaction_units'].astype(float)
    df=df[~pd.isnull(df['customer_id_hashed'])]
    df_sales=df.groupby(['location_id','transaction_dt','customer_id_hashed'])['subclass_transaction_amt','subclass_transaction_units'].sum().reset_index().rename(columns={"subclass_transaction_amt":"total_transaction_amt","subclass_transaction_units":"total_transaction_units"})
    df_trans=df[['location_id','transaction_dt','transaction_id','customer_id_hashed']].drop_duplicates().groupby(['location_id','transaction_dt','customer_id_hashed'])['transaction_id'].count().to_frame().reset_index().rename(columns={"transaction_id":"transactions"})
    df=pd.merge(df_sales,df_trans,on=['location_id','transaction_dt','customer_id_hashed'],how="left")
    combined_rewards_transaction_after_20180922_agg=combined_rewards_transaction_after_20180922_agg.append(df)
    print(count_i,"done",datetime.datetime.now())
    count_i+=1
    
combined_rewards_transaction_after_20180922_agg=combined_rewards_transaction_after_20180922_agg.rename(columns={"transaction_dt":"transaction_date"})

# combined_rewards_transaction_after_20180922_agg.to_csv("/home/jian/Projects/Big_Lots/Live_Ramp/Quarterly_Update/combined_agged_rewards_transactions_20180929_20181229.csv",index=False)


# In[ ]:

#Getting the store for an id

df_2018_transaction_by_id_1=dftrans_before_20180922[dftrans_before_20180922['transaction_date'].apply(lambda x: x[:4]=="2018")]
df_2018_transaction_by_id_1=df_2018_transaction_by_id_1[df_2018_transaction_by_id_1['total_transaction_amt']>0]
df_2018_transaction_by_id_1=df_2018_transaction_by_id_1.groupby(['customer_id_hashed','location_id'])['total_transaction_amt'].count().to_frame().reset_index()
df_2018_transaction_by_id_1=df_2018_transaction_by_id_1.rename(columns={"total_transaction_amt":"trans"})


df_2018_transaction_by_id_2=combined_rewards_transaction_after_20180922_agg[combined_rewards_transaction_after_20180922_agg['total_transaction_amt']>0]
df_2018_transaction_by_id_2=df_2018_transaction_by_id_2.groupby(['customer_id_hashed','location_id'])['transactions'].sum().to_frame().reset_index().rename(columns={"transactions":"trans"})



# In[ ]:

df_2018_transaction_by_id=df_2018_transaction_by_id_1.append(df_2018_transaction_by_id_2)
df_2018_transaction_by_id=df_2018_transaction_by_id.groupby(['customer_id_hashed','location_id'])['trans'].sum().to_frame().reset_index()
df_2018_transaction_by_id=df_2018_transaction_by_id.sort_values(['customer_id_hashed','trans'],ascending=[True,False]).drop_duplicates(['customer_id_hashed'])
df_2018_transaction_by_id.to_csv(folder_write+"id_by_store_based_on_2018_trans.csv",index=False)

