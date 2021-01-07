
# coding: utf-8

# In[1]:

import pandas as pd
import gc
gc.collect()
import datetime
import hashlib
import numpy as np
import logging
import os


# In[2]:
today_str=str(datetime.datetime.now().date())
writer_folder="/home/jian/Projects/Big_Lots/Loyal_members/sales_of_loyalty_member/"+today_str+"/"
try:
    os.stat(writer_folder)
except:
    os.mkdir(writer_folder)


# In[3]:

logging.basicConfig(filename='merge_sales_with_signing_up_location_id.log', level=logging.INFO)
logging.info("Start Running: "+str(datetime.datetime.now()))


# In[4]:

df_id_zip=pd.read_csv("/home/jian/Projects/Big_Lots/Loyal_members/Email_Zips/output_loyalty_member_by_id.csv",dtype=str)
del df_id_zip['sign_up_date']

df_id_zip=pd.read_csv("/home/jian/Projects/Big_Lots/Loyal_members/Email_Zips/output_loyalty_member_by_id.csv",dtype=str)
del df_id_zip['sign_up_date']
df_id_zip['customer_zip_code']=df_id_zip['customer_zip_code'].apply(lambda x: str(x).replace(" ",""))
df_id_zip['customer_zip_code']=df_id_zip['customer_zip_code'].apply(lambda x: str(x).replace(" ",""))
df_id_zip['customer_zip_code']=df_id_zip['customer_zip_code'].apply(lambda x: str(x).replace(".",""))
df_id_zip['customer_zip_code']=df_id_zip['customer_zip_code'].apply(lambda x: str(x).replace("-",""))
df_id_zip['customer_zip_code']=df_id_zip['customer_zip_code'].apply(lambda x: str(x).replace("nan",""))

df_id_zip['customer_zip_code']=df_id_zip['customer_zip_code'].fillna("00000")
df_id_zip['customer_zip_code']=df_id_zip['customer_zip_code'].apply(lambda x: x.zfill(5)[0:5])
logging.info("Finished Reading Zip by member: "+str(datetime.datetime.now()))


# In[7]:

data_1=pd.read_csv("/home/jian/Projects/Big_Lots/Loyal_members/sales_of_loyalty_member/MediaStormCustDtl.txt",header=None,dtype=str)
logging.info("Finished Reading Sales Data 1: "+str(datetime.datetime.now()))

data_2=pd.read_csv("/home/jian/Projects/Big_Lots/Loyal_members/sales_of_loyalty_member/MediaStorm customer transaction details - 2018-01-09 - 2018-03-31.txt",dtype=str)
logging.info("Finished Reading Sales Data 2: "+str(datetime.datetime.now()))

data_3=pd.read_csv("/home/jian/Projects/Big_Lots/Loyal_members/sales_of_loyalty_member/MediaStorm customer transaction details - 2018-04-01 - 2018-04-15.txt",dtype=str)
logging.info("Finished Reading Sales Data 3: "+str(datetime.datetime.now()))

data_1.columns=data_2.columns.tolist()  
data_1['customer_id_hashed']=data_1['customer_id_hashed'].apply(lambda x: hashlib.sha256(x.encode('UTF-8')).hexdigest())
logging.info("Finished Hashing: "+str(datetime.datetime.now()))
gc.collect()

def clean_data(df):
    del df['merch_cat']
    df=df[df['location_id']!="6990"]
    df=df[df['location_id']!="145"]
    df['total_transaction_amt']=df['total_transaction_amt'].astype(float)
    df=df.drop_duplicates()    
    df['transaction_date']=df['transaction_date'].apply(lambda x: datetime.datetime.strptime(x,"%Y-%m-%d").date())
    df=df[['customer_id_hashed','location_id','transaction_date','total_transaction_amt']]
    df_sales=df.groupby(['customer_id_hashed','location_id','transaction_date'])['total_transaction_amt'].sum().to_frame().reset_index()
    df_trans=df[df['total_transaction_amt']>0]
    df_trans=df.groupby(['customer_id_hashed','location_id','transaction_date'])['total_transaction_amt'].count().to_frame().reset_index()
    df_trans=df_trans.rename(columns={"total_transaction_amt":"total_transaction_count"})
    df=pd.merge(df_sales,df_trans,on=['customer_id_hashed','location_id','transaction_date'],how="left")
    return df
data_1=clean_data(data_1)
data_2=clean_data(data_2)
data_3=clean_data(data_3)
data_all=data_1.append(data_2).append(data_3).drop_duplicates()
data_all=data_all.drop_duplicates()

del data_1
del data_2
del data_3
gc.collect()

data_all=data_all[data_all['transaction_date']>=datetime.datetime(2017,5,7).date()]
data_all['weekday']=data_all['transaction_date'].apply(lambda x: x.weekday())

data_all['week_end_date']=np.where(data_all['weekday']==6,
                                   data_all['transaction_date'].apply(lambda x: x+datetime.timedelta(days=6)),
                                  data_all['transaction_date'].apply(lambda x: x+datetime.timedelta(days=5-x.weekday()))
                                  )

full_weeks_only=data_all[['week_end_date','transaction_date','location_id']].drop_duplicates()
full_weeks_only=full_weeks_only.groupby(['location_id','week_end_date'])['transaction_date'].count().to_frame().reset_index()
full_weeks_only=full_weeks_only[full_weeks_only['transaction_date']==7]
full_weeks_only.to_csv(writer_folder+"full_46_weeks_loyalty_before.csv",index=False)
    
full_weeks_only['str_week_end_dt']=full_weeks_only['week_end_date'].apply(lambda x: str(x))
full_weeks_only['key']=full_weeks_only['location_id']+"|"+full_weeks_only['str_week_end_dt']

data_all['str_week_end_dt']=data_all['week_end_date'].apply(lambda x: str(x))
data_all['key']=data_all['location_id']+"|"+data_all['str_week_end_dt']
del data_all['str_week_end_dt']
del data_all['transaction_date']
del data_all['weekday']

data_all=data_all[data_all['key'].isin(full_weeks_only['key'])]
data_all_key=data_all['key'].unique().tolist() # clean with common key later
# del data_all['key']
logging.info("Total weeks: "+str(len(data_all['week_end_date'].unique())))


gc.collect()
logging.info("Finished Reading Sales by member: "+str(datetime.datetime.now()))



gc.collect()


# In[8]:

sales_by_store_both=pd.read_csv("/home/jian/BiglotsCode/outputs/combined_sales_long_2018-06-09.csv",dtype=str)
sales_by_store_both['sales']=sales_by_store_both['sales'].astype(float)
sales_by_store_both=sales_by_store_both[sales_by_store_both['sales']>0]

sales_by_store_both['key']=sales_by_store_both['location_id']+"|"+sales_by_store_both['week_end_date']
sales_long_key=sales_by_store_both['key'].unique().tolist()

common_key=list(set(data_all_key).intersection(sales_long_key))

sales_by_store_both=sales_by_store_both[sales_by_store_both['key'].isin(common_key)]
sales_by_store_both=sales_by_store_both.groupby(['location_id'])['sales'].sum().reset_index()
logging.info("Store Sales Common done: "+str(datetime.datetime.now()))

full_weeks_only['str_week_end_dt']=full_weeks_only['week_end_date'].apply(lambda x: str(x))
full_weeks_only['key']=full_weeks_only['location_id']+"|"+full_weeks_only['str_week_end_dt']
full_weeks_only=full_weeks_only[full_weeks_only['key'].isin(common_key)]
full_weeks_only.to_csv(writer_folder+"full_46_weeks_common_after.csv",index=False)

data_all_exception=data_all[~data_all['key'].isin(common_key)]
data_all_exception=data_all_exception.groupby(['customer_id_hashed','week_end_date','location_id'])['total_transaction_amt','total_transaction_count'].sum()
data_all_exception_store=data_all_exception.groupby(['week_end_date','location_id'])['total_transaction_amt','total_transaction_count'].sum()
data_all_exception.to_csv(writer_folder+"exception_store_id"+today_str+".csv",index=True)
data_all_exception_store.to_csv(writer_folder+"exception_store_level"+today_str+".csv",index=True)


data_all=data_all[data_all['key'].isin(common_key)]
data_all=data_all.groupby(['customer_id_hashed','week_end_date','location_id'])['total_transaction_amt','total_transaction_count'].sum()

try:
    data_all=data_all.to_frame()
except:
    logging.info("No need to do 'to_frame' in data_all: "+str(datetime.datetime.now()))


data_all=data_all.reset_index()
    
try:
    del data_all['index']
except:
    logging.info("No index deleted: in data_all"+str(datetime.datetime.now()))
    
logging.info("Finished aggregating by zip by week: "+str(datetime.datetime.now()))
logging.info("Loyalty Sales Common done: "+str(datetime.datetime.now()))
logging.info("Good to go: "+str(datetime.datetime.now()))
# In[9]:

logging.info("Start Merging: "+str(datetime.datetime.now()))
data_all=pd.merge(data_all,df_id_zip,on="customer_id_hashed",how="left")
logging.info("Finished merging: "+str(datetime.datetime.now()))

gc.collect()

logging.info("Start Groupby on zip and location: "+str(datetime.datetime.now()))
data_all_46_full_weeks_zip_location=data_all.groupby(['customer_zip_code','location_id'])['total_transaction_amt'].sum().reset_index()
data_all_46_full_weeks_location=data_all_46_full_weeks_zip_location.groupby(['location_id'])['total_transaction_amt'].sum().reset_index()

data_all_46_full_weeks_zip_location=data_all_46_full_weeks_zip_location.rename(columns={"total_transaction_amt":"loyal_sales_zip"})
gc.collect()
data_all_46_full_weeks_location=data_all_46_full_weeks_location.rename(columns={"total_transaction_amt":"loyal_sales_total"})
data_all_46_full_weeks_location=pd.merge(data_all_46_full_weeks_location,sales_by_store_both,on="location_id",how="left")
data_all_46_full_weeks_location['non_loyalty_sales_total']=data_all_46_full_weeks_location['sales']-data_all_46_full_weeks_location['loyal_sales_total']
# data_all_46_full_weeks_location['non_loyalty_trans_total']=data_all_46_full_weeks_location['transactions']-data_all_46_full_weeks_location['loyal_trans_total']
# non_loyalty_sales=data_all_46_full_weeks_location[['location_id','non_loyalty_sales_total']]
# del data_all_46_full_weeks_location['sales']
data_all_46_full_weeks_location=data_all_46_full_weeks_location.rename(columns={'sales':'total_sales_both'})

# In[16]:
# customer_zip_code
data_all_46_full_weeks_zip_location=pd.merge(data_all_46_full_weeks_zip_location,data_all_46_full_weeks_location,on="location_id",how="left")
data_all_46_full_weeks_zip_location['loyal_sales_pctg']=data_all_46_full_weeks_zip_location['loyal_sales_zip']/data_all_46_full_weeks_zip_location['loyal_sales_total']
data_all_46_full_weeks_zip_location['non_loyalty_sales_zip']=data_all_46_full_weeks_zip_location['non_loyalty_sales_total']*data_all_46_full_weeks_zip_location['loyal_sales_pctg']
data_all_46_full_weeks_location['total_sales_both_zip']=data_all_46_full_weeks_zip_location['total_sales_both']*data_all_46_full_weeks_zip_location['loyal_sales_pctg']
data_all_46_full_weeks_zip_location.to_csv(writer_folder+"/sales_by_location_id_agg_"+today_str+".csv",index=False)




# In[ ]:




# In[21]:

'''
# Do the last kernel above (Read output above)
data_all_46_full_weeks=pd.read_csv("/home/jian/Projects/Big_Lots/Loyal_members/sales_of_loyalty_member/sales_by_zip_20180529.csv",dtype=str)
data_all_46_full_weeks['total_transaction_count']=data_all_46_full_weeks['total_transaction_count'].astype(int)
data_all_46_full_weeks['total_transaction_amt']=data_all_46_full_weeks['total_transaction_amt'].astype(float)
data_all_46_full_weeks_agg=data_all_46_full_weeks.groupby(['customer_zip_code','week_end_date'])['total_transaction_amt','total_transaction_count'].sum().reset_index()
'''

