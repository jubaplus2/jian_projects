
# coding: utf-8

# In[84]:

import pandas as pd
import numpy as np
import datetime
import os
import logging
import json
logging.basicConfig(filename='Extract_Dec17_Nov18_RewShoppers.log', level=logging.INFO)
samplerows=None


# In[77]:

def recursive_file_gen(my_root_dir):
    for root, dirs, files in os.walk(my_root_dir):
        for file in files:
            yield os.path.join(root, file)


# In[78]:

historical_daily_subclass_files=list(recursive_file_gen("/home/jian/BigLots/hist_daily_data_subclasslevel/"))
df_historical_daily_subclass_files=pd.DataFrame({"file_path":historical_daily_subclass_files})
df_historical_daily_subclass_files['Week_Date']=df_historical_daily_subclass_files['file_path'].apply(lambda x: x.split("es_week_ending_")[1][:10])
df_historical_daily_subclass_files=df_historical_daily_subclass_files[df_historical_daily_subclass_files['Week_Date']>="2017-12-03"]
df_historical_daily_subclass_files=df_historical_daily_subclass_files.sort_values("Week_Date",ascending=True)


# In[79]:

_daily_2018_subclass_files=list(recursive_file_gen("/home/jian/BigLots/2018_by_weeks/"))
_daily_2018_subclass_files=[x for x in _daily_2018_subclass_files if "aily" in x]
_daily_2018_subclass_files=[x for x in _daily_2018_subclass_files if ".txt" in x]
df_daily_2018_subclass_files=pd.DataFrame({"file_path":_daily_2018_subclass_files})

df_daily_2018_subclass_files['Week_Date']=df_daily_2018_subclass_files['file_path'].apply(lambda x: x.split("2018_by_weeks/MediaStorm_")[1][:10])
df_daily_2018_subclass_files=df_daily_2018_subclass_files[df_daily_2018_subclass_files['Week_Date']>=df_historical_daily_subclass_files['Week_Date'].max()]
df_daily_2018_subclass_files=df_daily_2018_subclass_files[df_daily_2018_subclass_files['Week_Date']<="2018-12-01"]

df_daily_2018_subclass_files=df_daily_2018_subclass_files.sort_values("Week_Date",ascending=True)


# In[80]:

all_12_month_sales=df_historical_daily_subclass_files.append(df_daily_2018_subclass_files).reset_index()
del all_12_month_sales['index']
logging.info(str(all_12_month_sales.shape))
logging.info(all_12_month_sales['Week_Date'].min())
logging.info(all_12_month_sales['Week_Date'].max())

logging.info("Started: "+str(datetime.datetime.now()))


# In[81]:

all_shopped_rewards_Dec17_Nov18=pd.DataFrame()

for i in range(len(all_12_month_sales)):
    week_end_date=all_12_month_sales['Week_Date'][i]
    file=all_12_month_sales['file_path'][i]
    df=pd.read_table(file,nrows=samplerows,dtype=str,sep="|",usecols=["customer_id_hashed","subclass_transaction_amt"])
    df=df[~pd.isnull(df['customer_id_hashed'])]
    df['subclass_transaction_amt']=df['subclass_transaction_amt'].astype(float)
    df=df[df['subclass_transaction_amt']>0]
    del df['subclass_transaction_amt']
    df=df.drop_duplicates()
    df['week_end']=week_end_date
    
    all_shopped_rewards_Dec17_Nov18=all_shopped_rewards_Dec17_Nov18.append(df)
    logging.info(str(i)+"|"+str(datetime.datetime.now())+"|"+week_end_date)
    
all_shopped_rewards_list=all_shopped_rewards_Dec17_Nov18['customer_id_hashed'].unique().tolist()


# In[86]:

with open("/home/jian/Projects/Big_Lots/Analysis/2018_Q4/URL_Pages/RewardsShoppersList_in_12_month_JL_"+str(datetime.datetime.now().date())+".json", 'w') as outfile:
    json.dump(all_shopped_rewards_list, outfile)


# In[82]:

all_shopped_rewards_Dec17_Nov18.to_csv("/home/jian/Projects/Big_Lots/Analysis/2018_Q4/URL_Pages/RewardsShoppers_in_12_month_JL_"+str(datetime.datetime.now().date())+".csv",index=False)



# In[ ]:



