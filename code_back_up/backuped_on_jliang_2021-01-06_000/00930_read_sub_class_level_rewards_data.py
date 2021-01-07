
# coding: utf-8

# In[1]:

import pandas as pd
import glob
import os
import datetime
import numpy as np


# In[2]:

def recrusive_file_gen(root_path):
    for root,dirs,files in os.walk(root_path):
        for file in files:
            yield os.path.join(root,file)


# In[3]:

historical_daily=glob.glob("/home/jian/BigLots/hist_daily_data_subclasslevel/*.txt")
historical_daily_df=pd.DataFrame({"file_path":historical_daily})
historical_daily_df['date']=historical_daily_df['file_path'].apply(lambda x: x.split("vel/MediaStormDailySales_week_ending_")[1][:10])
historical_daily_df=historical_daily_df[historical_daily_df['date']>="2017-02-05"]
print(historical_daily_df.shape)
print(historical_daily_df['date'].max())


# In[4]:

daily_2018=list(recrusive_file_gen("/home/jian/BigLots/2018_by_weeks/"))
daily_2018=[x for x in daily_2018 if ("aily" in x) & (".txt" in x)]

daily_2018_df=pd.DataFrame({"file_path":daily_2018})

daily_2018_df['date']=daily_2018_df['file_path'].apply(lambda x: x.split("2018_by_weeks/MediaStorm_")[1][:10])

daily_2018_df=daily_2018_df[daily_2018_df['date']>"2018-06-09"]

print(daily_2018_df.shape)
print(daily_2018_df['date'].max())


# In[5]:

daily_2019=list(recrusive_file_gen("/home/jian/BigLots/2019_by_weeks/"))
daily_2019=[x for x in daily_2019 if ("aily" in x) & (".txt" in x)]

daily_2019_df=pd.DataFrame({"file_path":daily_2019})

daily_2019_df['date']=daily_2019_df['file_path'].apply(lambda x: x.split("2019_by_weeks/MediaStorm_")[1][:10])

daily_2019_df=daily_2019_df[daily_2019_df['date']<="2019-02-02"]

print(daily_2019_df.shape)
print(daily_2019_df['date'].max())


# In[6]:

all_file=historical_daily_df['file_path'].tolist()+daily_2018_df['file_path'].tolist()+daily_2019_df['file_path'].tolist()
len(all_file)


# In[9]:

i_counter=0
df_rewards_summary=pd.DataFrame()
for file in all_file:
    df=pd.read_csv(file,sep="|",dtype=str,usecols=['location_id','transaction_dt','customer_id_hashed','subclass_transaction_amt'])
    df['subclass_transaction_amt']=df['subclass_transaction_amt'].astype(float)
    df=df[df['location_id']!="6990"]
    del df['location_id']
    df_date_max=df['transaction_dt'].max()
    df_date_min=df['transaction_dt'].min()
    
    df['rewards_label']=np.where(pd.isnull(df['customer_id_hashed']),"Non_Rewards","Rewards")
    del df['transaction_dt']
    del df['customer_id_hashed']
    # Sales only
    df=df.groupby(['rewards_label'])['subclass_transaction_amt'].sum().to_frame().reset_index()
    
    df['week_start']=df_date_min
    df['week_end']=df_date_max
    
    df_rewards_summary=df_rewards_summary.append(df)
    print(datetime.datetime.now(),i_counter)
    
    i_counter+=1
    
    if i_counter%10==1:
        print(datetime.datetime.now(),i_counter)
        
    


# In[10]:

df_rewards_summary.shape


# In[11]:

os.getcwd()


# In[12]:

df_rewards_summary.to_csv("/home/jian/Projects/Big_Lots/Analysis/2019_Q1/Planner_Request/Jian_Checking_Friend_and_Family_Dates/rewards_sales_in_MMM_JL_0430.csv",index=False)


# In[ ]:

df_rewards_summary['rewards_ratio']=df_rewards_summary


# In[14]:

df_rewards_summary.head(6)


# In[29]:

df_rewards_summary_wide=df_rewards_summary.pivot(index='week_start',columns='rewards_label',values='subclass_transaction_amt').reset_index()
df_rewards_summary_wide=df_rewards_summary_wide.sort_values('week_start')
df_rewards_summary_wide['total']=df_rewards_summary_wide['Rewards']+df_rewards_summary_wide['Non_Rewards']
df_rewards_summary_wide['rewards_ratio']=df_rewards_summary_wide['Rewards']/df_rewards_summary_wide['total']


# In[31]:

df_rewards_summary_wide.to_csv("/home/jian/Projects/Big_Lots/Analysis/2019_Q1/Planner_Request/Jian_Checking_Friend_and_Family_Dates/wide_rewards_sales_in_MMM_JL_0430.csv",index=False)


# In[26]:

4.458004/(4.458004+3.005504)


# In[ ]:



