#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import datetime
import glob
import os
import gc
print(datetime.datetime.now())

def recursive_file_gen(root_folder):
    for root, dirs, files in os.walk(root_folder):
        for file in files:
            yield os.path.join(root,file)

start_date=datetime.date(2017,2,5)


# In[2]:


all_POS_1_item_weekly=list(recursive_file_gen("/home/jian/BigLots/"))
all_POS_1_item_weekly=[x for x in all_POS_1_item_weekly if "/MediaStorm_20" in x and "Daily" in x]
all_POS_1_item_weekly.sort()
all_POS_1_item_weekly=[x for x in all_POS_1_item_weekly if x.split("/MediaStorm_")[1][:10]>="2019-02-16"] #hard-code
print(all_POS_1_item_weekly[0])
df_file_1=pd.DataFrame({"file_path":all_POS_1_item_weekly})
df_file_1['week_end_dt']=df_file_1['file_path'].apply(lambda x: x.split("/MediaStorm_")[1][:10])


# In[3]:


all_POS_2_item_hist=glob.glob("/home/jian/BigLots/hist_daily_data_itemlevel_decompressed/*.txt")
all_POS_2_item_hist.sort()
print(all_POS_2_item_hist[0])
print(all_POS_2_item_hist[-1])
df_file_2=pd.DataFrame({"file_path":all_POS_2_item_hist})
df_file_2['week_end_dt']=df_file_2['file_path'].apply(lambda x: x.split("/MediaStormDailySalesHistory")[1][:10])
df_file_2['week_end_dt']=df_file_2['week_end_dt'].apply(lambda x: os.path.basename(x)[:4]+"-"+os.path.basename(x)[4:6]+"-"+os.path.basename(x)[6:8])
df_file_2.head(2)


# In[4]:


all_POS_3_subclass_hist=glob.glob("/home/jian/BigLots/hist_daily_data_subclasslevel/*.txt")
all_POS_3_subclass_hist.sort()
all_POS_3_subclass_hist=[x for x in all_POS_3_subclass_hist if x.split("ySales_week_ending_")[1][:10]>=str(start_date)]
all_POS_3_subclass_hist=[x for x in all_POS_3_subclass_hist if x.split("ySales_week_ending_")[1][:10]<="2018-08-04"]
print(all_POS_3_subclass_hist[0])
print(all_POS_3_subclass_hist[-1])
df_file_3=pd.DataFrame({"file_path":all_POS_3_subclass_hist})
df_file_3['week_end_dt']=df_file_3['file_path'].apply(lambda x: x.split("MediaStormDailySales_week_ending_")[1][:10])


# In[5]:


all_POS_4_subclass_weekly=list(recursive_file_gen("/home/jian/BigLots/"))
all_POS_4_subclass_weekly=[x for x in all_POS_4_subclass_weekly if "/MediaStorm_20" in x and "Daily" in x]
all_POS_4_subclass_weekly=[x for x in all_POS_4_subclass_weekly if "/MediaStorm_20" in x and ".txt" in x]

all_POS_4_subclass_weekly.sort()
all_POS_4_subclass_weekly=[x for x in all_POS_4_subclass_weekly if x.split("/MediaStorm_")[1][:10]>="2018-06-09"] #hard-code
all_POS_4_subclass_weekly=[x for x in all_POS_4_subclass_weekly if x.split("/MediaStorm_")[1][:10]<="2018-08-04"] #hard-code
print(all_POS_4_subclass_weekly[0])
print(all_POS_4_subclass_weekly[-1])
df_file_4=pd.DataFrame({"file_path":all_POS_4_subclass_weekly})
df_file_4['week_end_dt']=df_file_4['file_path'].apply(lambda x: x.split("/MediaStorm_")[1][:10])


# In[6]:


df_all_sales=pd.concat([df_file_1,df_file_2,df_file_3,df_file_4])
df_all_sales=df_all_sales.sort_values("week_end_dt")
df_all_sales=df_all_sales.reset_index()
del df_all_sales['index']
print(df_all_sales.shape)
print(df_all_sales['week_end_dt'].nunique())
print(df_all_sales['week_end_dt'].min(),df_all_sales['week_end_dt'].max())


# In[7]:


df_all_sales['date']=df_all_sales['week_end_dt'].apply(lambda x: datetime.datetime.strptime(x,"%Y-%m-%d").date())
df_all_sales['diff_week_counts']=df_all_sales['date'].apply(lambda x: int((x-datetime.date(2017,2,5)).days/7)+1)
df_all_sales['diff_year_counts']=df_all_sales['date'].apply(lambda x: int(int((x-datetime.date(2017,2,5)).days/7)/52))


# In[8]:


df_all_sales['Quarter']=df_all_sales['diff_week_counts'].apply(lambda x: int(x/13)%4+1)
df_all_sales['Year']=df_all_sales['diff_year_counts'].apply(lambda x: 2017+x)
df_all_sales['quarter_str']=df_all_sales[['Year','Quarter']].values.tolist()
df_all_sales['quarter_str']=df_all_sales['quarter_str'].apply(lambda x: str(x[0])+"_Q"+str(x[1]))


# In[9]:


df_all_sales.groupby("quarter_str")['week_end_dt'].agg({"week_end_dt":['min','max','count']}).reset_index()


# In[10]:


df_all_sales.head(2)


# In[11]:


# samplerows=100000
samplerows=None
df_prod_taxonomy=pd.read_table("/home/jian/BigLots/static_files/ProductTaxonomy/MediaStormProductTaxonomy20200301-134228-899.txt",
                               sep="|",dtype=str,usecols=['division_id','class_code_id','subclass_id']).drop_duplicates()
df_output_by_week_store=pd.DataFrame()
dict_output_by_week_shoppers=dict()
df_output_by_quater_shoppers=pd.DataFrame()

i=0
for quarter,df_file_group in df_all_sales.groupby("quarter_str"):
    df_quarter_shopper=pd.DataFrame()
    for ind,row in df_file_group.iterrows():
        file_path=row['file_path']
        week_end_dt=row['week_end_dt']
        try:
            df=pd.read_table(file_path,sep="|",dtype=str,nrows=samplerows,
                             usecols=['location_id','transaction_dt','transaction_id','customer_id_hashed','class_code_id','subclass_id','subclass_transaction_units','subclass_transaction_amt'])
            df=df.rename(columns={"subclass_transaction_units":"units","subclass_transaction_amt":"sales"})
        except:
            try:
                df=pd.read_table(file_path,sep="|",dtype=str,nrows=samplerows,
                                 usecols=['location_id','transaction_dt','transaction_id','customer_id_hashed','class_code_id','subclass_id','item_transaction_units','item_transaction_amt'])
                df=df.rename(columns={"item_transaction_units":"units","item_transaction_amt":"sales"})
            except:
                print(file_path)
                break
        df['units']=df['units'].astype(int)
        df['sales']=df['sales'].astype(float)
        df=pd.merge(df,df_prod_taxonomy,on=['class_code_id','subclass_id'],how="left")
        df['division_id']=df['division_id'].fillna("others")
        df_rewards=df[pd.notnull(df['customer_id_hashed'])]
        df_nonrewards=df[pd.isnull(df['customer_id_hashed'])]

        df_rewards_sales=df_rewards.groupby(["location_id",'division_id'])['sales','units'].sum().reset_index()
        df_rewards_sales=df_rewards_sales.rename(columns={"sales":"rewards_sales","units":"rewards_units"})
        df_rewards_trans=df_rewards[['location_id','transaction_dt','transaction_id','customer_id_hashed','division_id']].drop_duplicates()
        df_rewards_trans=df_rewards_trans.groupby(["location_id",'division_id'])['transaction_id'].count().to_frame().reset_index()
        df_rewards_trans=df_rewards_trans.rename(columns={"transaction_id":"rewards_trans"})
        df_rewards_shoppers=df_rewards.groupby(["location_id",'division_id'])['customer_id_hashed'].nunique().to_frame().reset_index()
        df_rewards_shoppers=df_rewards_shoppers.rename(columns={"customer_id_hashed":"unique_shoppers"})

        df_nonrewards_sales=df_nonrewards.groupby(["location_id",'division_id'])['sales','units'].sum().reset_index()
        df_nonrewards_sales=df_nonrewards_sales.rename(columns={"sales":"nonrewards_sales","units":"nonrewards_units"})
        df_nonrewards_trans=df_rewards[['location_id','transaction_dt','transaction_id','customer_id_hashed','division_id']].drop_duplicates()
        df_nonrewards_trans=df_nonrewards_trans.groupby(["location_id",'division_id'])['transaction_id'].count().to_frame().reset_index()
        df_nonrewards_trans=df_nonrewards_trans.rename(columns={"transaction_id":"nonrewards_trans"})

        df_by_store=pd.merge(df_rewards_sales,df_rewards_trans,on=["location_id",'division_id'],how="outer")
        df_by_store=pd.merge(df_by_store,df_rewards_shoppers,on=["location_id",'division_id'],how="outer")
        df_by_store=pd.merge(df_by_store,df_nonrewards_sales,on=["location_id",'division_id'],how="outer")
        df_by_store=pd.merge(df_by_store,df_nonrewards_trans,on=["location_id",'division_id'],how="outer")
        df_by_store=df_by_store.fillna(0)
        df_by_store.insert(0,"week_end_dt",[week_end_dt]*len(df_by_store))
        df_by_store.insert(0,"quarter",[quarter]*len(df_by_store))

        df_output_by_week_store=df_output_by_week_store.append(df_by_store)

        shoppers_week=df_rewards['customer_id_hashed'].nunique()
        dict_output_by_week_shoppers.update({week_end_dt:shoppers_week})
        df_quarter_shopper=df_quarter_shopper.append(df_rewards[['division_id','customer_id_hashed']].drop_duplicates())
        i+=1
        if i%10==1:
            print(datetime.datetime.now(),week_end_dt)
            
    df_id_quarter_shopper=df_quarter_shopper.groupby(['division_id'])['customer_id_hashed'].nunique().to_frame().reset_index()
    df_id_quarter_shopper['quarter']='quarter'
    df_output_by_quater_shoppers=df_output_by_quater_shoppers.append(df_id_quarter_shopper)
    print(datetime.datetime.now(),"done of quater: ",quarter)


# In[12]:


df_shopper_weekly=pd.DataFrame(dict_output_by_week_shoppers,index=[0]).T.reset_index()
df_shopper_weekly.columns=['week_end_dt','shopper_count']


# In[13]:


df_output_by_quater_shoppers.head(2)


# In[14]:


df_output_by_week_store.head(3)


# In[15]:


df_output_quarter_agg=df_output_by_week_store.groupby(['quarter','division_id'])['rewards_sales','rewards_units','rewards_trans',
                                                                                'nonrewards_sales','nonrewards_units','nonrewards_trans'].sum().reset_index()
df_output_quarter_store_count=df_output_by_week_store.groupby(['quarter','division_id'])['location_id'].nunique().to_frame().reset_index()


# In[16]:


df_output_quarter_agg=pd.merge(df_output_quarter_agg,df_output_by_quater_shoppers,on=['quarter','division_id'],how="left")
df_output_quarter_agg=pd.merge(df_output_quarter_agg,df_output_quarter_store_count,on=['quarter','division_id'],how="left")
df_output_quarter_agg=df_output_quarter_agg.fillna(0)


# In[17]:


df_store_available=df_output_by_week_store.groupby(['location_id','division_id','quarter'])['week_end_dt'].count().to_frame().reset_index()
df_store_available.head(2)


# In[19]:


print("df_output_by_week_store.shape",df_output_by_week_store.shape)


# In[18]:


writer=pd.ExcelWriter("./BL_divsion_sales_2_years_JL_"+str(datetime.datetime.now().date())+".xlsx",engine="xlsxwriter")
df_output_quarter_agg.to_excel(writer,"performace",index=False)
df_store_available.to_excel(writer,"store_weeks_available",index=False)
writer.save()

df_output_by_week_store.to_csv("./BL_2_years_sales_by_store_division_week_JL_"+str(datetime.datetime.now().date())+".csv",index=False)

