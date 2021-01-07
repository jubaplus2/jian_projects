
# coding: utf-8

# In[1]:

# Changed on 1207:
#1.Positve Transction both quarter
#2.Correct the mistake of missing 6 days for Q2 for Rewards sales, transactions and unique shoppers
import pandas as pd
import os
import numpy as np
import datetime
import hashlib
import gc
samplerows=None

import logging
logging.basicConfig(filename='BL_2017_2018_Q2_Q3_Sales_Trans_Ids.log', level=logging.INFO)
logging.info(str(datetime.datetime.now())+" | Check_Point_1")


# In[4]:

Q2_Start_week_2018=datetime.date(2018,8,11)-datetime.timedelta(days=13*7) #Campared with Q3
Q2_End_week_2018=datetime.date(2018,11,3)-datetime.timedelta(days=13*7)
Q2_Start_week_2017=datetime.date(2017,8,12)-datetime.timedelta(days=13*7)
Q2_End_week_2017=datetime.date(2017,11,4)-datetime.timedelta(days=13*7)
Q2_2017_Weeks=[str(Q2_Start_week_2017+datetime.timedelta(days=7*i)) for i in range(13)]
Q2_2018_Weeks=[str(Q2_Start_week_2018+datetime.timedelta(days=7*i)) for i in range(13)]


Q3_Start_week_2018=datetime.date(2018,8,11)
Q3_End_week_2018=datetime.date(2018,11,3)
Q3_Start_week_2017=datetime.date(2017,8,12)
Q3_End_week_2017=datetime.date(2017,11,4)
Q3_2017_Weeks=[str(Q3_Start_week_2017+datetime.timedelta(days=7*i)) for i in range(13)]
Q3_2018_Weeks=[str(Q3_Start_week_2018+datetime.timedelta(days=7*i)) for i in range(13)]
logging.info(str(datetime.datetime.now())+" | Check_Point_2")


# In[26]:

weird_id_removed="5efa406f90b87aee3110c8dcebfaddcc246364bc6817e83a8f2ef508ec42f597"

rewards_sales_from_SP=pd.read_csv("/home/jian/Projects/Big_Lots/Loyal_members/loyalty_sales_data/From_Sp/combinedtransactions_0811.csv",
                                 dtype=str,nrows=samplerows,usecols=['customer_id_hashed','transaction_date','transaction_id','location_id','total_transaction_amt','merch_cat'])
rewards_removed=rewards_sales_from_SP[rewards_sales_from_SP['merch_cat']=="80040410"]
rewards_sales_from_SP=rewards_sales_from_SP[rewards_sales_from_SP['merch_cat']!="80040410"]
del rewards_sales_from_SP['merch_cat']
rewards_sales_from_SP=rewards_sales_from_SP.drop_duplicates()
rewards_sales_from_SP['transaction_date']=rewards_sales_from_SP['transaction_date'].apply(lambda x: datetime.datetime.strptime(x,"%Y-%m-%d").date())

rewards_sales_from_SP_2017Q3=rewards_sales_from_SP[(rewards_sales_from_SP['transaction_date']>=(Q3_Start_week_2017-datetime.timedelta(days=6))) & (rewards_sales_from_SP['transaction_date']<=Q3_End_week_2017)]
rewards_sales_from_SP_2017Q2=rewards_sales_from_SP[(rewards_sales_from_SP['transaction_date']>=(Q2_Start_week_2017-datetime.timedelta(days=6))) & (rewards_sales_from_SP['transaction_date']<=Q2_End_week_2017)]
rewards_sales_from_SP_2018Q2=rewards_sales_from_SP[(rewards_sales_from_SP['transaction_date']>=(Q2_Start_week_2018-datetime.timedelta(days=6))) & (rewards_sales_from_SP['transaction_date']<=Q2_End_week_2018)]

weird_id_removed_df=rewards_sales_from_SP[rewards_sales_from_SP['customer_id_hashed']==weird_id_removed]
rewards_sales_from_SP=rewards_sales_from_SP[rewards_sales_from_SP['customer_id_hashed']!=weird_id_removed]


del rewards_sales_from_SP
gc.collect()

rewards_sales_from_SP_2017Q2['total_transaction_amt']=rewards_sales_from_SP_2017Q2['total_transaction_amt'].astype(float)
rewards_sales_from_SP_2017Q3['total_transaction_amt']=rewards_sales_from_SP_2017Q3['total_transaction_amt'].astype(float)
rewards_sales_from_SP_2018Q2['total_transaction_amt']=rewards_sales_from_SP_2018Q2['total_transaction_amt'].astype(float)

rewards_sales_from_SP_2017Q2=rewards_sales_from_SP_2017Q2.drop_duplicates()
rewards_sales_from_SP_2017Q3=rewards_sales_from_SP_2017Q3.drop_duplicates()
rewards_sales_from_SP_2018Q2=rewards_sales_from_SP_2018Q2.drop_duplicates()

gc.collect()

# Check date range

Date_2017_Q2_Min=rewards_sales_from_SP_2017Q2['transaction_date'].min()
Date_2017_Q2_Max=rewards_sales_from_SP_2017Q2['transaction_date'].max()

Date_2017_Q3_Min=rewards_sales_from_SP_2017Q3['transaction_date'].min()
Date_2017_Q3_Max=rewards_sales_from_SP_2017Q3['transaction_date'].max()

Date_2018_Q2_Min=rewards_sales_from_SP_2018Q2['transaction_date'].min()
Date_2018_Q2_Max=rewards_sales_from_SP_2018Q2['transaction_date'].max()


logging.info("2017Q2_min: "+str(Date_2017_Q2_Min))
logging.info("2017Q2_max: "+str(Date_2017_Q2_Max))
logging.info(str((Date_2017_Q2_Max-Date_2017_Q2_Min).days))

logging.info("2017Q3_min: "+str(Date_2017_Q3_Min))
logging.info("2017Q3_max: "+str(Date_2017_Q3_Max))
logging.info(str((Date_2017_Q3_Max-Date_2017_Q3_Min).days))

logging.info("2018Q2_min: "+str(Date_2018_Q2_Min))
logging.info("2018Q2_max: "+str(Date_2018_Q2_Max))
logging.info(str((Date_2018_Q2_Max-Date_2018_Q2_Min).days))
logging.info(str(datetime.datetime.now())+" | Check_Point_3")


# In[ ]:




# In[6]:

rewards_removed_2018_Q3=pd.DataFrame()
rewards_sales_from_SP_2018Q3=pd.read_table("/home/jian/Projects/Big_Lots/Loyal_members/loyalty_sales_data/MediaStormSalesBiWeekly20180814-131127-683.txt",usecols=['customer_id_hashed','transaction_dt','transaction_id','location_id','total_transaction_amt','merch_cat'],sep="|",dtype=str,nrows=samplerows)
rewards_sales_from_SP_2018Q3=rewards_sales_from_SP_2018Q3.rename(columns={"transaction_dt":"transaction_date"})
rewards_removed_2018_Q3=rewards_removed_2018_Q3.append(rewards_sales_from_SP_2018Q3[rewards_sales_from_SP_2018Q3['merch_cat']=="80040410"])
rewards_sales_from_SP_2018Q3=rewards_sales_from_SP_2018Q3[rewards_sales_from_SP_2018Q3['merch_cat']!="80040410"]
rewards_sales_from_SP_2018Q3['transaction_date']=rewards_sales_from_SP_2018Q3['transaction_date'].apply(lambda x: datetime.datetime.strptime(x,"%Y-%m-%d").date())
rewards_sales_from_SP_2018Q3=rewards_sales_from_SP_2018Q3[rewards_sales_from_SP_2018Q3['transaction_date']>=(Q3_Start_week_2018-datetime.timedelta(days=6))]
print(datetime.datetime.now())

biweekly_rewards=pd.read_table("/home/jian/Projects/Big_Lots/Loyal_members/loyalty_sales_data/MediaStormSalesBiWeekly20180828-131144-584.txt",usecols=['customer_id_hashed','transaction_dt','transaction_id','location_id','total_transaction_amt','merch_cat'],sep="|",dtype=str,nrows=samplerows)
biweekly_rewards=biweekly_rewards.rename(columns={"transaction_dt":"transaction_date"})
rewards_removed_2018_Q3=rewards_removed_2018_Q3.append(rewards_sales_from_SP_2018Q3[rewards_sales_from_SP_2018Q3['merch_cat']=="80040410"])
biweekly_rewards=biweekly_rewards[biweekly_rewards['merch_cat']!="80040410"]
biweekly_rewards['transaction_date']=biweekly_rewards['transaction_date'].apply(lambda x: datetime.datetime.strptime(x,"%Y-%m-%d").date())
rewards_sales_from_SP_2018Q3=rewards_sales_from_SP_2018Q3.append(biweekly_rewards)
print(datetime.datetime.now())

biweekly_rewards=pd.read_table("/home/jian/Projects/Big_Lots/Loyal_members/loyalty_sales_data/MediaStormSalesBiWeekly20180911-133008-590.txt",usecols=['customer_id_hashed','transaction_dt','transaction_id','location_id','total_transaction_amt','merch_cat'],sep="|",dtype=str,nrows=samplerows)
biweekly_rewards=biweekly_rewards.rename(columns={"transaction_dt":"transaction_date"})
rewards_removed_2018_Q3=rewards_removed_2018_Q3.append(rewards_sales_from_SP_2018Q3[rewards_sales_from_SP_2018Q3['merch_cat']=="80040410"])
biweekly_rewards=biweekly_rewards[biweekly_rewards['merch_cat']!="80040410"]
biweekly_rewards['transaction_date']=biweekly_rewards['transaction_date'].apply(lambda x: datetime.datetime.strptime(x,"%Y-%m-%d").date())
rewards_sales_from_SP_2018Q3=rewards_sales_from_SP_2018Q3.append(biweekly_rewards)
print(datetime.datetime.now())

biweekly_rewards=pd.read_table("/home/jian/Projects/Big_Lots/Loyal_members/loyalty_sales_data/MediaStormSalesBiWeekly20180925-132958-543.txt",usecols=['customer_id_hashed','transaction_dt','transaction_id','location_id','total_transaction_amt','merch_cat'],sep="|",dtype=str,nrows=samplerows)
biweekly_rewards=biweekly_rewards.rename(columns={"transaction_dt":"transaction_date"})
rewards_removed_2018_Q3=rewards_removed_2018_Q3.append(rewards_sales_from_SP_2018Q3[rewards_sales_from_SP_2018Q3['merch_cat']=="80040410"])
biweekly_rewards=biweekly_rewards[biweekly_rewards['merch_cat']!="80040410"]
biweekly_rewards['transaction_date']=biweekly_rewards['transaction_date'].apply(lambda x: datetime.datetime.strptime(x,"%Y-%m-%d").date())
rewards_sales_from_SP_2018Q3=rewards_sales_from_SP_2018Q3.append(biweekly_rewards)
print(datetime.datetime.now())

biweekly_rewards=pd.read_table("/home/jian/Projects/Big_Lots/Loyal_members/loyalty_sales_data/MediaStormSalesBiWeekly20181009-132856-945.txt",usecols=['customer_id_hashed','transaction_dt','transaction_id','location_id','total_transaction_amt','merch_cat'],sep="|",dtype=str,nrows=samplerows)
biweekly_rewards=biweekly_rewards.rename(columns={"transaction_dt":"transaction_date"})
rewards_removed_2018_Q3=rewards_removed_2018_Q3.append(rewards_sales_from_SP_2018Q3[rewards_sales_from_SP_2018Q3['merch_cat']=="80040410"])
biweekly_rewards=biweekly_rewards[biweekly_rewards['merch_cat']!="80040410"]
biweekly_rewards['transaction_date']=biweekly_rewards['transaction_date'].apply(lambda x: datetime.datetime.strptime(x,"%Y-%m-%d").date())
rewards_sales_from_SP_2018Q3=rewards_sales_from_SP_2018Q3.append(biweekly_rewards)
print(datetime.datetime.now())

biweekly_rewards=pd.read_table("/home/jian/Projects/Big_Lots/Loyal_members/loyalty_sales_data/MediaStormSalesBiWeekly20181023-132316-829.txt",usecols=['customer_id_hashed','transaction_dt','transaction_id','location_id','total_transaction_amt','merch_cat'],sep="|",dtype=str,nrows=samplerows)
biweekly_rewards=biweekly_rewards.rename(columns={"transaction_dt":"transaction_date"})
rewards_removed_2018_Q3=rewards_removed_2018_Q3.append(rewards_sales_from_SP_2018Q3[rewards_sales_from_SP_2018Q3['merch_cat']=="80040410"])
biweekly_rewards=biweekly_rewards[biweekly_rewards['merch_cat']!="80040410"]
biweekly_rewards['transaction_date']=biweekly_rewards['transaction_date'].apply(lambda x: datetime.datetime.strptime(x,"%Y-%m-%d").date())
rewards_sales_from_SP_2018Q3=rewards_sales_from_SP_2018Q3.append(biweekly_rewards)
print(datetime.datetime.now())

biweekly_rewards=pd.read_table("/home/jian/Projects/Big_Lots/Loyal_members/loyalty_sales_data/MediaStormSalesBiWeekly20181106-132358-296.txt",usecols=['customer_id_hashed','transaction_dt','transaction_id','location_id','total_transaction_amt','merch_cat'],sep="|",dtype=str,nrows=samplerows)
biweekly_rewards=biweekly_rewards.rename(columns={"transaction_dt":"transaction_date"})
rewards_removed_2018_Q3=rewards_removed_2018_Q3.append(rewards_sales_from_SP_2018Q3[rewards_sales_from_SP_2018Q3['merch_cat']=="80040410"])
biweekly_rewards=biweekly_rewards[biweekly_rewards['merch_cat']!="80040410"]
biweekly_rewards['transaction_date']=biweekly_rewards['transaction_date'].apply(lambda x: datetime.datetime.strptime(x,"%Y-%m-%d").date())
biweekly_rewards=biweekly_rewards[biweekly_rewards['transaction_date']<=Q3_End_week_2018]
rewards_sales_from_SP_2018Q3=rewards_sales_from_SP_2018Q3.append(biweekly_rewards)
print(datetime.datetime.now())

logging.info(str(datetime.datetime.now())+" | Check_Point_4")

rewards_sales_from_SP_2018Q3['total_transaction_amt']=rewards_sales_from_SP_2018Q3['total_transaction_amt'].astype(float)
del rewards_sales_from_SP_2018Q3['merch_cat']
rewards_sales_from_SP_2018Q3=rewards_sales_from_SP_2018Q3.drop_duplicates()

weird_id_removed_df=weird_id_removed_df.append(rewards_sales_from_SP_2018Q3[rewards_sales_from_SP_2018Q3['customer_id_hashed']==weird_id_removed])
rewards_sales_from_SP_2018Q3=rewards_sales_from_SP_2018Q3[rewards_sales_from_SP_2018Q3['customer_id_hashed']!=weird_id_removed]

Date_2018_Q3_Min=rewards_sales_from_SP_2018Q3['transaction_date'].min()
Date_2018_Q3_Max=rewards_sales_from_SP_2018Q3['transaction_date'].max()

logging.info("2018Q3_min: "+str(Date_2018_Q3_Min))
logging.info("2018Q3_max: "+str(Date_2018_Q3_Max))
logging.info(str((Date_2018_Q3_Max-Date_2018_Q3_Min).days))
del biweekly_rewards
gc.collect()

all_transaction_excluded=weird_id_removed_df.append(rewards_removed_2018_Q3).append(rewards_removed)
logging.info(str(datetime.datetime.now())+" | Check_Point_5")


# In[28]:

rewards_by_store_sales_2017Q2=rewards_sales_from_SP_2017Q2.groupby(['location_id'])['total_transaction_amt'].sum().to_frame().reset_index()
rewards_by_store_sales_2017Q2=rewards_by_store_sales_2017Q2.rename(columns={"total_transaction_amt":"Q2_rewards_sales_2017"})

rewards_by_store_sales_2017Q3=rewards_sales_from_SP_2017Q3.groupby(['location_id'])['total_transaction_amt'].sum().to_frame().reset_index()
rewards_by_store_sales_2017Q3=rewards_by_store_sales_2017Q3.rename(columns={"total_transaction_amt":"Q3_rewards_sales_2017"})

rewards_by_store_sales_2018Q2=rewards_sales_from_SP_2018Q2.groupby(['location_id'])['total_transaction_amt'].sum().to_frame().reset_index()
rewards_by_store_sales_2018Q2=rewards_by_store_sales_2018Q2.rename(columns={"total_transaction_amt":"Q2_rewards_sales_2018"})

rewards_by_store_sales_2018Q3=rewards_sales_from_SP_2018Q3.groupby(['location_id'])['total_transaction_amt'].sum().to_frame().reset_index()
rewards_by_store_sales_2018Q3=rewards_by_store_sales_2018Q3.rename(columns={"total_transaction_amt":"Q3_rewards_sales_2018"})

rewards_sales_by_store=pd.merge(rewards_by_store_sales_2017Q2,rewards_by_store_sales_2017Q3,on=['location_id'],how="outer")
rewards_sales_by_store=pd.merge(rewards_sales_by_store,rewards_by_store_sales_2018Q2,on=['location_id'],how="outer")
rewards_sales_by_store=pd.merge(rewards_sales_by_store,rewards_by_store_sales_2018Q3,on=['location_id'],how="outer")
logging.info(str(datetime.datetime.now())+" | Check_Point_6")


# In[29]:

def count_unique(x):
    y=len(set(x))
    return y

total_id_count_2017Q2=rewards_sales_from_SP_2017Q2.groupby(['location_id'])['customer_id_hashed'].apply(count_unique).to_frame().reset_index().rename(columns={"customer_id_hashed":"2017_Q2_rewards_id_shopped"})
total_id_count_2017Q3=rewards_sales_from_SP_2017Q3.groupby(['location_id'])['customer_id_hashed'].apply(count_unique).to_frame().reset_index().rename(columns={"customer_id_hashed":"2017_Q3_rewards_id_shopped"})
total_id_count_2018Q2=rewards_sales_from_SP_2018Q2.groupby(['location_id'])['customer_id_hashed'].apply(count_unique).to_frame().reset_index().rename(columns={"customer_id_hashed":"2018_Q2_rewards_id_shopped"})
total_id_count_2018Q3=rewards_sales_from_SP_2018Q3.groupby(['location_id'])['customer_id_hashed'].apply(count_unique).to_frame().reset_index().rename(columns={"customer_id_hashed":"2018_Q3_rewards_id_shopped"})

rewards_shoppers_by_store=pd.merge(total_id_count_2017Q2,total_id_count_2017Q3,on=['location_id'],how="outer")
rewards_shoppers_by_store=pd.merge(rewards_shoppers_by_store,total_id_count_2018Q2,on=['location_id'],how="outer")
rewards_shoppers_by_store=pd.merge(rewards_shoppers_by_store,total_id_count_2018Q3,on=['location_id'],how="outer")
logging.info(str(datetime.datetime.now())+" | Check_Point_7")


# In[30]:

rewards_sales_from_SP_2017Q2=rewards_sales_from_SP_2017Q2[rewards_sales_from_SP_2017Q2['total_transaction_amt']>=0]
rewards_sales_from_SP_2017Q3=rewards_sales_from_SP_2017Q3[rewards_sales_from_SP_2017Q3['total_transaction_amt']>=0]
rewards_sales_from_SP_2018Q2=rewards_sales_from_SP_2018Q2[rewards_sales_from_SP_2018Q2['total_transaction_amt']>=0]
rewards_sales_from_SP_2018Q3=rewards_sales_from_SP_2018Q3[rewards_sales_from_SP_2018Q3['total_transaction_amt']>=0]


rewards_by_store_trans_2017Q2=rewards_sales_from_SP_2017Q2.groupby(['location_id'])['total_transaction_amt'].count().to_frame().reset_index()
rewards_by_store_trans_2017Q2=rewards_by_store_trans_2017Q2.rename(columns={"total_transaction_amt":"Q2_rewards_trans_2017"})

rewards_by_store_trans_2017Q3=rewards_sales_from_SP_2017Q3.groupby(['location_id'])['total_transaction_amt'].count().to_frame().reset_index()
rewards_by_store_trans_2017Q3=rewards_by_store_trans_2017Q3.rename(columns={"total_transaction_amt":"Q3_rewards_trans_2017"})

rewards_by_store_trans_2018Q2=rewards_sales_from_SP_2018Q2.groupby(['location_id'])['total_transaction_amt'].count().to_frame().reset_index()
rewards_by_store_trans_2018Q2=rewards_by_store_trans_2018Q2.rename(columns={"total_transaction_amt":"Q2_rewards_trans_2018"})

rewards_by_store_trans_2018Q3=rewards_sales_from_SP_2018Q3.groupby(['location_id'])['total_transaction_amt'].count().to_frame().reset_index()
rewards_by_store_trans_2018Q3=rewards_by_store_trans_2018Q3.rename(columns={"total_transaction_amt":"Q3_rewards_trans_2018"})

rewards_trans_by_store=pd.merge(rewards_by_store_trans_2017Q2,rewards_by_store_trans_2017Q3,on=['location_id'],how="outer")
rewards_trans_by_store=pd.merge(rewards_trans_by_store,rewards_by_store_trans_2018Q2,on=['location_id'],how="outer")
rewards_trans_by_store=pd.merge(rewards_trans_by_store,rewards_by_store_trans_2018Q3,on=['location_id'],how="outer")
logging.info(str(datetime.datetime.now())+" | Check_Point_8")


# In[31]:

output=pd.merge(rewards_sales_by_store,rewards_trans_by_store,on="location_id",how="outer")
output=pd.merge(output,rewards_shoppers_by_store,on="location_id",how="outer")


# In[32]:

writer=pd.ExcelWriter("/home/jian/Projects/Big_Lots/Analysis/2018_Q3/Q3_Store_Quadrants/BL_rewards_shoppers_sales_trans_JL_"+str(datetime.datetime.today().date())+".xlsx",engine="xlsxwriter")
output.to_excel(writer,"date_both_year_Q2Q3",index=False)
all_transaction_excluded.to_excel(writer,"all_transaction_excluded",index=False)
writer.save()


# In[ ]:




# In[ ]:




# In[ ]:



