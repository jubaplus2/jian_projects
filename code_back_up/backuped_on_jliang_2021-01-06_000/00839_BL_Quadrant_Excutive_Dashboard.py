#!/usr/bin/env python
# coding: utf-8

# In[1]:


# Can be autimated across quarter if needed
# To be noted that rolled up to store levels first, and then sum up which results in inflation 
sheetname_forcecode="2019_Q3_Store_Quad_Defination"
quadrant_file_hardcode="/home/jian/celery/Excutive_Dashboard/quadrant_file/Excel_BL_2019_Q3_post_quadrants_JL_2019-11-26.xlsx"


import pandas as pd
import numpy as np
import datetime
import os
import glob
import logging
logging.basicConfig(filename='/home/jian/BL_weekly_crontab/cron_3_Excutive_Dashboard/weekly_Quadrant_Excutive_Dashboard.log', level=logging.INFO)
logging.info("Start: "+str(datetime.datetime.now()))

def recursive_file_gen(my_root_dir):
    for root, dirs, files in os.walk(my_root_dir):
        for file in files:
            yield os.path.join(root, file)



# In[2]:


# In[2]:


# Fix fisrt week
last_saturday=datetime.datetime.now().date()-datetime.timedelta(days=datetime.datetime.now().date().weekday()+2)


First_week_ending_Q3_2019=datetime.date(2019,8,10)

last_year_week=last_saturday-datetime.timedelta(days=52*7)

Nth_week=int((last_saturday-First_week_ending_Q3_2019).days/7)+1

write_folder="/home/jian/Projects/Big_Lots/Analysis/2019_Q3/BL_Excutive_Dashboard/output/output_"+str(last_saturday)+"/"
try:
    os.stat(write_folder)
except:
    os.mkdir(write_folder)
str(last_saturday)
Nth_week
logging.info("last_saturday: "+str(last_saturday))
logging.info("Nth_week: "+str(Nth_week))


# In[3]:


# Checking for the recent weeks that haven't moved to the folder
possible_recent_folders="/home/jian/BigLots/MediaStorm_"+str(last_saturday)+"/"
daily_data_recent=[x for x in list(recursive_file_gen(possible_recent_folders)) if ("aily" in x) & (".txt" in x)]

archived_folders_2018="/home/jian/BigLots/2018_by_weeks/MediaStorm_"+str(last_saturday)+"/"
daily_data_2018_achived=[x for x in list(recursive_file_gen(archived_folders_2018)) if ("aily" in x) & (".txt" in x)]

archived_folders_2019="/home/jian/BigLots/2019_by_weeks/MediaStorm_"+str(last_saturday)+"/"
daily_data_2019_achived=[x for x in list(recursive_file_gen(archived_folders_2019)) if ("aily" in x) & (".txt" in x)]

archived_folders_2020="/home/jian/BigLots/2020_by_weeks/MediaStorm_"+str(last_saturday)+"/"
daily_data_2020_achived=[x for x in list(recursive_file_gen(archived_folders_2020)) if ("aily" in x) & (".txt" in x)]

daily_data_list=daily_data_recent+daily_data_2018_achived+daily_data_2019_achived+daily_data_2020_achived
if len(daily_data_list)!=1:
    print("Error of Daily Sales Data Path")
    logging.info("Error of Daily Sales Data Path: "+str(datetime.datetime.now()))

else:
    daily_data_path_this_year=daily_data_list[0]


# In[4]:


daily_data_path_this_year


# In[6]:


daily_data_list_hist=[x for x in list(recursive_file_gen("/home/jian/BigLots/")) if ("aily" in x) & (".txt" in x)]
daily_data_list_hist=[x for x in daily_data_list_hist if (str(last_year_week) in x)&("DailySales" in x)]
if len(daily_data_list_hist)!=1:
    print("Error of Daily Sales Data Path")
    logging.info("Error of Daily Sales Data Path: "+str(datetime.datetime.now()))
else:
    daily_data_path_last_year=daily_data_list_hist[0]
print("daily_data_path_last_year: ",daily_data_path_last_year)



Df_Quadrant=pd.read_excel(quadrant_file_hardcode,
    dype=str,sheet_name=sheetname_forcecode,usecols=['location_id','Quadrant'])
Df_Quadrant['location_id']=Df_Quadrant['location_id'].astype(str)
Df_Quadrant.shape


# In[7]:


def agg_daily_data_by_store_item_level(file_path_daily):
    df=pd.read_table(file_path_daily,dtype=str,sep="|",usecols=['location_id','transaction_dt','transaction_id','customer_id_hashed','item_transaction_amt'])
    df=df[df['location_id']!="6990"] # 6990 removed
    df['item_transaction_amt']=df['item_transaction_amt'].astype(float)
    df['Reward_Level']=np.where(pd.isnull(df['customer_id_hashed']),"Non_Rewards","Rewards")
    
    print("Total_Sales:",df['item_transaction_amt'].sum())
    df_sales=df.groupby(['location_id','Reward_Level'])['item_transaction_amt'].sum().to_frame().reset_index().rename(columns={"item_transaction_amt":"Sales"})
    df_trans=df[['location_id','transaction_dt','transaction_id','customer_id_hashed','Reward_Level']].drop_duplicates().groupby(['location_id','Reward_Level'])['transaction_id'].count().to_frame().reset_index().rename(columns={"transaction_id":"Transactions"})
    
    df=df[df['Reward_Level']=="Rewards"]
    df_ids=df.groupby(['location_id','Reward_Level'])['customer_id_hashed'].nunique().to_frame().reset_index().rename(columns={"customer_id_hashed":"shopped_unique_ids_in_the_week"})
    
    df=pd.merge(df_sales,df_trans,on=["location_id",'Reward_Level'],how="outer")
    df=pd.merge(df,df_ids,on=["location_id",'Reward_Level'],how="outer")
    
    return df


print(daily_data_path_this_year)
print(daily_data_path_last_year)
logging.info("daily_data_path_this_year: "+str(daily_data_path_this_year))
logging.info("daily_data_path_last_year: "+str(daily_data_path_last_year))



df_daily_this_year_by_store=agg_daily_data_by_store_item_level(daily_data_path_this_year)
df_daily_last_year_by_store=agg_daily_data_by_store_item_level(daily_data_path_last_year)


df_daily_this_year_by_store.columns=df_daily_this_year_by_store.columns.tolist()[:2]+["This_Year_"+x for x in df_daily_this_year_by_store.columns.tolist()[2:]]
df_daily_last_year_by_store.columns=df_daily_last_year_by_store.columns.tolist()[:2]+["Last_Year_"+x for x in df_daily_last_year_by_store.columns.tolist()[2:]]
both_year=pd.merge(df_daily_this_year_by_store,df_daily_last_year_by_store,on=['location_id','Reward_Level'],how="outer")
both_year=pd.merge(both_year,Df_Quadrant,on="location_id",how="left")


both_year_exlusion_last_year=both_year[(pd.isnull(both_year['Last_Year_Sales'])) | (pd.isnull(both_year['Last_Year_Transactions']))]
both_year_exlusion_this_year=both_year[(pd.isnull(both_year['This_Year_Sales'])) | (pd.isnull(both_year['This_Year_Transactions']))]
exlusion_no_quad=both_year[pd.isnull(both_year['Quadrant'])]

all_exclusion_stores=set(both_year_exlusion_last_year['location_id'].tolist()+both_year_exlusion_this_year['location_id'].tolist()+exlusion_no_quad['location_id'].tolist())



both_year_inclusions=both_year[~both_year['location_id'].isin(all_exclusion_stores)]

both_year_inclusions_Total=both_year_inclusions.groupby(['Reward_Level'])['This_Year_Sales','This_Year_Transactions','This_Year_shopped_unique_ids_in_the_week','Last_Year_Sales','Last_Year_Transactions','Last_Year_shopped_unique_ids_in_the_week'].sum().reset_index()
both_year_inclusions_Total["Summary_Level"]="Total"
both_year_inclusions_Total_store_counts=both_year_inclusions.groupby(['Reward_Level'])['location_id'].nunique().reset_index().rename(columns={"location_id":"store_counts"})
both_year_inclusions_Total=pd.merge(both_year_inclusions_Total,both_year_inclusions_Total_store_counts,on="Reward_Level")

both_year_inclusions_Quad=both_year_inclusions.groupby(['Reward_Level','Quadrant'])['This_Year_Sales','This_Year_Transactions','This_Year_shopped_unique_ids_in_the_week','Last_Year_Sales','Last_Year_Transactions','Last_Year_shopped_unique_ids_in_the_week'].sum().reset_index()
both_year_inclusions_Quad_store_counts=both_year_inclusions.groupby(['Reward_Level','Quadrant'])['location_id'].nunique().reset_index().rename(columns={"location_id":"store_counts"})
both_year_inclusions_Quad=pd.merge(both_year_inclusions_Quad,both_year_inclusions_Quad_store_counts,on=["Reward_Level","Quadrant"])
both_year_inclusions_Quad=both_year_inclusions_Quad.rename(columns={"Quadrant":"Summary_Level"})

output=both_year_inclusions_Total.append(both_year_inclusions_Quad)
output=output.sort_values(['Summary_Level','Reward_Level'],ascending=[True,False])

output=output[output['Summary_Level']=="Total"].append(output[output['Summary_Level']!="Total"])

output_sales_both_R_N=output.groupby(['Summary_Level'])['This_Year_Sales'].sum().to_frame().reset_index().rename(columns={"This_Year_Sales":"This_week_Total_R_and_N"})
output=pd.merge(output,output_sales_both_R_N,on="Summary_Level")



output['YoY_Incr_Sales']=np.round((output['This_Year_Sales']-output['Last_Year_Sales'])/output['Last_Year_Sales'],4)
output['YoY_Incr_Rew_Shoppers']=np.round((output['This_Year_shopped_unique_ids_in_the_week']-output['Last_Year_shopped_unique_ids_in_the_week'])/output['Last_Year_shopped_unique_ids_in_the_week'],4)
output['YoY_Incr_Transactions']=np.round((output['This_Year_Transactions']-output['Last_Year_Transactions'])/output['Last_Year_Transactions'],4)
output['Avg_Order_Value']=np.round(output['This_Year_Sales']/output['This_Year_Transactions'],4)

output['Rew/Non-Rew_Share_of_Sales']=np.round(output['This_Year_Sales']/output['This_week_Total_R_and_N'],4)
output['Week_Ending_Date']=str(last_saturday)



output_final=output[['Week_Ending_Date','Summary_Level','Reward_Level','store_counts','YoY_Incr_Sales','YoY_Incr_Rew_Shoppers','YoY_Incr_Transactions','Avg_Order_Value',
                     'Rew/Non-Rew_Share_of_Sales','This_Year_Transactions','This_Year_Sales','This_Year_shopped_unique_ids_in_the_week']].rename(columns={"This_Year_Transactions":"Transactions_last_7_days",
                     "This_Year_Sales":"Sales_last_7_days","This_Year_shopped_unique_ids_in_the_week":"Shopped_Rew_Unique_IDs_last_7_days"})



exclusion_1=exlusion_no_quad[['location_id']]
exclusion_1['exclusion']="Not_Defined_Quadrant"

exclusion_2=both_year_exlusion_last_year[['location_id']]
exclusion_2['exclusion']="Lack_of_week_last_year"

exclusion_3=both_year_exlusion_this_year[['location_id']]
exclusion_3['exclusion']="Lack_of_week_this_year"

exclusion_df=exclusion_3.append(exclusion_2).append(exclusion_1).drop_duplicates('location_id')
exclusion_df['Week_Ending_Date']=str(last_saturday)


def int_ceil_not_NA(x):
    if ~np.isnan(x):
        y=int(np.ceil(x))
    else:
        y=x
    return y


output_final=output_final.rename(columns={"Week_Ending_Date":"Week Ending Date","Summary_Level":"Summary Level","Reward_Level":"Reward Level",
                         "store_counts":"# Stores","YoY_Incr_Sales":"YOY incr Sales/Store","YoY_Incr_Rew_Shoppers":"YOY incr Rew Shoppers/Store",
                         "YoY_Incr_Transactions":"YOY incr Transactions/Store","Avg_Order_Value":"Avg Order Value",
                         "Rew/Non-Rew_Share_of_Sales":"Rew share of Sales","Transactions_last_7_days":"Transactions last 7 days",
                         "Sales_last_7_days":"Total Sales Last 7 Days","Shopped_Rew_Unique_IDs_last_7_days":"Shopped Rew IDs Last 7 days"})
output_final['Reward Level']=output_final['Reward Level'].replace("Non_Rewards","Non-Rewards")
output_final['Avg # of Reward IDs shopped per store']=output_final['Shopped Rew IDs Last 7 days']/output_final['# Stores']

output_final['Avg # of Reward IDs shopped per store']=output_final['Avg # of Reward IDs shopped per store'].apply(lambda x: int_ceil_not_NA(x))
output_final['Weekly Cost']=np.nan


output_final.to_csv(write_folder+"output_"+str(last_saturday)+".csv",index=False)
exclusion_df.to_csv(write_folder+"exclusion_stores_"+str(last_saturday)+".csv",index=False)
both_year_inclusions.to_csv(write_folder+"inclusion_by_store_"+str(last_saturday)+".csv",index=False)


Simeng_recent_weekly_data_folder="/home/simeng/outputs_"+str(last_saturday)+"/"

output_final.to_csv(Simeng_recent_weekly_data_folder + 'output_quadrant.csv',index = False)


logging.info("Done: "+str(datetime.datetime.now()))
print(datetime.datetime.now())

