
# coding: utf-8

# In[82]:

import pandas as pd
import numpy as np
import logging
import datetime
import os
import glob
Read_rows=None


# In[71]:

os.getcwd()
logging.basicConfig(filename='Load_All_Daily_and_Compare_to_Rewards_'+str(datetime.datetime.now().date())+'_files.log', level=logging.INFO)


# # Load Daily Data

# In[72]:

daily_2017_file_list_1=glob.glob("/home/jian/BigLots/2017_Daily_Sales_Weeks_Quarter_as_Transfer/2017_Q3/*.txt")
daily_2017_file_list_2=glob.glob("/home/jian/BigLots/2017_Daily_Sales_Weeks_Quarter_as_Transfer/2017_Q4/*.txt")
daily_2017_file_list_3=glob.glob("/home/jian/BigLots/2017_Daily_Sales_Weeks_Quarter_as_Transfer/2018_Q1/*.txt")


# In[73]:

start_date_2018_daily=datetime.date(2018,6,16)

all_2018_received_dates=[start_date_2018_daily+datetime.timedelta(days=x*7) for x in range(24)]
max(all_2018_received_dates)
daily_2018_file_list=list()
for date_folder in all_2018_received_dates:
    folder_path="/home/jian/BigLots/2018_by_weeks/MediaStorm_"+str(date_folder)+"/"
    file_daily=[x for x in glob.glob(folder_path+"*.txt") if "ailySales" in x]
    if len(file_daily)!=1:
        logging.info("Missing daily date in teh folder: "+str(date_folder))
    daily_2018_file_list.append(file_daily[0])

all_file_daily=daily_2017_file_list_1+daily_2017_file_list_2+daily_2017_file_list_3+daily_2018_file_list


# In[74]:

daily_aggregated=pd.DataFrame()
i=0
for file in all_file_daily:
    logging.info(datetime.datetime.now())
    df=pd.read_table(file,dtype=str,sep="|",nrows=Read_rows)
    df=df.drop_duplicates()
    df['subclass_transaction_amt']=df['subclass_transaction_amt'].astype(float)
    df['subclass_transaction_units']=df['subclass_transaction_units'].astype(int)
    df['rewards_label']=np.where(pd.isnull(df['customer_id_hashed']),"Non_Rewards","Rewards")
    df['transaction_dt']=df['transaction_dt'].apply(lambda x: datetime.datetime.strptime(x,"%Y-%m-%d").date())
    i+=1
    logging.info("File "+str(i)+": "+str(df['transaction_dt'].min())+" to "+str(df['transaction_dt'].max()))
    df_sales=df.groupby(['location_id','rewards_label','transaction_dt'])['subclass_transaction_amt'].sum().to_frame().reset_index()
    df_sales=df_sales.rename(columns={"subclass_transaction_amt":"sales"})
    df_trans=df.groupby(['location_id','rewards_label','transaction_dt'])['subclass_transaction_amt'].count().to_frame().reset_index()
    df_trans=df_trans.rename(columns={"subclass_transaction_amt":"trans"})
    
    df=pd.merge(df_sales,df_trans,on=['location_id','rewards_label','transaction_dt'],how="left")
    daily_aggregated=daily_aggregated.append(df)


# In[75]:

daily_aggregated['location_id']=daily_aggregated['location_id'].astype(int)
def add_week_end_dt(x):
    if x.weekday()==6:
        y=x+datetime.timedelta(days=6)
    else:
        y=x+datetime.timedelta(days=5-x.weekday())
    return y
daily_aggregated['week_end_dt']=daily_aggregated['transaction_dt'].apply(lambda x: add_week_end_dt(x))
daily_aggregated=daily_aggregated.sort_values(['location_id','transaction_dt','rewards_label'])

daily_aggregated_rewards=daily_aggregated[daily_aggregated['rewards_label']=="Rewards"]
daily_aggregated_Non_rewards=daily_aggregated[daily_aggregated['rewards_label']=="Non_Rewards"]

daily_aggregated_rewards_by_week=daily_aggregated_rewards.groupby(['location_id','week_end_dt'])['sales','trans'].sum().reset_index()
daily_aggregated_Non_rewards_by_week=daily_aggregated_Non_rewards.groupby(['location_id','week_end_dt'])['sales','trans'].sum().reset_index()
daily_aggregated_Total_by_week=daily_aggregated.groupby(['location_id','week_end_dt'])['sales','trans'].sum().reset_index()

daily_aggregated_rewards_by_week=daily_aggregated_rewards_by_week.rename(columns={"sales":"R_sales_DailyData","trans":"R_trans_DailyData"})
daily_aggregated_Non_rewards_by_week=daily_aggregated_Non_rewards_by_week.rename(columns={"sales":"NonR_sales_DailyData","trans":"NonR_trans_DailyData"})
daily_aggregated_Total_by_week=daily_aggregated_Total_by_week.rename(columns={"sales":"Total_sales_DailyData","trans":"Total_trans_DailyData"})

daily_data_by_week=pd.merge(daily_aggregated_rewards_by_week,daily_aggregated_Non_rewards_by_week,on=['location_id','week_end_dt'],how="left")
daily_data_by_week=pd.merge(daily_data_by_week,daily_aggregated_Total_by_week,on=['location_id','week_end_dt'],how="left")


# # Load Rewards Data removing 80040410

# In[76]:

data_from_SP=pd.read_csv("/home/jian/Projects/Big_Lots/Loyal_members/loyalty_sales_data/From_Sp/combinedtransactions_0811.csv",nrows=Read_rows,
                         dtype=str,usecols=['customer_id_hashed','location_id','transaction_date','transaction_id','total_transaction_amt','merch_cat'])
data_from_SP=data_from_SP[data_from_SP['merch_cat']!="80040410"]
data_from_SP=data_from_SP.drop_duplicates()
data_from_SP['total_transaction_amt']=data_from_SP['total_transaction_amt'].astype(float)
data_from_SP_sales=data_from_SP.groupby(['location_id','transaction_date'])['total_transaction_amt'].sum().to_frame().reset_index()
data_from_SP_trans=data_from_SP.groupby(['location_id','transaction_date'])['total_transaction_amt'].count().to_frame().reset_index()
data_from_SP_sales=data_from_SP_sales.rename(columns={"total_transaction_amt":"rewards_sales"})
data_from_SP_trans=data_from_SP_trans.rename(columns={"total_transaction_amt":"rewards_trans"})

data_from_SP=pd.merge(data_from_SP_sales,data_from_SP_trans,on=['location_id','transaction_date'],how="left")
data_from_SP['transaction_date']=data_from_SP['transaction_date'].apply(lambda x: datetime.datetime.strptime(x,"%Y-%m-%d").date())
data_from_SP['week_end_dt']=data_from_SP['transaction_date'].apply(lambda x: add_week_end_dt(x))
data_from_SP_weekly=data_from_SP.groupby(['location_id','week_end_dt'])['rewards_sales','rewards_trans'].sum().reset_index()


# In[77]:

start_date_2018_BiWeekly=datetime.date(2018,8,25)

all_2018_received_dates=[start_date_2018_BiWeekly+datetime.timedelta(days=x*7) for x in range(16)]
max(all_2018_received_dates)
BiWeekly_2018_file_list=list()
for date_folder in all_2018_received_dates:
    folder_path="/home/jian/BigLots/2018_by_weeks/MediaStorm_"+str(date_folder)+"/"
    file_biweekly=[x for x in glob.glob(folder_path+"*.txt") if "SalesBiWeekly" in x]
    if len(file_biweekly)!=1:
        logging.info("Missing daily date in teh folder: "+str(date_folder))
    else:
        BiWeekly_2018_file_list.append(file_biweekly[0])


# In[78]:

Biweekly_aggregated=pd.DataFrame()
for file in BiWeekly_2018_file_list:
    df=pd.read_table(file,sep="|",dtype=str,nrows=Read_rows,usecols=['customer_id_hashed','location_id','transaction_dt','transaction_id','total_transaction_amt','merch_cat'])
    df=df[df['merch_cat']!="80040410"]
    df=df.drop_duplicates()
    df['total_transaction_amt']=df['total_transaction_amt'].astype(float)
    df_sales=df.groupby(['location_id','transaction_dt'])['total_transaction_amt'].sum().to_frame().reset_index()
    df_trans=df.groupby(['location_id','transaction_dt'])['total_transaction_amt'].count().to_frame().reset_index()
    df_sales=df_sales.rename(columns={"total_transaction_amt":"rewards_sales"})
    df_trans=df_trans.rename(columns={"total_transaction_amt":"rewards_trans"})

    df=pd.merge(df_sales,df_trans,on=['location_id','transaction_dt'],how="left")
    df['transaction_dt']=df['transaction_dt'].apply(lambda x: datetime.datetime.strptime(x,"%Y-%m-%d").date())
    df['week_end_dt']=df['transaction_dt'].apply(lambda x: add_week_end_dt(x))
    df_weekly=df.groupby(['location_id','week_end_dt'])['rewards_sales','rewards_trans'].sum().reset_index()
    Biweekly_aggregated=Biweekly_aggregated.append(df_weekly)
    
All_rewards=data_from_SP_weekly.append(Biweekly_aggregated)
All_rewards=All_rewards.rename(columns={"rewards_sales":"R_sales_BiWeeklyDate","rewards_trans":"R_trans_BiWeeklyDate",})


# # Load weekly sales

# In[79]:

Weekly_Long=pd.read_csv("/home/jian/BiglotsCode/outputs/combined_sales_long_2018-12-01.csv",dtype=str,nrows=Read_rows)
Weekly_Long=Weekly_Long[['location_id','week_end_date','sales','transactions']]
Weekly_Long['sales']=Weekly_Long['sales'].astype(float)
Weekly_Long['transactions']=Weekly_Long['transactions'].astype(float).astype(int)
Weekly_Long['week_end_date']=Weekly_Long['week_end_date'].apply(lambda x: datetime.datetime.strptime(x,"%Y-%m-%d").date())
Weekly_Long=Weekly_Long.rename(columns={'week_end_date':'week_end_dt',"sales":"sales_WeeklyData","transactions":"trans_WeeklyData"})

daily_data_by_week['location_id']=daily_data_by_week['location_id'].astype(str)
All_rewards['location_id']=All_rewards['location_id'].astype(str)
Weekly_Long['location_id']=Weekly_Long['location_id'].astype(str)


# In[84]:

output=pd.merge(daily_data_by_week,All_rewards,on=["location_id",'week_end_dt'],how="left")
output=pd.merge(output,Weekly_Long,on=["location_id",'week_end_dt'],how="left")
output=output[output['location_id']!="6990"]
output['TotalSales_Ratio_DailyToWeekly']=output['Total_sales_DailyData']/output['sales_WeeklyData']
output['R_Sales_Ratio_DailyToBiweekly']=output['R_sales_DailyData']/output['R_sales_BiWeeklyDate']
output['location_id']=output['location_id'].astype(int)
output=output.sort_values(['week_end_dt','location_id'],ascending=[True,True])

# In[88]:

output.to_csv("/home/jian/BigLots/2017_Daily_Sales_Weeks_Quarter_as_Transfer/BL_Compare_daily_data_to_previously_received_JL_"+str(datetime.datetime.now().date())+".csv",index=False)


# In[ ]:



