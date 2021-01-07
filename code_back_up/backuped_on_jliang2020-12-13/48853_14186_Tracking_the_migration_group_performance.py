
# coding: utf-8

# In[1]:

# Request from JT

import pandas as pd
import os
import datetime
import numpy as np
import gc
import logging
import glob
# Change the print content to all of strings for the log writing
samplerows=None
logging.basicConfig(filename='Tracking_Migration_Group_Performance.log', level=logging.INFO)
logging.info("Start: "+str(datetime.datetime.now()))


# In[2]:

def recursive_file_gen(root_path):
    for root, dirs, files in os.walk(root_path):
        for file in files:
            yield os.path.join(root,file)
            
last_sturday = (datetime.datetime.now()-datetime.timedelta(days=(datetime.datetime.now().weekday()+2))).date()
logging.info("last_sturday: "+str(last_sturday))



# In[3]:

last_day_of_2018Q4=datetime.date(2019,2,2)

year_of_quarter=(last_sturday-last_day_of_2018Q4).days/(52*7)
year_of_quarter=str(int(2019+np.floor(year_of_quarter)))
logging.info("Year: "+str(year_of_quarter))


quarter_of_quarter=(last_sturday-last_day_of_2018Q4).days/7
quarter_of_quarter=np.floor(quarter_of_quarter/13)%4
quarter_of_quarter=str(int(1+quarter_of_quarter))
logging.info("Quarter: "+str(quarter_of_quarter))

str_current_quarter=year_of_quarter+"_Q"+quarter_of_quarter

logging.info("Current_Quarter: "+str(str_current_quarter))

current_week=int((last_sturday-last_day_of_2018Q4).days/7%13)
logging.info("current_week: "+str(current_week))

if current_week==0:
    logging.info("Need to change the 0th week to 13th week")
    quarter_of_quarter=int(quarter_of_quarter)-1
    str_current_quarter=year_of_quarter+"_Q"+str(quarter_of_quarter)
    current_week=13
    logging.info("Quarter: "+str(quarter_of_quarter))
    logging.info("Current_Quarter: "+str(str_current_quarter))
    logging.info("current_week: "+str(current_week))
    
    
logging.info("Final Quarter Name: "+str(quarter_of_quarter))


# In[4]:

'''
Naming
e.g. current is 2019Q3, 7th week
Prior quarter: 2 quarter earlier, in example, 2019Q1
Last/Recent quarter: 1 quarter earlier, in example, 2019Q2
Current/Ongoing quarter: this quarter started but not end yet, in example, 2019Q3
'''



current_quarter_beginning=last_day_of_2018Q4+datetime.timedelta(days=(int(year_of_quarter)-2019)*52*7+                                                                (int(quarter_of_quarter)-1)*13*7+1)

logging.info("current_quarter_beginning: "+str(current_quarter_beginning))


recent_complete_quarter_End=current_quarter_beginning-datetime.timedelta(days=1)
recent_complete_quarter_Start=recent_complete_quarter_End-datetime.timedelta(days=13*7-1)

logging.info("recent_complete_quarter_End: "+str(recent_complete_quarter_End))
logging.info("recent_complete_quarter_Start: "+str(recent_complete_quarter_Start))

'''
prior_quarter_End=recent_complete_quarter_End-datetime.timedelta(days=13*7)
prior_quarter_Start=recent_complete_quarter_Start-datetime.timedelta(days=13*7)

logging.info("prior_quarter_End: "+str(prior_quarter_End))
logging.info("prior_quarter_Start: "+str(prior_quarter_Start))
'''


# In[5]:

recent_complete_quarter_Start


# In[6]:

# As the 1st week, re-run the migration group 
# Otherwise, use the group defination at the beginning of the quarter
force_run=0

'''
force_run: variable to force run the migration group by each single id 

1--run; -1 -- dont't run; 0 only consider the current week number

'''
if current_week==1 or force_run==1:
    
    lapsed_date_begin=recent_complete_quarter_Start-datetime.timedelta(days=1)-datetime.timedelta(days=(4*365+1+6)) # Prior quarter back 4years
    logging.info("lapsed_date_begin: "+str(lapsed_date_begin))

    logging.info("As the last week, re-caculate the migration group at the beginning of the ongoing quarter")
    lapsed=pd.read_table("/home/jian/Projects/Big_Lots/Loyal_members/loyalty_sales_data/lapsed20140826_20170226/MediaStormLapsedCustDtl.txt",
                     sep=",",nrows=samplerows,usecols=['customer_id_hashed','transaction_date'],dtype=str).drop_duplicates()
    lapsed=lapsed[lapsed['transaction_date']>=str(lapsed_date_begin)]
    
    chunksize_num = 10**7
    filename='/home/jian/Projects/Big_Lots/Live_Ramp/Quarterly_Update_2019Q1/crm_newscore_0922/combinedtransactions_0922.csv'
    dftrans_before_20180922=pd.DataFrame()
    count_i=0

    for chunk in pd.read_csv(filename, chunksize=chunksize_num,dtype=str,nrows=samplerows,
                             usecols=['customer_id_hashed','transaction_date']):
        chunk = chunk.drop_duplicates()
        chunk=chunk[chunk['transaction_date']>=str(lapsed_date_begin)]
        dftrans_before_20180922=dftrans_before_20180922.append(chunk)
        count_i+=1
        logging.info(str(count_i)+" read chunk "+str(datetime.datetime.now()))
    dftrans_before_20180922=dftrans_before_20180922.drop_duplicates()  
    
    all_rewards_most_recent=dftrans_before_20180922.append(lapsed)
    all_rewards_most_recent=all_rewards_most_recent.sort_values(["customer_id_hashed","transaction_date"],ascending=[True,False])
    all_rewards_most_recent=all_rewards_most_recent.drop_duplicates(['customer_id_hashed'])
    logging.info("all_rewards_most_recent.shape: "+str(all_rewards_most_recent.shape))

    del chunk
    del dftrans_before_20180922
    gc.collect()

    # Before the end of 2019Q1, up to 2019-02-09
    historical_daily_data_folder="/home/jian/BigLots/hist_daily_data_itemlevel_decompressed/"
    historical_daily_data_list=list(recursive_file_gen(historical_daily_data_folder))
    historical_daily_data_list=[x for x in historical_daily_data_list if (".txt" in x) & ("DailySales" in x)]
    historical_daily_df=pd.DataFrame({"file_path":historical_daily_data_list})
    historical_daily_df['week_end_dt']=historical_daily_df['file_path'].apply(lambda x: datetime.datetime.strptime(x.split(".")[0].split("MediaStormDailySalesHistory")[1],"%Y%m%d").date())
    historical_daily_df=historical_daily_df[(historical_daily_df['week_end_dt']<=datetime.date(2019,5,4)) & (historical_daily_df['week_end_dt']>datetime.date(2018,9,22))] # hard-code due to the 1st week of daily in item available
    historical_daily_df=historical_daily_df.sort_values("week_end_dt")
    logging.info("historical_daily_df.shape"+str(historical_daily_df.shape))
    
    
    # All daily files since 2019
    weekly_daily_df=list(recursive_file_gen("/home/jian/BigLots/"))
    weekly_daily_df=[x for x in weekly_daily_df if ("2018" not in x) & ("2017" not in x) & ("2016" not in x) & ("hist" not in x.lower())]
    weekly_daily_df=[x for x in weekly_daily_df if (".txt" in x) & ("aily" in x)]

    weekly_daily_df=pd.DataFrame({"file_path":weekly_daily_df})
    weekly_daily_df['week_end_dt']=weekly_daily_df['file_path'].apply(lambda x: datetime.datetime.strptime(x.split("/MediaStorm_")[1][:10],"%Y-%m-%d").date())
    weekly_daily_df=weekly_daily_df[weekly_daily_df['week_end_dt']>historical_daily_df['week_end_dt'].max()]
    logging.info("weekly_daily_df.shape: "+ str(weekly_daily_df.shape))

    weekly_daily_df_in_recent_Q=weekly_daily_df[(weekly_daily_df['week_end_dt']>=recent_complete_quarter_Start) & (weekly_daily_df['week_end_dt']<=recent_complete_quarter_End)]
    # For later use to read last quarter sales
    logging.info("weekly_daily_df_in_recent_Q.shape: "+ str(weekly_daily_df_in_recent_Q.shape))
    
    weekly_daily_df_upto_pri_quarter_end=weekly_daily_df[weekly_daily_df['week_end_dt']<recent_complete_quarter_Start]
    weekly_daily_df_upto_pri_quarter_end=weekly_daily_df_upto_pri_quarter_end.append(historical_daily_df).sort_values("week_end_dt")
    
    
    rew_df_after_20180929_before_pri_quarter=pd.DataFrame()
    rew_df_last_quarter_only=pd.DataFrame()

    for file in weekly_daily_df_upto_pri_quarter_end['file_path'].tolist():
        df=pd.read_table(file,sep="|",dtype=str,nrows=samplerows,
                         usecols=["customer_id_hashed","transaction_dt"]).drop_duplicates().rename(columns={"transaction_dt":"transaction_date"})
        df=df[~pd.isnull(df['customer_id_hashed'])]
        rew_df_after_20180929_before_pri_quarter=rew_df_after_20180929_before_pri_quarter.append(df)
        # logging.info(file,datetime.datetime.now())
        
    for file in weekly_daily_df_in_recent_Q['file_path'].tolist():
        df=pd.read_table(file,sep="|",dtype=str,nrows=samplerows,
                         usecols=["customer_id_hashed","transaction_dt"]).drop_duplicates().rename(columns={"transaction_dt":"transaction_date"})
        df=df[~pd.isnull(df['customer_id_hashed'])]
        rew_df_last_quarter_only=rew_df_last_quarter_only.append(df)
        # logging.info(file,datetime.datetime.now()) 
        
    all_rewards_most_recent=all_rewards_most_recent.append(rew_df_after_20180929_before_pri_quarter)
    del rew_df_after_20180929_before_pri_quarter
    all_rewards_most_recent=all_rewards_most_recent.sort_values(["customer_id_hashed","transaction_date"],ascending=[True,False])
    all_rewards_most_recent=all_rewards_most_recent.drop_duplicates(['customer_id_hashed'])
    all_rewards_most_recent['transaction_date_before_the_Quarter']=all_rewards_most_recent['transaction_date']

    all_rewards_most_recent_After=all_rewards_most_recent[['customer_id_hashed','transaction_date']].append(rew_df_last_quarter_only)
    del rew_df_last_quarter_only
    
    all_rewards_most_recent_After=all_rewards_most_recent_After.sort_values(["customer_id_hashed","transaction_date"],ascending=[True,False])
    all_rewards_most_recent_After=all_rewards_most_recent_After.drop_duplicates(['customer_id_hashed']).rename(columns={"transaction_date":"transaction_date_after_the_Quarter"})

    all_rewards_most_recent=pd.merge(all_rewards_most_recent,all_rewards_most_recent_After,on="customer_id_hashed",how="outer")
    del all_rewards_most_recent_After

    gc.collect()
    # Filled the na before with a later date 2019-12-31
    all_rewards_most_recent['transaction_date_before_the_Quarter']=all_rewards_most_recent['transaction_date_before_the_Quarter'].fillna("2099-12-31")
    # Change here to use unique dates and merge
    all_rewards_most_recent_date=all_rewards_most_recent[['transaction_date_before_the_Quarter']].drop_duplicates()
    all_rewards_most_recent_date['date']=pd.to_datetime(all_rewards_most_recent_date['transaction_date_before_the_Quarter'],format="%Y-%m-%d").dt.date
    all_rewards_most_recent_date['Days_to_pre_Quarter_End']=recent_complete_quarter_Start-datetime.timedelta(days=1)-all_rewards_most_recent_date['date']
    all_rewards_most_recent_date['Month_to_pre_Quarter_End']=all_rewards_most_recent_date['Days_to_pre_Quarter_End'].apply(lambda x: x.days/(365.25/12))
    all_rewards_most_recent_date['Group_before_the_Quarter']=np.where((all_rewards_most_recent_date['Month_to_pre_Quarter_End']>12) & (all_rewards_most_recent_date['Month_to_pre_Quarter_End']<=48),"Lapsed_13_48",
                                                           np.where(all_rewards_most_recent_date['Month_to_pre_Quarter_End']>48,"WD_48+",
                                                                    np.where((all_rewards_most_recent_date['Month_to_pre_Quarter_End']>=0) & (all_rewards_most_recent_date['Month_to_pre_Quarter_End']<=12),"Active",
                                                                            np.where(all_rewards_most_recent_date['Month_to_pre_Quarter_End']<0,"NotAvailable_Before_Quarter","NaN")
                                                                            )
                                                                   )
                                                           )
    
    all_rewards_most_recent=pd.merge(all_rewards_most_recent,all_rewards_most_recent_date,on="transaction_date_before_the_Quarter",how="left")
    del all_rewards_most_recent_date
    del all_rewards_most_recent['transaction_date_before_the_Quarter']
    all_rewards_most_recent=all_rewards_most_recent.rename(columns={"date":"transaction_date_before_the_Quarter"})
    
    
    #######
    all_rewards_most_recent_date=all_rewards_most_recent[['transaction_date_after_the_Quarter']].drop_duplicates()
    all_rewards_most_recent_date['date']=pd.to_datetime(all_rewards_most_recent_date['transaction_date_after_the_Quarter'],format="%Y-%m-%d").dt.date
    all_rewards_most_recent_date['Days_to_recentt_Quarter_End']=recent_complete_quarter_End-all_rewards_most_recent_date['date']
    all_rewards_most_recent_date['Month_to_recent_Quarter_End']=all_rewards_most_recent_date['Days_to_recentt_Quarter_End'].apply(lambda x: x.days/(365.25/12))
    all_rewards_most_recent_date['Group_after_the_Quarter']=np.where((all_rewards_most_recent_date['Month_to_recent_Quarter_End']>12) & (all_rewards_most_recent_date['Month_to_recent_Quarter_End']<=48),"Lapsed_13_48",
                                                           np.where(all_rewards_most_recent_date['Month_to_recent_Quarter_End']>48,"WD_48+",
                                                                    np.where((all_rewards_most_recent_date['Month_to_recent_Quarter_End']>=0) & (all_rewards_most_recent_date['Month_to_recent_Quarter_End']<=12),"Active",
                                                                            np.where(all_rewards_most_recent_date['Month_to_recent_Quarter_End']<0,"NotAvailable_Before_Q4","NaN")
                                                                            )
                                                                   )
                                                           )
    
    
    all_rewards_most_recent=pd.merge(all_rewards_most_recent,all_rewards_most_recent_date,on="transaction_date_after_the_Quarter",how="left")
    del all_rewards_most_recent_date
    del all_rewards_most_recent['transaction_date_after_the_Quarter']
    all_rewards_most_recent=all_rewards_most_recent.rename(columns={"date":"transaction_date_after_the_Quarter"})
    
    logging.info(str(all_rewards_most_recent['Group_before_the_Quarter'].unique().tolist()))
    logging.info(str(all_rewards_most_recent['Group_after_the_Quarter'].unique()))
    gc.collect()
    
    RecentQuarter_sales_by_id=pd.DataFrame()

    for file in weekly_daily_df_in_recent_Q['file_path'].tolist():
        df=pd.read_table(file,sep="|",dtype=str,nrows=samplerows,
                         usecols=['location_id','transaction_dt','transaction_id','customer_id_hashed','item_transaction_amt'])
        df=df[~pd.isnull(df['customer_id_hashed'])]
        df['item_transaction_amt']=df['item_transaction_amt'].astype(float)
        df_sales=df.groupby(['customer_id_hashed'])['item_transaction_amt'].sum().to_frame().reset_index().rename(columns={"item_transaction_amt":"sales_recent_Quarter"})

        df_trans=df[['location_id','transaction_dt','transaction_id','customer_id_hashed']].drop_duplicates()
        df_trans=df_trans.groupby(["customer_id_hashed"])['transaction_id'].count().to_frame().reset_index().rename(columns={"transaction_id":"trans_recent_Quarter"})

        df=pd.merge(df_sales,df_trans,on="customer_id_hashed",how="outer")

        RecentQuarter_sales_by_id=RecentQuarter_sales_by_id.append(df)
        # logging.info(file,datetime.datetime.now())
    RecentQuarter_sales_by_id=RecentQuarter_sales_by_id.groupby("customer_id_hashed")['sales_recent_Quarter','trans_recent_Quarter'].sum().reset_index()
    RecentQuarter_sales_by_id['Recent_Quarter_Shopping_Group']="Recent_Quarter_Shopped"
    
    
    all_rewards_most_recent=pd.merge(all_rewards_most_recent,RecentQuarter_sales_by_id,on="customer_id_hashed",how="outer")
    del RecentQuarter_sales_by_id
    gc.collect()
    # New sign ups in recent quarter
    Recent_Quarter_new_sign_ups_files=list(recursive_file_gen("/home/jian/BigLots/"))
    Recent_Quarter_new_sign_ups_files=[x for x in Recent_Quarter_new_sign_ups_files if ("Master" in x) & (".txt" in x) & ("2018" not in x) & ("2017" not in x) & ("2016" not in x)]
    Recent_Quarter_new_sign_ups_files_df=pd.DataFrame({"file_path":Recent_Quarter_new_sign_ups_files})
    Recent_Quarter_new_sign_ups_files_df['week_end_dt']=Recent_Quarter_new_sign_ups_files_df['file_path'].apply(lambda x: x.split("/MediaStorm_")[1][:10])
    Recent_Quarter_new_sign_ups_files_df=Recent_Quarter_new_sign_ups_files_df[Recent_Quarter_new_sign_ups_files_df['week_end_dt']<=str(recent_complete_quarter_End)]
    Recent_Quarter_new_sign_ups_files_df=Recent_Quarter_new_sign_ups_files_df[Recent_Quarter_new_sign_ups_files_df['week_end_dt']>=str(recent_complete_quarter_Start)]
    logging.info("Recent_Quarter_new_sign_ups_files_df.shape: " + str(Recent_Quarter_new_sign_ups_files_df.shape))

    # Only needed for the Q2
    df_new_sign_the_gap="/home/jian/BigLots/New_Sing_Ups_2018_Fiscal_Year/MediaStorm Rewards Master P4 2019 - no transaction info.zip"
    df_new_sign_the_gap=pd.read_table(df_new_sign_the_gap,sep="|",usecols=['customer_id_hashed','sign_up_date'])
    
    recent_new_sign_ups=df_new_sign_the_gap.copy()
    for file in Recent_Quarter_new_sign_ups_files_df['file_path'].unique().tolist():
        df=pd.read_table(file,sep="|",usecols=['customer_id_hashed','sign_up_date'])
        recent_new_sign_ups=recent_new_sign_ups.append(df)
    recent_new_sign_ups=recent_new_sign_ups[(recent_new_sign_ups['sign_up_date']<=str(recent_complete_quarter_End))  & (recent_new_sign_ups['sign_up_date']>=str(recent_complete_quarter_Start))]
    recent_new_sign_ups=recent_new_sign_ups.sort_values(['customer_id_hashed','sign_up_date']).drop_duplicates("customer_id_hashed")
    recent_new_sign_ups['NewRewards_RecentQuarter']="Recent_Quarter_New_Sign_Ups"
    logging.info("recent_new_sign_ups.shape: "+str(recent_new_sign_ups.shape))
    del recent_new_sign_ups['sign_up_date']
    
    all_rewards_most_recent=pd.merge(all_rewards_most_recent,recent_new_sign_ups,on="customer_id_hashed",how="outer")
    all_rewards_most_recent['NewRewards_RecentQuarter']=all_rewards_most_recent['NewRewards_RecentQuarter'].fillna("Old_Rewards_Members")
    del recent_new_sign_ups
    gc.collect()
    
    
    
    all_rewards_most_recent['sales_recent_Quarter']=all_rewards_most_recent['sales_recent_Quarter'].fillna(0)
    all_rewards_most_recent['trans_recent_Quarter']=all_rewards_most_recent['trans_recent_Quarter'].fillna(0)

    all_rewards_most_recent['Recent_Quarter_Shopping_Group']=all_rewards_most_recent['Recent_Quarter_Shopping_Group'].fillna("Recent_Quarter_No_Shop")
    all_rewards_most_recent['Group_before_the_Quarter']=all_rewards_most_recent['Group_before_the_Quarter'].fillna("nan")
    all_rewards_most_recent['Group_after_the_Quarter']=all_rewards_most_recent['Group_after_the_Quarter'].fillna("nan")

    output_id_recentquarter_count=all_rewards_most_recent.groupby(['Group_before_the_Quarter','Group_after_the_Quarter','NewRewards_RecentQuarter','Recent_Quarter_Shopping_Group'])['customer_id_hashed'].count().to_frame().reset_index().rename(columns={"customer_id_hashed":"id_count"})
    output_id_recentquarter_sales=all_rewards_most_recent.groupby(['Group_before_the_Quarter','Group_after_the_Quarter','NewRewards_RecentQuarter','Recent_Quarter_Shopping_Group'])['sales_recent_Quarter','trans_recent_Quarter'].sum().reset_index()

    output_recentquarter=pd.merge(output_id_recentquarter_count,output_id_recentquarter_sales,on=['Group_before_the_Quarter','Group_after_the_Quarter','NewRewards_RecentQuarter','Recent_Quarter_Shopping_Group'],how="outer")
    all_rewards_most_recent.to_csv('/home/jian/celery/Migration_Performance/quarterly_report/migration_group_for_recent_quarter_before_'+                               year_of_quarter+"Q"+str(quarter_of_quarter)+".csv",index=False)
    output_recentquarter.to_csv('/home/jian/celery/Migration_Performance/quarterly_report/summary/summary_migration_group_for_recent_quarter_before_'+                                   year_of_quarter+"Q"+str(quarter_of_quarter)+".csv",index=False)


# In[7]:

all_rewards_most_recent=glob.glob("/home/jian/celery/Migration_Performance/quarterly_report/*.csv")
all_rewards_most_recent=[x for x in all_rewards_most_recent if year_of_quarter+"Q"+str(quarter_of_quarter) in x]
if len(all_rewards_most_recent)!=1:
    logging.info("Error: multiple files about id by group at the begining of the quarter are saved, please check")
else:
    df_all_rewards_most_recent=pd.read_csv(all_rewards_most_recent[0],dtype=str)
    df_all_rewards_most_recent['sales_recent_Quarter']=df_all_rewards_most_recent['sales_recent_Quarter'].astype(float)
    df_all_rewards_most_recent['trans_recent_Quarter']=df_all_rewards_most_recent['trans_recent_Quarter'].astype(float).astype(int)
if 'sign_up_date' in df_all_rewards_most_recent.columns.tolist():
    del df_all_rewards_most_recent['sign_up_date']
    gc.collect()


# In[8]:

# Read this onging quarter performance
weekly_daily_df=list(recursive_file_gen("/home/jian/BigLots/"))
weekly_daily_df=[x for x in weekly_daily_df if ("2018" not in x) & ("2017" not in x) & ("2016" not in x) & ("hist" not in x.lower())]
weekly_daily_df=[x for x in weekly_daily_df if (".txt" in x) & ("aily" in x)]

weekly_daily_df=pd.DataFrame({"file_path":weekly_daily_df})
weekly_daily_df['week_end_dt']=weekly_daily_df['file_path'].apply(lambda x: datetime.datetime.strptime(x.split("/MediaStorm_")[1][:10],"%Y-%m-%d").date())
weekly_daily_df=weekly_daily_df[weekly_daily_df['week_end_dt']>current_quarter_beginning]


if weekly_daily_df.shape[0]!=current_week:
    logging.info("Error: ongoing quarter daily file count doesn't match")
else:
    current_quarter_file_list=weekly_daily_df['file_path'].tolist()
    current_quarter_sales=pd.DataFrame()
    for file in current_quarter_file_list:
        df=pd.read_table(file,sep="|",dtype=str,nrows=samplerows,
                         usecols=['location_id','transaction_dt','transaction_id','customer_id_hashed','item_transaction_amt'])
        df=df[~pd.isnull(df['customer_id_hashed'])]
        df['item_transaction_amt']=df['item_transaction_amt'].astype(float)
        df_sales=df.groupby(['customer_id_hashed'])['item_transaction_amt'].sum().to_frame().reset_index().rename(columns={"item_transaction_amt":"sales_ongoing_Quarter"})

        df_trans=df[['location_id','transaction_dt','transaction_id','customer_id_hashed']].drop_duplicates()
        df_trans=df_trans.groupby(["customer_id_hashed"])['transaction_id'].count().to_frame().reset_index().rename(columns={"transaction_id":"trans_ongoing_Quarter"})

        df=pd.merge(df_sales,df_trans,on="customer_id_hashed",how="outer")

        current_quarter_sales=current_quarter_sales.append(df)
    del df
    current_quarter_sales=current_quarter_sales.groupby('customer_id_hashed')['sales_ongoing_Quarter','trans_ongoing_Quarter'].sum().reset_index()
    current_quarter_sales['Ongoing_Quarter_Shopping_Group']="Ongoing_Quarter_Shopped"

    Ongoing_Quarter_new_sign_ups_files=list(recursive_file_gen("/home/jian/BigLots/"))
    Ongoing_Quarter_new_sign_ups_files=[x for x in Ongoing_Quarter_new_sign_ups_files if ("Master" in x) & (".txt" in x) & ("2018" not in x) & ("2017" not in x) & ("2016" not in x)]
    Ongoing_Quarter_new_sign_ups_files_df=pd.DataFrame({"file_path":Ongoing_Quarter_new_sign_ups_files})
    Ongoing_Quarter_new_sign_ups_files_df['week_end_dt']=Ongoing_Quarter_new_sign_ups_files_df['file_path'].apply(lambda x: x.split("/MediaStorm_")[1][:10])
    Ongoing_Quarter_new_sign_ups_files_df=Ongoing_Quarter_new_sign_ups_files_df[Ongoing_Quarter_new_sign_ups_files_df['week_end_dt']>=str(current_quarter_beginning)]
    logging.info("Ongoing_Quarter_new_sign_ups_files_df.shape: " + str(Ongoing_Quarter_new_sign_ups_files_df.shape))


    ongoing_new_sign_ups=pd.DataFrame()
    for file in Ongoing_Quarter_new_sign_ups_files_df['file_path'].unique().tolist():
        df=pd.read_table(file,sep="|",usecols=['customer_id_hashed','sign_up_date'])
        ongoing_new_sign_ups=ongoing_new_sign_ups.append(df)
    ongoing_new_sign_ups=ongoing_new_sign_ups[ongoing_new_sign_ups['sign_up_date']>=str(current_quarter_beginning)]
    ongoing_new_sign_ups=ongoing_new_sign_ups.sort_values(['customer_id_hashed','sign_up_date']).drop_duplicates("customer_id_hashed")
    ongoing_new_sign_ups['NewRewards_OngoingQuarter']="Ongoing_Quarter_New_Sign_Ups"
    del ongoing_new_sign_ups['sign_up_date']
    logging.info("Ongoing_new_sign_ups.shape: "+str(ongoing_new_sign_ups.shape))

    df_all_rewards_most_recent=pd.merge(df_all_rewards_most_recent,current_quarter_sales,on="customer_id_hashed",how="outer")
    del current_quarter_sales
    df_all_rewards_most_recent['Ongoing_Quarter_Shopping_Group']=df_all_rewards_most_recent['Ongoing_Quarter_Shopping_Group'].fillna("Ongoing_Quarter_No_Shop")
    gc.collect()
    
    df_all_rewards_most_recent=pd.merge(df_all_rewards_most_recent,ongoing_new_sign_ups,on="customer_id_hashed",how="outer")
    del ongoing_new_sign_ups
    df_all_rewards_most_recent['NewRewards_OngoingQuarter']=df_all_rewards_most_recent['NewRewards_OngoingQuarter'].fillna("Old_Rewards_Members")
    
    df_all_rewards_most_recent['sales_recent_Quarter']=df_all_rewards_most_recent['sales_recent_Quarter'].fillna(0)
    df_all_rewards_most_recent['trans_recent_Quarter']=df_all_rewards_most_recent['trans_recent_Quarter'].fillna(0)
    df_all_rewards_most_recent['sales_ongoing_Quarter']=df_all_rewards_most_recent['sales_ongoing_Quarter'].fillna(0)
    df_all_rewards_most_recent['trans_ongoing_Quarter']=df_all_rewards_most_recent['trans_ongoing_Quarter'].fillna(0)
    


# In[9]:

deminsion_cols=['Group_before_the_Quarter','Group_after_the_Quarter','NewRewards_RecentQuarter','Recent_Quarter_Shopping_Group','NewRewards_OngoingQuarter','Ongoing_Quarter_Shopping_Group']
for col in deminsion_cols:
    df_all_rewards_most_recent[col]=df_all_rewards_most_recent[col].fillna('nan')
    logging.info(str(datetime.datetime.now())+col)
    gc.collect()


# In[10]:

output_id_count_everygroup=df_all_rewards_most_recent.groupby(deminsion_cols)['customer_id_hashed'].count().to_frame().reset_index().rename(columns={"customer_id_hashed":"id_count"})
output_id_sales_everygroup=df_all_rewards_most_recent.groupby(deminsion_cols)['sales_recent_Quarter','trans_recent_Quarter','sales_ongoing_Quarter','trans_ongoing_Quarter'].sum().reset_index()

output_everygroup=pd.merge(output_id_count_everygroup,output_id_sales_everygroup,on=deminsion_cols,how="outer")


output_everygroup['Recent_Quarteer_Sale_per_ID']=output_everygroup['sales_recent_Quarter']/output_everygroup['id_count']
output_everygroup['Recent_Quarteer_Sale_per_ID']=output_everygroup['Recent_Quarteer_Sale_per_ID'].apply(lambda x: np.round(x,2))
output_everygroup['Recent_Quarteer_Trans_per_ID']=output_everygroup['trans_recent_Quarter']/output_everygroup['id_count']
output_everygroup['Recent_Quarteer_Trans_per_ID']=output_everygroup['Recent_Quarteer_Trans_per_ID'].apply(lambda x: np.round(x,2))

output_everygroup['ongoing_Quarteer_Sale_per_ID']=output_everygroup['sales_ongoing_Quarter']/output_everygroup['id_count']
output_everygroup['ongoing_Quarteer_Sale_per_ID']=output_everygroup['ongoing_Quarteer_Sale_per_ID'].apply(lambda x: np.round(x,2))
output_everygroup['ongoing_Quarteer_Trans_per_ID']=output_everygroup['trans_ongoing_Quarter']/output_everygroup['id_count']
output_everygroup['ongoing_Quarteer_Trans_per_ID']=output_everygroup['ongoing_Quarteer_Trans_per_ID'].apply(lambda x: np.round(x,2))


# In[14]:

output_everygroup['before_ongoing_quarter_label']=np.where(output_everygroup['Group_after_the_Quarter']=="Active","Active",
                                                          np.where(output_everygroup['Group_after_the_Quarter']=="Lapsed_13_48","Lapsed",
                                                                  np.where(((output_everygroup['Group_after_the_Quarter']=="nan") &\
                                                                            (output_everygroup['NewRewards_RecentQuarter']=="Recent_Quarter_New_Sign_Ups") &\
                                                                            (output_everygroup['Recent_Quarter_Shopping_Group']=="Recent_Quarter_No_Shop"))," Sign Up No Purchase (Previous Quarter)","nan")
                                                                  )
                                                          )
count_pre=output_everygroup.groupby("before_ongoing_quarter_label")['id_count'].sum().to_frame().reset_index()
count_pre=count_pre[count_pre['before_ongoing_quarter_label']!="nan"]

output_everygroup['in_quarter_label']=np.where(((output_everygroup['Group_after_the_Quarter']=="Active") &                                                (output_everygroup['Ongoing_Quarter_Shopping_Group']=="Ongoing_Quarter_Shopped")),"Active Shopper",
                                               np.where(((output_everygroup['Group_after_the_Quarter']=="Lapsed_13_48") &\
                                                         (output_everygroup['Ongoing_Quarter_Shopping_Group']=="Ongoing_Quarter_Shopped")),"Activated Lapsed",
                                                        np.where(((output_everygroup['NewRewards_RecentQuarter']=="Recent_Quarter_New_Sign_Ups") &\
                                                                 (output_everygroup['Recent_Quarter_Shopping_Group']=="Recent_Quarter_No_Shop") &\
                                                                 (output_everygroup['Ongoing_Quarter_Shopping_Group']=="Ongoing_Quarter_Shopped") &\
                                                                 (output_everygroup['Group_before_the_Quarter']=="nan") &\
                                                                 (output_everygroup['Group_after_the_Quarter']=="nan")),"Activated Recent Sign Up No Purchase",
                                                                 np.where(((output_everygroup['Group_after_the_Quarter'].isin(['WD_48+','nan'])) &\
                                                                            (output_everygroup['Ongoing_Quarter_Shopping_Group']=="Ongoing_Quarter_Shopped") &\
                                                                            (output_everygroup['NewRewards_OngoingQuarter']=="Old_Rewards_Members") &\
                                                                            (output_everygroup['NewRewards_RecentQuarter']!="Recent_Quarter_New_Sign_Ups")),"Resurrected Lapsed",
                                                                           np.where(((output_everygroup['NewRewards_OngoingQuarter']=="Ongoing_Quarter_New_Sign_Ups") &\
                                                                                     (output_everygroup['Ongoing_Quarter_Shopping_Group']=="Ongoing_Quarter_Shopped") &\
                                                                                     (output_everygroup['Group_after_the_Quarter']=="nan")),"New Rewards Purchaser",
                                                                                    "nan")
                                                                          )
                                                                )
                                                       )
                                              )
count_in_ongoing=output_everygroup.groupby("in_quarter_label")['id_count'].sum().to_frame().reset_index()
count_in_ongoing=count_in_ongoing[count_in_ongoing['in_quarter_label']!="nan"]


# In[15]:

count_in_ongoing


# In[16]:

writer_my=pd.ExcelWriter("/home/jian/celery/Migration_Performance/weekly_report/BL_tracking_migration_status_JL_"+str(datetime.datetime.now().date())+".xlsx",engine="xlsxwriter")
output_everygroup.to_excel(writer_my,"pivot",index=False)
count_pre.to_excel(writer_my,"count_pre",index=False)
count_in_ongoing.to_excel(writer_my,"count_in_ongoing",index=False)
writer_my.save()



# In[17]:

writer_Simeng=pd.ExcelWriter("/home/simeng/outputs_"+str(last_sturday)+"/BL_tracking_migration_status_JL_"+str(last_sturday)+".xlsx",engine="xlsxwriter")
output_everygroup.to_excel(writer_Simeng,"pivot",index=False)
count_pre.to_excel(writer_Simeng,"count_pre",index=False)
count_in_ongoing.to_excel(writer_Simeng,"count_in_ongoing",index=False)
writer_Simeng.save()

