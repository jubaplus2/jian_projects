#!/usr/bin/env python
# coding: utf-8

# In[1]:


######### Upload to LiveRamp-Bing
import pandas as pd
import datetime
import os
import numpy as np
import hashlib
import gc
import logging

logging.basicConfig(filename="/home/jian/BL_weekly_crontab/cron_2_Google_Bing_LR/log_BL_Weekly_Google_and_Bing_LR_uploading.log", level=logging.INFO)
logging.info("Started: "+str(datetime.datetime.now()))
def recursive_file_gen(my_root_dir):
    for root, dirs, files in os.walk(my_root_dir):
        for file in files:
            yield os.path.join(root, file)


# In[2]:


thismonday=datetime.datetime.now().date()-datetime.timedelta(days=datetime.datetime.now().date().weekday())
# thismonday=datetime.date(2019,3,25)
print("thismonday", thismonday)

last_week_end_saturday=thismonday-datetime.timedelta(days=2)

writer_pather="/home/jian/celery/Bing_LiveRamp/output/"

posibble_recent_folder="/home/jian/BigLots/MediaStorm_"+str(last_week_end_saturday)+"/"
daily_files_recent=[x for x in list(recursive_file_gen(posibble_recent_folder)) if "Daily" in x]

list_1_after_201806_2019=[x for x in list(recursive_file_gen("/home/jian/BigLots/2019_by_weeks/")) if ("aily" in x) & (".txt" in x) ]
list_1_after_201806_2019=[x for x in list_1_after_201806_2019 if str(last_week_end_saturday) in x]


daily_files_last_week=daily_files_recent+list_1_after_201806_2019
if len(daily_files_last_week)==1:
    daily_file_last_week=daily_files_last_week[0]
    print("Good to load")
else:
    daily_file_last_week=np.nan
    print("Last week daily data not avaiable")



qc_weekly=pd.read_table(daily_file_last_week,sep="|",dtype=str)
qc_weekly=qc_weekly[qc_weekly['location_id']!="6990"]
qc_weekly['item_transaction_amt']=qc_weekly['item_transaction_amt'].astype(float)
qc_weekly_sales=qc_weekly.groupby(['location_id'])['item_transaction_amt'].sum().to_frame().reset_index().rename(columns={"item_transaction_amt":"sales_from_Daily"})
qc_weekly_trans=qc_weekly[['location_id','transaction_dt','transaction_id','customer_id_hashed']].drop_duplicates()
qc_weekly_trans=qc_weekly_trans.groupby(['location_id'])['transaction_id'].count().to_frame().reset_index().rename(columns={"transaction_id":"trans_from_Daily"})

qc_weekly_from_daily=pd.merge(qc_weekly_sales,qc_weekly_trans,on="location_id",how="outer")
qc_weekly_from_daily.shape


import glob
weekly_data_path=glob.glob("/home/jian/BigLots/2019_by_weeks/MediaStorm_"+str(last_week_end_saturday)+"/*")
weekly_data_path=[x for x in weekly_data_path if "SalesWeekly" in x]
if len(weekly_data_path)==0:    
    weekly_data_path=glob.glob("/home/jian/BigLots/MediaStorm_"+str(last_week_end_saturday)+"/*")
    weekly_data_path=[x for x in weekly_data_path if "SalesWeekly" in x]

if len(weekly_data_path)==1:
    weekly_data_path=weekly_data_path[0]

else:
    print("Checking the new weekly data")


# In[3]:


thismonday=datetime.datetime.now().date()-datetime.timedelta(days=datetime.datetime.now().date().weekday())
# thismonday=datetime.date(2019,3,25)
print("thismonday", thismonday)

last_week_end_saturday=thismonday-datetime.timedelta(days=2)

writer_pather="/home/jian/celery/Bing_LiveRamp/output/"

posibble_recent_folder="/home/jian/BigLots/MediaStorm_"+str(last_week_end_saturday)+"/"
daily_files_recent=[x for x in list(recursive_file_gen(posibble_recent_folder)) if "Daily" in x]

list_1_after_201806_2019=[x for x in list(recursive_file_gen("/home/jian/BigLots/2019_by_weeks/")) if ("aily" in x) & (".txt" in x) ]
list_1_after_201806_2019=[x for x in list_1_after_201806_2019 if str(last_week_end_saturday) in x]


daily_files_last_week=daily_files_recent+list_1_after_201806_2019
if len(daily_files_last_week)==1:
    daily_file_last_week=daily_files_last_week[0]
    print("Good to load")
else:
    daily_file_last_week=np.nan
    print("Last week daily data not avaiable")



qc_weekly=pd.read_table(daily_file_last_week,sep="|",dtype=str)
qc_weekly=qc_weekly[qc_weekly['location_id']!="6990"]
qc_weekly['item_transaction_amt']=qc_weekly['item_transaction_amt'].astype(float)
qc_weekly_sales=qc_weekly.groupby(['location_id'])['item_transaction_amt'].sum().to_frame().reset_index().rename(columns={"item_transaction_amt":"sales_from_Daily"})
qc_weekly_trans=qc_weekly[['location_id','transaction_dt','transaction_id','customer_id_hashed']].drop_duplicates()
qc_weekly_trans=qc_weekly_trans.groupby(['location_id'])['transaction_id'].count().to_frame().reset_index().rename(columns={"transaction_id":"trans_from_Daily"})

qc_weekly_from_daily=pd.merge(qc_weekly_sales,qc_weekly_trans,on="location_id",how="outer")
qc_weekly_from_daily.shape


import glob
weekly_data_path=glob.glob("/home/jian/BigLots/2019_by_weeks/MediaStorm_"+str(last_week_end_saturday)+"/*")
weekly_data_path=[x for x in weekly_data_path if "SalesWeekly" in x]
if len(weekly_data_path)==0:    
    weekly_data_path=glob.glob("/home/jian/BigLots/MediaStorm_"+str(last_week_end_saturday)+"/*")
    weekly_data_path=[x for x in weekly_data_path if "SalesWeekly" in x]

if len(weekly_data_path)==1:
    weekly_data_path=weekly_data_path[0]

else:
    print("Checking the new weekly data")


Weekly_Data=pd.read_table(weekly_data_path,dtype=str,sep="|",usecols=["location_id",'week_end_dt','gross_sales_amt','gross_transaction_cnt'])
Weekly_Data=Weekly_Data[Weekly_Data['location_id']!="6990"]
Weekly_Data=Weekly_Data.drop_duplicates()
Weekly_Data['gross_sales_amt']=Weekly_Data['gross_sales_amt'].astype(float)
Weekly_Data['gross_transaction_cnt']=Weekly_Data['gross_transaction_cnt'].astype(int)


QC_df=pd.merge(Weekly_Data,qc_weekly_from_daily,on="location_id",how="outer")


QC_df['Sales_Diff']=(QC_df['gross_sales_amt']-QC_df['sales_from_Daily'])/QC_df['sales_from_Daily']
QC_df['Trans_Diff']=(QC_df['gross_transaction_cnt']-QC_df['trans_from_Daily'])/QC_df['gross_transaction_cnt']

print("1% store sales variances: "+str(QC_df[(QC_df['Sales_Diff'].apply(lambda x: np.abs(x)>0.01))].shape[0]))
print("4% store trans variances: "+str(QC_df[(QC_df['Trans_Diff'].apply(lambda x: np.abs(x)>0.04))].shape[0]))


# In[4]:


sales_daily_lastweek=pd.read_table(daily_file_last_week,sep="|",dtype=str,usecols=['location_id','customer_id_hashed','transaction_dt','item_transaction_amt'])
sales_daily_lastweek=sales_daily_lastweek[~pd.isnull(sales_daily_lastweek['customer_id_hashed'])]
sales_daily_lastweek=sales_daily_lastweek[sales_daily_lastweek['location_id']!="6990"]
sales_daily_lastweek['item_transaction_amt']=sales_daily_lastweek['item_transaction_amt'].astype(float)
sales_daily_lastweek_agg=sales_daily_lastweek.groupby(['customer_id_hashed','transaction_dt'])['item_transaction_amt'].sum().to_frame().reset_index()
sales_daily_lastweek_agg=sales_daily_lastweek_agg.rename(columns={"transaction_dt":"Timestamp","item_transaction_amt":"Conversion_Amount"})
sales_daily_lastweek_agg['Timestamp']=sales_daily_lastweek_agg['Timestamp'].apply(lambda x: datetime.datetime.strptime(x,"%Y-%m-%d").date())


posibble_recent_Master="/home/jian/BigLots/MediaStorm_"+str(last_week_end_saturday)+"/"
master_files_recent=[x for x in list(recursive_file_gen(posibble_recent_Master)) if "ster" in x]

if len(master_files_recent)==1:
    master_files_recent=master_files_recent[0]
    print("Good to load")
else:
    master_files_recent=np.nan
    print("Last week Master file not avaiable, Check the cell below if already in")

recent_date=last_week_end_saturday

Master_2019_weekly=[x for x in list(recursive_file_gen("/home/jian/BigLots/2019_by_weeks/")) if ("aster" in x) & (".txt" in x) ]


weekly_df=pd.DataFrame({"file_path":Master_2019_weekly})
weekly_df['date']=weekly_df['file_path'].apply(lambda x: x.split("_by_weeks/MediaStorm_")[1][:10])

weekly_df['date']=weekly_df['date'].apply(lambda x: datetime.datetime.strptime(x,"%Y-%m-%d").date())
weekly_df=weekly_df[weekly_df['date']>=datetime.date(2019,6,8)]

weekly_df=weekly_df.sort_values("date",ascending=False).reset_index()
del weekly_df['index']

if pd.notnull(master_files_recent):
    weekly_df=pd.DataFrame({"file_path":master_files_recent,"date":recent_date},index=[0]).append(weekly_df)
weekly_df=weekly_df.drop_duplicates().reset_index()
del weekly_df['index'] 


# In[5]:


data_0=pd.read_table("/home/jian/Projects/Big_Lots/Loyal_members/loyalty_register_data/MediaStorm_Lapsed_Reward_Member_Master_from2014-08-26to2017-02-26.zip",
    dtype=str,sep="|",usecols=['customer_id_hashed','email_address_hash','customer_zip_code']).drop_duplicates()
data_1=pd.read_csv("/home/jian/Projects/Big_Lots/Loyal_members/loyalty_register_data/MediaStormCustTot-hashed-email.txt",
    dtype=str,header=None,usecols=[0,1,5])
data_1.columns=['customer_id_hashed','email_address_hash','customer_zip_code']
data_1['customer_id_hashed']=data_1['customer_id_hashed'].apply(lambda x: hashlib.sha256(x.encode('utf-8')).hexdigest())
data_2 = pd.read_csv('/home/jian/Projects/Big_Lots/Loyal_members/loyalty_register_data/'+'MediaStormCustomerTransactionTotals_2018-01-09_2018-03-31.txt',
    sep = ',',dtype = str,usecols=['customer_id_hashed','email_address_hash','customer_zip_code'])
print(data_2.shape)
data_3 = pd.read_csv('/home/jian/Projects/Big_Lots/Loyal_members/loyalty_register_data/'+'Existing Reward Member Master as of 2018-06-05.txt',
    dtype = str,sep = '|',usecols=['customer_id_hashed','email_address_hash','customer_zip_code'])
print(data_3.shape)
data_4 = pd.read_csv('/home/jian/Projects/Big_Lots/Loyal_members/loyalty_register_data/'+'New Reward Member Master as of 2018-06-05.txt',
    dtype = str,sep = '|',usecols=['customer_id_hashed','email_address_hash','customer_zip_code'])
print(data_4.shape)

data_5 = pd.read_csv('/home/jian/BigLots/New_Sing_Ups_2018_Fiscal_Year/All Rewards Members 2018-02-04 - 2019-05-04.zip',
    dtype = str,sep = '|',compression="zip",usecols=['customer_id_hashed','email_address_hash','customer_zip_code'])
print(data_5.shape)

data_6 = pd.read_csv('/home/jian/BigLots/New_Sing_Ups_2018_Fiscal_Year/MediaStorm Rewards Master P4 2019 - no transaction info.zip',
    dtype = str,sep = '|',usecols=['customer_id_hashed','email_address_hash','customer_zip_code'])
print(data_6.shape)

master_old=data_6.append(data_5).append(data_4).append(data_3).append(data_2).append(data_1).append(data_0).drop_duplicates("customer_id_hashed")
del data_6
del data_5
del data_4
del data_3
del data_2
del data_1
del data_0
gc.collect()

all_weekly_biweekly_master_file=pd.DataFrame()
for file_path in weekly_df['file_path'].tolist():
    df=pd.read_table(file_path,dtype=str,usecols=['customer_id_hashed','email_address_hash','customer_zip_code'],sep="|")
    all_weekly_biweekly_master_file=all_weekly_biweekly_master_file.append(df)

all_weekly_biweekly_master_file=all_weekly_biweekly_master_file.append(master_old)
del master_old
gc.collect()

print(all_weekly_biweekly_master_file.shape)
print(len(all_weekly_biweekly_master_file['customer_id_hashed'].unique()))

all_weekly_biweekly_master_file=all_weekly_biweekly_master_file.drop_duplicates('customer_id_hashed')

sales_daily_lastweek_agg=pd.merge(sales_daily_lastweek_agg,all_weekly_biweekly_master_file,on="customer_id_hashed",how="left").rename(columns={"email_address_hash":"Email_1","customer_zip_code":"Zip"})
sales_daily_lastweek_agg.head(2)


print("Null Email rows excluded: "+str(sales_daily_lastweek_agg[pd.isnull(sales_daily_lastweek_agg['Email_1'])].shape[0]))
print(sales_daily_lastweek_agg.shape[0]/sales_daily_lastweek_agg[pd.isnull(sales_daily_lastweek_agg['Email_1'])].shape[0])

sales_daily_lastweek_agg=sales_daily_lastweek_agg[~pd.isnull(sales_daily_lastweek_agg['Email_1'])]
del sales_daily_lastweek_agg['customer_id_hashed']

sales_daily_lastweek_agg=sales_daily_lastweek_agg[["Email_1","Zip","Timestamp", "Conversion_Amount"]]
sales_daily_lastweek_agg['Conversion_Amount']=sales_daily_lastweek_agg['Conversion_Amount'].apply(lambda x: np.round(x,2)).astype(str)
sales_daily_lastweek_agg['Conversion_Amount']=sales_daily_lastweek_agg['Conversion_Amount'].apply(lambda x: x.split(".")[0]+"."+x.split(".")[1].ljust(2,"0"))
sales_daily_lastweek_agg['Product_Group']="In_Store"

sales_daily_lastweek_agg['Zip']="00000"

data_max_date=sales_daily_lastweek_agg['Timestamp'].max()
data_max_date

data_min_date=sales_daily_lastweek_agg['Timestamp'].min()
data_min_date

local_path=writer_pather+"/BL_LR_BingStoreSales_"+str(data_min_date)+"_"+str(data_max_date)+"_JL_"+str(datetime.datetime.now().date())+".txt"

sales_daily_lastweek_agg.to_csv(local_path,index=False,sep="|")


# In[6]:


import paramiko

host = "files.liveramp.com" #hard-coded
port = 22
transport = paramiko.Transport((host, port))

password = "Jubaplus2019!" #hard-coded
username = "bing-big-lots" #hard-coded
transport.connect(username = username, password = password)
sftp = paramiko.SFTPClient.from_transport(transport)

# local_path defined above before saving the local txt
remote_path="/uploads/"+os.path.basename(local_path)
sftp.put(local_path,remote_path)
sftp.close()
transport.close()

logging.info("Done of Bing: "+str(datetime.datetime.now()))





# Google below from the output of Bing
df_google=sales_daily_lastweek_agg.rename(columns={"Zip":"Zip_Code",
  "Timestamp":"transaction_timestamp",
  "Product_Group":"transaction_category",
  "Conversion_Amount":"transaction_amount"})

df_google=df_google[['Zip_Code','Email_1','transaction_category','transaction_timestamp','transaction_amount']]

local_path_google="/home/jian/celery/Google_LiveRamp/output/BL_LR_GoogleStoreSales_"+str(data_min_date)+"_"+str(data_max_date)+"_JL_"+str(datetime.datetime.now().date())+".txt"
df_google.to_csv(local_path_google,index=False,sep="|")

import paramiko

host = "files.liveramp.com" #hard-coded
port = 22
transport = paramiko.Transport((host, port))

password = "Jubaplus2019!" #hard-coded
username = "big-lots-ga-aw" #hard-coded
transport.connect(username = username, password = password)
sftp = paramiko.SFTPClient.from_transport(transport)

remote_path="/uploads/"+os.path.basename(local_path_google)
sftp.put(local_path_google,remote_path)
sftp.close()
transport.close()
logging.info("Done of Google: "+str(datetime.datetime.now()))


import smtplib
message = """From: Juba <jubapluscc@gmail.com>
To: Jian <jian@jubaplus.com>, Mike Mahler <mmahler@mediastorm.biz>, Maggie Chiu <mchiu@mediastorm.biz>, Naja Aldefri <naldefri@mediastorm.biz>, Daniela Balboni <dbalboni@mediastorm.biz>, Zhenya Brisker <zbrisker@mediastorm.biz>, John Thomas <jthomas@mediastorm.biz>, Simeng Sun <ssun@mediastorm.biz>, Mohammed Uddin <muddin@mediastorm.biz>
MIME-Version: 1.0
Content-type: text
Subject: Big Lots Rewards Sales in Store uploaded to LiveRamp 

Hi Mike,

The last week Big Lots Rewards Sales in Store uploaded to LiveRamp Bing & Google.

Thanks,
Jian
"""
smtpObj = smtplib.SMTP('smtp.gmail.com',587)
smtpObj.ehlo()
smtpObj.starttls()
smtpObj.login('jubapluscc@gmail.com','mfppxsfikqmazbqj')


sender="jubapluscc@gmail.com"
receivers=['jian@jubaplus.com','mmahler@mediastorm.biz','mchiu@mediastorm.biz', 'naldefri@mediastorm.biz', 'dbalboni@mediastorm.biz', 'zbrisker@mediastorm.biz', 'jthomas@mediastorm.biz', 'ssun@mediastorm.biz', 'muddin@mediastorm.biz','GAbouJaoude@mediastorm.biz']
try:
    smtpObj.sendmail(sender, receivers, message)         
    print("Successfully sent email")
except:
    print("Error: unable to send email")
print("done celery: "+ str(datetime.datetime.now()))


# In[ ]:





# In[ ]:




