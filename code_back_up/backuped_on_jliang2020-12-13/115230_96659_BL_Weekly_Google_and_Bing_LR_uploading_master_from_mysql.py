# Update mysql CRM table and use it to matching emails
# 20200114

import pandas as pd
import datetime
import os
import numpy as np
import hashlib
import gc
import logging
import sqlalchemy
import glob
BL_SQL_CONNECTION= 'mysql+pymysql://jian:JubaPlus-2017@localhost/BigLots' 
BL_engine = sqlalchemy.create_engine(
        BL_SQL_CONNECTION, 
        pool_recycle=1800
    )

logging.basicConfig(filename="/home/jian/BL_weekly_crontab/cron_2_Google_Bing_LR/log_BL_Weekly_Google_and_Bing_LR_uploading.log", level=logging.INFO)
logging.info("Started: "+str(datetime.datetime.now()))
def recursive_file_gen(my_root_dir):
    for root, dirs, files in os.walk(my_root_dir):
        for file in files:
            yield os.path.join(root, file)

thismonday=datetime.datetime.now().date()-datetime.timedelta(days=datetime.datetime.now().date().weekday())
# thismonday=datetime.date(2019,3,25)
print("thismonday", thismonday)
logging.info("thismonday: "+str(thismonday))

last_week_end_saturday=thismonday-datetime.timedelta(days=2)

writer_pather="/home/jian/celery/Bing_LiveRamp/output/"

posibble_recent_folder="/home/jian/BigLots/MediaStorm_"+str(last_week_end_saturday)+"/"
daily_files_recent=[x for x in list(recursive_file_gen(posibble_recent_folder)) if "Daily" in x]

if len(daily_files_recent)==1:
    daily_file_last_week=daily_files_recent[0]
    print("Good to load")
    logging.info(str(datetime.datetime.now())+": Good to load")
else:
    daily_file_last_week=np.nan
    print(str(datetime.datetime.now())+": Last week daily data not avaiable")
    logging.info(str(datetime.datetime.now())+": Last week daily data not avaiable")



df_daily_sales_last_week=pd.read_table(daily_file_last_week,sep="|",dtype=str)
df_daily_sales_last_week=df_daily_sales_last_week[df_daily_sales_last_week['location_id']!="6990"]
df_daily_sales_last_week['item_transaction_amt']=df_daily_sales_last_week['item_transaction_amt'].astype(float)
qc_weekly_sales=df_daily_sales_last_week.groupby(['location_id'])['item_transaction_amt'].sum().to_frame().reset_index().rename(columns={"item_transaction_amt":"sales_from_Daily"})
qc_weekly_trans=df_daily_sales_last_week[['location_id','transaction_dt','transaction_id','customer_id_hashed']].drop_duplicates()
qc_weekly_trans=qc_weekly_trans.groupby(['location_id'])['transaction_id'].count().to_frame().reset_index().rename(columns={"transaction_id":"trans_from_Daily"})
df_daily_sales_last_week=pd.merge(qc_weekly_sales,qc_weekly_trans,on="location_id",how="outer")

weekly_data_path=glob.glob("/home/jian/BigLots/MediaStorm_"+str(last_week_end_saturday)+"/*")
weekly_data_path=[x for x in weekly_data_path if "SalesWeekly" in x]
if len(weekly_data_path)!=1:
	print("Last weekly file not only 1")
	logging.info("Last weekly file not only 1")

else :
	print("Last weekly file good as 1")
	logging.info("Last weekly file good as 1")


weekly_data_path=glob.glob("/home/jian/BigLots/2020_by_weeks/MediaStorm_"+str(last_week_end_saturday)+"/*")
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


QC_df=pd.merge(Weekly_Data,df_daily_sales_last_week,on="location_id",how="outer")


QC_df['Sales_Diff']=(QC_df['gross_sales_amt']-QC_df['sales_from_Daily'])/QC_df['sales_from_Daily']
QC_df['Trans_Diff']=(QC_df['gross_transaction_cnt']-QC_df['trans_from_Daily'])/QC_df['gross_transaction_cnt']

print("1% store sales variances: "+str(QC_df[(QC_df['Sales_Diff'].apply(lambda x: np.abs(x)>0.01))].shape[0]))
print("4% store trans variances: "+str(QC_df[(QC_df['Trans_Diff'].apply(lambda x: np.abs(x)>0.04))].shape[0]))
logging.info("1% store sales variances: "+str(QC_df[(QC_df['Sales_Diff'].apply(lambda x: np.abs(x)>0.01))].shape[0]))
logging.info("4% store trans variances: "+str(QC_df[(QC_df['Trans_Diff'].apply(lambda x: np.abs(x)>0.04))].shape[0]))

# Above is the QC df, below is the update df


sales_daily_lastweek=pd.read_table(daily_file_last_week,sep="|",dtype=str,usecols=['location_id','customer_id_hashed','transaction_dt','item_transaction_amt'])
sales_daily_lastweek=sales_daily_lastweek[~pd.isnull(sales_daily_lastweek['customer_id_hashed'])]
sales_daily_lastweek=sales_daily_lastweek[sales_daily_lastweek['location_id']!="6990"]
sales_daily_lastweek['item_transaction_amt']=sales_daily_lastweek['item_transaction_amt'].astype(float)
sales_daily_lastweek_agg=sales_daily_lastweek.groupby(['customer_id_hashed','transaction_dt'])['item_transaction_amt'].sum().to_frame().reset_index()
sales_daily_lastweek_agg=sales_daily_lastweek_agg.rename(columns={"transaction_dt":"Timestamp","item_transaction_amt":"Conversion_Amount"})
sales_daily_lastweek_agg['Timestamp']=sales_daily_lastweek_agg['Timestamp'].apply(lambda x: datetime.datetime.strptime(x,"%Y-%m-%d").date())


###
all_master_file=pd.read_sql("select customer_id_hashed, email_address_hash, customer_zip_code from BL_Rewards_Master;",con=BL_engine)

print(all_master_file.shape)
print(len(all_master_file['customer_id_hashed'].unique()))
logging.info("all_master_file.shape: "+str(all_master_file.shape))
logging.info("len(all_master_file['customer_id_hashed'].unique()): "+str(len(all_master_file['customer_id_hashed'].unique())))

all_master_file=all_master_file.drop_duplicates('customer_id_hashed')

sales_daily_lastweek_agg=pd.merge(sales_daily_lastweek_agg,all_master_file,on="customer_id_hashed",how="left").rename(columns={"email_address_hash":"Email_1","customer_zip_code":"Zip"})

print("Null Email rows excluded: "+str(sales_daily_lastweek_agg[pd.isnull(sales_daily_lastweek_agg['Email_1'])].shape[0]))
print(sales_daily_lastweek_agg.shape[0]/sales_daily_lastweek_agg[pd.isnull(sales_daily_lastweek_agg['Email_1'])].shape[0])

logging.info("Null Email rows excluded: "+str(sales_daily_lastweek_agg[pd.isnull(sales_daily_lastweek_agg['Email_1'])].shape[0]))
logging.info(str(sales_daily_lastweek_agg.shape[0]/sales_daily_lastweek_agg[pd.isnull(sales_daily_lastweek_agg['Email_1'])].shape[0]))

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

writer_pather="/home/jian/celery/Bing_LiveRamp/output/"

local_path=writer_pather+"/BL_LR_BingStoreSales_"+str(data_min_date)+"_"+str(data_max_date)+"_JL_"+str(datetime.datetime.now().date())+".txt"

sales_daily_lastweek_agg.to_csv(local_path,index=False,sep="|")


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
    logging.info("Successfully sent email")
except:
    print("Error: unable to send email")
    logging.info("Error: unable to send email")
print("done celery: "+ str(datetime.datetime.now()))
logging.info("done celery: "+ str(datetime.datetime.now()))


